package erasure

import (
	"context"
	"crypto/sha256"
	"errors"
	"path"
	"sync"

	"github.com/lojadopocket/gostore/internal/configstore"
	"github.com/lojadopocket/gostore/internal/storage"
)

// The erasure Pool implements configstore.Backend: IAM and per-bucket config
// are replicated to *every* disk in *every* set (bucket "", object
// ".gostore.sys/<key>") and read back majority-wins. In cluster mode the set
// spans every node's disks, so config written on one node is durably visible
// on all of them — this is what makes multi-node IAM a single source of
// truth instead of per-node JSON.

var _ configstore.Backend = (*Pool)(nil)

const configReservedBucket = ""

func configObjectPath(key string) string {
	return path.Join(".gostore.sys", key)
}

// allDisks returns every disk across every set in the pool.
func (p *Pool) allDisks() []Disk {
	var out []Disk
	for _, s := range p.sets {
		out = append(out, s.disks...)
	}
	return out
}

func configQuorum(n int) int { return n/2 + 1 }

// ReadConfig reads key from all disks and returns the majority content.
func (p *Pool) ReadConfig(ctx context.Context, key string) ([]byte, error) {
	disks := p.allDisks()
	obj := configObjectPath(key)

	blobs := make([][]byte, len(disks))
	var wg sync.WaitGroup
	for i, d := range disks {
		wg.Add(1)
		go func(i int, d Disk) {
			defer wg.Done()
			b, err := d.ReadAll(ctx, configReservedBucket, obj)
			if err == nil {
				blobs[i] = b
			}
		}(i, d)
	}
	wg.Wait()

	tally := map[[32]byte]int{}
	sample := map[[32]byte][]byte{}
	found := 0
	for _, b := range blobs {
		if b == nil {
			continue
		}
		found++
		h := sha256.Sum256(b)
		tally[h]++
		sample[h] = b
	}
	if found == 0 {
		return nil, configstore.ErrNotFound
	}
	var best [32]byte
	bestN := 0
	for h, c := range tally {
		if c > bestN {
			best, bestN = h, c
		}
	}
	winner := sample[best]

	// Read-repair: if we have a solid quorum winner but some disk is missing
	// or stale, write the winner back (once at a time per key) so a later
	// quorum failure can't lose the config.
	if bestN >= configQuorum(len(disks)) && bestN < len(disks) && repairGate(key) {
		repairWG.Add(1)
		go func(w []byte) {
			defer repairWG.Done()
			defer repairDone(key)
			for i, d := range disks {
				if len(blobs[i]) != len(w) || sha256.Sum256(blobs[i]) != best {
					_ = d.WriteAll(context.Background(), configReservedBucket, obj, w)
				}
			}
		}(append([]byte(nil), winner...))
	}
	return winner, nil
}

var (
	repairMu sync.Mutex
	repairIP = map[string]bool{}
	repairWG sync.WaitGroup
)

// WaitConfigRepair blocks until every in-flight config read-repair goroutine
// has finished writing. Exposed for tests and orderly shutdown so async disk
// writes don't outlive the caller.
func WaitConfigRepair() { repairWG.Wait() }

func repairGate(key string) bool {
	repairMu.Lock()
	defer repairMu.Unlock()
	if repairIP[key] {
		return false
	}
	repairIP[key] = true
	return true
}
func repairDone(key string) {
	repairMu.Lock()
	delete(repairIP, key)
	repairMu.Unlock()
}

// WriteConfig replicates data to every disk; succeeds on a write quorum.
func (p *Pool) WriteConfig(ctx context.Context, key string, data []byte) error {
	disks := p.allDisks()
	obj := configObjectPath(key)

	// Best-effort ensure the reserved namespace exists (no-op when bucket "").
	errs := make([]error, len(disks))
	var wg sync.WaitGroup
	for i, d := range disks {
		wg.Add(1)
		go func(i int, d Disk) {
			defer wg.Done()
			errs[i] = d.WriteAll(ctx, configReservedBucket, obj, data)
		}(i, d)
	}
	wg.Wait()

	if okCount(errs) < configQuorum(len(disks)) {
		return ErrWriteQuorum
	}
	return nil
}

// DeleteConfig removes key from every disk. Missing is not an error.
func (p *Pool) DeleteConfig(ctx context.Context, key string) error {
	disks := p.allDisks()
	obj := configObjectPath(key)
	_ = p.forEachAllDisks(func(d Disk) error {
		err := d.Delete(ctx, configReservedBucket, obj, false)
		if errors.Is(err, storage.ErrFileNotFound) {
			return nil
		}
		return err
	}, disks)
	return nil
}

// ListConfig returns keys under prefix, read from the first disk that answers.
func (p *Pool) ListConfig(ctx context.Context, prefix string) ([]string, error) {
	dir := configObjectPath(prefix)
	for _, d := range p.allDisks() {
		if !d.IsOnline() {
			continue
		}
		names, err := d.ListDir(ctx, configReservedBucket, dir)
		if err != nil {
			continue
		}
		out := make([]string, 0, len(names))
		for _, n := range names {
			out = append(out, path.Join(prefix, n))
		}
		return out, nil
	}
	return nil, nil
}

func (p *Pool) forEachAllDisks(fn func(Disk) error, disks []Disk) []error {
	errs := make([]error, len(disks))
	var wg sync.WaitGroup
	for i, d := range disks {
		wg.Add(1)
		go func(i int, d Disk) {
			defer wg.Done()
			errs[i] = fn(d)
		}(i, d)
	}
	wg.Wait()
	return errs
}
