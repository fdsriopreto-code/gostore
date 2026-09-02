package erasure

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"path"
	"sort"
	"time"

	"github.com/lojadopocket/gostore/internal/configstore"
	"github.com/lojadopocket/gostore/internal/logger"
	"github.com/lojadopocket/gostore/internal/object"
)

// Pool decommission & rebalance. A set can be marked "draining": new writes
// route to the remaining sets (setFor excludes it) while a background worker
// relocates every object off it; when it's empty the operator can physically
// remove those disks. Rebalance is the same machinery applied to *all* sets —
// it relocates any object that now hashes to a different set than the one it
// currently lives on (e.g. after the set count changed).
//
// The layout (which set indices are draining) is persisted via configstore so
// it survives a restart and is visible cluster-wide.

const poolLayoutKey = "pool/layout.json"

type poolLayoutDoc struct {
	Draining  []int     `json:"draining"`
	UpdatedAt time.Time `json:"updatedAt"`
}

// decomProgress is plain data; access is guarded by Pool.progMu.
type decomProgress struct {
	Running   bool      `json:"running"`
	Kind      string    `json:"kind"` // "decommission" | "rebalance" | ""
	SetIndex  int       `json:"setIndex"`
	Moved     int64     `json:"moved"`
	Failed    int64     `json:"failed"`
	Started   time.Time `json:"started"`
	Finished  time.Time `json:"finished"`
	LastError string    `json:"lastError,omitempty"`
}

// PoolStatus is the introspection payload for the admin API.
type PoolStatus struct {
	Sets     int           `json:"sets"`
	Draining []int         `json:"draining"`
	Progress decomProgress `json:"progress"`
}

// PoolStatus reports the current layout and any running job.
func (p *Pool) PoolStatus() PoolStatus {
	p.layoutMu.RLock()
	dr := make([]int, 0, len(p.draining))
	for i := range p.draining {
		dr = append(dr, i)
	}
	p.layoutMu.RUnlock()
	sort.Ints(dr)
	p.progMu.Lock()
	prog := p.decom
	p.progMu.Unlock()
	return PoolStatus{Sets: len(p.sets), Draining: dr, Progress: prog}
}

// LoadLayout attaches the config store and restores a persisted layout.
func (p *Pool) LoadLayout(be configstore.Backend) {
	p.cfgStore = be
	p.draining = map[int]bool{}
	b, err := be.ReadConfig(context.Background(), poolLayoutKey)
	if err != nil {
		return
	}
	var doc poolLayoutDoc
	if json.Unmarshal(b, &doc) != nil {
		return
	}
	for _, i := range doc.Draining {
		if i >= 0 && i < len(p.sets) {
			p.draining[i] = true
		}
	}
	p.hasInactive.Store(len(p.draining) > 0)
	if len(p.draining) > 0 {
		logger.Info("pool: restored draining layout", "sets", doc.Draining)
	}
}

func (p *Pool) saveLayout() {
	if p.cfgStore == nil {
		return
	}
	p.layoutMu.RLock()
	dr := make([]int, 0, len(p.draining))
	for i := range p.draining {
		dr = append(dr, i)
	}
	p.layoutMu.RUnlock()
	sort.Ints(dr)
	b, _ := json.Marshal(poolLayoutDoc{Draining: dr, UpdatedAt: time.Now().UTC()})
	_ = p.cfgStore.WriteConfig(context.Background(), poolLayoutKey, b)
}

// activeSets returns the sets that are not draining, in index order.
func (p *Pool) activeSets() []*Set {
	p.layoutMu.RLock()
	defer p.layoutMu.RUnlock()
	out := make([]*Set, 0, len(p.sets))
	for i, s := range p.sets {
		if !p.draining[i] {
			out = append(out, s)
		}
	}
	return out
}

// locate returns the set that actually holds bucket/key. Fast path (no
// decommission in flight) is just setFor; otherwise it probes the other sets.
func (p *Pool) locate(ctx context.Context, bucket, key string) *Set {
	s := p.setFor(key)
	if !p.hasInactive.Load() {
		return s
	}
	if _, err := s.readMeta(ctx, bucket, key); err == nil {
		return s
	}
	if v, err := s.readVlog(ctx, bucket, key); err == nil && len(v) > 0 {
		return s
	}
	for _, cand := range p.sets {
		if cand == s {
			continue
		}
		if _, err := cand.readMeta(ctx, bucket, key); err == nil {
			return cand
		}
		if v, err := cand.readVlog(ctx, bucket, key); err == nil && len(v) > 0 {
			return cand
		}
	}
	return s
}

// Decommission marks set setIdx draining and starts the background move.
func (p *Pool) Decommission(ctx context.Context, setIdx int) error {
	if setIdx < 0 || setIdx >= len(p.sets) {
		return fmt.Errorf("erasure: set index %d out of range (have %d)", setIdx, len(p.sets))
	}
	if len(p.sets) < 2 {
		return fmt.Errorf("erasure: cannot decommission the only set")
	}
	if !p.decomMu.TryLock() {
		return fmt.Errorf("erasure: a decommission/rebalance is already running")
	}

	p.layoutMu.Lock()
	if p.draining == nil {
		p.draining = map[int]bool{}
	}
	activeCount := len(p.sets) - len(p.draining)
	if p.draining[setIdx] {
		p.layoutMu.Unlock()
		p.decomMu.Unlock()
		return fmt.Errorf("erasure: set %d is already draining", setIdx)
	}
	if activeCount-1 < 1 {
		p.layoutMu.Unlock()
		p.decomMu.Unlock()
		return fmt.Errorf("erasure: draining set %d would leave no active set", setIdx)
	}
	p.draining[setIdx] = true
	p.hasInactive.Store(true)
	p.layoutMu.Unlock()
	p.saveLayout()

	p.progMu.Lock()
	p.decom = decomProgress{Running: true, Kind: "decommission", SetIndex: setIdx, Started: time.Now().UTC()}
	p.progMu.Unlock()

	go func() {
		defer p.decomMu.Unlock()
		p.runDecommission(context.Background(), setIdx)
	}()
	return nil
}

// Rebalance relocates every object that no longer hashes to the set it lives
// on (across all active sets). Run it after changing the set count.
func (p *Pool) Rebalance(ctx context.Context) error {
	if len(p.sets) < 2 {
		return fmt.Errorf("erasure: nothing to rebalance with a single set")
	}
	if !p.decomMu.TryLock() {
		return fmt.Errorf("erasure: a decommission/rebalance is already running")
	}
	p.progMu.Lock()
	p.decom = decomProgress{Running: true, Kind: "rebalance", SetIndex: -1, Started: time.Now().UTC()}
	p.progMu.Unlock()
	go func() {
		defer p.decomMu.Unlock()
		p.runRebalance(context.Background())
	}()
	return nil
}

func (p *Pool) finishJob(err error) {
	p.progMu.Lock()
	p.decom.Running = false
	p.decom.Finished = time.Now().UTC()
	if err != nil {
		p.decom.LastError = err.Error()
	}
	moved, failed, kind := p.decom.Moved, p.decom.Failed, p.decom.Kind
	p.progMu.Unlock()
	logger.Info("pool: "+kind+" complete", "moved", moved, "failed", failed, "err", err)
}

func (p *Pool) bump(moved bool) {
	p.progMu.Lock()
	if moved {
		p.decom.Moved++
	} else {
		p.decom.Failed++
	}
	p.progMu.Unlock()
}

func (p *Pool) runDecommission(ctx context.Context, setIdx int) {
	src := p.sets[setIdx]
	buckets, err := src.ListBuckets(ctx)
	if err != nil {
		p.finishJob(err)
		return
	}
	for _, b := range buckets {
		if b.Name == "" {
			continue
		}
		keys := p.keysOnSet(ctx, src, b.Name)
		for _, k := range keys {
			if ctx.Err() != nil {
				p.finishJob(ctx.Err())
				return
			}
			dst := p.setFor(k) // hashes over the remaining active sets
			if dst == src {
				// hash still maps here but the set is draining — force onto
				// the first active set instead.
				if act := p.activeSets(); len(act) > 0 {
					dst = act[0]
				}
			}
			if err := p.moveObject(ctx, b.Name, k, src, dst); err != nil {
				logger.Warn("pool: decommission move failed", "bucket", b.Name, "key", k, "err", err)
				p.bump(false)
				continue
			}
			p.bump(true)
		}
	}
	p.invalidateList("")
	p.finishJob(nil)
}

func (p *Pool) runRebalance(ctx context.Context) {
	buckets, err := p.sets[0].ListBuckets(ctx)
	if err != nil {
		p.finishJob(err)
		return
	}
	for _, b := range buckets {
		if b.Name == "" {
			continue
		}
		for i, s := range p.sets {
			keys := p.keysOnSet(ctx, s, b.Name)
			for _, k := range keys {
				if ctx.Err() != nil {
					p.finishJob(ctx.Err())
					return
				}
				dst := p.setFor(k)
				if dst == s {
					continue
				}
				if err := p.moveObject(ctx, b.Name, k, p.sets[i], dst); err != nil {
					logger.Warn("pool: rebalance move failed", "bucket", b.Name, "key", k, "err", err)
					p.bump(false)
					continue
				}
				p.bump(true)
			}
		}
	}
	p.invalidateList("")
	p.finishJob(nil)
}

// keysOnSet returns the object keys physically present on one set (live
// objects + keys that only have a version log / delete marker).
func (p *Pool) keysOnSet(ctx context.Context, s *Set, bucket string) []string {
	seen := map[string]bool{}
	var out []string
	live, _ := s.walkKeys(ctx, bucket)
	vk, _ := s.walkVlogKeys(ctx, bucket)
	for _, k := range append(live, vk...) {
		if !seen[k] {
			seen[k] = true
			out = append(out, k)
		}
	}
	sort.Strings(out)
	return out
}

// moveObject relocates one object (all versions) from `from` to `to`, then
// deletes it from `from`.
func (p *Pool) moveObject(ctx context.Context, bucket, key string, from, to *Set) error {
	if from == to {
		return nil
	}
	lk := p.NewNSLock(bucket, key)
	c, _ := lk.GetLock(ctx, 0)
	defer lk.Unlock(c)

	vlog, _ := from.readVlog(ctx, bucket, key)
	if len(vlog) == 0 {
		// plain (unversioned) object. If a client already wrote it to the
		// target while we waited for the lock, don't clobber — just drop the
		// stale source copy.
		if _, err := to.readMeta(ctx, bucket, key); err == nil {
			return from.deleteObject(ctx, bucket, key)
		}
		if err := p.copyCurrent(ctx, bucket, key, from, to); err != nil {
			return err
		}
		return from.deleteObject(ctx, bucket, key)
	}

	// versioned: replay every entry oldest -> newest onto `to`
	for _, e := range vlog {
		if e.DeleteMarker {
			if _, err := p.deleteVersionedOn(ctx, to, bucket, key, object.ObjectOptions{Versioned: true}); err != nil {
				return err
			}
			continue
		}
		dir, _, derr := from.resolveVersionDir(ctx, bucket, key, e.ID)
		if derr != nil {
			return derr
		}
		rc, size, m, gerr := p.getPlain(ctx, from, bucket, dir)
		if gerr != nil {
			return gerr
		}
		opts := object.ObjectOptions{
			Versioned:       true,
			UserDefined:     rebuildUserDefined(m),
			MTime:           e.ModTime,
			LockMode:        e.LockMode,
			LockRetainUntil: e.RetainUntil,
		}
		if e.LegalHold {
			opts.LockLegalHold = "ON"
		}
		pr := object.NewPutObjReader(rc, size, size)
		_, perr := p.putObjectVersionedOn(ctx, to, bucket, key, pr, opts)
		_ = rc.Close()
		if perr != nil {
			return perr
		}
	}

	// drop the source copy entirely
	_ = from.forEachDisk(func(d Disk) error { return d.Delete(ctx, bucket, key, true) })
	_ = from.forEachDisk(func(d Disk) error {
		return d.Delete(ctx, bucket, vsDir(key, ""), true)
	})
	_ = from.forEachDisk(func(d Disk) error {
		return d.Delete(ctx, bucket, vlogPath(key), true)
	})
	return nil
}

// copyCurrent moves an unversioned object's bytes + metadata to `to`.
func (p *Pool) copyCurrent(ctx context.Context, bucket, key string, from, to *Set) error {
	rc, size, m, err := p.getPlain(ctx, from, bucket, key)
	if err != nil {
		return err
	}
	defer rc.Close()

	var sp *sseParams
	if m.SSE != "" && p.kms != nil {
		if sp, err = p.newSSEParams(); err != nil {
			return err
		}
	}
	meta, err := to.putObjectSSE(ctx, bucket, key,
		[]partSource{{Number: 1, Size: size, Reader: rc}}, userMetaFromMeta(m), sp, false)
	if err != nil {
		return err
	}
	if !m.ModTime.IsZero() {
		meta.ModTime = m.ModTime
		mb, _ := meta.marshal()
		_ = to.forEachDisk(func(d Disk) error {
			return d.WriteAll(ctx, bucket, path.Join(key, metaFile), mb)
		})
	}
	return nil
}

// getPlain returns a plaintext reader for the object (or version) at
// bucket/dir on `set`, decrypting transparently if it was SSE.
func (p *Pool) getPlain(ctx context.Context, set *Set, bucket, dir string) (io.ReadCloser, int64, *XLMeta, error) {
	m, err := set.readMeta(ctx, bucket, dir)
	if err != nil {
		return nil, 0, nil, err
	}
	if m.SSE != "" {
		if p.kms == nil {
			return nil, 0, nil, object.ErrCorruptedData
		}
		logical := m.PlainSize
		pr, pw := io.Pipe()
		go func() { _ = pw.CloseWithError(set.getObject(ctx, bucket, dir, 0, m.Size, pw)) }()
		dr, derr := p.decryptReader(m, pr, 0, logical)
		if derr != nil {
			_ = pr.CloseWithError(derr)
			return nil, 0, nil, derr
		}
		return readCloser2{dr, pr}, logical, m, nil
	}
	if m.Compressed != "" {
		pr, pw := io.Pipe()
		go func() { _ = pw.CloseWithError(set.getObject(ctx, bucket, dir, 0, m.Size, pw)) }()
		dr, derr := zstdDecompressRange(pr, 0, m.PlainSize)
		if derr != nil {
			_ = pr.CloseWithError(derr)
			return nil, 0, nil, derr
		}
		return dr, m.PlainSize, m, nil
	}
	pr, pw := io.Pipe()
	go func() { _ = pw.CloseWithError(set.getObject(ctx, bucket, dir, 0, m.Size, pw)) }()
	return pr, m.Size, m, nil
}

type readCloser2 struct {
	io.Reader
	c io.Closer
}

func (r readCloser2) Close() error { return r.c.Close() }

func userMetaFromMeta(m *XLMeta) userMeta {
	return userMeta{
		contentType: m.ContentType,
		contentEnc:  m.ContentEnc,
		user:        m.UserMeta,
		tags:        m.UserTags,
		compress:    m.Compressed != "", // keep it compressed after the move
	}
}

func rebuildUserDefined(m *XLMeta) map[string]string {
	ud := map[string]string{}
	for k, v := range m.UserMeta {
		ud[k] = v
	}
	if m.ContentType != "" {
		ud["content-type"] = m.ContentType
	}
	if m.SSE != "" {
		ud["x-amz-server-side-encryption"] = "AES256"
	}
	return ud
}
