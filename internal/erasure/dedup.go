package erasure

import (
	"context"
	"encoding/json"
	"path"
	"time"
)

// Content-addressed dedup (opt-in, GOSTORE_DEDUP=1). When two objects have
// byte-identical plaintext they share one set of shard files under
// .gostore.sys/cas/<sha256>/; each object's xl.meta just carries DataRef.
// There is no refcount — deletion is mark-and-sweep GC (GCDedup) with a grace
// period, which is race-free (the GC always has a complete reference view)
// where a refcount is not.
//
// Scope: erasure backend, non-SSE, non-inline objects only.

const casPrefix = ".gostore.sys/cas"

var dedupEnabled bool

// SetDedup toggles content-addressed dedup process-wide (call at startup).
func SetDedup(on bool) { dedupEnabled = on }

// DedupEnabled reports the current setting.
func DedupEnabled() bool { return dedupEnabled }

func casDir(h string) string { return path.Join(casPrefix, h) }

// identityDist is the shard distribution used for every CAS blob: shard j on
// disk j. A shared blob can't be placed by a per-key hash.
func identityDist(n int) []int {
	d := make([]int, n)
	for i := range d {
		d[i] = i
	}
	return d
}

// casMarker is a tiny file written into every CAS dir: it proves the blob is
// fully installed and carries the age used by GC.
type casMarker struct {
	Size        int64     `json:"size"`
	Parts       int       `json:"parts"`
	InstalledAt time.Time `json:"installedAt"`
}

// casExists reports whether the CAS blob for h is installed (its marker is
// readable on a read quorum of disks).
func (s *Set) casExists(ctx context.Context, h string) bool {
	ok := 0
	for _, d := range s.disks {
		if !d.IsOnline() {
			continue
		}
		if _, err := d.ReadAll(ctx, "", path.Join(casDir(h), "cas.json")); err == nil {
			ok++
		}
	}
	return ok >= s.readQuorum()
}

// installCAS moves the just-encoded shard files from staging into the CAS
// namespace and writes the marker. Returns true on a write quorum.
func (s *Set) installCAS(ctx context.Context, staging, h string, size int64, parts int) bool {
	mk, _ := json.Marshal(casMarker{Size: size, Parts: parts, InstalledAt: time.Now().UTC()})
	_ = s.forEachDisk(func(d Disk) error {
		return d.WriteAll(ctx, "", path.Join(staging, "cas.json"), mk)
	})
	errs := s.forEachDisk(func(d Disk) error {
		return d.RenameDir(ctx, "", staging, "", casDir(h))
	})
	if okCount(errs) < s.writeQuorum() {
		_ = s.forEachDisk(func(d Disk) error { return d.Delete(ctx, "", casDir(h), true) })
		return false
	}
	return true
}

// referencedCAS walks every object in every bucket and returns the set of CAS
// hashes still pointed at by some xl.meta.
func (p *Pool) referencedCAS(ctx context.Context) (map[string]struct{}, error) {
	refs := map[string]struct{}{}
	buckets, err := p.sets[0].ListBuckets(ctx)
	if err != nil {
		return nil, err
	}
	for _, b := range buckets {
		for _, set := range p.sets {
			keys, err := set.walkKeys(ctx, b.Name)
			if err != nil {
				continue
			}
			for _, k := range keys {
				if m, err := set.readMeta(ctx, b.Name, k); err == nil && m.DataRef != "" {
					refs[m.DataRef] = struct{}{}
				}
			}
		}
	}
	return refs, nil
}

// GCDedup deletes CAS blobs no object references and that were installed more
// than grace ago (the grace window avoids racing a PUT that has installed the
// blob but not yet committed its xl.meta). Returns the number removed.
func (p *Pool) GCDedup(ctx context.Context, grace time.Duration) (int, error) {
	if !dedupEnabled {
		return 0, nil
	}
	refs, err := p.referencedCAS(ctx)
	if err != nil {
		return 0, err
	}
	removed := 0
	for _, set := range p.sets {
		var names []string
		for _, d := range set.disks {
			if !d.IsOnline() {
				continue
			}
			if ns, err := d.ListDir(ctx, "", casPrefix); err == nil {
				names = ns
				break
			}
		}
		for _, name := range names {
			h := path.Clean(name)
			h = pathBase(h)
			if h == "" || h == "." {
				continue
			}
			if _, live := refs[h]; live {
				continue
			}
			// Age check via the marker.
			old := true
			for _, d := range set.disks {
				b, err := d.ReadAll(ctx, "", path.Join(casDir(h), "cas.json"))
				if err != nil {
					continue
				}
				var mk casMarker
				if json.Unmarshal(b, &mk) == nil && time.Since(mk.InstalledAt) < grace {
					old = false
				}
				break
			}
			if !old {
				continue
			}
			if okCount(set.forEachDisk(func(d Disk) error {
				return d.Delete(ctx, "", casDir(h), true)
			})) >= set.writeQuorum() {
				removed++
			}
		}
	}
	return removed, nil
}

func pathBase(p string) string {
	for i := len(p) - 1; i >= 0; i-- {
		if p[i] == '/' {
			return p[i+1:]
		}
	}
	return p
}
