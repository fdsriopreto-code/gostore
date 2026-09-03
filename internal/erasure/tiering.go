package erasure

import (
	"context"
	"errors"
	"io"
	"path"
	"sync"

	"github.com/lojadopocket/gostore/internal/object"
	"github.com/lojadopocket/gostore/internal/remotes3"
)

// Lifecycle tiering: a rule can move cold objects to a remote S3-compatible
// backend (B2 / R2 / Wasabi / another gostore). The local object becomes a
// stub xl.meta (no shard files); a GET streams the bytes back from the
// remote. Tier targets are registered at startup from GOSTORE_TIER_<NAME>.

var errUnknownTier = errors.New("erasure: unknown tier target")

var (
	tierMu     sync.RWMutex
	tierByName = map[string]*remotes3.Client{}
)

// RegisterTier adds/replaces a named remote tier target.
func RegisterTier(name string, c *remotes3.Client) {
	tierMu.Lock()
	tierByName[name] = c
	tierMu.Unlock()
}

// TierNames returns the configured tier names.
func TierNames() []string {
	tierMu.RLock()
	defer tierMu.RUnlock()
	out := make([]string, 0, len(tierByName))
	for n := range tierByName {
		out = append(out, n)
	}
	return out
}

func tierClient(name string) *remotes3.Client {
	tierMu.RLock()
	defer tierMu.RUnlock()
	return tierByName[name]
}

// tierObjectKey is the remote key an object lands under: "<bucket>/<key>".
func tierObjectKey(bucket, key string) string { return bucket + "/" + key }

// TransitionObject uploads bucket/key's current bytes to the named tier and
// rewrites its xl.meta as a stub. It takes the object's write lock. A no-op
// if the object is already tiered, missing, SSE (can't stub-and-restream an
// encrypted object cleanly for MVP), or the tier is unknown.
func (p *Pool) TransitionObject(ctx context.Context, bucket, key, tierName string) error {
	cl := tierClient(tierName)
	if cl == nil {
		return errUnknownTier
	}
	set := p.locate(ctx, bucket, key)

	lk := p.NewNSLock(bucket, key)
	c, _ := lk.GetLock(ctx, 0)
	defer lk.Unlock(c)

	m, err := set.readMeta(ctx, bucket, key)
	if err != nil {
		return mapErr(err)
	}
	if m.Tier != "" || m.SSE != "" || m.DataRef != "" {
		return nil // already tiered, or not eligible
	}

	// Stream the (possibly compressed/decoded) plaintext to the remote.
	rk := tierObjectKey(bucket, key)
	pr, pw := io.Pipe()
	go func() {
		gr, gerr := p.GetObjectNInfo(ctx, bucket, key, nil, nil, object.ObjectOptions{})
		if gerr != nil {
			_ = pw.CloseWithError(gerr)
			return
		}
		_, cerr := io.Copy(pw, gr)
		gr.Close()
		_ = pw.CloseWithError(cerr)
	}()
	logical := m.Size
	if m.Compressed != "" {
		logical = m.PlainSize
	}
	if err := cl.Put(ctx, rk, pr, logical, m.ContentType); err != nil {
		return err
	}

	// Rewrite xl.meta as a stub: keep identity, drop the data location.
	stub := *m
	stub.Tier = tierName
	stub.TierKey = rk
	stub.DataRef = ""
	stub.Compressed = ""
	stub.Inline = nil
	stub.Size = logical
	stub.PlainSize = 0
	stub.Erasure.Distribution = nil

	// Drop the local shard files first (per part), then overwrite xl.meta in
	// place with the stub — the object stays readable (from the remote) once
	// the stub lands; if we crash mid-way, the old meta still points at
	// now-missing shards and the next scan retries the transition.
	parts := m.Parts
	_ = set.forEachDisk(func(d Disk) error {
		for _, pm := range parts {
			_ = d.Delete(ctx, bucket, path.Join(key, partFile(pm.Number)), false)
		}
		return nil
	})
	stub.Revision = m.Revision + 1
	sb, _ := (&stub).marshal()
	if okCount(set.forEachDisk(func(d Disk) error {
		return d.WriteAll(ctx, bucket, path.Join(key, metaFile), sb)
	})) < set.writeQuorum() {
		return ErrWriteQuorum
	}
	return nil
}

// getTiered streams a tiered object's requested byte range from its remote.
func (p *Pool) getTiered(ctx context.Context, m *XLMeta, oi object.ObjectInfo, off, length int64) (*object.GetObjectReader, error) {
	cl := tierClient(m.Tier)
	if cl == nil {
		return nil, object.ErrCorruptedData
	}
	rc, _, err := cl.Get(ctx, m.TierKey)
	if err != nil {
		return nil, err
	}
	var r io.Reader = rc
	if off > 0 {
		if _, e := io.CopyN(io.Discard, r, off); e != nil {
			rc.Close()
			return nil, e
		}
	}
	if length >= 0 {
		r = io.LimitReader(r, length)
	}
	return &object.GetObjectReader{ObjInfo: oi, ReadCloser: readCloser{r, rc}}, nil
}
