package erasure

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/lojadopocket/gostore/internal/configstore"
	"github.com/lojadopocket/gostore/internal/object"
	"github.com/lojadopocket/gostore/internal/storage"
)

// blockingDisk wraps a Disk and fails all writes while "down" is set.
type blockingDisk struct {
	Disk
	down atomic.Bool
}

func (b *blockingDisk) WriteAll(ctx context.Context, bucket, obj string, data []byte) error {
	if b.down.Load() {
		return io.ErrClosedPipe
	}
	return b.Disk.WriteAll(ctx, bucket, obj, data)
}

func (b *blockingDisk) CreateFile(ctx context.Context, bucket, obj string, size int64, r io.Reader) error {
	if b.down.Load() {
		_, _ = io.Copy(io.Discard, r)
		return io.ErrClosedPipe
	}
	return b.Disk.CreateFile(ctx, bucket, obj, size, r)
}

func (b *blockingDisk) RenameDir(ctx context.Context, sb, so, db, do string) error {
	if b.down.Load() {
		return io.ErrClosedPipe
	}
	return b.Disk.RenameDir(ctx, sb, so, db, do)
}

func TestMRFQueuesAndHealsPartialWrite(t *testing.T) {
	base := t.TempDir()
	raw := make([]Disk, 6)
	roots := make([]string, 6)
	var bd *blockingDisk
	for i := 0; i < 6; i++ {
		roots[i] = filepath.Join(base, "d", string(rune('a'+i)))
		d, err := storage.OpenLocalDisk(roots[i], 0, i)
		if err != nil {
			t.Fatal(err)
		}
		if i == 5 {
			bd = &blockingDisk{Disk: d}
			raw[i] = bd
		} else {
			raw[i] = d
		}
	}
	p, err := FromDisks(raw)
	if err != nil {
		t.Fatal(err)
	}
	p.EnableMRF(configstore.NewDir(t.TempDir()))

	if err := p.MakeBucket(ctx(), "buck", object.MakeBucketOptions{}); err != nil {
		t.Fatal(err)
	}

	// One disk is down during the write: quorum (4) is met, disk f misses it.
	bd.down.Store(true)
	data := make([]byte, 300*1024) // > inline threshold, real shards
	for i := range data {
		data[i] = byte(i)
	}
	put(t, p, "buck", "obj", data)
	bd.down.Store(false)

	// The object must be recorded for background heal.
	if _, ok := p.mrf.pending["buck/obj"]; !ok {
		t.Fatalf("partial write not queued in MRF: %v", p.mrf.pending)
	}
	// Disk f has no shard yet.
	if _, err := os.Stat(filepath.Join(roots[5], "buck", "obj", "part.00001")); !os.IsNotExist(err) {
		t.Fatalf("expected missing shard on downed disk, err=%v", err)
	}

	// Run a heal pass; the entry should drain and the shard should appear.
	p.drainMRF(ctx())

	if _, ok := p.mrf.pending["buck/obj"]; ok {
		t.Fatalf("MRF entry not drained after successful heal")
	}
	if _, err := os.Stat(filepath.Join(roots[5], "buck", "obj", "part.00001")); err != nil {
		t.Fatalf("shard not healed onto recovered disk: %v", err)
	}
	if got := get(t, p, "buck", "obj"); len(got) != len(data) {
		t.Fatalf("post-heal read size %d want %d", len(got), len(data))
	}
}
