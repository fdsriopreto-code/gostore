package erasure

import (
	"bytes"
	"crypto/rand"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/lojadopocket/gostore/internal/object"
)

func TestInlineSmallObjectLayout(t *testing.T) {
	p, roots := newTestPool(t, 6)
	_ = p.MakeBucket(ctx(), "buck", object.MakeBucketOptions{})

	data := make([]byte, 4000)
	_, _ = rand.Read(data)
	put(t, p, "buck", "small", data)

	// No shard files: every disk has xl.meta and nothing else under the key.
	for _, r := range roots {
		if _, err := os.Stat(filepath.Join(r, "buck", "small", "xl.meta")); err != nil {
			t.Fatalf("xl.meta missing on %s: %v", r, err)
		}
		if _, err := os.Stat(filepath.Join(r, "buck", "small", "part.00001")); !os.IsNotExist(err) {
			t.Fatalf("inline object should have no part file on %s (err=%v)", r, err)
		}
	}

	if got := get(t, p, "buck", "small"); !bytes.Equal(got, data) {
		t.Fatalf("inline round trip mismatch")
	}
}

func TestInlineSurvivesDiskLoss(t *testing.T) {
	p, roots := newTestPool(t, 6)
	_ = p.MakeBucket(ctx(), "buck", object.MakeBucketOptions{})
	data := make([]byte, 9001)
	_, _ = rand.Read(data)
	put(t, p, "buck", "k", data)

	// Wipe 3 of 6 disks (parity count): inline data rides in xl.meta, which
	// every disk holds a full copy of, so the same read quorum that recovers
	// metadata recovers the object — no shard reconstruction needed.
	for _, r := range roots[:3] {
		if err := os.RemoveAll(filepath.Join(r, "buck", "k")); err != nil {
			t.Fatal(err)
		}
	}
	if got := get(t, p, "buck", "k"); !bytes.Equal(got, data) {
		t.Fatalf("inline object lost after 3/6 disk wipe")
	}
}

func TestInlineRangeRead(t *testing.T) {
	p, _ := newTestPool(t, 4)
	_ = p.MakeBucket(ctx(), "buck", object.MakeBucketOptions{})
	data := make([]byte, 5000)
	_, _ = rand.Read(data)
	put(t, p, "buck", "k", data)

	gr, err := p.GetObjectNInfo(ctx(), "buck", "k",
		&object.HTTPRangeSpec{Start: 1000, End: 1999}, nil, object.ObjectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(gr)
	gr.Close()
	if !bytes.Equal(got, data[1000:2000]) {
		t.Fatalf("inline range mismatch: got %d bytes", len(got))
	}
}
