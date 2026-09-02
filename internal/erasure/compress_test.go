package erasure

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/lojadopocket/gostore/internal/object"
)

func TestCompressionRoundTripAndSavesSpace(t *testing.T) {
	p, roots := newTestPool(t, 4)
	_ = p.MakeBucket(ctx(), "buck", object.MakeBucketOptions{})

	// Very compressible: 4 MiB of a repeating pattern.
	payload := bytes.Repeat([]byte("the quick brown fox jumps over the lazy dog\n"), 100_000)
	want := hex.EncodeToString(func() []byte { s := md5.Sum(payload); return s[:] }())

	oi, err := p.PutObject(ctx(), "buck", "log.txt",
		object.NewPutObjReader(bytes.NewReader(payload), int64(len(payload)), int64(len(payload))),
		object.ObjectOptions{
			Compress:    true,
			UserDefined: map[string]string{"content-type": "text/plain"},
		})
	if err != nil {
		t.Fatal(err)
	}
	if oi.Size != int64(len(payload)) {
		t.Fatalf("reported Size = %d, want the logical %d", oi.Size, len(payload))
	}
	if oi.ETag != want {
		t.Fatalf("ETag = %s, want the plaintext md5 %s", oi.ETag, want)
	}

	m, _ := p.setFor("log.txt").readMeta(ctx(), "buck", "log.txt")
	if m.Compressed != "zstd" {
		t.Fatalf("meta.Compressed = %q, want zstd", m.Compressed)
	}
	if m.Size >= int64(len(payload))/2 {
		t.Fatalf("compressed on-disk size %d not < half of %d", m.Size, len(payload))
	}
	if m.PlainSize != int64(len(payload)) {
		t.Fatalf("PlainSize = %d, want %d", m.PlainSize, len(payload))
	}

	// The shard files on disk really are smaller than the plaintext.
	var shardTotal int64
	for i := range roots {
		fi, err := os.Stat(filepath.Join(roots[i], "buck", "log.txt", "part.00001"))
		if err != nil {
			continue
		}
		shardTotal += fi.Size()
	}
	if shardTotal >= int64(len(payload)) {
		t.Fatalf("shard bytes on disk (%d) not smaller than plaintext (%d)", shardTotal, len(payload))
	}

	// Full read.
	if got := get(t, p, "buck", "log.txt"); !bytes.Equal(got, payload) {
		t.Fatal("full read of a compressed object mismatch")
	}

	// Range reads at a few offsets.
	for _, rg := range [][2]int64{{0, 100}, {1_000_000, 4096}, {int64(len(payload)) - 10, 10}} {
		off, ln := rg[0], rg[1]
		gr, err := p.GetObjectNInfo(ctx(), "buck", "log.txt",
			&object.HTTPRangeSpec{Start: off, End: off + ln - 1}, nil, object.ObjectOptions{})
		if err != nil {
			t.Fatalf("range %v: %v", rg, err)
		}
		got := readAllRC(gr)
		if !bytes.Equal(got, payload[off:off+ln]) {
			t.Fatalf("range %v: got %d bytes, mismatch", rg, len(got))
		}
	}
}

func TestCompressionSkippedForCompressedTypes(t *testing.T) {
	p, _ := newTestPool(t, 4)
	_ = p.MakeBucket(ctx(), "buck", object.MakeBucketOptions{})
	payload := bytes.Repeat([]byte{0}, 1<<20)
	_, err := p.PutObject(ctx(), "buck", "clip.mp4",
		object.NewPutObjReader(bytes.NewReader(payload), int64(len(payload)), int64(len(payload))),
		object.ObjectOptions{Compress: true, UserDefined: map[string]string{"content-type": "video/mp4"}})
	if err != nil {
		t.Fatal(err)
	}
	m, _ := p.setFor("clip.mp4").readMeta(ctx(), "buck", "clip.mp4")
	if m.Compressed != "" {
		t.Fatalf("video/mp4 should not be compressed, got %q", m.Compressed)
	}
}

func readAllRC(gr *object.GetObjectReader) []byte {
	var b bytes.Buffer
	_, _ = b.ReadFrom(gr)
	gr.Close()
	return b.Bytes()
}

var _ = fmt.Sprint
