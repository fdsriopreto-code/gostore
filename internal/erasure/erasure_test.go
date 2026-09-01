package erasure

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/lojadopocket/gostore/internal/object"
	"github.com/lojadopocket/gostore/internal/storage"
)

func ctx() context.Context { return context.Background() }

// newTestPool builds an n-disk pool rooted in a temp dir, returning the pool
// plus the on-disk root paths (for fault injection).
func newTestPool(t *testing.T, n int) (*Pool, []string) {
	t.Helper()
	base := t.TempDir()
	disks := make([]Disk, n)
	roots := make([]string, n)
	for i := 0; i < n; i++ {
		roots[i] = filepath.Join(base, fmt.Sprintf("disk%d", i))
		d, err := storage.OpenLocalDisk(roots[i], 0, i)
		if err != nil {
			t.Fatalf("OpenLocalDisk: %v", err)
		}
		disks[i] = d
	}
	p, err := FromDisks(disks)
	if err != nil {
		t.Fatalf("FromDisks: %v", err)
	}
	return p, roots
}

func put(t *testing.T, p *Pool, bucket, key string, data []byte) object.ObjectInfo {
	t.Helper()
	oi, err := p.PutObject(ctx(), bucket, key,
		object.NewPutObjReader(bytes.NewReader(data), int64(len(data)), int64(len(data))),
		object.ObjectOptions{UserDefined: map[string]string{"content-type": "application/octet-stream"}})
	if err != nil {
		t.Fatalf("PutObject %s (%d bytes): %v", key, len(data), err)
	}
	return oi
}

func get(t *testing.T, p *Pool, bucket, key string) []byte {
	t.Helper()
	gr, err := p.GetObjectNInfo(ctx(), bucket, key, nil, nil, object.ObjectOptions{})
	if err != nil {
		t.Fatalf("GetObjectNInfo %s: %v", key, err)
	}
	b, err := io.ReadAll(gr)
	gr.Close()
	if err != nil {
		t.Fatalf("read body %s: %v", key, err)
	}
	return b
}

func md5hex(b []byte) string { s := md5.Sum(b); return hex.EncodeToString(s[:]) }

func TestRoundTripSizes(t *testing.T) {
	p, _ := newTestPool(t, 4) // 2 data + 2 parity
	if err := p.MakeBucket(ctx(), "buck", object.MakeBucketOptions{}); err != nil {
		t.Fatal(err)
	}
	stripeInput := blockSizeV2 * 2 // dataBlocks == 2

	sizes := []int{
		0, 1, 100, 4095, 4096,
		blockSizeV2 - 1, blockSizeV2, blockSizeV2 + 1,
		stripeInput - 1, stripeInput, stripeInput + 1,
		stripeInput*3 + 12345,
	}
	for _, sz := range sizes {
		data := make([]byte, sz)
		_, _ = rand.Read(data)
		key := fmt.Sprintf("obj-%d", sz)
		oi := put(t, p, "buck", key, data)
		if oi.ETag != md5hex(data) {
			t.Fatalf("size %d: etag %s want %s", sz, oi.ETag, md5hex(data))
		}
		if oi.Size != int64(sz) {
			t.Fatalf("size %d: reported %d", sz, oi.Size)
		}
		got := get(t, p, "buck", key)
		if !bytes.Equal(got, data) {
			t.Fatalf("size %d: body mismatch (got %d bytes)", sz, len(got))
		}
	}
}

func TestRangeReads(t *testing.T) {
	p, _ := newTestPool(t, 6) // 3 data + 3 parity
	_ = p.MakeBucket(ctx(), "buck", object.MakeBucketOptions{})
	data := make([]byte, blockSizeV2*3*2+777) // multi-stripe
	_, _ = rand.Read(data)
	put(t, p, "buck", "r", data)

	cases := [][2]int64{
		{0, 10},
		{5, 100},
		{blockSizeV2 - 3, blockSizeV2 + 50}, // straddle a stripe boundary
		{int64(len(data)) - 20, 20},
	}
	for _, c := range cases {
		start, length := c[0], c[1]
		gr, err := p.GetObjectNInfo(ctx(), "buck", "r",
			&object.HTTPRangeSpec{Start: start, End: start + length - 1}, nil, object.ObjectOptions{})
		if err != nil {
			t.Fatalf("range [%d,%d): %v", start, start+length, err)
		}
		got, _ := io.ReadAll(gr)
		gr.Close()
		if !bytes.Equal(got, data[start:start+length]) {
			t.Fatalf("range [%d,%d): mismatch len got=%d want=%d", start, start+length, len(got), length)
		}
	}
}

func TestSurvivesDiskLoss(t *testing.T) {
	p, roots := newTestPool(t, 6) // 3 data + 3 parity -> tolerate 3 lost
	_ = p.MakeBucket(ctx(), "buck", object.MakeBucketOptions{})
	data := make([]byte, blockSizeV2*3+4096)
	_, _ = rand.Read(data)
	put(t, p, "buck", "obj", data)

	// Wipe the object dir from 3 of 6 disks (== parity count).
	for i := 0; i < 3; i++ {
		if err := os.RemoveAll(filepath.Join(roots[i], "buck", "obj")); err != nil {
			t.Fatal(err)
		}
	}
	got := get(t, p, "buck", "obj")
	if !bytes.Equal(got, data) {
		t.Fatalf("reconstruction after 3 disk loss failed (got %d bytes)", len(got))
	}

	// Losing a 4th disk must break the read (quorum), not return garbage.
	if err := os.RemoveAll(filepath.Join(roots[3], "buck", "obj")); err != nil {
		t.Fatal(err)
	}
	if _, err := p.GetObjectNInfo(ctx(), "buck", "obj", nil, nil, object.ObjectOptions{}); err == nil {
		gr, _ := p.GetObjectNInfo(ctx(), "buck", "obj", nil, nil, object.ObjectOptions{})
		if gr != nil {
			b, rerr := io.ReadAll(gr)
			gr.Close()
			if rerr == nil && bytes.Equal(b, data) {
				t.Fatal("expected read failure after losing 4/6 disks")
			}
		}
	}
}

func TestHealRewritesLostAndCorruptShards(t *testing.T) {
	p, roots := newTestPool(t, 6) // 3 data + 3 parity
	_ = p.MakeBucket(ctx(), "buck", object.MakeBucketOptions{})
	data := make([]byte, blockSizeV2*3+9000)
	_, _ = rand.Read(data)
	put(t, p, "buck", "obj", data)

	// disk 0: wipe the whole object dir. disk 1: corrupt the shard.
	if err := os.RemoveAll(filepath.Join(roots[0], "buck", "obj")); err != nil {
		t.Fatal(err)
	}
	shard := filepath.Join(roots[1], "buck", "obj", "part.00001")
	b, _ := os.ReadFile(shard)
	for i := 0; i < 128 && i < len(b); i++ {
		b[i] ^= 0xAA
	}
	_ = os.WriteFile(shard, b, 0o644)

	rep, err := p.Heal(ctx())
	if err != nil {
		t.Fatalf("Heal: %v", err)
	}
	if rep.ObjectsHealed == 0 || rep.ShardsRewritten < 2 {
		t.Fatalf("unexpected heal report: %+v", rep)
	}

	// After healing, every disk must hold a byte-correct, bitrot-valid shard:
	// wiping any 3 more disks and reading must still succeed.
	for i := 2; i < 5; i++ {
		_ = os.RemoveAll(filepath.Join(roots[i], "buck", "obj"))
	}
	got := get(t, p, "buck", "obj")
	if !bytes.Equal(got, data) {
		t.Fatal("post-heal read after fresh 3-disk loss failed")
	}
}

func TestBitrotDetectedAndRepaired(t *testing.T) {
	p, roots := newTestPool(t, 4) // 2 data + 2 parity
	_ = p.MakeBucket(ctx(), "buck", object.MakeBucketOptions{})
	data := make([]byte, blockSizeV2+12345)
	_, _ = rand.Read(data)
	put(t, p, "buck", "obj", data)

	// Corrupt one disk's shard file in place (same length).
	shard := filepath.Join(roots[0], "buck", "obj", "part.00001")
	b, err := os.ReadFile(shard)
	if err != nil {
		t.Fatal(err)
	}
	for i := range b[:64] {
		b[i] ^= 0xFF
	}
	if err := os.WriteFile(shard, b, 0o644); err != nil {
		t.Fatal(err)
	}

	got := get(t, p, "buck", "obj")
	if !bytes.Equal(got, data) {
		t.Fatal("bitrot not corrected via reconstruction")
	}
}

func TestMultipartRoundTrip(t *testing.T) {
	p, _ := newTestPool(t, 4)
	_ = p.MakeBucket(ctx(), "buck", object.MakeBucketOptions{})

	res, err := p.NewMultipartUpload(ctx(), "buck", "big", object.ObjectOptions{
		UserDefined: map[string]string{"content-type": "application/octet-stream"},
	})
	if err != nil {
		t.Fatal(err)
	}

	p1 := make([]byte, 5*1024*1024+1000)
	p2 := make([]byte, 33333)
	_, _ = rand.Read(p1)
	_, _ = rand.Read(p2)

	i1, err := p.PutObjectPart(ctx(), "buck", "big", res.UploadID, 1,
		object.NewPutObjReader(bytes.NewReader(p1), int64(len(p1)), int64(len(p1))), object.ObjectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	i2, err := p.PutObjectPart(ctx(), "buck", "big", res.UploadID, 2,
		object.NewPutObjReader(bytes.NewReader(p2), int64(len(p2)), int64(len(p2))), object.ObjectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if i1.ETag != md5hex(p1) || i2.ETag != md5hex(p2) {
		t.Fatalf("part etags: %s %s", i1.ETag, i2.ETag)
	}

	oi, err := p.CompleteMultipartUpload(ctx(), "buck", "big", res.UploadID, []object.CompletePart{
		{PartNumber: 1, ETag: i1.ETag}, {PartNumber: 2, ETag: i2.ETag},
	}, object.ObjectOptions{})
	if err != nil {
		t.Fatalf("Complete: %v", err)
	}
	want := md5.Sum(append(mustHex(t, i1.ETag), mustHex(t, i2.ETag)...))
	if oi.ETag != hex.EncodeToString(want[:])+"-2" {
		t.Fatalf("multipart etag %s", oi.ETag)
	}
	full := append(append([]byte{}, p1...), p2...)
	got := get(t, p, "buck", "big")
	if !bytes.Equal(got, full) {
		t.Fatalf("assembled mismatch: got %d want %d", len(got), len(full))
	}
}

func mustHex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestListAndDelete(t *testing.T) {
	p, _ := newTestPool(t, 4)
	_ = p.MakeBucket(ctx(), "buck", object.MakeBucketOptions{})
	for _, k := range []string{"a.txt", "d/1", "d/2", "d/e/3", "z"} {
		put(t, p, "buck", k, []byte(k))
	}
	li, err := p.ListObjectsV2(ctx(), "buck", "", "", "/", 1000, false, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(li.Objects) != 2 || len(li.Prefixes) != 1 || li.Prefixes[0] != "d/" {
		t.Fatalf("top list: objs=%d prefixes=%v", len(li.Objects), li.Prefixes)
	}
	li, _ = p.ListObjectsV2(ctx(), "buck", "d/", "", "/", 1000, false, "")
	if len(li.Objects) != 2 || li.Prefixes[0] != "d/e/" {
		t.Fatalf("nested list: objs=%d prefixes=%v", len(li.Objects), li.Prefixes)
	}

	if _, err := p.DeleteObject(ctx(), "buck", "d/1", object.ObjectOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := p.GetObjectInfo(ctx(), "buck", "d/1", object.ObjectOptions{}); err == nil {
		t.Fatal("expected d/1 gone")
	}
	if _, err := p.GetObjectInfo(ctx(), "buck", "d/2", object.ObjectOptions{}); err != nil {
		t.Fatalf("d/2 should still exist: %v", err)
	}
}

// TestDeleteBucketAfterEmptying mirrors the /gostore/health/selftest flow:
// nested object created, deleted, then the bucket must delete cleanly (no
// leftover empty parent dirs).
func TestDeleteBucketAfterEmptying(t *testing.T) {
	p, _ := newTestPool(t, 4)
	if err := p.MakeBucket(ctx(), "selftest-buck", object.MakeBucketOptions{}); err != nil {
		t.Fatal(err)
	}
	put(t, p, "selftest-buck", "probe/data.bin", []byte("hello"))
	if _, err := p.DeleteObject(ctx(), "selftest-buck", "probe/data.bin", object.ObjectOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := p.DeleteBucket(ctx(), "selftest-buck", object.DeleteBucketOptions{}); err != nil {
		t.Fatalf("DeleteBucket after emptying: %v", err)
	}
}

// newTestPoolSets builds a pool of `nsets` erasure sets, each with
// `perSet` disks.
func newTestPoolSets(t *testing.T, nsets, perSet int) *Pool {
	t.Helper()
	base := t.TempDir()
	sets := make([]*Set, nsets)
	for si := 0; si < nsets; si++ {
		disks := make([]Disk, perSet)
		for di := 0; di < perSet; di++ {
			root := filepath.Join(base, fmt.Sprintf("s%d-d%d", si, di))
			d, err := storage.OpenLocalDisk(root, si, di)
			if err != nil {
				t.Fatal(err)
			}
			disks[di] = d
		}
		set, err := NewSet(disks)
		if err != nil {
			t.Fatal(err)
		}
		sets[si] = set
	}
	p, err := NewPool(sets...)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func TestServerPoolsSpreadAndList(t *testing.T) {
	p := newTestPoolSets(t, 3, 4) // 3 sets of 4 disks
	if err := p.MakeBucket(ctx(), "buck", object.MakeBucketOptions{}); err != nil {
		t.Fatal(err)
	}
	want := map[string]string{}
	for i := 0; i < 60; i++ {
		k := fmt.Sprintf("k/%03d", i)
		body := fmt.Sprintf("value-%d-%d", i, i*7)
		put(t, p, "buck", k, []byte(body))
		want[k] = body
	}
	// Every key must be readable and correct regardless of which set holds it.
	for k, v := range want {
		if got := string(get(t, p, "buck", k)); got != v {
			t.Fatalf("%s: got %q want %q", k, got, v)
		}
	}
	// Listing must merge across all sets.
	seen := map[string]bool{}
	token := ""
	for {
		li, err := p.ListObjectsV2(ctx(), "buck", "k/", token, "", 17, false, "")
		if err != nil {
			t.Fatal(err)
		}
		for _, o := range li.Objects {
			if seen[o.Name] {
				t.Fatalf("dup %s", o.Name)
			}
			seen[o.Name] = true
		}
		if !li.IsTruncated {
			break
		}
		token = li.NextContinuationToken
	}
	if len(seen) != len(want) {
		t.Fatalf("listed %d keys, want %d", len(seen), len(want))
	}
}

func TestServerPoolsMultipart(t *testing.T) {
	p := newTestPoolSets(t, 2, 4)
	_ = p.MakeBucket(ctx(), "buck", object.MakeBucketOptions{})
	res, err := p.NewMultipartUpload(ctx(), "buck", "spread/big", object.ObjectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	p1 := make([]byte, 5*1024*1024+7)
	p2 := []byte("tail-bytes")
	_, _ = rand.Read(p1)
	i1, err := p.PutObjectPart(ctx(), "buck", "spread/big", res.UploadID, 1,
		object.NewPutObjReader(bytes.NewReader(p1), int64(len(p1)), int64(len(p1))), object.ObjectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	i2, err := p.PutObjectPart(ctx(), "buck", "spread/big", res.UploadID, 2,
		object.NewPutObjReader(bytes.NewReader(p2), int64(len(p2)), int64(len(p2))), object.ObjectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.CompleteMultipartUpload(ctx(), "buck", "spread/big", res.UploadID, []object.CompletePart{
		{PartNumber: 1, ETag: i1.ETag}, {PartNumber: 2, ETag: i2.ETag},
	}, object.ObjectOptions{}); err != nil {
		t.Fatalf("complete: %v", err)
	}
	got := get(t, p, "buck", "spread/big")
	if !bytes.Equal(got, append(append([]byte{}, p1...), p2...)) {
		t.Fatal("assembled mismatch")
	}
}

func TestBucketOps(t *testing.T) {
	p, _ := newTestPool(t, 4)
	if err := p.MakeBucket(ctx(), "one", object.MakeBucketOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := p.MakeBucket(ctx(), "one", object.MakeBucketOptions{}); err == nil {
		t.Fatal("expected bucket-exists error")
	}
	bs, err := p.ListBuckets(ctx())
	if err != nil || len(bs) != 1 || bs[0].Name != "one" {
		t.Fatalf("ListBuckets: %v %+v", err, bs)
	}
	put(t, p, "one", "x", []byte("x"))
	if err := p.DeleteBucket(ctx(), "one", object.DeleteBucketOptions{}); err == nil {
		t.Fatal("expected not-empty error")
	}
	_, _ = p.DeleteObject(ctx(), "one", "x", object.ObjectOptions{})
	if err := p.DeleteBucket(ctx(), "one", object.DeleteBucketOptions{}); err != nil {
		t.Fatalf("DeleteBucket: %v", err)
	}
}
