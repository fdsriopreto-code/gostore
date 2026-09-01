package fs

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/lojadopocket/gostore/internal/object"
)

func newTestFS(t *testing.T) *FS {
	t.Helper()
	f, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return f
}

func ctx() context.Context { return context.Background() }

func TestBucketLifecycle(t *testing.T) {
	f := newTestFS(t)
	if err := f.MakeBucket(ctx(), "my-bucket", object.MakeBucketOptions{}); err != nil {
		t.Fatalf("MakeBucket: %v", err)
	}
	if err := f.MakeBucket(ctx(), "my-bucket", object.MakeBucketOptions{}); err == nil {
		t.Fatal("expected error re-creating bucket")
	}
	bi, err := f.GetBucketInfo(ctx(), "my-bucket")
	if err != nil || bi.Name != "my-bucket" {
		t.Fatalf("GetBucketInfo: %v %+v", err, bi)
	}
	bs, err := f.ListBuckets(ctx())
	if err != nil || len(bs) != 1 {
		t.Fatalf("ListBuckets: %v %d", err, len(bs))
	}
	if err := f.DeleteBucket(ctx(), "my-bucket", object.DeleteBucketOptions{}); err != nil {
		t.Fatalf("DeleteBucket: %v", err)
	}
	if _, err := f.GetBucketInfo(ctx(), "my-bucket"); err == nil {
		t.Fatal("expected bucket gone")
	}
}

func TestInvalidBucketName(t *testing.T) {
	f := newTestFS(t)
	for _, n := range []string{"ab", "UPPER", "has_underscore", "192.168.1.1", "-lead", "trail-"} {
		if err := f.MakeBucket(ctx(), n, object.MakeBucketOptions{}); err == nil {
			t.Errorf("expected %q rejected", n)
		}
	}
}

func putString(t *testing.T, f *FS, bucket, key, body string) object.ObjectInfo {
	t.Helper()
	pr := object.NewPutObjReader(strings.NewReader(body), int64(len(body)), int64(len(body)))
	oi, err := f.PutObject(ctx(), bucket, key, pr, object.ObjectOptions{
		UserDefined: map[string]string{"content-type": "text/plain"},
	})
	if err != nil {
		t.Fatalf("PutObject %s: %v", key, err)
	}
	return oi
}

func TestObjectPutGetDelete(t *testing.T) {
	f := newTestFS(t)
	if err := f.MakeBucket(ctx(), "buck", object.MakeBucketOptions{}); err != nil {
		t.Fatal(err)
	}
	body := "hello gostore"
	oi := putString(t, f, "buck", "dir/file.txt", body)
	sum := md5.Sum([]byte(body))
	if oi.ETag != hex.EncodeToString(sum[:]) {
		t.Fatalf("etag mismatch: %s", oi.ETag)
	}

	gr, err := f.GetObjectNInfo(ctx(), "buck", "dir/file.txt", nil, nil, object.ObjectOptions{})
	if err != nil {
		t.Fatalf("GetObjectNInfo: %v", err)
	}
	got, _ := io.ReadAll(gr)
	gr.Close()
	if string(got) != body {
		t.Fatalf("body mismatch: %q", got)
	}

	// range: bytes=6-10 => "gosto"
	gr, err = f.GetObjectNInfo(ctx(), "buck", "dir/file.txt", &object.HTTPRangeSpec{Start: 6, End: 10}, nil, object.ObjectOptions{})
	if err != nil {
		t.Fatalf("range get: %v", err)
	}
	got, _ = io.ReadAll(gr)
	gr.Close()
	if string(got) != "gosto" {
		t.Fatalf("range body mismatch: %q", got)
	}

	// suffix range: bytes=-6 => "gostore"[-6:] of "hello gostore" => "ostore"
	gr, _ = f.GetObjectNInfo(ctx(), "buck", "dir/file.txt", &object.HTTPRangeSpec{IsSuffixLength: true, Start: -6}, nil, object.ObjectOptions{})
	got, _ = io.ReadAll(gr)
	gr.Close()
	if string(got) != "ostore" {
		t.Fatalf("suffix range mismatch: %q", got)
	}

	if _, err := f.DeleteObject(ctx(), "buck", "dir/file.txt", object.ObjectOptions{}); err != nil {
		t.Fatalf("DeleteObject: %v", err)
	}
	if _, err := f.GetObjectInfo(ctx(), "buck", "dir/file.txt", object.ObjectOptions{}); err == nil {
		t.Fatal("expected object gone")
	}
}

func TestListObjectsV2PrefixDelimiter(t *testing.T) {
	f := newTestFS(t)
	_ = f.MakeBucket(ctx(), "buck", object.MakeBucketOptions{})
	for _, k := range []string{"a.txt", "photos/1.jpg", "photos/2.jpg", "photos/sub/3.jpg", "z.txt"} {
		putString(t, f, "buck", k, "x")
	}
	li, err := f.ListObjectsV2(ctx(), "buck", "", "", "/", 1000, false, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(li.Objects) != 2 { // a.txt, z.txt
		t.Fatalf("want 2 top objects, got %d", len(li.Objects))
	}
	if len(li.Prefixes) != 1 || li.Prefixes[0] != "photos/" {
		t.Fatalf("want [photos/], got %v", li.Prefixes)
	}

	li, _ = f.ListObjectsV2(ctx(), "buck", "photos/", "", "/", 1000, false, "")
	if len(li.Objects) != 2 || li.Prefixes[0] != "photos/sub/" {
		t.Fatalf("nested list wrong: objs=%d prefixes=%v", len(li.Objects), li.Prefixes)
	}
}

func TestListObjectsV2Pagination(t *testing.T) {
	f := newTestFS(t)
	_ = f.MakeBucket(ctx(), "buck", object.MakeBucketOptions{})
	for i := 0; i < 10; i++ {
		putString(t, f, "buck", fmt.Sprintf("k%02d", i), "x")
	}
	seen := map[string]bool{}
	token := ""
	for {
		li, err := f.ListObjectsV2(ctx(), "buck", "", token, "", 3, false, "")
		if err != nil {
			t.Fatal(err)
		}
		for _, o := range li.Objects {
			if seen[o.Name] {
				t.Fatalf("dup key %s", o.Name)
			}
			seen[o.Name] = true
		}
		if !li.IsTruncated {
			break
		}
		token = li.NextContinuationToken
	}
	if len(seen) != 10 {
		t.Fatalf("want 10 keys, saw %d", len(seen))
	}
}

func TestMultipartRoundTrip(t *testing.T) {
	f := newTestFS(t)
	_ = f.MakeBucket(ctx(), "buck", object.MakeBucketOptions{})

	res, err := f.NewMultipartUpload(ctx(), "buck", "big.bin", object.ObjectOptions{
		UserDefined: map[string]string{"content-type": "application/octet-stream"},
	})
	if err != nil {
		t.Fatal(err)
	}

	part1 := bytes.Repeat([]byte("A"), 6*1024*1024) // 6 MiB (>= min part size)
	part2 := []byte("tail")
	p1, err := f.PutObjectPart(ctx(), "buck", "big.bin", res.UploadID, 1,
		object.NewPutObjReader(bytes.NewReader(part1), int64(len(part1)), int64(len(part1))), object.ObjectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	p2, err := f.PutObjectPart(ctx(), "buck", "big.bin", res.UploadID, 2,
		object.NewPutObjReader(bytes.NewReader(part2), int64(len(part2)), int64(len(part2))), object.ObjectOptions{})
	if err != nil {
		t.Fatal(err)
	}

	oi, err := f.CompleteMultipartUpload(ctx(), "buck", "big.bin", res.UploadID, []object.CompletePart{
		{PartNumber: 1, ETag: p1.ETag}, {PartNumber: 2, ETag: p2.ETag},
	}, object.ObjectOptions{})
	if err != nil {
		t.Fatalf("Complete: %v", err)
	}
	if oi.Size != int64(len(part1)+len(part2)) {
		t.Fatalf("size %d", oi.Size)
	}
	if !strings.HasSuffix(oi.ETag, "-2") {
		t.Fatalf("multipart etag should end in -2: %s", oi.ETag)
	}

	gr, _ := f.GetObjectNInfo(ctx(), "buck", "big.bin", nil, nil, object.ObjectOptions{})
	got, _ := io.ReadAll(gr)
	gr.Close()
	if !bytes.Equal(got, append(append([]byte{}, part1...), part2...)) {
		t.Fatal("assembled body mismatch")
	}
}

func TestMultipartRejectsSmallNonFinalPart(t *testing.T) {
	f := newTestFS(t)
	_ = f.MakeBucket(ctx(), "buck", object.MakeBucketOptions{})
	res, _ := f.NewMultipartUpload(ctx(), "buck", "o", object.ObjectOptions{})
	small := []byte("small")
	p1, _ := f.PutObjectPart(ctx(), "buck", "o", res.UploadID, 1,
		object.NewPutObjReader(bytes.NewReader(small), int64(len(small)), int64(len(small))), object.ObjectOptions{})
	p2, _ := f.PutObjectPart(ctx(), "buck", "o", res.UploadID, 2,
		object.NewPutObjReader(bytes.NewReader(small), int64(len(small)), int64(len(small))), object.ObjectOptions{})
	_, err := f.CompleteMultipartUpload(ctx(), "buck", "o", res.UploadID, []object.CompletePart{
		{PartNumber: 1, ETag: p1.ETag}, {PartNumber: 2, ETag: p2.ETag},
	}, object.ObjectOptions{})
	if err == nil {
		t.Fatal("expected ErrPartTooSmall")
	}
}

func TestCopyObject(t *testing.T) {
	f := newTestFS(t)
	_ = f.MakeBucket(ctx(), "src", object.MakeBucketOptions{})
	_ = f.MakeBucket(ctx(), "dst", object.MakeBucketOptions{})
	putString(t, f, "src", "a.txt", "copy me")
	si, _ := f.GetObjectInfo(ctx(), "src", "a.txt", object.ObjectOptions{})
	oi, err := f.CopyObject(ctx(), "src", "a.txt", "dst", "b.txt", si, object.ObjectOptions{}, object.ObjectOptions{})
	if err != nil {
		t.Fatalf("CopyObject: %v", err)
	}
	if oi.ETag != si.ETag {
		t.Fatalf("etag changed on copy: %s vs %s", oi.ETag, si.ETag)
	}
	gr, _ := f.GetObjectNInfo(ctx(), "dst", "b.txt", nil, nil, object.ObjectOptions{})
	got, _ := io.ReadAll(gr)
	gr.Close()
	if string(got) != "copy me" {
		t.Fatalf("copied body: %q", got)
	}
}
