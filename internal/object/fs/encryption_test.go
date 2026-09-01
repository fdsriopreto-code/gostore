package fs

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/md5"
	"crypto/rand"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/lojadopocket/gostore/internal/object"
)

// fakeKMS is a self-contained key manager for tests.
type fakeKMS struct{ master [32]byte }

func newFakeKMS() *fakeKMS { k := &fakeKMS{}; rand.Read(k.master[:]); return k }
func (k *fakeKMS) GenerateDataKey() ([]byte, error) {
	b := make([]byte, 32)
	_, err := rand.Read(b)
	return b, err
}
func (k *fakeKMS) gcm() cipher.AEAD {
	bl, _ := aes.NewCipher(k.master[:])
	g, _ := cipher.NewGCM(bl)
	return g
}
func (k *fakeKMS) WrapKey(dek []byte) ([]byte, error) {
	g := k.gcm()
	n := make([]byte, g.NonceSize())
	rand.Read(n)
	return append(n, g.Seal(nil, n, dek, nil)...), nil
}
func (k *fakeKMS) UnwrapKey(w []byte) ([]byte, error) {
	g := k.gcm()
	ns := g.NonceSize()
	return g.Open(nil, w[:ns], w[ns:], nil)
}

func md5b(b []byte) string { s := md5.Sum(b); return hex.EncodeToString(s[:]) }

func TestSSERoundTrip(t *testing.T) {
	f := newTestFS(t)
	f.SetKMS(newFakeKMS())
	if err := f.MakeBucket(ctx(), "sbuck", object.MakeBucketOptions{}); err != nil {
		t.Fatal(err)
	}

	for _, size := range []int{0, 10, 70000, 200000} {
		data := make([]byte, size)
		_, _ = rand.Read(data)
		key := "enc/obj"
		oi, err := f.PutObject(ctx(), "sbuck", key,
			object.NewPutObjReader(bytes.NewReader(data), int64(size), int64(size)),
			object.ObjectOptions{UserDefined: map[string]string{"x-amz-server-side-encryption": "AES256"}})
		if err != nil {
			t.Fatalf("size %d: PutObject: %v", size, err)
		}
		if oi.Size != int64(size) {
			t.Fatalf("size %d: reported %d", size, oi.Size)
		}
		if oi.ETag != md5b(data) {
			t.Fatalf("size %d: etag not plaintext md5", size)
		}
		if oi.UserDefined["x-amz-server-side-encryption"] != "AES256" {
			t.Fatalf("size %d: missing sse marker", size)
		}

		// bytes on disk must NOT equal the plaintext
		raw, _ := os.ReadFile(filepath.Join(f.root, "sbuck", "enc", "obj"))
		if size > 0 && bytes.Equal(raw, data) {
			t.Fatalf("size %d: data stored in plaintext!", size)
		}
		if size > 0 && int64(len(raw)) <= int64(size) {
			t.Fatalf("size %d: ciphertext not larger than plaintext (%d)", size, len(raw))
		}

		// full read
		gr, err := f.GetObjectNInfo(ctx(), "sbuck", key, nil, nil, object.ObjectOptions{})
		if err != nil {
			t.Fatalf("size %d: get: %v", size, err)
		}
		got, _ := io.ReadAll(gr)
		gr.Close()
		if !bytes.Equal(got, data) {
			t.Fatalf("size %d: decrypt mismatch (got %d)", size, len(got))
		}

		if size <= 65536 {
			continue
		}
		// range read straddling the 64 KiB chunk boundary
		start, ln := int64(65536-20), int64(40)
		if start+ln > int64(size) {
			ln = int64(size) - start
		}
		gr, err = f.GetObjectNInfo(ctx(), "sbuck", key,
			&object.HTTPRangeSpec{Start: start, End: start + ln - 1}, nil, object.ObjectOptions{})
		if err != nil {
			t.Fatalf("size %d: range get: %v", size, err)
		}
		got, _ = io.ReadAll(gr)
		gr.Close()
		if !bytes.Equal(got, data[start:start+ln]) {
			t.Fatalf("size %d: encrypted range mismatch (got %d want %d)", size, len(got), ln)
		}
	}
}
