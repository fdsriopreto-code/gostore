package sse

import (
	"bytes"
	"crypto/rand"
	"io"
	"testing"
)

func TestRoundTripAndRange(t *testing.T) {
	key := make([]byte, 32)
	_, _ = rand.Read(key)
	prefix, _ := NewNoncePrefix()

	for _, size := range []int{0, 1, 100, ChunkPlain - 1, ChunkPlain, ChunkPlain + 1, 3*ChunkPlain + 777, 10 * ChunkPlain} {
		plain := make([]byte, size)
		_, _ = rand.Read(plain)

		var ct bytes.Buffer
		if _, err := io.Copy(&ct, EncryptReader(bytes.NewReader(plain), key, prefix)); err != nil {
			t.Fatalf("size %d encrypt: %v", size, err)
		}
		if int64(ct.Len()) != CipherSize(int64(size)) {
			t.Fatalf("size %d: ciphertext %d want %d", size, ct.Len(), CipherSize(int64(size)))
		}
		if PlainSize(int64(ct.Len())) != int64(size) {
			t.Fatalf("size %d: PlainSize(%d)=%d", size, ct.Len(), PlainSize(int64(ct.Len())))
		}

		// full decrypt
		fc, coff, skip := CipherOffsetForPlain(0)
		dec := DecryptRange(bytes.NewReader(ct.Bytes()[coff:]), key, prefix, fc, skip, int64(size))
		got, _ := io.ReadAll(dec)
		if !bytes.Equal(got, plain) {
			t.Fatalf("size %d: full decrypt mismatch (got %d)", size, len(got))
		}

		if size == 0 {
			continue
		}
		// random ranges
		for _, r := range [][2]int64{{0, 1}, {int64(size) / 2, int64(size) / 3}, {int64(size) - 1, 1},
			{ChunkPlain - 5, 20}, {0, int64(size)}} {
			off, ln := r[0], r[1]
			if off >= int64(size) {
				continue
			}
			if off+ln > int64(size) {
				ln = int64(size) - off
			}
			fc, coff, skip := CipherOffsetForPlain(off)
			dr := DecryptRange(bytes.NewReader(ct.Bytes()[coff:]), key, prefix, fc, skip, ln)
			g, _ := io.ReadAll(dr)
			if !bytes.Equal(g, plain[off:off+ln]) {
				t.Fatalf("size %d range [%d,%d): mismatch (got %d want %d)", size, off, off+ln, len(g), ln)
			}
		}
	}
}

func TestTamperDetected(t *testing.T) {
	key := make([]byte, 32)
	_, _ = rand.Read(key)
	prefix, _ := NewNoncePrefix()
	plain := bytes.Repeat([]byte("x"), ChunkPlain+50)
	var ct bytes.Buffer
	_, _ = io.Copy(&ct, EncryptReader(bytes.NewReader(plain), key, prefix))
	b := ct.Bytes()
	b[10] ^= 0xFF
	fc, coff, skip := CipherOffsetForPlain(0)
	_, err := io.ReadAll(DecryptRange(bytes.NewReader(b[coff:]), key, prefix, fc, skip, int64(len(plain))))
	if err == nil {
		t.Fatal("expected authentication failure on tampered ciphertext")
	}
}
