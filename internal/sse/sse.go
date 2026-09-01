// Package sse implements SSE-S3 at-rest encryption: object bytes are
// AES-256-GCM sealed in fixed-size chunks so streams of any length can be
// encrypted and any byte range decrypted from a chunk boundary.
//
// On-disk layout of an encrypted object: a bare concatenation of sealed
// chunks (each = ciphertext + 16-byte tag). The 4-byte random nonce prefix
// and the wrapped data key live in the object metadata, not the file.
package sse

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"io"
)

const (
	// ChunkPlain is the plaintext bytes per chunk.
	ChunkPlain = 64 * 1024
	tagLen     = 16
	// ChunkCipher is the on-disk bytes per full chunk.
	ChunkCipher    = ChunkPlain + tagLen
	noncePrefixLen = 4
	nonceLen       = 12 // 4-byte random prefix + 8-byte big-endian counter
)

// Algorithm is the value reported in x-amz-server-side-encryption.
const Algorithm = "AES256"

// NewNoncePrefix returns a fresh 4-byte random nonce prefix (per object).
func NewNoncePrefix() ([]byte, error) {
	p := make([]byte, noncePrefixLen)
	_, err := rand.Read(p)
	return p, err
}

// CipherSize returns the on-disk size for a plaintext of n bytes.
func CipherSize(n int64) int64 {
	if n == 0 {
		return 0
	}
	full := n / ChunkPlain
	rem := n % ChunkPlain
	sz := full * ChunkCipher
	if rem > 0 {
		sz += rem + tagLen
	}
	return sz
}

// PlainSize is the inverse of CipherSize.
func PlainSize(n int64) int64 {
	if n == 0 {
		return 0
	}
	full := n / ChunkCipher
	rem := n % ChunkCipher
	sz := full * ChunkPlain
	if rem > 0 {
		sz += rem - tagLen
	}
	return sz
}

func newGCM(key []byte) (cipher.AEAD, error) {
	b, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(b)
}

func nonce(prefix []byte, counter uint64) []byte {
	n := make([]byte, nonceLen)
	copy(n, prefix)
	binary.BigEndian.PutUint64(n[noncePrefixLen:], counter)
	return n
}

// EncryptReader wraps r, yielding the sealed chunk stream. It reads the whole
// of r; the total output length equals CipherSize(len(r)).
func EncryptReader(r io.Reader, key, prefix []byte) io.Reader {
	g, err := newGCM(key)
	return &encReader{r: r, g: g, prefix: prefix, err: err, buf: make([]byte, ChunkPlain)}
}

type encReader struct {
	r       io.Reader
	g       cipher.AEAD
	prefix  []byte
	counter uint64
	buf     []byte
	pending []byte
	eof     bool
	err     error
}

func (e *encReader) Read(p []byte) (int, error) {
	if e.err != nil {
		return 0, e.err
	}
	for len(e.pending) == 0 && !e.eof {
		n, rerr := io.ReadFull(e.r, e.buf)
		if n > 0 {
			e.pending = e.g.Seal(nil, nonce(e.prefix, e.counter), e.buf[:n], nil)
			e.counter++
		}
		if rerr == io.EOF || rerr == io.ErrUnexpectedEOF {
			e.eof = true
		} else if rerr != nil {
			e.err = rerr
			if len(e.pending) == 0 {
				return 0, rerr
			}
		}
	}
	if len(e.pending) == 0 {
		return 0, io.EOF
	}
	k := copy(p, e.pending)
	e.pending = e.pending[k:]
	return k, nil
}

// DecryptRange returns a reader over plaintext bytes [off, off+length) of an
// object whose on-disk cipher stream is src. src must already be positioned
// at the start of the chunk containing off, given by CipherOffsetForPlain.
func DecryptRange(src io.Reader, key, prefix []byte, firstChunk uint64, intraSkip, length int64) io.Reader {
	g, err := newGCM(key)
	return &decReader{src: src, g: g, prefix: prefix, counter: firstChunk, skip: intraSkip, remain: length, err: err}
}

// CipherOffsetForPlain maps a plaintext offset to (firstChunkIndex,
// cipherByteOffset, intraChunkSkip).
func CipherOffsetForPlain(off int64) (firstChunk uint64, cipherOff int64, intraSkip int64) {
	fc := off / ChunkPlain
	return uint64(fc), fc * ChunkCipher, off % ChunkPlain
}

type decReader struct {
	src     io.Reader
	g       cipher.AEAD
	prefix  []byte
	counter uint64
	skip    int64
	remain  int64
	buf     []byte
	out     []byte
	err     error
}

func (d *decReader) Read(p []byte) (int, error) {
	if d.err != nil {
		return 0, d.err
	}
	for len(d.out) == 0 {
		if d.remain <= 0 {
			return 0, io.EOF
		}
		if d.buf == nil {
			d.buf = make([]byte, ChunkCipher)
		}
		n, rerr := io.ReadFull(d.src, d.buf)
		if n == 0 {
			if rerr == io.EOF {
				return 0, io.EOF
			}
			d.err = errOr(rerr, io.ErrUnexpectedEOF)
			return 0, d.err
		}
		if n < tagLen+1 && rerr != nil {
			d.err = errors.New("sse: truncated chunk")
			return 0, d.err
		}
		pt, oerr := d.g.Open(nil, nonce(d.prefix, d.counter), d.buf[:n], nil)
		if oerr != nil {
			d.err = errors.New("sse: chunk authentication failed")
			return 0, d.err
		}
		d.counter++
		if d.skip > 0 {
			if d.skip >= int64(len(pt)) {
				d.skip -= int64(len(pt))
				pt = nil
			} else {
				pt = pt[d.skip:]
				d.skip = 0
			}
		}
		if int64(len(pt)) > d.remain {
			pt = pt[:d.remain]
		}
		d.out = pt
		if rerr == io.EOF || rerr == io.ErrUnexpectedEOF {
			// last chunk consumed; if remain still >0 it just ends
		}
	}
	k := copy(p, d.out)
	d.out = d.out[k:]
	d.remain -= int64(k)
	return k, nil
}

func errOr(a, b error) error {
	if a != nil {
		return a
	}
	return b
}
