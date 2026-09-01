package erasure

import (
	"crypto/md5"
	"encoding/hex"
	"io"

	"github.com/lojadopocket/gostore/internal/sse"
)

// kmsWrapper is the subset of *kms.Manager the erasure backend needs.
type kmsWrapper interface {
	GenerateDataKey() ([]byte, error)
	WrapKey(dek []byte) ([]byte, error)
	UnwrapKey(wrapped []byte) ([]byte, error)
}

// SetKMS enables SSE-S3 at-rest encryption for single-part PutObject.
func (p *Pool) SetKMS(k kmsWrapper) { p.kms = k }

// sseParams carries the material for encrypting one object.
type sseParams struct {
	dek    []byte
	prefix []byte
	encDEK string // hex(wrapped dek)

	// filled once the plaintext stream is fully consumed:
	finish   func()
	plainMD5 string
	plainLen int64
}

func (p *Pool) newSSEParams() (*sseParams, error) {
	dek, err := p.kms.GenerateDataKey()
	if err != nil {
		return nil, err
	}
	prefix, err := sse.NewNoncePrefix()
	if err != nil {
		return nil, err
	}
	wrapped, err := p.kms.WrapKey(dek)
	if err != nil {
		return nil, err
	}
	return &sseParams{dek: dek, prefix: prefix, encDEK: hex.EncodeToString(wrapped)}, nil
}

// wrapForEncrypt returns a reader that yields the sealed chunk stream and, as
// a side effect once fully read, records the plaintext md5 + length into sp.
func (sp *sseParams) wrapForEncrypt(plain io.Reader) io.Reader {
	h := md5.New()
	cr := &countReader{r: io.TeeReader(plain, h)}
	sp.finish = func() {
		sp.plainMD5 = hex.EncodeToString(h.Sum(nil))
		sp.plainLen = cr.n
	}
	return sse.EncryptReader(cr, sp.dek, sp.prefix)
}

func (sp *sseParams) callFinish() {
	if sp != nil && sp.finish != nil {
		sp.finish()
	}
}

type countReader struct {
	r io.Reader
	n int64
}

func (c *countReader) Read(b []byte) (int, error) {
	k, err := c.r.Read(b)
	c.n += int64(k)
	return k, err
}

// decryptReader turns a ciphertext stream into plaintext for a byte range.
func (p *Pool) decryptReader(m *XLMeta, src io.Reader, plainOff, plainLen int64) (io.Reader, error) {
	wrapped, err := hex.DecodeString(m.EncDEK)
	if err != nil {
		return nil, err
	}
	dek, err := p.kms.UnwrapKey(wrapped)
	if err != nil {
		return nil, err
	}
	prefix, err := hex.DecodeString(m.NoncePrefix)
	if err != nil {
		return nil, err
	}
	firstChunk, _, intraSkip := sse.CipherOffsetForPlain(plainOff)
	return sse.DecryptRange(src, dek, prefix, firstChunk, intraSkip, plainLen), nil
}

// cipherRange maps a plaintext range to the ciphertext byte range to read
// from the stored stream.
func cipherRange(plainOff, plainLen, cipherTotal int64) (cipherOff, cipherLen int64) {
	_, co, intra := sse.CipherOffsetForPlain(plainOff)
	end := intra + plainLen
	chunks := (end + int64(sse.ChunkPlain) - 1) / int64(sse.ChunkPlain)
	cl := chunks * int64(sse.ChunkCipher)
	if co+cl > cipherTotal {
		cl = cipherTotal - co
	}
	return co, cl
}
