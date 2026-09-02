package api

import (
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base64"
	"hash"
	"hash/crc32"
	"io"
	"net/http"
	"strings"
)

// Additional checksums (S3, 2022+): a client sends x-amz-checksum-<algo> as
// base64 and the server verifies it against the body, stores it, and returns
// it on GET/HEAD. gostore verifies inline while the backend streams the body,
// so a mismatch aborts the write (no object stored) — same as AWS. MinIO
// only started supporting these recently and not for every algorithm.

var checksumAlgos = []string{"crc32", "crc32c", "sha1", "sha256"}

func newChecksumHash(algo string) hash.Hash {
	switch algo {
	case "crc32":
		return crc32.NewIEEE()
	case "crc32c":
		return crc32.New(crc32.MakeTable(crc32.Castagnoli))
	case "sha1":
		return sha1.New()
	case "sha256":
		return sha256.New()
	}
	return nil
}

// checksumFromHeaders returns (algo, base64Expected) for the first
// x-amz-checksum-<algo> header present, or ("","") if none.
func checksumFromHeaders(h http.Header) (string, string) {
	for _, a := range checksumAlgos {
		if v := strings.TrimSpace(h.Get("x-amz-checksum-" + a)); v != "" {
			return a, v
		}
	}
	return "", ""
}

// verifyingReader streams src, accumulating a checksum. On the read that hits
// EOF it compares to want (base64) and, on mismatch, returns errBadChecksum
// instead of io.EOF so the backend write aborts.
type verifyingReader struct {
	src  io.Reader
	h    hash.Hash
	want string
	done bool
}

var errBadChecksum = &checksumError{}

type checksumError struct{}

func (*checksumError) Error() string { return "provided x-amz-checksum does not match the body" }

func newVerifyingReader(src io.Reader, algo, want string) *verifyingReader {
	return &verifyingReader{src: src, h: newChecksumHash(algo), want: want}
}

func (v *verifyingReader) Read(p []byte) (int, error) {
	n, err := v.src.Read(p)
	if n > 0 {
		v.h.Write(p[:n])
	}
	if err == io.EOF && !v.done {
		v.done = true
		got := base64.StdEncoding.EncodeToString(v.h.Sum(nil))
		if !strings.EqualFold(got, v.want) {
			return n, errBadChecksum
		}
	}
	return n, err
}
