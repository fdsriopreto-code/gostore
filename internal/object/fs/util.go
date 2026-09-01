package fs

import (
	"crypto/md5"
	"encoding/hex"
	"hash"
	"io"
	"os"
)

// newMD5 returns a fresh MD5 hasher. ETags for single-part objects are the
// hex MD5 of the content, matching S3.
func newMD5() hash.Hash { return md5.New() }

// md5File returns the hex MD5 of a file's contents.
func md5File(path string) (string, error) {
	fh, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer fh.Close()
	h := md5.New()
	if _, err := io.Copy(h, fh); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
