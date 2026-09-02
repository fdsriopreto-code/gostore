package erasure

import (
	"io"
	"os"
	"strings"
	"sync"

	"github.com/klauspost/compress/zstd"
)

// Transparent compression at rest: when a bucket enables it, an object's
// plaintext is zstd-compressed before erasure coding. xl.meta records
// Compressed="zstd", PlainSize (logical) and Size (compressed, on disk); the
// S3 ETag stays the plaintext md5. Skipped for SSE, inline objects, and
// content-types that are already compressed. GOSTORE_COMPRESS_DISABLE=1 forces
// it off regardless of bucket config.

const compressAlgo = "zstd"

var compressDisabled = os.Getenv("GOSTORE_COMPRESS_DISABLE") == "1"

// alreadyCompressed lists content-type prefixes not worth recompressing.
var alreadyCompressed = []string{
	"image/jpeg", "image/png", "image/gif", "image/webp", "image/avif",
	"video/", "audio/",
	"application/zip", "application/gzip", "application/x-gzip", "application/x-7z-compressed",
	"application/x-rar-compressed", "application/x-bzip2", "application/x-xz",
	"application/zstd", "application/x-compressed",
	"application/octet-stream", // unknown — assume opaque/possibly compressed
}

// shouldCompress decides whether an object with this content-type and size is
// worth compressing.
func shouldCompress(want bool, sse bool, contentType string, size int64) bool {
	if !want || compressDisabled || sse {
		return false
	}
	if size >= 0 && size < 512 { // not worth the header overhead
		return false
	}
	ct := strings.ToLower(strings.TrimSpace(contentType))
	for _, p := range alreadyCompressed {
		if strings.HasPrefix(ct, p) {
			return false
		}
	}
	return true
}

var zstdEncPool = sync.Pool{New: func() any {
	e, _ := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
	return e
}}

// zstdCompressStream returns a reader that yields the zstd-compressed form of
// src, compressing in a background goroutine.
func zstdCompressStream(src io.Reader) io.Reader {
	pr, pw := io.Pipe()
	go func() {
		enc := zstdEncPool.Get().(*zstd.Encoder)
		enc.Reset(pw)
		_, err := io.Copy(enc, src)
		if cerr := enc.Close(); err == nil {
			err = cerr
		}
		zstdEncPool.Put(enc)
		_ = pw.CloseWithError(err)
	}()
	return pr
}

// zstdDecompressRange decompresses src and returns a reader over the plaintext
// byte range [off, off+length). length < 0 means "to end". src (a pipe from
// the shard assembler) is closed by the returned closer.
func zstdDecompressRange(src io.ReadCloser, off, length int64) (io.ReadCloser, error) {
	dec, err := zstd.NewReader(src)
	if err != nil {
		return nil, err
	}
	r := io.Reader(dec.IOReadCloser())
	if off > 0 {
		if _, err := io.CopyN(io.Discard, r, off); err != nil {
			dec.Close()
			_ = src.Close()
			return nil, err
		}
	}
	if length >= 0 {
		r = io.LimitReader(r, length)
	}
	return &zstdReadCloser{r: r, dec: dec, src: src}, nil
}

type zstdReadCloser struct {
	r   io.Reader
	dec *zstd.Decoder
	src io.Closer
}

func (z *zstdReadCloser) Read(p []byte) (int, error) { return z.r.Read(p) }
func (z *zstdReadCloser) Close() error {
	z.dec.Close()
	if z.src != nil {
		return z.src.Close()
	}
	return nil
}
