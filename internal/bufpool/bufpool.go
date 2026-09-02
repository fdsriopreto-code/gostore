// Package bufpool hands out reusable 64 KiB byte slices for streaming copies,
// so a high request rate doesn't churn the GC with per-copy 32 KiB io.Copy
// allocations.
package bufpool

import (
	"io"
	"sync"
)

const size = 64 << 10

var pool = sync.Pool{New: func() any { b := make([]byte, size); return &b }}

// Copy is io.Copy with a pooled buffer.
func Copy(dst io.Writer, src io.Reader) (int64, error) {
	bp := pool.Get().(*[]byte)
	defer pool.Put(bp)
	return io.CopyBuffer(dst, src, *bp)
}
