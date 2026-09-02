package erasure

import (
	"context"
	"os"
	"sync"
	"time"
)

// A hung disk (dead hardware, a partitioned peer) must not stall a metadata
// operation past the namespace lock's TTL — if it did, a second writer could
// acquire the lock and both would write. withDiskDeadline caps every
// fast/metadata disk op (WriteAll, ReadAll, RenameDir, Delete, ListDir).
// Streaming ops (CreateFile / ReadFileStream of object data) are intentionally
// left uncapped — a large transfer over a slow link is legitimately long.
//
//	GOSTORE_DISK_OP_TIMEOUT   default 30s; 0 disables the cap.
var (
	diskOpTimeout     time.Duration
	diskOpTimeoutOnce sync.Once
)

func loadDiskOpTimeout() {
	diskOpTimeout = 30 * time.Second
	if v := os.Getenv("GOSTORE_DISK_OP_TIMEOUT"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			diskOpTimeout = d // 0 disables
		}
	}
}

func withDiskDeadline(ctx context.Context) (context.Context, context.CancelFunc) {
	diskOpTimeoutOnce.Do(loadDiskOpTimeout)
	if diskOpTimeout <= 0 {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, diskOpTimeout)
}
