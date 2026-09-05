package main

import (
	"context"
	"os"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"github.com/lojadopocket/gostore/internal/logger"
)

// applyMemLimit gives the Go runtime a soft memory ceiling so GC stays
// aggressive and pages are handed back to the OS on a small VPS. Priority:
//  1. GOMEMLIMIT (Go's own env — leave it to the runtime, just log it)
//  2. GOSTORE_MEM_LIMIT (bytes, or "256MiB" / "1g")
//  3. the container's cgroup memory limit * 90%
func applyMemLimit() {
	if v := strings.TrimSpace(os.Getenv("GOMEMLIMIT")); v != "" {
		logger.Info("memory limit from GOMEMLIMIT", "value", v)
		return
	}
	if n := parseBytes(os.Getenv("GOSTORE_MEM_LIMIT")); n > 0 {
		debug.SetMemoryLimit(n)
		logger.Info("soft memory limit set", "bytes", n, "source", "GOSTORE_MEM_LIMIT")
		return
	}
	if n := cgroupMemLimit(); n > 0 {
		lim := n / 10 * 9
		if lim < 64<<20 {
			lim = 64 << 20
		}
		debug.SetMemoryLimit(lim)
		logger.Info("soft memory limit set from container cgroup", "cgroupBytes", n, "limitBytes", lim)
	}
}

// startMemScavenger periodically returns idle heap to the OS after an activity
// spike, so RSS doesn't sit at its high-water mark forever.
func startMemScavenger(ctx context.Context) {
	go func() {
		t := time.NewTicker(3 * time.Minute)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				var m runtime.MemStats
				runtime.ReadMemStats(&m)
				// Only bother if a good chunk of heap is idle and unreleased.
				if m.HeapIdle-m.HeapReleased > 32<<20 {
					debug.FreeOSMemory()
				}
			}
		}
	}()
}

func cgroupMemLimit() int64 {
	// cgroup v2
	if b, err := os.ReadFile("/sys/fs/cgroup/memory.max"); err == nil {
		s := strings.TrimSpace(string(b))
		if s != "max" {
			if n, e := strconv.ParseInt(s, 10, 64); e == nil {
				return sane(n)
			}
		}
	}
	// cgroup v1
	if b, err := os.ReadFile("/sys/fs/cgroup/memory/memory.limit_in_bytes"); err == nil {
		if n, e := strconv.ParseInt(strings.TrimSpace(string(b)), 10, 64); e == nil {
			return sane(n)
		}
	}
	return 0
}

// sane rejects the "unlimited" sentinels cgroups report (near int64 max, or
// absurdly large) so we don't set a meaningless limit on a bare host.
func sane(n int64) int64 {
	if n <= 0 || n > 64<<30 { // > 64 GiB -> treat as "no container limit"
		return 0
	}
	return n
}

func parseBytes(s string) int64 {
	s = strings.TrimSpace(strings.ToLower(s))
	if s == "" {
		return 0
	}
	mult := int64(1)
	switch {
	case strings.HasSuffix(s, "gib"), strings.HasSuffix(s, "g"):
		mult = 1 << 30
		s = strings.TrimRight(s, "gib")
	case strings.HasSuffix(s, "mib"), strings.HasSuffix(s, "m"):
		mult = 1 << 20
		s = strings.TrimRight(s, "mib")
	case strings.HasSuffix(s, "kib"), strings.HasSuffix(s, "k"):
		mult = 1 << 10
		s = strings.TrimRight(s, "kib")
	}
	n, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
	if err != nil {
		return 0
	}
	return n * mult
}
