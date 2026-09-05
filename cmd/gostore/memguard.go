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

// applyCPULimit pins GOMAXPROCS to the container's CPU quota. Without this the
// Go runtime sees every host core, spins up that many Ps, burns the cgroup CFS
// quota in a few ms of wall time and then the kernel freezes the whole
// container until the next period — turning a 1 ms handler into 100s of ms of
// tail latency, worst of all during GC. Priority:
//  1. GOMAXPROCS (Go's own env — leave it alone, just log)
//  2. GOSTORE_MAXPROCS (explicit integer)
//  3. ceil(cgroup cpu quota / period), clamped to [1, NumCPU]
func applyCPULimit() {
	if v := strings.TrimSpace(os.Getenv("GOMAXPROCS")); v != "" {
		logger.Info("GOMAXPROCS from env", "value", v)
		return
	}
	if v := strings.TrimSpace(os.Getenv("GOSTORE_MAXPROCS")); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			runtime.GOMAXPROCS(n)
			logger.Info("GOMAXPROCS set", "value", n, "source", "GOSTORE_MAXPROCS")
			return
		}
	}
	q := cgroupCPUQuota()
	if q <= 0 {
		return // no container CPU limit — leave the runtime default
	}
	procs := int(q + 0.999) // round up so a 1.5-CPU quota gets 2
	if procs < 1 {
		procs = 1
	}
	if hw := runtime.NumCPU(); procs > hw {
		procs = hw
	}
	if procs != runtime.GOMAXPROCS(0) {
		runtime.GOMAXPROCS(procs)
		logger.Info("GOMAXPROCS pinned to container CPU quota", "quota", q, "gomaxprocs", procs, "hostCPU", runtime.NumCPU())
	}
}

// cgroupCPUQuota returns the container's CPU allowance in whole cores
// (e.g. 0.5, 2.0), or 0 when there is no limit.
func cgroupCPUQuota() float64 {
	// cgroup v2: "<quota> <period>" in microseconds, or "max <period>".
	if b, err := os.ReadFile("/sys/fs/cgroup/cpu.max"); err == nil {
		f := strings.Fields(strings.TrimSpace(string(b)))
		if len(f) == 2 && f[0] != "max" {
			quota, e1 := strconv.ParseFloat(f[0], 64)
			period, e2 := strconv.ParseFloat(f[1], 64)
			if e1 == nil && e2 == nil && quota > 0 && period > 0 {
				return quota / period
			}
		}
	}
	// cgroup v1.
	qb, err1 := os.ReadFile("/sys/fs/cgroup/cpu/cpu.cfs_quota_us")
	pb, err2 := os.ReadFile("/sys/fs/cgroup/cpu/cpu.cfs_period_us")
	if err1 == nil && err2 == nil {
		quota, e1 := strconv.ParseFloat(strings.TrimSpace(string(qb)), 64)
		period, e2 := strconv.ParseFloat(strings.TrimSpace(string(pb)), 64)
		if e1 == nil && e2 == nil && quota > 0 && period > 0 {
			return quota / period
		}
	}
	return 0
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
