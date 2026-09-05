package gostorecli

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// cmdBench: gostore bench NAME/bucket [--duration 30s] [--size 1MiB]
//
//	[--concurrency 20] [--mix put,get,delete]
func cmdBench(a []string) error {
	dur := 30 * time.Second
	size := int64(1 << 20)
	conc := 20
	doPut, doGet, doDel := true, true, true

	var pos []string
	for i := 0; i < len(a); i++ {
		switch a[i] {
		case "--duration":
			i++
			if d, e := time.ParseDuration(a[i]); e == nil {
				dur = d
			}
		case "--size":
			i++
			if n, ok := parseSizeArg(a[i]); ok {
				size = n
			}
		case "--concurrency", "-c":
			i++
			conc, _ = strconv.Atoi(a[i])
		case "--mix":
			i++
			doPut = contains(a[i], "put")
			doGet = contains(a[i], "get")
			doDel = contains(a[i], "delete")
		default:
			pos = append(pos, a[i])
		}
	}
	if len(pos) != 1 || conc < 1 {
		return fmt.Errorf("usage: gostore bench NAME/bucket [--duration 30s] [--size 1MiB] [--concurrency 20] [--mix put,get,delete]")
	}
	t, err := parseTarget(pos[0])
	if err != nil {
		return err
	}
	if !t.isRemote || t.bucket == "" {
		return fmt.Errorf("bench needs NAME/bucket")
	}

	// Create the bench bucket if it's not there yet (ignore "already exists").
	if resp, e := t.do(http.MethodPut, "/"+t.bucket, nil, nil, nil); e == nil && resp != nil {
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}

	payload := make([]byte, size)
	_, _ = rand.Read(payload)

	var (
		puts, gets, dels, errs, misses atomic.Int64
		putBytes, getBytes             atomic.Int64
		latMu                          sync.Mutex
		lats                           []time.Duration
	)

	// When reading/deleting without a PUT in the mix, first lay down a small
	// corpus so GET/DELETE hit real objects instead of 404ing. Each worker
	// then cycles its key index within [0,corpus).
	const corpus = 64
	if (doGet || doDel) && !doPut {
		fmt.Printf("warming up %d objects (%s each)…\n", conc*corpus, humanSize(size))
		var wwg sync.WaitGroup
		for w := 0; w < conc; w++ {
			wwg.Add(1)
			go func(w int) {
				defer wwg.Done()
				for i := 0; i < corpus; i++ {
					resp, e := t.do(http.MethodPut, objPath(t.bucket, fmt.Sprintf("bench/w%d-%d", w, i)), nil, payload, nil)
					_ = okResp(resp, e)
				}
			}(w)
		}
		wwg.Wait()
	}

	ctx, cancel := context.WithTimeout(context.Background(), dur)
	defer cancel()

	fmt.Printf("bench %s  size=%s concurrency=%d duration=%s\n", pos[0], humanSize(size), conc, dur)
	start := time.Now()
	var wg sync.WaitGroup
	for w := 0; w < conc; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			n := 0
			for ctx.Err() == nil {
				idx := n
				if !doPut {
					idx = n % corpus // stay within the warm-up corpus
				}
				key := fmt.Sprintf("bench/w%d-%d", w, idx)
				n++
				if doPut {
					s := time.Now()
					resp, e := t.do(http.MethodPut, objPath(t.bucket, key), nil, payload, nil)
					record(&latMu, &lats, time.Since(s))
					if okResp(resp, e) {
						puts.Add(1)
						putBytes.Add(size)
					} else {
						errs.Add(1)
						continue
					}
				}
				if doGet {
					s := time.Now()
					resp, e := t.do(http.MethodGet, objPath(t.bucket, key), nil, nil, nil)
					switch {
					case e == nil && resp.StatusCode/100 == 2:
						nn, _ := io.Copy(io.Discard, resp.Body)
						resp.Body.Close()
						record(&latMu, &lats, time.Since(s))
						gets.Add(1)
						getBytes.Add(nn)
					case e == nil && resp.StatusCode == http.StatusNotFound:
						resp.Body.Close()
						misses.Add(1)
					default:
						if resp != nil {
							resp.Body.Close()
						}
						errs.Add(1)
					}
				}
				if doDel {
					resp, e := t.do(http.MethodDelete, objPath(t.bucket, key), nil, nil, nil)
					switch {
					case okResp(resp, e):
						dels.Add(1)
					case e == nil && resp != nil && resp.StatusCode == http.StatusNotFound:
						misses.Add(1)
					default:
						errs.Add(1)
					}
				}
			}
		}(w)
	}
	wg.Wait()
	el := time.Since(start).Seconds()

	latMu.Lock()
	sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })
	pct := func(p float64) time.Duration {
		if len(lats) == 0 {
			return 0
		}
		return lats[int(float64(len(lats))*p)%len(lats)]
	}
	p50, p95, p99 := pct(0.50), pct(0.95), pct(0.99)
	latMu.Unlock()

	total := puts.Load() + gets.Load() + dels.Load()
	fmt.Printf("\n  ops        %d  (%.0f/s)\n", total, float64(total)/el)
	fmt.Printf("  PUT        %d   %s written  (%.1f MiB/s)\n", puts.Load(), humanSize(putBytes.Load()), float64(putBytes.Load())/el/(1<<20))
	fmt.Printf("  GET        %d   %s read     (%.1f MiB/s)\n", gets.Load(), humanSize(getBytes.Load()), float64(getBytes.Load())/el/(1<<20))
	fmt.Printf("  DELETE     %d\n", dels.Load())
	fmt.Printf("  errors     %d\n", errs.Load())
	if m := misses.Load(); m > 0 {
		fmt.Printf("  misses     %d  (404 — key not in the corpus, not a failure)\n", m)
	}
	fmt.Printf("  latency    p50 %s   p95 %s   p99 %s\n", p50.Round(time.Millisecond), p95.Round(time.Millisecond), p99.Round(time.Millisecond))
	if total > 0 && errs.Load() > total/20 {
		fmt.Fprintln(os.Stderr, "\n  (>5% errors — likely hit a limit: admission control / rate limit / disk. That's the ceiling.)")
	}
	return nil
}

func record(mu *sync.Mutex, l *[]time.Duration, d time.Duration) {
	mu.Lock()
	if len(*l) < 200_000 {
		*l = append(*l, d)
	}
	mu.Unlock()
}

func okResp(resp *http.Response, err error) bool {
	if err != nil {
		return false
	}
	ok := resp.StatusCode/100 == 2
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	return ok
}

func contains(csv, want string) bool {
	for _, p := range bytes.Split([]byte(csv), []byte(",")) {
		if string(bytes.TrimSpace(p)) == want {
			return true
		}
	}
	return false
}

func parseSizeArg(s string) (int64, bool) {
	n := parseBytesLoose(s)
	return n, n > 0
}

func parseBytesLoose(s string) int64 {
	s = trimLowerBytes(s)
	mult := int64(1)
	for _, suf := range []struct {
		s string
		m int64
	}{{"gib", 1 << 30}, {"g", 1 << 30}, {"mib", 1 << 20}, {"m", 1 << 20}, {"kib", 1 << 10}, {"k", 1 << 10}} {
		if hasSuffixB(s, suf.s) {
			mult = suf.m
			s = s[:len(s)-len(suf.s)]
			break
		}
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0
	}
	return n * mult
}

func trimLowerBytes(s string) string {
	out := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c == ' ' {
			continue
		}
		if c >= 'A' && c <= 'Z' {
			c += 32
		}
		out = append(out, c)
	}
	return string(out)
}

func hasSuffixB(s, suf string) bool { return len(s) >= len(suf) && s[len(s)-len(suf):] == suf }

func humanSize(n int64) string {
	f := float64(n)
	for _, u := range []string{"B", "KiB", "MiB", "GiB", "TiB"} {
		if f < 1024 || u == "TiB" {
			return fmt.Sprintf("%.1f %s", f, u)
		}
		f /= 1024
	}
	return fmt.Sprintf("%d B", n)
}
