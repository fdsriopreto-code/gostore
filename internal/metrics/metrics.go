// Package metrics is a tiny dependency-free Prometheus exposition. The API
// middleware calls Record for every request; the /gostore/metrics handler
// gathers point-in-time gauges (capacity, per-bucket usage) and renders the
// text format.
package metrics

import (
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	start = time.Now()

	mu       sync.Mutex
	reqTotal = map[reqKey]uint64{}
	errTotal = map[string]uint64{}

	bytesIn  atomic.Uint64
	bytesOut atomic.Uint64

	// integrityFail counts objects whose assembled bytes failed the
	// end-to-end checksum check on read (shard-assembly / decode bug that
	// per-block bitrot hashes cannot catch).
	integrityFail atomic.Uint64
	healObjects   atomic.Uint64
	healFail      atomic.Uint64
	repairQueued  atomic.Uint64 // objects enqueued for heal from the read path

	// request-duration histogram (seconds), fixed buckets.
	durBuckets = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}
	durCounts  = make([]uint64, len(durBuckets)+1) // last = +Inf
	durSum     float64
	durN       uint64

	version = "dev"
)

type reqKey struct {
	method string
	class  string // "2xx", "4xx", ...
}

// SetVersion records the build version for gostore_build_info.
func SetVersion(v string) { version = v }

// APIError counts one S3 error response by its code (e.g. "NoSuchKey").
func APIError(code string) {
	mu.Lock()
	errTotal[code]++
	mu.Unlock()
}

// IntegrityFailure records one object that failed end-to-end checksum
// verification when it was read back.
func IntegrityFailure() { integrityFail.Add(1) }

// RepairQueued records that a read reconstructed around a bad shard and
// enqueued the object for background heal.
func RepairQueued() { repairQueued.Add(1) }

// HealResult records the outcome of one background heal attempt.
func HealResult(ok bool) {
	if ok {
		healObjects.Add(1)
	} else {
		healFail.Add(1)
	}
}

// Record accounts one finished HTTP request.
func Record(method string, status int, inBytes, outBytes int64, seconds float64) {
	if status <= 0 {
		status = 200
	}
	k := reqKey{method: method, class: strconv.Itoa(status/100) + "xx"}
	mu.Lock()
	reqTotal[k]++
	i := 0
	for ; i < len(durBuckets); i++ {
		if seconds <= durBuckets[i] {
			break
		}
	}
	durCounts[i]++
	durSum += seconds
	durN++
	mu.Unlock()
	if inBytes > 0 {
		bytesIn.Add(uint64(inBytes))
	}
	if outBytes > 0 {
		bytesOut.Add(uint64(outBytes))
	}
}

// BucketUsage is one bucket's accounted size for the gauges.
type BucketUsage struct {
	Bucket  string
	Objects int64
	Bytes   int64
}

// Gauges are the point-in-time values the handler collects per scrape.
type Gauges struct {
	Mode          string
	DisksTotal    int
	DisksOnline   int
	CapacityBytes uint64
	FreeBytes     uint64
	Buckets       []BucketUsage
	HealingDrives int
	Healthy       bool
}

// WritePrometheus renders the exposition format.
func WritePrometheus(w io.Writer, g Gauges) {
	var b strings.Builder

	line := func(s string) { b.WriteString(s); b.WriteByte('\n') }
	help := func(name, typ, doc string) {
		line("# HELP " + name + " " + doc)
		line("# TYPE " + name + " " + typ)
	}

	help("gostore_build_info", "gauge", "Build metadata; value is always 1.")
	line(fmt.Sprintf("gostore_build_info{version=%q,mode=%q} 1", version, g.Mode))

	help("gostore_uptime_seconds", "gauge", "Seconds since the process started.")
	line(fmt.Sprintf("gostore_uptime_seconds %d", int64(time.Since(start).Seconds())))

	help("gostore_up", "gauge", "1 when storage reports healthy, else 0.")
	up := 0
	if g.Healthy {
		up = 1
	}
	line(fmt.Sprintf("gostore_up %d", up))

	help("gostore_http_requests_total", "counter", "HTTP requests by method and status class.")
	mu.Lock()
	keys := make([]reqKey, 0, len(reqTotal))
	for k := range reqTotal {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].method != keys[j].method {
			return keys[i].method < keys[j].method
		}
		return keys[i].class < keys[j].class
	})
	for _, k := range keys {
		line(fmt.Sprintf("gostore_http_requests_total{method=%q,status=%q} %d", k.method, k.class, reqTotal[k]))
	}
	mu.Unlock()

	help("gostore_http_request_bytes_total", "counter", "Total request body bytes received.")
	line(fmt.Sprintf("gostore_http_request_bytes_total %d", bytesIn.Load()))
	help("gostore_http_response_bytes_total", "counter", "Total response body bytes sent.")
	line(fmt.Sprintf("gostore_http_response_bytes_total %d", bytesOut.Load()))

	help("gostore_http_request_duration_seconds", "histogram", "Request latency.")
	mu.Lock()
	var cum uint64
	for i, ub := range durBuckets {
		cum += durCounts[i]
		line(fmt.Sprintf("gostore_http_request_duration_seconds_bucket{le=%q} %d", strconv.FormatFloat(ub, 'g', -1, 64), cum))
	}
	cum += durCounts[len(durBuckets)]
	line(fmt.Sprintf("gostore_http_request_duration_seconds_bucket{le=\"+Inf\"} %d", cum))
	line(fmt.Sprintf("gostore_http_request_duration_seconds_sum %s", strconv.FormatFloat(durSum, 'g', -1, 64)))
	line(fmt.Sprintf("gostore_http_request_duration_seconds_count %d", durN))
	mu.Unlock()

	mu.Lock()
	if len(errTotal) > 0 {
		help("gostore_s3_errors_total", "counter", "S3 error responses by code.")
		ecodes := make([]string, 0, len(errTotal))
		for c := range errTotal {
			ecodes = append(ecodes, c)
		}
		sort.Strings(ecodes)
		for _, c := range ecodes {
			line(fmt.Sprintf("gostore_s3_errors_total{code=%q} %d", c, errTotal[c]))
		}
	}
	mu.Unlock()

	if v := integrityFail.Load(); v > 0 {
		help("gostore_integrity_failures_total", "counter", "Objects that failed end-to-end checksum verification on read.")
		line(fmt.Sprintf("gostore_integrity_failures_total %d", v))
	}
	if ok, bad := healObjects.Load(), healFail.Load(); ok > 0 || bad > 0 {
		help("gostore_heal_objects_total", "counter", "Background heal attempts by outcome.")
		line(fmt.Sprintf("gostore_heal_objects_total{result=%q} %d", "ok", ok))
		line(fmt.Sprintf("gostore_heal_objects_total{result=%q} %d", "error", bad))
	}
	if v := repairQueued.Load(); v > 0 {
		help("gostore_read_repair_queued_total", "counter", "Objects enqueued for heal because a read reconstructed around a bad shard.")
		line(fmt.Sprintf("gostore_read_repair_queued_total %d", v))
	}

	help("gostore_disks", "gauge", "Disk counts by state.")
	line(fmt.Sprintf("gostore_disks{state=\"total\"} %d", g.DisksTotal))
	line(fmt.Sprintf("gostore_disks{state=\"online\"} %d", g.DisksOnline))
	line(fmt.Sprintf("gostore_disks{state=\"healing\"} %d", g.HealingDrives))

	help("gostore_capacity_bytes", "gauge", "Total raw capacity across disks.")
	line(fmt.Sprintf("gostore_capacity_bytes %d", g.CapacityBytes))
	help("gostore_capacity_free_bytes", "gauge", "Free capacity across disks.")
	line(fmt.Sprintf("gostore_capacity_free_bytes %d", g.FreeBytes))

	if len(g.Buckets) > 0 {
		bs := append([]BucketUsage(nil), g.Buckets...)
		sort.Slice(bs, func(i, j int) bool { return bs[i].Bucket < bs[j].Bucket })
		help("gostore_bucket_objects", "gauge", "Objects per bucket (from the last scan).")
		for _, u := range bs {
			line(fmt.Sprintf("gostore_bucket_objects{bucket=%q} %d", u.Bucket, u.Objects))
		}
		help("gostore_bucket_bytes", "gauge", "Logical bytes per bucket (from the last scan).")
		for _, u := range bs {
			line(fmt.Sprintf("gostore_bucket_bytes{bucket=%q} %d", u.Bucket, u.Bytes))
		}
	}

	_, _ = io.WriteString(w, b.String())
}
