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

	bytesIn  atomic.Uint64
	bytesOut atomic.Uint64

	version = "dev"
)

type reqKey struct {
	method string
	class  string // "2xx", "4xx", ...
}

// SetVersion records the build version for gostore_build_info.
func SetVersion(v string) { version = v }

// Record accounts one finished HTTP request.
func Record(method string, status int, inBytes, outBytes int64) {
	if status <= 0 {
		status = 200
	}
	k := reqKey{method: method, class: strconv.Itoa(status/100) + "xx"}
	mu.Lock()
	reqTotal[k]++
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
