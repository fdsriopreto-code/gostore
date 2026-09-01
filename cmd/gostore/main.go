// Command gostore is an S3-compatible object storage server.
//
// Usage:
//
//	gostore server [flags] VOLUME [VOLUME...]
//	gostore version
//
// A single VOLUME runs in single-disk mode; multiple VOLUMEs (even count,
// >=4) run in erasure mode (M4+). Ellipsis specs like ./data/disk{1...4}
// are expanded.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/lojadopocket/gostore/internal/api"
	"github.com/lojadopocket/gostore/internal/bucketcfg"
	"github.com/lojadopocket/gostore/internal/config"
	"github.com/lojadopocket/gostore/internal/erasure"
	"github.com/lojadopocket/gostore/internal/event"
	"github.com/lojadopocket/gostore/internal/iam"
	"github.com/lojadopocket/gostore/internal/kms"
	"github.com/lojadopocket/gostore/internal/logger"
	"github.com/lojadopocket/gostore/internal/object"
	fsbackend "github.com/lojadopocket/gostore/internal/object/fs"
	"github.com/lojadopocket/gostore/internal/scanner"
	"github.com/lojadopocket/gostore/internal/storage"
)

var version = "0.3.0-m3"

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}

	switch os.Args[1] {
	case "server":
		if err := runServer(os.Args[2:]); err != nil {
			logger.Fatal("server stopped with error", "err", err)
		}
	case "version", "-version", "--version", "-v":
		fmt.Printf("gostore %s %s/%s (%s)\n", version, runtime.GOOS, runtime.GOARCH, runtime.Version())
	case "help", "-h", "--help":
		usage()
	default:
		fmt.Fprintf(os.Stderr, "gostore: unknown command %q\n\n", os.Args[1])
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `gostore - S3-compatible object storage

Usage:
  gostore server [flags] VOLUME [VOLUME...]
  gostore version

Flags (server):
  --address ADDR           S3 API listen address (default ":9000")
  --console-address ADDR   Web console listen address (default ":9001")
  --log-level LEVEL         debug|info|warn|error (default "info")
  --log-json               emit logs as JSON

Environment:
  GOSTORE_ROOT_USER, GOSTORE_ROOT_PASSWORD, GOSTORE_REGION,
  GOSTORE_ADDRESS, GOSTORE_CONSOLE_ADDRESS, GOSTORE_DOMAIN,
  GOSTORE_LOG_LEVEL, GOSTORE_LOG_JSON

Examples:
  gostore server ./data
  gostore server --address :9000 ./data/disk{1...4}
`)
}

func runServer(args []string) error {
	fs := flag.NewFlagSet("server", flag.ContinueOnError)
	var (
		address        = fs.String("address", "", "S3 API listen address")
		consoleAddress = fs.String("console-address", "", "web console listen address")
		logLevel       = fs.String("log-level", "", "debug|info|warn|error")
		logJSON        = fs.Bool("log-json", false, "emit logs as JSON")
	)
	if err := fs.Parse(args); err != nil {
		return err
	}

	cfg := config.Default()
	cfg.ApplyEnv()
	if *address != "" {
		cfg.Address = *address
	}
	if *consoleAddress != "" {
		cfg.ConsoleAddress = *consoleAddress
	}
	if *logLevel != "" {
		cfg.LogLevel = *logLevel
	}
	if *logJSON {
		cfg.LogJSON = true
	}

	groups, err := expandVolumeGroups(fs.Args())
	if err != nil {
		return err
	}
	cfg.VolumeGroups = groups
	for _, g := range groups {
		cfg.Volumes = append(cfg.Volumes, g...)
	}

	if err := cfg.Validate(); err != nil {
		return err
	}

	logger.Setup(cfg.LogLevel, cfg.LogJSON)
	logger.Info("starting gostore",
		"version", version,
		"api", cfg.Address,
		"console", cfg.ConsoleAddress,
		"volumes", cfg.Volumes,
		"mode", modeString(cfg),
	)

	km, err := kms.New(cfg.Volumes)
	if err != nil {
		return fmt.Errorf("init KMS: %w", err)
	}

	var obj object.Layer
	if cfg.SingleDisk() {
		backend, err := fsbackend.New(cfg.VolumeGroups[0][0])
		if err != nil {
			return fmt.Errorf("open volume %s: %w", cfg.VolumeGroups[0][0], err)
		}
		backend.SetKMS(km)
		obj = backend
	} else {
		sets := make([]*erasure.Set, len(cfg.VolumeGroups))
		for si, group := range cfg.VolumeGroups {
			disks := make([]erasure.Disk, len(group))
			for di, v := range group {
				d, err := storage.OpenLocalDisk(v, si, di)
				if err != nil {
					return fmt.Errorf("open disk %s: %w", v, err)
				}
				disks[di] = d
			}
			set, err := erasure.NewSet(disks)
			if err != nil {
				return fmt.Errorf("init erasure set %d: %w", si+1, err)
			}
			sets[si] = set
		}
		pool, err := erasure.NewPool(sets...)
		if err != nil {
			return fmt.Errorf("init erasure pool: %w", err)
		}
		pool.SetKMS(km)
		obj = pool
		n := len(cfg.VolumeGroups[0])
		logger.Info("erasure backend ready",
			"sets", len(sets), "disksPerSet", n,
			"dataBlocks", n-n/2, "parityBlocks", n/2)
	}

	iamMgr, err := iam.New(cfg.RootUser, cfg.RootPassword, cfg.Volumes)
	if err != nil {
		return fmt.Errorf("init IAM: %w", err)
	}
	logger.Info("IAM ready", "rootAccessKey", cfg.RootUser, "users", len(iamMgr.ListUsers()))
	if os.Getenv("GOSTORE_ALLOW_ANONYMOUS") == "1" {
		logger.Warn("GOSTORE_ALLOW_ANONYMOUS=1: unsigned requests are accepted and skip authorization")
	}

	bcfg, err := bucketcfg.Open(cfg.Volumes)
	if err != nil {
		return fmt.Errorf("init bucket config: %w", err)
	}
	bus := event.New(bcfg)

	scanInterval := time.Hour
	if v := os.Getenv("GOSTORE_SCAN_INTERVAL"); v != "" {
		if d, perr := time.ParseDuration(v); perr == nil && d > 0 {
			scanInterval = d
		}
	}
	scanCtx, stopScan := context.WithCancel(context.Background())
	defer stopScan()
	go scanner.New(obj, bcfg, scanInterval).Run(scanCtx)
	logger.Info("lifecycle scanner started", "interval", scanInterval)

	apiSrv := &http.Server{
		Addr:              cfg.Address,
		Handler:           api.NewServer(cfg, obj, iamMgr, bcfg, bus),
		ReadHeaderTimeout: 10 * time.Second,
	}
	consoleSrv := &http.Server{
		Addr:              cfg.ConsoleAddress,
		Handler:           consolePlaceholder(cfg),
		ReadHeaderTimeout: 10 * time.Second,
	}

	errCh := make(chan error, 2)
	go func() {
		logger.Info("S3 API listening", "addr", cfg.Address)
		if err := apiSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- fmt.Errorf("api server: %w", err)
		}
	}()
	go func() {
		logger.Info("console listening", "addr", cfg.ConsoleAddress)
		if err := consoleSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- fmt.Errorf("console server: %w", err)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	select {
	case err := <-errCh:
		return err
	case sig := <-sigCh:
		logger.Info("shutdown signal received", "signal", sig.String())
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_ = apiSrv.Shutdown(shutdownCtx)
	_ = consoleSrv.Shutdown(shutdownCtx)
	if err := obj.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("backend shutdown: %w", err)
	}
	logger.Info("stopped cleanly")
	return nil
}

func modeString(c config.Config) string {
	if c.SingleDisk() {
		return "single-disk"
	}
	if len(c.VolumeGroups) == 1 {
		return fmt.Sprintf("erasure (1 set, %d disks)", len(c.VolumeGroups[0]))
	}
	return fmt.Sprintf("erasure pool (%d sets)", len(c.VolumeGroups))
}

// expandVolumeGroups turns each CLI argument into a group of disk paths,
// expanding a MinIO-style ellipsis spec ("./data/disk{1...4}"). Each group
// becomes one erasure set; all groups form one pool.
func expandVolumeGroups(args []string) ([][]string, error) {
	if len(args) == 0 {
		return nil, errors.New("no volumes given")
	}
	var groups [][]string
	for _, a := range args {
		openIdx := strings.IndexByte(a, '{')
		closeIdx := strings.IndexByte(a, '}')
		if openIdx < 0 || closeIdx < 0 || closeIdx < openIdx {
			groups = append(groups, []string{a})
			continue
		}
		inner := a[openIdx+1 : closeIdx]
		lo, hi, ok := parseEllipsis(inner)
		if !ok {
			return nil, fmt.Errorf("invalid ellipsis spec %q (want {N...M})", a)
		}
		prefix, suffix := a[:openIdx], a[closeIdx+1:]
		var g []string
		for i := lo; i <= hi; i++ {
			g = append(g, fmt.Sprintf("%s%d%s", prefix, i, suffix))
		}
		groups = append(groups, g)
	}
	return groups, nil
}

func parseEllipsis(s string) (lo, hi int, ok bool) {
	i := strings.Index(s, "...")
	if i < 0 {
		return 0, 0, false
	}
	var err1, err2 error
	lo, err1 = strconv.Atoi(strings.TrimSpace(s[:i]))
	hi, err2 = strconv.Atoi(strings.TrimSpace(s[i+3:]))
	if err1 != nil || err2 != nil || hi < lo {
		return 0, 0, false
	}
	return lo, hi, true
}

// consolePlaceholder is the interim web console: a single status page. The
// real UI is milestone M14.
func consolePlaceholder(cfg config.Config) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		fmt.Fprintf(w, `<!doctype html><meta charset=utf-8><title>gostore console</title>
<body style="font:14px/1.5 system-ui;margin:3rem;max-width:44rem;color:#1a1a1a">
<h1 style="margin-bottom:.2rem">gostore</h1>
<p style="color:#666;margin-top:0">S3-compatible object storage &mdash; version %s</p>
<table style="border-collapse:collapse">
<tr><td style="padding:.2rem 1rem .2rem 0;color:#666">S3 API port</td><td><code>%s</code></td></tr>
<tr><td style="padding:.2rem 1rem .2rem 0;color:#666">Console port</td><td><code>%s</code> (this page)</td></tr>
<tr><td style="padding:.2rem 1rem .2rem 0;color:#666">Region</td><td><code>%s</code></td></tr>
<tr><td style="padding:.2rem 1rem .2rem 0;color:#666">Mode</td><td><code>%s</code></td></tr>
</table>
<p style="margin-top:1.5rem">Status page only. Point your reverse proxy / PaaS at the
<strong>S3 API port (%s)</strong> &mdash; that port serves the S3 API, the
web console, and the health endpoints:</p>
<ul>
<li><strong>Web console:</strong> <code>&lt;api-host&gt;/gostore/console/</code></li>
<li><code>GET /gostore/health/live</code> &nbsp;/&nbsp; <code>/gostore/health/ready</code></li>
<li><code>GET /gostore/health/selftest</code> &mdash; full write/read/verify/delete round-trip</li>
</ul>
<p>The S3 API requires AWS Signature V4 &mdash; use the console, <code>mc</code>, the
AWS CLI/SDKs, or any S3 client (path-style).</p>
</body>`, version, cfg.Address, cfg.ConsoleAddress, cfg.Region, modeString(cfg), cfg.Address)
	})
}
