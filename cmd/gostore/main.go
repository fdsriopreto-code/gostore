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
	"github.com/lojadopocket/gostore/internal/auth"
	"github.com/lojadopocket/gostore/internal/config"
	"github.com/lojadopocket/gostore/internal/logger"
	"github.com/lojadopocket/gostore/internal/object"
	fsbackend "github.com/lojadopocket/gostore/internal/object/fs"
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

	vols, err := expandVolumes(fs.Args())
	if err != nil {
		return err
	}
	cfg.Volumes = vols

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

	if !cfg.SingleDisk() {
		return fmt.Errorf("this build (M1–M3) supports single-disk mode only: pass exactly one volume, got %d. "+
			"Erasure-coded / distributed mode is milestone M4", len(cfg.Volumes))
	}

	backend, err := fsbackend.New(cfg.Volumes[0])
	if err != nil {
		return fmt.Errorf("open volume %s: %w", cfg.Volumes[0], err)
	}
	var obj object.Layer = backend

	creds := auth.NewRoot(cfg.RootUser, cfg.RootPassword)
	logger.Info("root credential ready", "accessKey", cfg.RootUser)
	if os.Getenv("GOSTORE_ALLOW_ANONYMOUS") == "1" {
		logger.Warn("GOSTORE_ALLOW_ANONYMOUS=1: unsigned requests are accepted")
	}

	apiSrv := &http.Server{
		Addr:              cfg.Address,
		Handler:           api.NewServer(cfg, obj, creds),
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
	return fmt.Sprintf("erasure (%d volumes)", len(c.Volumes))
}

// expandVolumes expands MinIO-style ellipsis specs. Currently supports a
// single numeric range per path, e.g. "./data/disk{1...4}" ->
// ["./data/disk1", ... "./data/disk4"]. Multiple/nested ranges: M4.
func expandVolumes(args []string) ([]string, error) {
	if len(args) == 0 {
		return nil, errors.New("no volumes given")
	}
	var out []string
	for _, a := range args {
		openIdx := strings.IndexByte(a, '{')
		closeIdx := strings.IndexByte(a, '}')
		if openIdx < 0 || closeIdx < 0 || closeIdx < openIdx {
			out = append(out, a)
			continue
		}
		inner := a[openIdx+1 : closeIdx]
		lo, hi, ok := parseEllipsis(inner)
		if !ok {
			return nil, fmt.Errorf("invalid ellipsis spec %q (want {N...M})", a)
		}
		prefix, suffix := a[:openIdx], a[closeIdx+1:]
		for i := lo; i <= hi; i++ {
			out = append(out, fmt.Sprintf("%s%d%s", prefix, i, suffix))
		}
	}
	return out, nil
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
<strong>S3 API port (%s)</strong> &mdash; that port also serves the health and
self-test endpoints:</p>
<ul>
<li><code>GET /gostore/health/live</code></li>
<li><code>GET /gostore/health/ready</code></li>
<li><code>GET /gostore/health/selftest</code> &mdash; full write/read/verify/delete round-trip</li>
</ul>
<p>The S3 API itself requires AWS Signature V4 &mdash; use <code>mc</code>, the AWS
CLI/SDKs, or any S3 client (path-style). A full web console is milestone M14.</p>
</body>`, version, cfg.Address, cfg.ConsoleAddress, cfg.Region, modeString(cfg), cfg.Address)
	})
}
