package api

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"io"
	"net/http"
	"os"
	"runtime/debug"
	"time"

	"github.com/lojadopocket/gostore/internal/logger"
	"github.com/lojadopocket/gostore/internal/metrics"
)

type ctxKey int

const ctxKeyRequestID ctxKey = iota

// newRequestID returns a random 16-hex-char id.
func newRequestID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

func requestIDFrom(r *http.Request) string {
	if v, ok := r.Context().Value(ctxKeyRequestID).(string); ok {
		return v
	}
	return ""
}

// statusRecorder captures the response status for access logging.
type statusRecorder struct {
	http.ResponseWriter
	status    int
	bytes     int
	accessKey string // filled by handleS3 once the caller is known
	s3action  string // filled by handleS3 once the request is parsed
}

func (s *statusRecorder) WriteHeader(code int) {
	s.status = code
	s.ResponseWriter.WriteHeader(code)
}

func (s *statusRecorder) Write(b []byte) (int, error) {
	if s.status == 0 {
		s.status = http.StatusOK
	}
	n, err := s.ResponseWriter.Write(b)
	s.bytes += n
	return n, err
}

// Unwrap lets http.NewResponseController reach the real ResponseWriter for
// Flush / SetReadDeadline / Hijack.
func (s *statusRecorder) Unwrap() http.ResponseWriter { return s.ResponseWriter }

// chain applies middlewares in order (first listed runs outermost).
func chain(h http.Handler, mw ...func(http.Handler) http.Handler) http.Handler {
	for i := len(mw) - 1; i >= 0; i-- {
		h = mw[i](h)
	}
	return h
}

// withRequestID assigns a request id and echoes it back.
func withRequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := newRequestID()
		w.Header().Set("x-amz-request-id", id)
		ctx := context.WithValue(r.Context(), ctxKeyRequestID, id)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// withRecover turns a panic into a 500 InternalError instead of dropping the
// connection.
func withRecover(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				logger.Error("panic serving request",
					"panic", rec, "path", r.URL.Path, "stack", string(debug.Stack()))
				writeErrorResponse(w, r, ErrInternalError, r.URL.Path)
			}
		}()
		next.ServeHTTP(w, r)
	})
}

// withAccessLog logs one line per request after it completes.
func withAccessLog(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w}
		next.ServeHTTP(rec, r)
		dur := time.Since(start)
		inBytes := r.ContentLength
		if inBytes < 0 {
			inBytes = 0
		}
		metrics.Record(r.Method, rec.status, inBytes, int64(rec.bytes), dur.Seconds())
		if !isInternalPath(r.URL.Path) {
			activity.add(activityEntry{
				Time: start.UTC(), Method: r.Method, Path: r.URL.Path,
				Status: rec.status, Bytes: rec.bytes, DurMS: dur.Milliseconds(),
				IP: clientIP(r), Access: rec.accessKey, ReqID: requestIDFrom(r),
				S3Action: rec.s3action,
				Err:      rec.Header().Get("x-gostore-error"),
				Cache:    rec.Header().Get("x-gostore-cache"),
			})
		}
		logger.Info("s3 request",
			"method", r.Method,
			"path", r.URL.Path,
			"query", r.URL.RawQuery,
			"status", rec.status,
			"bytes", rec.bytes,
			"remote", r.RemoteAddr,
			"dur_ms", time.Since(start).Milliseconds(),
			"rid", requestIDFrom(r),
		)
	})
}

// withBodyIdleTimeout bumps the connection read deadline before every request
// body Read, so a client that opens a PUT and then stalls can't pin the
// object's namespace lock forever — a stuck read fails and the handler
// unwinds. A slow-but-progressing upload is fine (each Read pushes the
// deadline out). GOSTORE_IDLE_TIMEOUT (default 90s), 0 disables.
func idleTimeoutSetting() time.Duration {
	if v := os.Getenv("GOSTORE_IDLE_TIMEOUT"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return 90 * time.Second
}

func withBodyIdleTimeout(next http.Handler) http.Handler {
	d := idleTimeoutSetting()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if d > 0 && r.Body != nil && r.ContentLength != 0 {
			r.Body = &deadlineBody{rc: r.Body, ctl: http.NewResponseController(w), d: d}
		}
		next.ServeHTTP(w, r)
	})
}

type deadlineBody struct {
	rc  io.ReadCloser
	ctl *http.ResponseController
	d   time.Duration
}

func (b *deadlineBody) Read(p []byte) (int, error) {
	_ = b.ctl.SetReadDeadline(time.Now().Add(b.d))
	return b.rc.Read(p)
}
func (b *deadlineBody) Close() error { return b.rc.Close() }
