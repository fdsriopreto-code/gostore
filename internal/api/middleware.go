package api

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net/http"
	"runtime/debug"
	"time"

	"github.com/lojadopocket/gostore/internal/logger"
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
	status int
	bytes  int
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
