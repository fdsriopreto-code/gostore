package api

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/lojadopocket/gostore/internal/event"
	"github.com/lojadopocket/gostore/internal/object"
)

// appendMaxBytes caps the size an append-target object may reach. Append is
// read-modify-write (like this backend's own multipart-complete), so an
// unbounded target would mean unbounded memory per append. Rotate/segment
// beyond this. GOSTORE_APPEND_MAX, default 64 MiB.
func appendMaxBytes() int64 {
	if v := os.Getenv("GOSTORE_APPEND_MAX"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 {
			return n
		}
	}
	return 64 << 20
}

// handleAppendObject implements PutObject with x-amz-write-offset-bytes: the
// body is appended to the object iff the offset equals its current size
// (S3 Express "append"). Concurrency-safe via optimistic ETag matching — a
// racing append makes one of them fail with 409 InvalidWriteOffset to retry.
// Neither MinIO nor plain S3 (non-Express) support this.
func (s *Server) handleAppendObject(w http.ResponseWriter, r *http.Request, bucket, key string, size int64) {
	res := "/" + bucket + "/" + key

	off, perr := strconv.ParseInt(strings.TrimSpace(r.Header.Get("x-amz-write-offset-bytes")), 10, 64)
	if perr != nil || off < 0 {
		writeErrorResponse(w, r, ErrInvalidArgument, res)
		return
	}
	if s.bcfg != nil && s.bcfg.Get(bucket).Versioning == "Enabled" {
		writeErrorResponse(w, r, ErrNotImplemented, res) // append is undefined for versioned buckets
		return
	}
	maxBytes := appendMaxBytes()

	cur, statErr := s.obj.GetObjectInfo(r.Context(), bucket, key, object.ObjectOptions{})
	creating := statErr != nil
	if creating {
		if off != 0 {
			writeErrorResponse(w, r, ErrInvalidWriteOffset, res)
			return
		}
	} else {
		if cur.Size != off {
			w.Header().Set("x-amz-object-size", strconv.FormatInt(cur.Size, 10))
			writeErrorResponse(w, r, ErrInvalidWriteOffset, res)
			return
		}
		if cur.Size+size > maxBytes {
			writeErrorResponse(w, r, ErrEntityTooLarge, res)
			return
		}
	}

	delta, rerr := io.ReadAll(io.LimitReader(r.Body, size+1))
	if rerr != nil || int64(len(delta)) != size {
		writeErrorResponse(w, r, ErrIncompleteBody, res)
		return
	}

	opts := object.ObjectOptions{}
	var combined []byte
	if creating {
		combined = delta
		opts.UserDefined = extractMetadata(r, key)
	} else {
		gr, gerr := s.obj.GetObjectNInfo(r.Context(), bucket, key, nil, nil, object.ObjectOptions{})
		if gerr != nil {
			writeErrorResponse(w, r, toAPIError(gerr), res)
			return
		}
		existing, xerr := io.ReadAll(gr)
		_ = gr.Close()
		if xerr != nil {
			writeErrorResponse(w, r, ErrInternalError, res)
			return
		}
		if int64(len(existing)) != cur.Size {
			writeErrorResponse(w, r, ErrInvalidWriteOffset, res) // changed under us
			return
		}
		combined = append(existing, delta...)
		// Append keeps the object's original content-type / user metadata;
		// headers on the append request are ignored (matches S3 Express).
		opts.UserDefined = make(map[string]string, len(cur.UserDefined))
		for k, v := range cur.UserDefined {
			opts.UserDefined[k] = v
		}
		wantETag := strings.Trim(cur.ETag, `"`)
		opts.CheckPrecondFn = func(oi object.ObjectInfo) bool {
			return strings.Trim(oi.ETag, `"`) != wantETag // abort if a racing append moved it
		}
	}

	if s.quotaExceeded(bucket, int64(len(combined))) {
		writeErrorResponse(w, r, ErrQuotaExceeded, res)
		return
	}

	ni, err := s.obj.PutObject(r.Context(), bucket, key,
		object.NewPutObjReader(bytes.NewReader(combined), int64(len(combined)), int64(len(combined))), opts)
	if err != nil {
		if errors.Is(err, object.ErrPreconditionFailed) {
			writeErrorResponse(w, r, ErrInvalidWriteOffset, res)
			return
		}
		writeErrorResponse(w, r, toAPIError(err), res)
		return
	}

	w.Header().Set("ETag", quoteETag(ni.ETag))
	w.Header().Set("x-amz-object-size", strconv.FormatInt(ni.Size, 10))
	s.notify(r, event.ObjectCreated, bucket, key, ni.Size, ni.ETag)
	writeSuccessOK(w)
}
