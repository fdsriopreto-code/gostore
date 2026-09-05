package api

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/lojadopocket/gostore/internal/bucketcfg"
	"github.com/lojadopocket/gostore/internal/event"
	"github.com/lojadopocket/gostore/internal/object"
)

// Ingest keys: a long-lived, write-only, prefix-scoped upload token so any
// backend can push a backup with one header — no SigV4, no SDK.
//
//   curl -X PUT --data-binary @db.sql \
//        -H "Authorization: Bearer gik_..." \
//        https://storage.example/gostore/ingest/backups/pg/db-2026-09-05.sql
//
// A key that ends in "/" (or an empty object part) gets an auto-dated
// filename. MinIO / S3 have nothing like this — they force SigV4 or
// presigned URLs.

func newIngestToken() string {
	var b [24]byte
	_, _ = rand.Read(b[:])
	return "gik_" + hex.EncodeToString(b[:])
}

func hashToken(tok string) string {
	sum := sha256.Sum256([]byte(tok))
	return hex.EncodeToString(sum[:])
}

// --- admin management -------------------------------------------------

// GET /gostore/admin/v1/ingest-keys?bucket=X
func (s *Server) adminIngestKeysList(w http.ResponseWriter, r *http.Request) {
	bucket := r.URL.Query().Get("bucket")
	keys := s.bcfg.Get(bucket).IngestKeys
	out := make([]map[string]any, 0, len(keys))
	for _, k := range keys {
		out = append(out, map[string]any{
			"id": k.ID, "prefix": k.Prefix, "label": k.Label, "createdAt": k.CreatedAt,
		})
	}
	writeJSON(w, http.StatusOK, out)
}

// POST /gostore/admin/v1/ingest-keys?bucket=X   body: {"prefix":"pg/","label":"nightly pg dump"}
func (s *Server) adminIngestKeyCreate(w http.ResponseWriter, r *http.Request) {
	bucket := r.URL.Query().Get("bucket")
	if bucket == "" {
		writeJSONError(w, http.StatusBadRequest, "bucket query param required")
		return
	}
	if _, err := s.obj.GetBucketInfo(r.Context(), bucket); err != nil {
		writeJSONError(w, http.StatusNotFound, "no such bucket")
		return
	}
	var body struct{ Prefix, Label string }
	if !decodeJSON(w, r, &body) {
		return
	}
	prefix := strings.TrimPrefix(strings.TrimSpace(body.Prefix), "/")
	tok := newIngestToken()
	k := bucketcfg.IngestKey{
		ID:   hex.EncodeToString(func() []byte { b := make([]byte, 6); _, _ = rand.Read(b); return b }()),
		Hash: hashToken(tok), Prefix: prefix, Label: strings.TrimSpace(body.Label),
		CreatedAt: time.Now().UTC(),
	}
	if err := s.bcfg.Update(bucket, func(c *bucketcfg.Config) {
		c.IngestKeys = append(c.IngestKeys, k)
	}); err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{
		"id": k.ID, "prefix": k.Prefix, "label": k.Label,
		"token": tok, // shown once
		"example": "curl -X PUT --data-binary @file -H 'Authorization: Bearer " + tok + "' " +
			schemeHost(r) + "/gostore/ingest/" + bucket + "/" + prefix,
	})
}

// DELETE /gostore/admin/v1/ingest-keys?bucket=X&id=Y
func (s *Server) adminIngestKeyDelete(w http.ResponseWriter, r *http.Request) {
	bucket, id := r.URL.Query().Get("bucket"), r.URL.Query().Get("id")
	_ = s.bcfg.Update(bucket, func(c *bucketcfg.Config) {
		out := c.IngestKeys[:0]
		for _, k := range c.IngestKeys {
			if k.ID != id {
				out = append(out, k)
			}
		}
		c.IngestKeys = out
	})
	writeJSON(w, http.StatusOK, map[string]any{"deleted": id})
}

// --- the ingest endpoint -------------------------------------------------

// handleIngest serves PUT|POST /gostore/ingest/{bucket}/{key...}.
func (s *Server) handleIngest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut && r.Method != http.MethodPost {
		http.Error(w, "ingest accepts PUT or POST only", http.StatusMethodNotAllowed)
		return
	}
	if s.ro.on() {
		w.Header().Set("Retry-After", "10")
		http.Error(w, "server is read-only", http.StatusServiceUnavailable)
		return
	}
	rel := strings.TrimPrefix(r.URL.Path, "/gostore/ingest/")
	bucket, key, _ := strings.Cut(rel, "/")
	if bucket == "" {
		http.Error(w, "path must be /gostore/ingest/<bucket>/<key>", http.StatusBadRequest)
		return
	}

	tok := bearerToken(r)
	if tok == "" {
		tok = r.Header.Get("X-Gostore-Ingest-Key")
	}
	if tok == "" {
		http.Error(w, "missing ingest token (Authorization: Bearer ... or X-Gostore-Ingest-Key)", http.StatusUnauthorized)
		return
	}
	want := hashToken(tok)
	var matched bucketcfg.IngestKey
	found := false
	for _, k := range s.bcfg.Get(bucket).IngestKeys {
		if subtle.ConstantTimeCompare([]byte(k.Hash), []byte(want)) == 1 {
			matched, found = k, true
			break
		}
	}
	if !found {
		http.Error(w, "invalid ingest token for this bucket", http.StatusForbidden)
		return
	}
	if matched.Prefix != "" && !strings.HasPrefix(key, matched.Prefix) {
		http.Error(w, "this ingest token is scoped to prefix "+matched.Prefix, http.StatusForbidden)
		return
	}
	// Auto-date when the caller targets a "folder".
	if key == "" || strings.HasSuffix(key, "/") {
		key += time.Now().UTC().Format("20060102T150405Z") + ".bin"
	}

	size := bodySize(r)
	if size < 0 {
		http.Error(w, "Content-Length required", http.StatusLengthRequired)
		return
	}
	if s.quotaExceeded(bucket, size) {
		http.Error(w, "bucket quota exceeded", http.StatusForbidden)
		return
	}
	rel2, ok := s.adm.admit(size)
	if !ok {
		w.Header().Set("Retry-After", "1")
		http.Error(w, "server busy, retry", http.StatusServiceUnavailable)
		return
	}
	defer rel2()

	opts := s.vopts(bucket, r)
	opts.UserDefined = map[string]string{}
	if ct := strings.TrimSpace(r.Header.Get("Content-Type")); ct != "" && ct != "application/octet-stream" {
		opts.UserDefined["content-type"] = ct
	} else if g := sniffContentType(key); g != "" {
		opts.UserDefined["content-type"] = g
	}
	opts.UserDefined["x-amz-meta-ingest-key"] = matched.ID

	oi, err := s.obj.PutObject(r.Context(), bucket, key,
		object.NewPutObjReader(r.Body, size, size), opts)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.notify(r, event.ObjectCreated, bucket, key, oi.Size, oi.ETag)
	w.Header().Set("ETag", quoteETag(oi.ETag))
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_, _ = w.Write([]byte(`{"bucket":"` + bucket + `","key":"` + key + `","etag":"` + oi.ETag + `","size":` + strconv.FormatInt(oi.Size, 10) + `}`))
}

func bearerToken(r *http.Request) string {
	h := r.Header.Get("Authorization")
	if strings.HasPrefix(strings.ToLower(h), "bearer ") {
		return strings.TrimSpace(h[7:])
	}
	return ""
}

func schemeHost(r *http.Request) string { return scheme(r) + "://" + r.Host }
