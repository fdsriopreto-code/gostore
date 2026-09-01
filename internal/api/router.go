package api

import (
	"context"
	"net"
	"net/http"
	"os"
	"strings"

	"github.com/lojadopocket/gostore/internal/config"
	"github.com/lojadopocket/gostore/internal/iam"
	"github.com/lojadopocket/gostore/internal/object"
)

// Server is the S3 + admin API HTTP handler.
type Server struct {
	cfg config.Config
	obj object.Layer
	iam *iam.Manager

	domainNames []string
}

type ctxKeyAccessKey struct{}

// NewServer builds the S3 API handler.
func NewServer(cfg config.Config, obj object.Layer, im *iam.Manager) http.Handler {
	s := &Server{cfg: cfg, obj: obj, iam: im}
	if v := strings.TrimSpace(os.Getenv("GOSTORE_DOMAIN")); v != "" {
		for _, d := range strings.Split(v, ",") {
			if d = strings.TrimSpace(d); d != "" {
				s.domainNames = append(s.domainNames, d)
			}
		}
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /gostore/health/live", s.handleHealthLive)
	mux.HandleFunc("GET /gostore/health/ready", s.handleHealthReady)
	mux.HandleFunc("GET /gostore/health/cluster", s.handleHealthReady)
	mux.HandleFunc("GET /gostore/health/selftest", s.handleSelfTest)
	mux.Handle("/gostore/admin/v1/", http.HandlerFunc(s.handleAdmin))
	mux.Handle("/", http.HandlerFunc(s.handleS3))

	return chain(mux,
		withRequestID,
		withRecover,
		withAccessLog,
	)
}

type s3Request struct {
	Bucket string
	Object string
	Style  string
}

func (s *Server) parseRequest(r *http.Request) s3Request {
	host := r.Host
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}
	for _, d := range s.domainNames {
		if host == d {
			break
		}
		if strings.HasSuffix(host, "."+d) {
			return s3Request{
				Bucket: strings.TrimSuffix(host, "."+d),
				Object: strings.TrimPrefix(r.URL.Path, "/"),
				Style:  "vhost",
			}
		}
	}
	p := strings.TrimPrefix(r.URL.Path, "/")
	if p == "" {
		return s3Request{Style: "path"}
	}
	if i := strings.IndexByte(p, '/'); i >= 0 {
		return s3Request{Bucket: p[:i], Object: p[i+1:], Style: "path"}
	}
	return s3Request{Bucket: p, Style: "path"}
}

// handleS3 authenticates, authorizes, then dispatches to the operation handler.
func (s *Server) handleS3(w http.ResponseWriter, r *http.Request) {
	setCommonHeaders(w, s.cfg.Region)
	req := s.parseRequest(r)

	newBody, accessKey, errCode := s.authenticate(r)
	if errCode != ErrNone {
		writeErrorResponse(w, r, errCode, r.URL.Path)
		return
	}
	if newBody != nil {
		r.Body = newBody
	}
	r = r.WithContext(context.WithValue(r.Context(), ctxKeyAccessKey{}, accessKey))

	// STS AssumeRole is allowed for any authenticated caller, ahead of authz.
	if req.Bucket == "" && isSTSRequest(r) {
		s.handleAssumeRole(w, r, accessKey)
		return
	}

	q := r.URL.Query()

	// Authorize (skipped for anonymous requests, which are only reachable when
	// GOSTORE_ALLOW_ANONYMOUS=1).
	if accessKey != "" {
		if code := s.authorizeS3(r, req, q, accessKey); code != ErrNone {
			writeErrorResponse(w, r, code, r.URL.Path)
			return
		}
	}

	switch {
	case req.Bucket == "":
		if r.Method == http.MethodGet {
			s.handleListBuckets(w, r)
			return
		}
		writeErrorResponse(w, r, ErrMethodNotAllowed, "/")

	case req.Object == "":
		s.dispatchBucket(w, r, req.Bucket, q)

	default:
		s.dispatchObject(w, r, req.Bucket, req.Object, q)
	}
}

func (s *Server) dispatchBucket(w http.ResponseWriter, r *http.Request, bucket string, q map[string][]string) {
	has := func(k string) bool { _, ok := q[k]; return ok }
	switch r.Method {
	case http.MethodGet:
		switch {
		case has("location"):
			s.handleGetBucketLocation(w, r, bucket)
		case has("uploads"):
			s.handleListMultipartUploads(w, r, bucket)
		case has("versioning"):
			s.handleGetBucketVersioning(w, r, bucket)
		case has("versions"):
			s.handleListObjectVersions(w, r, bucket)
		case q["list-type"] != nil && q["list-type"][0] == "2":
			s.handleListObjectsV2(w, r, bucket)
		default:
			s.handleListObjectsV1(w, r, bucket)
		}
	case http.MethodPut:
		if has("versioning") || has("acl") || has("policy") || has("tagging") ||
			has("lifecycle") || has("cors") || has("object-lock") {
			// Feature endpoints (M10). Accept-and-ignore keeps clients happy.
			writeSuccessOK(w)
			return
		}
		s.handleCreateBucket(w, r, bucket)
	case http.MethodDelete:
		if has("policy") || has("tagging") || has("lifecycle") || has("cors") {
			writeSuccessNoContent(w)
			return
		}
		s.handleDeleteBucket(w, r, bucket)
	case http.MethodHead:
		s.handleHeadBucket(w, r, bucket)
	case http.MethodPost:
		if has("delete") {
			s.handleDeleteObjects(w, r, bucket)
			return
		}
		writeErrorResponse(w, r, ErrMethodNotAllowed, "/"+bucket)
	default:
		writeErrorResponse(w, r, ErrMethodNotAllowed, "/"+bucket)
	}
}

func (s *Server) dispatchObject(w http.ResponseWriter, r *http.Request, bucket, object string, q map[string][]string) {
	has := func(k string) bool { _, ok := q[k]; return ok }
	res := "/" + bucket + "/" + object
	switch r.Method {
	case http.MethodGet:
		if has("uploadId") {
			s.handleListObjectParts(w, r, bucket, object)
			return
		}
		if has("acl") || has("tagging") {
			writeErrorResponse(w, r, ErrNotImplemented, res)
			return
		}
		s.handleGetObject(w, r, bucket, object)
	case http.MethodHead:
		s.handleHeadObject(w, r, bucket, object)
	case http.MethodPut:
		if has("uploadId") && has("partNumber") {
			if r.Header.Get("x-amz-copy-source") != "" {
				s.handleCopyObjectPart(w, r, bucket, object)
			} else {
				s.handlePutObjectPart(w, r, bucket, object)
			}
			return
		}
		if r.Header.Get("x-amz-copy-source") != "" {
			s.handleCopyObject(w, r, bucket, object)
			return
		}
		if has("acl") || has("tagging") {
			writeSuccessOK(w)
			return
		}
		s.handlePutObject(w, r, bucket, object)
	case http.MethodPost:
		if has("uploads") {
			s.handleNewMultipartUpload(w, r, bucket, object)
			return
		}
		if has("uploadId") {
			s.handleCompleteMultipartUpload(w, r, bucket, object)
			return
		}
		writeErrorResponse(w, r, ErrMethodNotAllowed, res)
	case http.MethodDelete:
		if has("uploadId") {
			s.handleAbortMultipartUpload(w, r, bucket, object)
			return
		}
		s.handleDeleteObject(w, r, bucket, object)
	default:
		writeErrorResponse(w, r, ErrMethodNotAllowed, res)
	}
}
