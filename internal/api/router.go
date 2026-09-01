package api

import (
	"context"
	"net"
	"net/http"
	"os"
	"strings"

	"github.com/lojadopocket/gostore/internal/bucketcfg"
	"github.com/lojadopocket/gostore/internal/config"
	"github.com/lojadopocket/gostore/internal/console"
	"github.com/lojadopocket/gostore/internal/event"
	"github.com/lojadopocket/gostore/internal/iam"
	"github.com/lojadopocket/gostore/internal/object"
	"github.com/lojadopocket/gostore/internal/replication"
	"github.com/lojadopocket/gostore/internal/scanner"
)

// Server is the S3 + admin API HTTP handler.
type Server struct {
	cfg  config.Config
	obj  object.Layer
	iam  *iam.Manager
	bcfg *bucketcfg.Store
	bus  *event.Bus
	repl *replication.Manager
	scan *scanner.Scanner

	domainNames []string
}

type ctxKeyAccessKey struct{}

// NewServer builds the S3 API handler.
func NewServer(cfg config.Config, obj object.Layer, im *iam.Manager, bc *bucketcfg.Store, bus *event.Bus) http.Handler {
	s := &Server{
		cfg: cfg, obj: obj, iam: im, bcfg: bc, bus: bus,
		repl: replication.New(bc, obj),
		scan: scanner.New(obj, bc, 0),
	}
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
	mux.Handle("/gostore/console/", http.StripPrefix("/gostore/console", console.Handler()))
	mux.HandleFunc("/gostore/console", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/gostore/console/", http.StatusFound)
	})
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

	// CORS: answer preflight, and stamp headers on the eventual response.
	if s.applyCORS(w, r, req.Bucket) {
		return
	}

	if code := s.authorizeS3(r, req, q, accessKey); code != ErrNone {
		writeErrorResponse(w, r, code, r.URL.Path)
		return
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
		case has("policy"):
			s.handleGetBucketPolicy(w, r, bucket)
		case has("tagging"):
			s.handleGetBucketTagging(w, r, bucket)
		case has("cors"):
			s.handleGetBucketCORS(w, r, bucket)
		case has("object-lock"):
			s.handleGetBucketObjectLock(w, r, bucket)
		case has("replication"):
			s.handleGetBucketReplication(w, r, bucket)
		case has("lifecycle"):
			s.handleGetBucketLifecycle(w, r, bucket)
		case has("notification"):
			s.handleGetBucketNotification(w, r, bucket)
		case q["list-type"] != nil && q["list-type"][0] == "2":
			s.handleListObjectsV2(w, r, bucket)
		default:
			s.handleListObjectsV1(w, r, bucket)
		}
	case http.MethodPut:
		switch {
		case has("policy"):
			s.handlePutBucketPolicy(w, r, bucket)
		case has("tagging"):
			s.handlePutBucketTagging(w, r, bucket)
		case has("cors"):
			s.handlePutBucketCORS(w, r, bucket)
		case has("notification"):
			s.handlePutBucketNotification(w, r, bucket)
		case has("versioning"):
			s.handlePutBucketVersioning(w, r, bucket)
		case has("object-lock"):
			s.handlePutBucketObjectLock(w, r, bucket)
		case has("replication"):
			s.handlePutBucketReplication(w, r, bucket)
		case has("lifecycle"):
			s.handlePutBucketLifecycle(w, r, bucket)
		case has("acl"):
			writeSuccessOK(w) // accept-and-ignore
		default:
			s.handleCreateBucket(w, r, bucket)
		}
	case http.MethodDelete:
		switch {
		case has("policy"):
			s.handleDeleteBucketPolicy(w, r, bucket)
		case has("tagging"):
			s.handleDeleteBucketTagging(w, r, bucket)
		case has("cors"):
			s.handleDeleteBucketCORS(w, r, bucket)
		case has("replication"):
			s.handleDeleteBucketReplication(w, r, bucket)
		case has("lifecycle"):
			s.handleDeleteBucketLifecycle(w, r, bucket)
		default:
			s.handleDeleteBucket(w, r, bucket)
		}
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
		if has("tagging") {
			s.handleGetObjectTagging(w, r, bucket, object)
			return
		}
		if has("retention") {
			s.handleGetObjectRetention(w, r, bucket, object)
			return
		}
		if has("legal-hold") {
			s.handleGetObjectLegalHold(w, r, bucket, object)
			return
		}
		if has("acl") {
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
		if has("tagging") {
			s.handlePutObjectTagging(w, r, bucket, object)
			return
		}
		if has("retention") {
			s.handlePutObjectRetention(w, r, bucket, object)
			return
		}
		if has("legal-hold") {
			s.handlePutObjectLegalHold(w, r, bucket, object)
			return
		}
		if has("acl") {
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
		if has("tagging") {
			s.handleDeleteObjectTagging(w, r, bucket, object)
			return
		}
		s.handleDeleteObject(w, r, bucket, object)
	default:
		writeErrorResponse(w, r, ErrMethodNotAllowed, res)
	}
}
