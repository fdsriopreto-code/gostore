package api

import (
	"net"
	"net/http"

	"github.com/lojadopocket/gostore/internal/iam/policy"
)

// authorizeS3 checks the authenticated access key is permitted to perform the
// operation the request maps to. Returns ErrNone when allowed.
func (s *Server) authorizeS3(r *http.Request, req s3Request, q map[string][]string, accessKey string) APIErrorCode {
	action := s3Action(r, req, q)
	args := policy.Args{
		Action:     action,
		BucketName: req.Bucket,
		ObjectName: req.Object,
		SourceIP:   clientIP(r),
		ConditionValues: map[string][]string{
			"s3:prefix": first(q["prefix"]),
		},
	}
	if s.iam.IsAllowed(accessKey, args) {
		return ErrNone
	}
	return ErrAccessDenied
}

func first(v []string) []string {
	if len(v) == 0 {
		return []string{""}
	}
	return v[:1]
}

func clientIP(r *http.Request) string {
	if h := r.Header.Get("X-Forwarded-For"); h != "" {
		if i := indexByteStr(h, ','); i >= 0 {
			return trimSpace(h[:i])
		}
		return trimSpace(h)
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

func indexByteStr(s string, b byte) int {
	for i := 0; i < len(s); i++ {
		if s[i] == b {
			return i
		}
	}
	return -1
}

func trimSpace(s string) string {
	for len(s) > 0 && (s[0] == ' ' || s[0] == '\t') {
		s = s[1:]
	}
	for len(s) > 0 && (s[len(s)-1] == ' ' || s[len(s)-1] == '\t') {
		s = s[:len(s)-1]
	}
	return s
}

// s3Action maps an HTTP request to its S3 IAM action name.
func s3Action(r *http.Request, req s3Request, q map[string][]string) string {
	has := func(k string) bool { _, ok := q[k]; return ok }

	switch {
	case req.Bucket == "":
		return "s3:ListAllMyBuckets"

	case req.Object == "":
		switch r.Method {
		case http.MethodGet, http.MethodHead:
			switch {
			case has("location"):
				return "s3:GetBucketLocation"
			case has("uploads"):
				return "s3:ListBucketMultipartUploads"
			case has("versioning"):
				return "s3:GetBucketVersioning"
			case has("policy"):
				return "s3:GetBucketPolicy"
			case has("tagging"):
				return "s3:GetBucketTagging"
			default:
				return "s3:ListBucket"
			}
		case http.MethodPut:
			switch {
			case has("versioning"):
				return "s3:PutBucketVersioning"
			case has("policy"):
				return "s3:PutBucketPolicy"
			case has("tagging"):
				return "s3:PutBucketTagging"
			default:
				return "s3:CreateBucket"
			}
		case http.MethodDelete:
			switch {
			case has("policy"):
				return "s3:DeleteBucketPolicy"
			case has("tagging"):
				return "s3:PutBucketTagging"
			default:
				return "s3:DeleteBucket"
			}
		case http.MethodPost:
			if has("delete") {
				return "s3:DeleteObject"
			}
		}
		return "s3:ListBucket"

	default: // object-level
		switch r.Method {
		case http.MethodGet:
			if has("uploadId") {
				return "s3:ListMultipartUploadParts"
			}
			if has("tagging") {
				return "s3:GetObjectTagging"
			}
			return "s3:GetObject"
		case http.MethodHead:
			return "s3:GetObject"
		case http.MethodPut:
			if has("tagging") {
				return "s3:PutObjectTagging"
			}
			if r.Header.Get("x-amz-copy-source") != "" {
				return "s3:PutObject"
			}
			return "s3:PutObject"
		case http.MethodPost:
			if has("uploads") || has("uploadId") {
				return "s3:PutObject"
			}
		case http.MethodDelete:
			if has("uploadId") {
				return "s3:AbortMultipartUpload"
			}
			return "s3:DeleteObject"
		}
		return "s3:GetObject"
	}
}
