package api

import (
	"encoding/base64"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/lojadopocket/gostore/internal/auth"
	"github.com/lojadopocket/gostore/internal/event"
	"github.com/lojadopocket/gostore/internal/iam/policy"
	"github.com/lojadopocket/gostore/internal/object"
)

// maxPostFormMem bounds how much of a browser POST-upload form is buffered in
// memory before the file part spills to a temp file.
const maxPostFormMem = 16 << 20

// isPostFormUpload reports whether this is an S3 "POST Object" browser upload
// (RFC 1867 multipart form against the bucket root, not an ?uploads call).
func isPostFormUpload(r *http.Request, req s3Request) bool {
	if r.Method != http.MethodPost || req.Bucket == "" || req.Object != "" {
		return false
	}
	ct := r.Header.Get("Content-Type")
	return strings.HasPrefix(strings.ToLower(strings.TrimSpace(ct)), "multipart/form-data")
}

// handlePostObjectForm implements the S3 POST Object operation: a browser
// uploads a file directly to a bucket using an HTML form whose fields carry a
// base64 policy document and a SigV4 signature over it. No Authorization
// header or presigned query string is involved — the policy signature is the
// credential. Neither SeaweedFS nor Garage implement this fully; MinIO and AWS
// do, and web apps rely on it to upload without a backend proxy.
func (s *Server) handlePostObjectForm(w http.ResponseWriter, r *http.Request, bucket string) {
	if err := r.ParseMultipartForm(maxPostFormMem); err != nil {
		writeErrorResponse(w, r, ErrMalformedPOSTRequest, "/"+bucket)
		return
	}
	form := r.MultipartForm
	if form == nil {
		writeErrorResponse(w, r, ErrMalformedPOSTRequest, "/"+bucket)
		return
	}
	defer func() { _ = form.RemoveAll() }()

	field := func(name string) string {
		for k, v := range form.Value {
			if strings.EqualFold(k, name) && len(v) > 0 {
				return v[0]
			}
		}
		return ""
	}

	// The uploaded file.
	var filePart string
	for k := range form.File {
		if strings.EqualFold(k, "file") {
			filePart = k
			break
		}
	}
	if filePart == "" || len(form.File[filePart]) == 0 {
		writeErrorResponse(w, r, ErrMalformedPOSTRequest, "/"+bucket)
		return
	}
	fh := form.File[filePart][0]

	key := field("key")
	if key == "" {
		writeErrorResponse(w, r, ErrMalformedPOSTRequest, "/"+bucket)
		return
	}
	key = strings.ReplaceAll(key, "${filename}", fh.Filename)

	// --- verify the policy + signature -------------------------------------
	policyB64 := field("policy")
	if policyB64 == "" {
		// A policy is only optional for a fully public (anonymous) bucket; we
		// require it so an upload is always explicitly authorised.
		writeErrorResponse(w, r, ErrAccessDenied, "/"+bucket)
		return
	}
	rawPolicy, err := base64.StdEncoding.DecodeString(policyB64)
	if err != nil {
		writeErrorResponse(w, r, ErrInvalidPolicyDocument, "/"+bucket)
		return
	}
	var pol postPolicy
	if err := json.Unmarshal(rawPolicy, &pol); err != nil {
		writeErrorResponse(w, r, ErrInvalidPolicyDocument, "/"+bucket)
		return
	}
	if exp, err := time.Parse(time.RFC3339, pol.Expiration); err != nil || time.Now().UTC().After(exp) {
		writeErrorResponse(w, r, ErrAccessDenied, "/"+bucket)
		return
	}

	cred := field("x-amz-credential")
	seg := strings.Split(cred, "/")
	if field("x-amz-algorithm") != "AWS4-HMAC-SHA256" || len(seg) != 5 {
		writeErrorResponse(w, r, ErrMalformedPOSTRequest, "/"+bucket)
		return
	}
	accessKey, dateStamp, region, service := seg[0], seg[1], seg[2], seg[3]
	secret, ok := s.iam.LookupSecret(accessKey)
	if !ok {
		writeErrorResponse(w, r, ErrInvalidAccessKeyID, "/"+bucket)
		return
	}
	signingKey := auth.SigningKey(secret, dateStamp, region, service)
	if !auth.SecureCompare(auth.HexHMACSHA256(signingKey, policyB64), field("x-amz-signature")) {
		writeErrorResponse(w, r, ErrSignatureDoesNotMatch, "/"+bucket)
		return
	}

	// --- check every policy condition against the submitted fields --------
	submitted := map[string]string{
		"bucket":       bucket,
		"key":          key,
		"content-type": fh.Header.Get("Content-Type"),
	}
	for k, v := range form.Value {
		if len(v) > 0 {
			submitted[strings.ToLower(k)] = v[0]
		}
	}
	if !pol.satisfiedBy(submitted, fh.Size) {
		writeErrorResponse(w, r, ErrInvalidPolicyDocument, "/"+bucket)
		return
	}
	if submitted["bucket"] != bucket {
		writeErrorResponse(w, r, ErrInvalidPolicyDocument, "/"+bucket)
		return
	}

	// Defence in depth: the signer must also be allowed s3:PutObject by IAM.
	if !s.iam.IsAllowed(accessKey, policy.Args{
		AccountName: accessKey, Action: "s3:PutObject", BucketName: bucket, ObjectName: key,
		SourceIP: clientIP(r),
	}) {
		writeErrorResponse(w, r, ErrAccessDenied, "/"+bucket)
		return
	}

	// --- store the object ------------------------------------------------
	src, err := fh.Open()
	if err != nil {
		writeErrorResponse(w, r, ErrInternalError, "/"+bucket)
		return
	}
	defer src.Close()

	if s.quotaExceeded(bucket, fh.Size) {
		writeErrorResponse(w, r, ErrQuotaExceeded, "/"+bucket)
		return
	}

	opts := s.vopts(bucket, r)
	ud := map[string]string{}
	if ct := submitted["content-type"]; ct != "" {
		ud["content-type"] = ct
	}
	for k, v := range form.Value {
		if lk := strings.ToLower(k); strings.HasPrefix(lk, "x-amz-meta-") && len(v) > 0 {
			ud[lk] = v[0]
		}
	}
	opts.UserDefined = ud
	if tag := field("x-amz-tagging"); tag != "" {
		opts.UserTags = tag
	}

	oi, err := s.obj.PutObject(r.Context(), bucket, key,
		object.NewPutObjReader(src, fh.Size, fh.Size), opts)
	if err != nil {
		writeErrorResponse(w, r, toAPIError(err), "/"+bucket+"/"+key)
		return
	}
	s.notify(r, event.ObjectCreated, bucket, key, oi.Size, oi.ETag)

	// --- respond as the form asked ------------------------------------
	w.Header().Set("ETag", quoteETag(oi.ETag))
	if oi.VersionID != "" {
		w.Header().Set("x-amz-version-id", oi.VersionID)
	}
	loc := fmt.Sprintf("%s://%s/%s/%s", scheme(r), r.Host, bucket, key)
	w.Header().Set("Location", loc)

	if redir := firstNonEmpty(field("success_action_redirect"), field("redirect")); redir != "" {
		sep := "?"
		if strings.Contains(redir, "?") {
			sep = "&"
		}
		u := fmt.Sprintf("%s%sbucket=%s&key=%s&etag=%s", redir, sep,
			url.QueryEscape(bucket), url.QueryEscape(key), url.QueryEscape(quoteETag(oi.ETag)))
		http.Redirect(w, r, u, http.StatusSeeOther)
		return
	}
	switch field("success_action_status") {
	case "200":
		w.WriteHeader(http.StatusOK)
	case "201":
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusCreated)
		body, _ := xml.Marshal(postResponse{Location: loc, Bucket: bucket, Key: key, ETag: quoteETag(oi.ETag)})
		_, _ = w.Write([]byte(xml.Header))
		_, _ = w.Write(body)
	default:
		w.WriteHeader(http.StatusNoContent)
	}
}

type postResponse struct {
	XMLName  xml.Name `xml:"PostResponse"`
	Location string   `xml:"Location"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	ETag     string   `xml:"ETag"`
}

// postPolicy is the decoded form-upload policy document.
type postPolicy struct {
	Expiration string        `json:"expiration"`
	Conditions []interface{} `json:"conditions"`
}

// satisfiedBy checks that fields (lower-cased keys) and the file size meet
// every condition. Supported forms:
//
//	{"field": "value"}                     exact match
//	["eq", "$field", "value"]              exact match
//	["starts-with", "$field", "prefix"]    prefix match ("" allows anything)
//	["content-length-range", min, max]     file size bounds
//
// Unknown well-formed conditions are ignored (AWS accepts a few we don't act
// on, e.g. x-amz-algorithm); a malformed condition fails closed.
func (p postPolicy) satisfiedBy(fields map[string]string, fileSize int64) bool {
	ignored := map[string]bool{
		"x-amz-algorithm": true, "x-amz-credential": true, "x-amz-date": true,
		"x-amz-signature": true, "policy": true,
	}
	for _, c := range p.Conditions {
		switch cond := c.(type) {
		case map[string]interface{}:
			for k, v := range cond {
				lk := strings.ToLower(k)
				if ignored[lk] {
					continue
				}
				want, _ := v.(string)
				if fields[lk] != want {
					return false
				}
			}
		case []interface{}:
			if len(cond) < 3 {
				return false
			}
			op, _ := cond[0].(string)
			switch strings.ToLower(op) {
			case "eq", "starts-with":
				name := strings.ToLower(strings.TrimPrefix(fmt.Sprint(cond[1]), "$"))
				want, _ := cond[2].(string)
				if ignored[name] {
					continue
				}
				got := fields[name]
				if op == "eq" && got != want {
					return false
				}
				if strings.EqualFold(op, "starts-with") && !strings.HasPrefix(got, want) {
					return false
				}
			case "content-length-range":
				lo := toInt64(cond[1])
				hi := toInt64(cond[2])
				if fileSize < lo || (hi > 0 && fileSize > hi) {
					return false
				}
			default:
				// Unknown array condition — ignore rather than fail.
			}
		}
	}
	return true
}

func toInt64(v interface{}) int64 {
	switch n := v.(type) {
	case float64:
		return int64(n)
	case string:
		i, _ := strconv.ParseInt(n, 10, 64)
		return i
	case json.Number:
		i, _ := n.Int64()
		return i
	}
	return 0
}

func scheme(r *http.Request) string {
	if r.TLS != nil || strings.EqualFold(r.Header.Get("X-Forwarded-Proto"), "https") {
		return "https"
	}
	return "http"
}

func firstNonEmpty(a, b string) string {
	if a != "" {
		return a
	}
	return b
}
