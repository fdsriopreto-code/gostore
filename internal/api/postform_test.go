package api_test

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"testing"
	"time"

	"github.com/lojadopocket/gostore/internal/auth"
)

// buildPostForm returns a signed multipart/form-data body for an S3 POST
// Object upload plus its Content-Type header.
func buildPostForm(t *testing.T, bucket, key string, extraConds []string, fileBody []byte, extraFields map[string]string) (*bytes.Buffer, string) {
	t.Helper()
	now := time.Now().UTC()
	dateStamp := now.Format("20060102")
	cred := fmt.Sprintf("%s/%s/%s/s3/aws4_request", testAK, dateStamp, region)

	conds := `{"bucket":"` + bucket + `"},["starts-with","$key",""],["content-length-range",0,1048576]`
	for _, c := range extraConds {
		conds += "," + c
	}
	policyJSON := fmt.Sprintf(`{"expiration":"%s","conditions":[%s]}`,
		now.Add(1*time.Hour).Format(time.RFC3339), conds)
	policyB64 := base64.StdEncoding.EncodeToString([]byte(policyJSON))
	signingKey := auth.SigningKey(testSK, dateStamp, region, "s3")
	sig := auth.HexHMACSHA256(signingKey, policyB64)
	if ov := extraFields["__sig"]; ov != "" {
		sig = ov
		delete(extraFields, "__sig")
	}

	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	_ = mw.WriteField("key", key)
	_ = mw.WriteField("x-amz-algorithm", "AWS4-HMAC-SHA256")
	_ = mw.WriteField("x-amz-credential", cred)
	_ = mw.WriteField("x-amz-date", now.Format("20060102T150405Z"))
	_ = mw.WriteField("policy", policyB64)
	_ = mw.WriteField("x-amz-signature", sig)
	for k, v := range extraFields {
		_ = mw.WriteField(k, v)
	}
	fw, _ := mw.CreateFormFile("file", "hello.txt")
	_, _ = fw.Write(fileBody)
	_ = mw.Close()
	return &buf, mw.FormDataContentType()
}

func TestPostObjectFormUpload(t *testing.T) {
	srv := newTestServer(t)
	defer srv.Close()

	if resp := do(t, srv, http.MethodPut, "/formbucket", nil, nil); resp.StatusCode != 200 {
		t.Fatalf("create bucket: %d %s", resp.StatusCode, readBody(t, resp))
	}

	body, ct := buildPostForm(t, "formbucket", "uploads/hello.txt", nil, []byte("hi from a browser form"), nil)
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/formbucket", body)
	req.Header.Set("Content-Type", ct)
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("POST form upload: want 204, got %d %s", resp.StatusCode, readBody(t, resp))
	}
	resp.Body.Close()

	// The object is now readable via a normal signed GET.
	got := do(t, srv, http.MethodGet, "/formbucket/uploads/hello.txt", nil, nil)
	if got.StatusCode != 200 {
		t.Fatalf("GET uploaded object: %d", got.StatusCode)
	}
	b, _ := io.ReadAll(got.Body)
	got.Body.Close()
	if string(b) != "hi from a browser form" {
		t.Fatalf("object body = %q", b)
	}
}

func TestPostObjectFormRejectsBadSignature(t *testing.T) {
	srv := newTestServer(t)
	defer srv.Close()
	do(t, srv, http.MethodPut, "/formbucket", nil, nil)

	body, ct := buildPostForm(t, "formbucket", "k.txt", nil, []byte("x"),
		map[string]string{"__sig": "deadbeefdeadbeefdeadbeefdeadbeef"})
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/formbucket", body)
	req.Header.Set("Content-Type", ct)
	resp, _ := srv.Client().Do(req)
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("bad signature: want 403, got %d %s", resp.StatusCode, readBody(t, resp))
	}
	resp.Body.Close()
}

func TestPostObjectFormEnforcesConditions(t *testing.T) {
	srv := newTestServer(t)
	defer srv.Close()
	do(t, srv, http.MethodPut, "/formbucket", nil, nil)

	// Policy demands key starts with "locked/" but the form sends "other/".
	body, ct := buildPostForm(t, "formbucket", "other/x.txt",
		[]string{`["starts-with","$key","locked/"]`}, []byte("x"), nil)
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/formbucket", body)
	req.Header.Set("Content-Type", ct)
	resp, _ := srv.Client().Do(req)
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("condition violation: want 403, got %d %s", resp.StatusCode, readBody(t, resp))
	}
	resp.Body.Close()
}

func TestConditionalPutIfMatchIfNoneMatch(t *testing.T) {
	srv := newTestServer(t)
	defer srv.Close()
	do(t, srv, http.MethodPut, "/condbucket", nil, nil)

	// Initial object.
	r1 := do(t, srv, http.MethodPut, "/condbucket/o", []byte("v1"), nil)
	if r1.StatusCode != 200 {
		t.Fatalf("put v1: %d", r1.StatusCode)
	}
	etag := r1.Header.Get("ETag")
	r1.Body.Close()

	// If-Match with the current ETag → allowed.
	if r := do(t, srv, http.MethodPut, "/condbucket/o", []byte("v2"), map[string]string{"If-Match": etag}); r.StatusCode != 200 {
		t.Fatalf("If-Match current etag: want 200, got %d", r.StatusCode)
	}

	// If-Match with a stale ETag → 412.
	if r := do(t, srv, http.MethodPut, "/condbucket/o", []byte("v3"), map[string]string{"If-Match": `"` + hex.EncodeToString([]byte("0000000000000000")) + `"`}); r.StatusCode != http.StatusPreconditionFailed {
		t.Fatalf("If-Match stale etag: want 412, got %d %s", r.StatusCode, readBody(t, r))
	}

	// If-None-Match with the current ETag → 412 (would overwrite a match).
	cur := do(t, srv, http.MethodGet, "/condbucket/o", nil, nil)
	nowTag := cur.Header.Get("ETag")
	cur.Body.Close()
	if r := do(t, srv, http.MethodPut, "/condbucket/o", []byte("v4"), map[string]string{"If-None-Match": nowTag}); r.StatusCode != http.StatusPreconditionFailed {
		t.Fatalf("If-None-Match current etag: want 412, got %d", r.StatusCode)
	}

	// If-Match against a missing object → 412.
	if r := do(t, srv, http.MethodPut, "/condbucket/missing", []byte("x"), map[string]string{"If-Match": `"whatever"`}); r.StatusCode != http.StatusPreconditionFailed {
		t.Fatalf("If-Match on missing object: want 412, got %d", r.StatusCode)
	}
}
