package api_test

import (
	"bytes"
	"crypto/hmac"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/lojadopocket/gostore/internal/api"
	"github.com/lojadopocket/gostore/internal/auth"
	"github.com/lojadopocket/gostore/internal/bucketcfg"
	"github.com/lojadopocket/gostore/internal/config"
	"github.com/lojadopocket/gostore/internal/event"
	"github.com/lojadopocket/gostore/internal/iam"
	"github.com/lojadopocket/gostore/internal/kms"
	fsbackend "github.com/lojadopocket/gostore/internal/object/fs"
)

const (
	testAK = "gostoreadmin"
	testSK = "gostoreadmin123"
	region = "us-east-1"
)

func newTestServer(t *testing.T) *httptest.Server {
	t.Helper()
	backend, err := fsbackend.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	cfg := config.Default()
	cfg.Region = region
	dir := t.TempDir()
	if km, kerr := kms.New([]string{dir}); kerr == nil {
		backend.SetKMS(km)
	}
	im, err := iam.New(testAK, testSK, []string{dir})
	if err != nil {
		t.Fatal(err)
	}
	bc, err := bucketcfg.Open([]string{dir})
	if err != nil {
		t.Fatal(err)
	}
	return httptest.NewServer(api.NewServer(cfg, backend, im, bc, event.New(bc)))
}

// signV4 signs req (header auth) as the root test credential.
func signV4(t *testing.T, req *http.Request, payload []byte) {
	t.Helper()
	signV4As(t, req, payload, testAK, testSK)
}

// signV4As signs req with the given access/secret key.
func signV4As(t *testing.T, req *http.Request, payload []byte, ak, sk string) {
	t.Helper()
	now := time.Now().UTC()
	amzDate := now.Format("20060102T150405Z")
	dateStamp := now.Format("20060102")
	payloadHash := sha256Hex(payload)

	req.Header.Set("X-Amz-Date", amzDate)
	req.Header.Set("X-Amz-Content-Sha256", payloadHash)

	host := req.URL.Host
	signed := "host;x-amz-content-sha256;x-amz-date"
	canonHeaders := "host:" + host + "\n" +
		"x-amz-content-sha256:" + payloadHash + "\n" +
		"x-amz-date:" + amzDate + "\n"
	canonReq := req.Method + "\n" +
		auth.EncodePath(req.URL.Path) + "\n" +
		auth.CanonicalQuery(req.URL.Query()) + "\n" +
		canonHeaders + "\n" +
		signed + "\n" +
		payloadHash
	scope := dateStamp + "/" + region + "/s3/aws4_request"
	sts := "AWS4-HMAC-SHA256\n" + amzDate + "\n" + scope + "\n" + sha256Hex([]byte(canonReq))
	key := auth.SigningKey(sk, dateStamp, region, "s3")
	sig := hex.EncodeToString(hmacSHA256(key, sts))
	req.Header.Set("Authorization",
		"AWS4-HMAC-SHA256 Credential="+ak+"/"+scope+
			", SignedHeaders="+signed+", Signature="+sig)
}

func sha256Hex(b []byte) string { s := sha256.Sum256(b); return hex.EncodeToString(s[:]) }
func hmacSHA256(k []byte, d string) []byte {
	h := hmac.New(sha256.New, k)
	h.Write([]byte(d))
	return h.Sum(nil)
}

func do(t *testing.T, srv *httptest.Server, method, path string, body []byte, hdr map[string]string) *http.Response {
	t.Helper()
	var r io.Reader
	if body != nil {
		r = bytes.NewReader(body)
	}
	req, err := http.NewRequest(method, srv.URL+path, r)
	if err != nil {
		t.Fatal(err)
	}
	if body != nil {
		req.ContentLength = int64(len(body))
	}
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	signV4(t, req, body)
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	return resp
}

func readBody(t *testing.T, resp *http.Response) string {
	t.Helper()
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	return string(b)
}

func TestS3EndToEnd(t *testing.T) {
	srv := newTestServer(t)
	defer srv.Close()

	// unsigned request is rejected
	{
		resp, err := srv.Client().Get(srv.URL + "/")
		if err != nil {
			t.Fatal(err)
		}
		if resp.StatusCode != http.StatusForbidden {
			t.Fatalf("unsigned ListBuckets: want 403, got %d", resp.StatusCode)
		}
		resp.Body.Close()
	}

	// create bucket
	if resp := do(t, srv, http.MethodPut, "/mybucket", []byte{}, nil); resp.StatusCode != 200 {
		t.Fatalf("CreateBucket: %d %s", resp.StatusCode, readBody(t, resp))
	}

	// list buckets
	{
		resp := do(t, srv, http.MethodGet, "/", nil, nil)
		body := readBody(t, resp)
		if resp.StatusCode != 200 || !strings.Contains(body, "<Name>mybucket</Name>") {
			t.Fatalf("ListBuckets: %d %s", resp.StatusCode, body)
		}
	}

	// put object
	payload := []byte("the quick brown fox")
	{
		resp := do(t, srv, http.MethodPut, "/mybucket/animals/fox.txt", payload,
			map[string]string{"Content-Type": "text/plain", "x-amz-meta-color": "brown"})
		if resp.StatusCode != 200 {
			t.Fatalf("PutObject: %d %s", resp.StatusCode, readBody(t, resp))
		}
		sum := md5hex(payload)
		if got := strings.Trim(resp.Header.Get("ETag"), `"`); got != sum {
			t.Fatalf("PutObject ETag: got %s want %s", got, sum)
		}
		resp.Body.Close()
	}

	// head object
	{
		resp := do(t, srv, http.MethodHead, "/mybucket/animals/fox.txt", nil, nil)
		if resp.StatusCode != 200 {
			t.Fatalf("HeadObject: %d", resp.StatusCode)
		}
		if resp.Header.Get("x-amz-meta-color") != "brown" {
			t.Fatalf("HeadObject missing user meta: %v", resp.Header)
		}
		if resp.ContentLength != int64(len(payload)) {
			t.Fatalf("HeadObject content-length: %d", resp.ContentLength)
		}
		resp.Body.Close()
	}

	// get object
	{
		resp := do(t, srv, http.MethodGet, "/mybucket/animals/fox.txt", nil, nil)
		body := readBody(t, resp)
		if resp.StatusCode != 200 || body != string(payload) {
			t.Fatalf("GetObject: %d %q", resp.StatusCode, body)
		}
	}

	// range get
	{
		req, _ := http.NewRequest(http.MethodGet, srv.URL+"/mybucket/animals/fox.txt", nil)
		req.Header.Set("Range", "bytes=4-8")
		signV4(t, req, nil)
		resp, err := srv.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		body := readBody(t, resp)
		if resp.StatusCode != http.StatusPartialContent || body != "quick" {
			t.Fatalf("Range GET: %d %q (cr=%s)", resp.StatusCode, body, resp.Header.Get("Content-Range"))
		}
	}

	// list objects v2
	{
		resp := do(t, srv, http.MethodGet, "/mybucket?list-type=2&prefix=animals/", nil, nil)
		body := readBody(t, resp)
		if resp.StatusCode != 200 || !strings.Contains(body, "<Key>animals/fox.txt</Key>") {
			t.Fatalf("ListObjectsV2: %d %s", resp.StatusCode, body)
		}
	}

	// multipart upload
	{
		resp := do(t, srv, http.MethodPost, "/mybucket/big.bin?uploads", nil, nil)
		body := readBody(t, resp)
		if resp.StatusCode != 200 {
			t.Fatalf("NewMultipartUpload: %d %s", resp.StatusCode, body)
		}
		var init struct {
			UploadID string `xml:"UploadId"`
		}
		_ = xml.Unmarshal([]byte(body), &init)
		if init.UploadID == "" {
			t.Fatalf("no upload id in %s", body)
		}

		part1 := bytes.Repeat([]byte("Z"), 5*1024*1024+10)
		part2 := []byte("!end!")
		r1 := do(t, srv, http.MethodPut,
			fmt.Sprintf("/mybucket/big.bin?partNumber=1&uploadId=%s", init.UploadID), part1, nil)
		e1 := strings.Trim(r1.Header.Get("ETag"), `"`)
		r1.Body.Close()
		r2 := do(t, srv, http.MethodPut,
			fmt.Sprintf("/mybucket/big.bin?partNumber=2&uploadId=%s", init.UploadID), part2, nil)
		e2 := strings.Trim(r2.Header.Get("ETag"), `"`)
		r2.Body.Close()
		if r1.StatusCode != 200 || r2.StatusCode != 200 {
			t.Fatalf("UploadPart: %d %d", r1.StatusCode, r2.StatusCode)
		}

		cmu := fmt.Sprintf(`<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>%s</ETag></Part><Part><PartNumber>2</PartNumber><ETag>%s</ETag></Part></CompleteMultipartUpload>`, e1, e2)
		resp = do(t, srv, http.MethodPost,
			fmt.Sprintf("/mybucket/big.bin?uploadId=%s", init.UploadID), []byte(cmu), nil)
		body = readBody(t, resp)
		if resp.StatusCode != 200 || !strings.Contains(body, "CompleteMultipartUploadResult") {
			t.Fatalf("CompleteMultipartUpload: %d %s", resp.StatusCode, body)
		}

		resp = do(t, srv, http.MethodGet, "/mybucket/big.bin", nil, nil)
		got := readBody(t, resp)
		if len(got) != len(part1)+len(part2) {
			t.Fatalf("assembled size %d want %d", len(got), len(part1)+len(part2))
		}
	}

	// delete object + bucket
	{
		if resp := do(t, srv, http.MethodDelete, "/mybucket/animals/fox.txt", nil, nil); resp.StatusCode != 204 {
			t.Fatalf("DeleteObject: %d", resp.StatusCode)
		}
		if resp := do(t, srv, http.MethodDelete, "/mybucket/big.bin", nil, nil); resp.StatusCode != 204 {
			t.Fatalf("DeleteObject: %d", resp.StatusCode)
		}
		if resp := do(t, srv, http.MethodDelete, "/mybucket", nil, nil); resp.StatusCode != 204 {
			t.Fatalf("DeleteBucket: %d %s", resp.StatusCode, readBody(t, resp))
		}
	}
}

func TestBucketPolicyPublicRead(t *testing.T) {
	srv := newTestServer(t)
	defer srv.Close()

	do(t, srv, http.MethodPut, "/pub", []byte{}, nil).Body.Close()
	do(t, srv, http.MethodPut, "/pub/hello.txt", []byte("world"), nil).Body.Close()

	// anonymous GET denied before a policy exists
	if resp, _ := srv.Client().Get(srv.URL + "/pub/hello.txt"); resp.StatusCode != 403 {
		t.Fatalf("anon GET before policy: want 403, got %d", resp.StatusCode)
	}

	pol := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":["s3:GetObject"],"Resource":["arn:aws:s3:::pub/*"]}]}`
	if resp := doAs(t, srv, testAK, testSK, http.MethodPut, "/pub?policy", []byte(pol)); resp.StatusCode/100 != 2 {
		t.Fatalf("put bucket policy: %d %s", resp.StatusCode, readBody(t, resp))
	}

	// now anonymous GET works
	resp, err := srv.Client().Get(srv.URL + "/pub/hello.txt")
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 200 || readBody(t, resp) != "world" {
		t.Fatalf("anon GET after policy: %d", resp.StatusCode)
	}
	// anonymous PUT still denied (policy only granted GetObject)
	req, _ := http.NewRequest(http.MethodPut, srv.URL+"/pub/x.txt", bytes.NewReader([]byte("x")))
	if resp, _ := srv.Client().Do(req); resp.StatusCode != 403 {
		t.Fatalf("anon PUT: want 403, got %d", resp.StatusCode)
	}
}

func TestBucketVersioningHTTP(t *testing.T) {
	srv := newTestServer(t)
	defer srv.Close()
	do(t, srv, http.MethodPut, "/vbk", []byte{}, nil).Body.Close()

	// enable versioning
	vc := `<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Status>Enabled</Status></VersioningConfiguration>`
	if r := do(t, srv, http.MethodPut, "/vbk?versioning", []byte(vc), nil); r.StatusCode/100 != 2 {
		t.Fatalf("put versioning: %d %s", r.StatusCode, readBody(t, r))
	}
	g := do(t, srv, http.MethodGet, "/vbk?versioning", nil, nil)
	if !strings.Contains(readBody(t, g), "<Status>Enabled</Status>") {
		t.Fatal("get versioning did not report Enabled")
	}

	r1 := do(t, srv, http.MethodPut, "/vbk/f.txt", []byte("one"), nil)
	v1 := r1.Header.Get("x-amz-version-id")
	r1.Body.Close()
	r2 := do(t, srv, http.MethodPut, "/vbk/f.txt", []byte("two"), nil)
	v2 := r2.Header.Get("x-amz-version-id")
	r2.Body.Close()
	if v1 == "" || v2 == "" || v1 == v2 {
		t.Fatalf("version ids: %q %q", v1, v2)
	}

	if b := readBody(t, do(t, srv, http.MethodGet, "/vbk/f.txt", nil, nil)); b != "two" {
		t.Fatalf("latest GET = %q", b)
	}
	if b := readBody(t, do(t, srv, http.MethodGet, "/vbk/f.txt?versionId="+v1, nil, nil)); b != "one" {
		t.Fatalf("versioned GET = %q", b)
	}

	lv := readBody(t, do(t, srv, http.MethodGet, "/vbk?versions", nil, nil))
	if strings.Count(lv, "<Version>") != 2 {
		t.Fatalf("ListObjectVersions: %s", lv)
	}

	// delete marker
	dr := do(t, srv, http.MethodDelete, "/vbk/f.txt", nil, nil)
	if dr.Header.Get("x-amz-delete-marker") != "true" || dr.StatusCode != 204 {
		t.Fatalf("delete marker resp: %d %v", dr.StatusCode, dr.Header)
	}
	if g := do(t, srv, http.MethodGet, "/vbk/f.txt", nil, nil); g.StatusCode != 404 {
		t.Fatalf("GET after delete marker: want 404, got %d", g.StatusCode)
	}
	lv = readBody(t, do(t, srv, http.MethodGet, "/vbk?versions", nil, nil))
	if strings.Count(lv, "<DeleteMarker>") != 1 {
		t.Fatalf("expected 1 DeleteMarker: %s", lv)
	}
	// permanent delete of a specific version
	if r := do(t, srv, http.MethodDelete, "/vbk/f.txt?versionId="+v1, nil, nil); r.StatusCode != 204 {
		t.Fatalf("versioned delete: %d", r.StatusCode)
	}
	lv = readBody(t, do(t, srv, http.MethodGet, "/vbk?versions", nil, nil))
	if strings.Count(lv, "<Version>") != 1 {
		t.Fatalf("after permanent delete want 1 Version: %s", lv)
	}
}

func TestSSES3HTTP(t *testing.T) {
	srv := newTestServer(t)
	defer srv.Close()
	do(t, srv, http.MethodPut, "/enc", []byte{}, nil).Body.Close()

	payload := bytes.Repeat([]byte("secret-"), 20000) // ~140 KB, multi-chunk
	r := do(t, srv, http.MethodPut, "/enc/data.bin", payload,
		map[string]string{"x-amz-server-side-encryption": "AES256"})
	if r.StatusCode != 200 || r.Header.Get("x-amz-server-side-encryption") != "AES256" {
		t.Fatalf("PUT sse: %d hdr=%q", r.StatusCode, r.Header.Get("x-amz-server-side-encryption"))
	}
	if strings.Trim(r.Header.Get("ETag"), `"`) != md5hex(payload) {
		t.Fatalf("sse etag should be plaintext md5")
	}
	r.Body.Close()

	g := do(t, srv, http.MethodGet, "/enc/data.bin", nil, nil)
	if g.Header.Get("x-amz-server-side-encryption") != "AES256" {
		t.Fatalf("GET missing sse header: %v", g.Header)
	}
	if got := readBody(t, g); got != string(payload) {
		t.Fatalf("sse round-trip mismatch: %d vs %d", len(got), len(payload))
	}

	// HEAD reports the plaintext size
	h := do(t, srv, http.MethodHead, "/enc/data.bin", nil, nil)
	if h.ContentLength != int64(len(payload)) {
		t.Fatalf("HEAD content-length %d want %d", h.ContentLength, len(payload))
	}
	h.Body.Close()

	// range GET on an encrypted object
	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/enc/data.bin", nil)
	req.Header.Set("Range", "bytes=65530-65550")
	signV4(t, req, nil)
	rr, _ := srv.Client().Do(req)
	if rr.StatusCode != http.StatusPartialContent {
		t.Fatalf("range status %d", rr.StatusCode)
	}
	if got := readBody(t, rr); got != string(payload[65530:65551]) {
		t.Fatalf("encrypted range mismatch: %q", got)
	}
}

func TestObjectTagging(t *testing.T) {
	srv := newTestServer(t)
	defer srv.Close()
	do(t, srv, http.MethodPut, "/tags", []byte{}, nil).Body.Close()
	do(t, srv, http.MethodPut, "/tags/f", []byte("data"), nil).Body.Close()

	body := `<Tagging><TagSet><Tag><Key>env</Key><Value>prod</Value></Tag><Tag><Key>team</Key><Value>core</Value></Tag></TagSet></Tagging>`
	if resp := do(t, srv, http.MethodPut, "/tags/f?tagging", []byte(body), nil); resp.StatusCode/100 != 2 {
		t.Fatalf("put tags: %d %s", resp.StatusCode, readBody(t, resp))
	}
	resp := do(t, srv, http.MethodGet, "/tags/f?tagging", nil, nil)
	got := readBody(t, resp)
	if !strings.Contains(got, "<Key>env</Key>") || !strings.Contains(got, "<Value>prod</Value>") {
		t.Fatalf("get tags: %s", got)
	}
	// tags also surface as x-amz-tagging-count on HEAD
	h := do(t, srv, http.MethodHead, "/tags/f", nil, nil)
	if h.Header.Get("x-amz-tagging-count") != "2" {
		t.Fatalf("tagging-count header: %q", h.Header.Get("x-amz-tagging-count"))
	}
	h.Body.Close()
}

func TestBadSignatureRejected(t *testing.T) {
	srv := newTestServer(t)
	defer srv.Close()
	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/", nil)
	req.Header.Set("X-Amz-Date", time.Now().UTC().Format("20060102T150405Z"))
	req.Header.Set("X-Amz-Content-Sha256", sha256Hex(nil))
	req.Header.Set("Authorization",
		"AWS4-HMAC-SHA256 Credential="+testAK+"/20250101/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature=deadbeef")
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("want 403 for bad signature, got %d", resp.StatusCode)
	}
	resp.Body.Close()
}

func md5hex(b []byte) string {
	h := md5.New()
	h.Write(b)
	return hex.EncodeToString(h.Sum(nil))
}

func doAs(t *testing.T, srv *httptest.Server, ak, sk, method, path string, body []byte) *http.Response {
	t.Helper()
	var r io.Reader
	if body != nil {
		r = bytes.NewReader(body)
	}
	req, _ := http.NewRequest(method, srv.URL+path, r)
	if body != nil {
		req.ContentLength = int64(len(body))
	}
	signV4As(t, req, body, ak, sk)
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatalf("%s %s as %s: %v", method, path, ak, err)
	}
	return resp
}

func TestIAMEndToEnd(t *testing.T) {
	srv := newTestServer(t)
	defer srv.Close()

	// root creates a bucket and an object
	if resp := do(t, srv, http.MethodPut, "/shared", []byte{}, nil); resp.StatusCode != 200 {
		t.Fatalf("create bucket: %d", resp.StatusCode)
	}
	if resp := do(t, srv, http.MethodPut, "/shared/readme.txt", []byte("hi"), nil); resp.StatusCode != 200 {
		t.Fatalf("root put: %d", resp.StatusCode)
	}

	// root creates a read-only user via the admin API
	body := []byte(`{"accessKey":"reader01","secretKey":"readersecret1","policies":["readonly"]}`)
	if resp := doAs(t, srv, testAK, testSK, http.MethodPut, "/gostore/admin/v1/users", body); resp.StatusCode != 200 {
		t.Fatalf("add-user: %d %s", resp.StatusCode, readBody(t, resp))
	}

	// reader can GET
	if resp := doAs(t, srv, "reader01", "readersecret1", http.MethodGet, "/shared/readme.txt", nil); resp.StatusCode != 200 {
		t.Fatalf("reader GET: want 200, got %d", resp.StatusCode)
	}
	// reader can LIST
	if resp := doAs(t, srv, "reader01", "readersecret1", http.MethodGet, "/shared?list-type=2", nil); resp.StatusCode != 200 {
		t.Fatalf("reader LIST: want 200, got %d", resp.StatusCode)
	}
	// reader CANNOT PUT
	if resp := doAs(t, srv, "reader01", "readersecret1", http.MethodPut, "/shared/evil.txt", []byte("x")); resp.StatusCode != 403 {
		t.Fatalf("reader PUT: want 403, got %d %s", resp.StatusCode, readBody(t, resp))
	}
	// reader CANNOT use the admin API
	if resp := doAs(t, srv, "reader01", "readersecret1", http.MethodGet, "/gostore/admin/v1/users", nil); resp.StatusCode != 403 {
		t.Fatalf("reader admin: want 403, got %d", resp.StatusCode)
	}
	// unknown key is rejected at auth
	if resp := doAs(t, srv, "ghost", "ghostsecret1", http.MethodGet, "/shared/readme.txt", nil); resp.StatusCode != 403 {
		t.Fatalf("ghost: want 403, got %d", resp.StatusCode)
	}

	// service account for the reader, then delete the reader -> svc account gone
	scResp := doAs(t, srv, testAK, testSK, http.MethodPost, "/gostore/admin/v1/service-accounts",
		[]byte(`{"parentUser":"reader01"}`))
	if scResp.StatusCode != 200 {
		t.Fatalf("add svc acct: %d %s", scResp.StatusCode, readBody(t, scResp))
	}
	var sc struct{ AccessKey, SecretKey string }
	_ = json.Unmarshal([]byte(readBody(t, scResp)), &sc)
	if sc.AccessKey == "" {
		t.Fatal("no svc acct key returned")
	}
	if resp := doAs(t, srv, sc.AccessKey, sc.SecretKey, http.MethodGet, "/shared/readme.txt", nil); resp.StatusCode != 200 {
		t.Fatalf("svc acct GET: %d", resp.StatusCode)
	}
	if resp := doAs(t, srv, sc.AccessKey, sc.SecretKey, http.MethodPut, "/shared/x", []byte("x")); resp.StatusCode != 403 {
		t.Fatalf("svc acct PUT should inherit readonly deny: %d", resp.StatusCode)
	}
}

const emptySHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

// TestStreamingChunkedUpload exercises the STREAMING-AWS4-HMAC-SHA256-PAYLOAD
// path that real AWS SDKs / the CLI use for PutObject.
func TestStreamingChunkedUpload(t *testing.T) {
	srv := newTestServer(t)
	defer srv.Close()

	if resp := do(t, srv, http.MethodPut, "/cbucket", []byte{}, nil); resp.StatusCode != 200 {
		t.Fatalf("CreateBucket: %d", resp.StatusCode)
	}

	raw := bytes.Repeat([]byte("chunked-data-"), 5000) // ~65 KB, multiple chunks
	chunkSize := 16 * 1024

	now := time.Now().UTC()
	amzDate := now.Format("20060102T150405Z")
	dateStamp := now.Format("20060102")
	scope := dateStamp + "/" + region + "/s3/aws4_request"
	signingKey := auth.SigningKey(testSK, dateStamp, region, "s3")

	// Build encoded body first so we know its length.
	var chunks [][]byte
	for off := 0; off < len(raw); off += chunkSize {
		end := off + chunkSize
		if end > len(raw) {
			end = len(raw)
		}
		chunks = append(chunks, raw[off:end])
	}

	path := "/cbucket/stream.bin"
	host := strings.TrimPrefix(srv.URL, "http://")

	// Seed signature: sign the request with the streaming content-sha256.
	signed := "content-length;host;x-amz-content-sha256;x-amz-date;x-amz-decoded-content-length"
	// compute encoded length
	encodedLen := 0
	for _, c := range chunks {
		encodedLen += len(fmt.Sprintf("%x", len(c))) + len(";chunk-signature=") + 64 + 2 + len(c) + 2
	}
	encodedLen += 1 + len(";chunk-signature=") + 64 + 2 + 2 // final 0-chunk

	canonHeaders := "content-length:" + fmt.Sprint(encodedLen) + "\n" +
		"host:" + host + "\n" +
		"x-amz-content-sha256:" + auth.StreamingPayload + "\n" +
		"x-amz-date:" + amzDate + "\n" +
		"x-amz-decoded-content-length:" + fmt.Sprint(len(raw)) + "\n"
	canonReq := "PUT\n" + auth.EncodePath(path) + "\n\n" + canonHeaders + "\n" + signed + "\n" + auth.StreamingPayload
	sts := "AWS4-HMAC-SHA256\n" + amzDate + "\n" + scope + "\n" + sha256Hex([]byte(canonReq))
	seedSig := hex.EncodeToString(hmacSHA256(signingKey, sts))

	// Encode body with per-chunk signatures chained from the seed.
	var body bytes.Buffer
	prev := seedSig
	chunkSTS := func(data []byte) string {
		return "AWS4-HMAC-SHA256-PAYLOAD\n" + amzDate + "\n" + scope + "\n" + prev + "\n" +
			emptySHA256 + "\n" + sha256Hex(data)
	}
	for _, c := range chunks {
		sig := hex.EncodeToString(hmacSHA256(signingKey, chunkSTS(c)))
		prev = sig
		fmt.Fprintf(&body, "%x;chunk-signature=%s\r\n", len(c), sig)
		body.Write(c)
		body.WriteString("\r\n")
	}
	finalSig := hex.EncodeToString(hmacSHA256(signingKey, chunkSTS([]byte{})))
	fmt.Fprintf(&body, "0;chunk-signature=%s\r\n\r\n", finalSig)

	if body.Len() != encodedLen {
		t.Fatalf("encoded length mismatch: computed %d actual %d", encodedLen, body.Len())
	}

	req, _ := http.NewRequest(http.MethodPut, srv.URL+path, bytes.NewReader(body.Bytes()))
	req.ContentLength = int64(encodedLen)
	req.Header.Set("X-Amz-Date", amzDate)
	req.Header.Set("X-Amz-Content-Sha256", auth.StreamingPayload)
	req.Header.Set("X-Amz-Decoded-Content-Length", fmt.Sprint(len(raw)))
	req.Header.Set("Content-Encoding", "aws-chunked")
	req.Header.Set("Authorization",
		"AWS4-HMAC-SHA256 Credential="+testAK+"/"+scope+", SignedHeaders="+signed+", Signature="+seedSig)

	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("chunked PutObject: %d %s", resp.StatusCode, readBody(t, resp))
	}
	if got := strings.Trim(resp.Header.Get("ETag"), `"`); got != md5hex(raw) {
		t.Fatalf("chunked ETag mismatch: %s vs %s", got, md5hex(raw))
	}
	resp.Body.Close()

	// Read it back and compare.
	resp = do(t, srv, http.MethodGet, path, nil, nil)
	got := readBody(t, resp)
	if got != string(raw) {
		t.Fatalf("chunked round-trip mismatch: len %d vs %d", len(got), len(raw))
	}
}
