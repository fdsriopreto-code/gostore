package api_test

import (
	"net/http"
	"strings"
	"testing"
)

func TestStaticWebsiteHosting(t *testing.T) {
	srv := newTestServer(t)
	defer srv.Close()
	do(t, srv, http.MethodPut, "/site", nil, nil)

	// Public-read policy so anonymous visitors can fetch pages.
	pol := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":["s3:GetObject"],"Resource":["arn:aws:s3:::site/*"]}]}`
	if r := do(t, srv, http.MethodPut, "/site?policy", []byte(pol), nil); r.StatusCode/100 != 2 {
		t.Fatalf("put policy: %d", r.StatusCode)
	}

	do(t, srv, http.MethodPut, "/site/index.html", []byte("<h1>home</h1>"), map[string]string{"Content-Type": "text/html"})
	do(t, srv, http.MethodPut, "/site/about/index.html", []byte("<h1>about</h1>"), map[string]string{"Content-Type": "text/html"})
	do(t, srv, http.MethodPut, "/site/404.html", []byte("<h1>missing</h1>"), map[string]string{"Content-Type": "text/html"})

	// Enable website hosting.
	wcfg := `<WebsiteConfiguration><IndexDocument><Suffix>index.html</Suffix></IndexDocument><ErrorDocument><Key>404.html</Key></ErrorDocument></WebsiteConfiguration>`
	if r := do(t, srv, http.MethodPut, "/site?website", []byte(wcfg), nil); r.StatusCode/100 != 2 {
		t.Fatalf("put website config: %d %s", r.StatusCode, readBody(t, r))
	}

	cl := srv.Client()

	// "/" -> index.html
	resp, _ := cl.Get(srv.URL + "/site")
	if body := readBody(t, resp); resp.StatusCode != 200 || !strings.Contains(body, "home") {
		t.Fatalf("root: %d %q", resp.StatusCode, body)
	}

	// "about/" -> about/index.html
	resp, _ = cl.Get(srv.URL + "/site/about/")
	if body := readBody(t, resp); resp.StatusCode != 200 || !strings.Contains(body, "about") {
		t.Fatalf("about/: %d %q", resp.StatusCode, body)
	}

	// "about" (no slash) -> about/index.html
	resp, _ = cl.Get(srv.URL + "/site/about")
	if body := readBody(t, resp); resp.StatusCode != 200 || !strings.Contains(body, "about") {
		t.Fatalf("about: %d %q", resp.StatusCode, body)
	}

	// missing page -> error document + 404
	resp, _ = cl.Get(srv.URL + "/site/nope.html")
	if body := readBody(t, resp); resp.StatusCode != 404 || !strings.Contains(body, "missing") {
		t.Fatalf("404 doc: %d %q", resp.StatusCode, body)
	}

	// A real S3 sub-resource call still works on a website bucket.
	r := do(t, srv, http.MethodGet, "/site?list-type=2", nil, nil)
	if r.StatusCode != 200 {
		t.Fatalf("list on website bucket: %d", r.StatusCode)
	}
	if !strings.Contains(readBody(t, r), "index.html") {
		t.Fatal("list should still enumerate objects")
	}

	// Turn it off.
	if r := do(t, srv, http.MethodDelete, "/site?website", nil, nil); r.StatusCode/100 != 2 {
		t.Fatalf("delete website config: %d", r.StatusCode)
	}
	resp, _ = cl.Get(srv.URL + "/site")
	if resp.StatusCode == 200 {
		// with hosting off, "/site" is a ListObjects (needs auth) -> 403 for anon
		t.Fatalf("after disabling website, anon root GET should not 200")
	}
	resp.Body.Close()
}
