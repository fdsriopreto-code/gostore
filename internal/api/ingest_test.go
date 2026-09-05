package api_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"testing"
)

func TestIngestKeyUpload(t *testing.T) {
	srv := newTestServer(t)
	defer srv.Close()
	do(t, srv, http.MethodPut, "/ingbucket", nil, nil)

	// Mint an ingest key scoped to "pg/".
	r := do(t, srv, http.MethodPost, "/gostore/admin/v1/ingest-keys?bucket=ingbucket",
		[]byte(`{"prefix":"pg/","label":"nightly"}`), map[string]string{"Content-Type": "application/json"})
	if r.StatusCode != http.StatusCreated {
		t.Fatalf("create ingest key: %d %s", r.StatusCode, readBody(t, r))
	}
	var mk struct{ Token, ID string }
	_ = json.Unmarshal([]byte(readBody(t, r)), &mk)
	if mk.Token == "" {
		t.Fatal("no token returned")
	}

	// Upload via the ingest endpoint — no SigV4, just the bearer token.
	body := []byte("-- pg_dump output --\n")
	req, _ := http.NewRequest(http.MethodPut, srv.URL+"/gostore/ingest/ingbucket/pg/db-2026.sql", bytes.NewReader(body))
	req.ContentLength = int64(len(body))
	req.Header.Set("Authorization", "Bearer "+mk.Token)
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("ingest upload: %d %s", resp.StatusCode, func() string { b, _ := io.ReadAll(resp.Body); return string(b) }())
	}
	resp.Body.Close()

	// It's readable via a normal signed GET.
	g := do(t, srv, http.MethodGet, "/ingbucket/pg/db-2026.sql", nil, nil)
	if b := readBody(t, g); b != string(body) {
		t.Fatalf("stored body = %q", b)
	}

	// Wrong token -> 403.
	req2, _ := http.NewRequest(http.MethodPut, srv.URL+"/gostore/ingest/ingbucket/pg/x", bytes.NewReader([]byte("x")))
	req2.ContentLength = 1
	req2.Header.Set("Authorization", "Bearer gik_wrong")
	r2, _ := srv.Client().Do(req2)
	if r2.StatusCode != http.StatusForbidden {
		t.Fatalf("bad token: want 403, got %d", r2.StatusCode)
	}
	r2.Body.Close()

	// Out-of-scope prefix -> 403.
	req3, _ := http.NewRequest(http.MethodPut, srv.URL+"/gostore/ingest/ingbucket/other/x", bytes.NewReader([]byte("x")))
	req3.ContentLength = 1
	req3.Header.Set("Authorization", "Bearer "+mk.Token)
	r3, _ := srv.Client().Do(req3)
	if r3.StatusCode != http.StatusForbidden {
		t.Fatalf("out-of-scope prefix: want 403, got %d", r3.StatusCode)
	}
	r3.Body.Close()

	// Folder target auto-dates the filename.
	req4, _ := http.NewRequest(http.MethodPut, srv.URL+"/gostore/ingest/ingbucket/pg/", bytes.NewReader(body))
	req4.ContentLength = int64(len(body))
	req4.Header.Set("Authorization", "Bearer "+mk.Token)
	r4, _ := srv.Client().Do(req4)
	if r4.StatusCode != http.StatusCreated {
		t.Fatalf("folder ingest: %d", r4.StatusCode)
	}
	var out struct{ Key string }
	_ = json.NewDecoder(r4.Body).Decode(&out)
	r4.Body.Close()
	if out.Key == "pg/" || out.Key == "" {
		t.Fatalf("folder target should have been auto-named, got %q", out.Key)
	}

	// Revoke -> next upload fails.
	do(t, srv, http.MethodDelete, "/gostore/admin/v1/ingest-keys?bucket=ingbucket&id="+mk.ID, nil, nil)
	req5, _ := http.NewRequest(http.MethodPut, srv.URL+"/gostore/ingest/ingbucket/pg/y", bytes.NewReader([]byte("y")))
	req5.ContentLength = 1
	req5.Header.Set("Authorization", "Bearer "+mk.Token)
	r5, _ := srv.Client().Do(req5)
	if r5.StatusCode != http.StatusForbidden {
		t.Fatalf("revoked token: want 403, got %d", r5.StatusCode)
	}
	r5.Body.Close()
}
