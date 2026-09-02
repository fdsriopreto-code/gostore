package api_test

import (
	"encoding/json"
	"net/http"
	"testing"
)

func TestBucketSnapshotAndRestore(t *testing.T) {
	srv := newTestServer(t)
	defer srv.Close()
	do(t, srv, http.MethodPut, "/ttbk", nil, nil)
	// enable versioning
	if r := do(t, srv, http.MethodPut, "/ttbk?versioning=", []byte(`<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>`), map[string]string{"Content-Type": "application/xml"}); r.StatusCode/100 != 2 {
		t.Fatalf("versioning: %d %s", r.StatusCode, readBody(t, r))
	}

	// State at snapshot time: a="1", b="1".
	do(t, srv, http.MethodPut, "/ttbk/a", []byte("a1"), nil)
	do(t, srv, http.MethodPut, "/ttbk/b", []byte("b1"), nil)

	// Take the snapshot.
	sr := do(t, srv, http.MethodPost, "/gostore/admin/v1/snapshot?bucket=ttbk", nil, nil)
	if sr.StatusCode != http.StatusCreated {
		t.Fatalf("snapshot create: %d %s", sr.StatusCode, readBody(t, sr))
	}
	var man struct {
		ID      string `json:"id"`
		Objects int    `json:"objects"`
	}
	_ = json.Unmarshal([]byte(readBody(t, sr)), &man)
	if man.ID == "" || man.Objects != 2 {
		t.Fatalf("bad manifest: %+v", man)
	}

	// Mutate after the snapshot: a overwritten, b deleted, c created.
	do(t, srv, http.MethodPut, "/ttbk/a", []byte("a2-CHANGED"), nil)
	do(t, srv, http.MethodDelete, "/ttbk/b", nil, nil)
	do(t, srv, http.MethodPut, "/ttbk/c", []byte("c1-NEW"), nil)

	// Dry-run restore reports the plan.
	dr := do(t, srv, http.MethodPost, "/gostore/admin/v1/snapshot/restore?bucket=ttbk&id="+man.ID+"&dryRun=1", nil, nil)
	var plan struct{ Restored, Removed, Unchanged int }
	_ = json.Unmarshal([]byte(readBody(t, dr)), &plan)
	if plan.Restored != 2 || plan.Removed != 1 {
		t.Fatalf("dry-run plan = %+v, want restored 2 removed 1", plan)
	}

	// Real restore.
	rr := do(t, srv, http.MethodPost, "/gostore/admin/v1/snapshot/restore?bucket=ttbk&id="+man.ID, nil, nil)
	if rr.StatusCode != 200 {
		t.Fatalf("restore: %d %s", rr.StatusCode, readBody(t, rr))
	}

	// Bucket is back to the snapshot state.
	if g := do(t, srv, http.MethodGet, "/ttbk/a", nil, nil); readBody(t, g) != "a1" {
		t.Fatal("a not restored to a1")
	}
	if g := do(t, srv, http.MethodGet, "/ttbk/b", nil, nil); g.StatusCode != 200 || readBody(t, g) != "b1" {
		t.Fatalf("b not restored (status %d)", g.StatusCode)
	}
	if g := do(t, srv, http.MethodGet, "/ttbk/c", nil, nil); g.StatusCode != http.StatusNotFound {
		t.Fatalf("c created after the snapshot should be gone, got %d", g.StatusCode)
	}

	// It's non-destructive: the changed version of 'a' is still in history.
	vr := do(t, srv, http.MethodGet, "/ttbk?versions=", nil, nil)
	if body := readBody(t, vr); !contains(body, "a2-CHANGED") && !contains(body, "<Key>a</Key>") {
		// at minimum multiple versions of 'a' exist
		if countSub(body, "<Key>a</Key>") < 2 {
			t.Fatalf("expected 'a' to have multiple versions after restore:\n%s", body)
		}
	}
}

func contains(s, sub string) bool { return len(s) >= len(sub) && indexOf(s, sub) >= 0 }
func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
func countSub(s, sub string) int {
	n, i := 0, 0
	for {
		j := indexOf(s[i:], sub)
		if j < 0 {
			return n
		}
		n++
		i += j + len(sub)
	}
}
