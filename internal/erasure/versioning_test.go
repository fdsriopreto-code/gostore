package erasure

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/lojadopocket/gostore/internal/object"
)

func removeAll(t *testing.T, parts ...string) {
	t.Helper()
	if err := os.RemoveAll(filepath.Join(parts...)); err != nil {
		t.Fatal(err)
	}
}

func putVer(t *testing.T, p *Pool, bucket, key, body string, opts object.ObjectOptions) object.ObjectInfo {
	t.Helper()
	opts.UserDefined = map[string]string{"content-type": "text/plain"}
	oi, err := p.PutObject(ctx(), bucket, key,
		object.NewPutObjReader(bytes.NewReader([]byte(body)), int64(len(body)), int64(len(body))), opts)
	if err != nil {
		t.Fatalf("put %s: %v", key, err)
	}
	return oi
}

func getVerBody(t *testing.T, p *Pool, bucket, key string, opts object.ObjectOptions) (string, error) {
	t.Helper()
	gr, err := p.GetObjectNInfo(ctx(), bucket, key, nil, nil, opts)
	if err != nil {
		return "", err
	}
	b, _ := io.ReadAll(gr)
	gr.Close()
	return string(b), nil
}

func TestErasureVersioningLifecycle(t *testing.T) {
	p, _ := newTestPool(t, 4)
	if err := p.MakeBucket(ctx(), "vbuck", object.MakeBucketOptions{}); err != nil {
		t.Fatal(err)
	}
	en := object.ObjectOptions{Versioned: true}

	v1 := putVer(t, p, "vbuck", "doc", "one", en)
	v2 := putVer(t, p, "vbuck", "doc", "two", en)
	if v1.VersionID == "" || v1.VersionID == v2.VersionID {
		t.Fatalf("version ids %q %q", v1.VersionID, v2.VersionID)
	}

	if b, _ := getVerBody(t, p, "vbuck", "doc", en); b != "two" {
		t.Fatalf("latest = %q", b)
	}
	if b, _ := getVerBody(t, p, "vbuck", "doc", object.ObjectOptions{VersionID: v1.VersionID}); b != "one" {
		t.Fatalf("v1 = %q", b)
	}

	lv, err := p.ListObjectVersions(ctx(), "vbuck", "", "", "", "", 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(lv.Objects) != 2 {
		t.Fatalf("want 2 versions, got %d", len(lv.Objects))
	}

	// delete marker
	dm, err := p.DeleteObject(ctx(), "vbuck", "doc", en)
	if err != nil || !dm.DeleteMarker {
		t.Fatalf("delete marker: %+v %v", dm, err)
	}
	if _, err := getVerBody(t, p, "vbuck", "doc", en); err == nil {
		t.Fatal("GET latest after delete marker should 404")
	}
	if b, _ := getVerBody(t, p, "vbuck", "doc", object.ObjectOptions{VersionID: v2.VersionID}); b != "two" {
		t.Fatalf("v2 still readable = %q", b)
	}
	lv, _ = p.ListObjectVersions(ctx(), "vbuck", "", "", "", "", 100)
	if len(lv.Objects) != 3 {
		t.Fatalf("want 3 entries (2 versions + marker), got %d", len(lv.Objects))
	}

	// delete the marker -> latest becomes v2 again
	if _, err := p.DeleteObject(ctx(), "vbuck", "doc", object.ObjectOptions{VersionID: dm.VersionID}); err != nil {
		t.Fatal(err)
	}
	if b, _ := getVerBody(t, p, "vbuck", "doc", en); b != "two" {
		t.Fatalf("after removing marker, latest = %q", b)
	}

	// permanent delete v1
	if _, err := p.DeleteObject(ctx(), "vbuck", "doc", object.ObjectOptions{VersionID: v1.VersionID}); err != nil {
		t.Fatal(err)
	}
	if _, err := getVerBody(t, p, "vbuck", "doc", object.ObjectOptions{VersionID: v1.VersionID}); err == nil {
		t.Fatal("v1 should be gone")
	}
	lv, _ = p.ListObjectVersions(ctx(), "vbuck", "", "", "", "", 100)
	if len(lv.Objects) != 1 {
		t.Fatalf("want 1 entry left, got %d", len(lv.Objects))
	}

	// ListObjectsV2 shows only the current head
	li, _ := p.ListObjectsV2(ctx(), "vbuck", "", "", "", 100, false, "")
	if len(li.Objects) != 1 || li.Objects[0].Name != "doc" {
		t.Fatalf("v2 list on versioned bucket: %+v", li.Objects)
	}
}

func TestErasureVersioningSurvivesDiskLoss(t *testing.T) {
	p, roots := newTestPool(t, 6) // 3+3
	_ = p.MakeBucket(ctx(), "vbuck", object.MakeBucketOptions{})
	en := object.ObjectOptions{Versioned: true}
	big := bytes.Repeat([]byte("A"), blockSizeV2*3+123)
	if _, err := p.PutObject(ctx(), "vbuck", "k", object.NewPutObjReader(bytes.NewReader(big), int64(len(big)), int64(len(big))), en); err != nil {
		t.Fatal(err)
	}
	v2 := putVer(t, p, "vbuck", "k", "small current", en)
	_ = v2

	// wipe the live object dir + one archived version dir from 3 of 6 disks
	for i := 0; i < 3; i++ {
		removeAll(t, roots[i], "vbuck", "k")
		removeAll(t, roots[i], "vbuck", ".gostore.sys")
	}
	// current still reads
	if b, _ := getVerBody(t, p, "vbuck", "k", en); b != "small current" {
		t.Fatalf("current after 3-disk loss = %q", b)
	}
}

func TestErasureObjectLock(t *testing.T) {
	p, _ := newTestPool(t, 4)
	_ = p.MakeBucket(ctx(), "vbuck", object.MakeBucketOptions{})
	future := time.Now().Add(24 * time.Hour)
	oi := putVer(t, p, "vbuck", "k", "locked", object.ObjectOptions{
		Versioned: true, LockMode: "GOVERNANCE", LockRetainUntil: future,
	})
	if _, err := p.DeleteObject(ctx(), "vbuck", "k", object.ObjectOptions{VersionID: oi.VersionID}); !errors.Is(err, object.ErrObjectLocked) {
		t.Fatalf("expected ErrObjectLocked, got %v", err)
	}
	if _, err := p.DeleteObject(ctx(), "vbuck", "k", object.ObjectOptions{VersionID: oi.VersionID, BypassGovernance: true}); err != nil {
		t.Fatalf("bypass delete: %v", err)
	}

	oi2 := putVer(t, p, "vbuck", "k2", "held", object.ObjectOptions{Versioned: true})
	if err := p.PutObjectLegalHold(ctx(), "vbuck", "k2", oi2.VersionID, "ON"); err != nil {
		t.Fatal(err)
	}
	if st, _ := p.GetObjectLegalHold(ctx(), "vbuck", "k2", oi2.VersionID); st != "ON" {
		t.Fatalf("legal hold %q", st)
	}
	if _, err := p.DeleteObject(ctx(), "vbuck", "k2", object.ObjectOptions{VersionID: oi2.VersionID, BypassGovernance: true}); !errors.Is(err, object.ErrObjectLocked) {
		t.Fatalf("legal hold must block, got %v", err)
	}
}
