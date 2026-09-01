package fs

import (
	"io"
	"strings"
	"testing"

	"github.com/lojadopocket/gostore/internal/object"
)

func putV(t *testing.T, f *FS, bucket, key, body string, opts object.ObjectOptions) object.ObjectInfo {
	t.Helper()
	pr := object.NewPutObjReader(strings.NewReader(body), int64(len(body)), int64(len(body)))
	opts.UserDefined = map[string]string{"content-type": "text/plain"}
	oi, err := f.PutObject(ctx(), bucket, key, pr, opts)
	if err != nil {
		t.Fatalf("PutObject: %v", err)
	}
	return oi
}

func getBody(t *testing.T, f *FS, bucket, key string, opts object.ObjectOptions) (string, error) {
	t.Helper()
	gr, err := f.GetObjectNInfo(ctx(), bucket, key, nil, nil, opts)
	if err != nil {
		return "", err
	}
	b, _ := io.ReadAll(gr)
	gr.Close()
	return string(b), nil
}

func TestVersioningLifecycle(t *testing.T) {
	f := newTestFS(t)
	if err := f.MakeBucket(ctx(), "vbuck", object.MakeBucketOptions{}); err != nil {
		t.Fatal(err)
	}
	en := object.ObjectOptions{Versioned: true}

	v1 := putV(t, f, "vbuck", "doc.txt", "version one", en)
	v2 := putV(t, f, "vbuck", "doc.txt", "version two", en)
	if v1.VersionID == "" || v1.VersionID == v2.VersionID {
		t.Fatalf("bad version ids: %q %q", v1.VersionID, v2.VersionID)
	}

	// latest
	if got, _ := getBody(t, f, "vbuck", "doc.txt", en); got != "version two" {
		t.Fatalf("latest = %q", got)
	}
	// specific old version
	if got, _ := getBody(t, f, "vbuck", "doc.txt", object.ObjectOptions{VersionID: v1.VersionID}); got != "version one" {
		t.Fatalf("v1 = %q", got)
	}

	lv, err := f.ListObjectVersions(ctx(), "vbuck", "", "", "", "", 1000)
	if err != nil {
		t.Fatal(err)
	}
	if len(lv.Objects) != 2 {
		t.Fatalf("want 2 versions, got %d", len(lv.Objects))
	}

	// delete marker
	dm, err := f.DeleteObject(ctx(), "vbuck", "doc.txt", en)
	if err != nil {
		t.Fatal(err)
	}
	if !dm.DeleteMarker || dm.VersionID == "" {
		t.Fatalf("expected delete marker, got %+v", dm)
	}
	if _, err := getBody(t, f, "vbuck", "doc.txt", en); err == nil {
		t.Fatal("latest GET after delete marker should 404")
	}
	// old version still reachable
	if got, _ := getBody(t, f, "vbuck", "doc.txt", object.ObjectOptions{VersionID: v2.VersionID}); got != "version two" {
		t.Fatalf("v2 after marker = %q", got)
	}
	lv, _ = f.ListObjectVersions(ctx(), "vbuck", "", "", "", "", 1000)
	if len(lv.Objects) != 3 {
		t.Fatalf("want 3 entries (2 versions + marker), got %d", len(lv.Objects))
	}

	// permanent delete of v1
	if _, err := f.DeleteObject(ctx(), "vbuck", "doc.txt", object.ObjectOptions{VersionID: v1.VersionID}); err != nil {
		t.Fatal(err)
	}
	if _, err := getBody(t, f, "vbuck", "doc.txt", object.ObjectOptions{VersionID: v1.VersionID}); err == nil {
		t.Fatal("v1 should be gone after permanent delete")
	}
	lv, _ = f.ListObjectVersions(ctx(), "vbuck", "", "", "", "", 1000)
	if len(lv.Objects) != 2 {
		t.Fatalf("want 2 entries after permanent delete, got %d", len(lv.Objects))
	}
}

func TestVersioningMigratesExistingObject(t *testing.T) {
	f := newTestFS(t)
	_ = f.MakeBucket(ctx(), "vbuck", object.MakeBucketOptions{})
	// object written BEFORE versioning is enabled
	putV(t, f, "vbuck", "old.txt", "pre-existing", object.ObjectOptions{})

	// now versioning is on and a new version is written
	putV(t, f, "vbuck", "old.txt", "updated", object.ObjectOptions{Versioned: true})

	lv, _ := f.ListObjectVersions(ctx(), "vbuck", "", "", "", "", 1000)
	if len(lv.Objects) != 2 {
		t.Fatalf("want 2 versions (null + new), got %d", len(lv.Objects))
	}
	if got, _ := getBody(t, f, "vbuck", "old.txt", object.ObjectOptions{VersionID: "null"}); got != "pre-existing" {
		t.Fatalf("null version = %q", got)
	}
	if got, _ := getBody(t, f, "vbuck", "old.txt", object.ObjectOptions{Versioned: true}); got != "updated" {
		t.Fatalf("latest = %q", got)
	}
	// ListObjectsV2 on the versioned bucket shows only the current head
	li, _ := f.ListObjectsV2(ctx(), "vbuck", "", "", "", 1000, false, "")
	if len(li.Objects) != 1 || li.Objects[0].ETag == "" {
		t.Fatalf("v2 list on versioned bucket: %+v", li.Objects)
	}
}

func TestVersioningSuspended(t *testing.T) {
	f := newTestFS(t)
	_ = f.MakeBucket(ctx(), "vbuck", object.MakeBucketOptions{})
	putV(t, f, "vbuck", "s.txt", "a", object.ObjectOptions{Versioned: true})
	putV(t, f, "vbuck", "s.txt", "b", object.ObjectOptions{Versioned: true})
	// suspended write overwrites the null version rather than adding one
	putV(t, f, "vbuck", "s.txt", "c-null", object.ObjectOptions{VersionSuspended: true})
	putV(t, f, "vbuck", "s.txt", "d-null", object.ObjectOptions{VersionSuspended: true})

	lv, _ := f.ListObjectVersions(ctx(), "vbuck", "", "", "", "", 1000)
	// 2 real versions + 1 null (the last suspended write)
	nulls := 0
	for _, o := range lv.Objects {
		if o.VersionID == "null" {
			nulls++
		}
	}
	if nulls != 1 {
		t.Fatalf("want exactly 1 null version, got %d (total %d)", nulls, len(lv.Objects))
	}
	if got, _ := getBody(t, f, "vbuck", "s.txt", object.ObjectOptions{VersionSuspended: true}); got != "d-null" {
		t.Fatalf("latest after suspended writes = %q", got)
	}
}
