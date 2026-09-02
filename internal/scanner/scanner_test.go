package scanner

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/lojadopocket/gostore/internal/bucketcfg"
	"github.com/lojadopocket/gostore/internal/configstore"
	"github.com/lojadopocket/gostore/internal/object"
	fsb "github.com/lojadopocket/gostore/internal/object/fs"
)

func ctx() context.Context { return context.Background() }

func put(t *testing.T, f *fsb.FS, bucket, key, body string, age time.Duration, opts object.ObjectOptions) {
	t.Helper()
	opts.MTime = time.Now().Add(-age)
	opts.UserDefined = map[string]string{"content-type": "text/plain"}
	pr := object.NewPutObjReader(strings.NewReader(body), int64(len(body)), int64(len(body)))
	if _, err := f.PutObject(ctx(), bucket, key, pr, opts); err != nil {
		t.Fatalf("put %s: %v", key, err)
	}
}

func TestLifecycleExpiration(t *testing.T) {
	f, err := fsb.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := f.MakeBucket(ctx(), "lcbucket", object.MakeBucketOptions{}); err != nil {
		t.Fatal(err)
	}
	put(t, f, "lcbucket", "logs/old.txt", "old", 10*24*time.Hour, object.ObjectOptions{})
	put(t, f, "lcbucket", "logs/fresh.txt", "fresh", 1*time.Hour, object.ObjectOptions{})
	put(t, f, "lcbucket", "keep/x.txt", "keep", 30*24*time.Hour, object.ObjectOptions{})

	store, _ := bucketcfg.Open(configstore.NewDir(t.TempDir()))
	_ = store.Update("lcbucket", func(c *bucketcfg.Config) {
		c.Lifecycle = []bucketcfg.LifecycleRule{{
			ID: "expire-logs", Prefix: "logs/", Status: "Enabled", ExpirationDays: 7,
		}}
	})

	rep := New(f, store, time.Hour).ScanOnce(ctx())
	if rep.ObjectsExpired != 1 {
		t.Fatalf("expected 1 object expired, got %d (report %+v)", rep.ObjectsExpired, rep)
	}
	if _, err := f.GetObjectInfo(ctx(), "lcbucket", "logs/old.txt", object.ObjectOptions{}); err == nil {
		t.Fatal("logs/old.txt should be expired")
	}
	if _, err := f.GetObjectInfo(ctx(), "lcbucket", "logs/fresh.txt", object.ObjectOptions{}); err != nil {
		t.Fatal("logs/fresh.txt should survive")
	}
	if _, err := f.GetObjectInfo(ctx(), "lcbucket", "keep/x.txt", object.ObjectOptions{}); err != nil {
		t.Fatal("keep/x.txt (non-matching prefix) should survive")
	}
}

func TestLifecycleNoncurrentVersionExpiration(t *testing.T) {
	f, _ := fsb.New(t.TempDir())
	_ = f.MakeBucket(ctx(), "lcbucket", object.MakeBucketOptions{})
	// three versions of the same key; the two older ones are noncurrent
	put(t, f, "lcbucket", "d/k", "v1", 40*24*time.Hour, object.ObjectOptions{Versioned: true})
	put(t, f, "lcbucket", "d/k", "v2", 35*24*time.Hour, object.ObjectOptions{Versioned: true})
	put(t, f, "lcbucket", "d/k", "v3-current", 1*time.Hour, object.ObjectOptions{Versioned: true})

	store, _ := bucketcfg.Open(configstore.NewDir(t.TempDir()))
	_ = store.Update("lcbucket", func(c *bucketcfg.Config) {
		c.Lifecycle = []bucketcfg.LifecycleRule{{
			ID: "purge-old-versions", Prefix: "", Status: "Enabled", NoncurrentVersionExpirationDays: 30,
		}}
	})

	rep := New(f, store, time.Hour).ScanOnce(ctx())
	if rep.VersionsExpired != 2 {
		t.Fatalf("expected 2 noncurrent versions expired, got %d (%+v)", rep.VersionsExpired, rep)
	}
	lv, _ := f.ListObjectVersions(ctx(), "lcbucket", "", "", "", "", 100)
	if len(lv.Objects) != 1 {
		t.Fatalf("expected 1 version left, got %d", len(lv.Objects))
	}
}

func TestLifecycleDisabledRuleNoop(t *testing.T) {
	f, _ := fsb.New(t.TempDir())
	_ = f.MakeBucket(ctx(), "lcbucket", object.MakeBucketOptions{})
	put(t, f, "lcbucket", "a.txt", "x", 100*24*time.Hour, object.ObjectOptions{})
	store, _ := bucketcfg.Open(configstore.NewDir(t.TempDir()))
	_ = store.Update("lcbucket", func(c *bucketcfg.Config) {
		c.Lifecycle = []bucketcfg.LifecycleRule{{ID: "r", Status: "Disabled", ExpirationDays: 1}}
	})
	if rep := New(f, store, time.Hour).ScanOnce(ctx()); rep.ObjectsExpired != 0 {
		t.Fatalf("disabled rule should do nothing, expired %d", rep.ObjectsExpired)
	}
}

func TestDataUsageAccounting(t *testing.T) {
	f, _ := fsb.New(t.TempDir())
	_ = f.MakeBucket(ctx(), "usage", object.MakeBucketOptions{})
	put(t, f, "usage", "a.txt", "hello", 0, object.ObjectOptions{})       // 5
	put(t, f, "usage", "d/b.txt", "worldwide", 0, object.ObjectOptions{}) // 9
	put(t, f, "usage", "d/e/c.txt", "xyz", 0, object.ObjectOptions{})     // 3

	store, _ := bucketcfg.Open(configstore.NewDir(t.TempDir()))
	sc := New(f, store, time.Hour)
	if sc.Usage() != nil {
		t.Fatal("usage should be nil before first pass")
	}
	sc.ScanOnce(ctx())

	u := sc.Usage()
	if u == nil {
		t.Fatal("usage nil after scan")
	}
	bu := u.Buckets["usage"]
	if bu.Objects != 3 || bu.Bytes != 17 {
		t.Fatalf("bucket usage = %+v, want {3, 17}", bu)
	}
	if u.TotalObjects != 3 || u.TotalBytes != 17 {
		t.Fatalf("totals = %d/%d, want 3/17", u.TotalObjects, u.TotalBytes)
	}
}
