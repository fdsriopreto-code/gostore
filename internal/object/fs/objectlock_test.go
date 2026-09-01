package fs

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/lojadopocket/gostore/internal/object"
)

func TestObjectLockRetention(t *testing.T) {
	f := newTestFS(t)
	_ = f.MakeBucket(ctx(), "lbuck", object.MakeBucketOptions{})
	future := time.Now().Add(24 * time.Hour)

	oi, err := f.PutObject(ctx(), "lbuck", "k",
		object.NewPutObjReader(strings.NewReader("data"), 4, 4),
		object.ObjectOptions{Versioned: true, LockMode: "GOVERNANCE", LockRetainUntil: future,
			UserDefined: map[string]string{"content-type": "text/plain"}})
	if err != nil {
		t.Fatal(err)
	}
	vid := oi.VersionID

	// delete of the locked version is blocked
	if _, err := f.DeleteObject(ctx(), "lbuck", "k", object.ObjectOptions{VersionID: vid}); !errors.Is(err, object.ErrObjectLocked) {
		t.Fatalf("expected ErrObjectLocked, got %v", err)
	}
	// bypass GOVERNANCE succeeds
	if _, err := f.DeleteObject(ctx(), "lbuck", "k", object.ObjectOptions{VersionID: vid, BypassGovernance: true}); err != nil {
		t.Fatalf("bypass delete: %v", err)
	}

	// COMPLIANCE cannot be bypassed
	oi2, _ := f.PutObject(ctx(), "lbuck", "k2",
		object.NewPutObjReader(strings.NewReader("d2"), 2, 2),
		object.ObjectOptions{Versioned: true, LockMode: "COMPLIANCE", LockRetainUntil: future})
	if _, err := f.DeleteObject(ctx(), "lbuck", "k2", object.ObjectOptions{VersionID: oi2.VersionID, BypassGovernance: true}); !errors.Is(err, object.ErrObjectLocked) {
		t.Fatalf("COMPLIANCE must not be bypassable, got %v", err)
	}
	// and cannot be shortened
	if err := f.PutObjectRetention(ctx(), "lbuck", "k2", oi2.VersionID, "COMPLIANCE", time.Now().Add(time.Hour), true); !errors.Is(err, object.ErrObjectLocked) {
		t.Fatalf("shortening COMPLIANCE must fail, got %v", err)
	}
	// extending is fine
	if err := f.PutObjectRetention(ctx(), "lbuck", "k2", oi2.VersionID, "COMPLIANCE", time.Now().Add(72*time.Hour), false); err != nil {
		t.Fatalf("extending COMPLIANCE: %v", err)
	}
}

func TestObjectLockLegalHold(t *testing.T) {
	f := newTestFS(t)
	_ = f.MakeBucket(ctx(), "lbuck", object.MakeBucketOptions{})
	oi, _ := f.PutObject(ctx(), "lbuck", "k",
		object.NewPutObjReader(strings.NewReader("x"), 1, 1),
		object.ObjectOptions{Versioned: true})

	if err := f.PutObjectLegalHold(ctx(), "lbuck", "k", oi.VersionID, "ON"); err != nil {
		t.Fatal(err)
	}
	if st, _ := f.GetObjectLegalHold(ctx(), "lbuck", "k", oi.VersionID); st != "ON" {
		t.Fatalf("legal hold status %q", st)
	}
	if _, err := f.DeleteObject(ctx(), "lbuck", "k", object.ObjectOptions{VersionID: oi.VersionID, BypassGovernance: true}); !errors.Is(err, object.ErrObjectLocked) {
		t.Fatalf("legal hold must block delete, got %v", err)
	}
	// lift it, then delete works
	_ = f.PutObjectLegalHold(ctx(), "lbuck", "k", oi.VersionID, "OFF")
	if _, err := f.DeleteObject(ctx(), "lbuck", "k", object.ObjectOptions{VersionID: oi.VersionID}); err != nil {
		t.Fatalf("delete after legal hold lifted: %v", err)
	}
}
