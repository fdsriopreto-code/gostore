package erasure

import (
	"fmt"
	"testing"

	"github.com/lojadopocket/gostore/internal/object"
)

func TestListCacheServesPagesWithoutRewalk(t *testing.T) {
	p, _ := newTestPool(t, 4)
	_ = p.MakeBucket(ctx(), "buck", object.MakeBucketOptions{})
	for i := 0; i < 30; i++ {
		put(t, p, "buck", fmt.Sprintf("k/%03d", i), []byte("x"))
	}

	walkKeysCalls.Store(0)

	// Page through the whole bucket, 10 keys at a time.
	token := ""
	total := 0
	pages := 0
	for {
		li, err := p.ListObjectsV2(ctx(), "buck", "", token, "", 10, false, "")
		if err != nil {
			t.Fatal(err)
		}
		total += len(li.Objects)
		pages++
		if !li.IsTruncated {
			break
		}
		token = li.NextContinuationToken
	}
	if total != 30 || pages != 3 {
		t.Fatalf("paged %d objects over %d pages, want 30 over 3", total, pages)
	}
	// One walk for page 1; pages 2 and 3 come from cache.
	if n := walkKeysCalls.Load(); n != 1 {
		t.Fatalf("namespace walked %d times across 3 pages, want 1", n)
	}
}

func TestListCacheInvalidatedByWrite(t *testing.T) {
	p, _ := newTestPool(t, 4)
	_ = p.MakeBucket(ctx(), "buck", object.MakeBucketOptions{})
	put(t, p, "buck", "a", []byte("x"))

	li, _ := p.ListObjectsV2(ctx(), "buck", "", "", "", 100, false, "")
	if len(li.Objects) != 1 {
		t.Fatalf("want 1 object, got %d", len(li.Objects))
	}
	// A fresh local write must be visible immediately, not after the TTL.
	put(t, p, "buck", "b", []byte("y"))
	li, _ = p.ListObjectsV2(ctx(), "buck", "", "", "", 100, false, "")
	if len(li.Objects) != 2 {
		t.Fatalf("write not visible after cache invalidation: got %d objects", len(li.Objects))
	}
}
