package scanner

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/lojadopocket/gostore/internal/bucketcfg"
	"github.com/lojadopocket/gostore/internal/configstore"
	"github.com/lojadopocket/gostore/internal/object"
	fsb "github.com/lojadopocket/gostore/internal/object/fs"
)

type countingHealer struct {
	obj  object.Layer
	seen atomic.Int64
}

func (h *countingHealer) HealObject(ctx context.Context, bucket, key string) error {
	h.seen.Add(1)
	return nil
}

func TestDeepScrubVisitsEveryObject(t *testing.T) {
	f, err := fsb.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := f.MakeBucket(ctx(), "scrubbkt", object.MakeBucketOptions{}); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"a", "d/b", "d/c", "d/e/f"} {
		put(t, f, "scrubbkt", k, "x", 0, object.ObjectOptions{})
	}

	cfg, _ := bucketcfg.Open(configstore.NewDir(t.TempDir()))
	s := New(f, cfg, 0)
	h := &countingHealer{obj: f}
	s.healer = h // inject (erasure provides a real one in prod)

	s.DeepScrub(context.Background())

	st := s.ScrubStatus()
	if st == nil || st.Running {
		t.Fatalf("scrub status = %+v, want a finished run", st)
	}
	if st.ObjectsDone != 4 || h.seen.Load() != 4 {
		t.Fatalf("scrub visited %d objects (healer saw %d), want 4", st.ObjectsDone, h.seen.Load())
	}
	if st.FinishedAt.IsZero() {
		t.Fatal("FinishedAt not stamped")
	}
}

func TestDeepScrubIsSingleFlight(t *testing.T) {
	f, _ := fsb.New(t.TempDir())
	_ = f.MakeBucket(ctx(), "scrubbkt", object.MakeBucketOptions{})
	cfg, _ := bucketcfg.Open(configstore.NewDir(t.TempDir()))
	s := New(f, cfg, 0)
	s.healer = &countingHealer{obj: f}

	s.scrubRunning.Store(true) // pretend one is in progress
	s.DeepScrub(context.Background())
	if st := s.ScrubStatus(); st != nil {
		t.Fatal("a second concurrent scrub must be a no-op")
	}
}
