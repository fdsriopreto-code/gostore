package erasure

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/lojadopocket/gostore/internal/configstore"
	"github.com/lojadopocket/gostore/internal/object"
)

func waitJob(t *testing.T, p *Pool) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if !p.PoolStatus().Progress.Running {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("pool job did not finish in time")
}

func TestDecommissionMovesEverythingOffTheSet(t *testing.T) {
	p := newTestPoolSets(t, 3, 4)
	p.LoadLayout(configstore.NewDir(t.TempDir()))
	_ = p.MakeBucket(ctx(), "buck", object.MakeBucketOptions{})

	want := map[string][]byte{}
	for i := 0; i < 40; i++ {
		sz := 300 + i*937 // mix of inline (<128K) and sharded
		if i%7 == 0 {
			sz = 200*1024 + i
		}
		b := make([]byte, sz)
		_, _ = rand.Read(b)
		k := fmt.Sprintf("d/obj-%03d", i)
		put(t, p, "buck", k, b)
		want[k] = b
	}

	// Which set does object 0's key live on? Decommission that one.
	victim := -1
	for si, s := range p.sets {
		if ks := p.keysOnSet(ctx(), s, "buck"); len(ks) > 0 {
			victim = si
			break
		}
	}
	if victim < 0 {
		t.Fatal("no set holds any object?")
	}

	if err := p.Decommission(ctx(), victim); err != nil {
		t.Fatalf("Decommission: %v", err)
	}
	waitJob(t, p)

	st := p.PoolStatus()
	if len(st.Draining) != 1 || st.Draining[0] != victim {
		t.Fatalf("status draining = %v, want [%d]", st.Draining, victim)
	}
	if st.Progress.Failed != 0 {
		t.Fatalf("%d moves failed: %s", st.Progress.Failed, st.Progress.LastError)
	}
	if got := p.keysOnSet(ctx(), p.sets[victim], "buck"); len(got) != 0 {
		t.Fatalf("drained set still holds %d objects: %v", len(got), got)
	}

	// Every object still reads back correctly, and setFor never returns the
	// drained set for new writes.
	for k, b := range want {
		if got := get(t, p, "buck", k); !bytes.Equal(got, b) {
			t.Fatalf("object %s corrupted after decommission (%d vs %d bytes)", k, len(got), len(b))
		}
		if p.setFor(k) == p.sets[victim] {
			t.Fatalf("setFor(%q) still routes to the drained set", k)
		}
	}
}

func TestDecommissionVersionedObject(t *testing.T) {
	p := newTestPoolSets(t, 2, 4)
	p.LoadLayout(configstore.NewDir(t.TempDir()))
	_ = p.MakeBucket(ctx(), "vbuck", object.MakeBucketOptions{})

	vput := func(key, body string) {
		_, err := p.PutObject(ctx(), "vbuck", key,
			object.NewPutObjReader(bytes.NewReader([]byte(body)), int64(len(body)), int64(len(body))),
			object.ObjectOptions{Versioned: true, UserDefined: map[string]string{"content-type": "text/plain"}})
		if err != nil {
			t.Fatalf("versioned put: %v", err)
		}
	}
	vput("k", "v1")
	vput("k", "v2")
	vput("k", "v3")

	// figure out which set holds it, then decommission that set
	victim := 0
	if len(p.keysOnSet(ctx(), p.sets[0], "vbuck")) == 0 {
		victim = 1
	}
	if err := p.Decommission(ctx(), victim); err != nil {
		t.Fatal(err)
	}
	waitJob(t, p)

	if st := p.PoolStatus(); st.Progress.Failed != 0 {
		t.Fatalf("versioned move failed: %s", st.Progress.LastError)
	}

	lv, err := p.ListObjectVersions(ctx(), "vbuck", "", "", "", "", 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(lv.Objects) != 3 {
		t.Fatalf("want 3 versions after move, got %d", len(lv.Objects))
	}
	// current version reads as v3
	gr, err := p.GetObjectNInfo(ctx(), "vbuck", "k", nil, nil, object.ObjectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(gr)
	gr.Close()
	if string(got) != "v3" {
		t.Fatalf("current version = %q, want v3", got)
	}
}

func TestRebalanceRelocatesMisplacedObject(t *testing.T) {
	p := newTestPoolSets(t, 2, 4)
	p.LoadLayout(configstore.NewDir(t.TempDir()))
	_ = p.MakeBucket(ctx(), "rebuck", object.MakeBucketOptions{})

	body := make([]byte, 5000)
	_, _ = rand.Read(body)

	// Find a key that hashes to set 1, then physically write it onto set 0
	// (the "wrong" set) so rebalance has something to relocate.
	var key string
	for i := 0; i < 200; i++ {
		cand := fmt.Sprintf("obj-%d", i)
		if p.setFor(cand) == p.sets[1] {
			key = cand
			break
		}
	}
	if key == "" {
		t.Skip("no key hashed to set 1")
	}
	wrong := p.sets[0]
	if _, err := wrong.putObject(ctx(), "rebuck", key, []partSource{
		{Number: 1, Size: int64(len(body)), Reader: bytes.NewReader(body)},
	}, userMeta{contentType: "application/octet-stream"}); err != nil {
		t.Fatal(err)
	}

	if err := p.Rebalance(ctx()); err != nil {
		t.Fatal(err)
	}
	waitJob(t, p)

	if st := p.PoolStatus(); st.Progress.Moved != 1 {
		t.Fatalf("rebalance moved %d objects, want 1 (failed=%d, err=%q)",
			st.Progress.Moved, st.Progress.Failed, st.Progress.LastError)
	}
	if got := p.keysOnSet(ctx(), wrong, "rebuck"); contains(got, key) {
		t.Fatalf("rebalance left %q on the wrong set: %v", key, got)
	}
	if got := p.keysOnSet(ctx(), p.sets[1], "rebuck"); !contains(got, key) {
		t.Fatalf("rebalance did not place %q on the right set: %v", key, got)
	}
	if got := get(t, p, "rebuck", key); !bytes.Equal(got, body) {
		t.Fatalf("%q unreadable after rebalance", key)
	}
}

func contains(ss []string, s string) bool {
	for _, x := range ss {
		if x == s {
			return true
		}
	}
	return false
}
