package cluster

import (
	"bytes"
	"context"
	"crypto/rand"
	"io"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"

	"github.com/lojadopocket/gostore/internal/erasure"
	"github.com/lojadopocket/gostore/internal/kms"
	"github.com/lojadopocket/gostore/internal/object"
	"github.com/lojadopocket/gostore/internal/storage"
)

const testSecret = "cluster-test-secret"

// node bundles one gostore node's local disks + its RPC server.
type node struct {
	rpc   *RPCServer
	srv   *httptest.Server
	disks []*storage.LocalDisk
}

func newNode(t *testing.T, nDisks, setIdx int) *node {
	t.Helper()
	base := t.TempDir()
	ds := make([]*storage.LocalDisk, nDisks)
	rd := make([]diskRPC, nDisks)
	for i := 0; i < nDisks; i++ {
		d, err := storage.OpenLocalDisk(filepath.Join(base, "d", string(rune('a'+i))), setIdx, i)
		if err != nil {
			t.Fatal(err)
		}
		ds[i] = d
		rd[i] = d
	}
	rpc := NewRPCServer(rd, testSecret)
	srv := httptest.NewServer(rpc.Handler())
	t.Cleanup(srv.Close)
	return &node{rpc: rpc, srv: srv, disks: ds}
}

func ctx() context.Context { return context.Background() }

func TestClusterErasureAcrossNodes(t *testing.T) {
	a := newNode(t, 4, 0)
	b := newNode(t, 4, 0)

	// Pool from A's point of view: A's local disks + remote clients for B.
	disks := make([]erasure.Disk, 0, 8)
	for _, d := range a.disks {
		disks = append(disks, d)
	}
	for i := 0; i < 4; i++ {
		disks = append(disks, NewRemoteDisk(b.srv.URL, i, testSecret))
	}
	set, err := erasure.NewSet(disks)
	if err != nil {
		t.Fatal(err)
	}
	pool, err := erasure.NewPool(set)
	if err != nil {
		t.Fatal(err)
	}
	km, _ := kms.New([]string{t.TempDir()})
	pool.SetKMS(km)
	coord := NewLockCoordinator(a.rpc, []string{b.srv.URL}, testSecret)
	pool.SetLocker(coord.NewNSLock)

	if err := pool.MakeBucket(ctx(), "cbuck", object.MakeBucketOptions{}); err != nil {
		t.Fatal(err)
	}
	data := make([]byte, 1<<20+345)
	_, _ = rand.Read(data)
	if _, err := pool.PutObject(ctx(), "cbuck", "obj",
		object.NewPutObjReader(bytes.NewReader(data), int64(len(data)), int64(len(data))), object.ObjectOptions{}); err != nil {
		t.Fatalf("put across cluster: %v", err)
	}
	gr, err := pool.GetObjectNInfo(ctx(), "cbuck", "obj", nil, nil, object.ObjectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(gr)
	gr.Close()
	if !bytes.Equal(got, data) {
		t.Fatal("cluster round-trip mismatch")
	}

	// Node B goes down: 4 remote shards vanish. Reads still work (== dataBlocks),
	// writes fail (write quorum needs 5).
	b.srv.Close()
	gr, err = pool.GetObjectNInfo(ctx(), "cbuck", "obj", nil, nil, object.ObjectOptions{})
	if err != nil {
		t.Fatalf("read after node loss: %v", err)
	}
	got, _ = io.ReadAll(gr)
	gr.Close()
	if !bytes.Equal(got, data) {
		t.Fatal("post-node-loss read mismatch")
	}
	if _, err := pool.PutObject(ctx(), "cbuck", "obj2",
		object.NewPutObjReader(bytes.NewReader(data), int64(len(data)), int64(len(data))), object.ObjectOptions{}); err == nil {
		t.Fatal("write should fail with a node down (no write quorum)")
	}
}

func TestDistributedLockMutualExclusion(t *testing.T) {
	a := newNode(t, 1, 0)
	b := newNode(t, 1, 0)
	coord := NewLockCoordinator(a.rpc, []string{b.srv.URL}, testSecret)

	l1 := coord.NewNSLock("bk", "key")
	if _, err := l1.GetLock(ctx(), 0); err != nil {
		t.Fatalf("first lock should succeed: %v", err)
	}
	// second exclusive lock on the same resource must be denied (A already holds it)
	l2 := coord.NewNSLock("bk", "key")
	if _, err := l2.GetLock(ctx(), 0); err == nil {
		l2.Unlock(ctx())
		t.Fatal("second concurrent exclusive lock should be denied")
	}
	l1.Unlock(ctx())
	// now it's free again
	l3 := coord.NewNSLock("bk", "key")
	if _, err := l3.GetLock(ctx(), 0); err != nil {
		t.Fatalf("lock after release should succeed: %v", err)
	}
	l3.Unlock(ctx())
}

func TestDistributedLockConcurrent(t *testing.T) {
	a := newNode(t, 1, 0)
	b := newNode(t, 1, 0)
	coord := NewLockCoordinator(a.rpc, []string{b.srv.URL}, testSecret)

	var mu sync.Mutex
	inside := 0
	maxInside := 0
	var wg sync.WaitGroup
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for tries := 0; tries < 40; tries++ {
				l := coord.NewNSLock("bk", "hot")
				if _, err := l.GetLock(ctx(), 0); err != nil {
					continue
				}
				mu.Lock()
				inside++
				if inside > maxInside {
					maxInside = inside
				}
				mu.Unlock()
				mu.Lock()
				inside--
				mu.Unlock()
				l.Unlock(ctx())
				return
			}
		}()
	}
	wg.Wait()
	if maxInside > 1 {
		t.Fatalf("distributed lock allowed %d concurrent holders", maxInside)
	}
}
