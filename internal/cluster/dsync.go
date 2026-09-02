package cluster

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lojadopocket/gostore/internal/object"
)

// dsync-lite: a namespace lock is held when a quorum (N/2+1) of the cluster's
// nodes have granted it. Each grant carries a TTL so a crashed holder's locks
// expire; the holder refreshes in the background. It is advisory and assumes
// bounded clock skew — good enough to serialise object writes, not a
// consensus primitive.

var (
	lockTTL     = 60 * time.Second
	lockRefresh = 20 * time.Second
	lockTimeout = 10 * time.Second
)

const (
	lockBackoffLo = 50 * time.Millisecond
	lockBackoffHi = 1 * time.Second
)

// --- per-node lock table (the RPC server side) --------------------------

type hold struct {
	token     string // exclusive holder's token
	exclusive bool
	readers   map[string]struct{} // shared holders' tokens
	expiry    time.Time
}

type lockTable struct {
	mu sync.Mutex
	m  map[string]*hold
}

func newLockTable() *lockTable { return &lockTable{m: map[string]*hold{}} }

func (t *lockTable) acquire(res, token string, exclusive bool) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	h := t.m[res]
	if h != nil && time.Now().After(h.expiry) {
		h = nil // expired holder — the slot is free
	}
	if h == nil {
		nh := &hold{token: token, exclusive: exclusive, expiry: time.Now().Add(lockTTL)}
		if !exclusive {
			nh.readers = map[string]struct{}{token: {}}
		}
		t.m[res] = nh
		return true
	}
	if exclusive || h.exclusive {
		// The same holder refreshing its own exclusive lock is fine.
		if exclusive && h.exclusive && h.token == token {
			h.expiry = time.Now().Add(lockTTL)
			return true
		}
		return false
	}
	// shared + shared
	h.readers[token] = struct{}{}
	h.expiry = time.Now().Add(lockTTL)
	return true
}

func (t *lockTable) release(res, token string, exclusive bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	h := t.m[res]
	if h == nil {
		return
	}
	if exclusive {
		if h.exclusive && h.token == token {
			delete(t.m, res)
		}
		return
	}
	// Shared unlock must name a current reader — otherwise a late runlock
	// whose grant already expired (and whose slot may since have been taken by
	// a *different* holder) would corrupt that holder's state or free its
	// lock.
	if h.exclusive {
		return
	}
	if _, ok := h.readers[token]; !ok {
		return
	}
	delete(h.readers, token)
	if len(h.readers) == 0 {
		delete(t.m, res)
	}
}

func (t *lockTable) refresh(res, token string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	h := t.m[res]
	if h == nil {
		return false
	}
	if h.exclusive {
		if h.token != token {
			return false
		}
	} else if _, ok := h.readers[token]; !ok {
		return false
	}
	h.expiry = time.Now().Add(lockTTL)
	return true
}

func (s *RPCServer) handleLock(w http.ResponseWriter, r *http.Request) {
	op := r.URL.Path[len("/gostore/internal/lock/"):]
	q := r.URL.Query()
	res, token := q.Get("resource"), q.Get("token")
	switch op {
	case "lock":
		ok(w, s.locks.acquire(res, token, true))
	case "rlock":
		ok(w, s.locks.acquire(res, token, false))
	case "unlock":
		s.locks.release(res, token, true)
		w.WriteHeader(http.StatusOK)
	case "runlock":
		s.locks.release(res, token, false)
		w.WriteHeader(http.StatusOK)
	case "refresh":
		ok(w, s.locks.refresh(res, token))
	default:
		http.Error(w, "unknown lock op", http.StatusNotFound)
	}
}

func ok(w http.ResponseWriter, granted bool) {
	if granted {
		w.WriteHeader(http.StatusOK)
	} else {
		w.WriteHeader(http.StatusConflict)
	}
}

// --- coordinator (the client side) -----------------------------------

// lockPeer is one vote source: either the local table or a remote node.
type lockPeer struct {
	local  *lockTable
	base   string
	secret string
	hc     *http.Client
}

func (p *lockPeer) do(op, res, token string) bool {
	if p.local != nil {
		switch op {
		case "lock":
			return p.local.acquire(res, token, true)
		case "rlock":
			return p.local.acquire(res, token, false)
		case "unlock":
			p.local.release(res, token, true)
			return true
		case "runlock":
			p.local.release(res, token, false)
			return true
		case "refresh":
			return p.local.refresh(res, token)
		}
		return false
	}
	u := p.base + "/gostore/internal/lock/" + op + "?" +
		url.Values{"resource": {res}, "token": {token}}.Encode()
	req, _ := http.NewRequest(http.MethodPost, u, nil)
	req.Header.Set("X-Gostore-Cluster", p.secret)
	resp, err := p.hc.Do(req)
	if err != nil {
		return false
	}
	_ = resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

// LockCoordinator builds quorum namespace locks for the cluster.
type LockCoordinator struct {
	peers  []*lockPeer
	quorum int
}

// NewLockCoordinator wires the local table plus every peer node.
func NewLockCoordinator(local *RPCServer, peerBases []string, secret string) *LockCoordinator {
	peers := []*lockPeer{{local: local.locks}}
	hc := &http.Client{Timeout: 8 * time.Second}
	for _, b := range peerBases {
		peers = append(peers, &lockPeer{base: strings.TrimRight(b, "/"), secret: secret, hc: hc})
	}
	return &LockCoordinator{peers: peers, quorum: len(peers)/2 + 1}
}

// NewNSLock returns a distributed RWLocker for the resource.
func (c *LockCoordinator) NewNSLock(bucket string, objects ...string) object.RWLocker {
	res := bucket
	if len(objects) > 0 {
		res = bucket + "/" + objects[0]
	}
	return &distLock{c: c, res: res}
}

type distLock struct {
	c      *LockCoordinator
	res    string
	token  string
	stop   chan struct{}
	cancel context.CancelFunc // cancels the lock context if quorum is lost
}

func (l *distLock) GetLock(ctx context.Context, timeout time.Duration) (context.Context, error) {
	return l.grab(ctx, timeout, "lock", "unlock")
}
func (l *distLock) GetRLock(ctx context.Context, timeout time.Duration) (context.Context, error) {
	return l.grab(ctx, timeout, "rlock", "runlock")
}
func (l *distLock) Unlock(context.Context)  { l.drop("unlock") }
func (l *distLock) RUnlock(context.Context) { l.drop("runlock") }

// tryAll sends op to every peer concurrently and returns the number that
// answered yes. Sequential fan-out made a lock acquire cost N network round
// trips and widened the unlocked window under contention.
func (l *distLock) tryAll(op string) int {
	peers := l.c.peers
	if len(peers) == 1 {
		if peers[0].do(op, l.res, l.token) {
			return 1
		}
		return 0
	}
	var grants int32
	var wg sync.WaitGroup
	wg.Add(len(peers))
	for _, p := range peers {
		go func(p *lockPeer) {
			defer wg.Done()
			if p.do(op, l.res, l.token) {
				atomic.AddInt32(&grants, 1)
			}
		}(p)
	}
	wg.Wait()
	return int(grants)
}

// grab acquires the lock, retrying with capped exponential backoff until it
// wins a quorum, the caller's context ends, or timeout elapses. On success it
// returns a child context that is cancelled if the background refresher can
// no longer confirm a quorum — so an in-flight operation that respects the
// context is aborted instead of proceeding under a silently-lost lock.
func (l *distLock) grab(ctx context.Context, timeout time.Duration, op, undo string) (context.Context, error) {
	if timeout <= 0 {
		timeout = lockTimeout
	}
	deadline := time.Now().Add(timeout)
	backoff := lockBackoffLo
	for {
		l.token = randToken()
		if l.tryAll(op) >= l.c.quorum {
			lkCtx, cancel := context.WithCancel(ctx)
			l.cancel = cancel
			l.stop = make(chan struct{})
			go l.refresher()
			return lkCtx, nil
		}
		l.tryAll(undo) // drop partial grants before retrying

		if time.Now().After(deadline) {
			return ctx, object.ErrWriteQuorum
		}
		select {
		case <-ctx.Done():
			return ctx, ctx.Err()
		case <-time.After(backoff):
		}
		if backoff *= 2; backoff > lockBackoffHi {
			backoff = lockBackoffHi
		}
	}
}

func (l *distLock) drop(undo string) {
	if l.stop != nil {
		close(l.stop)
		l.stop = nil
	}
	if l.cancel != nil {
		l.cancel() // CancelFunc is idempotent and safe to call again
	}
	if l.token == "" {
		return
	}
	l.tryAll(undo)
}

func (l *distLock) refresher() {
	stop, cancel := l.stop, l.cancel // set before this goroutine started
	t := time.NewTicker(lockRefresh)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case <-t.C:
			if l.tryAll("refresh") < l.c.quorum {
				// Quorum lost: abort the holder's operation and stop.
				if cancel != nil {
					cancel()
				}
				return
			}
		}
	}
}

func randToken() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}
