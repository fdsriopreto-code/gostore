package cluster

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/lojadopocket/gostore/internal/object"
)

// dsync-lite: a namespace lock is held when a quorum (N/2+1) of the cluster's
// nodes have granted it. Each grant carries a TTL so a crashed holder's locks
// expire; the holder refreshes in the background. It is advisory and assumes
// bounded clock skew — good enough to serialise object writes, not a
// consensus primitive.

const (
	lockTTL     = 60 * time.Second
	lockRefresh = 20 * time.Second
	lockTimeout = 20 * time.Second
)

// --- per-node lock table (the RPC server side) --------------------------

type hold struct {
	token     string
	exclusive bool
	readers   int
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
		h = nil
	}
	if h == nil {
		nh := &hold{token: token, exclusive: exclusive, expiry: time.Now().Add(lockTTL)}
		if !exclusive {
			nh.readers = 1
		}
		t.m[res] = nh
		return true
	}
	if exclusive || h.exclusive {
		// same holder refreshing an exclusive lock is fine
		if exclusive && h.exclusive && h.token == token {
			h.expiry = time.Now().Add(lockTTL)
			return true
		}
		return false
	}
	// shared + shared
	h.readers++
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
		if h.token == token {
			delete(t.m, res)
		}
		return
	}
	h.readers--
	if h.readers <= 0 {
		delete(t.m, res)
	}
}

func (t *lockTable) refresh(res, token string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	h := t.m[res]
	if h == nil || (h.exclusive && h.token != token) {
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
	c     *LockCoordinator
	res   string
	token string
	stop  chan struct{}
}

func (l *distLock) GetLock(ctx context.Context, _ time.Duration) (context.Context, error) {
	return ctx, l.grab("lock", "unlock")
}
func (l *distLock) GetRLock(ctx context.Context, _ time.Duration) (context.Context, error) {
	return ctx, l.grab("rlock", "runlock")
}
func (l *distLock) Unlock(context.Context)  { l.drop("unlock") }
func (l *distLock) RUnlock(context.Context) { l.drop("runlock") }

func (l *distLock) grab(op, undo string) error {
	l.token = randToken()
	grants := 0
	for _, p := range l.c.peers {
		if p.do(op, l.res, l.token) {
			grants++
		}
	}
	if grants < l.c.quorum {
		for _, p := range l.c.peers {
			p.do(undo, l.res, l.token)
		}
		return object.ErrWriteQuorum
	}
	l.stop = make(chan struct{})
	go l.refresher()
	return nil
}

func (l *distLock) drop(undo string) {
	if l.stop != nil {
		close(l.stop)
		l.stop = nil
	}
	for _, p := range l.c.peers {
		p.do(undo, l.res, l.token)
	}
}

func (l *distLock) refresher() {
	t := time.NewTicker(lockRefresh)
	defer t.Stop()
	for {
		select {
		case <-l.stop:
			return
		case <-t.C:
			for _, p := range l.c.peers {
				p.do("refresh", l.res, l.token)
			}
		}
	}
}

func randToken() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}
