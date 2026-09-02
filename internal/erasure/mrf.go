package erasure

import (
	"context"
	"encoding/json"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/lojadopocket/gostore/internal/configstore"
	"github.com/lojadopocket/gostore/internal/logger"
)

// MRF (Metadata/data Replication Failure) queue: when an object write reaches
// write quorum but not every disk, the object is recorded here and a
// background worker re-heals it until all shards/metadata are in place. This
// is what turns a transient disk or node blip into eventual self-repair
// instead of a permanent under-replicated object. Mirrors MinIO's mrf.
//
// The queue is persisted through configstore (an object replicated across
// the cluster) so a restart mid-blip doesn't lose the repair list.

const mrfKey = "heal/mrf.json"
const mrfMaxEntries = 20000

var mrfInterval = 5 * time.Minute

// SetMRFInterval overrides the background heal cadence. Call at startup.
func SetMRFInterval(d time.Duration) {
	if d > 0 {
		mrfInterval = d
	}
}

type mrfQueue struct {
	be configstore.Backend

	mu      sync.Mutex
	pending map[string]int64 // "bucket/key" -> first-seen unix seconds
	dirty   bool
}

func newMRFQueue(be configstore.Backend) *mrfQueue {
	q := &mrfQueue{be: be, pending: map[string]int64{}}
	if b, err := be.ReadConfig(context.Background(), mrfKey); err == nil {
		_ = json.Unmarshal(b, &q.pending)
	}
	return q
}

func (q *mrfQueue) add(bucket, key string) {
	id := bucket + "/" + key
	q.mu.Lock()
	if _, ok := q.pending[id]; !ok {
		if len(q.pending) >= mrfMaxEntries {
			q.mu.Unlock()
			return
		}
		q.pending[id] = time.Now().Unix()
		q.dirty = true
	}
	q.mu.Unlock()
}

func (q *mrfQueue) snapshot() map[string]int64 {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := make(map[string]int64, len(q.pending))
	for k, v := range q.pending {
		out[k] = v
	}
	return out
}

func (q *mrfQueue) done(id string) {
	q.mu.Lock()
	if _, ok := q.pending[id]; ok {
		delete(q.pending, id)
		q.dirty = true
	}
	q.mu.Unlock()
}

func (q *mrfQueue) flush(ctx context.Context) {
	q.mu.Lock()
	if !q.dirty {
		q.mu.Unlock()
		return
	}
	b, _ := json.Marshal(q.pending)
	q.dirty = false
	q.mu.Unlock()
	if err := q.be.WriteConfig(ctx, mrfKey, b); err != nil {
		logger.Debug("mrf: flush failed", "err", err)
	}
}

// EnableMRF wires partial-write detection on every set and loads the
// persisted queue. Call before StartMRF.
func (p *Pool) EnableMRF(be configstore.Backend) {
	q := newMRFQueue(be)
	p.mrf = q
	for _, s := range p.sets {
		s.mrf = q.add
	}
}

// StartMRF runs the background re-heal loop until ctx is done.
func (p *Pool) StartMRF(ctx context.Context) {
	if p.mrf == nil {
		return
	}
	t := time.NewTicker(mrfInterval)
	go func() {
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				p.mrf.flush(context.Background())
				return
			case <-t.C:
				p.drainMRF(ctx)
			}
		}
	}()
	logger.Info("MRF self-heal worker started", "interval", mrfInterval)
}

func (p *Pool) drainMRF(ctx context.Context) {
	pend := p.mrf.snapshot()
	if len(pend) == 0 {
		return
	}
	ids := make([]string, 0, len(pend))
	for id := range pend {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	healed, failed := 0, 0
	for _, id := range ids {
		if ctx.Err() != nil {
			break
		}
		slash := strings.IndexByte(id, '/')
		if slash <= 0 || slash == len(id)-1 {
			p.mrf.done(id) // malformed, drop
			continue
		}
		bucket, key := id[:slash], id[slash+1:]
		set := p.setFor(key)
		release := healThrottle()
		_, _, err := set.healObject(ctx, bucket, key)
		release()
		if err != nil {
			failed++
			continue
		}
		p.mrf.done(id)
		healed++
	}
	p.mrf.flush(ctx)
	if healed > 0 || failed > 0 {
		logger.Info("MRF pass complete", "healed", healed, "stillPending", failed)
	}
}
