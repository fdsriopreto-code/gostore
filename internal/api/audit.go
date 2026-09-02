package api

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// A tamper-evident audit log: every mutating request is recorded as a
// hash-chained entry (entry.Hash = sha256(prevHash || entry-without-hash)).
// Altering or removing any entry breaks the chain from that point on, which
// GET /gostore/admin/v1/audit/verify detects. Entries are also appended to a
// daily JSONL file under <vol0>/.gostore.sys/audit/. MinIO's audit is a
// fire-and-forget webhook stream with no integrity guarantee.

const auditCap = 20000

// auditL is the process-wide audit log, initialised by NewServer.
var auditL *auditLog

// splitBucketKey pulls bucket/key out of a request path for the audit record.
func splitBucketKey(p string) (bucket, key string) {
	p = strings.TrimPrefix(p, "/")
	if i := strings.IndexByte(p, '/'); i >= 0 {
		return p[:i], p[i+1:]
	}
	return p, ""
}

type auditEntry struct {
	Seq      uint64    `json:"seq"`
	Time     time.Time `json:"time"`
	Method   string    `json:"method"`
	Path     string    `json:"path"`
	Bucket   string    `json:"bucket,omitempty"`
	Key      string    `json:"key,omitempty"`
	Action   string    `json:"action,omitempty"`
	Status   int       `json:"status"`
	Access   string    `json:"accessKey,omitempty"`
	IP       string    `json:"ip,omitempty"`
	ReqID    string    `json:"reqId,omitempty"`
	PrevHash string    `json:"prevHash"`
	Hash     string    `json:"hash"`
}

type auditLog struct {
	mu   sync.Mutex
	buf  []auditEntry
	next int
	n    int
	seq  uint64
	prev string
	dir  string // <vol0>/.gostore.sys/audit, "" = no file persistence
}

func newAuditLog(vol0 string) *auditLog {
	a := &auditLog{buf: make([]auditEntry, auditCap)}
	if vol0 != "" {
		a.dir = filepath.Join(vol0, ".gostore.sys", "audit")
		_ = os.MkdirAll(a.dir, 0o755)
		a.prev, a.seq = a.lastPersisted()
	}
	return a
}

// lastPersisted reads today's file (if any) and returns its last hash + seq so
// the chain continues across a restart.
func (a *auditLog) lastPersisted() (string, uint64) {
	if a.dir == "" {
		return "", 0
	}
	b, err := os.ReadFile(a.todayFile())
	if err != nil {
		return "", 0
	}
	last := ""
	for i := len(b) - 1; i >= 0; i-- {
		if b[i] == '\n' && i != len(b)-1 {
			last = string(b[i+1:])
			break
		}
	}
	if last == "" {
		last = string(b)
	}
	var e auditEntry
	if json.Unmarshal([]byte(last), &e) == nil {
		return e.Hash, e.Seq
	}
	return "", 0
}

func (a *auditLog) todayFile() string {
	return filepath.Join(a.dir, "audit-"+time.Now().UTC().Format("20060102")+".jsonl")
}

func (a *auditLog) record(e auditEntry) {
	a.mu.Lock()
	a.seq++
	e.Seq = a.seq
	e.PrevHash = a.prev
	e.Hash = auditHash(a.prev, e)
	a.prev = e.Hash
	a.buf[a.next] = e
	a.next = (a.next + 1) % auditCap
	if a.n < auditCap {
		a.n++
	}
	dir := a.dir
	a.mu.Unlock()

	if dir != "" {
		if line, err := json.Marshal(e); err == nil {
			if f, ferr := os.OpenFile(a.todayFile(), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600); ferr == nil {
				_, _ = f.Write(append(line, '\n'))
				_ = f.Close()
			}
		}
	}
}

// auditHash chains prev with the entry's identity fields (everything but Hash).
func auditHash(prev string, e auditEntry) string {
	e.Hash = ""
	e.PrevHash = prev
	b, _ := json.Marshal(e)
	sum := sha256.Sum256(append([]byte(prev), b...))
	return hex.EncodeToString(sum[:])
}

func (a *auditLog) snapshot(limit int, after uint64) []auditEntry {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]auditEntry, 0, a.n)
	for i := 0; i < a.n; i++ {
		idx := (a.next - a.n + i + auditCap*2) % auditCap
		if e := a.buf[idx]; e.Seq > after {
			out = append(out, e)
		}
	}
	if limit > 0 && len(out) > limit {
		out = out[len(out)-limit:]
	}
	return out
}

// verify walks the in-memory chain and returns the first seq where the hash
// linkage is broken, or 0 if intact.
func (a *auditLog) verify() (okCount int, brokenAt uint64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	prev := ""
	for i := 0; i < a.n; i++ {
		idx := (a.next - a.n + i + auditCap*2) % auditCap
		e := a.buf[idx]
		if i == 0 {
			prev = e.PrevHash // chain may start mid-stream (ring wrapped)
		}
		if e.PrevHash != prev || e.Hash != auditHash(prev, e) {
			return okCount, e.Seq
		}
		prev = e.Hash
		okCount++
	}
	return okCount, 0
}

func (s *Server) handleAdminAudit(w http.ResponseWriter, r *http.Request) {
	if auditL == nil {
		writeJSON(w, http.StatusOK, []auditEntry{})
		return
	}
	limit := atoiClamp(r.URL.Query().Get("limit"), 1, auditCap)
	if limit == 0 {
		limit = 200
	}
	var after uint64
	if v := r.URL.Query().Get("after"); v != "" {
		after = uint64(atoiClamp(v, 0, 1<<62))
	}
	writeJSON(w, http.StatusOK, auditL.snapshot(limit, after))
}

func (s *Server) handleAdminAuditVerify(w http.ResponseWriter, r *http.Request) {
	if auditL == nil {
		writeJSON(w, http.StatusOK, map[string]any{"entries": 0, "intact": true})
		return
	}
	ok, broken := auditL.verify()
	writeJSON(w, http.StatusOK, map[string]any{
		"entries": ok, "intact": broken == 0, "brokenAtSeq": broken,
	})
}
