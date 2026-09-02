package api

import (
	"testing"
	"time"
)

func TestAuditChainDetectsTampering(t *testing.T) {
	a := newAuditLog("") // in-memory only
	for i := 0; i < 25; i++ {
		a.record(auditEntry{Time: time.Now(), Method: "PUT", Path: "/b/k", Status: 200})
	}
	if ok, broken := a.verify(); broken != 0 || ok != 25 {
		t.Fatalf("fresh chain should verify: ok=%d brokenAt=%d", ok, broken)
	}

	// Tamper with a middle entry in place.
	a.mu.Lock()
	idx := (a.next - a.n + 12 + auditCap*2) % auditCap
	a.buf[idx].Path = "/b/hacked"
	a.mu.Unlock()

	ok, broken := a.verify()
	if broken == 0 {
		t.Fatal("tampering with a middle entry must break the chain")
	}
	if ok >= 25 {
		t.Fatalf("verify should stop before the end, ok=%d", ok)
	}
}
