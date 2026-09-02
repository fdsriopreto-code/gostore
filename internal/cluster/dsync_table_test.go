package cluster

import "testing"

// TestLockTableStaleReadUnlockIsIgnored guards the fix where a shared unlock
// carrying an expired grant's token could free (or corrupt) whatever holder
// occupies the slot now.
func TestLockTableStaleReadUnlockIsIgnored(t *testing.T) {
	tb := newLockTable()

	// Reader A takes a shared lock, then its grant is "lost" (we just forget
	// to unlock in time and the slot gets reused).
	if !tb.acquire("buck/obj", "reader-A", false) {
		t.Fatal("A should get the shared lock")
	}
	// Simulate expiry so the slot is reusable.
	tb.m["buck/obj"].expiry = tb.m["buck/obj"].expiry.Add(-2 * lockTTL)

	// Writer B now takes it exclusively.
	if !tb.acquire("buck/obj", "writer-B", true) {
		t.Fatal("B should get the exclusive lock after A's grant expired")
	}

	// A's late runlock arrives — it must NOT release B's exclusive lock.
	tb.release("buck/obj", "reader-A", false)
	if _, held := tb.m["buck/obj"]; !held {
		t.Fatal("stale reader unlock freed the exclusive holder's lock")
	}
	if h := tb.m["buck/obj"]; !h.exclusive || h.token != "writer-B" {
		t.Fatalf("exclusive hold corrupted by stale unlock: %+v", h)
	}

	// B's own unlock still works.
	tb.release("buck/obj", "writer-B", true)
	if _, held := tb.m["buck/obj"]; held {
		t.Fatal("B's unlock should have freed the slot")
	}
}

// TestLockTableFencesStaleToken: a stalled exclusive holder that comes back
// after its lock was reassigned (and expired) must not re-acquire with its
// now-stale token.
func TestLockTableFencesStaleToken(t *testing.T) {
	tb := newLockTable()

	stale := randToken() // issued first -> lower sequence
	fresh := randToken() // issued second -> higher sequence

	if !tb.acquire("b/k", stale, true) {
		t.Fatal("stale holder should get the lock initially")
	}
	// Its lock expires and the fresh holder takes over.
	tb.m["b/k"].expiry = tb.m["b/k"].expiry.Add(-2 * lockTTL)
	if !tb.acquire("b/k", fresh, true) {
		t.Fatal("fresh holder should acquire after expiry")
	}
	// Fresh holder's lock also lapses...
	tb.m["b/k"].expiry = tb.m["b/k"].expiry.Add(-2 * lockTTL)
	// ...and the original, resurrected holder tries again with its old token.
	if tb.acquire("b/k", stale, true) {
		t.Fatal("a token older than the highest granted must be fenced out")
	}
	// A brand-new token still works.
	if !tb.acquire("b/k", randToken(), true) {
		t.Fatal("a newer token should still acquire the free slot")
	}
}

// TestLockTableSharedRefcount confirms multi-reader shared locks are released
// only when the last distinct reader unlocks.
func TestLockTableSharedRefcount(t *testing.T) {
	tb := newLockTable()
	for _, tok := range []string{"r1", "r2", "r3"} {
		if !tb.acquire("b/k", tok, false) {
			t.Fatalf("%s should share the lock", tok)
		}
	}
	// A duplicate acquire by an existing reader must not inflate the count.
	tb.acquire("b/k", "r2", false)

	tb.release("b/k", "r1", false)
	tb.release("b/k", "r2", false)
	if _, held := tb.m["b/k"]; !held {
		t.Fatal("lock freed while r3 still holds it")
	}
	// An exclusive request must still be refused while r3 holds shared.
	if tb.acquire("b/k", "w", true) {
		t.Fatal("exclusive granted over a live shared holder")
	}
	tb.release("b/k", "r3", false)
	if _, held := tb.m["b/k"]; held {
		t.Fatal("lock not freed after the last reader unlocked")
	}
}
