package cluster

import (
	"errors"
	"testing"
	"time"
)

func TestBreakerTripsAndRecovers(t *testing.T) {
	b := &breaker{}
	boom := errors.New("dial tcp: connection refused")

	// Below the threshold the breaker stays closed.
	for i := 0; i < breakerThreshold-1; i++ {
		if !b.allow() {
			t.Fatalf("breaker opened early after %d failures", i)
		}
		b.fail(boom)
	}
	if !b.allow() {
		t.Fatal("breaker should still allow the threshold-th call")
	}
	b.fail(boom) // this is the breakerThreshold-th failure -> trip

	if b.allow() {
		t.Fatal("breaker should be open immediately after tripping")
	}
	if !errors.Is(b.reason(), boom) {
		t.Fatalf("reason() = %v, want the last transport error", b.reason())
	}

	// After the cooldown a single probe is allowed; a second call in the same
	// window is not.
	b.trippedAt = time.Now().Add(-breakerCooldown - time.Millisecond)
	if !b.allow() {
		t.Fatal("breaker should allow one probe after the cooldown")
	}
	if b.allow() {
		t.Fatal("breaker should only allow one probe per cooldown window")
	}

	// A success on the probe closes it.
	b.ok()
	if !b.allow() {
		t.Fatal("breaker should be closed after a successful probe")
	}
	if b.reason() != errBreakerOpen {
		t.Fatalf("reason() after ok() = %v, want the generic open error", b.reason())
	}
}
