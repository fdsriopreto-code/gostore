package cluster

import (
	"testing"
)

func TestPeerHealthReflectsBreaker(t *testing.T) {
	base := "http://peerhealth.invalid:9000"
	b := getBreaker(base)

	got := findPeer(PeerHealth(), base)
	if got == nil || !got.Up {
		t.Fatalf("a fresh peer should read as up: %+v", got)
	}

	for i := 0; i < breakerThreshold; i++ {
		b.fail(errBreakerOpen)
	}
	got = findPeer(PeerHealth(), base)
	if got == nil || got.Up || got.ConsecutiveFails < breakerThreshold {
		t.Fatalf("after %d failures the peer should read as down: %+v", breakerThreshold, got)
	}

	b.ok()
	got = findPeer(PeerHealth(), base)
	if got == nil || !got.Up {
		t.Fatalf("after a success the peer should read as up again: %+v", got)
	}
}

func findPeer(ps []PeerStatus, base string) *PeerStatus {
	for i := range ps {
		if ps[i].Base == base {
			return &ps[i]
		}
	}
	return nil
}
