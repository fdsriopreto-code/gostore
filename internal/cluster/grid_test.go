package cluster

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestGridUnaryRoundTripAndReconnect(t *testing.T) {
	node := newNode(t, 2, 0)
	rd := NewRemoteDisk(node.srv.URL, 0, testSecret)

	if err := rd.MakeVol(ctx(), "gridbucket"); err != nil {
		t.Fatalf("MakeVol over grid: %v", err)
	}
	if err := rd.WriteAll(ctx(), "gridbucket", "k/x", []byte("payload-over-grid")); err != nil {
		t.Fatalf("WriteAll over grid: %v", err)
	}
	got, err := rd.ReadAll(ctx(), "gridbucket", "k/x")
	if err != nil || string(got) != "payload-over-grid" {
		t.Fatalf("ReadAll over grid: %q %v", got, err)
	}
	// Confirm the shared connection is actually up (not falling back to HTTP).
	g := getGridConn(node.srv.URL, testSecret)
	g.mu.Lock()
	up := g.up
	g.mu.Unlock()
	if !up {
		t.Fatal("expected the grid connection to be established")
	}

	// Kill the underlying connection; the next call must transparently redial.
	g.fail(errGridUnavailable)
	g.mu.Lock()
	g.lastDial = time.Time{} // allow an immediate redial
	g.mu.Unlock()

	got, err = rd.ReadAll(ctx(), "gridbucket", "k/x")
	if err != nil || string(got) != "payload-over-grid" {
		t.Fatalf("ReadAll after forced reconnect: %q %v", got, err)
	}
}

func TestGridFallsBackToHTTPWhenNoRoute(t *testing.T) {
	// A server that has the disk RPC but NOT the grid route (old binary).
	node := newNode(t, 2, 0)
	mux := http.NewServeMux()
	mux.HandleFunc("/gostore/internal/disk/", node.rpc.auth(node.rpc.handleDisk))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	rd := NewRemoteDisk(srv.URL, 0, testSecret)
	if err := rd.MakeVol(ctx(), "fallbk"); err != nil {
		t.Fatalf("MakeVol should work via HTTP fallback: %v", err)
	}
	if err := rd.WriteAll(ctx(), "fallbk", "a", []byte("hi")); err != nil {
		t.Fatalf("WriteAll via fallback: %v", err)
	}
	got, err := rd.ReadAll(ctx(), "fallbk", "a")
	if err != nil || string(got) != "hi" {
		t.Fatalf("ReadAll via fallback: %q %v", got, err)
	}
}
