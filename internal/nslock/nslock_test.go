package nslock

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestMutualExclusionSameKey(t *testing.T) {
	s := New()
	var inside, maxInside int32
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			l := s.For("bk", "hot")
			c, _ := l.GetLock(context.Background(), 0)
			n := atomic.AddInt32(&inside, 1)
			for {
				m := atomic.LoadInt32(&maxInside)
				if n <= m || atomic.CompareAndSwapInt32(&maxInside, m, n) {
					break
				}
			}
			time.Sleep(time.Millisecond)
			atomic.AddInt32(&inside, -1)
			l.Unlock(c)
		}()
	}
	wg.Wait()
	if maxInside != 1 {
		t.Fatalf("same-key lock allowed %d concurrent writers", maxInside)
	}
}

func TestDifferentKeysAndRLockDontDeadlock(t *testing.T) {
	s := New()
	done := make(chan struct{})
	go func() {
		var wg sync.WaitGroup
		for i := 0; i < 200; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				l := s.For("bk", string(rune('a'+i%26)))
				c, _ := l.GetRLock(context.Background(), 0)
				l.RUnlock(c)
				c, _ = l.GetLock(context.Background(), 0)
				l.Unlock(c)
			}(i)
		}
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("striped locks deadlocked")
	}
}
