// Package event is a minimal bucket-notification bus: it matches object
// create/remove events against a bucket's configured webhook targets and
// POSTs an S3-style event record to each, asynchronously with retry.
package event

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/lojadopocket/gostore/internal/bucketcfg"
	"github.com/lojadopocket/gostore/internal/logger"
)

// Kind is the event type.
type Kind string

const (
	ObjectCreated Kind = "s3:ObjectCreated:Put"
	ObjectRemoved Kind = "s3:ObjectRemoved:Delete"
)

// Event is a single notification.
type Event struct {
	Kind      Kind
	Bucket    string
	Key       string
	Size      int64
	ETag      string
	VersionID string
	SourceIP  string
	Time      time.Time
}

// Bus dispatches events to configured webhook targets.
type Bus struct {
	cfg    *bucketcfg.Store
	client *http.Client
	wg     sync.WaitGroup
}

// New builds a Bus reading targets from the bucket config store.
func New(cfg *bucketcfg.Store) *Bus {
	return &Bus{cfg: cfg, client: &http.Client{Timeout: 10 * time.Second}}
}

// Publish enqueues an event for asynchronous delivery. It never blocks the
// caller for network I/O.
func (b *Bus) Publish(e Event) {
	if b == nil || b.cfg == nil {
		return
	}
	n := b.cfg.Get(e.Bucket).Notification
	if n == nil || len(n.Webhooks) == 0 {
		return
	}
	if e.Time.IsZero() {
		e.Time = time.Now().UTC()
	}
	for _, wh := range n.Webhooks {
		if !matches(wh, e) {
			continue
		}
		b.wg.Add(1)
		go func(wh bucketcfg.WebhookTarget) {
			defer b.wg.Done()
			b.deliver(wh, e)
		}(wh)
	}
}

// Wait blocks until in-flight deliveries finish (used on shutdown).
func (b *Bus) Wait(ctx context.Context) {
	done := make(chan struct{})
	go func() { b.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-ctx.Done():
	}
}

func matches(wh bucketcfg.WebhookTarget, e Event) bool {
	if wh.Prefix != "" && !strings.HasPrefix(e.Key, wh.Prefix) {
		return false
	}
	if wh.Suffix != "" && !strings.HasSuffix(e.Key, wh.Suffix) {
		return false
	}
	if len(wh.Events) == 0 {
		return true
	}
	for _, pat := range wh.Events {
		if eventMatch(pat, string(e.Kind)) {
			return true
		}
	}
	return false
}

func eventMatch(pat, kind string) bool {
	if strings.HasSuffix(pat, ":*") {
		return strings.HasPrefix(kind, strings.TrimSuffix(pat, "*"))
	}
	return pat == kind
}

func (b *Bus) deliver(wh bucketcfg.WebhookTarget, e Event) {
	payload := map[string]any{
		"EventName": string(e.Kind),
		"Key":       e.Bucket + "/" + e.Key,
		"Records": []map[string]any{{
			"eventVersion": "2.1",
			"eventSource":  "gostore:s3",
			"eventTime":    e.Time.Format(time.RFC3339),
			"eventName":    string(e.Kind),
			"requestParameters": map[string]string{"sourceIPAddress": e.SourceIP},
			"s3": map[string]any{
				"bucket": map[string]any{"name": e.Bucket},
				"object": map[string]any{
					"key":       e.Key,
					"size":      e.Size,
					"eTag":      e.ETag,
					"versionId": e.VersionID,
				},
			},
		}},
	}
	body, _ := json.Marshal(payload)

	for attempt := 0; attempt < 3; attempt++ {
		req, err := http.NewRequest(http.MethodPost, wh.URL, bytes.NewReader(body))
		if err != nil {
			return
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("User-Agent", "gostore-notify/1")
		resp, err := b.client.Do(req)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode < 300 {
				return
			}
		}
		time.Sleep(time.Duration(attempt+1) * 500 * time.Millisecond)
	}
	logger.Warn("event delivery failed", "target", wh.ID, "url", wh.URL, "bucket", e.Bucket, "key", e.Key)
}
