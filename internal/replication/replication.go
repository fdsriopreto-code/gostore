// Package replication asynchronously copies object create/delete events to a
// destination — another bucket on this server, or a remote S3-compatible
// endpoint (SigV4-signed). Best-effort: each event is attempted a few times
// then logged; there is no persistent retry queue yet.
package replication

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/lojadopocket/gostore/internal/bucketcfg"
	"github.com/lojadopocket/gostore/internal/logger"
	"github.com/lojadopocket/gostore/internal/object"
)

// Op is the replicated operation.
type Op int

const (
	Put Op = iota
	Delete
)

// Event is a replication work item.
type Event struct {
	Op        Op
	Bucket    string
	Key       string
	VersionID string
}

// Manager runs replication for a pool of buckets.
type Manager struct {
	cfg    *bucketcfg.Store
	obj    object.Layer
	client *http.Client
	wg     sync.WaitGroup
	sem    chan struct{}
}

// New builds a Manager. maxConcurrent bounds in-flight replications.
func New(cfg *bucketcfg.Store, obj object.Layer) *Manager {
	return &Manager{
		cfg: cfg, obj: obj,
		client: &http.Client{Timeout: 60 * time.Second},
		sem:    make(chan struct{}, 16),
	}
}

// Publish enqueues an event for the buckets that have matching rules.
func (m *Manager) Publish(e Event) {
	if m == nil || m.cfg == nil {
		return
	}
	rules := m.cfg.Get(e.Bucket).Replication
	for _, r := range rules {
		if r.Prefix != "" && !strings.HasPrefix(e.Key, r.Prefix) {
			continue
		}
		rule := r
		m.wg.Add(1)
		go func() {
			defer m.wg.Done()
			m.sem <- struct{}{}
			defer func() { <-m.sem }()
			m.run(rule, e)
		}()
	}
}

// Wait blocks until in-flight replications finish.
func (m *Manager) Wait(ctx context.Context) {
	done := make(chan struct{})
	go func() { m.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-ctx.Done():
	}
}

func (m *Manager) run(rule bucketcfg.ReplicationRule, e Event) {
	var err error
	for attempt := 0; attempt < 3; attempt++ {
		if e.Op == Delete {
			err = m.replicateDelete(rule, e)
		} else {
			err = m.replicatePut(rule, e)
		}
		if err == nil {
			return
		}
		time.Sleep(time.Duration(attempt+1) * time.Second)
	}
	logger.Warn("replication failed", "rule", rule.ID, "bucket", e.Bucket, "key", e.Key, "err", err)
}

func (m *Manager) replicatePut(rule bucketcfg.ReplicationRule, e Event) error {
	gr, err := m.obj.GetObjectNInfo(context.Background(), e.Bucket, e.Key, nil, nil, object.ObjectOptions{})
	if err != nil {
		return err
	}
	defer gr.Close()

	if rule.DestEndpoint == "" {
		// local destination bucket
		pr := object.NewPutObjReader(gr, gr.ObjInfo.Size, gr.ObjInfo.Size)
		ud := map[string]string{}
		for k, v := range gr.ObjInfo.UserDefined {
			ud[k] = v
		}
		_, err = m.obj.PutObject(context.Background(), rule.DestBucket, e.Key, pr, object.ObjectOptions{UserDefined: ud})
		return err
	}

	// remote S3 destination
	buf, err := io.ReadAll(gr)
	if err != nil {
		return err
	}
	ct := gr.ObjInfo.ContentType
	if ct == "" {
		ct = "application/octet-stream"
	}
	return m.signedRequest(rule, http.MethodPut, e.Key, buf, ct)
}

func (m *Manager) replicateDelete(rule bucketcfg.ReplicationRule, e Event) error {
	if rule.DestEndpoint == "" {
		_, err := m.obj.DeleteObject(context.Background(), rule.DestBucket, e.Key, object.ObjectOptions{})
		return err
	}
	return m.signedRequest(rule, http.MethodDelete, e.Key, nil, "")
}

// signedRequest sends a SigV4-signed request to the remote endpoint.
func (m *Manager) signedRequest(rule bucketcfg.ReplicationRule, method, key string, body []byte, contentType string) error {
	region := rule.DestRegion
	if region == "" {
		region = "us-east-1"
	}
	endpoint := strings.TrimRight(rule.DestEndpoint, "/")
	path := "/" + rule.DestBucket + "/" + key
	u := endpoint + encPath(path)

	now := time.Now().UTC()
	amzDate := now.Format("20060102T150405Z")
	dateStamp := now.Format("20060102")
	host := hostOf(endpoint)
	payloadHash := sha256hex(body)

	hdr := map[string]string{
		"host":                 host,
		"x-amz-date":           amzDate,
		"x-amz-content-sha256": payloadHash,
	}
	if contentType != "" {
		hdr["content-type"] = contentType
	}
	signed := sortedKeys(hdr)
	var ch strings.Builder
	for _, h := range signed {
		ch.WriteString(h + ":" + strings.TrimSpace(hdr[h]) + "\n")
	}
	canonReq := method + "\n" + encPath(path) + "\n\n" + ch.String() + "\n" +
		strings.Join(signed, ";") + "\n" + payloadHash
	scope := dateStamp + "/" + region + "/s3/aws4_request"
	sts := "AWS4-HMAC-SHA256\n" + amzDate + "\n" + scope + "\n" + sha256hex([]byte(canonReq))
	sig := hex.EncodeToString(hmacSHA256(sigKey(rule.DestSecretKey, dateStamp, region), sts))

	req, err := http.NewRequest(method, u, bytes.NewReader(body))
	if err != nil {
		return err
	}
	for k, v := range hdr {
		if k != "host" {
			req.Header.Set(k, v)
		}
	}
	req.Header.Set("Authorization",
		"AWS4-HMAC-SHA256 Credential="+rule.DestAccessKey+"/"+scope+
			", SignedHeaders="+strings.Join(signed, ";")+", Signature="+sig)

	resp, err := m.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 && resp.StatusCode != 404 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return &httpError{resp.StatusCode, string(b)}
	}
	return nil
}

type httpError struct {
	code int
	body string
}

func (e *httpError) Error() string { return "remote replied " + itoa(e.code) + ": " + e.body }

// --- tiny SigV4 / util helpers (self-contained) ------------------------

func hmacSHA256(key []byte, data string) []byte {
	h := hmac.New(sha256.New, key)
	h.Write([]byte(data))
	return h.Sum(nil)
}
func sha256hex(b []byte) string { s := sha256.Sum256(b); return hex.EncodeToString(s[:]) }
func sigKey(secret, dateStamp, region string) []byte {
	k := hmacSHA256([]byte("AWS4"+secret), dateStamp)
	k = hmacSHA256(k, region)
	k = hmacSHA256(k, "s3")
	return hmacSHA256(k, "aws4_request")
}
func sortedKeys(m map[string]string) []string {
	ks := make([]string, 0, len(m))
	for k := range m {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	return ks
}
func encPath(p string) string {
	var b strings.Builder
	for _, c := range []byte(p) {
		switch {
		case c >= 'A' && c <= 'Z', c >= 'a' && c <= 'z', c >= '0' && c <= '9',
			c == '-', c == '_', c == '.', c == '~', c == '/':
			b.WriteByte(c)
		default:
			b.WriteByte('%')
			const hexd = "0123456789ABCDEF"
			b.WriteByte(hexd[c>>4])
			b.WriteByte(hexd[c&0xf])
		}
	}
	return b.String()
}
func hostOf(endpoint string) string {
	s := endpoint
	s = strings.TrimPrefix(s, "https://")
	s = strings.TrimPrefix(s, "http://")
	if i := strings.IndexByte(s, '/'); i >= 0 {
		s = s[:i]
	}
	return s
}
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}
