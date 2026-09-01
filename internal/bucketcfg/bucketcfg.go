// Package bucketcfg stores the per-bucket configuration the S3 API layer
// owns (policy, CORS, tag set, event notifications) as JSON replicated
// across the volume roots — no external database. It is intentionally
// separate from the object backend's own bucket bookkeeping.
package bucketcfg

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
)

// Config is one bucket's API-level configuration.
type Config struct {
	Policy       json.RawMessage `json:"policy,omitempty"`       // raw bucket policy document
	CORS         []CORSRule      `json:"cors,omitempty"`
	Tags         map[string]string `json:"tags,omitempty"`
	Notification *Notification   `json:"notification,omitempty"`
}

// CORSRule mirrors an S3 CORS rule.
type CORSRule struct {
	AllowedOrigins []string `json:"allowedOrigins"`
	AllowedMethods []string `json:"allowedMethods"`
	AllowedHeaders []string `json:"allowedHeaders,omitempty"`
	ExposeHeaders  []string `json:"exposeHeaders,omitempty"`
	MaxAgeSeconds  int      `json:"maxAgeSeconds,omitempty"`
}

// Notification is a minimal bucket notification config: fire webhooks on
// object create/remove events matching optional prefix/suffix.
type Notification struct {
	Webhooks []WebhookTarget `json:"webhooks,omitempty"`
}

// WebhookTarget is one notification endpoint.
type WebhookTarget struct {
	ID     string   `json:"id"`
	URL    string   `json:"url"`
	Events []string `json:"events"` // "s3:ObjectCreated:*", "s3:ObjectRemoved:*"
	Prefix string   `json:"prefix,omitempty"`
	Suffix string   `json:"suffix,omitempty"`
}

// Store is the replicated JSON config store.
type Store struct {
	mu   sync.RWMutex
	dirs []string
	all  map[string]*Config
}

// Open loads (or initialises) the store from the given volume roots.
func Open(volumeDirs []string) (*Store, error) {
	seen := map[string]bool{}
	var dirs []string
	for _, d := range volumeDirs {
		abs, err := filepath.Abs(d)
		if err != nil || seen[abs] {
			continue
		}
		seen[abs] = true
		dirs = append(dirs, abs)
	}
	s := &Store{dirs: dirs, all: map[string]*Config{}}
	for _, d := range dirs {
		b, err := os.ReadFile(s.path(d))
		if err != nil {
			continue
		}
		var m map[string]*Config
		if json.Unmarshal(b, &m) == nil {
			s.all = m
			break
		}
	}
	if s.all == nil {
		s.all = map[string]*Config{}
	}
	return s, nil
}

func (s *Store) path(dir string) string {
	return filepath.Join(dir, ".gostore.sys", "bucketcfg", "config.json")
}

// Get returns a copy of the bucket config (never nil).
func (s *Store) Get(bucket string) *Config {
	s.mu.RLock()
	defer s.mu.RUnlock()
	c, ok := s.all[bucket]
	if !ok || c == nil {
		return &Config{}
	}
	cp := *c
	return &cp
}

// Update applies fn to the bucket's config and persists.
func (s *Store) Update(bucket string, fn func(*Config)) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	c := s.all[bucket]
	if c == nil {
		c = &Config{}
	}
	fn(c)
	s.all[bucket] = c
	return s.saveLocked()
}

// Delete removes a bucket's config entirely (on DeleteBucket).
func (s *Store) Delete(bucket string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.all, bucket)
	return s.saveLocked()
}

func (s *Store) saveLocked() error {
	b, err := json.MarshalIndent(s.all, "", "  ")
	if err != nil {
		return err
	}
	var firstErr error
	wrote := 0
	for _, d := range s.dirs {
		fp := s.path(d)
		if err := os.MkdirAll(filepath.Dir(fp), 0o755); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		tmp := fp + ".tmp"
		if err := os.WriteFile(tmp, b, 0o600); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if err := os.Rename(tmp, fp); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		wrote++
	}
	if wrote == 0 {
		return firstErr
	}
	return nil
}
