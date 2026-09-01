package iam

import (
	"encoding/json"
	"os"
	"path/filepath"
	"time"
)

// persisted is the on-disk shape of the IAM database. A byte-identical copy
// is written to <vol>/.gostore.sys/iam/store.json on every volume, and read
// back from the first one available — no external database.
type persisted struct {
	Users     map[string]userRec    `json:"users"`
	Policies  map[string]string     `json:"policies"` // name -> JSON document
	SvcAccts  map[string]svcAcctRec `json:"serviceAccounts"`
	UpdatedAt time.Time             `json:"updatedAt"`
}

type userRec struct {
	SecretKey string   `json:"secretKey"`
	Policies  []string `json:"policies"`
	Status    string   `json:"status"` // "enabled" | "disabled"
}

type svcAcctRec struct {
	SecretKey    string `json:"secretKey"`
	ParentUser   string `json:"parentUser"`
	InlinePolicy string `json:"inlinePolicy,omitempty"` // JSON document or ""
	Status       string `json:"status"`
}

type store struct {
	dirs []string
}

func newStore(dirs []string) *store {
	// De-dup and keep only the top-level volume roots.
	seen := map[string]bool{}
	var out []string
	for _, d := range dirs {
		abs, err := filepath.Abs(d)
		if err != nil || seen[abs] {
			continue
		}
		seen[abs] = true
		out = append(out, abs)
	}
	return &store{dirs: out}
}

func (s *store) path(dir string) string {
	return filepath.Join(dir, ".gostore.sys", "iam", "store.json")
}

func (s *store) load() (persisted, error) {
	var p persisted
	for _, d := range s.dirs {
		b, err := os.ReadFile(s.path(d))
		if err != nil {
			continue
		}
		if err := json.Unmarshal(b, &p); err == nil {
			return p, nil
		}
	}
	return persisted{
		Users: map[string]userRec{}, Policies: map[string]string{}, SvcAccts: map[string]svcAcctRec{},
	}, nil
}

func (s *store) save(p persisted) error {
	p.UpdatedAt = time.Now().UTC()
	b, err := json.MarshalIndent(p, "", "  ")
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
