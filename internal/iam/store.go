package iam

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/lojadopocket/gostore/internal/configstore"
)

// persisted is the serialised shape of the IAM database. It is stored as a
// single blob ("iam/store.json") through configstore.Backend — i.e. as an
// object replicated across every disk of every erasure set, so it is
// cluster-wide with no external database. On a single disk the blob lands at
// the same <root>/.gostore.sys/iam/store.json path older builds used.
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

const storeKey = "iam/store.json"

type store struct {
	be configstore.Backend
}

func newStore(be configstore.Backend) *store { return &store{be: be} }

func emptyPersisted() persisted {
	return persisted{
		Users:    map[string]userRec{},
		Policies: map[string]string{},
		SvcAccts: map[string]svcAcctRec{},
	}
}

func (s *store) load() (persisted, error) {
	b, err := s.be.ReadConfig(context.Background(), storeKey)
	if errors.Is(err, configstore.ErrNotFound) {
		return emptyPersisted(), nil
	}
	if err != nil {
		return persisted{}, err
	}
	var p persisted
	if err := json.Unmarshal(b, &p); err != nil {
		return emptyPersisted(), nil
	}
	if p.Users == nil {
		p.Users = map[string]userRec{}
	}
	if p.Policies == nil {
		p.Policies = map[string]string{}
	}
	if p.SvcAccts == nil {
		p.SvcAccts = map[string]svcAcctRec{}
	}
	return p, nil
}

func (s *store) save(p persisted) error {
	if p.UpdatedAt.IsZero() {
		p.UpdatedAt = time.Now().UTC()
	}
	b, err := json.MarshalIndent(p, "", "  ")
	if err != nil {
		return err
	}
	return s.be.WriteConfig(context.Background(), storeKey, b)
}
