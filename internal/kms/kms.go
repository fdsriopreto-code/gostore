// Package kms is a minimal local key-management service: it holds one
// 256-bit master key (loaded from GOSTORE_KMS_MASTER_KEY or generated and
// persisted to the volume) and wraps/unwraps per-object data keys with it.
// No external KMS — same "everything on the volume" principle as the rest.
package kms

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"os"
	"path/filepath"
)

// Manager holds the master key.
type Manager struct {
	master [32]byte
}

// New loads the master key: GOSTORE_KMS_MASTER_KEY (base64 std/url, 32 bytes)
// takes precedence; otherwise <firstVolume>/.gostore.sys/kms/master.key is
// used, generated on first run.
func New(volumeDirs []string) (*Manager, error) {
	m := &Manager{}
	if env := os.Getenv("GOSTORE_KMS_MASTER_KEY"); env != "" {
		raw, err := decodeB64(env)
		if err != nil || len(raw) != 32 {
			return nil, errors.New("kms: GOSTORE_KMS_MASTER_KEY must be base64 of exactly 32 bytes")
		}
		copy(m.master[:], raw)
		return m, nil
	}
	if len(volumeDirs) == 0 {
		return nil, errors.New("kms: no volume to store the master key and no GOSTORE_KMS_MASTER_KEY")
	}
	p := filepath.Join(volumeDirs[0], ".gostore.sys", "kms", "master.key")
	if b, err := os.ReadFile(p); err == nil {
		raw, derr := decodeB64(string(b))
		if derr != nil || len(raw) != 32 {
			return nil, errors.New("kms: corrupt master.key")
		}
		copy(m.master[:], raw)
		return m, nil
	}
	if _, err := rand.Read(m.master[:]); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(p), 0o700); err != nil {
		return nil, err
	}
	if err := os.WriteFile(p, []byte(base64.StdEncoding.EncodeToString(m.master[:])), 0o600); err != nil {
		return nil, err
	}
	return m, nil
}

func decodeB64(s string) ([]byte, error) {
	if b, err := base64.StdEncoding.DecodeString(s); err == nil {
		return b, nil
	}
	return base64.RawURLEncoding.DecodeString(s)
}

// GenerateDataKey returns a fresh random 32-byte data key.
func (m *Manager) GenerateDataKey() ([]byte, error) {
	k := make([]byte, 32)
	_, err := rand.Read(k)
	return k, err
}

// WrapKey encrypts a data key with the master key (AES-256-GCM). Output is
// nonce(12) || ciphertext || tag(16).
func (m *Manager) WrapKey(dek []byte) ([]byte, error) {
	g, err := m.gcm()
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, g.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}
	return append(nonce, g.Seal(nil, nonce, dek, nil)...), nil
}

// UnwrapKey reverses WrapKey.
func (m *Manager) UnwrapKey(wrapped []byte) ([]byte, error) {
	g, err := m.gcm()
	if err != nil {
		return nil, err
	}
	ns := g.NonceSize()
	if len(wrapped) < ns+16 {
		return nil, errors.New("kms: wrapped key too short")
	}
	return g.Open(nil, wrapped[:ns], wrapped[ns:], nil)
}

func (m *Manager) gcm() (cipher.AEAD, error) {
	b, err := aes.NewCipher(m.master[:])
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(b)
}
