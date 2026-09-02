// Package kms is gostore's key-management service. By default it holds one
// local 256-bit master key (from GOSTORE_KMS_MASTER_KEY or generated on the
// volume) and wraps/unwraps per-object data keys with it. When
// GOSTORE_KMS_VAULT_ADDR + GOSTORE_KMS_VAULT_TOKEN are set it instead
// delegates wrap/unwrap to HashiCorp Vault's Transit engine — the master key
// then never lives in the gostore process and Vault handles rotation.
package kms

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// Manager holds the master key (local mode) and/or Vault Transit settings.
type Manager struct {
	master [32]byte
	local  bool

	vaultAddr  string
	vaultToken string
	vaultKey   string
	hc         *http.Client
}

// Mode describes where key wrapping happens ("local" or "vault-transit").
func (m *Manager) Mode() string {
	if m.vaultAddr != "" {
		return "vault-transit"
	}
	return "local"
}

// New loads the master key: GOSTORE_KMS_MASTER_KEY (base64 std/url, 32 bytes)
// takes precedence; otherwise <firstVolume>/.gostore.sys/kms/master.key is
// used, generated on first run.
func New(volumeDirs []string) (*Manager, error) {
	m := &Manager{}

	// Vault Transit: no local master key needed at all.
	if addr := strings.TrimRight(os.Getenv("GOSTORE_KMS_VAULT_ADDR"), "/"); addr != "" {
		tok := os.Getenv("GOSTORE_KMS_VAULT_TOKEN")
		if tok == "" {
			return nil, errors.New("kms: GOSTORE_KMS_VAULT_ADDR set but GOSTORE_KMS_VAULT_TOKEN is empty")
		}
		m.vaultAddr, m.vaultToken = addr, tok
		m.vaultKey = os.Getenv("GOSTORE_KMS_VAULT_KEY")
		if m.vaultKey == "" {
			m.vaultKey = "gostore"
		}
		m.hc = &http.Client{Timeout: 10 * time.Second}
		if err := m.vaultHealth(); err != nil {
			return nil, fmt.Errorf("kms: Vault Transit unreachable: %w", err)
		}
		return m, nil
	}

	m.local = true
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

// vaultPrefix marks a wrapped DEK produced by Vault Transit (Vault's own
// "vault:vN:" convention), so UnwrapKey can route old local blobs and new
// Vault blobs correctly even after a mode switch.
var vaultPrefix = []byte("vault:")

// WrapKey encrypts a data key: via Vault Transit if configured, else with the
// local master key (AES-256-GCM: nonce(12) || ciphertext || tag(16)).
func (m *Manager) WrapKey(dek []byte) ([]byte, error) {
	if m.vaultAddr != "" {
		return m.vaultEncrypt(dek)
	}
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

// UnwrapKey reverses WrapKey. A "vault:" blob always goes to Vault; anything
// else is a local-master-key blob.
func (m *Manager) UnwrapKey(wrapped []byte) ([]byte, error) {
	if bytes.HasPrefix(wrapped, vaultPrefix) {
		if m.vaultAddr == "" {
			return nil, errors.New("kms: object was wrapped by Vault Transit but Vault is not configured")
		}
		return m.vaultDecrypt(wrapped)
	}
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

// --- Vault Transit ----------------------------------------------------

func (m *Manager) vaultDo(pathSuffix string, in any) (map[string]any, error) {
	body, _ := json.Marshal(in)
	req, _ := http.NewRequest(http.MethodPost, m.vaultAddr+"/v1/transit/"+pathSuffix, bytes.NewReader(body))
	req.Header.Set("X-Vault-Token", m.vaultToken)
	req.Header.Set("Content-Type", "application/json")
	resp, err := m.hc.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	rb, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<16))
	if resp.StatusCode/100 != 2 {
		return nil, fmt.Errorf("vault %s: %s: %s", pathSuffix, resp.Status, strings.TrimSpace(string(rb)))
	}
	var out struct {
		Data map[string]any `json:"data"`
	}
	if err := json.Unmarshal(rb, &out); err != nil {
		return nil, err
	}
	return out.Data, nil
}

func (m *Manager) vaultEncrypt(dek []byte) ([]byte, error) {
	d, err := m.vaultDo("encrypt/"+m.vaultKey, map[string]string{
		"plaintext": base64.StdEncoding.EncodeToString(dek),
	})
	if err != nil {
		return nil, err
	}
	ct, _ := d["ciphertext"].(string)
	if !strings.HasPrefix(ct, "vault:") {
		return nil, errors.New("kms: unexpected Vault ciphertext")
	}
	return []byte(ct), nil
}

func (m *Manager) vaultDecrypt(wrapped []byte) ([]byte, error) {
	d, err := m.vaultDo("decrypt/"+m.vaultKey, map[string]string{"ciphertext": string(wrapped)})
	if err != nil {
		return nil, err
	}
	pt, _ := d["plaintext"].(string)
	return base64.StdEncoding.DecodeString(pt)
}

// vaultHealth confirms the transit key is reachable (creating it if the token
// is allowed to), so a misconfiguration fails at startup, not first PUT.
func (m *Manager) vaultHealth() error {
	req, _ := http.NewRequest(http.MethodGet, m.vaultAddr+"/v1/transit/keys/"+m.vaultKey, nil)
	req.Header.Set("X-Vault-Token", m.vaultToken)
	resp, err := m.hc.Do(req)
	if err != nil {
		return err
	}
	_ = resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		return nil
	}
	if resp.StatusCode == http.StatusNotFound {
		// Try to create the key.
		cr, _ := http.NewRequest(http.MethodPost, m.vaultAddr+"/v1/transit/keys/"+m.vaultKey, bytes.NewReader([]byte(`{"type":"aes256-gcm96"}`)))
		cr.Header.Set("X-Vault-Token", m.vaultToken)
		cr.Header.Set("Content-Type", "application/json")
		cresp, cerr := m.hc.Do(cr)
		if cerr != nil {
			return cerr
		}
		_ = cresp.Body.Close()
		if cresp.StatusCode/100 == 2 {
			return nil
		}
		return fmt.Errorf("transit key %q missing and could not be created (%s)", m.vaultKey, cresp.Status)
	}
	return fmt.Errorf("checking transit key: %s", resp.Status)
}

func (m *Manager) gcm() (cipher.AEAD, error) {
	b, err := aes.NewCipher(m.master[:])
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(b)
}
