package kms

import (
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// fakeVault mimics just enough of Vault's Transit engine for the tests: it
// "encrypts" by base64-tagging the plaintext and "decrypts" by reversing it.
func fakeVault(t *testing.T) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/transit/keys/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"data":{"name":"gostore"}}`))
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/v1/transit/encrypt/", func(w http.ResponseWriter, r *http.Request) {
		var in struct{ Plaintext string }
		_ = json.NewDecoder(r.Body).Decode(&in)
		ct := "vault:v1:" + base64.StdEncoding.EncodeToString([]byte("WRAP("+in.Plaintext+")"))
		_, _ = io.WriteString(w, `{"data":{"ciphertext":"`+ct+`"}}`)
	})
	mux.HandleFunc("/v1/transit/decrypt/", func(w http.ResponseWriter, r *http.Request) {
		var in struct{ Ciphertext string }
		_ = json.NewDecoder(r.Body).Decode(&in)
		raw, _ := base64.StdEncoding.DecodeString(strings.TrimPrefix(in.Ciphertext, "vault:v1:"))
		pt := strings.TrimSuffix(strings.TrimPrefix(string(raw), "WRAP("), ")")
		_, _ = io.WriteString(w, `{"data":{"plaintext":"`+pt+`"}}`)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func TestVaultTransitWrapUnwrap(t *testing.T) {
	v := fakeVault(t)
	t.Setenv("GOSTORE_KMS_VAULT_ADDR", v.URL)
	t.Setenv("GOSTORE_KMS_VAULT_TOKEN", "test-token")

	m, err := New(nil) // no volume dir needed in Vault mode
	if err != nil {
		t.Fatal(err)
	}
	if m.Mode() != "vault-transit" {
		t.Fatalf("mode = %q, want vault-transit", m.Mode())
	}

	dek, _ := m.GenerateDataKey()
	wrapped, err := m.WrapKey(dek)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(string(wrapped), "vault:") {
		t.Fatalf("wrapped blob should be a vault ciphertext, got %q", wrapped)
	}
	got, err := m.UnwrapKey(wrapped)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(dek) {
		t.Fatal("unwrap did not return the original data key")
	}
}

func TestLocalAndVaultBlobsCoexist(t *testing.T) {
	// A local-mode manager wraps a key...
	t.Setenv("GOSTORE_KMS_MASTER_KEY", base64.StdEncoding.EncodeToString(make([]byte, 32)))
	local, err := New(nil)
	if err != nil {
		t.Fatal(err)
	}
	dek, _ := local.GenerateDataKey()
	localBlob, _ := local.WrapKey(dek)

	// ...and a Vault-mode manager must still be able to unwrap it (no "vault:"
	// prefix -> local path).
	v := fakeVault(t)
	t.Setenv("GOSTORE_KMS_VAULT_ADDR", v.URL)
	t.Setenv("GOSTORE_KMS_VAULT_TOKEN", "test-token")
	vault, err := New(nil)
	if err != nil {
		t.Fatal(err)
	}
	// vault manager has a zero master key (same as the local one here), so the
	// local-blob unwrap succeeds.
	if got, err := vault.UnwrapKey(localBlob); err != nil || string(got) != string(dek) {
		t.Fatalf("vault-mode manager could not unwrap a legacy local blob: %v", err)
	}
}
