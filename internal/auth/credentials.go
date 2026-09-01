// Package auth implements AWS Signature Version 4 verification (header,
// presigned query, and STREAMING-AWS4-HMAC-SHA256-PAYLOAD chunked uploads)
// plus the credential store the S3 API authenticates against.
//
// M2 ships a single root credential. M8 replaces the store with the IAM
// subsystem (users, service accounts) behind the same Lookup interface.
package auth

import "sync"

// Credentials is the credential store.
type Credentials struct {
	mu   sync.RWMutex
	keys map[string]string // accessKey -> secretKey
	root string
}

// NewRoot builds a store containing just the root credential.
func NewRoot(accessKey, secretKey string) *Credentials {
	return &Credentials{
		keys: map[string]string{accessKey: secretKey},
		root: accessKey,
	}
}

// Lookup returns the secret key for an access key.
func (c *Credentials) Lookup(accessKey string) (secret string, ok bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	s, ok := c.keys[accessKey]
	return s, ok
}

// IsRoot reports whether accessKey is the root credential.
func (c *Credentials) IsRoot(accessKey string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return accessKey == c.root
}

// Add inserts or updates a credential (used by IAM later).
func (c *Credentials) Add(accessKey, secretKey string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.keys[accessKey] = secretKey
}

// Delete removes a credential (no-op for the root key).
func (c *Credentials) Delete(accessKey string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if accessKey != c.root {
		delete(c.keys, accessKey)
	}
}
