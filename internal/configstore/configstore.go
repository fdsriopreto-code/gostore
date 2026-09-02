// Package configstore is the seam between gostore's cluster-wide
// configuration (IAM users/policies, per-bucket config) and where those
// bytes actually live.
//
// MinIO keeps its IAM and bucket metadata as *objects* in a reserved system
// bucket, so config is erasure-coded and shared by every node for free.
// gostore does the same: a Backend persists opaque blobs under the reserved
// ".gostore.sys/" path of the object backend — replicated to every disk in
// every erasure set (quorum read, majority wins) so a user created on one
// node is visible on all of them.
//
// Keys are slash-separated relative paths, e.g. "iam/store.json". The
// backend maps a key onto its own layout; the legacy single-disk path
// (<root>/.gostore.sys/iam/store.json) is preserved byte-for-byte so an
// in-place upgrade needs no migration.
package configstore

import (
	"context"
	"errors"
)

// ErrNotFound is returned by ReadConfig when the key has never been written.
var ErrNotFound = errors.New("configstore: key not found")

// Backend persists configuration blobs. Implemented by the fs and erasure
// object backends.
type Backend interface {
	// ReadConfig returns the bytes stored at key, or ErrNotFound.
	ReadConfig(ctx context.Context, key string) ([]byte, error)
	// WriteConfig stores data at key atomically and durably (quorum of disks
	// in distributed mode).
	WriteConfig(ctx context.Context, key string, data []byte) error
	// DeleteConfig removes key. Missing key is not an error.
	DeleteConfig(ctx context.Context, key string) error
	// ListConfig returns the keys present under prefix (non-recursive is fine
	// for gostore's needs; recursive is acceptable too).
	ListConfig(ctx context.Context, prefix string) ([]string, error)
}
