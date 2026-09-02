package storage

import (
	"crypto/rand"
	"encoding/hex"
	"os"
	"path/filepath"
	"sync/atomic"
)

// dirSync gates the post-rename directory fsync (see syncDir). On by default;
// GOSTORE_FSYNC=0 turns it off for throughput on storage that doesn't need it
// (battery-backed cache, or when losing the very last write on power-cut is
// acceptable).
var dirSync atomic.Bool

func init() { dirSync.Store(true) }

// SetDirSync enables or disables durable directory fsyncs process-wide.
func SetDirSync(on bool) { dirSync.Store(on) }

func dirSyncEnabled() bool { return dirSync.Load() }

func randomID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// NewID returns a random 32-hex-char identifier (staging dirs, upload ids).
func NewID() string { return randomID() }

// writeFileSync writes data to path atomically (tmp in same dir + fsync + rename).
func writeFileSync(path string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".tmp-*")
	if err != nil {
		return err
	}
	name := tmp.Name()
	defer os.Remove(name)
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(name, path); err != nil {
		return err
	}
	// Make the rename itself durable — the file bytes were fsynced above, but
	// the directory entry that points at them was not.
	return syncDir(filepath.Dir(path))
}
