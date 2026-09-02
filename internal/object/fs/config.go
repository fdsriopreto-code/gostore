package fs

import (
	"context"
	"errors"
	"os"
	"path/filepath"

	"github.com/lojadopocket/gostore/internal/configstore"
)

// ReadConfig / WriteConfig / DeleteConfig / ListConfig implement
// configstore.Backend. On a single disk this is just a file under
// <root>/.gostore.sys/<key> — the same path the legacy per-volume IAM and
// bucket-config stores used, so an in-place upgrade sees its data unchanged.

var _ configstore.Backend = (*FS)(nil)

func (f *FS) configPath(key string) string {
	return filepath.Join(f.root, sysDir, filepath.FromSlash(key))
}

// ReadConfig returns the bytes stored at key, or configstore.ErrNotFound.
func (f *FS) ReadConfig(_ context.Context, key string) ([]byte, error) {
	b, err := os.ReadFile(f.configPath(key))
	if errors.Is(err, os.ErrNotExist) {
		return nil, configstore.ErrNotFound
	}
	return b, err
}

// WriteConfig stores data at key atomically (tmp + fsync + rename).
func (f *FS) WriteConfig(_ context.Context, key string, data []byte) error {
	p := f.configPath(key)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return err
	}
	return writeFileAtomic(p, data, 0o600)
}

// DeleteConfig removes key; a missing key is not an error.
func (f *FS) DeleteConfig(_ context.Context, key string) error {
	err := os.Remove(f.configPath(key))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

// ListConfig returns the keys present under prefix (recursive).
func (f *FS) ListConfig(_ context.Context, prefix string) ([]string, error) {
	root := f.configPath(prefix)
	var out []string
	base := filepath.Join(f.root, sysDir)
	err := filepath.WalkDir(root, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil
			}
			return err
		}
		if d.IsDir() {
			return nil
		}
		rel, rerr := filepath.Rel(base, p)
		if rerr != nil {
			return nil
		}
		out = append(out, filepath.ToSlash(rel))
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}
