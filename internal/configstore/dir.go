package configstore

import (
	"context"
	"errors"
	"os"
	"path/filepath"
)

// Dir is a plain single-directory Backend: key -> <root>/<key>. It is used by
// tests and could back a trivial single-node deployment. Production single-
// disk and erasure backends implement Backend directly on the object layer.
type Dir struct{ root string }

// NewDir returns a Backend rooted at dir.
func NewDir(dir string) *Dir { return &Dir{root: dir} }

var _ Backend = (*Dir)(nil)

func (d *Dir) file(key string) string {
	return filepath.Join(d.root, filepath.FromSlash(key))
}

func (d *Dir) ReadConfig(_ context.Context, key string) ([]byte, error) {
	b, err := os.ReadFile(d.file(key))
	if errors.Is(err, os.ErrNotExist) {
		return nil, ErrNotFound
	}
	return b, err
}

func (d *Dir) WriteConfig(_ context.Context, key string, data []byte) error {
	p := d.file(key)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return err
	}
	tmp := p + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, p)
}

func (d *Dir) DeleteConfig(_ context.Context, key string) error {
	err := os.Remove(d.file(key))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

func (d *Dir) ListConfig(_ context.Context, prefix string) ([]string, error) {
	var out []string
	base := d.root
	err := filepath.WalkDir(d.file(prefix), func(p string, de os.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil
			}
			return err
		}
		if de.IsDir() {
			return nil
		}
		if rel, rerr := filepath.Rel(base, p); rerr == nil {
			out = append(out, filepath.ToSlash(rel))
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}
