// Package fs implements object.Layer on top of a single local filesystem
// directory. It is the M1 backend: no erasure coding, no distribution, but a
// complete and correct S3 object model (buckets, objects, multipart, ranges,
// conditional requests, server-side copy, listing with prefix/delimiter/
// pagination).
//
// On-disk layout under the volume root:
//
//	<root>/<bucket>/<key>                       object data (mirrors the key path)
//	<root>/.gostore.sys/format.json             disk identity + format version
//	<root>/.gostore.sys/buckets/<bucket>.json   per-bucket metadata
//	<root>/.gostore.sys/meta/<bucket>/<key>.json  per-object metadata sidecar
//	<root>/.gostore.sys/multipart/<bucket>/<uploadID>/  multipart staging
//	<root>/.gostore.sys/tmp/                     staging for atomic writes
package fs

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/lojadopocket/gostore/internal/object"
)

const (
	sysDir        = ".gostore.sys"
	formatFile    = "format.json"
	formatVersion = 1
	minPartSize   = 5 * 1024 * 1024 // 5 MiB, matches S3's minimum non-final part
)

// FS is a single-disk object.Layer.
type FS struct {
	root string

	nsMu   sync.Mutex
	nsLock map[string]*sync.RWMutex

	format diskFormat
	kms    kmsWrapper
}

// kmsWrapper is the subset of *kms.Manager the fs backend needs (kept as an
// interface so tests don't need a real KMS).
type kmsWrapper interface {
	GenerateDataKey() ([]byte, error)
	WrapKey(dek []byte) ([]byte, error)
	UnwrapKey(wrapped []byte) ([]byte, error)
}

// SetKMS enables SSE-S3 at-rest encryption using the given key manager.
func (f *FS) SetKMS(k kmsWrapper) { f.kms = k }

type diskFormat struct {
	Version int    `json:"version"`
	ID      string `json:"id"`
	Created string `json:"created"`
}

var _ object.Layer = (*FS)(nil)

// New opens (formatting if needed) the volume at root and returns an FS.
func New(root string) (*FS, error) {
	abs, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	f := &FS{root: abs, nsLock: map[string]*sync.RWMutex{}}
	for _, d := range []string{
		abs,
		filepath.Join(abs, sysDir),
		filepath.Join(abs, sysDir, "buckets"),
		filepath.Join(abs, sysDir, "meta"),
		filepath.Join(abs, sysDir, "multipart"),
		filepath.Join(abs, sysDir, "tmp"),
	} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			if os.IsPermission(err) {
				return nil, fmt.Errorf("fs: cannot write under volume %q (%v) — "+
					"fix the directory ownership: `chown -R $(id -u):$(id -g) %s`, "+
					"or for the Docker image `chown -R 65532:65532`", abs, err, abs)
			}
			return nil, fmt.Errorf("fs: preparing volume layout: %w", err)
		}
	}
	if err := f.loadOrInitFormat(); err != nil {
		return nil, err
	}
	// Clear stale tmp files from a previous crash.
	_ = os.RemoveAll(filepath.Join(abs, sysDir, "tmp"))
	_ = os.MkdirAll(filepath.Join(abs, sysDir, "tmp"), 0o755)
	return f, nil
}

func (f *FS) loadOrInitFormat() error {
	p := filepath.Join(f.root, sysDir, formatFile)
	b, err := os.ReadFile(p)
	if err == nil {
		return json.Unmarshal(b, &f.format)
	}
	if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	f.format = diskFormat{Version: formatVersion, ID: newID(), Created: time.Now().UTC().Format(time.RFC3339)}
	nb, _ := json.MarshalIndent(f.format, "", "  ")
	return writeFileAtomic(p, nb, 0o644)
}

// --- path helpers ---------------------------------------------------------

func (f *FS) bucketDir(bucket string) string { return filepath.Join(f.root, bucket) }

func (f *FS) objDataPath(bucket, object string) string {
	return filepath.Join(f.root, bucket, filepath.FromSlash(object))
}

func (f *FS) objMetaPath(bucket, object string) string {
	return filepath.Join(f.root, sysDir, "meta", bucket, filepath.FromSlash(object)+".json")
}

func (f *FS) metaBucketDir(bucket string) string {
	return filepath.Join(f.root, sysDir, "meta", bucket)
}

func (f *FS) bucketMetaPath(bucket string) string {
	return filepath.Join(f.root, sysDir, "buckets", bucket+".json")
}

func (f *FS) mpBucketDir(bucket string) string {
	return filepath.Join(f.root, sysDir, "multipart", bucket)
}

func (f *FS) mpDir(bucket, uploadID string) string {
	return filepath.Join(f.root, sysDir, "multipart", bucket, uploadID)
}

func (f *FS) tmpPath() string {
	return filepath.Join(f.root, sysDir, "tmp", newID())
}

// --- namespace lock ----------------------------------------------------

func (f *FS) NewNSLock(bucket string, objects ...string) object.RWLocker {
	key := bucket
	if len(objects) > 0 {
		key = bucket + "/" + objects[0]
	}
	f.nsMu.Lock()
	mu, ok := f.nsLock[key]
	if !ok {
		mu = &sync.RWMutex{}
		f.nsLock[key] = mu
	}
	f.nsMu.Unlock()
	return &nsLock{mu: mu}
}

type nsLock struct{ mu *sync.RWMutex }

func (l *nsLock) GetLock(ctx context.Context, _ time.Duration) (context.Context, error) {
	l.mu.Lock()
	return ctx, nil
}
func (l *nsLock) Unlock(context.Context) { l.mu.Unlock() }
func (l *nsLock) GetRLock(ctx context.Context, _ time.Duration) (context.Context, error) {
	l.mu.RLock()
	return ctx, nil
}
func (l *nsLock) RUnlock(context.Context) { l.mu.RUnlock() }

// --- lifecycle / introspection --------------------------------------

func (f *FS) Shutdown(context.Context) error { return nil }

func (f *FS) StorageInfo(context.Context) (object.StorageInfo, []error) {
	var si object.StorageInfo
	si.Backend.Type = "single"
	total, free := diskUsage(f.root)
	si.Disks = []object.DiskMetrics{{
		Endpoint: f.root, State: "ok", RootDisk: true,
		TotalSpace: total, FreeSpace: free, UsedSpace: total - free,
	}}
	return si, nil
}

func (f *FS) Health(context.Context, object.HealthOptions) object.HealthResult {
	// Writable check: create+remove a probe file in tmp.
	probe := f.tmpPath()
	if err := os.WriteFile(probe, []byte("ok"), 0o644); err != nil {
		return object.HealthResult{Healthy: false, Reason: "volume not writable: " + err.Error()}
	}
	_ = os.Remove(probe)
	return object.HealthResult{Healthy: true, WriteQuorum: 1}
}

// --- shared small helpers ------------------------------------------

func newID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// writeFileAtomic writes data to a temp file in the same directory then
// renames it into place, so readers never see a partial file.
func writeFileAtomic(path string, data []byte, perm os.FileMode) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer func() { _ = os.Remove(tmpName) }()
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
	if err := os.Chmod(tmpName, perm); err != nil {
		return err
	}
	return os.Rename(tmpName, path)
}

// validBucketName applies the S3 bucket naming rules (DNS-compatible subset).
func validBucketName(name string) bool {
	if len(name) < 3 || len(name) > 63 {
		return false
	}
	if strings.HasPrefix(name, "-") || strings.HasSuffix(name, "-") ||
		strings.HasPrefix(name, ".") || strings.HasSuffix(name, ".") {
		return false
	}
	if strings.Contains(name, "..") || strings.Contains(name, ".-") || strings.Contains(name, "-.") {
		return false
	}
	for _, r := range name {
		if !(r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '-' || r == '.') {
			return false
		}
	}
	// Reject IP-address-like names.
	if isIPLike(name) {
		return false
	}
	return name != sysDir
}

func isIPLike(s string) bool {
	parts := strings.Split(s, ".")
	if len(parts) != 4 {
		return false
	}
	for _, p := range parts {
		if p == "" || len(p) > 3 {
			return false
		}
		for _, r := range p {
			if r < '0' || r > '9' {
				return false
			}
		}
	}
	return true
}

// validObjectName rejects names that would escape the bucket directory or
// collide with filesystem semantics.
func validObjectName(name string) bool {
	if name == "" || len(name) > 1024 {
		return false
	}
	if strings.Contains(name, "\x00") {
		return false
	}
	// No absolute paths, no "." / ".." path elements, no backslashes.
	if strings.HasPrefix(name, "/") || strings.Contains(name, "\\") {
		return false
	}
	for _, seg := range strings.Split(name, "/") {
		if seg == "." || seg == ".." {
			return false
		}
	}
	return true
}

// isReserved reports whether a top-level name is the internal system dir.
func isReserved(name string) bool { return name == sysDir }

// copyToTmpAndHash streams r into a fresh tmp file, returning its path, the
// number of bytes written and the hex md5 of the content.
func (f *FS) copyToTmpAndHash(r io.Reader, expected int64) (tmpPath string, n int64, md5hex string, err error) {
	tmpPath = f.tmpPath()
	out, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return "", 0, "", err
	}
	h := newMD5()
	mw := io.MultiWriter(out, h)
	n, err = io.Copy(mw, r)
	if err != nil {
		_ = out.Close()
		_ = os.Remove(tmpPath)
		return "", 0, "", err
	}
	if err = out.Sync(); err != nil {
		_ = out.Close()
		_ = os.Remove(tmpPath)
		return "", 0, "", err
	}
	if err = out.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return "", 0, "", err
	}
	if expected >= 0 && n != expected {
		_ = os.Remove(tmpPath)
		return "", 0, "", object.ErrIncompleteBody
	}
	return tmpPath, n, hex.EncodeToString(h.Sum(nil)), nil
}
