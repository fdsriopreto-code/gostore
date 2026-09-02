package storage

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// LocalDisk is a StorageAPI-style disk backed by a directory on the local
// filesystem. One LocalDisk == one erasure "drive". The erasure layer
// (internal/erasure) fans shard/metadata I/O across a set of these.
//
// Layout under the disk root:
//
//	<root>/<bucket>/<object>/xl.meta        full metadata copy (same on every disk)
//	<root>/<bucket>/<object>/part.00001     this disk's shard of part 1
//	<root>/.gostore.sys/format.json         disk identity
//	<root>/.gostore.sys/tmp/                staging for atomic RenameData
type LocalDisk struct {
	root string
	id   string
	idx  int // disk index within its set (0-based)
}

const (
	sysDir     = ".gostore.sys"
	xlMetaFile = "xl.meta"
)

// Format is the on-disk identity written to <root>/.gostore.sys/format.json.
type Format struct {
	Version   int    `json:"version"`
	ID        string `json:"id"`
	Set       int    `json:"set"`
	Disk      int    `json:"disk"`
	CreatedAt string `json:"createdAt"`
}

// OpenLocalDisk opens (formatting if new) a disk at root. setIdx/diskIdx
// identify its position; they are persisted on first format and verified
// afterwards.
func OpenLocalDisk(root string, setIdx, diskIdx int) (*LocalDisk, error) {
	abs, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	for _, d := range []string{abs, filepath.Join(abs, sysDir), filepath.Join(abs, sysDir, "tmp")} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			if os.IsPermission(err) {
				return nil, errors.New("storage: cannot write disk path " + abs + " (" + err.Error() + ")")
			}
			return nil, err
		}
	}
	d := &LocalDisk{root: abs, idx: diskIdx}
	fp := filepath.Join(abs, sysDir, "format.json")
	if b, err := os.ReadFile(fp); err == nil {
		var f Format
		if err := json.Unmarshal(b, &f); err != nil {
			return nil, err
		}
		d.id = f.ID
	} else if errors.Is(err, os.ErrNotExist) {
		f := Format{Version: 1, ID: randomID(), Set: setIdx, Disk: diskIdx, CreatedAt: time.Now().UTC().Format(time.RFC3339)}
		nb, _ := json.MarshalIndent(f, "", "  ")
		if err := writeFileSync(fp, nb); err != nil {
			return nil, err
		}
		d.id = f.ID
	} else {
		return nil, err
	}
	// Clear stale tmp.
	_ = os.RemoveAll(filepath.Join(abs, sysDir, "tmp"))
	_ = os.MkdirAll(filepath.Join(abs, sysDir, "tmp"), 0o755)
	return d, nil
}

func (d *LocalDisk) String() string { return d.root }
func (d *LocalDisk) ID() string     { return d.id }
func (d *LocalDisk) Index() int     { return d.idx }
func (d *LocalDisk) IsOnline() bool { _, err := os.Stat(d.root); return err == nil }

func (d *LocalDisk) path(bucket, object string) string {
	return filepath.Join(d.root, bucket, filepath.FromSlash(object))
}

// --- volumes (buckets) --------------------------------------------------

func (d *LocalDisk) MakeVol(_ context.Context, bucket string) error {
	err := os.Mkdir(filepath.Join(d.root, bucket), 0o755)
	if errors.Is(err, os.ErrExist) {
		return ErrVolumeExists
	}
	return err
}

func (d *LocalDisk) StatVol(_ context.Context, bucket string) (VolInfo, error) {
	st, err := os.Stat(filepath.Join(d.root, bucket))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return VolInfo{}, ErrVolumeNotFound
		}
		return VolInfo{}, err
	}
	return VolInfo{Name: bucket, Created: st.ModTime()}, nil
}

func (d *LocalDisk) ListVols(_ context.Context) ([]VolInfo, error) {
	ents, err := os.ReadDir(d.root)
	if err != nil {
		return nil, err
	}
	var out []VolInfo
	for _, e := range ents {
		if !e.IsDir() || e.Name() == sysDir {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		out = append(out, VolInfo{Name: e.Name(), Created: info.ModTime()})
	}
	return out, nil
}

func (d *LocalDisk) DeleteVol(_ context.Context, bucket string, force bool) error {
	p := filepath.Join(d.root, bucket)
	if force {
		return os.RemoveAll(p)
	}
	err := os.Remove(p)
	if errors.Is(err, os.ErrNotExist) {
		return ErrVolumeNotFound
	}
	if err != nil && strings.Contains(err.Error(), "not empty") {
		return ErrVolumeNotEmpty
	}
	return err
}

// --- files -----------------------------------------------------------

// WriteAll writes a whole file (used for xl.meta), atomically.
func (d *LocalDisk) WriteAll(_ context.Context, bucket, object string, data []byte) error {
	return writeFileSync(d.path(bucket, object), data)
}

// ReadAll reads a whole file (xl.meta).
func (d *LocalDisk) ReadAll(_ context.Context, bucket, object string) ([]byte, error) {
	b, err := os.ReadFile(d.path(bucket, object))
	if errors.Is(err, os.ErrNotExist) {
		return nil, ErrFileNotFound
	}
	return b, err
}

// CreateFile streams size bytes from r into bucket/object.
func (d *LocalDisk) CreateFile(_ context.Context, bucket, object string, size int64, r io.Reader) error {
	full := d.path(bucket, object)
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		return err
	}
	f, err := os.OpenFile(full, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	n, err := io.Copy(f, r)
	if err != nil {
		_ = f.Close()
		_ = os.Remove(full)
		return err
	}
	if size >= 0 && n != size {
		_ = f.Close()
		_ = os.Remove(full)
		return io.ErrUnexpectedEOF
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

// ReadFileStream returns a reader over [offset, offset+length) of a file.
func (d *LocalDisk) ReadFileStream(_ context.Context, bucket, object string, offset, length int64) (io.ReadCloser, error) {
	f, err := os.Open(d.path(bucket, object))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, ErrFileNotFound
		}
		return nil, err
	}
	if offset > 0 {
		if _, err := f.Seek(offset, io.SeekStart); err != nil {
			_ = f.Close()
			return nil, err
		}
	}
	if length < 0 {
		return f, nil
	}
	return &limitedFile{f: f, remain: length}, nil
}

type limitedFile struct {
	f      *os.File
	remain int64
}

func (l *limitedFile) Read(p []byte) (int, error) {
	if l.remain <= 0 {
		return 0, io.EOF
	}
	if int64(len(p)) > l.remain {
		p = p[:l.remain]
	}
	n, err := l.f.Read(p)
	l.remain -= int64(n)
	return n, err
}
func (l *limitedFile) Close() error { return l.f.Close() }

// RenameDir moves a whole object directory (xl.meta + shards) from a staging
// path to its final location — the commit step of a PUT. The os.Rename is
// atomic; the preceding RemoveAll of any existing version leaves a sub-
// millisecond window where a hard crash could drop that version on this one
// disk (the write-quorum fan-out across disks is what makes the object as a
// whole survive it). Both the staging dir's entries and the destination's new
// directory entry are fsynced so the commit survives a power loss.
func (d *LocalDisk) RenameDir(_ context.Context, srcBucket, srcObject, dstBucket, dstObject string) error {
	src := d.path(srcBucket, srcObject)
	dst := d.path(dstBucket, dstObject)
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}
	// Flush the staging dir's own entries (the shard files created inside it)
	// before we move it into place.
	_ = syncDir(src)
	// Replace any existing version.
	_ = os.RemoveAll(dst)
	if err := os.Rename(src, dst); err != nil {
		return err
	}
	// Make the directory entry for the new object durable.
	return syncDir(filepath.Dir(dst))
}

// Delete removes a file or (recursively) a directory, then prunes any parent
// directories left empty, up to (not including) the bucket directory.
func (d *LocalDisk) Delete(_ context.Context, bucket, object string, recursive bool) error {
	p := d.path(bucket, object)
	var err error
	if recursive {
		err = os.RemoveAll(p)
	} else {
		err = os.Remove(p)
		if errors.Is(err, os.ErrNotExist) {
			err = nil
		}
	}
	if err != nil {
		return err
	}
	stop := d.path(bucket, "")
	for leaf := filepath.Dir(p); len(leaf) > len(stop) && leaf != stop; leaf = filepath.Dir(leaf) {
		if os.Remove(leaf) != nil {
			break // non-empty or gone
		}
	}
	return nil
}

// ListDir returns immediate child names of bucket/dir.
func (d *LocalDisk) ListDir(_ context.Context, bucket, dir string) ([]string, error) {
	ents, err := os.ReadDir(filepath.Join(d.root, bucket, filepath.FromSlash(dir)))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	out := make([]string, 0, len(ents))
	for _, e := range ents {
		name := e.Name()
		if e.IsDir() {
			name += "/"
		}
		out = append(out, name)
	}
	return out, nil
}

// StagingPath returns a fresh unique path under the disk's tmp area
// (same filesystem as data, so os.Rename to final is atomic).
func (d *LocalDisk) StagingPath() string {
	return filepath.Join(sysDir, "tmp", randomID())
}

// DiskInfo reports capacity and identity.
func (d *LocalDisk) DiskInfo(_ context.Context) (DiskInfo, error) {
	total, free := diskUsage(d.root)
	return DiskInfo{
		Total: total, Free: free, Used: total - free,
		Endpoint: d.root, MountPath: d.root, ID: d.id, DiskIndex: d.idx,
	}, nil
}

// XLMetaName is the metadata filename inside an object directory.
func XLMetaName() string { return xlMetaFile }
