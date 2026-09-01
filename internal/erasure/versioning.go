package erasure

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/lojadopocket/gostore/internal/object"
	"github.com/lojadopocket/gostore/internal/storage"
)

// Versioning layout, per disk, INSIDE the bucket dir (so a RenameDir of the
// live object never drags it along, and walkKeys already skips it):
//
//	<bucket>/<key>/xl.meta + part.*                 the current version
//	<bucket>/.gostore.sys/vlog/<key>/vlog.json      ordered version index (replicated)
//	<bucket>/.gostore.sys/vs/<key>/<versionID>/…    an archived (non-current) version
//
// A bucket is "versioned" per request via opts.Versioned / opts.VersionSuspended.

const nullVersionID = "null"

type vlogEntry struct {
	ID           string    `json:"id"`
	DeleteMarker bool      `json:"dm,omitempty"`
	ModTime      time.Time `json:"modTime"`

	LockMode    string    `json:"lockMode,omitempty"`
	RetainUntil time.Time `json:"retainUntil,omitempty"`
	LegalHold   bool      `json:"legalHold,omitempty"`
}

func (e vlogEntry) lockBlocks(bypassGovernance bool) bool {
	if e.LegalHold {
		return true
	}
	if e.RetainUntil.IsZero() || !time.Now().Before(e.RetainUntil) {
		return false
	}
	switch e.LockMode {
	case "COMPLIANCE":
		return true
	case "GOVERNANCE":
		return !bypassGovernance
	}
	return false
}

func newVersionID() string {
	var b [12]byte
	binary.BigEndian.PutUint64(b[:8], uint64(time.Now().UnixNano()))
	_, _ = rand.Read(b[8:])
	return hex.EncodeToString(b[:])
}

func vlogPath(key string) string { return path.Join(".gostore.sys", "vlog", key, "vlog.json") }
func vsDir(key, vid string) string {
	return path.Join(".gostore.sys", "vs", key, vid)
}

func (s *Set) readVlog(ctx context.Context, bucket, key string) ([]vlogEntry, error) {
	for _, d := range s.disks {
		b, err := d.ReadAll(ctx, bucket, vlogPath(key))
		if err != nil {
			continue
		}
		var v []vlogEntry
		if json.Unmarshal(b, &v) == nil {
			return v, nil
		}
	}
	return nil, nil
}

func (s *Set) writeVlog(ctx context.Context, bucket, key string, v []vlogEntry) error {
	b, _ := json.Marshal(v)
	errs := s.forEachDisk(func(d Disk) error { return d.WriteAll(ctx, bucket, vlogPath(key), b) })
	if okCount(errs) < s.writeQuorum() {
		return ErrWriteQuorum
	}
	return nil
}

// archiveCurrent moves the live object dir into the version store under its
// own version id, and returns that id (or "" if there was no live object).
func (s *Set) archiveCurrent(ctx context.Context, bucket, key string) (string, error) {
	m, err := s.readMeta(ctx, bucket, key)
	if err != nil {
		if errors.Is(err, storage.ErrFileNotFound) {
			return "", nil
		}
		return "", err
	}
	vid := m.VersionID
	if vid == "" {
		vid = nullVersionID
	}
	dst := vsDir(key, vid)
	errs := s.forEachDisk(func(d Disk) error { return d.RenameDir(ctx, bucket, key, bucket, dst) })
	if okCount(errs) < s.writeQuorum() {
		return "", ErrWriteQuorum
	}
	return vid, nil
}

// putObjectVersioned writes a new version and makes it current.
func (p *Pool) putObjectVersioned(ctx context.Context, bucket, key string, data *object.PutObjReader, opts object.ObjectOptions) (object.ObjectInfo, error) {
	if err := p.ensureBucket(ctx, bucket); err != nil {
		return object.ObjectInfo{}, err
	}
	if !validObjectName(key) {
		return object.ObjectInfo{}, object.ErrObjectNameInvalid
	}
	set := p.setFor(key)
	lk := p.NewNSLock(bucket, key)
	c, _ := lk.GetLock(ctx, 0)
	defer lk.Unlock(c)

	vlog, err := set.readVlog(ctx, bucket, key)
	if err != nil {
		return object.ObjectInfo{}, err
	}

	newVID := newVersionID()
	if opts.VersionSuspended {
		newVID = nullVersionID
		// drop an existing archived "null" and any tail "null" vlog entry
		_ = set.forEachDisk(func(d Disk) error { return d.Delete(ctx, bucket, vsDir(key, nullVersionID), true) })
		vlog = dropEntry(vlog, nullVersionID)
	} else {
		// archive whatever is currently live
		if _, err := set.archiveCurrent(ctx, bucket, key); err != nil {
			return object.ObjectInfo{}, err
		}
	}

	um := toUserMeta(opts)
	var sp *sseParams
	if p.kms != nil {
		if v := opts.UserDefined["x-amz-server-side-encryption"]; v == "AES256" {
			if sp, err = p.newSSEParams(); err != nil {
				return object.ObjectInfo{}, err
			}
		}
	}
	meta, err := set.putObjectSSE(ctx, bucket, key, []partSource{{Number: 1, Size: data.Size(), Reader: data}}, um, sp)
	if err != nil {
		return object.ObjectInfo{}, mapErr(err)
	}
	meta.VersionID = newVID
	meta.LockMode = opts.LockMode
	meta.RetainUntil = opts.LockRetainUntil
	meta.LegalHold = opts.LockLegalHold == "ON"
	if !opts.MTime.IsZero() {
		meta.ModTime = opts.MTime.UTC()
	}
	// rewrite xl.meta with the versioning fields
	mb, _ := meta.marshal()
	_ = set.forEachDisk(func(d Disk) error { return d.WriteAll(ctx, bucket, path.Join(key, metaFile), mb) })

	vlog = append(vlog, vlogEntry{
		ID: newVID, ModTime: meta.ModTime,
		LockMode: meta.LockMode, RetainUntil: meta.RetainUntil, LegalHold: meta.LegalHold,
	})
	if err := set.writeVlog(ctx, bucket, key, vlog); err != nil {
		return object.ObjectInfo{}, err
	}
	oi := metaToInfo(bucket, key, meta)
	oi.VersionID = newVID
	oi.IsLatest = true
	return oi, nil
}

func dropEntry(v []vlogEntry, id string) []vlogEntry {
	out := v[:0]
	for _, e := range v {
		if e.ID != id {
			out = append(out, e)
		}
	}
	return out
}

// resolveVersionDir returns the object dir for a version id ("" = current).
func (s *Set) resolveVersionDir(ctx context.Context, bucket, key, versionID string) (dir string, isCurrent bool, err error) {
	if versionID == "" {
		if _, e := s.readMeta(ctx, bucket, key); e == nil {
			return key, true, nil
		}
		return "", false, object.ObjectNotFound{Bucket: bucket, Object: key}
	}
	// current?
	if m, e := s.readMeta(ctx, bucket, key); e == nil && (m.VersionID == versionID || (m.VersionID == "" && versionID == nullVersionID)) {
		return key, true, nil
	}
	vd := vsDir(key, versionID)
	if _, e := s.readMeta(ctx, bucket, vd); e == nil {
		return vd, false, nil
	}
	return "", false, object.ObjectNotFound{Bucket: bucket, Object: key}
}

func (p *Pool) getVersioned(ctx context.Context, bucket, key string, rs *object.HTTPRangeSpec, opts object.ObjectOptions) (*object.GetObjectReader, error) {
	set := p.setFor(key)
	dir, _, err := set.resolveVersionDir(ctx, bucket, key, opts.VersionID)
	if err != nil {
		return nil, err
	}
	m, err := set.readMeta(ctx, bucket, dir)
	if err != nil {
		return nil, mapErr(err)
	}
	oi := metaToInfo(bucket, key, m)
	oi.VersionID = m.VersionID
	if oi.VersionID == "" {
		oi.VersionID = nullVersionID
	}
	if opts.CheckPrecondFn != nil && opts.CheckPrecondFn(oi) {
		return nil, object.ErrPreconditionFailed
	}

	logical := m.Size
	if m.SSE != "" {
		logical = m.PlainSize
	}
	var off, length int64 = 0, logical
	if rs != nil {
		o, l, rerr := resolveRange(rs, logical)
		if rerr != nil {
			return nil, rerr
		}
		off, length = o, l
		oi.Size = length
	}

	if m.SSE != "" {
		if p.kms == nil {
			return nil, object.ErrCorruptedData
		}
		cOff, cLen := cipherRange(off, length, m.Size)
		pr, pw := io.Pipe()
		go func() { _ = pw.CloseWithError(set.getObject(ctx, bucket, dir, cOff, cLen, pw)) }()
		dr, derr := p.decryptReader(m, pr, off, length)
		if derr != nil {
			_ = pr.CloseWithError(derr)
			return nil, derr
		}
		return &object.GetObjectReader{ObjInfo: oi, ReadCloser: readCloser{dr, pr}}, nil
	}
	pr, pw := io.Pipe()
	go func() { _ = pw.CloseWithError(set.getObject(ctx, bucket, dir, off, length, pw)) }()
	return &object.GetObjectReader{ObjInfo: oi, ReadCloser: pr}, nil
}

func (p *Pool) statVersioned(ctx context.Context, bucket, key, versionID string) (object.ObjectInfo, error) {
	set := p.setFor(key)
	dir, _, err := set.resolveVersionDir(ctx, bucket, key, versionID)
	if err != nil {
		return object.ObjectInfo{}, err
	}
	m, err := set.readMeta(ctx, bucket, dir)
	if err != nil {
		return object.ObjectInfo{}, mapErr(err)
	}
	oi := metaToInfo(bucket, key, m)
	oi.VersionID = m.VersionID
	if oi.VersionID == "" {
		oi.VersionID = nullVersionID
	}
	return oi, nil
}

func (p *Pool) deleteVersioned(ctx context.Context, bucket, key string, opts object.ObjectOptions) (object.ObjectInfo, error) {
	set := p.setFor(key)
	lk := p.NewNSLock(bucket, key)
	c, _ := lk.GetLock(ctx, 0)
	defer lk.Unlock(c)

	vlog, err := set.readVlog(ctx, bucket, key)
	if err != nil {
		return object.ObjectInfo{}, err
	}

	if opts.VersionID == "" {
		// add a delete marker; archive the live object first
		if !opts.VersionSuspended {
			if _, err := set.archiveCurrent(ctx, bucket, key); err != nil {
				return object.ObjectInfo{}, err
			}
		} else {
			_ = set.forEachDisk(func(d Disk) error { return d.Delete(ctx, bucket, key, true) })
			vlog = dropEntry(vlog, nullVersionID)
		}
		dmID := newVersionID()
		if opts.VersionSuspended {
			dmID = nullVersionID
		}
		vlog = append(vlog, vlogEntry{ID: dmID, DeleteMarker: true, ModTime: time.Now().UTC()})
		if err := set.writeVlog(ctx, bucket, key, vlog); err != nil {
			return object.ObjectInfo{}, err
		}
		return object.ObjectInfo{Bucket: bucket, Name: key, VersionID: dmID, DeleteMarker: true}, nil
	}

	// permanent delete of a specific version
	idx := -1
	for i := range vlog {
		if vlog[i].ID == opts.VersionID {
			idx = i
			break
		}
	}
	if idx < 0 {
		return object.ObjectInfo{}, object.ObjectNotFound{Bucket: bucket, Object: key}
	}
	if vlog[idx].lockBlocks(opts.BypassGovernance) {
		return object.ObjectInfo{}, object.ErrObjectLocked
	}

	wasLatest := idx == len(vlog)-1
	dir, isCurrent, _ := set.resolveVersionDir(ctx, bucket, key, opts.VersionID)
	if isCurrent {
		_ = set.forEachDisk(func(d Disk) error { return d.Delete(ctx, bucket, key, true) })
	} else if dir != "" {
		_ = set.forEachDisk(func(d Disk) error { return d.Delete(ctx, bucket, dir, true) })
	}

	vlog = append(vlog[:idx], vlog[idx+1:]...)
	if len(vlog) == 0 {
		_ = set.forEachDisk(func(d Disk) error { return d.Delete(ctx, bucket, path.Join(".gostore.sys", "vlog", key), true) })
		return object.ObjectInfo{Bucket: bucket, Name: key, VersionID: opts.VersionID}, nil
	}

	// If the removed entry was the newest one and the new head is a real
	// version with no live object, promote it back to current.
	if wasLatest && !vlog[len(vlog)-1].DeleteMarker {
		if _, e := set.readMeta(ctx, bucket, key); errors.Is(e, storage.ErrFileNotFound) {
			pv := vsDir(key, vlog[len(vlog)-1].ID)
			_ = set.forEachDisk(func(d Disk) error { return d.RenameDir(ctx, bucket, pv, bucket, key) })
		}
	}
	if err := set.writeVlog(ctx, bucket, key, vlog); err != nil {
		return object.ObjectInfo{}, err
	}
	return object.ObjectInfo{Bucket: bucket, Name: key, VersionID: opts.VersionID}, nil
}

// walkVlogKeys returns keys that have a version log (covers keys whose latest
// version is a delete marker and thus have no live object dir).
func (s *Set) walkVlogKeys(ctx context.Context, bucket string) ([]string, error) {
	var disk Disk
	for _, d := range s.disks {
		if d.IsOnline() {
			disk = d
			break
		}
	}
	if disk == nil {
		return nil, ErrReadQuorum
	}
	var keys []string
	var rec func(prefix string) error
	rec = func(prefix string) error {
		entries, err := disk.ListDir(ctx, bucket, path.Join(".gostore.sys", "vlog", prefix))
		if err != nil {
			return err
		}
		for _, e := range entries {
			if e == "vlog.json" {
				keys = append(keys, strings.TrimSuffix(prefix, "/"))
				return nil
			}
		}
		for _, e := range entries {
			if strings.HasSuffix(e, "/") {
				if err := rec(path.Join(prefix, e) + "/"); err != nil {
					return err
				}
			}
		}
		return nil
	}
	_ = rec("")
	sort.Strings(keys)
	return keys, nil
}

func (p *Pool) listVersions(ctx context.Context, bucket, prefix, delimiter string, maxKeys int, currentOnly bool) (object.ListObjectVersionsInfo, error) {
	if err := p.ensureBucket(ctx, bucket); err != nil {
		return object.ListObjectVersionsInfo{}, err
	}
	if maxKeys <= 0 || maxKeys > 1000 {
		maxKeys = 1000
	}
	set := p.sets[0]
	live, _ := set.walkKeys(ctx, bucket)
	vk, _ := set.walkVlogKeys(ctx, bucket)
	seen := map[string]bool{}
	var keys []string
	for _, k := range append(live, vk...) {
		if !seen[k] {
			seen[k] = true
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)

	var out object.ListObjectVersionsInfo
	prefixes := map[string]bool{}
	count := 0
	for _, k := range keys {
		if prefix != "" && !strings.HasPrefix(k, prefix) {
			continue
		}
		if delimiter != "" {
			rest := k[len(prefix):]
			if i := strings.Index(rest, delimiter); i >= 0 {
				cp := prefix + rest[:i+len(delimiter)]
				if !prefixes[cp] {
					prefixes[cp] = true
					out.Prefixes = append(out.Prefixes, cp)
				}
				continue
			}
		}
		s := p.setFor(k)
		vlog, _ := s.readVlog(ctx, bucket, k)
		if len(vlog) == 0 {
			if m, e := s.readMeta(ctx, bucket, k); e == nil {
				oi := metaToInfo(bucket, k, m)
				oi.VersionID = nullVersionID
				oi.IsLatest = true
				out.Objects = append(out.Objects, oi)
				count++
			}
			continue
		}
		if currentOnly {
			last := vlog[len(vlog)-1]
			if last.DeleteMarker {
				continue
			}
			if m, e := s.readMeta(ctx, bucket, k); e == nil {
				out.Objects = append(out.Objects, metaToInfo(bucket, k, m))
				count++
				if count >= maxKeys {
					out.IsTruncated = true
					break
				}
			}
			continue
		}
		for i := len(vlog) - 1; i >= 0; i-- {
			e := vlog[i]
			oi := object.ObjectInfo{
				Bucket: bucket, Name: k, VersionID: e.ID, ModTime: e.ModTime,
				IsLatest: i == len(vlog)-1, DeleteMarker: e.DeleteMarker, StorageClass: "STANDARD",
			}
			if !e.DeleteMarker {
				dir := vsDir(k, e.ID)
				if i == len(vlog)-1 {
					dir = k
				}
				if m, merr := s.readMeta(ctx, bucket, dir); merr == nil {
					oi.Size = m.Size
					if m.SSE != "" {
						oi.Size = m.PlainSize
					}
					oi.ETag = m.ETag
				}
			}
			out.Objects = append(out.Objects, oi)
			count++
			if count >= maxKeys {
				out.IsTruncated = true
				break
			}
		}
		if out.IsTruncated {
			break
		}
	}
	sort.Strings(out.Prefixes)
	return out, nil
}

// bucketHasVersions reports whether the bucket has any version logs.
func (p *Pool) bucketHasVersions(ctx context.Context, bucket string) bool {
	vk, _ := p.sets[0].walkVlogKeys(ctx, bucket)
	return len(vk) > 0
}

var _ = http.Header(nil)
