package fs

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"sort"
	"time"

	"github.com/lojadopocket/gostore/internal/object"
)

type bucketMeta struct {
	Name       string    `json:"name"`
	Created    time.Time `json:"created"`
	Versioning bool      `json:"versioning,omitempty"`
	ObjectLock bool      `json:"objectLock,omitempty"`
}

func (f *FS) MakeBucket(_ context.Context, bucket string, opts object.MakeBucketOptions) error {
	if !validBucketName(bucket) {
		return object.ErrBucketNameInvalid
	}
	dir := f.bucketDir(bucket)
	if _, err := os.Stat(dir); err == nil {
		if opts.ForceCreate {
			return nil
		}
		return object.BucketExists{Bucket: bucket}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	bm := bucketMeta{
		Name: bucket, Created: time.Now().UTC(),
		Versioning: opts.VersioningEnabled, ObjectLock: opts.LockEnabled,
	}
	b, _ := json.Marshal(bm)
	return writeFileAtomic(f.bucketMetaPath(bucket), b, 0o644)
}

func (f *FS) readBucketMeta(bucket string) (bucketMeta, error) {
	b, err := os.ReadFile(f.bucketMetaPath(bucket))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			// Data dir may exist without a meta file (older layout / manual);
			// synthesize from the directory's mtime.
			if st, e2 := os.Stat(f.bucketDir(bucket)); e2 == nil && st.IsDir() {
				return bucketMeta{Name: bucket, Created: st.ModTime().UTC()}, nil
			}
			return bucketMeta{}, object.BucketNotFound{Bucket: bucket}
		}
		return bucketMeta{}, err
	}
	var bm bucketMeta
	if err := json.Unmarshal(b, &bm); err != nil {
		return bucketMeta{}, err
	}
	return bm, nil
}

func (f *FS) GetBucketInfo(_ context.Context, bucket string) (object.BucketInfo, error) {
	if !validBucketName(bucket) {
		return object.BucketInfo{}, object.ErrBucketNameInvalid
	}
	bm, err := f.readBucketMeta(bucket)
	if err != nil {
		return object.BucketInfo{}, err
	}
	return object.BucketInfo{
		Name: bm.Name, Created: bm.Created,
		Versioning: bm.Versioning, ObjectLocking: bm.ObjectLock,
	}, nil
}

func (f *FS) ListBuckets(_ context.Context) ([]object.BucketInfo, error) {
	ents, err := os.ReadDir(f.root)
	if err != nil {
		return nil, err
	}
	var out []object.BucketInfo
	for _, e := range ents {
		if !e.IsDir() || isReserved(e.Name()) {
			continue
		}
		bm, err := f.readBucketMeta(e.Name())
		if err != nil {
			continue
		}
		out = append(out, object.BucketInfo{
			Name: bm.Name, Created: bm.Created,
			Versioning: bm.Versioning, ObjectLocking: bm.ObjectLock,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

func (f *FS) DeleteBucket(_ context.Context, bucket string, opts object.DeleteBucketOptions) error {
	if !validBucketName(bucket) {
		return object.ErrBucketNameInvalid
	}
	dir := f.bucketDir(bucket)
	if _, err := os.Stat(dir); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return object.BucketNotFound{Bucket: bucket}
		}
		return err
	}
	if !opts.Force {
		ents, err := os.ReadDir(dir)
		if err != nil {
			return err
		}
		if len(ents) > 0 {
			return object.ErrBucketNotEmpty
		}
	}
	if err := os.RemoveAll(dir); err != nil {
		return err
	}
	_ = os.Remove(f.bucketMetaPath(bucket))
	_ = os.RemoveAll(f.metaBucketDir(bucket))
	_ = os.RemoveAll(f.mpBucketDir(bucket))
	return nil
}
