package fs

import (
	"context"
	"os"
	"time"

	"github.com/lojadopocket/gostore/internal/object"
)

func (f *FS) PutObjectTags(_ context.Context, bucket, obj, tags string, _ object.ObjectOptions) (object.ObjectInfo, error) {
	if err := f.ensureBucket(bucket); err != nil {
		return object.ObjectInfo{}, err
	}
	lk := f.NewNSLock(bucket, obj)
	c, _ := lk.GetLock(context.Background(), 0)
	defer lk.Unlock(c)

	if _, err := os.Stat(f.objDataPath(bucket, obj)); err != nil {
		return object.ObjectInfo{}, object.ObjectNotFound{Bucket: bucket, Object: obj}
	}
	m, err := readMetaFile(f.objMetaPath(bucket, obj))
	if err != nil {
		if os.IsNotExist(err) {
			st, _ := os.Stat(f.objDataPath(bucket, obj))
			m = objMeta{Size: st.Size(), ModTime: st.ModTime().UTC()}
		} else {
			return object.ObjectInfo{}, err
		}
	}
	m.UserTags = tags
	m.ModTime = time.Now().UTC()
	if err := writeMetaFile(f.objMetaPath(bucket, obj), m); err != nil {
		return object.ObjectInfo{}, err
	}
	return m.toObjectInfo(bucket, obj), nil
}

func (f *FS) GetObjectTags(_ context.Context, bucket, obj string, _ object.ObjectOptions) (string, error) {
	oi, err := f.getInfoUnlocked(bucket, obj)
	if err != nil {
		return "", err
	}
	return oi.UserTags, nil
}

func (f *FS) DeleteObjectTags(ctx context.Context, bucket, obj string, opts object.ObjectOptions) error {
	_, err := f.PutObjectTags(ctx, bucket, obj, "", opts)
	return err
}
