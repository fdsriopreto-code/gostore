package erasure

import (
	"context"
	"path"
	"time"

	"github.com/lojadopocket/gostore/internal/object"
)

func (p *Pool) PutObjectTags(ctx context.Context, bucket, key, tags string, _ object.ObjectOptions) (object.ObjectInfo, error) {
	if err := p.ensureBucket(ctx, bucket); err != nil {
		return object.ObjectInfo{}, err
	}
	set := p.locate(ctx, bucket, key)
	lk := p.NewNSLock(bucket, key)
	c, _ := lk.GetLock(ctx, 0)
	defer lk.Unlock(c)

	m, err := set.readMeta(ctx, bucket, key)
	if err != nil {
		return object.ObjectInfo{}, mapErr(err)
	}
	m.UserTags = tags
	m.ModTime = time.Now().UTC()
	mb, err := m.marshal()
	if err != nil {
		return object.ObjectInfo{}, err
	}
	errs := set.forEachDisk(func(d Disk) error {
		return d.WriteAll(ctx, bucket, path.Join(key, metaFile), mb)
	})
	if okCount(errs) < set.writeQuorum() {
		return object.ObjectInfo{}, object.ErrWriteQuorum
	}
	return metaToInfo(bucket, key, m), nil
}

func (p *Pool) GetObjectTags(ctx context.Context, bucket, key string, _ object.ObjectOptions) (string, error) {
	m, err := p.locate(ctx, bucket, key).readMeta(ctx, bucket, key)
	if err != nil {
		return "", mapErr(err)
	}
	return m.UserTags, nil
}

func (p *Pool) DeleteObjectTags(ctx context.Context, bucket, key string, opts object.ObjectOptions) error {
	_, err := p.PutObjectTags(ctx, bucket, key, "", opts)
	return err
}
