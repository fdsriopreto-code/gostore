package erasure

import (
	"context"
	"errors"
	"hash/fnv"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/lojadopocket/gostore/internal/object"
	"github.com/lojadopocket/gostore/internal/storage"
)

// Pool is an object.Layer backed by one or more erasure sets. M4 runs a
// single set; M5 adds multiple sets with key-hashed placement.
type Pool struct {
	sets []*Set

	nsMu   sync.Mutex
	nsLock map[string]*sync.RWMutex
}

var _ object.Layer = (*Pool)(nil)

// NewPool builds a pool from erasure sets.
func NewPool(sets ...*Set) (*Pool, error) {
	if len(sets) == 0 {
		return nil, errors.New("erasure: no sets")
	}
	return &Pool{sets: sets, nsLock: map[string]*sync.RWMutex{}}, nil
}

// FromDisks is the common case: build a single set (and pool) from n disks.
func FromDisks(disks []Disk) (*Pool, error) {
	set, err := NewSet(disks)
	if err != nil {
		return nil, err
	}
	return NewPool(set)
}

func (p *Pool) setFor(key string) *Set {
	if len(p.sets) == 1 {
		return p.sets[0]
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return p.sets[int(h.Sum32())%len(p.sets)]
}

// --- lifecycle / introspection ---------------------------------------

func (p *Pool) Shutdown(context.Context) error { return nil }

func (p *Pool) StorageInfo(ctx context.Context) (object.StorageInfo, []error) {
	var si object.StorageInfo
	si.Backend.Type = "erasure"
	si.Backend.StandardSCParity = p.sets[0].parityBlocks
	for _, set := range p.sets {
		for _, d := range set.disks {
			di, err := d.DiskInfo(ctx)
			state := "ok"
			if err != nil || !d.IsOnline() {
				state = "offline"
			}
			si.Disks = append(si.Disks, object.DiskMetrics{
				Endpoint: d.String(), State: state,
				TotalSpace: di.Total, FreeSpace: di.Free, UsedSpace: di.Used,
				DiskIndex: d.Index(),
			})
		}
	}
	return si, nil
}

func (p *Pool) Health(ctx context.Context, _ object.HealthOptions) object.HealthResult {
	for _, set := range p.sets {
		online := 0
		for _, d := range set.disks {
			if d.IsOnline() {
				online++
			}
		}
		if online < set.writeQuorum() {
			return object.HealthResult{
				Healthy: false, WriteQuorum: set.writeQuorum(),
				Reason: "not enough disks online for write quorum",
			}
		}
	}
	return object.HealthResult{Healthy: true, WriteQuorum: p.sets[0].writeQuorum()}
}

func (p *Pool) NewNSLock(bucket string, objects ...string) object.RWLocker {
	key := bucket
	if len(objects) > 0 {
		key = bucket + "/" + objects[0]
	}
	p.nsMu.Lock()
	mu, ok := p.nsLock[key]
	if !ok {
		mu = &sync.RWMutex{}
		p.nsLock[key] = mu
	}
	p.nsMu.Unlock()
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

// --- buckets --------------------------------------------------------

func (p *Pool) MakeBucket(ctx context.Context, bucket string, _ object.MakeBucketOptions) error {
	if !validBucketName(bucket) {
		return object.ErrBucketNameInvalid
	}
	for _, set := range p.sets {
		if err := set.MakeBucket(ctx, bucket); err != nil {
			if errors.Is(err, storage.ErrVolumeExists) {
				return object.BucketExists{Bucket: bucket}
			}
			return err
		}
	}
	return nil
}

func (p *Pool) GetBucketInfo(ctx context.Context, bucket string) (object.BucketInfo, error) {
	vi, err := p.sets[0].StatBucket(ctx, bucket)
	if err != nil {
		return object.BucketInfo{}, object.BucketNotFound{Bucket: bucket}
	}
	return object.BucketInfo{Name: bucket, Created: vi.Created}, nil
}

func (p *Pool) ListBuckets(ctx context.Context) ([]object.BucketInfo, error) {
	vis, err := p.sets[0].ListBuckets(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]object.BucketInfo, 0, len(vis))
	for _, v := range vis {
		out = append(out, object.BucketInfo{Name: v.Name, Created: v.Created})
	}
	return out, nil
}

func (p *Pool) DeleteBucket(ctx context.Context, bucket string, opts object.DeleteBucketOptions) error {
	for _, set := range p.sets {
		if err := set.DeleteBucket(ctx, bucket, opts.Force); err != nil {
			if errors.Is(err, storage.ErrVolumeNotEmpty) {
				return object.ErrBucketNotEmpty
			}
			if errors.Is(err, storage.ErrVolumeNotFound) {
				return object.BucketNotFound{Bucket: bucket}
			}
			return err
		}
	}
	return nil
}

// --- objects ------------------------------------------------------

func (p *Pool) PutObject(ctx context.Context, bucket, key string, data *object.PutObjReader, opts object.ObjectOptions) (object.ObjectInfo, error) {
	if err := p.ensureBucket(ctx, bucket); err != nil {
		return object.ObjectInfo{}, err
	}
	if !validObjectName(key) {
		return object.ObjectInfo{}, object.ErrObjectNameInvalid
	}
	lk := p.NewNSLock(bucket, key)
	c, _ := lk.GetLock(ctx, 0)
	defer lk.Unlock(c)

	if opts.CheckPrecondFn != nil {
		if m, err := p.setFor(key).statObject(ctx, bucket, key); err == nil {
			if opts.CheckPrecondFn(metaToInfo(bucket, key, m)) {
				return object.ObjectInfo{}, object.ErrPreconditionFailed
			}
		}
	}

	meta, err := p.setFor(key).putObject(ctx, bucket, key, []partSource{
		{Number: 1, Size: data.Size(), Reader: data},
	}, toUserMeta(opts))
	if err != nil {
		return object.ObjectInfo{}, mapErr(err)
	}
	if !opts.MTime.IsZero() {
		meta.ModTime = opts.MTime.UTC()
	}
	return metaToInfo(bucket, key, meta), nil
}

func (p *Pool) GetObjectInfo(ctx context.Context, bucket, key string, _ object.ObjectOptions) (object.ObjectInfo, error) {
	if err := p.ensureBucket(ctx, bucket); err != nil {
		return object.ObjectInfo{}, err
	}
	m, err := p.setFor(key).statObject(ctx, bucket, key)
	if err != nil {
		return object.ObjectInfo{}, mapErr(err)
	}
	return metaToInfo(bucket, key, m), nil
}

func (p *Pool) GetObjectNInfo(ctx context.Context, bucket, key string, rs *object.HTTPRangeSpec, _ http.Header, opts object.ObjectOptions) (*object.GetObjectReader, error) {
	if err := p.ensureBucket(ctx, bucket); err != nil {
		return nil, err
	}
	set := p.setFor(key)
	m, err := set.statObject(ctx, bucket, key)
	if err != nil {
		return nil, mapErr(err)
	}
	oi := metaToInfo(bucket, key, m)
	if opts.CheckPrecondFn != nil && opts.CheckPrecondFn(oi) {
		return nil, object.ErrPreconditionFailed
	}

	var off, length int64 = 0, m.Size
	if rs != nil {
		o, l, rerr := resolveRange(rs, m.Size)
		if rerr != nil {
			return nil, rerr
		}
		off, length = o, l
		oi.Size = length
	}

	pr, pw := io.Pipe()
	go func() {
		err := set.getObject(ctx, bucket, key, off, length, pw)
		_ = pw.CloseWithError(err)
	}()
	return &object.GetObjectReader{ObjInfo: oi, ReadCloser: pr}, nil
}

func (p *Pool) DeleteObject(ctx context.Context, bucket, key string, _ object.ObjectOptions) (object.ObjectInfo, error) {
	if err := p.ensureBucket(ctx, bucket); err != nil {
		return object.ObjectInfo{}, err
	}
	lk := p.NewNSLock(bucket, key)
	c, _ := lk.GetLock(ctx, 0)
	defer lk.Unlock(c)
	if err := p.setFor(key).deleteObject(ctx, bucket, key); err != nil {
		return object.ObjectInfo{}, mapErr(err)
	}
	return object.ObjectInfo{Bucket: bucket, Name: key}, nil
}

func (p *Pool) DeleteObjects(ctx context.Context, bucket string, objs []object.ObjectToDelete, opts object.ObjectOptions) ([]object.DeletedObject, []error) {
	deleted := make([]object.DeletedObject, len(objs))
	errs := make([]error, len(objs))
	for i, o := range objs {
		if _, err := p.DeleteObject(ctx, bucket, o.ObjectName, opts); err != nil {
			errs[i] = err
			continue
		}
		deleted[i] = object.DeletedObject{ObjectName: o.ObjectName}
	}
	return deleted, errs
}

func (p *Pool) CopyObject(ctx context.Context, srcBucket, srcObject, dstBucket, dstObject string, _ object.ObjectInfo, _, dstOpts object.ObjectOptions) (object.ObjectInfo, error) {
	src, err := p.GetObjectNInfo(ctx, srcBucket, srcObject, nil, nil, object.ObjectOptions{})
	if err != nil {
		return object.ObjectInfo{}, err
	}
	defer src.Close()

	ud := map[string]string{}
	for k, v := range src.ObjInfo.UserDefined {
		ud[k] = v
	}
	if dstOpts.UserDefined["_directive"] == "REPLACE" {
		ud = dstOpts.UserDefined
	}
	pr := object.NewPutObjReader(src, src.ObjInfo.Size, src.ObjInfo.Size)
	return p.PutObject(ctx, dstBucket, dstObject, pr, object.ObjectOptions{UserDefined: ud, UserTags: dstOpts.UserTags})
}

// --- helpers -----------------------------------------------------

func (p *Pool) ensureBucket(ctx context.Context, bucket string) error {
	if !validBucketName(bucket) {
		return object.ErrBucketNameInvalid
	}
	if _, err := p.sets[0].StatBucket(ctx, bucket); err != nil {
		return object.BucketNotFound{Bucket: bucket}
	}
	return nil
}

func toUserMeta(opts object.ObjectOptions) userMeta {
	um := userMeta{tags: opts.UserTags, user: map[string]string{}}
	for k, v := range opts.UserDefined {
		switch k {
		case "content-type":
			um.contentType = v
		case "content-encoding":
			um.contentEnc = v
		case "_directive":
		default:
			um.user[k] = v
		}
	}
	if len(um.user) == 0 {
		um.user = nil
	}
	return um
}

func metaToInfo(bucket, key string, m *XLMeta) object.ObjectInfo {
	oi := object.ObjectInfo{
		Bucket: bucket, Name: key,
		Size: m.Size, ModTime: m.ModTime, ETag: m.ETag,
		ContentType: m.ContentType, ContentEncoding: m.ContentEnc,
		UserTags: m.UserTags, StorageClass: "STANDARD", IsLatest: true,
		UserDefined: map[string]string{},
	}
	for k, v := range m.UserMeta {
		oi.UserDefined[k] = v
	}
	if m.ContentType != "" {
		oi.UserDefined["content-type"] = m.ContentType
	}
	for _, pt := range m.Parts {
		oi.Parts = append(oi.Parts, object.ObjectPartInfo{
			Number: pt.Number, Size: pt.Size, ActualSize: pt.ActualSize, ETag: pt.ETag,
		})
	}
	return oi
}

func mapErr(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, storage.ErrFileNotFound):
		return object.ErrObjectNotFound
	case errors.Is(err, ErrReadQuorum):
		return object.ErrReadQuorum
	case errors.Is(err, ErrWriteQuorum):
		return object.ErrWriteQuorum
	case errors.Is(err, ErrBitrot), errors.Is(err, ErrCorrupt):
		return object.ErrCorruptedData
	default:
		return err
	}
}

func resolveRange(rs *object.HTTPRangeSpec, size int64) (off, length int64, err error) {
	if rs.IsSuffixLength {
		n := -rs.Start
		if n <= 0 {
			return 0, 0, object.ErrInvalidRange
		}
		if n > size {
			n = size
		}
		return size - n, n, nil
	}
	if rs.Start < 0 || rs.Start >= size {
		return 0, 0, object.ErrInvalidRange
	}
	end := rs.End
	if end < 0 || end >= size {
		end = size - 1
	}
	if end < rs.Start {
		return 0, 0, object.ErrInvalidRange
	}
	return rs.Start, end - rs.Start + 1, nil
}
