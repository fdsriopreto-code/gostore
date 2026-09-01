package object

import (
	"context"
	"net/http"
	"sync"
	"time"
)

// Stub is a placeholder Layer for M0: it satisfies the interface, reports a
// healthy (but empty) backend, and returns ErrNotImplemented for every real
// operation. M1 replaces it with the single-disk filesystem backend.
type Stub struct{}

// NewStub returns the M0 placeholder backend.
func NewStub() *Stub { return &Stub{} }

var _ Layer = (*Stub)(nil)

func (*Stub) Shutdown(context.Context) error { return nil }

func (*Stub) StorageInfo(context.Context) (StorageInfo, []error) {
	var si StorageInfo
	si.Backend.Type = "stub"
	return si, nil
}

func (*Stub) Health(context.Context, HealthOptions) HealthResult {
	return HealthResult{Healthy: true, WriteQuorum: 0, Reason: "M0 stub backend (no storage configured yet)"}
}

func (*Stub) NewNSLock(string, ...string) RWLocker { return &localRWLock{} }

func (*Stub) MakeBucket(context.Context, string, MakeBucketOptions) error { return ErrNotImplemented }
func (*Stub) GetBucketInfo(context.Context, string) (BucketInfo, error) {
	return BucketInfo{}, ErrNotImplemented
}
func (*Stub) ListBuckets(context.Context) ([]BucketInfo, error) { return nil, ErrNotImplemented }
func (*Stub) DeleteBucket(context.Context, string, DeleteBucketOptions) error {
	return ErrNotImplemented
}

func (*Stub) GetObjectNInfo(context.Context, string, string, *HTTPRangeSpec, http.Header, ObjectOptions) (*GetObjectReader, error) {
	return nil, ErrNotImplemented
}
func (*Stub) GetObjectInfo(context.Context, string, string, ObjectOptions) (ObjectInfo, error) {
	return ObjectInfo{}, ErrNotImplemented
}
func (*Stub) PutObject(context.Context, string, string, *PutObjReader, ObjectOptions) (ObjectInfo, error) {
	return ObjectInfo{}, ErrNotImplemented
}
func (*Stub) PutObjectTags(context.Context, string, string, string, ObjectOptions) (ObjectInfo, error) {
	return ObjectInfo{}, ErrNotImplemented
}
func (*Stub) GetObjectTags(context.Context, string, string, ObjectOptions) (string, error) {
	return "", ErrNotImplemented
}
func (*Stub) DeleteObjectTags(context.Context, string, string, ObjectOptions) error {
	return ErrNotImplemented
}
func (*Stub) PutObjectRetention(context.Context, string, string, string, string, time.Time, bool) error {
	return ErrNotImplemented
}
func (*Stub) GetObjectRetention(context.Context, string, string, string) (string, time.Time, error) {
	return "", time.Time{}, ErrNotImplemented
}
func (*Stub) PutObjectLegalHold(context.Context, string, string, string, string) error {
	return ErrNotImplemented
}
func (*Stub) GetObjectLegalHold(context.Context, string, string, string) (string, error) {
	return "", ErrNotImplemented
}
func (*Stub) CopyObject(context.Context, string, string, string, string, ObjectInfo, ObjectOptions, ObjectOptions) (ObjectInfo, error) {
	return ObjectInfo{}, ErrNotImplemented
}
func (*Stub) DeleteObject(context.Context, string, string, ObjectOptions) (ObjectInfo, error) {
	return ObjectInfo{}, ErrNotImplemented
}
func (*Stub) DeleteObjects(context.Context, string, []ObjectToDelete, ObjectOptions) ([]DeletedObject, []error) {
	return nil, []error{ErrNotImplemented}
}

func (*Stub) ListObjects(context.Context, string, string, string, string, int) (ListObjectsInfo, error) {
	return ListObjectsInfo{}, ErrNotImplemented
}
func (*Stub) ListObjectsV2(context.Context, string, string, string, string, int, bool, string) (ListObjectsV2Info, error) {
	return ListObjectsV2Info{}, ErrNotImplemented
}
func (*Stub) ListObjectVersions(context.Context, string, string, string, string, string, int) (ListObjectVersionsInfo, error) {
	return ListObjectVersionsInfo{}, ErrNotImplemented
}

func (*Stub) NewMultipartUpload(context.Context, string, string, ObjectOptions) (*NewMultipartUploadResult, error) {
	return nil, ErrNotImplemented
}
func (*Stub) CopyObjectPart(context.Context, string, string, string, string, string, int, int64, int64, ObjectInfo, ObjectOptions, ObjectOptions) (PartInfo, error) {
	return PartInfo{}, ErrNotImplemented
}
func (*Stub) PutObjectPart(context.Context, string, string, string, int, *PutObjReader, ObjectOptions) (PartInfo, error) {
	return PartInfo{}, ErrNotImplemented
}
func (*Stub) ListObjectParts(context.Context, string, string, string, int, int, ObjectOptions) (ListPartsInfo, error) {
	return ListPartsInfo{}, ErrNotImplemented
}
func (*Stub) ListMultipartUploads(context.Context, string, string, string, string, string, int) (ListMultipartsInfo, error) {
	return ListMultipartsInfo{}, ErrNotImplemented
}
func (*Stub) AbortMultipartUpload(context.Context, string, string, string, ObjectOptions) error {
	return ErrNotImplemented
}
func (*Stub) CompleteMultipartUpload(context.Context, string, string, string, []CompletePart, ObjectOptions) (ObjectInfo, error) {
	return ObjectInfo{}, ErrNotImplemented
}

// localRWLock is a process-local implementation of RWLocker, used in
// single-disk mode. Distributed mode (M6) swaps in a quorum lock.
type localRWLock struct{ mu sync.RWMutex }

func (l *localRWLock) GetLock(ctx context.Context, _ time.Duration) (context.Context, error) {
	l.mu.Lock()
	return ctx, nil
}
func (l *localRWLock) Unlock(context.Context) { l.mu.Unlock() }
func (l *localRWLock) GetRLock(ctx context.Context, _ time.Duration) (context.Context, error) {
	l.mu.RLock()
	return ctx, nil
}
func (l *localRWLock) RUnlock(context.Context) { l.mu.RUnlock() }
