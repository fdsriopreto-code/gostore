// Package object defines the storage abstraction the S3 API layer talks to.
//
// Layer is the single seam between "HTTP/S3 semantics" (internal/api) and
// "how bytes are actually stored" (internal/storage for single-disk,
// internal/erasure for distributed). Every backend implements this same
// interface, so the API layer never knows which one it's talking to — this
// mirrors MinIO's ObjectLayer.
package object

import (
	"context"
	"net/http"
	"time"
)

// Layer is the object storage interface. Method set tracks MinIO's
// ObjectLayer; unimplemented methods on a given backend return
// errNotImplemented until their milestone lands.
type Layer interface {
	// --- lifecycle / introspection ---------------------------------------

	// Shutdown flushes and releases resources. Called on graceful stop.
	Shutdown(ctx context.Context) error

	// LocalStorageInfo / StorageInfo report capacity and disk health.
	StorageInfo(ctx context.Context) (StorageInfo, []error)

	// Health backs the readiness probe.
	Health(ctx context.Context, opts HealthOptions) HealthResult

	// NewNSLock returns a namespace lock for the given bucket/objects. In
	// single-disk mode it's a local RWMutex; in distributed mode it's a
	// quorum lock across nodes (M6).
	NewNSLock(bucket string, objects ...string) RWLocker

	// --- buckets --------------------------------------------------------

	MakeBucket(ctx context.Context, bucket string, opts MakeBucketOptions) error
	GetBucketInfo(ctx context.Context, bucket string) (BucketInfo, error)
	ListBuckets(ctx context.Context) ([]BucketInfo, error)
	DeleteBucket(ctx context.Context, bucket string, opts DeleteBucketOptions) error

	// --- objects ------------------------------------------------------

	// GetObjectNInfo returns a streaming reader for (a range of) an object
	// together with its metadata. Caller must Close the reader.
	GetObjectNInfo(ctx context.Context, bucket, object string, rs *HTTPRangeSpec, header http.Header, opts ObjectOptions) (*GetObjectReader, error)

	GetObjectInfo(ctx context.Context, bucket, object string, opts ObjectOptions) (ObjectInfo, error)

	PutObject(ctx context.Context, bucket, object string, data *PutObjReader, opts ObjectOptions) (ObjectInfo, error)

	// PutObjectTags / GetObjectTags / DeleteObjectTags manage an object's tag
	// set without rewriting its data (S3 ?tagging sub-resource). tags is the
	// raw "k1=v1&k2=v2" form.
	PutObjectTags(ctx context.Context, bucket, object, tags string, opts ObjectOptions) (ObjectInfo, error)
	GetObjectTags(ctx context.Context, bucket, object string, opts ObjectOptions) (string, error)
	DeleteObjectTags(ctx context.Context, bucket, object string, opts ObjectOptions) error

	CopyObject(ctx context.Context, srcBucket, srcObject, dstBucket, dstObject string, srcInfo ObjectInfo, srcOpts, dstOpts ObjectOptions) (ObjectInfo, error)

	DeleteObject(ctx context.Context, bucket, object string, opts ObjectOptions) (ObjectInfo, error)
	DeleteObjects(ctx context.Context, bucket string, objects []ObjectToDelete, opts ObjectOptions) ([]DeletedObject, []error)

	// --- listing ----------------------------------------------------

	ListObjects(ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int) (ListObjectsInfo, error)
	ListObjectsV2(ctx context.Context, bucket, prefix, continuationToken, delimiter string, maxKeys int, fetchOwner bool, startAfter string) (ListObjectsV2Info, error)
	ListObjectVersions(ctx context.Context, bucket, prefix, marker, versionMarker, delimiter string, maxKeys int) (ListObjectVersionsInfo, error)

	// --- multipart -------------------------------------------------

	NewMultipartUpload(ctx context.Context, bucket, object string, opts ObjectOptions) (*NewMultipartUploadResult, error)
	CopyObjectPart(ctx context.Context, srcBucket, srcObject, dstBucket, dstObject, uploadID string, partID int, startOffset, length int64, srcInfo ObjectInfo, srcOpts, dstOpts ObjectOptions) (PartInfo, error)
	PutObjectPart(ctx context.Context, bucket, object, uploadID string, partID int, data *PutObjReader, opts ObjectOptions) (PartInfo, error)
	ListObjectParts(ctx context.Context, bucket, object, uploadID string, partNumberMarker, maxParts int, opts ObjectOptions) (ListPartsInfo, error)
	ListMultipartUploads(ctx context.Context, bucket, prefix, keyMarker, uploadIDMarker, delimiter string, maxUploads int) (ListMultipartsInfo, error)
	AbortMultipartUpload(ctx context.Context, bucket, object, uploadID string, opts ObjectOptions) error
	CompleteMultipartUpload(ctx context.Context, bucket, object, uploadID string, uploadedParts []CompletePart, opts ObjectOptions) (ObjectInfo, error)
}

// RWLocker is a distributed-capable reader/writer lock. timeout bounds how
// long GetLock/GetRLock will wait to acquire.
type RWLocker interface {
	GetLock(ctx context.Context, timeout time.Duration) (lkCtx context.Context, err error)
	Unlock(lkCtx context.Context)
	GetRLock(ctx context.Context, timeout time.Duration) (lkCtx context.Context, err error)
	RUnlock(lkCtx context.Context)
}
