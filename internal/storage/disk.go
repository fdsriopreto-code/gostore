// Package storage defines StorageAPI: the low-level, per-disk operation set.
//
// One StorageAPI == one physical disk (a directory on a filesystem, or a
// remote disk reached over RPC in distributed mode). The erasure layer
// (internal/erasure, M4+) fans reads/writes across a set of these; the
// single-disk object backend (M1) drives exactly one. This mirrors MinIO's
// StorageAPI in cmd/storage-interface.go.
package storage

import (
	"context"
	"errors"
	"io"
	"time"
)

// Common per-disk errors. The erasure layer interprets these to decide
// quorum, healing, and which disks to exclude.
var (
	ErrDiskNotFound     = errors.New("storage: disk not found / offline")
	ErrDiskFull         = errors.New("storage: disk full")
	ErrVolumeNotFound   = errors.New("storage: volume (bucket) not found")
	ErrVolumeExists     = errors.New("storage: volume already exists")
	ErrVolumeNotEmpty   = errors.New("storage: volume not empty")
	ErrFileNotFound     = errors.New("storage: file not found")
	ErrFileVersionNotFound = errors.New("storage: file version not found")
	ErrFileCorrupt      = errors.New("storage: file failed bitrot check")
	ErrIsNotRegular     = errors.New("storage: not a regular file")
	ErrPathNotFound     = errors.New("storage: path not found")
	ErrTooManyOpenFiles = errors.New("storage: too many open files")
	ErrUnformattedDisk  = errors.New("storage: disk not formatted for gostore")
	ErrInconsistentDisk = errors.New("storage: disk format id mismatch")
)

// VolInfo describes a volume (a volume maps 1:1 to a bucket).
type VolInfo struct {
	Name    string
	Created time.Time
}

// FileInfo is the per-disk view of an object (or object version). The
// erasure layer merges FileInfo across a set into an object.ObjectInfo.
type FileInfo struct {
	Volume string
	Name   string

	VersionID    string
	IsLatest     bool
	Deleted      bool // delete marker

	ModTime time.Time
	Size    int64
	Mode    uint32

	// Metadata holds both user metadata (x-amz-meta-*) and reserved internal
	// keys, exactly as it will be serialized into the on-disk metadata file
	// (xl.meta equivalent, M4).
	Metadata map[string]string

	// Parts + Erasure describe the physical layout; empty in single-disk mode.
	Parts   []ObjectPart
	Erasure ErasureInfo

	// Data, when non-nil, is the object content stored inline with metadata
	// (small-object optimization).
	Data []byte

	// DiskMTime is the metadata file's own mtime, used for consistency checks.
	DiskMTime time.Time
}

// ObjectPart is one part of a multipart object as laid out on disk.
type ObjectPart struct {
	Number     int
	ETag       string
	Size       int64
	ActualSize int64
}

// ErasureInfo captures the erasure-coding parameters for an object (M4).
type ErasureInfo struct {
	Algorithm    string // e.g. "reedsolomon"
	DataBlocks   int
	ParityBlocks int
	BlockSize    int64
	Index        int   // this disk's shard index within the set (1-based)
	Distribution []int // permutation of shard indices across the set
	// Checksums[i] is the bitrot checksum info for part i.
	Checksums []ChecksumInfo
}

// ChecksumInfo is the bitrot hash algorithm + seed for one part.
type ChecksumInfo struct {
	PartNumber int
	Algorithm  string // "highwayhash256" | "blake2b" | "none"
	Hash       []byte
}

// ReadOptions / WriteOptions carry per-call knobs (verification, sync, ...).
type ReadOptions struct {
	// VerifyBitrot recomputes and checks shard checksums while reading (M4).
	VerifyBitrot bool
}

type WriteOptions struct {
	// Sync forces an fsync before returning.
	Sync bool
}

// StorageAPI is the operation set for a single disk. Implementations:
//   - local.go       : a directory on the local filesystem (M1)
//   - remote/client.go: a disk on another node, over RPC (M6)
type StorageAPI interface {
	// String returns a human-readable endpoint identifier.
	String() string

	// IsOnline reports reachability; IsLocal distinguishes local vs remote.
	IsOnline() bool
	IsLocal() bool

	// Close releases file handles / connections.
	Close() error

	// --- disk-level ----------------------------------------------------

	// DiskInfo returns capacity + format identity.
	DiskInfo(ctx context.Context) (DiskInfo, error)

	// MakeVol / ListVols / StatVol / DeleteVol manage volumes (buckets).
	MakeVol(ctx context.Context, volume string) error
	ListVols(ctx context.Context) ([]VolInfo, error)
	StatVol(ctx context.Context, volume string) (VolInfo, error)
	DeleteVol(ctx context.Context, volume string, forceDelete bool) error

	// --- object metadata --------------------------------------------

	// ReadVersion returns the FileInfo for a specific version (or the latest
	// when versionID is "").
	ReadVersion(ctx context.Context, volume, path, versionID string, opts ReadOptions) (FileInfo, error)

	// WriteMetadata persists a FileInfo (the xl.meta write path).
	WriteMetadata(ctx context.Context, volume, path string, fi FileInfo) error

	// DeleteVersion removes one version's metadata (and data if it was the
	// last reference).
	DeleteVersion(ctx context.Context, volume, path string, fi FileInfo, forceDeleteMarker bool) error

	// --- object data ----------------------------------------------

	// CreateFile streams `size` bytes from r into volume/path.
	CreateFile(ctx context.Context, volume, path string, size int64, r io.Reader, opts WriteOptions) error

	// ReadFileStream returns a reader for [offset, offset+length) of a file.
	ReadFileStream(ctx context.Context, volume, path string, offset, length int64, opts ReadOptions) (io.ReadCloser, error)

	// RenameData atomically moves a temp-written object (data + metadata) into
	// its final location — the commit step of a PUT.
	RenameData(ctx context.Context, srcVolume, srcPath string, fi FileInfo, dstVolume, dstPath string) error

	// RenameFile moves a single file (used by multipart part staging).
	RenameFile(ctx context.Context, srcVolume, srcPath, dstVolume, dstPath string) error

	// Delete removes a file or (recursively) a path.
	Delete(ctx context.Context, volume, path string, recursive bool) error

	// --- listing / walking --------------------------------------

	// ListDir returns up to `count` immediate entries under dirPath.
	ListDir(ctx context.Context, volume, dirPath string, count int) ([]string, error)

	// WalkDir streams directory entries for listing/scanning. opts/results
	// are refined in M1/M5 — kept minimal here.
	WalkDir(ctx context.Context, opts WalkDirOptions, w io.Writer) error
}

// DiskInfo is the capacity + identity of a single disk.
type DiskInfo struct {
	Total   uint64
	Free    uint64
	Used    uint64
	FSType  string

	RootDisk   bool
	Healing    bool
	Endpoint   string
	MountPath  string
	ID         string // format.json UUID
	Error      string

	PoolIndex int
	SetIndex  int
	DiskIndex int
}

// WalkDirOptions parameterizes WalkDir.
type WalkDirOptions struct {
	Bucket         string
	BaseDir        string
	Recursive      bool
	ReportNotFound bool
	FilterPrefix   string
	ForwardTo      string
	Limit          int
}
