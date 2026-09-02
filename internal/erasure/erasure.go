// Package erasure implements object.Layer on top of N local disks using
// Reed-Solomon erasure coding, per-shard bitrot hashing, and quorum
// read/write semantics — the M4 backend, modelled on MinIO's erasure design.
//
// An object's data is split into stripes; each stripe is encoded into
// dataBlocks data shards + parityBlocks parity shards, one shard per disk.
// The object survives the loss of up to parityBlocks disks. Every disk also
// holds a full copy of the object's xl.meta, so metadata survives the same
// loss.
package erasure

import (
	"context"
	"errors"
	"io"

	"github.com/klauspost/reedsolomon"

	"github.com/lojadopocket/gostore/internal/storage"
)

// Errors surfaced by the erasure layer (mapped to object.Err* by pool.go).
var (
	ErrReadQuorum     = errors.New("erasure: read quorum not met")
	ErrWriteQuorum    = errors.New("erasure: write quorum not met")
	ErrBitrot         = errors.New("erasure: shard failed bitrot check")
	ErrCorrupt        = errors.New("erasure: object metadata corrupt or inconsistent")
	ErrObjectMismatch = errors.New("erasure: assembled object failed checksum verification")
)

// Disk is the subset of per-disk operations the erasure layer needs. It is
// implemented by *storage.LocalDisk now and by a remote RPC client in M6.
type Disk interface {
	String() string
	ID() string
	Index() int
	IsOnline() bool

	MakeVol(ctx context.Context, bucket string) error
	StatVol(ctx context.Context, bucket string) (storage.VolInfo, error)
	ListVols(ctx context.Context) ([]storage.VolInfo, error)
	DeleteVol(ctx context.Context, bucket string, force bool) error

	WriteAll(ctx context.Context, bucket, object string, data []byte) error
	ReadAll(ctx context.Context, bucket, object string) ([]byte, error)
	CreateFile(ctx context.Context, bucket, object string, size int64, r io.Reader) error
	ReadFileStream(ctx context.Context, bucket, object string, offset, length int64) (io.ReadCloser, error)
	RenameDir(ctx context.Context, srcBucket, srcObject, dstBucket, dstObject string) error
	Delete(ctx context.Context, bucket, object string, recursive bool) error
	ListDir(ctx context.Context, bucket, dir string) ([]string, error)
	StagingPath() string
	DiskInfo(ctx context.Context) (storage.DiskInfo, error)
}

// blockSizeV2 is the stripe block size per shard (1 MiB), matching MinIO.
const blockSizeV2 = 1 << 20

// inlineMaxBytes is the largest object stored inline in xl.meta instead of as
// shard files. Matches MinIO's default (128 KiB). Overridable at startup.
var inlineMaxBytes int64 = 128 << 10

// SetInlineMax sets the inline-object size threshold (bytes). <= 0 disables
// inlining. Call once at startup before serving.
func SetInlineMax(n int64) { inlineMaxBytes = n }

// Erasure wraps a Reed-Solomon codec plus its parameters.
type Erasure struct {
	enc          reedsolomon.Encoder
	dataBlocks   int
	parityBlocks int
	blockSize    int64
}

// NewErasure builds a codec for data+parity shards.
func NewErasure(dataBlocks, parityBlocks int) (*Erasure, error) {
	if dataBlocks <= 0 || parityBlocks < 0 || dataBlocks+parityBlocks > 256 {
		return nil, errors.New("erasure: invalid data/parity block counts")
	}
	enc, err := reedsolomon.New(dataBlocks, parityBlocks,
		reedsolomon.WithAutoGoroutines(int(blockSizeV2)))
	if err != nil {
		return nil, err
	}
	return &Erasure{enc: enc, dataBlocks: dataBlocks, parityBlocks: parityBlocks, blockSize: blockSizeV2}, nil
}

func (e *Erasure) shards() int { return e.dataBlocks + e.parityBlocks }

// stripeInputSize is the number of plaintext bytes consumed per full stripe
// (blockSize per data shard).
func (e *Erasure) stripeInputSize() int64 { return e.blockSize * int64(e.dataBlocks) }

// shardSize is the per-shard byte count for a full stripe.
func (e *Erasure) shardSize() int64 { return e.blockSize }

// shardFileSize returns the on-disk size of one shard file for an object part
// of totalLength plaintext bytes.
func (e *Erasure) shardFileSize(totalLength int64) int64 {
	if totalLength == 0 {
		return 0
	}
	if totalLength < 0 {
		return -1
	}
	full := totalLength / e.stripeInputSize()
	rem := totalLength % e.stripeInputSize()
	size := full * e.blockSize
	if rem > 0 {
		size += ceilInt64(rem, int64(e.dataBlocks))
	}
	return size
}

// lastStripeShardSize is the per-shard size for the final (possibly short)
// stripe of a part of totalLength plaintext bytes.
func (e *Erasure) lastStripeShardSize(totalLength int64) int64 {
	rem := totalLength % e.stripeInputSize()
	if rem == 0 {
		return e.blockSize
	}
	return ceilInt64(rem, int64(e.dataBlocks))
}

// EncodeData splits data (one stripe, <= blockSize*dataBlocks bytes) into
// dataBlocks+parityBlocks shards.
func (e *Erasure) EncodeData(data []byte) ([][]byte, error) {
	shards, err := e.enc.Split(data)
	if err != nil {
		return nil, err
	}
	if err := e.enc.Encode(shards); err != nil {
		return nil, err
	}
	return shards, nil
}

// DecodeData reconstructs missing shards (nil entries) in place and returns
// the joined original data of dataLen bytes.
func (e *Erasure) DecodeData(shards [][]byte, dataLen int, w io.Writer) error {
	ok, err := e.enc.Verify(shards)
	if err != nil || !ok {
		if rerr := e.enc.Reconstruct(shards); rerr != nil {
			return ErrReadQuorum
		}
	}
	return e.enc.Join(w, shards, dataLen)
}

// Reconstruct fills nil shards in place from the present ones.
func (e *Erasure) Reconstruct(shards [][]byte) error { return e.enc.Reconstruct(shards) }

func ceilInt64(a, b int64) int64 { return (a + b - 1) / b }

// defaultParity returns the standard parity count for n disks (n/2, like
// MinIO's default when EC:M is unset for small sets).
func defaultParity(n int) int {
	p := n / 2
	if p < 1 {
		p = 1
	}
	return p
}
