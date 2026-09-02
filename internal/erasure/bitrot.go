package erasure

import (
	"crypto/subtle"
	"encoding/hex"

	"github.com/minio/highwayhash"
)

// bitrotKey is the fixed 32-byte HighwayHash key used for every shard
// checksum (same approach as MinIO). Changing it invalidates all existing
// checksums, so it is a hard constant.
var bitrotKey = [32]byte{
	0x4b, 0x1f, 0x9c, 0x2a, 0x7d, 0x33, 0xe0, 0x58,
	0x91, 0xa6, 0x0c, 0xf4, 0x62, 0xbb, 0x17, 0x89,
	0xd5, 0x3e, 0x74, 0x20, 0xac, 0x6f, 0x11, 0x98,
	0x5b, 0xc3, 0x08, 0xe7, 0x42, 0x9a, 0x2d, 0x66,
}

// BitrotAlgo identifies the checksum algorithm recorded in xl.meta.
const BitrotAlgo = "highwayhash256"

// bitrotInterleaved is the PartMeta.Bitrot marker for the streaming format:
// each shard file is [hash|block][hash|block]... with the 32-byte HighwayHash
// of every block written immediately before it. Nothing is stored in
// xl.meta, so metadata size is constant regardless of object size. Parts
// written by older builds have PartMeta.Checksums set instead and are read
// via the legacy path.
const bitrotInterleaved = "highwayhash256-stream"

// bitrotHashSize is the raw (not hex) HighwayHash-256 digest length.
const bitrotHashSize = 32

// bitrotSum returns the hex HighwayHash-256 of one shard's bytes (legacy
// per-stripe format, kept for reading old objects).
func bitrotSum(shard []byte) string {
	return hex.EncodeToString(bitrotRaw(shard))
}

// bitrotRaw returns the raw HighwayHash-256 digest of b.
func bitrotRaw(b []byte) []byte {
	h, err := highwayhash.New(bitrotKey[:])
	if err != nil {
		panic("erasure: bad bitrot key: " + err.Error())
	}
	_, _ = h.Write(b)
	return h.Sum(nil)
}

// bitrotEqual is a constant-time digest comparison.
func bitrotEqual(a, b []byte) bool { return subtle.ConstantTimeCompare(a, b) == 1 }
