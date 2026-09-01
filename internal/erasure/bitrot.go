package erasure

import (
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

// bitrotSum returns the hex HighwayHash-256 of one shard's bytes. A checksum
// is stored per stripe per disk, so any single corrupted shard can be
// pinpointed and excluded before Reed-Solomon reconstruction.
func bitrotSum(shard []byte) string {
	h, err := highwayhash.New(bitrotKey[:])
	if err != nil {
		panic("erasure: bad bitrot key: " + err.Error())
	}
	_, _ = h.Write(shard)
	return hex.EncodeToString(h.Sum(nil))
}
