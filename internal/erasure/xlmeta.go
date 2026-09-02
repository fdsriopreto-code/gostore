package erasure

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"hash/fnv"
	"time"
)

// xlMetaVersion is the on-disk metadata schema version.
const xlMetaVersion = 1

// XLMeta is the per-object metadata. A byte-identical copy is written to
// every disk in the set, so a quorum of disks is enough to recover it and
// equality checks are a simple content comparison.
type XLMeta struct {
	Version int         `json:"version"`
	Erasure ErasureMeta `json:"erasure"`

	// Revision is a per-object monotonic counter bumped on every write. It
	// makes majority-wins deterministic on a count tie (higher wins) and lets
	// heal / read-modify-write abort when the object changed under them
	// (fencing against a stale writer whose lock expired).
	Revision uint64 `json:"rev,omitempty"`

	Size    int64     `json:"size"`
	ModTime time.Time `json:"modTime"`
	ETag    string    `json:"etag"`

	// Versioning + Object Lock (populated on versioned buckets).
	VersionID   string    `json:"versionId,omitempty"`
	LockMode    string    `json:"lockMode,omitempty"` // GOVERNANCE | COMPLIANCE
	RetainUntil time.Time `json:"retainUntil,omitempty"`
	LegalHold   bool      `json:"legalHold,omitempty"`

	ContentType string            `json:"contentType,omitempty"`
	ContentEnc  string            `json:"contentEncoding,omitempty"`
	UserMeta    map[string]string `json:"userMeta,omitempty"`
	UserTags    string            `json:"userTags,omitempty"`

	Parts []PartMeta `json:"parts"`

	// Inline holds the object's bytes directly in xl.meta for small objects
	// (<= inlineMax). xl.meta is replicated verbatim to every disk, so inline
	// data recovers on the same read quorum that recovers the metadata and
	// needs one file op per disk to read or write instead of an extra
	// shard-file-per-disk. For SSE objects Inline is the ciphertext
	// (ETag/PlainSize semantics unchanged).
	Inline []byte `json:"inline,omitempty"`

	// SSE-S3 at rest. When SSE == "AES256": Size/part sizes are ciphertext,
	// PlainSize is the logical object size, ETag the plaintext md5.
	SSE         string `json:"sse,omitempty"`
	PlainSize   int64  `json:"plainSize,omitempty"`
	EncDEK      string `json:"encDEK,omitempty"`      // hex, master-key-wrapped data key
	NoncePrefix string `json:"noncePrefix,omitempty"` // hex, 4 bytes
}

// ErasureMeta captures the coding parameters for the object.
type ErasureMeta struct {
	Algorithm    string `json:"algorithm"` // "reedsolomon"
	DataBlocks   int    `json:"data"`
	ParityBlocks int    `json:"parity"`
	BlockSize    int64  `json:"blockSize"`

	// Distribution[j] = index (0-based) of the disk holding shard j.
	// Shards 0..DataBlocks-1 are data shards; the rest are parity.
	Distribution []int `json:"distribution"`

	ChecksumAlgo string `json:"checksumAlgo"` // BitrotAlgo
}

// PartMeta describes one part of the object.
type PartMeta struct {
	Number     int    `json:"number"`
	Size       int64  `json:"size"`       // logical (plaintext) size
	ActualSize int64  `json:"actualSize"` // same as Size in M4 (no compression/SSE)
	ETag       string `json:"etag"`       // md5 hex of the part plaintext

	// Bitrot names the shard-integrity scheme for this part:
	//   bitrotInterleaved — hashes are stored inline in the shard files
	//                       ([hash|block] per stripe); Checksums is empty.
	//   "" (legacy)       — hashes are in Checksums below.
	Bitrot string `json:"bitrot,omitempty"`

	// Checksums[stripe][diskIndex] = hex bitrot hash of that disk's shard for
	// that stripe. Only populated for legacy (pre-streaming-bitrot) parts.
	Checksums [][]string `json:"checksums,omitempty"`
}

func (m *XLMeta) marshal() ([]byte, error) { return json.Marshal(m) }

func unmarshalXLMeta(b []byte) (*XLMeta, error) {
	var m XLMeta
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, err
	}
	return &m, nil
}

// contentHash returns a stable hash of the metadata's identity fields, used
// to pick the winning version across disks by majority.
func (m *XLMeta) contentHash() string {
	h := sha256.New()
	b, _ := json.Marshal(struct {
		S int64
		T int64
		E string
		P []PartMeta
	}{m.Size, m.ModTime.UnixNano(), m.ETag, m.Parts})
	h.Write(b)
	h.Write(m.Inline)
	return hex.EncodeToString(h.Sum(nil))
}

// shardForDisk returns the shard index j (0-based) stored on the given disk
// index, or -1 if that disk holds no shard for this object.
func (em ErasureMeta) shardForDisk(diskIdx int) int {
	for j, d := range em.Distribution {
		if d == diskIdx {
			return j
		}
	}
	return -1
}

// buildDistribution returns a rotated identity permutation of [0,n) seeded by
// the object key, so shard placement spreads across disks without a lookup
// table. Reversible via shardForDisk.
func buildDistribution(key string, n int) []int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	rot := int(h.Sum32()) % n
	if rot < 0 {
		rot += n
	}
	d := make([]int, n)
	for j := 0; j < n; j++ {
		d[j] = (j + rot) % n
	}
	return d
}
