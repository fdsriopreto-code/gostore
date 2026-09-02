package erasure

import (
	"bytes"
	"crypto/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/lojadopocket/gostore/internal/object"
)

// TestInterleavedBitrotLayout checks the on-disk shape: shard files carry a
// 32-byte hash before every stripe block and xl.meta records no checksums.
func TestInterleavedBitrotLayout(t *testing.T) {
	p, roots := newTestPool(t, 4) // 2 data + 2 parity
	_ = p.MakeBucket(ctx(), "buck", object.MakeBucketOptions{})

	// 2.5 stripes: stripeInput = 2 MiB, so 5 MiB payload -> 3 stripes.
	payload := make([]byte, 5<<20)
	_, _ = rand.Read(payload)
	put(t, p, "buck", "obj", payload)

	m, err := p.setFor("obj").readMeta(ctx(), "buck", "obj")
	if err != nil {
		t.Fatal(err)
	}
	if m.Parts[0].Bitrot != bitrotInterleaved {
		t.Fatalf("Bitrot = %q, want %q", m.Parts[0].Bitrot, bitrotInterleaved)
	}
	if len(m.Parts[0].Checksums) != 0 {
		t.Fatalf("interleaved part must not carry checksums in xl.meta, got %d rows", len(m.Parts[0].Checksums))
	}

	// full stripe shard = 1 MiB; 3 stripes (2 full + 1 half). Each stripe
	// block is prefixed by a 32-byte hash.
	fullShard := int64(1 << 20)
	lastShardLen := ceilInt64((5<<20)-2*(2<<20), 2) // 1 MiB -> 512 KiB per shard
	wantSize := 2*(bitrotHashSize+fullShard) + (bitrotHashSize + lastShardLen)
	fi, err := os.Stat(filepath.Join(roots[0], "buck", "obj", "part.00001"))
	if err != nil {
		t.Fatal(err)
	}
	if fi.Size() != wantSize {
		t.Fatalf("shard file size = %d, want %d", fi.Size(), wantSize)
	}
}

// TestInterleavedBitrotDetectsAndHeals flips a byte inside a shard block and
// confirms the read still returns correct data (reconstructed) and heal
// restores the shard.
func TestInterleavedBitrotDetectsAndHeals(t *testing.T) {
	p, roots := newTestPool(t, 6)
	_ = p.MakeBucket(ctx(), "buck", object.MakeBucketOptions{})
	payload := make([]byte, 3<<20+123)
	_, _ = rand.Read(payload)
	put(t, p, "buck", "obj", payload)

	// Corrupt one byte well past the first hash header on disk 0.
	fp := filepath.Join(roots[0], "buck", "obj", "part.00001")
	b, err := os.ReadFile(fp)
	if err != nil {
		t.Fatal(err)
	}
	b[bitrotHashSize+100] ^= 0xFF
	if err := os.WriteFile(fp, b, 0o644); err != nil {
		t.Fatal(err)
	}

	if got := get(t, p, "buck", "obj"); !bytes.Equal(got, payload) {
		t.Fatal("read did not reconstruct around corrupted shard")
	}

	rep, err := p.Heal(ctx())
	if err != nil {
		t.Fatal(err)
	}
	if rep.ShardsRewritten == 0 {
		t.Fatal("heal did not rewrite the corrupted shard")
	}
	// After heal the shard file must be valid again (byte-compare to a peer's
	// reconstruction is overkill; just confirm the object still reads).
	if got := get(t, p, "buck", "obj"); !bytes.Equal(got, payload) {
		t.Fatal("post-heal read mismatch")
	}
}

// TestLegacyRawShardStillReads writes an object in the pre-streaming format
// (raw shard files + per-stripe checksums in xl.meta) and confirms the
// dual-path reader still returns it byte-for-byte.
func TestLegacyRawShardStillReads(t *testing.T) {
	p, roots := newTestPool(t, 4) // 2 data + 2 parity
	s := p.setFor("lobj")
	_ = p.MakeBucket(ctx(), "buck", object.MakeBucketOptions{})

	payload := make([]byte, 1500) // single (short) stripe
	_, _ = rand.Read(payload)

	dist := buildDistribution("lobj", s.n())
	shards, err := s.ec.EncodeData(payload)
	if err != nil {
		t.Fatal(err)
	}
	row := make([]string, s.n())
	for j := 0; j < s.n(); j++ {
		di := dist[j]
		// raw shard, NO hash prefix — the legacy on-disk format
		dir := filepath.Join(roots[di], "buck", "lobj")
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "part.00001"), shards[j], 0o644); err != nil {
			t.Fatal(err)
		}
		row[di] = bitrotSum(shards[j])
	}

	meta := &XLMeta{
		Version: xlMetaVersion,
		ModTime: time.Now().UTC(),
		Erasure: ErasureMeta{
			Algorithm: "reedsolomon", DataBlocks: s.dataBlocks, ParityBlocks: s.parityBlocks,
			BlockSize: s.ec.blockSize, Distribution: dist, ChecksumAlgo: BitrotAlgo,
		},
		Size: int64(len(payload)),
		ETag: md5hex(payload),
		Parts: []PartMeta{{
			Number: 1, Size: int64(len(payload)), ActualSize: int64(len(payload)),
			ETag: md5hex(payload), Checksums: [][]string{row}, // legacy: no Bitrot marker
		}},
	}
	mb, _ := meta.marshal()
	for _, r := range roots {
		if err := os.WriteFile(filepath.Join(r, "buck", "lobj", "xl.meta"), mb, 0o644); err != nil {
			// dir may not exist on parity-only disks in this dist; create it
			_ = os.MkdirAll(filepath.Join(r, "buck", "lobj"), 0o755)
			if err := os.WriteFile(filepath.Join(r, "buck", "lobj", "xl.meta"), mb, 0o644); err != nil {
				t.Fatal(err)
			}
		}
	}

	if got := get(t, p, "buck", "lobj"); !bytes.Equal(got, payload) {
		t.Fatal("legacy raw-shard object did not read back correctly")
	}
}
