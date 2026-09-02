package erasure

import (
	"bytes"
	"context"
	"io"
	"path"

	"github.com/lojadopocket/gostore/internal/logger"
)

// HealReport summarises a heal pass.
type HealReport struct {
	ObjectsScanned  int      `json:"objectsScanned"`
	ObjectsHealed   int      `json:"objectsHealed"`
	ShardsRewritten int      `json:"shardsRewritten"`
	MetaRewritten   int      `json:"metaRewritten"`
	Unrecoverable   []string `json:"unrecoverable,omitempty"`
}

// Heal scans every object in every bucket and rewrites any xl.meta or shard
// that is missing or fails its bitrot check, as long as read quorum holds.
func (p *Pool) Heal(ctx context.Context) (HealReport, error) {
	var rep HealReport
	buckets, err := p.sets[0].ListBuckets(ctx)
	if err != nil {
		return rep, err
	}
	for _, b := range buckets {
		for _, set := range p.sets {
			keys, err := set.walkKeys(ctx, b.Name)
			if err != nil {
				continue
			}
			for _, k := range keys {
				rep.ObjectsScanned++
				meta, shards, hErr := set.healObject(ctx, b.Name, k)
				rep.MetaRewritten += meta
				rep.ShardsRewritten += shards
				if hErr != nil {
					rep.Unrecoverable = append(rep.Unrecoverable, b.Name+"/"+k)
					continue
				}
				if meta+shards > 0 {
					rep.ObjectsHealed++
				}
			}
		}
	}
	return rep, nil
}

// AutoHeal detects an online disk that is missing buckets its peers hold —
// typically a freshly-added or replaced disk — and, if found, recreates the
// bucket set on every disk and kicks off a full background heal so the new
// disk is repopulated without operator action.
func (p *Pool) AutoHeal(ctx context.Context) {
	fresh := false
	for _, s := range p.sets {
		present := make([]map[string]bool, len(s.disks))
		union := map[string]int{}
		online := 0
		for i, d := range s.disks {
			if !d.IsOnline() {
				continue
			}
			online++
			vols, err := d.ListVols(ctx)
			if err != nil {
				continue
			}
			m := map[string]bool{}
			for _, v := range vols {
				if v.Name == "" || v.Name == ".gostore.sys" {
					continue
				}
				m[v.Name] = true
				union[v.Name]++
			}
			present[i] = m
		}
		for name, c := range union {
			if c >= online {
				continue
			}
			for i, d := range s.disks {
				if d.IsOnline() && present[i] != nil && !present[i][name] {
					fresh = true
					_ = s.MakeBucket(ctx, name) // tolerates already-exists
				}
			}
		}
	}
	if !fresh {
		return
	}
	logger.Info("auto-heal: under-populated disk detected, starting background heal pass")
	go func() {
		rep, err := p.Heal(context.Background())
		if err != nil {
			logger.Warn("auto-heal pass failed", "err", err)
			return
		}
		logger.Info("auto-heal pass complete",
			"objectsHealed", rep.ObjectsHealed,
			"shardsRewritten", rep.ShardsRewritten,
			"metaRewritten", rep.MetaRewritten)
	}()
}

// HealObject repairs a single object's missing/corrupt shards and xl.meta
// copies in place. Implements the healer interface the scanner samples with.
func (p *Pool) HealObject(ctx context.Context, bucket, key string) error {
	_, _, err := p.setFor(key).healObject(ctx, bucket, key)
	return err
}

// healObject repairs one object. Returns (metaRewrites, shardRewrites, err).
func (s *Set) healObject(ctx context.Context, bucket, key string) (int, int, error) {
	m, err := s.readMeta(ctx, bucket, key)
	if err != nil {
		return 0, 0, err
	}
	mb, err := m.marshal()
	if err != nil {
		return 0, 0, err
	}

	metaFixed := 0
	for _, d := range s.disks {
		cur, rerr := d.ReadAll(ctx, bucket, path.Join(key, metaFile))
		if rerr != nil || !bytes.Equal(cur, mb) {
			if d.WriteAll(ctx, bucket, path.Join(key, metaFile), mb) == nil {
				metaFixed++
			}
		}
	}

	// Inline objects carry their data in xl.meta itself; re-replicating the
	// metadata above is the whole heal.
	if m.Inline != nil {
		return metaFixed, 0, nil
	}

	dist := m.Erasure.Distribution
	fullShard := m.Erasure.BlockSize
	shardFixed := 0

	for pi, pm := range m.Parts {
		partPath := path.Join(key, partFile(pm.Number))
		var bad []int
		for di := range s.disks {
			if !s.shardIntact(ctx, s.disks[di], bucket, partPath, pi, di, pm, m) {
				bad = append(bad, di)
			}
		}
		if len(bad) == 0 {
			continue
		}
		if len(s.disks)-len(bad) < m.Erasure.DataBlocks {
			return metaFixed, shardFixed, ErrReadQuorum
		}
		for _, di := range bad {
			if s.rebuildShard(ctx, bucket, partPath, pi, di, pm, m, dist, fullShard) == nil {
				shardFixed++
			}
		}
	}
	return metaFixed, shardFixed, nil
}

// shardIntact reports whether disk di holds a complete, bitrot-valid shard
// file for the given part.
func (s *Set) shardIntact(ctx context.Context, d Disk, bucket, partPath string, partIdx, diskIdx int, pm PartMeta, m *XLMeta) bool {
	rc, err := d.ReadFileStream(ctx, bucket, partPath, 0, -1)
	if err != nil {
		return false
	}
	defer rc.Close()

	interleaved := pm.Bitrot == bitrotInterleaved
	stripeInput := m.Erasure.BlockSize * int64(m.Erasure.DataBlocks)
	remaining := pm.Size
	stripe := 0
	for remaining > 0 {
		logical := stripeInput
		if remaining < stripeInput {
			logical = remaining
		}
		shardLen := ceilInt64(logical, int64(m.Erasure.DataBlocks))
		var hbuf []byte
		if interleaved {
			hbuf = make([]byte, bitrotHashSize)
			if _, err := io.ReadFull(rc, hbuf); err != nil {
				return false
			}
		}
		buf := make([]byte, shardLen)
		if _, err := io.ReadFull(rc, buf); err != nil {
			return false
		}
		if interleaved {
			if !bitrotEqual(hbuf, bitrotRaw(buf)) {
				return false
			}
		} else if stripe < len(pm.Checksums) && diskIdx < len(pm.Checksums[stripe]) {
			if bitrotSum(buf) != pm.Checksums[stripe][diskIdx] {
				return false
			}
		}
		remaining -= logical
		stripe++
	}
	return true
}

// rebuildShard reconstructs disk di's shard file for a part and writes it back.
func (s *Set) rebuildShard(ctx context.Context, bucket, partPath string, partIdx, di int, pm PartMeta, m *XLMeta, dist []int, fullShard int64) error {
	// target shard position j within the stripe (dist[j] == di)
	targetJ := -1
	for j, dd := range dist {
		if dd == di {
			targetJ = j
			break
		}
	}
	if targetJ < 0 {
		return nil
	}

	// open readers for every OTHER disk
	readers := make([]io.ReadCloser, s.n())
	for k, d := range s.disks {
		if k == di {
			continue
		}
		rc, err := d.ReadFileStream(ctx, bucket, partPath, 0, -1)
		if err == nil {
			readers[k] = rc
		}
	}
	defer func() {
		for _, rc := range readers {
			if rc != nil {
				_ = rc.Close()
			}
		}
	}()

	interleaved := pm.Bitrot == bitrotInterleaved
	stripeInput := m.Erasure.BlockSize * int64(m.Erasure.DataBlocks)
	var out bytes.Buffer
	remaining := pm.Size
	stripeIdx := 0

	for remaining > 0 {
		logical := stripeInput
		if remaining < stripeInput {
			logical = remaining
		}
		shardLen := int(ceilInt64(logical, int64(m.Erasure.DataBlocks)))

		shards := make([][]byte, s.n())
		have := 0
		for j := 0; j < s.n(); j++ {
			srcDisk := dist[j]
			if srcDisk == di || readers[srcDisk] == nil {
				continue
			}
			var hbuf []byte
			if interleaved {
				hbuf = make([]byte, bitrotHashSize)
				if _, err := io.ReadFull(readers[srcDisk], hbuf); err != nil {
					readers[srcDisk] = nil
					continue
				}
			}
			buf := make([]byte, shardLen)
			if _, err := io.ReadFull(readers[srcDisk], buf); err != nil {
				readers[srcDisk] = nil
				continue
			}
			if interleaved {
				if !bitrotEqual(hbuf, bitrotRaw(buf)) {
					continue
				}
			} else if stripeIdx < len(pm.Checksums) && srcDisk < len(pm.Checksums[stripeIdx]) &&
				bitrotSum(buf) != pm.Checksums[stripeIdx][srcDisk] {
				continue
			}
			shards[j] = buf
			have++
		}
		if have < m.Erasure.DataBlocks {
			return ErrReadQuorum
		}
		if err := s.ec.Reconstruct(shards); err != nil {
			return err
		}
		if interleaved {
			out.Write(bitrotRaw(shards[targetJ]))
		}
		out.Write(shards[targetJ])
		remaining -= logical
		stripeIdx++
	}

	return s.disks[di].CreateFile(ctx, bucket, partPath, int64(out.Len()), bytes.NewReader(out.Bytes()))
}
