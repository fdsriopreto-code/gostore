package erasure

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"path"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lojadopocket/gostore/internal/sse"

	"github.com/lojadopocket/gostore/internal/storage"
)

// Set is one erasure set: a fixed group of disks over which objects are
// Reed-Solomon coded. All disks mirror the bucket namespace and each holds
// one shard per object part plus a full xl.meta copy.
type Set struct {
	disks        []Disk
	ec           *Erasure
	dataBlocks   int
	parityBlocks int

	// mrf, if set, is called when a write commits on a quorum but not on
	// every disk, so a background worker can re-heal the object.
	mrf func(bucket, key string)
}

// NewSet builds a set from n disks (n even, >= 4). Parity defaults to n/2.
func NewSet(disks []Disk) (*Set, error) {
	n := len(disks)
	if n < 4 || n%2 != 0 {
		return nil, fmt.Errorf("erasure: a set needs an even number of disks >= 4, got %d", n)
	}
	parity := defaultParity(n)
	ec, err := NewErasure(n-parity, parity)
	if err != nil {
		return nil, err
	}
	return &Set{disks: disks, ec: ec, dataBlocks: n - parity, parityBlocks: parity}, nil
}

func (s *Set) n() int          { return len(s.disks) }
func (s *Set) readQuorum() int { return s.dataBlocks }
func (s *Set) writeQuorum() int { // dataBlocks+1, but never more than n
	if s.dataBlocks+1 > s.n() {
		return s.n()
	}
	return s.dataBlocks + 1
}

const (
	metaFile    = "xl.meta"
	tmpPrefix   = ".gostore.sys/tmp"
	mpartPrefix = ".gostore.sys/multipart"
)

func partFile(n int) string { return fmt.Sprintf("part.%05d", n) }

// ---------------------------------------------------------------------------
// Buckets — fan out to every disk, apply quorum.
// ---------------------------------------------------------------------------

func (s *Set) MakeBucket(ctx context.Context, bucket string) error {
	errs := s.forEachDisk(func(d Disk) error { return d.MakeVol(ctx, bucket) })
	ok, exists := 0, 0
	for _, e := range errs {
		switch {
		case e == nil:
			ok++
		case errors.Is(e, storage.ErrVolumeExists):
			exists++
		}
	}
	if ok+exists >= s.writeQuorum() {
		if ok == 0 {
			return storage.ErrVolumeExists
		}
		return nil
	}
	return ErrWriteQuorum
}

func (s *Set) DeleteBucket(ctx context.Context, bucket string, force bool) error {
	errs := s.forEachDisk(func(d Disk) error { return d.DeleteVol(ctx, bucket, force) })
	notEmpty, ok := 0, 0
	for _, e := range errs {
		switch {
		case e == nil || errors.Is(e, storage.ErrVolumeNotFound):
			ok++
		case errors.Is(e, storage.ErrVolumeNotEmpty):
			notEmpty++
		}
	}
	if notEmpty > 0 {
		return storage.ErrVolumeNotEmpty
	}
	if ok >= s.writeQuorum() {
		return nil
	}
	return ErrWriteQuorum
}

// statBucketCalls counts fan-out bucket-existence checks; the Pool's
// positive cache should keep this near zero on a hot bucket. Test signal.
var statBucketCalls atomic.Int64

func (s *Set) StatBucket(ctx context.Context, bucket string) (storage.VolInfo, error) {
	statBucketCalls.Add(1)
	type res struct {
		vi  storage.VolInfo
		err error
	}
	out := make([]res, s.n())
	var wg sync.WaitGroup
	for i, d := range s.disks {
		wg.Add(1)
		go func(i int, d Disk) {
			defer wg.Done()
			vi, err := d.StatVol(ctx, bucket)
			out[i] = res{vi, err}
		}(i, d)
	}
	wg.Wait()

	found := 0
	var earliest storage.VolInfo
	for _, r := range out {
		if r.err == nil {
			found++
			if earliest.Created.IsZero() || r.vi.Created.Before(earliest.Created) {
				earliest = r.vi
			}
		}
	}
	if found >= s.readQuorum() {
		return earliest, nil
	}
	return storage.VolInfo{}, storage.ErrVolumeNotFound
}

func (s *Set) ListBuckets(ctx context.Context) ([]storage.VolInfo, error) {
	count := map[string]int{}
	created := map[string]time.Time{}
	var wg sync.WaitGroup
	var mu sync.Mutex
	for _, d := range s.disks {
		wg.Add(1)
		go func(d Disk) {
			defer wg.Done()
			vols, err := d.ListVols(ctx)
			if err != nil {
				return
			}
			mu.Lock()
			for _, v := range vols {
				count[v.Name]++
				if c, ok := created[v.Name]; !ok || v.Created.Before(c) {
					created[v.Name] = v.Created
				}
			}
			mu.Unlock()
		}(d)
	}
	wg.Wait()

	var out []storage.VolInfo
	for name, c := range count {
		if c >= s.readQuorum() {
			out = append(out, storage.VolInfo{Name: name, Created: created[name]})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// ---------------------------------------------------------------------------
// Object write
// ---------------------------------------------------------------------------

// partSource is one part of an object to be encoded.
type partSource struct {
	Number int
	Size   int64 // logical size, or -1 if unknown (streamed)
	Reader io.Reader
}

// putObject erasure-codes the given parts into bucket/key and writes an
// identical xl.meta to every disk, all under a per-op staging dir that is
// atomically renamed into place. Returns the committed metadata.
func (s *Set) putObject(ctx context.Context, bucket, key string, parts []partSource, um userMeta) (*XLMeta, error) {
	return s.putObjectSSE(ctx, bucket, key, parts, um, nil)
}

// putObjectSSE is putObject with optional SSE-S3 encryption (single part
// only). When sp != nil the part reader is encrypted before erasure coding
// and the metadata records the plaintext md5 + size.
func (s *Set) putObjectSSE(ctx context.Context, bucket, key string, parts []partSource, um userMeta, sp *sseParams) (*XLMeta, error) {
	staging := path.Join(tmpPrefix, storage.NewID())
	defer s.cleanupStaging(ctx, staging)

	dist := buildDistribution(key, s.n())
	meta := &XLMeta{
		Version: xlMetaVersion,
		ModTime: time.Now().UTC(),
		Erasure: ErasureMeta{
			Algorithm:    "reedsolomon",
			DataBlocks:   s.dataBlocks,
			ParityBlocks: s.parityBlocks,
			BlockSize:    s.ec.blockSize,
			Distribution: dist,
			ChecksumAlgo: BitrotAlgo,
		},
		ContentType: um.contentType,
		ContentEnc:  um.contentEnc,
		UserMeta:    um.user,
		UserTags:    um.tags,
	}

	// Inline path: a single small part is buffered into xl.meta instead of
	// written as one shard file per disk. xl.meta is already replicated to
	// every disk, so the data is protected the same way the metadata is.
	if len(parts) == 1 && inlineMaxBytes > 0 && parts[0].Size >= 0 && parts[0].Size <= inlineMaxBytes {
		return s.putObjectInline(ctx, staging, bucket, key, meta, parts[0], sp)
	}

	var total int64
	partMD5Raw := make([]byte, 0, len(parts)*16)
	shardsPartial := false

	for _, ps := range parts {
		reader := ps.Reader
		if sp != nil {
			reader = sp.wrapForEncrypt(reader)
		}
		ctMD5, written, full, err := s.encodePart(ctx, staging, ps.Number, dist, reader)
		if err != nil {
			return nil, err
		}
		if !full {
			shardsPartial = true
		}
		partETag := ctMD5
		if sp != nil {
			sp.callFinish()
			partETag = sp.plainMD5 // S3 ETag is the plaintext md5
		}
		total += written
		meta.Parts = append(meta.Parts, PartMeta{
			Number: ps.Number, Size: written, ActualSize: written, ETag: partETag,
			Bitrot: bitrotInterleaved,
		})
		if raw, derr := hex.DecodeString(partETag); derr == nil {
			partMD5Raw = append(partMD5Raw, raw...)
		}
	}

	meta.Size = total
	if len(parts) == 1 {
		meta.ETag = meta.Parts[0].ETag
	} else {
		sum := md5.Sum(partMD5Raw)
		meta.ETag = hex.EncodeToString(sum[:]) + "-" + fmt.Sprint(len(parts))
	}
	if sp != nil {
		meta.SSE = sse.Algorithm
		meta.PlainSize = sp.plainLen
		meta.EncDEK = sp.encDEK
		meta.NoncePrefix = hex.EncodeToString(sp.prefix)
	}

	m, err := s.commitMeta(ctx, staging, bucket, key, meta)
	if err == nil && shardsPartial && s.mrf != nil && bucket != "" {
		s.mrf(bucket, key)
	}
	return m, err
}

// putObjectInline buffers a single small part into meta.Inline and commits
// xl.meta (which every disk holds a copy of) — no shard files.
func (s *Set) putObjectInline(ctx context.Context, staging, bucket, key string, meta *XLMeta, ps partSource, sp *sseParams) (*XLMeta, error) {
	reader := ps.Reader
	if sp != nil {
		reader = sp.wrapForEncrypt(reader)
	}
	buf, err := io.ReadAll(io.LimitReader(reader, inlineMaxBytes+1))
	if err != nil {
		return nil, err
	}
	if int64(len(buf)) > inlineMaxBytes {
		// The declared size lied; we've consumed part of the reader, so we
		// cannot cleanly stream now — but the part is still small enough to
		// finish in memory up to a hard ceiling.
		rest, rerr := io.ReadAll(reader)
		if rerr != nil {
			return nil, rerr
		}
		buf = append(buf, rest...)
	}

	partETag := fmt.Sprintf("%x", md5.Sum(buf))
	if sp != nil {
		sp.callFinish()
		partETag = sp.plainMD5
	}

	meta.Size = int64(len(buf))
	meta.ETag = partETag
	meta.Inline = buf
	meta.Parts = []PartMeta{{
		Number: ps.Number, Size: int64(len(buf)), ActualSize: int64(len(buf)), ETag: partETag,
	}}
	if sp != nil {
		meta.SSE = sse.Algorithm
		meta.PlainSize = sp.plainLen
		meta.EncDEK = sp.encDEK
		meta.NoncePrefix = hex.EncodeToString(sp.prefix)
	}
	return s.commitMeta(ctx, staging, bucket, key, meta)
}

// commitMeta writes an identical xl.meta to every disk's staging dir and
// atomically renames staging -> bucket/key on a write quorum.
func (s *Set) commitMeta(ctx context.Context, staging, bucket, key string, meta *XLMeta) (*XLMeta, error) {
	mb, err := meta.marshal()
	if err != nil {
		return nil, err
	}
	metaErrs := s.forEachDisk(func(d Disk) error {
		return d.WriteAll(ctx, "", path.Join(staging, metaFile), mb)
	})
	if okCount(metaErrs) < s.writeQuorum() {
		return nil, ErrWriteQuorum
	}
	commitErrs := s.forEachDisk(func(d Disk) error {
		return d.RenameDir(ctx, "", staging, bucket, key)
	})
	committed := okCount(commitErrs)
	if committed < s.writeQuorum() {
		_ = s.forEachDisk(func(d Disk) error { return d.Delete(ctx, bucket, key, true) })
		return nil, ErrWriteQuorum
	}
	// Quorum met but not every disk took the write: queue a background heal.
	if committed < s.n() && s.mrf != nil && bucket != "" {
		s.mrf(bucket, key)
	}
	return meta, nil
}

// encodePart streams r, erasure-codes it stripe by stripe, and writes one
// shard file per disk under staging in the interleaved-bitrot format: each
// stripe's shard is preceded by its 32-byte HighwayHash. Nothing is recorded
// in xl.meta. Returns the md5 of the plaintext, the number of plaintext
// bytes consumed, and whether every disk took the write.
func (s *Set) encodePart(ctx context.Context, staging string, partNum int, dist []int, r io.Reader) (string, int64, bool, error) {
	n := s.n()
	pipes := make([]*io.PipeWriter, n)
	errCh := make(chan error, n)
	var wg sync.WaitGroup

	for i := 0; i < n; i++ {
		pr, pw := io.Pipe()
		pipes[i] = pw
		wg.Add(1)
		go func(i int, pr *io.PipeReader) {
			defer wg.Done()
			err := s.disks[i].CreateFile(ctx, "", path.Join(staging, partFile(partNum)), -1, pr)
			if err != nil {
				pr.CloseWithError(err)
			}
			errCh <- err
		}(i, pr)
	}

	plainMD5 := md5.New()
	stripe := make([]byte, s.ec.stripeInputSize())
	var total int64
	var encErr error

	for {
		nr, rerr := io.ReadFull(r, stripe)
		if rerr != nil && rerr != io.EOF && rerr != io.ErrUnexpectedEOF {
			encErr = rerr
			break
		}
		if nr > 0 {
			chunk := stripe[:nr]
			plainMD5.Write(chunk)
			total += int64(nr)

			shards, err := s.ec.EncodeData(chunk)
			if err != nil {
				encErr = err
				break
			}
			for j := 0; j < n; j++ {
				di := dist[j]
				if _, err := pipes[di].Write(bitrotRaw(shards[j])); err != nil {
					encErr = err
					break
				}
				if _, err := pipes[di].Write(shards[j]); err != nil {
					encErr = err
					break
				}
			}
			if encErr != nil {
				break
			}
		}
		if rerr != nil { // EOF / ErrUnexpectedEOF => that was the final stripe
			break
		}
	}

	for i := 0; i < n; i++ {
		if encErr != nil {
			_ = pipes[i].CloseWithError(encErr)
		} else {
			_ = pipes[i].Close()
		}
	}
	wg.Wait()
	close(errCh)

	if encErr != nil {
		return "", 0, false, encErr
	}
	writeOK := 0
	for e := range errCh {
		if e == nil {
			writeOK++
		}
	}
	if writeOK < s.writeQuorum() {
		return "", 0, false, ErrWriteQuorum
	}
	return hex.EncodeToString(plainMD5.Sum(nil)), total, writeOK == n, nil
}

// ---------------------------------------------------------------------------
// Object read
// ---------------------------------------------------------------------------

// readMeta reads xl.meta and returns the majority version. It reads from a
// read quorum of disks first and only widens to the rest if they don't
// already agree — so a healthy GET/stat doesn't hit every disk (and, in a
// cluster, doesn't fan out to every peer).
func (s *Set) readMeta(ctx context.Context, bucket, key string) (*XLMeta, error) {
	n := s.n()
	q := s.readQuorum()

	metas := make([]*XLMeta, n)
	read := func(idxs []int) {
		var wg sync.WaitGroup
		for _, i := range idxs {
			if metas[i] != nil {
				continue
			}
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				b, err := s.disks[i].ReadAll(ctx, bucket, path.Join(key, metaFile))
				if err != nil {
					return
				}
				if m, err := unmarshalXLMeta(b); err == nil {
					metas[i] = m
				}
			}(i)
		}
		wg.Wait()
	}
	decide := func() (*XLMeta, int, int) { // winner, winnerCount, haveCount
		tally := map[string]int{}
		sample := map[string]*XLMeta{}
		have := 0
		for _, m := range metas {
			if m == nil {
				continue
			}
			have++
			h := m.contentHash()
			tally[h]++
			sample[h] = m
		}
		best, bestN := "", 0
		for h, c := range tally {
			if c > bestN {
				best, bestN = h, c
			}
		}
		return sample[best], bestN, have
	}

	first := make([]int, 0, q)
	for i := 0; i < q && i < n; i++ {
		first = append(first, i)
	}
	read(first)
	if m, bestN, _ := decide(); bestN >= q {
		return m, nil
	}

	rest := make([]int, 0, n-len(first))
	for i := len(first); i < n; i++ {
		rest = append(rest, i)
	}
	read(rest)
	m, bestN, have := decide()
	if bestN >= q {
		return m, nil
	}
	if have == 0 {
		return nil, storage.ErrFileNotFound
	}
	return nil, ErrCorrupt
}

// getObject streams the logical byte range [off, off+length) of bucket/key
// into w. length < 0 means "to end".
func (s *Set) getObject(ctx context.Context, bucket, key string, off, length int64, w io.Writer) error {
	meta, err := s.readMeta(ctx, bucket, key)
	if err != nil {
		return err
	}
	return s.getObjectMeta(ctx, bucket, key, meta, off, length, w)
}

// getObjectMeta is getObject with the xl.meta already in hand, so a GET that
// already did a stat doesn't pay for a second full metadata read.
func (s *Set) getObjectMeta(ctx context.Context, bucket, key string, meta *XLMeta, off, length int64, w io.Writer) error {
	if off < 0 {
		off = 0
	}
	if length < 0 || off+length > meta.Size {
		length = meta.Size - off
	}
	if length <= 0 {
		return nil
	}

	// Inline objects: bytes live in xl.meta, already recovered by readMeta.
	if meta.Inline != nil {
		end := off + length
		if end > int64(len(meta.Inline)) {
			end = int64(len(meta.Inline))
		}
		if off >= end {
			return nil
		}
		_, werr := w.Write(meta.Inline[off:end])
		return werr
	}

	dist := meta.Erasure.Distribution
	stripeLen := meta.Erasure.BlockSize * int64(meta.Erasure.DataBlocks)
	fullShard := meta.Erasure.BlockSize

	remaining := length
	skip := off // logical bytes to discard before we start emitting

	var partStartLogical int64
	for _, pm := range meta.Parts {
		if remaining <= 0 {
			break
		}
		partEnd := partStartLogical + pm.Size
		if skip >= pm.Size {
			skip -= pm.Size
			partStartLogical = partEnd
			continue
		}
		if err := s.readPart(ctx, bucket, key, meta, pm, dist, stripeLen, fullShard, &skip, &remaining, w); err != nil {
			return err
		}
		partStartLogical = partEnd
		skip = 0
	}
	return nil
}

// readPart decodes and emits the wanted slice of one part. skip/remaining are
// updated in place.
func (s *Set) readPart(ctx context.Context, bucket, key string, meta *XLMeta, pm PartMeta, dist []int, stripeLen, fullShard int64, skip, remaining *int64, w io.Writer) error {
	interleaved := pm.Bitrot == bitrotInterleaved
	var hdr int64
	stride := fullShard
	if interleaved {
		hdr = bitrotHashSize
		stride = fullShard + bitrotHashSize
	}

	firstStripe := *skip / stripeLen
	shardOffset := firstStripe * stride

	readers, closers := s.openShards(ctx, bucket, key, pm.Number, shardOffset)
	defer func() {
		for _, c := range closers {
			if c != nil {
				_ = c.Close()
			}
		}
	}()

	intraSkip := *skip - firstStripe*stripeLen
	partRemaining := pm.Size - firstStripe*stripeLen
	stripeIdx := int(firstStripe)

	for partRemaining > 0 && *remaining > 0 {
		thisStripeLogical := stripeLen
		if partRemaining < stripeLen {
			thisStripeLogical = partRemaining
		}
		thisShardLen := ceilInt64(thisStripeLogical, int64(meta.Erasure.DataBlocks))

		shards := make([][]byte, s.n())
		have := 0
		for j := 0; j < s.n(); j++ {
			di := dist[j]
			rd := readers[di]
			if rd == nil {
				continue
			}
			var hbuf []byte
			if hdr > 0 {
				hbuf = make([]byte, hdr)
				if _, err := io.ReadFull(rd, hbuf); err != nil {
					readers[di] = nil
					continue
				}
			}
			buf := make([]byte, thisShardLen)
			if _, err := io.ReadFull(rd, buf); err != nil {
				readers[di] = nil
				continue
			}
			// Bitrot check: a shard whose hash does not match is treated as
			// missing so Reed-Solomon reconstructs it from the good shards.
			if interleaved {
				if !bitrotEqual(hbuf, bitrotRaw(buf)) {
					continue
				}
			} else if stripeIdx < len(pm.Checksums) && di < len(pm.Checksums[stripeIdx]) {
				if bitrotSum(buf) != pm.Checksums[stripeIdx][di] {
					continue
				}
			}
			shards[j] = buf
			have++
		}
		if have < meta.Erasure.DataBlocks {
			return ErrReadQuorum
		}

		var stripeBuf bytes.Buffer
		if err := s.ec.DecodeData(shards, int(thisStripeLogical), &stripeBuf); err != nil {
			return err
		}
		data := stripeBuf.Bytes()

		if intraSkip > 0 {
			if intraSkip >= int64(len(data)) {
				intraSkip -= int64(len(data))
				data = nil
			} else {
				data = data[intraSkip:]
				intraSkip = 0
			}
		}
		if int64(len(data)) > *remaining {
			data = data[:*remaining]
		}
		if len(data) > 0 {
			if _, err := w.Write(data); err != nil {
				return err
			}
			*remaining -= int64(len(data))
		}
		partRemaining -= thisStripeLogical
		stripeIdx++
	}
	return nil
}

// openShards opens every disk's shard file for a part at the given byte
// offset. Missing/offline disks yield nil entries.
func (s *Set) openShards(ctx context.Context, bucket, key string, partNum int, offset int64) ([]io.Reader, []io.Closer) {
	readers := make([]io.Reader, s.n())
	closers := make([]io.Closer, s.n())
	var wg sync.WaitGroup
	for i, d := range s.disks {
		wg.Add(1)
		go func(i int, d Disk) {
			defer wg.Done()
			rc, err := d.ReadFileStream(ctx, bucket, path.Join(key, partFile(partNum)), offset, -1)
			if err != nil {
				return
			}
			readers[i] = rc
			closers[i] = rc
		}(i, d)
	}
	wg.Wait()
	return readers, closers
}

// ---------------------------------------------------------------------------
// Stat / Delete
// ---------------------------------------------------------------------------

func (s *Set) statObject(ctx context.Context, bucket, key string) (*XLMeta, error) {
	return s.readMeta(ctx, bucket, key)
}

func (s *Set) deleteObject(ctx context.Context, bucket, key string) error {
	errs := s.forEachDisk(func(d Disk) error { return d.Delete(ctx, bucket, key, true) })
	if okCount(errs) < s.writeQuorum() {
		return ErrWriteQuorum
	}
	return nil
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

type userMeta struct {
	contentType string
	contentEnc  string
	user        map[string]string
	tags        string
}

func (s *Set) forEachDisk(fn func(Disk) error) []error {
	errs := make([]error, s.n())
	var wg sync.WaitGroup
	for i, d := range s.disks {
		wg.Add(1)
		go func(i int, d Disk) {
			defer wg.Done()
			errs[i] = fn(d)
		}(i, d)
	}
	wg.Wait()
	return errs
}

func (s *Set) cleanupStaging(ctx context.Context, staging string) {
	_ = s.forEachDisk(func(d Disk) error { return d.Delete(ctx, "", staging, true) })
}

func okCount(errs []error) int {
	n := 0
	for _, e := range errs {
		if e == nil {
			n++
		}
	}
	return n
}
