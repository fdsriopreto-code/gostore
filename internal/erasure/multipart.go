package erasure

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/lojadopocket/gostore/internal/object"
	"github.com/lojadopocket/gostore/internal/storage"
)

const minPartSize = 5 * 1024 * 1024

type uploadInfo struct {
	Bucket      string            `json:"bucket"`
	Object      string            `json:"object"`
	Initiated   time.Time         `json:"initiated"`
	ContentType string            `json:"contentType,omitempty"`
	ContentEnc  string            `json:"contentEncoding,omitempty"`
	UserMeta    map[string]string `json:"userMeta,omitempty"`
	UserTags    string            `json:"userTags,omitempty"`
}

func mpUploadDir(id string) string  { return path.Join(mpartPrefix, id) }
func mpUploadJSON(id string) string { return path.Join(mpartPrefix, id, "upload.json") }
func mpPartKey(id string, n int) string {
	return path.Join(mpartPrefix, id, fmt.Sprintf("part.%05d", n))
}

func (p *Pool) NewMultipartUpload(ctx context.Context, bucket, key string, opts object.ObjectOptions) (*object.NewMultipartUploadResult, error) {
	if err := p.ensureBucket(ctx, bucket); err != nil {
		return nil, err
	}
	if !validObjectName(key) {
		return nil, object.ErrObjectNameInvalid
	}
	id := storage.NewID()
	um := toUserMeta(opts)
	ui := uploadInfo{
		Bucket: bucket, Object: key, Initiated: time.Now().UTC(),
		ContentType: um.contentType, ContentEnc: um.contentEnc,
		UserMeta: um.user, UserTags: um.tags,
	}
	b, _ := json.Marshal(ui)
	set := p.setFor(key)
	errs := set.forEachDisk(func(d Disk) error { return d.WriteAll(ctx, "", mpUploadJSON(id), b) })
	if okCount(errs) < set.writeQuorum() {
		return nil, object.ErrWriteQuorum
	}
	return &object.NewMultipartUploadResult{UploadID: id}, nil
}

// loadUpload finds an upload's metadata by scanning every set (the upload id
// alone does not tell us which set holds it).
func (p *Pool) loadUpload(ctx context.Context, id string) (uploadInfo, *Set, error) {
	for _, set := range p.sets {
		for _, d := range set.disks {
			b, err := d.ReadAll(ctx, "", mpUploadJSON(id))
			if err != nil {
				continue
			}
			var ui uploadInfo
			if err := json.Unmarshal(b, &ui); err == nil {
				return ui, set, nil
			}
		}
	}
	return uploadInfo{}, nil, object.ErrInvalidUploadID
}

func (p *Pool) PutObjectPart(ctx context.Context, bucket, key, uploadID string, partID int, data *object.PutObjReader, _ object.ObjectOptions) (object.PartInfo, error) {
	_, set, err := p.loadUpload(ctx, uploadID)
	if err != nil {
		return object.PartInfo{}, err
	}
	if partID < 1 || partID > 10000 {
		return object.PartInfo{}, object.ErrInvalidPart
	}
	meta, err := set.putObject(ctx, "", mpPartKey(uploadID, partID), []partSource{
		{Number: 1, Size: data.Size(), Reader: data},
	}, userMeta{})
	if err != nil {
		return object.PartInfo{}, mapErr(err)
	}
	return object.PartInfo{
		PartNumber: partID, ETag: meta.ETag, Size: meta.Size, ActualSize: meta.Size,
		LastModified: meta.ModTime,
	}, nil
}

func (p *Pool) CopyObjectPart(ctx context.Context, srcBucket, srcObject, dstBucket, dstObject, uploadID string, partID int, startOffset, length int64, _ object.ObjectInfo, _, _ object.ObjectOptions) (object.PartInfo, error) {
	gr, err := p.GetObjectNInfo(ctx, srcBucket, srcObject,
		&object.HTTPRangeSpec{Start: startOffset, End: rangeEnd(startOffset, length)}, nil, object.ObjectOptions{})
	if err != nil {
		return object.PartInfo{}, err
	}
	defer gr.Close()
	sz := length
	if sz < 0 {
		sz = gr.ObjInfo.Size
	}
	return p.PutObjectPart(ctx, dstBucket, dstObject, uploadID, partID,
		object.NewPutObjReader(gr, sz, sz), object.ObjectOptions{})
}

func rangeEnd(start, length int64) int64 {
	if length < 0 {
		return -1
	}
	return start + length - 1
}

func (p *Pool) listUploadedParts(ctx context.Context, set *Set, uploadID string) ([]object.PartInfo, error) {
	var disk Disk
	for _, d := range set.disks {
		if d.IsOnline() {
			disk = d
			break
		}
	}
	if disk == nil {
		return nil, object.ErrReadQuorum
	}
	entries, err := disk.ListDir(ctx, "", mpUploadDir(uploadID))
	if err != nil {
		return nil, err
	}
	var parts []object.PartInfo
	for _, e := range entries {
		if !strings.HasPrefix(e, "part.") || !strings.HasSuffix(e, "/") {
			continue
		}
		name := strings.TrimSuffix(e, "/")
		var n int
		if _, err := fmt.Sscanf(name, "part.%05d", &n); err != nil {
			continue
		}
		m, err := set.statObject(ctx, "", mpPartKey(uploadID, n))
		if err != nil {
			continue
		}
		parts = append(parts, object.PartInfo{
			PartNumber: n, ETag: m.ETag, Size: m.Size, ActualSize: m.Size, LastModified: m.ModTime,
		})
	}
	sort.Slice(parts, func(i, j int) bool { return parts[i].PartNumber < parts[j].PartNumber })
	return parts, nil
}

func (p *Pool) ListObjectParts(ctx context.Context, bucket, key, uploadID string, partNumberMarker, maxParts int, _ object.ObjectOptions) (object.ListPartsInfo, error) {
	ui, set, err := p.loadUpload(ctx, uploadID)
	if err != nil {
		return object.ListPartsInfo{}, err
	}
	all, err := p.listUploadedParts(ctx, set, uploadID)
	if err != nil {
		return object.ListPartsInfo{}, err
	}
	if maxParts <= 0 || maxParts > 1000 {
		maxParts = 1000
	}
	res := object.ListPartsInfo{
		Bucket: bucket, Object: key, UploadID: uploadID,
		PartNumberMarker: partNumberMarker, MaxParts: maxParts,
		UserDefined: ui.UserMeta,
	}
	for _, pt := range all {
		if pt.PartNumber <= partNumberMarker {
			continue
		}
		if len(res.Parts) >= maxParts {
			res.IsTruncated = true
			break
		}
		res.Parts = append(res.Parts, pt)
		res.NextPartNumberMarker = pt.PartNumber
	}
	return res, nil
}

func (p *Pool) ListMultipartUploads(ctx context.Context, bucket, prefix, keyMarker, uploadIDMarker, delimiter string, maxUploads int) (object.ListMultipartsInfo, error) {
	if err := p.ensureBucket(ctx, bucket); err != nil {
		return object.ListMultipartsInfo{}, err
	}
	if maxUploads <= 0 || maxUploads > 1000 {
		maxUploads = 1000
	}
	var ups []object.MultipartInfo
	seen := map[string]bool{}
	for _, set := range p.sets {
		var disk Disk
		for _, d := range set.disks {
			if d.IsOnline() {
				disk = d
				break
			}
		}
		if disk == nil {
			continue
		}
		entries, err := disk.ListDir(ctx, "", mpartPrefix)
		if err != nil {
			continue
		}
		for _, e := range entries {
			if !strings.HasSuffix(e, "/") {
				continue
			}
			id := strings.TrimSuffix(e, "/")
			if seen[id] {
				continue
			}
			seen[id] = true
			ui, _, err := p.loadUpload(ctx, id)
			if err != nil || ui.Bucket != bucket {
				continue
			}
			if prefix != "" && !strings.HasPrefix(ui.Object, prefix) {
				continue
			}
			ups = append(ups, object.MultipartInfo{
				Bucket: bucket, Object: ui.Object, UploadID: id,
				Initiated: ui.Initiated, UserDefined: ui.UserMeta,
			})
		}
	}
	sort.Slice(ups, func(i, j int) bool {
		if ups[i].Object != ups[j].Object {
			return ups[i].Object < ups[j].Object
		}
		return ups[i].UploadID < ups[j].UploadID
	})
	res := object.ListMultipartsInfo{MaxUploads: maxUploads}
	for _, u := range ups {
		if len(res.Uploads) >= maxUploads {
			res.IsTruncated = true
			break
		}
		res.Uploads = append(res.Uploads, u)
	}
	return res, nil
}

func (p *Pool) AbortMultipartUpload(ctx context.Context, bucket, key, uploadID string, _ object.ObjectOptions) error {
	_, set, err := p.loadUpload(ctx, uploadID)
	if err != nil {
		return err
	}
	_ = set.forEachDisk(func(d Disk) error { return d.Delete(ctx, "", mpUploadDir(uploadID), true) })
	return nil
}

func (p *Pool) CompleteMultipartUpload(ctx context.Context, bucket, key, uploadID string, uploaded []object.CompletePart, _ object.ObjectOptions) (object.ObjectInfo, error) {
	ui, set, err := p.loadUpload(ctx, uploadID)
	if err != nil {
		return object.ObjectInfo{}, err
	}
	if len(uploaded) == 0 {
		return object.ObjectInfo{}, object.ErrInvalidPart
	}
	have, err := p.listUploadedParts(ctx, set, uploadID)
	if err != nil {
		return object.ObjectInfo{}, err
	}
	byNum := map[int]object.PartInfo{}
	for _, pt := range have {
		byNum[pt.PartNumber] = pt
	}

	lk := p.NewNSLock(bucket, key)
	c, _ := lk.GetLock(ctx, 0)
	defer lk.Unlock(c)

	sources := make([]partSource, 0, len(uploaded))
	readers := make([]*io.PipeReader, 0, len(uploaded))
	prev := 0
	for i, cp := range uploaded {
		if cp.PartNumber <= prev {
			return object.ObjectInfo{}, object.ErrInvalidPartOrder
		}
		prev = cp.PartNumber
		pt, ok := byNum[cp.PartNumber]
		if !ok {
			return object.ObjectInfo{}, object.ErrInvalidPart
		}
		if normalizeETag(cp.ETag) != normalizeETag(pt.ETag) {
			return object.ObjectInfo{}, object.ErrInvalidPart
		}
		if i < len(uploaded)-1 && pt.Size < minPartSize {
			return object.ObjectInfo{}, object.ErrPartTooSmall
		}
		pr, pw := io.Pipe()
		readers = append(readers, pr)
		partKey := mpPartKey(uploadID, cp.PartNumber)
		go func(pw *io.PipeWriter, partKey string) {
			err := set.getObject(ctx, "", partKey, 0, -1, pw)
			_ = pw.CloseWithError(err)
		}(pw, partKey)
		sources = append(sources, partSource{Number: cp.PartNumber, Size: pt.Size, Reader: pr})
	}

	meta, err := set.putObject(ctx, bucket, key, sources, userMeta{
		contentType: ui.ContentType, contentEnc: ui.ContentEnc,
		user: ui.UserMeta, tags: ui.UserTags,
	})
	for _, r := range readers {
		_ = r.Close()
	}
	if err != nil {
		return object.ObjectInfo{}, mapErr(err)
	}

	_ = set.forEachDisk(func(d Disk) error { return d.Delete(ctx, "", mpUploadDir(uploadID), true) })
	return metaToInfo(bucket, key, meta), nil
}

func normalizeETag(s string) string {
	return strings.ToLower(strings.Trim(strings.TrimSpace(s), `"`))
}
