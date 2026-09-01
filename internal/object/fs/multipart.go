package fs

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/lojadopocket/gostore/internal/object"
)

type mpUploadMeta struct {
	Bucket      string            `json:"bucket"`
	Object      string            `json:"object"`
	Initiated   time.Time         `json:"initiated"`
	ContentType string            `json:"contentType,omitempty"`
	ContentEnc  string            `json:"contentEncoding,omitempty"`
	UserMeta    map[string]string `json:"userMeta,omitempty"`
	UserTags    string            `json:"userTags,omitempty"`
}

type mpPartMeta struct {
	Number     int       `json:"n"`
	Size       int64     `json:"size"`
	ActualSize int64     `json:"actualSize"`
	ETag       string    `json:"etag"`
	Modified   time.Time `json:"modified"`
}

func partDataName(n int) string { return fmt.Sprintf("part.%05d", n) }
func partMetaName(n int) string { return fmt.Sprintf("part.%05d.json", n) }

func (f *FS) NewMultipartUpload(_ context.Context, bucket, obj string, opts object.ObjectOptions) (*object.NewMultipartUploadResult, error) {
	if err := f.ensureBucket(bucket); err != nil {
		return nil, err
	}
	if !validObjectName(obj) {
		return nil, object.ErrObjectNameInvalid
	}
	uploadID := newID()
	dir := f.mpDir(bucket, uploadID)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	um := mpUploadMeta{
		Bucket: bucket, Object: obj, Initiated: time.Now().UTC(),
		ContentType: opts.UserDefined["content-type"],
		ContentEnc:  opts.UserDefined["content-encoding"],
		UserMeta:    stripReserved(opts.UserDefined),
		UserTags:    opts.UserTags,
	}
	b, _ := json.Marshal(um)
	if err := writeFileAtomic(filepath.Join(dir, "upload.json"), b, 0o644); err != nil {
		return nil, err
	}
	return &object.NewMultipartUploadResult{UploadID: uploadID}, nil
}

func (f *FS) loadUpload(bucket, uploadID string) (mpUploadMeta, error) {
	b, err := os.ReadFile(filepath.Join(f.mpDir(bucket, uploadID), "upload.json"))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return mpUploadMeta{}, object.ErrInvalidUploadID
		}
		return mpUploadMeta{}, err
	}
	var um mpUploadMeta
	if err := json.Unmarshal(b, &um); err != nil {
		return mpUploadMeta{}, err
	}
	return um, nil
}

func (f *FS) PutObjectPart(_ context.Context, bucket, obj, uploadID string, partID int, data *object.PutObjReader, _ object.ObjectOptions) (object.PartInfo, error) {
	if _, err := f.loadUpload(bucket, uploadID); err != nil {
		return object.PartInfo{}, err
	}
	if partID < 1 || partID > 10000 {
		return object.PartInfo{}, object.ErrInvalidPart
	}
	dir := f.mpDir(bucket, uploadID)
	tmp, n, md5hex, err := f.copyToTmpAndHash(data, data.Size())
	if err != nil {
		return object.PartInfo{}, err
	}
	defer os.Remove(tmp)
	if err := os.Rename(tmp, filepath.Join(dir, partDataName(partID))); err != nil {
		return object.PartInfo{}, err
	}
	pm := mpPartMeta{Number: partID, Size: n, ActualSize: n, ETag: md5hex, Modified: time.Now().UTC()}
	pb, _ := json.Marshal(pm)
	if err := writeFileAtomic(filepath.Join(dir, partMetaName(partID)), pb, 0o644); err != nil {
		return object.PartInfo{}, err
	}
	return object.PartInfo{PartNumber: partID, ETag: md5hex, Size: n, ActualSize: n, LastModified: pm.Modified}, nil
}

func (f *FS) CopyObjectPart(ctx context.Context, srcBucket, srcObject, dstBucket, dstObject, uploadID string, partID int, startOffset, length int64, srcInfo object.ObjectInfo, srcOpts, dstOpts object.ObjectOptions) (object.PartInfo, error) {
	src, err := os.Open(f.objDataPath(srcBucket, srcObject))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return object.PartInfo{}, object.ObjectNotFound{Bucket: srcBucket, Object: srcObject}
		}
		return object.PartInfo{}, err
	}
	defer src.Close()
	if startOffset > 0 {
		if _, err := src.Seek(startOffset, io.SeekStart); err != nil {
			return object.PartInfo{}, err
		}
	}
	var r io.Reader = src
	if length >= 0 {
		r = io.LimitReader(src, length)
	}
	return f.PutObjectPart(ctx, dstBucket, dstObject, uploadID, partID,
		object.NewPutObjReader(r, length, length), object.ObjectOptions{})
}

func (f *FS) listParts(bucket, uploadID string) ([]mpPartMeta, error) {
	ents, err := os.ReadDir(f.mpDir(bucket, uploadID))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, object.ErrInvalidUploadID
		}
		return nil, err
	}
	var parts []mpPartMeta
	for _, e := range ents {
		if !strings.HasPrefix(e.Name(), "part.") || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		b, err := os.ReadFile(filepath.Join(f.mpDir(bucket, uploadID), e.Name()))
		if err != nil {
			return nil, err
		}
		var pm mpPartMeta
		if err := json.Unmarshal(b, &pm); err != nil {
			return nil, err
		}
		parts = append(parts, pm)
	}
	sort.Slice(parts, func(i, j int) bool { return parts[i].Number < parts[j].Number })
	return parts, nil
}

func (f *FS) ListObjectParts(_ context.Context, bucket, obj, uploadID string, partNumberMarker, maxParts int, _ object.ObjectOptions) (object.ListPartsInfo, error) {
	um, err := f.loadUpload(bucket, uploadID)
	if err != nil {
		return object.ListPartsInfo{}, err
	}
	all, err := f.listParts(bucket, uploadID)
	if err != nil {
		return object.ListPartsInfo{}, err
	}
	if maxParts <= 0 || maxParts > 1000 {
		maxParts = 1000
	}
	res := object.ListPartsInfo{
		Bucket: bucket, Object: obj, UploadID: uploadID,
		PartNumberMarker: partNumberMarker, MaxParts: maxParts,
		UserDefined: um.UserMeta,
	}
	for _, p := range all {
		if p.Number <= partNumberMarker {
			continue
		}
		if len(res.Parts) >= maxParts {
			res.IsTruncated = true
			break
		}
		res.Parts = append(res.Parts, object.PartInfo{
			PartNumber: p.Number, ETag: p.ETag, Size: p.Size,
			ActualSize: p.ActualSize, LastModified: p.Modified,
		})
		res.NextPartNumberMarker = p.Number
	}
	return res, nil
}

func (f *FS) ListMultipartUploads(_ context.Context, bucket, prefix, keyMarker, uploadIDMarker, delimiter string, maxUploads int) (object.ListMultipartsInfo, error) {
	if err := f.ensureBucket(bucket); err != nil {
		return object.ListMultipartsInfo{}, err
	}
	ents, err := os.ReadDir(f.mpBucketDir(bucket))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return object.ListMultipartsInfo{MaxUploads: maxUploads}, nil
		}
		return object.ListMultipartsInfo{}, err
	}
	if maxUploads <= 0 || maxUploads > 1000 {
		maxUploads = 1000
	}
	var ups []object.MultipartInfo
	for _, e := range ents {
		if !e.IsDir() {
			continue
		}
		um, err := f.loadUpload(bucket, e.Name())
		if err != nil {
			continue
		}
		if prefix != "" && !strings.HasPrefix(um.Object, prefix) {
			continue
		}
		ups = append(ups, object.MultipartInfo{
			Bucket: bucket, Object: um.Object, UploadID: e.Name(),
			Initiated: um.Initiated, UserDefined: um.UserMeta,
		})
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
			res.NextKeyMarker = u.Object
			res.NextUploadIDMarker = u.UploadID
			break
		}
		res.Uploads = append(res.Uploads, u)
	}
	return res, nil
}

func (f *FS) AbortMultipartUpload(_ context.Context, bucket, obj, uploadID string, _ object.ObjectOptions) error {
	if _, err := f.loadUpload(bucket, uploadID); err != nil {
		return err
	}
	return os.RemoveAll(f.mpDir(bucket, uploadID))
}

func (f *FS) CompleteMultipartUpload(_ context.Context, bucket, obj, uploadID string, uploadedParts []object.CompletePart, _ object.ObjectOptions) (object.ObjectInfo, error) {
	um, err := f.loadUpload(bucket, uploadID)
	if err != nil {
		return object.ObjectInfo{}, err
	}
	if len(uploadedParts) == 0 {
		return object.ObjectInfo{}, object.ErrInvalidPart
	}
	haveParts, err := f.listParts(bucket, uploadID)
	if err != nil {
		return object.ObjectInfo{}, err
	}
	byNum := map[int]mpPartMeta{}
	for _, p := range haveParts {
		byNum[p.Number] = p
	}

	lk := f.NewNSLock(bucket, obj)
	ctx2, _ := lk.GetLock(context.Background(), 0)
	defer lk.Unlock(ctx2)

	// Validate: ascending order, parts exist, ETags match, size >= min (except last).
	prev := 0
	var totalSize int64
	etagConcat := make([]byte, 0, len(uploadedParts)*16)
	var metaParts []objMetaPart
	for i, cp := range uploadedParts {
		if cp.PartNumber <= prev {
			return object.ObjectInfo{}, object.ErrInvalidPartOrder
		}
		prev = cp.PartNumber
		pm, ok := byNum[cp.PartNumber]
		if !ok {
			return object.ObjectInfo{}, object.ErrInvalidPart
		}
		if normalizeETag(cp.ETag) != pm.ETag {
			return object.ObjectInfo{}, object.ErrInvalidPart
		}
		if i < len(uploadedParts)-1 && pm.Size < minPartSize {
			return object.ObjectInfo{}, object.ErrPartTooSmall
		}
		raw, decErr := hex.DecodeString(pm.ETag)
		if decErr != nil {
			return object.ObjectInfo{}, decErr
		}
		etagConcat = append(etagConcat, raw...)
		totalSize += pm.Size
		metaParts = append(metaParts, objMetaPart{
			Number: pm.Number, Size: pm.Size, ActualSize: pm.ActualSize, ETag: pm.ETag,
		})
	}

	// Assemble parts into a single tmp file.
	tmp := f.tmpPath()
	out, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return object.ObjectInfo{}, err
	}
	for _, cp := range uploadedParts {
		pf, err := os.Open(filepath.Join(f.mpDir(bucket, uploadID), partDataName(cp.PartNumber)))
		if err != nil {
			_ = out.Close()
			_ = os.Remove(tmp)
			return object.ObjectInfo{}, err
		}
		if _, err := io.Copy(out, pf); err != nil {
			_ = pf.Close()
			_ = out.Close()
			_ = os.Remove(tmp)
			return object.ObjectInfo{}, err
		}
		_ = pf.Close()
	}
	if err := out.Sync(); err != nil {
		_ = out.Close()
		_ = os.Remove(tmp)
		return object.ObjectInfo{}, err
	}
	_ = out.Close()

	h := newMD5()
	h.Write(etagConcat)
	multiETag := hex.EncodeToString(h.Sum(nil)) + "-" + strconv.Itoa(len(uploadedParts))

	dataPath := f.objDataPath(bucket, obj)
	if err := f.checkPathConflicts(bucket, obj); err != nil {
		_ = os.Remove(tmp)
		return object.ObjectInfo{}, err
	}
	if err := os.MkdirAll(filepath.Dir(dataPath), 0o755); err != nil {
		_ = os.Remove(tmp)
		return object.ObjectInfo{}, err
	}
	if err := os.Rename(tmp, dataPath); err != nil {
		_ = os.Remove(tmp)
		return object.ObjectInfo{}, err
	}

	m := objMeta{
		Size: totalSize, ModTime: time.Now().UTC(), ETag: multiETag,
		ContentType: um.ContentType, ContentEnc: um.ContentEnc,
		UserMeta: um.UserMeta, UserTags: um.UserTags, Parts: metaParts,
	}
	if err := writeMetaFile(f.objMetaPath(bucket, obj), m); err != nil {
		return object.ObjectInfo{}, err
	}
	_ = os.RemoveAll(f.mpDir(bucket, uploadID))
	return m.toObjectInfo(bucket, obj), nil
}

func normalizeETag(s string) string {
	s = strings.TrimSpace(s)
	s = strings.TrimPrefix(s, "\"")
	s = strings.TrimSuffix(s, "\"")
	return strings.ToLower(s)
}
