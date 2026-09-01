package fs

import (
	"context"
	"encoding/base64"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/lojadopocket/gostore/internal/object"
)

// walkKeys returns every object key in the bucket, slash-separated, sorted in
// byte order (matching S3 listing order). M1 implementation: full walk +
// in-memory sort. Fine for moderate buckets; a streaming/indexed listing
// arrives with the erasure backend.
func (f *FS) walkKeys(bucket string) ([]string, error) {
	root := f.bucketDir(bucket)
	var keys []string
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if p == root {
			return nil
		}
		rel, rerr := filepath.Rel(root, p)
		if rerr != nil {
			return rerr
		}
		rel = filepath.ToSlash(rel)
		if d.IsDir() {
			return nil
		}
		keys = append(keys, rel)
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Strings(keys)
	return keys, nil
}

type listParams struct {
	prefix, delimiter string
	startAfter        string // exclusive lower bound
	maxKeys           int
}

type listPage struct {
	objects     []object.ObjectInfo
	prefixes    []string
	isTruncated bool
	nextMarker  string
}

func (f *FS) doList(bucket string, p listParams) (listPage, error) {
	if err := f.ensureBucket(bucket); err != nil {
		return listPage{}, err
	}
	if p.maxKeys <= 0 {
		p.maxKeys = 1000
	}
	if p.maxKeys > 1000 {
		p.maxKeys = 1000
	}
	keys, err := f.walkKeys(bucket)
	if err != nil {
		return listPage{}, err
	}

	var page listPage
	seenPrefix := map[string]bool{}
	count := 0

	for _, k := range keys {
		if p.prefix != "" && !strings.HasPrefix(k, p.prefix) {
			continue
		}
		if p.startAfter != "" && k <= p.startAfter {
			continue
		}

		if p.delimiter != "" {
			rest := k[len(p.prefix):]
			if idx := strings.Index(rest, p.delimiter); idx >= 0 {
				cp := p.prefix + rest[:idx+len(p.delimiter)]
				if seenPrefix[cp] {
					continue
				}
				if count >= p.maxKeys {
					page.isTruncated = true
					page.nextMarker = lastMarker(page)
					return page, nil
				}
				seenPrefix[cp] = true
				page.prefixes = append(page.prefixes, cp)
				page.nextMarker = cp
				count++
				continue
			}
		}

		if count >= p.maxKeys {
			page.isTruncated = true
			page.nextMarker = lastMarker(page)
			return page, nil
		}
		oi, gerr := f.getInfoUnlocked(bucket, k)
		if gerr != nil {
			if os.IsNotExist(gerr) {
				continue
			}
			return listPage{}, gerr
		}
		page.objects = append(page.objects, oi)
		page.nextMarker = k
		count++
	}
	sort.Strings(page.prefixes)
	return page, nil
}

func lastMarker(p listPage) string {
	last := ""
	if n := len(p.objects); n > 0 {
		last = p.objects[n-1].Name
	}
	if n := len(p.prefixes); n > 0 && p.prefixes[n-1] > last {
		last = p.prefixes[n-1]
	}
	return last
}

func encodeToken(s string) string {
	if s == "" {
		return ""
	}
	return base64.StdEncoding.EncodeToString([]byte(s))
}

func decodeToken(s string) string {
	if s == "" {
		return ""
	}
	b, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return s // tolerate a raw key as token
	}
	return string(b)
}

func (f *FS) ListObjects(_ context.Context, bucket, prefix, marker, delimiter string, maxKeys int) (object.ListObjectsInfo, error) {
	page, err := f.doList(bucket, listParams{
		prefix: prefix, delimiter: delimiter, startAfter: marker, maxKeys: maxKeys,
	})
	if err != nil {
		return object.ListObjectsInfo{}, err
	}
	return object.ListObjectsInfo{
		IsTruncated: page.isTruncated,
		NextMarker:  page.nextMarker,
		Objects:     page.objects,
		Prefixes:    page.prefixes,
	}, nil
}

func (f *FS) ListObjectsV2(_ context.Context, bucket, prefix, continuationToken, delimiter string, maxKeys int, _ bool, startAfter string) (object.ListObjectsV2Info, error) {
	start := startAfter
	if continuationToken != "" {
		start = decodeToken(continuationToken)
	}
	page, err := f.doList(bucket, listParams{
		prefix: prefix, delimiter: delimiter, startAfter: start, maxKeys: maxKeys,
	})
	if err != nil {
		return object.ListObjectsV2Info{}, err
	}
	out := object.ListObjectsV2Info{
		IsTruncated:       page.isTruncated,
		ContinuationToken: continuationToken,
		Objects:           page.objects,
		Prefixes:          page.prefixes,
	}
	if page.isTruncated {
		out.NextContinuationToken = encodeToken(page.nextMarker)
	}
	return out, nil
}

func (f *FS) ListObjectVersions(ctx context.Context, bucket, prefix, marker, versionMarker, delimiter string, maxKeys int) (object.ListObjectVersionsInfo, error) {
	// M1 has no versioning: report the current objects as their sole "null" version.
	li, err := f.ListObjects(ctx, bucket, prefix, marker, delimiter, maxKeys)
	if err != nil {
		return object.ListObjectVersionsInfo{}, err
	}
	for i := range li.Objects {
		li.Objects[i].VersionID = "null"
		li.Objects[i].IsLatest = true
	}
	return object.ListObjectVersionsInfo{
		IsTruncated: li.IsTruncated,
		NextMarker:  li.NextMarker,
		Objects:     li.Objects,
		Prefixes:    li.Prefixes,
	}, nil
}
