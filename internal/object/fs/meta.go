package fs

import (
	"encoding/json"
	"os"
	"time"

	"github.com/lojadopocket/gostore/internal/object"
)

// objMeta is the on-disk metadata sidecar for one object. Stored as JSON in
// M1 (msgpack + the xl.meta directory layout arrive with erasure coding in
// M4). It is the source of truth for everything the data file itself cannot
// carry: ETag, content-type, user metadata, multipart part list.
type objMeta struct {
	Version     int               `json:"v"`
	Size        int64             `json:"size"`
	ModTime     time.Time         `json:"modTime"`
	ETag        string            `json:"etag"`
	ContentType string            `json:"contentType,omitempty"`
	ContentEnc  string            `json:"contentEncoding,omitempty"`
	UserMeta    map[string]string `json:"userMeta,omitempty"` // without x-amz-meta- prefix
	UserTags    string            `json:"userTags,omitempty"`
	Parts       []objMetaPart     `json:"parts,omitempty"` // set for multipart objects
	// VersionID/DeleteMarker land in M10.
}

type objMetaPart struct {
	Number     int    `json:"n"`
	Size       int64  `json:"size"`
	ActualSize int64  `json:"actualSize"`
	ETag       string `json:"etag"`
}

func (m objMeta) toObjectInfo(bucket, name string) object.ObjectInfo {
	oi := object.ObjectInfo{
		Bucket:          bucket,
		Name:            name,
		Size:            m.Size,
		ModTime:         m.ModTime,
		ETag:            m.ETag,
		ContentType:     m.ContentType,
		ContentEncoding: m.ContentEnc,
		UserDefined:     map[string]string{},
		UserTags:        m.UserTags,
		StorageClass:    "STANDARD",
		IsLatest:        true,
	}
	for k, v := range m.UserMeta {
		oi.UserDefined[k] = v
	}
	if m.ContentType != "" {
		oi.UserDefined["content-type"] = m.ContentType
	}
	for _, p := range m.Parts {
		oi.Parts = append(oi.Parts, object.ObjectPartInfo{
			Number: p.Number, Size: p.Size, ActualSize: p.ActualSize, ETag: p.ETag,
		})
	}
	return oi
}

func writeMetaFile(path string, m objMeta) error {
	m.Version = 1
	b, err := json.Marshal(m)
	if err != nil {
		return err
	}
	return writeFileAtomic(path, b, 0o644)
}

func readMetaFile(path string) (objMeta, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return objMeta{}, err
	}
	var m objMeta
	if err := json.Unmarshal(b, &m); err != nil {
		return objMeta{}, err
	}
	return m, nil
}
