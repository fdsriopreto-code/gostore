package api

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"time"

	"github.com/lojadopocket/gostore/internal/configstore"
	"github.com/lojadopocket/gostore/internal/object"
)

// Bucket snapshots + point-in-time restore ("time travel"). A snapshot records
// the version id of every live object at an instant; restore rolls every
// object back to that version — non-destructively, because each rollback is a
// *new* version, so a restore can itself be undone. Built entirely on
// versioning primitives. Neither AWS S3 nor MinIO's community edition offers
// one-shot bucket time travel.

const (
	snapMaxEntries = 200_000
	snapKeyPrefix  = "snapshots/"
)

type snapEntry struct {
	Key       string `json:"k"`
	VersionID string `json:"v"`
	Size      int64  `json:"s"`
	ETag      string `json:"e,omitempty"`
}

type snapshotManifest struct {
	ID        string      `json:"id"`
	Bucket    string      `json:"bucket"`
	CreatedAt time.Time   `json:"createdAt"`
	Objects   int         `json:"objects"`
	Bytes     int64       `json:"bytes"`
	Truncated bool        `json:"truncated,omitempty"`
	Entries   []snapEntry `json:"entries"`
}

func snapConfigKey(bucket, id string) string {
	return snapKeyPrefix + bucket + "/" + id
}

func newSnapID() string {
	var b [4]byte
	_, _ = rand.Read(b[:])
	return time.Now().UTC().Format("20060102T150405Z") + "-" + hex.EncodeToString(b[:])
}

func (s *Server) requireCStore(w http.ResponseWriter) (configstore.Backend, bool) {
	cb, ok := s.obj.(configstore.Backend)
	if !ok {
		writeJSONError(w, http.StatusNotImplemented, "snapshots require the erasure or dir backend")
		return nil, false
	}
	return cb, true
}

// POST /gostore/admin/v1/snapshot?bucket=X
func (s *Server) adminSnapshotCreate(w http.ResponseWriter, r *http.Request) {
	bucket := r.URL.Query().Get("bucket")
	if bucket == "" {
		writeJSONError(w, http.StatusBadRequest, "bucket query param required")
		return
	}
	cb, ok := s.requireCStore(w)
	if !ok {
		return
	}
	if s.bcfg != nil && s.bcfg.Get(bucket).Versioning != "Enabled" {
		writeJSONError(w, http.StatusConflict, "enable bucket versioning before taking a snapshot (restore needs prior versions to roll back to)")
		return
	}

	ctx := r.Context()
	man := snapshotManifest{ID: newSnapID(), Bucket: bucket, CreatedAt: time.Now().UTC()}
	marker, vmarker := "", ""
	for {
		lv, err := s.obj.ListObjectVersions(ctx, bucket, "", marker, vmarker, "", 1000)
		if err != nil {
			writeJSONError(w, http.StatusInternalServerError, err.Error())
			return
		}
		for _, o := range lv.Objects {
			if !o.IsLatest || o.DeleteMarker {
				continue
			}
			if len(man.Entries) >= snapMaxEntries {
				man.Truncated = true
				break
			}
			vid := o.VersionID
			if vid == "" {
				vid = "null"
			}
			man.Entries = append(man.Entries, snapEntry{Key: o.Name, VersionID: vid, Size: o.Size, ETag: o.ETag})
			man.Bytes += o.Size
		}
		if man.Truncated || !lv.IsTruncated {
			break
		}
		marker, vmarker = lv.NextMarker, lv.NextVersionIDMarker
	}
	man.Objects = len(man.Entries)

	blob, _ := json.Marshal(man)
	if err := cb.WriteConfig(ctx, snapConfigKey(bucket, man.ID), blob); err != nil {
		writeJSONError(w, http.StatusInternalServerError, "persist snapshot: "+err.Error())
		return
	}
	man.Entries = nil // don't echo the full manifest
	writeJSON(w, http.StatusCreated, man)
}

// GET /gostore/admin/v1/snapshots?bucket=X
func (s *Server) adminSnapshotList(w http.ResponseWriter, r *http.Request) {
	bucket := r.URL.Query().Get("bucket")
	cb, ok := s.requireCStore(w)
	if !ok {
		return
	}
	keys, err := cb.ListConfig(r.Context(), snapKeyPrefix+bucket)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	out := make([]snapshotManifest, 0, len(keys))
	for _, k := range keys {
		b, err := cb.ReadConfig(r.Context(), k)
		if err != nil {
			continue
		}
		var m snapshotManifest
		if json.Unmarshal(b, &m) == nil {
			m.Entries = nil
			out = append(out, m)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.After(out[j].CreatedAt) })
	writeJSON(w, http.StatusOK, out)
}

// DELETE /gostore/admin/v1/snapshot?bucket=X&id=Y
func (s *Server) adminSnapshotDelete(w http.ResponseWriter, r *http.Request) {
	bucket, id := r.URL.Query().Get("bucket"), r.URL.Query().Get("id")
	cb, ok := s.requireCStore(w)
	if !ok {
		return
	}
	if bucket == "" || id == "" {
		writeJSONError(w, http.StatusBadRequest, "bucket and id required")
		return
	}
	_ = cb.DeleteConfig(r.Context(), snapConfigKey(bucket, id))
	writeJSON(w, http.StatusOK, map[string]any{"deleted": id})
}

// POST /gostore/admin/v1/snapshot/restore?bucket=X&id=Y[&dryRun=1]
func (s *Server) adminSnapshotRestore(w http.ResponseWriter, r *http.Request) {
	bucket, id := r.URL.Query().Get("bucket"), r.URL.Query().Get("id")
	dryRun := r.URL.Query().Get("dryRun") == "1"
	cb, ok := s.requireCStore(w)
	if !ok {
		return
	}
	if bucket == "" || id == "" {
		writeJSONError(w, http.StatusBadRequest, "bucket and id required")
		return
	}
	blob, err := cb.ReadConfig(r.Context(), snapConfigKey(bucket, id))
	if err != nil {
		writeJSONError(w, http.StatusNotFound, "no such snapshot")
		return
	}
	var man snapshotManifest
	if err := json.Unmarshal(blob, &man); err != nil {
		writeJSONError(w, http.StatusInternalServerError, "corrupt snapshot manifest")
		return
	}

	ctx := r.Context()
	want := make(map[string]string, len(man.Entries)) // key -> version at snapshot time
	for _, e := range man.Entries {
		want[e.Key] = e.VersionID
	}

	// Current live state.
	curLatest := map[string]string{}
	marker, vmarker := "", ""
	for {
		lv, lerr := s.obj.ListObjectVersions(ctx, bucket, "", marker, vmarker, "", 1000)
		if lerr != nil {
			writeJSONError(w, http.StatusInternalServerError, lerr.Error())
			return
		}
		for _, o := range lv.Objects {
			if !o.IsLatest {
				continue
			}
			if o.DeleteMarker {
				curLatest[o.Name] = "" // currently deleted
				continue
			}
			vid := o.VersionID
			if vid == "" {
				vid = "null"
			}
			curLatest[o.Name] = vid
		}
		if !lv.IsTruncated {
			break
		}
		marker, vmarker = lv.NextMarker, lv.NextVersionIDMarker
	}

	var restored, removed, unchanged int
	// Roll every snapshot object back to its recorded version.
	for _, e := range man.Entries {
		if curLatest[e.Key] == e.VersionID {
			unchanged++
			continue
		}
		restored++
		if dryRun {
			continue
		}
		if _, cerr := s.obj.CopyObject(ctx, bucket, e.Key, bucket, e.Key, object.ObjectInfo{},
			object.ObjectOptions{VersionID: e.VersionID},
			object.ObjectOptions{Versioned: true}); cerr != nil {
			writeJSONError(w, http.StatusInternalServerError,
				fmt.Sprintf("restore %q@%s: %v", e.Key, e.VersionID, cerr))
			return
		}
	}
	// Objects created after the snapshot: add a delete marker so the bucket
	// matches the point-in-time state (still non-destructive).
	for key, vid := range curLatest {
		if vid == "" { // already a delete marker
			continue
		}
		if _, inSnap := want[key]; inSnap {
			continue
		}
		removed++
		if dryRun {
			continue
		}
		if _, derr := s.obj.DeleteObject(ctx, bucket, key, object.ObjectOptions{Versioned: true}); derr != nil {
			writeJSONError(w, http.StatusInternalServerError, "remove post-snapshot object "+key+": "+derr.Error())
			return
		}
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"snapshot": id, "dryRun": dryRun,
		"restored": restored, "removed": removed, "unchanged": unchanged,
		"note": "non-destructive: every change is a new version; take a fresh snapshot first if you might want to undo",
	})
}
