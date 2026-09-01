package api

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/lojadopocket/gostore/internal/object"
)

var disableSelfTest = os.Getenv("GOSTORE_DISABLE_SELFTEST") == "1"

// handleHealthLive is a liveness probe: the process is up and serving.
func (s *Server) handleHealthLive(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

// handleHealthReady is a readiness probe: storage has enough quorum to serve
// reads/writes. In M0 the object layer is a stub, so this reports the stub's
// self-assessment.
func (s *Server) handleHealthReady(w http.ResponseWriter, r *http.Request) {
	res := s.obj.Health(r.Context(), object.HealthOptions{})
	code := http.StatusOK
	if !res.Healthy {
		code = http.StatusServiceUnavailable
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"healthy":       res.Healthy,
		"writeQuorum":   res.WriteQuorum,
		"healingDrives": res.HealingDrives,
		"reason":        res.Reason,
	})
}

// handleSelfTest runs a full write/read/verify/delete round-trip through the
// object layer and reports the outcome as JSON. It lets you validate a
// deployment from a browser when you have no shell / S3 client on the host.
// It creates a throwaway bucket, cleans up after itself, and is unauthenticated
// (health namespace). Disable with GOSTORE_DISABLE_SELFTEST=1.
func (s *Server) handleSelfTest(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")

	if disableSelfTest {
		w.WriteHeader(http.StatusForbidden)
		_ = enc.Encode(map[string]any{"ok": false, "error": "self-test disabled"})
		return
	}

	ctx := r.Context()
	bucket := "gostore-selftest-" + newRequestID()
	key := "probe/data.bin"
	payload := []byte("gostore self-test @ " + time.Now().UTC().Format(time.RFC3339Nano))
	var steps []string
	t0 := time.Now()

	fail := func(step string, err error) {
		_, _ = s.obj.DeleteObject(ctx, bucket, key, object.ObjectOptions{})
		_ = s.obj.DeleteBucket(ctx, bucket, object.DeleteBucketOptions{Force: true})
		w.WriteHeader(http.StatusInternalServerError)
		_ = enc.Encode(map[string]any{
			"ok": false, "failedStep": step, "error": err.Error(),
			"stepsCompleted": steps,
		})
	}

	if err := s.obj.MakeBucket(ctx, bucket, object.MakeBucketOptions{}); err != nil {
		fail("MakeBucket", err)
		return
	}
	steps = append(steps, "MakeBucket")

	oi, err := s.obj.PutObject(ctx, bucket, key,
		object.NewPutObjReader(bytes.NewReader(payload), int64(len(payload)), int64(len(payload))),
		object.ObjectOptions{UserDefined: map[string]string{"content-type": "application/octet-stream"}})
	if err != nil {
		fail("PutObject", err)
		return
	}
	steps = append(steps, "PutObject")

	gr, err := s.obj.GetObjectNInfo(ctx, bucket, key, nil, r.Header, object.ObjectOptions{})
	if err != nil {
		fail("GetObject", err)
		return
	}
	got, _ := io.ReadAll(gr)
	_ = gr.Close()
	steps = append(steps, "GetObject")

	if !bytes.Equal(got, payload) {
		fail("VerifyBytes", errMismatch{len(payload), len(got)})
		return
	}
	steps = append(steps, "VerifyBytes")

	li, err := s.obj.ListObjectsV2(ctx, bucket, "probe/", "", "", 10, false, "")
	if err != nil || len(li.Objects) != 1 {
		fail("ListObjectsV2", errList{err, len(li.Objects)})
		return
	}
	steps = append(steps, "ListObjectsV2")

	if _, err := s.obj.DeleteObject(ctx, bucket, key, object.ObjectOptions{}); err != nil {
		fail("DeleteObject", err)
		return
	}
	steps = append(steps, "DeleteObject")

	if err := s.obj.DeleteBucket(ctx, bucket, object.DeleteBucketOptions{}); err != nil {
		fail("DeleteBucket", err)
		return
	}
	steps = append(steps, "DeleteBucket")

	w.WriteHeader(http.StatusOK)
	_ = enc.Encode(map[string]any{
		"ok":            true,
		"steps":         steps,
		"bytes":         len(payload),
		"etag":          oi.ETag,
		"region":        s.cfg.Region,
		"mode":          "single-disk",
		"durationMillis": time.Since(t0).Milliseconds(),
	})
}

type errMismatch struct{ want, got int }

func (e errMismatch) Error() string {
	return "read-back bytes differ from written (wrote " +
		strconv.Itoa(e.want) + ", read " + strconv.Itoa(e.got) + ")"
}

type errList struct {
	err error
	n   int
}

func (e errList) Error() string {
	if e.err != nil {
		return e.err.Error()
	}
	return "expected 1 object, listed " + strconv.Itoa(e.n)
}
