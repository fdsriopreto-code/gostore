// Package cluster turns a set of gostore nodes into one logical erasure pool.
// Each node exposes its local disks over an internal HTTP RPC (bearer-token
// authed — run cluster traffic on a trusted network or behind mTLS); every
// node builds an erasure.Disk list mixing local disks with RemoteDisk
// clients for the peers. Namespace locks are coordinated with a quorum
// (dsync-lite) protocol — see dsync.go.
package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/lojadopocket/gostore/internal/storage"
)

// RemoteDisk is an erasure.Disk backed by a peer node's internal RPC.
type RemoteDisk struct {
	base   string // e.g. https://node2:9000
	idx    int
	secret string
	hc     *http.Client
}

// NewRemoteDisk builds a client for disk `idx` on the peer at base.
func NewRemoteDisk(base string, idx int, secret string) *RemoteDisk {
	return &RemoteDisk{
		base:   strings.TrimRight(base, "/"),
		idx:    idx,
		secret: secret,
		hc:     &http.Client{Timeout: 90 * time.Second},
	}
}

func (r *RemoteDisk) String() string { return fmt.Sprintf("%s#%d", r.base, r.idx) }
func (r *RemoteDisk) ID() string     { return r.base + "#" + strconv.Itoa(r.idx) }
func (r *RemoteDisk) Index() int     { return r.idx }

func (r *RemoteDisk) IsOnline() bool {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	req, _ := r.newReq(ctx, "ping", nil, nil)
	resp, err := r.hc.Do(req)
	if err != nil {
		return false
	}
	_ = resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

func (r *RemoteDisk) newReq(ctx context.Context, op string, q url.Values, body io.Reader) (*http.Request, error) {
	if q == nil {
		q = url.Values{}
	}
	q.Set("disk", strconv.Itoa(r.idx))
	u := r.base + "/gostore/internal/disk/" + op + "?" + q.Encode()
	method := http.MethodPost
	if body == nil {
		method = http.MethodGet
	}
	req, err := http.NewRequestWithContext(ctx, method, u, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-Gostore-Cluster", r.secret)
	return req, nil
}

func (r *RemoteDisk) call(ctx context.Context, op string, q url.Values, body io.Reader, out any) error {
	req, err := r.newReq(ctx, op, q, body)
	if err != nil {
		return err
	}
	resp, err := r.hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return decodeErr(resp.StatusCode, string(b))
	}
	if out != nil {
		return json.NewDecoder(resp.Body).Decode(out)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	return nil
}

// --- erasure.Disk methods -------------------------------------------------

func (r *RemoteDisk) MakeVol(ctx context.Context, bucket string) error {
	return r.call(ctx, "makevol", url.Values{"bucket": {bucket}}, bytes.NewReader(nil), nil)
}

func (r *RemoteDisk) StatVol(ctx context.Context, bucket string) (storage.VolInfo, error) {
	var vi storage.VolInfo
	err := r.call(ctx, "statvol", url.Values{"bucket": {bucket}}, nil, &vi)
	return vi, err
}

func (r *RemoteDisk) ListVols(ctx context.Context) ([]storage.VolInfo, error) {
	var out []storage.VolInfo
	err := r.call(ctx, "listvols", nil, nil, &out)
	return out, err
}

func (r *RemoteDisk) DeleteVol(ctx context.Context, bucket string, force bool) error {
	return r.call(ctx, "deletevol", url.Values{"bucket": {bucket}, "force": {strconv.FormatBool(force)}}, bytes.NewReader(nil), nil)
}

func (r *RemoteDisk) WriteAll(ctx context.Context, bucket, object string, data []byte) error {
	return r.call(ctx, "writeall", url.Values{"bucket": {bucket}, "object": {object}}, bytes.NewReader(data), nil)
}

func (r *RemoteDisk) ReadAll(ctx context.Context, bucket, object string) ([]byte, error) {
	req, err := r.newReq(ctx, "readall", url.Values{"bucket": {bucket}, "object": {object}}, nil)
	if err != nil {
		return nil, err
	}
	resp, err := r.hc.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return nil, decodeErr(resp.StatusCode, string(b))
	}
	return io.ReadAll(resp.Body)
}

func (r *RemoteDisk) CreateFile(ctx context.Context, bucket, object string, size int64, rd io.Reader) error {
	req, err := r.newReq(ctx, "createfile", url.Values{
		"bucket": {bucket}, "object": {object}, "size": {strconv.FormatInt(size, 10)},
	}, rd)
	if err != nil {
		return err
	}
	req.ContentLength = size
	resp, err := r.hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return decodeErr(resp.StatusCode, string(b))
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	return nil
}

func (r *RemoteDisk) ReadFileStream(ctx context.Context, bucket, object string, offset, length int64) (io.ReadCloser, error) {
	req, err := r.newReq(ctx, "readfilestream", url.Values{
		"bucket": {bucket}, "object": {object},
		"offset": {strconv.FormatInt(offset, 10)}, "length": {strconv.FormatInt(length, 10)},
	}, nil)
	if err != nil {
		return nil, err
	}
	resp, err := r.hc.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		_ = resp.Body.Close()
		return nil, decodeErr(resp.StatusCode, string(b))
	}
	return resp.Body, nil
}

func (r *RemoteDisk) RenameDir(ctx context.Context, srcBucket, srcObject, dstBucket, dstObject string) error {
	return r.call(ctx, "renamedir", url.Values{
		"srcBucket": {srcBucket}, "srcObject": {srcObject},
		"dstBucket": {dstBucket}, "dstObject": {dstObject},
	}, bytes.NewReader(nil), nil)
}

func (r *RemoteDisk) Delete(ctx context.Context, bucket, object string, recursive bool) error {
	return r.call(ctx, "delete", url.Values{
		"bucket": {bucket}, "object": {object}, "recursive": {strconv.FormatBool(recursive)},
	}, bytes.NewReader(nil), nil)
}

func (r *RemoteDisk) ListDir(ctx context.Context, bucket, dir string) ([]string, error) {
	var out []string
	err := r.call(ctx, "listdir", url.Values{"bucket": {bucket}, "dir": {dir}}, nil, &out)
	return out, err
}

func (r *RemoteDisk) StagingPath() string {
	return ".gostore.sys/tmp/" + storage.NewID()
}

func (r *RemoteDisk) DiskInfo(ctx context.Context) (storage.DiskInfo, error) {
	var di storage.DiskInfo
	err := r.call(ctx, "diskinfo", nil, nil, &di)
	return di, err
}
