package cluster

import (
	"context"
	"crypto/hmac"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/lojadopocket/gostore/internal/storage"
)

// diskRPC is what the RPC server needs from a local disk (satisfied by
// *storage.LocalDisk).
type diskRPC interface {
	MakeVol(ctx context.Context, bucket string) error
	StatVol(ctx context.Context, bucket string) (storage.VolInfo, error)
	ListVols(ctx context.Context) ([]storage.VolInfo, error)
	DeleteVol(ctx context.Context, bucket string, force bool) error
	WriteAll(ctx context.Context, bucket, object string, data []byte) error
	ReadAll(ctx context.Context, bucket, object string) ([]byte, error)
	CreateFile(ctx context.Context, bucket, object string, size int64, r io.Reader) error
	ReadFileStream(ctx context.Context, bucket, object string, offset, length int64) (io.ReadCloser, error)
	RenameDir(ctx context.Context, srcBucket, srcObject, dstBucket, dstObject string) error
	Delete(ctx context.Context, bucket, object string, recursive bool) error
	ListDir(ctx context.Context, bucket, dir string) ([]string, error)
	DiskInfo(ctx context.Context) (storage.DiskInfo, error)
}

// RPCServer serves the internal disk + lock RPC for one node.
type RPCServer struct {
	disks  []diskRPC
	secret string
	locks  *lockTable
}

// NewRPCServer builds the handler. localDisks are this node's disks in the
// same order the cluster addresses them.
func NewRPCServer(localDisks []diskRPC, secret string) *RPCServer {
	return &RPCServer{disks: localDisks, secret: secret, locks: newLockTable()}
}

// Handler returns the http.Handler to mount at /gostore/internal/.
func (s *RPCServer) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/gostore/internal/disk/", s.auth(s.handleDisk))
	mux.HandleFunc("/gostore/internal/lock/", s.auth(s.handleLock))
	mux.HandleFunc(gridPath, s.auth(s.handleGrid))
	return mux
}

func (s *RPCServer) auth(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if s.secret == "" || !hmac.Equal([]byte(r.Header.Get("X-Gostore-Cluster")), []byte(s.secret)) {
			http.Error(w, "cluster auth failed", http.StatusForbidden)
			return
		}
		h(w, r)
	}
}

func (s *RPCServer) disk(r *http.Request) (diskRPC, bool) {
	i, err := strconv.Atoi(r.URL.Query().Get("disk"))
	if err != nil || i < 0 || i >= len(s.disks) {
		return nil, false
	}
	return s.disks[i], true
}

func (s *RPCServer) handleDisk(w http.ResponseWriter, r *http.Request) {
	op := r.URL.Path[len("/gostore/internal/disk/"):]
	if op == "ping" {
		w.WriteHeader(http.StatusOK)
		return
	}
	d, ok := s.disk(r)
	if !ok {
		http.Error(w, "bad disk index", http.StatusBadRequest)
		return
	}
	q := r.URL.Query()
	ctx := r.Context()

	// Bulk-streaming ops keep their own HTTP request (a dedicated connection
	// for a multi-MB transfer is fine); everything else shares dispatchUnary.
	switch op {
	case "createfile":
		size, _ := strconv.ParseInt(q.Get("size"), 10, 64)
		writeRPCErr(w, d.CreateFile(ctx, q.Get("bucket"), q.Get("object"), size, r.Body))
		return
	case "readfilestream":
		off, _ := strconv.ParseInt(q.Get("offset"), 10, 64)
		length, _ := strconv.ParseInt(q.Get("length"), 10, 64)
		rc, err := d.ReadFileStream(ctx, q.Get("bucket"), q.Get("object"), off, length)
		if err != nil {
			writeRPCErr(w, err)
			return
		}
		defer rc.Close()
		w.Header().Set("Content-Type", "application/octet-stream")
		_, _ = io.Copy(w, rc)
		return
	}

	var body []byte
	if r.Body != nil {
		body, _ = io.ReadAll(io.LimitReader(r.Body, 64<<20))
	}
	code, errStr, out := dispatchUnary(ctx, d, op, q, body)
	if code != 0 {
		http.Error(w, errStr, int(code))
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	_, _ = w.Write(out)
}

// dispatchUnary runs one non-streaming disk op and returns (code, errStr,
// body). code 0 == success; a non-zero code is the same status the HTTP path
// uses so the grid client can recover storage.Err* sentinels via decodeErr.
// The success body is JSON for ops that return a value, empty otherwise.
func dispatchUnary(ctx context.Context, d diskRPC, op string, q url.Values, body []byte) (uint16, string, []byte) {
	j := func(v any) []byte { b, _ := json.Marshal(v); return b }
	fail := func(err error) (uint16, string, []byte) {
		if err == nil {
			return 0, "", nil
		}
		for sentinel, c := range errCodes {
			if errors.Is(err, sentinel) {
				return uint16(c), err.Error(), nil
			}
		}
		return http.StatusInternalServerError, err.Error(), nil
	}

	switch op {
	case "ping":
		return 0, "", nil
	case "makevol":
		return fail(d.MakeVol(ctx, q.Get("bucket")))
	case "statvol":
		vi, err := d.StatVol(ctx, q.Get("bucket"))
		if c, e, _ := fail(err); c != 0 {
			return c, e, nil
		}
		return 0, "", j(vi)
	case "listvols":
		vs, err := d.ListVols(ctx)
		if c, e, _ := fail(err); c != 0 {
			return c, e, nil
		}
		return 0, "", j(vs)
	case "deletevol":
		force, _ := strconv.ParseBool(q.Get("force"))
		return fail(d.DeleteVol(ctx, q.Get("bucket"), force))
	case "writeall":
		return fail(d.WriteAll(ctx, q.Get("bucket"), q.Get("object"), body))
	case "readall":
		b, err := d.ReadAll(ctx, q.Get("bucket"), q.Get("object"))
		if c, e, _ := fail(err); c != 0 {
			return c, e, nil
		}
		return 0, "", b
	case "renamedir":
		return fail(d.RenameDir(ctx, q.Get("srcBucket"), q.Get("srcObject"), q.Get("dstBucket"), q.Get("dstObject")))
	case "delete":
		rec, _ := strconv.ParseBool(q.Get("recursive"))
		return fail(d.Delete(ctx, q.Get("bucket"), q.Get("object"), rec))
	case "listdir":
		names, err := d.ListDir(ctx, q.Get("bucket"), q.Get("dir"))
		if c, e, _ := fail(err); c != 0 {
			return c, e, nil
		}
		return 0, "", j(names)
	case "diskinfo":
		di, err := d.DiskInfo(ctx)
		if c, e, _ := fail(err); c != 0 {
			return c, e, nil
		}
		return 0, "", j(di)
	default:
		return http.StatusNotFound, "unknown op", nil
	}
}

// handleGrid serves the multiplexed unary transport. It hijacks the
// connection (like a websocket) and speaks the raw [type|id|len|payload]
// frame protocol both ways — no chunked encoding, no auto-drain, clean
// full duplex.
func (s *RPCServer) handleGrid(w http.ResponseWriter, r *http.Request) {
	if !strings.EqualFold(r.Header.Get("Upgrade"), gridUpgradeToken) {
		http.Error(w, "expected grid upgrade", http.StatusBadRequest)
		return
	}
	hj, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, "grid needs a hijackable connection", http.StatusInternalServerError)
		return
	}
	conn, buf, err := hj.Hijack()
	if err != nil {
		return
	}
	defer conn.Close()

	if _, err := io.WriteString(conn,
		"HTTP/1.1 101 Switching Protocols\r\nUpgrade: "+gridUpgradeToken+"\r\nConnection: Upgrade\r\n\r\n"); err != nil {
		return
	}

	var writeMu sync.Mutex
	writeFrame := func(typ byte, id uint64, payload []byte) error {
		var hdr [13]byte
		hdr[0] = typ
		binary.BigEndian.PutUint64(hdr[1:9], id)
		binary.BigEndian.PutUint32(hdr[9:13], uint32(len(payload)))
		writeMu.Lock()
		defer writeMu.Unlock()
		_ = conn.SetWriteDeadline(time.Now().Add(30 * time.Second))
		if _, err := buf.Write(hdr[:]); err != nil {
			return err
		}
		if len(payload) > 0 {
			if _, err := buf.Write(payload); err != nil {
				return err
			}
		}
		return buf.Flush()
	}

	hdr := make([]byte, 13)
	for {
		if _, err := io.ReadFull(buf, hdr); err != nil {
			return
		}
		typ := hdr[0]
		id := binary.BigEndian.Uint64(hdr[1:9])
		n := binary.BigEndian.Uint32(hdr[9:13])
		var payload []byte
		if n > 0 {
			payload = make([]byte, n)
			if _, err := io.ReadFull(buf, payload); err != nil {
				return
			}
		}
		switch typ {
		case fPing:
			_ = writeFrame(fPong, id, nil)
		case fCall:
			go func(id uint64, payload []byte) {
				op, q, body, okp := decodeCallPayload(payload)
				if !okp {
					_ = writeFrame(fResp, id, encodeRespPayload(http.StatusBadRequest, "bad grid call payload", nil))
					return
				}
				di, ok := s.diskByQ(q)
				if !ok {
					_ = writeFrame(fResp, id, encodeRespPayload(http.StatusBadRequest, "bad disk index", nil))
					return
				}
				code, errStr, out := dispatchUnary(context.Background(), di, op, q, body)
				_ = writeFrame(fResp, id, encodeRespPayload(code, errStr, out))
			}(id, payload)
		}
	}
}

func (s *RPCServer) diskByQ(q url.Values) (diskRPC, bool) {
	i, err := strconv.Atoi(q.Get("disk"))
	if err != nil || i < 0 || i >= len(s.disks) {
		return nil, false
	}
	return s.disks[i], true
}

// --- error wire format --------------------------------------------------

var errCodes = map[error]int{
	storage.ErrVolumeNotFound: 461,
	storage.ErrVolumeExists:   462,
	storage.ErrVolumeNotEmpty: 463,
	storage.ErrFileNotFound:   464,
	storage.ErrDiskNotFound:   465,
	storage.ErrDiskFull:       466,
}

func writeRPCErr(w http.ResponseWriter, err error) {
	if err == nil {
		w.WriteHeader(http.StatusOK)
		return
	}
	for sentinel, code := range errCodes {
		if errors.Is(err, sentinel) {
			http.Error(w, err.Error(), code)
			return
		}
	}
	http.Error(w, err.Error(), http.StatusInternalServerError)
}

func writeJSONOrErr(w http.ResponseWriter, v any, err error) {
	if err != nil {
		writeRPCErr(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

// decodeErr turns an RPC status code back into a sentinel (client side).
func decodeErr(code int, body string) error {
	for sentinel, c := range errCodes {
		if c == code {
			return sentinel
		}
	}
	if body == "" {
		body = "cluster rpc failed"
	}
	return errors.New("cluster: " + body)
}
