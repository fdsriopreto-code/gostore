package cluster

import (
	"context"
	"crypto/hmac"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"

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

	switch op {
	case "makevol":
		writeRPCErr(w, d.MakeVol(ctx, q.Get("bucket")))
	case "statvol":
		vi, err := d.StatVol(ctx, q.Get("bucket"))
		writeJSONOrErr(w, vi, err)
	case "listvols":
		vs, err := d.ListVols(ctx)
		writeJSONOrErr(w, vs, err)
	case "deletevol":
		force, _ := strconv.ParseBool(q.Get("force"))
		writeRPCErr(w, d.DeleteVol(ctx, q.Get("bucket"), force))
	case "writeall":
		b, _ := io.ReadAll(io.LimitReader(r.Body, 64<<20))
		writeRPCErr(w, d.WriteAll(ctx, q.Get("bucket"), q.Get("object"), b))
	case "readall":
		b, err := d.ReadAll(ctx, q.Get("bucket"), q.Get("object"))
		if err != nil {
			writeRPCErr(w, err)
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		_, _ = w.Write(b)
	case "createfile":
		size, _ := strconv.ParseInt(q.Get("size"), 10, 64)
		writeRPCErr(w, d.CreateFile(ctx, q.Get("bucket"), q.Get("object"), size, r.Body))
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
	case "renamedir":
		writeRPCErr(w, d.RenameDir(ctx, q.Get("srcBucket"), q.Get("srcObject"), q.Get("dstBucket"), q.Get("dstObject")))
	case "delete":
		rec, _ := strconv.ParseBool(q.Get("recursive"))
		writeRPCErr(w, d.Delete(ctx, q.Get("bucket"), q.Get("object"), rec))
	case "listdir":
		names, err := d.ListDir(ctx, q.Get("bucket"), q.Get("dir"))
		writeJSONOrErr(w, names, err)
	case "diskinfo":
		di, err := d.DiskInfo(ctx)
		writeJSONOrErr(w, di, err)
	default:
		http.Error(w, "unknown op", http.StatusNotFound)
	}
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
