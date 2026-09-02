package cluster

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// grid is a persistent, multiplexed, full-duplex transport for the small
// unary disk/lock RPCs between two nodes. One raw TCP (or TLS) connection per
// peer, obtained via an HTTP `Upgrade: gostore-grid` handshake, then a
// stream of length-prefixed frames each way; many in-flight calls share it,
// keyed by a 64-bit id. It replaces one-TCP-connection-per-operation for
// everything except bulk shard streaming (CreateFile / ReadFileStream still
// get their own HTTP request, which is what you want for a multi-MB transfer
// anyway).
//
// If a peer doesn't answer the handshake (e.g. an older binary), the caller
// transparently falls back to per-op HTTP.

const gridPath = "/gostore/internal/grid"
const gridUpgradeToken = "gostore-grid"

const (
	fCall byte = 1
	fResp byte = 2
	fPing byte = 3
	fPong byte = 4
)

const (
	gridPingEvery = 15 * time.Second
	gridRedialGap = 20 * time.Second
	gridCallWait  = 90 * time.Second
	gridDialWait  = 6 * time.Second
)

// errGridUnavailable signals the caller should use the HTTP fallback.
var errGridUnavailable = errors.New("cluster: grid transport unavailable")

type gridReply struct {
	body []byte
	err  error
}

// gridConn is one client-side multiplexed connection to a peer base URL.
type gridConn struct {
	base   string
	secret string

	mu       sync.Mutex
	conn     net.Conn
	bw       *bufio.Writer
	writeMu  sync.Mutex
	pending  map[uint64]chan gridReply
	nextID   uint64
	up       bool
	lastDial time.Time
	dialErr  error
}

func newGridConn(base, secret string) *gridConn {
	return &gridConn{base: base, secret: secret, pending: map[uint64]chan gridReply{}}
}

// ensure (re)establishes the connection if it's down and a redial is due.
func (g *gridConn) ensure() error {
	g.mu.Lock()
	if g.up {
		g.mu.Unlock()
		return nil
	}
	if time.Since(g.lastDial) < gridRedialGap {
		err := g.dialErr
		g.mu.Unlock()
		if err == nil {
			err = errGridUnavailable
		}
		return err
	}
	g.lastDial = time.Now()
	g.mu.Unlock()

	conn, err := g.dial()
	if err != nil {
		g.mu.Lock()
		g.up = false
		g.dialErr = err
		g.mu.Unlock()
		return err
	}

	g.mu.Lock()
	g.conn = conn
	g.bw = bufio.NewWriterSize(conn, 32<<10)
	g.up = true
	g.dialErr = nil
	g.mu.Unlock()

	go g.readLoop(conn)
	go g.pingLoop()
	return nil
}

// dial opens the raw connection and performs the HTTP upgrade handshake.
func (g *gridConn) dial() (net.Conn, error) {
	u, err := url.Parse(g.base)
	if err != nil {
		return nil, err
	}
	host := u.Host
	if u.Port() == "" {
		if u.Scheme == "https" {
			host = net.JoinHostPort(host, "443")
		} else {
			host = net.JoinHostPort(host, "80")
		}
	}
	d := &net.Dialer{Timeout: gridDialWait}
	var conn net.Conn
	if u.Scheme == "https" {
		conn, err = tls.DialWithDialer(d, "tcp", host, &tls.Config{ServerName: u.Hostname()})
	} else {
		conn, err = d.DialContext(context.Background(), "tcp", host)
	}
	if err != nil {
		return nil, err
	}

	_ = conn.SetDeadline(time.Now().Add(gridDialWait))
	req := "GET " + gridPath + " HTTP/1.1\r\n" +
		"Host: " + u.Host + "\r\n" +
		"Connection: Upgrade\r\n" +
		"Upgrade: " + gridUpgradeToken + "\r\n" +
		"X-Gostore-Cluster: " + g.secret + "\r\n\r\n"
	if _, err := io.WriteString(conn, req); err != nil {
		_ = conn.Close()
		return nil, err
	}
	br := bufio.NewReader(conn)
	status, err := br.ReadString('\n')
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	if !strings.Contains(status, " 101 ") {
		_ = conn.Close()
		return nil, errGridUnavailable
	}
	// drain the rest of the response headers
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			_ = conn.Close()
			return nil, err
		}
		if line == "\r\n" || line == "\n" {
			break
		}
	}
	_ = conn.SetDeadline(time.Time{})
	if br.Buffered() > 0 {
		// unlikely, but preserve any bytes already read past the headers
		pre, _ := br.Peek(br.Buffered())
		return &prefixConn{Conn: conn, pre: append([]byte(nil), pre...)}, nil
	}
	return conn, nil
}

// prefixConn prepends already-buffered bytes to a Conn's read stream.
type prefixConn struct {
	net.Conn
	pre []byte
}

func (p *prefixConn) Read(b []byte) (int, error) {
	if len(p.pre) > 0 {
		n := copy(b, p.pre)
		p.pre = p.pre[n:]
		return n, nil
	}
	return p.Conn.Read(b)
}

func (g *gridConn) fail(err error) {
	g.mu.Lock()
	g.up = false
	g.dialErr = err
	conn := g.conn
	g.conn = nil
	g.bw = nil
	pend := g.pending
	g.pending = map[uint64]chan gridReply{}
	g.mu.Unlock()

	if conn != nil {
		_ = conn.Close()
	}
	for _, ch := range pend {
		select {
		case ch <- gridReply{err: err}:
		default:
		}
	}
}

func (g *gridConn) readLoop(conn net.Conn) {
	br := bufio.NewReaderSize(conn, 64<<10)
	hdr := make([]byte, 13)
	for {
		if _, err := io.ReadFull(br, hdr); err != nil {
			g.fail(err)
			return
		}
		typ := hdr[0]
		id := binary.BigEndian.Uint64(hdr[1:9])
		n := binary.BigEndian.Uint32(hdr[9:13])
		var payload []byte
		if n > 0 {
			payload = make([]byte, n)
			if _, err := io.ReadFull(br, payload); err != nil {
				g.fail(err)
				return
			}
		}
		switch typ {
		case fPong:
			// liveness only
		case fResp:
			g.mu.Lock()
			ch := g.pending[id]
			delete(g.pending, id)
			g.mu.Unlock()
			if ch != nil {
				ch <- decodeRespPayload(payload)
			}
		}
	}
}

func (g *gridConn) pingLoop() {
	t := time.NewTicker(gridPingEvery)
	defer t.Stop()
	for range t.C {
		g.mu.Lock()
		up := g.up
		g.mu.Unlock()
		if !up {
			return
		}
		if err := g.writeFrame(fPing, 0, nil); err != nil {
			g.fail(err)
			return
		}
	}
}

func (g *gridConn) writeFrame(typ byte, id uint64, payload []byte) error {
	g.mu.Lock()
	bw, conn := g.bw, g.conn
	g.mu.Unlock()
	if bw == nil || conn == nil {
		return errGridUnavailable
	}
	var hdr [13]byte
	hdr[0] = typ
	binary.BigEndian.PutUint64(hdr[1:9], id)
	binary.BigEndian.PutUint32(hdr[9:13], uint32(len(payload)))

	g.writeMu.Lock()
	defer g.writeMu.Unlock()
	_ = conn.SetWriteDeadline(time.Now().Add(30 * time.Second))
	if _, err := bw.Write(hdr[:]); err != nil {
		return err
	}
	if len(payload) > 0 {
		if _, err := bw.Write(payload); err != nil {
			return err
		}
	}
	return bw.Flush()
}

// call performs one unary RPC over the multiplexed connection.
func (g *gridConn) call(ctx context.Context, op string, q url.Values, body []byte) ([]byte, error) {
	if err := g.ensure(); err != nil {
		return nil, err
	}
	id := atomic.AddUint64(&g.nextID, 1)
	ch := make(chan gridReply, 1)

	g.mu.Lock()
	if !g.up {
		g.mu.Unlock()
		return nil, errGridUnavailable
	}
	g.pending[id] = ch
	g.mu.Unlock()

	if err := g.writeFrame(fCall, id, encodeCallPayload(op, q, body)); err != nil {
		g.mu.Lock()
		delete(g.pending, id)
		g.mu.Unlock()
		g.fail(err)
		return nil, err
	}

	timeout := time.NewTimer(gridCallWait)
	defer timeout.Stop()
	select {
	case <-ctx.Done():
		g.mu.Lock()
		delete(g.pending, id)
		g.mu.Unlock()
		return nil, ctx.Err()
	case <-timeout.C:
		g.mu.Lock()
		delete(g.pending, id)
		g.mu.Unlock()
		return nil, errors.New("cluster: grid call timed out")
	case rep := <-ch:
		return rep.body, rep.err
	}
}

// --- payload codec ------------------------------------------------------

func encodeCallPayload(op string, q url.Values, body []byte) []byte {
	qb, _ := json.Marshal(q)
	out := make([]byte, 0, 2+len(op)+4+len(qb)+len(body))
	out = binary.BigEndian.AppendUint16(out, uint16(len(op)))
	out = append(out, op...)
	out = binary.BigEndian.AppendUint32(out, uint32(len(qb)))
	out = append(out, qb...)
	out = append(out, body...)
	return out
}

func decodeCallPayload(p []byte) (op string, q url.Values, body []byte, ok bool) {
	if len(p) < 2 {
		return "", nil, nil, false
	}
	ol := int(binary.BigEndian.Uint16(p[:2]))
	p = p[2:]
	if len(p) < ol+4 {
		return "", nil, nil, false
	}
	op = string(p[:ol])
	p = p[ol:]
	ql := int(binary.BigEndian.Uint32(p[:4]))
	p = p[4:]
	if len(p) < ql {
		return "", nil, nil, false
	}
	q = url.Values{}
	_ = json.Unmarshal(p[:ql], &q)
	body = p[ql:]
	return op, q, body, true
}

// resp payload: [2 code][4 errLen][err][body]. code 0 = success; otherwise
// it's the same status code the HTTP path uses (see errCodes), so the client
// runs it back through decodeErr to recover storage.Err* sentinels.
func encodeRespPayload(code uint16, errStr string, body []byte) []byte {
	out := make([]byte, 0, 2+4+len(errStr)+len(body))
	out = binary.BigEndian.AppendUint16(out, code)
	out = binary.BigEndian.AppendUint32(out, uint32(len(errStr)))
	out = append(out, errStr...)
	out = append(out, body...)
	return out
}

func decodeRespPayload(p []byte) gridReply {
	if len(p) < 6 {
		return gridReply{err: errors.New("cluster: short grid response")}
	}
	code := binary.BigEndian.Uint16(p[:2])
	el := int(binary.BigEndian.Uint32(p[2:6]))
	p = p[6:]
	if len(p) < el {
		return gridReply{err: errors.New("cluster: corrupt grid response")}
	}
	errStr := string(p[:el])
	body := append([]byte(nil), p[el:]...)
	if code != 0 {
		return gridReply{err: decodeErr(int(code), errStr)}
	}
	return gridReply{body: body}
}
