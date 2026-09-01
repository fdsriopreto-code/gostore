package auth

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"strconv"
	"strings"
)

// Errors returned by the chunked reader.
var (
	ErrMalformedChunk    = errors.New("auth: malformed aws-chunked encoding")
	ErrChunkSignature    = errors.New("auth: chunk signature mismatch")
)

const chunkPayloadSTS = "AWS4-HMAC-SHA256-PAYLOAD"

// ChunkedReaderOpts configures NewChunkedReader.
type ChunkedReaderOpts struct {
	SeedSignature string
	SigningKey    []byte
	AmzDate       string // yyyymmddThhmmssZ
	Scope         CredentialScope
	Verify        bool // verify per-chunk signatures
}

// chunkedReader decodes STREAMING-AWS4-HMAC-SHA256-PAYLOAD (and the
// UNSIGNED / *-TRAILER variants) request bodies, yielding the plain object
// bytes. When Verify is set, each chunk signature is checked and chained.
type chunkedReader struct {
	br      *bufio.Reader
	opts    ChunkedReaderOpts
	prevSig string

	buf  bytes.Buffer
	done bool
	err  error
}

// NewChunkedReader wraps r (the raw request body) with an aws-chunked decoder.
func NewChunkedReader(r io.Reader, opts ChunkedReaderOpts) io.ReadCloser {
	return &chunkedReader{
		br:      bufio.NewReaderSize(r, 64*1024),
		opts:    opts,
		prevSig: opts.SeedSignature,
	}
}

func (c *chunkedReader) Read(p []byte) (int, error) {
	if c.err != nil {
		return 0, c.err
	}
	for c.buf.Len() == 0 && !c.done {
		if err := c.readChunk(); err != nil {
			c.err = err
			if c.buf.Len() > 0 && errors.Is(err, io.EOF) {
				break
			}
			return 0, err
		}
	}
	if c.buf.Len() == 0 && c.done {
		return 0, io.EOF
	}
	return c.buf.Read(p)
}

func (c *chunkedReader) Close() error { return nil }

func (c *chunkedReader) readChunk() error {
	line, err := c.readLine()
	if err != nil {
		return err
	}
	// "<hexsize>[;chunk-signature=<sig>]"
	var sizeHex, sig string
	if i := strings.IndexByte(line, ';'); i >= 0 {
		sizeHex = line[:i]
		for _, kv := range strings.Split(line[i+1:], ";") {
			if strings.HasPrefix(kv, "chunk-signature=") {
				sig = strings.TrimPrefix(kv, "chunk-signature=")
			}
		}
	} else {
		sizeHex = line
	}
	size, perr := strconv.ParseInt(strings.TrimSpace(sizeHex), 16, 64)
	if perr != nil || size < 0 {
		return ErrMalformedChunk
	}

	data := make([]byte, size)
	if size > 0 {
		if _, err := io.ReadFull(c.br, data); err != nil {
			return ErrMalformedChunk
		}
	}
	// trailing CRLF after chunk data
	if crlf, err := c.readLine(); err != nil || crlf != "" {
		if err != nil {
			return err
		}
		return ErrMalformedChunk
	}

	if c.opts.Verify && sig != "" {
		want := c.chunkSignature(data)
		if !SecureCompare(want, sig) {
			return ErrChunkSignature
		}
		c.prevSig = sig
	}

	if size == 0 {
		c.done = true
		c.drainTrailer()
		return nil
	}
	c.buf.Write(data)
	return nil
}

// chunkSignature computes the expected signature for one chunk.
func (c *chunkedReader) chunkSignature(data []byte) string {
	sum := sha256.Sum256(data)
	sts := chunkPayloadSTS + "\n" +
		c.opts.AmzDate + "\n" +
		c.opts.Scope.String() + "\n" +
		c.prevSig + "\n" +
		EmptyStringSHA256 + "\n" +
		hex.EncodeToString(sum[:])
	return HexHMACSHA256(c.opts.SigningKey, sts)
}

// drainTrailer consumes optional trailing headers (checksum trailers on the
// *-TRAILER content-sha256 variants) up to the terminating blank line. Best
// effort: trailer checksums are not currently validated.
func (c *chunkedReader) drainTrailer() {
	for i := 0; i < 8; i++ {
		line, err := c.readLine()
		if err != nil || line == "" {
			return
		}
	}
}

// readLine reads one CRLF-terminated line and returns it without the CRLF.
func (c *chunkedReader) readLine() (string, error) {
	var sb strings.Builder
	for {
		b, err := c.br.ReadByte()
		if err != nil {
			if err == io.EOF && sb.Len() > 0 {
				return sb.String(), nil
			}
			return "", err
		}
		if b == '\n' {
			s := sb.String()
			return strings.TrimSuffix(s, "\r"), nil
		}
		sb.WriteByte(b)
	}
}
