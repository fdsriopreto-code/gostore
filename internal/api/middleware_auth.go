package api

import (
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/lojadopocket/gostore/internal/auth"
)

const maxClockSkew = 15 * time.Minute

// authenticate verifies the request's AWS SigV4 credentials (header or
// presigned). On success it returns a replacement body when the payload is
// aws-chunked (nil otherwise), the authenticated access key ("" for an
// allowed anonymous request), and ErrNone. On failure it returns the S3
// error code to emit.
func (s *Server) authenticate(r *http.Request) (io.ReadCloser, string, APIErrorCode) {
	authHeader := r.Header.Get("Authorization")
	isPresign := r.URL.Query().Get("X-Amz-Signature") != ""

	if authHeader == "" && !isPresign {
		if os.Getenv("GOSTORE_ALLOW_ANONYMOUS") == "1" {
			return nil, "", ErrNone
		}
		return nil, "", ErrAccessDenied
	}

	if isPresign {
		ak, code := s.verifyPresigned(r)
		return nil, ak, code
	}
	return s.verifyHeader(r, authHeader)
}

func (s *Server) verifyHeader(r *http.Request, authHeader string) (io.ReadCloser, string, APIErrorCode) {
	parsed, ok := auth.ParseAuthHeader(authHeader)
	if !ok {
		return nil, "", ErrAuthHeaderEmpty
	}
	secret, ok := s.iam.LookupSecret(parsed.Scope.AccessKey)
	if !ok {
		return nil, "", ErrInvalidAccessKeyID
	}

	ak := parsed.Scope.AccessKey
	amzDate := r.Header.Get("X-Amz-Date")
	if amzDate == "" {
		amzDate = r.Header.Get("Date")
	}
	if amzDate == "" {
		return nil, "", ErrMissingDateHeader
	}
	t, err := parseAMZDate(amzDate)
	if err != nil {
		return nil, "", ErrMissingDateHeader
	}
	if absDuration(time.Since(t)) > maxClockSkew {
		return nil, "", ErrRequestTimeTooSkewed
	}

	hashedPayload := r.Header.Get("X-Amz-Content-Sha256")
	if hashedPayload == "" {
		hashedPayload = auth.UnsignedPayload
	}

	canonURI := auth.EncodePath(r.URL.Path)
	canonQuery := auth.CanonicalQuery(r.URL.Query())
	lookup := headerLookup(r, parsed.Scope.Region)
	canonReq := auth.CanonicalRequest(r.Method, canonURI, canonQuery, parsed.SignedHeaders, lookup, hashedPayload)
	sts := auth.StringToSign(amzDate, parsed.Scope, canonReq)
	key := auth.SigningKey(secret, parsed.Scope.Date, parsed.Scope.Region, parsed.Scope.Service)
	want := auth.HexHMACSHA256(key, sts)

	if !auth.SecureCompare(want, parsed.Signature) {
		return nil, "", ErrSignatureDoesNotMatch
	}

	if strings.HasPrefix(hashedPayload, "STREAMING-") {
		verify := strings.Contains(hashedPayload, "AWS4-HMAC-SHA256") &&
			os.Getenv("GOSTORE_SKIP_CHUNK_VERIFY") != "1"
		body := auth.NewChunkedReader(r.Body, auth.ChunkedReaderOpts{
			SeedSignature: parsed.Signature,
			SigningKey:    key,
			AmzDate:       amzDate,
			Scope:         parsed.Scope,
			Verify:        verify,
		})
		return body, ak, ErrNone
	}
	return nil, ak, ErrNone
}

func (s *Server) verifyPresigned(r *http.Request) (string, APIErrorCode) {
	q := r.URL.Query()
	if q.Get("X-Amz-Algorithm") != auth.Algorithm {
		return "", ErrUnsupportedSignatureVersion
	}
	credRaw := q.Get("X-Amz-Credential")
	seg := strings.Split(credRaw, "/")
	if len(seg) != 5 {
		return "", ErrInvalidArgument
	}
	scope := auth.CredentialScope{AccessKey: seg[0], Date: seg[1], Region: seg[2], Service: seg[3]}
	secret, ok := s.iam.LookupSecret(scope.AccessKey)
	if !ok {
		return "", ErrInvalidAccessKeyID
	}

	amzDate := q.Get("X-Amz-Date")
	t, err := parseAMZDate(amzDate)
	if err != nil {
		return "", ErrMissingDateHeader
	}
	expSecs, _ := strconv.Atoi(q.Get("X-Amz-Expires"))
	if expSecs <= 0 || expSecs > 7*24*3600 {
		return "", ErrInvalidArgument
	}
	if time.Now().UTC().After(t.Add(time.Duration(expSecs) * time.Second)) {
		return "", ErrRequestTimeTooSkewed
	}

	signedHeaders := strings.Split(q.Get("X-Amz-SignedHeaders"), ";")
	providedSig := q.Get("X-Amz-Signature")

	canonURI := auth.EncodePath(r.URL.Path)
	canonQuery := auth.CanonicalQuery(q, "X-Amz-Signature")
	lookup := headerLookup(r, scope.Region)
	canonReq := auth.CanonicalRequest(r.Method, canonURI, canonQuery, sortedLower(signedHeaders), lookup, auth.UnsignedPayload)
	sts := auth.StringToSign(amzDate, scope, canonReq)
	key := auth.SigningKey(secret, scope.Date, scope.Region, scope.Service)
	want := auth.HexHMACSHA256(key, sts)

	if !auth.SecureCompare(want, providedSig) {
		return "", ErrSignatureDoesNotMatch
	}
	return scope.AccessKey, ErrNone
}

// headerLookup returns a function that resolves a signed-header name to the
// value that must appear in the canonical request.
func headerLookup(r *http.Request, region string) func(string) string {
	return func(name string) string {
		switch strings.ToLower(name) {
		case "host":
			return r.Host
		case "content-length":
			if r.ContentLength >= 0 {
				return strconv.FormatInt(r.ContentLength, 10)
			}
			return r.Header.Get("Content-Length")
		default:
			if v := r.Header.Get(name); v != "" {
				return v
			}
			// x-amz-* headers occasionally only present canonicalized
			return r.Header.Get(http.CanonicalHeaderKey(name))
		}
	}
}

func sortedLower(hs []string) []string {
	out := make([]string, len(hs))
	for i, h := range hs {
		out[i] = strings.ToLower(strings.TrimSpace(h))
	}
	// insertion sort (tiny slices)
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j-1] > out[j]; j-- {
			out[j-1], out[j] = out[j], out[j-1]
		}
	}
	return out
}

func parseAMZDate(s string) (time.Time, error) {
	for _, layout := range []string{"20060102T150405Z", time.RFC1123, time.RFC1123Z, http.TimeFormat} {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UTC(), nil
		}
	}
	return time.Time{}, strconvErr
}

var strconvErr = &timeParseError{}

type timeParseError struct{}

func (*timeParseError) Error() string { return "invalid time format" }

func absDuration(d time.Duration) time.Duration {
	if d < 0 {
		return -d
	}
	return d
}
