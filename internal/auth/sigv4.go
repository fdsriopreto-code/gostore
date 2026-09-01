package auth

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"sort"
	"strings"
)

// AWS SigV4 constants.
const (
	Algorithm        = "AWS4-HMAC-SHA256"
	StreamingPayload = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"
	UnsignedPayload  = "UNSIGNED-PAYLOAD"
	serviceS3        = "s3"
	terminator       = "aws4_request"
	// EmptyStringSHA256 is sha256("").
	EmptyStringSHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
)

func hmacSHA256(key []byte, data string) []byte {
	h := hmac.New(sha256.New, key)
	h.Write([]byte(data))
	return h.Sum(nil)
}

// SHA256Hex returns the hex-encoded SHA-256 of b.
func SHA256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// SigningKey derives the SigV4 signing key for a given date/region/service.
func SigningKey(secret, dateStamp, region, service string) []byte {
	kDate := hmacSHA256([]byte("AWS4"+secret), dateStamp)
	kRegion := hmacSHA256(kDate, region)
	kService := hmacSHA256(kRegion, service)
	return hmacSHA256(kService, terminator)
}

// HexHMACSHA256 returns hex(HMAC-SHA256(key, data)).
func HexHMACSHA256(key []byte, data string) string {
	return hex.EncodeToString(hmacSHA256(key, data))
}

// CredentialScope is the parsed "AK/date/region/service/aws4_request" value.
type CredentialScope struct {
	AccessKey string
	Date      string // yyyymmdd
	Region    string
	Service   string
}

func (c CredentialScope) String() string {
	return c.Date + "/" + c.Region + "/" + c.Service + "/" + terminator
}

// ParsedAuthHeader holds the fields of an Authorization: AWS4-HMAC-SHA256 header.
type ParsedAuthHeader struct {
	Scope         CredentialScope
	SignedHeaders []string
	Signature     string
}

// ParseAuthHeader parses the Authorization header value. It returns ok=false
// when the header is absent or not SigV4.
func ParseAuthHeader(v string) (ParsedAuthHeader, bool) {
	if !strings.HasPrefix(v, Algorithm+" ") {
		return ParsedAuthHeader{}, false
	}
	var out ParsedAuthHeader
	for _, part := range strings.Split(v[len(Algorithm)+1:], ",") {
		part = strings.TrimSpace(part)
		switch {
		case strings.HasPrefix(part, "Credential="):
			seg := strings.Split(strings.TrimPrefix(part, "Credential="), "/")
			if len(seg) != 5 {
				return ParsedAuthHeader{}, false
			}
			out.Scope = CredentialScope{AccessKey: seg[0], Date: seg[1], Region: seg[2], Service: seg[3]}
		case strings.HasPrefix(part, "SignedHeaders="):
			out.SignedHeaders = strings.Split(strings.TrimPrefix(part, "SignedHeaders="), ";")
		case strings.HasPrefix(part, "Signature="):
			out.Signature = strings.TrimPrefix(part, "Signature=")
		}
	}
	if out.Scope.AccessKey == "" || len(out.SignedHeaders) == 0 || out.Signature == "" {
		return ParsedAuthHeader{}, false
	}
	sort.Strings(out.SignedHeaders)
	return out, true
}

// CanonicalRequest builds the SigV4 canonical request string.
//
//	method \n
//	canonicalURI \n
//	canonicalQueryString \n
//	canonicalHeaders \n
//	signedHeaders \n
//	hashedPayload
func CanonicalRequest(method, canonURI, canonQuery string, signedHeaders []string, headerLookup func(string) string, hashedPayload string) string {
	var ch strings.Builder
	for _, h := range signedHeaders {
		ch.WriteString(h)
		ch.WriteByte(':')
		ch.WriteString(trimAll(headerLookup(h)))
		ch.WriteByte('\n')
	}
	return method + "\n" +
		canonURI + "\n" +
		canonQuery + "\n" +
		ch.String() + "\n" +
		strings.Join(signedHeaders, ";") + "\n" +
		hashedPayload
}

// StringToSign builds the SigV4 string-to-sign.
func StringToSign(amzDate string, scope CredentialScope, canonicalRequest string) string {
	return Algorithm + "\n" +
		amzDate + "\n" +
		scope.String() + "\n" +
		SHA256Hex([]byte(canonicalRequest))
}

// trimAll collapses internal whitespace runs to a single space and trims ends
// (per the SigV4 header-value normalization rules for non-quoted values).
func trimAll(s string) string {
	s = strings.TrimSpace(s)
	var b strings.Builder
	space := false
	for _, r := range s {
		if r == ' ' || r == '\t' {
			space = true
			continue
		}
		if space && b.Len() > 0 {
			b.WriteByte(' ')
		}
		space = false
		b.WriteRune(r)
	}
	return b.String()
}

// SecureCompare is a constant-time string comparison.
func SecureCompare(a, b string) bool {
	return hmac.Equal([]byte(a), []byte(b))
}
