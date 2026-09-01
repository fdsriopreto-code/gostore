package auth

import (
	"net/url"
	"sort"
	"strings"
)

// EncodePath URI-encodes an object path the way AWS SigV4 for S3 expects:
// every byte except the RFC3986 unreserved set and '/' is percent-encoded
// with uppercase hex. No path normalization is performed.
func EncodePath(p string) string {
	if p == "" {
		return "/"
	}
	var b strings.Builder
	for _, c := range []byte(p) {
		switch {
		case c >= 'A' && c <= 'Z', c >= 'a' && c <= 'z', c >= '0' && c <= '9':
			b.WriteByte(c)
		case c == '-' || c == '_' || c == '.' || c == '~' || c == '/':
			b.WriteByte(c)
		default:
			b.WriteByte('%')
			b.WriteByte(hexUpper[c>>4])
			b.WriteByte(hexUpper[c&0xf])
		}
	}
	return b.String()
}

const hexUpper = "0123456789ABCDEF"

// encodeQueryComponent percent-encodes a query key or value per RFC3986
// (space => %20, unreserved chars pass through).
func encodeQueryComponent(s string) string {
	var b strings.Builder
	for _, c := range []byte(s) {
		switch {
		case c >= 'A' && c <= 'Z', c >= 'a' && c <= 'z', c >= '0' && c <= '9':
			b.WriteByte(c)
		case c == '-' || c == '_' || c == '.' || c == '~':
			b.WriteByte(c)
		default:
			b.WriteByte('%')
			b.WriteByte(hexUpper[c>>4])
			b.WriteByte(hexUpper[c&0xf])
		}
	}
	return b.String()
}

// CanonicalQuery builds the SigV4 canonical query string from parsed values,
// omitting any key in omit (used to drop X-Amz-Signature for presigned URLs).
func CanonicalQuery(q url.Values, omit ...string) string {
	skip := map[string]bool{}
	for _, o := range omit {
		skip[o] = true
	}
	keys := make([]string, 0, len(q))
	for k := range q {
		if skip[k] {
			continue
		}
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var parts []string
	for _, k := range keys {
		vals := append([]string(nil), q[k]...)
		sort.Strings(vals)
		ek := encodeQueryComponent(k)
		for _, v := range vals {
			parts = append(parts, ek+"="+encodeQueryComponent(v))
		}
	}
	return strings.Join(parts, "&")
}
