package api

import (
	"strconv"
	"strings"
	"time"

	"github.com/lojadopocket/gostore/internal/object"
)

// gostore lets a client set a per-object time-to-live without configuring a
// bucket lifecycle rule — something neither MinIO nor plain S3 offer. The
// expiry is stored as object metadata (object.ExpiryMetaKey); it is enforced
// lazily on the next GET/HEAD and swept for good by the scanner's walk.

const expiryMetaKey = object.ExpiryMetaKey

// parseExpiryDirective reads the request's expiry intent, if any:
//
//	X-Gostore-Expires:       an absolute RFC3339 timestamp or an HTTP date
//	X-Gostore-Expire-After:  a relative span — Go duration ("72h"), or
//	                         "<n>d" / "<n>w" / "<n>h" / "<n>m"
//
// Returns the zero time when neither header is present or parseable.
func parseExpiryDirective(get func(string) string, now time.Time) time.Time {
	if v := strings.TrimSpace(get("X-Gostore-Expires")); v != "" {
		if t, err := time.Parse(time.RFC3339, v); err == nil {
			return t.UTC()
		}
		if t, err := time.Parse(time.RFC1123, v); err == nil {
			return t.UTC()
		}
		return time.Time{}
	}
	if v := strings.TrimSpace(get("X-Gostore-Expire-After")); v != "" {
		if d, ok := parseSpan(v); ok {
			return now.Add(d).UTC()
		}
	}
	return time.Time{}
}

func parseSpan(v string) (time.Duration, bool) {
	if d, err := time.ParseDuration(v); err == nil && d > 0 {
		return d, true
	}
	if len(v) < 2 {
		return 0, false
	}
	n, err := strconv.Atoi(v[:len(v)-1])
	if err != nil || n <= 0 {
		return 0, false
	}
	switch v[len(v)-1] {
	case 'm', 'M':
		return time.Duration(n) * time.Minute, true
	case 'h', 'H':
		return time.Duration(n) * time.Hour, true
	case 'd', 'D':
		return time.Duration(n) * 24 * time.Hour, true
	case 'w', 'W':
		return time.Duration(n) * 7 * 24 * time.Hour, true
	}
	return 0, false
}

// objectHasExpired reports whether an object's TTL has passed as of now.
func objectHasExpired(userDefined map[string]string, now time.Time) bool {
	return object.HasExpired(userDefined, now)
}
