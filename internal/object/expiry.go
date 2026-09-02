package object

import (
	"strings"
	"time"
)

// ExpiryMetaKey is the user-metadata key under which a per-object TTL's
// absolute expiry instant is stored (RFC3339). gostore honours this without a
// bucket lifecycle rule — the API layer sets it from the X-Gostore-Expires /
// X-Gostore-Expire-After request headers, the GET path enforces it lazily,
// and the scanner sweeps expired objects during its namespace walk.
const ExpiryMetaKey = "x-amz-meta-gostore-expires"

// ExpiryOf returns the stored expiry instant for an object's user metadata, or
// the zero time when it has none. Keys are matched by suffix so the value
// still resolves through whatever "x-amz-meta-" prefixing a backend applies.
func ExpiryOf(userDefined map[string]string) time.Time {
	for k, v := range userDefined {
		if lk := strings.ToLower(k); lk == "gostore-expires" || strings.HasSuffix(lk, "gostore-expires") {
			if t, err := time.Parse(time.RFC3339, strings.TrimSpace(v)); err == nil {
				return t.UTC()
			}
		}
	}
	return time.Time{}
}

// HasExpired reports whether an object's TTL has passed as of now.
func HasExpired(userDefined map[string]string, now time.Time) bool {
	exp := ExpiryOf(userDefined)
	return !exp.IsZero() && now.After(exp)
}
