package erasure

import (
	"context"
	"time"

	"github.com/lojadopocket/gostore/internal/object"
)

// Object Lock (WORM) is not implemented on the erasure backend yet — it
// depends on per-version metadata that the erasure xl.meta does not carry.
// Use the single-disk backend for Object Lock.

func (p *Pool) PutObjectRetention(context.Context, string, string, string, string, time.Time, bool) error {
	return object.ErrNotImplemented
}
func (p *Pool) GetObjectRetention(context.Context, string, string, string) (string, time.Time, error) {
	return "", time.Time{}, object.ErrNotImplemented
}
func (p *Pool) PutObjectLegalHold(context.Context, string, string, string, string) error {
	return object.ErrNotImplemented
}
func (p *Pool) GetObjectLegalHold(context.Context, string, string, string) (string, error) {
	return "", object.ErrNotImplemented
}
