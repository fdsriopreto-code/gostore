package fs

import (
	"context"
	"time"

	"github.com/lojadopocket/gostore/internal/object"
)

// findVersionEntry returns the index position of the target version (latest
// when versionID == ""), the loaded index, and whether it was found.
func (f *FS) findVersionEntry(bucket, obj, versionID string) (int, []verEntry, error) {
	idx, err := f.readVerIndex(bucket, obj)
	if err != nil {
		return -1, nil, err
	}
	if len(idx) == 0 {
		return -1, nil, object.ErrNotVersioned
	}
	if versionID == "" {
		return len(idx) - 1, idx, nil
	}
	for i := range idx {
		if idx[i].ID == versionID {
			return i, idx, nil
		}
	}
	return -1, idx, object.ObjectNotFound{Bucket: bucket, Object: obj}
}

func (f *FS) PutObjectRetention(_ context.Context, bucket, obj, versionID, mode string, until time.Time, bypassGovernance bool) error {
	lk := f.NewNSLock(bucket, obj)
	c, _ := lk.GetLock(context.Background(), 0)
	defer lk.Unlock(c)

	i, idx, err := f.findVersionEntry(bucket, obj, versionID)
	if err != nil {
		return err
	}
	cur := idx[i]

	// Guard rails: COMPLIANCE cannot be shortened or lifted; GOVERNANCE can
	// only be shortened with the bypass flag.
	if cur.LockMode == "COMPLIANCE" && !cur.RetainUntil.IsZero() && time.Now().Before(cur.RetainUntil) {
		if mode != "COMPLIANCE" || until.Before(cur.RetainUntil) {
			return object.ErrObjectLocked
		}
	}
	if cur.LockMode == "GOVERNANCE" && !cur.RetainUntil.IsZero() && time.Now().Before(cur.RetainUntil) {
		if (mode == "" || until.Before(cur.RetainUntil)) && !bypassGovernance {
			return object.ErrObjectLocked
		}
	}

	idx[i].LockMode = mode
	idx[i].RetainUntil = until
	return f.writeVerIndex(bucket, obj, idx)
}

func (f *FS) GetObjectRetention(_ context.Context, bucket, obj, versionID string) (string, time.Time, error) {
	i, idx, err := f.findVersionEntry(bucket, obj, versionID)
	if err != nil {
		return "", time.Time{}, err
	}
	return idx[i].LockMode, idx[i].RetainUntil, nil
}

func (f *FS) PutObjectLegalHold(_ context.Context, bucket, obj, versionID, status string) error {
	lk := f.NewNSLock(bucket, obj)
	c, _ := lk.GetLock(context.Background(), 0)
	defer lk.Unlock(c)

	i, idx, err := f.findVersionEntry(bucket, obj, versionID)
	if err != nil {
		return err
	}
	idx[i].LegalHold = status == "ON"
	return f.writeVerIndex(bucket, obj, idx)
}

func (f *FS) GetObjectLegalHold(_ context.Context, bucket, obj, versionID string) (string, error) {
	i, idx, err := f.findVersionEntry(bucket, obj, versionID)
	if err != nil {
		return "", err
	}
	if idx[i].LegalHold {
		return "ON", nil
	}
	return "OFF", nil
}
