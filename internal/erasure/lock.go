package erasure

import (
	"context"
	"path"
	"time"

	"github.com/lojadopocket/gostore/internal/object"
)

// findVlogEntry returns the index of the target version (latest when
// versionID == "") in the version log.
func (s *Set) findVlogEntry(ctx context.Context, bucket, key, versionID string) (int, []vlogEntry, error) {
	vlog, err := s.readVlog(ctx, bucket, key)
	if err != nil {
		return -1, nil, err
	}
	if len(vlog) == 0 {
		return -1, nil, object.ErrNotVersioned
	}
	if versionID == "" {
		return len(vlog) - 1, vlog, nil
	}
	for i := range vlog {
		if vlog[i].ID == versionID {
			return i, vlog, nil
		}
	}
	return -1, vlog, object.ObjectNotFound{Bucket: bucket, Object: key}
}

// syncCurrentLock copies lock fields into the live xl.meta when the targeted
// version is the current one.
func (s *Set) syncCurrentLock(ctx context.Context, bucket, key string, e vlogEntry) {
	m, err := s.readMeta(ctx, bucket, key)
	if err != nil || m.VersionID != e.ID {
		return
	}
	m.LockMode, m.RetainUntil, m.LegalHold = e.LockMode, e.RetainUntil, e.LegalHold
	mb, _ := m.marshal()
	_ = s.forEachDisk(func(d Disk) error { return d.WriteAll(ctx, bucket, path.Join(key, metaFile), mb) })
}

func (p *Pool) PutObjectRetention(ctx context.Context, bucket, key, versionID, mode string, until time.Time, bypassGovernance bool) error {
	set := p.setFor(key)
	lk := p.NewNSLock(bucket, key)
	c, _ := lk.GetLock(ctx, 0)
	defer lk.Unlock(c)

	i, vlog, err := set.findVlogEntry(ctx, bucket, key, versionID)
	if err != nil {
		return err
	}
	cur := vlog[i]
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
	vlog[i].LockMode = mode
	vlog[i].RetainUntil = until
	if err := set.writeVlog(ctx, bucket, key, vlog); err != nil {
		return err
	}
	set.syncCurrentLock(ctx, bucket, key, vlog[i])
	return nil
}

func (p *Pool) GetObjectRetention(ctx context.Context, bucket, key, versionID string) (string, time.Time, error) {
	i, vlog, err := p.setFor(key).findVlogEntry(ctx, bucket, key, versionID)
	if err != nil {
		return "", time.Time{}, err
	}
	return vlog[i].LockMode, vlog[i].RetainUntil, nil
}

func (p *Pool) PutObjectLegalHold(ctx context.Context, bucket, key, versionID, status string) error {
	set := p.setFor(key)
	lk := p.NewNSLock(bucket, key)
	c, _ := lk.GetLock(ctx, 0)
	defer lk.Unlock(c)

	i, vlog, err := set.findVlogEntry(ctx, bucket, key, versionID)
	if err != nil {
		return err
	}
	vlog[i].LegalHold = status == "ON"
	if err := set.writeVlog(ctx, bucket, key, vlog); err != nil {
		return err
	}
	set.syncCurrentLock(ctx, bucket, key, vlog[i])
	return nil
}

func (p *Pool) GetObjectLegalHold(ctx context.Context, bucket, key, versionID string) (string, error) {
	i, vlog, err := p.setFor(key).findVlogEntry(ctx, bucket, key, versionID)
	if err != nil {
		return "", err
	}
	if vlog[i].LegalHold {
		return "ON", nil
	}
	return "OFF", nil
}
