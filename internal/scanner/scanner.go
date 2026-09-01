// Package scanner runs a periodic background pass over every bucket applying
// lifecycle (ILM) rules: object expiration, noncurrent-version expiration and
// aborting stale multipart uploads. It is also the natural home for future
// background healing and usage accounting.
package scanner

import (
	"context"
	"errors"
	"time"

	"github.com/lojadopocket/gostore/internal/bucketcfg"
	"github.com/lojadopocket/gostore/internal/logger"
	"github.com/lojadopocket/gostore/internal/object"
)

// Scanner walks the object layer on an interval.
type Scanner struct {
	obj      object.Layer
	cfg      *bucketcfg.Store
	interval time.Duration
}

// New builds a Scanner. interval <= 0 defaults to one hour.
func New(obj object.Layer, cfg *bucketcfg.Store, interval time.Duration) *Scanner {
	if interval <= 0 {
		interval = time.Hour
	}
	return &Scanner{obj: obj, cfg: cfg, interval: interval}
}

// Run scans once immediately, then every interval, until ctx is done.
func (s *Scanner) Run(ctx context.Context) {
	t := time.NewTicker(s.interval)
	defer t.Stop()
	if rep := s.ScanOnce(ctx); rep.nonZero() {
		logger.Info("lifecycle scan", rep.args()...)
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if rep := s.ScanOnce(ctx); rep.nonZero() {
				logger.Info("lifecycle scan", rep.args()...)
			}
		}
	}
}

// Report summarises a scan pass.
type Report struct {
	BucketsScanned  int `json:"bucketsScanned"`
	ObjectsExpired  int `json:"objectsExpired"`
	VersionsExpired int `json:"noncurrentVersionsExpired"`
	UploadsAborted  int `json:"multipartUploadsAborted"`
	Errors          int `json:"errors"`
}

func (r Report) nonZero() bool {
	return r.ObjectsExpired+r.VersionsExpired+r.UploadsAborted+r.Errors > 0
}
func (r Report) args() []any {
	return []any{
		"buckets", r.BucketsScanned, "objectsExpired", r.ObjectsExpired,
		"versionsExpired", r.VersionsExpired, "uploadsAborted", r.UploadsAborted, "errors", r.Errors,
	}
}

// ScanOnce runs a single pass and returns what it did.
func (s *Scanner) ScanOnce(ctx context.Context) Report {
	var rep Report
	buckets, err := s.obj.ListBuckets(ctx)
	if err != nil {
		rep.Errors++
		return rep
	}
	now := time.Now().UTC()
	for _, b := range buckets {
		rules := s.cfg.Get(b.Name).Lifecycle
		if len(rules) == 0 {
			continue
		}
		rep.BucketsScanned++
		for _, rule := range rules {
			if rule.Status != "Enabled" {
				continue
			}
			s.applyRule(ctx, b.Name, rule, now, &rep)
		}
	}
	return rep
}

func (s *Scanner) applyRule(ctx context.Context, bucket string, rule bucketcfg.LifecycleRule, now time.Time, rep *Report) {
	var expireDate time.Time
	if rule.ExpirationDate != "" {
		if t, err := time.Parse(time.RFC3339, rule.ExpirationDate); err == nil {
			expireDate = t
		}
	}

	// 1. current-version / plain object expiration
	if rule.ExpirationDays > 0 || !expireDate.IsZero() {
		token := ""
		for {
			li, err := s.obj.ListObjectsV2(ctx, bucket, rule.Prefix, token, "", 1000, false, "")
			if err != nil {
				rep.Errors++
				break
			}
			for _, o := range li.Objects {
				if !dueForExpiry(o.ModTime, rule.ExpirationDays, expireDate, now) {
					continue
				}
				if _, err := s.obj.DeleteObject(ctx, bucket, o.Name, object.ObjectOptions{Versioned: true}); err != nil {
					if !errors.Is(err, object.ErrObjectLocked) {
						rep.Errors++
					}
					continue
				}
				rep.ObjectsExpired++
			}
			if !li.IsTruncated {
				break
			}
			token = li.NextContinuationToken
		}
	}

	// 2. noncurrent version expiration
	if rule.NoncurrentVersionExpirationDays > 0 {
		lv, err := s.obj.ListObjectVersions(ctx, bucket, rule.Prefix, "", "", "", 1000)
		if err != nil {
			rep.Errors++
		} else {
			for _, o := range lv.Objects {
				if o.IsLatest || o.VersionID == "" || o.VersionID == "null" {
					continue
				}
				if now.Sub(o.ModTime) < time.Duration(rule.NoncurrentVersionExpirationDays)*24*time.Hour {
					continue
				}
				if _, err := s.obj.DeleteObject(ctx, bucket, o.Name, object.ObjectOptions{VersionID: o.VersionID}); err != nil {
					if !errors.Is(err, object.ErrObjectLocked) {
						rep.Errors++
					}
					continue
				}
				rep.VersionsExpired++
			}
		}
	}

	// 3. abort incomplete multipart uploads
	if rule.AbortIncompleteMultipartDays > 0 {
		mu, err := s.obj.ListMultipartUploads(ctx, bucket, rule.Prefix, "", "", "", 1000)
		if err != nil {
			rep.Errors++
		} else {
			cutoff := now.Add(-time.Duration(rule.AbortIncompleteMultipartDays) * 24 * time.Hour)
			for _, u := range mu.Uploads {
				if u.Initiated.After(cutoff) {
					continue
				}
				if err := s.obj.AbortMultipartUpload(ctx, bucket, u.Object, u.UploadID, object.ObjectOptions{}); err != nil {
					rep.Errors++
					continue
				}
				rep.UploadsAborted++
			}
		}
	}
}

func dueForExpiry(mod time.Time, days int, date, now time.Time) bool {
	if !date.IsZero() && now.After(date) {
		return true
	}
	return days > 0 && now.Sub(mod) >= time.Duration(days)*24*time.Hour
}
