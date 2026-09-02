// Package scanner runs a periodic background pass over every bucket. It
// applies lifecycle (ILM) rules — object expiration, noncurrent-version
// expiration, aborting stale multipart uploads — and while it is already
// walking the namespace it also accumulates per-bucket data-usage stats and
// opportunistically heals a sample of objects (erasure backend).
package scanner

import (
	"context"
	"errors"
	"hash/fnv"
	"sync/atomic"
	"time"

	"github.com/lojadopocket/gostore/internal/bucketcfg"
	"github.com/lojadopocket/gostore/internal/logger"
	"github.com/lojadopocket/gostore/internal/object"
)

// objectHealer is implemented by the erasure backend: repair one object's
// missing/corrupt shards and metadata in place.
type objectHealer interface {
	HealObject(ctx context.Context, bucket, key string) error
}

// healSampleRate: 1-in-N objects are opportunistically healed per pass
// (matches MinIO's data-scanner sampling idea).
const healSampleRate = 128

// BucketUsage is the accounted size of one bucket.
type BucketUsage struct {
	Objects int64 `json:"objects"`
	Bytes   int64 `json:"bytes"`
}

// DataUsage is the result of the most recent accounting pass.
type DataUsage struct {
	LastUpdate   time.Time              `json:"lastUpdate"`
	Buckets      map[string]BucketUsage `json:"buckets"`
	TotalObjects int64                  `json:"totalObjects"`
	TotalBytes   int64                  `json:"totalBytes"`
}

// Scanner walks the object layer on an interval.
type Scanner struct {
	obj      object.Layer
	cfg      *bucketcfg.Store
	interval time.Duration
	healer   objectHealer // nil if the backend can't heal

	usage       atomic.Pointer[DataUsage]
	healScanned uint64
}

// New builds a Scanner. interval <= 0 defaults to one hour.
func New(obj object.Layer, cfg *bucketcfg.Store, interval time.Duration) *Scanner {
	if interval <= 0 {
		interval = time.Hour
	}
	s := &Scanner{obj: obj, cfg: cfg, interval: interval}
	if h, ok := obj.(objectHealer); ok {
		s.healer = h
	}
	return s
}

// Usage returns the most recent data-usage snapshot (nil before the first
// pass completes).
func (s *Scanner) Usage() *DataUsage { return s.usage.Load() }

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
	ObjectsHealed   int `json:"objectsHealed"`
	Errors          int `json:"errors"`
}

func (r Report) nonZero() bool {
	return r.ObjectsExpired+r.VersionsExpired+r.UploadsAborted+r.ObjectsHealed+r.Errors > 0
}
func (r Report) args() []any {
	return []any{
		"buckets", r.BucketsScanned, "objectsExpired", r.ObjectsExpired,
		"versionsExpired", r.VersionsExpired, "uploadsAborted", r.UploadsAborted,
		"objectsHealed", r.ObjectsHealed, "errors", r.Errors,
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

	usage := &DataUsage{LastUpdate: now, Buckets: map[string]BucketUsage{}}
	for _, b := range buckets {
		// Walk every object once: accumulate usage + opportunistic heal.
		bu := s.walkBucket(ctx, b.Name, &rep)
		usage.Buckets[b.Name] = bu
		usage.TotalObjects += bu.Objects
		usage.TotalBytes += bu.Bytes

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
	s.usage.Store(usage)
	return rep
}

// walkBucket lists every object in the bucket, tallies size, and heals a
// 1-in-healSampleRate sample when the backend supports it.
func (s *Scanner) walkBucket(ctx context.Context, bucket string, rep *Report) BucketUsage {
	var bu BucketUsage
	token := ""
	for {
		li, err := s.obj.ListObjectsV2(ctx, bucket, "", token, "", 1000, false, "")
		if err != nil {
			rep.Errors++
			return bu
		}
		for _, o := range li.Objects {
			bu.Objects++
			bu.Bytes += o.Size
			if s.healer != nil && s.shouldHeal(bucket, o.Name) {
				if err := s.healer.HealObject(ctx, bucket, o.Name); err == nil {
					rep.ObjectsHealed++
				} else {
					rep.Errors++
				}
			}
		}
		if !li.IsTruncated {
			return bu
		}
		token = li.NextContinuationToken
	}
}

func (s *Scanner) shouldHeal(bucket, key string) bool {
	// Deterministic per-object sampling, plus a rotating offset so a
	// different slice is covered each pass.
	h := fnv.New32a()
	_, _ = h.Write([]byte(bucket + "/" + key))
	pass := uint32(atomic.AddUint64(&s.healScanned, 1) / 4096)
	return (h.Sum32()+pass)%healSampleRate == 0
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
