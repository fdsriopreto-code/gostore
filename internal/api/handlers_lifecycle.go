package api

import (
	"encoding/xml"
	"io"
	"net/http"
	"time"

	"github.com/lojadopocket/gostore/internal/bucketcfg"
)

type lifecycleConfigXML struct {
	XMLName xml.Name           `xml:"LifecycleConfiguration"`
	XMLNS   string             `xml:"xmlns,attr,omitempty"`
	Rules   []lifecycleRuleXML `xml:"Rule"`
}

type lifecycleRuleXML struct {
	ID     string `xml:"ID"`
	Status string `xml:"Status"`
	Filter *struct {
		Prefix string `xml:"Prefix"`
	} `xml:"Filter"`
	Prefix     string `xml:"Prefix"` // legacy top-level prefix
	Expiration *struct {
		Days                      int    `xml:"Days"`
		Date                      string `xml:"Date"`
		ExpiredObjectDeleteMarker bool   `xml:"ExpiredObjectDeleteMarker"`
	} `xml:"Expiration"`
	NoncurrentVersionExpiration *struct {
		NoncurrentDays int `xml:"NoncurrentDays"`
	} `xml:"NoncurrentVersionExpiration"`
	AbortIncompleteMultipartUpload *struct {
		DaysAfterInitiation int `xml:"DaysAfterInitiation"`
	} `xml:"AbortIncompleteMultipartUpload"`
	Transition *struct {
		Days         int    `xml:"Days"`
		StorageClass string `xml:"StorageClass"` // a gostore tier name
	} `xml:"Transition"`
}

func (s *Server) handleGetBucketLifecycle(w http.ResponseWriter, r *http.Request, bucket string) {
	rules := s.bcfg.Get(bucket).Lifecycle
	if len(rules) == 0 {
		writeErrorResponse(w, r, ErrNoSuchLifecycleConfiguration, "/"+bucket)
		return
	}
	out := lifecycleConfigXML{XMLNS: s3XMLNS}
	for _, rr := range rules {
		x := lifecycleRuleXML{ID: rr.ID, Status: rr.Status}
		x.Filter = &struct {
			Prefix string `xml:"Prefix"`
		}{Prefix: rr.Prefix}
		if rr.ExpirationDays > 0 || rr.ExpirationDate != "" || rr.ExpiredObjectDeleteMarker {
			x.Expiration = &struct {
				Days                      int    `xml:"Days"`
				Date                      string `xml:"Date"`
				ExpiredObjectDeleteMarker bool   `xml:"ExpiredObjectDeleteMarker"`
			}{rr.ExpirationDays, rr.ExpirationDate, rr.ExpiredObjectDeleteMarker}
		}
		if rr.NoncurrentVersionExpirationDays > 0 {
			x.NoncurrentVersionExpiration = &struct {
				NoncurrentDays int `xml:"NoncurrentDays"`
			}{rr.NoncurrentVersionExpirationDays}
		}
		if rr.AbortIncompleteMultipartDays > 0 {
			x.AbortIncompleteMultipartUpload = &struct {
				DaysAfterInitiation int `xml:"DaysAfterInitiation"`
			}{rr.AbortIncompleteMultipartDays}
		}
		if rr.TransitionTier != "" {
			x.Transition = &struct {
				Days         int    `xml:"Days"`
				StorageClass string `xml:"StorageClass"`
			}{rr.TransitionDays, rr.TransitionTier}
		}
		out.Rules = append(out.Rules, x)
	}
	writeXML(w, http.StatusOK, out)
}

func (s *Server) handlePutBucketLifecycle(w http.ResponseWriter, r *http.Request, bucket string) {
	if _, err := s.obj.GetBucketInfo(r.Context(), bucket); err != nil {
		writeErrorResponse(w, r, toAPIError(err), "/"+bucket)
		return
	}
	b, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	var in lifecycleConfigXML
	if err := xml.Unmarshal(b, &in); err != nil {
		writeErrorResponse(w, r, ErrMalformedXML, "/"+bucket)
		return
	}
	var rules []bucketcfg.LifecycleRule
	for _, x := range in.Rules {
		rule := bucketcfg.LifecycleRule{ID: x.ID, Status: x.Status, Prefix: x.Prefix}
		if x.Filter != nil && x.Filter.Prefix != "" {
			rule.Prefix = x.Filter.Prefix
		}
		if x.Expiration != nil {
			rule.ExpirationDays = x.Expiration.Days
			rule.ExpiredObjectDeleteMarker = x.Expiration.ExpiredObjectDeleteMarker
			if x.Expiration.Date != "" {
				if t, err := time.Parse(time.RFC3339, x.Expiration.Date); err == nil {
					rule.ExpirationDate = t.UTC().Format(time.RFC3339)
				}
			}
		}
		if x.NoncurrentVersionExpiration != nil {
			rule.NoncurrentVersionExpirationDays = x.NoncurrentVersionExpiration.NoncurrentDays
		}
		if x.AbortIncompleteMultipartUpload != nil {
			rule.AbortIncompleteMultipartDays = x.AbortIncompleteMultipartUpload.DaysAfterInitiation
		}
		if x.Transition != nil && x.Transition.StorageClass != "" {
			rule.TransitionDays = x.Transition.Days
			rule.TransitionTier = x.Transition.StorageClass
		}
		if rule.Status == "" {
			rule.Status = "Enabled"
		}
		rules = append(rules, rule)
	}
	_ = s.bcfg.Update(bucket, func(c *bucketcfg.Config) { c.Lifecycle = rules })
	writeSuccessOK(w)
}

func (s *Server) handleDeleteBucketLifecycle(w http.ResponseWriter, r *http.Request, bucket string) {
	_ = s.bcfg.Update(bucket, func(c *bucketcfg.Config) { c.Lifecycle = nil })
	writeSuccessNoContent(w)
}
