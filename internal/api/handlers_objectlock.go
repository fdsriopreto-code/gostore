package api

import (
	"encoding/xml"
	"io"
	"net/http"
	"time"

	"github.com/lojadopocket/gostore/internal/bucketcfg"
	"github.com/lojadopocket/gostore/internal/iam/policy"
)

// --- XML shapes ------------------------------------------------------------

type retentionXML struct {
	XMLName         xml.Name `xml:"Retention"`
	XMLNS           string   `xml:"xmlns,attr,omitempty"`
	Mode            string   `xml:"Mode,omitempty"`
	RetainUntilDate string   `xml:"RetainUntilDate,omitempty"`
}

type legalHoldXML struct {
	XMLName xml.Name `xml:"LegalHold"`
	XMLNS   string   `xml:"xmlns,attr,omitempty"`
	Status  string   `xml:"Status,omitempty"`
}

type objectLockConfigXML struct {
	XMLName           xml.Name `xml:"ObjectLockConfiguration"`
	XMLNS             string   `xml:"xmlns,attr,omitempty"`
	ObjectLockEnabled string   `xml:"ObjectLockEnabled,omitempty"`
	Rule              *struct {
		DefaultRetention struct {
			Mode  string `xml:"Mode"`
			Days  int    `xml:"Days,omitempty"`
			Years int    `xml:"Years,omitempty"`
		} `xml:"DefaultRetention"`
	} `xml:"Rule,omitempty"`
}

// bypassAllowed reports whether the caller may bypass GOVERNANCE retention.
func (s *Server) bypassAllowed(r *http.Request, bucket, key, accessKey string) bool {
	if r.Header.Get("x-amz-bypass-governance-retention") != "true" {
		return false
	}
	return accessKey != "" && s.iam.IsAllowed(accessKey, policy.Args{
		Action: "s3:BypassGovernanceRetention", BucketName: bucket, ObjectName: key,
	})
}

// --- object retention ------------------------------------------------

func (s *Server) handleGetObjectRetention(w http.ResponseWriter, r *http.Request, bucket, key string) {
	mode, until, err := s.obj.GetObjectRetention(r.Context(), bucket, key, r.URL.Query().Get("versionId"))
	if err != nil {
		writeErrorResponse(w, r, toAPIError(err), "/"+bucket+"/"+key)
		return
	}
	if mode == "" {
		writeErrorResponse(w, r, ErrNoSuchObjectLockConfiguration, "/"+bucket+"/"+key)
		return
	}
	writeXML(w, http.StatusOK, retentionXML{XMLNS: s3XMLNS, Mode: mode, RetainUntilDate: until.UTC().Format(time.RFC3339)})
}

func (s *Server) handlePutObjectRetention(w http.ResponseWriter, r *http.Request, bucket, key string) {
	b, _ := io.ReadAll(io.LimitReader(r.Body, 1<<16))
	var in retentionXML
	if err := xml.Unmarshal(b, &in); err != nil {
		writeErrorResponse(w, r, ErrMalformedXML, "/"+bucket+"/"+key)
		return
	}
	var until time.Time
	if in.RetainUntilDate != "" {
		t, perr := time.Parse(time.RFC3339, in.RetainUntilDate)
		if perr != nil {
			writeErrorResponse(w, r, ErrInvalidArgument, "/"+bucket+"/"+key)
			return
		}
		until = t
	}
	bypass := s.bypassAllowed(r, bucket, key, accessKeyFrom(r))
	if err := s.obj.PutObjectRetention(r.Context(), bucket, key, r.URL.Query().Get("versionId"), in.Mode, until, bypass); err != nil {
		writeErrorResponse(w, r, toAPIError(err), "/"+bucket+"/"+key)
		return
	}
	writeSuccessOK(w)
}

// --- object legal hold ---------------------------------------------

func (s *Server) handleGetObjectLegalHold(w http.ResponseWriter, r *http.Request, bucket, key string) {
	st, err := s.obj.GetObjectLegalHold(r.Context(), bucket, key, r.URL.Query().Get("versionId"))
	if err != nil {
		writeErrorResponse(w, r, toAPIError(err), "/"+bucket+"/"+key)
		return
	}
	writeXML(w, http.StatusOK, legalHoldXML{XMLNS: s3XMLNS, Status: st})
}

func (s *Server) handlePutObjectLegalHold(w http.ResponseWriter, r *http.Request, bucket, key string) {
	b, _ := io.ReadAll(io.LimitReader(r.Body, 1<<16))
	var in legalHoldXML
	if err := xml.Unmarshal(b, &in); err != nil || (in.Status != "ON" && in.Status != "OFF") {
		writeErrorResponse(w, r, ErrMalformedXML, "/"+bucket+"/"+key)
		return
	}
	if err := s.obj.PutObjectLegalHold(r.Context(), bucket, key, r.URL.Query().Get("versionId"), in.Status); err != nil {
		writeErrorResponse(w, r, toAPIError(err), "/"+bucket+"/"+key)
		return
	}
	writeSuccessOK(w)
}

// --- bucket object-lock config -----------------------------------

func (s *Server) handleGetBucketObjectLock(w http.ResponseWriter, r *http.Request, bucket string) {
	cfg := s.bcfg.Get(bucket).ObjectLock
	if cfg == nil || !cfg.Enabled {
		writeErrorResponse(w, r, ErrNoSuchObjectLockConfiguration, "/"+bucket)
		return
	}
	out := objectLockConfigXML{XMLNS: s3XMLNS, ObjectLockEnabled: "Enabled"}
	if cfg.DefaultMode != "" {
		out.Rule = &struct {
			DefaultRetention struct {
				Mode  string `xml:"Mode"`
				Days  int    `xml:"Days,omitempty"`
				Years int    `xml:"Years,omitempty"`
			} `xml:"DefaultRetention"`
		}{}
		out.Rule.DefaultRetention.Mode = cfg.DefaultMode
		out.Rule.DefaultRetention.Days = cfg.DefaultDays
		out.Rule.DefaultRetention.Years = cfg.DefaultYears
	}
	writeXML(w, http.StatusOK, out)
}

func (s *Server) handlePutBucketObjectLock(w http.ResponseWriter, r *http.Request, bucket string) {
	b, _ := io.ReadAll(io.LimitReader(r.Body, 1<<16))
	var in objectLockConfigXML
	if err := xml.Unmarshal(b, &in); err != nil {
		writeErrorResponse(w, r, ErrMalformedXML, "/"+bucket)
		return
	}
	nc := &bucketcfg.ObjectLockConfig{Enabled: true}
	if in.Rule != nil {
		nc.DefaultMode = in.Rule.DefaultRetention.Mode
		nc.DefaultDays = in.Rule.DefaultRetention.Days
		nc.DefaultYears = in.Rule.DefaultRetention.Years
	}
	if err := s.bcfg.Update(bucket, func(c *bucketcfg.Config) {
		c.ObjectLock = nc
		if c.Versioning == "" {
			c.Versioning = "Enabled"
		}
	}); err != nil {
		writeErrorResponse(w, r, ErrInternalError, "/"+bucket)
		return
	}
	writeSuccessOK(w)
}

// defaultRetainUntil computes the retain-until date from a bucket default rule.
func defaultRetainUntil(c *bucketcfg.ObjectLockConfig) (string, time.Time) {
	if c == nil || c.DefaultMode == "" {
		return "", time.Time{}
	}
	d := time.Duration(c.DefaultDays)*24*time.Hour + time.Duration(c.DefaultYears)*365*24*time.Hour
	if d == 0 {
		return "", time.Time{}
	}
	return c.DefaultMode, time.Now().UTC().Add(d)
}
