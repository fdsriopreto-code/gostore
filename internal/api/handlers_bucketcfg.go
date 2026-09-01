package api

import (
	"encoding/json"
	"encoding/xml"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"

	"github.com/lojadopocket/gostore/internal/bucketcfg"
	"github.com/lojadopocket/gostore/internal/object"
)

// --- Tagging XML <-> "k=v&k=v" -----------------------------------------

type tagXML struct {
	Key   string `xml:"Key"`
	Value string `xml:"Value"`
}
type taggingXML struct {
	XMLName xml.Name `xml:"Tagging"`
	XMLNS   string   `xml:"xmlns,attr,omitempty"`
	TagSet  struct {
		Tags []tagXML `xml:"Tag"`
	} `xml:"TagSet"`
}

func tagsFromXML(b []byte) (string, error) {
	var t taggingXML
	if err := xml.Unmarshal(b, &t); err != nil {
		return "", err
	}
	vals := url.Values{}
	for _, tag := range t.TagSet.Tags {
		vals.Set(tag.Key, tag.Value)
	}
	return vals.Encode(), nil
}

func tagsToXML(raw string) taggingXML {
	var t taggingXML
	t.XMLNS = s3XMLNS
	vals, _ := url.ParseQuery(raw)
	keys := make([]string, 0, len(vals))
	for k := range vals {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		t.TagSet.Tags = append(t.TagSet.Tags, tagXML{Key: k, Value: vals.Get(k)})
	}
	return t
}

// --- object tagging ------------------------------------------------

func (s *Server) handleGetObjectTagging(w http.ResponseWriter, r *http.Request, bucket, key string) {
	raw, err := s.obj.GetObjectTags(r.Context(), bucket, key, object.ObjectOptions{})
	if err != nil {
		writeErrorResponse(w, r, toAPIError(err), "/"+bucket+"/"+key)
		return
	}
	writeXML(w, http.StatusOK, tagsToXML(raw))
}

func (s *Server) handlePutObjectTagging(w http.ResponseWriter, r *http.Request, bucket, key string) {
	b, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	raw, err := tagsFromXML(b)
	if err != nil {
		writeErrorResponse(w, r, ErrMalformedXML, "/"+bucket+"/"+key)
		return
	}
	if _, err := s.obj.PutObjectTags(r.Context(), bucket, key, raw, object.ObjectOptions{}); err != nil {
		writeErrorResponse(w, r, toAPIError(err), "/"+bucket+"/"+key)
		return
	}
	writeSuccessOK(w)
}

func (s *Server) handleDeleteObjectTagging(w http.ResponseWriter, r *http.Request, bucket, key string) {
	if err := s.obj.DeleteObjectTags(r.Context(), bucket, key, object.ObjectOptions{}); err != nil {
		writeErrorResponse(w, r, toAPIError(err), "/"+bucket+"/"+key)
		return
	}
	writeSuccessNoContent(w)
}

// --- bucket tagging ----------------------------------------------

func (s *Server) handleGetBucketTagging(w http.ResponseWriter, r *http.Request, bucket string) {
	if _, err := s.obj.GetBucketInfo(r.Context(), bucket); err != nil {
		writeErrorResponse(w, r, toAPIError(err), "/"+bucket)
		return
	}
	tags := s.bcfg.Get(bucket).Tags
	vals := url.Values{}
	for k, v := range tags {
		vals.Set(k, v)
	}
	writeXML(w, http.StatusOK, tagsToXML(vals.Encode()))
}

func (s *Server) handlePutBucketTagging(w http.ResponseWriter, r *http.Request, bucket string) {
	b, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	var t taggingXML
	if err := xml.Unmarshal(b, &t); err != nil {
		writeErrorResponse(w, r, ErrMalformedXML, "/"+bucket)
		return
	}
	m := map[string]string{}
	for _, tag := range t.TagSet.Tags {
		m[tag.Key] = tag.Value
	}
	if err := s.bcfg.Update(bucket, func(c *bucketcfg.Config) { c.Tags = m }); err != nil {
		writeErrorResponse(w, r, ErrInternalError, "/"+bucket)
		return
	}
	writeSuccessNoContent(w)
}

func (s *Server) handleDeleteBucketTagging(w http.ResponseWriter, r *http.Request, bucket string) {
	_ = s.bcfg.Update(bucket, func(c *bucketcfg.Config) { c.Tags = nil })
	writeSuccessNoContent(w)
}

// --- bucket policy ---------------------------------------------

func (s *Server) handleGetBucketPolicy(w http.ResponseWriter, r *http.Request, bucket string) {
	doc := s.bcfg.Get(bucket).Policy
	if len(doc) == 0 {
		writeErrorResponse(w, r, ErrNoSuchBucketPolicy, "/"+bucket)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(doc)
}

func (s *Server) handlePutBucketPolicy(w http.ResponseWriter, r *http.Request, bucket string) {
	if _, err := s.obj.GetBucketInfo(r.Context(), bucket); err != nil {
		writeErrorResponse(w, r, toAPIError(err), "/"+bucket)
		return
	}
	b, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if !json.Valid(b) {
		writeErrorResponse(w, r, ErrMalformedXML, "/"+bucket)
		return
	}
	if err := s.bcfg.Update(bucket, func(c *bucketcfg.Config) { c.Policy = append([]byte(nil), b...) }); err != nil {
		writeErrorResponse(w, r, ErrInternalError, "/"+bucket)
		return
	}
	writeSuccessNoContent(w)
}

func (s *Server) handleDeleteBucketPolicy(w http.ResponseWriter, r *http.Request, bucket string) {
	_ = s.bcfg.Update(bucket, func(c *bucketcfg.Config) { c.Policy = nil })
	writeSuccessNoContent(w)
}

// --- bucket CORS ---------------------------------------------

type corsConfigXML struct {
	XMLName xml.Name      `xml:"CORSConfiguration"`
	Rules   []corsRuleXML `xml:"CORSRule"`
}
type corsRuleXML struct {
	AllowedOrigin []string `xml:"AllowedOrigin"`
	AllowedMethod []string `xml:"AllowedMethod"`
	AllowedHeader []string `xml:"AllowedHeader"`
	ExposeHeader  []string `xml:"ExposeHeader"`
	MaxAgeSeconds int      `xml:"MaxAgeSeconds"`
}

func (s *Server) handleGetBucketCORS(w http.ResponseWriter, r *http.Request, bucket string) {
	rules := s.bcfg.Get(bucket).CORS
	if len(rules) == 0 {
		writeErrorResponse(w, r, ErrNoSuchCORSConfiguration, "/"+bucket)
		return
	}
	var out corsConfigXML
	for _, rr := range rules {
		out.Rules = append(out.Rules, corsRuleXML{
			AllowedOrigin: rr.AllowedOrigins, AllowedMethod: rr.AllowedMethods,
			AllowedHeader: rr.AllowedHeaders, ExposeHeader: rr.ExposeHeaders,
			MaxAgeSeconds: rr.MaxAgeSeconds,
		})
	}
	writeXML(w, http.StatusOK, out)
}

func (s *Server) handlePutBucketCORS(w http.ResponseWriter, r *http.Request, bucket string) {
	b, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	var in corsConfigXML
	if err := xml.Unmarshal(b, &in); err != nil {
		writeErrorResponse(w, r, ErrMalformedXML, "/"+bucket)
		return
	}
	var rules []bucketcfg.CORSRule
	for _, rr := range in.Rules {
		rules = append(rules, bucketcfg.CORSRule{
			AllowedOrigins: rr.AllowedOrigin, AllowedMethods: rr.AllowedMethod,
			AllowedHeaders: rr.AllowedHeader, ExposeHeaders: rr.ExposeHeader,
			MaxAgeSeconds: rr.MaxAgeSeconds,
		})
	}
	_ = s.bcfg.Update(bucket, func(c *bucketcfg.Config) { c.CORS = rules })
	writeSuccessOK(w)
}

func (s *Server) handleDeleteBucketCORS(w http.ResponseWriter, r *http.Request, bucket string) {
	_ = s.bcfg.Update(bucket, func(c *bucketcfg.Config) { c.CORS = nil })
	writeSuccessNoContent(w)
}

// applyCORS sets response CORS headers when the request Origin matches a rule.
// Returns true if it fully handled an OPTIONS preflight.
func (s *Server) applyCORS(w http.ResponseWriter, r *http.Request, bucket string) bool {
	origin := r.Header.Get("Origin")
	if origin == "" || bucket == "" {
		return false
	}
	for _, rule := range s.bcfg.Get(bucket).CORS {
		if !originAllowed(rule.AllowedOrigins, origin) {
			continue
		}
		method := r.Header.Get("Access-Control-Request-Method")
		if method == "" {
			method = r.Method
		}
		if !contains(rule.AllowedMethods, method) && !contains(rule.AllowedMethods, "*") {
			continue
		}
		h := w.Header()
		h.Set("Access-Control-Allow-Origin", origin)
		h.Set("Vary", "Origin")
		h.Set("Access-Control-Allow-Methods", strings.Join(rule.AllowedMethods, ", "))
		if len(rule.AllowedHeaders) > 0 {
			h.Set("Access-Control-Allow-Headers", strings.Join(rule.AllowedHeaders, ", "))
		}
		if len(rule.ExposeHeaders) > 0 {
			h.Set("Access-Control-Expose-Headers", strings.Join(rule.ExposeHeaders, ", "))
		}
		if rule.MaxAgeSeconds > 0 {
			h.Set("Access-Control-Max-Age", strconv.Itoa(rule.MaxAgeSeconds))
		}
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return true
		}
		return false
	}
	return false
}

func originAllowed(patterns []string, origin string) bool {
	for _, p := range patterns {
		if p == "*" || p == origin {
			return true
		}
		if strings.HasPrefix(p, "*") && strings.HasSuffix(origin, p[1:]) {
			return true
		}
	}
	return false
}

func contains(ss []string, v string) bool {
	for _, s := range ss {
		if strings.EqualFold(s, v) {
			return true
		}
	}
	return false
}

// --- bucket notification ------------------------------------

func (s *Server) handleGetBucketNotification(w http.ResponseWriter, r *http.Request, bucket string) {
	n := s.bcfg.Get(bucket).Notification
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if n == nil {
		_, _ = w.Write([]byte(`{"webhooks":[]}`))
		return
	}
	_ = json.NewEncoder(w).Encode(n)
}

func (s *Server) handlePutBucketNotification(w http.ResponseWriter, r *http.Request, bucket string) {
	b, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	var n bucketcfg.Notification
	if err := json.Unmarshal(b, &n); err != nil {
		writeErrorResponse(w, r, ErrMalformedXML, "/"+bucket)
		return
	}
	_ = s.bcfg.Update(bucket, func(c *bucketcfg.Config) { c.Notification = &n })
	writeSuccessOK(w)
}

// --- bucket replication (native JSON) --------------------------

func (s *Server) handleGetBucketReplication(w http.ResponseWriter, r *http.Request, bucket string) {
	rules := s.bcfg.Get(bucket).Replication
	if rules == nil {
		rules = []bucketcfg.ReplicationRule{}
	}
	// Never echo secrets back.
	out := make([]bucketcfg.ReplicationRule, len(rules))
	copy(out, rules)
	for i := range out {
		if out[i].DestSecretKey != "" {
			out[i].DestSecretKey = "***"
		}
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) handlePutBucketReplication(w http.ResponseWriter, r *http.Request, bucket string) {
	if _, err := s.obj.GetBucketInfo(r.Context(), bucket); err != nil {
		writeErrorResponse(w, r, toAPIError(err), "/"+bucket)
		return
	}
	b, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	var rules []bucketcfg.ReplicationRule
	if err := json.Unmarshal(b, &rules); err != nil {
		writeErrorResponse(w, r, ErrMalformedXML, "/"+bucket)
		return
	}
	for i := range rules {
		if rules[i].DestBucket == "" {
			writeErrorResponse(w, r, ErrInvalidArgument, "/"+bucket)
			return
		}
	}
	// Preserve an existing secret when the client sends the masked "***".
	prev := s.bcfg.Get(bucket).Replication
	for i := range rules {
		if rules[i].DestSecretKey == "***" {
			for _, p := range prev {
				if p.ID == rules[i].ID {
					rules[i].DestSecretKey = p.DestSecretKey
				}
			}
		}
	}
	_ = s.bcfg.Update(bucket, func(c *bucketcfg.Config) { c.Replication = rules })
	writeSuccessOK(w)
}

func (s *Server) handleDeleteBucketReplication(w http.ResponseWriter, r *http.Request, bucket string) {
	_ = s.bcfg.Update(bucket, func(c *bucketcfg.Config) { c.Replication = nil })
	writeSuccessNoContent(w)
}
