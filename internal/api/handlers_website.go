package api

import (
	"encoding/xml"
	"io"
	"net/http"
	"strings"

	"github.com/lojadopocket/gostore/internal/bucketcfg"
	"github.com/lojadopocket/gostore/internal/object"
)

// --- ?website sub-resource (S3 WebsiteConfiguration) --------------------

type websiteConfigXML struct {
	XMLName       xml.Name `xml:"WebsiteConfiguration"`
	IndexDocument struct {
		Suffix string `xml:"Suffix"`
	} `xml:"IndexDocument"`
	ErrorDocument struct {
		Key string `xml:"Key"`
	} `xml:"ErrorDocument"`
}

func (s *Server) handleGetBucketWebsite(w http.ResponseWriter, r *http.Request, bucket string) {
	ws := s.bcfg.Get(bucket).Website
	if ws == nil {
		writeErrorResponse(w, r, ErrNoSuchWebsiteConfiguration, "/"+bucket)
		return
	}
	var out websiteConfigXML
	out.IndexDocument.Suffix = ws.IndexDocument
	out.ErrorDocument.Key = ws.ErrorDocument
	writeXML(w, http.StatusOK, out)
}

func (s *Server) handlePutBucketWebsite(w http.ResponseWriter, r *http.Request, bucket string) {
	b, _ := io.ReadAll(io.LimitReader(r.Body, 256<<10))
	var in websiteConfigXML
	if err := xml.Unmarshal(b, &in); err != nil {
		writeErrorResponse(w, r, ErrMalformedXML, "/"+bucket)
		return
	}
	idx := strings.TrimSpace(in.IndexDocument.Suffix)
	if idx == "" {
		idx = "index.html"
	}
	if strings.Contains(idx, "/") {
		writeErrorResponse(w, r, ErrInvalidArgument, "/"+bucket)
		return
	}
	_ = s.bcfg.Update(bucket, func(c *bucketcfg.Config) {
		c.Website = &bucketcfg.WebsiteConfig{
			IndexDocument: idx,
			ErrorDocument: strings.TrimSpace(in.ErrorDocument.Key),
		}
	})
	writeSuccessOK(w)
}

func (s *Server) handleDeleteBucketWebsite(w http.ResponseWriter, r *http.Request, bucket string) {
	_ = s.bcfg.Update(bucket, func(c *bucketcfg.Config) { c.Website = nil })
	writeSuccessNoContent(w)
}

// --- static-site serving ----------------------------------------------

// websiteRequest reports whether this request should be served as static-site
// content for a website-enabled bucket: a plain GET/HEAD with no S3
// sub-resource query, for a bucket that has website config.
func (s *Server) websiteConfigFor(bucket string, q map[string][]string) *bucketcfg.WebsiteConfig {
	if bucket == "" || s.bcfg == nil {
		return nil
	}
	if len(q) > 0 {
		// Any query param means "treat as an S3 API call", not site content
		// (?list-type, ?versionId, ?w=, ?uploads, …).
		return nil
	}
	return s.bcfg.Get(bucket).Website
}

// serveWebsite resolves object per index/error-document rules and serves it
// through the normal object path (cache, range, conditionals all apply).
func (s *Server) serveWebsite(w http.ResponseWriter, r *http.Request, bucket, obj string, ws *bucketcfg.WebsiteConfig, accessKey string) {
	target := obj
	if target == "" || strings.HasSuffix(target, "/") {
		target += ws.IndexDocument
	}

	// Authorize s3:GetObject on the resolved key (the outer authz ran against
	// the original path, which for "/" was s3:ListBucket).
	if code := s.authorizeS3(r, s3Request{Bucket: bucket, Object: target}, map[string][]string{}, accessKey); code != ErrNone {
		s.websiteError(w, r, bucket, ws, http.StatusForbidden, "403 Forbidden")
		return
	}

	_, err := s.obj.GetObjectInfo(r.Context(), bucket, target, object.ObjectOptions{})
	if err != nil {
		// "dir" with no trailing slash → retry as "dir/index".
		if !strings.HasSuffix(obj, "/") && obj != "" {
			alt := obj + "/" + ws.IndexDocument
			if _, e2 := s.obj.GetObjectInfo(r.Context(), bucket, alt, object.ObjectOptions{}); e2 == nil {
				target = alt
				err = nil
			}
		}
	}
	if err != nil {
		s.websiteError(w, r, bucket, ws, http.StatusNotFound, "404 Not Found")
		return
	}

	r2 := r.Clone(r.Context())
	r2.URL.Path = "/" + bucket + "/" + target
	s.getOrHeadObject(w, r2, bucket, target, r.Method == http.MethodGet)
}

// websiteError serves the configured error document (with the given status),
// falling back to a tiny plain-text body.
func (s *Server) websiteError(w http.ResponseWriter, r *http.Request, bucket string, ws *bucketcfg.WebsiteConfig, status int, fallback string) {
	if ws.ErrorDocument != "" {
		if gr, err := s.obj.GetObjectNInfo(r.Context(), bucket, ws.ErrorDocument, nil, nil, object.ObjectOptions{}); err == nil {
			defer gr.Close()
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			w.WriteHeader(status)
			if r.Method == http.MethodGet {
				_, _ = io.Copy(w, gr)
			}
			return
		}
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(status)
	if r.Method == http.MethodGet {
		_, _ = io.WriteString(w, fallback+"\n")
	}
}
