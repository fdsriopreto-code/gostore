package api

import (
	"encoding/xml"
	"net/http"
	"strconv"
	"time"
)

const (
	iso8601Full = "2006-01-02T15:04:05.000Z"
	rfc1123GMT  = "Mon, 02 Jan 2006 15:04:05 GMT"
)

func amzTime(t time.Time) string  { return t.UTC().Format(iso8601Full) }
func httpTime(t time.Time) string { return t.UTC().Format(rfc1123GMT) }

// quoteETag wraps an etag value in double quotes if not already quoted.
func quoteETag(e string) string {
	if e == "" {
		return e
	}
	if e[0] == '"' {
		return e
	}
	return `"` + e + `"`
}

// writeXML marshals v (adding the XML header) and writes it with the given
// status code.
func writeXML(w http.ResponseWriter, status int, v any) {
	body, err := xml.Marshal(v)
	if err != nil {
		http.Error(w, "xml marshal error", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/xml")
	w.Header().Set("Content-Length", strconv.Itoa(len(xml.Header)+len(body)))
	w.WriteHeader(status)
	_, _ = w.Write([]byte(xml.Header))
	_, _ = w.Write(body)
}

// writeSuccessNoContent sends 204.
func writeSuccessNoContent(w http.ResponseWriter) { w.WriteHeader(http.StatusNoContent) }

// writeSuccessOK sends 200 with no body.
func writeSuccessOK(w http.ResponseWriter) { w.WriteHeader(http.StatusOK) }

// setCommonHeaders adds headers every S3 response carries.
func setCommonHeaders(w http.ResponseWriter, region string) {
	w.Header().Set("Server", "gostore")
	w.Header().Set("Accept-Ranges", "bytes")
	if region != "" {
		w.Header().Set("x-amz-bucket-region", region)
	}
}
