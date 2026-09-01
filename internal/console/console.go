// Package console serves the embedded web console (a dependency-free SPA)
// that talks to the S3 + admin API on the same origin, signing every request
// with AWS SigV4 in the browser via Web Crypto.
package console

import (
	"embed"
	"io/fs"
	"net/http"
	"strings"
)

//go:embed assets
var assets embed.FS

// Handler serves the console. Mount it under "/gostore/console/".
func Handler() http.Handler {
	sub, _ := fs.Sub(assets, "assets")
	fileServer := http.FileServer(http.FS(sub))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p := strings.TrimPrefix(r.URL.Path, "/")
		if p == "" {
			p = "index.html"
		}
		if _, err := fs.Stat(sub, p); err != nil {
			// SPA fallback: unknown path -> index.html
			r = r.Clone(r.Context())
			r.URL.Path = "/"
		}
		w.Header().Set("Cache-Control", "no-cache")
		fileServer.ServeHTTP(w, r)
	})
}
