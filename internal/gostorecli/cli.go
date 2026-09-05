// Package gostorecli is the `gostore` command-line client — ls / cp / rm / mb
// / rb / cat / stat / admin — shipped in the same binary as the server. It
// speaks S3 with SigV4 header auth. Aliases (endpoint + credentials) live in
// ~/.gostore/aliases.json, like mc.
package gostorecli

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/lojadopocket/gostore/internal/auth"
)

// httpClient is shared by every CLI request. The stdlib default caps idle
// connections per host at 2, so a concurrent workload (bench, cp -r) pays a
// fresh TCP+TLS handshake on almost every call — murder on a high-latency
// link. Pool generously and keep connections warm.
var httpClient = &http.Client{
	Transport: &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		MaxIdleConns:          512,
		MaxIdleConnsPerHost:   512,
		MaxConnsPerHost:       0,
		IdleConnTimeout:       90 * time.Second,
		ForceAttemptHTTP2:     true,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: time.Second,
		WriteBufferSize:       64 << 10,
		ReadBufferSize:        64 << 10,
	},
}

func init() {
	if t, ok := httpClient.Transport.(*http.Transport); ok && t.MaxIdleConnsPerHost < runtime.GOMAXPROCS(0)*4 {
		t.MaxIdleConnsPerHost = runtime.GOMAXPROCS(0) * 4
	}
}

// Run executes one CLI subcommand and returns a process exit code.
func Run(args []string) int {
	if len(args) == 0 {
		usage()
		return 2
	}
	cmd, rest := args[0], args[1:]
	var err error
	switch cmd {
	case "alias":
		err = cmdAlias(rest)
	case "ls":
		err = cmdLs(rest)
	case "cp":
		err = cmdCp(rest)
	case "rm":
		err = cmdRm(rest)
	case "mb":
		err = cmdMb(rest)
	case "rb":
		err = cmdRb(rest)
	case "cat":
		err = cmdCat(rest)
	case "stat":
		err = cmdStat(rest)
	case "admin":
		err = cmdAdmin(rest)
	case "bench":
		err = cmdBench(rest)
	default:
		fmt.Fprintf(os.Stderr, "gostore: unknown command %q\n\n", cmd)
		usage()
		return 2
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "gostore: "+err.Error())
		return 1
	}
	return 0
}

func usage() {
	fmt.Fprint(os.Stderr, `gostore - S3 client (same binary as the server)

  gostore alias set   NAME URL ACCESS SECRET   save an endpoint
  gostore alias ls                             list aliases
  gostore alias rm    NAME

  gostore ls    NAME[/bucket[/prefix]]         [-r]
  gostore cp    SRC DST                        [-r]   (local <-> NAME/bucket/key)
  gostore rm    NAME/bucket/key                [-r]
  gostore mb    NAME/bucket
  gostore rb    NAME/bucket                    [--force]
  gostore cat   NAME/bucket/key
  gostore stat  NAME/bucket/key

  gostore admin info      NAME
  gostore admin heal      NAME
  gostore admin scrub     NAME [status]
  gostore admin readonly  NAME on|off
  gostore admin snapshot  NAME BUCKET [restore ID | list]

  gostore bench  NAME/bucket [--duration 30s] [--size 1MiB]
                             [--concurrency 20] [--mix put,get,delete]
`)
}

// --- aliases ---------------------------------------------------------------

type alias struct {
	URL    string `json:"url"`
	Access string `json:"accessKey"`
	Secret string `json:"secretKey"`
	Region string `json:"region,omitempty"`
}

func aliasPath() string {
	if p := os.Getenv("GOSTORE_ALIAS_FILE"); p != "" {
		return p
	}
	home, _ := os.UserHomeDir()
	return filepath.Join(home, ".gostore", "aliases.json")
}

func loadAliases() (map[string]alias, error) {
	m := map[string]alias{}
	b, err := os.ReadFile(aliasPath())
	if err != nil {
		if os.IsNotExist(err) {
			return m, nil
		}
		return nil, err
	}
	return m, json.Unmarshal(b, &m)
}

func saveAliases(m map[string]alias) error {
	p := aliasPath()
	if err := os.MkdirAll(filepath.Dir(p), 0o700); err != nil {
		return err
	}
	b, _ := json.MarshalIndent(m, "", "  ")
	return os.WriteFile(p, b, 0o600)
}

func cmdAlias(a []string) error {
	if len(a) == 0 {
		a = []string{"ls"}
	}
	m, err := loadAliases()
	if err != nil {
		return err
	}
	switch a[0] {
	case "set":
		if len(a) != 5 {
			return fmt.Errorf("usage: gostore alias set NAME URL ACCESS SECRET")
		}
		m[a[1]] = alias{URL: strings.TrimRight(a[2], "/"), Access: a[3], Secret: a[4], Region: "us-east-1"}
		return saveAliases(m)
	case "rm":
		if len(a) != 2 {
			return fmt.Errorf("usage: gostore alias rm NAME")
		}
		delete(m, a[1])
		return saveAliases(m)
	case "ls":
		names := make([]string, 0, len(m))
		for n := range m {
			names = append(names, n)
		}
		sort.Strings(names)
		for _, n := range names {
			fmt.Printf("%-12s %s  (%s)\n", n, m[n].URL, m[n].Access)
		}
		return nil
	}
	return fmt.Errorf("unknown alias subcommand %q", a[0])
}

// --- target parsing ------------------------------------------------------

type target struct {
	al          alias
	bucket, key string
	isRemote    bool
	localPath   string
}

func parseTarget(spec string) (target, error) {
	// local path if it exists or looks like a path
	if spec == "-" || strings.HasPrefix(spec, "./") || strings.HasPrefix(spec, "/") || strings.HasPrefix(spec, "..") {
		return target{localPath: spec}, nil
	}
	name, rest, _ := strings.Cut(spec, "/")
	m, err := loadAliases()
	if err != nil {
		return target{}, err
	}
	al, ok := m[name]
	if !ok {
		// not an alias -> treat as a local path
		return target{localPath: spec}, nil
	}
	t := target{al: al, isRemote: true}
	if rest != "" {
		t.bucket, t.key, _ = strings.Cut(rest, "/")
	}
	return t, nil
}

// --- signed HTTP -------------------------------------------------------------

func (t target) do(method, objPath string, query url.Values, body []byte, hdr map[string]string) (*http.Response, error) {
	u := t.al.URL + objPath
	if len(query) > 0 {
		u += "?" + query.Encode()
	}
	var rd io.Reader
	if body != nil {
		rd = bytes.NewReader(body)
	}
	req, err := http.NewRequest(method, u, rd)
	if err != nil {
		return nil, err
	}
	if body != nil {
		req.ContentLength = int64(len(body))
	}
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	region := t.al.Region
	if region == "" {
		region = "us-east-1"
	}
	signV4(req, body, t.al.Access, t.al.Secret, region)
	return httpClient.Do(req)
}

func signV4(req *http.Request, payload []byte, ak, sk, region string) {
	now := time.Now().UTC()
	amzDate := now.Format("20060102T150405Z")
	dateStamp := now.Format("20060102")
	ph := sha256Hex(payload)
	req.Header.Set("X-Amz-Date", amzDate)
	req.Header.Set("X-Amz-Content-Sha256", ph)
	host := req.URL.Host
	signed := "host;x-amz-content-sha256;x-amz-date"
	canonHeaders := "host:" + host + "\n" +
		"x-amz-content-sha256:" + ph + "\n" +
		"x-amz-date:" + amzDate + "\n"
	canonReq := req.Method + "\n" + auth.EncodePath(req.URL.Path) + "\n" + auth.CanonicalQuery(req.URL.Query()) + "\n" +
		canonHeaders + "\n" + signed + "\n" + ph
	scope := dateStamp + "/" + region + "/s3/aws4_request"
	sts := "AWS4-HMAC-SHA256\n" + amzDate + "\n" + scope + "\n" + sha256Hex([]byte(canonReq))
	key := hmac256([]byte("AWS4"+sk), dateStamp)
	key = hmac256(key, region)
	key = hmac256(key, "s3")
	key = hmac256(key, "aws4_request")
	sig := hex.EncodeToString(hmac256(key, sts))
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential="+ak+"/"+scope+
		", SignedHeaders="+signed+", Signature="+sig)
}

func hmac256(k []byte, d string) []byte {
	h := hmac.New(sha256.New, k)
	h.Write([]byte(d))
	return h.Sum(nil)
}
func sha256Hex(b []byte) string { s := sha256.Sum256(b); return hex.EncodeToString(s[:]) }

func objPath(bucket, key string) string {
	if key == "" {
		return "/" + bucket
	}
	return "/" + bucket + "/" + key
}

func checkOK(resp *http.Response) error {
	if resp.StatusCode/100 == 2 {
		return nil
	}
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	resp.Body.Close()
	msg := strings.TrimSpace(string(b))
	if i := strings.Index(msg, "<Message>"); i >= 0 {
		if j := strings.Index(msg, "</Message>"); j > i {
			msg = msg[i+9 : j]
		}
	}
	return fmt.Errorf("%s: %s", resp.Status, msg)
}

// --- commands -----------------------------------------------------------

type lsBucketsXML struct {
	Buckets struct {
		Bucket []struct {
			Name         string `xml:"Name"`
			CreationDate string `xml:"CreationDate"`
		} `xml:"Bucket"`
	} `xml:"Buckets"`
}

type lsV2XML struct {
	Contents []struct {
		Key          string `xml:"Key"`
		Size         int64  `xml:"Size"`
		LastModified string `xml:"LastModified"`
	} `xml:"Contents"`
	CommonPrefixes []struct {
		Prefix string `xml:"Prefix"`
	} `xml:"CommonPrefixes"`
	IsTruncated           bool   `xml:"IsTruncated"`
	NextContinuationToken string `xml:"NextContinuationToken"`
}

func cmdLs(a []string) error {
	rec := popFlag(&a, "-r")
	if len(a) != 1 {
		return fmt.Errorf("usage: gostore ls NAME[/bucket[/prefix]] [-r]")
	}
	t, err := parseTarget(a[0])
	if err != nil {
		return err
	}
	if !t.isRemote {
		return fmt.Errorf("ls needs a remote target (an alias)")
	}
	if t.bucket == "" {
		resp, err := t.do("GET", "/", nil, nil, nil)
		if err != nil {
			return err
		}
		if err := checkOK(resp); err != nil {
			return err
		}
		var x lsBucketsXML
		dec(resp, &x)
		for _, b := range x.Buckets.Bucket {
			fmt.Printf("%s  %s/\n", b.CreationDate[:10], b.Name)
		}
		return nil
	}
	prefix := t.key
	token := ""
	for {
		q := url.Values{"list-type": {"2"}, "prefix": {prefix}, "max-keys": {"1000"}}
		if !rec {
			q.Set("delimiter", "/")
		}
		if token != "" {
			q.Set("continuation-token", token)
		}
		resp, err := t.do("GET", "/"+t.bucket, q, nil, nil)
		if err != nil {
			return err
		}
		if err := checkOK(resp); err != nil {
			return err
		}
		var x lsV2XML
		dec(resp, &x)
		for _, p := range x.CommonPrefixes {
			fmt.Printf("%25s  %s\n", "", p.Prefix)
		}
		for _, o := range x.Contents {
			fmt.Printf("%s  %12d  %s\n", o.LastModified[:19], o.Size, o.Key)
		}
		if !x.IsTruncated {
			return nil
		}
		token = x.NextContinuationToken
	}
}

func cmdCat(a []string) error {
	if len(a) != 1 {
		return fmt.Errorf("usage: gostore cat NAME/bucket/key")
	}
	t, err := parseTarget(a[0])
	if err != nil {
		return err
	}
	resp, err := t.do("GET", objPath(t.bucket, t.key), nil, nil, nil)
	if err != nil {
		return err
	}
	if err := checkOK(resp); err != nil {
		return err
	}
	defer resp.Body.Close()
	_, err = io.Copy(os.Stdout, resp.Body)
	return err
}

func cmdStat(a []string) error {
	if len(a) != 1 {
		return fmt.Errorf("usage: gostore stat NAME/bucket/key")
	}
	t, err := parseTarget(a[0])
	if err != nil {
		return err
	}
	resp, err := t.do("HEAD", objPath(t.bucket, t.key), nil, nil, nil)
	if err != nil {
		return err
	}
	if err := checkOK(resp); err != nil {
		return err
	}
	resp.Body.Close()
	fmt.Printf("Key      : %s/%s\n", t.bucket, t.key)
	fmt.Printf("Size     : %s\n", resp.Header.Get("Content-Length"))
	fmt.Printf("ETag     : %s\n", strings.Trim(resp.Header.Get("ETag"), `"`))
	fmt.Printf("Modified : %s\n", resp.Header.Get("Last-Modified"))
	fmt.Printf("Type     : %s\n", resp.Header.Get("Content-Type"))
	if v := resp.Header.Get("x-amz-version-id"); v != "" {
		fmt.Printf("Version  : %s\n", v)
	}
	return nil
}

func cmdMb(a []string) error {
	if len(a) != 1 {
		return fmt.Errorf("usage: gostore mb NAME/bucket")
	}
	t, err := parseTarget(a[0])
	if err != nil {
		return err
	}
	resp, err := t.do("PUT", "/"+t.bucket, nil, nil, nil)
	if err != nil {
		return err
	}
	return checkOK(resp)
}

func cmdRb(a []string) error {
	force := popFlag(&a, "--force")
	if len(a) != 1 {
		return fmt.Errorf("usage: gostore rb NAME/bucket [--force]")
	}
	t, err := parseTarget(a[0])
	if err != nil {
		return err
	}
	hdr := map[string]string{}
	if force {
		hdr["x-amz-force-delete"] = "true"
	}
	resp, err := t.do("DELETE", "/"+t.bucket, nil, nil, hdr)
	if err != nil {
		return err
	}
	return checkOK(resp)
}

func cmdRm(a []string) error {
	rec := popFlag(&a, "-r")
	if len(a) != 1 {
		return fmt.Errorf("usage: gostore rm NAME/bucket/key [-r]")
	}
	t, err := parseTarget(a[0])
	if err != nil {
		return err
	}
	if !rec {
		resp, err := t.do("DELETE", objPath(t.bucket, t.key), nil, nil, nil)
		if err != nil {
			return err
		}
		return checkOK(resp)
	}
	// recursive: list then delete
	token := ""
	for {
		q := url.Values{"list-type": {"2"}, "prefix": {t.key}, "max-keys": {"1000"}}
		if token != "" {
			q.Set("continuation-token", token)
		}
		resp, err := t.do("GET", "/"+t.bucket, q, nil, nil)
		if err != nil {
			return err
		}
		if err := checkOK(resp); err != nil {
			return err
		}
		var x lsV2XML
		dec(resp, &x)
		for _, o := range x.Contents {
			r, e := t.do("DELETE", objPath(t.bucket, o.Key), nil, nil, nil)
			if e != nil {
				return e
			}
			r.Body.Close()
			fmt.Println("removed " + o.Key)
		}
		if !x.IsTruncated {
			return nil
		}
		token = x.NextContinuationToken
	}
}

func cmdCp(a []string) error {
	rec := popFlag(&a, "-r")
	if len(a) != 2 {
		return fmt.Errorf("usage: gostore cp SRC DST [-r]")
	}
	src, err := parseTarget(a[0])
	if err != nil {
		return err
	}
	dst, err := parseTarget(a[1])
	if err != nil {
		return err
	}
	switch {
	case !src.isRemote && dst.isRemote:
		if rec {
			return cpUpDir(src.localPath, dst)
		}
		return cpUp(src.localPath, dst)
	case src.isRemote && !dst.isRemote:
		return cpDown(src, dst.localPath)
	default:
		return fmt.Errorf("cp supports local<->remote only (one side must be an alias, the other a path)")
	}
}

func cpUp(localPath string, dst target) error {
	b, err := os.ReadFile(localPath)
	if err != nil {
		return err
	}
	key := dst.key
	if key == "" || strings.HasSuffix(key, "/") {
		key += filepath.Base(localPath)
	}
	resp, err := dst.do("PUT", objPath(dst.bucket, key), nil, b, nil)
	if err != nil {
		return err
	}
	if err := checkOK(resp); err != nil {
		return err
	}
	fmt.Printf("uploaded %s -> %s/%s (%d bytes)\n", localPath, dst.bucket, key, len(b))
	return nil
}

func cpUpDir(dir string, dst target) error {
	return filepath.Walk(dir, func(p string, fi os.FileInfo, err error) error {
		if err != nil || fi.IsDir() {
			return err
		}
		rel, _ := filepath.Rel(dir, p)
		d := dst
		d.key = path.Join(strings.TrimSuffix(dst.key, "/"), filepath.ToSlash(rel))
		return cpUp(p, d)
	})
}

func cpDown(src target, localPath string) error {
	resp, err := src.do("GET", objPath(src.bucket, src.key), nil, nil, nil)
	if err != nil {
		return err
	}
	if err := checkOK(resp); err != nil {
		return err
	}
	defer resp.Body.Close()
	out := localPath
	if fi, e := os.Stat(localPath); e == nil && fi.IsDir() {
		out = filepath.Join(localPath, path.Base(src.key))
	}
	f, err := os.Create(out)
	if err != nil {
		return err
	}
	defer f.Close()
	n, err := io.Copy(f, resp.Body)
	if err == nil {
		fmt.Printf("downloaded %s/%s -> %s (%d bytes)\n", src.bucket, src.key, out, n)
	}
	return err
}

func cmdAdmin(a []string) error {
	if len(a) < 2 {
		return fmt.Errorf("usage: gostore admin <info|heal|scrub|readonly|snapshot> NAME ...")
	}
	sub, name, rest := a[0], a[1], a[2:]
	t, err := parseTarget(name + "/")
	if err != nil {
		return err
	}
	call := func(method, p string, q url.Values) error {
		resp, err := t.do(method, p, q, nil, nil)
		if err != nil {
			return err
		}
		if err := checkOK(resp); err != nil {
			return err
		}
		defer resp.Body.Close()
		var pretty bytes.Buffer
		raw, _ := io.ReadAll(resp.Body)
		if json.Indent(&pretty, raw, "", "  ") == nil {
			fmt.Println(pretty.String())
		} else {
			fmt.Println(string(raw))
		}
		return nil
	}
	switch sub {
	case "info":
		return call("GET", "/gostore/admin/v1/info", nil)
	case "heal":
		return call("POST", "/gostore/admin/v1/heal", nil)
	case "scrub":
		if len(rest) > 0 && rest[0] == "status" {
			return call("GET", "/gostore/admin/v1/scrub", nil)
		}
		return call("POST", "/gostore/admin/v1/scrub", nil)
	case "readonly":
		on := len(rest) > 0 && (rest[0] == "on" || rest[0] == "true")
		b, _ := json.Marshal(map[string]bool{"enabled": on})
		resp, err := t.do("POST", "/gostore/admin/v1/readonly", nil, b, map[string]string{"Content-Type": "application/json"})
		if err != nil {
			return err
		}
		return checkOK(resp)
	case "snapshot":
		if len(rest) < 1 {
			return fmt.Errorf("usage: gostore admin snapshot NAME BUCKET [list | restore ID]")
		}
		bucket := rest[0]
		switch {
		case len(rest) >= 2 && rest[1] == "list":
			return call("GET", "/gostore/admin/v1/snapshots", url.Values{"bucket": {bucket}})
		case len(rest) >= 3 && rest[1] == "restore":
			return call("POST", "/gostore/admin/v1/snapshot/restore", url.Values{"bucket": {bucket}, "id": {rest[2]}})
		default:
			return call("POST", "/gostore/admin/v1/snapshot", url.Values{"bucket": {bucket}})
		}
	}
	return fmt.Errorf("unknown admin subcommand %q", sub)
}

// --- small helpers -----------------------------------------------------

func dec(resp *http.Response, v any) {
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	_ = xml.Unmarshal(b, v)
}

func popFlag(a *[]string, flag string) bool {
	out := (*a)[:0]
	found := false
	for _, x := range *a {
		if x == flag {
			found = true
			continue
		}
		out = append(out, x)
	}
	*a = out
	return found
}
