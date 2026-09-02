package api_test

import (
	"bytes"
	"image"
	"image/color"
	"image/jpeg"
	"image/png"
	"net/http"
	"testing"
)

func TestImagePipelineResizeCropFormat(t *testing.T) {
	srv := newTestServer(t)
	defer srv.Close()
	do(t, srv, http.MethodPut, "/pics", nil, nil)

	img := image.NewRGBA(image.Rect(0, 0, 400, 300))
	for y := 0; y < 300; y++ {
		for x := 0; x < 400; x++ {
			img.Set(x, y, color.RGBA{uint8(x), uint8(y), 200, 255})
		}
	}
	var buf bytes.Buffer
	_ = png.Encode(&buf, img)
	do(t, srv, http.MethodPut, "/pics/src.png", buf.Bytes(), map[string]string{"Content-Type": "image/png"})

	// fit=cover to an exact 100x100 square, PNG output.
	r := do(t, srv, http.MethodGet, "/pics/src.png?w=100&h=100&fit=cover&format=png", nil, nil)
	if r.StatusCode != 200 || r.Header.Get("Content-Type") != "image/png" {
		t.Fatalf("cover/png: %d %s", r.StatusCode, r.Header.Get("Content-Type"))
	}
	b, _ := readAll(r)
	out, err := png.Decode(bytes.NewReader(b))
	if err != nil {
		t.Fatalf("decode png: %v", err)
	}
	if out.Bounds().Dx() != 100 || out.Bounds().Dy() != 100 {
		t.Fatalf("cover crop = %dx%d, want 100x100", out.Bounds().Dx(), out.Bounds().Dy())
	}

	// contain to width 200 → 200x150, JPEG (default).
	r2 := do(t, srv, http.MethodGet, "/pics/src.png?w=200", nil, nil)
	if r2.Header.Get("Content-Type") != "image/jpeg" {
		t.Fatalf("default format should be jpeg, got %s", r2.Header.Get("Content-Type"))
	}
	b2, _ := readAll(r2)
	out2, err := jpeg.Decode(bytes.NewReader(b2))
	if err != nil {
		t.Fatalf("decode jpeg: %v", err)
	}
	if out2.Bounds().Dx() != 200 || out2.Bounds().Dy() != 150 {
		t.Fatalf("contain w=200 => %dx%d, want 200x150", out2.Bounds().Dx(), out2.Bounds().Dy())
	}

	// The transform is cached: second identical request is a hot-cache HIT.
	r3 := do(t, srv, http.MethodGet, "/pics/src.png?w=200", nil, nil)
	if r3.Header.Get("x-gostore-cache") != "HIT" {
		t.Fatal("repeated identical transform should be served from cache")
	}
	readAll(r3)
}

func readAll(r *http.Response) ([]byte, error) {
	defer r.Body.Close()
	var b bytes.Buffer
	_, err := b.ReadFrom(r.Body)
	return b.Bytes(), err
}
