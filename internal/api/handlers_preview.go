package api

import (
	"bytes"
	"image"
	"image/color"
	"image/draw"
	_ "image/gif"
	"image/jpeg"
	_ "image/png"
	"io"
	"net/http"
	"strconv"
)

// previewMaxDecodeBytes caps how much of an object the preview endpoint will
// pull in to decode, so a huge upload can't blow memory.
const previewMaxDecodeBytes = 40 << 20

// handleObjectPreview serves GET /{bucket}/{key}?preview[=N] — a small JPEG
// thumbnail of an image object (jpeg/png/gif), longest side N px (default
// 480, max 1024). Non-images get 415. Pure stdlib, box-average downscale.
func (s *Server) handleObjectPreview(w http.ResponseWriter, r *http.Request, bucket, key string) {
	res := "/" + bucket + "/" + key
	maxDim := 480
	if v := r.URL.Query().Get("preview"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 16 {
			maxDim = n
		}
	}
	if maxDim > 1024 {
		maxDim = 1024
	}

	gr, err := s.obj.GetObjectNInfo(r.Context(), bucket, key, nil, r.Header, s.vopts(bucket, r))
	if err != nil {
		writeErrorResponse(w, r, toAPIError(err), res)
		return
	}
	defer gr.Close()

	raw, err := io.ReadAll(io.LimitReader(gr, previewMaxDecodeBytes))
	if err != nil {
		writeErrorResponse(w, r, ErrInternalError, res)
		return
	}
	src, _, err := image.Decode(bytes.NewReader(raw))
	if err != nil {
		writeErrorResponse(w, r, ErrNotImplemented, res) // "not an image we can preview"
		return
	}

	thumb := downscale(src, maxDim)
	var buf bytes.Buffer
	if err := jpeg.Encode(&buf, thumb, &jpeg.Options{Quality: 80}); err != nil {
		writeErrorResponse(w, r, ErrInternalError, res)
		return
	}
	w.Header().Set("Content-Type", "image/jpeg")
	w.Header().Set("Cache-Control", "private, max-age=300")
	w.Header().Set("Content-Length", strconv.Itoa(buf.Len()))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(buf.Bytes())
}

// downscale returns src shrunk so its longest side is <= maxDim, using a
// simple box average. If src already fits it is returned as an *image.RGBA
// copy (jpeg.Encode is happy with any image.Image, but this keeps it simple).
func downscale(src image.Image, maxDim int) image.Image {
	b := src.Bounds()
	sw, sh := b.Dx(), b.Dy()
	if sw <= 0 || sh <= 0 {
		return src
	}
	longest := sw
	if sh > sw {
		longest = sh
	}
	scale := 1.0
	if longest > maxDim {
		scale = float64(maxDim) / float64(longest)
	}
	dw := int(float64(sw)*scale + 0.5)
	dh := int(float64(sh)*scale + 0.5)
	if dw < 1 {
		dw = 1
	}
	if dh < 1 {
		dh = 1
	}
	if dw == sw && dh == sh {
		dst := image.NewRGBA(image.Rect(0, 0, sw, sh))
		draw.Draw(dst, dst.Bounds(), src, b.Min, draw.Src)
		return dst
	}

	dst := image.NewRGBA(image.Rect(0, 0, dw, dh))
	xr := float64(sw) / float64(dw)
	yr := float64(sh) / float64(dh)
	for dy := 0; dy < dh; dy++ {
		sy0 := int(float64(dy) * yr)
		sy1 := int(float64(dy+1) * yr)
		if sy1 <= sy0 {
			sy1 = sy0 + 1
		}
		for dx := 0; dx < dw; dx++ {
			sx0 := int(float64(dx) * xr)
			sx1 := int(float64(dx+1) * xr)
			if sx1 <= sx0 {
				sx1 = sx0 + 1
			}
			var rr, gg, bb, aa, n uint64
			for sy := sy0; sy < sy1; sy++ {
				for sx := sx0; sx < sx1; sx++ {
					cr, cg, cb, ca := src.At(b.Min.X+sx, b.Min.Y+sy).RGBA()
					rr += uint64(cr)
					gg += uint64(cg)
					bb += uint64(cb)
					aa += uint64(ca)
					n++
				}
			}
			if n == 0 {
				n = 1
			}
			dst.SetRGBA(dx, dy, color.RGBA{
				R: uint8(rr / n >> 8),
				G: uint8(gg / n >> 8),
				B: uint8(bb / n >> 8),
				A: uint8(aa / n >> 8),
			})
		}
	}
	return dst
}
