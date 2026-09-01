package api

import (
	"net/http"
)

func (s *Server) handleListBuckets(w http.ResponseWriter, r *http.Request) {
	buckets, err := s.obj.ListBuckets(r.Context())
	if err != nil {
		writeErrorResponse(w, r, toAPIError(err), "/")
		return
	}
	res := listAllMyBucketsResult{XMLNS: s3XMLNS}
	res.Owner = canonicalOwner{ID: "gostore", DisplayName: "gostore"}
	for _, b := range buckets {
		res.Buckets.Bucket = append(res.Buckets.Bucket, bucketXML{
			Name:         b.Name,
			CreationDate: amzTime(b.Created),
		})
	}
	writeXML(w, http.StatusOK, res)
}
