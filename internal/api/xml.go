package api

import "encoding/xml"

const s3XMLNS = "http://s3.amazonaws.com/doc/2006-03-01/"

// --- service ------------------------------------------------------------

type listAllMyBucketsResult struct {
	XMLName xml.Name `xml:"ListAllMyBucketsResult"`
	XMLNS   string   `xml:"xmlns,attr"`
	Owner   canonicalOwner
	Buckets struct {
		Bucket []bucketXML `xml:"Bucket"`
	} `xml:"Buckets"`
}

type bucketXML struct {
	Name         string `xml:"Name"`
	CreationDate string `xml:"CreationDate"`
}

type canonicalOwner struct {
	ID          string `xml:"ID"`
	DisplayName string `xml:"DisplayName,omitempty"`
}

// --- location ---------------------------------------------------------

type locationConstraint struct {
	XMLName  xml.Name `xml:"LocationConstraint"`
	XMLNS    string   `xml:"xmlns,attr"`
	Location string   `xml:",chardata"`
}

// --- ListObjects (v1) --------------------------------------------

type listBucketResult struct {
	XMLName        xml.Name `xml:"ListBucketResult"`
	XMLNS          string   `xml:"xmlns,attr"`
	Name           string   `xml:"Name"`
	Prefix         string   `xml:"Prefix"`
	Marker         string   `xml:"Marker"`
	NextMarker     string   `xml:"NextMarker,omitempty"`
	MaxKeys        int      `xml:"MaxKeys"`
	Delimiter      string   `xml:"Delimiter,omitempty"`
	IsTruncated    bool     `xml:"IsTruncated"`
	Contents       []objectXML
	CommonPrefixes []commonPrefixXML
}

// --- ListObjectsV2 --------------------------------------------

type listBucketV2Result struct {
	XMLName               xml.Name `xml:"ListBucketResult"`
	XMLNS                 string   `xml:"xmlns,attr"`
	Name                  string   `xml:"Name"`
	Prefix                string   `xml:"Prefix"`
	StartAfter            string   `xml:"StartAfter,omitempty"`
	ContinuationToken     string   `xml:"ContinuationToken,omitempty"`
	NextContinuationToken string   `xml:"NextContinuationToken,omitempty"`
	KeyCount              int      `xml:"KeyCount"`
	MaxKeys               int      `xml:"MaxKeys"`
	Delimiter             string   `xml:"Delimiter,omitempty"`
	IsTruncated           bool     `xml:"IsTruncated"`
	Contents              []objectXML
	CommonPrefixes        []commonPrefixXML
}

type objectXML struct {
	XMLName      xml.Name        `xml:"Contents"`
	Key          string          `xml:"Key"`
	LastModified string          `xml:"LastModified"`
	ETag         string          `xml:"ETag"`
	Size         int64           `xml:"Size"`
	StorageClass string          `xml:"StorageClass"`
	Owner        *canonicalOwner `xml:"Owner,omitempty"`
}

type commonPrefixXML struct {
	XMLName xml.Name `xml:"CommonPrefixes"`
	Prefix  string   `xml:"Prefix"`
}

// --- multipart -------------------------------------------------

type initiateMultipartUploadResult struct {
	XMLName  xml.Name `xml:"InitiateMultipartUploadResult"`
	XMLNS    string   `xml:"xmlns,attr"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	UploadID string   `xml:"UploadId"`
}

type completeMultipartUpload struct {
	XMLName xml.Name          `xml:"CompleteMultipartUpload"`
	Parts   []completePartXML `xml:"Part"`
}

type completePartXML struct {
	PartNumber int    `xml:"PartNumber"`
	ETag       string `xml:"ETag"`
}

type completeMultipartUploadResult struct {
	XMLName  xml.Name `xml:"CompleteMultipartUploadResult"`
	XMLNS    string   `xml:"xmlns,attr"`
	Location string   `xml:"Location"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	ETag     string   `xml:"ETag"`
}

type listPartsResult struct {
	XMLName              xml.Name `xml:"ListPartsResult"`
	XMLNS                string   `xml:"xmlns,attr"`
	Bucket               string   `xml:"Bucket"`
	Key                  string   `xml:"Key"`
	UploadID             string   `xml:"UploadId"`
	StorageClass         string   `xml:"StorageClass"`
	PartNumberMarker     int      `xml:"PartNumberMarker"`
	NextPartNumberMarker int      `xml:"NextPartNumberMarker"`
	MaxParts             int      `xml:"MaxParts"`
	IsTruncated          bool     `xml:"IsTruncated"`
	Parts                []partXML
	Initiator            canonicalOwner `xml:"Initiator"`
	Owner                canonicalOwner `xml:"Owner"`
}

type partXML struct {
	XMLName      xml.Name `xml:"Part"`
	PartNumber   int      `xml:"PartNumber"`
	LastModified string   `xml:"LastModified"`
	ETag         string   `xml:"ETag"`
	Size         int64    `xml:"Size"`
}

type listMultipartUploadsResult struct {
	XMLName            xml.Name `xml:"ListMultipartUploadsResult"`
	XMLNS              string   `xml:"xmlns,attr"`
	Bucket             string   `xml:"Bucket"`
	KeyMarker          string   `xml:"KeyMarker"`
	UploadIDMarker     string   `xml:"UploadIdMarker"`
	NextKeyMarker      string   `xml:"NextKeyMarker"`
	NextUploadIDMarker string   `xml:"NextUploadIdMarker"`
	MaxUploads         int      `xml:"MaxUploads"`
	IsTruncated        bool     `xml:"IsTruncated"`
	Uploads            []uploadXML
}

type uploadXML struct {
	XMLName      xml.Name `xml:"Upload"`
	Key          string   `xml:"Key"`
	UploadID     string   `xml:"UploadId"`
	Initiated    string   `xml:"Initiated"`
	StorageClass string   `xml:"StorageClass"`
}

// --- DeleteObjects -------------------------------------------

type deleteRequest struct {
	XMLName xml.Name          `xml:"Delete"`
	Quiet   bool              `xml:"Quiet"`
	Objects []deleteObjectXML `xml:"Object"`
}

type deleteObjectXML struct {
	Key       string `xml:"Key"`
	VersionID string `xml:"VersionId"`
}

type deleteResult struct {
	XMLName xml.Name `xml:"DeleteResult"`
	XMLNS   string   `xml:"xmlns,attr"`
	Deleted []deletedXML
	Errors  []deleteErrorXML
}

type deletedXML struct {
	XMLName xml.Name `xml:"Deleted"`
	Key     string   `xml:"Key"`
}

type deleteErrorXML struct {
	XMLName xml.Name `xml:"Error"`
	Key     string   `xml:"Key"`
	Code    string   `xml:"Code"`
	Message string   `xml:"Message"`
}

// --- CopyObject ---------------------------------------------

type copyObjectResult struct {
	XMLName      xml.Name `xml:"CopyObjectResult"`
	XMLNS        string   `xml:"xmlns,attr"`
	LastModified string   `xml:"LastModified"`
	ETag         string   `xml:"ETag"`
}

type copyPartResult struct {
	XMLName      xml.Name `xml:"CopyPartResult"`
	XMLNS        string   `xml:"xmlns,attr"`
	LastModified string   `xml:"LastModified"`
	ETag         string   `xml:"ETag"`
}
