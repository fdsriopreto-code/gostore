package policy

// Builtin returns the canned policy documents, matching MinIO's names so
// existing tooling/muscle-memory carries over.
func Builtin() map[string]*Policy {
	return map[string]*Policy{
		"readwrite":     mustParse(readWriteJSON),
		"readonly":      mustParse(readOnlyJSON),
		"writeonly":     mustParse(writeOnlyJSON),
		"diagnostics":   mustParse(diagnosticsJSON),
		"consoleAdmin":  mustParse(consoleAdminJSON),
	}
}

// IsBuiltin reports whether name is a canned policy.
func IsBuiltin(name string) bool {
	_, ok := Builtin()[name]
	return ok
}

func mustParse(s string) *Policy {
	p, err := Parse([]byte(s))
	if err != nil {
		panic("policy: bad builtin: " + err.Error())
	}
	return p
}

const readWriteJSON = `{
  "Version": "2012-10-17",
  "Statement": [{ "Effect": "Allow", "Action": ["s3:*"], "Resource": ["arn:aws:s3:::*"] }]
}`

const readOnlyJSON = `{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Action": ["s3:GetObject","s3:GetObjectVersion","s3:ListBucket","s3:ListAllMyBuckets","s3:GetBucketLocation","s3:ListBucketMultipartUploads","s3:ListMultipartUploadParts"],
    "Resource": ["arn:aws:s3:::*"]
  }]
}`

const writeOnlyJSON = `{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Action": ["s3:PutObject","s3:AbortMultipartUpload","s3:ListMultipartUploadParts","s3:ListBucketMultipartUploads","s3:GetBucketLocation"],
    "Resource": ["arn:aws:s3:::*"]
  }]
}`

const diagnosticsJSON = `{
  "Version": "2012-10-17",
  "Statement": [{ "Effect": "Allow", "Action": ["admin:ServerInfo","admin:StorageInfo","admin:HealthInfo"], "Resource": ["arn:aws:s3:::*"] }]
}`

const consoleAdminJSON = `{
  "Version": "2012-10-17",
  "Statement": [
    { "Effect": "Allow", "Action": ["s3:*"], "Resource": ["arn:aws:s3:::*"] },
    { "Effect": "Allow", "Action": ["admin:*"], "Resource": ["arn:aws:s3:::*"] }
  ]
}`
