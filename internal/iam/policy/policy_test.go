package policy

import "testing"

func TestWildcardMatch(t *testing.T) {
	cases := []struct {
		pat, s string
		want   bool
	}{
		{"*", "anything", true},
		{"s3:Get*", "s3:GetObject", true},
		{"s3:Get*", "s3:PutObject", false},
		{"photos/*", "photos/a/b.jpg", true},
		{"photos/*", "videos/a.mp4", false},
		{"a?c", "abc", true},
		{"a?c", "ac", false},
	}
	for _, c := range cases {
		if got := wildcardMatch(c.pat, c.s); got != c.want {
			t.Errorf("wildcardMatch(%q,%q)=%v want %v", c.pat, c.s, got, c.want)
		}
	}
}

func TestReadOnlyPolicy(t *testing.T) {
	p := Builtin()["readonly"]
	if !p.IsAllowed(Args{Action: "s3:GetObject", BucketName: "b", ObjectName: "k"}) {
		t.Fatal("readonly should allow GetObject")
	}
	if p.IsAllowed(Args{Action: "s3:PutObject", BucketName: "b", ObjectName: "k"}) {
		t.Fatal("readonly must deny PutObject")
	}
	if !p.IsAllowed(Args{Action: "s3:ListBucket", BucketName: "b"}) {
		t.Fatal("readonly should allow ListBucket")
	}
}

func TestExplicitDenyWins(t *testing.T) {
	doc := `{
	  "Version":"2012-10-17",
	  "Statement":[
	    {"Effect":"Allow","Action":["s3:*"],"Resource":["arn:aws:s3:::*"]},
	    {"Effect":"Deny","Action":["s3:DeleteObject"],"Resource":["arn:aws:s3:::locked/*"]}
	  ]
	}`
	p, err := Parse([]byte(doc))
	if err != nil {
		t.Fatal(err)
	}
	if !p.IsAllowed(Args{Action: "s3:DeleteObject", BucketName: "other", ObjectName: "k"}) {
		t.Fatal("delete on non-locked bucket should be allowed")
	}
	if p.IsAllowed(Args{Action: "s3:DeleteObject", BucketName: "locked", ObjectName: "k"}) {
		t.Fatal("explicit deny on locked/* must win")
	}
}

func TestResourceScopedPolicy(t *testing.T) {
	doc := `{"Version":"2012-10-17","Statement":[
	  {"Effect":"Allow","Action":["s3:GetObject","s3:PutObject"],"Resource":["arn:aws:s3:::team-*/*"]},
	  {"Effect":"Allow","Action":["s3:ListBucket"],"Resource":["arn:aws:s3:::team-*"]}
	]}`
	p, _ := Parse([]byte(doc))
	if !p.IsAllowed(Args{Action: "s3:GetObject", BucketName: "team-a", ObjectName: "f"}) {
		t.Fatal("should allow team-a object")
	}
	if p.IsAllowed(Args{Action: "s3:GetObject", BucketName: "other", ObjectName: "f"}) {
		t.Fatal("must deny other bucket")
	}
	if !p.IsAllowed(Args{Action: "s3:ListBucket", BucketName: "team-b"}) {
		t.Fatal("should allow list on team-b")
	}
}
