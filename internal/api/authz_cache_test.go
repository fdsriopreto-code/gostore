package api

import "testing"

const testPolicyDoc = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":["s3:GetObject"],"Resource":["arn:aws:s3:::pub/*"]}]}`

func TestBucketPolicyCacheMemoisesAndRefreshes(t *testing.T) {
	c := newBucketPolicyCache()

	p1, ok := c.get("pub", []byte(testPolicyDoc))
	if !ok || p1 == nil {
		t.Fatal("first get should parse and return a policy")
	}
	p2, ok := c.get("pub", []byte(testPolicyDoc))
	if !ok || p2 != p1 {
		t.Fatal("second get with identical doc should return the cached *Policy pointer")
	}

	// A changed document must be recompiled (different pointer).
	changed := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":["s3:GetObject","s3:ListBucket"],"Resource":["arn:aws:s3:::pub/*"]}]}`
	p3, ok := c.get("pub", []byte(changed))
	if !ok || p3 == p1 {
		t.Fatal("changed doc should be recompiled, not served from cache")
	}

	// Empty and malformed docs report not-ok.
	if _, ok := c.get("pub", nil); ok {
		t.Fatal("empty doc should be not-ok")
	}
	if _, ok := c.get("bad", []byte("{not json")); ok {
		t.Fatal("malformed doc should be not-ok")
	}
	// A malformed doc is still cached (as nil) so we don't re-parse it every hit.
	c.mu.RLock()
	_, cached := c.m["bad"]
	c.mu.RUnlock()
	if !cached {
		t.Fatal("malformed doc should be negatively cached")
	}
}
