// Package policy implements a practical subset of the AWS IAM / S3 bucket
// policy language: Version, Statement[], Effect, Action[], Resource[], and a
// small set of string/IP conditions. Evaluation is: any explicit Deny wins;
// otherwise an Allow is required; default is Deny.
package policy

import (
	"encoding/json"
	"net"
	"strings"
)

// Policy is a parsed policy document.
type Policy struct {
	Version    string      `json:"Version"`
	ID         string      `json:"Id,omitempty"`
	Statements []Statement `json:"Statement"`
}

// Statement is one policy statement.
type Statement struct {
	SID       string     `json:"Sid,omitempty"`
	Effect    string     `json:"Effect"` // "Allow" | "Deny"
	Principal Principal  `json:"Principal,omitempty"`
	Action    StringSet  `json:"Action"`
	NotAction StringSet  `json:"NotAction,omitempty"`
	Resource  StringSet  `json:"Resource"`
	Condition Conditions `json:"Condition,omitempty"`
}

// Principal is "*" or {"AWS": [...]} — only "*" (public) is meaningful here.
type Principal struct {
	AWS StringSet `json:"AWS,omitempty"`
	raw string
}

func (p *Principal) UnmarshalJSON(b []byte) error {
	var s string
	if json.Unmarshal(b, &s) == nil {
		p.raw = s
		return nil
	}
	var obj struct {
		AWS StringSet `json:"AWS"`
	}
	if err := json.Unmarshal(b, &obj); err != nil {
		return err
	}
	p.AWS = obj.AWS
	return nil
}

// IsPublic reports whether the statement targets everyone.
func (p Principal) IsPublic() bool {
	if p.raw == "*" {
		return true
	}
	for _, a := range p.AWS {
		if a == "*" {
			return true
		}
	}
	return false
}

// StringSet is a JSON value that may be a single string or an array.
type StringSet []string

func (s *StringSet) UnmarshalJSON(b []byte) error {
	var one string
	if json.Unmarshal(b, &one) == nil {
		*s = []string{one}
		return nil
	}
	var many []string
	if err := json.Unmarshal(b, &many); err != nil {
		return err
	}
	*s = many
	return nil
}

// Conditions is Condition -> operator -> key -> values.
type Conditions map[string]map[string]StringSet

// Args is the request context evaluated against a policy.
type Args struct {
	AccountName string // access key of the requester
	Action      string // e.g. "s3:GetObject"
	BucketName  string
	ObjectName  string
	IsOwner     bool
	SourceIP    string
	// ConditionValues carries extra keys (e.g. "s3:prefix").
	ConditionValues map[string][]string
}

// Parse parses a JSON policy document.
func Parse(b []byte) (*Policy, error) {
	var p Policy
	if err := json.Unmarshal(b, &p); err != nil {
		return nil, err
	}
	return &p, nil
}

// MarshalJSON keeps the canonical field order.
func (p *Policy) MarshalJSON() ([]byte, error) {
	type alias Policy
	if p.Version == "" {
		p.Version = "2012-10-17"
	}
	return json.Marshal((*alias)(p))
}

// IsAllowed evaluates args against the policy.
func (p *Policy) IsAllowed(a Args) bool {
	// Explicit deny short-circuits.
	for _, st := range p.Statements {
		if st.Effect == "Deny" && st.matches(a) {
			return false
		}
	}
	for _, st := range p.Statements {
		if st.Effect == "Allow" && st.matches(a) {
			return true
		}
	}
	return false
}

func (st Statement) matches(a Args) bool {
	if !st.matchAction(a.Action) {
		return false
	}
	if !st.matchResource(a) {
		return false
	}
	if !st.matchConditions(a) {
		return false
	}
	return true
}

func (st Statement) matchAction(action string) bool {
	if len(st.NotAction) > 0 {
		for _, pat := range st.NotAction {
			if wildcardMatch(pat, action) {
				return false
			}
		}
		return true
	}
	for _, pat := range st.Action {
		if wildcardMatch(pat, action) {
			return true
		}
	}
	return false
}

func (st Statement) matchResource(a Args) bool {
	res := a.BucketName
	if a.ObjectName != "" {
		res = a.BucketName + "/" + a.ObjectName
	}
	for _, pat := range st.Resource {
		p := strings.TrimPrefix(pat, "arn:aws:s3:::")
		if wildcardMatch(p, res) {
			return true
		}
		// "bucket" should also match a policy resource of "bucket/*"? No — but
		// a bare-bucket action (ListBucket) must match "arn:...:::bucket".
		if a.ObjectName == "" && wildcardMatch(p, a.BucketName) {
			return true
		}
	}
	return false
}

func (st Statement) matchConditions(a Args) bool {
	for op, kv := range st.Condition {
		for key, want := range kv {
			got := conditionValue(a, key)
			if !evalCondition(op, got, want) {
				return false
			}
		}
	}
	return true
}

func conditionValue(a Args, key string) []string {
	switch key {
	case "aws:username":
		return []string{a.AccountName}
	case "aws:SourceIp":
		return []string{a.SourceIP}
	default:
		if v, ok := a.ConditionValues[key]; ok {
			return v
		}
		return nil
	}
}

func evalCondition(op string, got []string, want StringSet) bool {
	switch op {
	case "StringEquals":
		return anyEqual(got, want, false)
	case "StringNotEquals":
		return !anyEqual(got, want, false)
	case "StringEqualsIgnoreCase":
		return anyEqual(got, want, true)
	case "StringLike":
		for _, g := range got {
			for _, w := range want {
				if wildcardMatch(w, g) {
					return true
				}
			}
		}
		return len(got) == 0
	case "IpAddress":
		return ipMatch(got, want)
	case "NotIpAddress":
		return !ipMatch(got, want)
	default:
		return true // unknown operator: don't block
	}
}

func anyEqual(got []string, want StringSet, ci bool) bool {
	for _, g := range got {
		for _, w := range want {
			if g == w || (ci && strings.EqualFold(g, w)) {
				return true
			}
		}
	}
	return false
}

func ipMatch(got []string, want StringSet) bool {
	for _, g := range got {
		ip := net.ParseIP(g)
		if ip == nil {
			continue
		}
		for _, w := range want {
			if _, cidr, err := net.ParseCIDR(w); err == nil && cidr.Contains(ip) {
				return true
			}
			if ip.Equal(net.ParseIP(w)) {
				return true
			}
		}
	}
	return false
}

// wildcardMatch matches AWS-style patterns with '*' (any run) and '?' (one).
func wildcardMatch(pattern, s string) bool {
	if pattern == "*" {
		return true
	}
	return deepMatch([]rune(pattern), []rune(s))
}

func deepMatch(pat, str []rune) bool {
	for len(pat) > 0 {
		switch pat[0] {
		case '*':
			return deepMatch(pat[1:], str) ||
				(len(str) > 0 && deepMatch(pat, str[1:]))
		case '?':
			if len(str) == 0 {
				return false
			}
		default:
			if len(str) == 0 || pat[0] != str[0] {
				return false
			}
		}
		pat = pat[1:]
		str = str[1:]
	}
	return len(str) == 0
}
