package erasure

import "strings"

func validBucketName(name string) bool {
	if len(name) < 3 || len(name) > 63 {
		return false
	}
	if strings.HasPrefix(name, "-") || strings.HasSuffix(name, "-") ||
		strings.HasPrefix(name, ".") || strings.HasSuffix(name, ".") ||
		strings.Contains(name, "..") {
		return false
	}
	for _, r := range name {
		if !(r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '-' || r == '.') {
			return false
		}
	}
	return name != ".gostore.sys"
}

func validObjectName(name string) bool {
	if name == "" || len(name) > 1024 || strings.Contains(name, "\x00") {
		return false
	}
	if strings.HasPrefix(name, "/") || strings.Contains(name, "\\") {
		return false
	}
	for _, seg := range strings.Split(name, "/") {
		if seg == "." || seg == ".." {
			return false
		}
	}
	return !strings.HasPrefix(name, ".gostore.sys/")
}
