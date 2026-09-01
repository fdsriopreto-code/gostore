//go:build !unix

package fs

// diskUsage is a portable no-op fallback (Windows dev boxes). The Linux
// build (deployment target) uses the real syscall implementation.
func diskUsage(path string) (total, free uint64) { return 0, 0 }
