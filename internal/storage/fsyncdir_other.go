//go:build !unix

package storage

// syncDir is a no-op on platforms (Windows) where directory handles cannot be
// fsynced. The production target is Linux; this keeps local dev builds working.
func syncDir(string) error { return nil }
