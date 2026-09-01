//go:build !unix

package storage

func diskUsage(path string) (total, free uint64) { return 0, 0 }
