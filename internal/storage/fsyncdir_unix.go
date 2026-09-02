//go:build unix

package storage

import "os"

// syncDir fsyncs a directory so that a create/rename/unlink of an entry inside
// it survives a power loss. POSIX requires this in addition to fsyncing the
// file itself — without it a freshly renamed object can vanish on a crash even
// though its bytes were flushed.
func syncDir(dir string) error {
	if !dirSyncEnabled() {
		return nil
	}
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	serr := f.Sync()
	cerr := f.Close()
	if serr != nil {
		return serr
	}
	return cerr
}
