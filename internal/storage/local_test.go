package storage

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
)

// TestWriteAllAndRenameDirDurablePath exercises the commit primitives with the
// directory-fsync path enabled (the default). On Linux this runs the real
// syncDir; everywhere it proves WriteAll + RenameDir still round-trip data.
func TestWriteAllAndRenameDirDurablePath(t *testing.T) {
	ctx := context.Background()
	d, err := OpenLocalDisk(t.TempDir(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := d.MakeVol(ctx, "buck"); err != nil {
		t.Fatal(err)
	}

	// Stage an object dir (xl.meta + one shard), then commit it atomically.
	stg := "staging-obj"
	if err := d.WriteAll(ctx, "", filepath.Join(stg, "xl.meta"), []byte(`{"v":1}`)); err != nil {
		t.Fatalf("WriteAll xl.meta: %v", err)
	}
	if err := d.CreateFile(ctx, "", filepath.Join(stg, "part.00001"), 4, bytes.NewReader([]byte("data"))); err != nil {
		t.Fatalf("CreateFile shard: %v", err)
	}
	if err := d.RenameDir(ctx, "", stg, "buck", "a/b/obj"); err != nil {
		t.Fatalf("RenameDir: %v", err)
	}

	got, err := d.ReadAll(ctx, "buck", filepath.Join("a/b/obj", "xl.meta"))
	if err != nil || string(got) != `{"v":1}` {
		t.Fatalf("xl.meta after commit = %q, %v", got, err)
	}
	rc, err := d.ReadFileStream(ctx, "buck", filepath.Join("a/b/obj", "part.00001"), 0, -1)
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 4)
	_, _ = rc.Read(buf)
	_ = rc.Close()
	if string(buf) != "data" {
		t.Fatalf("shard after commit = %q", buf)
	}
	// Staging dir must be gone.
	if _, err := os.Stat(filepath.Join(d.root, sysDir, stg)); !os.IsNotExist(err) {
		// stg was written under bucket "" which maps to root, not sysDir; just
		// assert the rename source no longer exists at its real location.
	}
	if _, err := os.Stat(filepath.Join(d.root, stg)); !os.IsNotExist(err) {
		t.Fatalf("staging dir still present after RenameDir")
	}
}

func TestOpenLocalDiskRejectsSwappedFormat(t *testing.T) {
	root := t.TempDir()
	if _, err := OpenLocalDisk(root, 0, 2); err != nil { // first format: set 0, disk 2
		t.Fatal(err)
	}
	// Re-open the same path claiming a different position -> refused.
	if _, err := OpenLocalDisk(root, 1, 2); err == nil {
		t.Fatal("expected a format-mismatch error when the disk position changed")
	}
	// Same position -> fine.
	if _, err := OpenLocalDisk(root, 0, 2); err != nil {
		t.Fatalf("re-open at the same position should succeed: %v", err)
	}
	// Override escape hatch.
	t.Setenv("GOSTORE_ALLOW_FORMAT_MISMATCH", "1")
	if _, err := OpenLocalDisk(root, 3, 3); err != nil {
		t.Fatalf("override should allow a mismatch: %v", err)
	}
}

func TestSetDirSyncToggle(t *testing.T) {
	SetDirSync(false)
	if dirSyncEnabled() {
		t.Fatal("expected dir sync disabled")
	}
	SetDirSync(true)
	if !dirSyncEnabled() {
		t.Fatal("expected dir sync enabled")
	}
}
