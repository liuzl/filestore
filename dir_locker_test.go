package filestore

import (
	"path/filepath"
	"testing"
)

func TestDirLocker(t *testing.T) {
	locker := NewDirLocker()
	defer locker.Close()

	dir := filepath.Join(".", "testlocker")
	err := locker.TryLockDir(dir)
	if err != nil {
		t.Error(err)
	}
	err = locker.TryLockDir(dir)
	if err != nil {
		t.Error(err)
	}
}
