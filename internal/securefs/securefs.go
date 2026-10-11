package securefs

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"
)

const DefaultMaxPrivateBytes = int64(1 << 20)

func EnsurePrivateDir(path string) error {
	if !filepath.IsAbs(path) {
		return fmt.Errorf("private directory must be absolute")
	}
	if err := rejectSymlinkAncestors(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		if err := os.MkdirAll(path, 0700); err != nil {
			return err
		}
		info, err = os.Lstat(path)
	}
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return fmt.Errorf("private path is not a real directory")
	}
	if info.Mode().Perm()&0077 != 0 {
		return fmt.Errorf("private directory permissions are too broad")
	}
	return nil
}

func CheckPrivateFile(path string, allowMissing bool) error {
	if !filepath.IsAbs(path) {
		return fmt.Errorf("protected path must be absolute")
	}
	if err := rejectSymlinkAncestors(filepath.Dir(path)); err != nil {
		return err
	}
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) && allowMissing {
		return nil
	}
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return fmt.Errorf("protected path is not a regular file")
	}
	if info.Mode().Perm()&0077 != 0 {
		return fmt.Errorf("protected file permissions are too broad")
	}
	parent, err := os.Lstat(filepath.Dir(path))
	if err != nil {
		return err
	}
	if parent.Mode()&os.ModeSymlink != 0 || !parent.IsDir() || parent.Mode().Perm()&0077 != 0 {
		return fmt.Errorf("protected parent is unsafe")
	}
	return nil
}

func AtomicPrivateWrite(path string, data []byte) error {
	if err := EnsurePrivateDir(filepath.Dir(path)); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".write-*")
	if err != nil {
		return err
	}
	name := tmp.Name()
	defer os.Remove(name)
	if err := tmp.Chmod(0600); err != nil {
		tmp.Close()
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(name, path)
}
func LoadPrivate(path string) ([]byte, error) {
	raw, _, err := LoadPrivateMaybe(path, DefaultMaxPrivateBytes)
	return raw, err
}

// LoadPrivateMaybe performs one O_NOFOLLOW open followed by fstat and a bounded
// read. This closes the final-component check/use replacement window. Ancestor
// components are rejected by lstat; callers must still place managed roots in
// non-writable parents because portable Go cannot atomically open an entire
// path component chain on every supported macOS version.
func LoadPrivateMaybe(path string, maxBytes int64) ([]byte, bool, error) {
	if maxBytes <= 0 || !filepath.IsAbs(path) {
		return nil, false, fmt.Errorf("invalid protected read")
	}
	if err := rejectSymlinkAncestors(filepath.Dir(path)); err != nil {
		return nil, false, err
	}
	fd, err := syscall.Open(path, syscall.O_RDONLY|syscall.O_CLOEXEC|syscall.O_NOFOLLOW, 0)
	if errors.Is(err, syscall.ENOENT) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("protected open rejected")
	}
	file := os.NewFile(uintptr(fd), path)
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 || info.Size() > maxBytes {
		return nil, false, fmt.Errorf("protected file rejected")
	}
	parent, err := os.Lstat(filepath.Dir(path))
	if err != nil || parent.Mode()&os.ModeSymlink != 0 || !parent.IsDir() || parent.Mode().Perm()&0077 != 0 {
		return nil, false, fmt.Errorf("protected parent is unsafe")
	}
	raw, err := io.ReadAll(io.LimitReader(file, maxBytes+1))
	if err != nil || int64(len(raw)) > maxBytes {
		Zero(raw)
		return nil, false, fmt.Errorf("protected read rejected")
	}
	after, err := file.Stat()
	if err != nil || !os.SameFile(info, after) || !stablePrivateRead(info.Size(), after.Size(), int64(len(raw))) {
		Zero(raw)
		return nil, false, fmt.Errorf("protected file changed during read")
	}
	return raw, true, nil
}

func stablePrivateRead(beforeSize, afterSize, bytesRead int64) bool {
	return beforeSize == afterSize && afterSize == bytesRead
}

func Zero(value []byte) {
	for i := range value {
		value[i] = 0
	}
}

func rejectSymlinkAncestors(path string) error {
	clean := filepath.Clean(path)
	if !filepath.IsAbs(clean) {
		return fmt.Errorf("protected path must be absolute")
	}
	for current := clean; current != string(filepath.Separator); current = filepath.Dir(current) {
		// macOS exposes these system compatibility prefixes as symlinks. They
		// are outside every managed root and are accepted only as the prefix.
		if current == "/var" || current == "/tmp" || current == "/etc" {
			break
		}
		info, err := os.Lstat(current)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("protected ancestor is a symlink")
		}
	}
	return nil
}

func CheckNoSymlinkAncestors(path string) error {
	return rejectSymlinkAncestors(path)
}
