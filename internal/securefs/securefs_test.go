package securefs

import (
	"os"
	"path/filepath"
	"testing"
)

func TestRejectsUnsafeAndSymlink(t *testing.T) {
	root := t.TempDir()
	private := filepath.Join(root, "p")
	if err := os.Mkdir(private, 0700); err != nil {
		t.Fatal(err)
	}
	f := filepath.Join(private, "x")
	if err := os.WriteFile(f, []byte("x"), 0644); err != nil {
		t.Fatal(err)
	}
	if CheckPrivateFile(f, false) == nil {
		t.Fatal("accepted broad mode")
	}
	if err := os.Chmod(f, 0600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(private, "l")
	if err := os.Symlink(f, link); err != nil {
		t.Fatal(err)
	}
	if CheckPrivateFile(link, false) == nil {
		t.Fatal("accepted symlink")
	}
}
func TestAtomicPrivateWrite(t *testing.T) {
	p := filepath.Join(t.TempDir(), "private", "value")
	if err := AtomicPrivateWrite(p, []byte("ok")); err != nil {
		t.Fatal(err)
	}
	if err := CheckPrivateFile(p, false); err != nil {
		t.Fatal(err)
	}
}

func TestLoadPrivateNoFollowAncestorAndSwapBoundaries(t *testing.T) {
	base := t.TempDir()
	realParent := filepath.Join(base, "real", "private")
	if err := os.MkdirAll(realParent, 0700); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(realParent, "value")
	if err := os.WriteFile(file, []byte("secret"), 0600); err != nil {
		t.Fatal(err)
	}
	raw, err := LoadPrivate(file)
	if err != nil || string(raw) != "secret" {
		t.Fatal("safe private file rejected")
	}
	Zero(raw)
	ancestor := filepath.Join(base, "ancestor")
	if err := os.Symlink(filepath.Join(base, "real"), ancestor); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadPrivate(filepath.Join(ancestor, "private", "value")); err == nil {
		t.Fatal("ancestor symlink followed")
	}
	if err := os.Remove(file); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(realParent, "missing"), file); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadPrivate(file); err == nil {
		t.Fatal("final-component replacement symlink followed")
	}
	if err := os.Remove(file); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(file, make([]byte, 32), 0600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := LoadPrivateMaybe(file, 16); err == nil {
		t.Fatal("bounded private read accepted oversized file")
	}
}

func TestZeroClearsCallerBuffer(t *testing.T) {
	raw := []byte("sensitive")
	Zero(raw)
	for _, value := range raw {
		if value != 0 {
			t.Fatal("buffer was not cleared")
		}
	}
}

func TestStablePrivateReadRejectsGrowthAndTruncation(t *testing.T) {
	if !stablePrivateRead(6, 6, 6) {
		t.Fatal("unchanged complete read rejected")
	}
	for _, sizes := range [][3]int64{
		{6, 7, 7},
		{7, 6, 6},
		{6, 6, 5},
	} {
		if stablePrivateRead(sizes[0], sizes[1], sizes[2]) {
			t.Fatalf("changed private read accepted: before=%d after=%d read=%d", sizes[0], sizes[1], sizes[2])
		}
	}
}
