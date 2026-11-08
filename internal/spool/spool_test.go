package spool

import (
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"
)

func TestAppendRead_RoundTrip(t *testing.T) {
	sp, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	want := []byte("hello, spool")
	seg, err := sp.Append(want)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}

	got, err := sp.Read(seg)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}

	if string(got) != string(want) {
		t.Errorf("Read = %q, want %q", got, want)
	}
}

func TestAppend_NumbersFromZeroInEmptySpool(t *testing.T) {
	sp, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if got := sp.First(); got != 0 {
		t.Errorf("First() = %d, want 0", got)
	}
	for want := uint32(0); want < 3; want++ {
		got, err := sp.Append([]byte("data"))
		if err != nil {
			t.Fatalf("Append: %v", err)
		}
		if got != want {
			t.Errorf("Append returned segment %d, want %d", got, want)
		}
	}
}

func TestOpen_ContinuesAfterHighestSegment(t *testing.T) {
	// The directory is the only record of segment numbers, so a reopened spool
	// must pick up after what is on disk rather than reuse a number.
	dir := t.TempDir()
	writeSegmentFiles(t, dir, 4, 5, 6)

	sp, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if got := sp.First(); got != 4 {
		t.Errorf("First() = %d, want 4", got)
	}
	seg, err := sp.Append([]byte("data"))
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if seg != 7 {
		t.Errorf("Append returned segment %d, want 7", seg)
	}
}

func TestOpen_FailsOnMissingSegment(t *testing.T) {
	// Segments are removed oldest-first and appended newest-last, so a gap
	// means a segment was lost. Delivering around it would skip its events.
	dir := t.TempDir()
	writeSegmentFiles(t, dir, 1, 3)

	if _, err := Open(dir); err == nil {
		t.Fatal("Open succeeded on a spool missing segment 2, want an error")
	}
}

func TestOpen_IgnoresTmpFiles(t *testing.T) {
	// A crash mid-Append can leave a tmp file. It is not a segment.
	dir := t.TempDir()
	writeSegmentFiles(t, dir, 0)
	if err := os.WriteFile(filepath.Join(dir, "123456.tmp"), []byte("partial"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	if _, err := Open(dir); err != nil {
		t.Fatalf("Open: %v", err)
	}
	segs, err := listSegments(dir)
	if err != nil {
		t.Fatalf("listSegments: %v", err)
	}
	if want := []uint32{0}; !slices.Equal(segs, want) {
		t.Errorf("listSegments = %v, want %v", segs, want)
	}
}

func TestAppend_DoesNotReplaceExistingSegment(t *testing.T) {
	// A segment on disk may hold the only copy of events whose resume token is
	// already saved. Two processes sharing a spool directory both number their
	// next segment the same; the second Append must fail rather than replace
	// the first's segment.
	dir := t.TempDir()
	first, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	second, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	seg, err := first.Append([]byte("first"))
	if err != nil {
		t.Fatalf("first Append: %v", err)
	}

	if _, err := second.Append([]byte("second")); !errors.Is(err, os.ErrExist) {
		t.Errorf("second Append error = %v, want it to wrap os.ErrExist", err)
	}

	got, err := first.Read(seg)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if string(got) != "first" {
		t.Errorf("Read = %q, want %q", got, "first")
	}
}

func TestRead_NotExist(t *testing.T) {
	sp, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	_, err = sp.Read(42)
	if err == nil {
		t.Fatal("Read of nonexistent segment returned nil error")
	}
	if !errors.Is(err, os.ErrNotExist) {
		t.Errorf("error = %v, want it to wrap os.ErrNotExist", err)
	}
}

func TestListSegments_ReturnsOrderedList(t *testing.T) {
	// Names are zero-padded to six digits, so past 999999 they gain a digit and
	// sorting by name would put 1000000 first.
	dir := t.TempDir()
	want := []uint32{999998, 999999, 1000000}
	writeSegmentFiles(t, dir, want...)

	segs, err := listSegments(dir)
	if err != nil {
		t.Fatalf("listSegments: %v", err)
	}
	if !slices.Equal(segs, want) {
		t.Errorf("listSegments = %v, want %v", segs, want)
	}
}

func TestListSegments_EmptyDir(t *testing.T) {
	segs, err := listSegments(t.TempDir())
	if err != nil {
		t.Fatalf("listSegments: %v", err)
	}

	if len(segs) != 0 {
		t.Errorf("listSegments = %v, want empty slice", segs)
	}
}

// writeSegmentFiles creates segment files directly, bypassing Append, to set up
// a directory as a previous run would have left it.
func writeSegmentFiles(t *testing.T, dir string, segs ...uint32) {
	t.Helper()
	for _, seg := range segs {
		path := (&Spool{dir: dir}).path(seg)
		if err := os.WriteFile(path, []byte("data"), 0o644); err != nil {
			t.Fatalf("WriteFile(%s): %v", path, err)
		}
	}
}
