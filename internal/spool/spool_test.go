package spool

import (
	"errors"
	"os"
	"testing"
)

func TestWriteRead_RoundTrip(t *testing.T) {
	sp, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer sp.Close()

	want := []byte("hello, spool")
	if err := sp.Write(1, want); err != nil {
		t.Fatalf("Write: %v", err)
	}

	got, err := sp.Read(1)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}

	if string(got) != string(want) {
		t.Errorf("Read = %q, want %q", got, want)
	}
}

func TestRead_NotExist(t *testing.T) {
	sp, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer sp.Close()

	_, err = sp.Read(42)
	if err == nil {
		t.Fatal("Read of nonexistent segment returned nil error")
	}
	if !errors.Is(err, os.ErrNotExist) {
		t.Errorf("error = %v, want it to wrap os.ErrNotExist", err)
	}
}

func TestSegments_ReturnsOrderedList(t *testing.T) {
	sp, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer sp.Close()

	// Write out of order.
	for _, seg := range []uint32{5, 2, 8} {
		if err := sp.Write(seg, []byte("data")); err != nil {
			t.Fatalf("Write(%d): %v", seg, err)
		}
	}

	segs, err := sp.Segments()
	if err != nil {
		t.Fatalf("Segments: %v", err)
	}

	want := []uint32{2, 5, 8}
	if len(segs) != len(want) {
		t.Fatalf("Segments() returned %d items, want %d", len(segs), len(want))
	}
	for i := range want {
		if segs[i] != want[i] {
			t.Errorf("Segments()[%d] = %d, want %d", i, segs[i], want[i])
		}
	}
}

func TestSegments_EmptyDir(t *testing.T) {
	sp, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer sp.Close()

	segs, err := sp.Segments()
	if err != nil {
		t.Fatalf("Segments: %v", err)
	}

	if len(segs) != 0 {
		t.Errorf("Segments() = %v, want empty slice", segs)
	}
}
