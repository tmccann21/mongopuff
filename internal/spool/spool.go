package spool

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
)

// Spool is a directory of numbered segment files. The directory is the only
// record of segment numbers: there is no separate counter to fall out of step
// with the files.
type Spool struct {
	dir   string
	first uint32 // oldest segment on disk at Open, or next if the spool was empty
	next  uint32 // number the next Append uses
}

// Open opens the spool in dir, creating it if needed. It lists the segments
// already on disk and continues numbering after the highest one. Delivery
// removes the oldest segment and Append adds the newest, so the numbers on
// disk are always consecutive; Open fails on a gap, which means a segment was
// lost.
func Open(dir string) (*Spool, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	segs, err := listSegments(dir)
	if err != nil {
		return nil, err
	}
	for i := 1; i < len(segs); i++ {
		if segs[i] != segs[i-1]+1 {
			return nil, fmt.Errorf("spool %s: segment %d is missing between %d and %d", dir, segs[i-1]+1, segs[i-1], segs[i])
		}
	}

	s := &Spool{dir: dir}
	if len(segs) > 0 {
		s.first = segs[0]
		s.next = segs[len(segs)-1] + 1
	}
	return s, nil
}

// First returns where delivery starts: the oldest segment on disk when the
// spool was opened, or the number the first Append uses if it was empty.
func (s *Spool) First() uint32 {
	return s.first
}

// Append writes data as the next segment and returns its number. It never
// replaces an existing segment, so a second process appending to the same
// directory gets an error wrapping os.ErrExist instead of overwriting.
// Append is not safe for concurrent use; a spool has one writer.
func (s *Spool) Append(data []byte) (uint32, error) {
	seg := s.next

	// A unique tmp name, so that two writers on one directory cannot write
	// into each other's tmp file.
	tmp, err := os.CreateTemp(s.dir, "*.tmp")
	if err != nil {
		return 0, fmt.Errorf("spool append: %w", err)
	}
	defer os.Remove(tmp.Name())

	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return 0, fmt.Errorf("spool append: %w", err)
	}
	// The data must reach disk before the segment's name does. Otherwise a
	// crash can leave the name pointing at an empty or partial file, which
	// delivery cannot decode.
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return 0, fmt.Errorf("spool append: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return 0, fmt.Errorf("spool append: %w", err)
	}

	// Link, unlike Rename, fails if the segment already exists. Either way the
	// segment appears complete or not at all.
	if err := os.Link(tmp.Name(), s.path(seg)); err != nil {
		return 0, fmt.Errorf("spool append segment %d: %w", seg, err)
	}
	s.next++

	// The caller saves the segment's resume token once Append returns, which
	// leaves the segment as the only copy of its events. Its name has to
	// survive a crash.
	if err := syncDir(s.dir); err != nil {
		return 0, fmt.Errorf("spool append segment %d: %w", seg, err)
	}
	return seg, nil
}

func (s *Spool) Read(segment uint32) ([]byte, error) {
	data, err := os.ReadFile(s.path(segment))
	if err != nil {
		return nil, fmt.Errorf("spool read: %w", err)
	}
	return data, nil
}

func (s *Spool) Remove(segment uint32) error {
	if err := os.Remove(s.path(segment)); err != nil {
		return fmt.Errorf("spool remove: %w", err)
	}
	// A delivered segment whose removal is lost in a crash is delivered again
	// on the next run, after the segments that followed it, and its upserts
	// can bring back documents those segments deleted.
	if err := syncDir(s.dir); err != nil {
		return fmt.Errorf("spool remove segment %d: %w", segment, err)
	}
	return nil
}

// Segments returns the numbers of the segments on disk, in ascending order.
func (s *Spool) Segments() ([]uint32, error) {
	return listSegments(s.dir)
}

func (s *Spool) Close() error {
	return nil
}

func (s *Spool) path(segment uint32) string {
	return filepath.Join(s.dir, fmt.Sprintf("%06d.spool", segment))
}

// syncDir fsyncs dir so that names added to or removed from it survive a
// crash. Syncing a file makes its contents durable but not its name.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// listSegments returns the segment numbers in dir in ascending order, ignoring
// any file that is not a segment, such as a tmp file left by a crash.
func listSegments(dir string) ([]uint32, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("spool list: %w", err)
	}

	var segments []uint32
	for _, e := range entries {
		name := e.Name()
		if !strings.HasSuffix(name, ".spool") {
			continue
		}
		n, err := strconv.ParseUint(strings.TrimSuffix(name, ".spool"), 10, 32)
		if err != nil {
			continue
		}
		segments = append(segments, uint32(n))
	}
	slices.Sort(segments)
	return segments, nil
}
