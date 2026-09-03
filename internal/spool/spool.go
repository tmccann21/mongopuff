package spool

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

type Spool struct {
	dir string
}

func Open(dir string) (*Spool, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}
	return &Spool{
		dir: dir
	}, nil
}

func (s *Spool) Write(segment uint32, data []byte) error {
	tmp := filepath.Join(s.dir, fmt.Sprintf("%06d.tmp", segment))
	final := filepath.Join(s.dir, fmt.Sprintf("%06d.spool", segment))

	if err := os.WriteFile(tmp, data, 0644); err != nil {
		return fmt.Errorf("spool write: %w", err)
	}

	// rename the tmp file to the final file; os.Rename is an atomic operation to ensure a valid file
	// was created
	if err := os.Rename(tmp, final); err != nil {
		return fmt.Errorf("spool rename: %w", err)
	}
	return nil
}

func (s *Spool) Read(segment uint32) ([]byte, error) {
	path := filepath.Join(s.dir, fmt.Sprintf("%06d.spool", segment))
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("spool read: %w", err)
	}
	return data, nil
}

func (s *Spool) Remove(segment uint32) error {
	path := filepath.Join(s.dir, fmt.Sprintf("%06d.spool", segment))
	if err := os.Remove(path); err != nil {
		return fmt.Errorf("spool remove: %w", err)
	}
	return nil
}

func (s *Spool) Segments() ([]uint32, error) {
	entries, err := os.ReadDir(s.dir)
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
	sort.Slice(segments, func(i, j int) bool { return segments[i] < segments[j] })
	return segments, nil
}

func (s *Spool) Close() error {
	return nil
}
