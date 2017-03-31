package wal

import (
	"io"
	"sync"
)

// MemorySink is a Sink implementation that only stores data in memory.
type MemorySink struct {
	mu       sync.RWMutex
	segments []*Segment
}

// NewMemorySink returns a Sink implementation that stores segments in memory.
func NewMemorySink() (*MemorySink, error) {
	return &MemorySink{
		segments: make([]*Segment, 0),
	}, nil
}

func (s *MemorySink) Analyze() error {
	return nil
}

func (s *MemorySink) LoadSegment(offset Offset) (*Segment, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if offset.Equal(ZeroOffset) {
		return s.segments[0], nil
	}

	for i, seg := range s.segments {
		a, b := seg.Limits()
		if offset.Within(a, b) || offset.Before(a) {
			return s.segments[i], nil
		}
	}
	return nil, io.EOF
}

func (s *MemorySink) WriteSegment(seg *Segment) error {
	first, last := seg.Limits()
	if first.Equal(ZeroOffset) && last.Equal(ZeroOffset) {
		return nil
	}

	s.mu.Lock()
	s.segments = append(s.segments, seg)
	s.mu.Unlock()
	return nil
}

func (s *MemorySink) Offsets() (first, last Offset) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	first, _ = s.segments[0].Limits()
	_, last = s.segments[len(s.segments)-1].Limits()
	return first, last
}

func (s *MemorySink) NumSegments() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.segments)
}

func (s *MemorySink) Truncate(offset Offset) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// See if there are any whole segments we can remove.
	removed := 0
	for _, seg := range s.segments {
		_, end := seg.Limits()
		if end.Before(offset) {
			removed++
		} else {
			break
		}
	}
	if removed > 0 {
		s.segments = s.segments[removed:]
	}

	// See if we need to truncate the first segment.
	if offset.Within(s.segments[0].Limits()) {
		s.segments[0].Truncate(offset)
	}

	return nil
}

func (s *MemorySink) Close() error {
	return nil
}
