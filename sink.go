package wal

import "io"

// Sink defines the interface of a type that can persist, and subsequently
// load, write-ahead logging segments.
type Sink interface {
	Analyzer
	SegmentLoader
	SegmentWriter
	io.Closer

	// Offsets returns the first, and last (most-recent) offsets known
	// to a Sink.
	Offsets() (first Offset, last Offset)

	// NumSegments returns the number of segments currently known to
	// the sink.
	NumSegments() int

	// Truncate permanently deletes all data chunks prior to the given
	// offset.
	Truncate(Offset) error
}

// Analyzer defines the interface of a type that can perform analysis on a
// persistent storage medium for write-ahead logs.
type Analyzer interface {
	Analyze() error
}

// SegmentLoader defines the interface of a type that can retrieve segments
// from their persistent storage medium.
type SegmentLoader interface {
	// LoadSegment returns the WAL segment containing the given Offset.
	//
	// If ZeroOffset is specified, then the segment with the lowest
	// offset will be returned.
	//
	// Should the given offset be greater than one contained in any
	// available segments, no segment will be returned, and err will be
	// io.EOF.
	LoadSegment(Offset) (*Segment, error)
}

// SegmentWriter defines the interface of a type that is able to store
// WAL segments.
type SegmentWriter interface {
	WriteSegment(*Segment) error
}
