package wal

import (
	"io"

	"github.com/pkg/errors"
)

// Reader loads data segments from a Sink, and progresses through a
// data segment until there are no more chunks to be read. When the end of a
// current segment is reached, a Reader will attempt to increment the
// last-known chunk offset by one, and load the next-available data segment.
//
// It is not safe to call a Reader from multiple goroutines.
//
// Example:
//
//	r := NewReader(sink)
//
//	for r.Next() {
//		fmt.Printf("% x\n", r.Data())
//	}
//
//	if err := r.Error(); err != nil {
//		log.Println("error:", err)
//	}
type Reader struct {
	sink Sink
	off  Offset   // The last-known offset.
	seg  *Segment // Current segment being read.
	err  error
}

// NewReader returns a *Reader that reads data chunks from sink, starting
// at the earliest-possible offset.
func NewReader(sink Sink) *Reader {
	return NewReaderOffset(sink, ZeroOffset)
}

// NewReaderOffset returns a *Reader that starts reading data chunks from
// sink, at the specified offset.
func NewReaderOffset(sink Sink, offset Offset) *Reader {
	return &Reader{
		sink: sink,
		off:  offset,
	}
}

// Next reports whether or not there is another data chunk that can be read
// using the Data method.
//
// A false return value means there are no more data chunks that can be
// read from the current segment, and no more segments can be loaded.
func (r *Reader) Next() bool {
	if r.seg == nil {
		if seg, err := r.loadSegment(r.off); err != nil {
			r.err = err
			return false
		} else {
			r.seg = seg
		}
	}

NextDataChunk:
	// Is there more that can be read in the current segment?
	if r.seg.Next() {
		r.off = r.seg.CurrentReadOffset()
		return true
	}

	// Attempt to load the next segment.
	if seg, err := r.loadSegment(r.off + 1); err != nil {
		r.err = err
		return false
	} else if seg == nil {
		return false
	} else {
		r.seg = seg
	}
	goto NextDataChunk
}

func (r *Reader) loadSegment(off Offset) (*Segment, error) {
	seg, err := r.sink.LoadSegment(off)
	if err != nil && err == io.EOF {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	return seg, nil
}

// Data returns the []byte of the current data chunk. Successive calls to
// Data, without calling Next, will return the same []byte.
func (r *Reader) Data() []byte {
	return r.seg.Chunk().Data()
}

// Offset returns the offset of the current data chunk. Multiple calls to
// Offset, without calling Next, will return the same offset.
func (r *Reader) Offset() Offset {
	return r.off
}

// Error returns the most-recent error encountered by the *Reader.
func (r *Reader) Error() error {
	if r.err != nil {
		return errors.Wrap(r.err, "wal reader")
	}
	return nil
}
