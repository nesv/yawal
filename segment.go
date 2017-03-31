package wal

import (
	"bytes"
	"io"
	"io/ioutil"
	"sync"

	"github.com/pkg/errors"
)

const (
	// DefaultSegmentSize is the default size of a data segment (16MB).
	DefaultSegmentSize uint64 = 16777216
)

func NewSegment() *Segment {
	return NewSegmentSize(DefaultSegmentSize)
}

func NewSegmentSize(size uint64) *Segment {
	return &Segment{
		size:     size,
		chunks:   make([]*chunk, 0),
		chunkIdx: -1,
	}
}

// Segment is a size-bounded type that chunks are written to.
//
// While a segment is safe for concurrent reading and writing, it is not
// recommended to do so.
type Segment struct {
	size     uint64 // Maximum size of the segment, in bytes.
	mu       sync.Mutex
	chunks   []*chunk
	chunkIdx int // Index of chunk that will be returned by Data().
}

var (
	ErrNotEnoughSpace = errors.New("not enough space in segment")
	ErrSegmentFull    = errors.New("segment full")
)

// Write writes a copy of p to the segment, as a new data chunk.
//
// If the length of p is greater than the remaining capacity of the
// segment, this method will return ErrNotEnoughSpace.
func (s *Segment) Write(p []byte) (int, error) {
	// If p is nil, or has a length of zero, return early.
	if p == nil || len(p) == 0 {
		return 0, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if int64(len(p)) > s.remaining() {
		return 0, ErrNotEnoughSpace
	}
	return s.write(p)
}

func (s *Segment) write(p []byte) (int, error) {
	s.chunks = append(s.chunks, newChunk(p))
	return len(p), nil
}

// Data returns the current chunk.
// Successive calls to Data will yield the same chunk. To advance to the
// next chunk in the segment, call the Next() method.
func (s *Segment) Chunk() chunk {
	s.mu.Lock()
	defer s.mu.Unlock()
	c := s.chunks[s.chunkIdx]
	return *c
}

// Next reports whether or not there is another chunk that can be read with
// the Chunk() method.
//
// For example:
//
//	for s.Next() {
//		c := s.Chunk()
//		...
//	}
//
func (s *Segment) Next() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.chunkIdx+1 == len(s.chunks) {
		return false
	}
	s.chunkIdx++
	return true
}

// CurrentReadOffset returns the offset of the []byte that will be returned
// by Data.
//
// CurrentReadOffset will panic if it is called before Next.
func (s *Segment) CurrentReadOffset() Offset {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.chunks[s.chunkIdx].Offset()
}

// ReadFrom implements the io.ReaderFrom interface, and is primarily used to
// load a segment from disk.
//
// Calling ReadFrom on a non-empty segment will return a non-nil error.
func (s *Segment) ReadFrom(r io.Reader) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.chunks) != 0 {
		return 0, errors.New("read from: will not load into populated segment")
	}

	p, err := ioutil.ReadAll(r)
	if err != nil {
		return 0, errors.Wrap(err, "read from")
	}
	rows := bytes.Split(p, []byte("\n"))
	s.chunks = []*chunk{}
	for i, row := range rows {
		// Skip empty rows.
		if len(row) == 0 {
			continue
		}
		c := new(chunk)
		if err := c.UnmarshalText(row); err != nil {
			return 0, errors.Wrapf(err, "unmarshal chunk %d", i)
		}
		s.chunks = append(s.chunks, c)
	}

	return int64(len(p)), nil
}

// WriteTo implements the io.WriterTo interface, and is primarily used to
// persist a segment to disk.
//
// The returned int64 is the number of bytes that have been written to w,
// and not the current size of the segment.
func (s *Segment) WriteTo(w io.Writer) (int64, error) {
	// Return early if there are no chunks to write.
	if len(s.chunks) == 0 {
		return 0, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	var n int64
	for i := range s.chunks {
		p, err := s.chunks[i].MarshalText()
		if err != nil {
			return n, errors.Wrapf(err, "marshal chunk %d", i)
		}

		b, err := w.Write(append(p, '\n'))
		if err != nil {
			return n, errors.Wrap(err, "write chunk")
		} else if b < len(p) {
			return n, errors.Wrap(io.ErrShortWrite, "write chunk")
		}
		n += int64(b)
	}
	return n, nil
}

// Chunks returns the current number of chunks in this segment.
func (s *Segment) Chunks() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.chunks)
}

// Size returns the size of the current segment, in bytes.
func (s *Segment) Size() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	var n int64 = 0
	for i := range s.chunks {
		c := s.chunks[i]
		n += int64(len(*c))
	}
	return n
}

// EncodedSize returns the encoded size of the segment, in bytes. This is the
// number of bytes that should be returned by WriteTo, assuming no more chunks
// are added to the segment.
func (s *Segment) EncodedSize() (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var n int64 = 0
	for i := range s.chunks {
		p, err := s.chunks[i].MarshalText()
		if err != nil {
			return 0, errors.Wrapf(err, "marshal chunk %d", i)
		}
		n += int64(len(p)) + 1 // Add 1 for the newline character
	}
	return n, nil
}

// Remaining returns the number of bytes left before the segment is
// at capacity.
func (s *Segment) Remaining() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.remaining()
}

func (s *Segment) remaining() int64 {
	var n uint64
	for i := range s.chunks {
		n += uint64(len(*s.chunks[i]))
	}
	return int64(s.size - n)
}

// Limits returns the oldest and newest offsets of the data chunks
// in the segment.
//
// If there are no data chunks in the segment, this method will return
// ZeroOffset for both offsets.
//
// If there is only one data chunk in the segment, that chunk's offset is
// returned for both oldest, and newest.
func (s *Segment) Limits() (oldest, newest Offset) {
	s.mu.Lock()
	defer s.mu.Unlock()

	switch len(s.chunks) {
	case 0:
		return ZeroOffset, ZeroOffset
	case 1:
		offset := s.chunks[0].Offset()
		return offset, offset
	}
	oldest, newest = s.chunks[0].Offset(), s.chunks[len(s.chunks)-1].Offset()
	return oldest, newest
}

// Truncate removes all chunks from the segment, whose offsets are <= offset.
//
// If the current segment is being read, the internal pointer of the chunk to
// read will be adjusted.
func (s *Segment) Truncate(offset Offset) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for i, c := range s.chunks {
		if c.Offset().After(offset) {
			// Shrink the current chunk slice.
			s.chunks = s.chunks[i:]

			// Adjust the internal read pointer.
			if s.chunkIdx > 0 {
				s.chunkIdx -= i + 1
			}

			break
		}
	}
}
