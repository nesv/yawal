package wal

import (
	"sync"

	"github.com/pkg/errors"
)

// New creates a new write-ahead logger that will persist records to sink.
func New(sink Sink, options ...Option) (*Logger, error) {
	if sink == nil {
		return nil, errors.New("nil sink")
	}
	logger := &Logger{
		sink:    sink,
		segSize: DefaultSegmentSize,
	}
	for _, option := range options {
		if err := option(logger); err != nil {
			return nil, errors.Wrap(err, "applying option")
		}
	}
	logger.seg = NewSegmentSize(logger.segSize)
	return logger, nil
}

// Logger is a type that can be used for maintaining write-ahead logs.
//
// A Logger always maintains an "active" segment that data will be written to.
// For more details, see the Write method's documentation.
type Logger struct {
	sink    Sink
	segSize uint64

	mu     sync.RWMutex
	seg    *Segment // The currently-active segment that data will be written to.
	closed bool     // Indicates if the logger is "closed" for writing.
}

// lock runs the given function fn, while holding a write lock on a *Logger's
// internal mutex.
func (l *Logger) lock(fn func() error) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := fn(); err != nil {
		return err
	}
	return nil
}

// Latest returns the offsets of the first (oldest), and last (newest)
// data chunks.
func (l *Logger) Offsets() (first, last Offset) {
	return l.sink.Offsets()
}

var (
	ErrTooBig       = errors.New("wal: data too large for segment")
	ErrLoggerClosed = errors.New("wal: logger closed")
)

// Write implements the io.Writer interface for a *Logger.
//
// When len(p) > the amount of space left in a segment, the current segment
// will be written to the *Logger's internal Sink, and a new segment will
// be started.
// Should len(p) be larger than the size of a new, empty segment, this method
// will return ErrTooBig.
//
// Any attempt to write to a *Logger, after its Close method has been called,
// will yield ErrLoggerClosed.
func (l *Logger) Write(p []byte) (int, error) {
	if uint64(len(p)) > l.segSize {
		return 0, ErrTooBig
	}

	if err := l.lock(func() error {
		if l.closed {
			return ErrLoggerClosed
		}

	WriteData:
		_, err := l.seg.Write(p)
		if err != nil && err == ErrNotEnoughSpace {
			if err := l.flush(); err != nil {
				return err
			}
			goto WriteData
		}
		return nil
	}); err != nil {
		return 0, errors.Wrap(err, "write")
	}
	return len(p), nil
}

// NewReader returns a new *Reader that can sequentially read chunks of data
// from the earliest-known offset.
func (l *Logger) NewReader() *Reader {
	return NewReader(l.sink)
}

// NewReaderOffset returns a new *Reader that can be used to sequentially read
// chunks of data, starting at offset.
func (l *Logger) NewReaderOffset(offset Offset) *Reader {
	return NewReaderOffset(l.sink, offset)
}

// Close persists the current segment, by writing it to the *Logger's Sink,
// then subsequently closes the Sink.
//
// Close implements the io.Closer interface.
func (l *Logger) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil
	}

	if err := l.flush(); err != nil {
		return errors.Wrap(err, "flush")
	}
	if err := l.sink.Close(); err != nil {
		return errors.Wrap(err, "close sink")
	}

	l.closed = true
	return nil
}

// Flush locks the *Logger for writing, and writes the currently-active
// data segment to the *Logger's internal Sink. If the segment was successfully
// written, a new, empty segment is started, and the *Logger will be unlocked.
//
// Attempting to call Flush after Close will return ErrLoggerClosed.
func (l *Logger) Flush() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return ErrLoggerClosed
	}
	if err := l.flush(); err != nil {
		return errors.Wrap(err, "flush")
	}
	return nil
}

// flush dumps the currently-active data segment to the
// *Logger's internal Sink, and replaces the segment with a new, empty
// one.
func (l *Logger) flush() error {
	if err := l.sink.WriteSegment(l.seg); err != nil {
		return errors.Wrap(err, "write segment")
	}
	l.seg = NewSegmentSize(l.segSize)
	return nil
}

// Truncate removes all data chunks whose offsets are <= offset.
//
// This method attempts to call the underlying Sink's Truncate method, before
// truncating the current segment.
func (l *Logger) Truncate(offset Offset) error {
	if err := l.sink.Truncate(offset); err != nil {
		return errors.Wrap(err, "truncate wal")
	}
	l.lock(func() error {
		l.seg.Truncate(offset)
		return nil
	})
	return nil
}
