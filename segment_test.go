package wal

import (
	"bytes"
	"strconv"
	"testing"
)

func TestSegmentWriteAndLoad(t *testing.T) {
	s := NewSegmentSize(1048576) // Use a 1MB segment.

	// Populate the segment with data.
	for i := 0; ; i++ {
		message := []byte(strconv.FormatInt(int64(i), 10) + ".hello")
		if _, err := s.Write(message); err != nil && err == ErrNotEnoughSpace {
			break
		} else if err != nil {
			t.Error(err)
		}
	}

	// Write out the segment.
	buf := new(bytes.Buffer)
	nwritten, err := s.WriteTo(buf)
	if err != nil {
		t.Error(err)
	}

	// Create a new segment, and read the data in from the buffer.
	g := NewSegment()
	nread, err := g.ReadFrom(buf)
	if err != nil {
		t.Error(err)
	}

	if nread != nwritten {
		t.Errorf("mismatched number of bytes: wanted=%v got=%v", nwritten, nread)
	}
}
