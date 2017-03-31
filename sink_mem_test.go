package wal

import (
	"math/rand"
	"testing"
	"time"
)

func TestMemorySink(t *testing.T) {
	sink, err := NewMemorySink()
	if err != nil {
		t.Error(err)
	}

	t.Run("Analyze", func(t *testing.T) {
		if err := sink.Analyze(); err != nil {
			t.Error(err)
		}
	})

	t.Run("WriteSegment", func(t *testing.T) {
		seg := NewSegment()
		for i := 0; i < 100; i++ {
			_, err := seg.Write([]byte("Hello, memory sink!"))
			if err != nil && err == ErrNotEnoughSpace {
				break
			} else if err != nil {
				t.Error(err)
			}
		}
		if err := sink.WriteSegment(seg); err != nil {
			t.Error(err)
		}
	})

	t.Run("NumSegments", func(t *testing.T) {
		if n := sink.NumSegments(); n != 1 {
			t.Errorf("wrong number of segments: wanted=%d got=%d", 1, n)
		}
	})

	t.Run("WriteTenSegments", func(t *testing.T) {
		for i := 0; i < 10; i++ {
			seg := NewSegment()
			for j := 0; j < 1000; j++ {
				_, err := seg.Write([]byte("I'm just writing out data to a segment"))
				if err != nil && err == ErrNotEnoughSpace {
					break
				} else if err != nil {
					t.Error(err)
				}
			}
			if err := sink.WriteSegment(seg); err != nil {
				t.Error(err)
			}
		}

		t.Run("NumSegments", func(t *testing.T) {
			if n := sink.NumSegments(); n != 11 {
				t.Errorf("wrong number of segments: wanted=%d got=%d", 11, n)
			}
		})
	})

	// Initialize a random seed, which will be used by the next set of
	// tests.
	rand.Seed(time.Now().UnixNano())

	t.Run("LoadSegment", func(t *testing.T) {
		t.Run("First", func(t *testing.T) {
			if _, err := sink.LoadSegment(ZeroOffset); err != nil {
				t.Error(err)
			}
		})
		t.Run("Random10", func(t *testing.T) {
			_, last := sink.Offsets()
			for i := 0; i < 10; i++ {
				offset := Offset(rand.Int63n(int64(last / 2)))
				_, err := sink.LoadSegment(offset)
				if err != nil {
					t.Errorf("error loading segment at offset %v: %v", offset, err)
					return
				}
			}
		})
	})

	t.Run("Truncate", func(t *testing.T) {
		// Gather all available offsets, so we have something to compare
		// against, after we call sink.Truncate().
		t.Logf("num segments before truncate=%v", sink.NumSegments())
		before := make([][2]Offset, sink.NumSegments())
		woff := ZeroOffset // Working offset.
		for i := 0; i < sink.NumSegments(); i++ {
			seg, err := sink.LoadSegment(woff)
			if err != nil {
				t.Errorf("error loading segment at offset %v: %v", woff, err)
				return
			}
			if seg == nil {
				t.Errorf("loaded nil segment with offset %v", woff)
			}

			a, b := seg.Limits()
			before[i] = [2]Offset{a, b}
			t.Logf("loaded segment %v..%v", a, b)

			woff = b + 1
		}

		// Create a random offset to pass to sink.Truncate().
		first, last := sink.Offsets()
		offset := Offset(rand.Int63n(int64(last-first)) + int64(first))
		if err := sink.Truncate(offset); err != nil {
			t.Error(err)
		}
		t.Logf("truncating at offset %v", offset)

		// Now, gather another list of available offsets, and compare
		// it to the first list.
		t.Logf("num segments after truncate=%v", sink.NumSegments())
		after := make([][2]Offset, sink.NumSegments())
		woff = ZeroOffset // Reset the working offset.
		for i := 0; i < sink.NumSegments(); i++ {
			seg, err := sink.LoadSegment(woff)
			if err != nil {
				t.Error(err)
			}

			a, b := seg.Limits()
			after[i] = [2]Offset{a, b}

			woff = b + 1
		}

		// Compare the two offset collections.
		removed := len(before) - len(after)
		if removed == 0 {
			if old, new := before[0][0], after[0][0]; old.After(new) {
				t.Errorf("old starting segment offset is after new truncated segment offset: old=%v new=%d", old, new)
			} else {
				return
			}
		}

		// Falling through to here, means that more than one segment
		// was completely removed from the sink, and we should load
		// each segment in the sink, to make sure that only one
		// segment was truncated.
		truncated := 0
		for i, j := len(before)-1, len(after)-1; i > 0 && j > 0; i-- {
			oldStart := before[i][0]
			newStart := after[j][0]
			if oldStart.Before(newStart) {
				truncated++
			}
			j--
		}
		// We should only ever - at most - have one (1) truncated
		// segment.
		if truncated > 1 {
			t.Errorf("more than one record truncated")
		}

		t.Logf("removed=%d truncated=%d", removed, truncated)
	})
}
