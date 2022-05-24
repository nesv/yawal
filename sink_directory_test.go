package wal

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func fmtTempDir(prefix string) string {
	return filepath.Join(os.TempDir(), fmt.Sprintf("%s.%d", prefix, testTimestamp.UnixNano()))
}

var testTimestamp = time.Now()

func TestDirectorySink(t *testing.T) {
	tempdir := fmtTempDir("gca-wal")

	// Make sure to clean up after ourselves.
	defer func() {
		t.Log("rm -rf", tempdir)
		os.RemoveAll(tempdir)
	}()

	t.Run("NewDirectorySink", func(t *testing.T) {
		s, err := NewDirectorySink(tempdir)
		if err != nil {
			t.Error(err)
		}

		// Check to see if the sink created the directory.
		info, err := os.Stat(tempdir)
		if err != nil && os.IsNotExist(err) {
			t.Error("DirectorySink did not create wal dir:", err)
		} else if err != nil {
			t.Error(err)
		}
		if !info.IsDir() {
			t.Error(tempdir, "is not a directory")
		}

		t.Run("WriteSegment", func(t *testing.T) {
			// Create a segment, write some data to it, and then use the sink to
			// write the segment.
			seg := NewSegment()
			for i := 0; i < 100; i++ {
				_, err := seg.Write([]byte("hello, wal"))
				if err != nil && err != ErrNotEnoughSpace {
					break
				} else if err != nil {
					t.Error("unexpected error writing data to segment:", err)
				}
			}
			if err := s.WriteSegment(seg); err != nil {
				t.Error(err)
			}

			// Make sure the sink wrote the segment to disk.
			start, end := seg.Limits()
			segFile := filepath.Join(tempdir, start.String()+"-"+end.String())
			if fi, err := os.Stat(segFile); err != nil {
				t.Error(err)
			} else {
				want, err := seg.EncodedSize()
				if err != nil {
					t.Error("failed to calculate size of encoded segment:", err)
				}
				got := fi.Size()

				if want != got {
					t.Errorf("mismatched segment file size: want=%d got=%d", want, got)
				}
			}
		})

		if err := s.Close(); err != nil {
			t.Error("error closing sink:", err)
		}
	})

	// Test the sink's Analyze method.
	t.Run("Analyze", func(t *testing.T) {
		if matches, err := filepath.Glob(filepath.Join(tempdir, "*")); err != nil {
			t.Error(err)
		} else {
			for _, match := range matches {
				t.Log(match)
				t.Log(filepath.Base(match))
			}
		}

		s, err := NewDirectorySink(tempdir)
		if err != nil {
			t.Error(err)
		}

		t.Run("findFiles", func(t *testing.T) {
			files, _, err := s.findFiles()
			if err != nil {
				t.Error(err)
			}
			t.Logf("found %d segment files", len(files))
		})

		if err := s.Analyze(); err != nil {
			t.Error(err)
		}

		t.Run("NumSegments", func(t *testing.T) {
			// On this "clean" sink that has analyzed its working
			// directory, make sure it only picks up the one segment
			// we have previously written.
			want := 1
			if got := s.NumSegments(); got != want {
				t.Errorf("wrong number of segments: want=%d got=%d", want, got)
			}
		})
	})
}

func TestDirectorySinkMulti(t *testing.T) {
	tempdir := fmtTempDir("gca-wal") + "-multi"
	defer func() {
		t.Log("rm -rf", tempdir)
		os.RemoveAll(tempdir)
	}()

	// Use a 1MB segment size, for testing.
	segSize := 1024 * 1024

	// Generate 20 segments-worth of data.
	t.Run("Write20", func(t *testing.T) {
		ds, err := NewDirectorySink(tempdir)
		if err != nil {
			t.Error(err)
		}
		message := []byte("hello")
	Outer:
		for i := 0; i < 20; i++ {
			t.Log("generating segment", i+1)
			seg := NewSegmentSize(uint64(segSize))
			for {
				if _, err := seg.Write(message); err != nil && err == ErrNotEnoughSpace {
					if err := ds.WriteSegment(seg); err != nil {
						t.Error(err)
					}
					continue Outer
				} else if err != nil {
					t.Error("unexpected error:", err)
				}
			}
		}
	})

	t.Run("Reader", func(t *testing.T) {
		sink, err := NewDirectorySink(tempdir)
		if err != nil {
			t.Error(err)
		}
		if err := sink.Analyze(); err != nil {
			t.Error(err)
		}

		r := NewReader(sink)
		for r.Next() {
			p := r.Data()
			want := []byte("hello")
			if !bytes.Equal(p, want) {
				t.Errorf("want=%q got=%q", string(want), string(p))
			}
		}
		if err := r.Error(); err != nil {
			t.Error(err)
		}
	})

	t.Run("Analyze20", func(t *testing.T) {
		sink, err := NewDirectorySink(tempdir)
		if err != nil {
			t.Error(err)
		}

		if err := sink.Analyze(); err != nil {
			t.Error(err)
		}

		t.Run("TruncateAtHalf", func(t *testing.T) {
			first, last := sink.Offsets()
			t.Logf("from=%s to=%s", first, last)
			offset := Offset((int64(first) + int64(last)) / 2)
			if err := sink.Truncate(offset); err != nil {
				t.Error(err)
			}
			first, last = sink.Offsets()
			t.Logf("from=%s to=%s", first, last)
		})

		sink.Close()
	})

	t.Run("ReaderAfterTruncate", func(t *testing.T) {
		sink, err := NewDirectorySink(tempdir)
		if err != nil {
			t.Error(err)
		}
		if err := sink.Analyze(); err != nil {
			t.Error(err)
		}

		r := NewReader(sink)
		for r.Next() {
			p := r.Data()
			want := []byte("hello")
			if !bytes.Equal(p, want) {
				t.Errorf("want=%q got=%q", string(want), string(p))
			}
		}
		if err := r.Error(); err != nil {
			t.Error(err)
		}
	})
}
