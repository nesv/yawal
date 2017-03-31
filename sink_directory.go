package wal

import (
	"bytes"
	"encoding/hex"
	"hash"
	"hash/crc64"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/pkg/errors"
)

// DirectorySink implements a Sink that can persist WAL segments to,
// and load them from, a directory.
//
// The nomenclature of the on-disk WAL segment files is:
//
//	<chunkOffset0>-<chunkOffsetN>
//
// where chunkOffsetN is the offset of the last data chunk in the segment.
// As an example, for a segment holding data chunks written between
// January 1 2017 00:00 and January 1 2017 01:00, the resulting segment's
// file name would be:
//
//	1483228800000000000-1483232400000000000
//
// Each WAL segment file is accompanied by another file containing a
// checksum used for verifying the contents of the segment. The checksum
// file name, for the above segment, would be:
//
//	1483228800000000000-1483232400000000000.CHECKSUM
//
type DirectorySink struct {
	dir string

	mu       sync.RWMutex
	segments [][2]Offset
	segPaths []string // holds the basename of each segment file
}

// NewDirectorySink returns a *DirectorySink that can read and write
// WAL segments to directory dir.
//
// The permissions of dir will be checked to ensure the *DirectorySink
// can read and write to dir. If the directory does not exist, it will be
// created with mode 0777 (before umask).
func NewDirectorySink(dir string) (*DirectorySink, error) {
	dir, err := filepath.Abs(dir)
	if err != nil {
		return nil, errors.Wrap(err, "new directory sink")
	}
	if err := checkDirPerms(dir); err != nil && os.IsNotExist(errors.Cause(err)) {
		// Create the directory.
		if err := os.MkdirAll(dir, 0777); err != nil {
			return nil, errors.Wrap(err, "mkdir all")
		}
	} else if err != nil {
		return nil, errors.Wrap(err, "new directory sink")
	}

	ds := &DirectorySink{
		dir: dir,
	}
	return ds, nil
}

// Analyze scans the directory the *DirectorySink was initialized with, and
// gathers all of the currently-available offsets.
//
// This method also attempts to verify each found segment, by calculating a
// checksum of the segment file, and comparing it to the checksum in the
// segment's checksum file.
func (ds *DirectorySink) Analyze() error {
	// "Reset" the slices containing the currently-known segment offsets,
	// and the paths to them.
	//
	// This is to force a clean state of operation.
	ds.mu.Lock()
	defer ds.mu.Unlock()

	ds.reset()

	if len(ds.segments) != 0 {
		ds.segments = [][2]Offset{}
	}
	if len(ds.segPaths) != 0 {
		ds.segPaths = []string{}
	}

	// Now, search through the sink's working directory to find all
	// segment files.
	files, chksums, err := ds.findFiles()
	if err != nil {
		return errors.Wrap(err, "find files")
	}
	for i, name := range files {
		// Verify the segment file by checksumming its contents, and
		// comparing it to the accompanying ".CHECKSUM" file.
		if err := ds.verifySegment(name, chksums[i]); err != nil {
			return errors.Wrapf(err, "failed checksum for segment %s", name)
		}

		start, end, err := ds.parseOffsets(name)
		if err != nil {
			return errors.Wrap(err, "analyze")
		}
		ds.segments = append(ds.segments, [2]Offset{start, end})
		ds.segPaths = append(ds.segPaths, name)
	}
	return nil
}

func (ds *DirectorySink) verifySegment(segmentPath, chksumPath string) error {
	chksum, err := ds.loadChecksum(filepath.Join(ds.dir, chksumPath))
	if err != nil {
		return errors.Wrap(err, "load checksum")
	}

	calc := ds.newChecksum()
	f, err := os.Open(filepath.Join(ds.dir, segmentPath))
	if err != nil {
		return errors.Wrap(err, "open segment file")
	}
	defer f.Close()
	if _, err := io.Copy(calc, f); err != nil {
		return errors.Wrap(err, "calculate checksum")
	}

	if got := calc.Sum(nil); !bytes.Equal(got, chksum) {
		return errors.Errorf("checksum mismatch (want=%v got=%v)",
			hex.EncodeToString(chksum),
			hex.EncodeToString(got),
		)
	}
	return nil
}

func (ds *DirectorySink) loadChecksum(name string) ([]byte, error) {
	src, err := ioutil.ReadFile(name)
	if err != nil {
		return nil, errors.Wrap(err, "read checksum file")
	}
	dst := make([]byte, hex.DecodedLen(len(src)))
	if _, err := hex.Decode(dst, src); err != nil {
		return nil, errors.Wrap(err, "decode checksum")
	}
	return dst, nil
}

// parseOffsets parses a segment file's offset boundaries from its filename.
func (ds *DirectorySink) parseOffsets(name string) (start, end Offset, err error) {
	sep := strings.Index(name, "-")
	if sep == -1 {
		return ZeroOffset, ZeroOffset, errors.Errorf("no separator in filename: %s", filepath.Join(ds.dir, name))
	}

	start, err = ParseOffset(name[:sep])
	if err != nil {
		return ZeroOffset, ZeroOffset, errors.Wrap(err, "parse starting offset")
	}

	end, err = ParseOffset(name[sep+1:])
	if err != nil {
		return ZeroOffset, ZeroOffset, errors.Wrap(err, "parse ending offset")
	}

	return start, end, nil
}

// reset effectively removes all stateful information from a DirectorySink.
func (ds *DirectorySink) reset() {
	ds.segments = [][2]Offset{}
	ds.segPaths = []string{}
}

// findFiles walks the sink's working directory, looking for segment files, and
// checksum files.
//
// This method does not descend into child directories.
func (ds *DirectorySink) findFiles() (segments, checksums []string, err error) {
	segments = []string{}
	checksums = []string{}
	if err := filepath.Walk(ds.dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return errors.Wrap(err, "walk dir")
		}
		if path == ds.dir {
			return nil
		}
		if info.IsDir() {
			return filepath.SkipDir
		}

		name := filepath.Base(path)

		// Is it a checksum file?
		if ok, err := filepath.Match("*.CHECKSUM", name); err != nil {
			return errors.Wrap(err, "match checksum pattern")
		} else if ok {
			checksums = append(checksums, name)
			return nil
		}

		// Is it a segment file?
		if ok, err := filepath.Match("*\\-*", name); err != nil {
			return errors.Wrap(err, "match segment pattern")
		} else if ok {
			segments = append(segments, name)
		}

		return nil
	}); err != nil {
		return nil, nil, err
	}
	return segments, checksums, nil
}

// LoadSegment implements the SegmentLoader interface.
func (ds *DirectorySink) LoadSegment(offset Offset) (*Segment, error) {
	ds.mu.RLock()
	defer ds.mu.RUnlock()

	if offset.Equal(ZeroOffset) {
		if len(ds.segPaths) == 0 {
			return nil, errors.New("no segments to load")
		}
		return ds.loadSegment(ds.segPaths[0])
	}

	for i, offs := range ds.segments {
		if offset.Within(offs[0], offs[1]) {
			return ds.loadSegment(ds.segPaths[i])
		}
	}

	return nil, io.EOF
}

func (ds *DirectorySink) loadSegment(name string) (*Segment, error) {
	f, err := os.Open(filepath.Join(ds.dir, name))
	if err != nil {
		return nil, errors.Wrap(err, "open segment file")
	}
	defer f.Close()

	seg := new(Segment)
	if _, err := seg.ReadFrom(f); err != nil {
		return nil, errors.Wrap(err, "load segment")
	}
	return seg, nil
}

// WriteSegment implements the SegmentWriter interface.
//
// It will write each data segment out to a file, along with a second
// file with a .CHECKSUM extension.
func (ds *DirectorySink) WriteSegment(seg *Segment) error {
	start, end := seg.Limits()
	if start == ZeroOffset && end == ZeroOffset {
		return nil
	}
	if err := ds.writeSegment(seg); err != nil {
		return err
	}
	ds.mu.Lock()
	ds.segments = append(ds.segments, [2]Offset{start, end})
	ds.segPaths = append(ds.segPaths, fmtSegFileName(seg))
	ds.mu.Unlock()
	return nil
}

func fmtSegFileName(seg *Segment) string {
	start, end := seg.Limits()
	return start.String() + "-" + end.String()
}

func (ds *DirectorySink) writeSegment(seg *Segment) error {
	name := filepath.Join(ds.dir, fmtSegFileName(seg))
	f, err := os.Create(name)
	if err != nil {
		return errors.Wrap(err, "create segment file")
	}
	defer f.Close()

	// Initialize the hash.Hash to be used for calculating a checksum.
	chksum := ds.newChecksum()

	mw := io.MultiWriter(f, chksum)
	if _, err := seg.WriteTo(mw); err != nil {
		return errors.Wrap(err, "write segment")
	}

	if err := ds.writeChecksum(name, chksum); err != nil {
		return errors.Wrap(err, "write checksum")
	}

	return nil
}

func (ds *DirectorySink) newChecksum() hash.Hash {
	return crc64.New(crc64.MakeTable(crc64.ISO))
}

func (ds *DirectorySink) writeChecksum(segmentName string, chksum hash.Hash) error {
	f, err := os.Create(segmentName + ".CHECKSUM")
	if err != nil {
		return errors.Wrap(err, "create checksum file")
	}
	defer f.Close()
	if _, err := io.WriteString(f, hex.EncodeToString(chksum.Sum(nil))); err != nil {
		return errors.Wrap(err, "write checksum")
	}
	return nil
}

// Close implements the io.Closer interface.
//
// In this particular Sink implementation, Close does nothing, as a
// DirectorySink does not hold any open file descriptors beyond those
// when calling WriteSegment, or LoadSegment.
func (ds *DirectorySink) Close() error {
	return nil
}

// Offsets returns the oldest, and newest offsets known to the DirectorySink.
// Initially, the offsets would be gathered by calling the Sink's Analyze()
// method. After initialization, and analysis, the offset range is extended by
// each call to WriteSequence.
//
// Offsets implements the Sink interface.
func (ds *DirectorySink) Offsets() (oldest, newest Offset) {
	ds.mu.RLock()
	defer ds.mu.RUnlock()
	lastSeg := len(ds.segments) - 1
	return ds.segments[0][0], ds.segments[lastSeg][1]
}

// NumSegments implements the Sink interface by returning the number of
// data segments currently known to the sink.
func (ds *DirectorySink) NumSegments() int {
	ds.mu.RLock()
	defer ds.mu.RUnlock()
	return len(ds.segments)
}

// Truncate implements the Sink interface.
//
// Truncate will delete any on-disk segment files, along with their checksum
// files, if the last offset in the segment file is older than the given
// offset.
//
// Should the offset fall within the offsets of a segment file, the
// segment file will be truncated, re-written to disk, and its checksum
// re-calculated.
func (ds *DirectorySink) Truncate(offset Offset) error {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	// Find segments whose most-recent offset is older than the offset
	// passed to this function.
	removed := 0
	var err error
	for i, offsets := range ds.segments {
		// If the most-recent offset of the segment's boundiares is
		// older than the given offset, mark it for removal.
		if offsets[1].Before(offset) {
			// If we encounter an error while deleting a segment
			// file, keep the error, but break out of this loop,
			// so that we fall through to remove any references to
			// segments that we were able to delete.
			if err = ds.deleteSegmentFile(ds.segPaths[i]); err != nil {
				break
			}
			removed++
		} else {
			// Break early so as to not waste cycles iterating
			// through the rest of the segments.
			break
		}
	}

	// Drop the segment offsets and paths from the sink.
	if removed > 0 {
		ds.segments = ds.segments[removed:]
		ds.segPaths = ds.segPaths[removed:]
	}

	// Check to see if there was an error left over from deleting segment
	// files; return if there was.
	if err != nil {
		return errors.Wrap(err, "delete segment file")
	}

	// Of the remaining segments, see if our offset falls within the
	// boundaries of the (new) first segment.
	//
	// If it does, then load the segment, truncate it, write it
	// back out to disk, and adjust the values in the segments and
	// segPaths slices.
	if ds.segments[0][0].Before(offset) && ds.segments[0][1].After(offset) {
		seg, err := ds.loadSegment(ds.segPaths[0])
		if err != nil {
			return errors.Wrap(err, "truncate segment")
		}
		seg.Truncate(offset)
		if err := ds.writeSegment(seg); err != nil {
			return errors.Wrap(err, "write truncated segment")
		}
		start, _ := seg.Limits()
		ds.segments[0][0] = start
		ds.segPaths[0] = fmtSegFileName(seg)
	}

	return nil
}

func (ds *DirectorySink) deleteSegmentFile(name string) error {
	name = filepath.Join(ds.dir, name)
	if err := os.Remove(name); err != nil {
		return errors.Wrap(err, "rm")
	}
	if err := os.Remove(name + ".CHECKSUM"); err != nil {
		return errors.Wrap(err, "rm checksum")
	}
	return nil
}
