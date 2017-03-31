package wal

import (
	"strconv"
	"time"

	"github.com/pkg/errors"
)

// Offset represents the offset of a data chunk within a write-ahead logger.
type Offset int64

// ZeroOffset holds the value of the oldest-possible offset within a
// write-ahead logger.
var ZeroOffset = Offset(0)

// NewOffset returns a new Offset for the current time.
// This is a shorthand for:
//
//	NewOffsetTime(time.Now())
//
func NewOffset() Offset {
	return NewOffsetTime(time.Now())
}

// NewOffsetTime returns a new Offset for the given time.Time.
func NewOffsetTime(t time.Time) Offset {
	return Offset(t.UnixNano())
}

// ParseOffset returns an offset parsed from s.
func ParseOffset(s string) (Offset, error) {
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return ZeroOffset, errors.Wrap(err, "parse offset")
	}
	return Offset(n), nil
}

// Before reports whether the offset o is older than b.
func (o Offset) Before(b Offset) bool {
	return time.Unix(0, int64(o)).Before(time.Unix(0, int64(b)))
}

// After reports whether the offset o is newer than b.
func (o Offset) After(b Offset) bool {
	return time.Unix(0, int64(o)).Before(time.Unix(0, int64(b)))
}

// Equal reports whether the offset o is the same as b.
func (o Offset) Equal(b Offset) bool {
	return time.Unix(0, int64(o)).Equal(time.Unix(0, int64(b)))
}

// Within reports whether a <= o <= b.
func (o Offset) Within(a, b Offset) bool {
	n := int64(o)
	return int64(a) <= n && n <= int64(b)
}

// String implements the fmt.Stringer interface, and provides a means for
// representing an offset that can be later parsed with ParseOffset.
func (o Offset) String() string {
	return strconv.FormatInt(int64(o), 10)
}
