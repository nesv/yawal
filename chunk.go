package wal

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"strconv"

	"github.com/pkg/errors"
)

var (
	chunkOffsetSize = 8
	chunkSeparator  = byte(':')
)

type chunk []byte

func newChunk(data []byte) *chunk {
	return newChunkOffset(data, NewOffset())
}

func newChunkOffset(data []byte, o Offset) *chunk {
	// Create a chunk large enough to hold its offset + len(data).
	c := make(chunk, chunkOffsetSize+len(data))
	binary.LittleEndian.PutUint64(c[:chunkOffsetSize], uint64(o))
	copy(c[chunkOffsetSize:], data)
	return &c
}

// MarshalText implements the encoding.TextMarshaler interface, and is
// primarily used for encoding a data chunk before it is written to
// persistent storage.
func (c chunk) MarshalText() ([]byte, error) {
	// Convert the chunk's offset to a string, then write it out as-is,
	// followed by a separator ":".
	offset := []byte(strconv.FormatInt(int64(c.Offset()), 10))
	offset = append(offset, chunkSeparator)

	// Encode the data.
	enc := base64.RawStdEncoding
	data := make([]byte, enc.EncodedLen(len(c)-chunkOffsetSize))
	enc.Encode(data, c[chunkOffsetSize:])
	return append(offset, data...), nil
}

// UnmarshalText implements the encoding.TextUnmarshaler interface, and is
// primarily used for decoding a data chunk that has been read in from
// persistent storage.
func (c *chunk) UnmarshalText(p []byte) error {
	sep := bytes.Index(p, []byte{chunkSeparator})
	if sep == -1 {
		return errors.New("no chunk separator")
	}

	enc := base64.RawStdEncoding
	size := chunkOffsetSize + enc.DecodedLen(len(p[sep+1:]))
	*c = append([]byte{}, make([]byte, size)...)

	// Unmarshal the offset.
	off, err := strconv.ParseInt(string(p[:sep]), 10, 64)
	if err != nil {
		return errors.Wrap(err, "parse offset")
	}
	binary.LittleEndian.PutUint64((*c)[:chunkOffsetSize], uint64(off))

	// Decode the rest of the data.
	if _, err = enc.Decode((*c)[chunkOffsetSize:], p[sep+1:]); err != nil {
		return errors.Wrap(err, "unmarshal text")
	}

	return nil
}

func (c chunk) String() string {
	p, err := c.MarshalText()
	if err != nil {
		return ""
	}
	return string(p)
}

// Offset returns the chunk's offset.
func (c chunk) Offset() Offset {
	return Offset(binary.LittleEndian.Uint64(c[:chunkOffsetSize]))
}

func (c chunk) Data() []byte {
	return c[chunkOffsetSize:]
}
