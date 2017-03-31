package wal

import (
	"bytes"
	"testing"
	"time"
)

func TestChunkMarshalUnmarshal(t *testing.T) {
	msg := []byte("hello")
	offsets := []Offset{
		ZeroOffset,
		NewOffset(),
		NewOffsetTime(time.Now().Add(-48 * time.Hour)),
	}

	for _, offset := range offsets {
		a := newChunkOffset(msg, offset)
		txt, err := a.MarshalText()
		if err != nil {
			t.Error(err)
		}
		t.Log("A", string(txt))

		b := new(chunk)
		if err := b.UnmarshalText(txt); err != nil {
			t.Error(err)
		}
		t.Log("B", b.String())

		if !bytes.Equal([]byte(*a), []byte(*b)) {
			t.Error("a and b are not equal")
		}
	}
}
