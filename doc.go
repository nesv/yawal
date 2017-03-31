// Package wal provides a WAL (write-ahead logging) facility.
//
// This package aims to provide the most-basic implementation of a write-ahead
// logger as possible. For additional functionality, please see the
// "wal/walutil" package.
//
// When writing to a Logger, the []byte (herein referred to as a "chunk", or
// "data chunk") is written to a "segment". When a segment is full, or there
// is not enough room left for a write to fully succeed, the segment is
// passed to a Sink's WriteSegment method, and the []byte is written to a new,
// empty segment.
//
// A Sink is a type that is capable of storing, and retrieving segments. This
// package provides a Sink implementation that persists segments to a local
// directory: DirectorySink. Sinks do most of the "heavy lifting" for this
// package.
//
// Unlike most WAL implementations, the Logger type does not directly expose a
// means of persisting segments at a regular time interval. This is
// intentional, and was separated out to keep the implementation of Logger as
// simple as possible. If you wish to have segments written at a specific
// time interval, see the documentation for the wal/walutil.FlushInterval
// function.
//
// This package also provides the means of replaying a log, without requiring
// the creation of a Logger. For more deatils, see the NewReader and
// NewReaderOffset functions.
package wal
