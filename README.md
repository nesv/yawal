# yawal

(Yet another) write-ahead logging package, for Go.

[![Travis CI](https://img.shields.io/travis/nesv/yawal.svg?style=for-the-badge)](https://travis-ci.org/nesv/yawal)
[![Godoc](https://img.shields.io/badge/godoc-reference-blue.svg?&longCache=true&style=for-the-badge)](https://pkg.go.dev/github.com/nesv/yawal)

## Installing

```
go get -u github.com/nesv/yawal
```

## Why should I use this package?

I'm not saying you should. However, if you are looking for a fast, flexible
WAL package, then this should hopefully provide the initial groundwork for
what you need!

### Features

- Pluggable storage back-ends ("sinks");
- "Extra" functionality that isn't immediately crucial to the operation of
  the WAL is split out into a utilities pacakge (e.g. persisting WAL segments
  after a given time interval).

## Overall concepts

The main types for this package are:

- The `Logger`, which you write your data to.
- A `Sink`, for deciding where to persist your data.
- And a `Reader` for, well, reading ("replaying") your data.

For a step-by-step understanding of how this package works:

- You write your data to a _logger_. Each `[]byte` of data is herein referred
  to as a _chunk_.
- _Chunks_ have _offsets_; an _offset_ is nothing more than the timestamp at
  which the _chunk_ was written, precise to a nanosecond. An _offset_ is
  automatically added to a _chunk_ when it is written to a _logger_.
- _Chunks_ are stored in _segments_; a _segment_ is a size-bounded collection
  of _chunks_.
- When there isn't enough room in a _segment_ for another _chunk_, the _logger_
  passes the _segment_ along to a _sink_.
- _Sinks_ do most of the heavy lifting in this package; they handle the
  writing, and reading, of _segments_ to/from a persistent storage medium.

## Examples

### Create a new disk-backed log

```go
package main

import (
	"log"

	wal "gopkg.in/nesv/yawal.v1"
)

func main() {
	// Create a new DirectorySink.
	sink, err := wal.NewDirectorySink("wal")
	if err != nil {
		log.Fatalln(err)
	}
	
	// Create a new logger that will store data in 1MB segment files.
	logger, err := wal.New(sink, wal.SegmentSize(1024 * 1024))
	if err != nil {
		log.Fatalln(err)
	}

	// Note, calling a *wal.Logger's Close() method will also close the
	// underlying Sink.
	defer logger.Close()

	// Write data to your logger.
	for i := 0; i < 100; i++ {
		if err := logger.Write([]byte("Wooo, data!")); err != nil {
			log.Println("error:", err)
			return
		}
	}

	return
}
```

### Create an in-memory log

```go
sink, err := wal.NewMemorySink()
if err != nil {
	log.Fatalln(err)
}

logger, err := wal.New(sink, wal.SegmentSize(1024*1024))
if err != nil {
	log.Fatalln(err)
}
defer logger.Close()

// ...write data...
```

### Read data from an existing log

```go
sink, err := wal.NewDirectorySink("wal")
if err != nil {
	log.Fatalln(err)
}
defer sink.Close()

r := wal.NewReader(sink)
for r.Next() {
	data := r.Data()
	offset := r.Offset()

	fmt.Printf("Data at offset %s: %x\n", offset, data)
}
if err := r.Error(); err != nil {
	log.Println("error reading from wal:", err)
}
```

## Goals

- If it moves, document it.
- Do not rely on other, third-party packages.
- Use as many of the types, and interfaces, from the standard library, as
  possible (within reason).
- Be as fast as possible, without sacrificing safety.
- Do not clutter the core implementation; favour composition of components and
  functionality.
- Abstract layers as necessary.

