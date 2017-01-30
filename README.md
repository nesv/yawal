# yawal

(Yet another) write-ahead logging package, for Go.

```
go get -u gopkg.in/nesv/yawal.v1
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

## Goals

- If it moves, document it.
- Do not rely on other, third-party packages.
- Use as many of the types, and interfaces, from the standard library, as
  possible (within reason).
- Be as fast as possible, without sacrificing safety.
- Do not clutter the core implementation; favour composition of components and
  functionality.
- Abstract layers as necessary.

