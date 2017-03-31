package wal

// Option is a functional configuration type that can be used to configure
// the behaviour of a *Logger.
type Option func(*Logger) error

// SegmentSize sets the size of a data segment.
//
// Depending on the Sink provided to the *Logger, setting n too low may cause
// excessive amounts of I/O, thus slowing everything down. Another potential
// problem is attempting to write data, where len(data) > n.
func SegmentSize(n uint64) Option {
	return func(l *Logger) error {
		l.segSize = n
		return nil
	}
}
