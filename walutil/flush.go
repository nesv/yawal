package walutil

import (
	"time"

	wal "go.nesv.ca/yawal"
)

// FlushInterval creates a time.Timer to fire after the given time.Duration d,
// to call logger.Flush(). If logger.Flush() returns a non-nil error, the
// onError function is called, with the non-nil error as an argument.
//
// If the non-nil error returned from logger.Flush() is wal.ErrLoggerClosed,
// this function will exit. It is recommended to call this function in its own
// goroutine.
//
//	logger, err := wal.NewLogger(NewDirectorySink("/tmp/wal.d"))
//	if err != nil {
//		...
//	}
//
//	go FlushInterval(logger, 10*time.Second, func(err error) {
//		log.Println("error flushing wal:", err)
//	})
//
func FlushInterval(logger *wal.Logger, d time.Duration, onError func(error)) {
	timer := time.NewTimer(d)
	for range timer.C {
		if err := logger.Flush(); err != nil && err == wal.ErrLoggerClosed {
			break
		} else if err != nil {
			onError(err)
		}
		if !timer.Stop() {
			<-timer.C
		}
		timer.Reset(d)
	}
	if !timer.Stop() {
		<-timer.C
	}
}
