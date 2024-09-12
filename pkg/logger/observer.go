package logger

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

type Logs interface {
	// Len returns the number of items in the collection.
	Len() int

	// All returns a copy of all the observed logs.
	All() []observer.LoggedEntry

	// TakeAll returns a copy of all the observed logs, and truncates the observed
	// slice.
	TakeAll() []observer.LoggedEntry
}

var _ Logs = (*observer.ObservedLogs)(nil)

// NewObserverLogger creates a new logger that logs to an observer and returns the logger and the observer.
func NewObserverLogger(level string) (Logger, Logs) {
	lvl, err := zap.ParseAtomicLevel(level)
	if err != nil {
		lvl = zap.NewAtomicLevelAt(zap.DebugLevel)
	}

	observerLogger, logs := observer.New(lvl)
	logger := &ZapLogger{
		Logger: zap.New(observerLogger),
	}

	return logger, logs
}
