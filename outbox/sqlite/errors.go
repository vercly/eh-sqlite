package sqlite

import (
	"errors"
	"sync/atomic"
)

// ErrOutboxNotStarted is returned when publishing is attempted before Start.
var ErrOutboxNotStarted = errors.New("outbox not started")

// ErrorSeverity defines how an outbox error should be treated
type ErrorSeverity int

const (
	SeverityUnknown ErrorSeverity = iota
	SeverityFatal
	SeverityRetryable
)

// CategorizedError an interface that outbox handlers should wrap their errors in
// if they want to override the default retry behaviour. By default, all errors
// evaluate to SeverityRetryable.
type CategorizedError interface {
	error
	OutboxSeverity() ErrorSeverity
}

// ErrorCounts contains process-local outbox error counters by severity.
type ErrorCounts struct {
	Fatal     int64
	Retryable int64
	Unknown   int64
}

var (
	fatalErrors     atomic.Int64
	retryableErrors atomic.Int64
	unknownErrors   atomic.Int64
)

// GetSeverity unpacks the error chain to check if a specific ErrorSeverity
// has been assigned, returning SeverityUnknown if no compliant error is found.
func GetSeverity(err error) ErrorSeverity {
	if ce, ok := AsType[CategorizedError](err); ok {
		return ce.OutboxSeverity()
	}
	// Defaults to unknown (treated as retryable based on legacy design constraints)
	return SeverityUnknown
}

// ErrorSnapshot returns process-local outbox error counters by severity.
func ErrorSnapshot() ErrorCounts {
	return ErrorCounts{
		Fatal:     fatalErrors.Load(),
		Retryable: retryableErrors.Load(),
		Unknown:   unknownErrors.Load(),
	}
}

func recordErrorSeverity(severity ErrorSeverity) {
	switch severity {
	case SeverityFatal:
		fatalErrors.Add(1)
	case SeverityRetryable:
		retryableErrors.Add(1)
	default:
		unknownErrors.Add(1)
	}
}

// AsType is a generic helper to safely unwrap errors without type assertions,
// matching the vercly project's conventions.
func AsType[T any](err error) (T, bool) {
	var target T
	if errors.As(err, &target) {
		return target, true
	}
	return target, false
}
