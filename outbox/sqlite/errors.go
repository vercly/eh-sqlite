package sqlite

import (
	"errors"
	"sync/atomic"
	"time"
)

// ErrOutboxNotStarted is returned when publishing is attempted before Start.
var ErrOutboxNotStarted = errors.New("outbox not started")

// ErrOutboxAlreadyStarted is returned when AddHandler is called after Start has
// closed handler registration. Register all handlers, then call Start once.
var ErrOutboxAlreadyStarted = errors.New("outbox already started")

// ErrUnresolvedHandler is reported on Errors() (diagnostics only) when a
// delivery names a handler type that is not registered or no longer matches
// the event. The delivery stays visible with unresolved_at set and is never
// claimed or deleted implicitly.
var ErrUnresolvedHandler = errors.New("outbox: delivery handler cannot be resolved")

// ErrFinalizeStuck is reported on Errors() when finalize SQL and the safe claim
// release both failed. The delivery keeps taken_at and is recovered by the
// taken_at timeout (PeriodicSweepAge) once the database accepts writes again.
var ErrFinalizeStuck = errors.New("outbox: delivery finalize failed and claim could not be released")

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

var (
	errUnknownDispatchMode            = errors.New("unknown dispatch mode")
	errInvalidPartitionShards         = errors.New("partition shards must be >= 1")
	errInvalidQueueDepth              = errors.New("queue depth must be >= 1")
	errUnsupportedAvailableAtType     = errors.New("unsupported available_at value type")
	errEventHandlerMovedToDeadLetters = errors.New("event handler moved to dead letters")
)

// ErrInvalidQueueDepth is returned by WithQueueDepth when depth < 1.
var ErrInvalidQueueDepth = errInvalidQueueDepth

// ClaimSkipReason is the bounded label set for deliveries the fetcher saw but
// could not admit in a claim pass.
type ClaimSkipReason string

const (
	// SkipQueueFull: the dispatch key queue had no free slot.
	SkipQueueFull ClaimSkipReason = "queue_full"
	// SkipAdmissionFull: the global admission limit was reached.
	SkipAdmissionFull ClaimSkipReason = "admission_full"
	// SkipUnresolved: the delivery's handler is not registered or does not match.
	SkipUnresolved ClaimSkipReason = "unresolved_handler"
	// SkipRematchNoMatch: a rematch sentinel matched no registered handler.
	SkipRematchNoMatch ClaimSkipReason = "rematch_no_match"
	// SkipDecodeFailed: the stored event blob could not be decoded.
	SkipDecodeFailed ClaimSkipReason = "decode_failed"
)

// FinalizeOutcome is the bounded label set for per-delivery finalization.
type FinalizeOutcome string

const (
	FinalizeCompleted  FinalizeOutcome = "completed"
	FinalizeRetry      FinalizeOutcome = "retry"
	FinalizeDeadLetter FinalizeOutcome = "dead_letter"
	// FinalizeReleased: finalize SQL exhausted its retries; the claim was
	// released so the delivery becomes eligible again after PeriodicSweepAge.
	FinalizeReleased FinalizeOutcome = "released"
	// FinalizeStuck: release also failed; recovered by the taken_at timeout.
	FinalizeStuck FinalizeOutcome = "stuck"
)

// AdmissionStats is an optional, additive observer. A DispatchStats collector
// passed to WithDispatchStats that also implements AdmissionStats receives
// admission, claim-skip and finalize observations. Implementations must be
// safe for concurrent use, must not block and must not call back into the
// outbox. Labels are bounded (no ids, no keys).
type AdmissionStats interface {
	ObserveAdmission(used, limit int)
	ObserveClaimSkip(reason ClaimSkipReason)
	ObserveFinalize(outcome FinalizeOutcome, d time.Duration, retried bool, err error)
}

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
	case SeverityUnknown:
		unknownErrors.Add(1)
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
