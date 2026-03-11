package sqlite

import "errors"

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

// GetSeverity unpacks the error chain to check if a specific ErrorSeverity
// has been assigned, returning SeverityUnknown if no compliant error is found.
func GetSeverity(err error) ErrorSeverity {
	if ce, ok := AsType[CategorizedError](err); ok {
		return ce.OutboxSeverity()
	}
	// Defaults to unknown (treated as retryable based on legacy design constraints)
	return SeverityUnknown
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
