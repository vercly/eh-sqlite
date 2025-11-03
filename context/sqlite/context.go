// Package sqlite transaction context propagation
package sqlite

import (
	"context"
	"database/sql"
)

// txContextKey is an unexported type used as a key in context.Context,
// to avoid collisions with other packages.
type txContextKey struct{}

// NewContextWithTx creates a new context that contains an sql.Tx transaction.
func NewContextWithTx(ctx context.Context, tx *sql.Tx) context.Context {
	return context.WithValue(ctx, txContextKey{}, tx)
}

// TxFromContext retrieves the sql.Tx transaction from the context, if it exists.
func TxFromContext(ctx context.Context) (*sql.Tx, bool) {
	tx, ok := ctx.Value(txContextKey{}).(*sql.Tx)
	return tx, ok
}
