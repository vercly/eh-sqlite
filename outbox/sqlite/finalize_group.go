package sqlite

import (
	"context"
	"fmt"
	"sync"

	dl "github.com/vercly/eh-sqlite/deadletter"
)

// maxFinalizeGroup bounds how many finalize outcomes share one transaction.
const maxFinalizeGroup = 64

// finalizeGroup group-commits first finalize attempts from concurrent workers.
// There is no background goroutine and no added wait: the first worker to
// arrive leads and commits everything queued at that moment in one write
// transaction; workers arriving meanwhile queue for the next group, whose
// leader is handed off from the current one. Each outcome runs in its own
// savepoint, so one failing outcome rolls back alone and its worker retries
// it on the regular per-delivery path; a failed commit fails the whole group
// the same way.
type finalizeGroup struct {
	mu      sync.Mutex
	queue   []*finalizeRequest
	leading bool
}

type finalizeRequest struct {
	doc     *deliveryDoc
	outcome FinalizeOutcome
	reason  string
	pending []dl.Record
	err     error
	// wake is signalled once: either the request is done, or (done false)
	// its worker must lead the next group.
	wake chan struct{}
	done bool
}

func (o *Outbox) finalizeGrouped(ctx context.Context, doc *deliveryDoc, outcome FinalizeOutcome, reason string) ([]dl.Record, error) {
	req := &finalizeRequest{doc: doc, outcome: outcome, reason: reason, wake: make(chan struct{}, 1)}
	g := &o.finalizer
	g.mu.Lock()
	g.queue = append(g.queue, req)
	lead := !g.leading
	g.leading = true
	g.mu.Unlock()

	// req fields are written by the leader of its group; a follower reads
	// them only after the wake that the leader sends once they are final.
	if !lead {
		<-req.wake
	}
	for !req.done {
		o.leadFinalizeGroup(ctx)
		if !req.done {
			<-req.wake
		}
	}
	return req.pending, req.err
}

// leadFinalizeGroup commits one group from the head of the queue, completes
// its requests and hands leadership to the next queued request, if any.
func (o *Outbox) leadFinalizeGroup(ctx context.Context) {
	g := &o.finalizer
	g.mu.Lock()
	n := min(len(g.queue), maxFinalizeGroup)
	group := append([]*finalizeRequest(nil), g.queue[:n]...)
	g.queue = g.queue[n:]
	g.mu.Unlock()

	o.commitFinalizeGroup(ctx, group)

	g.mu.Lock()
	var next *finalizeRequest
	if len(g.queue) > 0 {
		next = g.queue[0]
	} else {
		g.leading = false
	}
	g.mu.Unlock()

	for _, req := range group {
		req.done = true
		req.wake <- struct{}{}
	}
	if next != nil {
		next.wake <- struct{}{}
	}
}

func (o *Outbox) commitFinalizeGroup(ctx context.Context, group []*finalizeRequest) {
	if len(group) == 1 {
		req := group[0]
		req.pending, req.err = o.finalizeTx(ctx, req.doc, req.outcome, req.reason, 1)
		return
	}
	fail := func(err error) {
		for _, req := range group {
			if req.err == nil {
				req.pending, req.err = nil, err
			}
		}
	}
	tx, err := o.beginWriteTx(ctx)
	if err != nil {
		fail(fmt.Errorf("could not begin finalize transaction: %w", err))
		return
	}
	defer rollbackTx(tx)
	for _, req := range group {
		if _, err := tx.ExecContext(ctx, `SAVEPOINT finalize`); err != nil {
			fail(fmt.Errorf("could not open finalize savepoint: %w", err))
			return
		}
		req.pending, req.err = o.applyFinalizeTx(ctx, tx, req.doc, req.outcome, req.reason, 1)
		if req.err != nil {
			req.pending = nil
			if _, err := tx.ExecContext(ctx, `ROLLBACK TO finalize`); err != nil {
				fail(fmt.Errorf("could not roll back finalize savepoint: %w", err))
				return
			}
		}
		if _, err := tx.ExecContext(ctx, `RELEASE finalize`); err != nil {
			fail(fmt.Errorf("could not release finalize savepoint: %w", err))
			return
		}
	}
	if err := tx.Commit(); err != nil {
		err = fmt.Errorf("could not commit finalize transaction: %w", err)
		for _, req := range group {
			req.pending, req.err = nil, err
		}
	}
}
