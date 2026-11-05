package batch

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/tmccann21/mongopuff/internal/config"
	"github.com/tmccann21/mongopuff/internal/transform"
)

// FlushFunc writes a batch and then saves resumeToken. It returns an error
// only when the batch was not durably handled (written to turbopuffer or the
// DLQ, or to the spool), in which case resumeToken must not have been saved.
type FlushFunc func(ctx context.Context, actions []transform.Action, resumeToken []byte) error

type actionKey struct {
	namespace  string
	documentID string
}

// pendingFlush holds a batch ready to be written, extracted under the lock.
type pendingFlush struct {
	actions     []transform.Action
	resumeToken []byte
}

// flushItem is sent to the flush goroutine for serial execution.
type flushItem struct {
	pending *pendingFlush
	ctx     context.Context
	errCh   chan<- error // nil for fire-and-forget (timer flushes)
}

// Batcher accumulates CDC actions and flushes when count or time thresholds are hit.
// All flushFn calls are serialized through a single background goroutine.
// After any flush fails, no further flush runs and Add and Flush return the
// error, so the caller stops and replays from its last saved resume token.
// Backfill does not use the batcher — pages are written directly by the backfill loop.
type Batcher struct {
	config  config.GlobalConfig
	flushFn FlushFunc

	mu          sync.Mutex
	actions     map[actionKey]transform.Action // keyed by (Namespace, DocumentID) for dedup
	order       []actionKey                    // first-insertion order of keys; see drainLocked
	timer       *time.Timer
	resumeToken []byte // token of the most recent event added
	closed      bool
	err         error // first flush error; once set, no further flush runs. Written only by flushLoop.

	flushCh    chan flushItem // pre-drained batches from Add/Flush/Close
	timerFired chan struct{}  // timer signals "time to flush"
	done       chan struct{}  // closed when flushLoop exits
}

func New(ctx context.Context, cfg config.GlobalConfig, flushFn FlushFunc) *Batcher {
	b := &Batcher{
		config:     cfg,
		flushFn:    flushFn,
		actions:    make(map[actionKey]transform.Action),
		flushCh:    make(chan flushItem, 2),
		timerFired: make(chan struct{}, 1),
		done:       make(chan struct{}),
	}
	go b.flushLoop(ctx)
	return b
}

// flushLoop is the single goroutine that executes all flushFn calls. Timer
// flushes run on ctx. The loop runs until Close, even after ctx is done, so
// that Close can still flush.
func (b *Batcher) flushLoop(ctx context.Context) {
	defer close(b.done)
	for {
		var item flushItem
		select {
		case <-b.timerFired:
			// Once ctx is done, a timer flush would fail on it after draining
			// the batch, and Close would find nothing left to flush. Leave the
			// batch for Close, which flushes on a context of its own.
			if ctx.Err() != nil {
				continue
			}
			b.mu.Lock()
			pending := b.drainLocked()
			b.mu.Unlock()
			if pending == nil {
				continue
			}
			item = flushItem{pending: pending, ctx: ctx} // no errCh: nobody is waiting

		case it, ok := <-b.flushCh:
			if !ok {
				return
			}
			item = it
		}

		// After a failed flush, no later flush may run: it would save a resume
		// token past the failed batch's events.
		b.mu.Lock()
		err := b.err
		b.mu.Unlock()
		if err == nil {
			err = b.flushFn(item.ctx, item.pending.actions, item.pending.resumeToken)
			if err != nil {
				b.mu.Lock()
				b.err = err
				b.mu.Unlock()
			}
		}

		if item.errCh != nil {
			item.errCh <- err
		} else if err != nil {
			slog.Error("timer flush failed", "error", err)
		}
	}
}

// Add adds the actions produced by a single change event, deduplicating by
// (Namespace, DocumentID) with the latest action winning.
//
// All of an event's actions enter the batch together, under one lock, so they
// are always flushed together. Otherwise a flush could save resumeToken while
// part of its event is still pending, and a crash before the next flush would
// lose that part. As a consequence a count-triggered flush may exceed
// BatchFlushCount by up to len(actions)-1.
func (b *Batcher) Add(ctx context.Context, actions []transform.Action, resumeToken []byte) error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return fmt.Errorf("batcher is closed")
	}
	if b.err != nil {
		b.mu.Unlock()
		return fmt.Errorf("batcher stopped after a failed flush: %w", b.err)
	}

	if len(actions) == 0 {
		b.mu.Unlock()
		return nil
	}
	for _, action := range actions {
		key := actionKey{namespace: action.Namespace, documentID: action.DocumentID}
		if _, exists := b.actions[key]; !exists {
			b.order = append(b.order, key)
		}
		b.actions[key] = action
	}
	b.resumeToken = resumeToken

	// Start the flush timer when the first event enters an empty batch.
	if b.timer == nil {
		b.timer = time.AfterFunc(b.config.FlushInterval(), func() {
			select {
			case b.timerFired <- struct{}{}:
			default:
			}
		})
	}

	var pending *pendingFlush
	if len(b.actions) >= b.config.BatchFlushCount {
		pending = b.drainLocked()
	}

	b.mu.Unlock()

	if pending != nil {
		errCh := make(chan error, 1)
		b.flushCh <- flushItem{pending: pending, ctx: ctx, errCh: errCh}
		return <-errCh
	}
	return nil
}

// Flush forces a flush of all pending actions via the flush goroutine.
func (b *Batcher) Flush(ctx context.Context) error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return fmt.Errorf("batcher is closed")
	}
	if b.err != nil {
		b.mu.Unlock()
		return fmt.Errorf("batcher stopped after a failed flush: %w", b.err)
	}
	pending := b.drainLocked()
	b.mu.Unlock()

	if pending == nil {
		return nil
	}

	errCh := make(chan error, 1)
	b.flushCh <- flushItem{pending: pending, ctx: ctx, errCh: errCh}
	return <-errCh
}

// Close flushes remaining data on ctx, shuts down the flush goroutine, and
// waits for it to exit. After Close returns, Add and Flush will return errors.
// It returns the batcher's first flush error, including one from a timer flush
// that no Add or Flush reported. After a failed flush, Close flushes nothing:
// the caller replays from its last saved resume token.
func (b *Batcher) Close(ctx context.Context) error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil
	}
	b.closed = true
	pending := b.drainLocked()
	b.mu.Unlock()

	if pending != nil {
		errCh := make(chan error, 1)
		b.flushCh <- flushItem{pending: pending, ctx: ctx, errCh: errCh}
		<-errCh // also recorded in b.err, returned below
	}

	close(b.flushCh)
	<-b.done
	// flushLoop has exited, so b.err can no longer change.
	return b.err
}

// drainLocked extracts the current batch and resets internal state.
// Must be called while holding b.mu.
func (b *Batcher) drainLocked() *pendingFlush {
	if len(b.actions) == 0 {
		return nil
	}

	// Walk order rather than the map so batches come out in a predictable
	// order; map iteration is randomized. This is for predictability only.
	// Correctness doesn't depend on it, because dedup leaves at most one action
	// per (Namespace, DocumentID) in a batch.
	batch := make([]transform.Action, 0, len(b.actions))
	for _, key := range b.order {
		batch = append(batch, b.actions[key])
	}

	token := b.resumeToken
	b.actions = make(map[actionKey]transform.Action)
	b.order = b.order[:0]
	b.resumeToken = nil

	if b.timer != nil {
		b.timer.Stop()
		b.timer = nil
	}

	return &pendingFlush{actions: batch, resumeToken: token}
}
