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

type FlushFunc func(ctx context.Context, actions []transform.Action, resumeToken []byte) error

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
// Backfill does not use the batcher — pages are written directly by the backfill loop.
type Batcher struct {
	config  config.GlobalConfig
	flushFn FlushFunc

	mu          sync.Mutex
	actions     map[string]transform.Action // keyed by DocumentID for dedup
	order       []string                    // insertion order of document IDs
	timer       *time.Timer
	resumeToken []byte // token of the most recent event added
	closed      bool

	ctx        context.Context // lifecycle context, used for timer-initiated flushes
	flushCh    chan flushItem   // pre-drained batches from Add/Flush/Close
	timerFired chan struct{}    // timer signals "time to flush"
	done       chan struct{}    // closed when flushLoop exits
}

func New(ctx context.Context, cfg config.GlobalConfig, flushFn FlushFunc) *Batcher {
	b := &Batcher{
		config:     cfg,
		flushFn:    flushFn,
		actions:    make(map[string]transform.Action),
		ctx:        ctx,
		flushCh:    make(chan flushItem, 2),
		timerFired: make(chan struct{}, 1),
		done:       make(chan struct{}),
	}
	go b.flushLoop()
	return b
}

// flushLoop is the single goroutine that executes all flushFn calls.
func (b *Batcher) flushLoop() {
	defer close(b.done)
	for {
		select {
		case <-b.timerFired:
			b.mu.Lock()
			pending := b.drainLocked()
			b.mu.Unlock()
			if pending != nil {
				if err := b.flushFn(b.ctx, pending.actions, pending.resumeToken); err != nil {
					slog.Error("timer flush failed", "error", err)
				}
			}

		case item, ok := <-b.flushCh:
			if !ok {
				return
			}
			err := b.flushFn(item.ctx, item.pending.actions, item.pending.resumeToken)
			if item.errCh != nil {
				item.errCh <- err
			}
		}
	}
}

// Add adds an action to the batch, deduplicating by document ID (latest wins).
func (b *Batcher) Add(ctx context.Context, action transform.Action, resumeToken []byte) error {
	if action.Type == transform.ActionSkip {
		return nil
	}

	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return fmt.Errorf("batcher is closed")
	}

	// Dedup: if this document ID is already in the batch, replace it.
	if _, exists := b.actions[action.DocumentID]; !exists {
		b.order = append(b.order, action.DocumentID)
	}
	b.actions[action.DocumentID] = action
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
	pending := b.drainLocked()
	b.mu.Unlock()

	if pending == nil {
		return nil
	}

	errCh := make(chan error, 1)
	b.flushCh <- flushItem{pending: pending, ctx: ctx, errCh: errCh}
	return <-errCh
}

// Close flushes remaining data, shuts down the flush goroutine, and waits
// for it to exit. After Close returns, Add and Flush will return errors.
func (b *Batcher) Close(ctx context.Context) error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil
	}
	b.closed = true
	pending := b.drainLocked()
	b.mu.Unlock()

	var flushErr error
	if pending != nil {
		errCh := make(chan error, 1)
		b.flushCh <- flushItem{pending: pending, ctx: ctx, errCh: errCh}
		flushErr = <-errCh
	}

	close(b.flushCh)
	<-b.done
	return flushErr
}

// drainLocked extracts the current batch and resets internal state.
// Must be called while holding b.mu.
func (b *Batcher) drainLocked() *pendingFlush {
	if len(b.actions) == 0 {
		return nil
	}

	batch := make([]transform.Action, 0, len(b.actions))
	for _, id := range b.order {
		if a, ok := b.actions[id]; ok {
			batch = append(batch, a)
		}
	}

	token := b.resumeToken
	b.actions = make(map[string]transform.Action)
	b.order = b.order[:0]
	b.resumeToken = nil

	if b.timer != nil {
		b.timer.Stop()
		b.timer = nil
	}

	return &pendingFlush{actions: batch, resumeToken: token}
}
