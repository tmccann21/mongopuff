package batch

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"github.com/tmccann21/mongopuff/internal/config"
	"github.com/tmccann21/mongopuff/internal/transform"
)

// FlushFunc is called when a batch is ready to be written. resumeToken is the
// token of the last event in the batch, to be persisted after a successful write.
// Partial failure handling (retry, DLQ) is the responsibility of the implementation.
type FlushFunc func(ctx context.Context, actions []transform.Action, resumeToken []byte) error

// pendingFlush holds a batch ready to be written, extracted under the lock.
type pendingFlush struct {
	actions     []transform.Action
	resumeToken []byte
}

// Batcher accumulates CDC actions and flushes when count, size, or time thresholds are hit.
// Backfill does not use the batcher — pages are written directly by the backfill loop.
type Batcher struct {
	config  config.GlobalConfig
	flushFn FlushFunc

	mu          sync.Mutex
	actions     map[string]transform.Action // keyed by DocumentID for dedup
	order       []string                    // insertion order of document IDs
	sizeBytes   int
	timer       *time.Timer
	resumeToken []byte // token of the most recent event added
}

func New(cfg config.GlobalConfig, flushFn FlushFunc) *Batcher {
	return &Batcher{
		config:  cfg,
		flushFn: flushFn,
		actions: make(map[string]transform.Action),
	}
}

// Add adds an action to the batch, deduplicating by document ID (latest wins).
// If a flush threshold is hit, the batch is flushed synchronously.
func (b *Batcher) Add(ctx context.Context, action transform.Action, resumeToken []byte) error {
	if action.Type == transform.ActionSkip {
		return nil
	}

	b.mu.Lock()

	// Dedup: if this document ID is already in the batch, replace it.
	if _, exists := b.actions[action.DocumentID]; !exists {
		b.order = append(b.order, action.DocumentID)
	}
	b.actions[action.DocumentID] = action
	b.sizeBytes += estimateSize(action)
	b.resumeToken = resumeToken

	// Start the flush timer when the first event enters an empty batch.
	if b.timer == nil {
		b.timer = time.AfterFunc(b.config.FlushInterval(), func() {
			if err := b.Flush(context.Background()); err != nil {
				slog.Error("timer flush failed", "error", err)
			}
		})
	}

	// Check count and size thresholds.
	var pending *pendingFlush
	if len(b.actions) >= b.config.BatchFlushCount || b.sizeBytes >= b.config.BatchFlushSize {
		pending = b.drainLocked()
	}

	b.mu.Unlock()

	if pending != nil {
		return b.flushFn(ctx, pending.actions, pending.resumeToken)
	}
	return nil
}

// Flush forces a flush of all pending actions.
func (b *Batcher) Flush(ctx context.Context) error {
	b.mu.Lock()
	pending := b.drainLocked()
	b.mu.Unlock()

	if pending == nil {
		return nil
	}
	return b.flushFn(ctx, pending.actions, pending.resumeToken)
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
	b.sizeBytes = 0
	b.resumeToken = nil

	if b.timer != nil {
		b.timer.Stop()
		b.timer = nil
	}

	return &pendingFlush{actions: batch, resumeToken: token}
}

func estimateSize(action transform.Action) int {
	switch action.Type {
	case transform.ActionDelete:
		return 50
	case transform.ActionUpsert, transform.ActionPatch:
		data, err := json.Marshal(action.Attributes)
		if err != nil {
			return 100
		}
		return len(data)
	default:
		return 0
	}
}
