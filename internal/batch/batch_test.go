package batch

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/tmccann21/mongopuff/internal/config"
	"github.com/tmccann21/mongopuff/internal/transform"
)

type flushSpy struct {
	mu    sync.Mutex
	calls []flushCall
	err   error
}

type flushCall struct {
	actions     []transform.Action
	resumeToken []byte
}

func (s *flushSpy) fn() FlushFunc {
	return func(ctx context.Context, actions []transform.Action, resumeToken []byte) error {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.calls = append(s.calls, flushCall{
			actions:     actions,
			resumeToken: resumeToken,
		})
		return s.err
	}
}

func (s *flushSpy) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.calls)
}

func (s *flushSpy) lastCall() flushCall {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls[len(s.calls)-1]
}

func (s *flushSpy) call(i int) flushCall {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls[i]
}

// testConfig returns an Effective() config with overridden thresholds.
func testConfig(count, intervalMs int) config.GlobalConfig {
	return config.GlobalConfig{
		BatchFlushCount:  count,
		BatchFlushTimeMs: intervalMs,
	}
}

// upsertAction builds an upsert action with the given ID and attributes.
func upsertAction(id string, attrs map[string]any) transform.Action {
	return transform.Action{
		Type:       transform.ActionUpsert,
		DocumentID: id,
		Attributes: attrs,
	}
}

// deleteAction builds a delete action with the given ID.
func deleteAction(id string) transform.Action {
	return transform.Action{
		Type:       transform.ActionDelete,
		DocumentID: id,
	}
}

// smallUpsert builds a small upsert (~20-30 bytes serialized).
func smallUpsert(id string) transform.Action {
	return upsertAction(id, map[string]any{"k": "v"})
}

func TestBatchFlushOnCount(t *testing.T) {
	spy := &flushSpy{}
	cfg := testConfig(3, 60000) // high time so only count triggers
	b := New(cfg, spy.fn())

	for i := range 3 {
		if err := b.Add(context.Background(),smallUpsert(fmt.Sprintf("id-%d", i)), []byte("tok")); err != nil {
			t.Fatalf("Add(%d): %v", i, err)
		}
	}

	if spy.callCount() != 1 {
		t.Fatalf("expected 1 flush call, got %d", spy.callCount())
	}
	if got := len(spy.lastCall().actions); got != 3 {
		t.Errorf("flushed %d actions, want 3", got)
	}
}

func TestBatchFlushOnInterval(t *testing.T) {
	spy := &flushSpy{}
	cfg := testConfig(9999, 50) // 50ms interval

	b := New(cfg, spy.fn())

	if err := b.Add(context.Background(),smallUpsert("a"), []byte("tok")); err != nil {
		t.Fatal(err)
	}

	// Wait for the timer to fire.
	time.Sleep(200 * time.Millisecond)

	if spy.callCount() != 1 {
		t.Fatalf("expected 1 flush after interval, got %d", spy.callCount())
	}
	if got := len(spy.lastCall().actions); got != 1 {
		t.Errorf("flushed %d actions, want 1", got)
	}
}

func TestBatchDedupSameID(t *testing.T) {
	spy := &flushSpy{}
	cfg := testConfig(9999, 60000)
	b := New(cfg, spy.fn())

	if err := b.Add(context.Background(),upsertAction("a", map[string]any{"v": 1}), []byte("t1")); err != nil {
		t.Fatal(err)
	}
	if err := b.Add(context.Background(),upsertAction("a", map[string]any{"v": 2}), []byte("t2")); err != nil {
		t.Fatal(err)
	}
	if err := b.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}

	if spy.callCount() != 1 {
		t.Fatalf("expected 1 flush, got %d", spy.callCount())
	}
	actions := spy.lastCall().actions
	if len(actions) != 1 {
		t.Fatalf("expected 1 deduped action, got %d", len(actions))
	}
	if actions[0].Attributes["v"] != 2 {
		t.Errorf("expected latest value 2, got %v", actions[0].Attributes["v"])
	}
}

func TestBatchDedupInsertThenDelete(t *testing.T) {
	spy := &flushSpy{}
	cfg := testConfig(9999, 60000)
	b := New(cfg, spy.fn())

	if err := b.Add(context.Background(),upsertAction("a", map[string]any{"v": 1}), []byte("t1")); err != nil {
		t.Fatal(err)
	}
	if err := b.Add(context.Background(),deleteAction("a"), []byte("t2")); err != nil {
		t.Fatal(err)
	}
	if err := b.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}

	actions := spy.lastCall().actions
	if len(actions) != 1 {
		t.Fatalf("expected 1 deduped action, got %d", len(actions))
	}
	if actions[0].Type != transform.ActionDelete {
		t.Errorf("expected ActionDelete, got %d", actions[0].Type)
	}
}

func TestBatchEmptyNoFlush(t *testing.T) {
	spy := &flushSpy{}
	cfg := testConfig(9999, 60000)
	b := New(cfg, spy.fn())

	if err := b.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if spy.callCount() != 0 {
		t.Errorf("expected 0 flush calls on empty batch, got %d", spy.callCount())
	}
}

func TestBatchSkipActionIgnored(t *testing.T) {
	spy := &flushSpy{}
	// Count threshold of 2: if skip leaks in, adding the upsert would make count=2 and trigger a flush.
	cfg := testConfig(2, 60000)
	b := New(cfg, spy.fn())

	skip := transform.Action{
		Type:       transform.ActionSkip,
		DocumentID: "skip-me",
		Attributes: map[string]any{"big": "data"},
	}
	if err := b.Add(context.Background(), skip, []byte("t1")); err != nil {
		t.Fatal(err)
	}

	if err := b.Add(context.Background(), smallUpsert("a"), []byte("t2")); err != nil {
		t.Fatal(err)
	}
	if spy.callCount() != 0 {
		t.Fatal("skip action should not have contributed to batch count")
	}

	if err := b.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if spy.callCount() != 1 {
		t.Fatalf("expected 1 flush, got %d", spy.callCount())
	}
	actions := spy.lastCall().actions
	if len(actions) != 1 {
		t.Fatalf("expected 1 action (upsert only), got %d", len(actions))
	}
	if actions[0].DocumentID != "a" {
		t.Errorf("expected doc ID %q, got %q", "a", actions[0].DocumentID)
	}
}

func TestBatchPreservesInsertionOrder(t *testing.T) {
	spy := &flushSpy{}
	cfg := testConfig(9999, 60000)
	b := New(cfg, spy.fn())

	for _, id := range []string{"c", "a", "b"} {
		if err := b.Add(context.Background(),smallUpsert(id), []byte("tok")); err != nil {
			t.Fatal(err)
		}
	}
	if err := b.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}

	actions := spy.lastCall().actions
	want := []string{"c", "a", "b"}
	if len(actions) != len(want) {
		t.Fatalf("got %d actions, want %d", len(actions), len(want))
	}
	for i, a := range actions {
		if a.DocumentID != want[i] {
			t.Errorf("action[%d] = %q, want %q", i, a.DocumentID, want[i])
		}
	}
}

func TestBatchResumeTokenTracksLatest(t *testing.T) {
	spy := &flushSpy{}
	cfg := testConfig(9999, 60000)
	b := New(cfg, spy.fn())

	for i := range 3 {
		tok := []byte(fmt.Sprintf("token-%d", i))
		if err := b.Add(context.Background(),smallUpsert(fmt.Sprintf("id-%d", i)), tok); err != nil {
			t.Fatal(err)
		}
	}
	if err := b.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}

	got := string(spy.lastCall().resumeToken)
	if got != "token-2" {
		t.Errorf("resume token = %q, want %q", got, "token-2")
	}
}

func TestBatchFlushErrorPropagated(t *testing.T) {
	flushErr := errors.New("write failed")
	spy := &flushSpy{err: flushErr}
	cfg := testConfig(1, 60000) // flush on every add

	b := New(cfg, spy.fn())

	err := b.Add(context.Background(),smallUpsert("a"), []byte("tok"))
	if !errors.Is(err, flushErr) {
		t.Errorf("expected flush error, got %v", err)
	}
}

func TestBatchReusableAfterFlush(t *testing.T) {
	spy := &flushSpy{}
	cfg := testConfig(2, 60000) // flush every 2

	b := New(cfg, spy.fn())

	// First batch: 2 actions → auto flush.
	for i := range 2 {
		if err := b.Add(context.Background(),smallUpsert(fmt.Sprintf("a-%d", i)), []byte("t1")); err != nil {
			t.Fatal(err)
		}
	}
	if spy.callCount() != 1 {
		t.Fatalf("expected 1 flush after first batch, got %d", spy.callCount())
	}
	if got := len(spy.call(0).actions); got != 2 {
		t.Errorf("first flush: got %d actions, want 2", got)
	}

	// Second batch: 1 action, manual flush.
	if err := b.Add(context.Background(),smallUpsert("b-0"), []byte("t2")); err != nil {
		t.Fatal(err)
	}
	if err := b.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if spy.callCount() != 2 {
		t.Fatalf("expected 2 total flushes, got %d", spy.callCount())
	}
	if got := len(spy.call(1).actions); got != 1 {
		t.Errorf("second flush: got %d actions, want 1", got)
	}
}

