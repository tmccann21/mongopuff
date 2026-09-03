package writer

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/tmccann21/mongopuff/internal/mongo"
	"github.com/tmccann21/mongopuff/internal/spool"
	"github.com/tmccann21/mongopuff/internal/transform"
	tpuf "github.com/turbopuffer/turbopuffer-go/v2"
)

// --- mocks ---

type writeBatchCall struct {
	namespace string
	actions   []transform.Action
}

type mockTpuf struct {
	mu    sync.Mutex
	calls []writeBatchCall
	errs  []error // if non-nil, shift and return; otherwise nil
}

func (m *mockTpuf) Write(ctx context.Context, namespace string, schema map[string]tpuf.AttributeSchemaConfigParam, actions []transform.Action) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, writeBatchCall{namespace: namespace, actions: actions})
	if len(m.errs) > 0 {
		err := m.errs[0]
		m.errs = m.errs[1:]
		return err
	}
	return nil
}

func (m *mockTpuf) callCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.calls)
}

func (m *mockTpuf) getCalls() []writeBatchCall {
	m.mu.Lock()
	defer m.mu.Unlock()
	dst := make([]writeBatchCall, len(m.calls))
	copy(dst, m.calls)
	return dst
}

type noopDLQ struct{}

func (noopDLQ) Write(_ context.Context, _ mongo.DLQEntry) error { return nil }

// --- helpers ---

func writeSegment(t *testing.T, sp *spool.Spool, seg uint32, actions []transform.Action) {
	t.Helper()
	data, err := json.Marshal(actions)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}
	if err := sp.Write(seg, data); err != nil {
		t.Fatalf("spool.Write(%d): %v", seg, err)
	}
}

func makeActions(docID string) []transform.Action {
	return []transform.Action{
		{
			Type:        transform.ActionUpsert,
			Operation:   mongo.OpInsert,
			DocumentID:  docID,
			Attributes:  map[string]any{"name": docID},
			ClusterTime: 1,
		},
	}
}

func waitFor(t *testing.T, condition func() bool, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("timed out waiting for condition")
}

// --- tests ---

func TestRunSpoolDelivery_DeliversExistingSegments(t *testing.T) {
	sp, err := spool.Open(t.TempDir())
	if err != nil {
		t.Fatalf("spool.Open: %v", err)
	}
	defer sp.Close()

	writeSegment(t, sp, 0, makeActions("doc-0"))
	writeSegment(t, sp, 1, makeActions("doc-1"))
	writeSegment(t, sp, 2, makeActions("doc-2"))

	mock := &mockTpuf{}
	w := New(mock, noopDLQ{})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	signal := make(chan struct{}, 1)

	done := make(chan struct{})
	go func() {
		w.RunSpoolDelivery(ctx, sp, "test-ns", nil, 0, signal)
		close(done)
	}()

	// Wait for all 3 writes, then cancel.
	waitFor(t, func() bool { return mock.callCount() >= 3 }, 2*time.Second)
	cancel()
	<-done

	calls := mock.getCalls()
	if len(calls) != 3 {
		t.Fatalf("got %d WriteBatch calls, want 3", len(calls))
	}
	for i, call := range calls {
		wantID := fmt.Sprintf("doc-%d", i)
		if call.actions[0].DocumentID != wantID {
			t.Errorf("call %d: DocumentID = %q, want %q", i, call.actions[0].DocumentID, wantID)
		}
	}

	// Verify segment files are removed.
	segs, err := sp.Segments()
	if err != nil {
		t.Fatalf("Segments: %v", err)
	}
	if len(segs) != 0 {
		t.Errorf("segments remaining: %v, want none", segs)
	}
}

func TestRunSpoolDelivery_WaitsForSignal(t *testing.T) {
	sp, err := spool.Open(t.TempDir())
	if err != nil {
		t.Fatalf("spool.Open: %v", err)
	}
	defer sp.Close()

	mock := &mockTpuf{}
	w := New(mock, noopDLQ{})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	signal := make(chan struct{}, 1)

	done := make(chan struct{})
	go func() {
		w.RunSpoolDelivery(ctx, sp, "test-ns", nil, 0, signal)
		close(done)
	}()

	// Give it a moment to enter the wait state.
	time.Sleep(50 * time.Millisecond)
	if mock.callCount() != 0 {
		t.Fatal("expected no calls before writing segment")
	}

	// Write a segment and signal.
	writeSegment(t, sp, 0, makeActions("doc-signal"))
	signal <- struct{}{}

	waitFor(t, func() bool { return mock.callCount() >= 1 }, 2*time.Second)
	cancel()
	<-done

	calls := mock.getCalls()
	if len(calls) != 1 {
		t.Fatalf("got %d WriteBatch calls, want 1", len(calls))
	}
	if calls[0].actions[0].DocumentID != "doc-signal" {
		t.Errorf("DocumentID = %q, want %q", calls[0].actions[0].DocumentID, "doc-signal")
	}
}

func TestRunSpoolDelivery_StopsOnContextCancel(t *testing.T) {
	sp, err := spool.Open(t.TempDir())
	if err != nil {
		t.Fatalf("spool.Open: %v", err)
	}
	defer sp.Close()

	mock := &mockTpuf{}
	w := New(mock, noopDLQ{})

	ctx, cancel := context.WithCancel(context.Background())
	signal := make(chan struct{})

	done := make(chan struct{})
	go func() {
		w.RunSpoolDelivery(ctx, sp, "test-ns", nil, 0, signal)
		close(done)
	}()

	cancel()

	select {
	case <-done:
		// OK: returned promptly.
	case <-time.After(2 * time.Second):
		t.Fatal("RunSpoolDelivery did not return after context cancellation")
	}
}

func TestRunSpoolDelivery_ContinuesAfterWriteError(t *testing.T) {
	sp, err := spool.Open(t.TempDir())
	if err != nil {
		t.Fatalf("spool.Open: %v", err)
	}
	defer sp.Close()

	writeSegment(t, sp, 0, makeActions("doc-fail"))
	writeSegment(t, sp, 1, makeActions("doc-ok"))

	mock := &mockTpuf{
		errs: []error{fmt.Errorf("transient turbopuffer error")},
	}
	w := New(mock, noopDLQ{})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	signal := make(chan struct{}, 1)

	done := make(chan struct{})
	go func() {
		w.RunSpoolDelivery(ctx, sp, "test-ns", nil, 0, signal)
		close(done)
	}()

	waitFor(t, func() bool { return mock.callCount() >= 2 }, 2*time.Second)
	cancel()
	<-done

	if mock.callCount() != 2 {
		t.Fatalf("got %d WriteBatch calls, want 2", mock.callCount())
	}

	// Both segments should be removed regardless of error.
	segs, err := sp.Segments()
	if err != nil {
		t.Fatalf("Segments: %v", err)
	}
	if len(segs) != 0 {
		t.Errorf("segments remaining: %v, want none", segs)
	}
}

func TestRunSpoolDelivery_MultipleSegmentsBeforeSignal(t *testing.T) {
	sp, err := spool.Open(t.TempDir())
	if err != nil {
		t.Fatalf("spool.Open: %v", err)
	}
	defer sp.Close()

	writeSegment(t, sp, 0, makeActions("doc-0"))
	writeSegment(t, sp, 1, makeActions("doc-1"))
	writeSegment(t, sp, 2, makeActions("doc-2"))

	mock := &mockTpuf{}
	w := New(mock, noopDLQ{})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	signal := make(chan struct{}, 1)
	// Send exactly one signal.
	signal <- struct{}{}

	done := make(chan struct{})
	go func() {
		w.RunSpoolDelivery(ctx, sp, "test-ns", nil, 0, signal)
		close(done)
	}()

	waitFor(t, func() bool { return mock.callCount() >= 3 }, 2*time.Second)
	cancel()
	<-done

	if mock.callCount() != 3 {
		t.Fatalf("got %d WriteBatch calls, want 3 (should drain all available)", mock.callCount())
	}
}

func TestActionJSON_RoundTrip(t *testing.T) {
	original := []transform.Action{
		{
			Type:       transform.ActionUpsert,
			Operation:  mongo.OpInsert,
			DocumentID: "upsert-1",
			Attributes: map[string]any{
				"name":   "Alice",
				"score":  float64(99.5),
				"active": true,
				"tags":   nil,
			},
			ClusterTime: (1000 << 32) | 1, // 4294967296001
		},
		{
			Type:        transform.ActionPatch,
			Operation:   mongo.OpUpdate,
			DocumentID:  "patch-1",
			Attributes:  map[string]any{"count": float64(42)},
			ClusterTime: 100,
		},
		{
			Type:        transform.ActionDelete,
			Operation:   mongo.OpDelete,
			DocumentID:  "delete-1",
			Attributes:  nil,
			ClusterTime: 200,
		},
	}

	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	var decoded []transform.Action
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	if len(decoded) != len(original) {
		t.Fatalf("len(decoded) = %d, want %d", len(decoded), len(original))
	}

	t.Run("ActionType survives round-trip", func(t *testing.T) {
		for i, orig := range original {
			if decoded[i].Type != orig.Type {
				t.Errorf("action[%d].Type = %d, want %d", i, decoded[i].Type, orig.Type)
			}
		}
	})

	t.Run("ClusterTime uint64 precision", func(t *testing.T) {
		want := uint64((1000 << 32) | 1)
		got := decoded[0].ClusterTime
		if got != want {
			t.Errorf("ClusterTime = %d, want %d", got, want)
		}
	})

	t.Run("Attributes types preserved", func(t *testing.T) {
		attrs := decoded[0].Attributes

		if s, ok := attrs["name"].(string); !ok || s != "Alice" {
			t.Errorf("name = %v (%T), want \"Alice\"", attrs["name"], attrs["name"])
		}
		if f, ok := attrs["score"].(float64); !ok || f != 99.5 {
			t.Errorf("score = %v (%T), want 99.5", attrs["score"], attrs["score"])
		}
		if b, ok := attrs["active"].(bool); !ok || b != true {
			t.Errorf("active = %v (%T), want true", attrs["active"], attrs["active"])
		}
		if attrs["tags"] != nil {
			t.Errorf("tags = %v, want nil", attrs["tags"])
		}
	})

	t.Run("Operation survives round-trip", func(t *testing.T) {
		for i, orig := range original {
			if decoded[i].Operation != orig.Operation {
				t.Errorf("action[%d].Operation = %q, want %q", i, decoded[i].Operation, orig.Operation)
			}
		}
	})

	t.Run("DocumentID survives round-trip", func(t *testing.T) {
		for i, orig := range original {
			if decoded[i].DocumentID != orig.DocumentID {
				t.Errorf("action[%d].DocumentID = %q, want %q", i, decoded[i].DocumentID, orig.DocumentID)
			}
		}
	})
}
