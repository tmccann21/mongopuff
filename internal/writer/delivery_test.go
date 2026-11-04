package writer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"slices"
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

	// hook, if set, runs at the start of every Write without holding mu. Its
	// error is returned in place of errs. Partition tests use it to fail, block,
	// or rendezvous specific namespaces, since the order of concurrent partition
	// writes is nondeterministic.
	hook func(ctx context.Context, namespace string) error
}

func (m *mockTpuf) Write(ctx context.Context, namespace string, schema map[string]tpuf.AttributeSchemaConfigParam, actions []transform.Action) error {
	var hookErr error
	if m.hook != nil {
		hookErr = m.hook(ctx, namespace)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, writeBatchCall{namespace: namespace, actions: actions})
	if hookErr != nil {
		return hookErr
	}
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

type recordingDLQ struct {
	mu      sync.Mutex
	entries []mongo.DLQEntry
	err     error // returned from every Write, after the entry is recorded
}

func (d *recordingDLQ) Write(_ context.Context, entry mongo.DLQEntry) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.entries = append(d.entries, entry)
	return d.err
}

func (d *recordingDLQ) getEntries() []mongo.DLQEntry {
	d.mu.Lock()
	defer d.mu.Unlock()
	dst := make([]mongo.DLQEntry, len(d.entries))
	copy(dst, d.entries)
	return dst
}

// --- helpers ---

// testCollection is the MongoDB collection the tests' actions come from. It
// differs from every test namespace, so a DLQ entry that records the namespace
// as its collection is caught.
const testCollection = "items"

// writeSegment appends actions to the spool as its next segment.
func writeSegment(t *testing.T, sp *spool.Spool, actions []transform.Action) {
	t.Helper()
	data, err := json.Marshal(actions)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}
	if _, err := sp.Append(data); err != nil {
		t.Fatalf("spool.Append: %v", err)
	}
}

func makeActions(docID string) []transform.Action {
	return []transform.Action{
		{
			Type:        transform.ActionUpsert,
			Operation:   mongo.OpInsert,
			Namespace:   "test-ns",
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

	writeSegment(t, sp, makeActions("doc-0"))
	writeSegment(t, sp, makeActions("doc-1"))
	writeSegment(t, sp, makeActions("doc-2"))

	mock := &mockTpuf{}
	w := New(mock, noopDLQ{})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	signal := make(chan struct{}, 1)

	done := make(chan struct{})
	go func() {
		w.RunSpoolDelivery(ctx, sp, testCollection, nil, 0, signal)
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
		w.RunSpoolDelivery(ctx, sp, testCollection, nil, 0, signal)
		close(done)
	}()

	// Give it a moment to enter the wait state.
	time.Sleep(50 * time.Millisecond)
	if mock.callCount() != 0 {
		t.Fatal("expected no calls before writing segment")
	}

	// Write a segment and signal.
	writeSegment(t, sp, makeActions("doc-signal"))
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
		w.RunSpoolDelivery(ctx, sp, testCollection, nil, 0, signal)
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

	writeSegment(t, sp, makeActions("doc-fail"))
	writeSegment(t, sp, makeActions("doc-ok"))

	mock := &mockTpuf{
		errs: []error{fmt.Errorf("transient turbopuffer error")},
	}
	w := New(mock, noopDLQ{})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	signal := make(chan struct{}, 1)

	done := make(chan struct{})
	go func() {
		w.RunSpoolDelivery(ctx, sp, testCollection, nil, 0, signal)
		close(done)
	}()

	waitFor(t, func() bool { return mock.callCount() >= 2 }, 2*time.Second)
	cancel()
	<-done

	if mock.callCount() != 2 {
		t.Fatalf("got %d WriteBatch calls, want 2", mock.callCount())
	}

	// The failed write went to the DLQ, so segment 0 is handled and removed.
	segs, err := sp.Segments()
	if err != nil {
		t.Fatalf("Segments: %v", err)
	}
	if len(segs) != 0 {
		t.Errorf("segments remaining: %v, want none", segs)
	}
}

// ctxDLQ fails every Write once ctx is done, as the MongoDB driver does.
type ctxDLQ struct{}

func (ctxDLQ) Write(ctx context.Context, _ mongo.DLQEntry) error { return ctx.Err() }

// assertSegmentsOnDisk fails unless every segment in want is still in the spool.
func assertSegmentsOnDisk(t *testing.T, sp *spool.Spool, want ...uint32) {
	t.Helper()
	segs, err := sp.Segments()
	if err != nil {
		t.Fatalf("Segments: %v", err)
	}
	for _, seg := range want {
		if !slices.Contains(segs, seg) {
			t.Errorf("segment %d was removed (segments on disk: %v)", seg, segs)
		}
	}
}

// startDelivery runs RunSpoolDelivery from segment 0 in a goroutine and
// returns a channel that receives its result.
func startDelivery(ctx context.Context, w *Writer, sp *spool.Spool) <-chan error {
	errCh := make(chan error, 1)
	go func() {
		errCh <- w.RunSpoolDelivery(ctx, sp, testCollection, nil, 0, make(chan struct{}, 1))
	}()
	return errCh
}

// awaitDelivery waits for RunSpoolDelivery to return and gives its result.
func awaitDelivery(t *testing.T, errCh <-chan error) error {
	t.Helper()
	select {
	case err := <-errCh:
		return err
	case <-time.After(2 * time.Second):
		t.Fatal("RunSpoolDelivery did not return")
		return nil
	}
}

// The resume token for a spooled segment is saved when the segment is written,
// so the segment file is the only copy of its events. Delivery may remove it
// only once it has been written to turbopuffer or the DLQ. When it cannot,
// delivery stops with an error and keeps the segment rather than moving on:
// deletes are unconditional, so delivering a newer delete before an older
// upsert would let the upsert bring a deleted document back.

func TestRunSpoolDelivery_StopsAtUndeliverableSegment(t *testing.T) {
	sp, err := spool.Open(t.TempDir())
	if err != nil {
		t.Fatalf("spool.Open: %v", err)
	}
	defer sp.Close()

	writeSegment(t, sp, makeActions("doc-fail"))
	writeSegment(t, sp, makeActions("doc-later"))

	mock := &mockTpuf{hook: func(context.Context, string) error {
		return errors.New("turbopuffer unavailable")
	}}
	dlqErr := errors.New("dlq unavailable")
	w := New(mock, &recordingDLQ{err: dlqErr})

	err = awaitDelivery(t, startDelivery(context.Background(), w, sp))
	if !errors.Is(err, dlqErr) {
		t.Errorf("RunSpoolDelivery error = %v, want it to wrap %v", err, dlqErr)
	}

	assertSegmentsOnDisk(t, sp, 0, 1)
	for _, call := range mock.getCalls() {
		if call.actions[0].DocumentID == "doc-later" {
			t.Error("delivered segment 1 before segment 0 was written or DLQ'd")
		}
	}
}

func TestRunSpoolDelivery_KeepsSegmentOnShutdown(t *testing.T) {
	// Shutdown cancels ctx while a delivery is in flight, which fails both the
	// turbopuffer write and the DLQ fallback. That segment must survive for the
	// next run to deliver, and shutting down is not an error.
	sp, err := spool.Open(t.TempDir())
	if err != nil {
		t.Fatalf("spool.Open: %v", err)
	}
	defer sp.Close()

	writeSegment(t, sp, makeActions("doc-0"))

	inFlight := make(chan struct{})
	var once sync.Once
	mock := &mockTpuf{hook: func(ctx context.Context, _ string) error {
		once.Do(func() { close(inFlight) })
		<-ctx.Done()
		return ctx.Err()
	}}
	w := New(mock, ctxDLQ{})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := startDelivery(ctx, w, sp)

	select {
	case <-inFlight:
	case <-time.After(2 * time.Second):
		t.Fatal("delivery never started")
	}
	cancel()

	if err := awaitDelivery(t, errCh); err != nil {
		t.Errorf("RunSpoolDelivery error = %v, want nil on shutdown", err)
	}
	assertSegmentsOnDisk(t, sp, 0)
}

func TestRunSpoolDelivery_StopsAtCorruptSegment(t *testing.T) {
	// A segment that cannot be decoded cannot be delivered, and skipping it
	// delivers later segments out of order and strands its events.
	sp, err := spool.Open(t.TempDir())
	if err != nil {
		t.Fatalf("spool.Open: %v", err)
	}
	defer sp.Close()

	if _, err := sp.Append([]byte("not json")); err != nil {
		t.Fatalf("spool.Append: %v", err)
	}
	writeSegment(t, sp, makeActions("doc-later"))

	mock := &mockTpuf{}
	w := New(mock, noopDLQ{})

	if err := awaitDelivery(t, startDelivery(context.Background(), w, sp)); err == nil {
		t.Error("RunSpoolDelivery returned nil, want an error for corrupt segment 0")
	}
	if n := mock.callCount(); n != 0 {
		t.Errorf("got %d turbopuffer calls, want 0: segment 1 was delivered past corrupt segment 0", n)
	}
	assertSegmentsOnDisk(t, sp, 0, 1)
}

func TestRunSpoolDelivery_StopsWhenRemoveFails(t *testing.T) {
	// A delivered segment that stays on disk is delivered again on the next
	// run. If delivery moved on, that redelivery would come after newer
	// segments.
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory permissions, so Remove cannot be made to fail")
	}

	dir := t.TempDir()
	sp, err := spool.Open(dir)
	if err != nil {
		t.Fatalf("spool.Open: %v", err)
	}
	defer sp.Close()

	writeSegment(t, sp, makeActions("doc-0"))

	// A read-only directory still allows reading segment 0 but not removing it.
	if err := os.Chmod(dir, 0o555); err != nil {
		t.Fatalf("Chmod: %v", err)
	}
	t.Cleanup(func() { os.Chmod(dir, 0o755) }) // let TempDir cleanup remove it

	mock := &mockTpuf{}
	w := New(mock, noopDLQ{})

	err = awaitDelivery(t, startDelivery(context.Background(), w, sp))
	if !errors.Is(err, os.ErrPermission) {
		t.Errorf("RunSpoolDelivery error = %v, want it to wrap os.ErrPermission", err)
	}
	if n := mock.callCount(); n != 1 {
		t.Errorf("got %d turbopuffer calls, want 1", n)
	}
	assertSegmentsOnDisk(t, sp, 0)
}

func TestRunSpoolDelivery_MultipleSegmentsBeforeSignal(t *testing.T) {
	sp, err := spool.Open(t.TempDir())
	if err != nil {
		t.Fatalf("spool.Open: %v", err)
	}
	defer sp.Close()

	writeSegment(t, sp, makeActions("doc-0"))
	writeSegment(t, sp, makeActions("doc-1"))
	writeSegment(t, sp, makeActions("doc-2"))

	mock := &mockTpuf{}
	w := New(mock, noopDLQ{})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	signal := make(chan struct{}, 1)
	// Send exactly one signal.
	signal <- struct{}{}

	done := make(chan struct{})
	go func() {
		w.RunSpoolDelivery(ctx, sp, testCollection, nil, 0, signal)
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
