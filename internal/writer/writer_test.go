package writer

import (
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tmccann21/mongopuff/internal/mongo"
	"github.com/tmccann21/mongopuff/internal/transform"
)

func nsUpsert(namespace, docID string) transform.Action {
	return transform.Action{
		Type:        transform.ActionUpsert,
		Operation:   mongo.OpInsert,
		Namespace:   namespace,
		DocumentID:  docID,
		Attributes:  map[string]any{"name": docID},
		ClusterTime: 1,
	}
}

// docIDsByNamespace flattens recorded calls into namespace → sorted document IDs,
// failing if any call carries an action that belongs to a different namespace.
func docIDsByNamespace(t *testing.T, calls []writeBatchCall) map[string][]string {
	t.Helper()
	got := make(map[string][]string, len(calls))
	for _, call := range calls {
		for _, a := range call.actions {
			if a.Namespace != call.namespace {
				t.Errorf("call to %q carried action %s with Namespace %q", call.namespace, a.DocumentID, a.Namespace)
			}
			got[call.namespace] = append(got[call.namespace], a.DocumentID)
		}
	}
	for _, ids := range got {
		slices.Sort(ids)
	}
	return got
}

func dlqDocIDs(entries []mongo.DLQEntry) []string {
	ids := make([]string, 0, len(entries))
	for _, e := range entries {
		ids = append(ids, e.DocumentID)
	}
	slices.Sort(ids)
	return ids
}

func TestWriteBatch_OneCallPerNamespace(t *testing.T) {
	mock := &mockTpuf{}
	w := New(mock, noopDLQ{})

	// Interleaved so that grouping by adjacency, rather than by namespace,
	// would produce extra calls.
	actions := []transform.Action{
		nsUpsert("tenant-a", "1"),
		nsUpsert("tenant-b", "2"),
		nsUpsert("tenant-a", "3"),
		nsUpsert("tenant-c", "4"),
		nsUpsert("tenant-b", "5"),
	}

	if err := w.WriteBatch(context.Background(), nil, actions); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}

	calls := mock.getCalls()
	if len(calls) != 3 {
		t.Fatalf("got %d turbopuffer calls, want 3 (one per namespace)", len(calls))
	}

	got := docIDsByNamespace(t, calls)
	want := map[string][]string{
		"tenant-a": {"1", "3"},
		"tenant-b": {"2", "5"},
		"tenant-c": {"4"},
	}
	for ns, ids := range want {
		if !slices.Equal(got[ns], ids) {
			t.Errorf("namespace %q: got doc IDs %v, want %v", ns, got[ns], ids)
		}
	}
}

func TestWriteBatch_NamespaceTransitionWritesBothNamespaces(t *testing.T) {
	mock := &mockTpuf{}
	w := New(mock, noopDLQ{})

	// The two actions MapChangeEvent emits when an update moves a document
	// between namespaces. They share a DocumentID.
	actions := []transform.Action{
		{Type: transform.ActionDelete, Operation: mongo.OpUpdate, Namespace: "tenant-old", DocumentID: "doc-1", ClusterTime: 7},
		{Type: transform.ActionUpsert, Operation: mongo.OpUpdate, Namespace: "tenant-new", DocumentID: "doc-1", ClusterTime: 7},
	}

	if err := w.WriteBatch(context.Background(), nil, actions); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}

	calls := mock.getCalls()
	if len(calls) != 2 {
		t.Fatalf("got %d turbopuffer calls, want 2", len(calls))
	}

	wantType := map[string]transform.ActionType{
		"tenant-old": transform.ActionDelete,
		"tenant-new": transform.ActionUpsert,
	}
	for _, call := range calls {
		want, ok := wantType[call.namespace]
		if !ok {
			t.Errorf("unexpected call to namespace %q", call.namespace)
			continue
		}
		if len(call.actions) != 1 || call.actions[0].Type != want {
			t.Errorf("namespace %q: got %+v, want a single action of type %d", call.namespace, call.actions, want)
		}
		delete(wantType, call.namespace)
	}
	for ns := range wantType {
		t.Errorf("no call to namespace %q", ns)
	}
}

func TestWriteBatch_WritesPartitionsConcurrently(t *testing.T) {
	const partitions = 3

	// Each write waits until every partition has started. Written serially,
	// the first write never sees the others arrive and times out instead.
	var started atomic.Int32
	allStarted := make(chan struct{})
	var serial atomic.Bool

	mock := &mockTpuf{
		hook: func(_ context.Context, _ string) error {
			if started.Add(1) == partitions {
				close(allStarted)
			}
			select {
			case <-allStarted:
			case <-time.After(time.Second):
				serial.Store(true)
			}
			return nil
		},
	}
	w := New(mock, noopDLQ{})

	actions := []transform.Action{
		nsUpsert("tenant-a", "1"),
		nsUpsert("tenant-b", "2"),
		nsUpsert("tenant-c", "3"),
	}

	if err := w.WriteBatch(context.Background(), nil, actions); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}
	if serial.Load() {
		t.Fatal("partitions were not written concurrently")
	}
}

func TestWriteBatch_FailedPartitionGoesToDLQ(t *testing.T) {
	mock := &mockTpuf{
		hook: func(_ context.Context, namespace string) error {
			if namespace == "tenant-a" {
				return errors.New("turbopuffer rejected write")
			}
			return nil
		},
	}
	dlq := &recordingDLQ{}
	w := New(mock, dlq)

	actions := []transform.Action{
		nsUpsert("tenant-a", "a-1"),
		nsUpsert("tenant-b", "b-1"),
		nsUpsert("tenant-a", "a-2"),
	}

	// The failed partition was made durable via the DLQ, so the flush
	// succeeds and the caller is free to advance the resume token.
	if err := w.WriteBatch(context.Background(), nil, actions); err != nil {
		t.Fatalf("WriteBatch: %v, want nil when the failed partition was DLQ'd", err)
	}

	if got, want := dlqDocIDs(dlq.getEntries()), []string{"a-1", "a-2"}; !slices.Equal(got, want) {
		t.Errorf("DLQ doc IDs = %v, want %v (only the failed partition)", got, want)
	}

	got := docIDsByNamespace(t, mock.getCalls())
	if want := []string{"b-1"}; !slices.Equal(got["tenant-b"], want) {
		t.Errorf("tenant-b: got doc IDs %v, want %v", got["tenant-b"], want)
	}
}

func TestWriteBatch_UndeliverablePartitionFailsFlush(t *testing.T) {
	// tenant-b simulates a turbopuffer request that is still in flight when
	// tenant-a fails. A real client aborts on context cancellation, so if one
	// partition's failure cancels its siblings, tenant-b fails too and lands
	// in the DLQ even though its write would have succeeded.
	mock := &mockTpuf{
		hook: func(ctx context.Context, namespace string) error {
			switch namespace {
			case "tenant-a":
				return errors.New("turbopuffer unavailable")
			case "tenant-b":
				select {
				case <-time.After(100 * time.Millisecond):
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			return nil
		},
	}
	dlqErr := errors.New("dlq unavailable")
	dlq := &recordingDLQ{err: dlqErr}
	w := New(mock, dlq)

	actions := []transform.Action{
		nsUpsert("tenant-a", "a-1"),
		nsUpsert("tenant-b", "b-1"),
	}

	// tenant-a can be neither written nor DLQ'd. The flush must fail so the
	// caller does not save the resume token.
	err := w.WriteBatch(context.Background(), nil, actions)
	if !errors.Is(err, dlqErr) {
		t.Fatalf("WriteBatch error = %v, want it to wrap %v", err, dlqErr)
	}

	if got, want := dlqDocIDs(dlq.getEntries()), []string{"a-1"}; !slices.Equal(got, want) {
		t.Errorf("DLQ doc IDs = %v, want %v (tenant-b must not be affected by tenant-a's failure)", got, want)
	}
}

func TestWriteBatch_ReturnsOnlyAfterAllPartitionsFinish(t *testing.T) {
	// The caller saves the resume token as soon as WriteBatch returns, so
	// returning after only some partitions finish would advance the token past
	// writes that are still in flight.
	release := make(chan struct{})
	releaseSlow := sync.OnceFunc(func() { close(release) })
	defer releaseSlow()

	mock := &mockTpuf{
		hook: func(_ context.Context, namespace string) error {
			if namespace == "tenant-slow" {
				<-release
			}
			return nil
		},
	}
	w := New(mock, noopDLQ{})

	actions := []transform.Action{
		nsUpsert("tenant-fast", "1"),
		nsUpsert("tenant-slow", "2"),
	}

	done := make(chan error, 1)
	go func() {
		done <- w.WriteBatch(context.Background(), nil, actions)
	}()

	waitFor(t, func() bool {
		return slices.ContainsFunc(mock.getCalls(), func(c writeBatchCall) bool {
			return c.namespace == "tenant-fast"
		})
	}, 2*time.Second)

	select {
	case err := <-done:
		t.Fatalf("WriteBatch returned (err=%v) while tenant-slow was still writing", err)
	case <-time.After(50 * time.Millisecond):
	}

	releaseSlow()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("WriteBatch: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("WriteBatch did not return after all partitions finished")
	}
}
