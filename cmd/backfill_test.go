package cmd

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/tmccann21/mongopuff/internal/config"
	"github.com/tmccann21/mongopuff/internal/mongo"
	"github.com/tmccann21/mongopuff/internal/transform"
	"github.com/tmccann21/mongopuff/internal/writer"
	tpuf "github.com/turbopuffer/turbopuffer-go/v2"
)

const backfillOpTime uint64 = 42

type tpufCall struct {
	namespace string
	actions   []transform.Action
}

// fakeTpuf records every Write. WriteBatch writes namespaces concurrently, so
// calls are guarded by mu.
type fakeTpuf struct {
	mu    sync.Mutex
	calls []tpufCall
}

func (f *fakeTpuf) Write(_ context.Context, namespace string, _ map[string]tpuf.AttributeSchemaConfigParam, actions []transform.Action) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, tpufCall{namespace: namespace, actions: actions})
	return nil
}

func (f *fakeTpuf) getCalls() []tpufCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.calls)
}

// recordingDLQ records the entries of each Write call. WriteBatch DLQs failed
// partitions concurrently, so calls are guarded by mu.
type recordingDLQ struct {
	mu    sync.Mutex
	calls [][]mongo.DLQEntry
	err   error // returned from every Write, after the call is recorded
}

func (d *recordingDLQ) Write(_ context.Context, entries ...mongo.DLQEntry) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.calls = append(d.calls, slices.Clone(entries))
	return d.err
}

// getCalls returns the entries of each Write call, in call order.
func (d *recordingDLQ) getCalls() [][]mongo.DLQEntry {
	d.mu.Lock()
	defer d.mu.Unlock()
	return slices.Clone(d.calls)
}

// getEntries returns the entries of every Write call, in call order.
func (d *recordingDLQ) getEntries() []mongo.DLQEntry {
	d.mu.Lock()
	defer d.mu.Unlock()
	return slices.Concat(d.calls...)
}

// scannedPage round-trips docs through BSON so their values have the Go types
// CollectionScanner.ScanPage yields from cursor.All.
func scannedPage(t *testing.T, docs ...bson.D) []map[string]any {
	t.Helper()
	page := make([]map[string]any, 0, len(docs))
	for _, d := range docs {
		raw, err := bson.Marshal(d)
		if err != nil {
			t.Fatalf("bson.Marshal: %v", err)
		}
		var m map[string]any
		if err := bson.Unmarshal(raw, &m); err != nil {
			t.Fatalf("bson.Unmarshal: %v", err)
		}
		page = append(page, m)
	}
	return page
}

func tenantDoc(id, tenant string) bson.D {
	return bson.D{{Key: "_id", Value: id}, {Key: "tenant", Value: tenant}, {Key: "name", Value: id}}
}

func backfillCollConfig() config.CollectionConfig {
	coll := collConfig("items", "items_{{tenant}}")
	coll.Mapping.Fields = []config.FieldMapping{{Name: "name", Type: config.FieldTypeString}}
	return coll
}

// staticDoc has no tenant field: a static namespace needs no field to resolve.
func staticDoc(id string) bson.D {
	return bson.D{{Key: "_id", Value: id}, {Key: "name", Value: id}}
}

// staticCollConfig's namespace differs from its collection name, so an action
// that carries the collection name instead of the namespace is caught.
func staticCollConfig() config.CollectionConfig {
	coll := collConfig("users", "user_profiles")
	coll.Mapping.Fields = []config.FieldMapping{{Name: "name", Type: config.FieldTypeString}}
	return coll
}

// writtenDocIDs flattens recorded calls into namespace → sorted document IDs,
// failing if any call carries an action that belongs to a different namespace.
func writtenDocIDs(t *testing.T, calls []tpufCall) map[string][]string {
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

func TestBackfillPage_OneCallPerNamespace(t *testing.T) {
	tp := &fakeTpuf{}
	dlq := &recordingDLQ{}
	w := writer.New(tp, dlq)

	// Interleaved so that grouping by adjacency, rather than by namespace,
	// would produce extra calls.
	page := scannedPage(t,
		tenantDoc("a-1", "a"),
		tenantDoc("b-1", "b"),
		tenantDoc("a-2", "a"),
	)

	p := &collectionPipeline{coll: backfillCollConfig(), writer: w, dlq: dlq}
	if err := p.backfillPage(context.Background(), page, backfillOpTime); err != nil {
		t.Fatalf("backfillPage: %v", err)
	}

	calls := tp.getCalls()
	if len(calls) != 2 {
		t.Fatalf("got %d turbopuffer calls, want 2 (one per namespace)", len(calls))
	}

	got := writtenDocIDs(t, calls)
	want := map[string][]string{
		"items_a": {"a-1", "a-2"},
		"items_b": {"b-1"},
	}
	if !maps.EqualFunc(got, want, slices.Equal) {
		t.Errorf("written doc IDs = %v, want %v", got, want)
	}

	if entries := dlq.getEntries(); len(entries) != 0 {
		t.Errorf("got %d DLQ entries, want 0: %+v", len(entries), entries)
	}
}

// transformFailureCase is a document that fails to transform under
// backfillCollConfig, and the DLQ entry it should produce.
type transformFailureCase struct {
	name     string
	doc      bson.D
	wantID   string
	wantKind mongo.ErrorKind
}

// transformFailureCases are shared by the backfill and change stream tests,
// which must classify a failure the same way.
func transformFailureCases(t *testing.T) []transformFailureCase {
	t.Helper()
	return []transformFailureCase{
		{
			name:     "namespace_field_absent",
			doc:      bson.D{{Key: "_id", Value: "bad"}, {Key: "name", Value: "bad"}},
			wantID:   "bad",
			wantKind: mongo.ErrNamespaceUnresolved,
		},
		{
			name:     "namespace_field_null",
			doc:      bson.D{{Key: "_id", Value: "bad"}, {Key: "tenant", Value: nil}, {Key: "name", Value: "bad"}},
			wantID:   "bad",
			wantKind: mongo.ErrNamespaceUnresolved,
		},
		{
			name:     "namespace_value_unsupported_type",
			doc:      bson.D{{Key: "_id", Value: "bad"}, {Key: "tenant", Value: true}, {Key: "name", Value: "bad"}},
			wantID:   "bad",
			wantKind: mongo.ErrNamespaceUnresolved,
		},
		{
			// Resolves to "items_acme corp", which turbopuffer rejects.
			name:     "namespace_value_invalid_name",
			doc:      tenantDoc("bad", "acme corp"),
			wantID:   "bad",
			wantKind: mongo.ErrNamespaceUnresolved,
		},
		{
			name:     "mapped_field_type_mismatch",
			doc:      bson.D{{Key: "_id", Value: "bad"}, {Key: "tenant", Value: "a"}, {Key: "name", Value: int32(7)}},
			wantID:   "bad",
			wantKind: mongo.ErrTypeMismatch,
		},
		{
			// The ID cannot be serialized, so the entry falls back to its
			// printed value to let an operator find the document.
			name:     "unsupported_id_type",
			doc:      bson.D{{Key: "_id", Value: mustDecimal128(t, "1.5")}, {Key: "tenant", Value: "a"}, {Key: "name", Value: "bad"}},
			wantID:   "1.5",
			wantKind: mongo.ErrIDMissing,
		},
	}
}

func TestBackfillPage_FailedDocumentGoesToDLQ(t *testing.T) {
	// Any document that cannot be transformed is DLQ'd with the same error
	// classification the change stream uses, and the rest of the page is
	// written. Stopping instead strands the backfill: the cursor never moves
	// past the bad document, so every rerun fails on it again.
	for _, tt := range transformFailureCases(t) {
		t.Run(tt.name, func(t *testing.T) {
			tp := &fakeTpuf{}
			dlq := &recordingDLQ{}
			w := writer.New(tp, dlq)

			// The bad document sits mid-page so that stopping at it, or
			// dropping everything after it, loses a good document.
			page := scannedPage(t,
				tenantDoc("a-1", "a"),
				tt.doc,
				tenantDoc("b-1", "b"),
			)

			p := &collectionPipeline{coll: backfillCollConfig(), writer: w, dlq: dlq}
			if err := p.backfillPage(context.Background(), page, backfillOpTime); err != nil {
				t.Fatalf("backfillPage: %v, want nil when the failed document was DLQ'd", err)
			}

			got := writtenDocIDs(t, tp.getCalls())
			want := map[string][]string{
				"items_a": {"a-1"},
				"items_b": {"b-1"},
			}
			if !maps.EqualFunc(got, want, slices.Equal) {
				t.Errorf("written doc IDs = %v, want %v", got, want)
			}

			entries := dlq.getEntries()
			if len(entries) != 1 {
				t.Fatalf("got %d DLQ entries, want 1: %+v", len(entries), entries)
			}
			e := entries[0]
			if e.Collection != "items" {
				t.Errorf("Collection = %q, want %q", e.Collection, "items")
			}
			if e.DocumentID != tt.wantID {
				t.Errorf("DocumentID = %q, want %q", e.DocumentID, tt.wantID)
			}
			if e.Operation != mongo.OpInsert {
				t.Errorf("Operation = %q, want %q", e.Operation, mongo.OpInsert)
			}
			// The document failed before it became a turbopuffer write.
			if e.Namespace != "" || e.ActionType != "" {
				t.Errorf("Namespace, ActionType = %q, %q; want both empty", e.Namespace, e.ActionType)
			}
			if e.ErrorKind != tt.wantKind {
				t.Errorf("ErrorKind = %q, want %q", e.ErrorKind, tt.wantKind)
			}
			if e.ErrorMessage == "" {
				t.Error("ErrorMessage is empty")
			}
			if e.ClusterTime != backfillOpTime {
				t.Errorf("ClusterTime = %d, want %d", e.ClusterTime, backfillOpTime)
			}
		})
	}
}

func TestBackfillPage_DLQWriteFailureFailsPage(t *testing.T) {
	// A document that is neither written nor DLQ'd must fail the page so the
	// caller does not save a cursor past it.
	dlqErr := errors.New("dlq unavailable")
	dlq := &recordingDLQ{err: dlqErr}
	tp := &fakeTpuf{}
	w := writer.New(tp, dlq)

	page := scannedPage(t,
		tenantDoc("a-1", "a"),
		bson.D{{Key: "_id", Value: "bad"}, {Key: "name", Value: "bad"}},
	)

	p := &collectionPipeline{coll: backfillCollConfig(), writer: w, dlq: dlq}
	err := p.backfillPage(context.Background(), page, backfillOpTime)
	if !errors.Is(err, dlqErr) {
		t.Fatalf("backfillPage error = %v, want it to wrap %v", err, dlqErr)
	}

	// The DLQ write comes first, so a page that fails on it spends no
	// turbopuffer writes.
	if calls := tp.getCalls(); len(calls) != 0 {
		t.Errorf("got %d turbopuffer calls, want 0: %+v", len(calls), calls)
	}
}

func TestBackfillPage_FailedDocumentsAreOneDLQCall(t *testing.T) {
	// A page's failed documents are DLQ'd in one round trip, not one per
	// document.
	dlq := &recordingDLQ{}
	w := writer.New(&fakeTpuf{}, dlq)

	page := scannedPage(t,
		bson.D{{Key: "_id", Value: "bad-1"}, {Key: "name", Value: "bad-1"}},
		tenantDoc("a-1", "a"),
		bson.D{{Key: "_id", Value: "bad-2"}, {Key: "name", Value: "bad-2"}},
	)

	p := &collectionPipeline{coll: backfillCollConfig(), writer: w, dlq: dlq}
	if err := p.backfillPage(context.Background(), page, backfillOpTime); err != nil {
		t.Fatalf("backfillPage: %v", err)
	}

	calls := dlq.getCalls()
	if len(calls) != 1 {
		t.Fatalf("got %d DLQ calls, want 1: %+v", len(calls), calls)
	}
	got := make([]string, 0, len(calls[0]))
	for _, e := range calls[0] {
		got = append(got, e.DocumentID)
	}
	if want := []string{"bad-1", "bad-2"}; !slices.Equal(got, want) {
		t.Errorf("DLQ doc IDs = %v, want %v", got, want)
	}
}

func mustDecimal128(t *testing.T, s string) bson.Decimal128 {
	t.Helper()
	d, err := bson.ParseDecimal128(s)
	if err != nil {
		t.Fatalf("bson.ParseDecimal128(%q): %v", s, err)
	}
	return d
}

func TestBackfillPage_StaticNamespaceIsOneCall(t *testing.T) {
	tp := &fakeTpuf{}
	dlq := &recordingDLQ{}
	w := writer.New(tp, dlq)

	const docs = 100
	page := make([]bson.D, 0, docs)
	wantIDs := make([]string, 0, docs)
	for i := range docs {
		id := fmt.Sprintf("doc-%03d", i)
		page = append(page, staticDoc(id))
		wantIDs = append(wantIDs, id)
	}

	p := &collectionPipeline{coll: staticCollConfig(), writer: w, dlq: dlq}
	if err := p.backfillPage(context.Background(), scannedPage(t, page...), backfillOpTime); err != nil {
		t.Fatalf("backfillPage: %v", err)
	}

	if entries := dlq.getEntries(); len(entries) != 0 {
		t.Fatalf("got %d DLQ entries, want 0: %+v", len(entries), entries)
	}
	calls := tp.getCalls()
	if len(calls) != 1 {
		t.Fatalf("got %d turbopuffer calls for a page of %d documents, want 1", len(calls), docs)
	}
	if got := writtenDocIDs(t, calls)["user_profiles"]; !slices.Equal(got, wantIDs) {
		t.Errorf("namespace \"user_profiles\": got doc IDs %v, want %v", got, wantIDs)
	}
}
