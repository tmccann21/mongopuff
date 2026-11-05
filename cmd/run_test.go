package cmd

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	mongodriver "go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/tmccann21/mongopuff/internal/batch"
	"github.com/tmccann21/mongopuff/internal/config"
	"github.com/tmccann21/mongopuff/internal/mongo"
	"github.com/tmccann21/mongopuff/internal/transform"
	"github.com/tmccann21/mongopuff/internal/writer"
)

type streamEvent struct {
	event mongo.ChangeEvent
	token string
}

// fakeStream replays a fixed list of events, then ends with err, which is nil
// for a stream that ends cleanly.
type fakeStream struct {
	events []streamEvent
	err    error
	pos    int // index of the next event; the current event is pos-1
}

func (s *fakeStream) Next(context.Context) bool {
	if s.pos >= len(s.events) {
		return false
	}
	s.pos++
	return true
}

func (s *fakeStream) Event() mongo.ChangeEvent {
	if s.pos == 0 {
		return mongo.ChangeEvent{}
	}
	return s.events[s.pos-1].event
}

func (s *fakeStream) Err() error {
	if s.pos < len(s.events) {
		return nil
	}
	return s.err
}

func (s *fakeStream) ResumeToken() []byte {
	if s.pos == 0 {
		return nil
	}
	return []byte(s.events[s.pos-1].token)
}

// insertEvent builds an insert whose fullDocument has the Go types the driver
// decodes a change event into.
func insertEvent(t *testing.T, doc bson.D) mongo.ChangeEvent {
	t.Helper()
	full := scannedPage(t, doc)[0]
	return mongo.ChangeEvent{
		Operation:    mongo.OpInsert,
		DocumentID:   full["_id"],
		FullDocument: full,
		ClusterTime:  1,
	}
}

// tokenRecorder is a batch.FlushFunc that records the resume token of every
// flush. A flush that returns nil is one whose token the real flush saves.
type tokenRecorder struct {
	mu     sync.Mutex
	tokens []string
}

func (r *tokenRecorder) flush(_ context.Context, _ []transform.Action, resumeToken []byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.tokens = append(r.tokens, string(resumeToken))
	return nil
}

func (r *tokenRecorder) getTokens() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.tokens)
}

func TestConsumeStream_DLQWriteFailureStopsStream(t *testing.T) {
	// An event that fails to transform and cannot be DLQ'd is in neither
	// turbopuffer nor the DLQ. If the stream moves on, the next flush saves a
	// resume token past it and a restart never replays it.
	dlqErr := errors.New("dlq unavailable")
	dlq := &recordingDLQ{err: dlqErr}
	rec := &tokenRecorder{}

	ctx := context.Background()
	// Flush on every event so that any event after the bad one saves its token.
	b := batch.New(ctx, config.GlobalConfig{BatchFlushCount: 1, BatchFlushTimeMs: 60000}, rec.flush)
	defer b.Close(ctx)

	stream := &fakeStream{events: []streamEvent{
		// No tenant field, so namespace resolution fails.
		{event: insertEvent(t, bson.D{{Key: "_id", Value: "bad"}, {Key: "name", Value: "bad"}}), token: "tok-bad"},
		{event: insertEvent(t, tenantDoc("good", "a")), token: "tok-good"},
	}}

	err := consumeStream(ctx, stream, b, backfillCollConfig(), dlq)
	if !errors.Is(err, dlqErr) {
		t.Errorf("consumeStream error = %v, want it to wrap %v", err, dlqErr)
	}
	if tokens := rec.getTokens(); len(tokens) != 0 {
		t.Errorf("flushed resume tokens %q past an event that was neither written nor DLQ'd", tokens)
	}
}

func TestConsumeStream_StreamErrorIsReturned(t *testing.T) {
	// A stream that fails after some events stops consumeStream with its
	// error, and the events read before it are still flushed. Only an error
	// the server labels NonResumableChangeStreamError, which it returns when
	// the resume token has fallen off the oplog, means the saved token is
	// useless and the collection must be re-backfilled.
	tests := []struct {
		name           string
		err            mongodriver.CommandError
		wantRebackfill bool
	}{
		{
			name: "non_resumable",
			err: mongodriver.CommandError{
				Code:    286,
				Name:    "ChangeStreamHistoryLost",
				Message: "Resume of change stream was not possible, as the resume point may no longer be in the oplog.",
				Labels:  []string{"NonResumableChangeStreamError"},
			},
			wantRebackfill: true,
		},
		{
			name: "unlabeled",
			err: mongodriver.CommandError{
				Code:    13,
				Name:    "Unauthorized",
				Message: "not authorized on app to execute command { getMore: 1 }",
			},
			wantRebackfill: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := &tokenRecorder{}
			ctx := context.Background()
			// Thresholds above the event count, so the only flush is the
			// one Close makes.
			b := batch.New(ctx, config.GlobalConfig{BatchFlushCount: 10, BatchFlushTimeMs: 60000}, rec.flush)

			stream := &fakeStream{
				events: []streamEvent{
					{event: insertEvent(t, tenantDoc("a-1", "a")), token: "tok-a-1"},
					{event: insertEvent(t, tenantDoc("b-1", "b")), token: "tok-b-1"},
				},
				err: tt.err,
			}

			err := consumeStream(ctx, stream, b, backfillCollConfig(), &recordingDLQ{})
			var cmdErr mongodriver.CommandError
			if !errors.As(err, &cmdErr) || cmdErr.Code != tt.err.Code {
				t.Fatalf("consumeStream error = %v, want it to wrap %v", err, tt.err)
			}
			if got := errors.Is(err, errRebackfillRequired); got != tt.wantRebackfill {
				t.Errorf("errors.Is(%v, errRebackfillRequired) = %t, want %t", err, got, tt.wantRebackfill)
			}
			if err := b.Close(ctx); err != nil {
				t.Fatalf("Close: %v", err)
			}
			if tokens := rec.getTokens(); !slices.Equal(tokens, []string{"tok-b-1"}) {
				t.Errorf("flushed resume tokens %q, want %q", tokens, []string{"tok-b-1"})
			}
		})
	}
}

// shutdownStream ends the way a live stream does on SIGTERM: once its events
// run out, Next cancels the run context and returns false. Set the embedded
// fakeStream's err to context.Canceled, which the driver's Err then reports.
type shutdownStream struct {
	*fakeStream
	cancel context.CancelFunc
}

func (s shutdownStream) Next(ctx context.Context) bool {
	if s.fakeStream.Next(ctx) {
		return true
	}
	s.cancel()
	return false
}

func TestCloseBatcher_FlushesAfterShutdown(t *testing.T) {
	// SIGTERM cancels the run context and ends the stream with events still
	// in the batcher. A flush on that context fails, their resume token is
	// never saved, and a restart replays them. closeBatcher must flush them
	// on a context that shutdown has not cancelled.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rec := &tokenRecorder{}
	// Thresholds above the event count, so the only flush is closeBatcher's.
	b := batch.New(ctx, config.GlobalConfig{BatchFlushCount: 10, BatchFlushTimeMs: 60000}, func(
		ctx context.Context, actions []transform.Action, resumeToken []byte,
	) error {
		// Like a real write, fail on a cancelled context.
		if err := ctx.Err(); err != nil {
			return err
		}
		return rec.flush(ctx, actions, resumeToken)
	})

	stream := shutdownStream{
		fakeStream: &fakeStream{
			events: []streamEvent{
				{event: insertEvent(t, tenantDoc("a-1", "a")), token: "tok-a-1"},
				{event: insertEvent(t, tenantDoc("b-1", "b")), token: "tok-b-1"},
			},
			err: context.Canceled,
		},
		cancel: cancel,
	}

	// consumeStream returns the stream's cancellation; this test is about
	// what closeBatcher flushes after it.
	_ = consumeStream(ctx, stream, b, backfillCollConfig(), &recordingDLQ{})

	var err error
	closeBatcher(ctx, b, time.Minute, &err)
	if err != nil {
		t.Fatalf("closeBatcher: %v", err)
	}
	if tokens := rec.getTokens(); !slices.Equal(tokens, []string{"tok-b-1"}) {
		t.Errorf("saved resume tokens %q, want %q", tokens, []string{"tok-b-1"})
	}
}

func TestConsumeStream_InvalidNamespaceGoesToDLQ(t *testing.T) {
	// A document whose namespace resolves to a name turbopuffer rejects is a
	// transform failure: it goes to the DLQ with no turbopuffer write, and the
	// events around it are still written.
	tp := &fakeTpuf{}
	dlq := &recordingDLQ{}
	w := writer.New(tp, dlq)
	coll := backfillCollConfig()

	ctx := context.Background()
	b := batch.New(ctx, config.GlobalConfig{BatchFlushCount: 10, BatchFlushTimeMs: 60000}, func(
		ctx context.Context, actions []transform.Action, _ []byte,
	) error {
		return w.WriteBatch(ctx, coll.Name, nil, actions)
	})

	stream := &fakeStream{events: []streamEvent{
		{event: insertEvent(t, tenantDoc("a-1", "a")), token: "tok-a-1"},
		// Resolves to "items_acme corp".
		{event: insertEvent(t, tenantDoc("bad", "acme corp")), token: "tok-bad"},
		{event: insertEvent(t, tenantDoc("b-1", "b")), token: "tok-b-1"},
	}}

	if err := consumeStream(ctx, stream, b, coll, dlq); err != nil {
		t.Fatalf("consumeStream: %v", err)
	}
	if err := b.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
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
	if e := entries[0]; e.DocumentID != "bad" || e.Collection != "items" {
		t.Errorf("DLQ entry DocumentID, Collection = %q, %q; want %q, %q", e.DocumentID, e.Collection, "bad", "items")
	}
	if e := entries[0]; e.ErrorKind != mongo.ErrNamespaceUnresolved {
		t.Errorf("ErrorKind = %q, want %q", e.ErrorKind, mongo.ErrNamespaceUnresolved)
	}
}

func TestConsumeStream_FailedEventGoesToDLQ(t *testing.T) {
	// An event that cannot be transformed is DLQ'd with the same error
	// classification backfill uses, and the events around it are written.
	for _, tt := range transformFailureCases(t) {
		t.Run(tt.name, func(t *testing.T) {
			tp := &fakeTpuf{}
			dlq := &recordingDLQ{}
			w := writer.New(tp, dlq)
			coll := backfillCollConfig()

			ctx := context.Background()
			b := batch.New(ctx, config.GlobalConfig{BatchFlushCount: 10, BatchFlushTimeMs: 60000}, func(
				ctx context.Context, actions []transform.Action, _ []byte,
			) error {
				return w.WriteBatch(ctx, coll.Name, nil, actions)
			})

			bad := insertEvent(t, tt.doc)
			stream := &fakeStream{events: []streamEvent{
				{event: insertEvent(t, tenantDoc("a-1", "a")), token: "tok-a-1"},
				{event: bad, token: "tok-bad"},
				{event: insertEvent(t, tenantDoc("b-1", "b")), token: "tok-b-1"},
			}}

			if err := consumeStream(ctx, stream, b, coll, dlq); err != nil {
				t.Fatalf("consumeStream: %v, want nil when the failed event was DLQ'd", err)
			}
			if err := b.Close(ctx); err != nil {
				t.Fatalf("Close: %v", err)
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
			// The event failed before it became a turbopuffer write.
			if e.Namespace != "" || e.ActionType != "" {
				t.Errorf("Namespace, ActionType = %q, %q; want both empty", e.Namespace, e.ActionType)
			}
			if e.ErrorKind != tt.wantKind {
				t.Errorf("ErrorKind = %q, want %q", e.ErrorKind, tt.wantKind)
			}
			if e.ErrorMessage == "" {
				t.Error("ErrorMessage is empty")
			}
			if e.ClusterTime != bad.ClusterTime {
				t.Errorf("ClusterTime = %d, want %d", e.ClusterTime, bad.ClusterTime)
			}
		})
	}
}

func TestTransformErrorKind_Unclassified(t *testing.T) {
	// An error that wraps none of transform's sentinels is reported as unknown
	// rather than guessed at.
	err := fmt.Errorf("wrapped: %w", errors.New("no sentinel"))
	if got := transformErrorKind(err); got != mongo.ErrUnknown {
		t.Errorf("transformErrorKind(%v) = %q, want %q", err, got, mongo.ErrUnknown)
	}
}

func TestConsumeStream_StaticNamespaceFlushIsOneCall(t *testing.T) {
	// Every action from a static collection targets its one namespace, so a
	// flush is one turbopuffer call however many documents it carries. An
	// operation that resolved any other Namespace would split the flush into
	// a second call. Events are shaped as a static stream delivers them: an
	// update without fullDocument and a delete without fullDocumentBeforeChange,
	// since a static stream requests neither image.
	const perOp = 25
	var events []streamEvent
	var wantIDs []string
	for i := range perOp {
		ins, rep, upd, del := fmt.Sprintf("ins-%02d", i), fmt.Sprintf("rep-%02d", i), fmt.Sprintf("upd-%02d", i), fmt.Sprintf("del-%02d", i)

		replace := insertEvent(t, staticDoc(rep))
		replace.Operation = mongo.OpReplace

		update := mongo.ChangeEvent{
			Operation:     mongo.OpUpdate,
			DocumentID:    upd,
			UpdatedFields: map[string]any{"name": upd},
			ClusterTime:   1,
		}

		events = append(events,
			streamEvent{event: insertEvent(t, staticDoc(ins)), token: "tok-" + ins},
			streamEvent{event: replace, token: "tok-" + rep},
			streamEvent{event: update, token: "tok-" + upd},
			streamEvent{event: mongo.ChangeEvent{Operation: mongo.OpDelete, DocumentID: del, ClusterTime: 1}, token: "tok-" + del},
		)
		wantIDs = append(wantIDs, ins, rep, upd, del)
	}
	slices.Sort(wantIDs)

	tp := &fakeTpuf{}
	dlq := &recordingDLQ{}
	w := writer.New(tp, dlq)
	coll := staticCollConfig()

	// Thresholds above the event count, so the only flush is the one Close
	// makes and every event lands in it.
	ctx := context.Background()
	var flushes atomic.Int32
	b := batch.New(ctx, config.GlobalConfig{BatchFlushCount: len(events) + 1, BatchFlushTimeMs: 60000}, func(
		ctx context.Context, actions []transform.Action, _ []byte,
	) error {
		flushes.Add(1)
		return w.WriteBatch(ctx, coll.Name, nil, actions)
	})

	if err := consumeStream(ctx, &fakeStream{events: events}, b, coll, dlq); err != nil {
		t.Fatalf("consumeStream: %v", err)
	}
	if err := b.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if entries := dlq.getEntries(); len(entries) != 0 {
		t.Fatalf("got %d DLQ entries, want 0: %+v", len(entries), entries)
	}
	if n := flushes.Load(); n != 1 {
		t.Fatalf("got %d flushes, want 1", n)
	}
	calls := tp.getCalls()
	if len(calls) != 1 {
		t.Fatalf("got %d turbopuffer calls for one flush of %d documents, want 1", len(calls), len(wantIDs))
	}
	if got := writtenDocIDs(t, calls)["user_profiles"]; !slices.Equal(got, wantIDs) {
		t.Errorf("namespace \"user_profiles\": got doc IDs %v, want %v", got, wantIDs)
	}
}
