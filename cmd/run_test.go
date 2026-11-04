package cmd

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"

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

// fakeStream replays a fixed list of events, then ends without error.
type fakeStream struct {
	events []streamEvent
	pos    int // index of the next event; the current event is pos-1
}

func (s *fakeStream) Next(context.Context) bool {
	if s.pos >= len(s.events) {
		return false
	}
	s.pos++
	return true
}

func (s *fakeStream) Event() (mongo.ChangeEvent, error) {
	if s.pos == 0 {
		return mongo.ChangeEvent{}, nil
	}
	return s.events[s.pos-1].event, nil
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

func TestConsumeStream_StaticNamespaceFlushIsOneCall(t *testing.T) {
	// Every action from a static collection targets its one namespace, so a
	// flush is one turbopuffer call however many documents it carries. An
	// operation that resolved any other Namespace would split the flush into
	// a second call. Events are shaped as a static stream delivers them:
	// without fullDocumentBeforeChange, which a static stream never requests.
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
		// updateLookup returns null in place of the document when it was
		// deleted before the lookup ran, so cover both shapes.
		if i%2 == 0 {
			update.FullDocument = scannedPage(t, staticDoc(upd))[0]
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

	// Thresholds above the event count, so the only flush is consumeStream's
	// final one and every event lands in it.
	ctx := context.Background()
	var flushes atomic.Int32
	b := batch.New(ctx, config.GlobalConfig{BatchFlushCount: len(events) + 1, BatchFlushTimeMs: 60000}, func(
		ctx context.Context, actions []transform.Action, _ []byte,
	) error {
		flushes.Add(1)
		return w.WriteBatch(ctx, coll.Name, nil, actions)
	})
	defer b.Close(ctx)

	if err := consumeStream(ctx, &fakeStream{events: events}, b, coll, dlq); err != nil {
		t.Fatalf("consumeStream: %v", err)
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
