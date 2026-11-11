package mongo

import (
	"context"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func TestDLQWriter_WriteNoEntries(t *testing.T) {
	// The driver rejects an empty InsertMany before contacting the server, so
	// a client with no server behind it is enough to catch a Write that
	// forwards zero entries.
	client, err := mongo.Connect(options.Client().ApplyURI("mongodb://localhost:1"))
	if err != nil {
		t.Fatalf("mongo.Connect: %v", err)
	}
	t.Cleanup(func() { _ = client.Disconnect(context.Background()) })

	d := &dlqWriter{coll: client.Database("test").Collection(mongopuffDLQCollection)}
	if err := d.Write(context.Background()); err != nil {
		t.Errorf("Write with no entries: %v, want nil", err)
	}
}

func TestDLQStoredEntry_DecodesEntryWithoutNamespaceOrActionType(t *testing.T) {
	// An entry as stored before namespace and actionType existed, which is
	// also how a transform failure is stored today: the encoder omits both
	// fields when they are empty. clusterTime is an int64 and createdAt a
	// date, as the driver encodes uint64 and time.Time.
	id := bson.NewObjectID()
	createdAt := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	raw, err := bson.Marshal(bson.D{
		{Key: "_id", Value: id},
		{Key: "collection", Value: "items"},
		{Key: "documentId", Value: "doc-1"},
		{Key: "operation", Value: "update"},
		{Key: "errorKind", Value: "write_rejected"},
		{Key: "errorMessage", Value: "400 Bad Request"},
		{Key: "clusterTime", Value: int64(7)},
		{Key: "createdAt", Value: bson.NewDateTimeFromTime(createdAt)},
	})
	if err != nil {
		t.Fatalf("bson.Marshal: %v", err)
	}

	var got DLQStoredEntry
	if err := bson.Unmarshal(raw, &got); err != nil {
		t.Fatalf("bson.Unmarshal: %v", err)
	}

	want := DLQStoredEntry{
		ID: id,
		DLQEntry: DLQEntry{
			Collection:   "items",
			DocumentID:   "doc-1",
			Operation:    OpUpdate,
			ErrorKind:    ErrWriteRejected,
			ErrorMessage: "400 Bad Request",
			ClusterTime:  7,
			CreatedAt:    createdAt,
		},
	}
	// CreatedAt decodes in the local time zone, so compare it with Equal.
	if !got.CreatedAt.Equal(want.CreatedAt) {
		t.Errorf("CreatedAt = %v, want %v", got.CreatedAt, want.CreatedAt)
	}
	got.CreatedAt = want.CreatedAt
	if got != want {
		t.Errorf("decoded %+v, want %+v", got, want)
	}
}
