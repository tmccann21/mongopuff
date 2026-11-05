package mongo

import (
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
)

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
