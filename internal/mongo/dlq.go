package mongo

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

const mongopuffDLQCollection = "_mongopuff_dlq"

type dlqWriter struct {
	coll *mongo.Collection
}

func (s *Store) NewDLQWriter() DLQWriter {
	return &dlqWriter{coll: s.db.Collection(mongopuffDLQCollection)}
}

func (d *dlqWriter) Write(ctx context.Context, entry DLQEntry) error {
	if _, err := d.coll.InsertOne(ctx, entry); err != nil {
		return fmt.Errorf("writing to DLQ: %w", err)
	}
	return nil
}

type DLQStoredEntry struct {
	ID bson.ObjectID `bson:"_id"`
	DLQEntry        `bson:",inline"`
}

func (s *Store) ListDLQ(ctx context.Context, collection string, limit int64) ([]DLQStoredEntry, error) {
	coll := s.db.Collection(mongopuffDLQCollection)

	filter := bson.M{}
	if collection != "" {
		filter["collection"] = collection
	}

	opts := options.Find().
		SetSort(bson.D{{Key: "_id", Value: -1}}).
		SetLimit(limit)

	cursor, err := coll.Find(ctx, filter, opts)
	if err != nil {
		return nil, fmt.Errorf("listing DLQ entries: %w", err)
	}
	defer cursor.Close(ctx)

	var entries []DLQStoredEntry
	if err := cursor.All(ctx, &entries); err != nil {
		return nil, fmt.Errorf("decoding DLQ entries: %w", err)
	}
	return entries, nil
}

func (s *Store) GetDLQEntry(ctx context.Context, idHex string) (DLQStoredEntry, error) {
	oid, err := bson.ObjectIDFromHex(idHex)
	if err != nil {
		return DLQStoredEntry{}, fmt.Errorf("invalid DLQ entry ID %q: %w", idHex, err)
	}

	coll := s.db.Collection(mongopuffDLQCollection)
	var entry DLQStoredEntry
	if err := coll.FindOne(ctx, bson.M{"_id": oid}).Decode(&entry); err != nil {
		return DLQStoredEntry{}, fmt.Errorf("reading DLQ entry: %w", err)
	}
	return entry, nil
}

func (s *Store) ClearDLQ(ctx context.Context, collection string) (int64, error) {
	if collection == "" {
		panic("ClearDLQ: collection must not be empty; pass \"*\" to clear all")
	}

	coll := s.db.Collection(mongopuffDLQCollection)

	filter := bson.M{}
	if collection != "*" {
		filter["collection"] = collection
	}

	result, err := coll.DeleteMany(ctx, filter)
	if err != nil {
		return 0, fmt.Errorf("clearing DLQ: %w", err)
	}
	return result.DeletedCount, nil
}
