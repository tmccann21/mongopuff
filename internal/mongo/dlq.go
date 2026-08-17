package mongo

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/mongo"
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
