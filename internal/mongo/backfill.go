package mongo

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type CollectionScanner struct {
	c *mongo.Collection
}

func (cs *CollectionScanner) ScanPage(ctx context.Context, afterID any, pageSize int) ([]map[string]any, error) {
	filter := bson.M{}
	if afterID != nil {
		filter = bson.M{"_id": bson.M{"$gt": afterID}}
	}

	opts := options.Find().
		SetSort(bson.D{{Key: "_id", Value: 1}}).
		SetLimit(int64(pageSize))

	cursor, err := cs.c.Find(ctx, filter, opts)
	if err != nil {
		return nil, fmt.Errorf("scanning page: %w", err)
	}
	defer cursor.Close(ctx)

	var docs []map[string]any
	err = cursor.All(ctx, &docs)
	if err != nil {
		return nil, fmt.Errorf("decoding page: %w", err)
	}
	return docs, nil
}