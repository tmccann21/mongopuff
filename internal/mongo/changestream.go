package mongo

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type changeStreamEvent struct {
	OperationType     string             `bson:"operationType"`
	DocumentKey       bson.M             `bson:"documentKey"`
	FullDocument      bson.M             `bson:"fullDocument"`
	UpdateDescription *updateDescription `bson:"updateDescription"`
	ClusterTime       bson.Timestamp     `bson:"clusterTime"`
}

type updateDescription struct {
	UpdatedFields bson.M   `bson:"updatedFields"`
	RemovedFields []string `bson:"removedFields"`
}

// LiveChangeStream wraps the driver's ChangeStream and satisfies ChangeStreamIterator.
type LiveChangeStream struct {
	cs      *mongo.ChangeStream
	current ChangeEvent
	err     error
}

// OpenChangeStream opens a change stream on the given collection.
// If resumeToken is non-nil, the stream resumes from that token.
func OpenChangeStream(ctx context.Context, db *mongo.Database, collection string, resumeToken []byte) (*LiveChangeStream, error) {
	opts := options.ChangeStream().
		SetFullDocument(options.UpdateLookup)

	if resumeToken != nil {
		opts.SetResumeAfter(bson.Raw(resumeToken))
	}

	cs, err := db.Collection(collection).Watch(ctx, mongo.Pipeline{}, opts)
	if err != nil {
		return nil, fmt.Errorf("opening change stream on %q: %w", collection, err)
	}

	return &LiveChangeStream{cs: cs}, nil
}

func (l *LiveChangeStream) Next(ctx context.Context) bool {
	if !l.cs.Next(ctx) {
		l.err = l.cs.Err()
		return false
	}

	var raw changeStreamEvent
	if err := l.cs.Decode(&raw); err != nil {
		l.err = fmt.Errorf("decoding change event: %w", err)
		return false
	}

	l.current = ChangeEvent{
		Operation:   Operation(raw.OperationType),
		DocumentID:  raw.DocumentKey["_id"],
		ClusterTime: uint64(raw.ClusterTime.T)<<32 | uint64(raw.ClusterTime.I),
	}

	if raw.FullDocument != nil {
		l.current.FullDocument = anyMap(raw.FullDocument)
	}

	if raw.UpdateDescription != nil {
		if raw.UpdateDescription.UpdatedFields != nil {
			l.current.UpdatedFields = anyMap(raw.UpdateDescription.UpdatedFields)
		}
		l.current.RemovedFields = raw.UpdateDescription.RemovedFields
	}

	l.err = nil
	return true
}

func (l *LiveChangeStream) Event() (ChangeEvent, error) {
	return l.current, l.err
}

func (l *LiveChangeStream) ResumeToken() []byte {
	return []byte(l.cs.ResumeToken())
}

func (l *LiveChangeStream) Close(ctx context.Context) error {
	return l.cs.Close(ctx)
}

// anyMap converts bson.M to map[string]any (they're the same underlying type,
// but this makes the conversion explicit for clarity).
func anyMap(m bson.M) map[string]any {
	return map[string]any(m)
}
