package mongo

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type changeStreamEvent struct {
	OperationType            string             `bson:"operationType"`
	DocumentKey              bson.M             `bson:"documentKey"`
	FullDocument             bson.M             `bson:"fullDocument"`
	FullDocumentBeforeChange bson.M             `bson:"fullDocumentBeforeChange"`
	UpdateDescription        *updateDescription `bson:"updateDescription"`
	ClusterTime              bson.Timestamp     `bson:"clusterTime"`
}

type updateDescription struct {
	UpdatedFields bson.M   `bson:"updatedFields"`
	RemovedFields []string `bson:"removedFields"`
}

// LiveChangeStream wraps the driver's ChangeStream and satisfies cmd.changeStream.
type LiveChangeStream struct {
	cs      *mongo.ChangeStream
	current ChangeEvent
	err     error
}

// OpenChangeStream opens a change stream on the given collection.
// If resumeToken is non-nil, the stream resumes from that token.
// Insert and replace events always carry fullDocument. If prePostImages is
// true, update events also carry fullDocument, the post-image, and update and
// delete events carry fullDocumentBeforeChange, the pre-image. Both are the
// document as of the event, and are present only when the collection has
// changeStreamPreAndPostImages enabled and the image has not expired. If
// prePostImages is false, update and delete events carry neither image.
func OpenChangeStream(ctx context.Context, db *mongo.Database, collection string, resumeToken []byte, prePostImages bool) (*LiveChangeStream, error) {
	opts := changeStreamOptions(resumeToken, prePostImages)

	cs, err := db.Collection(collection).Watch(ctx, mongo.Pipeline{}, opts)
	if err != nil {
		return nil, fmt.Errorf("opening change stream on %q: %w", collection, err)
	}

	return &LiveChangeStream{cs: cs}, nil
}

// changeStreamOptions never requests updateLookup, which reads the document
// when the event is delivered rather than as of the event. Its fullDocument
// can be several writes newer than the pre-image, or null if the document has
// since been deleted.
func changeStreamOptions(resumeToken []byte, prePostImages bool) *options.ChangeStreamOptionsBuilder {
	opts := options.ChangeStream()

	if prePostImages {
		opts.SetFullDocument(options.WhenAvailable).
			SetFullDocumentBeforeChange(options.WhenAvailable)
	}

	if resumeToken != nil {
		opts.SetResumeAfter(bson.Raw(resumeToken))
	}

	return opts
}

func (l *LiveChangeStream) Next(ctx context.Context) bool {
	// The driver's error is sticky but a decode error is not: another call
	// to cs.Next would move past the event that failed to decode.
	if l.err != nil {
		return false
	}

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

	if raw.FullDocumentBeforeChange != nil {
		l.current.FullDocumentBeforeChange = anyMap(raw.FullDocumentBeforeChange)
	}

	if raw.UpdateDescription != nil {
		if raw.UpdateDescription.UpdatedFields != nil {
			l.current.UpdatedFields = anyMap(raw.UpdateDescription.UpdatedFields)
		}
		l.current.RemovedFields = raw.UpdateDescription.RemovedFields
	}

	return true
}

func (l *LiveChangeStream) Event() ChangeEvent {
	return l.current
}

// Err returns the error from the last Next call, if any.
func (l *LiveChangeStream) Err() error {
	return l.err
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
