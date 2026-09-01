package mongo

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/connstring"
)

const (
	mongopuffStateCollection = "_mongopuff_state"
)

type Store struct {
	db *mongo.Database
	client *mongo.Client
}

func ParseDatabaseName(connStr string) (string, error) {
	cs, err := connstring.Parse(connStr)
	if err != nil {
		return "", fmt.Errorf("parsing connection string: %w", err)
	}
	if cs.Database == "" {
		return "", errors.New("connection string must include a database name (e.g. mongodb://host/mydb)")
	}
	return cs.Database, nil
}

func NewStore(ctx context.Context, connStr string) (*Store, error) {
	dbName, err := ParseDatabaseName(connStr)
	if err != nil {
		return nil, err
	}

	client, err := mongo.Connect(options.Client().ApplyURI(connStr))
	if err != nil {
		return nil, fmt.Errorf("connecting to mongodb: %w", err)
	}

	if err := client.Ping(ctx, nil); err != nil {
		return nil, fmt.Errorf("pinging mongodb: %w", err)
	}

	return &Store{db: client.Database(dbName), client: client}, nil
}

func (s *Store) Close(ctx context.Context) error {
	return s.client.Disconnect(ctx)
}

func (s *Store) DatabaseName() string {
	return s.db.Name()
}

func (s *Store) SaveResumeToken(ctx context.Context, collection string, resumeToken []byte) error {
	coll := s.db.Collection(mongopuffStateCollection)

	_, err := coll.UpdateOne(ctx, bson.M{"_id": collection}, bson.M{"$set": bson.M{"changeStreamResumeToken": resumeToken}}, options.UpdateOne().SetUpsert(true))
	return err
}

func (s *Store) SaveLastFlushTime(ctx context.Context, collection string, lastFlushTime time.Time) error {
	coll := s.db.Collection(mongopuffStateCollection)

	_, err := coll.UpdateOne(ctx, bson.M{"_id": collection}, bson.M{"$set": bson.M{"lastFlushTime": lastFlushTime}}, options.UpdateOne().SetUpsert(true))
	return err
}

func (s *Store) LoadCollectionState(ctx context.Context, collection string) (CollectionState, error) {
	coll := s.db.Collection(mongopuffStateCollection)

	var cs CollectionState
	err := coll.FindOne(ctx, bson.M{"_id": collection}).Decode(&cs)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return CollectionState{}, nil
	}
	if err != nil {
		return CollectionState{}, fmt.Errorf("reading collection state: %w", err)
	}
	return cs, nil
}

func (s *Store) OpenChangeStream(ctx context.Context, collection string, resumeToken []byte) (*LiveChangeStream, error) {
	return OpenChangeStream(ctx, s.db, collection, resumeToken)
}

func (s *Store) PingOperationTime(ctx context.Context) (uint64, error) {
	var result bson.M
	err := s.db.RunCommand(ctx, bson.D{{Key: "ping", Value: 1}}).Decode(&result)
	if err != nil {
		return 0, fmt.Errorf("ping: %w", err)
	}

	ts, ok := result["operationTime"].(bson.Timestamp)
	if !ok {
		return 0, fmt.Errorf("operationTime not found or unexpected type in ping response")
	}

	return uint64(ts.T)<<32 | uint64(ts.I), nil
}

func (s *Store) CollectionScanner(name string) *CollectionScanner {
	return &CollectionScanner{c: s.db.Collection(name)}
}

func (s *Store) SaveBackfillCursor(ctx context.Context, collection string, lastID any) error {
	coll := s.db.Collection(mongopuffStateCollection)
	_, err := coll.UpdateOne(ctx, bson.M{"_id": collection}, bson.M{"$set": bson.M{"backfillCursor": lastID}}, options.UpdateOne().SetUpsert(true))

	return err
}
