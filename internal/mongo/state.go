package mongo

import (
	"context"
	"errors"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/connstring"

	"github.com/tmccann21/mongopuff/internal/config"
)

const (
	mongopuffCollection = "_mongopuff"
	globalDocID         = "_global"
)

type Store struct {
	db *mongo.Database
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

	return &Store{db: client.Database(dbName)}, nil
}

func (s *Store) LoadCollectionConfigs(ctx context.Context) ([]config.CollectionConfig, error) {
	coll := s.db.Collection(mongopuffCollection)

	cursor, err := coll.Find(ctx, bson.M{"_id": bson.M{"$ne": globalDocID}})
	if err != nil {
		return nil, fmt.Errorf("querying _mongopuff: %w", err)
	}
	defer cursor.Close(ctx)

	var configs []config.CollectionConfig
	if err := cursor.All(ctx, &configs); err != nil {
		return nil, fmt.Errorf("decoding _mongopuff documents: %w", err)
	}

	// Default namespace to collection name if not set.
	for i := range configs {
		if configs[i].Mapping.Namespace == "" {
			configs[i].Mapping.Namespace = configs[i].Name
		}
	}

	return configs, nil
}

// LoadGlobalConfig reads the _global document from _mongopuff.
// Returns a zero GlobalConfig if the document does not exist.
func (s *Store) LoadGlobalConfig(ctx context.Context) (config.GlobalConfig, error) {
	coll := s.db.Collection(mongopuffCollection)

	var gc config.GlobalConfig
	err := coll.FindOne(ctx, bson.M{"_id": globalDocID}).Decode(&gc)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return config.GlobalConfig{}, nil
	}
	if err != nil {
		return config.GlobalConfig{}, fmt.Errorf("reading _global config: %w", err)
	}

	return gc, nil
}
