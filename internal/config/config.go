package config

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"
)

type FieldType string

const (
	FieldTypeString   FieldType = "string"
	FieldTypeInt      FieldType = "int"
	FieldTypeUint     FieldType = "uint"
	FieldTypeFloat    FieldType = "float"
	FieldTypeBool     FieldType = "bool"
	FieldTypeUUID     FieldType = "uuid"
	FieldTypeDatetime FieldType = "datetime"

	FieldTypeStringArray   FieldType = "[]string"
	FieldTypeIntArray      FieldType = "[]int"
	FieldTypeUintArray     FieldType = "[]uint"
	FieldTypeFloatArray    FieldType = "[]float"
	FieldTypeBoolArray     FieldType = "[]bool"
	FieldTypeUUIDArray     FieldType = "[]uuid"
	FieldTypeDatetimeArray FieldType = "[]datetime"

	FieldTypeVector FieldType = "vector"
)

type VectorPrecision string

const (
	VectorPrecisionF32 VectorPrecision = "f32"
	VectorPrecisionF16 VectorPrecision = "f16"
	VectorPrecisionI8  VectorPrecision = "i8"
)

type FieldMapping struct {
	Name      string          `bson:"name"`
	Type      FieldType       `bson:"type"`
	Dimension int             `bson:"dimension,omitempty"` // vector only
	Precision VectorPrecision `bson:"precision,omitempty"` // vector only
}

type MappingConfig struct {
	Namespace string         `bson:"namespace"`
	Fields    []FieldMapping `bson:"fields"`
}

// CollectionConfig is the shape of a document in the _mongopuff collection.
type CollectionConfig struct {
	Name                    string        `bson:"_id"`
	ChangeStreamResumeToken []byte        `bson:"changeStreamResumeToken,omitempty"`
	BackfillCursor          any           `bson:"backfillCursor,omitempty"`
	BackfillPageSize        int           `bson:"backfillPageSize,omitempty"`
	MirrorDeletes           *bool         `bson:"mirrorDeletes,omitempty"`
	LastFlushTime           time.Time     `bson:"lastFlushTime,omitempty"`
	Mapping                 MappingConfig `bson:"mapping"`
}

func (c *CollectionConfig) MirrorDeletesEnabled() bool {
	if c.MirrorDeletes == nil {
		return true
	}
	return *c.MirrorDeletes
}

func (c *CollectionConfig) EffectiveBackfillPageSize() int {
	if c.BackfillPageSize <= 0 {
		return 128
	}
	return c.BackfillPageSize
}

type GlobalConfig struct {
	BatchFlushCount  int `bson:"batchFlushCount,omitempty"`
	BatchFlushSize   int `bson:"batchFlushSize,omitempty"`
	BatchFlushTimeMs int `bson:"batchFlushTimeMs,omitempty"`
}

const (
	DefaultBatchFlushCount  = 1024
	DefaultBatchFlushSize   = 8 * 1024 * 1024 // 8MB
	DefaultBatchFlushTimeMs = 1000            // 1s
)

func (g GlobalConfig) FlushInterval() time.Duration {
	ms := g.BatchFlushTimeMs
	if ms <= 0 {
		ms = DefaultBatchFlushTimeMs
	}
	return time.Duration(ms) * time.Millisecond
}

func (g GlobalConfig) Effective() GlobalConfig {
	out := g
	if out.BatchFlushCount <= 0 {
		out.BatchFlushCount = DefaultBatchFlushCount
	}
	if out.BatchFlushSize <= 0 {
		out.BatchFlushSize = DefaultBatchFlushSize
	}
	if out.BatchFlushTimeMs <= 0 {
		out.BatchFlushTimeMs = DefaultBatchFlushTimeMs
	}
	return out
}

type AppConfig struct {
	HealthPort              int
	LogLevel                string
	MongoDBConnectionString string
	TurbopufferAPIKey       string

	Global      GlobalConfig
	Collections []CollectionConfig
}

// DatabaseName extracts the database name from the MongoDB connection string.
// TODO: use the mongo driver's connstring parser instead of url.Parse
func (a *AppConfig) DatabaseName() string {
	panic("not implemented: use mongo driver connstring parser")
}

// Collection looks up a collection config by name.
func (a *AppConfig) Collection(name string) (CollectionConfig, bool) {
	for _, c := range a.Collections {
		if c.Name == name {
			return c, true
		}
	}
	return CollectionConfig{}, false
}

// Load reads environment variables and _mongopuff collection documents.
func Load(ctx context.Context) (*AppConfig, error) {
	cfg, err := loadEnv()
	if err != nil {
		return nil, err
	}

	// TODO: connect to MongoDB and read _mongopuff documents.
	_ = ctx

	cfg.Global = GlobalConfig{}.Effective()
	return cfg, nil
}

func loadEnv() (*AppConfig, error) {
	port := 8080
	if s := os.Getenv("HEALTH_PORT"); s != "" {
		p, err := strconv.Atoi(s)
		if err != nil {
			return nil, fmt.Errorf("HEALTH_PORT: %w", err)
		}
		port = p
	}

	connStr := os.Getenv("MONGODB_CONNECTION_STRING")
	if connStr == "" {
		return nil, fmt.Errorf("MONGODB_CONNECTION_STRING is required")
	}

	apiKey := os.Getenv("TURBOPUFFER_API_KEY")
	if apiKey == "" {
		return nil, fmt.Errorf("TURBOPUFFER_API_KEY is required")
	}

	return &AppConfig{
		HealthPort:              port,
		LogLevel:                os.Getenv("LOG_LEVEL"),
		MongoDBConnectionString: connStr,
		TurbopufferAPIKey:       apiKey,
	}, nil
}
