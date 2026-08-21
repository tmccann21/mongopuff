package config

import (
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
	Filterable *bool           `bson:"filterable,omitempty"`
}

type MappingConfig struct {
	Namespace string         `bson:"namespace"`
	Fields    []FieldMapping `bson:"fields"`
}

// CollectionConfig is the shape of a document in the _mongopuff collection.
// Managed by the operator.
type CollectionConfig struct {
	Name             string        `bson:"_id"`
	BackfillPageSize int           `bson:"backfillPageSize,omitempty"`
	MirrorDeletes    *bool         `bson:"mirrorDeletes,omitempty"`
	Mapping          MappingConfig `bson:"mapping"`
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
	BatchFlushTimeMs int `bson:"batchFlushTimeMs,omitempty"`
}

const (
	DefaultBatchFlushCount  = 1024
	DefaultBatchFlushTimeMs = 1000 // 1s
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
	TurbopufferRegion       string

	Global      GlobalConfig
	Collections []CollectionConfig
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

func LoadEnv() (*AppConfig, error) {
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

	region := os.Getenv("TURBOPUFFER_REGION")
	if region == "" {
		region = "aws-us-west-2"
	}

	return &AppConfig{
		HealthPort:              port,
		LogLevel:                os.Getenv("LOG_LEVEL"),
		MongoDBConnectionString: connStr,
		TurbopufferAPIKey:       apiKey,
		TurbopufferRegion:       region,
	}, nil
}
