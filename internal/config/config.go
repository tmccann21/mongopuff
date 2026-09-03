package config

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"gopkg.in/yaml.v3"
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

type Embed struct {
	Model      string `yaml:"model"`
	Dimensions int    `yaml:"dimensions"`
	Attribute  string `yaml:"attribute"`
}

type FieldMapping struct {
	Name      string          `yaml:"name"`
	Type      FieldType       `yaml:"type"`
	Dimension int             `yaml:"dimension,omitempty"` // vector only
	Precision VectorPrecision `yaml:"precision,omitempty"` // vector only
	Filterable *bool          `yaml:"filterable,omitempty"`
	Embed      *Embed         `yaml:"embed,omitempty"`
}

type MappingConfig struct {
	Namespace string         `yaml:"namespace"`
	Fields    []FieldMapping `yaml:"fields"`
}

type CollectionConfig struct {
	Name             string        `yaml:"name"`
	BackfillPageSize int           `yaml:"backfillPageSize,omitempty"`
	MirrorDeletes    *bool         `yaml:"mirrorDeletes,omitempty"`
	Mapping          MappingConfig `yaml:"mapping"`
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
	BatchFlushCount  int `yaml:"batchFlushCount,omitempty"`
	BatchFlushTimeMs int `yaml:"batchFlushTimeMs,omitempty"`
	SpoolEnabled     bool `yaml:"spoolEnabled,omitempty"`
	SpoolDir         string `yaml:"spoolDir,omitempty"`
}

type ConfigFile struct {
	Collections []CollectionConfig `yaml:"collections"`
	Global GlobalConfig `yaml:"global"` 
}

const (
	DefaultBatchFlushCount  = 1024
	DefaultBatchFlushTimeMs = 1000 // 1s
	DefaultSpoolDir = "./data/spool"
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
	ConfigFilePath          string

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

	configFilePath := os.Getenv("CONFIG_FILE_PATH")
	if configFilePath == "" {
		configFilePath = "mongopuff.yaml"
	}

	return &AppConfig{
		HealthPort:              port,
		LogLevel:                os.Getenv("LOG_LEVEL"),
		MongoDBConnectionString: connStr,
		TurbopufferAPIKey:       apiKey,
		TurbopufferRegion:       region,
		ConfigFilePath:          configFilePath,
	}, nil
}

func LoadConfigFile(path string) ([]CollectionConfig, GlobalConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, GlobalConfig{}, err
	}

	var cfg ConfigFile
	err = yaml.Unmarshal(data, &cfg)
	if err != nil {
		return nil, GlobalConfig{}, err
	}

	return cfg.Collections, cfg.Global, nil
}
