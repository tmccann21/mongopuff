package cmd

import (
	"github.com/tmccann21/mongopuff/internal/config"
	"github.com/tmccann21/mongopuff/internal/mongo"
	"github.com/tmccann21/mongopuff/internal/writer"
	tpuf "github.com/turbopuffer/turbopuffer-go/v2"
)

// collectionPipeline TODO: document.
type collectionPipeline struct {
	coll   config.CollectionConfig
	schema map[string]tpuf.AttributeSchemaConfigParam
	writer *writer.Writer
	dlq    mongo.DLQWriter
	store  *mongo.Store
}
