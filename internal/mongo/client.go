package mongo

import (
	"context"
	"time"

	"github.com/tmccann21/mongopuff/internal/config"
)

type Operation string

const (
	OpInsert     Operation = "insert"
	OpUpdate     Operation = "update"
	OpReplace    Operation = "replace"
	OpDelete     Operation = "delete"
	OpInvalidate Operation = "invalidate"
	OpRename     Operation = "rename"
	OpDrop       Operation = "drop"
)

type ErrorKind string

const (
	ErrTypeMismatch  ErrorKind = "type_mismatch"
	ErrIDMissing     ErrorKind = "id_missing"
	ErrWriteRejected ErrorKind = "write_rejected"
	ErrNetworkError  ErrorKind = "network_error"
	ErrRateLimited   ErrorKind = "rate_limited"
	ErrServerError   ErrorKind = "server_error"
)

func (k ErrorKind) IsRetryable() bool {
	switch k {
	case ErrNetworkError, ErrRateLimited, ErrServerError:
		return true
	default:
		return false
	}
}

type DLQEntry struct {
	Collection   string    `bson:"collection"`
	DocumentID   string    `bson:"documentId"`
	Operation    Operation `bson:"operation"`
	ErrorKind    ErrorKind `bson:"errorKind"`
	ErrorMessage string    `bson:"errorMessage"`
	ClusterTime  uint64    `bson:"clusterTime"`
	CreatedAt    time.Time `bson:"createdAt"`
}

// ChangeEvent represents a parsed MongoDB change stream event.
type ChangeEvent struct {
	Operation     Operation
	DocumentID    any            // raw BSON _id value
	FullDocument  map[string]any // present for insert/replace, may be present for update
	UpdatedFields map[string]any // present for update operations
	RemovedFields []string       // present for update operations
	ClusterTime   uint64         // serialized BSON Timestamp: (T<<32)|I
	ResumeToken   []byte         // opaque token for resuming
}

type ChangeStreamIterator interface {
	Next(ctx context.Context) bool
	Event() (ChangeEvent, error)
	ResumeToken() []byte
	Close(ctx context.Context) error
}

type BackfillScanner interface {
	ScanPage(ctx context.Context, afterID any, pageSize int) ([]map[string]any, error)
	PingOperationTime(ctx context.Context) (uint64, error)
}

type ConfigLoader interface {
	LoadCollectionConfigs(ctx context.Context) ([]config.CollectionConfig, error)
	LoadGlobalConfig(ctx context.Context) (config.GlobalConfig, error)
}

type CDCCheckpointer interface {
	SaveResumeToken(ctx context.Context, collection string, token []byte) error
	SaveLastFlushTime(ctx context.Context, collection string, t time.Time) error
}

type BackfillCheckpointer interface {
	SaveBackfillCursor(ctx context.Context, collection string, lastID any) error
}

type DLQWriter interface {
	Write(ctx context.Context, entry DLQEntry) error
}
