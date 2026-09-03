package mongo

import (
	"context"
	"time"
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

type DLQEntry struct {
	Collection   string    `bson:"collection"`
	DocumentID   string    `bson:"documentId"`
	Operation    Operation `bson:"operation"`
	ErrorKind    ErrorKind `bson:"errorKind"`
	ErrorMessage string    `bson:"errorMessage"`
	ClusterTime  uint64    `bson:"clusterTime"`
	CreatedAt    time.Time `bson:"createdAt"`
}

// CollectionState is the shape of a document in the _mongopuff_state collection.
// Managed by the service.
type CollectionState struct {
	Name                    string    `bson:"_id"`
	ChangeStreamResumeToken []byte    `bson:"changeStreamResumeToken,omitempty"`
	BackfillCursor          any       `bson:"backfillCursor,omitempty"`
	LastFlushTime           time.Time `bson:"lastFlushTime,omitempty"`
	SpoolSegment            uint32    `bson:"spoolSegment,omitempty"`
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

type DLQWriter interface {
	Write(ctx context.Context, entry DLQEntry) error
}
