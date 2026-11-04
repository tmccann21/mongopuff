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

// DLQActionType is the turbopuffer write that a DLQ entry's action failed to
// make. It is stored, so its values must not change.
type DLQActionType string

const (
	DLQActionUpsert DLQActionType = "upsert"
	DLQActionPatch  DLQActionType = "patch"
	DLQActionDelete DLQActionType = "delete"
)

type DLQEntry struct {
	// Collection is the source MongoDB collection, never a turbopuffer namespace.
	Collection string    `bson:"collection"`
	DocumentID string    `bson:"documentId"`
	Operation  Operation `bson:"operation"`
	// Namespace and ActionType record the turbopuffer write that failed. Both
	// are empty when the event failed before it became a write, such as a
	// transform failure.
	Namespace    string        `bson:"namespace,omitempty"`
	ActionType   DLQActionType `bson:"actionType,omitempty"`
	ErrorKind    ErrorKind     `bson:"errorKind"`
	ErrorMessage string        `bson:"errorMessage"`
	ClusterTime  uint64        `bson:"clusterTime"`
	CreatedAt    time.Time     `bson:"createdAt"`
}

// CollectionState is the shape of a document in the _mongopuff_state collection.
// Managed by the service.
type CollectionState struct {
	Name                    string    `bson:"_id"`
	ChangeStreamResumeToken []byte    `bson:"changeStreamResumeToken,omitempty"`
	BackfillCursor          any       `bson:"backfillCursor,omitempty"`
	LastFlushTime           time.Time `bson:"lastFlushTime,omitempty"`
}

// ChangeEvent represents a parsed MongoDB change stream event.
type ChangeEvent struct {
	Operation                Operation
	DocumentID               any            // raw BSON _id value
	FullDocument             map[string]any // present for insert/replace, may be present for update
	FullDocumentBeforeChange map[string]any // present for delete (and update) when pre-images are enabled
	UpdatedFields            map[string]any // present for update operations
	RemovedFields            []string       // present for update operations
	ClusterTime              uint64         // serialized BSON Timestamp: (T<<32)|I
	ResumeToken              []byte         // opaque token for resuming
}

type DLQWriter interface {
	Write(ctx context.Context, entry DLQEntry) error
}
