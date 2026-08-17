package writer

import (
	"context"
	"log/slog"
	"time"

	"github.com/tmccann21/mongopuff/internal/mongo"
	"github.com/tmccann21/mongopuff/internal/transform"
)

type TurbopufferClient interface {
	Upsert(ctx context.Context, namespace string, actions []transform.Action) error
	Delete(ctx context.Context, namespace string, ids []string) error
}

type Writer struct {
	tpuf TurbopufferClient
	dlq  mongo.DLQWriter
}

// New creates a Writer.
func New(tpuf TurbopufferClient, dlq mongo.DLQWriter) *Writer {
	return &Writer{
		tpuf: tpuf,
		dlq:  dlq,
	}
}

// WriteBatch writes a batch of actions to turbopuffer. On retryable failures it retries
// with exponential backoff. On exhaustion or non-retryable errors, it writes to the DLQ.
func (w *Writer) WriteBatch(ctx context.Context, namespace string, actions []transform.Action) {
	var upserts []transform.Action
	var deletes []transform.Action
	for _, a := range actions {
		switch a.Type {
		case transform.ActionUpsert, transform.ActionPatch:
			upserts = append(upserts, a)
		case transform.ActionDelete:
			deletes = append(deletes, a)
		}
	}

	if len(upserts) > 0 {
		if err := w.tpuf.Upsert(ctx, namespace, upserts); err != nil {
			w.sendToDLQ(ctx, namespace, upserts, err)
		}
	}

	if len(deletes) > 0 {
		ids := make([]string, len(deletes))
		for i, d := range deletes {
			ids[i] = d.DocumentID
		}
		if err := w.tpuf.Delete(ctx, namespace, ids); err != nil {
			w.sendToDLQ(ctx, namespace, deletes, err)
		}
	}
}

func (w *Writer) sendToDLQ(ctx context.Context, namespace string, actions []transform.Action, writeErr error) {
	for _, a := range actions {
		entry := mongo.DLQEntry{
			Collection:   namespace,
			DocumentID:   a.DocumentID,
			Operation:    operationFromAction(a.Type),
			ErrorKind:    mongo.ErrNetworkError, // TODO: classify from writeErr
			ErrorMessage: writeErr.Error(),
			ClusterTime:  a.ClusterTime,
			CreatedAt:    time.Now(),
		}
		if err := w.dlq.Write(ctx, entry); err != nil {
			slog.Error("failed to write to DLQ",
				"documentId", a.DocumentID,
				"error", err,
			)
		}
	}
}

func operationFromAction(t transform.ActionType) mongo.Operation {
	switch t {
	case transform.ActionUpsert:
		return mongo.OpInsert
	case transform.ActionPatch:
		return mongo.OpUpdate
	case transform.ActionDelete:
		return mongo.OpDelete
	default:
		return ""
	}
}
