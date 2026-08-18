package writer

import (
	"context"
	"log/slog"
	"time"

	"github.com/tmccann21/mongopuff/internal/mongo"
	"github.com/tmccann21/mongopuff/internal/transform"
	"github.com/tmccann21/mongopuff/internal/turbopuffer"
)

type TurbopufferClient interface {
	Write(ctx context.Context, namespace string, actions []transform.Action) error
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

// WriteBatch writes a batch of actions to turbopuffer in a single API call.
// On failure, writes all actions to the DLQ.
func (w *Writer) WriteBatch(ctx context.Context, namespace string, actions []transform.Action) {
	if len(actions) == 0 {
		return
	}

	if err := w.tpuf.Write(ctx, namespace, actions); err != nil {
		w.sendToDLQ(ctx, namespace, actions, err)
	}
}

func (w *Writer) sendToDLQ(ctx context.Context, namespace string, actions []transform.Action, writeErr error) {
	for _, a := range actions {
		entry := mongo.DLQEntry{
			Collection:   namespace,
			DocumentID:   a.DocumentID,
			Operation:    operationFromAction(a.Type),
			ErrorKind:    turbopuffer.ClassifyError(writeErr),
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
