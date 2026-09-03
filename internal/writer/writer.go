package writer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/tmccann21/mongopuff/internal/mongo"
	"github.com/tmccann21/mongopuff/internal/spool"
	"github.com/tmccann21/mongopuff/internal/transform"
	"github.com/tmccann21/mongopuff/internal/turbopuffer"
	tpuf "github.com/turbopuffer/turbopuffer-go/v2"
)

type TurbopufferClient interface {
	Write(ctx context.Context, namespace string, schema map[string]tpuf.AttributeSchemaConfigParam, actions []transform.Action) error
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
func (w *Writer) WriteBatch(ctx context.Context, namespace string, schema map[string]tpuf.AttributeSchemaConfigParam, actions []transform.Action) error {
	if len(actions) == 0 {
		return nil
	}

	if err := w.tpuf.Write(ctx, namespace, schema, actions); err != nil {
		return w.sendToDLQ(ctx, namespace, actions, err)
	}

	return nil
}

func (w *Writer) RunSpoolDelivery(ctx context.Context, sp *spool.Spool, namespace string, schema map[string]tpuf.AttributeSchemaConfigParam, startSegment uint32, signal <-chan struct{}) {
	nextDeliver := startSegment

	for {
		data, err := sp.Read(nextDeliver)
		if errors.Is(err, os.ErrNotExist) {
			select {
			case <-signal:
				continue
			case <-ctx.Done():
				return
			}
		}
		if err != nil {
			slog.Error("spool read error", "namespace", namespace, "segment", nextDeliver, "error", err)
			return
		}

		var actions []transform.Action
		if err := json.Unmarshal(data, &actions); err != nil {
			slog.Error("spool deserialize error", "namespace", namespace, "segment", nextDeliver, "error", err)
			nextDeliver++
			continue
		}

		flushStart := time.Now()
		if err := w.WriteBatch(ctx, namespace, schema, actions); err != nil {
			slog.Error("spool delivery failed", "namespace", namespace, "segment", nextDeliver, "error", err)
		}
		slog.Info("spool delivery",
			"namespace", namespace,
			"batchSize", len(actions),
			"segment", nextDeliver,
			"flushDuration", time.Since(flushStart),
		)

		sp.Remove(nextDeliver)
		nextDeliver++
	}
}

func (w *Writer) sendToDLQ(ctx context.Context, namespace string, actions []transform.Action, writeErr error) error {
	errKind := turbopuffer.ClassifyError(writeErr)
	slog.Error("turbopuffer write failed, sending to DLQ",
		"namespace", namespace,
		"actions", len(actions),
		"errorKind", errKind,
		"error", writeErr,
	)

	for _, a := range actions {
		entry := mongo.DLQEntry{
			Collection:   namespace,
			DocumentID:   a.DocumentID,
			Operation:    a.Operation,
			ErrorKind:    errKind,
			ErrorMessage: writeErr.Error(),
			ClusterTime:  a.ClusterTime,
			CreatedAt:    time.Now(),
		}
		if err := w.dlq.Write(ctx, entry); err != nil {
			return fmt.Errorf("DLQ write failed for document %s: %w", a.DocumentID, err)
		}
	}

	return nil
}

