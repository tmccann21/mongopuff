package writer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
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

// WriteBatch writes actions to turbopuffer with one API call per namespace,
// writing namespaces concurrently. When a namespace's write fails, its actions
// go to the DLQ; other namespaces are unaffected.
//
// WriteBatch returns only after every namespace has been written or DLQ'd, so
// the caller may save its resume token on a nil return. It returns an error if
// any namespace could be neither.
func (w *Writer) WriteBatch(ctx context.Context, schema map[string]tpuf.AttributeSchemaConfigParam, actions []transform.Action) error {
	partitions := partitionByNamespace(actions)

	// Each goroutine writes only its own slot, so errs needs no lock. A plain
	// WaitGroup rather than errgroup.WithContext: one namespace failing must not
	// cancel writes to the others.
	errs := make([]error, len(partitions))
	var wg sync.WaitGroup
	for i, p := range partitions {
		wg.Go(func() {
			errs[i] = w.writePartition(ctx, schema, p)
		})
	}
	wg.Wait()

	return errors.Join(errs...)
}

// partition is the set of actions bound for one turbopuffer namespace.
type partition struct {
	namespace string
	actions   []transform.Action
}

// partitionByNamespace groups actions by Namespace. Partitions appear in the
// order their namespace first occurs, and each keeps its actions' relative order.
func partitionByNamespace(actions []transform.Action) []partition {
	var partitions []partition
	index := make(map[string]int)
	for _, a := range actions {
		i, ok := index[a.Namespace]
		if !ok {
			i = len(partitions)
			index[a.Namespace] = i
			partitions = append(partitions, partition{namespace: a.Namespace})
		}
		partitions[i].actions = append(partitions[i].actions, a)
	}
	return partitions
}

func (w *Writer) writePartition(ctx context.Context, schema map[string]tpuf.AttributeSchemaConfigParam, p partition) error {
	writeErr := w.tpuf.Write(ctx, p.namespace, schema, p.actions)
	if writeErr == nil {
		return nil
	}
	if err := w.sendToDLQ(ctx, p.namespace, p.actions, writeErr); err != nil {
		return fmt.Errorf("namespace %q: %w", p.namespace, err)
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
		if err := w.WriteBatch(ctx, schema, actions); err != nil {
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
