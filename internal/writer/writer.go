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
// any namespace could be neither. collection is the MongoDB collection the
// actions came from, recorded on DLQ entries.
func (w *Writer) WriteBatch(ctx context.Context, collection string, schema map[string]tpuf.AttributeSchemaConfigParam, actions []transform.Action) error {
	partitions := partitionByNamespace(actions)

	// Each goroutine writes only its own slot, so errs needs no lock. A plain
	// WaitGroup rather than errgroup.WithContext: one namespace failing must not
	// cancel writes to the others.
	errs := make([]error, len(partitions))
	var wg sync.WaitGroup
	for i, p := range partitions {
		wg.Go(func() {
			errs[i] = w.writePartition(ctx, collection, schema, p)
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

func (w *Writer) writePartition(ctx context.Context, collection string, schema map[string]tpuf.AttributeSchemaConfigParam, p partition) error {
	writeErr := w.tpuf.Write(ctx, p.namespace, schema, p.actions)
	if writeErr == nil {
		return nil
	}
	if err := w.sendToDLQ(ctx, collection, p.namespace, p.actions, writeErr); err != nil {
		return fmt.Errorf("namespace %q: %w", p.namespace, err)
	}
	return nil
}

// RunSpoolDelivery writes collection's spool segments to turbopuffer in order,
// starting at startSegment, and removes each one once it has been written or
// DLQ'd. It waits on signal for new segments and returns nil when ctx is done.
//
// It returns an error, leaving the segment on disk for the next run, when a
// segment cannot be read, decoded, delivered, or removed after delivery. The
// segment's resume token is already saved, so the file is the only copy of its
// events. Delivery stops rather than moving past it, which also keeps segments
// in order: deletes are unconditional, so delivering a newer delete before an
// older upsert would let the upsert bring a deleted document back.
func (w *Writer) RunSpoolDelivery(ctx context.Context, sp *spool.Spool, collection string, schema map[string]tpuf.AttributeSchemaConfigParam, startSegment uint32, signal <-chan struct{}) error {
	nextDeliver := startSegment

	for {
		data, err := sp.Read(nextDeliver)
		if errors.Is(err, os.ErrNotExist) {
			select {
			case <-signal:
				continue
			case <-ctx.Done():
				return nil
			}
		}
		if err != nil {
			return fmt.Errorf("reading spool segment %d: %w", nextDeliver, err)
		}

		var actions []transform.Action
		if err := json.Unmarshal(data, &actions); err != nil {
			return fmt.Errorf("spool segment %d cannot be decoded; its events cannot be delivered and the collection needs a re-backfill: %w", nextDeliver, err)
		}

		flushStart := time.Now()
		if err := w.WriteBatch(ctx, collection, schema, actions); err != nil {
			if ctx.Err() != nil {
				return nil // shutting down; the next run delivers this segment
			}
			return fmt.Errorf("delivering spool segment %d: %w", nextDeliver, err)
		}
		slog.Info("spool delivery",
			"collection", collection,
			"batchSize", len(actions),
			"segment", nextDeliver,
			"flushDuration", time.Since(flushStart),
		)

		if err := sp.Remove(nextDeliver); err != nil {
			return fmt.Errorf("removing delivered spool segment %d: %w", nextDeliver, err)
		}
		nextDeliver++
	}
}

func (w *Writer) sendToDLQ(ctx context.Context, collection, namespace string, actions []transform.Action, writeErr error) error {
	errKind := turbopuffer.ClassifyError(writeErr)
	slog.Error("turbopuffer write failed, sending to DLQ",
		"collection", collection,
		"namespace", namespace,
		"actions", len(actions),
		"errorKind", errKind,
		"error", writeErr,
	)

	now := time.Now()
	entries := make([]mongo.DLQEntry, 0, len(actions))
	for _, a := range actions {
		entries = append(entries, mongo.DLQEntry{
			Collection:   collection,
			DocumentID:   a.DocumentID,
			Operation:    a.Operation,
			Namespace:    a.Namespace,
			ActionType:   dlqActionType(a.Type),
			ErrorKind:    errKind,
			ErrorMessage: writeErr.Error(),
			ClusterTime:  a.ClusterTime,
			CreatedAt:    now,
		})
	}
	if err := w.dlq.Write(ctx, entries...); err != nil {
		return fmt.Errorf("DLQ write failed for %d actions: %w", len(entries), err)
	}
	return nil
}

// dlqActionType gives the stored form of t. An unknown type is recorded rather
// than rejected: failing the DLQ write would stop the pipeline over a label.
func dlqActionType(t transform.ActionType) mongo.DLQActionType {
	switch t {
	case transform.ActionUpsert:
		return mongo.DLQActionUpsert
	case transform.ActionPatch:
		return mongo.DLQActionPatch
	case transform.ActionDelete:
		return mongo.DLQActionDelete
	default:
		return mongo.DLQActionType(fmt.Sprintf("unknown(%d)", t))
	}
}
