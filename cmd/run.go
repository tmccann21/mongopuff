package cmd

import (
	"context"
	"errors"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/tmccann21/mongopuff/internal/batch"
	"github.com/tmccann21/mongopuff/internal/config"
	"github.com/tmccann21/mongopuff/internal/health"
	"github.com/tmccann21/mongopuff/internal/mongo"
	"github.com/tmccann21/mongopuff/internal/spool"
	"github.com/tmccann21/mongopuff/internal/transform"
	"github.com/tmccann21/mongopuff/internal/turbopuffer"
	"github.com/tmccann21/mongopuff/internal/writer"

	mongodriver "go.mongodb.org/mongo-driver/v2/mongo"
	"golang.org/x/sync/errgroup"
)

func runCDC() error {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	cfg, store, err := loadConfig(ctx)
	if err != nil {
		return err
	}
	defer store.Close(context.Background())

	if err := checkPreImages(ctx, store, cfg.Collections); err != nil {
		return fmt.Errorf("pre-image check: %w", err)
	}

	slog.Info("starting mongopuff CDC",
		"collections", len(cfg.Collections),
		"database", store.DatabaseName(),
		"spool", cfg.Global.SpoolEnabled,
	)

	status := health.NewStatus()
	mux := http.NewServeMux()
	status.Register(mux)
	srv := &http.Server{
		Addr:    fmt.Sprintf(":%d", cfg.HealthPort),
		Handler: mux,
	}
	go func() {
		slog.Info("http server starting", "addr", srv.Addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("http server error", "error", err)
		}
	}()

	dlqWriter := store.NewDLQWriter()
	w := writer.New(turbopuffer.New(cfg.TurbopufferAPIKey, cfg.TurbopufferRegion), dlqWriter)

	// Spawn a goroutine per collection.
	var wg sync.WaitGroup
	for _, coll := range cfg.Collections {
		p := &collectionPipeline{
			coll:   coll,
			schema: turbopuffer.BuildSchema(coll.Mapping.Fields),
			writer: w,
			dlq:    dlqWriter,
			store:  store,
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			var err error
			if cfg.Global.SpoolEnabled {
				err = p.runCDCSpooled(ctx, cfg.Global)
			} else {
				err = p.runCDCDirect(ctx, cfg.Global, status)
			}
			if err != nil {
				slog.Error("collection CDC stopped", "collection", coll.Name, "error", err)
			}
		}()
	}

	wg.Wait()
	if err := srv.Shutdown(context.Background()); err != nil {
		slog.Error("http server shutdown error", "error", err)
	}
	slog.Info("mongopuff shut down")
	return nil
}

// errRebackfillRequired means the change stream cannot be resumed, so the
// saved resume token is useless and the collection must be re-backfilled.
var errRebackfillRequired = errors.New("oplog position lost (re-backfill required)")

// changeStream is the part of *mongo.LiveChangeStream that consumeStream uses.
type changeStream interface {
	Next(ctx context.Context) bool
	Event() mongo.ChangeEvent
	Err() error
	ResumeToken() []byte
}

func consumeStream(ctx context.Context, stream changeStream, batcher *batch.Batcher, coll config.CollectionConfig, dlqWriter mongo.DLQWriter) error {
	for stream.Next(ctx) {
		event := stream.Event()

		slog.Debug("change event",
			"collection", coll.Name,
			"operation", event.Operation,
			"documentId", event.DocumentID,
			"clusterTime", event.ClusterTime,
			"clockTime", time.Now(),
		)

		actions, err := transform.MapChangeEvent(event, coll)
		if err != nil {
			slog.Error("transform error", "collection", coll.Name, "error", err)
			entry := transformFailureEntry(coll.Name, event.DocumentID, event.Operation, event.ClusterTime, err)
			// An event in neither turbopuffer nor the DLQ must stop the stream:
			// the next flush would save a resume token past it.
			if err := dlqWriter.Write(ctx, entry); err != nil {
				return fmt.Errorf("DLQ write for document %v: %w", event.DocumentID, err)
			}
			continue
		}

		if err := batcher.Add(ctx, actions, stream.ResumeToken()); err != nil {
			return fmt.Errorf("batch flush failed: %w", err)
		}
	}

	if err := batcher.Flush(ctx); err != nil {
		slog.Error("final flush failed", "collection", coll.Name, "error", err)
	}

	if err := stream.Err(); err != nil {
		var serverErr mongodriver.ServerError
		if errors.As(err, &serverErr) && serverErr.HasErrorLabel("NonResumableChangeStreamError") {
			slog.Error("oplog position lost: change stream is non-resumable, re-backfill required",
				"collection", coll.Name, "error", err)
			return fmt.Errorf("%w: %w", errRebackfillRequired, err)
		}
		return fmt.Errorf("change stream error: %w", err)
	}
	return nil
}

// transformFailureEntry builds the DLQ entry for a document that could not be
// transformed. The change stream and backfill share it so that a failure is
// classified the same way on both paths.
func transformFailureEntry(collection string, id any, op mongo.Operation, clusterTime uint64, transformErr error) mongo.DLQEntry {
	docID, err := transform.SerializeID(id)
	if err != nil {
		// Fall back to the printed _id so an operator can still find the document.
		docID = fmt.Sprintf("%v", id)
	}
	return mongo.DLQEntry{
		Collection:   collection,
		DocumentID:   docID,
		Operation:    op,
		ErrorKind:    transformErrorKind(transformErr),
		ErrorMessage: transformErr.Error(),
		ClusterTime:  clusterTime,
		CreatedAt:    time.Now(),
	}
}

// transformErrorKind classifies a transform failure by the sentinel it wraps.
func transformErrorKind(err error) mongo.ErrorKind {
	switch {
	case errors.Is(err, transform.ErrInvalidID):
		return mongo.ErrIDMissing
	case errors.Is(err, transform.ErrNamespaceUnresolved):
		return mongo.ErrNamespaceUnresolved
	case errors.Is(err, transform.ErrTypeMismatch):
		return mongo.ErrTypeMismatch
	default:
		return mongo.ErrUnknown
	}
}

// runCDCDirect is the default path: change stream → batcher → turbopuffer.
func (p *collectionPipeline) runCDCDirect(ctx context.Context, global config.GlobalConfig, status *health.Status) error {
	slog.Info("starting CDC (direct)", "collection", p.coll.Name, "namespace", p.coll.Mapping.Namespace.String())

	state, err := p.store.LoadCollectionState(ctx, p.coll.Name)
	if err != nil {
		return err
	}

	stream, err := p.store.OpenChangeStream(ctx, p.coll.Name, state.ChangeStreamResumeToken, p.coll.Mapping.Namespace.IsDynamic())
	if err != nil {
		return err
	}
	defer stream.Close(ctx)

	namespace := p.coll.Mapping.Namespace.String()

	batcher := batch.New(ctx, global, func(
		ctx context.Context, actions []transform.Action, resumeToken []byte,
	) error {
		flushStart := time.Now()
		if err := p.writer.WriteBatch(ctx, p.coll.Name, p.schema, actions); err != nil {
			return fmt.Errorf("write batch: %w", err)
		}
		slog.Info("flush",
			"namespace", namespace,
			"batchSize", len(actions),
			"flushDuration", time.Since(flushStart),
		)

		if err := p.store.SaveResumeToken(ctx, p.coll.Name, resumeToken); err != nil {
			slog.Error("failed to save resume token", "collection", p.coll.Name, "error", err)
		}

		status.SetCollectionFlushTime(p.coll.Name, time.Now())
		return nil
	})

	return consumeStream(ctx, stream, batcher, p.coll, p.dlq)
}

// runCDCSpooled is the durable path: change stream → batcher → spool → delivery → turbopuffer.
func (p *collectionPipeline) runCDCSpooled(ctx context.Context, global config.GlobalConfig) error {
	slog.Info("starting CDC (spooled)", "collection", p.coll.Name, "namespace", p.coll.Mapping.Namespace.String())

	state, err := p.store.LoadCollectionState(ctx, p.coll.Name)
	if err != nil {
		return err
	}

	stream, err := p.store.OpenChangeStream(ctx, p.coll.Name, state.ChangeStreamResumeToken, p.coll.Mapping.Namespace.IsDynamic())
	if err != nil {
		return err
	}
	defer stream.Close(ctx)

	spoolDir := global.SpoolDir
	if spoolDir == "" {
		spoolDir = config.DefaultSpoolDir
	}
	spoolDir = filepath.Join(spoolDir, p.coll.Name)
	sp, err := spool.Open(spoolDir)
	if err != nil {
		return fmt.Errorf("opening spool: %w", err)
	}
	defer sp.Close()

	namespace := p.coll.Mapping.Namespace.String()
	signal := make(chan struct{}, 1)

	batcher := batch.New(ctx, global, func(
		ctx context.Context, actions []transform.Action, resumeToken []byte,
	) error {
		data, err := json.Marshal(actions)
		if err != nil {
			return fmt.Errorf("serialize batch: %w", err)
		}

		segment, err := sp.Append(data)
		if err != nil {
			return fmt.Errorf("spool write: %w", err)
		}

		if err := p.store.SaveResumeToken(ctx, p.coll.Name, resumeToken); err != nil {
			slog.Error("failed to save resume token", "collection", p.coll.Name, "error", err)
		}

		slog.Info("spool flush",
			"namespace", namespace,
			"batchSize", len(actions),
			"segment", segment,
		)

		select {
		case signal <- struct{}{}:
		default:
		}
		return nil
	})

	// Delivery and the change stream stop together: whichever fails first
	// cancels the other, and Wait returns its error.
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return p.writer.RunSpoolDelivery(gctx, sp, p.coll.Name, p.schema, sp.First(), signal)
	})
	g.Go(func() error {
		return consumeStream(gctx, stream, batcher, p.coll, p.dlq)
	})
	return g.Wait()
}
