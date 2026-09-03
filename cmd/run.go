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
)

func runCDC() error {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	cfg, store, err := loadConfig(ctx)
	if err != nil {
		return err
	}
	defer store.Close(context.Background())

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
		wg.Add(1)
		go func() {
			defer wg.Done()
			var err error
			if cfg.Global.SpoolEnabled {
				err = runCollectionCDCSpooled(ctx, cfg, store, w, dlqWriter, coll, status)
			} else {
				err = runCollectionCDCDirect(ctx, cfg, store, w, dlqWriter, coll, status)
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

func consumeStream(ctx context.Context, stream *mongo.LiveChangeStream, batcher *batch.Batcher, coll config.CollectionConfig, dlqWriter mongo.DLQWriter) error {
	for stream.Next(ctx) {
		event, err := stream.Event()
		if err != nil {
			slog.Error("change stream event error", "collection", coll.Name, "error", err)
			continue
		}

		slog.Debug("change event",
			"collection", coll.Name,
			"operation", event.Operation,
			"documentId", event.DocumentID,
			"clusterTime", event.ClusterTime,
			"clockTime", time.Now(),
		)

		action, err := transform.MapChangeEvent(event, coll)
		if err != nil {
			slog.Error("transform error", "collection", coll.Name, "error", err)
			docID, idErr := transform.SerializeID(event.DocumentID)
			errKind := mongo.ErrTypeMismatch
			if idErr != nil {
				docID = fmt.Sprintf("%v", event.DocumentID)
				errKind = mongo.ErrIDMissing
			}
			err = dlqWriter.Write(ctx, mongo.DLQEntry{
				Collection:   coll.Name,
				DocumentID:   docID,
				Operation:    event.Operation,
				ErrorKind:    errKind,
				ErrorMessage: err.Error(),
				ClusterTime:  event.ClusterTime,
				CreatedAt:    time.Now(),
			})
			if err != nil {
				slog.Error("DLQ write error", "collection", coll.Name, "error", err)
			}
			continue
		}

		if err := batcher.Add(ctx, action, stream.ResumeToken()); err != nil {
			return fmt.Errorf("batch flush failed: %w", err)
		}
	}

	if err := batcher.Flush(ctx); err != nil {
		slog.Error("final flush failed", "collection", coll.Name, "error", err)
	}

	if _, err := stream.Event(); err != nil {
		var serverErr mongodriver.ServerError
		if errors.As(err, &serverErr) && serverErr.HasErrorLabel("NonResumableChangeStreamError") {
			slog.Error("oplog position lost: change stream is non-resumable, re-backfill required",
				"collection", coll.Name, "error", err)
			return fmt.Errorf("oplog position lost (re-backfill required): %w", err)
		}
		return fmt.Errorf("change stream error: %w", err)
	}
	return nil
}

// runCollectionCDCDirect is the default path: change stream → batcher → turbopuffer.
func runCollectionCDCDirect(ctx context.Context, cfg *config.AppConfig, store *mongo.Store, w *writer.Writer, dlqWriter mongo.DLQWriter, coll config.CollectionConfig, status *health.Status) error {
	slog.Info("starting CDC (direct)", "collection", coll.Name, "namespace", coll.Mapping.Namespace)

	state, err := store.LoadCollectionState(ctx, coll.Name)
	if err != nil {
		return err
	}

	stream, err := store.OpenChangeStream(ctx, coll.Name, state.ChangeStreamResumeToken)
	if err != nil {
		return err
	}
	defer stream.Close(ctx)

	namespace := coll.Mapping.Namespace
	schema := turbopuffer.BuildSchema(coll.Mapping.Fields)

	batcher := batch.New(ctx, cfg.Global, func(
		ctx context.Context, actions []transform.Action, resumeToken []byte,
	) error {
		flushStart := time.Now()
		if err := w.WriteBatch(ctx, namespace, schema, actions); err != nil {
			return fmt.Errorf("write batch: %w", err)
		}
		slog.Info("flush",
			"namespace", namespace,
			"batchSize", len(actions),
			"flushDuration", time.Since(flushStart),
		)

		if err := store.SaveResumeToken(ctx, coll.Name, resumeToken); err != nil {
			slog.Error("failed to save resume token", "collection", coll.Name, "error", err)
		}

		status.SetCollectionFlushTime(coll.Name, time.Now())
		return nil
	})

	return consumeStream(ctx, stream, batcher, coll, dlqWriter)
}

// runCollectionCDCSpooled is the durable path: change stream → batcher → spool → delivery → turbopuffer.
func runCollectionCDCSpooled(ctx context.Context, cfg *config.AppConfig, store *mongo.Store, w *writer.Writer, dlqWriter mongo.DLQWriter, coll config.CollectionConfig, status *health.Status) error {
	slog.Info("starting CDC (spooled)", "collection", coll.Name, "namespace", coll.Mapping.Namespace)

	state, err := store.LoadCollectionState(ctx, coll.Name)
	if err != nil {
		return err
	}

	stream, err := store.OpenChangeStream(ctx, coll.Name, state.ChangeStreamResumeToken)
	if err != nil {
		return err
	}
	defer stream.Close(ctx)

	spoolDir := cfg.Global.SpoolDir
	if spoolDir == "" {
		spoolDir = config.DefaultSpoolDir
	}
	spoolDir = filepath.Join(spoolDir, coll.Name)
	sp, err := spool.Open(spoolDir)
	if err != nil {
		return fmt.Errorf("opening spool: %w", err)
	}
	defer sp.Close()

	namespace := coll.Mapping.Namespace
	schema := turbopuffer.BuildSchema(coll.Mapping.Fields)
	nextSegment := state.SpoolSegment + 1
	signal := make(chan struct{}, 1)

	batcher := batch.New(ctx, cfg.Global, func(
		ctx context.Context, actions []transform.Action, resumeToken []byte,
	) error {
		data, err := json.Marshal(actions)
		if err != nil {
			return fmt.Errorf("serialize batch: %w", err)
		}

		if err := sp.Write(nextSegment, data); err != nil {
			return fmt.Errorf("spool write: %w", err)
		}

		if err := store.SaveResumeToken(ctx, coll.Name, resumeToken); err != nil {
			slog.Error("failed to save resume token", "collection", coll.Name, "error", err)
		}
		if err := store.SaveSpoolSegment(ctx, coll.Name, nextSegment); err != nil {
			slog.Error("failed to save spool segment", "collection", coll.Name, "error", err)
		}

		slog.Info("spool flush",
			"namespace", namespace,
			"batchSize", len(actions),
			"segment", nextSegment,
		)

		nextSegment++

		select {
		case signal <- struct{}{}:
		default:
		}
		return nil
	})

	// Determine where delivery starts: lowest existing segment on disk, or next expected.
	segments, err := sp.Segments()
	if err != nil {
		return fmt.Errorf("listing spool segments: %w", err)
	}
	startDeliver := nextSegment
	if len(segments) > 0 {
		startDeliver = segments[0]
	}

	var deliveryWg sync.WaitGroup
	deliveryWg.Add(1)
	go func() {
		defer deliveryWg.Done()
		w.RunSpoolDelivery(ctx, sp, namespace, schema, startDeliver, signal)
	}()

	err = consumeStream(ctx, stream, batcher, coll, dlqWriter)
	deliveryWg.Wait()
	return err
}
