package cmd

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/tmccann21/mongopuff/internal/batch"
	"github.com/tmccann21/mongopuff/internal/config"
	"github.com/tmccann21/mongopuff/internal/health"
	"github.com/tmccann21/mongopuff/internal/writer"
	"github.com/tmccann21/mongopuff/internal/turbopuffer"
	"github.com/tmccann21/mongopuff/internal/mongo"
	"github.com/tmccann21/mongopuff/internal/transform"
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
			if err := runCollectionCDC(ctx, cfg, store, w, dlqWriter, coll, status); err != nil {
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

func runCollectionCDC(ctx context.Context, cfg *config.AppConfig, store *mongo.Store, w *writer.Writer, dlqWriter mongo.DLQWriter, coll config.CollectionConfig, status *health.Status) error {
	slog.Info("starting CDC", "collection", coll.Name, "namespace", coll.Mapping.Namespace)

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
				Collection: coll.Name,
				DocumentID: docID,
				Operation:  event.Operation,
				ErrorKind:  errKind,
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

	if err := batcher.Close(ctx); err != nil {
		slog.Error("final flush failed", "collection", coll.Name, "error", err)
	}

	if _, err := stream.Event(); err != nil {
		return fmt.Errorf("change stream error: %w", err)
	}

	return nil
}
