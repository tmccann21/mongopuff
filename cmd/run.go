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

	dbName, _ := mongo.ParseDatabaseName(cfg.MongoDBConnectionString)
	slog.Info("starting mongopuff CDC",
		"collections", len(cfg.Collections),
		"database", dbName,
	)

	status := health.NewStatus()
	mux := http.NewServeMux()
	status.Register(mux)
	go func() {
		addr := fmt.Sprintf(":%d", cfg.HealthPort)
		slog.Info("http server starting", "addr", addr)
		if err := http.ListenAndServe(addr, mux); err != nil {
			slog.Error("http server error", "error", err)
		}
	}()

	dlqWriter := store.NewDLQWriter()
	w := writer.New(turbopuffer.New(cfg.TurbopufferAPIKey, "aws-us-west-2"), dlqWriter)

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
	batcher := batch.New(ctx, cfg.Global, func(
		ctx context.Context, actions []transform.Action, resumeToken []byte,
	) error {
		slog.Info("flush",
			"namespace", namespace,
			"actions", len(actions),
		)
		w.WriteBatch(ctx, namespace, actions)

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
		)

		action, err := transform.MapChangeEvent(event, coll)
		if err != nil {
			slog.Error("transform error", "collection", coll.Name, "error", err)
			return err
		}

		if err := batcher.Add(action, stream.ResumeToken()); err != nil {
			return fmt.Errorf("batch flush failed: %w", err)
		}
	}

	if err := batcher.Flush(); err != nil {
		slog.Error("final flush failed", "collection", coll.Name, "error", err)
	}

	if _, err := stream.Event(); err != nil {
		return fmt.Errorf("change stream error: %w", err)
	}

	return nil
}
