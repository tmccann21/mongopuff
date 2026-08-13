package cmd

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os/signal"
	"sync"
	"syscall"

	"github.com/tmccann21/mongopuff/internal/config"
	"github.com/tmccann21/mongopuff/internal/health"
	mpmongo "github.com/tmccann21/mongopuff/internal/mongo"
)

func runCDC() error {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	cfg, err := config.Load(ctx)
	if err != nil {
		return fmt.Errorf("loading config: %w", err)
	}

	if err := config.Validate(cfg); err != nil {
		return fmt.Errorf("validating config: %w", err)
	}

	dbName, _ := mpmongo.ParseDatabaseName(cfg.MongoDBConnectionString)
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

	// Spawn a goroutine per collection.
	var wg sync.WaitGroup
	for _, coll := range cfg.Collections {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := runCollectionCDC(ctx, cfg, coll, status); err != nil {
				slog.Error("collection CDC stopped", "collection", coll.Name, "error", err)
			}
		}()
	}

	wg.Wait()
	slog.Info("mongopuff shut down")
	return nil
}

func runCollectionCDC(ctx context.Context, cfg *config.AppConfig, coll config.CollectionConfig, status *health.Status) error {
	_ = ctx
	_ = cfg
	_ = coll
	_ = status
	// TODO: wire up change stream → transform → batch → write pipeline
	return nil
}
