package cmd

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
)

func runBackfill() error {
	fs := flag.NewFlagSet("backfill", flag.ContinueOnError)
	collection := fs.String("collection", "", "collection to backfill (required)")
	if err := fs.Parse(os.Args[2:]); err != nil {
		return err
	}

	if *collection == "" {
		return errors.New("--collection is required")
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	cfg, store, err := loadConfig(ctx)
	if err != nil {
		return err
	}

	collCfg, ok := cfg.Collection(*collection)
	if !ok {
		return fmt.Errorf("collection %q not found in _mongopuff config", *collection)
	}

	slog.Info("starting backfill", "collection", collCfg.Name)
	scanner := store.CollectionScanner(collCfg.Name)

	var lastID any
	for {
		opTime, err := store.PingOperationTime(ctx)
		if err != nil {
			return fmt.Errorf("ping operation time: %w", err)
		}

		slog.Info("got operation time", "collection", collCfg.Name, "operationTime", opTime)

		docs, err := scanner.ScanPage(ctx, lastID, collCfg.EffectiveBackfillPageSize())
		if err != nil {
			return fmt.Errorf("scan page: %w", err)
		}

		if len(docs) == 0 {
			slog.Info("no more docs", "collection", collCfg.Name, "lastID", lastID)
			break
		}

		slog.Info("scanned page", "collection", collCfg.Name, "docs", len(docs))

		lastID = docs[len(docs)-1]["_id"]
		slog.Info("last id", "collection", collCfg.Name, "id", lastID)

	}

	return nil
}
