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

	"github.com/tmccann21/mongopuff/internal/config"
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

	cfg, err := config.Load(ctx)
	if err != nil {
		return fmt.Errorf("loading config: %w", err)
	}

	if err := config.Validate(cfg); err != nil {
		return fmt.Errorf("validating config: %w", err)
	}

	collCfg, ok := cfg.Collection(*collection)
	if !ok {
		return fmt.Errorf("collection %q not found in _mongopuff config", *collection)
	}

	slog.Info("starting backfill", "collection", collCfg.Name)

	// TODO: wire up backfill loop: ping → scan page → upsert → advance cursor
	_ = ctx

	return nil
}
