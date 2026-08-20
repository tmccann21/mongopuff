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

	"github.com/tmccann21/mongopuff/internal/writer"
	"github.com/tmccann21/mongopuff/internal/turbopuffer"
	"github.com/tmccann21/mongopuff/internal/transform"
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

	dlqWriter := store.NewDLQWriter()
	w := writer.New(turbopuffer.New(cfg.TurbopufferAPIKey, "aws-us-west-2"), dlqWriter)

	schema := turbopuffer.BuildSchema(collCfg.Mapping.Fields)

	slog.Info("starting backfill", "collection", collCfg.Name)
	scanner := store.CollectionScanner(collCfg.Name)
	state, err := store.LoadCollectionState(ctx, collCfg.Name)
	if err != nil {
		return err
	}
	lastID := state.BackfillCursor
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
		actions := make([]transform.Action, 0, len(docs))
		for _, doc := range docs {
			action, err := transform.MapDocument(doc, opTime, collCfg)
			if err != nil {
				return fmt.Errorf("map document: %w", err)
			}
			actions = append(actions, action)
		}

		w.WriteBatch(ctx, collCfg.Mapping.Namespace, schema, actions)
		lastID = docs[len(docs)-1]["_id"]

		if err := store.SaveBackfillCursor(ctx, collCfg.Name, lastID); err != nil {
			return fmt.Errorf("saving backfill cursor: %w", err)
		}
		slog.Info("last id", "collection", collCfg.Name, "id", lastID)
	}

	return nil
}
