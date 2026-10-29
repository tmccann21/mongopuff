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
	"github.com/tmccann21/mongopuff/internal/mongo"
	"github.com/tmccann21/mongopuff/internal/transform"
	"github.com/tmccann21/mongopuff/internal/turbopuffer"
	"github.com/tmccann21/mongopuff/internal/writer"
	tpuf "github.com/turbopuffer/turbopuffer-go/v2"
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
	defer store.Close(ctx)

	collCfg, ok := cfg.Collection(*collection)
	if !ok {
		return fmt.Errorf("collection %q not found in _mongopuff config", *collection)
	}

	dlqWriter := store.NewDLQWriter()
	w := writer.New(turbopuffer.New(cfg.TurbopufferAPIKey, cfg.TurbopufferRegion), dlqWriter)

	schema := turbopuffer.BuildSchema(collCfg.Mapping.Fields)

	slog.Info("starting backfill", "collection", collCfg.Name)
	scanner := store.CollectionScanner(collCfg.Name)
	state, err := store.LoadCollectionState(ctx, collCfg.Name)
	if err != nil {
		return err
	}
	lastID := state.BackfillCursor
	pageSize := collCfg.EffectiveBackfillPageSize()
	page := 0
	for {
		page++
		opTime, err := store.PingOperationTime(ctx)
		if err != nil {
			return fmt.Errorf("ping operation time: %w", err)
		}

		docs, err := scanner.ScanPage(ctx, lastID, pageSize)
		if err != nil {
			return fmt.Errorf("scan page: %w", err)
		}

		if len(docs) == 0 {
			slog.Info("backfill complete", "collection", collCfg.Name, "pages", page-1)
			break
		}

		slog.Info("scanned page",
			"collection", collCfg.Name,
			"page", page,
			"pageSize", pageSize,
			"docs", len(docs),
			"cursor", lastID,
		)
		if err := backfillPage(ctx, w, dlqWriter, schema, collCfg, docs, opTime); err != nil {
			return err
		}
		lastID = docs[len(docs)-1]["_id"]

		if err := store.SaveBackfillCursor(ctx, collCfg.Name, lastID); err != nil {
			return fmt.Errorf("saving backfill cursor: %w", err)
		}
		slog.Info("page written", "collection", collCfg.Name, "page", page, "cursor", lastID)
	}

	return nil
}

// backfillPage maps one scanned page to upserts stamped with opTime and writes
// them with one turbopuffer call per resolved namespace. A document that fails
// to transform is DLQ'd and the rest of the page is still written. The caller
// saves the backfill cursor only on a nil return.
//
// Failed documents are DLQ'd before anything is written to turbopuffer, so an
// unavailable DLQ fails the page without spending writes on it.
func backfillPage(ctx context.Context, w *writer.Writer, dlq mongo.DLQWriter, schema map[string]tpuf.AttributeSchemaConfigParam, coll config.CollectionConfig, docs []map[string]any, opTime uint64) error {
	actions := make([]transform.Action, 0, len(docs))
	for _, doc := range docs {
		action, err := transform.MapDocument(doc, opTime, coll)
		if err != nil {
			slog.Error("transform error", "collection", coll.Name, "documentId", doc["_id"], "error", err)
			// action is zero on error; OpInsert matches what MapDocument stamps on success.
			entry := transformFailureEntry(coll.Name, doc["_id"], mongo.OpInsert, opTime, err)
			if err := dlq.Write(ctx, entry); err != nil {
				return fmt.Errorf("DLQ write for document %v: %w", doc["_id"], err)
			}
			continue
		}
		actions = append(actions, action)
	}

	if err := w.WriteBatch(ctx, schema, actions); err != nil {
		return fmt.Errorf("write batch: %w", err)
	}
	return nil
}
