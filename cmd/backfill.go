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

	"github.com/tmccann21/mongopuff/internal/mongo"
	"github.com/tmccann21/mongopuff/internal/transform"
	"github.com/tmccann21/mongopuff/internal/turbopuffer"
	"github.com/tmccann21/mongopuff/internal/writer"
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
	p := &collectionPipeline{
		coll:   collCfg,
		schema: turbopuffer.BuildSchema(collCfg.Mapping.Fields),
		writer: writer.New(turbopuffer.New(cfg.TurbopufferAPIKey, cfg.TurbopufferRegion), dlqWriter),
		dlq:    dlqWriter,
		store:  store,
	}
	return p.backfill(ctx)
}

// backfill TODO: document.
func (p *collectionPipeline) backfill(ctx context.Context) error {
	slog.Info("starting backfill", "collection", p.coll.Name)
	scanner := p.store.CollectionScanner(p.coll.Name)
	state, err := p.store.LoadCollectionState(ctx, p.coll.Name)
	if err != nil {
		return err
	}
	lastID := state.BackfillCursor
	pageSize := p.coll.EffectiveBackfillPageSize()
	page := 0
	for {
		page++
		opTime, err := p.store.PingOperationTime(ctx)
		if err != nil {
			return fmt.Errorf("ping operation time: %w", err)
		}

		docs, err := scanner.ScanPage(ctx, lastID, pageSize)
		if err != nil {
			return fmt.Errorf("scan page: %w", err)
		}

		if len(docs) == 0 {
			slog.Info("backfill complete", "collection", p.coll.Name, "pages", page-1)
			break
		}

		slog.Info("scanned page",
			"collection", p.coll.Name,
			"page", page,
			"pageSize", pageSize,
			"docs", len(docs),
			"cursor", lastID,
		)
		if err := p.backfillPage(ctx, docs, opTime); err != nil {
			return err
		}
		lastID = docs[len(docs)-1]["_id"]

		if err := p.store.SaveBackfillCursor(ctx, p.coll.Name, lastID); err != nil {
			return fmt.Errorf("saving backfill cursor: %w", err)
		}
		slog.Info("page written", "collection", p.coll.Name, "page", page, "cursor", lastID)
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
func (p *collectionPipeline) backfillPage(ctx context.Context, docs []map[string]any, opTime uint64) error {
	actions := make([]transform.Action, 0, len(docs))
	var failed []mongo.DLQEntry
	for _, doc := range docs {
		action, err := transform.MapDocument(doc, opTime, p.coll)
		if err != nil {
			slog.Error("transform error", "collection", p.coll.Name, "documentId", doc["_id"], "error", err)
			// action is zero on error; OpInsert matches what MapDocument stamps on success.
			failed = append(failed, transformFailureEntry(p.coll.Name, doc["_id"], mongo.OpInsert, opTime, err))
			continue
		}
		actions = append(actions, action)
	}

	if err := p.dlq.Write(ctx, failed...); err != nil {
		return fmt.Errorf("DLQ write for %d documents: %w", len(failed), err)
	}

	if err := p.writer.WriteBatch(ctx, p.coll.Name, p.schema, actions); err != nil {
		return fmt.Errorf("write batch: %w", err)
	}
	return nil
}
