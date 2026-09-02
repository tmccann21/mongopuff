package cmd

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"text/tabwriter"

	"github.com/tmccann21/mongopuff/internal/mongo"
)

func runDLQ() error {
	if len(os.Args) < 3 {
		return errors.New("usage: mongopuff dlq <list|show|clear>")
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	connStr := os.Getenv("MONGODB_CONNECTION_STRING")
	if connStr == "" {
		return fmt.Errorf("MONGODB_CONNECTION_STRING is required")
	}

	store, err := mongo.NewStore(ctx, connStr)
	if err != nil {
		return err
	}
	defer store.Close(ctx)

	switch os.Args[2] {
	case "list":
		return runDLQList(ctx, store)
	case "show":
		return runDLQShow(ctx, store)
	case "clear":
		return runDLQClear(ctx, store)
	default:
		return fmt.Errorf("unknown dlq subcommand: %s\nusage: mongopuff dlq <list|show|clear>", os.Args[2])
	}
}

func runDLQList(ctx context.Context, store *mongo.Store) error {
	fs := flag.NewFlagSet("dlq list", flag.ContinueOnError)
	collection := fs.String("collection", "", "filter by collection name")
	limit := fs.Int64("limit", 50, "maximum number of entries to show")
	if err := fs.Parse(os.Args[3:]); err != nil {
		return err
	}

	entries, err := store.ListDLQ(ctx, *collection, *limit)
	if err != nil {
		return err
	}

	if len(entries) == 0 {
		if *collection != "" {
			fmt.Printf("No DLQ entries for collection %q.\n", *collection)
		} else {
			fmt.Println("No DLQ entries found.")
		}
		return nil
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(w, "ID\tCOLLECTION\tDOCUMENT\tOPERATION\tERROR KIND\tCREATED")
	for _, e := range entries {
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\n",
			e.ID.Hex(),
			e.Collection,
			truncate(e.DocumentID, 24),
			e.Operation,
			e.ErrorKind,
			e.CreatedAt.Format("2006-01-02 15:04:05"),
		)
	}
	w.Flush()

	fmt.Printf("\nTotal: %d entries\n", len(entries))
	if int64(len(entries)) == *limit {
		fmt.Printf("(showing first %d — use --limit to see more)\n", *limit)
	}
	return nil
}

func runDLQShow(ctx context.Context, store *mongo.Store) error {
	fs := flag.NewFlagSet("dlq show", flag.ContinueOnError)
	if err := fs.Parse(os.Args[3:]); err != nil {
		return err
	}

	if fs.NArg() == 0 {
		return errors.New("usage: mongopuff dlq show <id>")
	}
	idHex := fs.Arg(0)

	entry, err := store.GetDLQEntry(ctx, idHex)
	if err != nil {
		return err
	}

	fmt.Printf("DLQ Entry %s\n", entry.ID.Hex())
	fmt.Println("────────────────────────────────────────")
	fmt.Printf("Collection:   %s\n", entry.Collection)
	fmt.Printf("Document ID:  %s\n", entry.DocumentID)
	fmt.Printf("Operation:    %s\n", entry.Operation)
	fmt.Printf("Error Kind:   %s\n", entry.ErrorKind)
	fmt.Printf("Cluster Time: %d\n", entry.ClusterTime)
	fmt.Printf("Created:      %s\n", entry.CreatedAt.Format("2006-01-02 15:04:05 MST"))
	fmt.Printf("\nError Message:\n  %s\n", entry.ErrorMessage)

	return nil
}

func runDLQClear(ctx context.Context, store *mongo.Store) error {
	fs := flag.NewFlagSet("dlq clear", flag.ContinueOnError)
	collection := fs.String("collection", "", "clear entries for a specific collection")
	all := fs.Bool("all", false, "clear all entries")
	if err := fs.Parse(os.Args[3:]); err != nil {
		return err
	}

	if *collection == "" && !*all {
		return errors.New("either --collection or --all must be specified")
	}
	if *collection != "" && *all {
		return errors.New("cannot use both --collection and --all")
	}

	target := *collection
	if *all {
		target = "*"
	}

	count, err := store.ClearDLQ(ctx, target)
	if err != nil {
		return err
	}

	if *collection != "" {
		fmt.Printf("Cleared %d DLQ entries for collection %q.\n", count, *collection)
	} else {
		fmt.Printf("Cleared %d DLQ entries.\n", count)
	}
	return nil
}

func truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max-3] + "..."
}
