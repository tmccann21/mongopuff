package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"runtime"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/tmccann21/mongopuff/internal/batch"
	"github.com/tmccann21/mongopuff/internal/config"
	"github.com/tmccann21/mongopuff/internal/mongo"
	"github.com/tmccann21/mongopuff/internal/transform"
)

var (
	totalEvents  int
	batchSize    int
	flushLatency time.Duration
	collections  int
)

func init() {
	flag.IntVar(&totalEvents, "events", 100_000, "total events to process")
	flag.IntVar(&batchSize, "batch-size", 1024, "batcher flush count threshold")
	flag.DurationVar(&flushLatency, "flush-latency", 200*time.Millisecond, "simulated Turbopuffer write duration")
	flag.IntVar(&collections, "collections", 1, "number of concurrent collections")
}

var collectionConfig = func() config.CollectionConfig {
	mirrorDeletes := true
	return config.CollectionConfig{
		Name:          "bench",
		MirrorDeletes: &mirrorDeletes,
		Mapping: config.MappingConfig{
			Namespace: "bench",
			Fields: []config.FieldMapping{
				{Name: "name", Type: config.FieldTypeString},
				{Name: "calories", Type: config.FieldTypeFloat},
				{Name: "vegetarian", Type: config.FieldTypeBool},
				{Name: "servings", Type: config.FieldTypeInt},
				{Name: "ingredients", Type: config.FieldTypeStringArray},
				{Name: "createdAt", Type: config.FieldTypeDatetime},
			},
		},
	}
}()

type collectionResult struct {
	name          string
	events        int
	batches       int
	elapsed       time.Duration
	flushTime     time.Duration
	transformTime time.Duration
}

func main() {
	flag.Parse()

	if totalEvents <= 0 || collections <= 0 {
		fmt.Fprintln(os.Stderr, "events and collections must be > 0")
		os.Exit(1)
	}

	runtime.GC()

	var baselineMem runtime.MemStats
	runtime.ReadMemStats(&baselineMem)

	perCollection := totalEvents / collections
	remainder := totalEvents % collections

	ctx := context.Background()
	start := time.Now()

	var wg sync.WaitGroup
	results := make([]collectionResult, collections)

	for i := range collections {
		n := perCollection
		if i < remainder {
			n++
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			results[i] = runCollection(ctx, i, n)
		}()
	}

	wg.Wait()
	elapsed := time.Since(start)

	var finalMem runtime.MemStats
	runtime.ReadMemStats(&finalMem)

	var totalProcessed, totalBatches int
	var totalFlushTime, totalTransformTime time.Duration
	for _, r := range results {
		totalProcessed += r.events
		totalBatches += r.batches
		totalFlushTime += r.flushTime
		totalTransformTime += r.transformTime
	}

	throughput := float64(totalProcessed) / elapsed.Seconds()
	avgBatchUtil := float64(0)
	if totalBatches > 0 {
		avgBatchUtil = float64(totalProcessed) / float64(totalBatches)
	}

	fmt.Println("=== mongopuff CDC benchmark ===")
	fmt.Println()
	fmt.Printf("  events:          %d\n", totalProcessed)
	fmt.Printf("  batch_size:      %d\n", batchSize)
	fmt.Printf("  flush_latency:   %s\n", flushLatency)
	fmt.Printf("  collections:     %d\n", collections)
	fmt.Println()
	fmt.Printf("  throughput:      %.0f events/sec\n", throughput)
	fmt.Printf("  elapsed:         %s\n", elapsed.Round(time.Millisecond))
	fmt.Printf("  batches:         %d\n", totalBatches)
	fmt.Printf("  avg batch util:  %.0f (%.1f%%)\n", avgBatchUtil, avgBatchUtil/float64(batchSize)*100)
	fmt.Println()
	totalAlloc := finalMem.TotalAlloc - baselineMem.TotalAlloc
	fmt.Printf("  total alloc:     %s\n", formatBytes(totalAlloc))
	fmt.Printf("  bytes/event:     %d\n", totalAlloc/uint64(max(totalProcessed, 1)))
	fmt.Println()

	if collections > 1 {
		fmt.Println("  per-collection:")
		for _, r := range results {
			ct := float64(r.events) / r.elapsed.Seconds()
			fmt.Printf("    %-20s %6d events  %8.0f events/sec\n", r.name, r.events, ct)
		}
		fmt.Println()
	}
}

func runCollection(ctx context.Context, index int, numEvents int) collectionResult {
	name := fmt.Sprintf("collection_%d", index)
	collCfg := collectionConfig
	collCfg.Name = name
	collCfg.Mapping.Namespace = name
	globalCfg := config.GlobalConfig{
		BatchFlushCount:  batchSize,
		BatchFlushTimeMs: 60_000, // effectively disable timer flushes — we want count-triggered only
	}

	var result collectionResult
	result.name = name

	flushFn := func(ctx context.Context, actions []transform.Action, resumeToken []byte) error {
		result.batches++
		start := time.Now()
		time.Sleep(flushLatency)
		result.flushTime += time.Since(start)
		return nil
	}

	batcher := batch.New(ctx, globalCfg, flushFn)
	resumeToken := []byte("fake-token")
	collStart := time.Now()

	for i := range numEvents {
		event := buildEvent(i)

		tStart := time.Now()
		action, err := transform.MapChangeEvent(event, collCfg)
		result.transformTime += time.Since(tStart)

		if err != nil {
			continue
		}

		if err := batcher.Add(ctx, action, resumeToken); err != nil {
			fmt.Fprintf(os.Stderr, "batcher error: %v\n", err)
			break
		}
		result.events++
	}

	if err := batcher.Close(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "batcher close error: %v\n", err)
	}

	result.elapsed = time.Since(collStart)
	return result
}

// buildEvent creates a single synthetic ChangeEvent.
// 70% inserts, 20% updates, 10% deletes.
func buildEvent(seq int) mongo.ChangeEvent {
	roll := seq % 10
	clusterTime := transform.SerializeClusterTime(uint32(1000+seq), 1)
	docID := bson.NewObjectID()

	switch {
	case roll < 7:
		return mongo.ChangeEvent{
			Operation:  mongo.OpInsert,
			DocumentID: docID,
			FullDocument: map[string]any{
				"_id":         bson.NewObjectID(),
				"name":        fmt.Sprintf("Recipe %d", seq),
				"calories":    float64(seq) + 0.5,
				"vegetarian":  seq%2 == 0,
				"servings":    int64(seq%8 + 1),
				"ingredients": bson.A{"flour", "sugar", "butter"},
				"createdAt":   time.Date(2025, 1, 1, seq, 0, 0, 0, time.UTC),
			},
			ClusterTime: clusterTime,
		}
	case roll < 9:
		return mongo.ChangeEvent{
			Operation:  mongo.OpUpdate,
			DocumentID: docID,
			UpdatedFields: map[string]any{
				"name":     fmt.Sprintf("Updated Recipe %d", seq),
				"calories": float64(seq) + 1.5,
				"servings": int64(seq%8 + 2),
			},
			ClusterTime: clusterTime,
		}
	default:
		return mongo.ChangeEvent{
			Operation:   mongo.OpDelete,
			DocumentID:  docID,
			ClusterTime: clusterTime,
		}
	}
}

func formatBytes(b uint64) string {
	switch {
	case b >= 1<<30:
		return fmt.Sprintf("%.1f GB", float64(b)/(1<<30))
	case b >= 1<<20:
		return fmt.Sprintf("%.1f MB", float64(b)/(1<<20))
	case b >= 1<<10:
		return fmt.Sprintf("%.1f KB", float64(b)/(1<<10))
	default:
		return fmt.Sprintf("%d B", b)
	}
}
