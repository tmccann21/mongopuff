# mongopuff
mongopuff is a logical replication layer between MongoDB and turbopuffer

## Install

### go install
```
go install github.com/tmccann21/mongopuff@latest
```

### Binary releases
Download a prebuilt binary from the [GitHub Releases](https://github.com/tmccann21/mongopuff/releases) page.

## Quickstart

```bash
# generate a config interactively
mongopuff init -o mongopuff.yaml

# set required env vars
export MONGODB_CONNECTION_STRING=mongodb://localhost:27017/mydb
export TURBOPUFFER_API_KEY=tpuf_...

# start replicating
mongopuff run
```

The connection string must include a database name. mongopuff operates on one database per process.

## Configuration

mongopuff reads a YAML config file (default `mongopuff.yaml`, override with `CONFIG_FILE_PATH`).

### Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `MONGODB_CONNECTION_STRING` | yes | | Connection string including database name |
| `TURBOPUFFER_API_KEY` | yes | | Turbopuffer API key |
| `TURBOPUFFER_REGION` | no | `aws-us-west-2` | Turbopuffer region |
| `CONFIG_FILE_PATH` | no | `mongopuff.yaml` | Path to config file |
| `HEALTH_PORT` | no | `8080` | Port for the health endpoint |
| `LOG_LEVEL` | no | `info` | `debug`, `info`, `warn`, or `error` |

### Config File Reference

```yaml
collections:
  - name: recipes                    # MongoDB collection name
    backfillPageSize: 256            # page size for backfill scans (default: 128)
    mirrorDeletes: true              # replicate deletes to turbopuffer (default: true)
    mapping:
      namespace: recipes             # turbopuffer namespace (defaults to collection name)
      fields:
        - name: title
          type: string
          filterable: true           # make this field filterable in turbopuffer

        - name: description
          type: string
          embed:                     # embed this field as a vector on write
            model: voyage/voyage-4
            dimensions: 1024
            attribute: desc_vector   # vector field name (default: {field}_vector)

        - name: tags
          type: "[]string"

        - name: embedding
          type: vector
          dimension: 1536            # required for vector fields
          precision: f32             # f32, f16, or i8

        - name: calories
          type: float

        - name: created_at
          type: datetime

global:
  batchFlushCount: 1024             # max events per batch (default: 1024)
  batchFlushTimeMs: 1000            # max ms before flushing a partial batch (default: 1000)
  spoolEnabled: false               # enable durable spool buffer (default: false)
  spoolDir: ./data/spool            # spool directory (default: ./data/spool)
```

### Field Types

**Scalars:** `string`, `int`, `uint`, `float`, `bool`, `uuid`, `datetime`

**Arrays:** `[]string`, `[]int`, `[]uint`, `[]float`, `[]bool`, `[]uuid`, `[]datetime`

**Vectors:** `vector` (requires `dimension` and `precision`)

Only fields listed in the config are synced. Unmapped fields are silently dropped.

### Config Validation

```
mongopuff validate mongopuff.yaml
```

Checks YAML syntax, duplicate namespaces, valid field types, and vector field completeness.

## Commands

### `mongopuff run`
Start the CDC pipeline. Opens a change stream per collection, batches events, and writes them to turbopuffer. Handles `SIGINT`/`SIGTERM` for graceful shutdown.

### `mongopuff backfill --collection=<name>`
Bulk-sync all existing documents from a collection to turbopuffer. Resume-safe: if interrupted, subsequent runs pick up from the last cursor position.

Run this before starting CDC for the first time, or after an oplog position loss.

### `mongopuff init [-o <path>]`
Interactive wizard that generates a config file. Walks through collections, fields, types, vector configuration, and embedding setup.

### `mongopuff validate <config-file>`
Validate a config file without starting the pipeline.

### `mongopuff dlq`
Manage the dead-letter queue.

```bash
mongopuff dlq list                          # list recent DLQ entries (default: 50)
mongopuff dlq list --collection=recipes     # filter by collection
mongopuff dlq list --limit=100              # change the limit
mongopuff dlq show <id>                     # show full error details for an entry
mongopuff dlq clear --collection=recipes    # clear entries for one collection
mongopuff dlq clear --all                   # clear all entries
```

## Why?

This project was inspired by the work of the A24 team on [puffgres](https://github.com/a24films/puffgres). Plumbing between MongoDB -> turbopuffer was feeling tedious while experimenting and iterating quickly so in the spirit of [building the tools you need](https://www.youtube.com/watch?v=_GpBkplsGus) mongopuff was born.

## How It Works

### Change Data Capture

mongopuff opens a [change stream](https://www.mongodb.com/docs/manual/changeStreams/) per collection. Each change event is transformed into a turbopuffer write:

| MongoDB operation | turbopuffer action | behavior |
|------|--------|----------|
| insert | upsert | write full document |
| replace | upsert | write full document |
| update | patch | write only changed fields, null removed fields |
| delete | delete | delete document (if `mirrorDeletes` enabled) |

Documents are identified by their `_id` field. Supported ID types are ObjectID, string, int32, int64, and binary UUID.

### Batching

Events are accumulated in an in-memory batch and flushed when either the count threshold (`batchFlushCount`) or time threshold (`batchFlushTimeMs`) is reached. Within a batch, if the same document is modified multiple times, only the latest action is kept.

### Delivery

mongopuff replication uses conditional writes in turbopuffer to perform effectively-once delivery. It persists a change stream cursor to resume event processing after a crash and a dead-letter queue to replay failed writes during a turbopuffer outage.

Under heavy write-load to MongoDB or degraded write performance from turbopuffer, the change stream cursor can drift from the head of the oplog. In extreme cases, the cursor can be evicted from the oplog window and mongopuff loses its position. This is unrecoverable and requires a full backfill. If your use case involves persistent high write-pressure, mongopuff can be run with a durable buffer on change stream reads. In this mode, mongopuff writes change events to disk before delivering to turbopuffer, allowing the consumer to keep pace with the oplog without blocking on network I/O.

```
# direct mode (default)
change stream --> batcher --> turbopuffer

# spooled mode (spoolEnabled: true)
change stream --> batcher --> disk --> delivery loop --> turbopuffer
```

Enable the spool by setting `spoolEnabled: true` in your config. Each collection gets its own spool directory under `spoolDir`. Segments are written atomically and deleted after successful delivery.

### Dead-Letter Queue

When a write to turbopuffer fails (network error, rate limit, server error) or a document can't be transformed (type mismatch, unsupported ID type), the event is written to the `_mongopuff_dlq` collection in your database. The stream continues without blocking.

Use `mongopuff dlq list` to inspect failures and `mongopuff dlq clear` to clean up after resolution.

### State

mongopuff persists its state in the `_mongopuff_state` collection:
- **Resume token** -- the change stream position, saved after every successful flush. On restart, the stream picks up where it left off.
- **Backfill cursor** -- the last `_id` processed during a backfill, allowing interrupted backfills to resume.
- **Spool segment** -- the last spool segment written, so the delivery loop knows where to start.

### Health Check

mongopuff exposes a health endpoint at `GET /healthz` (default port 8080, configurable via `HEALTH_PORT`).

```json
{
  "status": "ok",
  "collections": [
    {"name": "recipes", "lastFlushTime": "2025-02-15T14:32:10.123Z"}
  ]
}
```

A stale `lastFlushTime` indicates a stuck pipeline. Use this for liveness probes in orchestrators like Kubernetes.

## Benchmarks
The following benchmarks were measured on a single github action runner. Throughput was measured using an artificial
flush latency to simulate Turbopuffer's API delay.

| flush latency | throughput |
|---------------|------------|
| 0ms | 697384 events/sec |
| 100ms | 10007 events/sec |
| 500ms | 2032 events/sec |
| 850ms | 1197 events/sec |

According to Turbopuffer's published p50, p90, and p99 latencies the following throughput should be possible

| percentile | write latency | throughput |
|------------|---------------|------------|
| p50 | 165ms | 6113 events/sec |
| p90 | 248ms | 4082 events/sec |
| p99 | 850ms | 1197 events/sec |

Memory usage is fairly efficient for mongopuff, even when scaling to > 1000 collections. Memory usage is mainly bounded by batch size
and flush interval. Large batches with long flush intervals will see memory usage grow but this is intended to be tuned according to
your use case.

| collections | peak RSS | bytes/event | per-collection throughput |
|-------------|----------|-------------|--------------------------|
| 1 | 15.7 MB | 1199 | 10010/s |
| 10 | 19.5 MB | 1197 | 9460/s |
| 50 | 34.2 MB | 1238 | 7690/s |
| 100 | 34.0 MB | 1244 | 5292/s |
| 500 | 36.3 MB | 1130 | 1016/s |

## License

Apache 2.0
