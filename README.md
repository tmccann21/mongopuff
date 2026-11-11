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
  shutdownTimeoutMs: 10000          # max ms for each collection's final flush at shutdown (default: 10000)
```

### Field Types

**Scalars:** `string`, `int`, `uint`, `float`, `bool`, `uuid`, `datetime`

**Arrays:** `[]string`, `[]int`, `[]uint`, `[]float`, `[]bool`, `[]uuid`, `[]datetime`

**Vectors:** `vector` (requires `dimension` and `precision`)

Only fields listed in the config are synced. Unmapped fields are silently dropped.

### Dynamic Namespaces

A namespace can be a template that sends each document to a namespace built from its own fields:

```yaml
collections:
  - name: orders
    mapping:
      namespace: "{{tenantId}}_orders"   # must be quoted; see below
      fields:
        - name: total
          type: float
```

A document with `tenantId: "acme"` is written to the `acme_orders` namespace.

- A `{{field}}` token names a top-level field. Nested paths such as `{{customer.id}}` are not supported. A template can hold several tokens, such as `{{region}}-{{tenantId}}`.
- A field's value is substituted by type: a string as-is; an int32 or int64 in decimal; a double with no fractional part as an integer (`1e6` becomes `1000000`); an ObjectID as its 24-character hex; a binary UUID in hyphenated form. Any other type is rejected.
- Every namespace name, static or resolved, must match turbopuffer's rule `[A-Za-z0-9-_.]{1,128}`. A static name, or text outside a template's tokens, that breaks the rule fails when the config loads. A resolved name that breaks it fails for that document.
- Quote a template that starts with `{{`. Unquoted, YAML reads it as a map and the config fails to load with `cannot unmarshal !!map into string`, or with `did not find expected key` when text follows the token.

An event whose namespace can't be resolved goes to the DLQ with error kind `namespace_unresolved` and is not written to turbopuffer. That happens when a token's field is absent or null, holds a type not listed above, or resolves to a name that breaks the rule. It also happens when the change event lacks a document image it needs: a delete, update or replace needs its pre-image, and an update also needs its post-image, even when it changes no mapped field. An image is missing once it has expired, or when the event happened before pre- and post-images were enabled.

When a document's old namespace can't be resolved, for example because it was inserted without a `tenantId` and went to the DLQ, an update that gives it one writes the whole document to the new namespace.

#### Requirements

A dynamic namespace needs MongoDB 6.0 or later, and `changeStreamPreAndPostImages` enabled on the collection so that its change events carry the document as it was before and after each change:

```js
db.runCommand({ collMod: "orders", changeStreamPreAndPostImages: { enabled: true } })
```

At startup, `mongopuff run` checks each collection with a dynamic namespace and exits with an error if the collection doesn't exist or doesn't have the option enabled. mongopuff never changes the setting itself. The check runs `listCollections`, so the MongoDB user needs that privilege on the database; the built-in `read` role includes it. A static namespace needs none of this.

### Config Validation

```
mongopuff validate mongopuff.yaml
```

Checks YAML syntax, duplicate namespaces, namespace names and templates, valid field types, and vector field completeness. It doesn't connect to MongoDB, so the pre-image check for dynamic namespaces runs only when `mongopuff run` starts.

## Commands

### `mongopuff run`
Start the CDC pipeline. Opens a change stream per collection, batches events, and writes them to turbopuffer. Handles `SIGINT`/`SIGTERM` for graceful shutdown: each collection flushes its last batch and gives up after `shutdownTimeoutMs`. Events it didn't flush are replayed from the change stream on the next start. `run` logs a warning when `shutdownTimeoutMs` is over 30s, Kubernetes' default termination grace period, after which a pod that is still flushing is killed.

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

mongopuff opens a [change stream](https://www.mongodb.com/docs/manual/changeStreams/) per collection. Each change event is transformed into turbopuffer writes:

| MongoDB operation | turbopuffer action | behavior |
|------|--------|----------|
| insert | upsert | write full document |
| replace | upsert | write full document |
| update | patch | write only changed fields, null removed fields |
| update or replace that changes the namespace | delete and upsert | delete from the old namespace (even if `mirrorDeletes` is disabled), write full document to the new one |
| delete | delete | delete document (if `mirrorDeletes` enabled) |

Documents are identified by their `_id` field. Supported ID types are ObjectID, string, int32, int64, and binary UUID.

`mirrorDeletes` covers documents deleted in MongoDB. A document that changes namespace still exists, so its copy in the old namespace is always deleted.

Moving a document between namespaces takes two writes, one to each namespace, and is not atomic. Each write that fails goes to the DLQ on its own. A failed delete leaves the document in both namespaces, and a failed upsert leaves it in neither, until the DLQ entry is resolved. Where namespaces separate tenants, a failed delete means the old tenant can still see the document.

### Batching

Events are accumulated in an in-memory batch and flushed when either the count threshold (`batchFlushCount`) or time threshold (`batchFlushTimeMs`) is reached. Within a batch, if the same document is modified multiple times, only the latest action for it in each namespace is kept, so a document that moves namespaces keeps both its delete and its upsert.

### Delivery

mongopuff replication uses conditional writes in turbopuffer to perform effectively-once delivery. It persists a change stream cursor to resume event processing after a crash.

Under heavy write-load to MongoDB or degraded write performance from turbopuffer, the change stream cursor can drift from the head of the oplog. In extreme cases, the cursor can be evicted from the oplog window and mongopuff loses its position. This is unrecoverable and requires a full backfill. If your use case involves persistent high write-pressure, mongopuff can be run with a durable buffer on change stream reads. In this mode, mongopuff writes change events to disk before delivering to turbopuffer, allowing the consumer to keep pace with the oplog without blocking on network I/O.

```
# direct mode (default)
change stream --> batcher --> turbopuffer

# spooled mode (spoolEnabled: true)
change stream --> batcher --> disk --> delivery loop --> turbopuffer
```

Enable the spool by setting `spoolEnabled: true` in your config. Each collection gets its own spool directory under `spoolDir`. Segments are written atomically and deleted after successful delivery. The spool directory is the only record of which segments are still to be delivered: on restart, delivery resumes from the oldest segment on disk.

### Dead-Letter Queue

When a write to turbopuffer fails (network error, rate limit, server error) or a document can't be transformed (type mismatch, unsupported ID type, unresolved namespace), the event is written to the `_mongopuff_dlq` collection in your database and replication continues. A backfill does the same for a document that can't be transformed, and still writes the rest of its page.

If the DLQ write fails too, mongopuff stops that collection's change stream or backfill rather than skip the event. Its resume token or backfill cursor isn't saved past the event, so the event is retried on the next start.

Every entry records the source collection. An entry for a failed turbopuffer write also records the namespace it targeted and the write it attempted (`upsert`, `patch` or `delete`).

Use `mongopuff dlq list` to inspect failures and `mongopuff dlq clear` to clean up after resolution.

### State

mongopuff persists its state in the `_mongopuff_state` collection:
- **Resume token** -- the change stream position, saved after every successful flush. On restart, the stream picks up where it left off.
- **Backfill cursor** -- the last `_id` processed during a backfill, allowing interrupted backfills to resume.

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
