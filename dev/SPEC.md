# Purpose
Acts as a standalone service to listen to a MongoDB change stream and mirror data to a turbopuffer instance.

# Setup
## Configuration
An administrator can configure the service using the following mappings:

source collection -> turbopuffer namespace
 - 1-to-1; defaults to the collection name
document predicate
 - filter for which documents to include; feels out of scope for v1 due to the complexity of defining a language for this...
fields to persist
- subset of which fields get persisted to turbopuffer
  - _id is always included, mapped to a string and used as the tpuf id. This is required for delete operations. The following id types are supported:
    ObjectId -> hex string
    String -> as is
    Int32/Int64 -> decimal string
    Binary (UUID subtype) -> canonical UUID string
    Everything else -> reject at startup

    when app starts, query the first document from each collection and ensure the _id field is accepted. if the collection is empty, this validation is skipped and will be caught when the first document is added
- enforce strict typing on these fields; since turbopuffer does not support attribute nesting this project cannot support nested fields
  on a type mismatch, log and skip this document
- following types are supported:
  - string
  - int
  - uint
  - float
  - bool
  - uuid
  - datetime
  - []string
  - []int
  - []uint
  - []float
  - []bool
  - []uuid
  - []datetime
  - vector <dimension, precision> where precision is one of: f32, f16, i8
- BSON null values are written as turbopuffer null (attribute is nullable, field is included)

On startup, the service should verify the validity of the configuration. It should log and crash on a failure.
Startup validation checks:
- No two collections map to the same turbopuffer namespace
- Every field specifies a valid type from the supported list
- Vector fields have both dimension and precision set
- _id type is supported (query first document from each collection)

A .env file should be configured for secrets. The service will read:

HEALTH_PORT (default 8080)
LOG_LEVEL

MONGODB_CONNECTION_STRING (must include database name, e.g. mongodb://host/mydb)
TURBOPUFFER_API_KEY

The service is scoped to a single MongoDB database, determined by the database in the connection string. Startup fails if the connection string does not include a database. _mongopuff_state and _mongopuff_dlq collections are created by the service in this database. _mongopuff is managed by the operator.

## _mongopuff
_mongopuff is a collection managed by the operator to store configurations per-collection

documents in this collection have the following shape

```
{
  _id: <collection name>
  backfillPageSize: <int; default 128>
  mirrorDeletes: <boolean>
  mapping: {
    namespace: <string; tpuff namespace name (default to collection name)>
    fields: [
      {
        name: <field name in mongodb>,
        type: <type from list specified above>
        dimension: <for vectors only; int>
        precision: <for vectors only; string>
      }
    ]
  }
}
```

Mapping documents are managed directly by the operator in MongoDB
The service reads all _mongopuff documents once at startup; changes require a restart to take effect

## _mongopuff_state
_mongopuff_state is a collection created and managed by the service to store runtime state per-collection. Operators should not edit this collection.

documents in this collection have the following shape

```
{
  _id: <collection name>
  changeStreamResumeToken: <token bson document>
  backfillCursor: <_id of last backfilled document>
  lastFlushTime: <timestamp>
}
```

Service level configurations are created in a special document with id _global, this includes:
- batch flush count (default 1024)
- batch flush time (default 1s)

# Architecture
Service is written in modern Go, with minimal external dependencies. It is lightweight and performant, prioritising
base functionality and throughput over advanced features.

## Graceful Shutdown
intercept SIGTERM/SIGINT, flush pending batch and save resume token for each collection to _mongopuff_state. Each go-routine
handles this independently. Flush gets 1 try before writing remaining data to DLQ to not risk total loss.

# Modes
## CDC Mode
Listen to MongoDB change stream and emit updates from configured collections to turbopuffer

MongoDB change stream events are mapped as follows:
  - insert, replace — all map to a turbopuffer upsert
  - update - turbopuffer patch; when fields are removed this must be explicitly nulled in turbopuffer. patches that do not modify mapped fields are noops
  - delete — turbopuffer delete
  - invalidate — this means the change stream is no longer valid (e.g. the replica set config changed). You need to log an error and stop. The resume token is useless at this point — the operator needs to re-backfill.
  - rename, drop — log a warning, ignore

Each collection should spawn it's own go-routine for CDC mirroring, acting independently of eachother. One change stream per collection.

all changes persisted to turbopuffer have a `_clusterTime` field used for conditional writing in concurrent mode

### Deletion
Deletion events are replicated in turbopuffer by default. This behaviour can be configured within the config document for a collection

### Dead-Letter Queue
mongopuff creates a dead letter queue on the mongodb database with a collection named _mongopuff_dlq. documents in the dlq have the following shape

 {
    "_id": <default objectid>,
    "collection": <collection>,
    "documentId": "<original _id, as string>",
    "operation": <insert|update|replace|delete>,
    "errorKind": <write_rejected|network_error|rate_limited|server_error>,
    "errorMessage": <string>,
    "clusterTime": Timestamp(1234, 1),
    "createdAt": ISODate,
  }

### Error Handling
Error types with corresponding action

type_mismatch - log + skip
id_missing - log + skip
write_rejected - DLQ
network_error - DLQ
rate_limited - 429 from tpuff; DLQ
server_error - 500 from tpuff; DLQ

retry is handled by the turbopuffer Go SDK's built-in retry mechanism (exponential backoff, retries on 408/429/5xx/connection errors). mongopuff does not implement its own retry logic. When the SDK exhausts retries and returns an error, the failing documents are written to the DLQ.

## Backfill Mode
Scan a collection and mirror all matching documents to turbopuffer; resumable and progress-reporting. Page size is determined by the collection config document (default is 128)

Upserted documents should attach the cluster time queried before reading the page as `clusterTime` for conditional writes.

Backfill is complete once the next page returns 0 documents, and should update the cursor to the last document processed. Running subsequent
backfills before new data added will therefore terminate on the first page

### Concurrent Backfill + CDC
Backfill can be run concurrently with CDC, but CDC should be started first to guarantee no documents are missed. There is a
potential overlap issue where a document is added after the last page is scanned by backfill, but this is considered acceptable risk
and re-running backfill would pick it up immediately.
Both backfill and CDC upserts include a cluster time as a version field (`clusterTime`) in the turbopuffer conditional write. The BSON
Timestamp is serialized as a single uint64: `uint64(T)<<32 | uint64(I)`, preserving total ordering. On rejected conditional write,
log and skip.

- **CDC writes** use the `clusterTime` from the change stream event, which reflects when the write occurred on the oplog.
- **Backfill writes** use the `operationTime` from a `db.runCommand({ping: 1})` issued before each page query. This
  timestamp is guaranteed to be at least as old as the documents in that page

Backfill flow per page:
1. Issue `ping`, capture `operationTime` as the version for this page
2. Query the next page of documents (`_id > backfillCursor`, sorted by `_id`, limit by page size)
3. Upsert the page to turbopuffer using the captured `operationTime` as the conditional write version
4. Persist the last `_id` from the page as `backfillCursor`

# Writing to Turbopuffer
turbopuffer writes should be handled using the official turbopuffer go library

id field should use the _id field from the corresponding document; serialized accordingly for non objectid ids. Turbopuffer supports conditional writes which must be implemented in the upload. Uploads should be batched according to the configs batch sizing parameters.

# Requirements
## At-least-once delivery
Store resume token in _mongopuff_state for each collection; restart from resume token on crash/shutdown. Effectively-once delivery achieved
through two principles:

1. idempotent upsert/delete operations
2. use turbopuffer conditional writes with mongodb oplog cluster time
  - updates written to turbopuffer will contain a `_clusterTime` attribute. This is set by the writer (either CDC or backfill) and is used for conditional writes

## Batching
CDC uses a batcher with the following default properties (configurable):

flush document limit: 1024
flush interval: 1s

after flushing, and confirming the write to tpuff the new resume stream token should be written to the relevant collection. must wait for tpuff confirmation; failing to persist the token is not a large issue as we only guarantee at-least-once delivery and this failure should be rare

on partial failures within a batch, write the failing documents to DLQ and retry the batch without the offending documents
within a batch no two updates can reference the same document by id. if this collision occurs, use the most recent

Backfill batching is handled implicitly by page size, and therefore does not have any additional batcher logic

# CLI
mongopuff exposes two commands as a CLI

## run
`mongopuff run` starts the CDC service

## backfill
`mongopuff backfill --collection=<collection>` run backfill for a specific collection. `collection` is a required parameter. to be run
by the operator and not triggered implicitly

# Observability
## /healthz
Simple health endpoint exposed over http (default port 8080; configurable by env)

returns:
```
{
  status: 'ok'
  collections: [{
    name: <collection name>,
    lastFlushTime: <last flush time>
  }]
}
```

## Logging
App should expose structured logs using Gos slog utility

Key log-line details

CDC
  - `collection`
  - `operation`
  - `documentId`
  - `clusterTime`
  - `clockTime`

Batch Flushes
  - `batchSize` (count)
  - `flushDuration`

Errors
  - `errorKind`
  - `errorMessage` (if any)

Retries
  - `attempt`
  - `backoffMs`

DLQ
  - `dlqId`
  - `errorMessage`

Backfill
  - `page`
  - `pageSize`
  - `cursor` (current backfill cursor id)

Should use the following log levels
`info` - batch flushes / most useful details / retries
`debug` - individual document upserts
`error` - errors
