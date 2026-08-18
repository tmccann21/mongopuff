# Test Plan

## 1. Configuration Validation

Unit tests. Parse a `_mongopuff` config (or set of configs) and assert validation passes or fails with the expected error.
The system under test is the config loader/validator — no MongoDB or turbopuffer connection needed. Feed it
a struct or JSON representation of the `_mongopuff` documents + a `_global` document and assert the result.

### valid_config_minimal :white_check_mark:
Single collection, one string field, namespace defaults to collection name.
Validation passes. Confirm the resolved namespace equals the collection name.

### valid_config_custom_namespace :white_check_mark:
Collection `orders` maps to namespace `order_data`.
Validation passes. Confirm the resolved namespace is `order_data`, not `orders`.

### valid_config_all_field_types :white_check_mark:
One collection with a field for every supported type: string, int, uint, float, bool, uuid, datetime,
[]string, []int, []uint, []float, []bool, []uuid, []datetime, and a vector field with dimension=128 precision=f32.
Validation passes.

### invalid_config_duplicate_namespace :white_check_mark:
Two collections both map to namespace `users`. Validation fails with an error mentioning the duplicate namespace.

### invalid_config_empty_namespace :white_check_mark:
Collection `foo` has no namespace set. Validation fails — empty namespace means the config loader
didn't resolve the default, and the validator should reject it rather than silently compensating.

### invalid_config_unknown_field_type :white_check_mark:
A field specifies type `"map"`. Validation fails with an error identifying the bad type and which field it's on.

### invalid_config_vector_missing_dimension :white_check_mark:
Vector field has precision=f32 but no dimension. Validation fails.

### invalid_config_vector_missing_precision :white_check_mark:
Vector field has dimension=128 but no precision. Validation fails.

### invalid_config_vector_bad_precision :white_check_mark:
Vector field has dimension=128, precision=`"f64"` (not one of f32/f16/i8). Validation fails.

### invalid_config_no_database_in_connstring :white_check_mark:
Connection string is `mongodb://localhost` with no database component.
Validation fails at startup before any collection configs are read.

### valid_config_global_defaults :white_check_mark:
No `_global` document exists. After loading, the effective config should have
batch flush count=1024, size=8MB, interval=1s.

### valid_config_global_overrides :white_check_mark:
`_global` document sets batch flush count=512. After loading, effective flush count is 512;
size and interval remain at defaults.

## 2. ID Type Handling

Unit tests. The system under test is the ID serializer — takes a BSON `_id` value and returns a string
for use as the turbopuffer document ID. No external dependencies.

### id_objectid :white_check_mark:
ObjectId `_id`. Output should be the 24-character hex string (e.g. `"507f1f77bcf86cd799439011"`).

### id_string :white_check_mark:
String `_id` `"my-doc-1"`. Output should pass through unchanged.

### id_int32 :white_check_mark:
Int32 `_id` with value `42`. Output should be `"42"`.

### id_int64 :white_check_mark:
Int64 `_id` with a large value like `9223372036854775807`. Output should be the decimal string representation.

### id_binary_uuid :white_check_mark:
Binary `_id` with UUID subtype. Output should be the canonical UUID string (e.g. `"550e8400-e29b-41d4-a716-446655440000"`).

### id_int32_negative :white_check_mark:
Int32 `_id` with value `-1`. Output should be `"-1"`. Ensures sign is preserved in decimal serialization.

### id_binary_non_uuid :white_check_mark:
Binary `_id` with a non-UUID subtype (e.g. generic `0x00`). Serializer should return an error —
only the UUID subtype (`0x04`) is accepted.

### id_binary_uuid_wrong_length :white_check_mark:
Binary `_id` with UUID subtype but data length is not 16 bytes. Serializer should return an error.

### id_unsupported_type :white_check_mark:
`_id` is a Decimal128 or Array. Serializer should return an error. At startup this is fatal; during
event processing this would be logged and skipped.

### cluster_time_serialization :white_check_mark:
BSON Timestamp with T=1, I=2. Should serialize to uint64 `(1<<32)|2 = 4294967298`. Verify the
serialization preserves total ordering — a timestamp with a higher T or same T but higher I
must produce a larger uint64.

### id_missing_on_event :white_check_mark:
Change event arrives with no `_id` field present. Should log and skip the event (id_missing error kind).
No DLQ write, no retry.

## 3. Field Type Mapping

Unit tests. The system under test is the field converter — takes a BSON field value + a field config
and produces a turbopuffer attribute value. Focus on cases with real conversion logic or validation.

### field_uuid
Binary UUID BSON field configured as type `uuid`. Output should be the canonical UUID string
(e.g. `"550e8400-e29b-41d4-a716-446655440000"`).

### field_datetime
BSON DateTime field configured as type `datetime`. Output should be the turbopuffer datetime representation.

### field_vector_f32
BSON array of 128 floats, field configured as `vector <128, f32>`. Output should be a properly
typed f32 vector of dimension 128.

### field_vector_f16
Same as above but with precision `f16`. Values should be down-converted to half precision.

### field_vector_i8
Same as above but with precision `i8`. Values should be quantized to int8.

### field_vector_wrong_dimension
Field configured as `vector <256, f32>` but document contains a 128-element array.
Should log and skip the entire document (type_mismatch).

### field_null_value
BSON null on a mapped field. Output should be a turbopuffer null — the field is included
in the output but with a null value (attribute is nullable).

### field_type_mismatch
Field configured as `int` but document contains a string value. Should log and skip the
entire document. No DLQ write, no retry.

### field_unmapped_ignored
Document has fields `name`, `email`, `age` but only `name` and `email` are in the mapping config.
`age` should not appear in the turbopuffer output.

## 4. Change Stream Event Mapping

Unit tests. The system under test is the event mapper — takes a change stream event struct + collection
config and returns a turbopuffer action (upsert, patch, delete, stop, or skip). No external dependencies.

### event_insert :white_check_mark:
Insert event with a full document containing mapped fields. Should produce a turbopuffer upsert
with all mapped fields present plus a `_clusterTime` attribute from the event's cluster time.

### event_replace :white_check_mark:
Replace event with a full document. Should produce a turbopuffer upsert identical in shape to an insert —
full document replacement, all mapped fields + `_clusterTime`.

### event_update_mapped_fields :white_check_mark:
Update event where `updateDescription.updatedFields` includes a mapped field. Should produce a
turbopuffer patch containing only the changed mapped fields. Unchanged mapped fields should not
be included in the patch.

### event_update_unmapped_fields_only :white_check_mark:
Update event where `updateDescription.updatedFields` only contains fields not in the mapping config.
Should be a noop — no turbopuffer action produced.

### event_update_field_removed :white_check_mark:
Update event where `updateDescription.removedFields` includes a mapped field. Should produce a
turbopuffer patch with that field explicitly set to null.

### event_delete :white_check_mark:
Delete event. Should produce a turbopuffer delete action using the document's `_id`.

### event_delete_mirror_disabled :white_check_mark:
Delete event on a collection with `mirrorDeletes: false`. Should be skipped — no turbopuffer action.

### event_invalidate :white_check_mark:
Invalidate event. The mapper should return a stop/fatal signal. The resume token is no longer valid —
the operator needs to re-backfill.

### event_rename :white_check_mark:
Rename event. Should log a warning and return skip — no turbopuffer action.

### event_drop :white_check_mark:
Drop event. Should log a warning and return skip — no turbopuffer action.

## 5. Batching

Unit tests. The system under test is the batch buffer — accepts actions, flushes when a threshold
is hit. Uses a FlushFunc spy to record calls and assert what gets flushed without a real connection.

### batch_flush_on_count :white_check_mark:
Push exactly `flush_count` actions with unique IDs. Assert that a flush is triggered and all
actions are included.

### batch_flush_on_size :white_check_mark:
Set `flush_size` low, `flush_count` high. Push upsert actions with large Attributes until size
threshold is crossed. Assert flush fires before `flush_count` is reached. The triggering action
should be included in the flush.

### batch_flush_on_interval :white_check_mark:
Push 1 action (below count and size thresholds) with a short flush interval (50ms).
Wait for the timer to fire. Assert flush fires with whatever is buffered.

### batch_dedup_same_id :white_check_mark:
Push two upserts for the same `_id` into the buffer before a flush. Only the most recent update
should appear in the flushed batch. Batch should contain 1 action, not 2.

### batch_dedup_insert_then_delete :white_check_mark:
Push an upsert then a delete for the same `_id` before a flush. Only the delete should be sent
in the flushed batch.

### batch_empty_no_flush :white_check_mark:
Create batcher, call Flush() immediately. No flush should occur — the FlushFunc spy should
receive zero calls.

### batch_skip_action_ignored :white_check_mark:
Set `flush_size` to 100. Push an ActionSkip with large attrs (~80 bytes), then a real upsert
(~60 bytes). Assert no flush fires (skip's bytes were not accumulated — total is ~60, not ~140).
Flush manually and assert the batch contains only the upsert.

### batch_preserves_insertion_order :white_check_mark:
Push actions for IDs "c", "a", "b". Flush manually. Assert flushed batch order is ["c", "a", "b"].

### batch_resume_token_tracks_latest :white_check_mark:
Push 3 actions with different resume tokens. Flush manually. Assert the FlushFunc received the
third event's token.

### batch_flush_error_propagated :white_check_mark:
Set FlushFunc to return an error. Set `flush_count=1`. Call Add with 1 action. Assert Add
returns the FlushFunc's error.

### batch_reusable_after_flush :white_check_mark:
Set `flush_count=2`. Push 2 actions (triggers auto flush), then push 1 more and call Flush().
Assert FlushFunc was called twice — first with 2 actions, second with 1.

### batch_dedup_size_overcounts :white_check_mark:
Set `flush_size=150`. Push the same ID three times (~60 bytes each). The size accumulator
hits ~180 even though the logical batch is 1 deduped action of ~60 bytes. Assert flush fires.
Documents current over-counting behavior (safe — just flushes sooner).

## 6. Error Handling & Retry

Unit tests. The system under test is the writer and error classifier. Use a fake tpuf client to
control responses and a fake DLQ writer to capture entries. Retry is handled by the turbopuffer
SDK internally — these tests only verify that on SDK failure, actions are correctly routed to
the DLQ with the appropriate error kind.

### error_type_mismatch_skip
Document has a type mismatch. Should log and skip. No DLQ write, no call to tpuf.

### error_id_missing_skip
Document has no `_id`. Should log and skip. No DLQ write, no call to tpuf.

### error_write_success_no_dlq
Fake tpuf client succeeds. Assert no DLQ entries were written.

### error_write_failure_dlq
Fake tpuf client returns an error. Assert all actions in the batch are written to DLQ.

### error_classify_rate_limited
Fake tpuf client returns a 429 error. Assert DLQ entry has error kind `rate_limited`.

### error_classify_server_error
Fake tpuf client returns a 500 error. Assert DLQ entry has error kind `server_error`.

### error_classify_network_error
Fake tpuf client returns a non-HTTP error (e.g. connection refused). Assert DLQ entry
has error kind `network_error`.

## 7. Backfill

Integration tests. These require a real or faked MongoDB (for cursoring over documents) and a fake
tpuf client. The system under test is the backfill loop — ping for operationTime, query a page,
upsert to tpuf, advance the cursor, repeat until an empty page.

### backfill_single_page
Collection has 50 documents, page size 128. Exercises the full loop: ping for operationTime,
query page, upsert to tpuf, terminate on empty next page. Assert tpuf receives all 50 documents
and cursor is set to the last document's `_id`.

### backfill_multi_page
Collection has 300 documents, page size 128. Loop should iterate 3 times (128 + 128 + 44) and
terminate on the 4th empty page. Assert cursor advances after each page flush.

### backfill_cluster_time_from_ping
Assert that tpuf upserts use the `operationTime` from the ping command issued before each page
query — not the documents' own timestamps. Fake the ping to return a known operationTime and
verify it appears as `_clusterTime` on the upserted documents.

### backfill_custom_page_size
Collection config sets `backfillPageSize: 64`. With 100 documents, should produce 2 pages
(64 + 36) instead of 1 page of 100.
