<!-- SPDX-License-Identifier: Apache-2.0 -->

# Resident Native Results

Use a Unix build with `vortex-local-primitives` for the interfaces on this page.
They execute through the pinned native Vortex provider. Unsupported requests fail
explicitly; no external query engine participates.

## Typed Rust Memory Intake

Keep a `ResidentVortexSession` and prepared operation across calls. Typed intake
validates borrowed columns and copies their values into immutable native Vortex
buffers owned by that session. It becomes queryable without publishing a file.

```rust
use shardloom_vortex::{
    resident_memory_source::{
        MemoryColumn, MemoryColumnValues, MemorySourceBounds, ResidentMemorySource,
    },
    resident_session::ResidentVortexSession,
};

fn main() -> shardloom_core::Result<()> {
    let session = ResidentVortexSession::new(4 * 1024 * 1024, 2)?;
    let ids = [Some(i64::MAX), None];
    let labels = [Some("東京"), None];
    let source = ResidentMemorySource::from_columns(
        &session,
        &[
            MemoryColumn { name: "id", values: MemoryColumnValues::Int64(&ids) },
            MemoryColumn { name: "label", values: MemoryColumnValues::Utf8(&labels) },
        ],
        MemorySourceBounds::default(),
    )?;
    let prepared = source.prepare_projection(&["label", "id"], None, None)?;
    let arrays = prepared.execute_arrays()?;
    assert_eq!(arrays.row_count(), 2);
    drop(arrays);
    let rows = prepared.execute()?;
    println!("{}", rows.values_json.value());
    Ok(())
}
```

The JSON result contains both complete rows, including the exact signed 64-bit
integer and null values. `execute_arrays()` retains native arrays;
`execute()` explicitly materializes complete bounded JSON. Repeated calls execute
again, without an answer cache. Owned arrays and JSON may outlive their prepared
operation and source; their retained buffers keep the corresponding reservations.

`MemoryColumnValues` admits nullable `Int64`, finite `Float64`, `Bool`, and `Utf8`
slices. All four retain nullable native DTypes, including an all-valid or empty
column. Duplicate names, names outside 1–256 UTF-8 bytes, mismatched lengths,
nonfinite floats, and more than 64 columns are rejected before native allocation.
Rust intake permits empty typed sources. Nested, decimal, binary, and arbitrary
externally allocated `ArrayRef` intake are outside this interface.

`prepare_projection(columns, filter, limit)` accepts an optional native Vortex
boolean expression and an optional source-order limit. The provider binds and
executes the filter; a null predicate excludes the row. Filtering precedes the
limit. This is the Rust provider boundary; public generated-row collect below
admits the supplied rows without additional predicates or limits.

`MemorySourceBounds` defaults to 65,536 input/output rows, 32 MiB of validated
input bytes, and 32 MiB of native output bytes. Callers may choose stricter bounds.
JSON also has an 8 MiB ceiling, reduced by a smaller requested output-byte bound.
Exceeding a bound fails the complete request instead of returning a truncated
preview. Use an explicit admitted export workflow for larger results.

The session pool admits native value, offset, and validity buffers before their
allocation and retains credits for their lifetime. It also owns completed JSON
capacity. Caller/parser storage, array metadata, upstream scratch that does not
use the session allocator, and process RSS are outside this accounting scope.
This is an immutable memory snapshot API, not an asynchronous ingestion queue,
native Python binding, general shared-memory import, or durable write API.

## Public Generated-Row Collect

The existing public workflow command accepts a bounded supplied-row batch:

```sh
shardloom run dataframe \
  --generated-source-kind user_rows \
  --generated-schema 'id:int64,label:utf8' \
  --generated-rows 'id=9223372036854775807,label=%CE%BB;id=-9223372036854775808,label=hello' \
  --request collect --bounded true \
  --execution-policy native_vortex --materialization-policy bounded \
  --memory-gb 1 --max-parallelism 2 --format json
```

The complete response reports `generated_rows_memory_collect`,
`publication_state=visible_in_memory`, `durable=false`, `write_io_performed=false`,
`fallback_attempted=false`, and `external_engine_invoked=false`. There is no
intermediate file. A persistent public worker retains the runtime and budget;
each call validates and constructs a fresh immutable batch from the supplied
values. Worker request frames are capped at 16 MiB before JSON parsing, excluding
the newline delimiter. Each request admits at most 4,096 string arguments;
unknown request metadata is skipped without materializing its JSON tree, and
duplicate argument lists fail. An oversized frame returns an error and terminates the
worker; malformed bounded requests clear retained execution context. Starting a
new worker permits subsequent valid requests.

This adapter preserves the existing percent-encoded schema and row grammar:
commas separate fields, semicolons separate rows, and reserved characters inside
names or values must be percent-encoded. It admits `int64`, finite `float64`,
`bool`, and `utf8`. The grammar has no null token: `null` in a UTF-8 column is the
literal string; `null` in a numeric column is invalid. Use typed Rust intake for
nullable batches. Empty public row payloads are rejected.

The combined raw schema/rows payload is capped at 8 MiB, 64 columns, and 65,536
rows before parsing. Native and JSON output bounds also apply. Explicit supplied
`literal_table`, `calendar`, `dataframe_source_free_projection`, and
`dataframe_generated_with_column` batches use the same path with their existing
shape constraints. This route rejects input/output paths, SQL, filters, limits,
other operation payloads, `prepare_once`, and `zero_decode` materialization.

Python can call this public workflow through its existing CLI-backed worker
transport. This does not add an in-process native Python binding or change every
`LazyFrame` generated-source route.

## Prepared Local File Results

`local_primitives::collect::prepare_rows_in_session(request, session)` retains a
validated native file source and its bound projection/filter. Its
`execute_arrays()` and `execute()` methods return complete owned native arrays
and bounded JSON respectively. `prepare_count_in_session(request, session)`
returns a prepared exact footer count, whose `execute()` returns `u64`.

Direct single-file public count, projection, and exact filter collect calls retain
their prepared operation in the persistent worker. A changed request releases the
previous file handle. Every execution validates source generation; replacement or
mutation fails explicitly, clears the handle, and allows the next call to prepare
the current source. Compatibility-input preparation and manifest/directory routes
have separate policies and do not inherit this single-file reuse guarantee.

The final resident owner signals and joins its background CPU drivers before
ordinary caller teardown returns. Returned native arrays retain that owner until
their last reference is released. Upstream blocking I/O uses a separate provider
pool; an active blocking read remains cooperative and retains its allocator
credits until it returns. This is not a guarantee of interruptible I/O shutdown.

The count route reports zero row reads, decoding, and row materialization. Its
file I/O scope is source-generation checks plus the initial footer open. It does
not fabricate a row-execution report or independent correctness certificate.

### Shared-session serving scope

File execution currently serializes admission within one session. A short
metadata count submitted while a scan holds admission waits for that scan to
finish or return an error. The
[file-backed serving tests](../../shardloom-vortex/src/resident_file_serving_tests.rs)
cover contended arrivals at P1/P4, complete ordered scan values, complete counts,
cooperative scan cancellation after a native read returns, and recovery under
retained-result memory pressure. The focused three-test run passed on September
12; it does not establish full-workspace acceptance.

The deliberately gated fixture records arrival-to-native-callback queue residence
and completion latency. Queue residence includes source-generation admission;
these are fixture diagnostics, not production latency percentiles. The tests
establish completion of the submitted callers, not FIFO fairness, cancellation
while waiting on admission, or interruption of blocked I/O. They do not run
small-query traffic during ingest or change the scheduler. Native buffer credits
and constructed worker counts retain their existing accounting scope; they do
not measure total RSS or actual CPU utilization.

## Native Array Output

With `vortex-write` enabled, admitted single-file projection, filter/projection,
and source-column alias exports stream native arrays directly into a Vortex
writer. For example, an existing native source with nullable UTF-8 `label` and
integer `id` columns can write a renamed projection:

```sh
shardloom run dataframe --input source.vortex --input-format vortex \
  --request write_vortex --output selected.vortex --bounded true \
  --vortex-primitive expression_project --vortex-columns label,id \
  --vortex-expression-projection '{"structured_columns":[{"name":"renamed_label","source":"label"},{"name":"id","source":"id"}]}' \
  --vortex-source-order-limit 10003 --memory-gb 1 --max-parallelism 2 --format json
```

The writer completes one native leaf at a time. Its bounded handoff retains at
most three input batches across the active writer, channel and pending caller.
It validates source generation, reopens the completed output to check DType and
row count, computes a complete checksum, and then publishes the staged artifact.
Unknown staging files or replaced destination identities are preserved on error.

The response reports `native_vortex_result_export_kind=owned_native_array_stream`
and records native arrays, logical bytes, adapter copies, scalar materialization,
reserved-byte scope and checksum. This adapter performs no Arrow conversion or
scalar row construction. The provider may canonicalize lazy/unsupported physical
encodings for serialization; the counters do not establish zero provider decode
or zero-copy execution. DType and validity are preserved. Original file layout,
user metadata and file-level statistics are not copied or recomputed by this path.

An unfiltered source can report its exact pre-limit footer count. A filtered scan
that stops at its limit reports a lower bound on matches; it does not perform an
extra full scan for that count. Other structured expressions and compatibility
formats retain their separately admitted execution paths.

## Bounded Compatibility Ingest Ownership

With `vortex-write` and `universal-format-io`, the streaming Rust ingest request
accepts `shared_native_memory_budget_bytes(bytes)`. Public bounded compatibility
ingest forwards its memory budget to this option. Imported Arrow value, offset,
validity and child buffers are copied into the native session allocator before
handoff. Their allocation credits survive conversion, queued work and writer
ownership until the last buffer reference is dropped. This intake makes no
zero-copy claim: Arrow's public buffer capacity cannot establish the retained
allocation size of an arbitrary external owner.

The shared pool covers those native copies, prefetch admission, provider host
allocations made through the session allocator, and root layout references.
Original source/input owners until conversion, reader internals, provider codec
or metadata allocations that bypass that allocator, and process RSS are excluded.
Evidence reports the admitted scope and exclusions alongside reserved-byte peaks.

The writer completes one source batch subtree before admitting the next. Native
coalescing and dictionary domains are therefore limited to each source batch;
this physical policy can change output layout, size and throughput. The Rust
option remains explicit, and omitting it preserves the existing ingest policy.
Performance and memory claims require measurements of the selected policy.

### Rust streaming-source migration: held file generations

The September 12 source-generation change adds the required public field
`FlatLocalColumnarStreamSource.source_identities`. This is a Rust source
compatibility change for downstream callers constructing the struct directly;
existing struct literals must be updated. For a manually constructed in-memory
source, or a custom source that does not carry a file-generation guard, add this
field to the existing literal:

```rust
source_identities: Vec::new(),
```

This is a field fragment, not a complete construction example. An empty vector
does not certify source immutability. When wrapping an adapter-produced source,
preserve its identities along with its reader; a combined source must retain
every contributing identity through final writer validation.

For local Parquet intake, prefer
`shardloom_vortex::stream_flat_parquet_columnar_source(path, max_rows)` or
`stream_flat_parquet_columnar_source_with_parallelism(path, max_rows, parallelism)`
over manually assembling the struct. With `universal-format-io`, these
[adapter constructors](../../shardloom-vortex/src/universal_format_io.rs)
automatically capture a held identity on Unix. The optional
`stream_flat_parquet_columnar_source_with_batch_budget` variant lives in
`shardloom_vortex::universal_format_io`; `max_rows` bounds the complete
source, rather than truncating its result. Writing the stream additionally
requires `vortex-write`.

The guard compares the held descriptor and current path's device, inode, length,
modification time and change time. Parquet opens, source pulls and EOF are checked;
the streaming writer checks again after writing and before publication, including
empty streams. A detected change invalidates that identity and fails the request
without fallback. Recreate the source with an adapter constructor after resolving
the mutation. Existing conversion/provider errors remain the primary failure when
the reader itself fails.

These are generation checks, not an atomic filesystem snapshot or a content hash.
Keep the source immutable throughout intake; a source change after the final
check is not atomically excluded by output publication. Automatic compatibility
intake protection currently covers Unix Parquet, including identities carried by
the partition adapter. Arrow IPC, text and other adapters do not acquire this
guard. Non-Unix Parquet retains the existing input route and reports
`source_generation_guard=unavailable_non_unix`.

The
[streaming pressure and mutation tests](../../shardloom-vortex/src/vortex_ingest_pipeline_pressure_tests.rs)
exercise replacement, truncation and same-size mutation with restored modification
time before pull and after EOF, including empty sources, native output validation,
destination preservation and credit release. Their seven cases passed within the
focused 21-test streaming run on September 12. This evidence does not establish
blocked-codec cancellation, crash durability, or protection for other formats.
Ordinary streaming publication flushes, validates and renames; it does not add
file or parent-directory fsync. Broader checks and remaining acceptance stay in
the [implementation plan](../architecture/ingest-performance-implementation-2026-09-12.md).

## Validation Surfaces

`resident_memory_latency` measures full borrowed-view construction, validation,
native intake, binding, filtering/projection, and complete JSON return. It reports
isolated and concurrent shared-session load separately, verifies complete
foreground values against a literal fixture, and retains raw samples. The
default remains `resident_memory_latency 1000` with the original 32-row nullable
five-field fixture. `resident_memory_latency 1000 int64_64k` selects 4,096 rows
of two nonnullable native Int64 columns and returns the final eight exact rows.
That profile has exactly 65,536 raw value bytes; typed-input admission is 65,562
bytes including its 26 bytes of field names. Neither figure is total retained
process memory. Both profiles report p50/p95/p99 for isolated execution and the
same 16,384-row background workload sharing their resident session. Every
foreground result is checked completely; the background's complete independent
values are checked before overlap and its row count on every operation. Results
apply to the measured profile and do not establish a general latency guarantee.

The `resident_latency` example separates prepared file arrays from JSON rendering;
`scripts/run_resident_call_path_uat.py` compares the public process, persistent
worker, and Python client boundaries. Follow
[local development storage rules](../architecture/local-development-storage.md)
for retained benchmark artifacts. These interfaces alone do not close the broader
PERF acceptance gates or establish a latency claim.
