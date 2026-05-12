# RFC 0034: Three-Engine Certified Data Execution Fabric

## Purpose

Define CG-22 as the three-engine certified data execution fabric that extends
ShardLoom beyond a single batch-oriented Vortex execution path while preserving
the same Python, SQL, DataFrame, adapter, certificate, and no-fallback user
contract.

CG-22 exists because making ShardLoom exceptional is not a matter of chasing
every incumbent feature. The stronger target is to combine capabilities that
are usually separated:

```text
one importable user experience
+ three ShardLoom-native execution engines
+ Vortex-native storage
+ no fallback
+ machine-readable proof of what happened
```

This RFC is intentionally content-rich. It should remain stable before broad
cross-document refactors fold its details into CG-8, CG-9, CG-10, CG-11,
CG-16, CG-19, CG-20, CG-21, and the implementation plan.

## Status

Accepted as CG-22 intake material.

This RFC does not add runtime behavior, dependencies, readers, writers,
adapters, SQL execution, DataFrame runtime, UDF runtime, streaming runtime,
state-store runtime, benchmark execution, superiority claims, best-default
claims, or fallback execution.

The "unparalleled" language in this RFC is a design north star, not a product
claim. Public claims remain blocked until correctness, benchmark, native I/O,
execution-certificate, workload-scope, and no-fallback evidence exists.

## CG-22 definition

CG-22: Three-Engine Certified Data Execution Fabric

ShardLoom becomes a certified data execution fabric when one importable user
experience can express batch, live, and hybrid analytical workloads; ShardLoom
selects or honors a ShardLoom-native engine mode; execution remains
Vortex-native where supported; all materialization, freshness, fidelity,
state, snapshot, and fallback boundaries are reported; and external systems
remain baselines or sources/sinks, not fallback execution engines.

The category target is:

```text
ShardLoom is a certified data execution fabric: batch, live, and hybrid
analytics under one Python/SQL/DataFrame/adapter UX, with explicit evidence
for execution, freshness, materialization, fidelity, and fallback.
```

CG-22 is logically after CG-21. CG-21 defines the complete user data workflow
surface. CG-22 adds a multi-engine execution contract beneath that workflow.

## Market and systems lessons

ShardLoom should not be positioned merely as one of these:

```text
a Vortex query engine
a DataFrame engine
a streaming engine
a NoSQL database
a Spark replacement
```

The sharper category is a certifying native execution layer for multiple
execution modes.

Streaming databases and stream processors show that users value familiar table
and SQL abstractions even when computation becomes continuous. Dynamic-table
models connect streams, tables, continuous queries, and materialized-view-like
results. Stateful stream systems show that real-time computation requires
local state, changelogs, checkpoints, recovery, and memory/disk tradeoffs.
Vortex-backed accelerators show market interest in Vortex as a high-performance
analytical storage and acceleration substrate.

ShardLoom's opportunity is adjacent but more opinionated:

```text
not a broad application runtime
not an external-engine wrapper
not a black-box accelerator
but a certifying ShardLoom-native execution fabric
```

## Unparalleled formula

CG-22 should make ShardLoom strong along these axes:

1. Same UX for batch, live, and hybrid.
2. Vortex-native encoded execution where possible.
3. No external fallback.
4. No hidden materialization or decode.
5. Real-time state with snapshot and freshness certificates.
6. Hot/cold storage: NoSQL-like write path plus columnar analytical segments.
7. Workload-scoped capability certification.
8. Self-benchmarking against Spark, DataFusion, Polars, DuckDB, Dask, pandas,
   and related engines as baselines only.
9. Conda/importable Python-first distribution.
10. Agent-safe, deterministic diagnostics.

Most systems can claim some of these. CG-22 is about combining them without
weakening the no-fallback identity.

## Three-engine model

ShardLoom should formalize three internal engine modes:

```text
batch
live
hybrid
```

They share one user API and one logical plan surface. They differ in execution
policy, source boundedness, state requirements, freshness semantics, output
mode, and certification evidence.

Example target UX:

```python
import shardloom as sl

ctx = sl.context(
    engine="hybrid",          # "batch", "live", "hybrid", or "auto"
    freshness="30s",
    consistency="snapshot",
)

orders = ctx.read("s3://lake/orders/")
customers = ctx.read("s3://lake/customers/")

result = (
    orders
    .filter(sl.col("status") == "complete")
    .join(customers, on="customer_id")
    .group_by("customer_id")
    .agg(
        total_amount=sl.sum("amount"),
        order_count=sl.count(),
    )
)

result.explain()
result.certify()
result.write_vortex("s3://lake/customer_summary/")
```

The user expression stays stable:

```text
read
filter
select
join
group_by
agg
write
explain
profile
certify
```

The selected internal engine changes beneath it.

## Engine 1: batch

`batch` is the current foundation.

Purpose:

```text
historical analytics
batch ETL
large immutable Vortex datasets
local/object-store scans
benchmarkable exact workloads
```

Promise:

```text
ShardLoom executed this finite workload itself.
It used Vortex-native encoded data where supported.
It avoided decode/materialization where possible.
It emitted correctness, execution, Native I/O, and benchmark evidence.
It did not call Spark, DataFusion, Polars, DuckDB, Velox, or another engine as fallback.
```

Batch remains the engine that proves the core Vortex-native thesis. CG-2,
CG-5, CG-6, CG-7, CG-13, CG-16, and CG-19 continue to own the lower-level
evidence required before batch claims can be published.

## Engine 2: live

`live` is the real-time engine.

Purpose:

```text
event streams
CDC
append/upsert/delete streams
windowed aggregations
real-time dashboards
data quality monitoring
alerts
streaming joins
incremental materialized views
```

Promise:

```text
ShardLoom continuously maintains results as inputs change.
It tracks watermarks, checkpoints, state, lag, and output mode.
It emits freshness, state, and checkpoint certificates.
It does not pretend unbounded workloads are ordinary batch jobs.
```

The live engine needs streaming-specific concepts:

```text
event time
processing time
watermarks
late data
out-of-order data
window state
state TTL
checkpoints
idempotency
retractions
tombstones
output changelogs
continuous materialized tables
```

Live mode must not report support for operations that only make sense for
bounded inputs unless a bounded window, snapshot, or materialized-view
semantics makes the result well-defined.

## Engine 3: hybrid

`hybrid` is the main CG-22 opportunity.

The hybrid engine sits between batch analytics and NoSQL/streaming systems.

Purpose:

```text
fresh analytics over historical + recent data
real-time lakehouse tables
CDC into analytical state
feature-store-like offline/online consistency
hot dashboards over cold data
low-latency point/range lookups plus columnar scans
```

Storage shape:

```text
hot layer:
  WAL / changelog
  mutable keyed state
  recent rows/events
  tombstones
  dedupe state

warm layer:
  Vortex micro-segments
  recent statistics
  deletion vectors
  lightweight manifests

cold layer:
  compacted Vortex segments
  optimized encodings
  strong statistics
  object-store friendly layout
```

Query path:

```text
1. Pick a snapshot epoch.
2. Scan/prune cold Vortex segments.
3. Scan/prune warm Vortex micro-segments.
4. Query hot state/delta overlay.
5. Apply tombstones/deletion vectors.
6. Merge into one result.
7. Emit one certificate.
```

Write path:

```text
1. Accept append/upsert/delete.
2. Write to changelog.
3. Update hot state.
4. Flush to Vortex micro-segments.
5. Compact into optimized Vortex segments.
6. Publish a snapshot/manifest.
```

Hybrid is not an OLTP database and not merely a streaming engine. It is a
real-time analytical state engine:

```text
NoSQL-like freshness and mutation handling
+ analytical-columnar persistence
+ ShardLoom-native certification
```

## Why ShardLoom is suited for this

The current architecture already has many primitives required for CG-22:

- RFC 0031 defines native work/result streams, representation state, source
  and sink capability reports, pushdown reports, materialization reports,
  native I/O certificates, and explicit `fallback_attempted=false`.
- RFC 0032 defines broad user capability surfaces, including ingestion, schema
  contracts, cleaning, joins, aggregations, windows, incremental recompute,
  CDC-like change intake, checkpoints, watermarks, idempotency, writes,
  export, diagnostics, migration, benchmark evidence, and no-fallback evidence.
- RFC 0033 defines the complete user workflow from install/import through
  read, validate, transform, write, explain, certify, benchmark, and diagnose.

CG-22 does not bolt real-time onto an unrelated engine. It elevates existing
concepts into a multi-engine execution model.

## Engine choice certificates

ShardLoom should not merely let users choose an engine. It should prove which
engine ran and why.

Example:

```json
{
  "requested_engine": "auto",
  "selected_engine": "hybrid",
  "allowed_engines": ["batch", "hybrid"],
  "source_boundedness": "bounded_base_with_live_delta",
  "output_mode": "snapshot_table",
  "freshness_target_ms": 30000,
  "snapshot_epoch": 1842938123,
  "hot_delta_rows": 91822,
  "warm_micro_segments": 14,
  "cold_vortex_segments": 823,
  "tombstones_applied": 331,
  "fallback_attempted": false,
  "external_engine_invoked": false
}
```

Internal ShardLoom engine selection is allowed:

```text
Selecting batch, live, or hybrid inside ShardLoom
= certified internal execution choice.
```

External fallback remains forbidden:

```text
Calling Spark, DataFusion, Polars, DuckDB, Dask, Flink, Materialize, or another
engine because ShardLoom cannot execute something
= forbidden fallback.
```

## Single logical plan with engine-specific lowering

The user should write one pipeline. ShardLoom should lower it differently
depending on engine selection, source boundedness, and output mode.

```text
Logical plan:
  read -> filter -> join -> group_by -> write

Batch lowering:
  finite scan -> encoded kernels -> output file/table

Live lowering:
  stream source -> stateful operator graph -> changelog/materialized view

Hybrid lowering:
  cold segment scan + hot delta overlay + snapshot merge
```

The logical plan should be portable across modes only when semantics are
well-defined. Engine-specific lowering must fail with deterministic diagnostics
when the requested operation is unsupported for the selected engine.

## Capability matrix per engine

Every operator should eventually report support per engine mode.

Example:

```text
filter:
  batch: certified
  live: certified for append/upsert streams
  hybrid: certified with delta overlay

global sort:
  batch: supported
  live: unsupported unless bounded window
  hybrid: supported only for snapshot output

join:
  batch: planned/certified by join type
  live: supported for keyed/windowed/stateful joins
  hybrid: supported when snapshot/state constraints are satisfied
```

This prevents real-time support from being reported as fake "batch but
faster." Each engine mode must carry separate support, limitation, correctness,
state, materialization, and benchmark evidence.

## Hot/cold certificates

Hybrid execution should emit evidence that explains which temporal and storage
layers contributed to an answer.

Required certificate dimensions:

```text
base_snapshot_id
hot_changelog_range
watermark
checkpoint_id
tombstones_applied
micro_segments_scanned
cold_segments_scanned
delta_rows_merged
state_bytes
compaction_level
freshness_lag_ms
fallback_attempted=false
external_engine_invoked=false
```

Most tools say a query ran. CG-22 requires ShardLoom to say which temporal
layers contributed to the answer and what freshness, state, and fidelity
evidence backs it.

## Continuous materialized views

CG-22 should adopt the useful systems lesson that continuous queries can
maintain dynamic result tables with materialized-view-like semantics when the
input stream and query semantics permit it.

ShardLoom should add its own stricter evidence:

```text
encoded representation evidence
Native I/O evidence
materialization boundaries
hot/cold contribution evidence
fallback invariants
freshness certificates
checkpoint certificates
```

The view is not merely maintained. It is certified.

## NoSQL-inspired state, analytical by default

CG-22 should borrow from NoSQL and streaming state systems:

```text
WAL/changelog
primary keys
idempotency keys
upserts
tombstones
LSM-style flush/compaction
hot state
TTL
snapshot reads
range/point lookups
```

But ShardLoom remains analytical:

```text
columnar Vortex persistence
statistics/pruning
encoded execution
group/aggregate/window support
Python/SQL/DataFrame ETL
benchmark/certification
```

This is the hybrid niche.

## User-facing promise

Eventually users should be able to choose:

```python
ctx = sl.context(engine="batch")
ctx = sl.context(engine="live")
ctx = sl.context(engine="hybrid")
```

Or:

```python
ctx = sl.context(
    engine="auto",
    allowed_engines=["batch", "hybrid"],
    forbid_external_fallback=True,
)
```

Then ask:

```python
pipeline.explain()
pipeline.certify()
pipeline.profile()
```

And get an answer like:

```text
This workload can run in batch and hybrid.
It cannot run in live mode because it contains an unbounded global sort.
Hybrid mode will scan 812 cold Vortex segments, 12 warm micro-segments, and
48,991 hot delta rows.
Expected freshness: <= 30 seconds.
Fallback attempted: false.
External engine invoked: false.
```

## Core contract vocabulary

### EngineMode

Values:

```text
batch
live
hybrid
auto
```

Semantics:

- `batch`: finite workload over bounded inputs.
- `live`: continuous workload over unbounded or continuously updated inputs.
- `hybrid`: snapshot or fresh analytical workload over cold base data plus
  warm/hot deltas.
- `auto`: ShardLoom may choose among allowed ShardLoom-native engines and must
  emit an `EngineSelectionReport`.

### Boundedness

Values:

```text
bounded
unbounded
bounded_with_live_delta
```

Semantics:

- `bounded`: all inputs have a finite snapshot scope.
- `unbounded`: at least one input is continuously updated without an explicit
  finite snapshot boundary.
- `bounded_with_live_delta`: a finite base snapshot is combined with a tracked
  live delta range.

### UpdateMode

Values:

```text
append_only
upsert
delete_tombstone
changelog
retraction
```

Semantics:

- `append_only`: new rows/events only.
- `upsert`: keyed inserts and updates.
- `delete_tombstone`: deletes represented through tombstones or deletion
  vectors.
- `changelog`: ordered insert/update/delete event stream.
- `retraction`: prior output records can be withdrawn or corrected.

### OutputMode

Values:

```text
snapshot
append
changelog
materialized_view
serving_state
```

Semantics:

- `snapshot`: finite result at a selected epoch.
- `append`: append-only result stream or file output.
- `changelog`: result updates as insert/update/delete/retract events.
- `materialized_view`: continuously maintained table-like result.
- `serving_state`: keyed/range-queryable state intended for low-latency reads.

## Reports and certificates

### EngineSelectionReport

Fields:

```text
requested_engine
selected_engine
allowed_engines
source_boundedness
update_mode
output_mode
freshness_target_ms
consistency_target
semantic_profile
selection_basis
unsupported_engine_reasons
fallback_attempted=false
external_engine_invoked=false
diagnostics
```

Acceptance:

- `auto` selection must be deterministic for a fixed plan, capability snapshot,
  and configuration.
- Rejected engines must include machine-readable reasons.
- External fallback is never an engine selection option.

### FreshnessCertificate

Fields:

```text
certificate_id
engine_mode
freshness_target_ms
freshness_lag_ms
watermark
source_offsets
checkpoint_id
snapshot_epoch
late_data_policy
out_of_order_policy
fallback_attempted=false
diagnostics
```

Acceptance:

- Freshness claims require measured lag or explicit unknown status.
- Late and out-of-order policies must be visible.
- Missing freshness evidence blocks live/hybrid certification for freshness
  sensitive workloads.

### StateCertificate

Fields:

```text
certificate_id
engine_mode
state_kind
state_store_kind
key_schema
state_bytes
state_rows
window_state
ttl_policy
checkpoint_id
changelog_range
restore_required
recovery_status
fallback_attempted=false
diagnostics
```

Acceptance:

- Stateful live/hybrid operators must report state shape and recovery posture.
- Durable-state claims require checkpoint/changelog evidence.
- Exactly-once claims remain forbidden without commit/checkpoint/sink
  idempotency proof.

### DeltaOverlayCertificate

Fields:

```text
certificate_id
base_snapshot_id
snapshot_epoch
hot_changelog_range
warm_micro_segment_refs
cold_segment_refs
tombstones_applied
deletion_vectors_applied
delta_rows_merged
dedupe_policy
merge_ordering
consistency_target
fallback_attempted=false
diagnostics
```

Acceptance:

- Hybrid result certification requires explicit base/delta/tombstone evidence.
- Hot/cold merge ordering must be deterministic.
- Missing or ambiguous tombstone semantics block certification.

### HotColdContributionReport

Fields:

```text
hot_rows_scanned
hot_state_lookups
warm_micro_segments_scanned
cold_vortex_segments_scanned
segments_pruned
bytes_read
bytes_decoded
rows_materialized
state_bytes_read
changelog_bytes_read
fallback_attempted=false
```

Acceptance:

- Hybrid explain/profile/certify output must show contribution by layer.
- Any decode/materialization must be explicit and tied to a boundary report.

### ContinuousViewCertificate

Fields:

```text
view_id
engine_mode
input_sources
query_digest
state_certificate
freshness_certificate
checkpoint_id
output_mode
retraction_supported
materialization_boundary_refs
native_io_certificate_refs
execution_certificate_refs
fallback_attempted=false
diagnostics
```

Acceptance:

- A continuous view is certified only when its input, state, checkpoint,
  freshness, output, and no-fallback evidence are all present.
- Report-only or plan-only views must not be presented as executing.

## Roadmap

### Phase 1: engine modes in contracts

Scope:

```text
EngineMode
Boundedness
UpdateMode
OutputMode
EngineSelectionReport
unsupported-for-engine diagnostics
capabilities engines
```

Acceptance:

```text
capabilities engines
EngineSelectionReport
unsupported-for-engine diagnostics
no external fallback
no new dependencies
no runtime execution
```

### Phase 2: live source/change contract

Define ShardLoom-native change records:

```text
ChangeRecord:
  key
  op: insert/update/delete/retract
  sequence
  event_time
  processing_time
  source_offset
  schema_digest
  payload
```

Acceptance:

```text
append/upsert/delete vocabulary
bounded fixture streams
watermark/checkpoint report
no Kafka/Flink/Materialize runtime dependency
no external fallback
```

### Phase 3: in-memory live prototype

Start narrow:

```text
filter
project
count
count_where
simple group_count
```

Over fixture-backed bounded streams.

Acceptance:

```text
state report
checkpoint report
output changelog
fallback_attempted=false
external_engine_invoked=false
no production streaming claim
```

### Phase 4: hybrid base + delta overlay

First major hybrid milestone:

```text
local Vortex base
+ fixture-backed hot delta overlay
+ tombstones
+ snapshot epoch
+ certified merged result
```

Acceptance:

```text
DeltaOverlayCertificate
snapshot certificate
hot/warm/cold contribution counts
no object-store production claim
no broad table semantics claim
no external fallback
```

### Phase 5: Vortex micro-segment flush

Turn live data into Vortex:

```text
hot state / append batch
-> Vortex micro-segment
-> manifest
-> queryable warm layer
```

Acceptance:

```text
micro-segment write
local commit report
recovery/cleanup report
Native I/O certificate
no object-store production claim unless CG-10/CG-4 evidence exists
```

### Phase 6: compaction planner

Make hybrid sustainable:

```text
small segment pressure
tombstone pressure
partition skew
layout health
compaction candidates
```

Acceptance:

```text
compaction plan
layout health report
state/freshness impact estimate
no execution claim until commit/recovery is certified
```

### Phase 7: Python UX

Expose engine modes cleanly:

```python
ctx = sl.context(engine="hybrid")

pipeline = (
    ctx.read_table("orders", freshness="30s", primary_key="order_id")
    .filter(sl.col("status") == "complete")
    .group_by("customer_id")
    .agg(total=sl.sum("amount"))
)

pipeline.explain()
pipeline.certify()
pipeline.materialize("customer_order_totals")
```

Acceptance:

```text
Python engine mode selection
capability-checked plan lowering
unsupported-for-engine diagnostics
certificates exposed through Python
no Python-side fallback execution
```

## Non-goals

CG-22 must avoid scope inflation:

- Do not become a general NoSQL database.
- Do not become a hidden Flink, Kafka Streams, Materialize, Spark, DataFusion,
  Polars, DuckDB, or Dask wrapper.
- Do not make external engines runtime dependencies.
- Do not call file polling real streaming unless it has state, watermark, and
  checkpoint semantics.
- Do not claim exactly-once without commit/checkpoint/sink idempotency proof.
- Do not support every SQL query in live mode by lying about infinite input.
- Do not hide hot/cold merge semantics.
- Do not make the core Conda package heavy.
- Do not publish performance, freshness, replacement, or best-default claims
  without evidence.

The phrase to preserve:

```text
ShardLoom can be broad in user experience, but strict in execution truth.
```

## Certification blockers

The following block CG-22 certification for a declared workload:

- selected engine is not reported
- requested engine is ignored
- `auto` selection lacks deterministic selection basis
- source boundedness is unknown for a workload that needs live/hybrid semantics
- update mode is unknown for mutable inputs
- output mode is unknown for live/hybrid outputs
- freshness target is claimed without freshness evidence
- stateful operator lacks state/checkpoint/changelog evidence
- hybrid result lacks base/delta/tombstone contribution evidence
- hot/cold merge ordering is ambiguous
- exact-once or idempotency is claimed without proof
- live mode reports support for an unbounded operation with no bounded window or
  materialized-view semantics
- external engine is invoked as fallback
- materialization, decode, or fidelity loss is hidden
- missing `fallback_attempted=false`

## Shared policy and lifecycle contracts

CG-22 engine selection must consume and emit the same cross-surface contracts
used by CLI, Python, future REST, and agent workflows:

- `ShardLoomExecutionPolicy` supplies requested engine, allowed engines,
  fallback policy, materialization policy, result policy, evidence policy,
  effect policy, memory policy, spill policy, network policy, and agent policy.
- `QueryLifecycleContract` supplies shared accepted/planned/blocked/queued/
  running/cancelling/cancelled/failed/succeeded/expired states for batch, live,
  hybrid, and auto-selected work.
- `EvidenceArtifactEnvelope` and `EvidenceArtifactSafety` wrap engine selection,
  state, freshness, delta-overlay, hot/cold contribution, and continuous-view
  evidence with stable identity, retention, redaction, and no-fallback fields.

Internal engine selection is allowed. External query-engine fallback is not.
When `auto` selects batch, live, or hybrid, the engine selection report must
record the selected ShardLoom-native engine and the reason unsupported external
providers were not invoked.

## Compatibility with existing gates

CG-22 should not take over lower-level responsibilities:

| Area | Primary owner | CG-22 relationship |
| --- | --- | --- |
| Batch encoded primitives | CG-2 / CG-13 | Batch engine evidence foundation |
| Physical kernels | CG-7 | Engine-specific operator lowering depends on kernel evidence |
| Streaming/parallel/adaptive runtime | CG-8 | Live and hybrid execution mechanics |
| Table intelligence | CG-9 | Snapshots, partitions, deletes, tombstones, CDC |
| Object-store/distributed runtime | CG-10 | Remote hot/warm/cold data and commit behavior |
| Python/API | CG-11 / CG-20 / CG-21 | Engine modes surface through one importable UX |
| Correctness | CG-5 | Every engine path needs correctness evidence |
| Benchmarks | CG-6 | Engine claims need benchmark evidence |
| Execution certificates | CG-16 | Engine selection and execution evidence compose with certificates |
| Native I/O | CG-19 | Source/sink/result envelopes feed batch/live/hybrid modes |
| User data workflow | CG-21 | CG-22 is the multi-engine layer beneath that workflow |

## References

- Materialize streaming database overview:
  `https://materialize.com/guides/streaming-database/`
- Confluent Kafka Streams architecture:
  `https://docs.confluent.io/platform/current/streams/architecture.html`
- Spice Cayenne data accelerator:
  `https://spiceai.org/docs/components/data-accelerators/cayenne`
- Apache Flink dynamic tables:
  `https://nightlies.apache.org/flink/flink-docs-master/docs/concepts/sql-table-concepts/dynamic_tables/`
- Universal Native I/O Envelope:
  `docs/rfcs/0031-universal-native-io-envelope.md`
- World-Class SQL, Operator, Function, Adapter, and User Capability Surface:
  `docs/rfcs/0032-world-class-sql-operators-functions-adapters-user-capability.md`
- User Data Workflow and ETL Surface:
  `docs/rfcs/0033-user-data-workflow-etl-surface.md`

## Recommendation

Make CG-22 the architectural north star for a three-engine certified data
system:

```text
batch for historical encoded analytics
live for continuous incremental computation
hybrid for fresh analytical state over Vortex base plus NoSQL-like delta overlays
```

All three modes should live under one importable Python/SQL/DataFrame UX and
emit proof of:

```text
the selected engine
freshness
data-layer contribution
decode/materialization boundaries
skipped work
unsupported work
certified work
no fallback
```

That is the path to a system that can tell users:

```text
Here is the result.
Here is how fresh it is.
Here is which engine produced it.
Here is which data layer contributed.
Here is what was decoded or materialized.
Here is what was skipped.
Here is what was certified.
Here is what was not supported.
Here is proof no fallback happened.
```
