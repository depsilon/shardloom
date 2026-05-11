# RFC 0033: User Data Workflow and ETL Surface

## Purpose

Define CG-21 as the user data-workflow and ETL surface that extends the CG-20
capability roadmap into a complete, scenario-driven product contract.

CG-21 exists because "world-class capability" is not only a list of adapters,
SQL clauses, functions, or Python helpers. It is the full user journey:

```text
install
  -> import
  -> discover capabilities
  -> read data
  -> inspect schema
  -> define ETL
  -> explain/dry-run
  -> execute
  -> write output
  -> certify what happened
  -> benchmark or compare
  -> diagnose unsupported cases
```

This RFC is intentionally content-rich. It should remain stable before broad
cross-document refactors fold its details into CG-20, CG-19, CG-11, CG-5,
CG-6, CG-8, CG-9, CG-10, CG-16, and related implementation plans.

## Status

Accepted as CG-21 intake material.

This RFC does not add runtime behavior, dependencies, readers, writers,
adapters, SQL execution, DataFrame runtime, UDF runtime, benchmark execution,
superiority claims, best-default claims, or fallback execution.

## CG-21 definition

CG-21: User Data Capability and ETL Certification

ShardLoom is CG-21-certified for a declared workload only when users can
install/import it, declare inputs, inspect schemas, express transformations
through Python/DataFrame/SQL surfaces, execute supported paths natively, write
outputs, inspect diagnostics, prove materialization/fidelity/fallback
boundaries, and compare against baselines with correctness and benchmark
evidence.

CG-21 is the workflow certification layer around CG-20. CG-20 defines broad
user capability surfaces. CG-21 organizes those surfaces around the complete
data-engineering workflow:

```text
read -> validate -> transform -> write -> explain -> certify -> benchmark -> diagnose
```

## Product shape

The target user experience should eventually feel like this:

```python
import shardloom as sl

ctx = sl.context()

orders = ctx.read_parquet("s3://lake/orders/")
customers = ctx.read_vortex("s3://lake/customers.vortex")

active_orders = (
    orders
    .filter(sl.col("status") == "active")
    .select("order_id", "customer_id", "amount", "event_ts")
)

result = (
    active_orders
    .join(customers, on="customer_id")
    .group_by("customer_id")
    .agg(total_amount=sl.sum("amount"), order_count=sl.count())
)

result.explain()
result.profile()
result.write_vortex("s3://lake/output/customer_order_summary.vortex")
```

The contract underneath must remain:

```text
Python / SQL / DataFrame API
        |
        v
ShardLoom logical plan
        |
        v
ShardLoom capability checks
        |
        v
ShardLoom physical plan
        |
        v
ShardLoom-native execution where supported
        |
        v
Native I/O certificates, execution certificates, materialization reports
```

It must not become:

```text
Python API
        |
        v
pandas / Polars / Spark / DataFusion secretly executes the hard parts
        |
        v
ShardLoom reports success
```

CG-21 makes ShardLoom broadly usable without weakening the no-fallback
execution identity.

## Ownership split across gates

CG-21 owns the user-facing workflow contract. Lower-level gates still own the
engine evidence that makes each workflow step real.

| Area | Primary owner | Reason |
| --- | --- | --- |
| Python import, packaging, ergonomic API | CG-20 / CG-11 | User-facing package and API surface |
| DataFrame/query-builder API | CG-20 / CG-21 | User-facing ETL expression layer |
| SQL frontend | CG-20 / CG-21 | User-facing query language |
| Function/operator coverage | CG-20 + CG-7/CG-13 | User-visible breadth plus physical execution |
| Source/sink adapters | CG-20 + CG-19 | UX surface plus Native I/O proof |
| Vortex-native I/O | CG-19 | Preserve encoded representation and native envelopes |
| Writes, commits, recovery | CG-4 + CG-19 + CG-21 | Output side effects and user ETL completion |
| Catalog/table metadata | CG-9 + CG-21 | Table semantics, schemas, partitions, snapshots |
| Object storage/range reads | CG-10 + CG-19 + CG-21 | Remote source/sink behavior |
| Streaming/incremental/backpressure | CG-8 + CG-21 | Workload shape and runtime behavior |
| Correctness fixtures | CG-5 | Trust that outputs are right |
| Benchmarks/baselines | CG-6 | Trust performance/cost claims |
| Execution certificates | CG-16 | Prove what happened |
| No-fallback invariants | All gates | Core ShardLoom identity |

## Common user scenarios

### 1. Install and import

This is the first user experience.

Target:

```bash
conda install shardloom
```

```python
import shardloom as sl

ctx = sl.context()
ctx.smoke_check()
ctx.capabilities()
```

The user should immediately know:

- whether the Python package is importable
- whether the ShardLoom CLI binary is resolved
- which version and protocol are in use
- which features are supported, planned, unsupported, or certified
- whether fallback was attempted
- whether local Vortex paths can run
- whether SQL can run
- whether outputs can be written
- whether pandas/Arrow interop is available

Import and capability discovery must not probe datasets, connect to object
stores, run SQL, touch catalogs, execute adapters, invoke benchmarks, or call
external engines.

### 2. Local file ETL

This is the most common early adoption path.

Target inputs:

- `.vortex`
- `.parquet`
- `.csv`
- `.json` / `.ndjson`
- `.arrow` / Arrow IPC
- partitioned local directories
- compressed files where safe and explicit

Target outputs:

- `.vortex`
- `.parquet`
- `.csv`
- `.json` / `.ndjson`
- `.arrow` / Arrow IPC
- local partitioned outputs

User-facing API:

```python
events = sl.read_csv("events.csv", schema={"id": sl.int64(), "ts": sl.timestamp()})

clean = (
    events
    .filter(sl.col("id").is_not_null())
    .with_column("date", sl.col("ts").date())
    .select("id", "date")
)

clean.write_vortex("events_clean.vortex")
```

CG-21 requires the UX to report:

- input format
- schema source
- type coercions
- parse failures
- rejected rows
- materialization boundary
- output format
- metadata/fidelity loss
- `fallback_attempted=false`

CSV and JSON are compatibility ingestion paths into native envelopes or
Vortex-oriented representation. They are not evidence that the original data
was Vortex-native.

### 3. DataFrame-style API

The DataFrame/query-builder surface should feel familiar:

```python
df = (
    sl.read_vortex("orders.vortex")
    .filter(sl.col("status") == "complete")
    .select("customer_id", "amount")
    .group_by("customer_id")
    .agg(total=sl.sum("amount"))
)
```

It must be lazy and capability-checked.

Important methods:

- `.explain()`
- `.estimate()`
- `.profile()`
- `.certify()`
- `.collect()`
- `.to_pandas()`
- `.to_arrow()`
- `.write_vortex()`
- `.write_parquet()`

A DataFrame API must not immediately execute. It should build a
ShardLoom-native plan, then classify each part as:

- `supported_native`
- `supported_with_materialization`
- `supported_with_rewrite`
- `planned`
- `unsupported`
- `unsafe_rejected`

pandas, Arrow, NumPy, and Python-object conversion must be explicit
materialization boundaries.

### 4. SQL

SQL is part of the user workflow, but must be staged carefully.

```python
ctx.register("orders", sl.read_vortex("orders.vortex"))

result = ctx.sql("""
    SELECT customer_id, COUNT(*) AS n, SUM(amount) AS total
    FROM orders
    WHERE status = 'complete'
    GROUP BY customer_id
""")

result.explain()
result.write_vortex("customer_totals.vortex")
```

SQL maturity should be explicit:

- S0 unsupported
- S1 parse-only
- S2 parse + bind
- S3 native logical plan
- S4 native physical plan
- S5 native execution
- S6 encoded-capable native execution
- S7 workload-certified

Parse-only cannot be reported as SQL execution support. SQL must not call
DataFusion, Spark, DuckDB, Polars, or another engine to execute unsupported
statements.

### 5. pandas interop

pandas should be supported as an explicit boundary:

```python
pdf = pandas.read_csv("input.csv")

native = sl.from_pandas(pdf)
result = native.filter(sl.col("amount") > 100).select("id", "amount")

out_pdf = result.to_pandas()
```

Reports must say:

- `from_pandas`: materialized input boundary
- `to_pandas`: materialized output boundary
- encoded representation preserved: false at pandas boundary
- `fallback_attempted=false`

pandas is a user convenience and source/sink bridge, not a fallback engine.

### 6. Arrow interop

Useful Arrow paths:

- `from_arrow_table()`
- `to_arrow_table()`
- `read_arrow_ipc()`
- `write_arrow_ipc()`
- Arrow C Stream / FFI later

Arrow conversion may imply decoded columnar data, representation loss, or
materialization. That must appear in `MaterializationBoundaryReport` and
`AdapterFidelityReport`.

### 7. Schema discovery and schema evolution

Users need:

```python
src = sl.read_parquet("customers.parquet")

src.schema()
src.describe_schema()
src.validate_schema(required={
    "customer_id": sl.int64().not_null(),
    "email": sl.string().nullable(),
})
```

CG-21 includes:

- schema discovery
- metadata discovery
- type coercion diagnostics
- missing column diagnostics
- extra column policy
- nullable/non-nullable validation
- schema evolution report
- field rename/cast suggestions
- semantic profile

Schema drift, incompatible types, missing columns, duplicate columns, and
unexpected nullability are first-class ETL concerns.

### 8. Data quality checks

Example:

```python
validated = (
    orders
    .require_columns("order_id", "customer_id", "amount")
    .require_not_null("order_id", "customer_id")
    .require_unique("order_id")
    .require_freshness("event_ts", within="24h")
)

validated.write_vortex(
    "orders_valid.vortex",
    rejected_rows="orders_rejected.vortex",
)
```

CG-21 covers:

- required columns
- required types
- nullability
- uniqueness
- ordering
- freshness
- duplicate keys
- invalid records
- parse failures
- constraint violations
- quarantine outputs
- data quality summaries

### 9. Transformation breadth

Common ETL transformation families:

- projection
- filtering
- casts
- rename
- parse
- cleaning
- deduplication
- joins
- aggregations
- windows
- sorts
- limits
- set operations
- explode / unnest
- flatten
- pivot / unpivot
- nested-field projection
- date/time transformations
- string transformations
- numeric transformations
- conditional expressions

Each operation needs maturity status:

- declared
- planned
- plan-only
- native decoded
- native encoded-capable
- fixture-certified
- workload-certified

CG-21 says the user surface exists. CG-2, CG-7, and CG-13 prove whether it
actually executes natively and encoded.

### 10. Joins

Join support should include:

- inner join
- left join
- semi join
- anti join
- equi-join
- multi-key join
- broadcast/small-side strategy
- hash join
- sort/merge join later
- null join semantics
- duplicate key behavior
- join cardinality estimates
- join diagnostics

A join report should include:

- join strategy
- input cardinality estimate
- build/probe side
- memory budget
- spill risk
- materialization requirement
- encoded representation preserved or lost
- `fallback_attempted=false`

Unsupported join shapes should fail with rewrite suggestions, not fallback.

### 11. Aggregations

Common aggregates:

- count
- count_if
- sum
- avg
- min
- max
- first / last
- distinct count
- group by
- having
- approx_count_distinct
- quantiles later
- top-k / frequent values later

Approximate aggregates and sketches must expose:

- `exact=false`
- algorithm
- error bounds
- confidence
- seed/hash policy
- mergeability
- serialization format
- certificate

Approximate analytics must be honest about error, mergeability, and workload
certification.

### 12. Windows

Window support should be staged:

- row_number
- rank / dense_rank
- lag / lead
- running sum
- partitioned aggregates
- frame clauses
- time windows
- session windows later

Window diagnostics must be strong because semantics differ across systems.
Semantic profiles must capture nulls, ordering, timestamps, aggregate
empty-input behavior, and window frame defaults.

### 13. Incremental ETL

Common incremental needs:

- process only new files
- process only changed partitions
- CDC-like change intake
- merge/update/delete where table semantics support it
- checkpoint state
- watermarks
- idempotency keys
- replay boundaries
- late-arriving data handling
- retry/resume

User-facing shape:

```python
pipeline = (
    sl.read_table("orders")
    .incremental(key="order_id", watermark="updated_at")
    .filter(sl.col("updated_at") > sl.watermark("last_success"))
    .merge_into("orders_curated", key="order_id")
)

pipeline.dry_run()
pipeline.execute()
```

The UX belongs in CG-21. Commit/recovery/table/distributed correctness belongs
to CG-4, CG-8, CG-9, CG-10, and CG-19.

### 14. Writes and outputs

Output modes:

- write_vortex
- write_parquet
- write_csv
- write_json
- write_arrow_ipc
- write_partitioned
- append
- overwrite
- merge
- upsert
- delete
- copy/export

Each sink should report:

- sink kind
- write supported
- commit supported
- temporary path policy
- atomicity
- idempotency
- rollback cleanup
- metadata preserved
- statistics written
- partition layout
- fidelity loss
- materialization required
- side effects
- native I/O certificate refs

A sink is not "ETL supported" merely because bytes can be written. Read support,
pushdown support, write support, commit/recovery support, and
benchmarked/certified support are separate maturity levels.

### 15. Object storage

Object storage is a separate user scenario, not only a path string.

Support needs:

- S3-compatible
- GCS
- Azure Blob / ADLS
- HTTP range-read where safe
- credentials
- request budgets
- range reads
- coalescing
- prefetch
- retry
- idempotency
- object versioning
- eventual consistency diagnostics

Example:

```python
orders = sl.read_parquet(
    "s3://company-lake/orders/",
    credentials="foundry_or_env",
    partitioning="hive",
)

orders.explain_io()
```

Reports should include:

- object-store range support
- estimated requests
- bytes requested
- bytes read
- range coalescing
- credential boundary
- network effect
- `fallback_attempted=false`

### 16. Table and lakehouse semantics

File reads are not enough for many ETL users.

Common needs:

- Hive-style partition discovery
- Iceberg-compatible table metadata
- Delta-compatible table metadata
- table snapshots
- schema evolution
- delete/tombstone handling
- manifest import/export
- partition pruning
- compaction planning
- layout health

User-facing shape:

```python
table = sl.table("iceberg://warehouse/orders")

table.snapshot()
table.schema()
table.partitions()
table.layout_health()

result = table.filter(sl.col("country") == "US")
```

Metadata discovery is not table execution certification. ShardLoom may support
metadata discovery before full read/write/commit semantics.

### 17. Relational or warehouse inputs

Eventually users will ask for:

- PostgreSQL
- MySQL/MariaDB
- SQLite
- Snowflake-like exports
- BigQuery-like exports
- JDBC/ODBC bridges

Good support includes:

- metadata discovery
- schema import
- snapshot/export analysis
- proof-backed source pushdown
- migration report

Dangerous support would be ShardLoom sending SQL to PostgreSQL, Snowflake, or
BigQuery and calling that ShardLoom execution. Remote systems may provide
tables, metadata, snapshots, and proof-backed source pushdown. They must not
execute unsupported ShardLoom logic unless the report explicitly identifies
source pushdown and residual ShardLoom execution.

### 18. Logs and events

Logs are a common ETL source.

Support families:

- line-delimited logs
- JSON logs
- application events
- timestamp parsing
- sessionization
- dedup by event_id
- late event handling
- watermarks
- bounded streaming
- micro-batch processing
- quarantine invalid lines

Example:

```python
events = sl.read_ndjson("logs/*.jsonl")

clean = (
    events
    .parse_timestamp("ts", format="iso8601")
    .deduplicate("event_id")
    .with_watermark("ts", lateness="10m")
)
```

This belongs in CG-21 as ETL breadth, with CG-8 handling bounded streaming and
backpressure and CG-19 handling source envelopes.

### 19. Unstructured/document/media-related data

This must be modeled without silently decoding everything.

Supported shape:

- document references
- binary object references
- metadata
- manifests
- extracted text chunks
- embedding/vector references where explicitly enabled
- provenance
- redaction status
- extractor version
- confidence/quality fields

Not supported by default:

- silent OCR
- silent PDF parsing
- silent media decoding
- silent embedding generation
- silent LLM calls

Example:

```python
docs = sl.read_documents("s3://bucket/contracts/")

chunks = (
    docs
    .extract_text(engine="configured_external_extractor")
    .chunk(max_tokens=800)
    .select("document_id", "chunk_id", "text", "provenance")
)
```

This should emit:

- external effect report
- extractor provenance
- redaction status
- materialization cost
- credential boundary
- `fallback_attempted=false`

### 20. UDFs and custom logic

UDF categories:

- Rust-native scalar UDF
- Rust-native aggregate UDF
- Rust-native table function
- WASM scalar UDF
- WASM aggregate UDF later
- Python UDF
- external service UDF
- LLM/API/model-call UDF

A Python UDF should initially be an explicit materialization/effect boundary:

```python
@sl.udf(
    input_types=[sl.string()],
    output_type=sl.string(),
    deterministic=True,
    effects="none",
    materialization="required",
)
def normalize_email(x):
    return x.lower().strip()
```

Required metadata:

- types
- null behavior
- determinism
- volatility
- effect level
- sandbox/resource limits
- materialization requirement
- failure behavior
- timeouts
- retries
- idempotency
- redaction
- license/provenance
- `fallback_attempted=false`

### 21. Explain, estimate, profile, and certify

Every pipeline should support:

```python
pipeline.explain()
pipeline.estimate()
pipeline.profile()
pipeline.certify()
```

Reports should distinguish:

- planned work
- executed work
- native execution
- unsupported work
- materialization boundary
- decode boundary
- rows scanned
- rows materialized
- bytes read
- bytes decoded
- segments pruned
- object-store requests
- memory/spill
- selection-vector use
- encoded representation status
- `fallback_attempted=false`

### 22. Migration help

Migration should be report-only unless ShardLoom can execute the path natively.

```python
report = sl.migration.analyze_spark_sql("""
    SELECT customer_id, count(*)
    FROM orders
    WHERE status = 'active'
    GROUP BY customer_id
""")

report.supported_constructs
report.unsupported_constructs
report.rewrite_suggestions
report.semantic_differences
```

The report should classify constructs as:

- supported_native
- supported_with_rewrite
- supported_with_materialization
- requires_adapter
- requires_future_phase
- unsupported
- unsafe_rejected

External engines may compare semantics and benchmark baselines. They must not
execute unsupported ShardLoom workloads.

### 23. Benchmarks

Benchmarks are user-facing CG-21 capability, but evidence belongs to CG-6.

```python
bench = sl.benchmark.compare(
    workload=pipeline,
    baselines=["polars", "datafusion", "spark"],
    inputs=["orders.vortex", "orders.parquet"],
)

bench.run()
bench.report()
```

Rules:

- Spark/DataFusion/Polars are optional benchmark extras.
- They are never runtime dependencies of core ShardLoom.
- They are never fallback engines.
- Benchmark rows must include correctness, materialization, and fallback
  evidence.

### 24. Security, governance, credentials, and audit

CG-21 includes:

- credential boundary reporting
- secret redaction
- audit events
- external read/write permission
- destructive operation permission
- PII redaction policy
- data retention policy
- raw value redaction in diagnostics
- safe agent-facing API behavior

Example:

```python
ctx = sl.context(
    credentials="env",
    redaction_policy="strict",
    external_effects="deny_by_default",
)
```

Credentials, permissions, redaction, audit, external effects, and agent safety
are certification requirements for governed workloads.

### 25. Notebooks

Notebook ergonomics are a real lane.

Useful features:

- display schema
- display plan tree
- display capability status
- display unsupported reasons
- display sample output with explicit materialization
- display certificate summary
- display benchmark table
- display materialization/fidelity warning

Example:

```python
pipeline.preview(limit=20)
pipeline.explain(display="tree")
pipeline.certify().summary()
```

Previews are explicit materializations:

- preview materialized 20 rows
- encoded representation lost at display boundary
- `fallback_attempted=false`

### 26. Deployment readiness

Deployment is part of CG-21 because global usability depends on it.

Reports should cover:

- conda CLI package status
- conda Python package status
- metapackage status
- fresh environment certification
- platform targets
- binary version
- Python wrapper version
- protocol version
- configuration surface
- resource limits
- benchmark extras status
- license/provenance
- security scan status
- known limits

## Adapter maturity visibility

Expose adapter maturity directly:

```python
sl.capabilities("adapters")
```

Conceptual output:

```text
vortex.source      A7 benchmarked_certified for local fixture workload
vortex.sink        A3 read? A5 write? A6 commit? depends on evidence
parquet.source     A2 schema_metadata_discovery
parquet.sink       A0 declared_only
csv.source         A3 read_support, materialization_required=true
s3.source          A1 capability_discovery
iceberg.table      A2 schema_metadata_discovery
postgres.source    A0 declared_only
```

Adapter maturity levels:

- A0 declared only
- A1 capability discovery
- A2 schema/metadata discovery
- A3 read support
- A4 pushdown support
- A5 write support
- A6 commit/recovery support
- A7 benchmarked/certified

Maturity levels represent evidence, not aspirations. Read support does not imply
pushdown. Write support does not imply commit/recovery. Benchmarked/certified is
workload-scoped.

## User capability matrix

| Capability family | User need | First useful target | Certification boundary |
| --- | --- | --- | --- |
| Install/import | Use ShardLoom in Python/Conda | `import shardloom`, `smoke_check()` | No import-time execution/probing |
| Capability discovery | Know what works | `capabilities()` | Planned is not supported |
| Local Vortex | Native highest-fidelity path | `read_vortex`, `write_vortex` | CG-19 + CG-16 |
| Local files | Common ETL ingestion/export | CSV/Parquet/JSON/Arrow | Explicit materialization/fidelity reports |
| DataFrame API | Python-first ETL | lazy query builder | Lowers to ShardLoom-native plans |
| SQL | Familiar query expression | SELECT/FROM/WHERE/PROJECT first | Parse/bind/execute stages separated |
| pandas interop | Fit Python ecosystem | `from_pandas`, `to_pandas` | Explicit materialization boundary |
| Arrow interop | Columnar exchange | Arrow IPC/table boundary | Representation loss reported |
| Data contracts | Validate inputs | required columns/types/nulls | Rejected/quarantine outputs |
| Transformations | Real ETL breadth | filter/select/cast/join/group/agg | Operator/function evidence |
| Writes | Produce outputs | write Vortex/Parquet/CSV | Sink requirements + commit status |
| Object storage | Real data lakes | S3/GCS/ADLS later | Range/read/request/credential evidence |
| Tables/catalogs | Lakehouse ETL | partitions/snapshots/schema | CG-9 + CG-19 |
| Incremental | Production pipelines | checkpoints/watermarks/idempotency | CG-4/8/9/10 |
| Observability | Trust execution | explain/estimate/profile/certify | CG-16 + CG-19 |
| Benchmarks | Compare engines | Spark/DataFusion/Polars baselines | CG-6, no fallback |
| Migration | Move workloads | compatibility/rewrite reports | Report-only unless native support exists |
| UDFs/extensions | Custom logic | typed/effect-declared UDFs | Materialization/effect boundaries |
| Unstructured/media | Documents/logs/media | typed refs/manifests/chunks | No silent OCR/LLM/media decode |
| Governance | Enterprise safety | credentials/redaction/audit | Required for governed workloads |
| Deployment | Global usability | Conda package + fresh env cert | CG-21 deployment evidence |

## Suggested CG-21 lanes

### CG-21A: install, import, and runtime discovery

Scope:

- conda install
- import shardloom
- binary resolution
- version/protocol report
- smoke_check()
- capability discovery

Acceptance:

- Import has no filesystem, network, catalog, adapter, SQL, or benchmark side
  effects.
- Missing binary gives deterministic diagnostics.
- Python wrapper and CLI protocol versions are reported.
- `fallback_attempted=false` is visible.

### CG-21B: context and capability API

Scope:

```python
ctx = sl.context()
ctx.capabilities()
ctx.adapters()
ctx.functions()
ctx.operators()
ctx.sql_support()
```

Acceptance:

- All statuses are machine-readable.
- Planned features are not reported as supported.
- Unsupported constructs include reasons and rewrite suggestions.
- Capability snapshots are side-effect-free.

### CG-21C: source/sink registry

Scope:

```python
ctx.sources()
ctx.sinks()
ctx.adapter("parquet").capabilities()
```

Acceptance:

- Every adapter has A0-A7 maturity.
- Source and sink maturity are separate.
- Read, pushdown, write, and commit status are separate.
- Native I/O certificate requirements are visible.

### CG-21D: Python DataFrame/query-builder

Scope:

```python
sl.read_vortex(...).filter(...).select(...).write_vortex(...)
```

Acceptance:

- Lazy plan builder.
- No Python-side fallback.
- All actions go through ShardLoom-native capability checks.
- Unsupported operations fail/report deterministically.
- Materialization boundaries are explicit.

### CG-21E: SQL frontend

Scope:

```python
ctx.sql("SELECT ...")
```

Acceptance:

- Parse, bind, plan, execute, and certify are separate statuses.
- SQL support is semantic-profile-aware.
- Unsupported SQL reports exact blockers.
- No SQL execution delegation.

### CG-21F: pandas/Arrow/NumPy interop

Scope:

```python
sl.from_pandas(pdf)
result.to_pandas()
sl.from_arrow(table)
result.to_arrow()
```

Acceptance:

- Every conversion emits a materialization/fidelity report.
- These are source/sink boundaries, not fallback execution paths.
- Diagnostics preserve no-fallback status.

### CG-21G: data contracts and data quality

Scope:

```python
df.require_not_null(...)
df.require_unique(...)
df.validate(...)
df.quarantine(...)
```

Acceptance:

- Invalid rows can be counted, rejected, or quarantined.
- Constraint violations are machine-readable.
- Output quality reports are attached to result/certificate.

### CG-21H: local structured adapters

Scope:

- Vortex
- Parquet
- CSV
- JSON/NDJSON
- Arrow IPC
- partitioned directories

Acceptance:

- Each adapter has independent read/write/pushdown/commit maturity.
- Compatibility import paths are labeled as such.
- Vortex remains the highest-fidelity target.

### CG-21I: output and commit UX

Scope:

```python
pipeline.write_vortex(...)
pipeline.write_parquet(...)
pipeline.write_partitioned(...)
```

Acceptance:

- Sink requirements are visible before execution.
- Commit/recovery/idempotency are explicit.
- Partial writes and cleanup are reported.
- Unsafe writes require explicit confirmation/policy.

### CG-21J: object-store and remote data UX

Scope:

- S3
- GCS
- Azure Blob / ADLS
- HTTP range read

Acceptance:

- Credentials and network effects are explicit.
- Range-read/coalescing/prefetch/request-budget evidence is visible.
- Object-store paths do not imply distributed execution support.

### CG-21K: table/catalog/lakehouse UX

Scope:

- Hive partitions
- Iceberg metadata
- Delta metadata
- snapshots
- schema evolution
- deletes/tombstones

Acceptance:

- Metadata discovery is separate from read support.
- Table writes are separate from commit/recovery support.
- Delete/update/merge semantics are explicitly certified or blocked.

### CG-21L: observability UX

Scope:

```python
pipeline.explain()
pipeline.estimate()
pipeline.profile()
pipeline.certify()
```

Acceptance:

- Reports show planned vs executed work.
- Reports show work avoided, decode/materialization, memory/spill, bytes/rows,
  and object-store requests.
- Certificates are visible from Python, CLI, and JSON.

### CG-21M: benchmark and migration UX

Scope:

```python
sl.benchmark.compare(...)
sl.migration.analyze_spark_sql(...)
```

Acceptance:

- External engines are optional baselines only.
- Benchmark rows include correctness and no-fallback evidence.
- Migration reports give rewrite suggestions, not fallback execution.

### CG-21N: notebook UX

Scope:

- rich display of schema, plans, capabilities, diagnostics, certificates, and
  benchmark rows

Acceptance:

- Previews are explicit materializations.
- Sensitive values are redacted by default.
- Notebook display never hides unsupported behavior.

### CG-21O: UDF and extension UX

Scope:

- Rust UDFs
- WASM UDFs later
- Python UDF boundaries
- external service/model call boundaries
- adapter plugins

Acceptance:

- Type/null/determinism/effect metadata required.
- Effectful extensions do not execute during discovery/explain/dry-run.
- Python/external UDFs are explicit materialization/effect boundaries unless
  later certified native.

### CG-21P: unstructured/media UX

Scope:

- documents
- logs
- HTML/XML
- PDF references
- office document references
- images/audio/video references
- chunk manifests
- extracted text
- embedding references

Acceptance:

- Raw media is not silently decoded.
- Extraction/OCR/LLM/embedding/model calls are explicit effects.
- Provenance, confidence, redaction, and materialization costs are reported.

### CG-21Q: security/governance UX

Scope:

- credentials
- redaction
- audit
- external-effect permissions
- destructive-operation permissions
- data retention
- agent-facing safe APIs

Acceptance:

- Secrets and sensitive values are redacted.
- External reads/writes/model calls require explicit permission.
- Governance gaps block certification for governed workloads.

### CG-21R: workload scorecards

Scope:

```python
scorecard = pipeline.scorecard(workload="local_file_etl")
```

Acceptance:

- Scorecard shows correctness, performance, cost, SQL coverage, function
  coverage, adapter coverage, Python usability, observability, migration,
  deployment, governance, extension safety, and no-fallback integrity.
- Scorecard can publish `not_certified` or `partial` without pretending the
  workload is fully supported.

## Minimum lovable CG-21 ETL MVP

Practical sequencing should start with:

```text
CG-21 MVP: Python local structured ETL
```

User promise:

```text
A user can conda-install/import ShardLoom, read local Vortex/CSV/Parquet-ish
structured inputs where supported, build a lazy Python DataFrame-style ETL
plan, inspect schema/capabilities, run supported native paths, write local
Vortex output, and receive certificates/materialization/no-fallback diagnostics.
```

MVP features:

- conda/package/import smoke
- context/capabilities/adapters API
- read_vortex
- write_vortex
- compatibility read_csv/read_parquet as explicitly labeled paths
- lazy select/filter/project/cast/rename
- simple group/count/sum if engine evidence is ready
- explain/estimate/profile/certify
- to_pandas/from_pandas with materialization reports
- deterministic unsupported diagnostics
- optional benchmark extras, not core dependencies

MVP non-goals:

- full SQL
- object-store production certification
- lakehouse commit semantics
- remote database/warehouse adapters
- Python UDF execution
- unstructured/media decoding
- hidden pandas/Polars/DataFusion/Spark execution
- best-default or superiority claims

## Unsupported diagnostics

Unsupported work should produce actionable diagnostics, not generic "not
implemented" messages.

Example:

```json
{
  "status": "unsupported",
  "operation": "join",
  "reason": "hash_join_kernel_not_certified",
  "required_gate": "CG-20 operator coverage + CG-7 physical kernel + CG-5 correctness",
  "rewrite_suggestions": [
    "pre-filter inputs before join",
    "materialize smaller side explicitly",
    "use migration analyzer for Spark workload"
  ],
  "fallback_attempted": false,
  "materialization_required": false,
  "external_engine_invoked": false
}
```

## Certification rule

A feature counts as user-supported only when the user-facing API, native
execution or explicit materialization path, source/sink evidence, correctness
evidence, diagnostics, and no-fallback fields are all present for the declared
workload.

## Design warning

The biggest risk is not missing a few adapters. The biggest risk is letting:

```python
import shardloom as sl
sl.read_parquet(...).filter(...).write_parquet(...)
```

silently become:

```text
pandas/Polars/DataFusion did the real execution,
ShardLoom just wrapped it.
```

The stronger version is:

```text
ShardLoom gives users familiar Python, DataFrame, SQL, pandas, Arrow, and
adapter surfaces, but every surface lowers into ShardLoom-native plans or
explicit materialization/source/sink boundaries.

When unsupported, ShardLoom explains why.
When executed, ShardLoom certifies what happened.
When compared, external engines remain baselines only.
```

## Disqualifiers

The following block CG-21 certification for a declared workload:

- hidden fallback
- delegated execution
- external engine runtime dependencies
- planned-only features presented as supported
- missing native I/O certificates for required source/sink paths
- missing execution certificates for required execution paths
- missing correctness evidence
- missing benchmark evidence for performance claims
- unreported materialization or fidelity loss
- unsupported constructs without deterministic diagnostics
- missing `fallback_attempted=false`

## Recommendation

Keep CG-21 user-scenario-driven.

Do not define it as "add adapters", "add SQL", "add pandas", or "add a
DataFrame API". Define it as complete, inspectable, certified data workflows:

```text
read -> validate -> transform -> write -> explain -> certify -> benchmark -> diagnose
```

This turns ShardLoom from a well-defined engine into a globally usable data
tool without weakening its Vortex-native, no-fallback architecture.
