# ShardLoom Python CLI Client

This package is the first thin Python surface for ShardLoom, a Vortex-native,
no-fallback, evidence-certified local compute engine. It invokes the workspace
`shardloom` CLI with `--format json`, parses the stable `OutputEnvelope`, and
preserves typed result/artifact/certificate payloads, diagnostics, fallback
status, and the temporary legacy field mirror.

It is intentionally not a native binding, broad DataFrame API, broad SQL runtime, UDF runtime, or
fallback execution path. Importing the package has no ShardLoom side effects. Work happens only when
a caller explicitly invokes a CLI command through `ShardLoomClient` or one of the scoped Python
helpers that wraps an evidence-backed CLI smoke.

Public status is owned by `docs/release/public-status-matrix.md`. This README may describe scoped
local Python surfaces, the current source version, and the approved package track, but it does not
authorize production support, performance claims, Spark displacement, or hidden external execution.

## Local Use

From the repository root:

```powershell
$env:PYTHONPATH = "python\src"
python -c "from shardloom import ShardLoomClient; print(ShardLoomClient.from_repo().status().status)"
```

Or install the source-tree package in editable mode for notebook, job, or
Foundry-style imports:

```powershell
python -m pip install -e python
```

The current published local-engine release is 0.4.0, with verified GitHub, TestPyPI, PyPI and
Homebrew access. The source package reports its version through `shardloom.__version__`;
development checkouts should use editable installs so Python and CLI revisions remain aligned.
Operational hardening is in progress. The technical-preview designation refers to the remaining local workload,
resource and failure acceptance in the
[exit criteria](../docs/release/production-certification-gate.md#local-engine-preview-exit-criteria).
Package access does not imply production readiness, broad runtime support, or performance claims.

```sh
python -m pip install shardloom
```

Published supported-platform wheels resolve the packaged CLI before falling back to `PATH`.
Explicit binary/env/source configuration still wins. Use `SHARDLOOM_BIN` only when you want to pin a
specific CLI binary or when installing from a source distribution without a bundled platform CLI:

```powershell
$env:SHARDLOOM_BIN = "target\release\shardloom.exe"
```

Or pass an explicit binary:

```python
from shardloom import ShardLoomClient

client = ShardLoomClient(binary="target/release/shardloom")
print(client.status().status)
```

`ShardLoomClient.from_repo()` looks for `target/release/shardloom` and then
`target/debug/shardloom` when a command is invoked. It does not run commands or
probe the repository at import time.

`ShardLoomClient.from_env()` is the import-friendly constructor for managed
Python environments. It reads configuration only and does not run commands:

```python
from shardloom import ShardLoomClient

client = ShardLoomClient.from_env()
smoke = client.smoke_check()
print(smoke.commands)
print(smoke.deployment_capabilities.field("surface_components"))
print(smoke.fallback_attempted)
```

Supported environment variables:

- `SHARDLOOM_BIN`: explicit `shardloom` CLI binary path.
- `SHARDLOOM_REPO_ROOT`: source checkout containing `target/<profile>/shardloom`.
- `SHARDLOOM_PROFILE_ORDER`: comma-separated target profile order, for example `release,debug`.
- `SHARDLOOM_TIMEOUT_SECONDS`: per-command subprocess timeout.

If no CLI binary is available, explicit client commands raise
`ShardLoomBinaryNotFoundError` with installation/configuration guidance instead
of leaking a raw subprocess error. The exception carries deterministic
no-fallback diagnostics plus a `shardloom.output.v2`-shaped error payload via
`to_error_payload(command)` for agents and wrappers that need protocol-shaped
missing-binary evidence. Importing the package and constructing
`ShardLoomClient.from_env()` remain side-effect-free.

For the CG-21 user workflow surface, use `shardloom.context()` when you want a
short import-friendly entry point for smoke checks and capability discovery:

```python
import shardloom as sl

ctx = sl.context()
smoke = ctx.smoke_check()
capabilities = ctx.capabilities()

print(smoke.python_package_version)
print(smoke.resolved_cli_path)
print(smoke.protocol_version)
print(smoke.fallback_attempted)
print(capabilities.python.field("scope"))
print(capabilities.sql_support.capability_state)
print(capabilities.fallback_attempted)
```

Constructing the context does not run ShardLoom, inspect datasets, probe object
stores, touch catalogs, execute SQL, or invoke external engines. The explicit
`smoke_check()` and `capabilities()` methods run only no-dataset CLI JSON
commands and preserve no-fallback status.

Capability views also expose a normalized posture object so Python callers can
inspect support, claim, runtime, effect, and policy state without scraping raw
CLI text:

```python
posture = capabilities.sql_support.posture

print(posture.support_status)
print(posture.claim_gate_status)
print(posture.report_only, posture.unsupported, posture.claim_grade)
print(posture.runtime_execution)
print(posture.data_read, posture.write_io, posture.object_store_io)
print(posture.fallback_attempted, posture.external_engine_invoked)
print(posture.required_evidence)
```

The posture view does not widen runtime support. It is a typed convenience
surface over existing `OutputEnvelope` fields and diagnostics. Unsupported or
report-only scopes remain unsupported or report-only, and
`fallback_attempted=false` / `external_engine_invoked=false` stay visible.

Use `ctx.user_surface_graduation_matrix()` to decide whether a Python or CLI
surface belongs on the ergonomic context path. The matrix uses five postures:
`high_level_context`, `client_only`, `diagnostic_only`, `feature_gated`, and
`not_user_facing`. `high_level_context` rows are the scoped workflows promoted
for normal context use; `client_only` rows stay explicit lower-level CLI/client
access; `diagnostic_only` and `feature_gated` rows must not be described as
runtime support without the matching evidence.

For normal Python use, start from the simple context and query surface. `repo_root` and
`profile_order` are optional development configuration overrides, not arguments users should have
to put in ordinary application code. Source-tree or CI runs can set `SHARDLOOM_BIN` or
`SHARDLOOM_REPO_ROOT` in the environment when the CLI is not on `PATH`.

`ctx.read(path)` is the normal public read wrapper. It infers `.csv`, `.json`, `.jsonl`, `.ndjson`,
`.parquet`, `.arrow`, `.ipc`, `.feather`, `.avro`, `.orc`, and `.vortex` local source adapters from
the path extension. Explicit helpers such as `read_csv(...)`, `read_json(...)`,
`read_parquet(...)`, `read_arrow_ipc(...)`, `read_avro(...)`, and `read_orc(...)` remain available
for compatibility, tests, and schema-pinned examples. The context returns a lazy query that uses
the shared native workflow for admitted work. Collection returns the complete typed result; a
write executes the same query into the declared sink:

```python
import shardloom as sl

ctx = sl.context()
result = (
    ctx.read("target/orders.csv")
    .filter(sl.col("amount") >= 10)
    .select("id", "amount")
    .limit(100)
    .collect()
)

print(result.status)
print(result.result_rows)
print(result.fallback_attempted, result.external_engine_invoked)
```

`VortexWorkflowExecutionReport` exposes the result envelope and, for writes, fields such as
`output_path`, `rows_written`, `output_commit_status`, and
`native_io_certificate_status`. Use `status`, `fallback_attempted`,
`external_engine_invoked`, and `claim_gate_status` for execution posture. Exact fields depend on the
operation and sink; report-only capability views are not runtime evidence.

The same query shape can read admitted local formats through `ctx.read(...)` or the explicit
format helpers. CSV, flat JSON/JSONL/NDJSON, generated rows, and scoped local Vortex inputs are
the default public examples. Parquet, Arrow IPC/Feather, Avro, and ORC are admitted scoped
local-format surfaces when the matching feature-gated build is present; builds without those
readers return deterministic adapter blockers instead of invoking another engine. Compatibility
exports such as `write_json(...)`, `write_jsonl(...)`, `write_csv(...)`, feature-gated
Parquet/Arrow IPC/Avro/ORC writers, `write_vortex(...)`, and fanout use the same public workflow.
Admission depends on the selected input, operation, output format, and enabled build features;
unsupported combinations return a deterministic report.
Format-specific behavior belongs at read/ingest and write/sink boundaries only; compute semantics
should lower through the shared ShardLoom/Vortex runtime or return a deterministic unsupported
report.
Agents and automation should use `docs/reference/shardloom-user-surface-index.md` and
`docs/reference/shardloom-user-surface-index.json` as the canonical map of Python, SQL, CLI,
generated-source, materialization, and deterministic blocker surfaces.
The canonical local output/sink scope is `docs/architecture/v1-local-output-sink-scope.md`; inspect
it with `ctx.local_output_sink_scope_report()` before treating a write helper as broader than its
scoped local evidence.

SQL and DataFrame collection, `run()`, `route()` and local writers accept the same
`memory_gb` and `max_parallelism` request. Writer aliases preserve these settings.
Flat aggregate chains retain filters, grouping, measures, HAVING, ordering and
limits through the same native admission for collection and all eight writers.
Declared compatibility schemas are preserved, including files whose names do not
identify their format. SQL `NULLS FIRST`/`NULLS LAST` and DataFrame
`sort(..., nulls="first")` or `nulls="last"` place nulls independently of ASC/DESC.
Small aggregate collection returns every row within 65,536 rows and 8 MiB of
JSONL, including escaping; larger complete results use the existing writers.
For admitted ordering, `spill` explicitly permits temporary native Vortex runs in
an existing absolute local directory:

```python
spill = {"workspace": "/tmp/shardloom-query-work", "quota_bytes": 64 << 20,
         "buffer_bytes": 2 << 20}
ordered = ctx.read_vortex("shipments.vortex").limit(250_000).sort(
    "priority", "label", nulls="last",
)
ordered.write_parquet("ordered.parquet", memory_gb=1, max_parallelism=2, spill=spill)
```

Create the workspace before execution. `route()` validates the declaration without
probing or creating it. Composed relational order uses `buffer_bytes` as a retained
input flush threshold within one query memory grant; specialized numeric sort and
aggregate providers use their existing operator-memory admission. Native sort spill
also applies to the flat typed keys scoped by the [typed key contract](../docs/architecture/native-typed-keys-2026-10-03.md).
Supported keys,
minimum buffers and spill families remain provider-specific. A spill request does
not enable other relational state spill or fanout, relax collection limits, or
establish an RSS bound. Successful writes require verified spill cleanup before
publication. See `docs/reference/native-query-spill.md` for exact contracts.

Bounded materialization is explicit. Local-source workflows can carry a `limit(...)` or pass
`collect(limit=...)`; SQL workflows can also pass `collect(limit=...)` or chain
`.limit(...).collect()`. Those admitted routes return typed report rows from the ShardLoom CLI
envelope. Decoded Python-object, pandas, Arrow, NumPy, and notebook materialization helpers are
bounded container/output boundaries over the admitted ShardLoom result; optional packages are never
used as execution engines and missing packages return deterministic diagnostics:

```python
preview_report = (
    ctx.read("target/orders.csv")
    .select("id", "amount")
    .limit(20)
    .collect()
)
print(preview_report.result_rows)

rows = ctx.read("target/orders.csv").select("id").limit(20).to_python_objects()
print(rows)

pandas_view = ctx.read("target/orders.csv").select("id").limit(20).to_pandas(check=True)
print(pandas_view)
```

For workflows that need caller-scoped reuse evidence, `ctx.session(...)` and `sl.session(...)` expose
the same local read/SQL shapes as session-bound workflows:

```python
with ctx.session(session_id="orders-run") as sess:
    result = (
        sess.read_csv("target/orders.csv")
        .select("id", "amount")
        .limit(100)
        .collect()
    )
    repeat = sess.sql("SELECT id FROM 'target/orders.csv' LIMIT 100").collect()
    print(result.reuse_hit, repeat.source_state_reuse_hit)
```

The session is explicit, caller-owned, and closeable. Its client owns a native worker that can
reuse admitted source and prepared query state. Every collection executes the query again;
query answers are not cached. Collection, writes and fanout use the same native workflow.
Explicit `ctx.prepare_vortex(...)` calls also track source and prepared-artifact fingerprints
for reuse. Session lifetime and reuse evidence do not establish a performance claim.

Supply `memory_gb` and `max_parallelism` on each operation that needs an explicit
allocation. Session collection, counts, all `write_*` methods and `fanout` forward
those values to the same native runtime as standalone workflows. Session reuse
requires the resource request to match, so changing the allocation cannot reuse
an earlier operation's result report. Positive environment defaults
`SHARDLOOM_MEMORY_GB` and `SHARDLOOM_MAX_PARALLELISM` are read at Python import;
an explicit CPU value of `1` stays `1`. The built-in defaults remain 4 GiB and 2.

The runtime selects CPU concurrency within the supplied maximum and the CPU
capacity available to the process. Ingestion shares that grant among ready source,
conversion and writer work, with memory-admitted task windows. This works for
arbitrary positive allocations; P4/P6/P8 are examples. I/O, serial readers and
memory constraints can limit useful concurrency. The accounted memory budget
does not include every upstream allocation or establish a process RSS limit.

For the CLI-visible session lifecycle proof, `ShardLoomClient.session_cache_smoke()` runs
`session-cache-smoke --format json` and returns a typed `SessionCacheSmokeReport`. That smoke
exercises scoped SourceState, `VortexPreparedState`, OutputPlan, schema-cache, dictionary-cache,
fingerprint invalidation, scratch-buffer reuse accounting, optimizer-trace linkage, explicit close,
and cleanup evidence. It is local and claim-gated; persistent cross-process cache,
object-store/table reuse, and non-local workflow reuse remain outside this scoped session surface.

The explicit prepare-once Vortex lifecycle is available for advanced validation through a
feature-gated CLI/Python surface. Build the CLI with `--features vortex-write`, then call
`ctx.read_csv(...).prepare_vortex(workspace=...)` for a prepared source,
`ctx.read_csv(...).prepare_vortex(workspace=...).query(...).collect()` for the public
Prepare-Once First Query route,
`ctx.from_rows(...).prepare_vortex(workspace=...)`,
`ShardLoomClient.vortex_prepare(...)`, or `ctx.prepare_vortex(...)` when you intentionally need
to inspect the `UniversalIngress -> SourceState -> vortex_ingest -> VortexPreparedState` boundary:

```powershell
@"
id,label,amount
1,alpha,8
2,beta,15
"@ | Set-Content -Encoding utf8 target\vortex-ingest-source.csv

cargo run -q -p shardloom-cli --features vortex-write -- `
  vortex-prepare target\vortex-ingest-source.csv target\vortex-ingest-source.vortex `
  --allow-overwrite --format json

$env:PYTHONPATH = "python\src"
$env:SHARDLOOM_REPO_ROOT = "."
python -c "from shardloom import context; ctx=context(); r=ctx.read_csv('target/vortex-ingest-source.csv').prepare_vortex(workspace='target/shardloom-prepared', allow_overwrite=True); print(r.vortex_ingest_status, r.prepared_state_created, r.prepared_state_reuse_hit, r.prepared_state_reuse_reason, r.fallback_attempted, r.external_engine_invoked)"
```

Default CLI builds return a deterministic feature-gate blocker instead of writing an artifact. This
path is a local fixture smoke; it is not the primary user API, broad Vortex writer support,
object-store/table output support, production SQL/DataFrame support, or a performance claim.
`LazyFrame.prepare_vortex(...)` is the higher-level local `auto` source front door: it derives
`<workspace>/<source-stem>.vortex` when a workspace is supplied, calls the real Rust
`vortex-prepare` route, and exposes `prepared_state_reuse_hit`,
`prepared_state_reuse_reason`, `prepared_state_reuse_manifest_digest`, and
`prepared_state_invalidation_reason` through typed properties. It prepares the raw local source
before query operators; use `.write_vortex(...)` when the desired artifact is a query-result sink.
Generated-source `prepare_vortex(...)` uses the existing generated-source Vortex writer and returns
a `GeneratedSourceWriteReport` with `prepared_state_created` and manifest-backed reuse fields.
Repeated compatible generated-source preparation reuses the caller-owned local `.vortex` artifact
through the artifact-adjacent manifest, reports `prepared_state_reuse_hit=true`, and skips the
writer/reopen path when schema, row payload, plan, policy, and artifact fingerprints still match.
The route capability report exposes both public prepared front doors as machine-readable rows:

```python
routes = ctx.user_route_capability_report()

for row in routes.public_front_door_route_rows:
    print(row.front_door_id, row.public_user_surface, row.prepared_state_reuse_scope)
```

Those rows are route guidance and release-readiness evidence. They do not run a benchmark or allow
performance, production, or Spark-replacement claims.
The benchmark publication bundle mirrors them as `public_front_door_benchmark_rows`, where they are
route-identity rows rather than timing rows. The website uses those rows to show each public Python
prepared front door beside its owning route lane, timing boundary, reuse manifest scope, and
no-fallback evidence.
When capillary preparation is admitted, the report exposes
`vortex_capillary_preparation_prewrite_status`,
`vortex_capillary_preparation_prewrite_scheduler_applied`, and pre-write gate fields for array
build, write, reopen, and sink evidence so Python callers can see whether PulseWeave-shaped work
windows affected the local route before artifact creation.

For one concrete request, use the public workflow facade. `route()` is side-effect-free: it does
not read the input, write outputs, run SQL, or invoke external engines. `run()` and `prepare(...)`
execute only admitted ShardLoom-native wrapper paths and attach the same route metadata to the
runtime or preparation envelope:

```python
sql_route = ctx.sql("SELECT id FROM 'target/orders.csv' LIMIT 10").route()
df_route = ctx.read("target/orders.csv").select("id").limit(10).route()
execution = ctx.read("target/orders.csv").select("id").limit(10).run()
prepared = ctx.read_csv("target/orders.csv").prepare("target/orders.vortex")
native_vortex = ctx.client.public_workflow_run(
    "cli",
    input_uri="shardloom-vortex/tests/fixtures/local_primitive_struct_five.vortex",
    input_format="vortex",
    requested_output="collect",
    execution_policy="native_vortex",
    materialization_policy="zero_decode",
    evidence_level="runtime_smoke",
    bounded=True,
    vortex_primitive="filter_project",
    vortex_predicate="gte:value:3",
    vortex_columns=("metric",),
    vortex_source_order_limit=2,
    memory_gb=1,
    max_parallelism=2,
)

print(sql_route.route_id, sql_route.resolved_internal_command)
print(df_route.route_id, df_route.resolved_internal_command)
print(sql_route.fallback_attempted, sql_route.external_engine_invoked)
print(sql_route.side_effect_free, sql_route.blocker_id)
print(execution.facade_command, execution.route_id, execution.runtime_execution)
print(prepared.facade_command, prepared.route_id, prepared.preparation_included)
print(native_vortex.command, native_vortex.route_id, native_vortex.vortex_primitive)
```

For direct `.vortex` inputs, `route()` and `run()` infer the admitted primitive/provider payloads
for scoped count/filter/project/limit, no-argument row-level distinct, bounded source-order tail,
deterministic row-count
`sample(n=..., seed=...|random_state=<int>, weights="<numeric-column>", replace=False|True)` or
fractional
`sample(frac|fraction=..., seed=...|random_state=<int>, weights="<numeric-column>", replace=False|True)`,
and exact benchmark-family grouped aggregate, hash join, global top-N, cast/try-cast,
substring contains, and native
`write_vortex` sink shapes. Manual
`vortex_primitive` and `native_vortex_provider_scenario` arguments remain available on
`ctx.client.public_workflow_*` for low-level diagnostics, but normal Python/SQL facades route the
admitted shapes without requiring those flags.

Unbounded collect requests block at route admission and keep
`runtime_execution=false`, `fallback_attempted=false`, and `external_engine_invoked=false` in the
envelope. The equivalent CLI surfaces are
`shardloom route <sql|python|dataframe|cli> --format json`,
`shardloom run <sql|python|dataframe|cli> --format json`, and
`shardloom prepare <sql|python|dataframe|cli> --format json`.
Lazy DataFrame bounded `collect()` and admitted generated-source/source-free writes route through
the public `run` facade and return existing typed reports with attached `public_workflow_*` route
fields. `write_vortex(...)` remains the highest-fidelity native sink when the upstream provider
route is admitted. Exact provider-backed result summaries can export bounded `result_json` to
workspace-safe JSONL/CSV. Ordinary SQL/generated local result sinks and primitive
filter/project/filter-project row streams also admit JSON arrays; scoped primitive
filter/project/filter-project/distinct/tail/sample row streams can export JSONL/CSV or JSONL+CSV
fanout through `native_vortex_primitive_row_export`.
JSON and JSONL text outputs do not preserve static type or Vortex layout metadata. Broader
compatibility writes such as arbitrary `write(...)`, structured write aliases, unsupported formats,
and unsafe or non-admitted fanout return deterministic blockers until a native Vortex sink/export
route exists for the normalized plan. Native Vortex primitive and promoted provider helpers attach
the inferred route payloads to the same facade rather than relying on a separate payload-only path.

Admitted native DISTINCT, `drop_duplicates`, `duplicated`, tail, sample, scalar rewrites,
melt, explode, rolling and pivot collections now include their complete rows. Read
`report.result_rows` or call `to_python_objects()`; accessing an existing report does
not execute the query again. Repeated identical collections reuse the opened source
and prepared operation while computing fresh results. Source replacement fails the
current call; a later explicit call can prepare the changed source.

```python
workflow = (
    ctx.read_vortex("cargo.vortex")
    .select(["cargo_id", "load_units"])
    .drop_duplicates(subset=["cargo_id"], keep="last")
)
report = workflow.collect()
print(report.result_rows)
workflow.write_vortex("deduplicated-cargo.vortex")
```

These collections admit at most 65,536 rows, 128 scalar fields and 8 MiB of JSONL.
Larger results use bounded batches through `write_vortex`, `write_parquet`,
`write_arrow_ipc`, `write_avro`, `write_orc`, `write_json`, `write_jsonl` or `write_csv`,
subject to each format's dtype contract and the operation's state budget. Native
Vortex and text output admit supported mixed scalar melt/pivot results; mixed Variant
columns are not general binary compatibility output support.

Current source builds compose flat-scalar DISTINCT, `drop_duplicates`, `duplicated`,
tail, sample, scalar rewrites, melt and rolling with admitted filters, projections,
ordering, aggregates, joins and sets. Each operation consumes the preceding stage's
rows and column names. SQL and DataFrame calls carry the same source declarations,
CPU/memory allocation and writer policy through one native execution. Melt can omit
ID columns; inferred value columns come from the preceding output. See the
[composition contract and acceptance](../docs/architecture/native-unary-composition-2026-10-02.md).
Static List/FixedSizeList/Struct payloads compose through admitted relational
stages and ordered/repeated explode. Nested payloads can be written as Vortex,
JSON, JSONL, Arrow IPC, Parquet or Avro when the dtype is representable; nested
CSV translates nested values to quoted JSON text cells without preserving their
logical dtype; ORC rejects nested output. Exploded flat output supports all eight writers.
Current source builds extend relational keys to static List, FixedSizeList and
Struct values for equality, hashing and ordering in the existing join, set,
group, sort, window and subquery kernels. COUNT, COUNT DISTINCT, MIN/MAX,
comparisons, NULL tests and selected CASE/COALESCE/NULLIF results use the same
native nested values. Key schemas must match recursively, including field names
and order, list kind and width, leaf widths, decimal precision/scale and
temporal identity; recursive nullability does not affect key compatibility.
Retained nested values are admitted for DISTINCT/duplicate selection and masks,
tail, sampling, parent-level forward fill (a NULL parent takes the prior
complete value; child NULLs do not trigger filling), lossless same-shape melt
and rolling COUNT. See the [nested key and retained-state contract](../docs/architecture/native-nested-keys-state-2026-10-04.md);
it is merged with complete local and hosted check evidence. General Variant/extension
operations, nested arithmetic/string operations, wider analytic-window behavior,
adapters and general state spill remain outside this
scope. Scalar pivot type/domain restrictions remain in force. See also the
[nested payload contract](../docs/architecture/native-nested-composition-2026-10-02.md).
Binary, Decimal128 (precision 1–38, scale 0–precision), Date32 and timezone-free
microsecond timestamps can travel as payloads, including nested leaves. Their
flat equality, hashing and ordering are admitted for relational joins, sets,
groups, windows and subqueries, with COUNT/COUNT DISTINCT/MIN/MAX and scoped
comparisons and expressions; Decimal key precision and scale must match.
Nested keys follow the subsequent contract above. Current source
builds admit typed literals, explicit CAST/TRY_CAST, exact decimal
arithmetic/rounding and scoped binary/calendar functions through the shared
native expression binder. Decimal arithmetic output metadata binds before
execution; explicit decimal downscaling requires zero discarded digits. Key
compatibility still requires matching decimal precision/scale and preserves
distinct temporal types. Wider analytic-window semantics, broader adapters
and state spill remain separate. See the [typed expression contract](../docs/architecture/native-typed-expressions-2026-10-03.md).
Flat typed values also retain exact logical types through duplicate selection and
masks, tail/sample, replacement/forward-fill, lossless melt, rolling COUNT and
scoped pivot first/first-unique/COUNT. Python bytes, Decimal, date and datetime
declare exact native literals. Decimal rewrites reuse checked native arithmetic;
primitive predicate and typed sampling-weight restrictions remain.
See the [typed unary contract](../docs/architecture/native-typed-unary-2026-10-03.md).
Computed aggregate arguments and exact decimal aggregate, rolling and scalar
pivot reductions now have complete local acceptance through the shared native
engine. SUM uses precision 38 at the input scale; AVG uses precision 38 at
`max(input_scale,6)` and rejects inexact division. Final overflow fails explicitly.
ARRAY/STRUCT constructors preserve admitted logical child types. The
[revised engine report](../docs/benchmarks/native-typed-reductions-full43-2026-10-05.md)
records 20,445 public checks, 202 direct checks and all 129 Full43 executions,
with no benchmark-specific executor or external fallback.
See the [typed key contract](../docs/architecture/native-typed-keys-2026-10-03.md).
Binary supports all eight writers; ORC rejects decimal and temporal payloads. JSON/JSONL and collection
encode binary as lowercase hex, decimals as `decimal128(precision,scale):unscaled_integer`,
and temporal values as signed integer units. CSV uses the existing JSON scalar
cell convention for binary/decimal strings. Text output does not retain native
logical types. See the [typed payload contract](../docs/architecture/native-typed-payloads-2026-10-03.md).

Current source builds also compose scalar `pivot` and `pivot_table` at their
declared position, including renamed/ordered input, downstream filters,
projections, aggregates, joins, sets, windows, melt and successive pivots. SQL
uses `PIVOT((SELECT ...), '{"index":"entity","columns":"category","values":"amount","aggregate":"sum"}')`.
Observed domains determine the columns during native execution; preparation and
inspection do not read rows to guess them. Empty input keeps its actual index-only
schema, with the declared margins column when requested. Referencing an absent
domain fails explicitly. Correlated inner pivots bind independently for each
outer row. All eight writers accept representable scalar results above the small
collection limit, subject to the existing 128-field and memory limits. Pivot
state has no spill path. See the [dynamic pivot contract and acceptance](../docs/architecture/native-dynamic-pivot-composition-2026-10-03.md).

For explicit preparation, use `LazyFrame.prepare_vortex(...)` or
`ctx.prepare_vortex(source_path, target_path, ...)` with their documented arguments. These are
preparation APIs; ordinary query execution uses the shared workflow below.

Engine intent is explicit. `engine="auto"` selects the current bounded snapshot
batch path when allowed; `live` selects the CG-22 in-memory fixture path for
bounded/unbounded change streams; `hybrid` selects the CG-22 declared Vortex-base
plus in-memory hot-delta fixture for snapshot/bounded base overlays:

```python
import shardloom as sl

ctx = sl.context(engine="live")
selection = ctx.engine_selection(
    boundedness="unbounded",
    update_mode="append-only",
    output_mode="changelog",
)
matrix = ctx.engine_capability_matrix()

print(ctx.engine)
print(selection.selection_status)
print(selection.selected_engine_mode)
print(selection.rejection_reasons)
print(matrix.engine_modes)
print(matrix.live_hybrid_claim_blocked_count)
print(matrix.live_hybrid_fabric_gate_rows)
print(matrix.live_hybrid_fabric_gate_claim_gate_status)
print(matrix.live_hybrid_fabric_gate_no_fallback_no_external_engine)
```

These calls do not execute workloads, probe brokers, write checkpoints, invoke
external engines, or attempt fallback. They expose the same CG-22 contract as
`shardloom engine-selection-plan`, `shardloom engine-capability-matrix`, and
`shardloom capabilities engines`.

`ctx.engine_capability_matrix()` also exposes the GAR-0034-A live/hybrid fabric
freshness gate. The gate keeps broker, state-store, object-store, catalog,
production freshness, and exactly-once claims blocked unless future
workload-scoped evidence promotes them, while preserving
`fallback_attempted=false`, `external_engine_invoked=false`, and
`claim_gate_status=not_claim_grade`.

The executable live surface is intentionally narrower: a deterministic
in-memory fixture for filter, project, count, count_where, and group_count. It
does not read brokers or files and does not write checkpoints, but it does emit
freshness, state, continuous-view, execution, and Native I/O certificate fields:

```python
contract = ctx.live_change_contract_plan()
fixture = ctx.live_fixture_run("group-count", "metric")

print(contract.change_record_fields)
print(contract.operations)
print(fixture.output_rows)
print(fixture.all_certified)
print(fixture.fallback_attempted)
```

Equivalent CLI commands:

```powershell
shardloom live-change-contract-plan --format json
shardloom live-fixture-run group-count metric --format json
```

The executable hybrid surface is also fixture-scoped. It merges declared local
Vortex base rows with deterministic hot deltas, applies tombstones/deletion
vectors in memory, and emits delta-overlay, hot/cold contribution,
micro-segment flush, layout-health, freshness, execution, and Native I/O
evidence without reading or writing data:

```python
hybrid = sl.context(engine="hybrid").hybrid_overlay_run("group-count", "metric")

print(hybrid.output_rows)
print(hybrid.layout_health_status)
print(hybrid.all_certified)
print(hybrid.write_io)
```

Equivalent CLI command:

```powershell
shardloom hybrid-overlay-run group-count metric --format json
```

The first CG-23 REST/API surface is contract-first. It checks the versioned
OpenAPI `/v1` contract and the discovery-mode `serve` contract without starting
a server, opening a listener, probing datasets, touching object stores, or
executing queries:

```python
api = ctx.rest_api_contract_plan()
discovery = ctx.serve_discovery_contract()
preview = ctx.rest_api_plan_preview("certified-local-batch")
lifecycle = ctx.rest_api_local_lifecycle("certified-local-batch")
events = ctx.rest_api_event_stream("certified-live-fixture")
security = ctx.rest_api_security_governance("safe-local-default")
data_plane = ctx.rest_api_data_plane("standards-matrix")

print(api.openapi_contract_path)
print(api.represented_resources)
print(api.discovery_endpoint_paths)
print(api.rest_runtime_unsupported_rows)
print(api.rest_runtime_unsupported_claim_gate_status)
print(api.rest_runtime_no_server_no_fallback_no_external_engine)
print(api.server_started)
print(discovery.server_mode)
print(discovery.contract_only)
print(preview.plan_handle)
print(preview.stage_statuses)
print(preview.problem_details_emitted)
print(lifecycle.lifecycle_status)
print(lifecycle.result_ref)
print(lifecycle.result_policies)
print(lifecycle.arrow_ipc_materialization)
print(lifecycle.fallback_attempted)
print(events.event_stream_status)
print(events.delivery_protocols)
print(events.event_types)
print(events.asyncapi_contract_path)
print(events.broker_io)
print(security.governance_status)
print(security.auth_postures)
print(security.api_scopes)
print(security.mcp_tools)
print(security.evidence_model_signals)
print(security.secrets_redacted)
print(data_plane.transfer_modes)
print(data_plane.preferred_large_payload_modes)
print(data_plane.standards_names)
print(data_plane.flight_adbc_required_for_basic_local_use)
```

Equivalent CLI commands:

```powershell
shardloom rest-api-contract-plan --format json
shardloom rest-api-plan-preview certified-local-batch --format json
shardloom rest-api-plan-preview unsupported-operator --format json
shardloom rest-api-local-lifecycle certified-local-batch --format json
shardloom rest-api-local-lifecycle blocked-uncertified --format json
shardloom rest-api-event-stream certified-live-fixture --format json
shardloom rest-api-event-stream broker-requested --format json
shardloom rest-api-security-governance safe-local-default --format json
shardloom rest-api-security-governance destructive-policy-required --format json
shardloom rest-api-security-governance agent-mcp-discovery --format json
shardloom rest-api-data-plane artifact-reference-default --format json
shardloom rest-api-data-plane flight-ticket-requested --format json
shardloom rest-api-data-plane adbc-endpoint-requested --format json
shardloom rest-api-data-plane standards-matrix --format json
shardloom serve --mode discovery --format json
```

The GAR-0035-A REST runtime unsupported gate keeps HTTP listener, remote execution, Flight/ADBC
transport, external broker integration, and dependency-expanded server claims blocked. The REST
contract remains a checked-in OpenAPI/reporting surface until separate workload, server lifecycle,
security, Native I/O, execution-certificate, and no-fallback evidence exists.

Lazy workflow planning is also available without adding pandas, Polars, Spark,
DataFusion, or any other execution dependency:

```python
import shardloom as sl

ctx = sl.context()
workflow = (
    ctx.read_vortex("orders.vortex")
    .filter("gte:value:3")
    .select("order_id", "amount")
    .limit(10)
)

plan = workflow.plan()
explain = workflow.explain()
estimate = workflow.estimate()
certification = workflow.certify()
unsupported = workflow.unsupported_report()

print(workflow.operation_summary)
print(plan.field("plan_only"))
print(explain.status)
print(estimate.status)
print(certification.fallback_attempted)
print(unsupported.fallback_attempted)
```

The same top-level helpers are exported as `sl.read_vortex`, `sl.read_csv`,
`sl.read_json`, `sl.read_parquet`, `sl.read_arrow_ipc`, `sl.read_avro`, and
`sl.read_orc`. Most helper chains
still declare sources and transformations only. `plan()`, `explain()`,
`estimate()`, `certify()`, and
`unsupported_report()` are explicit report calls over CLI JSON surfaces; they do
not read input files, infer schemas, materialize rows, probe object stores,
write output, or invoke fallback engines.

## Public Local Runtime: Universal Ingest Into A Vortex Middle

ShardLoom's Python front door is format-neutral at the execution boundary. `ctx.read(path)` and
the explicit `ctx.read_csv(...)`, `ctx.read_json(...)`, `ctx.read_parquet(...)`,
`ctx.read_arrow_ipc(...)`, `ctx.read_avro(...)`, `ctx.read_orc(...)`, and
`ctx.read_vortex(...)` helpers are input adapters. They do not create separate CSV, JSON,
Parquet, Arrow, Avro, ORC, SQL, or DataFrame execution stacks.

Local compatibility inputs enter the shared ShardLoom workflow, which converts them to the common
Vortex-native execution representation before native computation. Source format, SQL, and the
Python query builder do not create separate execution engines. `collect()` returns the complete
typed result for admitted work; `write(...)` runs that same workflow to the declared sink.

The invariant on admitted public local workflows is:

```text
input adapter -> shared Vortex-native execution -> typed result or declared sink
fallback_attempted=false
external_engine_invoked=false
```

Feature-gated structured adapters such as Parquet, Arrow IPC, Avro, and ORC still require the
matching build/runtime feature. When an adapter, operator, or sink is not enabled or not admitted,
the Python client returns the CLI unsupported envelope with a stable blocker id and next action.
That is intentional: unsupported work fails closed instead of using pandas, Polars, DuckDB, Spark,
DataFusion, or another engine.

A normal local Python use looks like this:

```python
import shardloom as sl

ctx = sl.context()
orders = ctx.read("target/orders.csv")

result = (
    orders
    .filter(sl.col("amount") >= 10)
    .select("id", "amount", "status")
    .limit(10)
    .collect()
)

print(result.status)
print(result.result_rows)
print(result.fallback_attempted, result.external_engine_invoked)
```

The equivalent scoped SQL front door uses the same lifecycle after source parsing:

```python
sql_result = ctx.sql(
    "SELECT id, amount, status FROM 'target/orders.csv' "
    "WHERE amount >= 10 LIMIT 10"
).collect()

print(sql_result.status, sql_result.result_rows)
print(sql_result.fallback_attempted, sql_result.external_engine_invoked)
```

Direct native Vortex input skips compatibility preparation and starts at the Vortex boundary:

```python
vortex_result = (
    ctx.read_vortex("target/orders.vortex")
    .filter(sl.col("amount") >= 10)
    .select("id", "amount", "status")
    .limit(10)
    .collect()
)

print(vortex_result.status, vortex_result.result_rows)
print(vortex_result.fallback_attempted, vortex_result.external_engine_invoked)
```

Output adapters accept the selected workflow result when the input, operations, sink, and build
features are admitted. Vortex remains the native persistence target; compatibility formats are
explicit output translations. Unsupported combinations return a deterministic report. For example:

```python
result = orders.write_jsonl("target/orders.jsonl", check=False)
print(result.output_path)
print(result.output_commit_status)
print(result.native_io_certificate_status)
print(result.fallback_attempted, result.external_engine_invoked)
```

Use `write_json(...)` when a single top-level JSON array is preferred:

```python
result = orders.write_json("target/orders.json", check=False)
print(result.output_path)
print(result.output_commit_status)
```

Evidence-aware optimizer traces are planned as `GAR-PERF-2B`, not current Python runtime support. A
future Python `explain()` trace should expose optimizer rule status, before/after plan digests,
rewrite safety, evidence preservation, no-fallback fields, and claim gates without implying broad
SQL/DataFrame execution or Polars/DataFusion optimizer parity.

Reusable I/O state and broad cross-format fanout are separate capability/evidence questions. The
public Python query surface uses one shared native workflow for local files, typed-memory inputs,
and source-free SQL. Input formats and output adapters meet at that workflow; no format selects an
external execution engine. Capability reports describe their own scope and do not establish runtime
support or performance evidence.

Unsupported workflow operations return explicit diagnostics. Wrapper-level operations such as
an undeclared row UDF expose a blocker and the evidence needed to admit it:

```python
import shardloom as sl

ctx = sl.context()
workflow = ctx.read_csv("events.csv").filter("amount > 0")
blocked = workflow.apply("row_udf", check=False)
print(blocked.blocker_id)
print(blocked.required_evidence)
print(blocked.suggested_next_action)
print(blocked.fallback_attempted, blocked.external_engine_invoked)

native_rejection = ctx.sql("SELECT CALL_API('https://example.invalid/score') AS score").collect(check=False)
print(native_rejection.status, native_rejection.diagnostics)
```

Unsupported reports above are generated through `workflow-unsupported-plan` and return
`status="unsupported"` with `fallback_attempted=false`; admitted reports preserve the same
no-fallback fields on their success evidence. The methods do not
use pandas, pyarrow, or numpy as execution engines, parse SQL, execute
unsupported DataFrame expressions, render broad notebook runtime output, invoke Foundry/model
services, or use another engine as fallback. Valid pandas/Arrow inputs are treated as explicit
materialized snapshots that lower to generated-source user rows, not as hidden external execution.

The DataFrame-style surface also has a typed method capability matrix. Use it
when a wrapper, notebook, or agent needs to know which familiar method names are
lazy declarations, which have scoped runtime-smoke support, which are unsupported diagnostics, and
which evidence gates bound each method:

```python
import shardloom as sl

ctx = sl.context()
matrix = ctx.capabilities().dataframe_method_matrix

print(matrix.row_order)
print(matrix.plan_only_methods)
print(matrix.unsupported_methods)
print(matrix.all_no_fallback_no_external_engine)

join = matrix.row("join")
print(join.support_status)
print(join.blocker_id)
print(join.required_evidence)
print(join.claim_boundary)
```

This matrix is still claim-safe, but its statuses should be read through the Vortex-middle contract.
Local compatibility rows are not successful public runtime routes merely because a lower-level smoke
command exists. Terminal methods are one of: side-effect-free lazy declarations, production-admitted
local workflows that normalize through Vortex preparation or native Vortex input, source-free
GeneratedSource local-output rows, internal smoke safeguards, or deterministic unsupported reports.

For local compatibility inputs, admitted `collect()`, `count()`, `preview()`, `head()`, `take()`,
schema/data-quality summaries, bounded decoded materialization, local compatibility writes,
compatibility fanout, quarantine sinks, and exact benchmark-family provider shapes enter the same
Vortex-prepared/native route described above. Profile summaries and broad production/export
semantics require an admitted route contract: native `.vortex` metadata profiles are admitted for
base read/select/limit shapes, and scoped `describe(...)` lowers to that same metadata-first
profile route. Transformed row profiling, pandas-style percentile/options summaries, and broad
production profiling remain blocked until a native Vortex materialization/profile contract admits
them. Alias rows such as
`project`, `where`, `groupby`, `order_by`,
`sort_values`, `merge`, `nlargest`, scoped `tail`, and scoped deterministic `sample` are useful only when their normalized operation shape maps to
an admitted native route; otherwise they return the matching unsupported report before data is read.

Generated-source rows such as `ctx.from_rows(...)`, `ctx.range(...)`, `ctx.sequence(...)`, and
source-free SQL have their own local output contracts. Those are not proof that local CSV/JSON/etc.
compatibility files can use direct decoded sinks as a product runtime.

`schema_contract(...)` and `validate_schema(...)` are bounded local schema evidence surfaces after
Vortex preparation. They are not broad schema registry, table constraint manager, or object-store/
lakehouse enforcement surfaces.
`profile(...)` is admitted for metadata-first native `.vortex` base read/select/limit profiles and
otherwise returns deterministic blockers until a route-specific profile/materialization contract
exists. It is not a hidden pandas/Polars profiler, resource tracer, performance claim, or
production observability surface.
`quarantine(...)` is admitted for bounded local checks and optional local sink replay evidence. It is
not object-store/table quarantine, production remediation, or a broad data-governance engine.

When the question is broader than one DataFrame method, use the front-door parity matrix. It
separates workflows that already lower SQL, Python, and DataFrame-style code to the same ShardLoom
runtime path from the gaps that still block arbitrary SQL/Python/DataFrame flexibility and
performance-equivalence claims. The scoped v1 boundary is owned by
`docs/architecture/v1-front-door-runtime-scope.md`:

```python
parity = ctx.front_door_parity_matrix()

print(parity.scoped_local_front_door_parity_supported)
print(parity.flexible_anything_claim_allowed)
print(parity.performance_equivalence_claim_allowed)
print(parity.row("local_file_filter_project_limit").shared_runtime_path)
print(parity.row("arbitrary_sql_python_dataframe_breadth").blocker_id)
```

Use the semantic surface matrix when the question is "which API/SQL semantic family is covered?"
instead of "which route is selected?" It is the agent-facing companion to the human parity doc:

```python
semantic = ctx.front_door_semantic_surface_matrix()

print(semantic.dataframe_subset_claim_statement)
print(semantic.sql_claim_statement)
print(semantic.pandas_compatible_claim_allowed)
print(semantic.ansi_sql_compliant_claim_allowed)
print(semantic.row("dataframe_materialization").claim_boundary)
```

Scoped local-file rows are admitted only when they normalize through Vortex preparation or start
from native Vortex input and then match an admitted primitive/provider route. Generated-output rows
remain separate source-free local-output contracts. Bounded schema/data-quality previews and decoded
Python/pandas/Arrow/NumPy materialization are explicit gap rows until native Vortex-derived evidence
and export/materialization contracts close them. General Vortex workflows, object-store/lakehouse/
table I/O, arbitrary SQL/Python/DataFrame breadth, and cross-front-door performance equivalence
remain explicit gap rows until correctness, Native I/O, execution-certificate, no-fallback, and
benchmark evidence closes them.

The v1 Vortex runtime scope is owned by `docs/architecture/v1-vortex-runtime-scope.md`. Use
`ctx.local_vortex_primitive_route_report()` for the feature-gated local Vortex primitive route
ids, CLI commands, materialization boundaries, and no-fallback evidence posture; broad object-store
Vortex, table/catalog Vortex, generalized Source/Sink, and broad Vortex SQL/DataFrame support remain
outside that scope.
The v1 SourceState/prepared-state scope is owned by
`docs/architecture/v1-source-prepared-state-scope.md`. Use
`ctx.source_prepared_state_scope_report()` to inspect the
`UniversalIngress -> SourceState -> vortex_ingest -> VortexPreparedState` route, the direct
transient boundary, reuse/invalidation case ids, golden fixture refs, and required benchmark
evidence fields. This report is local and claim-gated; it is not a global hidden cache, external
cache service, object-store/table prepared-state reuse, broad non-local preparation, or performance
claim.

Package, DataFrame, and notebook readiness are also exposed as a separate typed
matrix so local install smoke is not confused with public package publication or
broad runtime support:

```python
readiness = ctx.dataframe_notebook_package_readiness()

print(readiness.schema_version)
print(readiness.local_install_smoke_supported)
print(readiness.package_publication_ready)
print(readiness.dataframe_runtime_supported)
print(readiness.notebook_runtime_supported)
print(readiness.all_rows_no_fallback_no_external_engine)

publication = readiness.row("public_package_publication")
print(publication.support_status)
print(publication.blocker_id)
print(publication.required_evidence)
print(publication.claim_boundary)
```

This readiness matrix is report-only capability posture. It does not publish to
PyPI/TestPyPI/Conda/Homebrew, import notebook or DataFrame dependencies, render
rich notebook output, execute broad DataFrame plans, call package repositories,
or invoke external engines. Public package publication, broad DataFrame runtime,
and notebook runtime remain blocked until release and execution evidence gates
pass.

The CG-21 ETL workflow surface also has a compact typed matrix for current local
workflow posture. Use it when a wrapper, notebook, or agent needs one place to
show which user workflows are ready or smoke-supported, which APIs are
report-only, and which production/runtime claims remain blocked:

```python
matrix = ctx.etl_workflow_matrix()

print(matrix.schema_version)
print(matrix.supported_local_rows)
print(matrix.report_only_rows)
print(matrix.blocked_rows)
print(matrix.all_no_fallback_no_external_engine)

blocked = matrix.row("object_store_runtime")
print(blocked.status)
print(blocked.blocker_id)
print(blocked.claim_boundary)
```

This matrix is side-effect-free capability posture. It does not run production
ETL, SQL/DataFrame execution, object-store/lakehouse runtime, Foundry runtime,
external engine execution, or package publication, and it does not create
performance or Spark-displacement claims.

`GAR-0037-A` adds a wrapper/connector implementation registry on the API-surface
capability view. Use it when a client, adapter, agent, or public docs page needs
to distinguish the current source-tree Python wrapper from planned or blocked
ecosystem connectors:

```python
caps = ctx.capabilities()
registry = caps.wrapper_connector_registry
# Or: registry = ctx.wrapper_connector_registry()

print(registry.schema_version)
print(registry.ready_local_count)
print(registry.report_only_count)
print(registry.blocked_count)
print(registry.all_rows_no_fallback_no_external_engine)

python = registry.row("python_cli_json_client")
sqlalchemy = registry.row("sqlalchemy")

print(python.support_status)
print(python.explicit_execution_available)
print(sqlalchemy.support_status)
print(sqlalchemy.deterministic_diagnostic_code)
print(sqlalchemy.claim_boundary)
```

The registry is capability posture, not connector implementation. It does not
add generated clients, DB-API, SQLAlchemy, Ibis, dbt, Airflow, Dagster, Prefect,
MCP, Flight SQL, ADBC, JDBC/ODBC, BI, Grafana, Foundry package, REST server,
dependency expansion, network listener, external engine execution, or fallback.
Rows preserve `fallback_attempted=false`, `external_engine_invoked=false`, and
`claim_gate_status=not_claim_grade`.

Source-free generated-output APIs are tracked under `GAR-GEN-1`. The full
contract is exposed through capability views as `generated_source_contract`, and
the per-API admission matrix is exposed as `generated_source_api_admission`.
`GAR-NOVEL-1A` also exposes `generated_source_evidence_alignment`, which ties the same
GeneratedSourceCertificate rows to report-only OpenLineage, OpenTelemetry, Bayesian-confidence,
and Foundry generated-output boundary refs without enabling exporters or platform runtime.
Scoped local JSONL/CSV smoke paths are runtime-supported for caller-provided rows,
Python literal tables, Python calendar/date dimensions, ShardLoom-native
range/sequence generators, SQL `VALUES`, SQL literal `SELECT`, and SQL
`generate_series`/`range`. Broader SQL/DataFrame
forms remain report-only unless a later evidence-backed slice admits them:

```python
caps = ctx.capabilities()
contract = caps.python.generated_source_contract
admission = caps.python.generated_source_api_admission
alignment = caps.python.generated_source_evidence_alignment
lineage = ctx.observability().openlineage_facet_mapping
telemetry = ctx.observability().opentelemetry_trace_export_contract

print(contract.schema_version)
print(contract.case_order)
print(contract.no_dataset_smoke_separate_from_generated_output)
print(contract.all_no_fallback_no_external_engine)
print(admission.row("python_ctx_from_rows").support_status)
print(admission.row("python_ctx_range").runtime_execution)
print(admission.row("python_ctx_sequence").runtime_execution)
print(admission.row("python_ctx_literal_table").support_status)
print(admission.row("python_ctx_calendar").runtime_execution)
print(admission.row("sql_values").support_status)
print(admission.row("sql_literal_select").runtime_execution)
print(admission.row("sql_generate_series_range").runtime_execution)
print(admission.all_no_fallback_no_external_engine)
print(alignment.schema_version)
print(alignment.openlineage_export_enabled)
print(alignment.opentelemetry_network_exporter_enabled)
print(alignment.row("foundry_generated_output").foundry_boundary_ref)
print(lineage.schema_version)
print(lineage.row("generated_source").facet_name)
print(lineage.all_rows_report_only)
print(lineage.all_no_fallback_no_external_engine)
print(telemetry.schema_version)
print(telemetry.row("operator_compute").timing_fields)
print(telemetry.network_exporter_enabled)
print(telemetry.no_export_side_effects)
```

The universal compatibility view also projects the same source-free generated-output posture so
callers do not need to join GAR-GEN docs by hand:

```python
compatibility = ctx.compatibility_scoreboard()
generated = compatibility.source_free_generated_output_contract

print(generated.schema_version)
print(generated.no_dataset_smoke_separate)
print(generated.local_output_only)
print(generated.output_certificate_required)
print(generated.row("python_ctx_from_rows").support_status)
print(generated.row("sql_values").support_status)
print(generated.row("local_output_only_generated_source_posture").blocker_id)
print(generated.all_no_fallback_no_external_engine)
```

This compatibility contract is a capability map, not a runtime report. Its generated-output rows
describe declared posture only. Use the shared workflow execution report to inspect a real
`collect()` or write request; unsupported shapes return deterministic diagnostics.

Source-free constructors such as `ctx.from_rows(...)`, `ctx.literal_table(...)`, `ctx.range(...)`,
`ctx.sequence(...)`, and `ctx.sql_values(...)` return the same `LazyFrame` used by file-backed
queries. Use `.collect()` for the complete typed result or a `write_*()` method for an admitted
local sink. For example:

```python
import shardloom as sl

ctx = sl.context()
frame = ctx.from_rows([{"id": 1, "label": "alpha"}, {"id": 2, "label": "beta"}])
collected = frame.collect()
written = frame.write_jsonl("target/generated-reference.jsonl")

print(collected.status, collected.result_rows)
print(written.status, written.output_path, written.output_commit_status)
print(written.native_io_certificate_status)
print(written.fallback_attempted, written.external_engine_invoked)
```

Source-free SQL expressions, `VALUES`, and range constructors use the same query path. A generated
input does not imply support for every operation or sink: dtype, operation, adapter, and build
feature admission still apply. The `generated_source_*` capability views are declarative maps; they
do not constitute execution reports. Consult the live workflow report for `status`, and sink reports
for `output_commit_status` and `native_io_certificate_status`.

The client also exposes the P7 claim gate closeout report:

```python
from shardloom import ShardLoomClient

client = ShardLoomClient.from_repo()
closeout = client.claim_gate_closeout()

print(closeout.claim_gate_status)
print(closeout.release_readiness_status)
print(closeout.allowed_claims)
print(closeout.blocked_claims)
print(closeout.out_of_scope_claims)
print(closeout.no_runtime, closeout.no_fallback, closeout.no_effects)
```

This maps to `shardloom claim-gate-closeout --format json`. It is report-only:
it does not run workloads, publish packages, probe APIs, run benchmarks, invoke
Foundry, or permit external-engine fallback.

For P7.4 compute-engine closeout, the client exposes the report-only compute
capability matrix and operator-family ladder:

```python
from shardloom import ShardLoomClient

client = ShardLoomClient.from_repo()
matrix = client.compute_capability_matrix()

for row in matrix.rows:
    print(row.row_id, row.support_status, row.provider_kind, row.blocker_id)

for family in matrix.operator_families:
    print(family.family_id, family.support_status, family.next_evidence)

print(matrix.matrix_status)
print(matrix.claim_grade_status)
print(matrix.no_runtime, matrix.no_fallback, matrix.no_effects)
```

This maps to `shardloom compute-capability-matrix --format json`. It performs
no runtime execution, data reads, writes, benchmark execution, external effects,
external engine invocation, or fallback execution.

The first ShardLoomNative semantic conformance surface is executable, but only
over side-effect-free in-memory fixtures. It records passed, planned, and
blocked semantic dimensions before any broad SQL/DataFrame runtime claims:

```python
from shardloom import ShardLoomClient

client = ShardLoomClient.from_repo()
suite = client.semantic_conformance_suite()

print(suite.semantic_profile)
print(suite.suite_status)
print(suite.executed_fixture_count, suite.passed_fixture_count)

for row in suite.rows:
    print(row.row_id, row.fixture_status, row.blocker_id)
```

This maps to `shardloom semantic-conformance-suite --format json`. Current
fixtures cover the supported in-memory semantic dimensions and keep external
oracles, dataset reads, SQL parsing, runtime execution, writes, and fallback
disabled.

Artifact-rich top-level execution result envelopes can be inspected with
`ExecutionResultEnvelopeView` when a command returns a `shardloom.output.v2`
execution envelope:

```python
from shardloom import ExecutionResultEnvelopeView

def inspect_execution_envelope(envelope):
    result = ExecutionResultEnvelopeView(envelope)

    print(result.plan_id)
    print(result.provider_version)
    print(result.result_refs)
    print(result.artifact_refs)
    print(result.inline_artifact_ids)
    print(result.execution_certificate_refs)
    print(result.native_io_certificate_refs)
    print(result.representation_transitions)
    print(result.evidence_completeness_status)
    print([slot.kind for slot in result.incomplete_evidence_slots])
    print(result.fallback_attempted, result.external_engine_invoked)
```

The view is a typed reader over the CLI protocol. It does not execute unsupported
work, create benchmark rows, write outputs, invoke external engines, or convert
report-only surfaces into runtime support.

## Package Build Smoke

The current source package version is owned by `shardloom.__version__` and the shared workspace
version sources. It is a Python client surface over ShardLoom's CLI, with bundled CLI resources in
supported platform wheels.
Bundled platform-wheel readiness can be checked locally without publishing:

```powershell
python -m pip install build
python scripts/release_dry_run_proof.py --rows 64 --iterations 1
```

That proof builds the CLI, stages it under `shardloom/bin/<system-arch>/` in a temporary package
tree, builds a platform-specific wheel/sdist, installs the wheel in a clean environment, and asserts
that `ShardLoomClient().binary_command()` resolves the bundled CLI without `SHARDLOOM_BIN` or
`SHARDLOOM_REPO_ROOT`.

For a client-only wheel smoke without bundled CLI proof:

```powershell
python -m build python
python -m venv $env:TEMP\shardloom-wheel-smoke
$wheel = Get-ChildItem python\dist\shardloom-*.whl | Select-Object -First 1
& $env:TEMP\shardloom-wheel-smoke\Scripts\python -m pip install $wheel.FullName
& $env:TEMP\shardloom-wheel-smoke\Scripts\python -c "import shardloom; print(shardloom.__version__)"
```

Conda packaging should stay split so the pure Python wrapper can remain
`noarch: python` while the Rust CLI binary is built as a platform-specific
package. Local recipe scaffolds live under `packaging/conda/`:

- `shardloom-cli`: compiled Rust `shardloom` binary.
- `shardloom-python`: pure Python wrapper/import surface.
- Optional `shardloom` metapackage: depends on both the wrapper and CLI for a
  one-command install path.

The recipes are not published packages. A release pass must align versions,
replace local sources with tagged source archives and hashes, review license
metadata, build packages in clean Conda environments, and receive explicit
human approval before publication.

Spark, DataFusion, Polars, DuckDB, pandas, and Dask belong only in optional
benchmark environments; they are not ShardLoom runtime dependencies or fallback
engines.

## Local Analytics Benchmark

Use the parameterized benchmark harness at `benchmarks/traditional_analytics/run.py` for local
analytics comparisons. The example wrapper in `examples/local-vortex-benchmark/` forwards its
arguments to that harness and selects the public ShardLoom workflow. Supply a built CLI binary and
workspace explicitly; the harness owns fixture generation, resource limits, and output handling.
Benchmark results are workload- and host-specific evidence, not a general performance claim.

For application queries, use `ctx.read(...)` or a source-free constructor, then call `collect()` or
write to a declared output with the same `LazyFrame`. See `docs/getting-started/examples.md` for
runnable SQL and Python examples.

## Quickstart Proof

The quickstart proof script stitches the local user flow together: import and
CLI smoke, capability discovery, lazy source planning, unsupported
explain/estimate diagnostics, compatibility-source planning, workflow
readiness, and optional certified local Vortex primitive execution.

```powershell
$env:PYTHONPATH = "python\src"
python python\examples\quickstart_proof.py --repo-root .
```

To include the currently certified fixture execution path, build the CLI with
the local primitive feature and opt in explicitly:

```powershell
python scripts\write_ci_version_env.py --format powershell | Invoke-Expression
$env:RUSTUP_TOOLCHAIN = $env:SHARDLOOM_RUST_MSRV_TOOLCHAIN
cargo build -p shardloom-cli --features vortex-local-primitives --bin shardloom

$env:PYTHONPATH = "python\src"
python python\examples\quickstart_proof.py --repo-root . --run-local-vortex
```

The optional execution path runs only the checked-in
`local_primitive_struct_five.vortex` fixture through explicit local Vortex
primitive flags. The planning portions remain no-write/no-probe, and the script
exits nonzero if fallback is attempted, planning writes occur, or requested
local primitive evidence is not certified.

Universal I/O is broader than local compatibility files. The current adapter
registry also makes object-store, catalog, effectful, and unstructured queues
visible from Python:

```python
adapters = client.input_adapters()
print(adapters.field("common_structured_adapter_order"))
print(adapters.field("critical_structured_adapter_order"))
print(adapters.field("object_store_adapter_order"))
print(adapters.field("catalog_adapter_order"))
print(adapters.field("database_adapter_order"))
print(adapters.field("parquet_status"))
print(adapters.field("sqlite_status"))

plan = client.input_plan("file://tmp/example.parquet")
print(plan.field("source_kind"))
print(plan.field("capability_status"))
print(plan.field("plan_only"))
```

Common structured inputs are tracked as `native_vortex`, `parquet`,
`arrow_ipc`, `csv`, JSON/NDJSON through `jsonl`, `avro`, and `orc`.
Database adapters are visible separately: SQLite has a local import/export
fixture smoke, while Postgres/MySQL, JDBC/ODBC, Snowflake, BigQuery, and
Databricks SQL remain credential/network-gated. Lakehouse/table, object-store,
catalog, effectful, and unstructured/media families are also represented in the
registry. The current implemented live paths are scoped local fixture/evidence
paths only: feature-gated local compatibility-file-to-Vortex benchmark smokes,
native `.vortex` replay, public/local object-store fixture smokes, local table
commit rehearsal, local SQLite import/export smoke, and the built-in
deterministic scalar UDF fixture. Production adapter certification, live
object-store runtime, catalogs, broad SQL/DataFrame runtime, arbitrary UDFs, and
network connectors remain future work.

For a single source/sink compatibility view, use the typed scoreboard instead
of scraping architecture prose:

```python
matrix = ctx.compatibility_scoreboard()
print(matrix.schema_version)
print(matrix.row("vortex").support_status)
print(matrix.row("object_store_s3_gcs_adls").support_status)
print(matrix.all_rows_no_fallback_no_external_engine)

object_store = matrix.object_store_admission_ladder
print(object_store.schema_version)
print(object_store.provider_scope)
print(object_store.runtime_supported)
for row in object_store.rows:
    print(
        row.row_id,
        row.support_status,
        row.credential_policy_status,
        row.no_effects_no_fallback,
    )
```

The scoreboard maps local files, Vortex, generated outputs, Python rows,
SQL literals, databases, object stores, table/lakehouse formats, remote APIs,
and Foundry to `runtime-supported`, `smoke-supported`, `report-only`,
`blocked`, or `not-planned`. It is a capability map only, not a production,
performance, SQL/DataFrame, object-store/lakehouse, Foundry, or package claim.
The `object_store_admission_ladder` keeps S3/GCS/ADLS URI recognition,
credential policy, public reads, authenticated reads, byte-range reads,
full-file reads, local cache, write staging, and commit protocol as separate
gates. Current rows keep credential resolution, provider probes, network
probes, object-store I/O, writes, commits, external engines, and fallback
disabled.
Important row IDs include `object_store_uri_parse`, `credential_policy`,
`public_no_credential_read`, `authenticated_read`, `byte_range_read`,
`full_file_read`, `local_cache`, `write_staging`, and `commit_protocol`.

For the first explicit object-store read runtime proof, use the local-emulator
smoke. It reads a local fixture file through an object-store-style profile and
emits SourceState, byte-range/full-file read, Native I/O, and no-fallback
evidence.

```python
read = client.object_store_read_smoke(
    "target/object-store-fixture.bin",
    byte_range=(0, 16),
)
print(read.field("object_store_read_status"))
print(read.field("source_state_id"))
print(read.field_bool("network_probe_performed"))
print(read.field_bool("fallback_attempted"))
```

For the public no-credential fixture profile, pass a supported S3/GCS/ADLS URI
and an explicit local fixture file. ShardLoom parses the provider URI and reads
the fixture bytes only; it does not resolve credentials, probe the provider, or
open a network connection.

```python
public_read = client.object_store_read_smoke(
    "s3://shardloom-public-fixtures/orders.vortex",
    profile="public-no-credential-fixture",
    public_fixture_path="target/object-store-public-fixture.vortex",
    fixture_listing=True,
    byte_range=(0, 16),
)
print(public_read.field("object_store_uri_parse_status"))
print(public_read.field("native_io_certificate_status"))
print(public_read.field_bool("public_no_credential_fixture_claim_allowed"))
print(public_read.field_bool("network_probe_performed"))
```

For the first explicit object-store write runtime proof, use the separate
local-emulator write smoke. It stages a local source file into a local-emulator
target path, commits a sidecar manifest, emits idempotency and digest evidence,
and can immediately roll back the object plus manifest for cleanup proof.

```python
write = client.object_store_write_smoke(
    "target/source.bin",
    "target/object-store-fixture.bin",
    idempotency_key="orders-batch-001",
    rollback_after_commit=True,
)
print(write.field("object_store_write_status"))
print(write.field("commit_protocol_status"))
print(write.field("rollback_status"))
print(write.field_bool("object_store_write_io"))
print(write.field_bool("fallback_attempted"))
```

Object-store read/write smokes remain fixture-scoped. Live real S3/GCS/ADLS
network reads, credentials, provider probes, signed URLs, authenticated cloud
reads or writes, cache writes, table/lakehouse commits, catalog interaction,
distributed runtime, and production object-store claims remain blocked.

For the local SQLite adapter fixture, create or point at a local SQLite file and
use the import/export smoke. The command table-scans a named table, writes a
workspace-safe JSONL export, and creates a roundtrip SQLite artifact. It does not
accept arbitrary SQL, push queries down, connect to network databases, resolve
credentials, load extensions, or use SQLite as a fallback engine. `order_by` is
post-scan fixture ordering in ShardLoom, and BLOB schemas/values are rejected.

```python
sqlite = client.sqlite_local_import_export_smoke(
    "target/orders.sqlite",
    table="orders",
    export_jsonl="target/orders-sqlite.jsonl",
    roundtrip_db="target/orders-roundtrip.sqlite",
    order_by="id",
    allow_overwrite=True,
)
print(sqlite.field("sqlite_sql_execution_scope"))
print(sqlite.field_bool("sqlite_query_pushdown_allowed"))
print(sqlite.field("sqlite_ordering_execution_scope"))
print(sqlite.field_bool("roundtrip_replay_verified"))
```

For the built-in deterministic scalar UDF fixture, use the nullable-int64
fixture smoke. It proves UDF metadata, determinism, null propagation, overflow
blocking, and effect policy for one built-in fixture only. It is not Python,
WASM, Rust plugin, SQL-defined, table-function, or external-service UDF support.

```python
registry = client.udf_registry()
print(registry.field("typed_udf_registry_support_status"))
print(registry.field_int("typed_udf_registry_admitted_local_fixture_count"))
print(registry.field_bool("typed_udf_registry_arbitrary_runtime_bridge_available"))

udf = client.udf_local_scalar_fixture_smoke([1, None, 3])
print(udf.field("udf_id"))
print(udf.field("output_values"))
print(udf.field_bool("external_effect_executed"))
print(udf.field_bool("fallback_attempted"))
```

Extension metadata and UDF runtime posture remain inspectable without executing
extension code. A local extension manifest can be inspected as bounded metadata;
the CLI does not load extension code, resolve credentials, probe networks, or
enable plugin runtime support. The same helpers are available on
`ShardLoomContext` when you want one high-level workflow surface:

```python
extensions = client.extension_registry()
extension_dir = client.extension_registry(manifest_dir="target/extensions")
manifest = client.extension_inspect(manifest_path="target/extension.json")
typed_udfs = client.udf_registry()
fixture_plan = client.udf_runtime_plan("fixture")
python_plan = client.udf_runtime_plan("python")
print(extensions.field("extension_manifest_effect_all_runtime_blocked"))
print(extension_dir.field("extension_registry_manifest_count"))
print(extension_dir.field_bool("extension_registry_extension_code_executed"))
print(manifest.field("extension_manifest_inspection_status"))
print(manifest.field_bool("extension_manifest_execution_contract_complete"))
print(manifest.field_bool("extension_manifest_extension_code_executed"))
print(typed_udfs.field("typed_udf_registry_row_order"))
print(typed_udfs.field_bool("typed_udf_registry_external_engine_invoked"))
print(fixture_plan.field("udf_runtime_kind"))
print(python_plan.field_bool("udf_runtime_sandboxing_required"))

ctx_extensions = ctx.extension_registry()
ctx_extension_dir = ctx.extension_registry(manifest_dir="target/extensions")
ctx_manifest = ctx.extension_inspect(manifest_path="target/extension.json")
ctx_typed_udfs = ctx.udf_registry()
ctx_udf = ctx.udf_local_scalar_fixture_smoke([1, None, 3])
print(ctx_extensions.field_bool("extension_code_executed"))
print(ctx_extension_dir.field_bool("extension_registry_runtime_execution"))
print(ctx_manifest.field_bool("extension_manifest_external_effect_executed"))
print(ctx_typed_udfs.field_bool("typed_udf_registry_fallback_attempted"))
print(ctx_udf.field_bool("fallback_attempted"))
```

For the scoped local table metadata read proof, use the local-manifest smoke.
It emits a typed metadata summary and digest evidence from ShardLoom's local
manifest fixture without reading data files, touching object stores, resolving
credentials, invoking table-format dependencies, or using fallback engines.

```python
metadata = client.local_table_metadata_read_smoke()
print(metadata.field("support_status"))
print(metadata.field("claim_gate_status"))
print(metadata.field_bool("table_metadata_read_performed"))
print(metadata.field_bool("object_store_io_performed"))
print(metadata.field_bool("fallback_attempted"))
```

For the first fixture-scoped table append commit rehearsal, use the local
manifest smoke. It writes a staged committed manifest plus sidecar table commit
record, reports base/append/committed snapshot ids and digest evidence, and can
immediately roll both artifacts back for cleanup proof.

```python
table = client.local_table_append_commit_rehearsal_smoke(
    "target/table-commit/metadata-v2.json",
    idempotency_key="orders-table-commit-001",
    rollback_after_commit=True,
)
print(table.field("table_append_commit_status"))
print(table.field("committed_snapshot_id"))
print(table.field("commit_protocol_status"))
print(table.field_bool("table_catalog_commit_performed"))
print(table.field_bool("object_store_io"))
print(table.field_bool("fallback_attempted"))
```

The table metadata and append-commit smokes are `local-manifest` fixtures only.
They are not Iceberg/Delta/Hudi production metadata/runtime support, catalog
transactions, object-store-backed table commits, merge/update/delete runtime,
distributed runtime, or performance claims.

The same scoreboard exposes table-format boundaries:

```python
tables = matrix.table_format_boundary_matrix
print(tables.schema_version)
print(tables.format_scope)
print(tables.local_metadata_smoke_available)
print(tables.runtime_supported)
for row in tables.rows:
    print(row.row_id, row.support_status, row.no_io_no_fallback)
```

The `table_format_boundary_matrix` keeps Iceberg, Delta, and Hudi metadata
reads, table scans, snapshot/time-travel, partition evolution, delete/tombstone,
append, merge/update/delete, commit, rollback, catalog interaction, and
object-store coupling as separate gates. Local manifest metadata, delete/
tombstone, and append commit rehearsal smokes are related evidence only; they
are not production table-format runtime, lakehouse runtime, catalog runtime,
object-store runtime, or commit support.
Important row IDs include `table_metadata_read`, `table_scan`,
`snapshot_time_travel`, `partition_evolution`, `delete_tombstone`, `append`,
`merge_update_delete`, `commit`, `rollback`, `catalog_interaction`, and
`object_store_coupling`.

The same scoreboard exposes database and warehouse import/export boundaries:

```python
endpoints = matrix.database_warehouse_boundary_matrix
print(endpoints.schema_version)
print(endpoints.endpoint_scope)
print(endpoints.runtime_supported)
for row in endpoints.rows:
    print(
        row.row_id,
        row.support_status,
        row.credential_required,
        row.network_required,
        row.no_effects_no_fallback,
    )
```

The `database_warehouse_boundary_matrix` keeps SQLite, Postgres, MySQL,
JDBC/ODBC, Snowflake, BigQuery, and Databricks SQL separate. SQLite is the only
admitted fixture exception: `sqlite_file` is smoke-supported for local named
table import/export through `sqlite-local-import-export-smoke`, with query
pushdown disabled and no credentials/network probes. Postgres/MySQL, JDBC/ODBC,
Snowflake, BigQuery, and Databricks SQL remain blocked as connectors and cannot
serve as fallback engines. Important row IDs include `sqlite_file`, `postgres`,
`mysql`, `jdbc_odbc`, `snowflake`, `bigquery`, and `databricks_sql`.

The client also exposes advisory optimization reports:

```python
dynamic = client.dynamic_work_shaping_plan("memory-pressure")
sizing = client.sizing_feedback_plan(8, ["task-too-large", "memory-pressure-high"])
```

These commands report planned/advisory state only; they do not mutate runtime
policy yet.

Planning and evidence commands may return `status="success"` while including
error-severity diagnostics that describe missing evidence or blocked future
work. The Python client preserves those diagnostics for inspection instead of
raising unless the CLI exits nonzero or the envelope status is `error` or
`unsupported`.

The example script wires the same calls together:

```powershell
$env:PYTHONPATH = "python\src"
python examples\local-vortex-benchmark\run.py `
  --shardloom-binary target\release\shardloom.exe `
  --workspace "$HOME\LocalData\shardloom\traditional-benchmarks" `
  --input-state raw --output-format collect --reference-engine pandas
```

## Test

```powershell
$env:PYTHONPATH = "python\src"
python -m unittest discover python\tests
```
