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

The package exposes a non-placeholder development version through
`shardloom.__version__`. It is still pre-release and is not published from this
repository session.

Use `SHARDLOOM_BIN` to point at a specific CLI binary:

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

For normal Python use, start from the format-neutral query surface. The source reader and sink writer
are the only places a user should need to name a file format; ShardLoom owns the SourceState,
preparation, execution, OutputPlan, replay, reuse, certificate, and no-fallback evidence behind that
surface:

```python
import shardloom as sl

ctx = sl.context(repo_root=".", profile_order=("debug", "release"))
result = (
    ctx.read_csv("target/orders.csv")
    .filter(sl.col("amount") >= 10)
    .select("id", "amount")
    .limit(100)
    .write_jsonl("target/orders-out.jsonl", allow_overwrite=True)
)

print(result.output_row_count)
print(result.fallback_attempted, result.external_engine_invoked)
```

The same query shape can read other admitted local formats through `read_json(...)`,
`read_parquet(...)`, `read_arrow_ipc(...)`, `read_avro(...)`, or `read_orc(...)` and write to
`write(...)`, `write_jsonl(...)`, `write_csv(...)`, or feature-gated structured sinks. Format-specific
behavior belongs at read/ingest and write/sink boundaries only; compute semantics should lower
through the shared ShardLoom SQL/Python runtime or return a deterministic unsupported report.

`ShardLoomSession`, `ctx.prepare_vortex(...)`, `ShardLoomClient.vortex_ingest_smoke(...)`, and raw
runtime-envelope inspection are lower-level engine-development and diagnostic surfaces. They remain
useful for validating prepare-once, cache invalidation, VortexPreparedState, OutputPlan, replay, and
claim evidence, but they should not be required for ordinary Python or SQL users. When a session is
used, it is in-process and caller-owned; it is not a daemon, remote server, hidden global cache,
object-store/table cache, broad DataFrame/SQL runtime, or performance claim. Reuse is invalidated
when source, prepared artifact, or output artifact fingerprints change. Schema/dictionary caches,
buffer pools, CLI batch sessions, object-store/table reuse, and broader workflow session reuse remain
planned under GAR-RUNTIME-IMPL-4L/5I.

Allocation profiling and scoped buffer-pool optimization are planned as `GAR-PERF-2G`, not current
Python runtime support. Any future Python-visible buffer reuse must stay opt-in or explicitly
scoped to a run/session and preserve correctness, evidence, no-fallback, and no-external-engine
fields.

The explicit prepare-once Vortex lifecycle is available for advanced validation through a
feature-gated CLI/Python surface. Build the CLI with `--features vortex-write`, then call
`ShardLoomClient.vortex_ingest_smoke(...)` or `ctx.prepare_vortex(...)` when you intentionally need
to inspect the `UniversalIngress -> SourceState -> vortex_ingest -> VortexPreparedState` boundary:

```powershell
@"
id,label,amount
1,alpha,8
2,beta,15
"@ | Set-Content -Encoding utf8 target\vortex-ingest-source.csv

cargo run -q -p shardloom-cli --features vortex-write -- `
  vortex-ingest-smoke target\vortex-ingest-source.csv target\vortex-ingest-source.vortex `
  --allow-overwrite --format json

$env:PYTHONPATH = "python\src"
python -c "from shardloom import context; ctx=context(repo_root='.', profile_order=('debug','release')); r=ctx.prepare_vortex('target/vortex-ingest-source.csv','target/vortex-ingest-source.vortex', allow_overwrite=True); print(r.vortex_ingest_status, r.prepared_state_created, r.fallback_attempted, r.external_engine_invoked)"
```

Default CLI builds return a deterministic feature-gate blocker instead of writing an artifact. This
path is a local fixture smoke; it is not the primary user API, broad Vortex writer support,
object-store/table output support, production SQL/DataFrame support, or a performance claim.

Traditional analytics compatibility inputs can also use a single-process prepare/batch route
through `ShardLoomClient.traditional_analytics_prepare_batch_run(...)` or the convenience
`prepare_and_run_traditional_analytics_vortex_batch(...)` helper. Both invoke
`traditional-analytics-prepare-batch-run`, prepare the local fact/dimension inputs once into
prepared Vortex artifacts, then run a prepared/native scenario batch while preserving
`prepare_batch_*`, source-state reuse, fallback, and claim-boundary fields:

```python
from shardloom import ShardLoomClient

client = ShardLoomClient.from_repo()
result = client.traditional_analytics_prepare_batch_run(
    ["selective filter", "filter + projection + limit"],
    "fact.csv",
    "dim.csv",
    workspace="target/prepare-batch",
    input_format="csv",
    evidence_level="certified",
)

print(result.field("prepare_batch_preparation_included_in_batch_timing"))
print(result.field("source_state_reuse_status"))
print(result.fallback.attempted)
```

This is a scoped local runtime route for avoiding repeated compatibility preparation inside a batch.
Use `prepare_traditional_analytics_vortex_artifacts(...)` only when the caller needs to manage
prepared artifacts explicitly across later commands. This is not a native Python binding,
persistent cache, object-store/table runtime, package-readiness claim, or performance claim.

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

One scoped local CSV plus flat JSON/JSONL/NDJSON and feature-gated flat scalar
Parquet/Arrow IPC/Avro/ORC query-builder workflow family is
executable through the same typed CLI bridge. A workflow shaped as
`read_csv(...).select(...).limit(...)`, with an optional `filter(...)`, lowers
to ShardLoom's `sql-local-source-smoke` path, runs ShardLoom-owned
projection/optional-filter/limit semantics, and returns a typed evidence
report. `preview(limit=n)`, `head(limit=n)`, and `take(n)` use the same bounded
local path with `SELECT *`. The same projection/optional-filter/limit
shape is admitted for `read_json(...)` when the source path is a local flat
`.json`, `.jsonl`, or `.ndjson` file; nested JSON expansion and JSONPath remain
deterministic unsupported surfaces. The same shape is admitted for
`read_parquet(...)` over local flat scalar `.parquet` files when the CLI is built
with `--features universal-format-io`; default binaries return an explicit
Parquet adapter blocker. `read_arrow_ipc(...)` admits the same scoped shape for
local flat scalar `.arrow`, `.ipc`, or `.feather` files under the same feature
gate; default binaries return an explicit Arrow IPC adapter blocker. This is a
file-backed local source adapter, not an in-memory Arrow table fallback,
zero-copy Arrow runtime, or Arrow IPC output surface. `read_avro(...)` and
`read_orc(...)` admit the same scoped shape for local flat scalar `.avro` and
`.orc` files under the same feature gate; default binaries return explicit Avro
or ORC adapter blockers. These are decoded local file smoke adapters, not Avro
schema-evolution support, ORC stripe/statistics runtime support, or Avro/ORC
output surfaces. Filters admit scoped comparison,
cast, date-literal, scoped UTC `TIMESTAMP 'YYYY-MM-DDTHH:MM:SS(.ffffff)Z'` literals,
Date32 extract predicates with `DATE_YEAR(...)` / `DATE_MONTH(...)` /
`DATE_DAY(...)`, UTC timestamp extract predicates with
`TIMESTAMP_YEAR(...)` / `TIMESTAMP_MONTH(...)` / `TIMESTAMP_DAY(...)` /
`TIMESTAMP_HOUR(...)` / `TIMESTAMP_MINUTE(...)` / `TIMESTAMP_SECOND(...)`,
Date32 day arithmetic with `DATE_ADD_DAYS(...)` / `DATE_SUB_DAYS(...)`,
scoped temporal-difference expressions with `DATE_DIFF_DAYS(...)` and
`TIMESTAMP_DIFF_SECONDS(...)` compared against numeric literals,
scoped numeric arithmetic predicates such as `<column> + 5 >= 20` and
`<column> * 2.0 > 1.0`,
bounded `IN (...)` / `NOT IN (...)`, scoped local
`IN (SELECT <column> FROM '<local-source>')` / `NOT IN (...)` subquery predicates, direct SQL
`BETWEEN` / `NOT BETWEEN`, inclusive Python `between(...)` range predicates, UTF-8
`LENGTH(column)` comparisons against integer literals, string `LIKE` / `NOT LIKE`, null, logical
`AND`/`OR`/`NOT`, and balanced grouping parentheses over already admitted leaves. `where(...)` is a
familiar alias for `filter(...)`. `IN` lists admit up to 32 literal values from one scalar family,
including `DATE 'YYYY-MM-DD'` lists and `NULL` literals with SQL three-valued `WHERE`-filter
semantics. Scoped local `IN` subqueries materialize a bounded scalar column from another admitted
local source and keep correlated, filtered, joined, grouped, ordered, limited, nested, and
multi-column subqueries blocked. Typed reports expose `in_predicate_runtime_execution`,
`in_list_value_count`, `in_list_null_value_count`, `in_predicate_null_semantics`,
`in_subquery_runtime_execution`, `in_subquery_source_columns`, `in_subquery_source_formats`,
`in_subquery_materialized_value_count`, and `in_subquery_materialized_null_value_count`, plus
`numeric_arithmetic_runtime_execution`,
`numeric_arithmetic_operator`, `numeric_arithmetic_source_column`, and
`numeric_arithmetic_rhs_dtype` when arithmetic predicates are used, plus
`numeric_abs_runtime_execution`, `numeric_abs_source_column`, and
`numeric_abs_rhs_dtype` when `ABS(column)` predicates are used, plus
`numeric_rounding_runtime_execution`, `numeric_rounding_operator`,
`numeric_rounding_source_column`, and `numeric_rounding_rhs_dtype` when
`FLOOR`/`CEIL`/`ROUND` predicates are used, plus
`generic_expression_predicate_runtime_execution`,
`generic_expression_predicate_source_columns`,
`generic_expression_predicate_operator_families`,
`generic_expression_predicate_binary_operator_count`, and
`generic_expression_predicate_comparison_operators` when generalized numeric expression-tree or
temporal-difference predicates are used, plus
`string_length_runtime_execution`, `string_length_source_column`, and `string_length_rhs_dtype`
when UTF-8 length predicates are used.
The Python query builder also exposes a scoped `sl.col(...)` predicate helper for admitted local
runtime predicates. It lowers comparisons, `is_null()`, `is_not_null()`, `contains()`,
`not_contains()`, `startswith()`, `not_startswith()`, `endswith()`, `not_endswith()`, `like(...)`,
`not_like(...)`, `between(...)`, bounded `isin(...)` / `not_in(...)`, local source-backed
`isin_source(source, column)` / `not_in_source(source, column)`, `cast(dtype)`,
`is_true()`, `is_false()`, `is_not_true()`, `is_not_false()`,
`date_year()`, `date_month()`, `date_day()`, `date_add_days(days)`, and
`date_sub_days(days)`, plus `timestamp_year()`, `timestamp_month()`, `timestamp_day()`,
`timestamp_hour()`, `timestamp_minute()`, `timestamp_second()`,
`timestamp_add_seconds(seconds)`, `timestamp_sub_seconds(seconds)`,
`date_diff_days(other)`, and `timestamp_diff_seconds(other)` comparisons, and the scoped
UTF-8 `length()` helper, numeric `abs()` / `floor()` / `ceil()` / `round()` helpers, and numeric `+`, `-`, `*`, and `/` operators for arithmetic predicates, including scoped generalized numeric expression-tree filters such as `(sl.col("amount") + sl.col("tax")) * 2 >= 40`, into the same
ShardLoom SQL smoke path; unsupported shapes still block in ShardLoom before fallback.
Input-backed computed `with_column(...)` is also admitted with or without an explicit `select(...)`
for local CSV, flat JSON/JSONL/NDJSON, and feature-gated flat scalar Parquet/Arrow IPC/Avro/ORC
projection/filter/sort/limit workflows. Without an explicit projection it lowers to ShardLoom-native
`SELECT *, <computed> AS <column>` over the shared local-source runtime path. The current slice
accepts deterministic `lit(...)` values,
direct bool/int/float literals, scoped numeric arithmetic expressions shaped as
`sl.col("amount") + 5`, `-`, `*`, or `/` with an int/finite-float literal, scoped
generalized numeric expression-tree projections such as
`(sl.col("amount") + sl.col("tax")) * 2`, `sl.abs(sl.col("amount") - sl.col("tax"))`,
and `sl.round((sl.col("amount") + sl.col("tax")) / 2.0)` over admitted numeric local
columns and finite numeric literals, scoped
`sl.col("amount").abs()` / `sl.abs(sl.col("amount"))` numeric absolute-value projections, scoped
`sl.col("amount").floor()` / `.ceil()` / `.round()` or `sl.floor(...)` / `sl.ceil(...)` /
`sl.round(...)` numeric rounding projections, plus scoped UTF-8
`sl.col("label").lower()`, `.upper()`, and `.trim()` projections, scoped
`sl.col("label").length()` / `sl.length(sl.col("label"))` projections, scoped
`sl.col("amount").cast("float64")` / `.cast("date32")` / `.cast("timestamp_micros")`
projections, and scoped Date32/UTC timestamp extract projections such as
`sl.col("event_date").cast("date32").date_year()` or
`sl.col("event_ts").cast("timestamp_micros").timestamp_hour()`, plus scoped Date32 day arithmetic
projections such as `sl.col("event_date").cast("date32").date_add_days(7)` or
`.date_sub_days(1)`, scoped UTC timestamp second arithmetic projections such as
`sl.col("event_ts").cast("timestamp_micros").timestamp_add_seconds(60)` or
`.timestamp_sub_seconds(45)`, scoped temporal-difference projections such as
`sl.col("end_date").cast("date32").date_diff_days(sl.col("start_date"))` or
`sl.col("end_ts").cast("timestamp_micros").timestamp_diff_seconds(sl.col("start_ts"))`, and
scoped null-cleanup projections such as
`sl.col("label").fill_null("unknown")` or
`sl.col("event_date").cast("date32").fill_null(date(2026, 1, 1))`, scoped null-sentinel
cleanup projections such as `sl.col("label").null_if("missing")` or
`sl.col("event_date").cast("date32").null_if(date(2026, 1, 1))`, plus scoped single-branch
conditional projections such as
`sl.case_when(sl.col("amount") >= 10, "large", "small")`. Literal projections emit
`literal_projection_*` evidence; cast projections emit `cast_projection_*` evidence; numeric
arithmetic projections emit `numeric_arithmetic_projection_*` evidence; numeric absolute-value
projections emit `numeric_abs_projection_*` evidence; numeric rounding projections emit
`numeric_rounding_projection_*` evidence; generalized numeric expression-tree and
temporal-difference projections emit `generic_expression_projection_*` evidence; generalized
numeric expression-tree and temporal-difference predicates emit `generic_expression_predicate_*` evidence; string transform
projections emit `string_transform_projection_*` evidence; string length projections emit
`string_length_projection_*` evidence; date/time extract projections emit
`date_extract_projection_*` and `timestamp_extract_projection_*` evidence; date arithmetic
projections emit `date_arithmetic_projection_*` evidence; UTC timestamp arithmetic predicates and
projections emit `timestamp_arithmetic_*` and `timestamp_arithmetic_projection_*` evidence; null coalesce projections emit
`null_coalesce_projection_*` evidence; nullif projections emit `nullif_projection_*` evidence;
conditional projections emit
`conditional_projection_*` evidence. Sorting after an input-backed computed projection is admitted
for bounded top-N workflows when the sort key resolves to a projected computed alias or a source
column; those workflows emit `computed_projection_top_n_runtime_execution=true`,
`computed_projection_operator_family=computed_projection_topn`, and the ordinary `sort_*` and
`top_n_*` evidence fields. Mixed `int64`/`float64` arithmetic promotes to `float64`
only when the `int64` operand is exactly representable as `float64`; lossy mixed coercions,
generic expression missing-source-column and division-by-zero cases, `COALESCE(..., NULL)`,
`NULLIF(..., NULL)`, non-null source/fallback dtype mismatches, and non-null source/sentinel dtype
mismatches block deterministically before fallback. `CASE WHEN` projections currently admit one
branch, admitted predicate leaves, non-NULL literal branches, and matching branch dtypes only.
Unsupported computed-column expressions still block before fallback.
CSV, local flat
JSON/JSONL/NDJSON, and feature-gated flat scalar Parquet/Arrow IPC/Avro/ORC are admitted for scoped scalar aggregates shaped as
`aggregate(...).limit(1)` with an optional filter for `COUNT`, `SUM`, `AVG`,
`MIN`, and `MAX`. The convenience `count()` method lowers to the same
`COUNT(*)` scalar aggregate smoke with a bounded `LIMIT 1`. Multi-key grouped aggregates shaped as
`group_by(...).agg(...).limit(n)` with an optional filter are supported, including named
aggregate aliases such as `agg(rows="count(*)", total="sum(amount)")`; those grouped aggregate
rows can also use scalar top-N ordering over aggregate output aliases and group keys via
`group_by(...).agg(...).sort(...).limit(n)`. Post-aggregate filtering is admitted through
explicit `having(...)` or through `filter(...)` after `agg(...)`, and binds only to aggregate
output aliases and selected group keys before optional `sort(...).limit(n)`. A multi-key
scalar top-N shape, `select(...).sort(...).limit(n)` with an optional filter,
over non-null numeric or UTF-8 sort
keys. Local-source joins also admit scalar and grouped aggregates, including scalar top-N
ordering over aggregate output aliases and group keys, when the workflow keeps the
same explicit aliases, qualified join-side columns, optional pre-aggregate filter, and bounded
`limit(...)`; joined aggregate rows can use the same aggregate-output `HAVING` filter before
ordering/limit. `collect()` returns bounded inline JSONL; `write()` writes a local JSONL/CSV file
by default, and local-source workflows can use `write(..., output_format="csv")`
or `write_csv(...)` for the scoped local CSV sink. They can also use
`write_parquet(...)` or `write(..., output_format="parquet")` for the scoped
feature-gated flat scalar Parquet sink when the CLI is built with
`--features universal-format-io`; default binaries return ShardLoom's
deterministic Parquet sink blocker. `write_vortex(...)` writes a scoped local
flat scalar `.vortex` result when the CLI is built with `--features vortex-write`;
default binaries return a deterministic Vortex sink blocker. The scoped
`.fanout(...)` helper can reuse one computed result for multiple admitted local
compatibility sinks such as JSONL and CSV, feature-gated flat scalar
Parquet/Arrow IPC/Avro/ORC when the CLI is built with `--features universal-format-io`,
and feature-gated local Vortex when built with `--features vortex-write`. Written local sinks emit
format-specific output Native I/O certificate fields plus scoped local replay/fidelity fields such
as `result_replay_verified`, `output_replay_status`, `output_fidelity_report_status`, and
`output_fidelity_loss`. Generated-source helpers use the same format-neutral rule: SQL/Python
expressions are planned before the write boundary, and `.fanout(...)` treats the first requested
sink as the primary output while writing remaining sinks from the same computed generated rows:

```powershell
New-Item -ItemType Directory -Force target | Out-Null
@"
id,label,amount
1,alpha,8
2,beta,15
3,gamma,
"@ | Set-Content -Encoding utf8 target\sql-local-source-smoke.csv
@"
id
1
3
NULL
"@ | Set-Content -Encoding utf8 target\sql-local-source-allowed.csv
@'
{"id":1,"label":"alpha","amount":8}
{"id":2,"label":"beta","amount":15}
{"id":3,"label":"gamma","amount":21}
'@ | Set-Content -Encoding utf8 target\sql-local-source-smoke.jsonl
$env:PYTHONPATH = "python\src"
@'
import shardloom as sl

ctx = sl.context(repo_root=".", profile_order=("debug", "release"))
workflow = (
    ctx.read_csv("target/sql-local-source-smoke.csv")
    .select("id", "label")
    .limit(1)
)
preview = ctx.read_csv("target/sql-local-source-smoke.csv").preview(limit=2)
head = ctx.read_csv("target/sql-local-source-smoke.csv").head(limit=2)
take = ctx.read_csv("target/sql-local-source-smoke.csv").take(2)
filtered = (
    ctx.read_csv("target/sql-local-source-smoke.csv")
    .select("id", "label")
    .filter("amount >= 10 AND (label LIKE '%ta' OR label LIKE 'gam%')")
    .limit(1)
    .collect()
)
predicate_builder_filtered = (
    ctx.read_csv("target/sql-local-source-smoke.csv")
    .select("id", "label")
    .where(sl.col("amount").between(10, 25) & sl.col("label").contains("ta"))
    .limit(10)
    .collect()
)
literal_column = (
    ctx.read_csv("target/sql-local-source-smoke.csv")
    .select("id", "label")
    .with_column("segment", "lit('north')")
    .filter(sl.col("amount") >= 10)
    .limit(10)
    .collect()
)
arithmetic_column = (
    ctx.read_csv("target/sql-local-source-smoke.csv")
    .select("id")
    .with_column("adjusted", sl.col("amount") + 5)
    .filter(sl.col("amount") >= 10)
    .limit(10)
    .collect()
)
abs_column = (
    ctx.read_csv("target/sql-local-source-smoke.csv")
    .select("id")
    .with_column("magnitude", sl.abs(sl.col("amount")))
    .filter(sl.col("amount").abs() >= 10)
    .limit(10)
    .collect()
)
rounding_column = (
    ctx.read_csv("target/sql-local-source-smoke.csv")
    .select("id")
    .with_column("amount_floor", sl.floor(sl.col("amount")))
    .filter(sl.col("amount").round() >= 10)
    .limit(10)
    .collect()
)
in_filtered = (
    ctx.read_csv("target/sql-local-source-smoke.csv")
    .select("id", "label")
    .filter("label IN ('alpha','gamma')")
    .limit(10)
    .collect()
)
allowed_ids = ctx.read_csv("target/sql-local-source-allowed.csv")
source_subquery_filtered = (
    ctx.read_csv("target/sql-local-source-smoke.csv")
    .select("id", "label")
    .filter(sl.col("id").isin_source(allowed_ids, "id"))
    .limit(10)
    .collect()
)
json_rows = (
    ctx.read_json("target/sql-local-source-smoke.jsonl")
    .select("id", "label")
    .limit(2)
    .write("target/sql-local-source-json-result.jsonl", allow_overwrite=True)
)

collected = workflow.collect()
python_objects = workflow.to_python_objects()
schema_report = workflow.schema()
schema_validation = workflow.validate_schema({"id": "int64", "label": "string"})
quality_report = workflow.data_quality_check("not_null:id", "unique:id")
written = workflow.write("target/sql-local-source-result.jsonl", allow_overwrite=True)
csv_written = workflow.write_csv("target/sql-local-source-result.csv", allow_overwrite=True)
fanout_written = workflow.fanout(
    {
        "jsonl": "target/sql-local-source-fanout.jsonl",
        "csv": "target/sql-local-source-fanout.csv",
    },
    allow_overwrite=True,
)
aggregate = (
    ctx.read_json("target/sql-local-source-smoke.jsonl")
    .aggregate("count(*)", "sum(amount)", "avg(amount)", "min(amount)", "max(amount)")
    .limit(1)
    .collect()
)
row_count = (
    ctx.read_csv("target/sql-local-source-smoke.csv")
    .filter(sl.col("amount") >= 10)
    .count()
)
grouped = (
    ctx.read_json("target/sql-local-source-smoke.jsonl")
    .group_by("label")
    .agg("count(*)", "sum(amount)")
    .limit(10)
    .collect()
)
grouped_having = (
    ctx.read_json("target/sql-local-source-smoke.jsonl")
    .group_by("label")
    .agg(rows="count(*)", total_amount="sum(amount)")
    .having((sl.col("total_amount") >= 10) & (sl.col("rows") >= 2))
    .sort("total_amount", descending=True)
    .limit(10)
    .collect()
)
topn = (
    ctx.read_json("target/sql-local-source-smoke.jsonl")
    .select("id", "label")
    .sort("amount", "id", descending=True)
    .limit(2)
    .collect()
)
joined = (
    ctx.read_csv("target/sql-local-source-join-fact.csv")
    .join(ctx.read_csv("target/sql-local-source-join-dim.csv"), on="customer_id")
    .select("f.id", "d.segment")
    .filter("f.amount >= 10")
    .limit(10)
    .collect()
)

print(collected.result_rows)
print(python_objects)
print(schema_report.schema_map)
print(schema_validation.valid)
print(quality_report.passed)
print(written.output_path)
print(written.output_native_io_certificate_status)
print(csv_written.output_path)
print(csv_written.output_format)
print(csv_written.output_native_io_certificate_status)
print(fanout_written.fanout_output_count)
print(fanout_written.fanout_output_formats)
print(fanout_written.fanout_result_reuse_hit)
print(written.fallback_attempted, written.external_engine_invoked)
print(written.evidence_summary.output_native_io_certificate_status)
print(written.claim_summary.claim_gate_status)
print(preview.result_rows)
print(head.result_rows)
print(take.result_rows)
print(filtered.logical_predicate_operator, filtered.logical_predicate_leaf_count)
print(
    arithmetic_column.result_rows,
    arithmetic_column.numeric_arithmetic_projection_operator,
    arithmetic_column.numeric_arithmetic_projection_output_columns,
)
print(
    abs_column.result_rows,
    abs_column.numeric_abs_projection_runtime_execution,
    abs_column.numeric_abs_projection_output_columns,
)
print(
    rounding_column.result_rows,
    rounding_column.numeric_rounding_projection_operators,
    rounding_column.numeric_rounding_projection_output_columns,
)
print(
    in_filtered.in_predicate_runtime_execution,
    in_filtered.in_list_value_count,
    in_filtered.in_list_null_value_count,
    in_filtered.in_predicate_null_semantics,
)
print(
    source_subquery_filtered.in_subquery_runtime_execution,
    source_subquery_filtered.in_subquery_source_columns,
    source_subquery_filtered.in_subquery_materialized_value_count,
    source_subquery_filtered.in_subquery_materialized_null_value_count,
)
print(json_rows.output_path, json_rows.envelope.field("source_format"))
print(aggregate.first_result_row)
print(aggregate.aggregate_operator_family)
print(aggregate.aggregate_functions)
print(row_count.first_result_row)
print(row_count.aggregate_functions)
print(grouped.result_rows)
print(grouped.aggregate_operator_family)
print(grouped.group_by_columns)
print(grouped_having.result_rows)
print(grouped_having.having_runtime_execution, grouped_having.having_source_columns)
print(topn.result_rows)
print(topn.order_by_runtime_execution, topn.sort_keys, topn.sort_direction)
print(joined.result_rows)
print(joined.join_runtime_execution, joined.join_type)
print(joined.evidence_summary.command)
print(joined.claim_summary.public_performance_claim_allowed)
'@ | python -
```

The same admitted local-source SQL can also be entered through `ctx.sql(...)`
when a user wants the PySpark-like "one SQL string" shape. The broad SQL engine
remains gated, but admitted statements dispatch to ShardLoom's scoped SQL smoke
instead of returning report-only by default:

```python
sql_rows = ctx.sql(
    "SELECT id,label FROM 'target/sql-local-source-smoke.csv' "
    "WHERE amount >= 10 AND (label LIKE '%ta' OR label LIKE 'gam%') LIMIT 2"
).collect()

sql_in_rows = ctx.sql(
    "SELECT id,label FROM 'target/sql-local-source-smoke.csv' "
    "WHERE label IN ('alpha','gamma') LIMIT 10"
).collect()

sql_in_subquery_rows = ctx.sql(
    "SELECT id,label FROM 'target/sql-local-source-smoke.csv' "
    "WHERE id IN (SELECT id FROM 'target/sql-local-source-allowed.csv') LIMIT 10"
).collect()

sql_written = ctx.sql(
    "SELECT id,label FROM 'target/sql-local-source-smoke.csv' "
    "WHERE amount >= 10 LIMIT 2"
).write("target/sql-local-source-from-sql.jsonl", allow_overwrite=True)

print(sql_rows.result_rows)
print(
    sql_in_rows.in_predicate_runtime_execution,
    sql_in_rows.in_list_value_count,
    sql_in_rows.in_list_null_value_count,
    sql_in_rows.in_predicate_null_semantics,
)
print(
    sql_in_subquery_rows.in_subquery_runtime_execution,
    sql_in_subquery_rows.in_subquery_source_formats,
    sql_in_subquery_rows.in_subquery_materialized_value_count,
)
print(sql_written.output_path)
print(sql_written.fallback_attempted, sql_written.external_engine_invoked)
```

This is a fixture-smoke local CSV plus flat JSON/JSONL/NDJSON and feature-gated flat scalar
Parquet/Arrow IPC/Avro/ORC bridge for the scoped
projection/optional-filter/limit, scalar aggregate, scalar aggregate-output top-N,
multi-key grouped aggregate, grouped aggregate-output top-N,
preview/head/take select-star, input-backed literal, scoped numeric arithmetic, scoped numeric
ABS, scoped numeric rounding, and scoped UTF-8 string length `with_column`,
multi-key scalar top-N, and scoped local-source join shapes covering inner, left/right/full outer,
left semi/anti, cross joins, and scoped expression-condition joins.
Joined workflows also admit scoped computed projections over qualified columns plus multi-key
scalar top-N over joined rows. Scoped scalar/grouped join aggregates over those same join shapes
lower through the same runtime, may filter aggregate output rows with `HAVING`, and may order by
numeric aggregate output aliases or UTF-8 group keys before a bounded `limit(...)`.
It does not make the Python client a
pandas/Polars-like execution engine, does not add broad SQL/DataFrame runtime,
expression-backed `with_column` beyond the admitted numeric/string/null/temporal/predicate families,
generalized grouped aggregation or HAVING expressions beyond emitted aggregate output columns,
ordering/collation parity, nested JSON,
broader Parquet/Arrow IPC/Avro/ORC type/nesting coverage, object stores, or table/lakehouse inputs, and does not create a performance or
production claim.

The Python query builder admits scoped local-source joins through the same scoped SQL local-source
smoke. Both sides must be admitted local sources such as CSV or flat JSON/JSONL/NDJSON, with
feature-gated flat scalar Parquet/Arrow IPC/Avro/ORC using the same deterministic adapter gates as
other local-source smokes. Use `join(..., on="key")` or
`join(..., on=("customer_id", "region"))` for inner, left/right/full outer, left semi, or left
anti joins over matching same-named key columns on both sides. Use `join(..., how="cross")`
without `on` for a scoped cross join and place filters in `filter(...)` / SQL `WHERE`. Qualified
expression joins use `join(..., condition="f.amount > d.threshold")` or direct SQL `ON`
predicates; the condition must bind qualified columns from both sides and remains independent of
the source file format. Qualified
projection columns such as `f.id` and `d.segment`, a qualified predicate such as `f.amount >= 10`,
and an explicit `limit(...)` are required. Left semi and left anti joins emit the left source only;
right-side projections outside the `ON` clause fail closed. A joined workflow can add admitted
`with_column(...)` expressions over
qualified columns and may use `sort("f.amount", "f.id", descending=True).limit(n)` for the scoped
multi-key scalar joined top-N path. A joined workflow can also end in `agg(...).limit(...)` or
`group_by(...).agg(...).limit(...)` for the admitted scalar/grouped join-aggregate subset, and can
place `having(...)` or post-aggregate `filter(...)` before
`sort("total_amount", descending=True).limit(n)` when the HAVING predicate binds only emitted
aggregate output aliases or group keys. Broad
DataFrame joins remain blocked: arbitrary join predicate trees beyond the admitted expression ON
families, unqualified join predicates, nested/complex structured data, and
object-store/table joins still return deterministic unsupported diagnostics or
fail closed through the scoped SQL binder.

Typed runtime reports expose `result_rows` and `first_result_row` helpers plus compact evidence and
claim helpers so examples do not need to parse raw JSONL or scrape raw envelope fields. The
`to_python_objects()` convenience returns the same validated bounded row objects directly for
admitted local-source workflows. Schema and data-quality helpers use the same bounded
`sql-local-source-smoke` path, so format-specific behavior remains isolated to read adapters and
write sinks:

```python
summary = written.evidence_summary
claim = written.claim_summary

print(collected.first_result_row)
print(workflow.schema().schema_map)
print(workflow.validate_schema({"id": "int64", "label": "string"}).valid)
print(workflow.data_quality_summary().null_counts)
print(workflow.data_quality_check("not_null:id", "unique:id").passed)
print(summary.output_path)
print(summary.output_io_performed)
print(summary.fallback_attempted, summary.external_engine_invoked)
print(claim.claim_gate_status)
print(claim.public_performance_claim_allowed)
```

The lower-level `client.sql_local_source_smoke(...)` helper can also call the
scoped local CSV scalar aggregate, grouped aggregate, aggregate-output order/top-N, projection
order/top-N, explicit local-source joins, joined row top-N, and joined aggregate-output order/top-N
smokes directly. Direct client calls are only a typed wrapper around the CLI fixture-smoke evidence:

```python
report = client.sql_local_source_smoke(
    "SELECT count(*),sum(amount),avg(amount) "
    "FROM 'target/sql-local-source-smoke.csv' "
    "WHERE amount >= 10 LIMIT 1",
)
print(report.first_result_row)
print(report.claim_gate_status)

grouped = client.sql_local_source_smoke(
    "SELECT region,segment,count(*) AS rows,sum(amount) AS total_amount "
    "FROM 'target/sql-local-source-smoke.csv' "
    "WHERE amount >= 10 GROUP BY region,segment LIMIT 10",
)
print(grouped.group_by_columns)
print(grouped.group_by_key_arity)
print(grouped.group_by_multi_key_runtime_execution)
print(grouped.aggregate_output_columns)
print(grouped.aggregate_aliases)
print(grouped.group_by_group_count)

grouped_topn = client.sql_local_source_smoke(
    "SELECT region,count(*) AS rows,sum(amount) AS total_amount "
    "FROM 'target/sql-local-source-smoke.csv' "
    "WHERE amount >= 10 GROUP BY region "
    "ORDER BY total_amount DESC,rows DESC LIMIT 2",
)
print(grouped_topn.sort_keys)
print(grouped_topn.sort_direction)
print(grouped_topn.top_n_limit)

grouped_having = client.sql_local_source_smoke(
    "SELECT region,count(*) AS rows,sum(amount) AS total_amount "
    "FROM 'target/sql-local-source-smoke.csv' "
    "WHERE amount >= 0 GROUP BY region "
    "HAVING total_amount >= 10 AND rows >= 2 "
    "ORDER BY total_amount DESC LIMIT 10",
)
print(grouped_having.having_runtime_execution)
print(grouped_having.having_operator_family)
print(grouped_having.having_source_columns)
print(grouped_having.having_input_row_count, grouped_having.having_selected_row_count)

topn = client.sql_local_source_smoke(
    "SELECT id,label FROM 'target/sql-local-source-smoke.csv' "
    "WHERE amount >= 0 ORDER BY amount DESC LIMIT 2",
)
print(topn.sort_keys)
print(topn.top_n_limit)

join = client.sql_local_source_smoke(
    "SELECT f.id,d.segment "
    "FROM 'target/sql-local-source-join-fact.csv' AS f "
    "INNER JOIN 'target/sql-local-source-join-dim.csv' AS d "
    "ON f.customer_id = d.customer_id AND f.region = d.region "
    "WHERE f.amount >= 10 LIMIT 10",
)
print(join.join_runtime_execution)
print(join.join_type)
print(join.join_left_keys, join.join_right_keys)
print(join.join_key_arity, join.join_multi_key_runtime_execution)
print(join.join_matched_row_count, join.join_rows_output)

join_topn = client.sql_local_source_smoke(
    "SELECT f.id,d.segment,f.amount + d.discount AS adjusted "
    "FROM 'target/sql-local-source-join-fact.csv' AS f "
    "INNER JOIN 'target/sql-local-source-join-dim.csv' AS d "
    "ON f.customer_id = d.customer_id AND f.region = d.region "
    "WHERE f.amount >= 10 ORDER BY f.amount DESC LIMIT 3",
)
print(join_topn.join_computed_projection_runtime_execution)
print(join_topn.join_order_by_top_n_runtime_execution)
print(join_topn.join_projection_operator_family)

join_grouped = client.sql_local_source_smoke(
    "SELECT d.segment,count(*) AS rows,sum(f.amount) AS total_amount "
    "FROM 'target/sql-local-source-join-fact.csv' AS f "
    "INNER JOIN 'target/sql-local-source-join-dim.csv' AS d "
    "ON f.customer_id = d.customer_id AND f.region = d.region "
    "WHERE f.amount >= 10 GROUP BY d.segment LIMIT 10",
)
print(join_grouped.join_aggregate_runtime_execution)
print(join_grouped.join_aggregate_operator_family)
print(join_grouped.join_aggregate_group_count)

join_grouped_topn = client.sql_local_source_smoke(
    "SELECT d.segment,count(*) AS rows,sum(f.amount) AS total_amount "
    "FROM 'target/sql-local-source-join-fact.csv' AS f "
    "INNER JOIN 'target/sql-local-source-join-dim.csv' AS d "
    "ON f.customer_id = d.customer_id AND f.region = d.region "
    "WHERE f.amount >= 10 GROUP BY d.segment "
    "ORDER BY total_amount DESC,rows DESC LIMIT 2",
)
print(join_grouped_topn.sort_keys)
print(join_grouped_topn.top_n_limit)
```

That path is still fixture-smoke evidence only. Broader grouped aggregate generality,
null ordering, collation parity,
broader correlated/multi-column/nested subquery semantics, arbitrary predicate-tree completeness
beyond the admitted parenthesized leaves, Python/DataFrame joins beyond
the scoped local-source query-builder bridge, broad expression-backed input-backed `with_column`,
arbitrary expression/non-equi join predicates beyond the admitted expression ON families, broad
HAVING over non-output source columns or aggregate-function expressions not emitted as aliases,
broad SQL/DataFrame planning, and
production query support remain blocked until later runtime slices.

Evidence-aware optimizer traces are planned as `GAR-PERF-2B`, not current Python runtime support. A
future Python `explain()` trace should expose optimizer rule status, before/after plan digests,
rewrite safety, evidence preservation, no-fallback fields, and claim gates without implying broad
SQL/DataFrame execution or Polars/DataFusion optimizer parity.

Reusable I/O state and broad cross-format fanout are planned as `GAR-IOREUSE-1`. The current Python
runtime exposes scoped local-source `.fanout(...)` smokes over admitted local compatibility sinks and
feature-gated local Vortex output/fanout with local artifact replay/fidelity reporting. Generated
source-free helpers also expose `.fanout(...)` over admitted generated rows and source-free SQL.
Current typed result objects expose scoped `SourceState`, `VortexPreparedState`, and `OutputPlan`
evidence where the CLI emits it; future Python capability/write views may broaden cache
invalidation, reuse levels, persistent OutputPlan reuse, and claim-grade replay/fidelity evidence.
Input and output formats remain decoupled, and reuse evidence will not imply performance,
production, object-store/lakehouse, Foundry, or SQL/DataFrame support.

Unsupported workflow affordances are explicit report surfaces too. These calls
show how familiar pandas/Arrow/DataFrame/notebook methods fail closed when they
are outside the admitted bounded local-source shapes:

```python
import shardloom as sl

ctx = sl.context()
workflow = ctx.read_csv("events.csv").filter("amount > 0")
selected_workflow = workflow.select("customer_id", "amount")

reports = [
    sl.from_pandas(object()),
    sl.from_arrow_table(object()),
    sl.from_arrow_ipc("events.arrow"),
    workflow.to_pandas(),
    workflow.to_arrow_table(),
    workflow.to_arrow_ipc(),
    workflow.to_numpy(),
    workflow.with_column("event_date", "to_date(ts)"),
    selected_workflow.group_by("customer_id", "region").agg(total="sum(amount)"),
    selected_workflow.group_by("customer_id").agg(total="sum(amount)"),
    selected_workflow.agg("count(*)"),
    workflow.sort("event_date"),
    workflow.data_quality_check("regex:id"),
    workflow.quarantine("bad-events.vortex"),
    workflow.preview(limit=20),
    workflow.display(),
    ctx.sql_parse("select * from events"),
    ctx.sql_bind("select * from events"),
    ctx.sql_plan("select * from events"),
    ctx.sql_execute("select * from events"),
]

for report in reports:
    print(report.operation)
    print(report.blocker_id)
    print(report.required_evidence)
    print(report.suggested_next_action)
    print(report.runtime_execution, report.data_read, report.write_io)
```

Every report above is generated through `workflow-unsupported-plan` and returns
`status="unsupported"` with `fallback_attempted=false`. The methods do not
import pandas or pyarrow, inspect the passed Python object, materialize pandas/Arrow/NumPy objects,
write quarantine outputs, parse SQL, execute unsupported DataFrame expressions, render
notebook display output, invoke Foundry/model services, or use another engine
as fallback.

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

This matrix is mostly report-only, with the scoped local CSV `collect` and
`write` rows plus the flat JSON/JSONL/NDJSON and feature-gated flat scalar Parquet/Arrow IPC/Avro/ORC
projection/optional-filter/limit bridges marked as
fixture-smoke-supported only for the admitted projection/optional-filter/limit,
preview/select-star, scalar aggregate, and multi-key grouped aggregate shapes described above.
It does not import DataFrame
libraries, invoke external engines, or upgrade DataFrame/notebook support to
claim-grade status. Other lazy source, `filter`, `select`, `limit`, and
`group_by` helpers remain side-effect-free declarations unless an admitted
terminal method is called. Joins, aggregations beyond admitted slices, windows,
schema/data-quality helpers, materialization to Python objects, and notebook
display remain deterministic unsupported diagnostic surfaces unless later
evidence-backed slices promote them.

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

This compatibility contract is still a capability map. It can say local JSONL/CSV generated-output
smokes exist for user rows, literal tables, calendar/date dimensions, range, sequence, scoped
generated-row projection/literal `with_column`, SQL `VALUES`, SQL literal `SELECT`, and SQL
`generate_series`/`range`, but it keeps
broader SQL runtime, broad DataFrame generated expressions, object-store/lakehouse output, and
Foundry generated-output runtime as report-only or blocked.

The supported user-row local smoke uses Python rows supplied by the caller,
writes a local JSONL/CSV file, and returns generated-source/output evidence:

```python
from shardloom import context

ctx = context(repo_root=".")
report = ctx.from_rows(
    [
        {"id": 1, "label": "alpha"},
        {"id": 2, "label": "beta"},
    ]
).write("target/generated-reference.jsonl")

print(report.generated_source_kind)
print(report.generated_source_row_count)
print(report.generated_source_certificate_status)
print(report.output_native_io_certificate_status)
print(report.fallback_attempted)
print(report.external_engine_invoked)
print(report.claim_gate_status)
```

The same `GeneratedRowsSource` can now perform a narrow source-free row transform before the write.
This is intentionally limited to projection plus deterministic literal `with_column` values, then
the transformed rows still pass through ShardLoom's generated-source local-output command:

```python
transformed = (
    ctx.from_rows(
        [
            {"id": 1, "label": "alpha"},
            {"id": 2, "label": "beta"},
        ]
    )
    .with_column("segment", "lit('north')")
    .select("id", "segment")
    .write("target/generated-reference-transformed.jsonl", allow_overwrite=True)
)

print(transformed.generated_source_kind)
print(transformed.generated_source_row_count)
print(transformed.generated_source_certificate_status)
print(transformed.output_native_io_certificate_status)
print(transformed.fallback_attempted)
print(transformed.external_engine_invoked)
print(transformed.claim_gate_status)
```

This slice is not a broad DataFrame runtime. `with_column` accepts only `lit(...)` expressions or
direct Python bool/int/float literals, `select` only projects existing generated-row columns, and
unsupported expressions fail before execution rather than falling back to pandas, Polars, Spark,
DataFusion, DuckDB, or another engine.

Equivalent CLI command:

```powershell
shardloom generated-source-user-rows-smoke target\generated-reference.jsonl id:int64,label:utf8 "id=1,label=alpha;id=2,label=beta" --format json
```

The supported literal-table helper uses the same local generated-source write path while reporting
`generated_source_kind=literal_table`:

```python
literal_report = ctx.literal_table(
    [
        {"code": "A", "weight": 1.5},
        {"code": "B", "weight": 2.0},
    ]
).write("target/generated-literal-table.jsonl", allow_overwrite=True)

print(literal_report.generated_source_kind)
print(literal_report.generated_source_row_count)
print(literal_report.claim_gate_status)
```

The calendar/date-dimension helper generates deterministic local rows in Python, writes JSONL
through the same ShardLoom generated-source command, and reports
`generated_source_kind=calendar`:

```python
calendar_report = ctx.calendar(
    "2026-05-18",
    "2026-05-21",
    column="dt",
).write("target/generated-calendar.jsonl", allow_overwrite=True)

print(calendar_report.generated_source_kind)
print(calendar_report.generated_source_row_count)
print(calendar_report.claim_gate_status)
```

The supported engine-native range smoke is separate. It generates deterministic
`int64` rows inside ShardLoom, writes local JSONL/CSV, and emits the same
generated-source/output/no-fallback evidence family. `limit(...)`, `head(...)`, and `take(...)`
adjust the range bounds before invoking the same engine-native range/sequence smoke; they do not
materialize rows in Python:

```python
range_report = ctx.range(0, 50, column="id").limit(5).write(
    "target/generated-range.jsonl",
    allow_overwrite=True,
)

print(range_report.generated_source_kind)
print(range_report.generated_source_range_start)
print(range_report.generated_source_range_end)
print(range_report.generated_source_range_step)
print(range_report.generated_source_row_count)
print(range_report.claim_gate_status)
```

Equivalent CLI command:

```powershell
shardloom generated-source-range-smoke target\generated-range.jsonl 0 5 --column id --format json
```

The supported engine-native sequence smoke uses the same integer generator contract while reporting
`generated_source_kind=sequence`. It is scoped to local JSONL/CSV output and does not admit broader
DataFrame generation:

```python
sequence_report = ctx.sequence(0, 50, column="id").take(5).write(
    "target/generated-sequence.jsonl",
    allow_overwrite=True,
)

print(sequence_report.generated_source_kind)
print(sequence_report.generated_source_range_start)
print(sequence_report.generated_source_range_end)
print(sequence_report.generated_source_range_step)
print(sequence_report.generated_source_row_count)
print(sequence_report.claim_gate_status)
```

Equivalent CLI command:

```powershell
shardloom generated-source-sequence-smoke target\generated-sequence.jsonl 0 5 --column id --format json
```

The supported source-free SQL smokes parse a deliberately tiny SQL subset inside ShardLoom and write
local JSONL/CSV with generated-source/output/no-fallback evidence. SQL `VALUES` uses generated column
names, literal `SELECT` accepts `AS` aliases, and `SELECT * FROM generate_series/range(...)`
creates an integer source-free table with range evidence:

```python
values_report = ctx.sql_values("VALUES (1, 'alpha'), (2, 'beta')").write(
    "target/generated-sql-values.jsonl",
    allow_overwrite=True,
)
select_report = ctx.sql_literal_select(
    "SELECT 1 AS id, 'alpha' AS label, true AS active"
).write("target/generated-sql-select.jsonl", allow_overwrite=True)
ctx_sql_report = ctx.sql("SELECT 2 AS id, 'beta' AS label").write(
    "target/generated-sql-from-context.jsonl",
    allow_overwrite=True,
)
series_report = ctx.sql("SELECT * FROM generate_series(0, 4)").write(
    "target/generated-sql-series.jsonl",
    allow_overwrite=True,
)
range_topn_report = (
    ctx.range(1, 8, column="id")
    .filter(sl.col("id") >= 3)
    .with_column("doubled", sl.col("id") * 2)
    .sort("doubled", descending=True)
    .limit(2)
    .write("target/generated-range-topn.jsonl", allow_overwrite=True)
)
range_fanout_report = (
    ctx.range(1, 8, column="id")
    .filter(sl.col("id") >= 3)
    .with_column("doubled", sl.col("id") * 2)
    .sort("doubled", descending=True)
    .limit(2)
    .fanout(
        {
            "jsonl": "target/generated-range-topn.jsonl",
            "csv": "target/generated-range-topn.csv",
        },
        allow_overwrite=True,
    )
)

print(values_report.generated_source_kind)
print(values_report.generated_source_row_count)
print(select_report.generated_source_kind)
print(select_report.claim_gate_status)
print(ctx_sql_report.generated_source_kind)
print(series_report.generated_source_kind)
print(series_report.generated_source_range_end_inclusive)
print(range_topn_report.sql_source_free_top_n_runtime_execution)
print(range_topn_report.sql_source_free_sort_keys)
print(range_fanout_report.output_route)
print(range_fanout_report.fanout_output_count)
print(range_fanout_report.fanout_result_reuse_hit)
```

Equivalent CLI command:

```powershell
shardloom generated-source-sql-smoke target\generated-sql-values.jsonl "VALUES (1, 'alpha'), (2, 'beta')" --format json
shardloom generated-source-sql-smoke target\generated-sql-series.jsonl "SELECT * FROM generate_series(0, 4)" --format json
```

This SQL smoke accepts only source-free literal `SELECT` expressions and `VALUES` tuples over int64,
finite float64, bool, and single-quoted UTF-8 string literals, plus `SELECT * FROM
generate_series(start, end[, step])` and `SELECT * FROM range(start, end[, step])` over int64
arguments. `generate_series` uses an inclusive end, while `range` uses the same exclusive-end
semantics as `ctx.range(...)`. The range SQL subset also admits scoped int64 projections,
single-branch int64 `CASE`, one range-column filter, `ORDER BY` over the range source column or
projected int64 aliases, and `LIMIT`, so fluent
`ctx.range(...).filter(...).with_column(...).sort(...).limit(...).write(...)` lowers through the
same generated-source SQL smoke. Source-free top-N reports
`sql_source_free_order_by_runtime_execution`, `sql_source_free_top_n_runtime_execution`,
`sql_source_free_sort_keys`, `sql_source_free_sort_direction`,
`sql_source_free_sort_operator_family`, and `sql_source_free_top_n_limit` alongside projection,
filter, and limit evidence. `ctx.sql(...).write(...)` dispatches those source-free forms to the
same generated-source SQL smoke, and `ctx.sql(...).fanout(...)` dispatches source-free generated
forms to the same generated-source fanout contract. Generated-source fanout reports
`output_route=local_sink_and_fanout`, `result_reuse_for_fanout=true`,
`fanout_result_reuse_hit=true`, per-fanout output formats/paths/digests, workspace path-safety,
certificate, replay, and fidelity fields. Source-free `ctx.sql(...).collect()` remains a
deterministic unsupported diagnostic because the generated-source evidence contract requires an
explicit output sink. The source-free path rejects input datasets, arbitrary `FROM` sources,
unsupported function projections, joins, subqueries, UDFs, object-store paths, table writes, and
broad SQL with deterministic no-fallback errors.

The contract separates three cases:

- `no_dataset_smoke`: status/capability/proof smoke only; no generated rows, no
  source Native I/O certificate, and no output data claim.
- `user_generated_source`: scoped local user rows, literal tables, calendar/date dimensions, and
  generated-row projection/literal `with_column` are supported for JSONL/CSV fixture-smoke writes
  through `ctx.from_rows(...).write(...)`, `ctx.from_rows(...).with_column(...).select(...).write(...)`,
  `ctx.literal_table(...).write(...)`, and `ctx.calendar(...).write(...)`; feature-gated flat scalar
  Parquet/Arrow IPC/Avro/ORC local sinks are available through `write_parquet(...)`,
  `write_arrow_ipc(...)`, `write_avro(...)`, and `write_orc(...)` when the CLI is built with
  `--features universal-format-io`, and feature-gated local Vortex output is available through
  `write_vortex(...)` when the CLI is built with `--features vortex-write`; `.fanout(...)` reuses
  the computed generated rows for primary plus fanout local sinks. Broader generated-source APIs
  remain report-only.
- `engine_native_generated_source`: scoped local `range`, `sequence`, and SQL
  `generate_series`/`range` JSONL/CSV fixture smokes are supported through
  `ctx.range(...).write(...)`, `ctx.range(...).filter(...).with_column(...).sort(...).limit(...).write(...)`,
  `ctx.sequence(...).write(...)`, and `ctx.sql("SELECT * FROM generate_series/range(...)").write(...)`;
  `.fanout(...)` is available for generated range/sequence and source-free SQL, and the same
  feature-gated flat scalar structured and Vortex sinks are available through the generated-source
  write helpers.
  Engine-native `values` and deterministic synthetic profiles remain report-only.

Source-free SQL `VALUES` and literal `SELECT` are runtime-supported as local JSONL/CSV fixture
smokes, plus feature-gated flat scalar Parquet/Arrow IPC/Avro/ORC and Vortex local sinks. Broad SQL
execution and broad DataFrame expression execution are not runtime-supported yet.
Current
source-free API admission rows classify:

- `python_ctx_from_rows`, `python_ctx_literal_table`, `python_ctx_calendar`,
  `python_ctx_range`, `python_ctx_sequence`, and `python_generated_source_write`
  as `fixture_smoke_supported` only for scoped local JSONL/CSV and feature-gated flat scalar
  Parquet/Arrow IPC/Avro/ORC/Vortex generated-output smokes with generated-source and output evidence.
  `GeneratedRowsSource.select(...)` and
  `GeneratedRowsSource.with_column(...)` are scoped Python conveniences over the user-row,
  literal-table, and calendar rows before that same write path.
- SQL literal `SELECT`, SQL `VALUES`, and SQL `generate_series`/`range`
  as `fixture_smoke_supported` only for scoped local JSONL/CSV and feature-gated flat scalar
  Parquet/Arrow IPC/Avro/ORC/Vortex source-free generated-output smokes with generated-source and output
  evidence.
- SQL source-free projection, broad DataFrame source-free projection, and expression-backed
  generated `with_column` forms as `report_only` with deterministic blocker IDs.

Admission capability discovery does not parse SQL, bind names, plan a query,
generate rows, write output, probe object stores, invoke Foundry, or invoke
external engines for report-only rows. Current
no-dataset smoke rows report
`input_dataset_count=0`, `source_io_performed=false`,
`generated_source_created=false`, `output_io_performed=false`, and
`generated_source_certificate_status=not_applicable_no_generated_rows`.
Generated-output runtime must report
`input_dataset_count=0`, `source_io_performed=false`,
`generated_source_created=true`, `generated_source_kind`,
`generated_source_schema_digest`, `generated_source_row_count`,
`generated_source_plan_digest`, optional `generated_source_seed`,
`generation_deterministic`, `output_io_performed`,
`output_native_io_certificate_status`, `generated_source_certificate_status`,
`fallback_attempted=false`, `external_engine_invoked=false`, and
`claim_gate_status`. The current user-row, transformed user-row, literal-table, calendar, range,
sequence, SQL `VALUES`, SQL literal `SELECT`, and SQL `generate_series`/`range` paths report
`claim_gate_status=fixture_smoke_only` in their scoped local JSONL/CSV lanes and feature-gated flat
scalar Parquet/Arrow IPC/Avro/ORC/Vortex lanes. Default binaries return deterministic blockers for
structured sinks until built with `--features universal-format-io`, and for Vortex until built with
`--features vortex-write`. Vortex generated-output reports include
`vortex_output_runtime_execution`, `vortex_output_reopen_verified`, `vortex_artifact_digest`,
`upstream_vortex_write_called`, and `upstream_vortex_scan_called`. S3/object-store writes remain
report-only/gated, and Foundry generated-output smoke must go through Foundry output APIs rather
than direct S3 paths.

The Python context also exposes deterministic unsupported report helpers for the
remaining source-free forms. These helpers do not execute a DataFrame plan, generate rows, write
outputs, probe object stores, invoke Foundry, or call an external engine; they return the same
`workflow-unsupported-plan` envelope with source-free blocker IDs and required evidence:

```python
ctx.dataframe_source_free_projection("lit(1).alias('value')")
ctx.dataframe_generated_with_column("value", "lit(1)")
ctx.generated_output_to_object_store("s3://bucket/out.jsonl")
ctx.foundry_generated_output("foundry://dataset/output")
```

Use these helpers when code wants a typed, no-effect diagnostic instead of a
missing-method failure. They preserve `fallback_attempted=false`,
`external_engine_invoked=false`, `runtime_execution=false`, and
`claim_gate_status=not_claim_grade`.

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

The current package is pure Python and has no runtime dependencies. Release
readiness can be checked locally without publishing:

```powershell
python -m pip install build
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

## Live ETL Smoke

The current live ETL surface is intentionally narrow and explicit.
Compatibility-file mode runs `traditional-analytics-run`, which imports CSV,
JSON/JSONL/NDJSON, Parquet, Arrow IPC, Avro, or ORC inputs into temporary local
Vortex files before running the temporary benchmark operator. Native Vortex mode
runs `traditional-analytics-vortex-run` from existing `.vortex` inputs. The
low-level `traditional_analytics_vortex_run` helper can also pass an explicit
`cdc_delta_vortex` artifact for the scoped prepared/native CDC overlay row; that
does not imply broad table CDC or transaction support.

```python
from shardloom import ShardLoomClient

client = ShardLoomClient.from_repo()
result = client.live_etl_smoke(
    "selective filter",
    "benchmarks/traditional_analytics/data/fact.csv",
    "benchmarks/traditional_analytics/data/dim.csv",
    input_format="csv",
    workspace="target/shardloom-python-live-etl",
    verify_native_replay=True,
    write_result_vortex=True,
)

print(result.status)
print(result.field("rows_scanned"))
print(result.field("materialization_boundary_reported"))
print(result.field("output_replay_verified"))
print(result.field("combined_output_digest"))
print(result.field("output_replay_native_io_certificate_status"))
print(result.field("computed_result_sink_replay_verified"))
print(result.field("computed_result_sink_native_io_certificate_status"))
print(result.field("runtime_task_graph_executed"))
print(result.field("runtime_execution_certificate_status"))
print(result.field("runtime_memory_reservations_released"))
print(result.fallback.attempted)
```

Resource sizing is automatic by default. ShardLoom derives applied parallelism,
batch rows, and target partition count from the local machine and source
footprint. Pass `memory_gb=` or `max_parallelism=` only when a job or benchmark
needs explicit caps.

`verify_native_replay=True` maps to the CLI `--verify-native-replay` flag. It
keeps the smoke workflow local, re-opens the emitted Vortex artifacts, compares
the replay result with the first execution, and returns workload-scoped evidence
fields such as `workload_constitution_id`, `benchmark_row_ref`,
`coverage_row_ref`, Vortex artifact digests, commit/cleanup status, and replay
Native I/O certificate status. It is only valid for compatibility-file inputs;
existing `.vortex` inputs already use the native Vortex smoke command directly.

`write_result_vortex=True` maps to `--write-result-vortex`. It writes the
computed result envelope to `result.vortex`, re-opens that Vortex artifact,
checks the stored result JSON and materialized-row count, and returns result-sink
digest, schema, replay, Native I/O certificate, and write-timing fields. A
workflow is reported as `workload_certified` only when source replay and computed
result-sink replay both pass.

The same response now includes local runtime closeout fields for the certified
workflow: deterministic task-graph scheduler refs, bounded queue/backpressure
status, cancellation and retry gate status, memory reservation/request/grant/
release counts, fail-before-OOM status, operator spill claim blockers, and the
runtime execution certificate status. These fields remain workload-scoped
evidence for `local_vortex_analytics_v1`, not broad SQL/DataFrame runtime
claims.

For the current compatibility-file universal-I/O path, use the replay helper
when you want to see both parts separately: boundary import into Vortex, then
steady-state native Vortex execution from the emitted artifacts.

```python
from shardloom import ShardLoomClient

client = ShardLoomClient.from_repo()
result = client.live_etl_csv_to_vortex_replay(
    "selective filter",
    "benchmarks/traditional_analytics/data/fact.csv",
    "benchmarks/traditional_analytics/data/dim.csv",
    workspace="target/shardloom-python-live-etl",
)

print(result.csv_import.field("fact_vortex_path"))
print(result.native_vortex.field("source_format") if result.native_vortex else None)
print(result.fallback_attempted)
```

For lower-level local Vortex primitive testing, the wrapper exposes a certified
fixture smoke workflow over the same explicit CLI JSON commands used by the
current CG-2/CG-13/CG-16/CG-19 evidence path:

```python
from shardloom import ShardLoomClient

client = ShardLoomClient.from_repo(profile_order=("debug", "release"))
result = client.local_vortex_primitive_smoke(
    "shardloom-vortex/tests/fixtures/local_primitive_struct_five.vortex",
)

print(result.commands)
print(result.all_certified)
print(result.filter_project.field("filter_project_local_execution_rows_projected"))
print(result.fallback_attempted)
```

Count-all uses the explicit `vortex-run <fixture> count <memory_gb>
<max_parallelism>` runtime command. Count-where, filter, project, and
filter-project use their `--execute-local-primitive <memory_gb>
<max_parallelism>` flags. Calls without those explicit execution paths use the
existing metadata/plan evidence surfaces where the CLI supports them; local
primitive execution also requires explicit resource caps.

The repository smoke script prints command, status, certificate, Native I/O,
materialization, work-metric, evidence-artifact, and no-fallback fields:

```powershell
$env:RUSTUP_TOOLCHAIN = "1.91.1"
cargo build -p shardloom-cli --features vortex-local-primitives --bin shardloom

$env:PYTHONPATH = "python\src"
python python\examples\local_vortex_primitives_smoke.py --repo-root .
```

The compatibility-source planning smoke shows the adjacent report-only boundary
for CSV, JSON/JSONL/NDJSON, Parquet, and Arrow IPC inputs before any execution claim.
It plans representative local paths without checking that the files exist,
reading data, writing data, or materializing rows:

```powershell
$env:PYTHONPATH = "python\src"
python python\examples\compatibility_source_smoke.py --repo-root .
```

Override planned sources when you want to inspect your own paths:

```powershell
python python\examples\compatibility_source_smoke.py --repo-root . `
  --source csv=data\fact.csv `
  --source ndjson=data\events.ndjson `
  --source parquet=data\fact.parquet
```

The workflow-readiness smoke pulls together the next no-write boundary: output
target preview, compatibility-output translation planning, staged Vortex
write/commit readiness, table/catalog/object-store/remote-source planning, and
migration/correctness/benchmark evidence status.

```python
from shardloom import ShardLoomClient

client = ShardLoomClient.from_repo(profile_order=("debug", "release"))
readiness = client.workflow_readiness_smoke()

print(readiness.plan_names)
print(readiness.all_no_write)
print(readiness.all_report_only_or_planned)
print(readiness.blocked_plan_names)
print(readiness.fallback_attempted)
```

The matching script prints the same surfaces grouped by output/commit,
table/remote, and evidence readiness:

```powershell
$env:PYTHONPATH = "python\src"
python python\examples\workflow_readiness_smoke.py --repo-root .
```

This smoke does not create the staged workspace, write manifests, write Vortex
payloads, open object-store credentials, read remote objects, query catalogs,
materialize rows, or invoke fallback engines. Actual write and commit commands
remain separate explicit CLI calls gated by their readiness signals and feature
flags.

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
$env:RUSTUP_TOOLCHAIN = "1.91.1"
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
print(adapters.field("parquet_status"))

plan = client.input_plan("file://tmp/example.parquet")
print(plan.field("source_kind"))
print(plan.field("capability_status"))
print(plan.field("plan_only"))
```

Common structured inputs are tracked as `native_vortex`, `parquet`,
`arrow_ipc`, `csv`, JSON/NDJSON through `jsonl`, `avro`, and `orc`.
Lakehouse/table, object-store, catalog, effectful, and unstructured/media
families are also represented in the registry. The current implemented live
path is the feature-gated local compatibility-file-to-Vortex benchmark smoke
and native `.vortex` replay; production adapter certification, object-store
runtime, catalogs, SQL, DataFrame runtime, and UDF runtime remain future work.

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
evidence. Real S3/GCS/ADLS URIs, credentials, network probes, writes, commits,
lakehouse runtime, and production object-store claims remain blocked.

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
object-store coupling as separate gates. Local manifest metadata and
delete/tombstone smokes are related evidence only; they are not table-format
runtime, lakehouse runtime, catalog runtime, object-store runtime, or commit
support.
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
JDBC/ODBC, Snowflake, BigQuery, and Databricks SQL separate from ShardLoom
runtime execution. Current rows do not load drivers, resolve credentials, probe
networks, import/export data, push queries down, or use external databases and
warehouses as fallback engines. Important row IDs include `sqlite_file`,
`postgres`, `mysql`, `jdbc_odbc`, `snowflake`, `bigquery`, and
`databricks_sql`.

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
python python\examples\live_etl_smoke.py `
  --mode csv `
  --scenario "selective filter" `
  --fact benchmarks\traditional_analytics\data\fact.csv `
  --dim benchmarks\traditional_analytics\data\dim.csv `
  --workspace target\shardloom-python-live-etl
```

## Test

```powershell
$env:PYTHONPATH = "python\src"
python -m unittest discover python\tests
```
