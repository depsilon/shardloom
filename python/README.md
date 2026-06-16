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
local Python surfaces, the v0.1.3 source track, and the approved v0.1.1 package track, but it does
not authorize production support, performance claims, Spark displacement, or hidden external
execution.

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

The source package exposes the v0.1.3 technical-preview version through
`shardloom.__version__`; the selected published package channels remain proof-backed at v0.1.1
until v0.1.3 publication proofs are recorded.
The PyPI package is published as `shardloom==0.1.1`; GitHub release assets and the
`depsilon/tap/shardloom` Homebrew formula are also published for v0.1.1. These channels are install
access only and do not imply production readiness, broad runtime support, or performance claims.

```sh
python -m pip install shardloom==0.1.1
```

Published v0.1.1 wheels require a CLI binary from Homebrew, source checkout, `PATH`,
`SHARDLOOM_BIN`, or `SHARDLOOM_REPO_ROOT`. The v0.1.3 source track resolves the CLI from installed
package resources before falling back to `PATH` for supported platform wheels; explicit
binary/env/source configuration still wins. Use `SHARDLOOM_BIN` only when you want to pin a
specific CLI binary:

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
for compatibility, tests, and schema-pinned examples. ShardLoom owns SourceState, preparation,
execution, OutputPlan, replay, reuse, certificate, and no-fallback evidence behind the query
surface:

```python
import shardloom as sl

ctx = sl.context()
result = (
    ctx.read("target/orders.csv")
    .filter(sl.col("amount") >= 10)
    .select("id", "amount")
    .limit(100)
    .write_jsonl("target/orders-out.jsonl", allow_overwrite=True)
)

print(result.output_row_count)
print(result.first_result_row)
print(result.evidence_summary.output_path)
print(result.claim_summary.claim_gate_status)
print(result.fallback_attempted, result.external_engine_invoked)
```

The same query shape can read admitted local formats through `ctx.read(...)` or the explicit
format helpers and write to `write(...)`, `write_jsonl(...)`, `write_csv(...)`, or feature-gated
structured sinks. CSV, flat JSON/JSONL/NDJSON, generated rows, and scoped local Vortex inputs are
the default public examples. Parquet, Arrow IPC/Feather, Avro, and ORC are admitted scoped
local-format surfaces when the matching feature-gated build is present; builds without those
readers return deterministic adapter blockers instead of invoking another engine. Format-specific
behavior belongs at read/ingest and write/sink boundaries only; compute semantics should lower
through the shared ShardLoom SQL/Python runtime or return a deterministic unsupported report.
Agents and automation should use `docs/reference/shardloom-user-surface-index.md` and
`docs/reference/shardloom-user-surface-index.json` as the canonical map of Python, SQL, CLI,
generated-source, materialization, and deterministic blocker surfaces.
The canonical local output/sink scope is `docs/architecture/v1-local-output-sink-scope.md`; inspect
it with `ctx.local_output_sink_scope_report()` before treating a write helper as broader than its
scoped local evidence.

Bounded materialization is explicit. Local-source workflows can carry a `limit(...)` or pass
`collect(limit=...)`; SQL workflows can also pass `collect(limit=...)` or chain
`.limit(...).collect()`. Those bounded routes can decode ShardLoom's inline JSONL result into
Python objects, a notebook preview, or optional pandas/Arrow/NumPy containers. Those optional
packages are not execution engines:

```python
preview = (
    ctx.read("target/orders.csv")
    .select("id", "amount")
    .limit(20)
    .display()
)
print(preview.row_count)
print(preview.to_python_objects())

df = (
    ctx.read("target/orders.csv")
    .select("id", "amount")
    .limit(20)
    .to_pandas()
)
```

Install the optional conversion containers with `shardloom[materialization]` when pandas, PyArrow,
or NumPy objects are needed. Without the optional package, the corresponding conversion returns a
deterministic unsupported report. Unbounded materialization conveniences still expose blockers and
no-fallback evidence rather than silently invoking another Python or query engine:

```python
blocked = ctx.read("target/orders.csv").select("id").to_pandas()
print(blocked.blocker_id)
print(blocked.required_evidence)
print(blocked.fallback_attempted, blocked.external_engine_invoked)
```

For workflows that need caller-scoped reuse evidence, `ctx.session(...)` and `sl.session(...)` expose
the same local read/SQL shapes as session-bound workflows:

```python
with ctx.session(session_id="orders-run") as sess:
    result = (
        sess.read_csv("target/orders.csv")
        .select("id", "amount")
        .limit(100)
        .write_jsonl("target/orders-out.jsonl", allow_overwrite=True)
    )
    repeat = sess.sql("SELECT id FROM 'target/orders.csv' LIMIT 100").collect()
    print(result.reuse_hit, repeat.source_state_reuse_hit)
```

The session is explicit, in-process, caller-owned, and closeable. It can reuse admitted local
`vortex_ingest` prepared-state reports plus admitted local `collect`/`write`/`fanout` reports when
source, prepared artifact, and output artifact fingerprints still match. `ctx.prepare_vortex(...)`,
`ShardLoomClient.vortex_ingest_smoke(...)`, and raw runtime-envelope inspection remain lower-level
diagnostic surfaces. Session reuse is not a daemon, remote server, hidden global cache,
object-store/table cache, broad DataFrame/SQL runtime, or performance claim.

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
`ShardLoomClient.vortex_ingest_smoke(...)`, or `ctx.prepare_vortex(...)` when you intentionally need
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
$env:SHARDLOOM_REPO_ROOT = "."
python -c "from shardloom import context; ctx=context(); r=ctx.read_csv('target/vortex-ingest-source.csv').prepare_vortex(workspace='target/shardloom-prepared', allow_overwrite=True); print(r.vortex_ingest_status, r.prepared_state_created, r.prepared_state_reuse_hit, r.prepared_state_reuse_reason, r.fallback_attempted, r.external_engine_invoked)"
```

Default CLI builds return a deterministic feature-gate blocker instead of writing an artifact. This
path is a local fixture smoke; it is not the primary user API, broad Vortex writer support,
object-store/table output support, production SQL/DataFrame support, or a performance claim.
`LazyFrame.prepare_vortex(...)` is the higher-level local `auto` source front door: it derives
`<workspace>/<source-stem>.vortex` when a workspace is supplied, calls the real Rust
`vortex-ingest-smoke` route, and exposes `prepared_state_reuse_hit`,
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
for scoped count/filter/project/limit and exact benchmark-family grouped aggregate, hash join,
global top-N, cast/try-cast, substring contains, and native `write_vortex` sink shapes. Manual
`vortex_primitive` and `native_vortex_provider_scenario` arguments remain available on
`ctx.client.public_workflow_*` for low-level diagnostics, but normal Python/SQL facades route the
admitted shapes without requiring those flags.

Unbounded collect requests block at route admission and keep
`runtime_execution=false`, `fallback_attempted=false`, and `external_engine_invoked=false` in the
envelope. The equivalent CLI surfaces are
`shardloom route <sql|python|dataframe|cli> --format json`,
`shardloom run <sql|python|dataframe|cli> --format json`, and
`shardloom prepare <sql|python|dataframe|cli> --format json`.
Lazy DataFrame bounded `collect()`, general `write(...)`, `write_jsonl(...)`, `write_csv(...)`,
structured write aliases, generated-source direct writes, source-free SQL writes, and admitted
local/generated fanout helpers route through the same public `run` facade and return existing typed
reports with attached `public_workflow_*` route fields. Native Vortex primitive and promoted
provider helpers now attach the inferred route payloads to the same facade rather than relying on a
separate payload-only path.

Traditional analytics compatibility inputs can also use the explicit context/session prepared route
or the lower-level client helpers. `ctx.prepare_vortex(..., workspace=...)` and
`session.prepare_vortex(..., workspace=...)` return a route handle for
`compatibility_import_certified -> prepared_vortex`; `query(...).collect()` runs a single prepared
query and `run_batch([...])` runs a prepared scenario batch. The first compatible call invokes
`traditional-analytics-prepare-batch-run`, prepares the local fact/dimension inputs once into
prepared Vortex artifacts, and writes a caller-owned workspace manifest. Later compatible calls
reuse that manifest and run `traditional-analytics-vortex-batch-run` directly over the existing
artifacts when source, artifact, and prepare-policy fingerprints match. The returned envelope keeps
`prepare_batch_*`, source-state reuse, fallback, claim-boundary, `prepared_state_reuse_hit`,
`prepared_state_reuse_reason`, `prepared_state_reuse_manifest_digest`, and `invalidation_reason`
fields visible:

```python
import shardloom as sl

ctx = sl.context()
prepared = ctx.prepare_vortex(
    "fact.csv",
    dim="dim.csv",
    workspace="target/prepare-batch",
    input_format="csv",
    evidence_level="certified",
)
result = prepared.run_batch(["selective filter", "filter + projection + limit"])

print(prepared.route_fields())
print(result.batch.field("prepare_batch_preparation_included_in_batch_timing"))
print(result.batch.field("source_state_reuse_status"))
print(result.batch.field("prepare_batch_lifecycle_status"))
print(result.prepared_state_reuse_hit, result.prepared_state_reuse_reason)
print(result.batch.field("scenario_selective-filter_prepared_native_vortex_lifecycle_status"))
print(result.fallback_attempted, result.external_engine_invoked)
```

This is a scoped local runtime route for avoiding repeated compatibility preparation inside a batch.
`PreparedVortexBatchResult.lifecycle_status`,
`PreparedVortexBatchResult.lifecycle_output_status`, and
`PreparedVortexBatchResult.lifecycle_no_standalone_lane` expose the combined route lifecycle
posture. `ExecutionResultEnvelopeView.prepared_native_vortex_lifecycle_status` and related output/
no-standalone accessors expose the per-scenario lifecycle fields when a typed execution result
contains them.
Use `ShardLoomClient.traditional_analytics_prepare_batch_run(...)`,
`prepare_and_run_traditional_analytics_vortex_batch(...)`, or
`prepare_traditional_analytics_vortex_artifacts(...)` only when the caller needs lower-level CLI
control or explicit artifact lifecycle management across later commands. This is not a native Python binding,
persistent cache, object-store/table runtime, package-readiness claim, or performance claim.

For existing native `.vortex` fact/dimension artifacts, use the route-level native handle when you
want the same benchmark-family runtime path rather than isolated primitive helpers:

```python
native = ctx.native_vortex_route(
    "fact.vortex",
    "dim.vortex",
    execution_mode="native_vortex",
    memory_gb=4,
    max_parallelism=1,
)

result = native.query("selective filter").collect()
sink = native.query("selective filter").write_vortex("target/native-result")

print(native.route_fields())
print(result.field("selected_execution_mode"))
print(result.fallback.attempted)
```

`read_vortex(...).count/filter/select/limit/collect`, admitted grouped aggregate/join/top-N/cast/
contains chains, and native `write_vortex` sinks route through the public native Vortex facade when
their shape has a certificate-backed provider route. `native_vortex_route(...)` remains the
explicit route-comparable surface for `traditional-analytics-vortex-run` /
`traditional-analytics-vortex-batch-run`; it keeps source, execution mode, scenario/operator,
memory/parallelism hints, result sink, and no-fallback evidence visible.

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
local path with `SELECT *`. `ctx.sql("SELECT ... FROM 'local.csv' ...")` exposes
the same bounded schema, validation, data-quality, preview, head, and take
helpers over admitted local-source SQL statements. The same
projection/optional-filter/limit shape is admitted for `read_json(...)` when the
source path is a local flat `.json`, `.jsonl`, or `.ndjson` file; nested JSON
expansion and JSONPath remain deterministic unsupported surfaces. The same shape is admitted for
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
cast, date-literal, scoped UTC-or-fixed-offset `TIMESTAMP 'YYYY-MM-DDTHH:MM:SS(.ffffff)(Z|+HH:MM|-HH:MM)'` literals,
Date32 extract predicates with `DATE_YEAR(...)` / `DATE_MONTH(...)` /
`DATE_DAY(...)`, UTC-or-fixed-offset timestamp extract predicates with
`TIMESTAMP_YEAR(...)` / `TIMESTAMP_MONTH(...)` / `TIMESTAMP_DAY(...)` /
`TIMESTAMP_HOUR(...)` / `TIMESTAMP_MINUTE(...)` / `TIMESTAMP_SECOND(...)`,
Date32 day arithmetic with `DATE_ADD_DAYS(...)` / `DATE_SUB_DAYS(...)`,
scoped temporal-difference expressions with `DATE_DIFF_DAYS(...)` and
`TIMESTAMP_DIFF_SECONDS(...)` compared against numeric literals,
scoped numeric arithmetic predicates such as `<column> + 5 >= 20` and
`<column> * 2.0 > 1.0`,
bounded `IN (...)` / `NOT IN (...)`, scoped literal row-value
`(<column>,...) IN ((...),...)` / `NOT IN` predicates, scoped local
`IN (SELECT <column> FROM '<local-source>')` / `NOT IN (...)` scalar subquery predicates,
scoped row-value `IN (SELECT <column>,...)` / `NOT IN (...)` subquery predicates, direct SQL
`EXISTS (SELECT <projection> FROM '<local-source>' ...)` / `NOT EXISTS (...)` subquery
predicates, direct SQL scoped quantified `ANY` / `ALL (SELECT <column> FROM '<local-source>' ...)`
subquery predicates, direct SQL `BETWEEN` / `NOT BETWEEN`, inclusive Python `between(...)` range predicates, UTF-8
`LENGTH(column)` comparisons against integer literals, string `LIKE` / `NOT LIKE`, null, logical
`AND`/`OR`/`NOT`, and balanced grouping parentheses over already admitted leaves. `where(...)` is a
familiar alias for `filter(...)`. `IN` lists admit up to 32 literal values from one scalar family,
including `DATE 'YYYY-MM-DD'` lists and `NULL` literals with SQL three-valued `WHERE`-filter
semantics. Row-value literal predicates admit up to 32 literal tuples with arity/type checks and
SQL three-valued row comparison semantics. Scoped local `IN`/`NOT IN` subqueries materialize a
bounded scalar column or row-value tuple set from another admitted local source. Scoped local
`EXISTS`/`NOT EXISTS` subqueries evaluate a two-valued bounded presence test over another admitted
local source. Scoped local
quantified `ANY` / `ALL` subqueries materialize a bounded scalar set from another admitted local
source and apply SQL three-valued comparison semantics. Source-qualified scalar IN/NOT IN,
row-value IN/NOT IN, EXISTS, NOT EXISTS, and quantified local subquery references are admitted when they bind to an
explicit subquery `AS <alias>` or SQL-identifier file stem; Python helpers can set that binding with
`source_alias=` and render the qualified column with `sl.col("alias.column")`. Scoped correlated `outer.<column>`
subquery filters are admitted for scalar `IN`/`NOT IN`, row-value `IN`/`NOT IN`,
`EXISTS`/`NOT EXISTS`, and quantified `ANY` / `ALL` predicates through the reserved outer-row alias.
Direct SQL predicate projections and CASE
predicates can now reuse admitted scalar `IN` subqueries, including scoped correlated
`outer.<column>` filters, over bounded local sources. Scalar-left multi-column,
unbound qualified, broad projected correlated joins/aggregates, and broader arbitrary subquery
shapes remain deterministic blockers.
Typed reports expose `in_predicate_runtime_execution`,
`in_list_value_count`, `in_list_null_value_count`, `row_value_in_predicate_runtime_execution`,
`row_value_in_source_columns`, `row_value_in_tuple_count`, `row_value_in_null_semantics`,
`in_predicate_null_semantics`,
`in_subquery_runtime_execution`, `in_subquery_source_columns`, `in_subquery_source_formats`,
`in_subquery_materialized_value_count`, `in_subquery_materialized_null_value_count`,
`exists_subquery_runtime_execution`, `exists_subquery_projection_kind`,
`exists_subquery_source_formats`, `exists_subquery_bounded_row_count`, and
`exists_subquery_result`, `quantified_subquery_runtime_execution`,
`quantified_subquery_quantifiers`, `quantified_subquery_source_columns`,
`quantified_subquery_materialized_value_count`, `quantified_subquery_null_semantics`,
`source_qualified_subquery_runtime_execution`, `source_qualified_subquery_source_qualifiers`,
`source_qualified_subquery_operator_families`, `source_qualified_subquery_source_columns`,
`correlated_subquery_runtime_execution`, `correlated_subquery_outer_aliases`,
`correlated_subquery_outer_columns`, `correlated_subquery_evaluation_strategy`, and
`correlated_subquery_outer_row_evaluation_count`, plus
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
`not_like(...)`, `between(...)`, bounded `isin(...)` / `not_in(...)`, `sl.row_in(...)` /
`sl.row_not_in(...)`, local source-backed
`isin_source(source, column)` / `not_in_source(source, column)`, row-value source-backed
`sl.row_in_source(columns, source, source_columns)` /
`sl.row_not_in_source(columns, source, source_columns)`, source-backed
`sl.exists_source(source, where=..., group_by=..., having=..., order_by=..., limit=...)` /
`sl.not_exists_source(source, ...)`, source-backed `sl.any_source(...)` / `sl.all_source(...)`
and `sl.col(...).any_source(...)` / `.all_source(...)` with the same grouped/HAVING tail
parameters, `sl.outer(column)` for the reserved
correlated outer-row alias, `cast(dtype)`,
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
`sl.col("amount").cast("float64")` / `.cast("date32")` / `.cast("timestamp_micros")` /
`.cast("decimal128(10,2)")` / `.try_cast("numeric(10,2)")` / `.cast("binary")`
projections, and scoped Date32/UTC-or-fixed-offset timestamp extract projections such as
`sl.col("event_date").cast("date32").date_year()` or
`sl.col("event_ts").cast("timestamp_micros").timestamp_hour()`, plus scoped Date32 day arithmetic
projections such as `sl.col("event_date").cast("date32").date_add_days(7)` or
`.date_sub_days(1)`, scoped UTC-or-fixed-offset timestamp second arithmetic projections such as
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
`sl.case_when(sl.col("amount") >= 10, "large", "small")`, scoped binary cast projections such as
`sl.col("payload").cast("binary")` / `.cast("blob")` / `.cast("varbinary")`, and scoped binary
helper projections such as `sl.col("hex_payload").unhex()` / `sl.unhex(sl.col("hex_payload"))` and
`sl.col("b64_payload").from_base64()` / `sl.from_base64(sl.col("b64_payload"))` over direct UTF-8
source columns, plus scoped complex projection helpers such as
`sl.array(1, 2, None)` and `sl.struct("label", "amount")` through the JSONL/result boundary only.
Literal projections emit
`literal_projection_*` evidence; cast projections emit `cast_projection_*` evidence; numeric
arithmetic projections emit `numeric_arithmetic_projection_*` evidence; numeric absolute-value
projections emit `numeric_abs_projection_*` evidence; numeric rounding projections emit
`numeric_rounding_projection_*` evidence; generalized numeric expression-tree and
temporal-difference projections emit `generic_expression_projection_*` evidence; generalized
numeric expression-tree and temporal-difference predicates emit `generic_expression_predicate_*` evidence; string transform
projections emit `string_transform_projection_*` evidence; string length projections emit
`string_length_projection_*` evidence; date/time extract projections emit
`date_extract_projection_*` and `timestamp_extract_projection_*` evidence; date arithmetic
projections emit `date_arithmetic_projection_*` evidence; UTC-or-fixed-offset timestamp arithmetic predicates and
projections emit `timestamp_arithmetic_*` and `timestamp_arithmetic_projection_*` evidence; null coalesce projections emit
`null_coalesce_projection_*` evidence; nullif projections emit `nullif_projection_*` evidence;
conditional projections emit
`conditional_projection_*` evidence; decimal cast projections/predicates emit `decimal_cast_*`
precision, scale, mode, and exact-output-boundary evidence while preserving generic
`cast_projection_*` / `cast_*` fields; binary cast projections emit `cast_projection_*` evidence with
`binary` target dtypes; binary helper projections emit
`binary_helper_projection_*` evidence; scoped complex projections emit
`complex_projection_*` evidence. Scoped decimal cast-plus-arithmetic projections use the generic
expression projection surface and emit `generic_expression_projection_*` evidence with exact
decimal string result rows. Sorting after an input-backed computed projection is admitted
for bounded top-N workflows when the sort key resolves to a projected computed alias or a source
column; those workflows emit `computed_projection_top_n_runtime_execution=true`,
`computed_projection_operator_family=computed_projection_topn`, and the ordinary `sort_*` and
`top_n_*` evidence fields. Mixed `int64`/`float64` arithmetic promotes to `float64`
only when the `int64` operand is exactly representable as `float64`; lossy mixed coercions,
generic expression missing-source-column and division-by-zero cases, `COALESCE(..., NULL)`,
`NULLIF(..., NULL)`, non-null source/fallback dtype mismatches, and non-null source/sentinel dtype
mismatches block deterministically before fallback. `CASE WHEN` projections currently admit one
branch, admitted predicate leaves, non-NULL literal branches, and matching branch dtypes only.
Binary helper projections admit direct source columns only, with strict even-length hexadecimal or
standard padded base64 decoding, null propagation, and deterministic invalid-input blockers.
Scoped SQL `BINARY '<utf8>'` / `BLOB '<utf8>'` byte literal projections and scoped binary cast
equality/inequality and bytewise lexicographic ordering predicates are admitted through the SQL
local-source runtime; broad binary source dtype decoding, SQL source-column binary ordering without
explicit cast, and nested binary helper expressions still block before fallback. Scoped decimal casts
are admitted for fixed-scale projection and predicate fixtures with
exact JSONL string and CSV text output; scoped `decimal128` add/subtract/multiply projections over
same-scale and mixed-scale decimal operands plus integer operands are admitted through the generic
expression route, and exact division emits a bounded `decimal128(38,max(input_scales,6))` result
when the quotient is exact at that scale. Feature-gated Parquet/Arrow IPC/Avro compatibility sinks
and local Vortex output preserve scoped decimal columns as typed `decimal128(p,s)`. ORC typed
decimal sinks still block before fallback because the pinned ORC writer provider does not preserve
decimal128 columns; non-exact decimal division and broad ANSI decimal coercion also remain
deterministic blockers.
Scoped SQL `ARRAY[...]` and `STRUCT(<source column>, ...)` projections are admitted for
bounded local-source JSONL/result rows; complex equality, DISTINCT, subquery membership, accessors,
casts, nested source decoding, and flat compatibility sinks still block before fallback.
Unsupported computed-column expressions still block before fallback.
For familiar Python/DataFrame call sites, `.project(...)` is an alias for `.select(...)`,
`.with_columns(...)` and `.assign(...)` are aliases over repeated admitted `with_column(...)`
projections, `.groupby(...)` is an alias for `.group_by(...)`, and `.order_by(...)`,
`.sort_by(...)`, and `.sort_values(...)` are aliases for `.sort(...)`. Row-level duplicate removal
is admitted for bounded local-source projection, aggregate/HAVING, join, and window output rows
through SQL `SELECT DISTINCT` and Python/DataFrame `.distinct()`, `.drop_duplicates()`, and
`.unique()` aliases; LIMIT is applied after duplicate removal and reports `distinct_projection_*`
evidence. Scoped local-source `UNION` / `UNION ALL` / `INTERSECT` / `EXCEPT` is admitted over
already-admitted branch `SELECT` plans through raw SQL or Python/DataFrame `.union(...)`,
`.union_all(...)`, `.intersect(...)`, `.except_(...)`, `.except_rows(...)`, and `.subtract(...)`;
branch output columns and non-null dtypes must match, branch-local `ORDER BY` / `LIMIT` remains
blocked, and the result emits `sql_set_operation_*` no-fallback evidence plus `sql_union_*`
compatibility aliases for the original union route fields. These aliases do not widen the expression
registry or execution providers; they lower to the same scoped ShardLoom runtime routes and evidence
fields as the canonical methods.
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
keys. Scoped local-source ranking windows are admitted through
`.window(sl.row_number(...), sl.rank(...), sl.dense_rank(...))`, lowering to
`ROW_NUMBER()`, `RANK()`, and `DENSE_RANK()` `OVER (...) AS <alias>` projections with
deterministic partition/order evidence and `window_*` typed report fields. Local-source joins also
admit scalar and grouped aggregates, including scalar top-N
ordering over aggregate output aliases and group keys, when the workflow keeps the
same explicit aliases, qualified join-side columns, optional pre-aggregate filter, and bounded
`limit(...)`; joined aggregate rows can use the same aggregate-output `HAVING` filter before
ordering/limit. `collect()` returns bounded inline JSONL through the public workflow `run` facade;
`write()` writes a local JSONL/CSV file through that same facade
by default, and local-source workflows can use `write(..., output_format="csv")`
or `write_csv(...)` for the scoped local CSV sink. They can also use
`write_parquet(...)` or `write(..., output_format="parquet")` for the scoped
feature-gated flat scalar Parquet sink when the CLI is built with
`--features universal-format-io`; default binaries return ShardLoom's
deterministic Parquet sink blocker. The structured write aliases attach route metadata to the
returned sink report.
`write_vortex(...)` writes a scoped local flat scalar `.vortex` result when the CLI is built with
`--features vortex-write`; default binaries return a deterministic Vortex sink blocker. The scoped
`.fanout(...)` helper can reuse one computed result for multiple admitted local
compatibility sinks such as JSONL and CSV, feature-gated flat scalar
Parquet/Arrow IPC/Avro/ORC when the CLI is built with `--features universal-format-io`,
and feature-gated local Vortex when built with `--features vortex-write`; fanout now passes an
explicit primary output plus `--fanout-output` payload through the public workflow `run` facade.
Written local sinks emit
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
id,active,score
1,true,30
3,true,20
NULL,false,10
"@ | Set-Content -Encoding utf8 target\sql-local-source-allowed.csv
@'
{"id":1,"label":"alpha","amount":8}
{"id":2,"label":"beta","amount":15}
{"id":3,"label":"gamma","amount":21}
'@ | Set-Content -Encoding utf8 target\sql-local-source-smoke.jsonl
$env:PYTHONPATH = "python\src"
@'
import shardloom as sl

ctx = sl.context()
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
ranked = (
    ctx.read_csv("target/sql-local-source-smoke.csv")
    .select("id", "label", "amount")
    .filter(sl.col("amount") >= 10)
    .window(sl.row_number(order_by="amount", descending=True, alias="rn"))
    .limit(10)
    .collect()
)
ranked_with_ties = (
    ctx.read_csv("target/sql-local-source-smoke.csv")
    .select("id", "label", "amount")
    .window(
        sl.rank(order_by="amount", descending=True, alias="rank"),
        sl.dense_rank(order_by="amount", descending=True, alias="dense_rank"),
    )
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
row_value_filtered = (
    ctx.read_csv("target/sql-local-source-smoke.csv")
    .select("id", "label")
    .filter(sl.row_in(["id", "label"], [(1, "alpha"), (3, "gamma"), (5, None)]))
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
correlated_source_subquery_filtered = (
    ctx.read_csv("target/sql-local-source-smoke.csv")
    .select("id", "label")
    .filter(
        sl.col("id").isin_source(
            allowed_ids,
            "allowed.id",
            source_alias="allowed",
            where=sl.col("allowed.id") == sl.outer("id"),
        )
    )
    .limit(10)
    .collect()
)
grouped_source_subquery_filtered = (
    ctx.read_csv("target/sql-local-source-smoke.csv")
    .select("id", "label")
    .filter(
        sl.col("id").isin_source(
            ctx.read_csv("target/sql-local-source-grouped.csv"),
            "id",
            group_by="id",
            having="count(*) >= 2 AND id = outer.id",
            order_by="id",
            limit=10,
        )
    )
    .limit(10)
    .collect()
)
allowed_pairs = ctx.read_csv("target/sql-local-source-allowed-pairs.csv")
row_source_subquery_filtered = (
    ctx.read_csv("target/sql-local-source-smoke.csv")
    .select("id", "label")
    .filter(sl.row_in_source(["id", "label"], allowed_pairs, ["allowed_id", "allowed_label"]))
    .limit(10)
    .collect()
)
active_allowed = ctx.read_csv("target/sql-local-source-allowed.csv")
exists_subquery_filtered = (
    ctx.read_csv("target/sql-local-source-smoke.csv")
    .select("id", "label")
    .filter(
        sl.exists_source(
            active_allowed,
            where=sl.col("active").is_true(),
            order_by="score",
            descending=True,
            limit=1,
        )
    )
    .limit(10)
    .collect()
)
thresholds = ctx.read_csv("target/sql-local-source-thresholds.csv")
quantified_subquery_filtered = (
    ctx.read_csv("target/sql-local-source-smoke.csv")
    .select("id", "label")
    .filter(
        sl.col("amount").all_source(
            ">",
            thresholds,
            "threshold",
            where=sl.col("active").is_true(),
            order_by="score",
            descending=True,
            limit=2,
        )
    )
    .limit(10)
    .collect()
)
unioned = (
    ctx.read_csv("target/sql-local-source-smoke.csv")
    .select("id")
    .union_all(ctx.read_csv("target/sql-local-source-allowed.csv").select("id"))
    .collect(limit=10)
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
schema_contract = workflow.schema_contract({"id": "int64", "label": "string"})
quality_report = workflow.data_quality_check("not_null:id", "unique:id")
profile_report = workflow.profile()
quarantine_report = workflow.quarantine(
    "target/sql-local-source-quarantine.jsonl",
    "not_null:label",
    output_format="jsonl",
    allow_overwrite=True,
)
sql_workflow = ctx.sql("SELECT id,label,amount FROM 'target/sql-local-source-smoke.csv'")
sql_schema = sql_workflow.schema()
sql_contract = sql_workflow.schema_contract({"id": "int64", "label": "string"})
sql_quality = sql_workflow.data_quality_summary()
sql_profile = sql_workflow.profile(limit=100)
sql_preview = sql_workflow.preview(limit=2)
sql_rows = sql_workflow.collect(limit=2)
sql_rows_from_chain = sql_workflow.limit(2).collect()
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
print(sql_schema.schema_map)
print(sql_quality.null_counts)
print(sql_preview.result_rows)
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
    row_value_filtered.row_value_in_predicate_runtime_execution,
    row_value_filtered.row_value_in_source_columns,
    row_value_filtered.row_value_in_tuple_count,
    row_value_filtered.row_value_in_null_semantics,
)
print(
    source_subquery_filtered.in_subquery_runtime_execution,
    source_subquery_filtered.in_subquery_source_columns,
    source_subquery_filtered.in_subquery_materialized_value_count,
    source_subquery_filtered.in_subquery_materialized_null_value_count,
)
print(
    correlated_source_subquery_filtered.correlated_subquery_runtime_execution,
    correlated_source_subquery_filtered.correlated_subquery_outer_columns,
    correlated_source_subquery_filtered.correlated_subquery_evaluation_strategy,
)
print(
    exists_subquery_filtered.exists_subquery_runtime_execution,
    exists_subquery_filtered.exists_subquery_projection_kind,
    exists_subquery_filtered.exists_subquery_bounded_row_count,
    exists_subquery_filtered.exists_subquery_result,
)
print(
    quantified_subquery_filtered.quantified_subquery_runtime_execution,
    quantified_subquery_filtered.quantified_subquery_quantifiers,
    quantified_subquery_filtered.quantified_subquery_materialized_value_count,
    quantified_subquery_filtered.quantified_subquery_null_semantics,
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

sql_row_value_rows = ctx.sql(
    "SELECT id,label FROM 'target/sql-local-source-smoke.csv' "
    "WHERE (id,label) IN ((1,'alpha'),(3,'gamma'),(5,NULL)) LIMIT 10"
).collect()

sql_in_subquery_rows = ctx.sql(
    "SELECT id,label FROM 'target/sql-local-source-smoke.csv' "
    "WHERE id IN (SELECT id FROM 'target/sql-local-source-allowed.csv') LIMIT 10"
).collect()

sql_exists_rows = ctx.sql(
    "SELECT id,label FROM 'target/sql-local-source-smoke.csv' "
    "WHERE EXISTS (SELECT * FROM 'target/sql-local-source-allowed.csv' "
    "WHERE active IS TRUE ORDER BY score DESC LIMIT 1) LIMIT 10"
).collect()

sql_quantified_rows = ctx.sql(
    "SELECT id,label FROM 'target/sql-local-source-smoke.csv' "
    "WHERE amount > ALL (SELECT threshold FROM 'target/sql-local-source-thresholds.csv' "
    "WHERE active IS TRUE ORDER BY score DESC LIMIT 2) LIMIT 10"
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
    sql_row_value_rows.row_value_in_predicate_runtime_execution,
    sql_row_value_rows.row_value_in_tuple_count,
    sql_row_value_rows.row_value_in_null_semantics,
)
print(
    sql_in_subquery_rows.in_subquery_runtime_execution,
    sql_in_subquery_rows.in_subquery_source_formats,
    sql_in_subquery_rows.in_subquery_materialized_value_count,
)
print(
    sql_exists_rows.exists_subquery_runtime_execution,
    sql_exists_rows.exists_subquery_source_formats,
    sql_exists_rows.exists_subquery_result,
)
print(
    sql_quantified_rows.quantified_subquery_runtime_execution,
    sql_quantified_rows.quantified_subquery_source_formats,
    sql_quantified_rows.quantified_subquery_materialized_value_count,
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
Schema-declared local-source `fillna(...)` / `fill_null(...)` now rewrite selected or declared
columns to scoped `COALESCE(column, literal)` projections, and schema-declared
`isna(...)` / `isnull(...)` / `notna(...)` / `notnull(...)` rewrite explicit or declared columns to
`IS NULL` / `IS NOT NULL` boolean projections. Broad pandas null-fill options, inferred schemas,
unsafe expression/join/aggregate/window shapes, and full mask result-shape parity remain gated.
It does not make the Python client a
pandas/Polars-like execution engine, does not add broad SQL/DataFrame runtime,
expression-backed `with_column` beyond the admitted numeric/string/null/temporal/predicate and
scoped JSONL/result-boundary complex projection families,
generalized grouped aggregation or HAVING expressions beyond emitted aggregate output columns,
ordering/collation parity, nested JSON source decoding, complex equality/accessors/casts,
broader binary source decoding, broader Parquet/Arrow IPC/Avro/ORC type/nesting coverage, object stores, or table/lakehouse inputs, and does not create a performance or
production claim.

The Python query builder admits scoped local-source joins through the same scoped SQL local-source
smoke. Both sides must be admitted local sources such as CSV or flat JSON/JSONL/NDJSON, with
feature-gated flat scalar Parquet/Arrow IPC/Avro/ORC using the same deterministic adapter gates as
other local-source smokes. Use `join(..., on="key")` or
`join(..., on=("customer_id", "region"))` for inner, left/right/full outer, left semi, or left
anti joins over matching same-named key columns on both sides. Use `join(..., how="cross")`
without `on` for a scoped cross join and place filters in `filter(...)` / SQL `WHERE`. Qualified
expression joins use `join(..., condition="f.amount > d.threshold")`, predicate objects such as
`join(..., condition=(sl.col("f.customer_id") == sl.col("d.customer_id")) | (sl.col("f.region") == sl.col("d.region")))`,
or direct SQL `ON` predicates. The condition must bind qualified columns from both sides, may use
scoped logical `OR` over admitted scalar leaves, and remains independent of the source file format.
Qualified
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
families, unqualified join predicates, complex equality/accessor/cast semantics, nested source
decoding, and
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
print(workflow.schema_contract({"id": "int64", "label": "string"}).valid)
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
broad ANSI subquery parity beyond admitted bounded local scalar IN-subqueries, row-value
IN-subqueries, scoped local EXISTS predicates, scoped quantified ANY/ALL predicates, scoped
source-qualified scalar/row-value IN/NOT IN, EXISTS/NOT EXISTS, and quantified local subquery refs, scoped
correlated `outer.<column>` source-subquery filters, scoped subquery-backed predicate/CASE
projections, and grouped/HAVING projected source-subquery tails for those families, arbitrary
predicate-tree completeness beyond the admitted
parenthesized leaves, Python/DataFrame joins beyond
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
feature-gated local Vortex output/fanout with local artifact replay/fidelity reporting, routed
through the public workflow `run` facade where the local output payload is admitted. Generated
source-free helpers also expose `.fanout(...)` over admitted generated rows and source-free SQL
through the same public facade.
Current typed result objects expose scoped `SourceState`, `VortexPreparedState`, and `OutputPlan`
evidence where the CLI emits it; future Python capability/write views may broaden cache
invalidation, reuse levels, persistent OutputPlan reuse, and claim-grade replay/fidelity evidence.
Input and output formats remain decoupled, and reuse evidence will not imply performance,
production, object-store/lakehouse, Foundry, or SQL/DataFrame support.

Unsupported workflow affordances are explicit report surfaces too. These calls show how familiar
pandas/Arrow/DataFrame/notebook methods fail closed when they are outside the admitted bounded
local-source or materialized-input shapes:

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
    sl.read_csv("events.data").display(),
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

This matrix is still claim-safe, but it now distinguishes scoped runnable rows from broad
unsupported rows. The scoped local CSV `collect` and `write` rows plus the flat
JSON/JSONL/NDJSON and feature-gated flat scalar Parquet/Arrow IPC/Avro/ORC
projection/optional-filter/limit bridges are marked as
fixture-smoke-supported only for the admitted projection/optional-filter/limit,
preview/select-star, scalar aggregate, multi-key grouped aggregate, join, sort, computed-column,
JSONL/result-boundary complex projection, and scoped ranking-window shapes described above.
Alias rows such as `project`, `with_columns`, `assign`, `groupby`, `order_by`, `sort_by`,
`sort_values`, `distinct`, `drop_duplicates`, and `unique` are included in the matrix so wrappers
and agents can distinguish familiar method names that lower to existing runtime evidence from
genuinely unsupported DataFrame requests. Schema-declared local-source `rename` /
`rename_columns` and `drop` / `drop_columns` are also fixture-smoke-supported: they rewrite the
declared projection to ShardLoom SQL aliases or column exclusion before an admitted terminal
collect/write route runs. Inferred-schema, join, aggregate, window, expression, unknown-column,
duplicate-output, and drop-all-column shapes still fail closed without invoking pandas, Polars, or
another DataFrame backend.
`ctx.sql(...)` is also fixture-smoke-supported only for scoped local-source
collect/write and source-free generated-output writes covered by the SQL ladder. Broad SQL
parse/bind/plan/execute, catalogs, object-store/table SQL, and generalized DataFrame runtime still
return deterministic blockers. The `dataframe_generated_with_column` row is fixture-smoke-supported
for the scoped literal helper and for concrete generated builders such as
`ctx.from_rows(...).with_column(...)` and `ctx.range(...).with_column(...)`; broad generated
DataFrame expression runtime still uses deterministic unsupported diagnostics.
It does not use DataFrame libraries as execution engines, invoke external engines, or upgrade
DataFrame/notebook support to claim-grade status. Other lazy source, `filter`, `select`/`project`,
`limit`, and `group_by`/`groupby` helpers remain side-effect-free declarations unless an admitted
terminal method is called. Joins, aggregations, windows, schema/data-quality helpers, bounded
runtime profile, scoped local-source quarantine, scoped `value_counts` grouped-count lowering,
scoped row-wise `concat` over two explicitly matching projected local-source branches, bounded
explicit-key `merge` over the admitted join route, scoped one-column `nunique` over
`count(DISTINCT ...)`, bounded Python-object materialization, optional pandas/Arrow/NumPy
conversion, and notebook preview remain fixture-scoped; broad pandas summary parity,
implicit/suffix merge, schema-union concat, axis=1 concat, unbounded materialization, production
observability, production quarantine governance, and production notebook display remain
deterministic unsupported surfaces unless later evidence-backed slices promote them.
`value_counts(...)` is supported only as a local-source `group_by(...).count(rows)` rewrite with
optional `IS NOT NULL` dropna filtering and rows-desc ordering; normalize/bin/axis behavior and
unsafe plan shapes remain blockers.
`concat(...)` is supported only as row-wise `UNION ALL` for two local-source branches that already
project the same bare columns explicitly; implicit schema alignment, path targets, multi-branch
concat, and column-wise concat remain blockers.
`merge(...)` is supported only as an explicit `on=...` alias to the admitted ShardLoom `join(...)`
route; implicit key inference, `left_on`/`right_on`, suffix handling, and right-side operations
remain blockers.
`nunique(...)` is supported only as one bare column with `dropna=True`, lowering to SQL
`count(DISTINCT column)`; multi-column result shapes, `dropna=False`, and axis semantics remain
blockers.
`schema_contract(...)` is supported as the exact bounded `validate_schema(...)` contract over the
same admitted local-source rows; it is not a broad schema registry, table constraint manager, or
object-store/lakehouse enforcement surface.
`profile(...)` is supported as a bounded local-source runtime profile over the same
`sql-local-source-smoke` path, reporting row count, field count, null counts, inline JSONL
materialization, and no-fallback evidence. It is not a hidden pandas/Polars profiler, resource
tracer, performance claim, or production observability surface.
`quarantine(...)` is supported for scoped local-source bounded classification; pushdownable
`not_null:column` quarantine rows can write to admitted local sinks through
`sql-local-source-smoke`, while non-pushdown checks remain explicit report-only bounded
classification. It is not object-store/table quarantine, production remediation, or a broad
data-governance engine.

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

The scoped local file, generated-output, bounded schema/data-quality/preview, and bounded decoded
materialization rows are admitted. General Vortex workflows, broad unbounded decoded
pandas/Arrow/NumPy materialization, object-store/lakehouse/table I/O, arbitrary
SQL/Python/DataFrame breadth, and cross-front-door performance equivalence remain explicit gap rows
until correctness, Native I/O, execution-certificate, no-fallback, and benchmark evidence closes
them.

The v1 Vortex runtime scope is owned by `docs/architecture/v1-vortex-runtime-scope.md`. Use
`ctx.local_vortex_primitive_route_report()` for the feature-gated local Vortex primitive route
ids, CLI commands, materialization boundaries, and no-fallback evidence posture; broad object-store
Vortex, table/catalog Vortex, generalized Source/Sink, and broad Vortex SQL/DataFrame support remain
outside that scope.
Use `ctx.native_vortex_provider_route_certificate_report()` for the exact feature-gated native
Vortex provider routes that admit benchmark-family grouped aggregation, hash join, global top-N,
cast/try-cast, substring contains, and native `write_vortex` sink shapes from Python and SQL.

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

ctx = context()
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
same generated-source SQL smoke; the generated range builder also accepts `project`,
`with_columns`/`assign`, and `order_by`/`sort_by`/`sort_values` aliases over those same operations.
Source-free top-N reports
`sql_source_free_order_by_runtime_execution`, `sql_source_free_top_n_runtime_execution`,
`sql_source_free_sort_keys`, `sql_source_free_sort_direction`,
`sql_source_free_sort_operator_family`, and `sql_source_free_top_n_limit` alongside projection,
filter, and limit evidence. `ctx.sql(...).write(...)` dispatches those source-free forms through the
public workflow `run` facade to the generated-source SQL runtime, and `ctx.sql(...).fanout(...)`
dispatches source-free generated forms through the same public facade and generated-source fanout
contract. Generated-source fanout reports
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
  `write_vortex(...)` when the CLI is built with `--features vortex-write`; direct writes and
  `.fanout(...)` route through the public workflow `run` facade, with fanout reusing the computed
  generated rows through the generated-source fanout evidence contract. Broader generated-source
  APIs remain report-only.
- `engine_native_generated_source`: scoped local `range`, `sequence`, and SQL
  `generate_series`/`range` JSONL/CSV fixture smokes are supported through
  `ctx.range(...).write(...)`, `ctx.range(...).filter(...).with_column(...).sort(...).limit(...).write(...)`,
  `ctx.sequence(...).write(...)`, and `ctx.sql("SELECT * FROM generate_series/range(...)").write(...)`;
  direct writes and `.fanout(...)` route through the public workflow `run` facade for generated
  range/sequence and source-free SQL, and the same feature-gated flat scalar structured and Vortex
  sinks are available through the generated-source write helpers.
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
- SQL source-free projection and scoped DataFrame literal projection
  as `fixture_smoke_supported` only for scoped local JSONL/CSV and feature-gated flat scalar
  structured/Vortex generated-output smokes with generated-source and output evidence.
- Broad expression-backed DataFrame projection and expression-backed generated `with_column` forms
  remain blocked/report-only with deterministic blocker IDs.

Admission capability discovery separates scoped runtime rows from report-only or blocked rows.
Scoped SQL `VALUES`, literal `SELECT`, `generate_series`/`range`, and local-source SQL ladder rows
carry parser/binder/planner/runtime evidence when they execute. Report-only or blocked admission
rows still do not parse SQL, bind names, plan a query, generate rows, write output, probe object
stores, invoke Foundry, or invoke external engines. Current no-dataset smoke rows report
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
sequence, SQL `VALUES`, SQL literal `SELECT`, SQL `generate_series`/`range`, scoped SQL range
projection, scoped DataFrame literal projection, and scoped generated DataFrame `with_column` paths
report
`claim_gate_status=fixture_smoke_only` in their scoped local JSONL/CSV lanes and feature-gated flat
scalar Parquet/Arrow IPC/Avro/ORC/Vortex lanes. Default binaries return deterministic blockers for
structured sinks until built with `--features universal-format-io`, and for Vortex until built with
`--features vortex-write`. Vortex generated-output reports include
`vortex_output_runtime_execution`, `vortex_output_reopen_verified`, `vortex_artifact_digest`,
`upstream_vortex_write_called`, and `upstream_vortex_scan_called`.
`ctx.generated_output_to_object_store(...)` now admits a scoped local-emulator fixture route by
staging generated rows through `generated-source-user-rows-smoke` and then committing them through
`object-store-write-smoke`; live S3/GCS/ADLS providers, table/lakehouse commits, and production
object-store claims remain gated. `ctx.foundry_generated_output(...)` admits only the local
Foundry-style result/evidence dataset proof; real Foundry output APIs, production Foundry runtime,
and direct S3/object-store shortcuts remain gated.

The scoped DataFrame source-free projection helper lowers literal aliases to the generated-source
local-output command and returns the same `GeneratedSourceWriteReport` as other generated-output
paths:

```python
ctx.dataframe_source_free_projection("lit(1).alias('value')").write("target/generated-df.jsonl")
```

The scoped generated DataFrame `with_column` helper admits a one-row literal column and writes
through the same generated-source local-output command:

```python
(
    ctx.dataframe_generated_with_column("value", "lit(1)")
    .write("target/generated-df-column.jsonl")
)
```

Generated rows can also be written through the scoped local-emulator object-store route:

```python
object_store = ctx.generated_output_to_object_store(
    "target/object-store/generated.jsonl",
    rows=[{"id": 1, "label": "alpha"}],
    allow_overwrite=True,
)

print(object_store.object_store_write_status)
print(object_store.fallback_attempted, object_store.external_engine_invoked)
```

The Foundry helper is similarly scoped to the local dev-stack proof. A local path writes generated
rows through ShardLoom into a result dataset-shaped directory and writes an evidence
dataset-shaped directory through the local Foundry-style output API:

```python
foundry = ctx.foundry_generated_output(
    "target/foundry/result-dataset",
    rows=[{"id": 1, "label": "alpha"}],
    allow_overwrite=True,
)

print(foundry.foundry_style_output_api_invoked)
print(foundry.fallback_attempted, foundry.external_engine_invoked)
```

Remote object-store generated-output targets and real Foundry references still expose deterministic
unsupported reports when called with `check=False`. Those reports do not stage rows, probe
credentials, invoke real Foundry, call an external engine, or attempt fallback:

```python
remote_report = ctx.generated_output_to_object_store("s3://bucket/out.jsonl", check=False)
foundry_report = ctx.foundry_generated_output("foundry://dataset/output")
```

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

The current published v0.1.1 package is a Python client surface over an external CLI installation.
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

client = ShardLoomClient.from_repo()
result = client.local_vortex_primitive_smoke(
    "shardloom-vortex/tests/fixtures/local_primitive_struct_five.vortex",
)

print(result.commands)
print(result.all_certified)
print(result.filter_project.field("filter_project_local_execution_rows_projected"))
print(result.fallback_attempted)
```

The Python helper path uses the public `run` facade for explicit local primitive
execution, so envelopes have `command=run` plus
`public_workflow_resolved_internal_command` set to `vortex-run`,
`vortex-count-where`, `vortex-filter`, `vortex-project`, or
`vortex-filter-project`. Count-all maps to `--vortex-primitive count`.
Count-where, filter, project, and filter-project map to explicit predicate,
projection, source-order limit, `memory_gb`, and `max_parallelism` payloads.
The lower `vortex-*` commands remain available for direct diagnostics, tests,
and benchmark evidence. Calls without explicit local primitive execution use
the existing metadata/plan evidence surfaces where the CLI supports them.

The repository smoke script prints command, status, certificate, Native I/O,
materialization, work-metric, evidence-artifact, and no-fallback fields:

```powershell
python scripts\write_ci_version_env.py --format powershell | Invoke-Expression
$env:RUSTUP_TOOLCHAIN = $env:SHARDLOOM_RUST_MSRV_TOOLCHAIN
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

client = ShardLoomClient.from_repo()
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
