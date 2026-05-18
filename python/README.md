# ShardLoom Python CLI Client

This package is the first thin Python surface for ShardLoom, a Vortex-native,
no-fallback, evidence-certified local compute engine. It invokes the workspace
`shardloom` CLI with `--format json`, parses the stable `OutputEnvelope`, and
preserves typed result/artifact/certificate payloads, diagnostics, fallback
status, and the temporary legacy field mirror.

It is intentionally not a native binding, DataFrame API, SQL runtime, UDF
runtime, or fallback execution path. Importing the package has no ShardLoom
side effects. Work happens only when a caller explicitly invokes a CLI command
through `ShardLoomClient`.

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

`ShardLoomSession` is planned as `GAR-PERF-2F`, not current Python runtime support. The future
surface should expose a caller-owned in-process local session over prepared/native artifacts with
`session_id`, cache hit/miss fields, source-state reuse counts, prepared-artifact reuse counts,
explicit close/drop status, and no-fallback/no-external-engine evidence. It must not imply a daemon,
remote server, hidden global cache, DataFrame/SQL runtime, object-store/lakehouse runtime, or
performance claim.

Allocation profiling and scoped buffer-pool optimization are planned as `GAR-PERF-2G`, not current
Python runtime support. Any future Python-visible buffer reuse must stay opt-in or explicitly
scoped to a run/session and preserve correctness, evidence, no-fallback, and no-external-engine
fields.

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
`sl.read_json`, and `sl.read_parquet`. They only declare sources and
transformations. `plan()`, `explain()`, `estimate()`, `certify()`, and
`unsupported_report()` are explicit report calls over CLI JSON surfaces; they do
not read input files, infer schemas, materialize rows, probe object stores,
write output, or invoke fallback engines.

Evidence-aware optimizer traces are planned as `GAR-PERF-2B`, not current Python runtime support. A
future Python `explain()` trace should expose optimizer rule status, before/after plan digests,
rewrite safety, evidence preservation, no-fallback fields, and claim gates without implying broad
SQL/DataFrame execution or Polars/DataFusion optimizer parity.

Reusable I/O state and cross-format fanout are planned as `GAR-IOREUSE-1`, not current Python
runtime support. Future Python capability/write views may expose `SourceState`,
`VortexPreparedState`, `OutputPlan`, cache invalidation, reuse levels, and fanout evidence, but
input and output formats remain decoupled and reuse evidence will not imply performance,
production, object-store/lakehouse, Foundry, or SQL/DataFrame support.

Unsupported workflow affordances are explicit report surfaces too. These calls
are meant to make familiar pandas/Arrow/DataFrame/notebook methods discoverable
without pretending they execute yet:

```python
import shardloom as sl

ctx = sl.context()
workflow = ctx.read_csv("events.csv").filter("amount > 0")

reports = [
    sl.from_pandas(object()),
    sl.from_arrow_table(object()),
    sl.from_arrow_ipc("events.arrow"),
    workflow.to_pandas(),
    workflow.to_arrow_table(),
    workflow.to_arrow_ipc(),
    workflow.to_numpy(),
    workflow.to_python_objects(),
    workflow.with_column("event_date", "to_date(ts)"),
    workflow.group_by("customer_id").agg(total="sum(amount)"),
    workflow.agg("count(*)"),
    workflow.sort("event_date"),
    workflow.schema(),
    workflow.describe_schema(),
    workflow.validate_schema({"id": "int64"}),
    workflow.data_quality_summary(),
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
import pandas or pyarrow, inspect the passed Python object, materialize rows,
write quarantine outputs, parse SQL, execute DataFrame expressions, render
notebook display output, invoke Foundry/model services, or use another engine
as fallback.

The DataFrame-style surface also has a typed method capability matrix. Use it
when a wrapper, notebook, or agent needs to know which familiar method names are
lazy declarations, which are unsupported diagnostics, and which evidence gates
must pass before a method can become runtime-supported:

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

This matrix is report-only. It does not execute a plan, inspect datasets, import
DataFrame libraries, materialize rows, write output, invoke external engines, or
upgrade DataFrame/notebook support to claim-grade status. Current lazy source,
`filter`, `select`, `limit`, and `group_by` helpers are side-effect-free
declarations; joins, aggregations, windows, writes, schema/data-quality helpers,
materialization, and notebook display remain deterministic unsupported
diagnostic surfaces unless later evidence-backed slices promote them.

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

Source-free generated-output APIs are tracked under `GAR-GEN-1`. The full
contract is exposed through capability views as `generated_source_contract`, and
the per-API admission matrix is exposed as `generated_source_api_admission`.
`GAR-NOVEL-1A` also exposes `generated_source_evidence_alignment`, which ties the same
GeneratedSourceCertificate rows to report-only OpenLineage, OpenTelemetry, Bayesian-confidence,
and Foundry generated-output boundary refs without enabling exporters or platform runtime.
Two scoped local JSONL smoke paths are runtime-supported: caller-provided rows
and one ShardLoom-native range generator. SQL and broader DataFrame forms remain
report-only unless a later evidence-backed slice admits them:

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
print(admission.row("sql_values").blocker_id)
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

This compatibility contract is still a capability map. It can say local JSONL generated-output
smokes exist for user rows and range, but it keeps `ctx.literal_table`, `ctx.calendar`, SQL
literal/`VALUES` execution, DataFrame generated expressions, object-store/lakehouse output, and
Foundry generated-output runtime as report-only or blocked.

The supported user-row local smoke uses Python rows supplied by the caller,
writes a local JSONL file, and returns generated-source/output evidence:

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

Equivalent CLI command:

```powershell
shardloom generated-source-user-rows-smoke target\generated-reference.jsonl id:int64,label:utf8 "id=1,label=alpha;id=2,label=beta" --format json
```

The supported engine-native range smoke is separate. It generates deterministic
`int64` rows inside ShardLoom, writes local JSONL, and emits the same
generated-source/output/no-fallback evidence family:

```python
range_report = ctx.range(0, 5, column="id").write(
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

The contract separates three cases:

- `no_dataset_smoke`: status/capability/proof smoke only; no generated rows, no
  source Native I/O certificate, and no output data claim.
- `user_generated_source`: scoped local user rows are supported for JSONL
  fixture-smoke writes through `ctx.from_rows(...).write(...)`; broader
  generated-source APIs remain report-only.
- `engine_native_generated_source`: one scoped local `range` JSONL fixture smoke
  is supported through `ctx.range(...).write(...)`; `sequence`, `values`,
  `literal_table`, calendar/date dimensions, and deterministic synthetic
  profiles remain report-only.

The broader intended Python shape is:

```python
ctx.literal_table(...).write(...)
ctx.calendar(...).write(...)
```

`ctx.literal_table`, `ctx.calendar`, SQL `VALUES`/literal execution, and broad
DataFrame expression execution are not runtime-supported yet. Current
source-free API admission rows classify:

- `python_ctx_from_rows`, `python_ctx_range`, and `python_generated_source_write`
  as `fixture_smoke_supported` only for scoped local JSONL generated-output
  smokes with generated-source and output evidence.
- `python_ctx_literal_table`, `python_ctx_calendar`, SQL literal `SELECT`, SQL
  `VALUES`, SQL source-free projection, SQL `generate_series`/`range`
  vocabulary, DataFrame source-free projection, and generated `with_column`
  forms as `report_only` with deterministic blocker IDs such as
  `gar-gen-1.sql_values_runtime_not_implemented`.

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
`claim_gate_status`. The current user-row and range paths report
`claim_gate_status=fixture_smoke_only`. S3/object-store writes remain
report-only/gated, and Foundry generated-output smoke must go through Foundry
output APIs rather than direct S3 paths.

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
JSONL/NDJSON, Parquet, Arrow IPC, Avro, or ORC inputs into temporary local
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
for CSV, JSONL/NDJSON, Parquet, and Arrow IPC inputs before any execution claim.
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
