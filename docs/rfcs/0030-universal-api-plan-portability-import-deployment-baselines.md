# RFC 0030 — Universal API, Plan Portability, Import/Deployment, and External Baselines

## Scope

This RFC defines implementation contracts for:
- CG-11 Python/API foundation surface later.
- CG-12 plan portability / semantic IR.
- CG-18 universal import/deployment/baseline harness.

## Universal API posture

- Thin Python wrapper over CLI JSON first.
- Stable command schema.
- No PyO3/maturin unless explicitly approved.
- No Spark fallback.
- CG-11 establishes the API/protocol foundation for Python and other clients. Mature Python wrapper,
  DataFrame/query-builder, notebook, Python UDF, packaging, and workload-certification evidence
  belongs to CG-20.

## Plan portability contract

- ShardLoom plan export contract.
- Optional Substrait-like export/import validation.
- Residual unsupported plan reporting.
- No external engine execution in runtime paths.
- Initial CG-12 report foundation is validation-only and does not serialize,
  import, export, parse, execute, probe, read, write, or delegate.

### CG-12 native-first plan portability report foundation

The first CG-12 milestone exposes a `PlanPortabilityReport` through existing
plan commands. It is a report contract for agents and future clients, not real
plan interchange behavior.

Required report fields:
- `schema_version`
- `report_id`
- `direction`
- `status`
- `interop_format`
- `native_plan_schema_version`
- `native_first=true`
- `validation_only=true`
- `validation_required`
- `capability_check_required`
- `supported_constructs`
- `native_only_nodes`
- `substrait_like_representable_nodes`
- `lossy_nodes`
- `unsupported_nodes`
- `residual_unsupported_constructs`
- `metadata_loss_boundaries`
- `encoded_semantics_loss`
- `redaction_required`
- `parser_executed=false`
- `import_export_serialization_performed=false`
- `runtime_execution=false`
- `external_engine_execution=false`
- `filesystem_probe=false`
- `network_probe=false`
- `catalog_probe=false`
- `adapter_probe=false`
- `read_io=false`
- `write_io=false`
- `side_effect_free=true`
- `fallback_execution_allowed=false`
- `fallback_attempted=false`
- `diagnostics`

Acceptance:
- `plan-ir --format json` emits native-first plan portability evidence for the
  current native plan skeleton.
- `plan-import --format json` emits unsupported/residual import evidence without
  parsing or lowering a payload.
- `plan-export --format json` emits unsupported/residual export evidence and
  records redaction requirements without serializing a payload.
- All three commands remain side-effect-free: no parser execution, no runtime
  execution, no external engine execution, no filesystem/network/catalog/adapter
  probing, no read/write IO, and no fallback execution.
- Real plan serialization/import/export remains deferred until a later CG-12
  implementation gate adds native capability checks and compatibility tests.

## Universal runner/deployment contract

- Universal CLI JSON runner contract.
- Package/import guidance independent of Foundry.
- Basic Foundry transform/deployment examples remain optional under CG-18.
- Richer Foundry packaging, governance, lineage, virtual-table, Marketplace, and Compute Module
  integration is governed by RFC 0036.
- Foundry is not the primary engine target.

## External baseline harness

- Spark baseline runner, external only.
- Polars baseline runner, external only.
- DataFusion baseline runner, external only.
- DuckDB baseline runner, external only.
- Dask baseline runner, external only.
- pandas baseline runner, external only.
- Stable comparison report dataset.
- No runtime fallback.

### CG-18 universal harness report foundation

The first CG-18 milestone exposes a `UniversalHarnessReport` through
`universal-harness-plan`. It is a report-only contract that ties the CLI JSON
runner, package/import guidance, deployment profile guidance, optional Foundry
examples, external baseline harnesses, comparison report datasets, and
portability checks into one machine-readable surface.

Required report fields:
- `schema_version`
- `report_id`
- `status`
- `runner_contract_fields`
- `surfaces`
- `external_baselines`
- `output_envelope_required`
- `stable_command_schema_required`
- `exit_code_required`
- `diagnostics_required`
- `side_effect_manifest_required`
- `output_artifacts_required`
- `metrics_required`
- `comparison_dataset_required`
- `correctness_evidence_required`
- `benchmark_evidence_required`
- `foundry_required=false`
- `foundry_optional_example=true`
- `package_import_performed=false`
- `deployment_performed=false`
- `external_baseline_execution=false`
- `runtime_execution=false`
- `filesystem_probe=false`
- `network_probe=false`
- `catalog_probe=false`
- `adapter_probe=false`
- `read_io=false`
- `write_io=false`
- `external_publish=false`
- `fallback_execution_allowed=false`
- `fallback_attempted=false`
- `production_claim_allowed=false`
- `diagnostics`

Harness surfaces:
- CLI JSON runner
- package/import
- deployment profile
- Foundry example
- external baseline runner
- comparison report dataset
- portability check

External baseline requirements:
- baseline engine
- engine version required
- workload id required
- fixture id required
- command or transform required
- correctness result required
- benchmark metrics required
- comparison report required
- external only true
- runner execution performed false
- fallback execution allowed false
- fallback attempted false

Acceptance:
- `universal-harness-plan --format json` emits the CG-18 contract with stable
  runner contract fields and deterministic surface/baseline ordering.
- Foundry remains optional example context in this RFC, never a required deployment target. Richer
  Foundry integration must follow RFC 0036 and cannot weaken no-fallback execution.
- Spark, DataFusion, Polars, DuckDB, Dask, and pandas are external comparison harness targets only.
- The report is side-effect-free: no package import, deployment, Foundry
  invocation, baseline runner execution, parser execution, runtime execution,
  filesystem/network/catalog/adapter probing, read/write IO, external publish,
  or fallback execution.
- Real import/deployment, comparison dataset materialization, baseline runner
  scripts, and portability execution remain deferred until later CG-18 gates.

### CG-18 import/deployment/baseline harness maturity

The CG-18 maturity surface extends `UniversalHarnessReport` beyond the initial foundation by naming
the required reproducible harness environments and optional baseline environments.

Harness environments:
- local package import and CLI binary resolution smoke
- CI workspace/package smoke
- container smoke
- optional Foundry transform smoke
- optional benchmark-extra environment smoke

External baseline environments:
- Spark
- DataFusion
- Polars
- DuckDB
- Dask
- pandas

Acceptance:
- `UniversalHarnessReport` records all required harness environments in stable order.
- `universal-harness-plan --format json` exposes environment counts, environment order, baseline
  order, required local/CI/container/optional Foundry/optional benchmark harness flags, and
  `external_engines_as_runtime_dependencies_allowed=false`.
- External engines remain optional comparison-only environments and must not become ShardLoom
  runtime dependencies.
- Foundry remains optional context in RFC 0030; richer Foundry packaging and platform integration
  remains governed by RFC 0036.
- No harness execution, package publication, container publication, Foundry invocation, benchmark
  execution, comparison dataset materialization, external engine invocation, runtime expansion, or
  fallback execution is authorized by this maturity surface.


### Universal CLI JSON runner contract

Required fields:
- `command`
- `schema_version`
- `exit_code`
- `status`
- `diagnostics`
- `fallback_execution_allowed=false`
- `side_effects`
- `output_artifacts`
- `metrics`

### CG-11 CLI/API JSON protocol foundation

The first CG-11 implementation surface is a report-only CLI/API protocol
contract. It standardizes the existing `OutputEnvelope` JSON boundary for
automation, agents, and a future thin Python wrapper before native Python
bindings or a DataFrame API exist.

Required report fields:
- `schema_version`
- `protocol_id`
- `protocol_stability`
- `output_envelope_schema_version`
- `required_envelope_fields`
- `required_fallback_fields`
- `required_diagnostic_fields`
- `required_field_entry_fields`
- `command_status_values`
- `output_formats`
- `thin_python_wrapper_boundary`
- `pyo3_maturin_allowed=false`
- `foundry_required=false`
- `dataframe_api_implemented=false`
- `side_effect_free=true`
- `filesystem_probe=false`
- `network_probe=false`
- `catalog_probe=false`
- `adapter_probe=false`
- `parser_executed=false`
- `runtime_execution=false`
- `write_io=false`
- `external_publish=not_performed`
- `external_publish_performed=false`
- `fallback_execution_allowed=false`
- `fallback_attempted=false`
- `diagnostics`

Acceptance:
- `api-compat-plan --format json` emits the protocol contract using the stable
  output envelope.
- The protocol contract documents the envelope, fallback, diagnostic, and field
  entry keys that clients can consume.
- The Python wrapper starts as a thin source-tree CLI JSON subprocess/client
  boundary.
- CG-11 does not add PyO3, maturin, native Python bindings, a DataFrame API,
  parser execution, adapter probing, filesystem/network probing at import time,
  external publication, or fallback execution.

### CG-11 thin Python wrapper foundation

The first Python-wrapper implementation is a source-tree, zero-dependency Python
package over the CLI JSON protocol. It is intentionally not a native binding,
published package, DataFrame API, notebook runtime, Python UDF runtime, SQL
runtime, adapter runtime, or fallback execution path.

Required report fields:
- `schema_version`
- `wrapper_id`
- `wrapper_status`
- `transport_protocol_id`
- `output_envelope_schema_version`
- `invocation_model`
- `initial_command_scope`
- `required_client_behaviors`
- `package_status=source_tree_created`
- `native_binding_status=not_created`
- `pyo3_maturin_allowed=false`
- `python_package_created=true`
- `native_extension_required=false`
- `dataframe_api_implemented=false`
- `notebook_api_implemented=false`
- `python_udf_runtime_implemented=false`
- `materialization_boundary_reporting_required=true`
- `diagnostics_passthrough_required=true`
- `side_effect_free=true`
- `filesystem_probe=false`
- `network_probe=false`
- `catalog_probe=false`
- `adapter_probe=false`
- `parser_executed=false`
- `runtime_execution=false`
- `write_io=false`
- `external_publish=not_performed`
- `external_publish_performed=false`
- `fallback_execution_allowed=false`
- `fallback_attempted=false`
- `diagnostics`

Acceptance:
- `python-wrapper-plan --format json` emits the wrapper foundation contract.
- The wrapper foundation starts as a CLI JSON subprocess/client over
  `shardloom ... --format json`.
- The source-tree wrapper preserves `OutputEnvelope` diagnostics, fallback
  status, and materialization-boundary fields instead of translating them into
  lossy Python exceptions only.
- Mature Python API, DataFrame/query-builder, notebook behavior, Python UDF,
  packaging, and distribution certification remain CG-20 work.
- Importing the wrapper must not execute ShardLoom commands, read datasets,
  contact networks, probe adapters/catalogs, or attempt fallback execution.
- CG-11 does not add PyO3/maturin, add a native extension, run a parser, publish
  packages, or attempt fallback execution.

### CG-11 Python live ETL helper surface

The source-tree wrapper may expose explicit helpers for currently supported CLI
smoke commands so users can run local live tests without memorizing long command
lines. These helpers must remain thin subprocess calls over `OutputEnvelope`.

Allowed initial helpers:
- `status`
- `capabilities`
- `api-compat-plan`
- `python-wrapper-plan`
- `vortex-run`
- `traditional-analytics-run`
- `traditional-analytics-vortex-run`
- `dynamic-work-shaping-plan`
- `sizing-feedback-plan`
- `benchmark-plan`
- `benchmark-claim-evidence-plan`

Helper boundaries:
- CSV live ETL smoke may invoke the existing CSV-to-Vortex local benchmark path
  only when called explicitly by the user.
- Native Vortex live ETL smoke may invoke existing `.vortex` inputs through the
  current benchmark-only native Vortex path.
- Dynamic sizing and dynamic work shaping helpers are advisory reports only and
  must not mutate runtime policy.
- Binary discovery may use explicit `SHARDLOOM_BIN`, an explicit binary path, or
  an explicit source-tree root, but must not run on import.
- These helpers are not mature DataFrame, SQL, UDF, adapter, notebook, package
  publication, benchmark-certification, or best-default evidence.

### ExternalBaselineRun

Required fields:
- `baseline_engine`
- `engine_version`
- `workload_id`
- `fixture_id`
- `command_or_transform`
- `result_status`
- `correctness_result`
- `runtime_ms`
- `memory_peak_bytes`
- `bytes_read`
- `bytes_written`
- `notes`

### ComparisonReportDataset

Required fields:
- `workload_id`
- `shardloom_result`
- `external_baseline_results`
- `correctness_passed`
- `benchmark_claim_allowed`
- `diagnostics`

Clarifications:
- Foundry is an optional example only.
- Baseline runners are external harnesses only.
- Baseline results never drive ShardLoom runtime fallback.
- Superiority claims require CG-5 correctness and CG-6 benchmark evidence.

## Non-goals

- No fallback/delegation to external engines.
- No mandatory Foundry dependency.


### Additional CG-18 reporting direction

- Foundry remains an optional example under universal import/deployment, not the primary engine
  target.
- Add an external baseline report dataset concept for stable, machine-readable cross-engine
  comparisons.
