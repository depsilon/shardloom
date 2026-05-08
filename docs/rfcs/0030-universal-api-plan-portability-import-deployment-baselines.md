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
- CG-11 establishes the API/protocol foundation for Python and other clients. Mature Python wrapper, DataFrame/query-builder, notebook, Python UDF, packaging, and workload-certification evidence belongs to CG-20.

## Plan portability contract

- ShardLoom plan export contract.
- Optional Substrait-like export/import validation.
- Residual unsupported plan reporting.
- No external engine execution in runtime paths.

## Universal runner/deployment contract

- Universal CLI JSON runner contract.
- Package/import guidance independent of Foundry.
- Foundry appears only as optional transform/deployment examples under CG-18.
- Foundry is not the primary engine target.

## External baseline harness

- Spark baseline runner, external only.
- Polars baseline runner, external only.
- DataFusion baseline runner, external only.
- Stable comparison report dataset.
- No runtime fallback.


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
- A future Python wrapper starts as a thin CLI JSON subprocess/client boundary.
- CG-11 does not add PyO3, maturin, a Python package, a DataFrame API, runtime
  execution, parser execution, adapter probing, filesystem/network probing,
  external publication, or fallback execution.

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

- Foundry remains an optional example under universal import/deployment, not the primary engine target.
- Add an external baseline report dataset concept for stable, machine-readable cross-engine comparisons.
