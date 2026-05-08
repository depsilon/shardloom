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
