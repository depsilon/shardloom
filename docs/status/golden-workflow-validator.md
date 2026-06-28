<!-- SPDX-License-Identifier: Apache-2.0 -->

# Golden Workflow Validator

Status: local runtime validator.

Run:

```powershell
python scripts\check_golden_workflows.py
```

The validator writes:

```text
target/golden-workflow-report.json
target/golden-workflows
```

Schema:

```text
shardloom.golden_workflow_validation_report.v1
```

## Covered Workflows

| Workflow | Runtime proof | Claim boundary |
| --- | --- | --- |
| local CSV/JSONL to `vortex_ingest` to prepared query to JSONL/CSV output | `vortex-prepare`, prepared `vortex-filter-project`, Python `ctx.prepare_vortex(...)`, and native Vortex JSONL/CSV fanout over a shared local source | Local runtime path only; the ad hoc ingested artifact has Native I/O evidence, while fixture execution certificates remain limited to checked-in primitive fixtures |
| generated source to local Vortex output and replay/fidelity evidence | `generated-source-user-rows --output-format vortex` plus local `vortex-filter-project` replay over the emitted `.vortex` artifact | Source-free local Vortex output/replay only; no broad generated SQL, object-store, table, or production sink claim |
| prepared/native Vortex count/filter/project with execution certificates | `vortex-count-where`, `vortex-project`, and `vortex-filter-project` over `local_primitive_struct_five.vortex` | Scoped fixture-certified native Vortex primitive coverage only |

## Required Evidence

The report must keep these markers true for every release-readiness run:

```text
golden_workflow_validator_status=passed
workflow_count=3
stage_count>=9
support_matrix_status=passed
source_route
preparation_route
execution_route
output_route
row_counts
artifact_refs
local_primitive_execution_certificate_status=certified
local_primitive_native_io_certificate_status=certified
output_native_io_certificate_status=certified_local_fanout_sinks|certified_local_vortex_sink
result_replay_verified=true
reopen_verification_status=reopen_metadata_row_count_verified for prepared-ingest certification
fallback_attempted=false
external_engine_invoked=false
production_claim_allowed=false
performance_claim_allowed=false
public_release_claim_allowed=false
public_package_claim_allowed=false
```

The validator also checks `docs/status/runs-today-support-matrix.json` so the repository current-
support rows stay aligned with the runnable workflows. The clean-slate website does not publish a
separate runs-today support-matrix data mirror; public status context now routes through the
Field Guide limitations page and repository evidence.

## Non-Goals

This validator does not authorize a production workflow claim, object-store/lakehouse/Foundry
production support, distributed runtime support, or performance superiority claim. It authorizes
no package publication. It does not invoke Spark, DataFusion, DuckDB, Polars, Velox, or another
external query engine.
