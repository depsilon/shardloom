<!-- SPDX-License-Identifier: Apache-2.0 -->

# Foundry Proof-Of-Use Certification

Status: `GAR-RUNTIME-IMPL-5P` local proof. This proof is Foundry-style only; it does not import
Foundry packages or invoke Foundry services.

For the user-facing local dev-stack path, see
[`docs/foundry/dev-stack-starter-kit.md`](dev-stack-starter-kit.md). The starter kit records
`shardloom.foundry_dev_stack_starter_kit.v1` and keeps Foundry runtime, compute, Spark, output API,
result dataset, evidence dataset, S3/object-store, external compute, and fallback execution disabled.

## Command

```powershell
python scripts\foundry_proof_of_use.py --rows 64 --iterations 1
```

## Evidence Fields

The generated `shardloom.foundry_proof_of_use_report.v1` report includes:

- `package_install_mode`
- `conda_internal_artifact_install_status`
- `transform_import_proven`
- `cli_binary_resolved`
- `no_dataset_smoke_performed`
- `staged_dataset_path_explicit`
- `supported_local_native_execution_smoke_performed`
- `generated_output_execution_performed`
- `generated_source_created`
- `generated_source_certificate_status`
- `staged_input_transform_execution_performed`
- `foundry_style_output_api_invoked`
- `foundry_style_result_dataset_written`
- `foundry_style_evidence_dataset_written`
- `certificate_metrics_dataset_output_written`
- `result_dataset_output_ref`
- `evidence_dataset_output_ref`
- `materialization_staging_boundary_report_ref`
- `foundry_dev_stack_starter_kit_status`
- `foundry_dev_stack_starter_kit_ref`
- `foundry_dev_stack_starter_kit_schema_version`
- `foundry_generated_output_fanout_status`
- `foundry_generated_output_fanout_posture`
- `foundry_generated_output_boundary_status`
- `foundry_generated_output_boundary`
- `foundry_scale_proof_boundary_status`
- `foundry_scale_proof_boundary`
- `foundry_package_proof_boundary_matrix_status`
- `foundry_package_proof_boundary_matrix_ref`
- `foundry_package_proof_boundary_matrix`
- `direct_s3_write_invoked=false`
- `direct_s3_read_invoked=false`
- `object_store_read_invoked=false`
- `object_store_write_invoked=false`
- `object_store_commit_invoked=false`
- `foundry_output_api_invoked=false`
- `foundry_runtime_invoked=false`
- `foundry_compute_invoked=false`
- `foundry_spark_invoked=false`
- `foundry_input_dataset_count=0`
- `foundry_output_dataset_count=0`
- `foundry_style_input_dataset_count=1`
- `foundry_style_output_dataset_count=2`
- `staged_input_bytes`
- `shardloom_execution_mode=local_foundry_style_generated_and_staged_transform_smoke`
- `split_count=0`
- `memory_budget_bytes=null`
- `output_evidence_dataset_written=true`
- `snowflake_databricks_bigquery_invoked=false`
- `virtual_tables_native_execution_claimed=false`
- `fallback_attempted=false`
- `external_engine_invoked=false`
- `public_foundry_claim_allowed=false`
- `local_foundry_style_proof_claim_allowed=true` when the local smoke passes

## Claim Scope

The only allowed claim from this proof is:

```text
local_foundry_style_generated_output_and_staged_transform_smoke_only
```

It is not a Foundry production claim, Foundry package publication claim, Foundry virtual-table native
execution claim, or external compute pushdown claim.

## Package And Proof Boundary Matrix

`GAR-0036-A` adds the report-only package/proof boundary matrix:

```text
schema_version=shardloom.foundry_package_proof_boundary_matrix.v1
gar_id=GAR-0036-A
support_status=report_only
claim_gate_status=not_claim_grade
row_count=9
local_style_claim_allowed_count=2
blocked_count=7
foundry_runtime_invoked=false
foundry_compute_invoked=false
foundry_spark_invoked=false
fallback_attempted=false
external_engine_invoked=false
public_foundry_claim_allowed=false
```

Rows with local smoke posture:

```text
local_style_transform_fixture
local_certificate_metrics_output
```

Blocked rows:

```text
shardloom_foundry_package
artifact_repository_publication
foundry_service_invocation
compute_module_surface
virtual_table_native_execution
dataset_transaction_runtime
f10_workload_certified_deployment
```

The matrix is canonicalized in
[`docs/foundry/package-proof-boundary-matrix.json`](package-proof-boundary-matrix.json) and explained
in [`docs/foundry/package-proof-boundary-matrix.md`](package-proof-boundary-matrix.md). It keeps the
current proof limited to local Foundry-style smoke and local certificate JSON output. It does not
authorize a `shardloom-foundry` package claim, Artifact Repository publication claim, Foundry service
invocation claim, Compute Module claim, virtual-table native execution claim, dataset transaction
runtime claim, F10 deployment claim, Spark fallback claim, external compute pushdown claim, or
production Foundry support claim.

## Generated-Output Boundary

The existing `no_dataset_smoke_performed` field is status/proof smoke only. It does not mean a
Foundry transform generated rows, wrote an output dataset, emitted a `GeneratedSourceCertificate`, or
proved a Foundry source-free generated-output runtime path. GAR-GEN-1C adds a separate local
user-row JSONL generated-output smoke outside Foundry; that local proof does not authorize Foundry
runtime, Foundry package, or direct object-store claims.

Future Foundry generated-output proof must stay separate from no-dataset smoke:

- `no_dataset_smoke`:
  - no data execution
  - no source Native I/O certificate
  - no generated-source certificate
  - no output data claim
- `user_generated_source`:
  - user Python code creates rows
  - ShardLoom consumes rows as a generated/literal/calendar source in scoped local JSONL/CSV smokes when
    deterministic generation evidence exists
  - Foundry output evidence is still required before any Foundry generated-output claim
- `engine_native_generated_source`:
  - ShardLoom executes generator nodes such as `range`, `sequence`, `values`, or deterministic
    synthetic profile
  - the scoped local `range` and `sequence` JSONL/CSV smokes are supported outside Foundry; other
    generator nodes remain report-only
  - ShardLoom writes output and emits generated-source and output evidence

Generated-output proof fields should align with the `GAR-GEN-1` contract. The CLI/Python capability
view exposes that vocabulary as `shardloom.generated_source_certificate_contract.v1`; GAR-GEN-1C
and GAR-GEN-1D emit the fields only for scoped local user-row, literal-table, calendar, range,
sequence, and source-free SQL JSONL/CSV smokes, not for Foundry:

`GAR-COMPAT-1B` also projects the same source-free generated-output posture into
`shardloom.universal_compatibility.generated_output_contract.v1`. That compatibility projection keeps
`no_dataset_smoke`, `python_ctx_from_rows`, `python_ctx_literal_table`, `python_ctx_calendar`,
`python_ctx_range`, SQL `VALUES`/literal rows, DataFrame generated-expression rows,
local-output-only posture, and Foundry/object-store blockers visible without treating Foundry
generated-output runtime as supported.

For typed Python diagnostics, `ctx.foundry_generated_output("foundry://dataset/output")` now returns
the same deterministic unsupported report without invoking Foundry, Spark, object-store I/O, output
dataset writes, fallback, or external engines. It is a no-effect planning surface only; real Foundry
generated-output proof still requires a transform wrapper that writes both the output dataset and an
evidence dataset.

```text
input_dataset_count=0
source_io_performed=false
generated_source_created=true
generated_source_kind
generated_source_schema_digest
generated_source_row_count
generated_source_plan_digest
generated_source_seed
generation_deterministic
output_io_performed
output_native_io_certificate_status
generated_source_certificate_status
fallback_attempted=false
external_engine_invoked=false
claim_gate_status
```

Current no-dataset smoke remains explicitly non-generated-output:

```text
input_dataset_count=0
source_io_performed=false
generated_source_created=false
output_io_performed=false
generated_source_certificate_status=not_applicable_no_generated_rows
```

S3/object-store boundaries remain blocked for this proof. Foundry generated-output smoke should write
through Foundry output APIs, not direct S3/object-store paths. This document does not authorize
credential resolution, network probes, S3 reads, S3 writes, object-store commits, lakehouse output,
Foundry production claims, package publication, or external engine fallback.

`GAR-RUNTIME-IMPL-5P` promotes the local/dev-stack shape from report-only posture to a local
Foundry-style runtime proof:

```text
no input dataset
generate deterministic source
prepare through ShardLoom/Vortex
write local result dataset through the dev-stack Foundry-style output API
write local evidence dataset through the dev-stack Foundry-style output API
```

The current report includes a `shardloom.foundry_generated_output_fanout_posture.v1` object. It
records local-style runtime evidence without pretending real Foundry output APIs or Foundry runtime
executed:

```text
support_status=local_style_smoke_supported
admission_status=local_style_generated_output_and_foundry_style_output_api_evidence_written
generated_output_execution_performed=true
no_dataset_smoke_separate_from_generated_output=true
input_dataset_count=0
source_io_performed=false
generated_source_created=true
generated_source_certificate_status=present
source_native_io_certificate_status=not_applicable_no_source_dataset
output_io_performed=true
output_native_io_certificate_status=certified_local_file_sink
result_dataset_output_status=written_local_foundry_style_dataset
evidence_dataset_output_status=written_local_foundry_style_dataset
foundry_output_api_required=true
foundry_style_output_api_invoked=true
foundry_style_result_dataset_written=true
foundry_style_evidence_dataset_written=true
foundry_runtime_invoked=false
foundry_compute_invoked=false
foundry_spark_invoked=false
foundry_output_api_invoked=false
direct_s3_write_invoked=false
object_store_write_invoked=false
fallback_attempted=false
external_engine_invoked=false
claim_gate_status=fixture_smoke_only
```

Future admitted runtime fields must include:

```text
input_dataset_count=0
generated_source_created=true
generated_source_certificate_status
output_plan_id
output_native_io_certificate_status
foundry_runtime_invoked=false unless real Foundry runtime proof exists
foundry_compute_invoked=false unless real Foundry runtime proof exists
foundry_spark_invoked=false
direct_s3_write_invoked=false
fallback_attempted=false
external_engine_invoked=false
```

No-input smoke and generated-output execution remain separate. A local Foundry-style
generated-output fanout row is not a real Foundry runtime claim, Foundry production claim, Foundry
package publication claim, direct S3/object-store write claim, or Spark fallback claim.

## Foundry Generated-Output Proof Boundary

`GAR-GEN-1F` adds a dedicated report-only Foundry generated-output proof boundary:

```text
schema_version=shardloom.foundry_generated_output_boundary.v1
support_status=local_style_smoke_supported
boundary_status=local_style_dataset_output_written_real_foundry_blocked
no_dataset_smoke_separate_from_generated_output=true
generated_output_execution_performed=true
generated_source_certificate_status=present
output_native_io_certificate_status=certified_local_file_sink
foundry_output_api_required=true
foundry_output_api_invoked=false
foundry_result_dataset_written=false
foundry_evidence_dataset_written=false
foundry_style_output_api_invoked=true
foundry_style_result_dataset_written=true
foundry_style_evidence_dataset_written=true
direct_s3_read_invoked=false
direct_s3_write_invoked=false
object_store_read_invoked=false
object_store_write_invoked=false
object_store_commit_invoked=false
lakehouse_output_invoked=false
foundry_runtime_invoked=false
foundry_compute_invoked=false
foundry_spark_invoked=false
fallback_attempted=false
external_engine_invoked=false
public_foundry_generated_output_claim_allowed=false
claim_gate_status=fixture_smoke_only
```

A future admitted Foundry generated-output smoke must write both the result dataset and evidence
dataset through Foundry output APIs. It must not write directly to S3/object storage, must not use
object-store commit protocols as a shortcut, and must not report Foundry Spark, virtual tables,
Snowflake, Databricks, BigQuery, or other managed compute as ShardLoom execution.

`GAR-NOVEL-1A` projects this boundary into the generated-source evidence alignment report as
`generated_source_evidence_alignment_row_foundry_generated_output_*`. That row points back to
`shardloom.foundry_generated_output_boundary.v1`, keeps OpenLineage and OpenTelemetry export
report-only/no-event, keeps Bayesian confidence unavailable until runtime proof exists, and
preserves `fallback_attempted=false` and `external_engine_invoked=false`.

## Scale Proof Boundary

`GAR-SCALE-1H` adds a report-only Foundry scale proof boundary:

```text
schema_version=shardloom.foundry_scale_proof_boundary.v1
support_status=local_style_smoke_supported
proof_boundary_status=local_style_staged_transform_and_evidence_dataset_written_real_foundry_blocked
foundry_runtime_invoked=false
foundry_compute_invoked=false
foundry_spark_invoked=false
foundry_input_dataset_count=0
foundry_output_dataset_count=0
foundry_style_input_dataset_count=1
foundry_style_output_dataset_count=2
staged_input_bytes
staged_input_transform_execution_performed=true
shardloom_execution_mode=local_foundry_style_generated_and_staged_transform_smoke
split_count=0
memory_budget_bytes=null
output_evidence_dataset_written=true
fallback_attempted=false
external_engine_invoked=false
public_foundry_claim_allowed=false
claim_gate_status=fixture_smoke_only
```

A real Foundry scale proof must distinguish Foundry orchestration from ShardLoom execution. Foundry
may orchestrate a transform, but Foundry Spark, virtual tables, Snowflake, Databricks, BigQuery, or
other managed compute cannot be silently reported as ShardLoom execution. Evidence dataset output is
mandatory before any proof claim can be promoted.

The current local proof remains report-only for scale. It does not invoke Foundry runtime, Foundry
compute, Foundry Spark, managed-platform execution, object-store writes, package publication, or
production Foundry support.
