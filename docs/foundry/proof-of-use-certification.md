<!-- SPDX-License-Identifier: Apache-2.0 -->

# Foundry Proof-Of-Use Certification

Status: P9.6 local proof. This proof is Foundry-style only; it does not import Foundry packages or
invoke Foundry services.

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
- `certificate_metrics_dataset_output_written`
- `materialization_staging_boundary_report_ref`
- `foundry_generated_output_fanout_status`
- `foundry_generated_output_fanout_posture`
- `foundry_scale_proof_boundary_status`
- `foundry_scale_proof_boundary`
- `direct_s3_write_invoked=false`
- `object_store_write_invoked=false`
- `foundry_runtime_invoked=false`
- `foundry_compute_invoked=false`
- `foundry_spark_invoked=false`
- `foundry_input_dataset_count=0`
- `foundry_output_dataset_count=0`
- `staged_input_bytes`
- `shardloom_execution_mode=local_foundry_style_smoke_only`
- `split_count=0`
- `memory_budget_bytes=null`
- `output_evidence_dataset_written=false`
- `snowflake_databricks_bigquery_invoked=false`
- `virtual_tables_native_execution_claimed=false`
- `fallback_attempted=false`
- `external_engine_invoked=false`
- `public_foundry_claim_allowed=false`
- `local_foundry_style_proof_claim_allowed=true` when the local smoke passes

## Claim Scope

The only allowed claim from this proof is:

```text
local_foundry_style_transform_and_local_vortex_execution_smoke_only
```

It is not a Foundry production claim, Foundry package publication claim, Foundry virtual-table native
execution claim, or external compute pushdown claim.

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
  - ShardLoom consumes rows as a generated/literal source in the scoped local JSONL smoke when
    deterministic generation evidence exists
  - Foundry output evidence is still required before any Foundry generated-output claim
- `engine_native_generated_source`:
  - ShardLoom executes generator nodes such as `range`, `sequence`, `values`, `literal_table`,
    calendar/date dimension, or deterministic synthetic profile
  - the scoped local `range` JSONL smoke is supported outside Foundry; other generator nodes remain
    report-only
  - ShardLoom writes output and emits generated-source and output evidence

Generated-output proof fields should align with the `GAR-GEN-1` contract. The CLI/Python capability
view exposes that vocabulary as `shardloom.generated_source_certificate_contract.v1`; GAR-GEN-1C
and GAR-GEN-1D emit the fields only for scoped local user-row and range JSONL smokes, not for
Foundry:

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

`GAR-IOREUSE-1G` extends this posture with report-only output fanout evidence. The intended future
runtime smoke is:

```text
no input dataset
generate deterministic source
prepare through ShardLoom/Vortex
write result dataset
write evidence dataset
```

The current report now includes a report-only
`shardloom.foundry_generated_output_fanout_posture.v1` object. It records the required field
vocabulary without pretending a generated-output runtime path executed:

```text
support_status=report_only
admission_status=blocked_until_generated_source_and_foundry_output_api_evidence
generated_output_execution_performed=false
no_dataset_smoke_separate_from_generated_output=true
input_dataset_count=0
source_io_performed=false
generated_source_created=false
generated_source_kind=planned_deterministic_literal_table
generated_source_certificate_status=not_emitted_report_only
source_native_io_certificate_status=not_applicable_no_source_dataset
output_plan_id=null
output_plan_reuse_hit=false
fanout_output_count=0
output_io_performed=false
output_native_io_certificate_status=not_emitted_report_only
result_dataset_output_status=not_written_report_only
evidence_dataset_output_status=not_written_report_only
foundry_output_api_required=true
foundry_runtime_invoked=false
foundry_compute_invoked=false
foundry_spark_invoked=false
direct_s3_write_invoked=false
object_store_write_invoked=false
fallback_attempted=false
external_engine_invoked=false
claim_gate_status=not_claim_grade
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

No-input smoke and generated-output execution remain separate. A Foundry-style generated-output
fanout row is not a Foundry production claim, Foundry package publication claim, direct S3/object
store write claim, or Spark fallback claim.

## Scale Proof Boundary

`GAR-SCALE-1H` adds a report-only Foundry scale proof boundary:

```text
schema_version=shardloom.foundry_scale_proof_boundary.v1
support_status=report_only
proof_boundary_status=blocked_until_real_foundry_runtime_and_evidence_dataset
foundry_runtime_invoked=false
foundry_compute_invoked=false
foundry_spark_invoked=false
foundry_input_dataset_count=0
foundry_output_dataset_count=0
staged_input_bytes
shardloom_execution_mode=local_foundry_style_smoke_only
split_count=0
memory_budget_bytes=null
output_evidence_dataset_written=false
fallback_attempted=false
external_engine_invoked=false
public_foundry_claim_allowed=false
claim_gate_status=not_foundry_scale_grade
```

A real Foundry scale proof must distinguish Foundry orchestration from ShardLoom execution. Foundry
may orchestrate a transform, but Foundry Spark, virtual tables, Snowflake, Databricks, BigQuery, or
other managed compute cannot be silently reported as ShardLoom execution. Evidence dataset output is
mandatory before any proof claim can be promoted.

The current local proof remains report-only for scale. It does not invoke Foundry runtime, Foundry
compute, Foundry Spark, managed-platform execution, object-store writes, package publication, or
production Foundry support.
