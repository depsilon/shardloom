<!-- SPDX-License-Identifier: Apache-2.0 -->

# Foundry Package And Proof Boundary Matrix

GAR-0036-A defines the Foundry package/proof boundary as a machine-readable report-only matrix. It
separates the local Foundry-style proof that exists today from real Foundry package publication,
service invocation, virtual-table native execution, dataset transactions, Compute Modules, Artifact
Repository publication, and F10 deployment certification.

The canonical matrix is `docs/foundry/package-proof-boundary-matrix.json`:

```text
schema_version=shardloom.foundry_package_proof_boundary_matrix.v1
gar_id=GAR-0036-A
support_status=report_only
claim_gate_status=not_claim_grade
foundry_runtime_invoked=false
foundry_compute_invoked=false
foundry_spark_invoked=false
fallback_attempted=false
external_engine_invoked=false
public_foundry_claim_allowed=false
```

## Rows

| Row | Status | Meaning |
| --- | --- | --- |
| `local_style_transform_fixture` | `smoke_supported` | Local source-checkout transform shape; not real Foundry runtime. |
| `local_certificate_metrics_output` | `smoke_supported` | Local certificate/metrics JSON plus local result/evidence dataset-shaped outputs; not a real Foundry dataset write. |
| `shardloom_foundry_package` | `blocked` | No `shardloom-foundry` package publication or install proof exists. |
| `artifact_repository_publication` | `blocked` | No Foundry Artifact Repository upload/install/rollback proof exists. |
| `foundry_service_invocation` | `blocked` | No Foundry service invocation or runtime context proof exists. |
| `compute_module_surface` | `blocked` | No Compute Module packaging or invocation proof exists. |
| `virtual_table_native_execution` | `blocked` | Virtual tables are not ShardLoom-native execution proof. |
| `dataset_transaction_runtime` | `blocked` | No result/evidence dataset transaction proof exists. |
| `f10_workload_certified_deployment` | `blocked` | No workload-certified Foundry deployment exists. |

## Report Fields

`scripts/foundry_proof_of_use.py` embeds the same posture under:

```text
foundry_package_proof_boundary_matrix_status
foundry_package_proof_boundary_matrix_ref
foundry_package_proof_boundary_matrix
```

The row-level fields keep Foundry external compute from being reported as ShardLoom execution:

```text
foundry_runtime_invoked=false
foundry_compute_invoked=false
foundry_spark_invoked=false
foundry_output_api_invoked=false
compute_module_invoked=false
virtual_table_native_execution_claimed=false
dataset_transaction_runtime_allowed=false
f10_deployment_certified=false
fallback_attempted=false
external_engine_invoked=false
```

## Claim Boundary

Allowed current claim:

```text
local_foundry_style_generated_output_and_staged_transform_smoke_only
```

Not allowed:

- no Foundry production support claim
- no `shardloom-foundry` package claim
- no Foundry Marketplace or Artifact Repository claim
- no service invocation claim
- no Compute Module claim
- no virtual-table native execution claim
- no dataset transaction runtime claim
- no F10 workload-certified deployment claim
- no Spark fallback claim
- no external compute pushdown claim

## Validation

Use:

```powershell
python scripts\check_foundry_package_proof_boundary.py
```

The validator checks that the matrix and docs preserve no-runtime, no-Foundry-compute,
no-Spark, no-fallback, no-external-engine, and no-public-Foundry-claim posture.
