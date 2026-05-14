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
- `foundry_runtime_invoked=false`
- `foundry_compute_invoked=false`
- `foundry_spark_invoked=false`
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
