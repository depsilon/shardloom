<!-- SPDX-License-Identifier: Apache-2.0 -->

# Foundry Integration Pack Readiness

Status: Priority 9 local proof and report-schema posture. This document does not invoke Foundry,
publish packages, create tags, add secrets, or authorize Foundry compute as ShardLoom execution.

## Scope

Foundry is an optional integration pack. It must make ShardLoom easier to install, import, run,
certify, and inspect inside a Foundry Python code repository without turning Foundry Spark,
Snowflake, Databricks, BigQuery, virtual tables, or external compute into ShardLoom-native
execution.

## Maturity Ladder

| Level | Meaning |
| --- | --- |
| F0 | declared only |
| F1 | docs and package posture |
| F2 | local Foundry-style transform fixture |
| F3 | CLI/import smoke proof |
| F4 | staged dataset path proof |
| F5 | certificate/metrics output proof |
| F6 | Data Health/Data Expectations bridge proof |
| F7 | governed source/sink transaction proof |
| F8 | benchmark/report boundary proof |
| F9 | release artifact install proof |
| F10 | workload-certified Foundry deployment |

Current state is F5 local proof plus local Foundry-style result/evidence dataset output. F10 remains
future.

## Required Report Schemas

Priority 9 report schemas:

- `FoundryExecutionContext`
- `FoundryDatasetTransactionReport`
- `FoundryBranchContextReport`
- `FoundryPreviewModeReport`
- `FoundryReleaseReadinessReport`
- `FoundryDatasetSource`
- `FoundryDatasetSink`
- `FoundryCertificateOutput`
- `FoundryIncrementalRunReport`
- `FoundryDataHealthBridge`
- `FoundryLineageFacet`
- `FoundryScheduleBuildReport`
- `FoundryDataConnectionBoundaryReport`
- `FoundryGovernanceBoundaryReport`
- `FoundryS3DatasetAdapter`
- `FoundryVirtualTableSource`
- `FoundryVirtualTableSink`
- `FoundryVirtualTableRef`
- `FoundryExternalComputeBoundaryReport`
- `FoundryIcebergTableSource`
- `FoundryIcebergTableSink`
- `FoundryMediaSetSource`
- `FoundryVirtualMediaSetSource`
- `FoundryMediaSetSink`
- `FoundryMediaExtractionBoundaryReport`
- `FoundryModelCallBoundaryReport`
- `FoundryEmbeddingBoundaryReport`
- `FoundryOntologyMappingReport`
- `FoundryFunctionSurface`
- `FoundryAipLogicBridge`
- `FoundryAipLogicBoundaryReport`
- `FoundryModelBoundaryReport`
- `FoundryUnstructuredWorkflowCertificate`
- `FoundryScenarioBoundaryReport`
- `FoundryByocImageReport`
- `FoundryComputeModuleSurface`
- `FoundryComputeModuleReadinessReport`
- `FoundryMarketplaceStarterProduct`

These names are report contracts until real platform integration exists.

## No-Fallback Boundary

All Foundry surfaces must preserve:

```text
fallback_attempted=false
external_engine_invoked=false
foundry_compute_invoked=false unless explicitly labeled external boundary
```

Virtual tables and external compute are governed handles, baselines, or migration/oracle references.
ShardLoom-native execution requires staged/native data plus execution and Native I/O certificates.

## Local Proof

For the user-facing starter path, see
[`docs/foundry/dev-stack-starter-kit.md`](dev-stack-starter-kit.md) and
`shardloom.foundry_dev_stack_starter_kit.v1`.

For the Foundry package/proof boundary, see
[`docs/foundry/package-proof-boundary-matrix.md`](package-proof-boundary-matrix.md) and
`shardloom.foundry_package_proof_boundary_matrix.v1`. That report-only matrix separates the current
local Foundry-style proof rows from blocked `shardloom-foundry` package publication, Artifact
Repository publication, Foundry service invocation, Compute Module invocation, virtual-table native
execution, dataset transaction runtime, and F10 workload-certified deployment. It preserves:

```text
foundry_package_proof_boundary_matrix_status=report_only
foundry_package_proof_boundary_matrix_ref=foundry_package_proof_boundary_matrix
foundry_runtime_invoked=false
foundry_compute_invoked=false
foundry_spark_invoked=false
fallback_attempted=false
external_engine_invoked=false
public_foundry_claim_allowed=false
claim_gate_status=not_claim_grade
```

Use:

```powershell
python scripts\foundry_proof_of_use.py --rows 64 --iterations 1
```

The proof emits:

```text
target/foundry-proof-of-use/report.json
target/foundry-proof-of-use/certificate-output.json
target/foundry-proof-of-use/local-vortex-benchmark-smoke.json
```

This covers local package/import posture, deterministic CLI binary resolution, no-dataset smoke,
source-free generated output, a staged local CSV transform, local Foundry-style result/evidence
dataset output, local ShardLoom execution smoke, certificate output, benchmark metrics output,
materialization/staging boundary refs, and no-fallback evidence.

The same report includes `shardloom.foundry_generated_output_boundary.v1`. That boundary keeps
local dev-stack generated-output proof separate from current no-dataset smoke and real Foundry
runtime claims: `foundry_output_api_required=true`, `foundry_style_output_api_invoked=true`,
`foundry_style_result_dataset_written=true`, `foundry_style_evidence_dataset_written=true`,
`foundry_output_api_invoked=false`, `foundry_result_dataset_written=false`,
`foundry_evidence_dataset_written=false`, `direct_s3_read_invoked=false`,
`direct_s3_write_invoked=false`, `object_store_write_invoked=false`,
`object_store_commit_invoked=false`, `fallback_attempted=false`, and
`external_engine_invoked=false`.
