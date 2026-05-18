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

Current state is F3-F5 local proof only. F10 remains future.

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
explicit staged dataset path, local ShardLoom execution smoke, certificate output, benchmark metrics
output, materialization/staging boundary refs, and no-fallback evidence.

The same report includes a report-only
`shardloom.foundry_generated_output_boundary.v1` object. That boundary keeps future Foundry
generated-output proof separate from current no-dataset smoke: `foundry_output_api_required=true`,
`foundry_output_api_invoked=false`, `foundry_result_dataset_written=false`,
`foundry_evidence_dataset_written=false`, `direct_s3_read_invoked=false`,
`direct_s3_write_invoked=false`, `object_store_write_invoked=false`,
`object_store_commit_invoked=false`, `fallback_attempted=false`, and
`external_engine_invoked=false`.
