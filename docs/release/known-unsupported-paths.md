<!-- SPDX-License-Identifier: Apache-2.0 -->

# Known Unsupported Paths

Status: release-gate input. This document scopes public claims and does not authorize new runtime
behavior.

ShardLoom currently exposes a certified local Vortex analytics slice and evidence-first planning
surfaces. These unsupported surfaces are explicit boundaries, not accidental blockers to the
finished-product v1 support rows. A surface is outside the current finished-product v1 support boundary
until a matching phase-plan item closes with implementation, correctness, benchmark, security,
documentation, and no-fallback evidence.

The v1 inclusion queue is tracked in
[`docs/release/v1-inclusion-scope-matrix.md`](v1-inclusion-scope-matrix.md). Broad platform
families that are marked as v1 candidates pending feasibility are not outside v1 by default;
deferred rows require deterministic unsupported diagnostics, `fallback_attempted=false`, and
`external_engine_invoked=false`.

ShardLoom must not be described as a broad replacement for every SQL, DataFrame, streaming,
object-store, platform, or Foundry workload until those exact paths have claim-grade evidence.

## Unsupported Or Future Before Public Claims

- broad SQL/DataFrame execution
  - `capabilities sql` and `capabilities dataframe` expose the GAR-0001A-A planner-readiness matrix
    as report-only/unsupported evidence, not SQL parser/planner or DataFrame runtime support
- broad live/hybrid production behavior
- object-store runtime
- distributed scheduler/runtime
- lakehouse/table runtime claims beyond report-only catalog/table compatibility planning
- real Foundry proof-of-use beyond the local Foundry-style smoke; Foundry remains a future
  validation target only, with no Palantir endorsement or Foundry-certified claim
- Foundry dataset source/sink execution
- external platform execution through Spark, Snowflake, Databricks, BigQuery, Trino, Ray, Dask, or
  Velox
- direct transient compatibility execution as a Vortex-native claim
- native Vortex operator coverage beyond the documented supported scenarios
- layout/write, device/GPU, object-store I/O, and managed-platform comparison claims unless
  `vortex_layout_device_managed_boundary_ref` has claim-grade execution, Native I/O, benchmark, and
  no-fallback evidence for the exact workload
- distributed, object-store, and lakehouse runtime claims unless
  `global_architecture_runtime_claim_gate` has workload-scoped execution, Native I/O, credential,
  benchmark, policy/no-fallback, and release-readiness evidence for the exact claim
- production security posture beyond the current release-gate evidence

## Required Reporting Rule

Unsupported paths must emit deterministic unsupported or blocked diagnostics. They must preserve:

```text
fallback_attempted=false
external_engine_invoked=false
```

External engines may appear only as local benchmark baselines, optional migration references, or
oracles in explicitly labeled tests. They must not execute unsupported ShardLoom work as fallback.
