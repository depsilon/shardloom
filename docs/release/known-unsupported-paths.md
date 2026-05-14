<!-- SPDX-License-Identifier: Apache-2.0 -->

# Known Unsupported Paths

Status: release-gate input. This document scopes public claims and does not authorize new runtime
behavior.

ShardLoom currently exposes a certified local Vortex analytics slice and evidence-first planning
surfaces. It must not be described as a broad replacement for every SQL, DataFrame, streaming,
object-store, platform, or Foundry workload until those paths have claim-grade evidence.

## Unsupported Or Future Before Public Claims

- broad SQL/DataFrame execution
- broad live/hybrid production behavior
- object-store runtime
- distributed scheduler/runtime
- real Foundry proof-of-use beyond the local Foundry-style smoke
- Foundry native dataset source/sink execution
- external platform execution through Spark, Snowflake, Databricks, BigQuery, Trino, Ray, Dask, or
  Velox
- direct transient compatibility execution as a Vortex-native claim
- native Vortex operator coverage beyond the documented supported scenarios
- layout/write, device/GPU, object-store I/O, and managed-platform comparison claims unless
  `vortex_layout_device_managed_boundary_ref` has claim-grade execution, Native I/O, benchmark, and
  no-fallback evidence for the exact workload
- production security posture beyond the current release-gate evidence

## Required Reporting Rule

Unsupported paths must emit deterministic unsupported or blocked diagnostics. They must preserve:

```text
fallback_attempted=false
external_engine_invoked=false
```

External engines may appear only as local benchmark baselines, optional migration references, or
oracles in explicitly labeled tests. They must not execute unsupported ShardLoom work as fallback.
