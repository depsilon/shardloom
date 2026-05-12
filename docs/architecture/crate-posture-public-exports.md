# Crate Posture And Public Exports

## Purpose

This document records the Priority 2.8 crate-level posture cleanup. It is documentation only and
does not authorize runtime expansion, dependency expansion, object-store I/O, package publication,
external engine invocation, or fallback execution.

## Public Export Rule

ShardLoom crate exports should be understood by role:

```text
executable local/prepared/source-backed paths
report-only contract surfaces
blocked/deferred runtime surfaces
future provider/adapter surfaces
prohibited external fallback
```

Historical setup-phase or compile-only wording should not describe current crates unless it is
explicitly labeled historical.

## Crate Posture

| Crate | Current posture | Executable or narrow local paths | Report-only / blocked surfaces |
| --- | --- | --- | --- |
| `shardloom-core` | Provider-neutral contracts and evidence vocabulary. | None directly; provider crates execute. | Diagnostics, encoded vocabulary, Native I/O certificates, benchmark/correctness, policy, release, capability, wrapper, unstructured, and governance reports. |
| `shardloom-plan` | Provider-neutral plan artifacts and planning reports. | Typed plans for local Vortex primitives plus prepared/source-backed/reader-backed encoded Vortex dispatch. | Estimate, explain, scan planning, object-store planning, optimizer posture, input planning, and plan import/export metadata. |
| `shardloom-exec` | Provider-neutral execution facade and runtime contracts. | `execute_with_provider` dispatch through admitted providers; neutral `execute` reports or blocks. | Memory, recovery, retry, cancellation, spill, runtime, streaming, and promotion-gate reports. |
| `shardloom-vortex` | Vortex-facing provider, execution, and evidence surfaces. | Narrow local primitive, prepared encoded, source-backed, reader-backed, feature-gated local artifact, and top-level provider paths where evidence exists. | Scan API, layout/write strategy, object-store, device/GPU, extension dtype, table/catalog, integration, benchmark, and broad adapter surfaces until evidence gates pass. |
| `shardloom-cli` | Current command router and JSON/text protocol surface. | Narrow local Vortex commands and explicit feature-gated local artifact helpers. | Report-only planning, diagnostics, promotion gates, release/package readiness, and capability discovery. |

## Export Grouping

`shardloom-plan` exports are documented in groups for:

```text
top-level execution-facade plan variants
input/scan planning
object-store planning
optimizer/adaptive planning
native plan IR and interop metadata
```

`shardloom-exec` exports are documented in groups for:

```text
memory/spill planning
recovery/retry/cancellation/cleanup/commit promotion
adaptive sizing
streaming and zero-copy boundary planning
spill-payload helpers
runtime task-graph planning
```

`shardloom-vortex` exports are documented in groups for:

```text
Vortex compatibility/provider/runtime-utilization reports
encoded-read, metadata, physical-kernel, and selection-vector surfaces
narrow executable encoded/local/source-backed paths
write/output/commit readiness and local artifact helpers
benchmark-only surfaces
runtime bridge, scheduler, bounded execution, local engine, and provider exports
metadata-summary helpers
```

`shardloom-cli` remains a large command router until the later typed-envelope and CLI modularity
lane. Until then, its crate docs define the public posture: unsupported behavior is deterministic,
external engines are baselines or oracles only, and fallback execution remains prohibited.

## Prohibited Fallback

The following may appear in benchmark, oracle, migration, or design-reference rows only:

```text
Spark
DataFusion
DuckDB
Polars
Velox
Trino
Dask
Ray
Vortex query-engine integrations
Snowflake
Databricks
BigQuery
Foundry compute pushdown
```

They must not execute unsupported ShardLoom runtime work as fallback.
