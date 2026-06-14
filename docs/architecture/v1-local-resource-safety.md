<!-- SPDX-License-Identifier: Apache-2.0 -->

# V1 Local Resource Safety

Schema marker: `shardloom.v1_local_resource_safety.v1`.

This document defines the v1 local resource, cancellation, and cleanup boundary for supported
source-checkout and local package workflows. It does not authorize larger-than-memory execution,
native spill runtime, distributed execution, object-store recovery, or public production claims.

## Supported V1 Boundary

ShardLoom v1 local resource safety is intentionally narrow and evidence-backed:

- deterministic memory-budget denial before process OOM for an admitted local fixture.
- reservation release and cleanup after the denial fixture.
- side-effect-free retry gate planning.
- side-effect-free cancellation gate planning with cleanup-completed evidence.
- prepared-state reuse boundaries that avoid hidden global caches and label direct transient routes
  as non-persistent.
- local output/sink scope evidence that reports write policy, replay, and partial-write cleanup
  boundaries.
- no fallback execution and no external engine invocation.

## Required Evidence

The v1 resource-safety report is produced by:

```text
python scripts/check_v1_local_resource_safety.py
```

The report writes:

```text
target/v1-local-resource-safety-report.json
```

The report validates these runtime and support surfaces:

- `pre-oom-memory-guard-smoke --format json`
- `retry-gate-plan retry-requested,retry-allowed,cleanup-completed --format json`
- `cancellation-gate-plan cancellation-requested,cleanup-required,cleanup-completed --format json`
- `cg14-memory-runtime-hardening-gate --format json`
- `fault-tolerance-promotion-gate --format json`
- `target/v1-source-prepared-state-scope-report.json`
- `target/v1-local-output-sink-scope-report.json`

## Claim Boundary

Allowed after the gate passes:

- local v1 resource-safety evidence is present.
- memory reservation denial fails before OOM for the fixture.
- cleanup evidence is present for the fixture and local output/prepared-state reports.
- retry and cancellation gates remain side-effect-free.

Not allowed after the gate passes:

- no larger-than-memory claim.
- no native spill runtime claim.
- no distributed OOM/resource claim.
- no production reliability claim.
- no public package or release claim.
- no Spark, DataFusion, DuckDB, Polars, Velox, or other external engine fallback claim.

## Technique Review

The v1 boundary uses ShardLoom-native resource controls where they are already meaningful:

- Dynamic admission is represented by deterministic budget denial and gate-open/closed signals.
- Capillary work units remain required for future resource-derived chunk sizing; v1 does not claim
  broad runtime chunk resizing.
- PulseWeave remains a future runtime-control path for in-flight work shaping and does not execute
  in this v1 local gate.
- Metadata-first checks keep scope reports and readiness validation local and side-effect-free.
- Timing-surface and evidence-tier controls are preserved by reporting cleanup/proof fields
  separately from hot runtime claims.

## Deferred Boundaries

The following remain unsupported until later implementation closes them with runtime evidence:

- native Vortex spill write/read runtime.
- spill cleanup execution over real spill artifacts.
- larger-than-memory local workloads.
- object-store recovery.
- distributed retry/cancellation/recovery.
- allocator integration and adaptive memory pressure reaction across all operators.
