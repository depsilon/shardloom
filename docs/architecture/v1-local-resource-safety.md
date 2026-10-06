<!-- SPDX-License-Identifier: Apache-2.0 -->

# V1 Local Resource Safety

Schema marker: `shardloom.v1_local_resource_safety.v1`.

This document distinguishes the original bounded v1 resource gate from the current local engine.
The gate alone does not establish general larger-than-memory execution or production reliability.
Current native ordering spill and its failure/cleanup evidence are defined by the
[native relational resource contract](native-relational-resources-2026-10-02.md), merged in PR
#1502. A query memory grant is not a total-process RSS ceiling.

## Current Local Engine Boundary

Public SQL, Python/DataFrame and CLI workflows carry the same declared memory and parallelism
through native execution and local output. Admitted native batches retain their ownership and
reservation credits through the consumer. Explicit native spill is available for selected
COUNT/DISTINCT/numeric-sort providers and nullable multi-key relational ordering. Relational
ordering shares the query memory pool and disk quota across nested ordering stages and merges;
spill permission is disabled by default.

The relational resource acceptance covers complete output through all eight local writers,
slow/failing consumers, cancellation, corrupt/truncated/replaced runs, source mutation, quota
denial and owned cleanup. It delivered 240,003 ordered rows under an 8 MiB shared reservation
grant and a 1 MiB flush threshold. This is evidence for that declared operator and allocation
scope, not a universal memory or recovery guarantee. See the linked contract for exact source,
executable, test and immutable packet identities.

General aggregate/join/window spill, complete reader/codec/upstream scratch accounting and
whole-process RSS bounds remain open. Other operators must retain their own resource admission
and deterministic denials. A supported reader, large input or successful ingest does not by
itself establish that every query can complete under the same resource limit.

## Original V1 Gate Scope

The retained `shardloom.v1_local_resource_safety.v1` gate checks a narrower evidence set:

- deterministic memory-budget denial before process OOM for an admitted local fixture.
- reservation release and cleanup after the denial fixture.
- side-effect-free retry gate planning.
- side-effect-free cancellation gate planning with cleanup-completed evidence.
- an admitted public native Vortex aggregate route that carries the shared resource envelope,
  memory-admission decision, reservation release, state-budget, spill fail-closed, native I/O
  certificate, and no-fallback evidence through the public workflow facade.
- prepared-state reuse boundaries that avoid hidden global caches and label internal source smoke routes
  as non-persistent.
- local output/sink scope evidence that reports write policy, replay, and partial-write cleanup
  boundaries.
- no fallback execution and no external engine invocation.

## Gate Evidence

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
- `run cli --input shardloom-vortex/tests/fixtures/local_primitive_struct_five.vortex --input-format vortex --request collect --execution-policy native_vortex`
- `cg14-memory-runtime-hardening-gate --format json`
- `fault-tolerance-promotion-gate --format json`
- `target/v1-source-prepared-state-scope-report.json`
- `target/v1-local-output-sink-scope-report.json`

## Gate Claim Boundary

Allowed after the gate passes:

- local v1 resource-safety evidence is present.
- memory reservation denial fails before OOM for the fixture.
- cleanup evidence is present for the fixture and local output/prepared-state reports.
- retry and cancellation gates remain side-effect-free.
- one admitted public Vortex aggregate route proves resource-envelope, memory-admission,
  reservation-release, and state-budget evidence survives the public facade boundary.

The original gate alone makes:

- no larger-than-memory claim.
- no native spill runtime claim; use the separate native resource contract and live execution
  evidence for the admitted spill families above.
- no distributed OOM/resource claim.
- no production reliability claim.
- no public package or release claim.
- no Spark, DataFusion, DuckDB, Polars, Velox, or other external engine fallback claim.

## Technique Review

The v1 boundary uses ShardLoom-native resource controls where they are already meaningful:

- Dynamic admission is represented by deterministic budget denial and gate-open/closed signals.
- Capillary work units remain required for future resource-derived chunk sizing; v1 does not claim
  broad runtime chunk resizing.
- PulseWeave pressure signals and capillary work-unit labels are emitted by the public native
  Vortex aggregate route, and its derived memory reservation is admitted and released against the
  same local resource envelope; broader allocator integration and in-flight adaptive resizing remain
  outside this local gate.
- Metadata-first checks keep scope reports and readiness validation local and side-effect-free.
- Timing-surface and evidence-tier controls are preserved by reporting cleanup/proof fields
  separately from hot runtime claims.

## Remaining Operational Acceptance

The following need separate implementation or acceptance before broader support is promised:

- general aggregate/join/window and other unadmitted operator spill transitions.
- reader/codec/upstream scratch accounting across complete local workflows.
- larger-than-resident-state guarantees beyond the admitted spill families.
- workload-wide pressure, interruption and recovery acceptance for a declared production envelope.
- object-store recovery.
- distributed retry/cancellation/recovery.
- allocator integration and adaptive memory pressure reaction across all operators.

The [local-engine preview exit criteria](../release/production-certification-gate.md#local-engine-preview-exit-criteria)
define how these obligations relate to a scoped stable local release. The fixture gate and the
native spill implementation do not independently satisfy that production decision.
