# Vortex Runtime Utilization Audit

## Purpose

This document records the Vortex-first runtime utilization surface for ShardLoom.

The goal is not to repeat that ShardLoom can read `.vortex` files. The goal is to make visible how
much of Vortex's runtime capability stack ShardLoom actually uses, wraps, or keeps blocked:

```text
arrays as execution objects
deferred and layered execution
Scan API Source/Sink/Split work units
field masks and dynamic predicate ordering
layout strategy and pruning
I/O coalescing and prefetch behavior
sessions and registries
device residency
extension types
benchmark discipline
Vortex query-engine integrations as baselines only
```

## Source Posture

This audit treats the public Spiral/Vortex direction as a design signal, not as a ShardLoom support
claim.

Relevant public references:

- [General Catalyst's Spiral investment note](https://www.generalcatalyst.com/stories/our-investment-in-spiral)
  frames Vortex around AI-native data infrastructure, random-access reads, scans/writes,
  GPU-native decompression, object-storage-native access, and multimodal/real-time systems.
- [Vortex arrays](https://docs.vortex.dev/concepts/arrays) are in-memory tree structures with data
  type, children, buffers, statistics, and vtables; the Vortex docs describe arrays as similar to a
  logical plan for decompression because operations can be deferred.
- [Vortex execution internals](https://docs.vortex.dev/developer-guide/internals/execution) have
  layered progress through `reduce`, `reduce_parent`, `execute_parent`, and the encoding's
  `execute` step.
- [Vortex Scan API](https://docs.vortex.dev/concepts/scanning) defines `Source`, `Sink`, scan
  requests, and independently executable splits. It also records filter/projection/limit pushdown,
  field masks for filter-only versus output columns, and dynamic predicate ordering using
  selectivity evidence.
- [Vortex layouts](https://docs.vortex.dev/concepts/layouts) cover hierarchical lazy storage such
  as flat, struct, chunked, dictionary, and zoned layouts.
- [Vortex I/O](https://docs.vortex.dev/developer-guide/internals/io) exposes backend-sensitive
  coalescing, concurrency, prefetch, memory backpressure, and segment cache behavior.
- [Vortex sessions and registries](https://docs.vortex.dev/developer-guide/internals/session) are
  explicit rather than hidden globals.
- [Vortex benchmarking guidance](https://docs.vortex.dev/developer-guide/benchmarking)
  distinguishes microbenchmarks from end-to-end SQL benchmarks and emphasizes clear timed scopes and
  deterministic setup.

## Report Surfaces

The code surface is intentionally report-only unless a later planned item promotes a provider path:

```text
VortexCapabilityUtilizationReport
VortexRuntimeUtilizationAuditReport
VortexScanExecutionSpineReport
VortexFieldMaskEvidence
VortexPredicateOrderingEvidence
VortexLayoutAdvisorReport
VortexArrayExecutionCertificate
ShardLoomSessionModelReport
```

These reports answer:

```text
Which Vortex capabilities are currently used?
Which capabilities are only wrapped as report surfaces?
Which capabilities are planned provider surfaces?
Which capabilities are blocked until runtime evidence exists?
Which integrations are allowed only as benchmark/oracle/reference baselines?
```

## Vortex-First Provider Check

Implementation note:

```text
Vortex-first provider check:
- Subject area: Vortex runtime utilization and execution-spine hardening.
- Upstream Vortex concepts checked:
  arrays, execution layers, Scan API, Source/Sink/Split, field masks, predicate ordering, layouts,
  I/O coalescing/prefetch, sessions/registries, device residency, extension types, benchmarks, and
  query-engine integrations.
- Decision:
  wrap_vortex_concept and blocked_until_vortex_or_shardloom_evidence.
- ShardLoom provider/report/certificate surface:
  VortexRuntimeUtilizationAuditReport, VortexScanExecutionSpineReport,
  VortexLayoutAdvisorReport, VortexArrayExecutionCertificate, and ShardLoomSessionModelReport.
- Residual handling:
  unsupported_blocked until a ShardLoom-native residual executor or admitted Vortex-native provider
  emits evidence.
- Materialization/decode boundary:
  required per provider path before support claims.
- Evidence added:
  report contracts, tests, and the GAR-0042A `vortex-api-inventory` source/split admission proof
  for the scoped local Vortex scan fixture path. GAR-0042B also adds the
  `shardloom.vortex_layout_device_managed_boundary.v1` matrix for layout/write, device execution,
  object-store I/O, and managed-platform comparison boundaries. GAR-0003-A adds the
  `shardloom.vortex_segment_extraction_admission.v1` sparse patch/fill segment extraction blocker
  with deterministic diagnostics and no-fallback evidence requirements.
- Gates still blocked:
  generalized upstream Source/Split runtime, field-mask evidence, predicate-ordering evidence,
  sparse segment extraction runtime, layout/write advisor evidence, object-store I/O metrics,
  device residency, and trace-backed array execution layers.
- fallback_attempted=false:
  preserved.
```

`GAR-PERF-2F` now has scoped follow-through from report-only `ShardLoomSessionModelReport` to
in-process prepared/native session evidence for local artifacts. The current slice keeps registries
explicit, preserves no hidden globals, exposes session/cache lifecycle evidence, and avoids
daemon/service, remote-server, public Python API, production, or performance claims.

`GAR-PERF-2B` is the planned evidence-aware logical optimizer pass. It should place optimizer rule
registry and trace evidence before Vortex Scan pushdown, encoded-kernel, or fused-pipeline promotion
so every rewrite records admitted/applied/blocked/unsupported status, before/after plan digests,
rewrite safety, materialization boundaries, `evidence_preserved=true`, `fallback_attempted=false`,
and `external_engine_invoked=false`. Optimizer traces do not prove Vortex-native provider coverage or
public performance.

`GAR-IOREUSE-1` is the planned reusable I/O state and cross-format fanout follow-through. It should
check Vortex Source/Sink/Split, file I/O, prepared artifact, and output concepts before introducing
parallel ShardLoom abstractions. The planned path is `InputAdapter -> SourceState ->
VortexPreparedState -> ExecutionPlan -> OutputPlan -> SinkArtifact`, with input and output formats
decoupled. SourceState, VortexPreparedState, OutputPlan, cache invalidation, and fanout benchmark
rows must preserve Native I/O, materialization/decode, output metadata, no-fallback, and claim-gate
evidence. Object-store runtime and table/lakehouse commits remain blocked unless separately
admitted.

`GAR-PERF-2C` is the planned Vortex Scan API pushdown completion pass. It turns existing
source-backed scan evidence into a complete per-scenario-family filter/projection/limit
pushdown-or-blocker matrix with filter-only and output-column read sets. It must preserve
materialization/decode evidence, `fallback_attempted=false`, `external_engine_invoked=false`, and an
explicit claim gate. Pushdown evidence remains Scan/source-boundary evidence only; it is not an
encoded-native operator claim, generalized Source/Split runtime claim, object-store/lakehouse claim,
SQL/DataFrame claim, or public performance claim.

`GAR-PERF-2D` is the planned compressed/encoded kernel registry pass. It should wrap Vortex
encoding/layout facts in ShardLoom kernel admission rows for bitpacked, sequence, dictionary,
constant, sorted/statistics, and FSST/dictionary string cases where available. Registry rows must
separate `kernel_admitted` from `kernel_executed`, record canonicalization/decode/materialization
boundaries, preserve validity semantics, and keep `encoded_native_claim_allowed=false` until
end-to-end evidence passes.

`GAR-PERF-2E` is the planned fused operator pipeline pass. It should combine admitted prepared/native
source-backed scan boundaries with ShardLoom-native residual operators for filter/projection/limit,
filter/aggregate, filter/group-by, and top-k/projection families. A fused row must prove correctness
digest parity with the unfused ShardLoom-native path and expose row counts plus materialization
avoidance evidence. Fusion remains residual-native unless later representation-state certificates
prove encoded-native execution.

`GAR-PERF-2G` adds scoped allocation/resource-profile evidence and buffer-pool blocker reporting for
prepared/native allocation families such as result buffers, temporary vectors, hash tables,
dictionary/string state, and source-state arrays. Current rows report allocation counts/bytes and
peak RSS as `not_available`, `buffer_pool_enabled=false`, `buffer_reuse_count=0`, deterministic
reuse blockers, no unsafe lifetime shortcuts, `allocation_fallback_attempted=false`, and
`allocation_external_engine_invoked=false`. Buffer reuse evidence is resource-profile evidence only;
it is not a Vortex encoded-native, performance, memory-efficiency, daemon/session,
object-store/lakehouse, or production claim.

`GAR-PERF-2H` is the planned optimized build-profile and PGO benchmark lane. It should keep
`release-lto`, `release-pgo`, and `release-native-benchmark` evidence separate from Vortex provider
evidence, record rustc/cargo versions, target triple, target CPU policy, LTO/PGO/native status,
correctness digest, no-fallback fields, and claim gate, and keep `target-cpu=native`
benchmark-only. Build-profile evidence does not prove Vortex-native operator coverage or public
performance.

## Layout Device Managed Boundary Matrix

GAR-0042B adds `VortexLayoutDeviceManagedBoundaryMatrix` as a report-only claim boundary. It has four
rows:

```text
layout_write_boundary
device_execution_boundary
object_store_io_boundary
managed_platform_comparison_boundary
```

Every row records:

```text
support_status
claim_gate_status=not_claim_grade
claim_boundary
evidence_required
benchmark_ref
release_gate_ref
unsupported_diagnostic_code
blocker_id
runtime_execution=false
write_io=false
object_store_io=false
device_execution=false
managed_platform_execution=false
external_engine_invoked=false
fallback_attempted=false
```

Managed-platform rows are comparison-only and do not add dependencies, credentials, or platform
execution. Device and object-store rows cannot satisfy native claims until execution certificates,
Native I/O certificates, and workload-scoped metrics exist.

## Non-Goals

This audit does not authorize:

```text
new upstream Vortex API calls
object-store reads
Vortex writes
GPU/device execution
external query-engine integrations as runtime fallback
managed-platform benchmark lanes
runtime provider promotion
performance claims
```

## Promotion Requirements

A future runtime promotion must add:

```text
provider kind
Vortex crate/version and API surface
feature gate
ShardLoom admission policy
array-tree and execution-step traces
source/split refs
Native I/O certificate refs
execution certificate refs
field-mask evidence where Scan pushdown is used
predicate-ordering evidence where dynamic ordering is used
layout/write/read tradeoff evidence where layout advice is claimed
object-store request/coalescing/prefetch metrics where remote I/O is used
device transfer/residency evidence where GPU/device paths are claimed
deterministic unsupported diagnostics for blocked residuals
fallback_attempted=false
external_engine_invoked=false
```

## Work-Avoidance Evidence Schema

GAR-FLOW-2D adds a benchmark/report schema for work-avoidance evidence before any optimization or
runtime promotion claim. Benchmark rows use only these statuses:

```text
measured
not_available
unsupported
not_applicable
```

Rows avoided, segments pruned, bytes avoided, encoded-vector reuse, and pushdown proof each carry a
status, value, and reason. `not_available` means the metric is meaningful but not yet measured; it
must not be interpreted as zero. This is especially important for Vortex Scan API features such as
filter/projection pushdown, split scheduling, pruning, and compressed-array reuse because those
features require explicit evidence before ShardLoom can make performance, Spark-displacement, or
best-default claims.

## Relationship To Planned Work

This audit sharpens Priority 2.6 and precedes Priority 2.7. It does not replace the planned
source-backed correctness/benchmark matrix. The next runtime-heavy work remains measured
source-backed correctness and benchmark population, with Vortex integrations kept as
baseline/oracle/reference rows only.
