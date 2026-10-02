<!-- SPDX-License-Identifier: Apache-2.0 -->

# Native relational resource and ordering continuation

Status: implementation contract; acceptance is pending. This follows the
[ordered composition unit](native-relational-composition-2026-10-02.md) under
PERF-03/06/07/10/12 and the existing CG-5/20/21 obligations. Active ordering belongs
to the [phase plan](phased-execution-plan.md). This contract does not close broader
reader/codec accounting, aggregate/join/window spill or total-process-memory work.

## Intended behavior

One execution request carries its memory and parallelism limits through collection
and each local writer. An explicit local spill request may let relational ordering
flush bounded sorted native batches and merge them into the same result consumer.
The user supplies storage permission and quota; derived SQL scopes do not create
another execution grant, another source preparation or an intermediate public file.

Preserve current flat scalar types, exact values, null-order requirements and stable
ties. Multi-key order, transformed inputs and complete output must use this shared
path. A spill buffer threshold controls when retained ordering input is flushed;
it is not a second query memory budget or an RSS guarantee. All charged run metadata,
reader work, key state, selections and output allocations remain in the existing
resident memory pool. Retaining a large backing buffer for a small selected batch
triggers native compaction into bounded blocks. A single oversized row, denied
reservation or exhausted disk quota fails explicitly. No unsupported operator
borrows another engine.

Spill is disabled by default. Configuration and route inspection remain inert,
including nonexistent workspace paths. Execution validates the existing caller-owned
workspace before creating a private owned directory. No source, output or unrelated
workspace file can be removed. A successful execution must finish verified cleanup
before returning its success certificate or allowing a writer to publish output.

## Existing components and extension boundaries

| Responsibility | Existing component | Extension in this unit |
| --- | --- | --- |
| Public resource request | `PublicWorkflowRouteRequest`, Python public workflow facade | Carry the same declared resources to collection and writers; parse spill configuration once at the shared native boundary. |
| Query admission | `ResidentVortexSession`, `NativeExecutionContext`, `LiveMemoryPool` | Reuse the call's grant and pool across every nested stage and ordering instance. |
| Source delivery | Existing bound scan, pinned `RepeatedScan` tasks | Bind once, reserve split metadata and drive one bounded row-range task at a time. Upstream stream concurrency multiplies by host workers even when configured as one per worker; it cannot enforce this query's retained-state overlap limit. |
| Key semantics and stable ordering | `native_relational_keys`, `native_relational_batch`, `native_relational_order` | Reuse native key ownership, comparisons, cancellation and ordinal sorting for both retained and spilled ordering. |
| Run persistence | `QueryRunStore`, `QueryRunSpec`, `QueryRunReader`, `QueryRunBlock` | Add a closed relational-order ownership namespace; retain exact native dtype/block geometry and existing quota, identity, checksum and cleanup rules. |
| Output construction | Native gather/take and `result_batch` | Persist and merge typed native batches. No scalar-row/JSON or Arrow execution intermediary. |
| Delivery | Existing relational consumers and eight local sinks | Consume one execution with existing generation validation, backpressure and commit protection. |

The existing numeric sort spill stores only non-null integer order keys and source
row references for its specialized provider. Relational stages can produce nullable,
text and computed payloads without a source row reference. Their spill run therefore
retains the bound native row schema. Reuse storage, keys, ordering and delivery where
the contracts match; keep these distinct run semantics explicit. Do not replace the
existing specialized strategy merely because a general implementation is available.
Existing numeric sort, exact-integer DISTINCT and weighted text-count run-store
callers must remain covered by validation.

## Vortex-first provider decision

Decision: `implement_shardloom_kernel` for the pressure/merge orchestration, using
the already admitted Vortex 0.85 native providers. The pinned Vortex `ArrayRef`,
`DType`, native take, sequential Flat writer, bounded file scan task and retained
source identity are the payload, persistence and read providers. The existing
`QueryRunStore` already writes a declared native schema and returns complete native
blocks with retained credits. It does not implement SQL stable ordering, per-query
spill permission, multi-stage quota ownership or public success/cleanup evidence.
Those remain ShardLoom policy and kernel responsibilities.

The pinned `ScanBuilder::with_concurrency` setting is per worker;
`RepeatedScan::execute_stream` multiplies it by available host workers. The shared
relational scan therefore uses `ScanBuilder::prepare` once and drives
`RepeatedScan::execute(Some(range))` for one 8,192-row range at a time. This applies
to every native relational input, with or without spill, and keeps original
predicate/projection pushdown and source-generation validation. No format-specific
scan loop or external provider is introduced. Physical reads may still exceed a
logical range because of the original file layout; denial remains explicit.

No dependency, alternative array model, query-engine integration, new file format
or format-specific executor is needed. Feature gates remain
`vortex-local-primitives`, `vortex-write` and Unix file identity for spill. Native
materialization and spill effects must be reported; decoded bytes and upstream
scratch remain unmeasured where existing providers do not expose them.

## Pressure and ownership contract

- Use one lazily created run store per relational execution. All ordering nodes
  share its disk quota, including simultaneous input/output runs during a merge.
  Repeated executions create fresh owned state and revalidate source generations.
- Flush before adding a batch that would cross the configured retained-input
  threshold. Account for retained native payload, key and ordinal work; never use
  a caught allocation failure as permission for hidden unbounded retry.
- Sort each retained portion with the existing comparator and original input
  ordinal tie-break. Merge adjacent runs in input order; on equal keys the earlier
  run wins. This preserves stable ties without adding a user-visible synthetic key.
- Read a bounded number of native blocks and retain every block's ownership credit
  until its selected values have been gathered. Bound run descriptors, merge inputs,
  metadata and output batches. Release consumed runs only after successor run
  publication succeeds and its contents validate.
- Runs contain at most 1,024 rows per block, further bounded by the downstream
  batch contract. Each merge opens two input readers. Nested operators can overlap
  with an upstream final reader; the report counts the actual simultaneous readers
  across the whole query, rather than reporting the per-merge constant as its peak.
- Check cancellation during input, comparisons, writes, merge and final delivery.
  Run storage and readers retain the call's existing cancellation token, including
  its parent scope; specialized providers keep sharing their original owner flags.
  Check original sources before and after consumption. Failed consumers, corrupt
  runs, replaced identities and interrupted cleanup cannot produce success.
- Recovery uses the existing explicit, namespace-checked owned-directory mechanism.
  Unknown files, symlinks and changed identities remain preserved and diagnosed.
  Cooperative filesystem identity checks are not a hostile same-user filesystem CAS.
- Reports distinguish configured permission, actual spilled runs, disk high-water,
  merge work, shared reserved-memory high-water and completed owned cleanup. No
  zero-decode, total-RSS bound or performance improvement follows from these fields.

## Public request

`SqlWorkflow` and `LazyFrame` collection, count where supported, route/run and local
writers carry `memory_gb`, `max_parallelism` and optional `spill` through the same
public facade. Writer aliases preserve the values. The client serializes `spill`
as `--spill` JSON; Rust parses it once with unknown-field rejection and a 32 KiB
configuration limit. The shape is:

```json
{"workspace":"/absolute/existing/local/directory","quota_bytes":67108864,"buffer_bytes":2097152}
```

Omitting the option preserves default behavior. `buffer_bytes` controls retained
input flushing for relational order. Existing specialized sort and aggregate
providers interpret it as their existing operator `memory_bytes` admission;
their stronger family/schema restrictions still apply. The relational minimum is
1 MiB, and the specialized aggregate minimum remains 2 MiB. No buffer may exceed
the public query memory grant. Both simultaneous `--spill` and embedded primitive
spill declarations and repeated `--spill` flags are rejected. Unsupported providers,
generated routes and preparation-only requests reject the option before effects.
Preparation inside an admitted workflow receives the requested memory and
parallelism; spill permission attaches to execution. Relational ordering permission
does not add aggregate, join, window or relational fanout spill support.

## Finite acceptance

Freeze exact expected rows before acceptance for nullable multi-key stable order,
duplicate ties across runs, signed/unsigned boundaries, floating order, UTF8 keys,
empty input, transformed join/set/aggregate inputs, successive ordering stages and
order/limit/filter composition. Use renamed schemas and independently specified
fixtures outside ClickBench. Test under-threshold execution and forced multiple-run
merges, including complete output beyond collection limits through all eight writers.

Require quota-overlap denial, missing/invalid workspace, resource denial, oversized
row, corrupt/truncated/replaced runs, pre/mid-operation cancellation, failing/slow
consumer, source mutation and repeated-call cleanup coverage. Verify route inspection
does not probe or create the declared workspace. Verify all resources and original
source declarations survive both Python and SQL spellings. Failure checks retain
`fallback_attempted=false` and no success certificate or published output prefix.

Run the existing run-store and specialized spill suites, native relational and writer
tests, Python public facade tests, required workspace gates, lean/MSRV feature checks
and public documentation validators. Freeze one final runtime/harness and perform the
complete public pressure matrix plus Full43 regression at the cohesive unit boundary.
Builds and large checks remain serial under existing local storage/process guards.
Performance comparisons, wider types, other operators' spill, large text/format
performance runs, native Python bindings and package publication remain separate.

The resource matrix's aggregate input includes a source-order input limit, so
it uses the composed relational plan. A plain DataFrame aggregate followed by
multi-key ordering can still be rejected by the older flat aggregate frontend;
that public-family routing gap belongs to the continuing PERF-02/10 breadth work,
along with the separate unary-family composition gaps. This unit does not claim
that every flat aggregate shape accepts relational ordering spill permission.
