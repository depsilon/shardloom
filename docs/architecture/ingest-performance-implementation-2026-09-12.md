# Ingest performance: implementation and test sequence

Status: bounded attribution and the first fixed-allocation screen are complete;
the allocation was slower and is dropped. Fresh-artifact Full43 passed. The
[September 12 packet](../benchmarks/ingest-stage-balance-2026-09-12.md) records
the measurements and retained pipeline tests. This refines existing PERF-03/08/09/12 under
[RFC 0044](../rfcs/0044-resident-runtime-resource-ownership.md); it introduces
no new phase IDs and closes no additional competitive gates. The
[phased plan](phased-execution-plan.md) remains the source of execution order.

## Starting evidence and objective

The [post-merge matched-owner packet](../benchmarks/retained-ingest-owner4-2026-09-08.md)
is complete. Accepted native runtime `572bd52c`, identical to the native code
merged as `d51429e3`, ingested 99,997,497 rows in 95.447305 seconds, producing
18,591,586,804 bytes with 2,811,117,568-byte observed peak process RSS. Control
`75fc09a0` took 99.446032 seconds with the same four constructed CPU owners.
All 11,199,719,664 logical values match; the physical bytes differ. This single
pair establishes neither a stable 4.02% speedup nor a cause for that difference.
The 91.215296-second Full43 figure is a query-suite result, not ingest time.
The retained 95.447305-second native ingest is the maintained baseline. The
September 12 extra 104.044137-second observation from the identical control
binary does not replace it. Per the maintainer's clarification, first screen
future candidates against existing evidence; do not change or rerun a control
until a candidate shows a credible material improvement.

Prioritize complete fresh-data workflow cost: ingest, publication and
first/repeated queries. A hypothetical 20% reduction of the recorded candidate
ingest would save about 19.09 seconds; this is a sizing calculation, not a
forecast. Preserve retained numeric compression and roughly the current 18.6 GB
representation. Do not fund faster ingestion by silently inflating the artifact,
removing statistics, weakening verification or deferring work until the first query.
Ordinary streaming publication currently flushes, validates and renames, without
file/parent-directory synchronization. Its measured process completion must not
be described as crash-durable fsync completion; the separate memory-generation
publication route has a stronger synchronization contract.

## Existing mechanisms and provider decision

Use the pinned Vortex native array, layout-writer, sequence and runtime providers
through the existing ShardLoom admission and certificate boundaries. This is
`use_vortex_native_provider` with ShardLoom resource ownership, not a new
scheduler, engine or file topology. Arrow remains an explicit input/validation
boundary; native Vortex remains the durable and execution representation.

- [CPU lanes](../../shardloom-vortex/src/ingest_cpu_lanes.rs) currently reserve
  caller, source and conversion owners before assigning remaining capacity to
  the provider. Conversion queue depth is separate from CPU ownership. Existing
  replacement validation is test-only and requires drained/joined owners.
- [Format I/O](../../shardloom-vortex/src/universal_format_io.rs) and
  [native ingest](../../shardloom-vortex/src/vortex_ingest.rs) already have
  bounded source/Parquet task queues, an existing compute pool, ordered
  conversion handoff and cancellation/drain behavior.
- [Bounded ingest layout](../../shardloom-vortex/src/ingest_bounded_layout.rs)
  awaits each source-batch subtree before pulling the next. Per-field/zone/codec
  work already overlaps inside a subtree. Further batch overlap must preserve
  this sequencing and EOF contract.
- Native ingest skips identity projection rebuilding but deliberately copies
  input into reservation-owned buffers. Text canonicalization/compaction and
  post-coalescing numeric compression have distinct ownership/representation
  contracts. A probe before coalescing cannot automatically replace the final
  encoding step; see [numeric encoding](../../shardloom-vortex/src/vortex_ingest_numeric_encoding.rs).
- [Memory-generation tests](../../shardloom-vortex/src/memory_file_generation_tests.rs)
  demonstrate serialization reuse for one scoped publication route. They do
  not certify ordinary streaming ingest or permit removal of its native-buffer
  ownership, readback, source-generation or commit checks.

## Ordered implementation work

### 1. CPU-stage balance — PERF-03/08/12

- [x] Profile one bounded representative native ingest before choosing a new
  allocation. Separate source work, conversion/copy work, codec work, blocked
  handoff, writer starvation and durable finalization where existing evidence
  permits. Add only missing measurements needed to discriminate the bottleneck.
  Overlapping elapsed spans cannot be summed into CPU time or exclusive wall time.
- [x] Compare one feasible stage allocation at the same actual admitted owner
  budget, memory envelope, source, codec/layout policy, compiler and runner.
  Record requested, granted and constructed owners separately, along with real
  activity/progress; a requested P value is not proof of core utilization.
  The unprofiled P4 candidate (1/1/0/2 owners) took 118.604707 seconds, already
  slower than the maintained 95.447305-second baseline. An unnecessary fresh
  control (1/1/1/1) took 104.044137 seconds, with identical physical output.
  Preserve that observation without promoting it to baseline. The runtime
  change is removed and wider allocation tuning stops; neither comparison is
  a stable regression estimate.
- [x] Evaluate the allocation change in the shared native admission path only
  if the measured limiting stage supports a material end-to-end benefit. Start
  with explicit fixed allocations; live reassignment needs a real production
  drained-owner transition and must not be inferred from its test-only seam.
  The profile supported the bounded trial, but its measured result did not support retention.
- [ ] Extend existing CPU-lane and integration tests for P1, narrow grants,
  source-heavy/codec-heavy inputs, teardown and failure at each stage boundary.
  Keep caller plus source/conversion/provider ownership within the grant and
  prove progress without oversubscription or starvation.

### 2. Bounded overlap between writer batches — PERF-03/09/12

- [ ] Proceed only if measurements identify useful idle time between complete
  subtrees; do not mistake existing conversion prefetch for missing overlap.
  Deferred after the screen: sampled provider waits are not localized to subtree
  tails, and conversion handoff already waits only 0.000875 seconds. This evidence
  does not justify adding another batch's live buffers and ordered segments.
- [ ] Admit a small byte-budgeted window of native subtree futures using the
  existing Vortex sequencing/runtime. Charge original inputs, native copies,
  codec scratch that is covered by the allocator, completed out-of-order work
  and footer references for their full live lifetimes. Keep allocator exclusions
  explicit and observe process RSS independently.
- [ ] Preserve ordered layout assembly, final partial batches, EOF, complete
  drain and the single-artifact publication barrier. Bound completed work as
  well as submitted work; a count-limited queue alone is insufficient for skewed
  variable-width batches. Do not spawn one thread per batch or add a second pool.
- [ ] Add end-to-end tests across source, conversion and writer boundaries;
  retain existing within-stage tests rather than treating them as full-pipeline proof.

### 3. Repeated representation work — PERF-07/08/09/11/12

- [ ] Attribute bytes copied, actual conversions/compactions, probe/encode calls
  and repeated traversals across ingest and persistence. Distinguish actual work
  avoided from work moved outside the reported preparation span.
- [ ] Reuse same-generation, same-lifetime, same-representation results where
  supported. Any removal of the intake copy needs equivalent buffer ownership,
  mutation isolation and reservation transfer through the last retained reference.
- [ ] Preserve post-coalescing numeric compression, required statistics, dtype,
  nullability, ordering and complete readback. Do not reuse incompatible probe
  output, introduce answer caching, or remove publication/checksum validation.
- [ ] Test identity projection, nullable/Unicode and integer-boundary values,
  encoded inputs, buffer/session drop, source mutation and durable reopen.
  Helper-specific representation changes remain deferred unless new attribution
  supersedes the old 7.441/187.601-second observation with a material opportunity.

The retained end-to-end pipeline tests cover an empty intermediate batch, final
partial batch, delayed source EOF, source/conversion failures, cooperative
prefetch cancellation after source I/O returns, concurrent destination creation,
complete native values, staging cleanup and reservation release. Parallel-codec
admission is checked across P1/2/3/4/5/8 on renamed, nullable Unicode and
precision-sensitive integer data. This does not close the broader skew,
source-mutation, codec-blocked cancellation or serving-fairness matrix below.

Fresh-artifact Full43 now passes all 129 complete results on the retained
18,591,586,804-byte representation. It is byte-identical to the earlier fully
compared candidate, linking all native values/schema and physical metadata by
SHA-256. This establishes metadata identity, not an independent statistic oracle
or a paired query-performance improvement.

## Additional acceptance tests

| Boundary | Required cases and evidence |
|---|---|
| EOF, errors and cancellation | Slow first subtree; later completed subtrees; final partial and empty batches; cancellation while source send, conversion handoff or codec completion is blocked; error after partial segment output. Preserve the primary error, join every owner, drain/drop all submitted work and release owned credits. |
| Transient memory and skew | Simultaneously live original input, native copy, codec scratch, completed arrays and footer references; large Parquet pages, wide variable-length strings and skewed batch sizes. Check reservation peaks/releases and separate OS peak memory; neither substitutes for the other. |
| Source and publication races | Replacement, truncation and same-size source mutation during ingestion; concurrent destination creation/replacement; failure before/after publication. Reject mixed-source results, preserve foreign files, and accurately distinguish failed publication from published-but-durability-unconfirmed output. |
| Fresh-artifact queries and metadata | Compare complete values, validate metadata/pruning separately, then run first/repeated native queries on the newly written candidate artifact before retirement. Full43 on the protected old artifact does not certify a different layout. |
| Serving fairness | Run a separate bounded file-backed small-query stream during ingest. Measure latency distribution, progress, cancellation and shared CPU/memory ownership. Do not mix this contention run into exclusive-ingest timing. |
| Held-out shapes | Renamed schemas, numeric-heavy and text-heavy data, nullable/empty input, low/high cardinality, skew and precision-sensitive values. Admission derives from physical capability and work shape, never benchmark query or column names. |

## Measurement and retain/drop gate

1. Reuse the completed four-owner packet; do not repeat it to obtain a preferred
   number. Identify one limiting stage and predeclare the expected benefit,
   acceptable memory/storage tradeoffs and stop condition before implementation.
2. Validate the candidate through existing focused ownership, EOF, native-value
   and publication tests, then required formatter, Clippy and broad native/default
   checks when runtime behavior changes. Keep all native measurements serial.
3. Screen against the retained 95.447305-second baseline first. Do not rerun or
   change a control until a candidate demonstrates credible material improvement.
   Only then use matched-owner alternating repeated runs for a performance claim; freeze
   source/binaries and record all samples, real CPU work, peak RSS, reservations,
   output bytes, failures and cache policy. Retain a change only with material
   complete-workflow benefit and no unaccepted correctness, memory, storage,
   serving-fairness or query-performance regression. The 20% sensitivity is not
   a promised result, and a narrower stage-only improvement is insufficient.
4. Keep one generated full-size artifact at a time under the existing storage
   guard. Complete exact value/schema validation and required metadata/query
   checks before durable proof and exact owned-file retirement. Preserve failed
   evidence; never weaken the 100 GiB workspace, 256 MiB logs or source-residency
   checks to obtain a result.
5. Publish a cohesive evidence packet with source/native-binary/build identities,
   complete-process and query clocks separately, proof scope, all regressions and
   outstanding limits. If the measured opportunity is marginal, record the drop
   and stop that experiment rather than expanding into a general controller.

This sequence does not resume rejected topology, local Top-K, codec/state
replacements, the isolated Python binding or unconditional PGO work. Broader
PERF and CG obligations retain their existing status; no external engine may
execute residual work and no package release is authorized.
