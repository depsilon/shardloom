# Numeric Consumers and Exact Aggregation Continuation

## Scope and source

The maintainer's post-merge planning request continues RFC 0044 and the existing
PERF-03/04/05/10/12 queue. PR #1433 merged as
`f257395bbe5f09215d42e1a57b4e5e473ac9e981`; its tree matches the corrected
control source `c71a558e879cc24490eb370cc0f6182575bce943`. This packet targets
numeric payload copies and shared per-key synchronization. It does not close
the broader operator, spill, layout, live-generation or compilation packets.

The supplied source review identifies an extra widened numeric vector after
native Primitive execution and shared entry/comparison atomics inside otherwise
partitioned string aggregation. These are source observations, not measured
attribution or a forecast of elapsed-time improvement. Old C7 timings remain
attached to their frozen source; this continuation starts with a fresh corrected
control and requires complete exact public results before retention.

## Native provider and ownership contracts

Pinned Vortex 0.85 already provides PrimitiveArray ownership, original-width
typed slices, validity masks and configured execution contexts. Reuse those
providers inside shardloom-vortex. Retain native owners and derive borrowed
slices only while their owner is borrowed; no self-referential structure,
borrowed caller pointers or new unsafe code is admitted. Execute necessary
native decoding with the query's configured context and allocator. Keep decode,
filter/validity work and eliminated adapter payload copies distinguishable.

Context reuse covers the migrated primitive-owner route across aggregate phases.
Existing dictionary-gather and standalone residual-expression helpers retain
their prior context boundary. Pinned Filter/FoR/bit-packed kernels can allocate
through BufferMut/builders outside HostAllocator; forwarding the configured
context does not turn those allocations into admitted owned-buffer credits.
The pinned primitive builder's `builder_with_capacity_in` also currently ignores
its allocator argument. Tests distinguish this observed gap from session-context
forwarding and native provider error propagation. Lifetime tests independently
prove credit retention for admitted buffers; no decoder-wide budget enforcement
claim follows from the configured context.

Dispatch on physical width, signedness and validity at array boundaries. Preserve
the existing contiguous 64-bit paths and exact native loops for narrower types;
widen in registers when required. Preserve integer identities, null behavior,
floating-point accumulation order, filter selections and dictionary domains.
Do not turn a typed hot path into per-row generic scalar dispatch. No external
engine executes residual work, and zero-decode policy remains explicit.

All-valid integer key views cover all eight signed/unsigned physical widths.
Single-key and pair iterators dispatch before scanning, including numeric/string,
numeric/minute/string and near-unique pair reductions. Selected rows keep their
order and multiplicity. Nullable and materialized inputs retain their prior
admission and lazy lookup behavior. Strategy tests exercise narrow integer keys
with mixed measures; 64 width-pair combinations check exact integer identities.

ShardLoom owns the global distinct-entry limit rather than the Vortex provider.
Reserve entry credits in bounded blocks, consuming them locally. The sum of
committed entries and outstanding reserved credits must never exceed the hard
limit. Keep actual table/arena byte reservations independent. Publish used-entry
counts and refund unused credits at block or partition exits. The published group
count is a lower bound while blocks are active and is exact after all reducers
drain; preserve that cumulative count after storage release. Publish checked
comparison totals at reconciliation boundaries. Refund unused credits on normal,
failure and cancellation exits. Temporary ownership must not cause false exhaustion.
Never wait for another worker's credits while holding a partition lock or unused
credits. Existing exact deferred-suffix handoff must consume each row weight once.

## Verification and retain/drop gates

- Prove primitive payload sharing, admitted-buffer credit retention beyond
  source/context lifetimes, configured-context forwarding, provider error
  propagation, all admitted widths, nulls, slices and compressed arrays. Record
  pinned allocator coverage gaps separately from those proofs.
- Verify renamed scalar, compound grouping, distinct, filtering and fused numeric
  operations against complete independently derived values, including signed
  extremes, unsigned values above the signed range and floating-point order.
- Exercise credit limits smaller than a block, skew, collisions, contention,
  cancellation, insertion failures, refunds, exact pressure handoff and evidence
  after release. Shared counter updates and entry-admission bookkeeping must
  occur at block/partition boundaries rather than per key; comparison totals
  continue to count actual matching-hash byte comparisons.
- Run focused tests, required workspace gates and combined native-feature gates.
- Compare frozen corrected control and candidate binaries on the same artifact,
  settings and complete 43-query results. Report total time, geometric mean,
  short-query behavior and the non-Q34/Q35 families separately. Preserve failed
  candidates and avoid attributing overlapping work counters to wall-clock savings.

Large runs use the existing sequential storage/residency/process guards and
local-only output paths. Source files, retained numeric compression and the
publication race correction stay intact. Cache, physical-layout, compound-key
parallelism and ingest-overlap proposals require their own measured contracts;
this packet does not silently promote them.

## Planning intake disposition

The September 6 suggestions extend existing RFC 0044 work rather than creating
new phase IDs. The implemented scope and the remaining experiments are distinct:

| Suggestion | Disposition | Existing queue and acceptance boundary |
|---|---|---|
| Measure the corrected merged source | Accepted in this packet | PERF-01/12; frozen source, artifact and complete public results |
| Retain original-width native numeric owners | Accepted in this packet | PERF-03/10; typed-copy removal, exact semantics and complete operation time |
| Block entry credits and local comparison counts | Accepted in this packet | PERF-03/04/05; exact admission, refunds and measured synchronization |
| Encoded constant/run/FoR/bit-packed algebra | Merged into existing checklist | PERF-10; pinned provider feasibility, selections, overflow and floating-order proofs before implementation |
| Scan-local compressed segment residency | Merged into existing checklist | PERF-03/07/10; immutable generation, full allocation ownership and fewer completed duplicate reads |
| Compound count and exact grouped distinct | Merged into existing checklist | PERF-04/05/06; complete semantic keys, dictionary domains and exact pressure transitions |
| Compact state, slabs and partition ownership | Merged into existing checklist | PERF-03/04/05; state bytes, probes, cancellation and bounded scheduling |
| Column-addressable logical layout over bounded writes | Merged into existing checklist | PERF-08/09; stable nonnullable struct contract, unchanged payloads and measured narrow-query lifecycle |
| Multi-segment memory generations and owned intake | Merged into existing checklist | PERF-07/11; retained immutable buffers, direct tiny-query route and durable publication |
| Ingest worker scaling and overlapping cohorts | Merged into existing checklist | PERF-03/08/09; matched resource controls precede new overlap machinery |
| Joint codec/consumer and task-level control policy | Merged into existing checklist | PERF-03/09/10; actual scheduling or representation changes with lifecycle evidence |
| Profile-guided optimization | V1 candidate pending feasibility | PERF-13; only after structural bottlenecks, broad training and held-out validation |

These continuations preserve the existing competitive gates. This packet does
not close any whole PERF item or CG gate, and the suggested sub-100-second suite
is a target rather than an acceptance forecast.
