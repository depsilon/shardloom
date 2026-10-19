# Native set membership under memory pressure

Status: finite local acceptance passed; hosted and other-platform proof remain
pending. The [acceptance report](../benchmarks/native-set-membership-spill-2026-10-18.md)
records complete values, failures and the source/build equality proof. This finite RFC 0044 unit
extends PERF-06.membership/recovery/strategy through PERF-03 ownership and
PERF-07/11/12 output/workflow acceptance. The broader membership/population
owner remains open. CG-5/14/16/19/20/21 apply; all CG-1 through CG-23 retain
their independent evidence requirements. No version bump is included.

## Grounded boundary and selection

Base `5829e71c` retains native `RowSet` membership for `UNION` (distinct),
`INTERSECT` and `EXCEPT`. `run_set` reaches the same credited `RowIndex` and
`Table` from file, resident and finite streaming inputs. The existing
`streaming_set_membership_growth_is_denied_with_or_without_replay_spill_permission`
regression passes against the restored baseline: enabling source replay spill
does not admit high-cardinality membership. This is a required capability gap,
not a prospective speed optimization or an arbitrary input-size target.

CPU package B was assessed first. The current vendored Zstd provider constructs
one compressor per compression call and one decoder per selected frame sequence;
both already reuse that context across frames. The October 7 decoder and
October 15 compression acceptance measure complete operations, not isolated
setup latency or actual production reconstruction frequency. No measured cache
benefit is established. Cross-operation preparation remains conditional and
requires a separate cost/identity/lease/reclaim protocol before a runtime change.

## Provider and algorithm

Vortex-first decision: use the pinned Vortex 0.85 native arrays, dtypes, validity,
selection, serialization and file providers through the existing ShardLoom
boundaries. Extend the existing ShardLoom relational ordering and query run
store, whose public spill policy, single shared memory grant, byte quota,
generation validation and exact owned cleanup already serve aggregate, join
and window consumers. No new format, dependency, external engine or fallback
is introduced. Full-key equality remains the existing `Batch`/`KeyColumn`
comparison; sorting uses that same total key comparison and explicit null order.

With explicit spill permission, the three distinct set variants use bounded
native records containing the aligned complete row, source side and checked
64-bit input ordinal. Private field aliases prevent user-name collisions.
Sort by the complete row and ordinal through the existing bounded run merger.
Reduce one equal-key group at a time, retaining only its first eligible native
row and left/right presence. A second bounded order restores first-occurrence
output order. `UNION` uses the first combined left/right occurrence;
`INTERSECT`/`EXCEPT` use the first qualifying left occurrence. Null equality,
typed/nested values, signed zero and exact first-row payload spelling remain
the resident semantics. Hot keys require no recursive hash repartitioning or
cardinality-sized state. Every group and completed output is released through
the shared owner.

The two orders overlap during key reduction. Set records and run blocks are
capped at 256 rows, or the caller's smaller requested batch size; the existing
byte threshold still splits batches, and every actual owner reserves
from the shared grant. This internal work granularity is not an input-size
limit. Initial fault testing caught default 1024-row run blocks overlapping
both orders and denying the constrained grant before the consumer. The
regression uses the default public batch route for successful fresh execution
and consumer failure, rather than hiding that failure with a smaller requested
output batch.

Fanout adds a native output replay owner while the set's final ordered reader
is still live. Direct-binding acceptance caught that replay merging enlarged
256-row set batches into 1024-row run blocks before the writer's delivery clamp.
Fanout now bounds both its producer handoff and replay storage blocks at 256
rows. Source replay retains its existing independent bound. All eight output
adapters must complete from one set execution under the original constrained
grant; independent file readback and exported verification tables have their
own resource admission and are not membership-pressure evidence.

The no-spill resident path and `UNION ALL` remain existing algorithms. Permission
selects the native pressure-capable strategy; its ordering buffer determines
whether actual files are needed. Strategy counters and actual run counters are
distinct, and source replay is not reported as set membership. A complete input
must be observed even when the result is empty. Retained public output owns its
credits independently of temporary runs and producer lifetime.

Ordered membership consumes both sources before its first output. Abandoning
that output still produces no final completion report. Separate resident and
spill early-close controls preserve this distinction and require child exit,
retained-value validity and exact owned cleanup.

## Verification contract

The expert comparator is an encoded-columnar maintainer reviewing exact set
semantics, bounded state, source completion, quota overlap and failure cleanup.
This is capability acceptance; no Full43 speed threshold decides whether the
capability is necessary. No speed claim is planned.

- Prove complete high-cardinality and duplicate-heavy results beyond the same
  memory grant that refuses the resident strategy. Include all-unique, skewed,
  empty, disjoint, overlapping and all-null inputs, repeated producers and
  nested/composite keys. Validate every field, type, null and defined order.
- Cover ordinary small inputs and `UNION ALL` controls, zero-limit composition,
  filters/projections, nested sets and output retained after plan teardown.
- Exercise quota exhaustion, admission failure, cancellation, producer/consumer
  failure, corrupt/truncated/replaced runs, changed sources and exact cleanup.
  Crash recovery uses existing owned-run recovery followed by fresh execution;
  it must not be called interrupted-execution resume.
- Check public SQL/DataFrame/direct-binding convergence and native/compatibility
  write/reopen through all eight existing destinations. No destination publishes
  partial input or failed output. Preserve unknown files and existing targets.
- Run focused positive selectors, native/provider/CLI and mandatory workspace
  formatting, strict lint and tests; one Full43 regression and the complete
  format matrix at the coherent acceptance boundary. Store source/build/result
  identities, complete values, cleanup and no-fallback certificates in local-only
  guarded evidence before closing any finite ledger scope.

Capillary batching and dynamic admission are the existing sorter/run-store
mechanisms. PulseWeave scheduling is unchanged. Metadata-first source pruning
continues before set consumption, but membership completion does not invent
metadata proofs. Native Vortex is the persistence target; compatibility writers
remain explicit translation boundaries. This work is neither a process-RSS cap
nor general population/sampling spill, distributed execution or release readiness.
