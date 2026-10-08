# Native general aggregation under an explicit spill policy

Status: implementation design; acceptance is pending. This extends PERF-03/06
and the general aggregation obligation in the
[remaining local scope](native-local-completion-scope-2026-10-07.md). It builds
on [completion-aware streamed ordering](native-streamed-ordering-2026-10-07.md)
and RFC 0044. Join, window, pivot, repeated-source streaming and execution
resume retain their separate obligations. This document does not close PERF-06.

## Contract and reuse

The expert comparator is an external-memory relational aggregate that preserves
the engine's exact public semantics and accounts for overlapping ownership.
When a prepared general relational aggregate has an explicit spill policy, use
the existing stable native ordering and query run store to bound grouping and
COUNT DISTINCT state. Without that policy, preserve the existing resident hash
aggregate and its deterministic reservation denial. Do not catch an arbitrary
execution failure and retry through another algorithm or engine.

Admit Aggregate into the finite, single-source streaming chain alongside Scan,
Project, Filter, Sort and Limit. Execute the whole tree once. All producers must
reach end-of-input; limits keep the mandatory drain rule. A nested aggregate
consumes its child's completed relation normally. File/resident plans may still
compose with their existing operators, but an aggregation spill setting does
not make those other operators spillable.

| Existing component | Reuse and extension |
| --- | --- |
| Aggregate binding and typed measures | Keep COUNT, COUNT DISTINCT, SUM, AVG/MEAN, MIN/MAX, output types, null rules and explicit argument projections. |
| Native Aggregate reducer | Reuse the existing numeric, decimal and selected-extremum kernels for one group at a time; bypass its distinct sets only in the private ordered reducer. |
| Native keys and Batch equality | Group equality and ordering share `KeyColumn::compare_at`; nulls compare equal for grouping. Do not substitute serialized bytes, hash-only identity or floating coercion. |
| Native stable Ordering | Sort native records by group, record kind and distinct value; restore output by first input ordinal using the same component. Preserve its adjacent-run stable merge schedule. |
| QueryRunStore and spill State | One execution shares its grant, disk quota, source validation, run readers, cleanup and abandoned-run ownership checks across all internal ordering stages. |
| Native payload/result builders | Compact retained input, group keys, cross-batch distinct comparison values, extrema and output through the existing credited allocator. |

## Vortex-first provider decision

Classification: `implement_shardloom_kernel` for grouping orchestration, using
the existing Vortex-native payload and file providers. Pinned Vortex 0.85.0
`aggregate_fn/accumulator_grouped.rs` consumes already-grouped ListView or
FixedSizeList values. Its grouped primitive SUM delegates to Vortex scalar
reductions, whose integer overflow and floating/NaN policies are not this
engine's ordered floating SUM/AVG contract. It does not provide this complete
flat-input grouping, exact DISTINCT, first-seen ordering and quota/cleanup
contract. Do not construct whole groups as lists or replace established
reductions merely to invoke that provider.

The implementation reuses ArrayRef/DType, native selection, allocator-backed
builders, native flat files and existing scan/write providers. It remains in
`shardloom-vortex`, gated by `vortex-local-primitives` and `vortex-write` for
the spill strategy. There is no dependency upgrade, external query integration,
Arrow execution, unsafe code or persistent answer cache. Reports identify the
native strategy, materialization, actual disk work and no-fallback evidence.

## Ordered records and exact semantics

Project only bound group and measure columns. Give internal fields generated
names in a private schema so user aliases cannot collide. Preserve each logical
dtype; nullable placeholders carry no observation for an inactive record kind.
Reserve descriptor capacity before allocating its names, types and mappings.

For each input batch, create these native records:

1. One base record per input row: group keys, columns required by ordinary
   measures, a monotonically checked input ordinal, and kind zero. Distinct
   ordering fields are null. Original floating payload bits are retained.
   A nested column observed only by COUNT contributes a parent-validity flag;
   it does not force evaluation or copying of unobserved child payloads.
2. One distinct record per nonnull value for each distinct input column: group
   keys, that typed value in its own ordering field, and a positive kind.
   Multiple COUNT DISTINCT aliases of the same column share one record kind.
   Ordinary measure fields and other distinct ordering fields are null.

Sort by the group keys, kind, then distinct ordering fields, with explicit null
placement. Base records compare equal after their group key and keep original
input order through the stable sorter. Distinct records for a group follow its
base records and put equal logical values together. This costs up to one base
record plus one record per distinct input column for each row. Nulls avoid
distinct records. Wide/many-distinct workloads may require more I/O or a larger
grant; the quota and admission rules remain effective.

Fold one group at a time. Base spans feed the same scalar accumulation code as
resident aggregation. Floating SUM/AVG therefore see the same row sequence;
decimal totals retain exact overflow/rounding rules. COUNT handles parent
validity, extrema preserve the first tied payload, and an empty global aggregate
returns its existing single result. Empty grouped input returns no groups.

Count distinct values by adjacent exact comparisons. Keep at most one compact
previous value across a batch boundary, dropping it when the kind/group changes.
Do not retain a cardinality-sized membership set, even when every value belongs
to one group. Null values never increment COUNT DISTINCT. Nested comparisons,
signed zero, integer extrema and decimal metadata use the shared key contract.

Keep the first base ordinal and a compact native group key. On group completion,
build its typed result and feed a second native Ordering keyed by that ordinal.
This restores first-seen group order without an in-memory map of every group.
Compact completed single-group arrays in a bounded buffer before ordering them,
so per-array metadata cannot accumulate once per retained group. Admit the old
owners and the compact replacement simultaneously; release old owners afterward.
Remove internal fields before delivery. GROUP BY with no measures uses the same
path for complete-row DISTINCT semantics.

Each private record reserves its parent schema and column-vector metadata before
construction and carries that credit on the mandatory native ordinal buffer,
without copying its payload. Removing that private field at delivery uses the
shared compact payload builder so the public schema's credit survives a retained
child, clone or slice. Output batching includes the parent metadata estimate.

## Ownership, failures and publication

Every stored record owns compact credited payload; it cannot retain the private
streamed input. The source release witness must expire before the next demand.
Views used during a reducer callback may borrow the current native batch, but
cross-callback keys/extrema retain only compact selected values. Ordinary
resident aggregation already compacts its retained keys and extrema; verify
that property on the newly admitted streaming route.

Input, projected records, sort buffers, run metadata/readers, one group's state,
result ordering and sink/output owners overlap under one query grant. A buffer
threshold is a flush estimate, not an allocator or RSS guarantee. A single
oversized value, excessive schema, exhausted grant or quota remains an explicit
failure. Record input rows, expanded distinct rows and ordered aggregate stages
separately from actual spill runs and bytes.

Producer/type/value errors, failed reservations, cancellation, corrupt runs,
quota exhaustion, consumer errors and cleanup failures prevent successful
completion. Writers preserve an existing destination until all upstream work,
source validation, sink work and owned spill cleanup succeed. Use the existing
abandoned-run recovery contract: identify and clean verified dead-owner runs,
then restart. This is not query resume or automatic recovery of a crashed output
staging transaction.

## Acceptance before support claims

- Compare the ordered and resident general aggregate with independent complete
  oracles across chunk sizes: compound/null/text/binary/decimal/temporal/nested
  groups and values, repeated aliases, all six measure families, empty input,
  group-only DISTINCT, extreme integers and signed zero. Include order-sensitive
  SUM and AVG permutations and null/invalid observations that must fail.
- Prove high-cardinality groups and a single group with high-cardinality distinct
  values complete using actual native runs, within a declared shared grant,
  when the no-spill control denies. Include an ample-memory control. Record
  original input size separately from expanded record size and process RSS.
- Exercise streamed source release witnesses, retained result clones/slices,
  nested aggregates and sort/filter/limit composition; run complete public
  SQL/DataFrame incremental output and native write/reopen checks. Public and
  native grants must be reported independently, without implying one tested the
  other's pressure condition.
- Exercise late producer errors, mid-operation cancellation, exhausted quota,
  corrupted real runs, failed consumer/overwrite, source mutation and exact
  cleanup. Verify dead-owner cleanup through the actual aggregate operation and
  preserve unknown files. Do not infer resume from cleanup.
- Freeze source/binary/fixtures/oracles and retain all observations under the
  existing serial storage/process guards. Run focused checks, required workspace
  checks, whole-workflow regressions and final Full43 correctness evidence,
  then complete adversarial review and hosted integration.

No speedup is promised. The explicit spill strategy deliberately pays for
ordering and typed record expansion to bound state while preserving semantics.
The separate merge-scheduling, rematerialization, caching and multiway-join
investigations remain conditional work with their own evidence gates.
