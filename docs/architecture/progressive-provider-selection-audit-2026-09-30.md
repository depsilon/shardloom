# R6.c: progressive provider selection

Status: **drop the duplicate scanner recipe at source admission**. Keep the
existing Vortex provider. This closes the proposed mechanism, not the Q23
performance target. No runtime changes or new speedup are claimed.

## Existing implementation and reuse

ClickBench Q23 is the 23rd SQL statement, not file line 23. It groups SearchPhrase
after Title contains `Google`, URL does not contain `.google.`, and SearchPhrase
is nonempty; measures include MIN(URL), MIN(Title), COUNT and exact DISTINCT
UserID. The paired harness assigns statement ordinals after removing comments.

ShardLoom's `AggregateLowering::new` already projects group/measure inputs,
separates residual predicates, rewrites available derived fields and passes the
supported filter into the native Vortex scan. The saved Q23 report confirms
filter and projection pushdown, including the derived SearchPhrase length
rewrite. The unfiltered winner-only DISTINCT route does not admit this query.

The pinned Vortex 0.85.0 implementation already supplies the proposed sequence:

- [`scan/tasks.rs`](https://github.com/vortex-data/vortex/blob/0.85.0/vortex-layout/src/scan/tasks.rs)
  intersects pruning masks, passes the current selection to each filter conjunct,
  stops on an empty mask and avoids awaiting projected output for empty results.
- [`scan/filter.rs`](https://github.com/vortex-data/vortex/blob/0.85.0/vortex-layout/src/scan/filter.rs)
  flattens conjunctions and learns their order from observed selectivity.
- A flat child reader builds a deferred expression plus selection when mask
  density is below its threshold. Calling `apply_bound` is not proof that the
  entire predicate has already executed before selection. Other child layouts
  may have different behavior; the saved report identifies only the chunked root.

Vortex-first classification: `use_vortex_native_provider`. Reuse the existing
feature-gated scan and Native I/O/execution reports; do not add a parallel
ShardLoom scanner or an external engine. This audit changes no provider version,
policy, residual handling, materialization boundary or certificate claim.
`fallback_attempted=false` and existing CG-1 through CG-23 scope remain intact.

## What the retained profile establishes

The final R3.b cohort `paired43_20260930T035437075429Z`, runtime
`dff85c33763ac773c51ca1dd5e61a675cef6e20f`, retains three candidate Q23 calls:

| Call | Complete CLI time | Accessor provider span | Dictionary binding span |
| --- | ---: | ---: | ---: |
| 1 | 4.323974 s | 4.100049 s | 0.002864 s |
| 2 | 4.482214 s | 4.220017 s | 0.003079 s |
| 3 | 4.296023 s | 4.088150 s | 0.002950 s |

Every call returns the exact retained values. Each reports 820 source chunks,
7,128 selected rows, 2,460 UTF8 accessor calls, 12,376 dictionary entries and
zero UTF8 bytes copied by the accessor. The 1,189,618 source-backed UTF8 bytes
are cumulative logical lengths, not simultaneous retained allocation. Source
array size estimates are likewise not unique disk bytes read. Complete envelopes,
identities, raw archive manifests and all samples are in the independently audited
[R3.b portable evidence](../benchmarks/evidence/mixed-distinct-workers-review-2026-09-30.json.xz).

The provider span includes deferred I/O, decompression, filtering and
canonicalization. It does not identify their exclusive costs. Flat readers
request separate array futures for filter and projection, while segment sources
can share/cache bytes. `SerializedArray::decode` deserializes an encoded array
through its registered encoding plugin; its name does not establish repeated
codec decompression. `ExecutionCtx` has no array memo cache. These source facts
do not prove a multi-second duplicate-decode opportunity.

## Reopening contract

Keep Q23 provider attribution in the final profiling refresh. A new candidate
needs a realized child-layout inventory and exclusive attribution that separates
I/O, codec execution, predicates and canonicalization. If duplicated work is
observed, first consider reusing provider-owned results within the same source
generation, with explicit selection, lifetime, byte-reservation and cancellation
contracts. Demonstrate fewer expensive operations and better complete-call time
without losing any exact values. No additional benchmark was run merely to
reconfirm that progressive filtering already exists.

This is a bounded source audit under PERF-INTAKE/PERF-02/03, with the broader
performance obligations still open. R10 and R2.b remain the finite queue.
