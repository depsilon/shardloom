# Direct aggregate consumption of exact join matches

## Status and work unit

Accepted for implementation by the November 16 maintainer feedback. This is an
item-20 shared execution extension under RFC 0044. The local implementation and
17 focused native tests and the adjacent join source-demand tests pass.
Broad source gates, installed public activation
and performance acceptance remain pending. The unfinished
item-19 spatial draft is preserved separately; its required roadmap remains open.

For admitted unique inner dimension joins, let the existing aggregate consume
matched source values before constructing joined payload columns. Start with
both existing star join orders and the single-measure report shapes. Preserve
the ordinary native execution of other shapes and representations. Runtime
uniqueness comes from completed exact indexes, including duplicates across build
batches, rather than schema declarations or benchmark identities.

## Semantic and ownership contract

- Admit resident unconditioned inner equijoins with nonempty keys, direct column
  mappings and equivalent pure aliases. A left-deep dimension chain may resolve
  each probe key back to its original fact input. Spill, grouping sets, effectful
  intermediate stages and correlated execution retain their existing owners.
- Build dimensions in the original execution order. Reuse the completed native
  index, optional dense integer lookup and retained build owners. A non-unique
  dimension uses the already-built ordinary join; it must not re-read a source.
- Carry bounded, credited source ordinals. Each join flush preserves the existing
  validation phase for every declared field, including fields discarded by a
  retained mask and rows rejected by a later join. Do not validate only final
  survivors or prepare deferred, unobserved source domains early.
- First admit one ordinary numeric measure, with no DISTINCT, FILTER, decimal
  transform or mixed-measure sequencing. Group identities use the existing exact
  key equality and first-observed group interning across dictionaries and chunks.
  Retain only observed group combinations.
- Preserve aggregate phases: complete the group batch, reserve existing state,
  prepare the admitted measure, then apply its original ordered updates. Integer
  SUM remains compensated F64 accumulation; floating SUM retains ordered F64
  addition. No multiplication by match count or reordered partial sums.
- Borrow immutable native value owners when their physical and validation
  contracts permit it. Unknown physical storage keeps ordinary native execution;
  no eager decode may be introduced solely to decide optimization eligibility.
- Reuse aggregate state and finalization. Only unique group payloads and ordinary
  final result arrays are constructed on the direct path. All source references,
  ordinal selections, group state and escaping output keep their memory credits.
  Cancellation and checked arithmetic remain active throughout.

This changes the producer/consumer boundary. It does not revive the rejected
join-to-ranking payload-mapping experiment or introduce another join-result
cache. Passing deferred joined arrays into the old aggregate interface would not
establish the requested capability.

## Vortex-first provider check

Decision: `implement_shardloom_kernel` through the existing join, key, grouping
and aggregate owners. The pinned Vortex 0.93.0 source was checked at
`vendor/vortex-array/src/aggregate_fn/{accumulator_grouped.rs,fns/sum/grouped.rs,fns/sum_v2/grouped.rs}`
and the array builtins. Its grouped reductions consume already grouped arrays;
they do not preserve ShardLoom's exact join-validation and ordered compensated
SUM contract. Constructing grouped lists would also reintroduce an intermediate.
Native Vortex arrays, validity, dictionaries and host allocator remain the value
and ownership providers. No dependency or query-engine integration is added.
`fallback_attempted=false`; `external_engine_invoked=false`.

Physical borrowing uses Vortex's `SliceReduce` over known host primitive, boolean
and validated view leaves, after resolving only transparent single-chunk/slice
coordinates. Offset-backed VarBin values retain their original bytes and native
offset owner; selected-value checks preserve the original join boundary. No
view expansion or whole-domain payload preparation is performed for admission.
The first join still performs its ordinary complete key preparation; subsequent
keys have already crossed the preceding join's validation barrier. Unknown
encodings and nullable-parent expressions keep ordinary native execution.

The transfer from [EmbeddingBag](https://docs.pytorch.org/docs/2.15/generated/torch.nn.EmbeddingBag.html)
is the combination of lookup and reduction without expanded intermediate
payloads. It is a design analogy, not copied implementation or a claim that this
general technique is new. [Vortex arrays](https://docs.vortex.dev/concepts/arrays)
remain the native representation. ShardLoom-specific work is admission,
validation, resource ownership and integration with shared exact consumers.

## Evidence selected before implementation

Use all 630 cases of the existing dense-integer-join cohort, including its 600
original requests and 30 versioned controls. No case is selected or omitted by
timing, outcome or predicted activation. This keeps both star orders and nearby
duplicate, outer, semi/anti and row-output controls across the existing formats
and public surfaces. The local `join-aggregate-2026-11-16/selection-r1.json` binds
every identity to the original selection and protocol before candidate code.
Reuse prior candidate observations only after exact request, runtime, input,
reference, resource and engine-clock verification. No Full43, full11k or new
competitor observation is part of this unit.

Before scoring, prove zero intermediate joined payload construction at admitted
edges, unchanged selected validation and contribution order, credited ownership,
and public route convergence. Test invalid intermediate values even when a later
join rejects the row, duplicate dimensions, null and missing keys, distinct
codebooks, multiple chunks, aliases, high group cardinality, tiny/empty inputs,
memory refusal, cancellation and result lifetime. Run the required source and
installed-package checks. Compare the complete cohort's engine-time sums and
preserve all regressions. No speedup or broad capability claim follows from this
design or reduced work counts.

The focused tests exercise both star orders, intermediate and final aliases,
multiple batch sizes, SUM/AVG/MIN/MAX/COUNT, nullable measures, empty scalar and
grouped results, missing keys and duplicate dimensions across build chunks.
Different dictionary codebooks produce the same logical groups across file
chunks; 2,049 observed groups exercise state growth. Negative tests preserve
the earlier join's numeric validation even when a later join rejects the row,
ordered floating cancellation and overflow, and hidden unselected bad values.
Memory denial and cancellation refund match frames and query state; completed
native group output remains owned after its prepared source is dropped.

The first broad native run exposed shared key metadata growth in the temporal
grid pressure control. A focused regression test reproduced an 80-byte variable
domain replacing the ordinary 16-byte view owner. Borrowed offset metadata now
lives in its own credited allocation, preserving the original shared slot size.
The regression test passes, including admission denial and credit release; the
full pressure recheck remains part of the pending broad gate. Existing source-
demand tests now assert zero joined copies and exact direct validation counts
for admitted joins while retaining the ordinary-path expectations elsewhere.

Debug work observations require zero intermediate joined rows and payload
cells on the admitted path. Completed certificates also expose considered and
declined chains, admitted and declined source batches, matched rows, selected
validation cells and aggregate contributions in `join_work.direct_aggregate`.
These are work observations, not timing claims. The normal representation
choice remains authoritative: unsupported borrowed leaves use the already-built
ordinary native joins. Float16, nested/extension leaves, numeric dictionaries
and nullable-parent expressions are outside this first physical admission.

## Later separate admission

Exact duplicate multiplicities may later feed COUNT when group dependencies and
validation permit collapsing pairs. Extrema require their own proof. General
SUM/AVG multiplicity transformations, vector-neighbor composition, source-demand
pruning and adaptive scheduling are separate work; none is implied by stage one.
