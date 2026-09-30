# Winner-only exact DISTINCT — R3.a

Status: **prototype admission**. No retain decision yet. This is the
next PERF-INTAKE item after R2.a. Its comparison baseline includes the scoped
query/recovery lock repair in PR #1484; that repair does not change aggregation.

## Measured opportunity

R2.a Full43 records Q10 at 4.720184 seconds best, including a 4.202439-second
group-update caller span and a 0.370499-second accessor span. The mixed numeric
family already uses exact pair preunion, chunk-local groups and block-bound
ordinary-measure kernels. These are reusable components, not new candidates.

A complete all-group execution returns 9,040 groups and 17,996,642 global exact
group/value pairs. The ten COUNT-selected groups contain 42,451,524 of 99,997,497
rows (42.45%) and 6,928,232 pairs (38.50%). Their complete mixed results match the
retained Q10 reference. The 21,922,971 earlier pair counter is chunk-local and
must not be used as the global denominator.

A three-pair screen using existing public calls first computes SUM/COUNT/AVG
winners, then exact DISTINCT for those freshly selected keys. It preserves all
values but loses: best complete control 3.616690 seconds versus 3.810418 seconds
for both calls and their composition (5.36% slower). The ordinary-only call
spends about 2.448 seconds updating groups and bypasses the already-shipped
block-bound kernels. This screen is insufficient to drop their reuse.

## Shared prototype contract

The first shared ordinary/DISTINCT-pass prototype preserved all six paired Q10
results, with best 3.794010 to 3.312719 seconds (12.69% lower). One candidate
sample was slower. Its all-row SUM/AVG work is avoidable because only COUNT(*)
selects winners. A second admission screen using the unchanged R2 final binary
computes COUNT-only winners and then all original measures for those keys. All
six complete results pass: control best/median 3.412807/3.494983 seconds versus
2.453370/2.454249 seconds for both native calls plus composition (28.11% lower
best). Every pair improves; peak child RSS is 672–679 MB versus 1,080–1,088 MB.
These are admission screens, not shipped single-call performance.

Refinement: reuse the existing single-integer COUNT state and comparator for a
key-only preliminary scan, then feed its exact winners through existing Vortex
IN/projection pushdown into the unchanged mixed-measure aggregate kernels. No
separate ordinary/DISTINCT update loop is needed. Retain OFFSET plus LIMIT keys,
apply OFFSET only to the final result, and preserve the complete key tie order.
The same held source, session, cancellation and provider owners span both scans.
Report auxiliary reads, count work, selected row weight and policy rejection.
A bounded prefix cost screen must reject high winner coverage and excessive
auxiliary state before completing the preliminary scan; its threshold remains
provisional until adverse-workload timing. A rejected cost screen uses the
original native plan; an execution error remains an error.

Vortex-first decision: `use_vortex_native_provider` for the existing 0.85.0
`ScanBuilder::with_projection`/`with_filter` and bound IN expression; ShardLoom's
existing exact COUNT, capillary comparator and mixed aggregate consumer supply
the reduction. No new array/scan abstraction, query engine or decoded Arrow
boundary. Reader splits and summary evidence include both passes.

The superseded first prototype reused the typed integer pair/chunk-group loop with explicit ordinary,
DISTINCT and combined measure passes. Every input row contributes to ordinary
measures, independently of pair deduplication. Select winners with the existing
aggregate ordering and complete-key tie comparator. Only then compute exact
DISTINCT for retained keys, preserving OFFSET plus LIMIT semantics. Do not copy
the kernels, introduce a benchmark-name route, cache answers or invoke another
engine.

Admission requires a nonnullable identity integer grouping key and DISTINCT
argument, one DISTINCT measure, ordinary COUNT(*)/SUM/AVG measures, COUNT(*)
descending winner selection, a finite small result window, and no HAVING or
residual predicate. Admission is a semantic proof, not a universal cost proof;
measure adverse low-cardinality and high-winner-share cases before retention.
All scans use the existing held source/generation, provider, cancellation and
result ownership boundaries. Charge the complete rescan, decode, membership,
state and finalization work. No new whole-query memory bound is claimed from the
general aggregate state's existing observational accounting.

ShardLoom technique review: reuse metadata-first lowering, block-bound kernels,
exact complete keys and capillary winner selection. Dynamic cost admission must
be justified by the measured crossover; do not add a scheduler or duplicate
worker family. R3.b's mixed-measure worker extension remains separately decidable.
Vortex-first review: the current native scan, projection and integer owners
already provide the required source surfaces. The change belongs in ShardLoom's
shared aggregate consumer, with Vortex-native inputs and outputs preserved.

## Decision evidence

First test exact values across renamed/reordered columns, multiple chunks,
duplicate pairs with differing ordinary measures, ties/OFFSET, signed extremes,
empty/null/unsupported admission, cancellation and resource refusal. Compare
complete Q10 calls sequentially against the frozen repaired R2 baseline, retaining
all samples and medians. Keep a useful smaller gain if it survives complete-query
and resource checks; remove the prototype if it does not. For retention, finish
full applicable query UAT, workspace/native/public-call validation and review.

Admission evidence is retained under
`/Users/dylan/LocalData/shardloom/performance-candidates-20260926` in
`r3a-current-q10-attribution.json`, `r3a-global-winner-share.json` and
`r3a-existing-two-pass-screen.json`. Complete outputs are archived in their
referenced guarded UAT directories. These are measured admissions, not a new
single-query implementation or independent correctness oracle.
