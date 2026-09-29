# Winner-only exact DISTINCT — R3.a

Status: **prototype admission**. No speedup or retain decision yet. This is the
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

Reuse the existing typed integer pair/chunk-group loop with explicit ordinary,
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
