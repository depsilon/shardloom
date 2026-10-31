# Combined native performance batch — October 31

## Decision and scope

The maintainer requests the ten remaining owner optimizations together, with one
implementation batch, a shared build and validation cycle, and one final comparison
campaign. This contract is prospective: it records no candidate execution or gain.
It follows the [pruned owner queue](native-shared-owner-optimizations-2026-10-30.md#remaining-active-optimization-work)
inside active item 20. The separately measured drops and judgment screens stay closed.
Connector/refresh completion and item-20 acceptance follow this batch; item 18's
comprehensive I/O/interface acceptance still gates Full43. CG-1 through CG-23 retain
their independent obligations. No release or new competitor execution is included.

The comparator is a native columnar engine maintainer reviewing complete workflows,
validation order, source generations, owned buffer lifetimes and bounded state. A
local reduction in instructions is insufficient: each admitted route must remove
the stated repeated work and pass the complete batch gates below.

## Shared invariants

- One ShardLoom runtime, Vortex-native input/state/output, and no external execution.
- No new dependency, unsafe implementation, global cache, answer cache or second pool.
- Preserve types, exact integer widths, NULLs, multiplicity, stable ties, analytical
  floating equality and selected original bits. Do not reassociate SUM or AVG.
- Preserve complete demanded-value validation and first semantic errors. Complete
  candidate chunks remain observable before semi/anti short-circuiting. Lazy branches
  and guard-inactive subqueries do not acquire eager evaluation.
- New containers and replacement overlap use the existing query memory pool.
  Source owners, prepared views, readers, temporary buffers and their credits drop
  at their stated boundaries. Output credits survive until the last output alias.
- Keep cancellation, quota overlap, source identity checks, recovery and cleanup.
  Optional admission declines before mutation; admitted failures propagate once.
- Instrument mechanism controls only in separate tests/diagnostics. Acceptance
  executables contain no test observation/timing hooks.

## Ten owner contracts

| ID | Owner and admitted change | Exclusions and proof obligations |
| --- | --- | --- |
| B1 | The demanded Parquet reader delivers only the complete compressed pages needed by an already admitted unfiltered prefix, including any required dictionary. Fetch and decoder use the same prefix and held file identity. | Start with flat non-repeated leaves and usable, checked page boundaries; otherwise retain whole-chunk delivery. No predicate-first selection, ranked payload narrowing, arbitrary page omission or speculative range crossing. A limit alone is not read-savings evidence. Keep full selected-page validation and the existing decoder workspace admission. |
| B2 | A projection-scoped prepared-value owner lazily reuses repeated direct input columns across different eager expressions. | Exact full input owner, logical dtype, immutable float policy and value-only purpose define the scope. No selected branch, nested-domain, hash-purpose, callback or cross-batch reuse. Single-use columns keep ordinary preparation. Complete first preparation and error order precede reuse; release the scope before returning to the producer. |
| B3 | Relational scalar reductions inspect actual Constant/RunEnd numeric inputs before expanded primitive preparation and use the existing weighted encoded owner for count/extrema. | Begin with global count/min/max and unfiltered domains; grouped, selected, distinct and additive states keep their existing paths unless separately proved before implementation. Preserve run-boundary validation, original winning bits and observed float policy. A Parquet dictionary is not activation evidence. |
| B4 | Append ASCII case-converted bytes and admitted concatenation directly to the existing credited variable-width result buffer. | Complete argument preparation remains unchanged. Unicode casing retains Rust's existing contextual conversion and scratch admission. Preserve argument NULL rules, output type/error precedence, checked sizes and replacement overlap. This extends the accepted single-evaluation builder, not the previously rejected construction experiment. |
| B5 | Evaluate a bounded pure fixed-width residual join condition through source/indexed operands, without constructing repeated-left/gathered-right condition tables. | Start with column/literal comparisons and Boolean composition in proved non-fallible integer/Boolean domains; decline float, text, nested, casts, arithmetic and lazy branches. Preserve selected-domain validation, all candidate rows, order, duplicates and owned output. Resident and stored joins share the condition boundary. |
| B6 | Rewrite a statically bound pure equality-correlated EXISTS/IN relation to the existing explicit-correlation membership owner. | No scalar/quantified, dynamic/deferred/effectful source, aggregate, limit, ordering, callback or fallible inner expression. Retain deferred first initialization after an active outer row, original guard behavior and empty-outer behavior. Admit only source/projection/equality shapes whose unused payloads cannot hide an old error. No per-parameter result cache. |
| B7 | Merge several adjacent relational spill runs in one generation using memory-derived bounded fan-in and the existing native run store. | Keep two-way geometry at low headroom. Charge reader metadata, held blocks, heads, output and replacement overlap. Earlier run wins ties; finish and validate a replacement before removing input runs. Preserve quota/fault semantics and complete output. No winning-span optimization. |
| B8 | Retain bounded charged block hash boundaries with completed ordered join storage and use them to narrow positional hash searches. | Metadata belongs to the exact run generation. Build while producing run blocks, not by a new payload scan; decline directory retention when budget/shape does not admit it. Exact key equality still resolves collisions and duplicate spans. No payload or probe-result cache. |
| B9 | Compact tiny replay fragments directly into bounded owned capture blocks before producer return. | Begin with physical flat fixed-width records and preserve unobserved bits; unsupported schemas retain immediate detach. Do not buffer source-backed arrays or add a second compaction copy to already suitable owned blocks. Large-batch controls, late errors, cancellation, resident/spilled replay and final alias refunds remain mandatory. |
| B10 | Store static global exact-quantile observations in charged append-only domains, then select requested exact ranks at completion. | Begin with global resident aggregates. Grouped/ordered-spill quantiles and dynamic window AVL state retain their existing owners. Stable input ordinals disambiguate analytical-equal values, preserving discrete selected bits. Validate values at observation time; use fallible cancellable bounded selection and existing rank/interpolation semantics. Share repeated levels over the same column/filter domain. |

Narrow domains are deliberate semantic boundaries, not claims that every variant
is optimized. A route may close at an evidence-backed screen if source inspection
proves the proposed work absent or unsafe under its existing contract. Record that
decision explicitly; do not substitute a smaller unrelated patch or count a decline
as implementation. Runtime fixes during shared validation remain part of this batch.

## Provider and source grounding

Pinned Vortex 0.90 array/compute providers already own Constant/RunEnd, primitive
storage, validity, variable buffers and native run persistence. Reuse those owners
inside `shardloom-vortex`; no query-engine integration is admitted. The existing
`encoded_numeric_reduction` owner provides checked structural resolution and run
traversal. `result_batch` already provides credited variable-buffer growth.

The patched Parquet 59.3.1 source provides offset indexes, selected page locations,
row selection and prefix limits. Current `SelectedChunks::read` still reads whole
projected compressed chunks; current `with_limit` only bounds decoding. Validate
the provider's actual page-reader requests against narrowed ranges before admitting
B1. Metadata/index loading must retain held identity, admission and byte reporting;
do not impose a broad index load on every unfiltered/full-source query.

The subsequent pre-implementation provider check resolves B1 through the existing
`SerializedPageReaderState::Values` path: it requests a header stream and each
compressed page body separately, and the admitted reader limit stops that stream.
For a partial final prefix group, `SelectedChunks` can therefore serve checked
page-body requests directly from the held source instead of eagerly buffering the
whole selected chunk. Header streams use at most 512 bytes of bounded read-ahead
inside the same chunk; report those bytes too. Complete groups retain the original
buffered path. This uses the provider's existing page parser, requires no new offset
index read, and does not change the flat/unfiltered prefix or validation domain.

`native_relational_spill` currently merges pairs; specialized native sort and
weighted-count owners already demonstrate bounded multi-run geometry. Reuse the
native store contracts rather than introducing another spill format. Stored hash
lookup currently binary-searches global ordinals while retaining one block.

## Exercising campaign frozen before runtime edits

The accepted 347-query observation remains **11.979219492 engine seconds**, with
all 12,565 result rows. Reuse it without another baseline execution. Preserve the
accepted 48-query observation (0.920503372 engine seconds) as historical evidence;
it is not a substitute for the following added routes and is not repeated here.

The supplemental campaign has **80 complete workflows**, eight per owner family:
two declared sizes crossed with four shapes. Use deterministic seed `0x31_2026_BA7`,
NULLs at row multiples of 19, duplicate keys modulo 127, and input order consisting
of odd positions followed by reversed even positions unless order is the control.
The executable fixture manifest must expand this table literally and bind input,
plan, independent full-result reference and resource identities before either role
executes. Fixture-only intake corrections may occur before execution, with original
attempts retained; never select or discard cases using observed timings.

| Family | Sizes | Four complete workflow shapes |
| --- | --- | --- |
| B1 prefix delivery | 65,537 / 524,309 source rows | Prefix 1; prefix 127; prefix 8,193; full-source control. One row group, 1,024-row data pages, plain fixed-width plus long text, complete ordered output. |
| B2 prepared views | 4,097 / 32,771 rows | One text expression control; lower/upper/length; trim/length/byte length; distinct repeated numeric expressions. Project then count/min/max over all produced columns so the work is consumed. |
| B3 encoded reductions | 4,097 / 131,101 rows | Constant extrema/count; run-end extrema/count; sliced run-end extrema/count; primitive control. Use actual persisted Vortex representations when available, verify physical activation separately, and preserve the ordinary provider decline if persistence changes encoding. |
| B4 text append | 2,051 / 16,387 rows | ASCII lower+upper; two-column concat; four-column concat; mixed Unicode control. Long values cycle lengths 0/7/127/1,025; consume all output through lengths and deterministic extrema. |
| B5 residual join | 257 / 2,051 left rows; 509 right rows | Inner integer range; left join with two comparisons; semi join; anti join. Key fanout modulo 31, NULL/nonmatching keys, full deterministic result. |
| B6 pure correlation | 127 / 1,025 outer rows; 4,097 inner rows | Equality EXISTS; equality IN; guarded EXISTS; uncorrelated membership control. Repeated and NULL parameters, no hidden effects or finite limit. |
| B7 spill merge | 8,193 / 65,537 rows | Full stable sort; duplicate-key join build; window rank; resident sort control. Native spill enabled with a 64 KiB input buffer and 16 MiB shared grant; 256 MiB disk quota. Preserve complete results and cleanup. |
| B8 stored lookup | 8,193 / 65,537 build rows; 257 probes | Unique dispersed probes; duplicate spans crossing blocks; misses/NULL probes; keyless condition control. Same spill limits as B7; hash collision semantics also have direct native controls. |
| B9 capture/replay | 4,097 / 32,771 rows | One-row fragments; 17-row fragments; 1,024-row large-batch control; pressured one-row fragments. Capture one definition and consume it twice through deterministic aggregates/joins; include capture and replay in the engine clock. |
| B10 finite quantiles | 4,097 / 131,101 rows | One median; five continuous levels; five discrete levels with duplicates; grouped control. Levels 0/0.1/0.5/0.9/1, exact complete results with independently derived ranks/interpolation. |

All 80 workflows use the real shared native execution path. A fixture that cannot
exercise its intended source/fragment boundary must be fixed before any timing, or
reported as a protocol blocker; do not benchmark a standalone kernel and label it
the full workflow. The same plans, inputs, formats and resource settings run once
on the retained accepted runtime and once on the final candidate. No external engine
runs, repeats, profiling, cache manipulation or artificial pacing are added.

The final campaign consists of those two supplemental passes and **one candidate
347 pass**. Keep every query result and per-query/family movement. Both complete
cohorts must have lower total `engine_execution_seconds`; unchanged/higher drops
the combined runtime. There is no minimum percentage gain and no per-mechanism
acceptance timing loop. Correctness, resources, ownership and no-fallback checks
must pass first. No causal attribution or statistical confidence follows from
these single observations.

## Shared verification and closeout

Implement all admitted owners and their focused semantic/activation/fault controls
before starting the shared build cycle. Then run focused native tests, strict native
and default workspace Clippy/tests, formatting, public surface/format fixtures and
installed-wheel proof against one final build identity. If a failure requires a
source correction, retain the failure and rerun the affected checks; the batching
decision is not permission to bypass verification.

Required controls include NULL/empty/last-page/last-block boundaries, duplicate
spans, original-width endpoints, strict/extended floating policy, Unicode,
guard-inactive and empty-outer behavior, nonadmitted expressions, exact first errors,
memory refusal/replacement overlap, cancellation, source-generation changes,
spill quotas/interrupted merges/recovery, and producer/last-alias credit release.
Native observation assertions must demonstrate each optimized route and its decline.

Freeze source and runtime identities before queries. Keep task-owned builds,
subagents, inventories, hashing, packaging and profiling idle during execution;
use existing storage/deadline/result/cleanup guards. Preserve every failed attempt.
Routine communication uses engine time only. Portable evidence must replay complete
results and decisions without the original runtimes or source paths. Complete one
cohesive PR, hosted CI/review/merge and verified temporary-artifact retirement under
the standing authorization. Do not publish a release.
