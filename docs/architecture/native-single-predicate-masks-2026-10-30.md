# Single-predicate masks — rejected candidate and frozen contract

Disposition: **dropped** after one complete 347-case observation at
12.199579628 engine seconds, against the accepted 12.055132613 seconds.
All 12,565 complete result rows pass. The candidate's runtime changes and
candidate-specific test sources are removed; exact source bytes and the patch
remain in the [experiment evidence](../benchmarks/native-single-predicate-masks-2026-10-30.md).
The accepted baseline does not advance. Grouped cost attribution follows.

The contract below was frozen before runtime edits on October 30 against
`7cdb2d643c5915d689f422ea18c108b556ee1f9c`. Development used the isolated
`codex/native-predicate-masks` branch. Performance execution waited for #1628
hosted acceptance; the packet preserves each earlier contract version.

## Hypothesis and scope

Extend the existing finite `native_relational_predicate_block::Recipe` owner to
accept one I64 column/non-null I64 literal comparison, either operand order,
and direct I64 column IS NULL/IS NOT NULL. Preserve AND/OR/NOT composition,
bounded program size and exact binding. No arithmetic, coercion, function,
column-to-column comparison, null literal, other width or nested value is newly
admitted. In particular, a fallible or lazy child cannot enter this program.

Single filters retained above the scan can build a Boolean array, prepare that
Boolean as a key column and traverse it to select true rows. The candidate uses the existing
credited packed true mask and LocalSelection consumer. Compare inputs still use
the same native numeric owner and complete declared input validation. Null tests
also prepare that owner; this candidate does not bypass payload validation or
claim to avoid reading/decoding numeric input. Projected predicates retain TRUE,
FALSE and NULL; null tests themselves yield non-null Booleans. Source demand and
the callback/observation/completion barriers are unchanged.

Provider decision: `implement_shardloom_kernel`, extending an already admitted
ShardLoom program. Pinned Vortex 0.90.0 has allocated primitive comparison arrays,
lazy IsNull/IsNotNull and Mask/BitBuffer. Inspected provider sources are
`vendor/vortex-array/src/scalar_fn/fns/binary/compare/primitive.rs`,
`vendor/vortex-array/src/scalar_fn/fns/is_null.rs` and the pinned
`vortex-mask-0.90.0/src/lib.rs`. Reuse Mask/BitBuffer and the session allocator.
The array-producing comparison path does not remove the downstream Boolean key
owner/selection loop by itself; validity-only null checks would change the
current numeric preparation boundary. No new dependency or foreign executor.

## Falsifiable work evidence

Debug-test observations will count mask batches/rows/allocated bytes and generic
Boolean-selection rows. Existing key-column preparation counts distinguish one
input owner from the former input/literal/Boolean preparation. Observe the
ordinary filter consumer, not only the private recipe helper. Compare exact
same-source prepared inputs with and without the recipe in tests where both
routes remain accessible, then verify complete rows and final memory refunds.
Production release measurements contain none of these debug counters.

## Correctness and controls

- All six comparisons, both operand orders, I64 endpoints and values above 2^53;
  nullable, all-valid and all-null inputs; NOT and mixed comparison/null-test
  truth tables. Check projected three-valued results and TRUE-only selection.
- Empty and one-row inputs, 63/64/65-bit boundaries, unaligned slices and tails;
  primitive, constant, dictionary and chunked representations. Check full,
  empty, sparse and dense selections and the U16/usize extent boundary.
- Repeated actions and many small/empty producer batches; selected payload
  lifetime, credited mask clones and last-alias refunds. Refused allocation,
  cancellation and changed/missing bound columns must release partial work.
- Exact declines for I32/U64/F64/text/nested columns, null or mismatched literals,
  column-to-column comparison and fallible/lazy expressions. No hidden failures
  may disappear merely because a null test or surrounding predicate rejects a row.
- Fresh public fixtures with seed 0x3020269A1 and rows 0, 65, 4097; include
  direct/renamed/named-relation forms, filtered global/grouped aggregates and
  projected Boolean controls. Exercise Vortex, Parquet and Arrow IPC across the
  five existing native interfaces with independent complete reference results,
  complete-consumption/no-fallback evidence and source ownership checks.

The existing million-row 347 corpus supplies the bounded larger sentinel and
regression cohort. New public controls are correctness/resource/scaling evidence,
not a replacement timing cohort or authority for broader type admission.

## Acceptance and observation discipline

Use the retained original 347 harness at `benchmark-harness-r2`, original order,
inputs, independent references, memory budget and parallelism. Exactly one
candidate execution of each case; no concurrent task work or agents, no cache
manipulation, artificial pacing, new external engine run or routine repeats.
Full input hashes and source/build inventories stay outside active queries.
Keep the standard lightweight guard, deadlines, active-output budget, complete
result validation and cleanup.

Previous accepted baseline is `broad-bounded-ordering-r1`, runtime
`bounded-ordering-release-r2`: 347 complete cases, 12,565 complete rows,
12.055132613 engine seconds. The application callback unit is separately proved;
this SQL corpus does not invoke callbacks or the corrected DataFrame dropna
helper. Preserve that source difference and make no isolated causal attribution
from a single cohort. `--family` declares diagnostic families only; it never
subsets execution. Declare predicate, nullable-expression, filtered global and
filtered grouped families, with all original cases retained as controls.

After correctness, resources, ownership and no-fallback pass, a lower complete
engine sum retains; unchanged or higher drops. Preserve every observation,
per-case/family regressions, build/runtime/source identities and failures.
Required default/full-native lint and tests, installed package and hosted checks
remain acceptance gates. Full43 remains held until item 18. No package release.

Grouped cost attribution follows this decision; it is a separate diagnostic
step and must not be mixed into the mask performance observation.

## Activation correction before performance execution

The first candidate test (`m002-mask-focused`) passed 91 selected tests and
failed two new fixture assertions; one manual benchmark remained ignored.
No performance observation had run. The activation assertion exposed an
overbroad premise in the original draft: directly pushable comparisons and null tests already
execute through Vortex's scan filter. They do not construct the general
ShardLoom Boolean filter result. `push_scan_filter` and its provider admission
stay unchanged. The candidate applies to predicates retained above a scan,
including a computed projection, and to projected Boolean expressions. Reversed
comparisons and negated predicates can also remain in the relational owner.
Ordinary directly filtered aggregates are controls, not assumed beneficiaries.

The real-pipeline activation fixture now uses an existing computed-projection
barrier (`n + 0`) and separately proves direct scan pushdown is preserved.
Public controls include filtered aggregate consumers above that barrier. The
other failing fixture exceeded the producer's existing row bound and would
have been split into smaller scan units; the U16/usize selection boundary is
therefore exercised directly through mask-to-selection with exact complete
row comparisons. Neither resource limits nor scan widths are changed.

Preserve the original draft and failed test receipt in the evidence packet.
This correction narrows expected reach; it does not change the frozen complete
347-case acceptance rule or authorize replacing an admitted upstream provider.

## Public fixture correction before performance execution

The first public run (`m018-mask-public-results`) retained 1,269 complete-value
comparisons and 36 refusals, with no value mismatch. The literal-left spelling
`17 > optional_value` is accepted by the typed relation interface but rejected
by the existing SQL predicate parser. All 36 refusals are that spelling across
the other four interfaces, three formats and three sizes. Keep the failed run
and every completed comparison; it is not complete public acceptance.

The corrected fixture retains the original literal-left reference and native
typed declaration. The four SQL-backed interfaces submit the equivalent
`optional_value < 17`, explicitly recorded beside each saved result. Private
native tests still cover both operand orders for all six comparisons. This
correction changes one test fixture, with byte-identical release products; it
does not add SQL syntax, change runtime source, alter the 347 corpus or relax
the complete 1,305-comparison acceptance requirement. The SQL spelling limit
remains explicit.

The public non-I64 signed-integer control uses the corpus's I16 `small_value`.
Its original case identifier is `mask_decline_i32`; the saved schema records
I16 accurately. I32 refusal is separately covered by the private native tests.

## Preconditions completed before the performance pass

PR #1628 passes all 57 hosted checks and merges as `b6883adc`; its source tree
equals the tested `7cdb2d64` tree. The application
[closeout](../benchmarks/evidence/native-application-batch-closeout-2026-10-30/closeout.json)
records integration and exact generated-artifact retirement. Candidate local
acceptance passes 6,581 checked native tests, 3,290 default tests, strict lint
and formatting, all 1,305 corrected public comparisons over 779,070 complete
rows, and 284 installed-package tests with no skips. Independent saved-result
replay verifies both public attempts, their unchanged independent references,
runtime/source binding and debug-only work observations. An initial collector
failure (`m024`) omitted a retained-path alias; its bytes already existed and
the corrected collection/replay (`m025`) passes without new query execution.

All original failures remain evidence. The subsequently completed 347-query
observation rejects the candidate under the frozen rule above.

## Retained harness compatibility before execution

The first freeze attempt (`m026`) stopped before creating the cohort directory
or executing any query. Current source discovery includes the new application
callback module, which is absent from the original retained harness. Eight
other Python package files differ solely across the already accepted
application change. Their retained bytes match `65be06c9`; their current bytes
match `7cdb2d64`. The new module is absent from the former source tree.

Keep every original harness byte. The original worker loads the declared
native extension directly; the `python_sql` adapter calls `native.Session.sql`
and consumes its complete native result. This does not exercise the new Python
callback or DataFrame helpers. Bind the exact nine source differences to those
two commits, and reject any other difference except the previously recorded
reporting-prefix correction. Require the complete retained harness inventory
to equal the accepted baseline's inventory. The original observer, worker,
supervisor, queries, order, resources and clocks remain unchanged. Current
native-runtime composition still differs from the earlier baseline, so this
is not an isolated attribution of any timing change to masks.

Preserve the failed freeze and original driver beside the corrected freeze.
This compatibility correction is recorded before the first performance query;
it neither refreshes the baseline nor changes the complete-cohort decision rule.
