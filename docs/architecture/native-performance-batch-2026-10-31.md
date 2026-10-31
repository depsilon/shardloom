# Combined native performance batch — October 31

Final local decision: **retain B6/B9 plus the shared metadata correctness repair;
drop the other eight optimizations**. The reduced candidate passes both unchanged
cohorts: 31.669017456 versus 40.838371451 engine seconds for 80 workflows, and
11.978234339 versus 11.979219492 for 347 queries. The latter is effectively flat
and passes the binary rule by 0.000985153 seconds in one observation. The
[complete result record](../benchmarks/native-performance-batch-2026-10-31.md)
preserves all movements and proof boundaries. The original ten-owner batch stays
rejected. The prospective sections below are historical contracts, with their
exact pre-execution versions preserved in the evidence packet.

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

B3's initial implementation admits integer storage only. In particular, floating
COUNT currently observes validity without rejecting an unobserved nonfinite value;
the existing encoded numeric visitor's scalar access has a stricter domain. Keep
all floating columns on the original path instead of changing that contract.
The B3/B5 physical checks also unwrap bounded, shape-identical single-chunk
ownership and native field/selection projections. They never flatten multiple
chunks or execute an unknown child merely to identify its encoding. B5's
multichunk sources retain the ordinary condition path; integration controls
exercise both that decline and the admitted file-backed single-chunk path.

B6 admits statically bound resident scans and direct column projections, with
an inner equality join or a pure equality filter over a cross join to `Outer`.
Both source projections must expose already validated host primitive integer or
Boolean buffers, including bounded native chunk composition and canonical
validity. Logical types alone do not establish that an unselected encoded value
is infallible. File, generated, opaque encoded and batch/callback sources retain
per-parameter execution; this narrows the original native-file proposal before
any campaign observation. Every exposed intermediate field is integer or Boolean.
Rewritten relations initialize only at the first active outer row, including when
there is no explicit guard. The SQL frontend's terminal Boolean `true` row-presence
projection for `EXISTS` is also infallible; it supplies no membership key origin.
B9 copies canonical primitive/Boolean fragments directly to
credited buffers; noncanonical, nested, text and large-batch inputs keep immediate
bounded detach. Raw floating storage moves without observing nonfinite values.

B7's extra-reader admission requires flat payload fields as well as flat ordering
keys. Installed-package validation found that first-occurrence ordering can have
a numeric ordinal key while retaining rich set-union payloads alongside the key
reducer. Looking only at the key admitted extra reader and writer overlap beyond
the existing 16 MiB contract. Rich payloads retain two-way merging; native controls
cover this decline even with ample headroom, and the existing complete rich-set
public control keeps its original grant.
The subsequent full native run found one stale reader-count assertion in the
typed-key fixture: its date/timestamp payload also requires two readers when
the ordering key is binary or decimal. Complete values had already matched;
the assertion now follows the payload admission boundary, with the failed run
retained and no further runtime change.

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
| B7 spill merge | 8,193 / 65,537 rows | Full stable sort; duplicate-key join build; window rank; resident sort control. Native spill enabled with a 1 MiB input buffer and 16 MiB shared grant; 256 MiB disk quota. Preserve complete results and cleanup. |
| B8 stored lookup | 8,193 / 65,537 build rows; 257 probes | Unique dispersed probes; duplicate spans crossing blocks; misses/NULL probes; keyless condition control. Same spill limits as B7; hash collision semantics also have direct native controls. |
| B9 capture/replay | 4,097 / 32,771 rows | One-row fragments; 17-row fragments; 1,024-row large-batch control; pressured one-row fragments. Capture one definition and consume it twice through deterministic aggregates/joins; include capture and replay in the engine clock. |
| B10 finite quantiles | 4,097 / 131,101 rows | One median; five continuous levels; five discrete levels with duplicates; grouped control. Levels 0/0.1/0.5/0.9/1, exact complete results with independently derived ranks/interpolation. |

All 80 workflows use the real shared native execution path. A fixture that cannot
exercise its intended source/fragment boundary must be fixed before any timing, or
reported as a protocol blocker; do not benchmark a standalone kernel and label it
the full workflow. The same plans, inputs, formats and resource settings run once
on the retained accepted runtime and once on the final candidate. No external engine
runs, repeats, profiling, cache manipulation or artificial pacing are added.

Pre-execution fixture intake correction: the original proposed 64 KiB public spill
buffer is invalid under `VortexRelationalSpillPolicy::new`, whose existing minimum
is 1 MiB. Use that minimum for both B7 and B8 without lowering runtime admission.
This correction precedes all campaign executions; the original proposal remains
in commit `787c6052`. Verify actual spill activation during separate controls.

Further fixture intake details, also fixed before either performance pass:

- B1 uses 127-character text and explicit 1,024-row writer batches/page limits;
  its final page may be shorter. B1 retains one row group. Every Parquet source
  is reopened with all schemas and values checked before execution.
- B3's constant column is the nonnullable integer 7; a constant cannot represent
  mixed NULLs. The other three shapes carry the declared NULL pattern. Native
  fixture export verifies stored encodings and reads back every value through the
  upstream Vortex provider without running a query engine. Terminal `lo+0` and
  `hi+0` projections require the composed relational path. The original bare
  aggregate plus simple HAVING shape can select the already existing standalone
  aggregate provider and would not establish B3 activation.
  Sliced RunEnd fixtures use Vortex 0.90's validated
  `RunEnd::try_new_offset_length` constructor with offset 7. A lazy `Slice`
  expression is not serializable; the failed fixture attempt remains recorded.
- B6 binds canonical resident integer inputs before the engine clock. This follows
  the physical admission proof above. Both runtime roles use identical bindings.
- B7/B8 retain a 1,034-character payload through sort, join or window state and
  compute its length afterward. The resident sort control uses a 16-character
  payload. Their sources use 256-row Parquet groups to bound row decoding.
  Complete source metadata must also fit the same grant. Join results
  are completely consumed by per-probe count, minimum,
  maximum and payload-width reductions, keeping final output within the same
  declared grant while preserving duplicate-build work.
- B9 names the same streaming definition in two aggregate consumers. Opening the
  producer, native conversion, owned capture and replay belong to the engine
  clock. The pressure case consumes all 16 additional integer columns and uses
  a 64 MiB grant and the same 1 MiB spill buffer as the other pressure fixtures.
- B10 uses nonnegative integer values for continuous interpolation and repeated
  values modulo 127 for discrete ranks. Independent references derive ranks from
  the exact binary floating level using Python rational arithmetic.

Separate small public controls use different input sizes (33, 2,051, 4,099 or
16,385 rows), complete independent references and spill/producer/ownership
assertions. They establish fixture binding and path activation; their clocks do
not enter either acceptance cohort. The four encoded-input controls also run in
the native test suite. The declared 80 workloads are never executed as warmups.

The first public control attempt completed 28 comparisons and rejected eight
resident/streaming bindings before result production: the fixture supplied a
display type name to the low-level API instead of its serialized Vortex DType.
Both public and campaign bindings now serialize the existing nullable Int64
declaration. Input values, SQL, references, resource settings and runtime sources
are unchanged; the original attempt is retained. This correction precedes any
acceptance workflow execution.

The corrected bindings also exposed two fixture-contract corrections before
timing: `CASE` with `EXISTS` and `FALSE` branches has a nonnullable Boolean result;
the 18-column replay control requires more than the original 16 MiB grant while
writing native spill blocks. B9's pressure shape now uses 64 MiB for both runtimes,
with the 1 MiB spill buffer unchanged and actual spill required by the public
control. The failed 16 MiB attempt remains retained. These changes do not alter
runtime admission or measured input populations.

The first accepted-runtime campaign attempted all 80 cases and has no valid
complete-cohort engine total: 57 results passed, 16 bindings failed, and seven
large spill cases were denied memory. Its protocol, inputs, observations and
complete successful outputs remain immutable. No candidate campaign had run.
This invalid observation requires a declared revision, not a performance
disposition or selective retry.

The driver correction restores bindings: JSON deserialization turns field tuples
into lists; the driver now restores tuples at both resident and batch API
boundaries before execution. For the seven 65,537-row B7/B8 spill cases, the
599,963-byte Parquet footer requires `32 * 599963 + 8192 = 19,207,008` bytes
under the existing metadata admission policy, exceeding the original 16 MiB
grant before row decoding. A proposed per-case increase to 64 MiB was withdrawn
before declaration or execution after the maintainer clarified that the engine
must adapt within the user's ceiling. The original 16 MiB grant, 1 MiB spill
buffer, 256 MiB quota, data bytes, SQL, expected results, case order and populations
remain the regression contract. Correct the shared metadata admission boundary
and audit how existing dynamic memory/work sizing applies before resuming timing.
Retain the invalid first pass separately; never combine its successful cases
with replacement timings. A revised comparison must explicitly declare how it
handles an accepted runtime that cannot complete the original low-memory cases.

The revised supplemental comparison uses the accepted production sources plus
**only the shared metadata-admission repair** as its reference. The original
accepted binary, failed 80-case observation and successful historical 347/48
observations remain immutable. Verify every Rust/Cargo input against the accepted
runtime manifest before applying the enumerated metadata patch; retain exact
patch, build, provider and runtime identities. The corrected reference contains
none of B1–B10. Apply the identical metadata repair to the combined candidate.
This is a declared correctness prerequisite, not a retroactive change to the
accepted runtime or its measurements.

The failed first attempt exposed this workload and informed the shared repair.
The revised pair is therefore a regression comparison over previously observed
cases, not an untouched holdout. Preserve the original fresh declaration, and
verify that the ten optimization implementations are unchanged since that
attempt; only the common metadata repair changes production inputs.

The first candidate release build reused both corrected-reference products from
the shared Cargo cache. Reject that build before freezing or executing the
candidate. Invalidate the three affected release packages, require fresh
compilation of the native engine and both entry points, and verify distinct
runtime product identities. Preserve the rejected build receipt and hashes;
compiler success alone does not establish which source tree executed.

The first corrected-reference attempt stopped after one successful query: the
revised harness copy accidentally included 13 generated Python bytecode caches,
and the unchanged generation guard detected one being rewritten. Preserve that
interrupted observation with no complete engine total. A third declaration copies
exactly the original declared Python source inventory, leaving generated caches
outside the immutable input list as in the original harness. Source bytes, query
execution, supervision, clocks, data and resource limits remain unchanged. Start
a new complete reference pass; do not reuse the interrupted query's timing.

After correctness and resource checks, execute one complete corrected-reference
80-case pass and one complete candidate 80-case pass, using the same frozen data,
SQL, order, original resources and tuple-binding correction. No successful case
from the failed pass supplies a replacement timing. Compare the complete totals;
retain the unchanged 347-case candidate gate against the saved accepted total.
The ten optimizations still require both complete-cohort gains. A loss drops the
optimization batch; it does not justify restoring the demonstrated metadata
admission defect or hiding the failed first pass. The broader engine-wide
integration remains [queued after universal I/O](phased-execution-plan.md#engine-wide-adaptive-execution).

### Dynamic execution within a user-set ceiling

The October 31 clarification governs the engine generally: use the input's actual
schema, representation, cardinality and current shared memory headroom to choose
execution geometry. Do not recognize fixture IDs or increase resource grants for
particular row counts. Keep the caller's memory/CPU ceiling authoritative. Apply
existing shared admission, sizing, streaming, pruning and spill mechanisms at
the ownership boundary that actually needs them; a downstream adaptive operator
cannot repair an earlier unconditional provider reservation. Fixed format rules,
semantic capability checks and bounded proof/cancellation policies remain
explicit safety contracts rather than workload tuning.

The demonstrated first correction is metadata admission for every ordinary local
Parquet source, including directory members. Reuse the existing `LiveMemoryPool`,
held source generation, cancellation and provider metadata parser. The provider
first inspects the borrowed footer structure without allocating row metadata.
Before each row group's metadata is parsed, admit the retained preceding groups
plus that group's bounded parsing workspace. After parsing, account for the
provider's retained metadata size and release temporary headroom. Keep the raw
footer read separately credited through the existing source reader. Preserve all
statistics, embedded Arrow logical metadata and source/row-group identities;
neither pruning semantics nor decoded values may change. Insufficient memory
still fails explicitly under the same ceiling. This is reservation accounting,
not a claim that the third-party decoder or process RSS uses a custom allocator.

Provider check: pinned Vortex remains the execution and persistence substrate;
it does not replace this compatibility-input Parquet metadata parser. The
already pinned Apache-2.0 Parquet provider owns metadata parsing and exposes a
retained-size calculation but lacks a shared admission callback. A narrow local
callback connects that parser to ShardLoom's existing memory owner; it introduces
no query engine or alternative execution path. Test both complete metadata
equality and denial/cancellation/cleanup, with multiple footer shapes and grants.

The wiring audit distinguishes actual runtime adaptation from planning reports.
`AdaptiveSizer` selects advisory segment/task shapes; its dynamic feedback mode
does not execute feedback. The demanded Parquet reader currently uses 8,192-row
batches, and relational spill/replay have bounded block sizes and independent
memory admissions. None consumes the advisory sizing feedback. Connecting a task
size report to these owners would not, by itself, establish safe decoder or merge
allocation behavior. The metadata correction therefore operates at the actual
allocation boundary through the shared pool. It establishes neither universal
adaptive batching nor a larger memory grant, and those broader capabilities must
not be implied by the existence of the planning APIs.

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

## Selective retention after the complete batch

The complete ten-owner candidate passes all correctness comparisons but misses
its original combined benefit gate. Its unchanged 347 cases total
**12.387645581 engine seconds**, versus **11.979219492** previously accepted;
all 12,565 rows match. Its supplemental 80 cases total **27.771706382**, versus
**40.838371451** for the metadata-corrected reference; all 871,209 rows match.
Preserve that whole-batch rejection. Grouped aggregation contributes 0.261693505
of the 0.408426089-second broad regression. Four individual cases trigger the
prospective diagnostic flag; no family does. These are single observations,
with no isolated causal attribution.

After reviewing those results, the maintainer explicitly requested retaining
only the named gain areas and dropping the rest. This authorizes a new reduced
composition: **B6 pure correlated membership and B9 tiny-fragment capture/replay**,
with the shared metadata-admission correctness repair. Remove B1–B5, B7–B8 and
B10, including their test-only observation hooks and implementation-only tests.
Keep all original experiment evidence and independent public/reference fixtures.
The recorded B6 totals are 0.861917833 → 0.007997251 seconds; B9 totals are
18.602654124 → 7.925751876 seconds. Neither is proof that the reduced binary has
already passed its own complete-query acceptance.

Retain the two owners' exact tested implementations and required dependencies:
the resident integer/Boolean proof and deferred first-active-row initialization
for B6; credited native primitive construction, bounded capture and producer/
last-alias lifetime controls for B9. In shared files retain only those hunks.
All remaining Rust/Cargo/Python production inputs must equal the independently
verified metadata-only reference. Record every retained, restored and removed
file with exact before/after identities before building. Do not restore the
known metadata denial, widen the user's resource ceiling or introduce another
optimization while making this reduction.

Run the focused correlation, replay/capture, original spill/recovery and metadata
controls, then native/default workspace checks, public interface/format controls
and installed-package proof for the reduced source. Freeze its release products
and exact reduction evidence. Reuse the successful corrected-reference 80-case
observation and the accepted 347-case observation without repeating either.
Execute one new complete candidate pass for each unchanged cohort, with the same
cases, order, references, engine clock, resource limits and isolated supervision.
Both complete totals must improve for performance acceptance; retain all case/
family movements and failures. The original ten-change result remains rejected
regardless of the reduced composition's outcome. No Full43, competitor run or
release publication is added. Item 23 remains queued after universal I/O.
