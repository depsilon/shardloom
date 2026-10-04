<!-- SPDX-License-Identifier: Apache-2.0 -->

# Native nested payload composition

Status: implemented with the local acceptance records below, including the
null-parent intake review correction; hosted acceptance remains pending.
This continues
the [universal workflow plan](universal-workflow-completion-2026-10-01.md) after
[flat unary composition](native-unary-composition-2026-10-02.md), under
PERF-02/03/07/10/11/12 and CG-3/5/19/20/21. The expert comparator is a columnar
engine maintainer reviewing logical types, nested validity, selected-buffer
ownership and complete output fidelity.

## Scope and contracts

Admit statically declared lists, fixed-size lists and structs whose leaves are
already admitted bool, integer, F32/F64 or UTF8 values. Preserve parent and child
nullability independently, list order, empty lists, fixed-size cardinality,
struct field order and exact integer domains. Recursively bound schema depth,
field metadata and selected child counts before allocating. This is an extension
of Vortex DType and native arrays, not a second row representation.

The implementation admits schema depth at most 24, at most 4,096 recursive nodes
and at most 8 MiB of conservatively charged schema metadata. Each nested struct
has 1–1,024 nonempty, distinct field names. Empty structs are explicitly rejected:
the compact result contract requires an owned buffer to retain structural credit.
Top-level results retain the existing 128-column bound. Selected child coordinates
and buffers reserve against the operation's memory grant before construction.

Public typed input coverage uses self-describing Arrow IPC lists/structs and their
native Vortex preparation. CSV/JSON text intake still normalizes nested text to
UTF8, and the source-schema hint grammar admits scalar hints. Typed nested text
intake remains an adapter obligation under PERF-11. Preparation now forwards
declared compatible-source hints instead of silently dropping them; an unsupported
nested hint is rejected before output creation. Only CSV/JSON adapters receive
these hints; self-describing columnar sources and native Vortex retain their
authoritative schema.

Typed compatible intake extends the existing streaming Arrow-to-Vortex adapter,
recursive input-buffer copying and native writer. It shares the payload schema
budget with execution, preserves the retained child domain's finite-float policy,
and rejects unsupported nested leaves and Arrow extensions before conversion.
Value validation follows valid parent ranges and sliced list offsets, so hidden
children beneath null parents and children outside the logical slice do not
cause rejection. Visible non-finite leaves remain rejected. The adapter reuses
Arrow's validity-range iterator and buffer-sharing slices before the existing
Vortex conversion; it constructs no replacement value or validity buffers.
It does not create scalar-row intermediates. Ingest's recorded memory exclusions
still include original Arrow owners, reader internals and codec/metadata
allocations that bypass the host allocator; this is not full RSS accounting.

Typed empty imports retain the reader's complete Arrow schema in an empty
record batch, including root-field nullability. In Vortex 0.85, recursively
decomposing a struct with a single nullable struct child can move the child's
validity to its parent on scan. Intake therefore uses the provider's existing
field-writer overrides to keep each nested top-level field in native Flat
subtrees under Chunked layouts. Scalar field strategies retain their selected
profile. The report records the nested layout and its lack of extra compression
or cross-batch coalescing. Budgeted streaming keeps the same source-batch owner
around these strategies; ordinary buffered/unbudgeted imports gain no new
bounded-memory claim. Both paths validate the file edition's allowed encodings.

Carry these payloads through native scan, column projection/rename, scalar-key
filtering and ordering, limits, joins, UNION ALL, window payload/navigation and
subquery projection. Scalar keys and scalar functions keep their existing
semantics and explicit type admission. Merely admitting a payload does not admit
it as a hash key, comparison operand, arithmetic operand or aggregate measure.
Validate those uses before execution, including empty input. Preserve existing
flat strategies and their metadata/pruning opportunities.

Connect the existing explode binder and coordinate expansion to the same
relational tree and execution context. Explode must consume its actual preceding
stage and may feed subsequent relational or unary stages. Support repeated
explosion, nested list/struct payloads and the existing element-field projection.
Preserve the existing policies: null lists emit one null element, empty lists
emit no rows, selected multiple lists require equal effective lengths per input
row, and a source-order limit applies after expansion. A nested field not being
exploded remains a typed payload. No prefix collection, intermediate file,
source sampling, query replay or Python execution is allowed.

Retained scalar unary state continues to use its existing admitted value domain.
General nested deduplication, sampling/rewrite state, nested key semantics,
heterogeneous Variant payloads, decimals/binary/extension types and dynamic pivot
binding require their own concrete type or state extensions under the existing
owners. Dynamic pivot is a separate dependency because its column names and
types become known during execution; widening a fixed-schema binder cannot
implement it. These remaining gaps are not completion of the broader PERF gates.

## Reuse and Vortex-first provider decision

| Existing component and callers | Shared extension |
| --- | --- |
| `native_relational_batch::take_column`, `take_batch` and `Table::gather`, used by joins, sets, ordering, windows, subqueries and expression delivery | Add recursive compact gathering for admitted nested payloads, preserving the current scalar implementation. Copy selected values and validity into the existing allocator-owned buffers; do not retain unselected source domains. |
| `ReservedHostAllocator`, native reservations and reserved containers | Reserve offset/index/validity/payload and metadata capacity before construction. Output buffers retain credits through clones, slices, producer drop and source drop. |
| `BoundUnary`, explode `Plan`/`Explode`, `NativeBatch` and `CompletedRows` | Reuse coordinate selection and bounded completion. Add native-array delivery where nested values cannot pass through a scalar accessor. Keep empty results typed. |
| Relational binder and shared expression/key binders | Separate payload admission from operated-key admission; preserve explicit diagnostics before rows are read. |
| SQL table-expression parser and Python ordered stages | Add the explode spelling and carry real source declarations, allocation and operation order through existing lowering. |
| Native Vortex writer, columnar compatibility writer, JSON collector and text writer | Consume the same completed arrays. Share nested JSON traversal and compatibility schema/expansion checks; retain destination, cancellation, publication and cleanup contracts. |

Vortex-first classification: `use_vortex_native_provider`. Pinned Vortex 0.85
provides DType, ListView/FixedSizeList/Struct arrays, validity, scalar leaf access,
selection and native constructors. They remain isolated in `shardloom-vortex`
behind existing feature gates, policy and certificates. ShardLoom extends its
existing ownership and delivery boundary around those providers; it does not add
a nested query engine, dependency or Arrow execution middle.

The pinned provider source establishes three restrictions:

- `ArrayRef::take` is a logical selection; list views retain their elements and
  string views can retain data buffers. It is not proof of compact ownership.
- `builders::builder_with_capacity_in` currently discards its allocator argument.
  Generic builders therefore cannot establish ShardLoom output buffer credits.
- `Canonical::empty` handles static nested shapes recursively but panics for
  Variant. Empty construction must be admitted by dtype before invoking it.

Use existing allocator-owned scalar construction at recursive leaves and Vortex
native structural constructors. Validate checked sizes, parent validity and
offsets before copying selected children. Avoid copying hidden values under null
parents. A decoded scalar/JSON tree is not the internal nested representation.
Provider scratch and process RSS exclusions remain explicit; this unit does not
claim complete upstream allocator coverage or a process-memory ceiling.

## Delivery, formats and resources

One operation retains its existing CPU/memory grant and synchronous backpressure.
Native Vortex output preserves the admitted logical type and validity. Extend the
existing JSON/JSONL terminal traversal and Arrow IPC/Parquet/Avro/ORC schema and
buffer admission where the destination represents the shape. Record any required
format translation explicitly. CSV has no nested type system: a nested final
payload must be rejected before publication rather than silently stringified.
An exploded flat result still uses all eight existing local writers.

The implemented nested destinations are Vortex, JSON, JSONL, Arrow IPC, Parquet
and Avro. Arrow IPC and Parquet preserve the tested fixed-size-list logical shape;
the pinned Avro reader reopens it as a variable list and widens integer widths,
which the output fidelity report declares. The pinned ORC writer cannot write
list/struct fields, so nested ORC output is rejected before execution/publication,
including empty results. Columnar export limits field names to 256 bytes and
individual UTF8 values to 64 KiB, with an 8-MiB expanded Arrow batch limit.
Recursive child domains count toward that admission and writer footer metadata.

For each destination, freeze the exact accepted nested shape and any genuine
format-specific denial in tests. Do not infer fidelity from successful file
creation. Reopen every accepted output and verify all values, field names,
nulls/order and representable types. No source format or frontend receives a
separate executor or output implementation.

Keep small collection's 65,536-row and 8-MiB bounds. Native writers retain bounded
batches, checked nested expansion and their existing publication guards. Reserve
expanded child coordinates before construction; a single large list must obey
the operation's resource grant. Keep cancellation checks inside child traversal,
not only between outer rows. Native ordering spill may carry admitted nested
payloads through its existing run format after round-trip proof; this does not
authorize unimplemented join, unary or aggregate state spill.

## Acceptance and verification

Freeze independently specified complete values for nullable lists, nullable
elements, empty lists, fixed-size lists, nullable structs, nested structs/lists,
Unicode/long strings and boundary integers. Include reordered and repeated
selections, all-null/empty outputs and null-extended outer joins. Prove that
unselected large domains and values under null parents are not retained by small
owned results. Retain cloned output after dropping producer, source and session;
verify that its credits are released only with the last buffer owner.

Exercise each admitted payload stage, repeated explode, transformed join and set
operands, windows, empty schemas and complete multi-batch results. Compare SQL
and DataFrame calls, native input and explicitly declared compatible nested
input. Verify outputs below/above small collection bounds, complete flat results
through all eight writers and admitted nested results through each destination.
Add inert inspection and invalid type/shape cases before output creation.

Test narrow grants, child-count overflow, slow/failing consumers, pre/mid-call
cancellation, source replacement, spill round trips where admitted and cleanup.
No failed call may publish output or return a success certificate. All successful
calls retain `fallback_attempted=false` and `external_engine_invoked=false`.

Run focused ownership/type/operator tests while code changes, then required
workspace formatting, strict Clippy and tests, native provider/CLI suites, Python,
lean/MSRV and affected documentation/site gates. Freeze the final executable and
complete public matrix, then Full43 regression under existing serial storage and
process guards. Preserve failed observations. Availability is gated by correctness
and resources, without a speedup claim. Paused large format/text performance work,
native Python binding experiments and package publication remain paused.

Focused Rust and Python verification includes complete values and
typed empty results through all six nested destinations, explicit CSV/ORC denials,
compact ownership after producer/session drop, large-child reservation denial,
cancellation within one list, recursive schema/key denial, subquery payloads,
native ordering spill and cleanup after cancellation/consumer failure. The new
public acceptance family covers declared Arrow IPC list/struct input, ordered/repeated
explode, joins, UNION ALL, windows, membership and output above small collection
bounds. A bounded test-only Arrow fixture generator supplies independently typed
inputs; every operation still executes through the ordinary public CLI. It also
checks the full nested UInt64 domain, Avro overflow cleanup, scalar preparation
hints and rejection of unsupported nested CSV hints. Its results are frozen
against the final executable below. Public verification exposed generated Avro
record-name metadata on nested fields. Reopen validation now accepts that format
metadata while requiring exact translated names, types and nullability; regression
fixtures cover lists of nullable structs, structs of structs and typed empty output.
Native fixed-size-list fidelity is exercised by the typed Rust fixtures; compatible
public input normalization does not claim to retain a fixed-size declaration.

The maintainer additionally requested full UAT to check unexpected gains or losses.
The frozen executable passes the complete public matrix and paired Full43. The
prior accepted measurements remain intact, with complete correctness, timing and
memory comparisons against the retained control binary under the same data and
resource settings. Uncontrolled cache or host effects cannot establish a causal
speedup from a single before/after observation.

The comparison runs all 43 queries three times on each of the candidate and
retained control executables, alternating their order within each query. Report
the sum of per-query minima alongside paired medians and observed process peak
RSS. Flag median timing changes of at least 10% and 100 ms, or median RSS changes
of at least 10% and 32 MiB, for a fresh targeted cohort with reversed role order.
An aggregate timing change of at least 5% and one second warrants a complete
reversed-order comparison even when no individual query crosses its threshold.
These are investigation thresholds, not a performance claim or a reason to omit
smaller measurements. Retain all observations and host snapshots.

The [immutable acceptance packet](../benchmarks/evidence/native-nested-composition-2026-10-02.json.xz)
records actual admission, complete public/Full43 results, source identities and
local gates. Hosted acceptance is still required before the finite unit closes.
CG-1 through CG-23 and broader PERF owners stay visible;
real Vortex output proof is distinct from placeholder artifacts.

## Frozen public acceptance

The original `9f172abf` record below is preserved. The current reviewed candidate
and fresh acceptance are recorded in the following section.

The optimized `release-user-surfaces` executable from clean source revision
`9f172abf8a087752aad8de280628ed889c885b7a` passes the complete public matrix on
October 3 UTC: 2,459 checks, including 496 nested checks, 6,636,187 complete row
comparisons and 5,585 verified execution envelopes. The executable SHA-256 is
`90cd3002521dad5cad27904939e8a22bd1b0aed55bde80586f721a4af13b0941`;
the complete public summary SHA-256 is
`0c453aea73eb33f363508ae92027931c0a7839f924f60671ba65b7195c4d191e`.
Every successful call retains no-fallback and no-external-engine evidence.
The final evidence verifier also checks all 166 inert inspections and 96 expected
denials, revalidates complete results from every paired query archive, and checks
all writer/source identities. The portable packet SHA-256 is
`1f1fd3a54c1890147eb77bef1a278ec1fa62f78e5650c15d375d3d30f653b608`.

Nested payload output covers all 65,541 rows through both public spellings and
all six representable destinations. Repeated explode produces all 131,082 flat
rows through every local writer. The small-collection guard and nested CSV/ORC
denials remain explicit. Typed native fixtures separately prove fixed-size-list
fidelity, ownership, narrow grants, cancellation and native sort-spill cleanup.

Default workspace tests pass 3,446; native Vortex passes 2,224 with 23 existing
ignored tests; native CLI passes 1,577. Python passes 717 with 144 existing skips.
These configuration counts overlap. Formatting, strict Clippy, lean/MSRV,
no-write, UAT harness, documentation and generated-site gates pass. The default
gates precede a borrow-only change to one test helper; the final native gates
include it, and the evidence verifier proves that exact source difference.

Before the paired Full43 run, twelve completed profiling sample logs from three
historical targeted cohorts were archived losslessly. Per-member hashes, original
file identities and archive readback were checked before removing the originals;
summaries and failed/incomplete cohorts remain unchanged. This recovered 5,074,944
accounted log bytes without raising storage limits.

The [paired Full43 report](../benchmarks/native-nested-full43-2026-10-03.md)
passes all 258 complete retained-result comparisons. The sum of per-query minima
is 50.739772 seconds control and 50.895000 seconds candidate (+0.3059%); median
sums are 51.688471 and 51.678695 seconds (−0.0189%). No query or aggregate timing
or memory screen crosses its predeclared repeat threshold. The observed behavior
is unchanged at those thresholds; availability does not depend on claiming a
speedup. The earlier unpaired control measurement remains preserved.

## Acceptance after hosted review repairs

Frozen `3b94ba2e1fdc7d1860398265856c44aa02a28f74` passes the complete public
matrix again: 2,459 checks, 496 nested checks, 6,636,187 row comparisons and 5,585
execution envelopes. The fresh verifier checks all 96 expected denials and 166
inert inspections. The three review findings are repaired: text-only schema-hint
forwarding, exact nested empty/intake nullability, and recursive JSON/JSONL UTF8
copy evidence. Expanded intake regressions additionally preserve complete nested
validity through the existing Vortex field-writer seam and four writer profiles.

The [fresh Full43 report](../benchmarks/native-nested-review-full43-2026-10-03.md)
records 258/258 matching results. Fastest-query sums are 51.112827 seconds control
and 51.254897 seconds candidate (+0.2780%); median sums differ by −0.8194%.
Q21's +10.4217% median RSS observation triggers the required reversed-order repeat,
which passes all six results and records −7.7394% median RSS with no threshold
crossed. The initial increase does not reproduce; both observations remain intact.
The unpaired public correctness-suite wall-time increase is reported separately.

All 24 selected local gates pass, including 3,446 default workspace tests,
2,227 native Vortex tests (23 existing ignored), 1,577 native CLI tests and 718
Python tests (144 existing skips). Required runtime/test hashes match the clean
frozen build; unchanged harness-only gates retain their verified earlier receipts.
The [new immutable packet](../benchmarks/evidence/native-nested-composition-review-2026-10-03.json.xz)
has SHA-256 `59260176b250dc1c80f0a218fe911ab16aeeb244e800aed5428af3b4cfe1955f`.

The [October 4 website dependency update](../dependencies/website-build-dependency-review.md#2026-10-04-registry-update)
restores a clean dependency audit without enabling the earlier exception. The
finite unit and broader PERF owners remain open until their respective gates close.

## Null-parent intake review correction

An additional review found that recursive finite-float admission traversed whole
child arrays, including values hidden by null list/struct parents and children
outside a sliced list's logical offsets. Two regressions reproduce the rejection
before the fix. Validation now visits only reachable child ranges while keeping
recursive schema admission independent of parent validity.

Focused coverage passes for lists, large lists, fixed-size lists, structs,
all-null and empty slices, visible non-finite rejection, and complete Arrow IPC
to Vortex readback through buffered, streamed and budgeted intake. All eight
selected source checks pass on `90891423b8cfcf0fd865aeb84bb3b7eb82160c37`:
formatting, default and native strict Clippy, default workspace tests (3,446),
native Vortex tests (2,246; 23 existing ignored), native CLI tests (1,577),
native-without-write Clippy and the lean workspace check. These configuration
counts overlap. The
[review packet](../benchmarks/evidence/native-nested-intake-review-2026-10-04.json.xz)
retains exact source hashes, all logs, the initial reproduction and the corrected
signed-width compiler check. Its SHA-256 is
`604e17f3826a745cb8aff2635fc22f540d6943eca1c612c6d697afb4098c0a41`.

The earlier immutable public and Full43 records remain evidence for their
original revisions. Full43 was not rerun for this correction: it reads an
existing native Vortex artifact and cannot reach the changed nested Arrow-intake
traversal. No timing, benchmark artifact or performance claim changes. The full
native suites include the new adapter regression; hosted checks still gate merge.
