<!-- SPDX-License-Identifier: Apache-2.0 -->

# Native nested payload composition

Status: implemented with focused verification; complete public UAT, Full43 and
hosted acceptance are still pending. This continues
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
nested hint is rejected before output creation. This does not override native
Vortex's authoritative dtype.

Typed compatible intake extends the existing streaming Arrow-to-Vortex adapter,
recursive input-buffer copying and native writer. It shares the payload schema
budget with execution, preserves the retained child domain's finite-float policy,
and rejects unsupported nested leaves and Arrow extensions before conversion.
It does not create scalar-row intermediates. Ingest's recorded memory exclusions
still include original Arrow owners, reader internals and codec/metadata
allocations that bypass the host allocator; this is not full RSS accounting.

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

Focused verification currently passes 82 relational tests, 27 SQL relational
tests and 24 Python relational routing tests. This includes complete values and
typed empty results through all six nested destinations, explicit CSV/ORC denials,
compact ownership after producer/session drop, large-child reservation denial,
cancellation within one list, recursive schema/key denial, subquery payloads,
native ordering spill and cleanup after cancellation/consumer failure. The new
public acceptance family covers declared Arrow IPC list/struct input, ordered/repeated
explode, joins, UNION ALL, windows, membership and output above small collection
bounds. A bounded test-only Arrow fixture generator supplies independently typed
inputs; every operation still executes through the ordinary public CLI. It also
checks the full nested UInt64 domain, Avro overflow cleanup, scalar preparation
hints and rejection of unsupported nested CSV hints. Its results must still be
frozen against the final executable. Native
fixed-size-list fidelity is exercised by the typed Rust fixtures; compatible
public input normalization does not claim to retain a fixed-size declaration.

The maintainer additionally requested full UAT to check unexpected gains or losses.
After the executable is frozen, rerun the complete public matrix and Full43, retain
the prior accepted measurements, compare complete correctness, timings and memory,
and repeat notable changes against the retained control binary under the same
data/resource settings. Uncontrolled cache or host effects cannot establish a
causal speedup from a single before/after observation.

Update this contract with actual admission and immutable evidence before marking
the finite unit complete. CG-1 through CG-23 and broader PERF owners stay visible;
real Vortex output proof is distinct from placeholder artifacts.
