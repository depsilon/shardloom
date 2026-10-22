<!-- SPDX-License-Identifier: Apache-2.0 -->

# Encoded Arrow control for the native exchange experiment

Status: accepted finite opt-in prerequisite, October 20, 2026. The
[control acceptance record](../benchmarks/evidence/native-exchange-control-2026-10-20/acceptance.json)
binds the checks and complete independent fixture results. This note supplies the
representation contract for the optimized Arrow control in the
[native exchange protocol](native-encoded-exchange-protocol.md). It does not
admit a transport candidate, report a measurement or extend default package
support. The four native-exchange substeps and all broader RFC 0044/CG-23 gates
remain open.

## Source finding and decision

Pinned Vortex 0.85 `ArrowSession::execute_arrow` accepts explicit Arrow target
fields. Its dictionary and run-end exporters preserve the codes/values and
ends/values representation when the native input has the corresponding
encoding. Arrow 58.3 IPC and Flight can carry those arrays. Flight's default
dictionary hydration must be replaced by `DictionaryHandling::Resend` for this
control. The three fixed IPC options will be uncompressed, LZ4 frame and Zstd;
every option is a separate comparison role, with conversion and codec work
included. A favorable option will not be selected after seeing candidate scores.

The current ShardLoom result exporter deliberately requests logical Arrow types
and canonicalizes its input. Its public Arrow intake has a closed type match
that rejects Dictionary and RunEndEncoded before copying buffers or invoking
the Vortex importer. Using those defaults alone would weaken the required
control. Provider support does not establish current application support.

The opt-in `native-encoded-exchange-experiment` prerequisite is implemented.
Its Arrow intake may admit integer-key dictionaries and signed Int16/Int32/Int64
run ends whose values are Boolean, integer, UTF-8 or binary. Those values have
no floating-point or decimal-domain validation rule to bypass. Both wrappers
may appear under already admitted static list/struct fields, but their own
values cannot contain another wrapper, a container, extension, float or decimal.
Run-end fields must be nonnullable; child extension annotations are rejected.
Default and `release-user-surfaces` builds retain their existing closed policy.

This is a finite comparison contract, not a claim that Vortex or Arrow cannot
support other encoded types. Unsupported combinations remain deterministic
errors. Expanding this profile later requires separate logical-value,
nullability, nesting and resource evidence.

Arrow's checked record-batch constructor checks physical NULLs. A dictionary
value or run value can therefore be logically NULL even when that check accepts
a nonnullable top-level field. The pinned importer rejects these NULL value
pools when importing a nonnullable field; it does not silently coerce them.
The focused tests cover this refusal and exact release, alongside successful
nonnullable fields with no NULL values. The finite transport fixtures declare
nullable fields for dictionaries and run ends. Nonnullable encoded children
with NULL values hidden by parent masks are not certified by this prerequisite.

## Ownership and export

Reuse `ResidentMemorySource::from_arrow_record_batch` and the existing recursive
owned-buffer copy. Validate complete Arrow coordinates and schema before the
copy. Provider import must produce the same logical Vortex dtype and preserve
Dictionary/RunEnd physical representation. Retained buffers and metadata keep
their native credits after transport/session wrappers are dropped. Denial,
cancellation and final owner release must leave no credits. Do not use an
Arrow-to-Vortex-IPC-to-source round trip for the Arrow control: that would add
an unnecessary serialization penalty.

The experiment's sender will request fixture-declared physical Arrow fields
directly from the pinned Vortex provider, before ordinary result export can
canonicalize them. It must validate the resulting Arrow arrays and exact schema,
retain their source/result owners, and charge conversion/work buffers before
allocation. Provider allocation observations remain separate from native
credit. No row/JSON bridge, query-engine integration, patched transport codec
or pointer transfer is admitted.

## Required prerequisite evidence

Add and discover nonzero tests for all eight dictionary key widths, all three
run-end widths, NULL keys/values, empty arrays, slices, nested placement, shared
children and differing dictionary domains. Compare complete values, logical
types and physical encoding IDs after import and subsequent native projection.
Check ownership after the caller's Arrow arrays and session handle are dropped,
and check denial/cancellation and exact release after the last extracted owner.
Reject float/decimal/container values, invalid key/end types, nullable end fields,
child extension annotations and excessive nested schema depth.

Independently exercise explicit provider export and Arrow IPC round trips for
the admitted encodings and all three codec options. A provider error is a failed
prerequisite, not permission to hydrate the control or remove a required fixture.
Run the ordinary policy's negative checks without the experimental feature.

The eleven focused tests pass. They also exercise List, LargeList and
FixedSizeList placement with nullable parents and sliced children. The ordinary
release feature group's separate negative test passes. The complete experimental
native suite passes 3,052 library tests, with 31 intentional ignored tests, plus
its enabled example tests. Required workspace formatting, strict Clippy and all
target tests pass. Strict experimental-feature Clippy and fresh focused checks
also pass after adding completion-digest evidence to the fixture test.

Thirteen independent upstream-produced native fixtures each execute four real
native projections through eight routes: original native intake, ordinary native
IPC round trip, three Arrow IPC codecs and three standard Flight codecs. Each
validation checks 416 complete results, logical schemas, certificates, applicable
physical encodings and final release. Two independent fixture generations and
fresh control processes produce identical inputs, references, native messages
and all 111 Flight frames. These checks close this finite representation/input
prerequisite; they do not establish a network or client result.

The [immutable experiment freeze](../benchmarks/evidence/native-exchange-control-2026-10-20/protocol-freeze.json)
binds complete independently specified values, schemas, fixture/recipe hashes,
exact preserving export targets, queue and process ceilings, both cohorts and
all numerical retention gates before candidate implementation or timing. The
eventual executable's source/build identity must be frozen after correctness
validation and before measurement. The original
transport protocol and its unexecuted security/lifetime/client obligations
remain binding.
