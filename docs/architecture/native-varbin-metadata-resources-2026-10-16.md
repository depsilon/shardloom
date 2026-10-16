<!-- SPDX-License-Identifier: Apache-2.0 -->

# Native variable-binary buffer table ownership — October 16, 2026

Status: the initial implementation completed local acceptance. Review found
missing content checks in safe handle construction and arbitrary view replacement.
The corrective implementation passes focused regressions but repeats a cost-gate
breach in both complete comparison orders. Removing redundant validation within
proven native transformations and refreshing acceptance remain in progress.
Hosted integration remains pending.
The [acceptance report](../benchmarks/native-varbin-metadata-resources-2026-10-16.md)
records the exact source, checks, cost decision and one-pass Full43 observation.
`NATIVE-VARBIN-METADATA-RESOURCES` extends PERF-03/06/08/09 under
[RFC 0044](../rfcs/0044-resident-runtime-resource-ownership.md). It follows the
completed Zstd frame-metadata unit and leaves the broader
[local completion contract](native-local-completion-scope-2026-10-07.md) open.

## Provider decision and reuse

Classification: `use_vortex_native_provider`. Extend the existing Vortex 0.85.0
`VarBinViewData`, native view construction, `VarBinViewBuilder`, FSST/RunEnd/Zstd
providers and session `HostAllocator`. Keep UTF-8 and binary DTypes, the 16-byte
view representation, validity, buffer order, native serialization and provider
dispatch. The `vortex-local-primitives`/native I/O feature boundaries and existing
execution and Native I/O certificates remain authoritative. There is no new
execution route, external engine, Arrow materialization or codec algorithm.

Source review finds two different ownership needs. Transformations such as
slice, take, filter, masking, nullability casts and RunEnd expansion reuse a
buffer-handle table. Decode, deserialization, buffer replacement and concatenation
create a new table, sometimes through several temporary `Vec`/`Arc` conversions.
The existing selected-Zstd wrapper and encoded-transfer plan already charge some
of those collections. Replace superseded charges; do not count the same storage
twice or transfer a source session's reservation into the receiver.

| Existing component and callers | Shared extension and proof boundary |
| --- | --- |
| `VarBinViewData` and context-aware constructors/deserializer | Retain one shared table reservation; admit newly created handle tables before allocation. Preserve host validation and device-handle behavior without claiming device execution. |
| Core canonical validity, cast, mask, slice, filter, take and RunEnd string expansion | Rebuild views/validity through the original array data, preserving the shared table owner. No new table or table charge for unchanged handles. |
| `build_views` / offsets conversion, FSST canonicalization and VarBin canonicalization | Reuse existing view-building loops with admitted temporary buffer collection capacity and a managed final table. The common single-buffer case needs no per-row counting pass. |
| Zstd full and selected text reconstruction | Admit the existing bounded collections and final table through the same allocator. Preserve selected-only view creation and decoded frame boundaries. |
| Admitted non-compacting native `Chunked` string builder | Add fallible table growth/finalization to the existing builder; reuse its view rebasing and validity logic. Avoid an intermediate per-leaf table when all buffers are kept. |
| Native encoded transfer and selected-Zstd retention | Use fresh receiving/provider table ownership, retain independent schema/slot/scratch charges, and remove replaced table wrappers/charges. |

## Ownership and admission

Keep the existing immutable `Arc<[BufferHandle]>` payload table. Its managed
array data also retains a shared allocator/reservation owner. A data clone
shares both; changing only views, dtype nullability or validity preserves that
owner. Drop the table reference before its reservation. Table identity and
reservation identity must survive the last managed array/data clone that retains
the table, including an explicitly reconstructed empty view and an array retained
after session closure. Ordinary empty-slice reduction discards the source table
and needs no table credit; it must not retain unused storage solely for identity.

New tables reserve their checked requested element capacity before allocation.
Use fallible exact-capacity vectors and verify their reported capacity. During
`Vec` to `Arc` conversion, retain both the temporary vector and final slice
charges until the vector allocation is freed. The final slice retains
`count * sizeof(BufferHandle)` bytes. Byte-buffer staging collections retain
their own `capacity * sizeof(ByteBuffer)` charge. Empty collections require no
element-storage charge. Fixed object/Arc headers, allocator bookkeeping and
unreported allocator slack remain outside this requested-capacity claim.
This does not guarantee recovery from a system allocation failure or bound RSS.

The managed concatenation builder admits growth before changing its collection:
old capacity and the entire requested new capacity overlap during reallocation.
After successful exact-capacity growth, release the old charge. Check arithmetic,
buffer-index limits and the pre-reserved view/validity extent before appending.
Its finalization admits the final handle table and temporary conversion storage
before consuming builder state. A denied append/finalization must leave either
a valid reusable builder or an explicitly terminal error state with complete
cleanup; it must never publish partial output. Preserve the existing separate
view/validity payload and empty-finalization-buffer reservations.

Typed allocation denial must reach ShardLoom unchanged. A custom allocator that
does not implement external reservations refuses nonempty managed tables. The
explicit default allocator remains unbounded. All intermediate owners release
on failure, cancellation or normal completion. No denial retries through an
unmanaged constructor or another executor.

Ordinary buffer replacement uses the table's allocator when one exists. Native
deserialization and cross-context transfer use the receiving session allocator.
The encoded transfer report includes the provider-owned retained table bytes
without retaining its superseded outer table charge. Temporary transfer lists,
schema/slots and fixed bookkeeping retain their existing independent ownership.

## Compatibility and finite scope

Preserve raw constructor, `data_buffers()` and `into_data_parts()` signatures.
Cloning/exporting their raw `Arc` is an explicit caller-owned boundary; it is not
a managed query-memory transfer. Native table-sharing transformations must use
the owner-preserving path instead of raw export/reconstruction. Individual
payload buffers keep their existing byte owners, independently of table credit.
Do not attach table reservations to every payload clone: that would keep table
charges alive after the table is gone and accumulate owner wrappers.

Legacy infallible raw builders, dictionary-training/deduplication collections,
selective compaction and zip scratch retain their separate allocation boundary.
This unit does not certify all allocations of every operation that can produce a
VarBinView array. Context-aware final table construction can be admitted without
claiming earlier caller-owned storage. Existing supported behavior remains;
remaining general builder/reader/provider allocations stay under PERF-03/06/08/09.
Payload decoding, validity scratch and other provider state retain their existing
finite contracts. No general spill, resumable recovery, transport expansion,
whole-process resource claim or version bump follows from this unit.

The already-transitive Apache-2.0 `vortex-fsst` and `vortex-runend` 0.85.0
providers need small pinned patches to call the shared managed construction and
owner-preserving APIs. Before vendoring, verify their registry archives against
Cargo.lock and every original source member; preserve original hashes, VCS dirty
status, license attribution and the Python license bundle. Keep versions and the
resolved dependency graph unchanged apart from the two local patch sources.
Do not add FFI, raw-pointer operations, custom allocation or workspace unsafe
code. Existing validated view construction can reuse its existing unsafe
constructors with the same documented invariants; prefer checked parts APIs
for owner-preserving reconstruction.

Safe constructors and arbitrary view replacement validate all host-readable
values, including null slots. Newly decoded bytes and arbitrary caller-supplied
views cannot bypass UTF-8, prefix, range or buffer-index validation. Proven
native selection, unchanged validity reconstruction and non-compacting append
may reuse existing validated values through explicit unsafe provider helpers
with per-call safety arguments. These helpers retain shape, dtype, validity and
ownership checks and reuse the existing unchecked constructor; they add no
raw-pointer operations or workspace unsafe code. Safe replacement can reuse
validation for a byte-identical host view buffer against the same payload table,
including an owner-only buffer wrapper, while still checking shape and dtype.
Opaque device contents are not read, and later host replacement validates them.

## Acceptance protocol

Before runtime edits, commit the unchanged-production baseline plus the same
opt-in lifecycle harness used for both versions. Freeze source hashes, the
optimized executable, compiler/features and test inventory. This is a resource
correctness change with an overhead gate, not a speedup claim.

The new screen has 32 predeclared cells: nullable UTF-8/binary; 32/65,536 input
rows; canonical, FSST, Zstd without dictionary and RunEnd encodings; and 2/16
concatenated view chunks. Each cell records one warmup and three samples of 25
complete operations. Deterministic fixtures, input serialization, selection
controls and complete reference values are prepared outside the operation
clock. Inside it: create the producer session, reopen/decode native input,
filter/take/slice views, concatenate through the admitted builder, copy into a
fresh receiving session, serialize/hash native output, close the producer,
verify every output value/null and release all output/session owners. Both
sessions have a 64-MiB grant and one admitted CPU. Frozen source/input/output
identities and every sample remain available, including failures.

Compare each cell's median sample total. An increase exceeding both 10% and
100 microseconds per operation triggers a complete reverse-order comparison.
A repeated breach in the same cell blocks retention for diagnosis. Native
process CPU growth exceeding both 10% and one second, or peak RSS growth
exceeding both 25% and 64 MiB, also triggers the complete reverse order.
Compare serialized bytes/hashes and complete values exactly. Record reservation
peaks separately because the new provider accounts for previously uncharged
storage. No cache flush, pacing, battery/thermal admission or selective sample
replacement is permitted. Full43 remains one execution of each query.

Required verification:

- Typed denial before collection allocation, exact/one-byte-short grants,
  competing owners, zero buffers, checked overflow, malformed views/metadata,
  failed builder growth/finalization and complete refund after errors.
- Table identity and credit lifetime across clones, nested slices and explicit
  empty views, with complete release when empty-slice reduction discards the table,
  casts, masking, filter/take, canonical validity execution and RunEnd expansion.
  Verify that a raw payload clone does not retain unrelated table credit.
- UTF-8/binary, inline/outlined values, empty/all-null/mixed validity,
  multi-buffer rollover with small test extents, many-leaf concatenation,
  selected/dense Zstd, FSST and exact native serialization.
- Cross-session copying with independently owned receiving metadata, correct
  retained-byte reporting, source closure, receiver denial and final release.
- Focused vendor/provider/transfer tests followed by default and native fmt,
  clippy and workspace tests; complete direct/public/format/streaming checks;
  the frozen lifecycle screen; one complete Full43 acceptance pass.
- Source/executable binding, complete portable evidence inspection, hosted
  checks and installed wheels, then docs/site and deployment verification.

All CG-1 through CG-23 retain their existing owners. This work contributes
finite resource and workflow evidence to CG-5/19/20/21 and does not close broader
competitive claims. `fallback_attempted=false` and `external_engine_invoked=false`
remain required throughout.

## Local acceptance

The following records the initial pre-review implementation. It does not accept
the constructor correction or its subsequent optimization; refreshed evidence is
required for the final source revision.

All six Rust gates pass: 3,221 workspace tests and 4,315 native tests, including
all 21 added regressions. Four standalone provider suites pass 3,587 tests;
their eight test/Clippy commands are bound to the exact accepted sources.
All 164 release-script tests, 100 direct binding methods and 4,827 public cases
across 36 families pass. The 32-cell lifecycle screen verifies 4,864 complete
operations and identical native serialized outputs without a declared breach.
Full43 passes all 43 queries once, with complete values and schemas checked.
Independent inspection verifies all 18,023 portable members. These are local
source-package observations; installed wheels and hosted integration remain
separate gates. The predeclared acceptance protocol above is unchanged.
