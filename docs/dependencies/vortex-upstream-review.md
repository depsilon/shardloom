# Vortex Upstream Dependency Review

## October 16, 2026 variable-binary buffer-table ownership

The [buffer-table contract](../architecture/native-varbin-metadata-resources-2026-10-16.md)
extends the existing `vortex-array` and `vortex-zstd` 0.85.0 patches and vendors
the already-transitive Apache-2.0 `vortex-fsst` and `vortex-runend` 0.85.0 crates.
The two new patches call shared managed construction and owner-preserving APIs;
they add no codec algorithm, dependency version or external execution engine.
The complete resolved lock graph changes only those two registry sources to
local patches. Native layout, serialization and feature boundaries are preserved.

The [FSST provenance](../../vendor/vortex-fsst/upstream-provenance.json) and
[RunEnd provenance](../../vendor/vortex-runend/upstream-provenance.json) retain
registry checksums, all 50 original archive-member hashes, license identities
and the published dirty-VCS markers. Supplemental upstream golden files retain
their exact Git-blob and SHA-256 identities. Existing array/Zstd original
provenance is preserved, and the Python license bundle contains all four exact
provider provenance records.

The [reviewed local acceptance report](../benchmarks/native-varbin-metadata-review-2026-10-16.md)
records all eight fresh standalone provider test/Clippy commands, 3,587 passing tests,
complete workspace/native/public validation and independent inspection of
19,538 evidence members. Safe construction/replacement validates host-readable
contents; checked native reuse preserves the same provider and ownership
boundaries. The portable audit verifies all 841 selected provider source files
and separately preserves earlier failed fixture/lint attempts and all six
rejected cost comparisons.
Hosted integration remains pending. This finite table-capacity contract leaves
raw APIs, general builder scratch, other allocation trees and process RSS open.

## October 16, 2026 frame metadata ownership

The [frame metadata contract](../architecture/native-zstd-metadata-resources-2026-10-16.md)
extends the same Apache-2.0 `vortex-zstd` 0.85.0 patch with immutable shared
frame tables. Compression and deserialization reserve the two vectors before
construction; clones, slices and nullability casts share their owner. Selected
frame copies and native transfers use the receiving allocator, replacing their
previous outer frame-metadata charges. The parser uses the already locked
Prost 0.14.3 APIs; no dependency, unsafe code or wire-format change is added.
The [codec provenance](../../vendor/vortex-zstd/upstream-provenance.json) and
Python license bundle carry the same local-change description. Original
archive, member and license identities remain unchanged.

The [complete local acceptance](../benchmarks/native-zstd-metadata-resources-2026-10-16.md)
passes six Rust gates, 67 standalone Zstd provider tests, 100 direct methods,
4,827 public cases, all 18 lifecycle cells and one complete Full43 pass.
All 13,728 portable evidence members pass independent inspection. PR #1562
completes hosted integration after all 44 checks, three installed-wheel lanes
and preview/production verification pass. The claim covers requested frame-vector capacity,
with raw caller-owned construction/export, fixed allocator/Arc overhead and
process RSS explicitly outside it. Independently supplied IPC still does not
admit Zstd. This extends the finite compression contract below without closing
the broader resource or transport queue.

## October 15, 2026 compression resource admission

The [compression contract](../architecture/native-compression-resources-2026-10-15.md)
extends the existing Apache-2.0 Vortex 0.85.0 patches without changing the
dependency graph. Allocator-backed input, offset, sample and output buffers
share the caller's grant with explicit lifetime reservations for the pinned
Zstandard 1.5.7 compressor and default sequential FASTCOVER trainer. Safe Rust
owners and the existing codec parameters remain in use. Pointer-free size
estimators, the default-level query and error predicate stay inside the existing vendored unsafe
exception. No C implementation is copied, and no workspace unsafe code,
alternate encoding or external execution engine is introduced.

The original archive and member hashes remain authoritative in the
[array provenance](../../vendor/vortex-array/upstream-provenance.json) and
[codec provenance](../../vendor/vortex-zstd/upstream-provenance.json); the
Python license bundle carries their matching local-change descriptions.
The [complete local acceptance](../benchmarks/native-compression-resources-2026-10-15.md)
passes ownership/value, lifecycle cost, workspace/native/vendor, direct/public
and single-pass Full43 gates. Hosted integration completes in PR #1555 after
all 44 checks, three installed-wheel lanes and production verification, as
recorded in the [hosted packet](../benchmarks/evidence/native-compression-hosted-2026-10-15.json).
Foreign requested
heap and named payload buffers are the finite accounting claim; structural
vectors, allocator bookkeeping, stacks and process RSS remain outside it.

## October 14, 2026 decoder and execution-context follow-through

The [combined correction](../benchmarks/native-admitted-context-2026-10-14.md)
uses the same pinned Vortex 0.85 graph and provider implementations. The
ShardLoom wrapper derives finite temporary-buffer bounds from reviewed native
append/canonicalization paths and preserves direct append and FoR fusion.
Helper callers pass their existing `ExecutionCtx`; list offsets/sizes are
prepared through that owner before upstream row convenience access, and struct
extraction uses the declared-field iterator. No dependency, vendor source,
unsafe code or external execution engine is added by this correction.
Its complete local source, behavior, cost and public/query proof is retained.
Hosted integration completes in [PR #1551](https://github.com/depsilon/shardloom/pull/1551)
with all 44 checks and three installed-wheel lanes passing, as recorded in the
[hosted receipt](../benchmarks/evidence/native-decoder-hosted-2026-10-15.json).
Unreviewed allocation trees receive no new accounting claim.

## October 13, 2026 selection resource follow-up

The [selection resource contract](../architecture/native-selection-resources-2026-10-13.md)
keeps the same pinned provider and registry/license identities below. Sparse
frame selection moves from the context-free local reduction rule into
ShardLoom's existing admitted execution-parent kernel, where frame metadata,
indices and output views have explicit resource owners. The vendored decoder
borrows contiguous frame ranges, counts validity bits without rank vectors and
pre-counts decoded view-buffer capacity. No dependency or C implementation changes.
The original sparse-filter reduction described below is historical; its frame
skipping policy is preserved at explicit execution. The
[acceptance report](../benchmarks/native-selection-resources-2026-10-13.md) records
passing final workspace/native, direct/public, Full43 and paired cost gates,
the exact dependency/source identities and both rejected earlier cost cohorts.
Hosted integration completes in [PR #1550](https://github.com/depsilon/shardloom/pull/1550)
after all 44 checks pass with the accepted runtime and dependency graph unchanged.

## October 13, 2026 sparse Zstd filter follow-up

The bounded sparse-filter change patches the existing vendored Apache-2.0
`vortex-zstd` 0.85.0 source. Its [provenance manifest](../../vendor/vortex-zstd/upstream-provenance.json)
records registry archive SHA-256
`03cae43b171152fd403b884d5e0b764e860261d38b2fd80dfd4e03678beac837`, upstream
VCS revision `afb005379dd9a1b7dc7ae2cb49f4f465d91cc2b3`, and the dirty-VCS
marker; the published registry archive remains the source of truth, and its
original member hashes are preserved. The existing FilterReduce extension
retains selected compressed frames and remaps the mask before the unchanged
native decoder. It introduces no new dependency, copied C implementation, or
external execution engine. The local evidence and acceptance status are
recorded in the [sparse-filter follow-up report](../benchmarks/native-query-revisit-2026-10-12.md#october-13-sparse-zstd-filter-follow-up).
The review correction removes frame-header reads from metadata reduction;
the native parts constructor retains matching validated frames, dtype and
normalized all-valid slots with checked row totals. Explicit constant-true
validity children collapse without payload reads when frames are retained.
The existing `vortex-array` owner
policy now also keeps owned filters as native views until explicit execution,
preserving metadata allocation ownership through reduction and child extraction.
At execution it applies deferred filter reductions while retaining the original
view through canonical execution, so transferred arrays also skip unused frames.

## October 12, 2026 encoded-transfer ownership hook

The encoded-transfer work extends the existing reviewed Apache-2.0
`vortex-array` 0.85.0 patch. Its original registry archive and license hashes
below remain authoritative; no provider upgrade is introduced.
An optional opaque owner in typed array data retains admitted metadata through
clones, child extraction, typed construction-parts round trips and structural
rewrites. It drops after the data. Attaching an owner requires a unique array
without an existing owner. Owned slices retain a native parent view until
execution; ordinary ownerless slices preserve their prior optimization path.

The [implementation contract](../architecture/native-encoded-interchange-2026-10-12.md)
separates buffer and metadata ownership, bare dtype/data clones, representation
and complete-lifecycle acceptance. The patch adds no unsafe code, encoding or
serialized field. Common-array footprint, lifetime, source isolation and
regression evidence remain required before the new transfer path is accepted.
This entry records implementation intake, not completed runtime proof.

The IPC feasibility reader also adds an optional direct reference to the existing
transitive `flatbuffers` 25.12.19 package, behind `vortex-write`. Its exact version
matches the pinned provider; the workspace lockfile adds only this dependency
edge, with no new package or version. The normalized registry manifest declares
Apache-2.0. Its 27,421-byte registry archive has SHA-256
`35f6839d7b3b98adde531effaf34f0c2badc6f4735d26fe74709d8e513a96ef3`.
The direct API is `VerifierOptions`, needed to constrain depth, table traversal
and apparent message size before provider construction. Generated Vortex schema
accessors remain supplied by `vortex::flatbuffers`; none are copied into this
repository. The default build gains no FlatBuffers dependency. This is format
validation, with no external execution engine or new serialization format.

## October 11, 2026 bounded file-statistics intake

The persisted-reuse work reviews the published `vortex-layout` 0.85.0 archive,
SHA-256 `36b9d7fcb265cbb4025a29ec3f6ecc0fbaa4739b75d900fcd353155c1f2ae1ca`.
Its 88 regular files contain 951,620 uncompressed bytes. The manifest declares
Apache-2.0; 82 of 83 Rust files include the SPDX header at their start, while
`src/layouts/zoned/mod.rs` starts with module documentation. Its package license
and the [license at the recorded VCS revision](https://raw.githubusercontent.com/vortex-data/vortex/afb005379dd9a1b7dc7ae2cb49f4f465d91cc2b3/LICENSE)
establish the same Apache-2.0 provenance. That license has SHA-256
`cfc7749b96f63bd31c3c42b5c471bf756814053e847c10f3eb003417bc523d30`.
The dirty VCS marker makes the registry archive authoritative; preserve every
original member hash and the original manifest. Record, then omit, the crate's
own lockfile because workspace resolution governs this patch.

The scoped change replaces file-statistics batch builders with bounded running
state using existing native statistics/precision/truncation semantics. It must
preserve unknown and overflowing observations, compact retained variable-length
scalars, and avoid execution when no statistics are requested. It does not
change layout or encoding identities, the file format, the provider version or
the existing feature boundary. The [persisted-reuse contract](../architecture/native-persisted-reuse-2026-10-11.md#bounded-provider-decision)
owns reservation and complete-lifecycle acceptance. This entry admits the
reviewed provider source; it does not assert that the patch or its tests pass.

Subsequent [local acceptance](../benchmarks/native-persisted-reuse-2026-10-11.md)
passes all seven provider regressions, native writer/resource/lifecycle checks,
complete public regression and fresh UAT. The report binds those results to
the exact patched source and keeps hosted integration separate from this intake.

## October 11, 2026 full-domain timestamp provider review

The timestamp follow-up reviews an Apache-2.0 patch of the published
`vortex-array` 0.85.0 archive, SHA-256
`e9cf8cf4f7f2fe72b14866ead164a6366933545289fd8f9cf9ce53c47f094393`.
The normalized manifest declares Apache-2.0, the source retains Vortex copyright
headers, and the [upstream license at its recorded VCS revision](https://raw.githubusercontent.com/vortex-data/vortex/afb005379dd9a1b7dc7ae2cb49f4f465d91cc2b3/LICENSE)
matches SHA-256 `cfc7749b96f63bd31c3c42b5c471bf756814053e847c10f3eb003417bc523d30`.
Its dirty VCS marker makes the registry archive authoritative. Retain its original
member hashes and `Cargo.toml.orig`; the crate-local lockfile is recorded but
excluded because workspace resolution owns the dependency graph.

The patch keeps the existing timestamp extension identity, Int64 storage,
metadata format and native provider APIs. It admits timezone-free microsecond
scalars over their full storage domain and formats values outside the calendar
as exact epoch microseconds. Other units, timezone policy and storage validation
retain their existing boundaries. The crate remains optional behind existing
Vortex features; no query engine, compatibility intermediate or dependency
upgrade is introduced. The [implementation contract](../architecture/native-timestamp-provider-2026-10-11.md)
requires endpoint, statistics, compression, pruning, resource and complete-workflow
proof before acceptance. This entry records reviewed intake, not completed proof.

The same timestamp acceptance also reproduces a null-semantics defect in the
published `vortex-datetime-parts` 0.85.0 comparison kernel. Timestamp validity
lives in its days child, but equality/inequality combine non-null seconds and
subseconds through Kleene Boolean operations. A false/true component can
therefore replace a null timestamp result. The complete native file filter and
whole-array comparison regressions both reproduce the defect; scalar-at-a-time
evaluation alone does not exercise that encoded kernel.

Its reviewed published archive has SHA-256
`bbc70f1030c5f6d4ebf498694b844666ddd7573bf7239949539f75eb06c57b41`,
21 regular files and 139,162 uncompressed bytes. All 16 Rust files retain
Apache-2.0/Vortex copyright headers; the manifest declares Apache-2.0 and the
recorded dirty VCS revision is the same reviewed revision above. Retain the
original hashes, license, manifest and metadata in a focused provider patch.
Reapply the days validity to handled encoded comparison results using the
existing native mask operation. Keep array identities, compression, file
representation, resource ownership and dependency versions unchanged. The
current upstream [comparison implementation](https://github.com/vortex-data/vortex/blob/develop/encodings/datetime-parts/src/compute/compare.rs)
also combines these components without that final validity mask; upgrading is
not an established remedy. This is reviewed patch intake, with final regression
and complete-workflow acceptance still pending.

## October 7, 2026 FSST admission correction

The input-completion integration retains Vortex 0.85.0 and `fsst-rs` 0.6.0.
ShardLoom validates each selected row's encoded symbol expansion, populated
symbol references and row-local escape boundaries before requesting decoded
payload from the existing provider. This closes the hosted malformed-length
assertion without adding a decoder, dependency, unsafe code or public feature.
The [correction report](../benchmarks/native-fsst-admission-2026-10-07.md)
preserves the original hosted failure, deterministic local reproduction, all
13 passing FSST tests and complete corrected-runtime acceptance. The original
input packet remains unchanged. Broader decoder scratch remains separate.

## October 7, 2026 native builder review

`NATIVE-BUILDER-RESOURCES` retains Vortex 0.85.0 and extends the existing session
provider around primitive, Boolean and decimal Chunked output builders. It keeps
upstream append strategies and native buffer ownership; the allocator argument
alone is insufficient because this pinned builder factory ignores it. The
existing Apache-2.0 `vortex-zstd` patch also routes primitive append through its
already-admitted native decoder, then native primitive append. The
[provenance manifest](../../vendor/vortex-zstd/upstream-provenance.json) records
that additional method-level change. No dependency, feature, codec implementation
or unsafe code is added.

The [contract](../architecture/native-builder-resources-2026-10-07.md) and
[acceptance](../benchmarks/native-builder-resources-2026-10-07.md) record the
capacity/lifetime boundary, 13 pinned upstream source checks, 14 new ownership
tests, the complete cost screen and source/public/Full43 regression proof.
Local packet inspection and all 39 hosted checks passed; PR #1529 merged with
the accepted runtime unchanged. Child decoder
scratch and structural metadata remain outside the new byte-buffer claim.

## October 7, 2026 pinned decoder workspace review

The merged `NATIVE-CODEC-WORKSPACES` unit retains Vortex 0.85.0 and the same
native encoding patch. It adds an exact direct `zstd-sys` 2.0.16 dependency only
inside that excluded provider crate, using its experimental static-API bindings.
The lockfile already contains `zstd-sys` 2.0.16+zstd.1.5.7 through `zstd` 0.13.3
and `zstd-safe` 7.2.4. No provider upgrade or external query engine is involved.
The binding crate declares MIT/Apache-2.0; Zstandard 1.5.7 is used under its
BSD-3-Clause license option. Existing license/provenance records remain intact.

The [workspace contract](../architecture/native-zstd-workspaces-2026-10-07.md)
records exact source surfaces, feature isolation, linked-version rejection,
private unsafe lifetime/alignment requirements and the legacy-format boundary.
RFC 0044 permits this scoped binding inside `vendor/vortex-zstd`; workspace
crates retain `unsafe_code = "forbid"`. The
[acceptance report](../benchmarks/native-codec-workspaces-2026-10-07.md) records
24 ownership tests, 17 source gates and complete public/Full43 regression proof.
PR #1528 merged after all 39 hosted checks passed with the accepted runtime
source unchanged. Package publication remains separate. Dependency
availability alone does not expand support.

## October 6, 2026 retained allocator patch

The accepted local provider resource unit retains Vortex 0.85.0 and an
Apache-2.0 patch of the published `vortex-zstd` crate. Reviewed payload, view
and nullable scatter allocations use the existing fallible session allocator;
concrete encoding, VTable and serialization identities remain unchanged.
The [resource contract](../architecture/native-provider-resources-2026-10-06.md)
defines the finite scope and remaining C decoder/dictionary scratch exclusions.
The [provenance manifest](../../vendor/vortex-zstd/upstream-provenance.json)
records the authoritative registry-archive hash, original file hashes and
license. Its dirty upstream VCS marker means a Git revision alone is not the
source archive identity. Local and combined regression acceptance passed;
hosted integration and publication remain separate. Earlier “no vendored code”
statements below describe their historical intake scope.

## October 3, 2026 release review

Released, non-yanked Vortex 0.87.0 is the latest provider inspected. The workspace
retains 0.85.0 because exact published source still sends buffer allocation
refusal to the allocation-error handler instead of the typed, recoverable error
required by ShardLoom's memory budget. The
[Rust/Vortex review](rust-vortex-refresh-2026-10-03.md) records live release
metadata, source hashes, the global Rust 1.99.0 update, retained-provider tests
and the feature adoption map. No 0.87 build or speedup is claimed.

## September 27, 2026 provider admission

Vortex 0.86.0/0.86.1 are released and have been reviewed under C1. The workspace
retains 0.85.0: the new buffer allocator no longer returns typed recoverable
budget denial through the current provider boundary. See the
[exact API/resource admission decision](../architecture/vortex-086-upgrade-admission-2026-09-27.md).
No upgraded build or performance result is claimed. Earlier references to
0.86.0 as a draft describe the historical intake, not current release status.

## Purpose

This document is a historical dependency-review ledger for ShardLoom's upstream
Vortex dependency. It does not define the current executable Vortex surface.

Current runtime support is summarized in
`docs/architecture/phased-execution-plan.md` and
`docs/architecture/vortex-public-api-inventory.md`. Do not infer current
executable support from older PR-specific sections that say "this PR" or
"actual IO implemented."

## Current support snapshot

- Upstream Vortex remains optional and isolated in `shardloom-vortex`.
- The tracked workspace dependency is `vortex = 0.85`; `shardloom-vortex` inherits it while keeping
  the dependency optional and feature-gated.
- Vortex `0.85.0` is the current optional dependency family after the provider-capability
  update recorded in `docs/architecture/vortex-public-api-inventory.md` and
  `docs/dependencies/vortex-dependency-footprint.md`.
- Vortex `0.75.0` remains historical intake material in this review and
  `docs/architecture/vortex-public-api-inventory.md`.
- Vortex `0.74.0` remains historical intake material in this review and
  `docs/architecture/vortex-public-api-inventory.md`.
- Vortex `0.73.0` remains historical intake material in this review and
  `docs/architecture/vortex-public-api-inventory.md`.
- Vortex `0.72.0` remains historical intake material in this review and
  `docs/architecture/vortex-public-api-inventory.md`.
- Vortex `0.71.0` remains historical intake material in
  `docs/dependencies/vortex-0.71-upstream-intake.md`.
- Approved historical metadata/footer fixture IO and approved local primitive
  scan paths are recorded in `docs/architecture/phased-execution-plan.md`.
- Vortex-native array, compute, scan, source, and sink APIs may be future native
  providers only when feature-gated, version-recorded, policy-admitted, and
  certificate-backed.
- Future upstream release intake is governed by
  `docs/dependencies/vortex-upstream-release-intake-runbook.md`.
- Vortex DataFusion, DuckDB, Spark, Trino, and similar integrations remain
  baseline/reference/oracle surfaces only; they must not execute unsupported
  ShardLoom residual work.
- Fallback execution remains disabled.

## Vortex 0.76 Dependabot Closeout And 0.85 Intake Direction

- Dependabot PR <https://github.com/depsilon/shardloom/pull/1379> was closed without merging.
- Crates.io reports `vortex 0.76.0` and `vortex-zstd 0.76.0` as yanked. They must not be used as
  ShardLoom's next pinned provider line.
- Crates.io reports current released upstream `vortex 0.85.0` and `vortex-zstd 0.85.0` with
  license `Apache-2.0`, Rust version `1.95`, documentation at
  <https://docs.rs/vortex/0.85.0>, and repository <https://github.com/spiraldb/vortex>.
- Upstream Vortex changelog docs on 2026-08-29 also list `0.86.0` as draft. Treat draft changes as
  migration surveillance only; do not pin or publish against an unreleased Vortex crate line.
- The retained Vortex update is a provider/capability intake, not a blind lockfile bump. It records
  `0.76.0` as a yanked hazard, reviews each intervening non-yanked release from 0.77 through the
  current released latest 0.85, migrates only the ShardLoom-native provider APIs needed for current
  feature-gated builds, and rejects Vortex query-engine integrations as runtime helpers.
- The direct Arrow/Parquet bridge stays on Arrow/Parquet `58.3.0` in this dependency hygiene pass
  because current Vortex `0.85.0` still declares `arrow-array ^58.3` and `parquet ^58.3`; upgrading
  only ShardLoom's direct Arrow bridge to `59.x` would create duplicate Arrow provider types.
- Vortex query-engine integrations remain prohibited as ShardLoom runtime helpers.

## Vortex 0.85 provider-capability update

- Dependabot PR <https://github.com/depsilon/shardloom/pull/1379> and the follow-on Vortex bump
  signal were reviewed as one upstream provider intake instead of merged directly.
- `cargo info vortex@0.85.0` reports license `Apache-2.0`, Rust version `1.95`, documentation
  at <https://docs.rs/vortex/0.85.0>, repository <https://github.com/spiraldb/vortex>, and crates.io
  version <https://crates.io/crates/vortex/0.85.0>.
- `cargo info vortex-zstd@0.85.0` reports license `Apache-2.0`, Rust version `1.95`,
  documentation at <https://docs.rs/vortex-zstd/0.85.0>, repository
  <https://github.com/spiraldb/vortex>, and crates.io version
  <https://crates.io/crates/vortex-zstd/0.85.0>.
- Root `Cargo.toml` now records workspace dependencies `vortex = 0.85` and
  `vortex-zstd = 0.85`; `shardloom-vortex` inherits both as optional feature-gated dependencies.
- `Cargo.lock` records the upstream Vortex crate family at `0.85.0`.
- Active provider-version evidence remains centralized through
  `shardloom_vortex::UPSTREAM_VORTEX_PROVIDER_VERSION`.
- Active provider-disposition reports now use `vortex_upstream_*` field/schema labels, so future
  provider-line updates do not leave current report keys hard-coded to an older release number.

Release-family review retained:

- `0.76.0` / `vortex-zstd 0.76.0`: yanked; recorded as a dependency hazard and skipped as a pinned
  provider line.
- `0.77.0`-`0.79.0`: reviewed as intermediate scan/expression/encoding compatibility releases; no
  separate ShardLoom runtime provider adoption was retained beyond the current 0.85 compile
  migration.
- `0.80.0`: `VortexSource` improvements, layout adapter error posture, piecewise sequence/index
  work, struct-layout reader allocation improvements, footer parse validation, and Vortex edition
  docs were mapped as provider-alignment pressure.
- `0.81.0`: file-level user metadata, serialization-context registry filtering, partition-expression
  locking, and `UnionArray` were mapped; ShardLoom now treats new union scalar/dtype shapes as
  explicit unsupported metadata/encoded-stat boundaries unless a later runtime item admits them.
- `0.82.0`-`0.84.0`: grouped aggregate kernels, scan statistics, layout-reader/file-cache work,
  JSON/Arrow import/export changes, interleave encoding, row encoder, byte-length/string
  transforms, zstd/binary scheme changes, mask/zip/layout performance, and dictionary/FSST LIKE
  behavior were mapped into the current upstream-provider disposition reports.
- `0.85.0`: retained code migrations cover `BoundExpression` scan pushdown, ArrowSession-only
  RecordBatch import/export, Vortex session/edition encoding selection, `DynLayout`, strict sorted
  row-index selection, run-end slot access, `Map`/`Union` unsupported-boundary handling, updated
  VarBin/FSST helper APIs, and writer strategy API changes.
- `0.86.0` draft: surveyed only as future pressure around `VortexWrite`/`OutputSink`, `RowFn`,
  fixed-width filtering, grouped-state behavior, sequence casts, and DataFusion/DuckDB integration
  changes. It is not a retained dependency line.

Claim boundary:

- The update proves feature-gated Vortex 0.85 build compatibility, provider-version freshness, and
  refreshed source-linked provider-disposition evidence.
- It does not admit Vortex query-engine integrations, hidden fallback execution, object-store/table
  runtime, GPU/device support, broad JSON/geospatial runtime support, package publication,
  performance superiority, or broad SQL/DataFrame support by dependency availability alone.

## Vortex 0.75 compatibility update

Historical note; superseded by the Vortex 0.85 provider-capability update above for current
dependency status.

- Dependabot PR: <https://github.com/depsilon/shardloom/pull/1223>.
- `cargo info vortex@0.75.0` reports license `Apache-2.0`, Rust version `1.91.0`, documentation
  at <https://docs.rs/vortex/0.75.0>, repository <https://github.com/spiraldb/vortex>, and crates.io
  version <https://crates.io/crates/vortex/0.75.0>.
- Root `Cargo.toml` now records workspace dependency `vortex = 0.75`; `shardloom-vortex` inherits
  it as optional through the `upstream-vortex` feature.
- `Cargo.lock` records the upstream Vortex crate family at `0.75.0`.
- ShardLoom provider-version evidence is centralized through
  `shardloom_vortex::UPSTREAM_VORTEX_PROVIDER_VERSION` so certificates, capability rows,
  scan/source admission rows, capillary/preparation spine evidence, benchmark route evidence, and
  compatibility reports do not claim a stale provider line.
- Upstream runtime-relevant release-note items mapped for later ShardLoom provider-gated work:
  grouped `sum`/`count` aggregate kernels, explicit validity/mask execution-context APIs, layout
  reader context/caching, JSON and WKB/geospatial extension import/export, Interleave array
  encoding, `byte_length()`, binary zstd schemes, row-oriented byte encoding, branchless zip
  kernels, mask `AllTrue`/`AllFalse` fast paths, dictionary validation/slice optimization, FSST
  state sharing, and layout child caches.
- GPU/device, JNI, cuDF, and DataFusion 54 items remain blocked or baseline/oracle-only until a
  separate ShardLoom provider gate adds device residency, external-boundary, certificate, and
  no-fallback evidence.
- The update is a dependency/build compatibility admission only. It does not by itself admit new
  Vortex runtime behavior, object-store/table support, SQL/DataFrame production support,
  performance claims, package publication, or fallback execution.
- Vortex query-engine integrations remain prohibited as ShardLoom runtime helpers.

## Vortex 0.74 compatibility update

Historical note; superseded by the Vortex 0.85 provider-capability update above for current
dependency status.

- Dependabot PR: <https://github.com/depsilon/shardloom/pull/1150>.
- `cargo info vortex@0.74.0` reports license `Apache-2.0`, Rust version `1.91.0`, documentation
  at <https://docs.rs/vortex/0.74.0>, repository <https://github.com/spiraldb/vortex>, and crates.io
  version <https://crates.io/crates/vortex/0.74.0>.
- `shardloom-vortex` now requests optional `vortex = 0.74`.
- `Cargo.lock` records the upstream Vortex crate family at `0.74.0`.
- Existing ShardLoom provider-version evidence was refreshed to `0.74` so certificates, capability
  rows, scan/source admission rows, capillary/preparation spine evidence, and compatibility reports
  do not claim the stale `0.73` provider line.
- The upstream release note calls out removal of an FFI `vx_file` scan surface; ShardLoom does not
  use that FFI surface in its approved runtime boundary.
- The update is a dependency/build compatibility admission only. It does not by itself admit new
  Vortex runtime behavior, object-store/table support, SQL/DataFrame production support,
  performance claims, package publication, or fallback execution.
- Vortex query-engine integrations remain prohibited as ShardLoom runtime helpers.

## Vortex 0.73 compatibility update

Historical note; superseded by the Vortex 0.85 provider-capability update above for current
dependency status.

- Dependabot PR: <https://github.com/depsilon/shardloom/pull/979>.
- `cargo info vortex@0.73.0` reports license `Apache-2.0`, Rust version `1.91.0`, documentation
  at <https://docs.rs/vortex/0.73.0>, repository <https://github.com/spiraldb/vortex>, and crates.io
  version <https://crates.io/crates/vortex/0.73.0>.
- `shardloom-vortex` now requests optional `vortex = 0.73`.
- `Cargo.lock` records the upstream Vortex crate family at `0.73.0`.
- Existing ShardLoom provider-version evidence was refreshed to `0.73` so certificates, capability
  rows, scan/source admission rows, capillary/preparation spine evidence, and compatibility reports
  do not claim the stale `0.72` provider line.
- The update is a dependency/build compatibility admission only. It does not by itself admit new
  Vortex runtime behavior, object-store/table support, SQL/DataFrame production support,
  performance claims, package publication, or fallback execution.
- Vortex query-engine integrations remain prohibited as ShardLoom runtime helpers.

## Vortex 0.72 compatibility update

Historical note; superseded by the Vortex 0.85 provider-capability update above for current
dependency status.

- `shardloom-vortex` now requests optional `vortex = 0.72`.
- `Cargo.lock` records Vortex `0.72.0` crate family versions.
- The update is a dependency/build compatibility admission only. It does not by itself admit new
  Vortex runtime behavior, object-store/table support, SQL/DataFrame production support,
  performance claims, package publication, or fallback execution.
- TurboQuant remains blocked capability metadata only (`vortex_turboquant_vector_encoding`) until
  ShardLoom has an admitted vector dtype contract, feature-gated compile/runtime proof, correctness
  tests, no-fallback policy evidence, and workload-scoped benchmark evidence.

## Vortex 0.71 bump update

Historical note; superseded by the Vortex 0.85 provider-capability update above for current
dependency status.

- `shardloom-vortex` now requests optional `vortex = 0.71`.
- `Cargo.lock` records Vortex `0.71.0` crate family versions.
- The bump required one ShardLoom source compatibility update: local primitive dtype mapping now
  rejects upstream `DType::Union` deterministically instead of relying on a non-exhaustive match.
- Feature-gated compile proof covered `upstream-vortex`, `vortex-file-io`, `vortex-write`,
  `vortex-local-primitives`, and `vortex-traditional-analytics-benchmark`.
- No new Vortex runtime support claim, fallback engine, object-store/table claim, SQL/DataFrame
  claim, performance claim, or package publication claim is introduced by the bump.

## Current status

- Upstream Vortex dependency has been added to `shardloom-vortex`.
- Historical scope in the original dependency PR was compile/readiness only.
- Current executable Vortex support is tracked in the phase plan rather than in
  this historical review section.
- Fallback execution remains disabled.

## Historical dependency review

- Crate name: `vortex`
- Version requested: `0.70`
- Repository: upstream Vortex repository
- License: Apache-2.0
- Purpose: native Vortex format/toolkit integration inside `shardloom-vortex`
- Original PR scope: dependency compile/readiness only
- Public APIs used in the original PR: none (compile marker only)
- Internal APIs used: none
- Actual IO implemented by the original PR: no
- Fallback engines introduced: no
- Copied upstream code: no
- Vendored upstream code: no

## License/provenance checklist

- Upstream license identified as Apache-2.0.
- License is compatible with Apache-2.0 project policy.
- No GPL/AGPL/SSPL/BUSL/proprietary code introduced.
- No copied upstream implementation code.
- No vendored upstream code.
- Dependency usage is isolated to `shardloom-vortex`.
- No fallback execution dependency was directly added.

## Historical dependency addition status

- Upstream Vortex dependency has been added to `shardloom-vortex`.
- The original dependency PR did not implement actual Vortex IO.
- The original dependency PR did not add fallback execution.
- The original dependency PR did not add DataFusion/Spark/DuckDB/Polars/Velox.

## Follow-up required

- Identify minimal metadata inspection API.
- Identify DType mapping API.
- Identify encoding/layout mapping API.
- Add adapter tests.
- Add unsupported diagnostics.
- Avoid decode-to-Arrow default path.


## API discovery update

- Added `docs/architecture/vortex-public-api-inventory.md` to track inspected upstream public API
  areas and adapter boundaries.
- Current adapter work remains mapping/readiness only.
- No actual Vortex IO is implemented in this phase.
- No fallback execution was introduced.


## Typed DType mapping update

- Typed DType mapping is deferred in this PR because a compile-safe public typed API could not be
  confirmed in this environment.
- Public upstream typed API used: none.
- Name-based DType mapping remains available as a temporary adapter utility.
- No actual Vortex IO was implemented.
- No fallback execution was introduced.

## Typed encoding/layout mapping update

- Typed encoding mapping: deferred pending confirmed stable public upstream non-IO API surface.
- Typed layout mapping: deferred pending confirmed stable public upstream non-IO API surface.
- Public upstream API used for typed mapping: none in this PR (deferred).
- Name-based mapping remains available for planning/reporting utility.
- No actual Vortex IO implemented.
- No fallback execution introduced.

## Typed statistics mapping update

- Typed statistics mapping: deferred in this PR (`deferred_api_unclear`) because a compile-safe,
  unambiguous public upstream API surface for typed statistics mapping was not confirmed in this
  environment.
- Public upstream statistics API used: none.
- `ShardLoom` `SegmentStats` placeholder mapping remains available for planning/reporting utility.
- No actual Vortex IO implemented.
- No fallback execution introduced.

## Metadata-only IO update

- Metadata-only local `Vortex` inspection: deferred to report-only mode in this PR.
- Public upstream API used for implemented runtime metadata inspection: none (deferred).
- Scan execution implemented: no.
- Data materialization implemented: no.
- Write implemented: no.
- Object-store IO implemented: no.
- Fallback execution introduced: no.

## Dependency footprint gating update

- Upstream `vortex` dependency is now feature-gated in `shardloom-vortex` (`upstream-vortex`).
- Default workspace builds do not enable the broad upstream Vortex graph by default.
- `vortex-file-io`, `vortex-object-store`, and `vortex-write` are declared as staged feature gates
  only.
- File IO and object-store IO remain disabled by default and unimplemented.
- No DataFusion/Spark/DuckDB/Polars/Velox/vortex-datafusion direct dependencies were introduced.
- No fallback execution behavior was introduced.

## Universal-format local benchmark bridge dependencies

- `shardloom-vortex` gates local structured-file bridge support behind
  `universal-format-io`, which is pulled by the existing
  `vortex-traditional-analytics-benchmark` feature and by the scoped
  `shardloom-cli --features universal-format-io` local Parquet source/output smoke.
- Rust crates added under that gate:
  - `parquet 58.3.0` for local Parquet record-batch reads/writes.
  - `arrow-ipc 58.3.0` for Arrow IPC reads/writes.
  - `arrow-avro 58.3.0` for Avro reads/writes.
  - `orc-rust 0.8.0` for ORC reads/writes.
  - `arrow-array 58.3.0` and `arrow-schema 58.3.0` for Arrow boundary arrays and schemas.
  - `arrow-json 58.2.0` is reserved under the same gate for JSON/NDJSON boundary work; the current
    deterministic JSONL fixture parser remains local and narrow.
- Dependabot Arrow/Parquet 59 PRs #1224, #1225, #1227, #1228, and #1229 were reviewed and
  deferred because Vortex 0.85 and `orc-rust 0.8.0` still expose Arrow 58-compatible provider
  boundaries in the feature-complete structured-format lane.
- License/provenance:
  - Apache Arrow Rust crates are Apache-2.0.
  - `orc-rust` is Apache-2.0.
  - Benchmark fixture generation uses Python `fastavro 1.12.2` in
    `benchmarks/traditional_analytics/requirements.txt`; `pip show fastavro` reports MIT license.
- Scope:
  - Default workspace builds remain lightweight and do not enable these dependencies.
  - These dependencies do not introduce Spark, DataFusion, DuckDB, Polars, Velox, Trino, Dask, Ray,
    Calcite, or `vortex-datafusion`.
  - They are file-format boundary readers/writers for local benchmark smoke, troubleshooting, and
    the feature-gated flat scalar local Parquet SQL source/output smoke, not execution engines or
    fallback paths.
  - Compatibility-format input is imported into native local Vortex output before the temporary
    benchmark operator runs.
  - Production adapter certification, object-store IO, catalog/table metadata IO, distributed
    execution, SQL/DataFrame/UDF runtime, and performance/superiority claims remain separate future
    work.
