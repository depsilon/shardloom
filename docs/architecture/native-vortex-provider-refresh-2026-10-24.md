<!-- SPDX-License-Identifier: Apache-2.0 -->

# Controlled Vortex provider refresh

Status: accepted October 24 maintainer addition, immediately after
[user-surface execution parity](native-user-surface-execution-parity-2026-10-24.md)
and before [warm source reuse](native-warm-source-reuse-2026-10-24.md), public
naming and ingestion. The reviewed target is the released Vortex **0.90.0** tag,
published October 23, 2026. The candidate now resolves Vortex 0.90.0, one
Arrow/Parquet 59.3.1 graph and ORC 0.9.0. Standalone provider tests, default/native
workspace tests and strict lint pass after the ownership repairs. All-feature,
minimal and Rust 1.96.0 checks pass. Installed Python/CLI proof passes all 140 native
integration tests with no skips. Fresh ingestion and isolated Full43 acceptance
now pass on the final matched build; the local runtime/UAT acceptance is complete.
Portable replay passes, including eight corruption controls. The provider
category PR, CI, review and merge remain pending. No performance improvement is established. RFC 0044 and the existing allocation, native I/O,
expression, fidelity and no-fallback contracts remain authoritative. CG-1
through CG-23 remain independent; this adds no implementation phase ID.

## Decision and scope

Evaluate and implement a controlled migration before building further custom
source, ingestion or export infrastructure around the older provider. Use three
coherent steps: API integration and ownership; activation in existing callers;
then type and optional physical-representation admission. The first two form
the immediate provider refresh. The third feeds the already queued rich-data,
I/O and AI-data packages, with optional encoding/index experiments kept
conditional. It does not move every later capability ahead of warm reuse or
create duplicate projects.

Keep upstream APIs inside approved `shardloom-vortex` providers, with explicit
features, version, allocation policy, representation transitions and certificates.
Use the ordinary ShardLoom interfaces. Vortex query-engine integrations and
external residual execution are prohibited. A bare version bump, extra public
engine mode or blanket new-encoding enablement does not satisfy this package.

## Step 1: integrate the provider and preserve ownership

The [workspace manifest](../../Cargo.toml) now declares eight Vortex path patches.
The former DateTimeParts patch is retired: all sixteen Rust files match the
published 0.90 archive, and its 87 standalone tests plus four native timestamp
regressions pass. Upstream Arrow/Python 59.3.1 replaces the separate path patch;
the unused vendored source is retired after malformed-capsule, ownership and installed-package
regressions pass. The [upstream review](../dependencies/vortex-upstream-review.md)
and each retained provider's provenance describe cumulative obligations:

| Existing patch | Invariants to compare with the replacement provider |
| --- | --- |
| [vortex-array](../../vendor/vortex-array/README.shardloom.md) | Timestamp storage domain, checked statistics arithmetic, array/metadata-owner lifetime, deferred owned views, external reservations, variable-binary table admission and checked text reconstruction |
| [vortex-buffer](../../vendor/vortex-buffer/README.shardloom.md), newly isolated | Explicit empty-buffer owners survive clones, typed conversion and Arrow/Bytes import/export; ordinary empty slices may detach their backing |
| [vortex-file](../../vendor/vortex-file/README.shardloom.md) | Footer/schema/layout buffers, realignment, segment-map transfer, metadata tables and locator ownership remain admitted and fallible; V1 opens leave unused V2 tables unallocated |
| [vortex-fsst](../../vendor/vortex-fsst/README.shardloom.md) | Canonical string/binary construction admits temporary/final view tables and preserves owners |
| [vortex-io](../../vendor/vortex-io/README.shardloom.md), newly isolated | Local and object-store destination buffers reserve before I/O, reject overflow and short/oversized responses, expose initialized bytes and retain credit through clones/slices |
| [vortex-layout](../../vendor/vortex-layout/README.shardloom.md) | Bounded conservative file statistics, nullable structural reads, cache/child metadata and chunk offsets retain credit; traversal errors propagate |
| [vortex-runend](../../vendor/vortex-runend/README.shardloom.md) | Expanded string/binary views retain their shared data-buffer table and reservation |
| [vortex-zstd](../../vendor/vortex-zstd/upstream-provenance.json) | Decode/compress/training/workspace and frame-metadata allocations, sparse selected-frame access, checked text reconstruction and last-owner release remain explicit |

For each invariant record its current regression, exact upstream replacement,
and outcome: retire the local change, port the smallest still-needed change, or
retain an explicit block. New allocator-taking APIs do not establish coverage
of metadata collections, codec heaps or external owners. Historical provider
READMEs still label some text work as candidates; reconcile those labels with
the accepted runtime and evidence before deciding retention. Preserve failed
experiments and do not revive them merely because the provider changes.

Review authoritative published package archives and licenses, then update
provenance, modified-file notices, Python license bundles, dependency records
and excluded-provider validation together. The tagged upstream workspace is
Apache-2.0; that does not replace review of newly resolved dependencies or
optional features. Keep the default build lightweight and CPU execution usable.

The [0.90 tagged workspace](https://github.com/vortex-data/vortex/blob/0.90.0/Cargo.toml)
declares Arrow/Parquet 59.2-compatible requirements; the candidate resolves the
shared 59.3.1 patch graph across array/schema/IPC/Parquet/Avro/Flight/Python
boundaries. Test those actual consumers.
Avoid duplicate boundary types or conversion bridges introduced only to make
two incompatible graphs compile. Resolve optional rich-type dependencies
separately if they cannot share that graph. Check toolchain/platform constraints
against the resolved packages; do not infer them from the release number.

The [0.90 release](https://github.com/vortex-data/vortex/releases/tag/0.90.0)
removes legacy-session entry points and makes more execution contexts explicit.
The [0.87 release](https://github.com/vortex-data/vortex/releases/tag/0.87.0)
also changes aggregate, compression-session and serialization-registration APIs.
Map actual callsites rather than replacing signatures without ownership review.
Allocation failure, cancellation, generation checks, nullable semantics,
retained-result lifetime and deterministic errors remain acceptance obligations.

The maintainer's subsequent October 24 clarification removes historical-file
compatibility as an acceptance requirement. Existing generated files, fixtures
and internal representations may be rebuilt or redesigned as needed; do not
maintain old readers, conversion bridges or writer editions solely to preserve
them. Regenerate needed datasets from their authoritative inputs with the
upgraded provider and validate complete fresh write/read/reopen/export behavior,
including rich, nullable and extension values. Record the chosen edition and
serialized IDs and their supported reader requirements. Newly enabled encodings
still need semantic, ownership and performance evidence; historical compatibility
does not constrain that choice. Preserve compact prior observations and their
identities as historical evidence, without retaining redundant bulk artifacts.

## Step 2: connect relevant improvements to real callers

The implemented candidate has the following bounded activation evidence:

- [Parquet-view intake regression](../../shardloom-vortex/src/vortex_ingest_referenced_cost_tests.rs)
  follows decoded views through native ownership and writer costing. Physical
  buffer retention stays fully charged while compression uses exact referenced
  bytes. This establishes the cost distinction, not faster full ingestion.
- [Immediate Parquet export](../../shardloom-vortex/src/native_arrow_conversion.rs)
  uses the upstream noncompacting export option only for admitted top-level
  canonical text/binary views. Large sparse backing buffers, tight budgets,
  contiguous values and nested text retain existing compaction/conversion.
  [Four regressions](../../shardloom-vortex/src/native_arrow_immediate_tests.rs)
  pass payload sharing, retained credit, nulls, schema metadata and reopen checks.
- [Sparse FSST selection](../../shardloom-vortex/src/native_provider_memory_tests.rs)
  passes actual compressed-code gathering before admitted decode, including
  fewer than 1,024 rows, slices, duplicates and null selections. The test also
  proves last-buffer-owner release. No Zstd-text speed claim follows from it.
- [Writer edition regression](../../shardloom-vortex/src/local_primitive_native_writer_tests.rs)
  pins the frozen `CORE_2026_08_3` edition, verifies fresh native write/reopen
  values and rejects the opt-in draft Zstd buffer encoding. Optional registrations
  do not silently become writer permissions.

V1 remains the admitted scan path. V2's unsupported list-layout coverage and
unproven retained-state allocation/lifecycle contract prevent replacing it in
this refresh. Carry source-state evaluation into the existing warm-reuse package;
there is no second cache, scheduler or public engine mode.

Cached V1 opens defer the upstream V2 segment-location copy and plan state until
an explicit `scan_file` call. Cold clones share that initialization; source
replacement resets source-dependent plans and can retain immutable locations.
The file-provider regressions verify complete V1/V2 values, metadata locations
and replacement-source isolation. They preserve the provider API, without
admitting V2's explicit-use allocation or execution contract to ShardLoom.

| Priority and upstream evidence | ShardLoom integration and required observation |
| --- | --- |
| Referenced-byte costing: [#10445](https://github.com/vortex-data/vortex/pull/10445) in 0.89 | Trace Parquet-decoded views → imported arrays → compression estimates → coalescing → native blocks. Observe whether the current path encounters inflated backing-buffer size, and retain separate referenced-size and retained-allocation accounting. |
| Builder/execution allocators: [0.86](https://github.com/vortex-data/vortex/releases/tag/0.86.0) and [0.87](https://github.com/vortex-data/vortex/releases/tag/0.87.0) | Use the admitted shared allocator where upstream now supplies the hook; preserve local ownership where it does not. Opaque Arrow-owner adoption still requires independent lifetime/allocation proof. |
| Arrow export options: [#10220](https://github.com/vortex-data/vortex/pull/10220) in 0.88 | Select compaction per existing result/writer consumer. Immediate compatible consumers may retain buffers; long-lived small results may benefit from compaction. Prove nested metadata, lifetimes, retained bytes and serialization behavior. This export change does not itself improve ingestion. |
| Push pipelines and retained file state: [#10391](https://github.com/vortex-data/vortex/pull/10391), [#10631](https://github.com/vortex-data/vortex/pull/10631) | Evaluate under the existing source owner and resource controller. Check backpressure, actual workers, cancellation, cache credit, generation invalidation and error coverage. Reuse source state with fresh query-specific execution. |
| Known row selections: [#10625](https://github.com/vortex-data/vortex/pull/10625) | Confirm that eligible gathers avoid empty chunks, unnecessary masks and unreachable children. That implementation excludes list layouts; preserve working broader paths and prove coverage before replacing them. |
| Sparse FSST gathers: [#10509](https://github.com/vortex-data/vortex/pull/10509) | Exercise actual FSST values, sliced spans, nulls, repeated indices and selected compressed bytes. Do not attribute the benefit to Zstd text or assume all string columns reach this kernel. |
| Scalar/statistics/aggregate and encoded CPU changes: [0.88](https://github.com/vortex-data/vortex/releases/tag/0.88.0), [0.89](https://github.com/vortex-data/vortex/releases/tag/0.89.0), [0.90](https://github.com/vortex-data/vortex/releases/tag/0.90.0) | Identify activated call paths and unchanged ShardLoom-owned operators. Upstream microbenchmarks are not whole-query evidence; DateTimeParts decoding is not automatically Q19's preparation fix. |

`with_layout_reader_cache()` is already used in production by
[`resident_session.rs`](../../shardloom-vortex/src/resident_session.rs) and
[`resident_execution_context.rs`](../../shardloom-vortex/src/resident_execution_context.rs).
Upstream explicitly describes V1 child-reader and decoded-dictionary reuse.
The new retained V2 state is not evidence that current ShardLoom has no such
reuse. Evaluate dictionary/value-cache lifetimes and static versus dynamically
bound expressions without reusing stale predicates. Do not add a second source
cache or scheduler merely because upstream introduces a new scan interface.

Activate straightforward compatible improvements here. Feed remaining
source-lifetime changes to the [warm package](native-warm-source-reuse-2026-10-24.md),
copy/provenance and complete-ingest changes to the
[ingest package](native-ingest-ownership-preparation-2026-10-24.md), and target-aware
conversion to [universal I/O](native-universal-io-performance-coverage-2026-10-24.md).
Update those plans against the migrated caller graph before implementing them.
Freeze a finite decision for a material performance mechanism and use the
existing acceptance process; do not create another optimization framework.

## Step 3: richer values and optional physical representations

The tagged [Parquet Variant implementation](https://github.com/vortex-data/vortex/blob/0.90.0/encodings/parquet-variant/src/lib.rs)
registers Arrow import/export and JSON-to-Variant support, with unshredded
bytes and typed shredded fields. Treat it as a concrete provider candidate.
The [rich-data package](native-rich-data-lifecycle-2026-10-24.md) should preserve
Variant and extension metadata through native retain/filter/join/write/reopen
and export before adding field access. Inspect spatial/UUID/vector/tensor
providers and type admission with the same discipline. Spatial functions need
explicit coordinate-system and distance semantics; a physical binary/list
representation alone is not logical-type fidelity. Model-data consumers remain
owned by the [AI-data package](native-ai-data-workflows-2026-10-24.md).

Blocked frame-of-reference and bit-packing, compressed binary offsets and
byte-oriented FSST are targeted investigations for suitable data. Check edition
admission, encoding/decoding cost, metadata, stored size and actual consumers.
Do not rerun rejected numeric/codec profiles unchanged or assume already
compressed image/audio/archive payloads benefit. Upstream encoded-memory size
results are not file-size or read-throughput proof.

The [split-block Bloom filter](https://github.com/vortex-data/vortex/pull/9398)
supports selected scalar families, not composite structs/lists. Preserve type,
hashing, null and source-generation semantics: a negative may prune; a possible
match still needs exact evaluation. The [indexed-layout work](https://github.com/vortex-data/vortex/pull/9706)
is explicitly a proof of concept. Neither is a complete ANN, lexical-search or
universal indexing product. Evaluate them only for a justified caller after the
immediate costing/allocator/export/scan work, with bounded build/refresh/storage
and invalidation costs. They remain optional to ordinary native execution.

The [0.90 umbrella features](https://github.com/vortex-data/vortex/blob/0.90.0/vortex/Cargo.toml)
expose object-store registry, `hf://` and OpenDAL support. Reuse suitable provider
primitives in the existing I/O package; enabling a feature does not complete a
connector. Authentication, stable generation identity, range access, retries,
cancellation and resource admission still belong to ShardLoom's source contract.
No transport or external-effect default changes as a side effect of the bump.

## Acceptance and remaining risks

1. Record exact old/new resolved graphs, patch decisions and provenance. Prove
   every retained semantic and ownership invariant through its existing tests,
   plus regressions for changed APIs. Preserve full allocation credit across
   clones/slices/results and release it after the last owner, including errors.
2. Run format, strict default/native workspace lint/tests, applicable standalone
   patched-provider checks, public Python/CLI result and schema tests, native
   import/export/write/reopen and package boundary checks. Include nullable,
   empty, malformed, cancellation, pressure and long-lived result cases.
3. Verify fresh regenerated inputs and chosen-edition writes independently. New
   types/encodings need their own complete read–compute–write–reopen evidence;
   successful registration or build output is insufficient.
4. Reuse parity's preserved complete-result and resource evidence, then perform
   one isolated standard Full43 after the coherent migration under the existing
   rules. Full43 is a regression check, not evidence for warm retention, large
   ingestion, selective access, rich types or writer throughput. Give each
   activated family its finite representative workflow and unchanged controls.
5. Keep all failures, unchanged-clock engine observations and non-engine
   diagnostic costs distinct. No ShardLoom speedup follows from release notes,
   smaller upstream micro-operations or an unactivated feature.

Open implementation questions are which local patches upstream fully replaces,
which scan/provider shapes cover current production data, which consumers can
safely retain uncompacted buffers, and whether costing changes activate in the
current ingest. Resolve them from code and finite measurements. If a provider
gap prevents migration, retain the accepted runtime and an explicit dependency
block instead of weakening its guarantees or silently delegating execution.

### Acceptance history and final matched-build results

The first matched build passes all 5,930 cases in the 39-family public suite
and all 140 installed native Python integration tests without skips. The modular
matrix passes its first two collection cohorts (704 records), then stops in the
first write cohort: the nested primitive writer emits duplicate report fields.
The output operation succeeds, but the strict report reader rejects the report.
The original failed run remains evidence; it is not a completed matrix.

The repair merges completed primitive observations into existing writer fields
and removes three repeated preparation timing entries. Regressions check unique
keys across eight output formats, single/fanout writes and format preparation,
plus complete result values. The 164 file-provider tests and 15 preparation
tests pass, alongside the writer regression and strict provider/native lint.
The final matched build then passed native workspace
tests (6,072 passed; 35 existing opt-in tests ignored), default workspace tests
(3,246 passed), strict lint and format, all-feature/minimal checks, and Rust
1.96.0 checks.

The final matched installed wheel passed all 140 native Python integration tests
with no skips. The public r2 suite passed 5,930 cases across 39 families; saved
semantic replay validated 5,639 actual/expected complete-value files plus 56
nested and 81 case JSONL archives. The recovered modular matrix accepted 1,408
records (704 native and 704 independent-oracle) across 18 cohorts. Preserve its
first duplicate-report failure and the r2 final-cohort storage interruption:
all 17 completed cohorts were reused, and only the final 44-record cohort was
rerun after verified lossless log compaction under the unchanged 192 MiB ceiling.

Fresh ingest r2 produced 99,997,497 rows. The initial r1 ingest attempt failed
before engine execution because it used system Python; the corrected run changed
PATH only. One isolated Full43 r1 then completed all 43 queries once, with 381
complete rows and exact schema continuity. Its engine time was 49.625514041
seconds versus the prior CLI observation of 48.701809680 seconds. These single
observations do not establish a causal speedup or regression. The authoritative
acceptance records are `fresh-ingest-acceptance-r2.json`,
`public-transcript-verification-r3.json`,
`modular-r1/provider-native-matrix-2-recovered-report.json`,
`provider-validation-primary-r5.json`, and `full43-acceptance-r1.json` in the
provider-refresh evidence directory. Runtime/UAT acceptance is complete; the
portable evidence replay passes and the provider category PR remains pending.

The final source audit repeats only the three build checks that preceded the
last repairs: `check-msrv-native-090-r2`, `check-msrv-no-default-090-r2` and
`check-workspace-all-features-090-r2`. All pass with source hashes identical to
the final build. The r5 validation catalog preserves 138 receipts, including
47 earlier failures and two filtered runs that executed no tests; their later
executed replacements remain distinct records. See the
[acceptance report](../benchmarks/native-vortex-provider-refresh-2026-10-24.md).
