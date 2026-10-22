<!-- SPDX-License-Identifier: Apache-2.0 -->

# Changed-source reuse: provider and identity intake — October 20, 2026

Status: historical prerequisite investigation for `PERF-02.changed_source`.
The subsequent [finite whole-file comparison](native-changed-source-reuse-2026-10-20.md)
rejects its unconditional adapter; all experimental code is removed. No
changed-generation public admission or retained optimization follows.
The [conditional sequence](native-conditional-investigations-2026-10-19.md)
continues to own row 13; RFC 0044 and all CG-1 through CG-23 remain in force.

## Current boundary

The public local preparation function in
`shardloom-cli/src/sql_local_source_runtime.rs` uses the source binding from
`shardloom-vortex/src/prepared_source_binding.rs`. Reusing an existing prepared
artifact requires an exact binding match. Explicit preparation of a new artifact
reads and converts the current source normally. The binding fingerprints local
generations; it is not independent per-file content provenance.

The separate append-only evaluator in `vortex_ingest.rs` has no production caller
in the tracked Rust, Python or shell sources. It checks the old CSV/JSONL byte
prefix, but does not establish generic partition reuse. Native Vortex preparation
has a distinct same-artifact or whole-file byte-copy path. Its lack of re-encoding
does not establish selected-region rewriting.

Vortex 0.85.0 is the pinned Apache-2.0 native provider. The active array, file and
layout crates are the reviewed `vendor/` patches selected by the workspace's
`[patch.crates-io]`, including bounded file statistics and lifetime ownership.
Initial external inventory notes incorrectly called the file/layout providers
unpatched registry sources; corrected inventories recheck the active files and
record that correction. Build source identities include these vendored sources.
Their ordered scan returns
encoded-capable `ArrayRef` objects with explicit row-range selection. Existing
dictionary tests prove one encoding survives a scan, not arbitrary representation
identity. `VortexWriteOptions::with_strategy`, `StructStrategy`, `ZonedStrategy`
and `FlatLayoutStrategy` can serialize arrays into a new file. No inspected API
provides a generic raw-segment copier for an existing file. Reusing arrays still
requires native input reads, serialization, complete output writes and metadata.

## Frozen provider prerequisite

Before refresh timing or production changes, run a test-only provider probe:

1. Write three ordered batches with the current normal native writer strategy,
   preserving the existing bounded input layout, file statistics and publication
   helper. Use 8,192, 8,192 and 521 rows with nullable integers and nullable UTF-8
   containing repeated strings, long strings, empty strings, Unicode and NUL.
2. Close the producer. A new runtime and session scan four predetermined ranges:
   `0..8192`, `8192..16384`, `16384..16905` and `4093..12111`.
3. Serialize those arrays with a bounded native struct/zone/flat strategy. Do not
   request Arrow conversion, recursive canonicalization or compression of reused
   payloads. Any unsupported serialization fails the probe; it does not silently
   change the route.
4. Close all producers/readers, move each result, then use a fresh runtime and
   session to check all ordered values, exact logical dtype/nullability, file
   statistics and encoding inventories against independently generated expected
   values. Check that encoded nodes actually occur in the source and survive the
   rewrite. Record any representation change rather than asserting raw byte-copy.
5. Bound native credits at 256 MiB, each file at 32 MiB and total generated probe
   payload at 160 MiB. Release every native owner, verify zero live credits and
   remove only the probe's generated files after compact evidence is saved.

This probe proves provider feasibility only. It does not prove content identity,
production allocator coverage, changed-source admission, cross-process trust,
query performance, generic pruning equivalence or recovery. It records no scored
timing. A failed provider check identifies a prerequisite; it is not an evaluated
refresh performance rejection.

### Explicit wrapper revision after the first provider refusal

The first guarded probe (`changed-source-probe-run-811`) writes its ordinary
source successfully, then refuses the selected-region write because
`vortex.shared` is not a serializable encoding. Vortex 0.85's dictionary layout
reader creates `SharedArray` wrappers around dictionary values; its serializer
explicitly rejects that wrapper. The failed source/build/command identities and
result are retained. No cost comparison or broad changed-source rejection follows.

The revised probe explicitly removes only the native `SharedArray` wrapper via
its public source accessor, reconstructing native Struct/Dict parents with their
checked constructors. It does not call the wrapper's canonicalizing execution
method, generic unsafe slot mutation, or a decoded execution engine. Recursion
is bounded. Any unsupported wrapper beneath another encoding still fails.
Record raw and normalized encoding trees separately; require the normalized
payload encoding tree and exact values to survive serialization. This is a new
documented provider operation, not silent canonicalization after a failed write.
The same range, dtype, statistics, ownership and storage gates apply. Fresh
runtime reopen remains distinct from later required fresh-process acceptance.

### Whole-batch admission after the cross-boundary refusal

The revised run (`changed-source-probe-run-812`) completes the three aligned
ranges with exact values, dtype, statistics, encoding trees and zero remaining
native credits. Its fourth, cross-boundary range fails: dictionary codes retain
`vortex.slice`, another explicitly nonserializable provider wrapper. The complete
four-range protocol therefore fails; its original source, build and failed result
remain recorded. Passing the first three ranges is a bounded fact, not a pass for
arbitrary selected regions.

The next prerequisite revision admits only complete original source batches.
It must pass the three original aligned ranges and reject the original
cross-boundary range before creating a destination. No slice canonicalization or
new encoding is added. A subsequent file-region candidate must prove that its
per-file row intervals comprise complete original preparation batches; it cannot
expand its interval merely to include neighbors. Each reused array must pass the
explicit wrapper check. Record every completed case before a later case can fail.
This restriction precedes any candidate cost comparison and does not alter a
performance threshold. General partial-batch reuse remains unsupported.

The whole-batch revision (`changed-source-probe-run-812b`) passes all three
aligned positive cases and the cross-boundary refusal. Each positive case
removes one Shared wrapper and preserves exact ordered values, logical schema,
statistics and the normalized encoding trees. All four generated native files
are retired after the compact result is saved and native credits reach zero.
The retained result digest is
`9cfe03a44c1da882ef0d24ba4fae0309d7c4122cdb17726f3061270314076d0d`.
The complete four-range failure remains a separate result; this success covers
whole original batches only and supplies no refresh speed measurement.

## Ordinary public preparation prerequisite

The [file-region proof design](native-changed-source-region-proof-design-2026-10-20.md)
specifies the content/provenance and complete-workflow obligations before a
candidate. Its first ordinary-route test now passes for both Parquet and Arrow
IPC directories. Three files created out of order preserve lexical file order,
NULLs, empty strings, Unicode/NUL and long text through the native writer and
native relational consumer. Repeat preparation succeeds; adding a directory
member invalidates the held preparation identity, and a failed new request
preserves the existing target bytes.

The first test run failed its final zero-credit assertion while its successful
reuse result still held the intentionally reserved generation metadata. The
corrected test verifies that ownership, verifies invalidation, drops the result,
then proves zero credits. The production lifetime contract did not change.
Both test outcomes and their source/build identities are retained. The subsequent
baseline observation uses sixteen Parquet files and the separately frozen
ordinary-preparation protocol in the design; it has no candidate or speedup gate.

That optimized observation (`changed-source-baseline-run-813d`) now passes all
three preparations and 54 native consumer calls. Each file supplies one observed
8,192-row batch, all 131,072 ordered values and both nullable input fields match,
and the filtered COUNT/SUM matches on first and repeated execution. The SUM schema
preserves the existing nullable F64 contract. Every output is closed, moved and
reopened; all native credits and generated payloads are released. The complete
result digest is
`87c10a46df267997c2c0032688f8a6f2d72702fde3c02a4724131fc8ff423b0b`.

Two earlier harness expectations were corrected with source evidence: primitive
type names use lowercase in native schema JSON, and primitive SUM emits F64.
Their failures and cleanup receipts remain distinct from the passing optimized
observation; an intervening unoptimized run proves correctness only. Normal
writer work dominates the recorded preparation diagnostics for this shape.
The default provider's explicit compression counter is zero, so it cannot be
treated as an isolated total encoding-cost measurement. A reuse probe must still
measure the complete replacement write, statistics and metadata costs rather
than assume that all current writer work disappears.

## Subsequent candidate admission

The file-identity, mixed-writer and separate-process prerequisites now pass.
Ten focused tests cover the complete map, changed/added/removed and renamed
files, empty/all-null data, actual batch boundaries, content/generation races,
corruption/truncation, cancellation, native-credit denial and failed-publication
cleanup. Fault-injection tests assert that their intended injection point ran.
An earlier negative fixture incorrectly assumed Parquet row groups force reader
batch boundaries; the corrected fixture uses the real reader's byte policy to
force multiple batches. Its original failure remains recorded.

Four separate processes then produce, refresh, consume/rewrite and consume again.
The parent checks full hashes and closed handles, moves each artifact after its
producer exits, and checks the next process's independent reopen. The 16-file
refresh prepares two files normally and reuses fourteen; the next write reuses
all sixteen. All 131,072 ordered values, logical schema, native statistics and
36 real native query calls pass, including filtered COUNT/SUM on first and
repeated execution. All native credits and generated payloads are released.

The first lifecycle used the no-advisor provider recipe. A public-route audit
identified its different block-size selection. The revised lifecycle uses the
admitted small-source writer child and checks its 4,096-row block, 1-MiB target,
compression/statistics and P1 fields against the ordinary public observation.
All sixteen initial Parquet files are byte-identical across those observations.
The revised result digest is
`b34ff71ae5fff15c55857330ce7c352f1875182d379f13ee9e40f7f29decab26`.
An incomplete provider-surface declaration was refused before that passing
revision; its failed tests and source/build identities remain separate.

This establishes the ordinary child recipe and bounded lifecycle, not complete
public preparation equivalence, untrusted-Parquet allocator coverage or changed-
generation public admission. The [finite comparison protocol](native-changed-source-comparison-design-2026-10-20.md)
compares the experimental adapter with the actual ordinary public preparation
helper before any decision to integrate it.

A refresh candidate requires immutable file or independently addressable region
proof first: held local generations plus complete content hashes; exact schema,
transform, provider, recipe, dictionary epoch and deletion semantics; and an
embedded mapping from source regions to native rows in one previous artifact.
Changed and new files must prepare normally. Removed files must not leak old rows.
Same-path/size/mtime replacement, mutation during hashing/reading, incompatible
recipes/schema, forged or incomplete mappings and corrupted previous artifacts
need explicit failure or a predeclared full-rebuild decision before reuse starts.

The initial candidate should use current public preparation's shared native
conversion, writer, memory and source-identity boundaries. Before its timing,
record baseline preparation cost and freeze workload identities, unchanged/new/
changed/deleted controls, complete refresh-versus-rebuild costs and retain/drop
criteria. Charge all current-source hashing, previous-artifact validation,
encoded reads, new conversion/compression, copied bytes, metadata, publication,
first/repeated consumption and cleanup. Do not infer benefit from avoided work.

Safe generation-conditional replacement remains required separately under
`PERF-07.publication`; a new-target probe does not implement compare-and-swap or
multi-output atomicity. No external execution engine or new provider dependency
is introduced by this intake.
