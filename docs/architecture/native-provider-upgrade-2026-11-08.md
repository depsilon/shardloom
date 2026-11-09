<!-- SPDX-License-Identifier: Apache-2.0 -->

# Vortex and Arrow provider update

Status: accepted and merged in [#1670](https://github.com/depsilon/shardloom/pull/1670)
as `d29bff42c5b3aacf7a61463bf2c8f9cec2bdf216`. All 58 PR checks and all 55
required checks on exact merged main passed, with the final review clear. The
[portable evidence packet](../release/evidence/native-provider-upgrade-2026-11-08/README.md)
binds all fifteen required stages to the same complete source snapshot and graph.
The November 8 maintainer instruction
places this work after the already-running calendar/transport/CI validation,
before resolving the remaining Dependabot PRs and resuming item 19's shared
exact vector expressions. This is a provider maintenance unit under the active
phase plan and RFC 0044; it does not close item 19, item 20 or CG-1 through CG-23.

The selected graph is Vortex 0.93.0 with one compatible Arrow/Parquet 59.3.2
family. Eight Vortex and four Arrow/Parquet provider trees were rebased from
verified registry archives; Arrow data was already 59.3.2. The former separate
RunEnd provider is removed after its code, owner contract and metadata fixture
move into `vortex-array`. ORC remains at its existing 0.9.0 boundary. Arrow 60's
consumer migration remains deferred for the compatibility reasons below.
The existing registry `vortex-arrow` 0.93.0 is now a fifteenth local provider
for scoped decimal storage preservation at admitted Arrow intake boundaries.

The final dependency reconciliation closes all five retained proposals:
#1665's CI action pins, #1667's tonic-prost 0.14.9 and #1668's Jiff patches are
included by ancestry; #1664's Vortex 0.91 proposal is superseded by 0.93.0.
#1666 was rebased after integration, reviewed again against its exact new head,
and closed as deferred because its four Arrow 60.1.0 pins would mix runtime
Arrow families. No permanent dependency-ignore policy was added. A fresh final
search found no open Dependabot proposals.

Verified retirement removed 40,614 generated entries, including 35,766 regular
files and six environment links, after repeated identities and closed-handle
checks. The accepted provider runtime/wheel and current inputs remain unchanged;
the complete portable packet replays after retirement. No payload backup or
storage-limit change was made. The main queue now resumes with
[shared exact vector expressions](native-vector-expressions-2026-11-08.md).

The default workspace passes 3,321 tests; the native workspace passes 6,996,
with 37 reported opt-in ignores and no failures. Formatting, strict default/native
lint, generated references, supported feature configurations, Rust 1.96 checks
and the unchanged dependency audit all pass. The canonical Python core shard
records 1,247 discovered cases, 857 reported runs and 56 optional/environment
skip records, with no failures or errors. The ordinary isolated wheel passes all
386 integration tests with no failures, errors or skips, including load and
uninstall checks against the newly built runtime.

All fifteen standalone provider configurations have passing tests and strict
lint tied to their current compiled inputs, configuration and lockfiles. This
includes the array provider's 5,773 passes and one existing ignore, the added
Arrow provider's 406 passes, and Parquet's 1,243 unit plus 105 enabled integration
tests with one existing ignored integration case. Unchanged checks are reused
only after exact input comparison; provenance-only record changes are
separately verified against the current archives.

The first broad native run exposed 22 compatibility regressions. Their targeted
24-test cohort and 19 statistics ownership/wide-schema checks pass under unchanged
grants, followed by the successful complete native run above. Earlier storage-floor
interruptions and evidence-tool corrections remain preserved. The interrupted
array/layout wrapper is not acceptance: a fresh array test/lint run completed
with clean before/after source checks, while the other affected providers have
their own completed evidence. The packet retains 70 original nonacceptance receipt
records, including child and preparation records; these are not 70 independent
runtime executions. Original and relocated replay pass, and all 13 corruption
controls are rejected.

The source and license records describe the current archives and retained
local modifications. The Python notice bundle verifies 431 registry packages,
15 path providers, 829 legal-file records and 249 distinct texts. The initial
standalone graph review checked 14 locks and the exact archives, declared
licenses and declared Rust minimums of 33 new development-only package identities.
The current review checks all 15 locks and 34 identities absent from the prior
standalone graphs. The extra identity is the already-reviewed registry Arrow
provider, now development-only relative to the engine's new local path provider.
Provider-local Arrow 58.4.1 remains only in ORC's unchanged test dependencies;
the engine graph contains only Arrow 59.3.2.

Decimal storage imports are explicitly scoped: native file reads preserve stored
width and raw bits, while resident and streaming Arrow intake validate reachable
values before opting into the same preservation. Null-parent children and values
outside the slice remain unobserved. Ordinary upstream deserialization/import
retains its precision validation and narrowing. Existing canonical dictionary
output now also makes validity concrete at that same serialization boundary.

The port preserves one running statistics tree, bounded independently owned
variable-length extrema, legacy Sum/overflow semantics, and lossless legacy
footer slots alongside 0.93's aggregate/nested statistics. Table/path/aggregate
admission covers conservative construction overlap and last-owner release.
Malformed statistics still fail explicitly. Shared-dictionary rewrites now
retain code/gather and output owners at both inner and root execution boundaries.
Nullable or encoded code trees retain their documented scratch exclusions while
the output owner remains enforced. V2 scans remain outside native admission.

## Version selection and compatibility boundary

The review covers released Vortex **0.93.0**, including the 0.91 and 0.92 changes
since the prior 0.90.0 provider. The original bot proposal targets 0.91.0, but
[0.93.0 was released on November 6](https://github.com/vortex-data/vortex/releases/tag/0.93.0).
Use exact published registry archives, checksums, member inventories and license
records for implementation. A tag alone does not establish the identity of a
published crate with a dirty VCS marker.

The [0.93 workspace](https://github.com/vortex-data/vortex/blob/0.93.0/Cargo.toml)
still requires Arrow/Parquet 59.2. At intake, ShardLoom already contained Arrow
buffer, data and schema 59.3.2, with other Arrow/Parquet providers at 59.3.1.
The verified [59.3.2 maintenance release](https://github.com/apache/arrow-rs/releases/tag/59.3.2)
completes the compatible family. The review also covers
[Arrow 60](https://github.com/apache/arrow-rs/releases/tag/60.0.0) and
[60.1](https://github.com/apache/arrow-rs/releases/tag/60.1.0) for useful changes,
backports and future migration requirements. They are not interchangeable with
the current provider's Arrow types.

The upgrade must resolve one Arrow/Parquet major across Vortex, direct format
adapters, optional Flight, Python capsules, ADBC and ORC. Do not introduce a
second major or a conversion bridge merely to make the partial bot proposal
compile. Advancing the whole graph to Arrow 60 would also require a reviewed
port of the upstream consumers; changing ShardLoom's four pins alone is not that
port. The compatibility decision and any unadopted update remain explicit in the
final disposition. Arrow 60 remains deferred; local acceptance of the selected
0.93/59.3.2 graph does not accept that separate consumer migration.

## Incorporate improvements through existing providers

The relevant [0.91 changes](https://github.com/vortex-data/vortex/releases/tag/0.91.0)
and [0.92 changes](https://github.com/vortex-data/vortex/releases/tag/0.92.0) affect
existing native boundaries. Trace them to actual ShardLoom callers and regressions:

| Upstream change | Integration obligation |
| --- | --- |
| Full-range timestamp storage, checked temporal values and extension scalar reads | Compare with local signed-Int64 storage, timezone validation, date/time display and DateTimeParts corrections. Keep every still-needed invariant and test the public native persistence boundary. |
| RunEnd moves into `vortex-array`; filtering and dictionary/byte export are revised | Move the retained view-table owner contract to the new implementation location. Verify sliced, repeated, nullable, empty and text/binary values and last-owner release. Removing the old crate is not permission to remove its guarantee. |
| Canonical decimal widths and lazy Widen arrays | Adapt API assumptions without narrowing decimal values or silently changing schema, nullability or integer interpretation. Cover native read/write, comparisons, aggregates and compatibility export through existing tests. |
| Array/layout plugins distinguish read and write IDs; layout planning APIs change | Record selected identities and writer edition. Preserve explicit admission and unsupported diagnostics; registration alone enables no optional encoding or execution route. |
| Exact uncompressed-size accounting, statistics accumulation and writer error propagation | Reconcile logical cost with retained allocation credit. Keep bounded conservative statistics, nullable roots, checked overflow, writer admission and propagation of child failures. |
| Encoded comparisons, iterative execution and dictionary/byte builder reuse | Use the revised admitted native providers where existing callers reach them. Preserve allocation refusal, owned deferred views, selection correctness and metadata. Upstream performance notes are not ShardLoom performance evidence. |
| Arrow import/export preserves more extension and sliced-child metadata | Check shared dictionary, map, run-end, tensor/vector and extension boundaries. Preserve known metadata and reject unsupported execution explicitly. Unknown extension round-trip support is not blanket expression admission. |
| V2 planning and per-root I/O session changes | Review against existing source ownership and V1 reader-cache use. Keep V2 execution unadmitted until its own complete allocation, cancellation and lifecycle contract is proven; do not add a second scheduler or cache. |

The Arrow maintenance review covers run-end/union/offset validation, empty IPC
validity buffers and Parquet metadata/encoding corrections. Preserve local
fixed-size-list row counts, inherited child masks, timestamp hints, memory
reservations and adaptive read controls while taking compatible upstream fixes.
Arrow 60's ordered metadata and changed FFI/page-index APIs require a coordinated
consumer migration; their existence does not change the current native execution
substrate or authorize new unsafe workspace code.

## Retained local contracts

Review all fourteen Arrow/Parquet/Vortex patches and the separate ORC boundary.
For each modified source or added helper record whether the upstream version
supersedes it, it is ported with its existing contract, or it blocks acceptance.
Retirement requires source evidence and equivalent regression coverage.

| Provider | Contract to preserve |
| --- | --- |
| `vortex-array` | Full temporal storage domain, bundled timezone policy, checked statistics, fallible host/external reservations, array owners, deferred owned views, admitted variable-binary tables and checked text reconstruction |
| `vortex-arrow` | Scoped recursive decimal storage import after ancestor-aware validation, preserving physical widths, bits, nulls and shared buffer ownership without changing default imports |
| `vortex-buffer` | Explicit empty buffer owners survive typed and Arrow/Bytes aliases |
| `vortex-datetime-parts` | Signed-Int64 day storage, exact split/recombine and encoded comparisons |
| `vortex-file` | Admitted footer/schema/layout and segment metadata, locator names, aligned buffers, coalescing amplification bound, lazy unused V2 state and owner release |
| `vortex-fsst` | Admitted temporary/final view tables and shared payload owners |
| `vortex-io` | Destination admission before reads, initialized storage, checked ranges and short/oversized-response errors |
| `vortex-layout` | Conservative bounded statistics, nullable structural reads, cache/child metadata/chunk-offset ownership and traversal error propagation |
| Former `vortex-runend` | Expanded text/binary views retain their shared table reservation after moving into the array provider |
| `vortex-zstd` | Codec/workspace/training/frame admission, selected-frame decoding, checked text reconstruction, sparse frame index and shared ORC decoder workspace |
| `arrow-array` | Width-zero fixed-size-list row counts and checked validity |
| `arrow-data` | Struct slices with independent parent/child offsets preserve values and validity |
| `arrow-avro` | Bounded next batches, framing/schema checks, controlled codec/decoder allocations and terminal-error behavior |
| `arrow-ipc` | Controlled schema/dictionary/array/decompression admission and retained metadata/batch owners |
| `parquet` | Stored timestamp units with presentation zones, row-group metadata admission, adaptive row limits and inherited nested fixed-size-list null masks |
| `orc-rust` | Current codec-error propagation, schema/value behavior and shared Arrow type identity; no unrelated ORC rewrite |

Original provider licenses and copyright notices remain. Regenerate provenance,
modified-source notices, dependency/license bundles, provider test lockfiles and
generated API identities from the final resolved source. Removed paths remain
recoverable from Git and their compact source comparison, rather than retaining
obsolete executable or dependency payload backups.

## Acceptance

The packet preserves the completed vector integration and separately replays
the earlier small-patch baseline from its own frozen source. Those earlier tests
are not tests of the upgraded provider. The first five acceptance obligations
below pass for the current graph; portable replay and corruption controls also
pass. Final-head hosted checks, review, merge, exact-main verification and
post-integration retirement are the remaining delivery boundary.

- Exact archive/member/license verification, complete dependency graph and
  no-fallback review; no unintended unused provider patch or duplicate Arrow major.
- Focused adapter regressions for changed APIs and overlapping fixes, including
  denial/cleanup, empty/null/sliced values, temporal and decimal boundaries,
  selection, metadata preservation and owner lifetimes.
- Standalone tests and strict lint for changed excluded providers; retain the
  upstream test-support provenance and meaningful local regressions.
- Formatting, strict default/native workspace lint and tests, feature matrix,
  supported Rust 1.96 checks, generated documentation and unchanged audit policy.
- Fresh native write/read/reopen and compatibility boundaries through ordinary
  CLI/Python workflows, canonical Python core checks, and the ordinary installed
  wheel with zero skips in the installed acceptance cohort.
- Portable evidence with every failed attempt, source/runtime identities and
  independently replayed verification, followed by final-head hosted checks,
  review, merge, exact-main verification and guarded artifact retirement.

Historical file compatibility remains outside the inherited acceptance contract;
fresh supported native lifecycles must pass. Keep current input generations and
the latest accepted runtime until a tested replacement is accepted. Existing
storage ceilings, process cleanup, concurrency controls and ownership policy
remain in force. This unit does not run a benchmark, claim a speedup, publish a
package, enable an external execution engine or widen optional effects.

Vortex-first decision: use and wrap the existing Vortex-native array, encoding,
file, layout and I/O providers inside `shardloom-vortex`. Residual execution stays
ShardLoom-native or fails explicitly. Arrow remains a declared compatibility
boundary. Existing feature gates, provider versions and certificates govern
admission, with `fallback_attempted=false`.
