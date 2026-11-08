<!-- SPDX-License-Identifier: Apache-2.0 -->

# Vortex and Arrow provider update

Status: source review and preparation. The November 8 maintainer instruction
places this work after the already-running calendar/transport/CI validation,
before resolving the remaining Dependabot PRs and resuming item 19's shared
exact vector expressions. This is a provider maintenance unit under the active
phase plan and RFC 0044; it does not close item 19, item 20 or CG-1 through CG-23.

## Candidate and compatibility boundary

Review released Vortex **0.93.0**, including the 0.91 and 0.92 changes since the
current 0.90.0 provider. The original bot proposal targets 0.91.0, but
[0.93.0 was released on November 6](https://github.com/vortex-data/vortex/releases/tag/0.93.0).
Use exact published registry archives, checksums, member inventories and license
records for implementation. A tag alone does not establish the identity of a
published crate with a dirty VCS marker.

The [0.93 workspace](https://github.com/vortex-data/vortex/blob/0.93.0/Cargo.toml)
still requires Arrow/Parquet 59.2. The current ShardLoom graph already contains
Arrow buffer, data and schema 59.3.2, with other Arrow/Parquet providers at 59.3.1.
Review the [59.3.2 maintenance release](https://github.com/apache/arrow-rs/releases/tag/59.3.2)
and complete the compatible family update after confirming published packages.
Review [Arrow 60](https://github.com/apache/arrow-rs/releases/tag/60.0.0) and
[60.1](https://github.com/apache/arrow-rs/releases/tag/60.1.0) for useful changes,
backports and future migration requirements. They are not interchangeable with
the current provider's Arrow types.

The upgrade must resolve one Arrow/Parquet major across Vortex, direct format
adapters, optional Flight, Python capsules, ADBC and ORC. Do not introduce a
second major or a conversion bridge merely to make the partial bot proposal
compile. Advancing the whole graph to Arrow 60 would also require a reviewed
port of the upstream consumers; changing ShardLoom's four pins alone is not that
port. The compatibility decision and any unadopted update remain explicit in the
final disposition. Source review may narrow or change the candidate before
implementation; no candidate is accepted by this design note.

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

Preserve the completed vector integration and the in-flight small-patch results
as distinct observations. They are not tests of the upgraded provider. Bind all
final acceptance roles to the same source, resolved graph and runtime:

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
