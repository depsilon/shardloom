<!-- SPDX-License-Identifier: Apache-2.0 -->

# Managed binary objects in native data workflows

Status: locally accepted for item 19, step 2, following the accepted
[dense embedding delivery](native-dense-embedding-lifecycle-2026-11-05.md).
The [acceptance record](../release/evidence/native-managed-binary-objects-2026-11-06.json)
binds the original source, checks and installed package. The
[surface follow-up](../release/evidence/native-managed-binary-surface-followup-2026-11-06.json)
binds the corrected Python capability metadata and replacement wheel. Hosted
integration is pending; local acceptance does not claim publication.
The remaining tensor, representation and vector-expression work keeps its
order in the [rich-data lifecycle](native-rich-data-lifecycle-2026-10-24.md).
CG-1 through CG-23 retain their independent obligations.

## Decision

Keep inline Binary values unchanged. Add a versioned binary-object extension
whose cells distinguish managed bytes from explicitly externally owned local
file ranges. Relational operations carry small descriptors. Retrieving bytes
is an explicit operation with complete-read and bounded-range interfaces.

Use one ordinary Vortex file for a complete owned export. A first-party Vortex
layout wraps the existing table layout and owns the raw payload segments and
their integrity directory. The table still uses the existing native
projection, filter, join, ordering, selection and spill operators. The new
layout delegates table evaluation to its child; it does not implement another
query engine or storage engine.

The persisted artifact includes every referenced managed object's bytes.
External references retain their declared ownership and source generation;
they are not represented as copied or independently portable bytes. A Vortex
file containing only managed objects must reopen and retrieve complete values
in a new process after its original sources and session are gone. Moving that
file must not require rebasing payload paths or a companion manifest.

## Vortex-first provider check

The checked provider is the pinned Vortex 0.90.0 graph and its reviewed local
patches. Relevant upstream concepts are extension DTypes, Binary arrays,
Layout/LayoutStrategy, SegmentSink, SegmentMap, VortexReadAt and the existing
file footer. The upstream
[layout model](https://docs.vortex.dev/concepts/layouts) and
[file format](https://docs.vortex.dev/specs/file-format) provide lazy segments
inside a single native artifact. The pinned source is authoritative for API
availability:

- `vendor/vortex-layout/src/{vtable,strategy,reader}.rs` provides a custom
  layout, delegation and ordered segment-writing boundary.
- `vendor/vortex-layout/src/segments/{source,sink}.rs` provides segment
  identifiers and bounded segment transfer.
- `vendor/vortex-file/src/footer/mod.rs` exposes the persisted segment map;
  `vendor/vortex-file/src/writer.rs` produces the final native footer.
- User metadata segments are opaque resident buffers with a finite segment
  count. They are not a streaming large-object owner and are not used to hold
  the payload collection.
- Existing ShardLoom `SourceIdentity`, `OwnedLocalRead`, live memory credits,
  cancellation and staged native writers provide generation checks, bounded
  local reads and failure-safe publication.

Classification: `use_vortex_native_provider` for native arrays, layouts,
segments and file persistence. ShardLoom supplies the object ownership,
descriptor validation, integrity directory, resource admission and
certification around those providers. No Vortex query-engine integration,
external residual executor, runtime dependency or unsafe workspace code is
introduced. `fallback_attempted=false`; `external_engine_invoked=false`.

## Logical and persisted contracts

The extension identity is `shardloom.binary-object.v1`, with Binary storage
and versioned metadata. Binary storage contains a checked descriptor, not the
payload's bytes. Plain Binary remains the inline-byte representation. Null is
distinct from a valid zero-length object. Binary objects are payloads, not
comparison, grouping, ordering or arithmetic keys.

The descriptor has a strict binary grammar with checked lengths and no trailing
bytes. Its managed form records a content identity and logical byte length.
Its external form records an explicit absolute local file path, bounded range
and captured strong local generation. Unknown versions, tags, impossible
ranges, invalid paths and malformed records fail with stable diagnostics.
Cell data does not pass through JSON.

Managed object identities bind the byte length, fixed-size chunk geometry and
ordered SHA-256 chunk digests using a domain-separated SHA-256 digest. Empty
objects have an explicit valid identity with no data chunks. Each range read
verifies every touched chunk against this directory, then returns precisely
the requested bytes. A full read additionally requires all declared bytes;
short reads, changed generations and missing segments never yield successful
empty or partial values. Hash identities establish byte integrity, not
authorship or authenticity.

The native object layout records each managed identity, total length, ordered
chunk digests and segment IDs. Its ordinary table child contains only
descriptors and existing columns. Decode validates versions, counts, checked
arithmetic, segment bounds, exact chunk lengths, identity derivation and child
schema/row consistency before admitting the directory. Duplicate identities
must describe identical content geometry. Directory and lookup capacities
are charged before allocation through the existing session owner.

## Ownership and resources

The resident session owns an explicit object arena, shared with its retained
native results and derived sources. The arena holds object metadata, immutable
managed input owners and held file generations. It contains no process-global
cache. Session closure does not invalidate retained results or readers.
Conservative arena retention may keep an unused imported object until the last
session/result owner drops; its resource charge remains visible throughout.

Importing a caller's file as managed content copies it in bounded chunks to a
fresh private local namespace under an explicitly selected nonsynced directory.
The input generation is checked through completion. Managed intake has an
explicit storage ceiling, checks memory and cancellation before requesting each
chunk, and publishes an object handle only after completion and identity
validation. It never takes ownership of the caller's original file.

The private namespace keeps an exact ledger of created files and generations.
Failure, cancellation and last-owner drop may remove only those unchanged
task-owned files. Unknown entries, substituted paths and external references
are never cleanup targets. No recursive deletion of an unverified directory,
global garbage collector or cross-process mutable object database is added.
Published Vortex outputs are persistent caller-owned artifacts and are never
removed by session cleanup. Deleting one descriptor or dropping one result
cannot reclaim another live owner's bytes.

All reads and writes use the existing shared operation, I/O, cancellation and
memory owners. Returned byte buffers keep their actual allocation credits
until their last view drops. A complete read may be rejected by memory
admission; callers can iterate bounded ranges instead. Range arithmetic
rejects overflow and out-of-bounds requests. A valid empty range still checks
the retained source generation. Chunk, directory and output allocations are
accounted separately; no process-RSS guarantee is implied.

## Shared execution and persistence

Source preparation reads the native footer and object directory without
reading object data. Ordinary metadata projection/filtering and descriptor
carriage do not fetch payload segments. Existing arrays, take/slice operations,
joins, native spill and retained results preserve descriptors and their
session owner. A query-local spill stores descriptors and retains their arena;
it does not copy every large object into every run.

The durable native writer observes the descriptors actually emitted by the
query, resolves their owned content, and writes each distinct selected managed
object once in bounded segments. Unselected managed payloads are not copied.
The layout and payload directory become visible together only through the
existing complete staged-file publication. Source invalidation, missing
content, cancellation, sink failure or integrity failure prevents publication
and reclaims only the uncommitted output.

External local references are created only through an explicit external
reference API. They remain external in saved descriptors and output evidence.
Retrieval validates their recorded generation and exact range; no URL/network
resolution, remote credentials, media decoding or implicit payload import is
performed. Compatibility sinks must either implement a separately explicit
complete-byte conversion or reject binary-object columns with an actionable
diagnostic. Exporting a bare descriptor must never be reported as exporting
its owned bytes.

Python constructors and readers wrap these same native owners. Ordinary SQL
and DataFrame operations use the shared extension and runtime; there is no
Python-only storage or execution path. Row conversion exposes an owned object
handle, while diagnostic output identifies ownership and byte length without
pretending that a descriptor is the payload. Cross-session imports must
transfer actual owners or re-admit a persistent native file, never trust an
unbound numeric handle.

Python's public `Context` supplies `binary_object`, `binary_object_store`,
`external_binary_object`, `import_binary_object` and `binary_object_snapshot`.
The `types.binary_object` declaration maps to the version-1 extension. Handles
expose `byte_length`, `ownership`, `read()` and `read_range(start, end)`; stores
expose `import_file`, `put_bytes` and a `snapshot`. Reads explicitly copy into
caller-owned Python bytes after native buffer admission. A producer callback
must not reenter its active session. Handles collected from a stream remain
retained, but their explicit reads require finishing or closing that stream
before acquiring another operation on the same owner.

Native cross-session transfer copies descriptor arrays and object directories
under the receiving memory grant. Memory-backed bytes also acquire receiving
ownership. Immutable private files share their exact temporary-file cleanup
owner and original storage quota; this is shared backing, not a claim that all
origin credit is released immediately. Escaped Arrow buffers retain the object
arena, including empty slices, without introducing an arena-to-array cycle.

## Alternatives and limits

A sidecar object store or companion bundle would add relocation, atomic
multi-path publication and independent garbage-collection contracts. The
native segment wrapper avoids those obligations. Placing all payloads in
resident user-metadata buffers would violate bounded streaming. Materializing
each large object as an ordinary Binary cell would prevent useful bounded
range reads. Reusing an external query engine would violate the execution
contract.

This delivery is local-only. It does not add remote object stores, an ANN
index, media parsing, tensor support, arbitrary extension execution, a general
object filesystem, cross-process shared caches or package publication.
Existing provider and representation limits remain explicit. No performance
advantage is claimed without a separately authorized comparison.

## Acceptance

Acceptance requires source-linked tests and installed public-interface proof:

- Mixed inline bytes, managed objects, external ranges, empty values and nulls
  survive native SQL/DataFrame projection, filtering, reordering, duplicate
  join matches, retained-result reuse and forced native spill.
- Metadata-only queries and unselected payloads register zero object-data
  reads. Selected reads verify exact bytes across chunk boundaries, nonzero
  offsets, empty ranges and complete retrieval.
- Live readers/results survive source/context drop. Shared references retain
  bytes and credits until the final owner; cancellation and failed intake
  refund reservations and remove only their own uncommitted files.
- Truncated/replaced sources, corrupt chunks/directories, unresolved identities,
  illegal versions/ranges, denied memory/storage and consumer cancellation
  produce deterministic failures without publication or empty placeholders.
- An owned native export reopens independently after source removal and file
  relocation. External references remain visibly external. Incompatible
  sinks reject before publication.
- Native/default workspace tests, strict lint and formatting, relevant minimal
  feature combinations, installed wheel tests, public documentation validators
  and hosted PR/main checks pass. Preserve original failed observations and
  retire superseded generated artifacts after the accepted replacement exists.

The design's main review risks are lifetime retention across spill and Arrow
views, directory admission before allocation, and publication after partial
payload failure. Tests must challenge all three. Implementation or acceptance
changes belong in this document before claims or queue advancement.

## November 6 local acceptance

The implementation passes the finite ownership and persistence contract above
on the local macOS ARM64 native build. All seven installed binary-object tests
pass, including complete 2-MiB payloads reopened in a separate process after
source removal and relocation; duplicate joins, forced spill and retained
stream rows; cross-session transfer; consumer cancellation; unsupported operands;
and failed producers and incompatible sinks that must not publish output.
DataFrame, native SQL, typed plan, embedded workflow and CLI SQL each write an
owned file that reopens with complete bytes after its source is removed.

The Rust controls verify zero object-data reads during preparation, exact reads
across integrity chunks, selected-object deduplication, omission of unselected
payloads, corrupt/truncated directory rejection before admission, lazy corrupt
payload rejection, memory/storage refusal, private-file last-owner cleanup and
escaped Arrow owners including empty slices. Native spill retains descriptors
and their arena; it does not independently publish descriptor-only artifacts.

| Verification | Result |
| --- | --- |
| Native workspace, all targets | 6,937 passed; 36 existing opt-in tests ignored |
| Default workspace, all targets | 3,319 passed; none ignored |
| Final focused binary-object tests | 26 passed; none ignored |
| Native and default strict Clippy; formatting | Passed |
| Seven-stage feature matrix and corrected write-only feature gate | Passed |
| Generated API, exports, user/status/governance/license checks and schema tests | Passed |
| Clean installed Python/CLI package | 319 passed; no failures, errors or skips; uninstall passed |

Full native/default suites precede one final `cfg` correction on the internal
`payload_storage` helper. Source-hash verification proves that attribute is the
only Rust/manifest difference from those suites. Final native lint, focused
tests and the complete installed suite cover the corrected build. The minimal
write-only check confirms the new unused-helper warning is removed; warnings
from unchanged minimal-feature sources remain in the original feature log.

The [compact packet](../release/evidence/native-managed-binary-objects-2026-11-06.tar.xz)
preserves original compile/lint/test failures, the deliberately interrupted
first release build, their corrections, source snapshots and final runtime/wheel
hashes. No failed observation is relabeled as a pass.

This accepts local managed/external byte ownership and single-file managed
export in native Unix sessions. External references still require their original
file generation. Conservative arena retention, explicit Python byte copies and
shared private-file quota ownership remain documented limits. Compatibility
files reject object descriptors. Tensor metadata, other representations, vector
expressions and resource-backed traversal remain separate item 19 work. No
benchmark, performance claim, package publication or independent CG-gate closure
follows from this acceptance.

## Hosted surface correction

The first PR #1659 Python check found that the five new Context methods lacked
entries in the static user-surface graduation matrix. Their new row explicitly
requires the native binary-object feature and Unix session, and distinguishes
explicit object operations from inert capability inspection. The generated API
source fingerprints are refreshed with that metadata change. An intervening
local check caught those stale fingerprints; its failed result remains saved.

The corrected core Python shard passes, reporting 833 tests and 47 existing
opt-in/environment skip records. The rebuilt wheel passes all 319 installed
integration tests without skips and passes uninstall. Its packaged context
module matches the validated source. Native Rust source, CLI and library bytes
are unchanged from the original acceptance; no native rebuild or benchmark
was needed for the metadata correction.

The [follow-up packet](../release/evidence/native-managed-binary-surface-followup-2026-11-06.tar.xz)
also retains the original hosted minimum-Rust-version download failure: a
registry connection reset occurred before compilation. GitHub refused a retry
while the workflow was active; the corrected revision receives new hosted
checks. This network observation and the metadata failure are separate, and
neither is counted as a passing hosted check.
