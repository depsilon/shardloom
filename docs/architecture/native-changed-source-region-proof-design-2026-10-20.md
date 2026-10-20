<!-- SPDX-License-Identifier: Apache-2.0 -->

# Changed-source reuse: bounded file-region proof design

Status: test-only prerequisite design for `PERF-02.changed_source`. No candidate
timing or public changed-generation admission is authorized by this document.
The [provider intake](native-changed-source-reuse-intake-2026-10-20.md) records
both failed arbitrary-range attempts and the subsequent whole-batch restriction.
The conditional sequence, RFC 0044 and all CG-1 through CG-23 remain in force.

## Scope and provider

Use a bounded directory of local Parquet files, each containing one complete
source batch. The initial fixture uses the provider probe's nullable `number`
and nullable UTF-8 `text` fields, with exact logical schema, empty strings,
Unicode, NUL, long values and repeated values. Input is an explicit compatibility
boundary; conversion and execution remain native ShardLoom/Vortex. No external
engine, new dependency, sidecar data file or custom Vortex encoding is added.

The active vendored Vortex 0.85.0 file/layout/array APIs supply ordered scans,
native arrays, native metadata and writers. ShardLoom's current
`BoundedIngestLayout` preserves original source-batch boundaries; the Parquet
adapter supplies held `SourceIdentity` guards and row-count metadata. Use the
standard source reader, owned Arrow-buffer conversion and normal writer strategy
for every new or changed file. Reused arrays retain their encoded payloads after
the explicitly checked Shared-wrapper operation. Partial-batch Slice wrappers
remain a refusal. Do not expand a requested region or silently decode it.

## Trust and identity

The initial preparation must establish all of the following together:

1. A sorted inventory of regular, nonsymlink source files within one explicit
   local directory. Bound the file count and metadata before allocation. The
   dataset order is file order followed by each file's source order.
2. A held `SourceIdentity` for each file, full content SHA-256 read through its
   checked descriptor, exact schema and source-batch boundaries. Revalidate
   before/after hashing, reading and final publication. Re-enumerate membership
   before publication; a held file descriptor alone cannot detect a new member.
3. A complete contiguous row mapping for every file, including empty files.
   Each entry records its relative file identity, content digest, byte length,
   row interval and batch boundaries. Validate uniqueness, checked arithmetic,
   complete coverage and agreement with the written native row count.
4. Exact engine/provider/recipe, format, schema and transform identity. The
   initial scoped transform is identity, with no row deletions inside a file and
   no external dictionary domain. Dictionary payloads belong to the recorded
   previous artifact and region; they are not process-local reusable tables.

Embed this bounded map in the existing prepared-source binding's caller-supplied
fingerprint field, with an explicit test-only version marker. The surrounding
binding is constructed and validated by `prepared_source_binding` as usual. Its
generation digest is still a hash of generation records; the map's individual
SHA-256 values are content hashes. Existing CLI fingerprint policy is unchanged.
The complete embedded binding retains its existing 64-KiB limit.

The previous artifact's trusted digest comes from successful initial publication,
not from a digest claimed by its own metadata. Keep that receipt in the owning
test workflow. Before reuse, hold the previous artifact's generation, hash its
complete bytes and compare them with the receipt. Then read only the bounded
known metadata segment and validate its complete map and compatibility. This
does not authenticate an arbitrary caller-supplied artifact or provenance claim.
A diagnostic receipt passed by the guarded parent to a fresh child is test
evidence, not a new public manifest or authentication mechanism.

## Refresh boundary

Hash and validate the entire current source inventory. Reuse a prior file's row
interval only when its file identity, full content digest and all compatibility
keys match. A newly added or changed file uses ordinary preparation. A removed
file contributes no rows. Exact content at a new file identity is prepared
normally in this first design; no cross-file deduplication is assumed.

The new map is rebuilt in current source order and embedded in one complete new
Vortex artifact. Reused arrays still incur previous-file validation, native
reads, serialization, copied bytes, file statistics, metadata and publication.
Do not call this raw segment copying or zero-decode writing: native statistics
and downstream consumers may execute arrays. Recompute statistics for the new
artifact; old whole-file summaries cannot describe changed membership.

Both routes use the current bounded native writer/publication contract and a
single shared native memory owner. Mixed writing must select the ordinary or
preserved child explicitly for each admitted input batch, reject a missing or
extra selection, and retain the original ordered bounded handoff. Charge the
map, hashing scratch, input descriptors, layout references, imported/native
payloads, statistics, staging buffer and retained result through their existing
owners where supported. State source-reader and allocator-bypassing allocation
limits separately. A failed reservation is a deterministic refusal.

All sources and the old artifact are revalidated before commit. Source mutation,
membership change, incompatible schema/recipe, corrupt old artifact, untrusted
or incomplete mapping, partial-batch wrappers, cancellation and resource denial
must stop before publishing a refreshed result. No full-rebuild recovery is
implicitly substituted after reuse starts. This first design writes only a new
target; required generation-conditional replacement remains `PERF-07.publication`.

## Prerequisite verification before performance admission

Prove ordinary partition-directory preparation through real adapters and the
standard native writer first. The existing CSV directory test and single-file
Parquet/IPC tests do not prove that composed columnar-directory workflow.
Record source batch boundaries, ordinary conversion/compression observations
and complete native outputs on the current build before admitting a timing
candidate. Historical full-source stage observations establish an earlier cost
signal; they do not measure this new shape or refresh savings.

The ordinary-preparation observation is frozen separately from a future
candidate: sixteen lexically ordered Parquet files, each with 8,192 rows; one
driver and a shared 256-MiB native grant; three new-target preparations of the
same immutable source inventory. Use the provider probe's independent value
formula over global row ordinals: nullable `row - 9000` integers (NULL every
17th row), and an eleven-way text cycle containing NULL, empty, Unicode/NUL,
one row-specific long string and repeated categories. Fixture generation is
outside preparation. Record full source/output digests, source-batch work counts,
all ordinary preparation fields and separate complete-operation diagnostics.
Each result is closed, moved and reopened. Verify all 131,072 ordered rows in
sixteen contiguous bounded native projection calls, then an exact filtered
COUNT/SUM twice. Record engine clocks from those calls without relabeling
preparation or validation as engine execution. All values, schema, source
identity, credit release and cleanup must pass. There is no scored speedup or
retention threshold in this baseline-only observation, and its repeats do not
establish cold-cache behavior.

The identity proof must cover unchanged files; changed, added and removed files;
empty and all-null files; same-size replacement with restored modification time;
mutation during hashing/reading and before commit; changed directory membership;
wrong or duplicate mappings; gaps/overlaps/row overflow; schema, format, provider,
recipe and transform mismatches; malformed/oversized metadata; corrupt/truncated
previous artifacts; missing trusted digest; cancellation; and native allocation
denial. Preserve original files and any existing destination on every refusal,
with no staged-file leak and no live native credit after the last owner drops.

Check exact complete ordered values, dtype/nullability and statistics against
an independently generated oracle. Observe actual unchanged-region activation
and new-file ordinary preparation. Close producers, exit their process, move
the completed artifact and consume it from a fresh process; then write and reopen
another native artifact. Exercise real ShardLoom prepared aggregate consumers
with first/repeated execution and collect their engine clocks separately from
preparation, validation and serialization. A provider scan alone is insufficient.

After these prerequisites pass, freeze a finite workload matrix, source/build
identities, role order, samples, controls and retain/drop thresholds. Charge
initial proof creation, complete refresh versus correct rebuild, first/repeated
consumption and cleanup. Candidate-only content hashing is not imposed on the
ordinary baseline. Preserve both failed provider attempts and all subsequent
negative results. No performance or broader capability claim follows from this
design or a passing prerequisite.

### Public writer recipe prerequisite

The initial component writer used the no-advisor provider strategy. Its ten
identity/writer checks and four-process lifecycle pass, including 36 real native
consumer calls and a second write, but that does not prove public recipe parity.
The ordinary public preparation observation applies the admitted small-source
strategy with 4,096-row blocks; the no-advisor provider builder defaults to 8,192.
All sixteen initial Parquet inputs in those observations have identical complete
content hashes. The different strategy is therefore an actual prerequisite gap.

Before any performance comparison, revise the component's ordinary child to use
the normal admitted small-source columnar advisor and its actual writer-decision
function. Fix P1, the same two-field schema and the same bounded source inventory.
Record the applied decision and assert its row block, compression, statistics and
parallelism fields against the ordinary observation. Change the embedded recipe
identity; do not reuse artifacts from the earlier no-advisor probe. This is a
writer-child equivalence check, not proof of complete CLI preparation equivalence.

Repeat the unchanged ten tests and four-process 16-file lifecycle with that
recipe. Whole-file regions must still produce a single serializable native
array; a recipe-induced split or unsupported wrapper is a prerequisite refusal.
Do not enlarge regions, canonicalize slices, or change the 8,192-row input files
to force activation. Preserve any refusal and its exact source/build. Performance
admission still requires the complete public preparation/refresh cost boundary.
