# Full-domain timestamp statistics and compression

Status: implementation in progress under PERF-07/08/11/12 and CG-19/20/21.
This follows the exact typed-input contract and the October 10–11 maintainer
direction. Native Python/resource integration retains its separate tested
commit and hosted acceptance. Version 0.5.1 and package publication are unchanged.

## Contract

Preserve every Int64 value of the already admitted timezone-free microsecond
timestamp type, including both endpoints, negative values, nulls and timestamps
inside lists, fixed-size lists and nullable structs. Restore useful native
compression and correct statistics without changing the logical type, silently
narrowing its range or introducing an intermediate Arrow representation.

The expert acceptance bar is a native columnar format maintainer's: exact scalar
and array semantics, complete file reopen, conservative pruning, preserved
validity and metadata, bounded resource ownership and reproducible writer costs.
Smaller files alone do not establish faster complete workflows.

The first paired public writer cohort preserves every value in all 84 samples,
but its nested nullable case exposes a read-cost regression. A frozen-file
crossover using the same candidate executable reproduces it: median native
collection is 1.253 seconds for the earlier layout and 1.438 seconds for the
compressed layout. Sampling identifies the existing compact-result builder's
per-value dictionary/selection traversal as the dominant cost. This is a result
ownership boundary already copying selected payloads, not a reason to decode
intermediate native operations or to disable timestamp compression wholesale.

Reuse the existing `NativeNumericOwner` after selection for numeric and temporal
leaves in that shared builder. Execute each selected native storage block and
its validity once, then construct the existing allocator-owned output with its
original dtype. Keep decimal validation and unobserved floating-point movement
on their existing paths. The change must retain hidden-parent null semantics,
selection exactness, resource denial/cancellation and owner lifetimes. Retain it
only if complete nested collection improves by at least 10% against the frozen
candidate, with exact results and no material regression in the flat controls.
The original cohort remains evidence of the rejected first candidate; refreshed
complete writer workflows decide the final compression cost/storage tradeoff.

## Provider decision and provenance

Vortex-first decision: `use_vortex_native_provider`. Retain the pinned Vortex
0.85.0 DType, ExtensionArray, native compressor, standard file writer and
statistics/pruning interfaces inside the existing ShardLoom boundary. Do not
introduce a second temporal type, statistics format, file writer or query engine.

The published `vortex-array` 0.85.0 timestamp scalar constructor calls Jiff's
infallible span constructor before its checked calendar conversion. A valid
Int64 microsecond endpoint can therefore panic before a diagnostic is returned.
The scalar display path repeats that calendar conversion. The current upstream
[timestamp implementation](https://github.com/vortex-data/vortex/blob/develop/vortex-array/src/extension/datetime/timestamp.rs)
still has this calendar validation. A provider upgrade is not evidence of a fix.

Use a focused, Apache-2.0-reviewed patch of the existing `vortex-array` package.
The authoritative published archive has SHA-256
`e9cf8cf4f7f2fe72b14866ead164a6366933545289fd8f9cf9ce53c47f094393`:
759 files and 6,124,741 uncompressed bytes. Its VCS metadata names
`afb005379dd9a1b7dc7ae2cb49f4f465d91cc2b3` with a dirty marker, so that Git
revision is not a substitute for the archive identity. Preserve original file
hashes, license text and an explicit method-level change description beside
the patch. The dependency graph keeps the same package version and features.

The correction admits the existing timezone-free microsecond storage domain in
scalar validation and unpacking. Ordinary calendar display stays compatible;
values outside the display calendar use exact epoch microseconds. Other temporal
units, timezone handling, storage-type validation and unsupported metadata retain
their existing policy. Do not bypass scalar validation with unsafe construction.

The encoded comparison acceptance also requires a focused patch of the existing
`vortex-datetime-parts` 0.85.0 provider. Its timestamp validity belongs to the days
child. Combining equality/inequality of non-null seconds and subseconds through
Kleene Boolean operations can turn a null timestamp comparison into false or
true. Preserve the existing component comparisons and reapply the days validity
to their handled results through the native mask operation. Test whole-array
execution and actual filtered files; scalar-at-a-time evaluation can bypass this
kernel. The dependency review records the authoritative 21-file archive, license
and original member hashes. No temporal encoding or file-format identity changes.

## Shared writer integration

Once scalar construction, min/max aggregation and metadata reopen are verified,
remove the global min/max omission for these timestamps. Use the standard writer's
field statistics, with exact bounds only when the provider proves them. Empty and
all-null inputs do not acquire invented extrema. Ordinary columns in the same
file regain their own valid statistics.

Use existing native compression and layout selection, retaining the same dtype,
extension metadata and validity. Check both ordinary storage compression and
the provider's temporal decomposition at endpoints before selecting the writer
route. Nested validity preservation remains mandatory. Compression tasks use the
existing session and explicit concurrency; no independent pool or hardware default
is admitted. Existing uninstrumented provider scratch and process-RSS exclusions
remain explicit rather than being relabeled as covered allocations.

Existing writer certificates must identify the applied compression and statistics
policy. Readers using the unpatched provider may still have its calendar defect;
this change cannot claim to repair those external readers. Vortex remains native
input/output, with `fallback_attempted=false` and `external_engine_invoked=false`.

## Acceptance and verification

- Prove scalar construction, storage identity, ordering, safe display and
  metadata serialization at endpoints and calendar boundaries. Preserve invalid
  storage/type/timezone rejection outside the changed domain.
- Exercise root, flat and nested timestamps, empty/all-null/nullable input,
  repeated constants, narrow ranges and mixed-sign endpoints through all existing
  writer compositions and array/stream/retained/column-layout routes.
- Reopen every value and dtype; inspect exact file statistics for timestamps and
  neighboring fields. Compare filtered results with complete unfiltered values for
  no-match, all-match and overlapping bounds, null predicates and missing statistics.
- Verify tiny-grant denial, retained-owner competition, cancellation, output
  preservation and reuse through existing resource and writer contracts.
- Record paired complete writer/reopen observations on frozen sources, including
  file sizes and compression policy. Retain a compression strategy only when its
  measured costs justify it; preserve correctness and restored statistics either way.
- Run required formatting, strict workspace lint and workspace tests, the native
  feature/profile gates, complete public regression and final Full43 acceptance
  for the cohesive engine change. Keep the original native-interface timings
  attached to their original source.

This closes the timestamp follow-up only. The six remaining areas, eight
investigations and CG-1 through CG-23 retain their separate obligations.
