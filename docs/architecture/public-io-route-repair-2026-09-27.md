<!-- SPDX-License-Identifier: Apache-2.0 -->

# Public I/O route integration repair

Status: implementation in progress, authorized during the September 27 format
baseline. This belongs to existing PERF-12/public-call and CG-21 workflow work.
The 0.3.1 publication/deployment train is complete. This note does not reopen the
seven queued performance experiments or claim a new published release.

## Observed failures

The released CLI collects ClickBench queries over native Vortex, but its public
SQL export admission still requires structured expression projections even where
the native aggregate result already owns complete typed columns. Plain COUNT(*)
also explicitly rejects write requests during SQL lowering. The native aggregate
writer already accepts owned results for a narrower set of COUNT/exact-DISTINCT
shapes in Vortex, Parquet and Arrow IPC.

Sort collection and text export share the existing sort/Top-K implementation.
Its binary-result handoff needs separate verification; availability of the shared
format writer alone does not establish that this producer is connected to it.

The public compatibility-input route prepares a deterministic local Vortex path.
A second Parquet query currently fails because that path exists and overwrite is
disabled. Reusing an artifact needs a source/artifact identity proof, not merely a
matching filename. Arbitrary user files must never be overwritten to repair this.

The captured pre-fix baseline is under
`/Users/dylan/LocalData/shardloom/clickbench-100m-uat/logs/io_pulse_20260927T154640181654Z`.
It contains actual CLI failures, not inferred capability gaps. Its single-sample
schedule was stopped for these repairs; completed calls remain evidence.

## Shared-boundary contract

- Keep SQL/Python/DataFrame/CLI lowering on the existing Vortex-native execution
  families and preserve their optimized kernels, pruning and late materialization.
- Feed complete typed native results into the existing sink providers. Avoid
  serialized JSON reconstruction as the binary export substrate. Each writer
  consumes its completed native result without executing the query again.
  Independent CLI calls still execute independently; existing text fanout retains
  its previous execution behavior. This repair does not add single-execution fanout.
- Check all exposed formats, including existing text and other compatibility
  options. Preserve explicit provider type limitations, nullability, empty schemas,
  exact numeric values, row order, resource ownership and atomic output publication.
- Bind any automatic preparation reuse to unchanged source and prepared artifact
  generations. Reject stale/unbound state and preserve unrelated user outputs.
  Generation checks detect local changes; they are not cryptographic artifact
  authentication.
- Keep all execution native. No external query-engine dependency or fallback.

Vortex-first classification: `use_vortex_native_provider`. Existing approved
Vortex 0.85.0 arrays, scans and writer, `OwnedVortexResultBatch`, `NativeSinkPlan`,
and compatibility boundary writers are the preferred integration points. Provider
APIs remain inside `shardloom-vortex`; this work does not add dependencies or a
parallel query implementation. RFCs 0010, 0012, 0013, 0014, 0017, 0031 and 0033
govern diagnostics, ownership, preparation, publication and the user workflow.

## Acceptance

Use small regressions for repeated preparation, invalidation, typed aggregate and
sorted-result exports, nullable/empty outputs, exact large integers, existing
target preservation, feature gates and every exposed format's admission boundary.
Run applicable Rust and public-call checks sequentially. Resume the requested
ClickBench I/O baseline with one sample per remaining case, rechecking changed
routes as needed for correctness. Preserve evidence, retire task-owned duplicate
artifacts after each complete input lane: retire its task-owned preparation cache
or generated plain-Vortex fixture after preserving timings, result validation,
identities and small output archives. Keep the original Parquet and protected
optimized Vortex references.

CSV, JSON and JSONL inputs/outputs receive setup and small correctness coverage
only. The maintainer declined additional text performance testing after the
full-size JSON space estimate exceeded free storage. Do not generate large text
fixtures, repeat the baseline, or run tests/builds/benchmarks concurrently.

## Implementation boundaries

The existing specialized owned aggregate finalizers remain in place. Other
admitted flat aggregates and sorted results bind completed native scalar values
to their declared schema before constructing Vortex arrays. This avoids a
serialized JSON round trip and source replay, but still has scalar-row and builder
materialization costs. General owned output is limited to 65,536 rows, 128 fields
and 8 MiB; explicit aggregate spill output and nested/extension result types are
not newly admitted. Existing SUM/AVG floating accumulation semantics are unchanged.

Binary writers share native result ownership and bounded Arrow batches at the
compatibility boundary. ORC uses checked signed widening and rejects UInt64 values
above Int64's maximum; Avro also cannot represent that unsigned range. Format
reports expose width, nullability and physical-metadata losses. Vortex remains the
native persistence target. JSON arrays frame the existing JSONL renderer's owned
temporary spool, with bounded per-row memory and cleanup on success or failure.
Generated-source writers also expose JSON arrays through their existing bounded
row renderer. CSV input now reads logical records across embedded LF/CRLF inside
quoted fields, sharing the record reader across schema inference, streaming and
the small decoded reference boundary. Each CSV record has an 8 MiB admission cap.

Ordinary local Python writes forward the source and SQL to the common public CLI
route, which owns preparation/reuse and native admission. Existing specialized
provider scenarios retain their Vortex/JSONL/CSV sink scope; this repair is not a
claim that every operator has every sink or that arbitrary SQL is supported.

Metadata-first execution, existing encoded aggregate kernels, Top-K and late
payload materialization are reused. The change is producer/sink integration and
preparation lifecycle repair, not a new PulseWeave scheduler or performance result.

## Validation so far

The real Python-to-CLI round-trip matrix passes 110 small cases across the eight
formats, including scalar/mixed/grouped aggregates, sorting, empty results,
DataFrame writes, generated JSON and existing-target preservation. Its temporary
fixtures were removed. Evidence:
`/Users/dylan/LocalData/shardloom/format-io-20260927/public-io-correctness.json`.

The default workspace suite passes 3,428 tests. The release-feature workspace
suite passes 4,823 tests (22 ignored); subsequent CLI regressions pass all 914
tests. Default and release-feature Clippy pass with warnings denied. The
optimized CLI build succeeds. The one-time full-size I/O baseline and final PR
acceptance remain pending; these checks do not establish performance gains.
