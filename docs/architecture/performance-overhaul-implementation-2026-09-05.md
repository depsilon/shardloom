# Performance Overhaul Implementation Evidence

## Reference State

The maintainer requested implementation of the supplied performance overhaul on
2026-09-05. PR #1429 (homepage productionization) is merged at `c6cda693`, the
starting point for this work. PR #1428 already removed dual exact-histogram/sketch
maintenance. That improvement must not be represented as new implementation.

This record distinguishes executable work from the thirteen complete PERF
packets. RFC 0044 and the active phased plan remain the acceptance contract.
The structured evidence bundle is
[`performance-overhaul-foundation-2026-09-05.json`](../benchmarks/performance-overhaul-foundation-2026-09-05.json).
It preserves all four full43 timing matrices, result digests, ingest summaries,
the independent Q20 reference, and prepared-count latency.

## Implemented Boundaries

- `LiveMemoryPool`: shared live/peak reservations, checked growth, ownership
  transfer, denial, contention, and drop/unwind release. This is not a process
  allocator or a claim that every upstream allocation is charged.
- `ComputePool`: reusable dynamically assigned workers, bounded queue slots and
  bytes, cooperative cancellation, panic containment, and owner shutdown/drain.
- Streaming writer prefetch: bounds queued, active, and completed out-of-order
  work together. A batch's reservation survives completion until writer handoff.
  Reader-internal allocations and upstream writer/codec buffers are outside that
  reservation scope. Oversized admitted batches fail explicitly.
- Parquet input batch sizing: uses existing row-group uncompressed-size metadata,
  a schema-width floor, and expansion headroom to lower decode batch rows under
  smaller caller memory budgets before allocation. The CLI shares one eighth of
  requested memory across requested readers for this estimate. Complete-source
  row admission is separate; actual post-decode byte checks are retained. Estimates
  do not prove a bound on dictionary expansion, large single values, or RSS.
- `ReservedHostAllocator`: uses Vortex 0.85's safe host-allocation hook and
  lifetime owners. Buffer clones and slices retain the full allocation credit.
- `ResidentVortexSession`: retains provider registries, runtime workers, an open
  OS file, Vortex reader metadata, and bound count/projection operations. Device,
  inode, size, modification time, and change time are checked against both the
  retained handle and pathname. Replacement, mutation, truncation, and recreation
  invalidate the prepared source. Concurrent calls serialize at admission.
- `OwnedVortexResultBatch`: actual executable native arrays, not an opaque report
  descriptor. Owned results and independently retained arrays outlive the session.
- Single-file public projection/filter collect: exact admitted native scan,
  source-order limit, complete scalar values, native I/O certificate, and an
  explicit JSON materialization boundary. Limit: 65,536 rows and 8 MiB JSON.
  Larger outputs fail rather than silently returning a truncated preview.
  Unsupported residuals/types fail explicitly; streaming exports retain their
  separate existing route. `zero_decode` cannot request JSON row rendering.
- Ingest inventory uses the logical DType display instead of Debug output that
  recursively dumps runtime registries. The field remains a schema summary;
  inventory digests intentionally no longer incorporate debug registry ordering.
- OLAP publication performs one complete artifact fingerprint instead of computing
  and discarding a first fingerprint before computing the same fingerprint again.
  The remaining publication checksum is not replaced with a caller-provided digest.
  `prepared_olap_publication_millis` reports the finalization span separately from
  `prepare_once_millis`; total ingest timing still includes both.

The CLI metadata-count route uses the same native prepared-count implementation,
but a fresh CLI invocation still creates a fresh session. Persistent Rust count
latency must not be reported as Python or fresh-process latency.

## Benchmark Integrity

The historical full43 run checked successful statuses and fallback fields. Its
Q20 payload was `projected_columns=UserID rows=4`, not four returned values.
The new runner rejects descriptors and truncated previews, compares complete
typed values, preserves integer precision, and records binary/query hashes,
resource settings, cache policy, timing boundaries, and source generation.

Q20's independent reference scans the official Parquet `UserID` column through
Arrow's typed Int64 API: 99,997,497 input rows and four exact matches of
435090932899640449. It does not execute a query in another engine. Other query
comparisons against retained outputs are regression checks, not independent
correctness oracles. The baseline-only descriptor allowance marks full-result
validation false and cannot be used to certify the candidate's Q20 output.

Both large-data runners use one exclusive workspace lock and local-only storage
admission. Native process duration is recorded independently of watchdog polling.
The ingest timing includes complete native output and process exit. OS page cache
is uncontrolled; no answer cache is used. These are not official ClickBench ranks.

## Same-Commit Measurement

Host: macOS 26.5.1 arm64, ten logical CPUs, 16 GiB physical RAM. The inherited UAT
configuration requests 24 GB memory and twelve query workers; these settings do
not mean the machine has 24 GB RAM or twelve cores. Ingest uses two requested
workers. Source: 99,997,497 rows in the official Parquet file.

Initial matched ingest measurements, before removal of the duplicate publication
fingerprint:

| Measurement | Merged `c6cda693` | Resident foundation |
| --- | ---: | ---: |
| Complete native process | 177.619641 s | 176.740326 s |
| Reported prepare span | 141.989 s | 142.745 s |
| Artifact bytes | 38,147,848,068 | 38,147,848,068 |
| Diagnostic stdout bytes | 16,289,730 | 155,612 |

One pair does not establish an ingest throughput improvement. Diagnostic output
is about 99.04% smaller. Both artifacts have SHA256
`6777eb4deea57cea7d83e772b3af4db2ebd77f003c38c1997ee0aadf02071c97`.
The roughly 34-36 seconds outside the reported prepare span prompted the duplicate
fingerprinting investigation; it must not be omitted from the ingest measurement.
Raw logs: `ingest_cli_uat_gated_20260905T133434Z` and
`ingest_cli_uat_gated_20260905T134828Z` under the local UAT log root.

After removing the unused publication fingerprint, the complete native ingest
took **159.331915 s**, versus **177.619641 s** on the merged baseline (18.287725 s,
10.30% less time in this comparison). Its prepare span was 141.285 s and the newly
measured publication span was 16.966 s; the complete process clock includes both
plus the remaining 1.081 s. The writer-stream digest and publication-read digest
both match the baseline SHA256 above, with identical row count and artifact bytes.
No validation or first-query work was deferred. This is a measured local sample,
not a confidence interval or a gain relative to the historical 271 s figure.
Raw run: `ingest_cli_uat_gated_20260905T141342Z`.

The first 4 GB configuration test, `ingest_cli_uat_gated_20260905T141737Z`, failed
explicitly at batch 215: 561,141,176 decoded input bytes exceeded the 512 MiB
conversion-input headroom of its 1 GiB prefetch slot. No artifact was published.
OS-reported process peak RSS was 7,635,533,824 bytes, so this was not a 4 GB
process-memory success. This evidence motivated metadata-aware Parquet batch
sizing; global upstream codec/source memory admission remains open even if a
smaller-batch rerun completes. No oversized-batch check was weakened or removed.

The metadata-sized 4 GB configuration then completed all 99,997,497 input rows
in 163.234471 s (`ingest_cli_uat_gated_20260905T142817Z`). It selected 32,768-row
source batches (3,120 observed batches), produced 37,965,397,844 artifact bytes,
and writer/publication checksums both equal
`3e5565a7c0b273a54b05cb13d324302598918eeae81409c344a6e1cea78bcfd5`.
Its OS process peak was **9,869,230,080 bytes**, about 9.19 GiB. Therefore the
source-admission repair passes, but the process-wide 4 GiB acceptance **fails**.
Do not label this a successful bounded-memory engine run. Source batching changed
the physical layout, so its query results/timings require their own complete UAT.

The final runtime was then rerun with the original 24 GB/two-worker ingest settings:
**161.489375 s**, 16.130266 s or **9.08% less** than the merged baseline sample.
The prepare span was 144.402 s and publication was 16.957 s. Peak RSS was
9,752,756,224 bytes. It retained 423 batches and the original 38,147,848,068-byte
artifact; writer and publication checksums both match the baseline exactly.
This verifies that metadata-aware batch sizing does not change the original
benchmark layout at these settings. The 159.332 s measurement above is an earlier
binary, not the final-code result. Raw run: `ingest_cli_uat_gated_20260905T144256Z`.

Initial full-query runs:

| Measurement | Merged baseline | Resident foundation |
| --- | ---: | ---: |
| Best-of-three total | 142.562774 s | 144.935266 s |
| Hot total | 142.651350 s | 145.166446 s |
| Geomean | 1.021849 s | 1.026966 s |
| All 129 raw seconds | 434.078413 s | 443.538388 s |
| Successful runs | 129/129 | 129/129 |
| Complete returned-value validation | 42/43 queries | 43/43 queries |

This is not a demonstrated query-speed win. The baseline exception is Q20's
descriptor-only output. Candidate Q20 passes the independent four-value reference.
Raw runs: `full43_20260905T134021077065Z` and
`full43_20260905T135149334231Z`. The subsequent merged-binary control,
`full43_20260905T135946103020Z`, took 150.033029 s best-of-three, 151.337556 s hot,
and 458.408100 s across all runs, with geomean 1.063424 s and the same 129/129
successful statuses/Q20 descriptor limitation. It used the byte-identical
candidate artifact. Candidate timing falls between the two baseline measurements;
neither a query improvement nor a regression is established by these samples.
The host was not isolated: a spot check observed foreground browser/window-server
CPU activity. No unrelated applications were stopped and no compiler overlapped
the query runs.

Final-runtime full43 on the smaller, 4 GB-configuration-ingested layout:

| Measurement | Result |
| --- | ---: |
| Best-of-three total | 145.533220 s |
| Hot total | 145.663363 s |
| Geomean | 1.126673 s |
| Hot geomean | 1.127836 s |
| All 129 raw seconds | 442.408496 s |
| Complete returned-value validation | 43/43 queries, 129/129 runs |

Raw run: `full43_20260905T143259007439Z`. Q23 fell from 4.534118 s in the
original-layout candidate run to 3.530769 s, but small-query overhead increased:
Q01 rose from 0.016204 s to 0.035987 s. The larger geomean is not a ranking win.
Keep the original layout for the benchmark envelope; retain smaller input batches
as a source-admission repair, not as a general layout optimization or a 4 GiB
process-memory success. The final runtime was full43-tested on the smaller
layout; its subsequently generated original-layout artifact is byte-identical
to the artifact covered by the earlier original-layout full43 runs. Another
final-binary/original-layout full43 was not run.

The prepared Rust metadata-count experiment ran 10,000 iterations with p50
2.875 us, p95 3.875 us, and p99 4.500 us. Preparation took 8.770 ms; one source
reader was opened and 10,001 operations completed including warmup. Provider
reserved-memory peak was 1,538,992 bytes. This measures metadata-only prepared
Rust execution, not Python calls, general SQL, ingestion, or mixed workloads.
Evidence: `performance_overhaul_gates_20260905/resident-latency.json`.

## Test Evidence and Existing Debt

Evidence root on the development host:
`/Users/dylan/LocalData/shardloom/clickbench-100m-uat/logs/performance_overhaul_gates_20260905`.

Default workspace tests and clippy passed during implementation. Focused tests
cover reservation contention, worker reuse/cancellation, output lifetimes,
source mutation, ordered prefetch pressure, and actual collected values. The
Python storage/query harness suite passes 18 tests, including interruption,
runaway output, source-path safety, exact-value comparison, timeout cleanup, and
preserving source/backup/lookalike files during exact-target replacement.

Final Rust validation passed:

- `cargo fmt --all -- --check`
- `cargo clippy --workspace --all-targets -- -D warnings`
- `cargo test --workspace --all-targets`
- `cargo clippy -p shardloom-vortex -p shardloom-cli -p shardloom-exec --all-targets --features shardloom-cli/release-user-surfaces -- -D warnings`
- 47 public workflow integration tests, focused native result/ownership/prefetch
  tests, two metadata-sized Parquet reader tests, and two publication-fingerprint
  tests. Detailed command transcripts are under the evidence root.

Expanded feature tests exposed pre-existing failures, reproduced independently
in a clean detached worktree at `c6cda693`:

- Eight CLI ingest integration failures concerning old scalar-row vs streaming
  layouts, nested-input quarantine, automatic refinement, and reopen evidence.
  See `baseline-native-ingest-tests.log`; baseline result: 164 pass, eight fail.
- Fourteen native Vortex unit failures concerning older residual-vs-pushdown
  expectations, domain-aggregate labels, digest algorithms, and prepared-state
  evidence. See `baseline-vortex-native-tests.log`; baseline result: 1,284 pass,
  fourteen fail, one ignored.
- One CLI unit test expected a compression decision for a default nested writer
  without an admitted layout advisor. Reproduced on the baseline, then corrected
  to expect zero applied decisions while retaining output-value/reopen checks.

These failures describe the initial foundation snapshot, not the final release
candidate. The release-validation follow-up repairs the stale expectations while
retaining full output, overwrite-permission, and no-fallback assertions. Native
Vortex now passes 1,317 tests with one intentionally ignored fixture-regeneration
helper; native CLI passes 881 unit, 47 public-workflow, and 172 ingest tests.
CI and the local release aggregate now execute these native-feature suites and
native feature clippy instead of relying on compilation alone.

The expanded checks also found real integration defects: JSON text-stream
evidence did not reflect inferred types, empty text streams lost declared scalar
types, and Python collect still requested zero-decode while the JSON sink requires
bounded materialization. The shared schema inference and SQL/DataFrame adapters
now preserve these contracts. SQL automatic preparation refreshes its internally
owned artifact, matching the existing DataFrame policy; explicit output targets
still require overwrite permission. Release transcripts retain usable build-cache
symlink references without relaxing cleanup or checksum verification.

The latest clean original-layout ingest and 43-query/129-run evidence is
`docs/benchmarks/clickbench-current-state-2026-09-05.md`: 162.781 s native ingest,
148.349 s query total, 1.044 s geomean. This replaces neither the historical
145.130 s baseline nor the separate smaller-layout experiment above. It does not
establish an overall query-speed gain. Package publication remains gated by the
release rehearsal, CI, and fresh selected-channel proofs.

## Retention and Local Cleanup

Retain the demonstrated duplicate-checksum removal, compact schema evidence,
native result correctness repair, and tested runtime ownership foundation. Do not
claim the global scheduler, aggregate migration, bounded-memory writer, or Python
resident API is complete. The smaller-batch layout is not promoted as a benchmark
performance win. Its slower geomean and failed memory acceptance remain evidence
for the next resource-governor work, not hidden exceptions to a completed phase.

The owned baseline worktree and redundant generated Vortex artifacts are removed
after their measurements. Keep the final original-layout artifact, official
source, raw logs, frozen baseline/candidate binaries, and the compact repository
evidence bundle. Regenerating the smaller layout requires its recorded ingest
command. No CloudDocs internals or unrelated user files are deleted. Replacement
cleanup now deletes only the exact explicitly requested target, not neighboring
backups, source files, numbered copies, or unknown staging files.

## Remaining Work

All thirteen complete PERF packets remain open. In particular, this foundation
does not provide a native Python binding, aggregate-family migration, global
CPU/I/O/codec admission, query-native spill, generalized exact small-memory
aggregation, costed compression, fused physical operators, memory-visible
ingest-to-query, mixed-load latency guarantees, or justified PGO. These require
production wiring and their own measured acceptance, not additional reports.
