# Performance Overhaul Implementation Evidence

## Current Scoped Closeout

The C3 foundation at `b3bb15adf4c0e74dac498000d78b931a9ca80674`
adds prepared count/collect reuse, owned native results and sinks, typed
memory-visible collection, shared admitted ingest-buffer ownership, and real
numeric-sort query spill. These are executable scoped additions. They do not
complete all thirteen PERF packets or establish a general performance claim.
The completed scope is recorded in the
[completed ledger](phased-execution-completed-ledger.md); the
[phase plan](phased-execution-plan.md) owns the remaining acceptance work.

The [drop/ship evidence packet](../benchmarks/perf-drop-ship-2026-09-05.md)
owns comparison boundaries, frozen revisions, binary/source hashes, raw runs
and retain/drop decisions. The retained batch passes the final scoped checks
below; whole-packet requirements remain open. Earlier paired C3
checks include 16/16 native-array sink reopen records and 640/640 held-out public
records, including baseline controls and warmups. Their frozen binary and raw
run identities are in the packet; these are not fresh C7 checks.
The C3 ingest sample
is 99.175 s with 3.428 GB peak RSS, compared with the 155.098 s, 10.955 GB peak-RSS
control. Its artifact is 37,846,260,172 bytes. C4 on that layout passes 129/129
complete-value checks with total 136.469 s versus 145.598 s, but its geometric
mean regresses from 1.019390 s to 1.236274 s. The expanded footer and C4/C5's
historical whole-layout control-plane traversal explain much of that short-query
overhead; the packet records measured setup spans and the limits of attribution.
C7's integrated query path now reports root-only inventory and defers complete
traversal to explicit inspection. C7's measured calls recover much of the added
latency, although its combined changes do not isolate inventory cost.

C5 numeric compression is retained by explicit maintainer direction. It ingests
in 98.344770 s with 3,188,228,096 bytes peak RSS and an 18,643,482,956-byte
artifact. Actual physical inspection verifies numeric encodings reach storage
while text control bytes stay unchanged. Its initial full43 run passes 129/129
complete-value checks but takes 196.321995 s best-of-three total and 1.576350 s
geometric mean. The encoded consumer regression is retained in the record.
Native typed accessor corrections and their materialization evidence are
integrated, and the combined C7 full43 run passes all 129 complete-value checks.
Different layouts and isolated ingest gains are not an overall
query-speed claim or byte-identical artifact evidence.

C6 at `16098c7eb15726f6d5cb4b8e1d5ffe3ec8e2f20c` adds complete-key UTF8 count
reconciliation on shared-worker partitions. The targeted Q34/Q35 comparison on
the original artifact passes all six complete outputs, with minima 4.662216 s
and 4.653582 s versus C4's 10.346895 s and 10.761406 s. Q34 run 1 reconciles
18,342,019 complete groups from 99,997,497 rows over 64 partitions, with nine
peak active workers. Final caller reconciliation is 1.333276 ms; worker work
and waits overlap and must not be summed into elapsed time. The packet retains
all samples, CPU, RSS, reservations and their accounting exclusions. This closes
a measured single-key count slice, not all PERF-04 families.

C7 is frozen at `3c7ea538d9c7d240d03be982390b65b9f0c6dc88`. Its full43 run on
the retained C5 numeric artifact passes 129/129, with 131.686635 s best-of-three
total and 132.309902 s hot total. Versus the baseline, total improves 9.55% while
geometric mean worsens 7.02%, from 1.019390 s to 1.090906 s. Q17's 16.155425 s
best recovers the initial C5 40–46 s regression but remains above the baseline's
15.877899 s. Q34/Q35 best times are 4.820260 s / 4.543462 s. These are combined
query-runtime results on C5 storage, not a new C7 ingest measurement.

Validation logs record 3,403 default-workspace tests, 2,947 native CLI/Vortex
tests with one ignore, 24 Python harness tests and a passing reference validator.
The broad native run was at `4d95116d`; eight resident-worker tests and both final
clippy checks cover the subsequent reset refactor frozen in C7. Default/Python/
validator logs do not embed an exact revision; no full native rerun at C7 is
claimed. Final formatting and contribution governance pass. Fresh C7 checks pass
744/744 public call records, 640/640 held-out records over 16 cases and requested
workers 1/2/4/8/12 at 4,096 rows, 16/16 native-output reopen records, and an
independent 131,072-row numeric-sort spill with exact large integers and owned
cleanup. Totals include controls/warmups where applicable. Two larger held-out
matrix attempts stop at unchanged guards, the second after 259 accepted records;
neither supplies full large-matrix acceptance.

The 1,000-sample native examples report isolated and mixed p50/p95/p99 separately.
Nullable 32-row intake/JSON is 0.028625/0.044333/0.052666 ms isolated and
0.391542/0.451375/0.528250 ms mixed; the 64 KiB i64 profile is
0.100583/0.144000/0.166125 ms isolated and 0.469166/0.497292/0.851583 ms mixed.
The shared-session 16,384-row background overlaps 999/1,000 and 1,000/1,000 timed
foreground calls respectively. Both profiles finish with zero owned live bytes
and zero denials; concurrent in-flight operations do not prove simultaneous CPU
work or the broader bulk-ingest latency envelope. Public worker/Python and fresh
process timings remain separate from prepared Rust operation timings.

The bounded immutable memory-file example verifies all 16,384 reopened rows and
100 alternating query pairs. At example-only source `9350cec1`, generation is
0.723042 ms, durable publication with complete independent readback/native
validation is 12.614 ms, and complete reopen is 6.813 ms. Publication reuses array
segment bytes without re-encoding; query-phase and publication counters remain
separate. Zero owned live bytes/denials follow complete drop. An earlier 64 KiB
demo output-bound failure publishes nothing; the example now uses the same
explicit 32 MiB bound as its ordinary-source comparison, with a focused test and
clippy pass. C7's CLI/library is unchanged. The packet owns all percentiles,
commands, hashes, byte scopes and raw samples; this is not a general speed claim.

The fixed 4 GiB process-memory target was explicitly removed by the maintainer.
Historical attempts below remain evidence of accounting gaps; they are neither
a current release requirement nor proof that arbitrary upstream allocations or
RSS are bounded. No external execution fallback is admitted.

## Reference State

The maintainer requested implementation of the supplied performance overhaul on
2026-09-05. PR #1429 (homepage productionization) is merged at `c6cda693`, the
starting point for this work. PR #1428 already removed dual exact-histogram/sketch
maintenance. That improvement must not be represented as new implementation.

This record distinguishes executable work from the thirteen complete PERF
packets. RFC 0044 and the active phased plan remain the acceptance contract.
The historical foundation evidence bundle is
[`performance-overhaul-foundation-2026-09-05.json`](../benchmarks/performance-overhaul-foundation-2026-09-05.json).
It preserves all four full43 timing matrices, result digests, ingest summaries,
the independent Q20 reference, and prepared-count latency.

## Implemented Boundaries

- `LiveMemoryPool`: shared live/peak reservations, checked growth, ownership
  transfer, denial, contention, and drop/unwind release. This is not a process
  allocator or a claim that every upstream allocation is charged.
- `ComputePool`: reusable dynamically assigned workers, bounded queue slots and
  bytes, cooperative cancellation, panic containment, and owner shutdown/drain.
- C6 exact non-null UTF8 COUNT(*) keeps every key through complete partition
  reconciliation on the shared workers, then selects final partition candidates
  after source drain. The admitted partition vectors and growth overlap retain
  pool credits. The measured Q34/Q35 calls need no native pressure handoff;
  unmeasured pressure paths and other aggregate families remain distinct.
- Streaming writer prefetch: bounds queued, active, and completed out-of-order
  work together. The bounded public ingest route now supplies an explicit shared
  native-memory budget. Input buffers are copied into admitted owned storage;
  their credits survive conversion and upstream writer retention. First, serial
  and prefetched batches use the same artifact-local pool as allocations made
  through the native host allocator. Original reader/input owners, provider
  scratch and allocations bypassing that allocator are excluded. Oversized
  admitted batches fail explicitly; this is not zero-copy intake.
- Parquet input batch sizing: uses existing row-group uncompressed-size metadata,
  a schema-width floor, and expansion headroom to lower decode batch rows under
  smaller caller memory budgets before allocation. The CLI shares one eighth of
  requested memory across requested readers for this estimate. Complete-source
  row admission is separate; actual post-decode byte checks are retained. Estimates
  do not prove a bound on dictionary expansion, large single values, or RSS.
- `ReservedHostAllocator`: uses Vortex 0.85's safe host-allocation hook and
  lifetime owners. Buffer clones and slices retain the full allocation credit.
- `ResidentVortexSession`: retains provider registries, runtime workers, an open
  OS file, Vortex reader metadata, and bound count/projection/filter operations. Device,
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
- Persistent public workers retain the matching prepared count or collect
  handle and execute it on every call. Request changes, parse failures and source
  binding changes release retained state. Input frames are capped before JSON
  parsing. Count replies preserve truthful metadata-only execution evidence.
- Typed in-memory intake builds immutable allocator-owned Vortex arrays from
  nullable Int64, Float64, Boolean and UTF8 batches. The native Rust API and
  bounded generated-row public collect route apply native filtering, projection
  and limits, returning owned arrays or complete bounded JSON. Input, output,
  column and byte caps are explicit; no file persistence or answer cache is
  required. Caller/parser vectors and general provider scratch remain excluded
  from native-buffer accounting. This is not a native Python binding.
- The single-file native array sink writes admitted projection/filter output
  directly to a native Vortex artifact, with complete reopen checks and owned
  staging/publication cleanup. It does not render JSON or build scalar rows as
  an intermediate. A pushed limit reports its pre-limit count as a lower bound
  unless the scan is exhausted; no extra full scan is hidden in that evidence.
- Numeric-sort spill now stores real query keys and row identities in native
  Flat Vortex runs, with owned temporary disk reservations, bounded merging,
  exact ties/offset/limit handling and cleanup. Both source passes use the
  retained file handle with generation checks. This detects ordinary mutation;
  an open file is not an immutable snapshot against arbitrary in-place writes.
  The route does not establish aggregate, distinct or join spill support.
- Bounded ingest and sink writers await admitted native subtrees or Flat leaves
  before accepting more work. The ingest route coalesces within each source
  batch, changing physical layout from cross-batch coalescing. Provider
  allocation gaps and source-batch sizing remain explicit.
- Ingest inventory uses the logical DType display instead of Debug output that
  recursively dumps runtime registries. The field remains a schema summary;
  inventory digests intentionally no longer incorporate debug registry ordering.
- OLAP publication performs one complete artifact fingerprint instead of computing
  and discarding a first fingerprint before computing the same fingerprint again.
  The remaining publication checksum is not replaced with a caller-provided digest.
  `prepared_olap_publication_millis` reports the finalization span separately from
  `prepare_once_millis`; total ingest timing still includes both.

Fresh CLI invocations still create fresh sessions. Persistent Rust execution,
persistent public-worker calls, Python client calls and fresh-process latency
are separate timing surfaces and must not be substituted for one another.

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

## Historical Foundation Measurements

The measurements and validation records in this section describe the earlier
foundation snapshots. Final-code comparisons belong to the drop/ship packet
linked above; later physical-policy changes require their own query acceptance.

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
The decision for that historical foundation comparison was to keep the original
layout as its benchmark control and retain smaller input batches as a source
admission repair. It did not establish a general layout optimization or a 4 GiB
process-memory success. That foundation runtime was full43-tested on the smaller
layout; its subsequently generated original-layout artifact was byte-identical
to the artifact covered by its earlier original-layout full43 runs. No further
foundation-binary/original-layout full43 was run in that comparison. This does
not prescribe the retained representation for C3–C7.

The prepared Rust metadata-count experiment ran 10,000 iterations with p50
2.875 us, p95 3.875 us, and p99 4.500 us. Preparation took 8.770 ms; one source
reader was opened and 10,001 operations completed including warmup. Provider
reserved-memory peak was 1,538,992 bytes. This measures metadata-only prepared
Rust execution, not Python calls, general SQL, ingestion, or mixed workloads.
Evidence: `performance_overhaul_gates_20260905/resident-latency.json`.

## Historical Foundation Test Evidence and Repairs

Evidence root on the development host:
`/Users/dylan/LocalData/shardloom/clickbench-100m-uat/logs/performance_overhaul_gates_20260905`.

Default workspace tests and clippy passed during implementation. Focused tests
cover reservation contention, worker reuse/cancellation, output lifetimes,
source mutation, ordered prefetch pressure, and actual collected values. The
Python storage/query harness suite passes 18 tests, including interruption,
runaway output, source-path safety, exact-value comparison, timeout cleanup, and
preserving source/backup/lookalike files during exact-target replacement.

Historical foundation Rust validation passed:

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

These failures describe the initial foundation snapshot. The release-0.2.3
validation follow-up repaired the stale expectations while retaining full output,
overwrite-permission, and no-fallback assertions. That follow-up passed 1,317
native Vortex tests with one intentionally ignored fixture-regeneration helper;
native CLI passed 881 unit, 47 public-workflow, and 172 ingest tests.
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

The release-0.2.3 original-layout ingest and 43-query/129-run evidence is
`docs/benchmarks/clickbench-current-state-2026-09-05.md`: 162.781 s native ingest,
148.349 s query total, 1.044 s geomean. This replaces neither the historical
145.130 s baseline nor the separate smaller-layout experiment above. It does not
establish an overall query-speed gain. Those measurements belonged to the
release-0.2.3 publication process, whose completed status is recorded in the
completed ledger. They do not authorize publication of this later performance
batch or substitute for its separate validation.

## Retention and Local Cleanup

The post-PR [text-layout continuation](../benchmarks/perf-text-layout-pruning-2026-09-05.md)
retains a bounded completed-file-read observer and paired native writer/query
fixtures. Both profiles pass 448 exact cases, and promoted public zoning passes
129/129 full43 values. Unconditional default zoning is nevertheless dropped:
40/43 bests are slower than C7, best total rises 2.80%, geometric mean rises 3.32%,
and one-ingest-plus-best arithmetic rises 2.11%. Production returns to the
unzoned C7 policy while numeric compression and earlier query/ownership work
remain retained. The rejected artifact was removed only after exact generation
verification; raw logs, hashes, binaries and the test-only candidate remain.
This continuation preserves all historical C7 numbers and leaves the wider
PERF-09/PERF-12 and competitive gates open.

Retain the demonstrated duplicate-checksum removal, compact schema evidence,
native result correctness repair, and tested runtime ownership foundation. The
later b3 implementation adds the scoped boundaries listed above; global admission,
all aggregate families and a native Python binding remain incomplete. The
historical smaller-batch layout is not promoted as a benchmark
performance win. Its slower geomean and failed memory acceptance remain evidence
for the next resource-governor work, not hidden exceptions to a completed phase.

Earlier foundation cleanup removed its owned baseline worktree and redundant
generated artifacts after measurement. Current retention includes the original
control artifact and retained numeric artifact, official source, raw logs,
frozen baseline/candidate binaries, and the compact repository evidence bundle.
Keep artifacts needed for active comparisons until an exact-target verification
and cleanup decision is recorded. Regeneration requires each artifact's recorded
ingest command. No CloudDocs internals or unrelated user files are deleted.
Replacement cleanup deletes only the exact explicitly requested target, not
neighboring backups, source files, numbered copies, or unknown staging files.

## Remaining Work

The whole PERF packets remain open until their declared scope and measured
acceptance pass. Existing bounded query spill and memory-visible collection must
not be described as absent, and narrow successes must not close broader families.

| Packet | Mandatory remaining acceptance |
| --- | --- |
| PERF-01 | Complete same-commit ingest/timing attribution around the recorded C7 full43 results; keep overlapping work separate from elapsed time. |
| PERF-02 | Remaining prepared operator families and native Python prototype/binding decision; extend invalidation and separately timed acceptance to those families. |
| PERF-03 | Shared CPU/I/O/codec admission beyond owned buffers, with observed progress, queue pressure and honest provider allocation gaps. |
| PERF-04 | Required aggregate families and deterministic parallel reductions; keep encoded/constant fast paths and ship/drop measured candidates. |
| PERF-05 | General exact key/state ownership and pressure behavior, including composite keys, nulls, all-unique keys, skew and non-URL cases. |
| PERF-06 | Shared aggregate/distinct/join spill and supported large-state acceptance beyond numeric sort, including recovery and ownership failure cases. |
| PERF-07 | Remaining materializing, multi-source and compatibility sink families; complete byte-work and retained-result lifetime evidence. |
| PERF-08 | Finish source/derived/codec duplication attribution and same-source durable ingest plus first/repeated-query lifecycle comparison. |
| PERF-09 | Codec CPU/cost and resource admission beyond the recorded C3/C5 physical encoding/byte inventories; complete lifecycle retain/drop evidence. |
| PERF-10 | Reusable costed physical composition beyond existing native filter/project and specialized kernels. |
| PERF-11 | Larger declared bulk-load/pressure and cancellation envelopes beyond the two measured queue-inclusive native profiles; no general sub-ms guarantee. |
| PERF-12 | Broader relational/resource coverage beyond the accepted 4,096-row matrix and meaningful non-ClickBench family gains for a general claim; larger matrix guard aborts remain incomplete. |
| PERF-13 | Conditional feasibility decision after remaining instruction/dispatch costs justify compilation work; no JIT or new dependency is required merely to close a checklist. |

Numeric compression is retained after the maintainer's explicit decision;
native consumer corrections and C7 full43 complete-value acceptance are recorded
with the total/geometric-mean tradeoff. C4's earlier tradeoff remains in the record;
C6 adds measured original-artifact Q34/Q35 partition reconciliation with six
complete-value checks, not a full43 or whole-aggregate-family completion.
Text zoning remains isolated and unmeasured. Native memory-file generation and
publication are implemented and integrated, with the bounded prototype's exact
query/publication/reopen and ownership measurements recorded above. None of these later
changes should be presented as original b3 behavior or as already published.
