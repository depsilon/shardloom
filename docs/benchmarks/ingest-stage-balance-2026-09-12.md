# Ingest stage balance and fresh-artifact query acceptance

Status: bounded diagnostic attribution and fresh-artifact Full43 acceptance are
complete. The allocation candidate took 118.604707 seconds and is **dropped**.
The retained **95.447305-second baseline remains unchanged**; the extra
104.044137-second control observation does not replace it. Useful lifecycle/admission tests and a
write-only feature-build correction are retained. This packet follows the
[September 12 implementation/test sequence](../architecture/ingest-performance-implementation-2026-09-12.md)
under PERF-03/08/09/12 and RFC 0044. It does not close broader competitive gates.

## Diagnostic and allocation hypothesis

The accepted native runtime `572bd52c46307a7853c37fb6ca08b6a11d1b9b69`, unchanged
in PR #1437's merged native code, ingested the resident 99,997,497-row Parquet
source with a 24 GiB memory request and P4. Observed caller/source/conversion/
provider ownership was 1/1/1/1, with three conversion-prefetch slots. Four
constructed owners are not four continuously occupied cores; the separate
async-I/O reactor is outside that count.

A ten-second macOS `sample` window began about 10.39 seconds after native process
launch. Each of five threads has 824 samples. Attribution subtracts direct-child
counts from each call-tree node and classifies each remaining count once; nested
frames and the report's recursive totals are not added. These are stack occupancy
counts, not CPU time, runnable time or synchronized utilization measurements.

| Owner | Disjoint observations from the 824 samples |
|---|---|
| Caller | Text/numeric compressors 397 (48.18%); file statistics 141 (17.11%); other native CPU tasks 96 (11.65%); numeric probe 43 (5.22%); digest 59 (7.16%); writes 15 (1.82%); waits 31 (3.76%); other 42 (5.10%). |
| Source | Parquet-reader stacks 288 (34.95%, including read syscalls); result-send backpressure 266 (32.28%); waiting for a source task 269 (32.65%); other 1. |
| Conversion | Empty-queue wait 536 (65.05%); source-result wait 69 (8.37%); derived metadata 75 (9.10%); owned input copy 57 (6.92%); identifiable Arrow work in partial stacks 84 (10.19%); other 3. |
| Provider | Text/numeric compressors 385 (46.72%); other native CPU tasks 111 (13.47%); probe 42 (5.10%); file statistics 26 (3.16%); waits 236 (28.64%); other 24 (2.91%). |
| Async-I/O reactor | `kevent` wait in all 824 samples. |

Full-run ordered conversion handoff waiting was only 0.000875 seconds. Selected
elapsed work spans were conversion 18.056 seconds, numeric compression 37.281,
numeric probe 7.719, text Zstd 53.838 and text canonicalization 5.168. These spans
overlap and cannot be summed into CPU or exclusive wall time. Byte counters are
provider array-size estimates, not unique allocated bytes or bytes copied.

The sampled process completed in 103.131698916 seconds, with observed native-child
peak RSS 2,549,448,704 bytes. Sampling perturbs execution: this is a diagnostic
observation, not an acceptance control or an ingest regression measurement.

Conversion appears to stay ahead of the writer. This justified one small
experiment: use the existing inline-conversion path and move its constructed
owner to the existing provider runtime, producing 1/1/0/2 with no conversion
prefetch. Candidate `4cae0641` selects this only for the existing admitted
parallel-compression writer and an actual 1/1/1/1 arrangement, before constructing
conversion/provider owners. Other arrangements retain their existing policy.
There is no second pool, scheduler or live reassignment.

The competing explanation remains important: the provider was waiting in 28.64%
of samples, while the caller waited in only 3.76%. Ready-work availability or
ordered subtree completion may constrain another driver. Inline conversion also
pulls the derived-metadata reader, so moving it onto the caller can delay writer
progress. The sample alone does not prove spare caller capacity or useful work
for a second provider driver.

## Predeclared gate and measured drop

The maintained baseline is the accepted native `572bd52c` measurement of
95.447305458 seconds from the [September 8 packet](retained-ingest-owner4-2026-09-08.md).
The maintainer clarified after this screen that existing control evidence must
be reused until a candidate shows a credible material improvement. Future work
screens candidates against that retained baseline first. Do not change or rerun
the control simply to reject a slower candidate, and do not lower the baseline
to a later slower observation. Fresh paired confirmation becomes relevant only
after a candidate has demonstrated a plausible gain.

For transparency, the original experiment note below was frozen before that
clarification. Its extra control run is retained as observed evidence, not as a
new accepted baseline or the screening procedure for future work.

The experiment note was frozen before candidate ingest. The first screen uses
one unprofiled candidate followed by one fresh unprofiled control, serially, with
frozen binaries, the same immutable source, P4, codec/layout policy and memory
envelope. Preserve every attempt. Stop this allocation experiment if its
advantage is below 5% or it regresses; do not tune around a marginal result.

A promising screen requires alternating repeated matched-owner runs before
retention, targeting at least 10% lower median complete ingest time. Retention
also requires complete values/schema, required metadata and no-fallback evidence,
no material expansion from approximately 18.6 GB, no more than 10% peak-RSS
regression, zero owned reservations after completion, focused lifecycle/admission
and broad native/default checks, Full43 on newly written output, and held-out
complete-result validation. These are experimental gates, not predicted gains.

| Unprofiled observation | Candidate `4cae0641` (first) | Control `572bd52c` (second) |
|---|---:|---:|
| Caller/source/conversion/provider owners | 1/1/0/2 | 1/1/1/1 |
| Conversion-prefetch slots | 0 | 3 |
| Complete native process, seconds | 118.604707458 | 104.044137250 |
| Native-child peak RSS, bytes | 2,173,272,064 | 2,610,970,624 |
| Native child user CPU, seconds | 220.908769 | 215.438978 |
| Native child system CPU, seconds | 12.641886 | 13.742499 |
| Artifact bytes | 18,591,586,804 | 18,591,586,804 |
| Final shared-native reserved bytes | 0 | 0 |

Both arms construct four owners at public P4. The candidate already failed
against the retained 95.447305-second baseline; the additional control run was
unnecessary for that rejection. Relative to that extra observation it took
14.560570 seconds longer (14.0%) while its observed peak RSS was 16.8% lower. Their artifact hashes
are identical to the fully verified retained output below. The wall-time result
fails the screen regardless of the memory reduction: remove the allocation
change and stop wider tuning. One sequential pair with uncontrolled OS cache and
host conditions is not a stable regression estimate. No sample is discarded and
no repeated performance claim is made. The sampled run is excluded from this pair.

Both generated outputs were checked against their recorded generations and
full SHA-256, linked to the complete native-value proof and fresh-artifact query
receipt below, then retired with durable evidence and exact owned-path checks.
Query results are reused through identical artifact bytes and unchanged query
code; the candidate binary did not receive a separate Full43 timing run. The
[machine-readable packet](ingest-stage-balance-2026-09-12.json) includes every
measurement, identities, all 129 per-query records and verification scope.

The shipping diff retains end-to-end tests for delayed source EOF, empty/final
partial batches, primary source/conversion errors, cooperative cancellation,
concurrent destination creation, full native values and released reservations.
Parallel-codec P1/2/3/4/5/8 tests exercise renamed nullable Unicode and integers
above 2^60 under the retained allocation. The existing buffered-columnar helper
is now gated with its sole caller's feature so write-only Clippy also passes.
No allocation or codec/layout change remains in the shipping runtime.

Final validation passed: formatter check; workspace Clippy and all-target tests
(3,409 passed); CLI/Vortex `release-user-surfaces` Clippy and all-target tests
(3,224 passed, nine existing ignored tests); minimal-native and write-only
Clippy; and 54 harness tests. The machine-readable packet records commands and
hashes for each log. Local retirement identity checks also rejected changed
paths, generations, binary/query identities, incomplete results and fallback
evidence before unlinking. No additional control benchmark was run after the
maintainer's clarification.

## Newly written artifact: complete values and Full43

The diagnostic output is 18,591,586,804 bytes with SHA-256
`7181c2e578659910da176ff6c0dcfe7ce563405337f3ae88cd44e7932d92a266`.
It is byte-identical to the candidate in the
[September 8 four-owner packet](retained-ingest-owner4-2026-09-08.md). That prior
packet directly compared all 11,199,719,664 values across 99,997,497 rows and
112 columns, including schema, order, nulls and valid primitive bits, against
the protected native reference. The new output inherits that complete-value
proof transitively through exact file identity; the full comparator was not
rerun on it. Byte identity also establishes unchanged physical metadata relative
to the previously verified candidate. It does **not** independently establish
that every persisted statistic is correct.

Before retirement, the fresh artifact passed all 43 queries three times:
**129/129 complete returned results match** the retained September 8 ShardLoom
outputs. Finite binary64 comparison is exact, including signed zero. This is
complete-result regression validation against retained engine outputs, not a
new independent oracle. No answer cache or external execution fallback is used.

| Fresh-artifact query measurement | Seconds |
|---|---:|
| Full43 sum of each query's best of three | 102.485398 |
| Hot total: sum of each query's best of runs two and three | 103.076700 |
| Sum of all 129 native executions | 313.564493 |

All query executions use frozen `572bd52c`, a 24 GiB memory request, requested
parallelism 12 and the existing native execution-region policy on the arm64
macOS host. Each query run starts a new process. Timing covers native process
creation through completed public CLI output and process exit; comparison and
lossless evidence archiving are outside the native clock. OS page cache is
uncontrolled, including the first run after ingest and verification.

The September 8 query reference read the protected 18,643,482,956-byte artifact
with SHA-256 `93acc7b9bbabed1f6e15a91aeacda45637bd5d6c5fed26e9b2052bf9b77e84f2`.
Its physical bytes differ, and the new run occurred on another day without an
alternating timing control. These totals establish the newly written artifact's
observed query cost and complete-result acceptance. They do not attribute a
timing difference to layout or to the dropped allocation candidate. The exact
profile output was retired at 2026-09-12 11:41:51 UTC after the saved proofs and
generation checks; its source and the protected reference were preserved.

## Completion boundary and conditional follow-up

Ordinary native ingest uses the existing workspace-safe producer, streaming
checksum, validation, buffered flush and same-directory staging publication.
This path does not call file or parent-directory `fsync`/`sync_all`. Consequently,
the complete-process clock is not a crash-durable fsync completion measurement.
Readback and durable evidence receipts do not upgrade that native writer
guarantee. The separate memory-generation publication contract is not evidence
for ordinary ingest durability.

Bounded writer-batch overlap remains conditional. `BoundedIngestLayout` awaits
each source-batch subtree, but native field, zone and codec work already overlaps
within it. The profile does not localize enough recoverable time to subtree
tails. The failed owner-allocation screen does not establish that overlap is the
next useful implementation. Reopen it only with a plausible material saving on
the critical path; a future small byte-admitted window must bound both submitted
and completed work, preserve local EOF and ordered segments, and validate skew,
cancellation/drain and publication. Per-stream codec/statistics buffers and
later segments waiting for ordered emission can multiply despite unchanged CPU
ownership.

Repeated representation work also requires evidence of actual duplication and
safe reuse. Existing owned copies, post-coalescing numeric compression, physical
statistics and checksums have distinct correctness contracts. Large elapsed
spans do not prove those operations can be removed. Stop marginal experiments;
do not resume the parked topology, broad codec/state or PGO work from this packet.

## Reproduction and retained receipts

The guarded ingest uses `scripts/run_clickbench_ingest_uat.sh`; native queries
use `scripts/run_clickbench_query_uat.py`. The receipts retain exact commands,
source generations, helper hashes, complete outputs and per-query measurements.
Existing 100 GiB workspace, 256 MiB logs and source/process guards remain in force;
one generated full-size artifact is kept at a time.

| Frozen input | Identity |
|---|---|
| Resident `hits.parquet` | 14,779,976,446 bytes; SHA-256 `a390f6cb782f6aaef278c72fc1dd86c4f30bc843ebab3c159e9bd4d45ddb079f` |
| Profile/query binary `candidate-572bd52c` | SHA-256 `9251e10babcfc235b984fd256bc67a123b0b4126b13b375b253b55558a8eef9e` |
| Dropped candidate `candidate-4cae0641` | SHA-256 `a36c2176f7670f389a8d156d2a450a8edfd6bbfd52ca5fe73cf8a0f9f907b1c2` |
| Full43 query definitions | SHA-256 `4afa04814edf3a4c52ff26fd87ea3b5dd92c7264b2d8d69ee718709f3df6f09b` |

Local diagnostic root:
`/Users/dylan/LocalData/shardloom/clickbench-100m-uat/logs/ingest-stage-profile-20260912`.
`stacks.txt` SHA-256 is
`b00f0c391e05cf9461d5892e11165c1a923b38496ff0debc2ddf247b93c516f1`;
the retired `manifest.json` SHA-256 is
`9244496cc604cea4b58a0607aeebc2093863df4a54f5b2f6fbb57973c057019b`.

Attribution and the predeclared gate live under
`/Users/dylan/LocalData/shardloom/perf-all-20260906`:
`ingest-stage-profile-attribution-20260912.json` SHA-256
`b24b4de63f4686815c1ded904e711f8ef7cc569844de833001fff3988433cab0`;
`ingest-stage-allocation-experiment-20260912.md` SHA-256
`019451f3ac3b5760f5595b72f4289d8f2242e9fbedf815522d6410dabc0066a8`.

Fresh-query receipt:
`/Users/dylan/LocalData/shardloom/clickbench-100m-uat/logs/full43_20260912T113159011731Z/summary.json`,
SHA-256 `6b0968ccf9a2004edd3ef0df51fc269594ef5ca3b6a39885108d27f662365768`.
The linked prior complete-value receipt has SHA-256
`c58221888e485e0e0822a76df4d056d5f97dd62ce02c820294911a67822e295d`;
`manifest.retirement.json` records its identity, the fresh-query receipt and the
exact retired output generation.

The unprofiled screen and retirement records are under
`/Users/dylan/LocalData/shardloom/perf-all-20260906/ingest-stage-screen-20260912`.
Both binaries use ordinary release builds with `release-user-surfaces`, without
PGO or native-CPU tuning. The machine is arm64 macOS 26.6.2 with ten logical CPUs;
the compiler is rustc 1.98.0 / LLVM 22.1.8. Exact original helper revisions and
commands are recorded; the query harness used lossless stdout archiving and no
topology override. Superseded held-out summaries were losslessly compressed with
hash-verified archive receipts to maintain the existing log ceiling. Pinned final
acceptance summaries, the input source and protected artifact were preserved.
