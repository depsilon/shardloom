# Native completion and ownership continuation

Status: focused and combined feature checks pass; frozen `2ad143da` completes
public, held-out and protected-artifact Full43 correctness acceptance. This
packet does not promote a Full43 query speedup or
close the entire performance plan. It accompanies the September 12
[plan reconciliation](../architecture/performance-plan-exhaustion-2026-09-12.md).

## Production changes

The explicitly admitted non-null UTF8 weighted COUNT spill route now uses the
existing aggregate job queue and exact string partitions. It reserves spill
scratch and output state before worker admission. When state fits, complete
partitions supply the bounded final selection without writing runs. Under
pressure, it drains workers, transfers committed state once, and consumes each
untouched suffix once through the existing native run store. Compound keys keep
their existing serial spill route. This is not general join or distinct spill.

Projected native dictionaries use the same structural field resolver as the
existing encoded consumers. The worker grows its existing task reservation when
the referenced dictionary domain is larger than the projected row estimate.
Dictionary execution evidence records actual consumers; selection of a contract
alone is not execution evidence.

Prepared integer aggregates additionally admit identity MIN, MAX and AVG through
the existing lowering and consumers. Every call executes with fresh state.
Integer extrema retain exact comparison, and AVG retains the ordinary engine's
ordered floating accumulation. This change adds source/lowering reuse, not
answer caching or different aggregate mathematics.

The already admitted non-null integer grouped COUNT DISTINCT family can finalize
bounded results directly into owned native columns. It preserves original
integer key widths and complete count-descending/key-ascending ranking, applying
offset and limit once. Vortex, Arrow IPC and Parquet output use the existing
writers and publication contracts. Owned results retain their session and memory
credits and can be written after the input file is removed; writing them does
not reopen the source or execute the query again. Unsupported nullable, measure,
ordering and unbounded result shapes fail explicitly before query execution.

Unix Parquet ingestion now retains the source generation through metadata reads,
batch pulls, EOF and final prepublication validation. Independent reader opens
avoid shared seek cursors. See the
[stream-source API migration and generation limits](../reference/resident-native-results.md)
for the required public struct field and platform scope. These checks are not an
atomic filesystem snapshot or a new durability guarantee.

Metadata proof rejects approximate, unknown or internally inconsistent facts.
The metadata report adapter does not fill missing exact column row counts from
unmarked summary counts, and it reports no total when any segment's selection
is unknown. Explicit exact column counts still permit complete all/none proofs.
Native file pruning is existing production behavior; the new fixtures verify
complete results with and without physical statistics instead of claiming a new
pruning mechanism. Registry reports now distinguish selected contracts from
observed dispatch. A production registry dispatch bridge remains open.

## Focused evidence

The retained logs live under the machine-local evidence directory
`/Users/dylan/LocalData/shardloom/perf-all-20260906/`. Counts below overlap where a
test matches multiple filters and must not be added together.

| Check | Result | Log |
|---|---|---|
| Canonical numeric probe preservation | 6 passed | `plan-exhaustion-numeric-focused-r2.log` |
| Streaming lifecycle, pressure and generation | 21 passed | `plan-exhaustion-streaming-focused.log` |
| Prepared aggregate native acceptance | 15 passed | `plan-exhaustion-prepared-focused.log` |
| Public resident aggregate worker | 6 passed | `plan-exhaustion-resident-worker-focused.log` |
| Owned aggregate columns, including committed worker handoff | 9 passed | `plan-exhaustion-owned-focused-r2.log` |
| Existing native and compatibility sinks | 40 passed, 1 pre-existing ignored | `plan-exhaustion-sinks-focused.log` |
| File-backed shared-session serving | 3 passed | `plan-exhaustion-serving-focused.log` |
| Native file pruning differential fixture | 1 passed | `plan-exhaustion-pruning-focused.log` |
| Core library, including conservative statistics proof | 687 passed | `plan-exhaustion-core-focused.log` |
| Query primitive contracts | 13 passed | `plan-exhaustion-query-contracts.log` |
| Encoded predicate report exactness and complete totals | 13 passed | `plan-exhaustion-predicate-evidence-focused-r3.log` |
| Registry selection reporting | 12 passed | `plan-exhaustion-registry-focused.log` |
| Weighted COUNT workers and native spill | 25 passed | `plan-exhaustion-count-spill-focused-r4.log` |

The serving fixture proves serialized progress, cooperative cancellation between
reads, pressure recovery and ownership release. It does not establish FIFO
fairness, preemptive queued/blocked-I/O cancellation, or ingest-plus-short-query
latency distributions. Owned-output pressure checks separately cover admission
rejection and a real partially committed worker handoff, with complete independent
values at multiple worker counts and offsets. These fixtures are not throughput
benchmarks or process RSS ceilings.

Failed compile, lint and fixture attempts remain in their original logs. The
combined validation checks rebuild local crate artifacts after the shared-cache
check exposed a stale dependency across isolated worktrees. Frozen benchmark
binaries and source/reference artifacts are unaffected.

The faster ingest change is retained under the maintainer's instruction, with
90.303309291- and 93.945037458-second observations and the previous
95.447305458-second control preserved as history in the
[numeric-probe packet](ingest-numeric-probe-2026-09-12.md). Storage changes remain
isolated until their changed physical bytes receive complete validation.

## Completed frozen-runtime acceptance

The ordinary release-user-surfaces binary for
`2ad143dae444d3027fb07f11e5b605e991e79b61` has SHA-256
`dfa831817fdb681cfefbfac19d24c7525854ae5d10ce2052158beb72758e71b2`.
All seven local validation stages in `retained-runtime-plan-exhaustion-r4.json`
pass, including 1,757 native Vortex library tests with nine ignored; additional
write-only and universal-format-only Clippy checks pass. These counts overlap
the focused tests above and must not be summed.

Completed packets under `/Users/dylan/LocalData/shardloom/clickbench-100m-uat/logs/`:

| Packet | Completed scope |
| --- | --- |
| `full43_20260912T144726665618Z` | 129/129 exact complete-result comparisons on the protected 18,643,482,956-byte artifact; 96.692272626 s best sum, 96.825921333 s hot total, 293.417033461 s across all executions |
| `resident_call_paths_20260912T145243931514Z` | 2,232/2,232 calls: 12 cases, three public surfaces, two binaries, one warmup plus 30 measured calls |
| `heldout_operators_20260912T150021159832Z` | 380/380 independent Python-oracle checks: 4,096 rows, 19 cases, requested P1/2/4/8/12, two binaries, warmup plus one sample |
| `heldout_operators_20260912T150332635432Z` | 80/80 checks on 131,072 Parquet-prepared rows; two exact/repeated integer-distinct TopK cases, required candidate worker evidence, P1/2/4/8/12, two binaries, warmup plus three samples |

The selected Parquet packet is not the complete 19-case matrix. PyArrow writes
the fixture before native preparation and timing; it performs no query execution.
The independent held-out checks preserve integer extrema, NULL/empty semantics,
complete ordering where required and explicit overflow diagnostics. Full43
compares retained ShardLoom results rather than an independent oracle.

For the three newly prepared MIN/MAX/AVG cases, resident p50/p95 in milliseconds
are below; each percentile uses nearest rank over 30 measured calls.

| Case | Persistent worker, baseline → candidate | Python client, baseline → candidate |
| --- | --- | --- |
| Integer extrema and average | 1.091/1.202 → 0.778/0.908 | 1.526/1.638 → 1.191/1.274 |
| Filtered | 1.177/1.341 → 0.844/0.939 | 1.614/1.731 → 1.262/1.371 |
| Empty | 0.973/1.076 → 0.701/0.767 | 1.419/1.509 → 1.099/1.209 |

These are 28–29% lower persistent-worker p50 and 22–23% lower Python-client
p50 on a 32-row fixture. Candidate repeated calls retain one source open and
record executions 1 through 31, with fresh aggregate state and native certificates.
The existing nine resident cases remain within approximately ±1.4% p50.
Fresh-process p50 for the new cases is 1.85–3.73% higher, so the gain applies to
repeated resident calls. Worker startup and Python import are excluded from
their repeated-call clocks; transport and complete response handling are included.
This does not measure production throughput, RSS, copies, decoding or kernel CPU.
The resident harness retains its historical tolerant float comparator. A separate
manual read-only audit of all 558 archived new-case envelopes matched exact
integer types/values and binary64 values to independent fixture expectations;
that audit did not change the harness comparator or add a checked-in verifier.

Full43's best sum is 6.00% higher than the September 8 `572bd52c` control.
The targeted diagnosis `count_lane_diagnostic_20260912T153933990362Z` completes
24/24 exact calls, including warmups. Measured baseline→candidate medians are
Q34 4.316019→4.117707 s (−4.60%), Q35 3.761960→4.002883 s (+6.40%), and
Q36 6.787560→6.760126 s (−0.40%). All query pair ratios are mixed; the historical
16–40% slowdowns did not recur consistently. Across 18 measured calls the
candidate totals 45.132676916 s versus 45.619603875 s; the sum of medians is
approximately 0.10% higher. This diagnosis does not prove stable global
nonregression, and the protected Full43 timing control remains `572bd52c`.

## Interrupted attempts and lossless evidence packing

The first Full43 attempt `full43_20260912T143854460209Z` remains incomplete at
52/52 recorded passing calls after the existing log guard stopped progress. Its
samples are not substituted into the completed 129-call packet. The first
targeted diagnosis likewise preserves its original 22 passing calls; only the
last two Q36 calls resumed, with the wall-clock interruption explicitly
recorded in the final diagnostic receipt. No earlier timing was replaced.

The initial Parquet held-out attempt failed before fixture generation or native
execution because its Python interpreter lacked PyArrow. The completed 80-check
packet used the existing fixture environment with PyArrow 25.0.1; the failed
attempt remains in `plan-exhaustion-public-acceptance-20260912.json`.

Completed resident and held-out folders retain `summary.json`, their fixture
identities, `verified-execution-transcripts.tar.gz` and the matching
`verified-execution-transcripts.archive.json`. Archive indices record original
member identities and the verified archive hash. Original transcript files were
removed only after byte-verified packing; complete stdout remains recoverable.
Later packing of completed Full43 non-stdout and stdout files is recorded in
the separate r3/r4 archive receipts. These operations preserve evidence within
the existing log budget; they do not weaken storage guards or alter the protected
complete-result reference. The targeted diagnostic's interruption and resumed
gap remain limitations when interpreting its timing samples.
