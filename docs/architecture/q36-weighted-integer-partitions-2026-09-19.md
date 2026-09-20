# Q36 weighted integer partition screen

Status: retained after paired speed/memory gates, Full43, broad validation and
independent review. This follows the selected next screen in
[remaining admission](remaining-performance-admission-2026-09-19.md), under the
existing PERF-02/03/07/08/09/11/12 obligations; broad CG completion remains paused.

The retained Q36 complete run is 4.144340 seconds, including 3.330456 seconds
merging partials on the caller. Existing chunk reduction produces 21,678,299
weighted entries from 99,997,497 rows. Preserve that reduction and native integer
width, then append weighted entries to leased complete-key partitions. At EOF,
sort and sum each complete partition on the same workers, retaining a bounded
union of partition winners for the existing renderer. No per-chunk Top-K is safe.

Admission requires the existing nonnullable identity physical-key proof, COUNT(*),
one descending COUNT order, no HAVING or spill, and positive OFFSET+LIMIT at most
128. Dependent outputs remain limited to the existing identity, integer constant
and AddOffset proof. Each sorted partial's minimum and maximum are validated
through the existing reconstruction function before routing: checked addition
is monotone on each admitted integer domain, so these endpoints cover errors
in losing groups too. Final output still uses the existing comparator and
derived-value reconstruction.

Native Constant input keeps its complete multiplicity. On a native Dict chunk,
drain prior jobs and transfer every accumulated weighted entry into the existing
global state before using the existing native dictionary path; disable persistent
integer partitions for that query. This transfer uses bounded batches, contributes
to caller merge timing and never truncates candidates. If initial partition
metadata cannot reserve capacity before input, only the optional partition
strategy declines; the existing worker route remains available.
Committed allocation or source failure cancels, joins and fails; it does not
invent a spill or pressure replay route. Vector capacity and growth overlap,
task output and coordinator ownership stay leased; legacy output-map allocations
remain a separately reported scope, not a claim of process RSS enforcement.

Vortex-first provider check: `implement_shardloom_kernel`. The pinned Vortex
0.85 native scan, primitive and Constant providers already supply the admitted
source/count boundaries. This changes ShardLoom's cross-chunk grouped-state
ownership and exact Top-K completion, which the current provider inventory does
not expose as a certified grouped executor. Reuse native scan and owned partials;
add no upstream dependency, Arrow conversion, alternate engine or persistence
format. Native certificates and `fallback_attempted=false` remain required.

Verification: renamed scalar keys, signed/unsigned extrema, native Constant and
mixed Dict, empty inputs, cross-chunk global winners, ties/OFFSET, non-winning
expression errors, rejected schemas/shapes, cancellation, reservation denial and
native-file source faults. A candidate must prove actual route activation before
timing. Retain only for at least one second of comparable complete-query savings
or at least 30% lower OS peak RSS with nonregressing time. Preserve all samples and
use the fastest valid run symmetrically. Winners receive Full43 UAT, broad gates,
independent review and a PR; failed prototypes are removed with evidence retained.

## Full-size comparison

The frozen candidate is `58f0443597cae4c90266dbe2d4fa4481e8c16495`, built with
`cargo build --release -p shardloom-cli --features release-user-surfaces`, SHA-256
`d13670fe73dc00cd515284713e51426eb836c8fe91149490c860cdac952206fe`.
The retained post-Q33 control is `06da983b5928876c7a07e75a4c77f06a2d931d61`,
SHA-256 `98e6fea73ea15a2d99c2cea97bb6f43f01530692c88e139aaaf90b83ecea9b0a`.
Subsequent changes through main `6c56a435` are tests, benchmark fixtures and docs;
the frozen control preserves its actual build identity.

The guarded paired runner uses the unchanged 18,591,586,804-byte native Vortex
artifact, 24 GiB policy memory and requested P12 (host ceiling 10, nine compute
workers), on an Apple M5 with 16 GiB physical RAM and macOS 27. The policy memory
value is not a statement of installed RAM or an OS RSS limit.
Each operation is a fresh CLI process; elapsed time includes startup,
complete output and exit. OS cache is shared and uncontrolled. Runs alternate
candidate/control order. Host snapshots and archive work are outside timing.

| Pair | Control seconds | Candidate seconds | Control peak GiB | Candidate peak GiB |
| --- | ---: | ---: | ---: | ---: |
| 1 | 4.595702 | 0.931720 | 3.419 | 0.623 |
| 2 | 4.402201 | 0.299872 | 4.253 | 0.624 |
| 3 | 4.511900 | 0.300789 | 4.535 | 0.644 |

The fastest valid call for each role saves **4.102329 seconds (93.2%)**, with
**85.3% lower OS peak RSS** for those calls. All three candidate calls are below
one second. This is achievable complete-call latency on this shared host, not a
production percentile or cold-storage guarantee. All six complete outputs match
the retained reference; that comparison is regression evidence, while renamed
unit/native fixtures independently check exact semantics.

Every candidate call reports 99,997,497 weighted rows, 21,678,299 partial entries,
9,762,046 complete groups, 1,550 completed source chunks and 64 completed partition
jobs, with zero outstanding jobs and no dictionary handoff. This preserves the
existing partial reduction and exact group cardinality. The fastest control
spends 3.670378 seconds merging on the caller; the candidate spends 0.131124
seconds routing/merging and 0.048815 seconds finishing complete partitions.
Candidate worker elapsed spans sum to 2.107210 seconds and overlap across nine
workers; they must not be added to wall time. Finalization falls from 0.593
seconds to 0.000087 seconds because only the bounded complete candidate union
reaches the final renderer. Typed vector capacity peaks at
382,214,432 bytes. Legacy output maps and process RSS remain distinct scopes.
Sort cancellation is checked before and after each partition, so preemption
inside a large skewed sort is not promised.

Raw paired evidence is
`/Users/dylan/LocalData/shardloom/clickbench-100m-uat/logs/paired43_20260920T004612699366Z`.
Reproduce with `scripts/run_clickbench_paired_query_uat.py`, the above binaries
and commits, `--query-ids 36 --memory-gb 24 --max-parallelism 12 --timeout 120`,
the retained `performance-pr-ingest-4f2c7b970078-r1.vortex` input, local UAT root
and `ship-drop-20260919/references`. Full43 uses
`scripts/run_clickbench_query_uat.py` with the same candidate, `--compress-logs`
and no query-ID restriction. No ingest or storage-size rerun is required by this
query-only change.

The unchanged 256 MiB log guard required lossless recompression of four closed
historical transcript archives. Each decoded tar stream and member proof is
preserved; adjacent addenda identify the new archive paths. The receipt is
`/Users/dylan/LocalData/shardloom/ship-drop-20260919/q36-uat-log-recompression.json`.
Accounted log bytes fell from 260,804,608 to 247,357,440. No source or benchmark
result was removed, and storage admission was not weakened.

## Full43 acceptance and next attribution

The same frozen binary passes **129/129 complete-result comparisons** on the
unchanged source. Every compressed output, native timing/RSS record and complete
reference result was independently re-read after completion. The new strategy
activates only for Q16 and Q36 in this suite. Q16 preserves 17,630,976 groups and
Q36 preserves 9,762,046 groups in each of their three executions. Q36's Full43
best is 0.339494 seconds. The suite's best-of-three sum is **79.856087 seconds**
(hot sum 82.009117 seconds); this is an unpaired observation, not a suite-wide
causal speedup claim. Raw evidence is
`/Users/dylan/LocalData/shardloom/clickbench-100m-uat/logs/full43_20260920T004655178095Z`.

The next formal target returns to Q29, whose current best complete call is
10.215333 seconds. Its recorded 7.657883-second accessor span already separates
5.080100 seconds of chunk-dictionary construction and 2.517873 seconds of provider
execution. It constructs 25,771,910 entries and copies 3,120,823,803 UTF8 bytes
across 81,032,736 accessor rows after native scan. Attribute dictionary construction first: its
hashing, equality, allocation and copying costs still need separation before
admitting another implementation. Domain transformation and weighted aggregation
belong to the separate 1.897909-second update span. Do not reopen the dropped
owned-string partial simply because it is related to this work.

Q34/Q35 follow at 4.960955/5.074347 seconds. Caller join waits dominate the caller
spans; worker reconciliation and lock-wait spans are substantial but overlap.
Those totals are attribution targets, not additive exclusive CPU costs or proof
that changing topology helps. The latest top ten and raw fields are saved in
`/Users/dylan/LocalData/shardloom/ship-drop-20260919/q36-next-profile-inventory.json`.
Keep the one-second complete-query / 30% RSS admission gate, no-fallback policy,
and parked experiments unchanged.

## Validation

The [machine-readable packet](../benchmarks/q36-weighted-integer-partitions-2026-09-19.json)
preserves every paired sample, source/binary identities, complete-suite scores,
per-query best observations, raw archive hashes and local validation receipts.

- Eight weighted-partition tests cover typed P1/P4 complete results, exact global
  winners, ties/OFFSET, empty input, checked overflow, cancellation, committed
  denial and optional initial admission. Six physical-key tests cover derived
  outputs, mixed Constant/Dict handoff, losing errors, schema rejection, native
  dispatch and the OFFSET+LIMIT boundary.
- Three native-file tests verify ordinary derived outputs, repeated prepared
  execution, committed source allocation/corruption faults, recovery and final
  lease release. Existing owned COUNT tests preserve exact values while allowing
  the newly certified execution strategy.
- `cargo fmt --all -- --check` passes.
- `cargo clippy --workspace --all-targets -- -D warnings` and native-feature
  Clippy pass.
- `cargo test --workspace --all-targets` passes 3,424 tests. Native-feature library
  tests pass 1,847 tests; nine explicitly ignored timing/benchmark fixtures remain
  ignored.
- Public-claim language, public-status docs and workspace-version validators
  pass. The architecture tracker passes its existing `--allow-blocked` contract;
  broader phase obligations remain open.

Independent review identified per-entry overhead and missing timing in Dict
handoff; both were corrected before the frozen build. Final review independently
verified all paired archive members and found no remaining actionable issue or
missing-work signal. Committed denial remains fatal, sort cancellation remains
at partition boundaries, and no broader spill, process-memory or production
latency guarantee is introduced.
