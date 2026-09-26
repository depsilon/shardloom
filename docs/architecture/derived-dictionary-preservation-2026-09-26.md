# Derived dictionary persistence — R1.a

Status: retained under the storage gate; `claim_gate_status=not_claim_grade` for
broader competitive claims.
This is the first experiment in the [September 26 intake](performance-candidate-intake-2026-09-26.md).
The maintainer authorized the complete queue. This is a measured storage win;
no query-suite or ingest speedup gate is claimed.

## Frozen workload and acceptance

Control source is merged `6db17c9f`, portable release profile with
`shardloom-cli/release-user-surfaces`. The source is the resident 99,997,497-row
`hits.parquet`; ingest uses P4 and 24 GiB, with the existing guarded runner.
The retained 18,591,586,804-byte native artifact is the value/schema/statistics
comparison reference. The candidate changes existing dictionary persistence;
source text Zstd, numeric data compression, worker counts and input batch policy
remain the comparison controls.

Retention requires one of the intake's frozen ingest, storage or suite gates:
10% lower complete durable ingest with no larger artifact; 15% fewer artifact
bytes with no ingest/affected-query regression; or 10% lower matched Full43
best-sum and 5% lower geometric mean without material family regression.
Charge any slower preparation to a demonstrated reuse workload before conditional
retention. Retain every sample and compare symmetric fastest valid complete calls.
All values, schema, row order, required statistics, native reopen and full query
outputs must pass, alongside independent renamed/null/Unicode/dictionary-epoch
fixtures and resource/error cleanup. Actual RSS is separate from reservations.

## Source and provider evidence

`EmbeddedUrlDomainInt32Builder` already emits Arrow dictionary arrays, and native
Arrow conversion preserves their Vortex Dict representation. The retained writer
does not preserve it end to end. Vortex 0.85 `RepartitionStrategy` canonicalizes
its emitted ChunkedArray even with `canonicalize=false`; that flag controls an
earlier per-input canonicalization. The default leaf enters this repartition
before its dictionary probe. The probe then uses an empty edition whitelist,
so enabling a dictionary probe alone cannot reuse the producer's dictionary.

The existing native Flat serializer accepts Dict arrays, the existing native
Zoned writer computes required statistics without replacing its input, and the
aggregate accessor already consumes Vortex Dict values/codes. Use those providers
before inventing a layout or another execution path. A candidate can preserve an
economical existing UTF8 dictionary before repartition, compress only its codes,
and keep the same dictionary values/epoch. No global dictionary, answer sidecar,
foreign encoding, dependency upgrade or external engine is involved.

Admit only existing UTF8 Dict chunks within the current row-block bound and a
conservative encoded-buffer cost bound. Other chunks use the retained native
strategy. Each preserved chunk has its own native zone, whose block size covers
exactly that chunk; no statistics are reused across dictionary epochs. Native
buffer owners and their allocator reservations survive through serialization.
The existing bounded source-batch writer controls outstanding input lifetimes;
this experiment adds no cross-batch queue.

## Evidence log

- Exact retained-file metadata confirms 18,591,586,804 bytes and full local
  allocation. Read-only physical inspection first hit its explicit 100,000 Flat
  reference limit. Add a bounded caller-selected inspection limit and repeat;
  do not treat the failed partial inspection as a completed inventory.
- Completed repeat: SHA-256
  `7181c2e578659910da176ff6c0dcfe7ce563405337f3ae88cd44e7932d92a266`,
  160,132 unique Flat segments, 18,577,006,684 segment bytes. Both derived URL-domain
  fields are plain `vortex.varbinview`: 1,781,937,792 and 1,770,608,144 bytes,
  totaling 3,552,545,936 bytes. This is current-artifact evidence, replacing the
  older file's attribution for this experiment.
- Recovered the rejected recipe: native Zstd of expanded helper text, 100.521644 s
  complete ingest and 15,466,554,020 bytes. Complete values/statistics and all 129
  outputs passed, but ingest and the unpaired query screen were slower. Preserve
  its historical drop; dictionary survival is a different mechanism.
- Independent design review found signed Arrow codes would miss the unsigned
  native accessor, and shared leaf callers without bounded input ownership would
  admit unbounded chunk scheduling. The prototype uses a checked native unsigned
  cast and is installed only inside the source-batch-bounded stream route.
- Evidence root: `/Users/dylan/LocalData/shardloom/performance-candidates-20260926`.
  Individual receipts will record binary hashes, source identity, full command,
  physical encoding counts and complete process timing.
- The first full control run is invalid: its 18 decimal GB watchdog ceiling was
  below the known 18.592 GB retained artifact. The stopped output's full hash
  equals the retained file, so only that owned duplicate was removed. Valid
  reruns use 19 decimal GB (conservatively reserved as 19 GiB), the same 12 GiB
  free-space floor, and a 110 GiB UAT workspace ceiling.
- Maintainer-requested cleanup removed the inspected rebuildable debug/test
  cache, reclaiming 156,599,050,240 bytes of measured free space, and 18 local
  branch references whose exact tips are reachable from `origin/main`. Local
  incremental builds are disabled to limit regrowth. Release binaries, sources,
  retained baselines, receipts and unfinished work remain. The app refused
  archival of two finished worktrees because the pinned task protects them;
  that protection was preserved. Exact cleanup receipts are in the evidence root.
- Valid first ingest pair: control 90.605667 s / 18,591,586,804 bytes;
  candidate 88.416283 s / 15,682,956,116 bytes (15.64% fewer bytes). These are
  complete native process clocks, not the watchdog's rounded elapsed time.
  Peak RSS was 2,970,468,352 / 3,006,267,392 bytes respectively. Candidate
  reservations returned to zero; its preservation stage admitted 1,604 chunks
  and 196,603,506 values. No ingest speedup gate is claimed from this pair.
- Full native comparison passed all 11,199,719,664 values across 112 columns and
  99,997,497 rows, exact schema/row order, and all 413 present semantic footer
  statistics. The comparator does not compare physical zone statistics or user
  metadata; those remain separate from its proof. Candidate SHA-256 is
  `31cc61cfc347cf19a0328c196d59cd1eb431679311294cdc92263fef31062b35`.
- Complete physical inspection changed only the two derived-domain data entries:
  Referer is 324,626,284 bytes and URL is 319,289,332 bytes, down from their
  combined 3,552,545,936 bytes. Both retain 817 data segments and native zone
  inventories; dictionary/code encodings are visible in the persisted artifact.
  Full paired result validation passed all 258 calls (129 per role), including
  final binary/harness/source-generation checks. The initial best-sums were
  104.016458 / 104.299174 s; this is not a suite speedup. Unrelated IEC Git
  repacking consumed multiple cores during the suite. Q17, Q23 and Q34 crossed
  the 10% and 150 ms regression-screen thresholds and require focused paired
  follow-up before retention. All samples and complete outputs are retained in
  `paired43_20260926T164906767815Z`.
- All six focused native tests passed, including renamed/null/Unicode/duplicate
  dictionary values, epoch changes, unsigned-code consumer activation,
  byte-identical non-admitted paths, all-null codes with unused dictionary values,
  and late source failure after an admitted chunk with final reservations at zero.

## Acceptance

On the Apple M5 / 10-CPU / 16-GiB Mac, the ordinary portable release binaries
produced these complete process times. No cache flush was performed; background
system activity and earlier IEC Git repacking were outside the runner's control.
All samples remain available. Per the frozen rule, compare fastest valid calls
symmetrically; do not treat these as latency distributions or cold-cache results.

| Role | Three valid ingest calls (s) | Best (s) | Artifact bytes |
| --- | --- | --- | --- |
| Control | 90.605667, 93.733527, 124.916838 | 90.605667 | 18,591,586,804 |
| Candidate | 88.416283, 104.059129, 132.527161 | 88.416283 | 15,682,956,116 |

Every repeat's full artifact hash matched its role's retained artifact. Only the
verified duplicate was removed, retaining the timing and cleanup receipt. The
candidate saves **2,908,630,688 bytes (15.644876%)**. The 10% ingest improvement
gate is not met; the storage gate is met with no slower fastest valid ingest.

The reversed-order query follow-up passed all 18 calls. Control/candidate bests
were Q17 **2.693855 / 2.837964 s**, Q23 **5.791652 / 4.923060 s**, and Q34
**6.408723 / 6.304487 s**. None reproduced the material regression screen.
Q17's pooled historical minima still favor the original control (2.482663 versus
2.837964 s); that sample is retained. A predeclared two-file check then ran both
binaries three times against each identical file. Control/candidate bests were
**2.996843 / 3.174864 s** on the control file and **3.091442 / 2.902519 s** on
the candidate file. The direction changed, and neither crossed the combined
10%/150-ms threshold. Q17's data columns were unchanged in the physical inventory.
The original Q17 flag is classified as not reproduced by these bounded checks,
not deleted or converted into a speedup claim. These checks do not establish
production tail latency or eliminate host variation.

Retention rests on the storage reduction, complete value/schema/statistics proof,
all 258 Full43 calls plus 30 focused calls, independent fixtures, and the absence
of a reproducible material query regression in the follow-up. The initial Full43
104.016458 / 104.299174 s best-sums remain separate from targeted follow-ups;
do not splice them into a new suite score or replace older measurements silently.

Validation passed: workspace fmt, Clippy and 3,425 tests; native-feature library
tests (1,909 passed, 10 intentionally ignored), six focused dictionary tests,
1,146 CLI/SQL/public-workflow/resident-worker tests, and native CLI/Vortex
all-target Clippy. The all-null test was added after the full native-library run
and passed in the six-test focused run. Subsequent source changes are tests and
documentation only; the runtime matches frozen candidate `c79aa89a`.

The [machine-readable receipt](../benchmarks/derived-dictionary-preservation-2026-09-26.json)
records commands, identities, all samples, proof limits and validation logs.
The next ranked experiment is R6.a; remeasure its actual remaining intermediates
against this retained representation before implementing a consumer change.
