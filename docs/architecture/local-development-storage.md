# Local Development Storage

## Scope

Bulk generated files must remain outside cloud-synced folders. A checkout in
Documents does not make its ignored build files exempt from iCloud syncing.
This is an operational safety rule, not a query performance optimization.

On this Mac, the local-only paths configured on 2026-09-05 are:

- Cargo outputs: `/Users/dylan/.cache/shardloom/cargo-target`.
- ClickBench workspace: `/Users/dylan/LocalData/shardloom/clickbench-100m-uat`.
- Resident official source: the workspace's `sources/hits.parquet`.

The existing Cargo output directory and resident source were moved, not copied.
The source retained its 14,779,976,446-byte length and allocated block count.
The historical Desktop evidence and cloud-only Vortex artifact were not moved,
downloaded, deleted, or represented as newly validated benchmark evidence.
Relocation does not itself reduce local disk use or repair an existing iCloud queue.

The source checkout remains at its current configured project path. Its local
`.cargo/config.toml` sets `build.target-dir`; `.git/info/exclude` keeps that
machine-specific setting out of commits. Explicit Cargo environment or command-line
overrides take precedence, so agents must verify the resolved output path.
The [September 26 cleanup](local-artifact-cleanup-2026-09-26.md) records retirement
of obsolete local test/UAT artifacts. Local incremental compilation is disabled
after its debug/test cache accumulated 146 GiB. Preserve frozen comparison
binaries and receipts; retire redundant bulk outputs only after identity checks.
On September 27, 5,822 completed historical per-call log files were compacted
losslessly into three `completed-call-logs-20260927.tar.xz` archives beside their
original summaries. Per-member size/SHA-256 manifests preserve old path lookup;
every archived byte was verified before removing its original. This recovered
about 22 MiB of allocated log space without discarding failed-run evidence or
raising the 256 MiB guard. The C2.b benchmark packet retains the compaction receipt.
The C5 compiler packet additionally retains the verified compaction of 760 old
held-out call logs into `completed-call-logs-c5-20260927.tar.xz`, recovering about
3 MiB of allocated space with the original summary and per-member manifest kept.
Before the October 2 aggregate acceptance, 3,096 completed per-call files from
six successful Full43 cohorts were compacted into
`completed-call-logs-aggregate-20261002.tar.xz` archives beside their unchanged
summaries. Each archive has a per-member size/SHA-256 manifest; every archived
byte and original file identity was verified before removing its original.
This recovered 10,620,928 allocated bytes. Failed-run logs remain intact and the
storage ceilings were unchanged. The aggregate acceptance packet records the
six archive identities, source summaries and full compaction receipt.
The repaired public aggregate rerun subsequently reached its unchanged 192-MiB
log ceiling after 342 passing checks. The earlier complete and interrupted public
cohorts' 3,811 completed call artifacts were archived losslessly, with per-member
hashes and original identities verified before removal. Their summaries and the
storage-error log remain unchanged. This recovered 198,561,792 allocated bytes
before a fresh complete run; the
[aggregate acceptance packet](../benchmarks/evidence/native-aggregate-ordering-2026-10-02.json.xz)
retains both archive manifests and the failed observation.
The subsequent unary-composition public matrix reached the same 192-MiB ceiling
after 846 passing checks, following an earlier fixture-correction run. Its 2,756
completed envelope files were archived losslessly with per-member hashes and
unchanged summaries, recovering 199,368,704 allocated bytes. Both failed
observations remain recorded. The public relational runner now accepts
`--compress-logs`: each new envelope is stored as gzip, read back byte-for-byte,
and recorded with its original and compressed hashes. This keeps the expanded
matrix inside the unchanged storage guards.
Before the nested-composition paired Full43 run, twelve completed profiling
sample logs from three successful historical targeted cohorts were compacted into
`completed-profile-samples-nested-20261003.tar.xz` archives beside their unchanged
summaries. Original file identities, per-member hashes and every archived byte
were checked before removal; archive manifests preserve the original paths.
This recovered 5,074,944 accounted log bytes. Failed and incomplete cohorts,
including the interrupted September 30 paired run, remain unchanged. The nested
acceptance evidence retains the compaction receipt; storage ceilings were unchanged.
Before the October 3 typed-key paired acceptance, 1,032 completed per-call files
from two successful historical Full43 cohorts were compacted into
`completed-call-logs-typed-keys-20261003.tar.xz` archives. Original file identities,
per-member sizes/hashes and archived bytes were verified before removal; summaries
remain unchanged. This recovered 3,264,512 accounted log bytes. Failed/incomplete
cohorts and all storage ceilings remain unchanged. The
[typed-key acceptance packet](../benchmarks/evidence/native-typed-keys-2026-10-03.json.xz)
retains the receipt and verifies every archived member.
Before the October 3 typed-expression review refresh, preflight stopped before
queries because accumulated logs exceeded the 252-MiB admission threshold that
reserves space below the unchanged 256-MiB ceiling. Twelve closed log files from
three successful September 27 count-selection screens were archived losslessly
beside their unchanged completion receipts. File identities, no-open-handle
checks and every archived byte were verified before removing redundant originals.
This recovered 5,943,296 accounted log bytes. Failed/incomplete runs and storage
ceilings remain unchanged; the
[review packet](../benchmarks/evidence/native-typed-expressions-review-2026-10-03.json.xz)
retains the failed preflight, compaction receipt and per-member manifests.
The typed-unary expansion subsequently reaches the unchanged 192-MiB public log
ceiling after 7,048 passing checks, despite gzip compression. The public runner
now accepts `--archive-logs` with `--compress-logs`: it batches up to 128 closed
gzip envelopes into an xz-compressed tar archive, verifies original identities,
every compressed byte, member hashes and a sidecar manifest, then removes only
the redundant originals. Existing archives are never replaced and readback
failures preserve originals. The accepted 9,300-check cohort retains 20,025
envelopes in 157 verified archives under the same ceiling. Both interrupted
public attempts remain intact, including the later resource-evidence reader
correction; all three executable artifacts and frozen oracles are identical
across attempts. The [typed-unary packet](../benchmarks/evidence/native-typed-unary-2026-10-03.json.xz)
reopens every archived envelope and preserves the complete failure history.
The October 4 nested-key public expansion reaches the same 192-MiB ceiling
after 13,717 passing checks and 8,628,943 complete row comparisons. Wrapping
already-compressed gzip members in xz saves too little space at this scale.
The archive helper now checks each closed gzip file's identity and stored hash,
decompresses it, and verifies the original JSON size/hash before archiving raw
JSON members together. It reads every member back before removing temporary
gzip files. Manifests explicitly identify `raw_json` members and retain both
the original JSON hash and the source gzip size/hash; archived JSON bytes are
identical to the original reports, while the gzip wrapper is no longer retained.
Original failed-run archives and summaries remain unchanged. A guarded check of
384 copied reports from three original batches reduces archive bytes from
2,265,960 to 203,656 and verifies every original JSON byte. Corrupt input, source
mutation and failed archive/manifest readback preserve all source files. These
are evidence-storage measurements, not query-performance results; storage
ceilings remain unchanged.
For complete development-folder isolation, relocate the checkout itself to an
unsynced directory in a separate, coordinated project-path migration.

Before the September 27 plain-Vortex format comparison, the superseded
18,591,586,804-byte `performance-pr-ingest-4f2c7b970078-r1.vortex` payload was
retired. Its complete hash matched the retained September 26 evidence, whose
full-value comparison proves all 99,997,497 rows and 112 columns equal to the
current 15,682,956,116-byte artifact. Both hashes and file generations were
rechecked before removal. The original Parquet, current optimized artifact and
protected older `perf-current-c71a558e.vortex` reference remain intact. Replaying
the superseded physical layout now requires regeneration.
Completed Full43 per-call logs were also archived losslessly with per-member
hashes, while summaries and failed-run evidence were preserved. Exact removals,
archive identities and before/after space are recorded in
`/Users/dylan/LocalData/shardloom/release-0.3.1-20260927/format-storage-cleanup.json`.
This supersedes the September 26 note that the 18.59 GB comparison payload remains
locally retained; it does not alter the historical timing or correctness evidence.

Before the fresh October 5 Full43 run, four completed historical Full43 cohorts
were compacted into verified archives of original JSON and companion files.
Existing archive manifests, summary bytes, original file identities and every
member's size/hash were checked before redundant containers or loose files were
removed. The original JSON bytes remain recoverable; superseded gzip wrappers
do not. This recovered 7,122,944 accounted log bytes without changing the
256-MiB ceiling, retaining all failed/incomplete observations. The
[fresh release UAT packet](../benchmarks/release-candidate-fresh-uat-2026-10-05.md)
includes the receipt, prior manifests and new archive identities. The resident
Parquet and new 15,713,610,545-byte Vortex output remain retained.

Before scalar-subquery Full43 acceptance, preflight stopped before queries at the
unchanged 252-MiB log-admission threshold. The latest completed release cohort's
516 per-call artifacts were compacted into a verified archive beside its
unchanged summary. Original identities, no-open-handle checks and every member's
size/hash were verified before removing redundant originals. This recovered
3,362,816 accounted log bytes without raising the 256-MiB ceiling. The
[scalar-subquery acceptance packet](../benchmarks/native-scalar-subqueries-full43-2026-10-05.md)
retains the failed preflight, compaction manifest and all 516 reopened members'
proofs. The continuation reused passed focused checks only after verifying their
receipts, immutable executable and all 901 frozen source hashes. Failed and
incomplete observations remain intact.

Before nested-pivot Full43 acceptance, the same log admission guard stopped
before queries. The completed scalar-subquery cohort's 516 per-call artifacts
were compacted into a verified raw-JSON/companion archive beside its unchanged
summary, recovering 3,362,816 accounted bytes. Original identities, closed-handle
checks and every member's size/hash were verified before redundant originals
were removed. Failed/incomplete evidence and storage ceilings remain unchanged.
The [nested-pivot acceptance packet](../benchmarks/native-nested-pivot-state-full43-2026-10-06.md)
retains the failed preflight, continuation proof and compaction manifest;
finalization reopened all 516 members. The continuation reused semantic and
golden checks only after verifying the executable and all 910 frozen source assets.

Before the October 7 Zstd workspace Full43 acceptance, the unchanged 252-MiB
log-admission threshold again stopped preflight before queries. Two completed
historical Full43 cohorts' 1,032 per-call files were compacted into
`completed-call-logs-codec-workspaces-20261007.tar.xz` archives beside their
unchanged summaries. Original identities, closed-handle checks and every member's
bytes/hash were verified before redundant originals were removed. This recovered
4,517,888 accounted log bytes. Failed/incomplete evidence, resident inputs and
storage ceilings remain unchanged. The
[codec acceptance packet](../benchmarks/native-codec-workspaces-2026-10-07.md)
retains the refusal, compaction manifests and independent reopening of all
1,032 members before accepting the fresh Full43 continuation.

Before native builder Full43 acceptance, the same 252-MiB admission threshold
stopped preflight before queries. Three completed historical cohorts were
repacked from gzip-wrapper archives into verified archives of original JSON and
companion bytes. Original manifests and summaries remain intact; all 1,548
members were reopened and hash-checked before acceptance. This recovered
3,452,928 accounted bytes. The first two-cohort attempt still failed storage
admission, and both that failure and the final successful continuation are
preserved in the [builder packet](../benchmarks/native-builder-resources-2026-10-07.md).
Only redundant completed containers were removed. Failed/incomplete evidence,
resident inputs and storage ceilings remain unchanged.

## Ingest Guard

`scripts/run_clickbench_ingest_uat.sh` defaults to the local-only workspace and
resolves the compiled binary through Cargo metadata unless `--binary` is supplied.
`--uat-root` also relocates the default source and target.

Before creating logs or replacing an artifact, the runner:

1. Rejects macOS Desktop, Documents, Mobile Documents, CloudStorage, and CloudDocs
   paths, including existing symlink aliases. This conservatively rejects Desktop
   and Documents even when their current sync setting is unknown or disabled.
2. Requires output and logs to stay inside the declared UAT workspace.
3. Checks existing workspace bytes plus a candidate reservation against 100 GiB.
4. Reserves the configured maximum artifact size, interpreted conservatively as
   GiB for admission, plus 12 GiB of free disk headroom.
5. Rejects more than 256 MiB of accumulated logs.
6. Takes an exclusive workspace lock before running the writer. A stale lock after
   a crash requires inspection; it is not deleted automatically.

The checks run again at each progress sample and after child completion.
Budget failure stops the native CLI and produces a nonzero result; a child that
ignores normal termination is forcibly stopped after a short grace period.
Interrupting the harness also stops the native process group and releases the
owned lock. A small supervisor records process duration independently of the
watchdog polling interval; `native_process_seconds` is the comparison clock,
while `elapsed_seconds` includes polling and harness overhead.
`native_peak_rss_bytes`, when present, is the OS-reported child-process high-water
mark, with macOS byte and Linux KiB units normalized to bytes. It is measured by
the single-child supervisor after exit, not estimated from progress samples and
not an enforced process-memory limit. Earlier records without this measurement
must not be treated as having zero peak memory.
Source residency checks still happen before removal of an existing artifact.
`--replace-existing` removes only the exact requested target. It does not delete
backups, source files, numbered copies, or unknown staging files. Existing staging
files remain visible to artifact/workspace accounting and require explicit review.

The limits can be set explicitly with `--min-free-gib`,
`--max-workspace-gib`, and `--max-log-mib`, or the corresponding
`SHARDLOOM_CLICKBENCH_UAT_*` environment variables. The existing artifact limit
and runtime limit also remain in effect. The workspace count uses the larger of
logical and allocated file bytes, counts hardlinks once, and does not follow
arbitrary directory symlinks.

These are sampled watchdog ceilings, not an APFS quota or an allocator guarantee.
Writes can overshoot between samples. They do not impose a limit on iCloud,
other applications, other workspaces, or commands that bypass the runner.
No automated deletion of old runs, cloud files, or user data is performed.

## Current runtime observation procedure

The latest complete current-main observation is recorded in
[the September 30 report](../benchmarks/current-runtime-e2e-2026-09-30.md):
65.806017 seconds native ingest, 55.251837 seconds for one Full43 pass,
121.057854 seconds combined native work, and 136.696318 seconds actual
supervised workflow wall time. Keep this single observation separate from the
earlier paired cohorts. It measures merged main after the 0.3.3 release, not
the published package binaries.

For a future authorized observation:

1. Freeze the exact revision, build receipt/features, executable SHA-256 and
   source-file hashes. Resolve build outputs with Cargo metadata if rebuilding.
   Freeze the complete query file, resident input identity/hash and all 43
   reference results. Record hardware, physical RAM and policy settings separately.
2. Give the run a unique manifest, log and target name in local-only storage.
   Admit it through the existing storage limits. Run public preparation through
   `run_clickbench_ingest_uat.sh`, then acquire the same exclusive UAT lock for
   the sequential query pass. Reject overlapping native builds/tests/queries
   with the workload guard before and during execution. Keep process-group
   deadlines and cleanup evidence. Do not raise storage ceilings for a rerun.
3. Capture host load and process CPU/RSS observations before and during the
   run without recording unrelated command arguments. Keep ordinary host
   activity visible and do not describe sampled process CPU as exclusive
   attribution. Record profiler/build overlap or other disturbances explicitly.
4. Time each complete public native process, including startup, full output
   and exit. Also time the actual workflow. Declare the exact treatment of
   input hashing, validation, archiving and final output hashing. Use one call
   per query for a single-pass observation; retain the existing symmetric
   fastest-valid-run rule only for explicitly paired comparative cohorts.
5. Validate every complete result and record no-fallback certificates. Preserve
   raw output, timing, PID, result/reference hashes and every sample. Record
   source prehashing and newly written-input cache effects; do not infer a
   cold-cache run. Compare the complete generated Vortex hash before retiring
   an identical duplicate and verify all owned processes/locks are gone.
6. Add a new immutable report and portable evidence packet, then update the
   phase/profile pointers. Keep historical samples intact. Retire only exact
   superseded artifacts with saved identity/hash, open-handle checks and an
   explicit retained replacement or regeneration record.

The portable packet linked from the report includes the executed driver and
guard helpers. Its local paths use placeholders; any replay must bind them to
resident local inputs and a new run identity. Do not run the historical driver
against its already completed paths. The
[cleanup record](local-artifact-cleanup-2026-09-30.md#current-runtime-follow-up)
identifies the current retained reference and superseded-file disposition.

## Storage guard verification

```sh
python3 -B -m unittest discover -s scripts -p test_local_uat_storage.py -v
bash -n scripts/run_clickbench_ingest_uat.sh
```

Tests cover safe destinations, macOS synced paths, symlink/case bypasses, sparse
files, hardlinks, free space, candidate reservations, workspace and log budgets,
root override behavior, preservation on failed preflight, runaway log output,
exact-target replacement, backup/source preservation, and concurrent-run exclusion.
Fixtures are small and require no engine build.

Every full-size run must remain blocked until its storage admission passes.
Do not infer that the existing CloudDocs backlog is disposable from these checks.

## Provider References

- [Cargo build-cache location configuration](https://doc.rust-lang.org/stable/cargo/reference/build-cache.html).
- [Apple iCloud Drive file management](https://support.apple.com/en-ie/guide/mac-help/-mchl1a02d711/mac).
