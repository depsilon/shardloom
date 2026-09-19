# Runtime lifecycle hardening and plan reconciliation

Status: complete and merged through
PR [#1446](https://github.com/depsilon/shardloom/pull/1446) at `40087458`.
The branch was `codex/runtime-hardening-cleanup-20260919`, based on `f5163364`.
This batch is not part of the already-published 0.2.4 artifacts.

This is the approved post-0.2.4 cleanup and resource/lifecycle batch under
PERF-03/06/08/09/11/12. It does not reopen broad capability completion, parked
topology/codec/binding experiments, or the completed publication train.

## Contract and provider decision

The expert comparator is a columnar-engine maintainer reviewing cancellation,
native-buffer lifetime and atomic file publication under concurrent writers.
Keep the existing Vortex `ArrayIterator`, `write_options`, native allocator,
resident session and worker grants. This is `use_vortex_native_provider` through
the existing feature-gated boundary; no new layout, scheduler, execution engine
or representation is introduced. Vortex produces the staged bytes; ShardLoom's
shared local publisher owns destination admission and cleanup. An external
engine is never invoked and fallback remains false.

Before this batch, the shared publisher used an existence check followed by
`fs::rename`, which could replace a concurrently created file. New destinations now use
same-directory `fs::hard_link` followed by removal of the staging name: creating
the link fails atomically when the destination exists. Existing destinations use
one same-directory `fs::rename` after final metadata and symlink revalidation.
The publisher never removes or moves the old target first, so the published name
remains available across replacement. Failed replacement cleans staging without
a remove/copy/backup retry. Report `atomic_replace_rename_same_directory` for
overwrite and `not_required_atomic_replace` for its rollback status.

This is visibility and collision handling, not a file/directory fsync durability
guarantee or a filesystem snapshot. Metadata checks cannot exclude every hostile
parent-directory or in-place mutation race. Explicit overwrite is not
compare-and-swap: a writer arriving after the final metadata check may be replaced.
Filesystems that cannot create hard links fail explicitly for new destinations;
overwrite does not require a hard-link probe or backup.

Provider references: Rust documents that [rename replaces an existing target](https://doc.rust-lang.org/std/fs/fn.rename.html)
and that [hard_link fails when the new link already exists](https://doc.rust-lang.org/std/fs/fn.hard_link.html).
The [rename manual](https://man7.org/linux/man-pages/man2/rename.2.html) specifies
the existing target's continuous visibility during replacement on Linux.

## Completed acceptance

- Deterministic competing destination, continuous replacement visibility and failed-rename tests through
  the shared publisher; original producer/validation errors remain primary.
- Native ingest failure/teardown and cancellation while work is held at a
  conversion or codec boundary; observe owner release and preserve bounded
  cooperative-cancellation limits rather than claim blocked-I/O preemption.
- Bounded same-session native writer/short-count contention, complete output,
  fixture latency distributions, shared owner accounting and recovery after failure.
- Reconcile stale current-action prose against the completed September 12
  packets, preserving historical measurements and all unfinished whole gates.
- Focused tests, formatter, default workspace Clippy/tests, native feature
  Clippy/tests, minimal-feature checks and affected docs/public-contract checks.

Large full-ingest/Full43 reruns are unnecessary unless a changed runtime layout
or performance claim needs them. Read existing query evidence after hardening
to select a dominant cost and a bounded next decision; no new speedup is claimed.

## Verification record

The original hardening checkpoint's source/test file hashes and logs are in
`/Users/dylan/LocalData/shardloom/runtime-hardening-20260919`:
`final-source-files.json`, `validation.json`, the five validator JSON reports,
and `native-serving-fixture.json`/`.log`. Hashes remained unchanged throughout
the final broad checks. Cargo resolved its output to
`/Users/dylan/.cache/shardloom/cargo-target`. Builds/tests ran serially with a
12 GiB free-space floor, bounded logs and a per-command watchdog.

| Check | Result |
|---|---|
| `cargo fmt --all -- --check` | Passed. |
| `cargo clippy --workspace --all-targets -- -D warnings` | Passed. |
| `cargo test --workspace --all-targets -- --test-threads=1` | 3,424 passed, zero failed/ignored. |
| `cargo clippy -p shardloom-cli -p shardloom-vortex --all-targets --features release-user-surfaces -- -D warnings` | Passed after correcting two fixture lints; the failed log is preserved. |
| `cargo test -p shardloom-cli -p shardloom-vortex --all-targets --features release-user-surfaces -- --test-threads=1` | 3,323 passed, zero failed; nine pre-existing manual cases ignored. Counts overlap the default suite. |
| `cargo check --workspace --no-default-features` | Passed. |
| `cargo clippy -p shardloom-vortex --all-targets --no-default-features --features vortex-write -- -D warnings` | Passed. |
| Local-output scope, public-status, workspace-version and public-claim validators | Passed. |
| Architecture tracker with existing CI `--allow-blocked` | Expected blocked state: exactly 116 open phase items, zero runtime-gap blockers. |

The original seven publisher tests covered creation during production,
creation after final validation, modified destinations, failed rollback,
restoration, symlink insertion, and unsupported hard-link filesystems. The
pre-merge review correction below replaces the backup-specific cases.
Existing success/error/overwrite publisher tests also passed.
The held-codec case performs real Zstd work before its fixture gate and then
proves cancellation prevents publication and drains native credits. Existing
source-generation, EOF, skew, denial and teardown cases passed in the native suite.

The new serving case runs 96 short calls for each P1/P4 successful/cancelled
writer combination (384 measured fixture calls). It reopens and compares every
value after success, rejects publication after cancellation, and verifies final
owner release. The log records p50/p95/p99 and maximum queue residence; its
deliberate gate and tiny workload make these fixture observations, not production
latency targets. It covers the existing same-session native writer, not a shared
global budget with the separate Parquet conversion pipeline. FIFO fairness,
preemptive blocked-I/O cancellation, provider allocations outside the native
allocator and production resource envelopes remain broader open requirements.

The original independent review found an unsupported-filesystem replacement
failure case. A later PR review found that moving the old target to a backup
still created a missing-name interval before publication. The current single-
rename replacement removes that interval and the entire backup/probe protocol.
macOS native test linking emitted the existing
large unwind-table warning; test exit codes were zero. No new full-size ingest,
Full43 timing control, cross-platform runtime certification or release is claimed.

### Pre-merge atomic replacement correction

PR #1446's follow-up changes only the shared output publisher, its regression
tests and documentation. Seven focused cases cover producer-time changes,
post-check new-file collisions, symlink insertion, a reader observing the old
target immediately before the single replacement syscall, injected rename
failure, and a real missing-staging rename failure. Successful replacement
reports the actual operation and leaves no sidecars. Query kernels and stored
benchmark samples remain unchanged; the full-source/binary identity statements
in earlier evidence describe their recorded checkpoint, not this later fix.

The corrected publisher passes formatting, workspace and native-feature Clippy,
3,424 workspace tests and 3,333 native-feature tests (nine existing manual cases
ignored; the suites overlap), plus the ten documentation/architecture checks
above. Independent review found no remaining actionable publication defect.
An initial validation wrapper timed out and its log overlapped a subsequent run;
that log includes an input-fixture setup failure and is excluded from acceptance.
Fresh, individually tracked serial runs have zero failures. Their exact commands,
exit codes and log hashes are recorded in
`/Users/dylan/LocalData/shardloom/ship-drop-20260919/atomic-replace-final-receipt.json`.
No new full-size query or ingest timing is attributed to this publication fix.

## Historical next performance decision from existing evidence

The following decision led to the now-executed
[ship/drop implementation packet](performance-ship-drop-2026-09-19.md). Its Q29
owned-partial candidate was dropped; Q36 and Q13 were retained. The phase plan
owns the current next action. The timings below remain historical evidence for
the original hardening decision, not a second active queue.

The completed source `4f2c7b97007864d0396b10bdc5dc2bbfef52df38` records
129/129 exact Full43 results and a 91.825940038-second sum of per-query best of
three. The five largest contributors are Q29 9.299822209 s (10.128%), Q19
8.705121375 s (9.480%), Q36 6.543806583 s (7.126%), Q13 6.337156333 s
(6.901%) and Q33 5.149699708 s (5.608%). They sum to 39.243% of that metric.

The frozen summary is
`/Users/dylan/LocalData/shardloom/clickbench-100m-uat/logs/full43_20260913T000635689641Z/summary.json`.
Its `queries_sha256` matches the current
[canonical statements](../../benchmarks/clickbench/queries.sql); the harness
numbers statements from one. Q29 is the `REGEXP_REPLACE(Referer, ...)` domain
grouping with `AVG(length(Referer))`, `COUNT(*)`, `MIN(Referer)`, HAVING and
ordered Top-K. The preceding CounterID/URL-length query is Q28.

Next target: attribute Q29's remaining transform, dictionary binding, weighted
measure updates and final reduction using the existing shared consumer. The
elapsed time identifies a large query, not which internal stage dominates.
Do not restart rejected transform memoization, persist query answers, or add a
query-number-specific route. Only implement a candidate after locating repeated
work with a credible saving of at least one second on this query, then screen
complete results and memory/storage costs before broader acceptance. Renamed
schemas, Unicode/NULL/empty values, exact MIN/order and floating accumulation
semantics must remain unchanged. No new query run or speedup is claimed here.

The subsequent [domain-transfer research](performance-domain-transfer-2026-09-19.md)
extracts existing first-run counters and audits the retained source. It identifies
accessor construction as the largest recorded Q29 first-pass span and proposes
reusing owned string-count partials before another transform-only experiment.
That span still needs internal attribution; it is not exclusive CPU time or a
measurement of the hardening binary. The research leaves this decision gate open.
