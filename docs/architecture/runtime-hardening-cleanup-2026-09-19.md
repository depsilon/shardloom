# Runtime lifecycle hardening and plan reconciliation

Status: complete and locally validated on branch
`codex/runtime-hardening-cleanup-20260919`, based on `f5163364`. This batch is
ready for PR review; it is not part of the already-published 0.2.4 artifacts.

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
the link fails atomically when the destination exists. Use the same exclusive
creation for replacement publication and rollback, so a competing destination
is preserved. Revalidate destination metadata and symlink policy before moving
an admitted old target, and preserve a backup if rollback cannot restore it.
Probe hard-link support with the staged file before moving an existing output,
so an unsupported filesystem leaves the original destination in place.
Report the actual commit operation and leftover staging/backup state.

This is visibility and collision handling, not a file/directory fsync durability
guarantee or a filesystem snapshot. Metadata checks cannot exclude every hostile
parent-directory or in-place mutation race. Filesystems that cannot create hard
links fail explicitly; there is no overwrite-capable rename retry.

Provider references: Rust documents that [rename replaces an existing target](https://doc.rust-lang.org/std/fs/fn.rename.html)
and that [hard_link fails when the new link already exists](https://doc.rust-lang.org/std/fs/fn.hard_link.html).

## Completed acceptance

- Deterministic competing destination, replacement and rollback tests through
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

The final source/test file hashes and logs are in
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

Seven deterministic publisher regression tests cover creation during production,
creation after final validation, modified destinations, failed rollback with
both owners preserved, restoration, symlink insertion, and unsupported hard-link
filesystems. Existing success/error/overwrite publisher tests also passed.
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

Independent review found the unsupported-filesystem replacement failure case;
the pre-move probe and regression test resolved it. The follow-up review found
no further actionable issue. macOS native test linking emitted the existing
large unwind-table warning; test exit codes were zero. No new full-size ingest,
Full43 timing control, cross-platform runtime certification or release is claimed.

## Next performance decision from existing evidence

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
