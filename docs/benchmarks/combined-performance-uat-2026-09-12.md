<!-- SPDX-License-Identifier: Apache-2.0 -->

# Combined performance candidate UAT

Frozen implementation: `4f2c7b97007864d0396b10bdc5dc2bbfef52df38`, based on
merged main `5e8af695c02459be4fe7c6d3c49d3459d72a103f`. Source package version
is `0.2.3`. This packet covers the four selected additions: UTF8-grouped integer
DISTINCT workers, owned integer COUNT, prepared/owned UTF8 COUNT, and exact
scalar integer footer aggregates. Accepted ingest and other retained runtime
work are inherited from main; broad PERF/CG completion remains open.

Status: full UAT for this selected performance scope passed: 1,310 accepted
protocol executions, including 20 expected overflow diagnostics, in addition to
the full artifact comparison and implementation tests. No versioned package publication
proof or historical timing-control promotion is claimed.

## Source and execution identity

Ordinary release CLI SHA-256:
`3af1c45d90b6466a0af26ee205cfff4219bffce61880b444bfd30b28a5f6feeb`.
Build receipt `phase-resume-4f2c7b97-build.json` has SHA-256
`5fc28656dcf90d8bbb47a71459934da96a4f8ec514490432e8e423f79a56ec8f`.
The build used Rust/Cargo 1.98.0 on `aarch64-apple-darwin`, ordinary release,
without PGO or RUSTFLAGS overrides:

```sh
cargo build --release -p shardloom-cli -p shardloom-vortex \
  --features release-user-surfaces --bin shardloom \
  --example owned_aggregate_cost --example resident_latency
```

Native work ran serially. Source, binary, helper and input identities are bound
in immutable machine-local receipts under
`/Users/dylan/LocalData/shardloom/perf-all-20260906` (EVID below).
Cargo output remained in `/Users/dylan/.cache/shardloom/cargo-target`.
Large artifacts remained under the guarded local UAT root, with unchanged
100 GiB workspace / 256 MiB log limits and 12 GiB free-space floor.

## Correctness and lifecycle acceptance

| Check | Result |
|---|---|
| Formatting and workspace Clippy | PASS: `cargo fmt --all -- --check`; `cargo clippy --workspace --all-targets -- -D warnings`. |
| Workspace tests | 3,417 passed with `cargo test --workspace --all-targets -- --test-threads=1`. |
| Native feature tests | 3,321 passed, nine existing manual fixtures ignored; `cargo test -p shardloom-vortex -p shardloom-cli --features release-user-surfaces --all-targets -- --test-threads=1`. |
| Focused and feature checks | COUNT/DISTINCT/footer/prepared/metadata/compound and owned-cost example checks passed; native all-target and minimal Vortex Clippy passed. Counts overlap the broad suites. |
| Owned result protocols | 552 accepted executions: 184 integer COUNT, 184 UTF8 COUNT and 184 existing integer DISTINCT controls. Complete values, physical types and return to captured reservation baselines passed. |
| Renamed DISTINCT fixtures | 96 complete exact results across duplicate-heavy, mostly-unique and skew fixtures; P1/P12, all-groups and offset/ties, worker/pair/group/row conservation. |
| Independent held-out fixtures | 460 accepted executions: 440 complete-value results and 20 expected checked-overflow diagnostics across P1/P2/P4/P8/P12. Both arms read candidate-prepared fixtures; these are independent fixture oracles, not the full-size input. |
| Fresh native artifact | All 99,997,497 rows × 112 columns: 11,199,719,664 values and native schema equal to the protected reference. All 560 loaded footer slots agree; 413 facts present, no column with vacuous statistic coverage; tasks drained and reservations returned to zero. |
| Full43 | 129/129 complete exact typed results, including finite binary64 signed zero. All 129 raw outputs, 43 retained references and native timing sidecars independently rechecked. |
| Matched query screens | 48 core calls plus 16 Q19/Q7 calls passed complete-value parity. The predeclared six-query feasibility gate passed. |
| Public UTF8 COUNT sessions | PASS: nine exact calls (279 rows / 558 cells) through fresh CLI, persistent CLI and Python client. Each retained session opened once and completed fresh executions 1/2/3; subsequent calls reused lowering and skipped a new footer open. |
| Static contracts | Governance, workspace versions, public docs, CI matrix and package catalog passed. Architecture remains explicitly blocked by 116 open phase items; local package/supply-chain/publication evidence is separate. |

Native feature tests also cover source-dropped COUNT persistence through Vortex,
Arrow IPC and Parquet, Unicode and empty output, ownership/slices, source
replacement, pressure, footer read avoidance versus disabled statistics,
aliases/HAVING and CSV/JSONL output. No broad unrelated matrix was substituted
for the newly changed behaviors.

## Fresh ingest and full workload

Fresh guarded Parquet ingest used P4 / 24 GiB, a 24 GB output ceiling and
24 GiB admission reservation. Native elapsed time was **95.923669 s**, peak
native RSS **2,825,338,880 bytes**. The new Vortex artifact is
**18,591,586,804 bytes**, SHA-256
`7181c2e578659910da176ff6c0dcfe7ce563405337f3ae88cd44e7932d92a266`.
It matches the previously accepted numeric-ingest bytes. Four constructed
owners and final zero shared reservations were observed; configured owners
are not proof of simultaneous CPU activity or an OS-RSS cap.

Full43 ran on this new artifact at P12 / 24 GiB:

| Metric | Seconds |
|---|---:|
| Sum of per-query best of three | 91.825940 |
| Sum of per-query best of runs 2–3 | 92.488320 |
| Sum of all 129 native calls | 281.793539 |

Peak native RSS was 5,829,623,808 bytes. Timing includes process creation,
complete output and exit; preparation and oracle verification are outside the
native clock. The 91.825940-second score is not the wall time for all 129 calls.
Full43 is parity against retained ShardLoom outputs, not an independent SQL
oracle. Footer parity preserves loaded facts, not independent recomputation or
physical-zone/user metadata equality. Cache state was uncontrolled; there was
no matched Full43 control, and historical 91.215296 s remains the timing control.

## Matched runtime and representation results

The core screen uses the protected `perf-current-c71a558e.vortex` for both
frozen binaries, comparing accepted `2ad143da` with `4f2c7b97`. One warmup pair
is excluded, followed by three alternating measured pairs per query at P12 /
24 GiB. Q14 requires median candidate/baseline ratio ≤0.80; each control ≤1.05.

| Query | Baseline median s | Candidate median s | Median paired ratio |
|---|---:|---:|---:|
| Q11 | 0.851606 | 0.834004 | 1.007104 |
| Q14 | 7.968275 | 1.554247 | 0.192701 |
| Q17 | 3.281191 | 3.336291 | 0.996560 |
| Q23 | 4.685933 | 4.694881 | 1.001910 |
| Q34 | 4.666016 | 4.203373 | 0.890578 |
| Q35 | 4.567489 | 4.782166 | 1.040576 |

All declared median gates pass. Q35 still has a +4.06% paired median and a
+16.39% worst pair; Q17 has a +11.78% worst pair. Three pairs are feasibility
evidence, not a confidence interval or proof of general timing stability.
The separately scoped 16-call Q19/Q7 screen reports Q7 medians
42.049459 → 13.536334 ms and Q19 8.618088 → 8.615336 s. All four candidate Q7
calls prove two measures from four exact statistics, with zero payload arrays
and visited rows. The native I/O certificate is certified; the separate full
execution certificate reports `not_available`. The Q7 benefit is small
relative to the full workload; it does not justify an overall speed claim.

At 32,768 output groups, same-binary JSON/owned median ratios are 6.354× / 8.630×
for integer COUNT and 2.743× / 3.505× for UTF8 COUNT at P1/P4. Existing integer
DISTINCT has 3.024× / 3.847× large-output ratios, while K32 is about 0.1–0.2%
slower. All small-output controls and raw samples remain included. These clocks
include execution, result construction and drop, excluding preparation and
complete-value validation; they compare representations, not source revisions.

All 12 renamed DISTINCT cells beat the baseline (median paired gains
1.501–3.952×). Candidate P12 is slower than P1 for duplicate-heavy/skew fixtures
and faster for mostly-unique pairs. Worker completion identities do not establish
simultaneity. No general scaling, zero-copy or superiority claim follows.

## Failure preservation and receipt index

The first Full43 attempt stopped after 86 validated results because canonical
logs exceeded its reserved limit: 247,476,224 > 247,463,936 bytes. Its final
native query exited zero but was not counted as validated. The original failed
receipt, all outputs and input remain preserved. After exact process/group
absence checks and durable removal of only the dead owned lock, a new complete
Full43 run passed. Its 129 results and totals exclude the interrupted prefix.

Headroom came from verified lossless archives of closed transcripts and four
old summaries, plus retirement of one explicitly identified reproducible slower
text-storage payload. That 15,466,554,020-byte payload was not archived; its
source, frozen producer, recipe and receipts remain. Official source, protected
reference and released v0.2.3 artifacts were untouched. Limits were not raised.

Key immutable EVID receipts:

- `phase-resume-performance-pr-uat-r1.json`, `phase-resume-broad-uat-r1.json`,
  `performance-pr-static-uat-r1.json`.
- `owned-cost-guarded-resume-4f2c7b97-{count-integer,count-utf8,count_distinct-integer}-fulluat-r1.json`;
  `utf8-distinct-4f2c7b97-fulluat-r1.json`.
- `performance-pr-ingest-4f2c7b970078-r1/receipt.json`: completed ingest/parity
  and preserved failed Full43, SHA `c6ae871269571f5e7e71def879721fa3ef1c7682cb995e2e003d1ab8a498ce04`.
- `performance-pr-full43-recovery-4f2c7b970078-r1/receipt.json`: complete new
  Full43, SHA `f588c4c0e37a3412fe99a7a796ad35b08e8791fb7ecee30aedded5d348fd0619`;
  summary SHA `caf2d8cfd1760db20f6438814e802941421033a1a93ad73f5069ac96767d281b`.
- `phase-acceptance-4f2c7b97-{heldout-json,heldout-distinct-workers}-fulluat-r1.json`.
- `phase-query-4f2c7b97-p12-{fulluat-core-r1,fulluat-footer-r1}.json` and
  `focused-query-gate-4f2c7b97-p12-fulluat-core-r1.json`.
- `public-utf8-count-4f2c7b97-fulluat-r1.json`: nine public calls with complete
  values, source reuse and all five owned native processes/groups drained.
- `performance-pr-cost-generic-4f2c7b97-20260912.{json,md}` and
  `performance-pr-fresh-full43-audit-20260912-r3.{json,md}` retain raw clock tables,
  helper/source identities and independent rechecks.

The guarded drivers and their exact commands remain alongside these receipts.
Reproduction must use local unsynced storage, resolve Cargo output explicitly,
retain excluded warmups and full-value oracles, and keep the same resource guards.
This is implementation UAT for the selected PR, not evidence for future versioned
packages, publication channels or unfinished capability/production gates.
