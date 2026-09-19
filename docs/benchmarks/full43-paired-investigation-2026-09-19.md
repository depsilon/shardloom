# Full43 matched performance investigation

Status: correctness passes; performance acceptance remains open for Q17. The
experiment was registered before execution to investigate the maintainer's
question about the 95.927383 s query acceptance sum versus the historical
91.825940 s, before further profiling or PR work. The
[implementation packet](../architecture/performance-ship-drop-2026-09-19.md)
preserves those observations and the accepted Q13/Q36 mechanisms.

## Results and current decision

The original full-size UAT did run and passed 129/129 complete results. Its timing
comparison with September 13 was not a matched performance experiment. This
investigation adds **318/318 passing native calls**, including both roles and the
rejected ablation, with every complete result replayed from verified raw archives.
It establishes a lower same-session full-suite score for the retained candidate,
but does not clear the remaining Q17 signal or identify the exact cause of the
earlier 4.47% historical difference.

| Full43 clock | Control `289fa42c` | Retained `69ce65ac` | Reduction |
| --- | ---: | ---: | ---: |
| Sum of each query's best of three | 141.156433 s | 133.708041 s | 7.448392 s (5.28%) |
| Sum of each query's median | 151.527853 s | 143.207853 s | 8.320000 s (5.49%) |
| Sum of best of runs 2/3 | 142.917901 s | 135.888341 s | 7.029560 s |
| All 129 calls per role | 464.395404 s | 435.205672 s | 29.189732 s |

Both binaries were substantially slower than the earlier runs. Q13 remains much
faster in this matched suite (median 8.815571 to 1.267267 s); Q36 is 10.228416 to
7.065157 s. This loaded-host run does not reproduce subsecond Q13 completion.
Keep the earlier subsecond observations scoped to their own host/cache conditions.

The registered screen flags Q11, Q12, Q17 and Q34. Reversed-order medians are:

| Query | Control | Candidate | Screen repeats? |
| --- | ---: | ---: | --- |
| Q11 | 0.855317 s | 0.881974 s | No |
| Q12 | 1.599244 s | 1.673731 s | No |
| Q17 | 5.164006 s | 5.733804 s | Yes |
| Q34 | 7.506358 s | 7.176170 s | No |

Q17 is not dismissed because the suite total improves. Its four control/candidate
blocks have median differences +2.320988, +0.569798, +4.044628 and -4.025641 s.
The last block reverses direction, but three earlier blocks retain the signal.
All 12 samples per role are preserved, with no outlier exclusions or post-hoc
replacement of the Full43 score. The control alone ranges from 4.711854 to
13.624002 s. Removing the extra profiling fields did not improve Q17 in either
ordering; that ablation is rejected. The final runtime is restored byte-for-byte
to the already-validated `69ce65ac` implementation.

Direct checks against the exact historical executable `4f2c7b97` are also mixed:
historical/retained medians are 6.470069 / 6.147163 s in normal order and
5.673144 / 7.513649 s in reverse order. These are targeted diagnostics, not a new
full-suite comparison against the released source. They cannot clear the
unexplained Q17 difference against `289fa42c`.

The measured host shows substantial memory compression and CPU contention. For
example, original Q17 control calls record 1.34–3.04 million host compression
events and range from 10.749 to 24.331 native system-CPU seconds. Reversed Q34's
11.479 s candidate call coincides with 75,837 host swap-ins and 155,645 swap-outs.
Counters are pages/events as reported by macOS, not unique bytes read or copied.
Host counters include unrelated work; correlation does not identify which process
caused the pressure or establish that it explains the entire candidate difference.

**Decision:** preserve the measured B/C changes and useful D attribution, with no
new runtime optimization retained from this investigation. Keep PR/release
performance acceptance and Q19 expansion behind a quieter-host Q17 check. Ask the
maintainer to pause unrelated heavy work; do not terminate their applications.
Then run six paired Q17 comparisons, balanced across both starting orders, using
the same frozen control/candidate and guards. Inspect VM/CPU evidence and retain
every sample. If the material signal persists, isolate B and C or their compiled
layout effects before accepting the branch. No change to memory/P12 settings or
historical baseline is used to make the current gate pass.

The [machine-readable audit](full43-paired-investigation-2026-09-19.json) binds
all eight summaries, all query samples, native CPU/RSS/page faults, host VM deltas,
binary identities, query/harness hashes and raw archive hashes. It also records
the late-comparison archive addendum: those prior closed `.stdout.json.gz` files
are now preserved byte-for-byte inside each directory's `completed-run-logs.tar.xz`.
Historical timing records remain unchanged. No replacement ingest was run.

Validation: 15 query/paired-harness tests pass. Nine public/status/version
validators pass; the architecture tracker exits successfully with `--allow-blocked`
while still reporting 116 unchecked phase items and 36 unchecked global review
items. Source comparison verifies that all runtime crates and Cargo manifests
match `69ce65ac`; its prior full workspace/native gates remain applicable. No
new runtime speedup, PR readiness, merge or publication is claimed.

## Method and decision

Run each of the 43 canonical queries three times on each frozen binary:
control `289fa42c` (SHA-256 `2fc973fac249216dc24f3b39a8b09946df734d1513f6cb945c9fca126038beda`)
and retained `69ce65ac` (`3fa5b72e098b02d3f5c8803df259d6c65d54ce98e636cfdd6ada30a398a24bed`).
For each query, alternate the role order across three pairs; reverse the starting
role on adjacent queries. This is 258 serial native calls, each checked against
the same complete native reference. It prevents one binary from always running
after the other, while keeping comparisons close in time. It is deterministic
counterbalancing, not randomization or a cold-cache benchmark.

Use the same 99,997,497-row real native Vortex artifact (18,591,586,804 bytes),
P12 request, 24 GiB policy budget and source-generation checks as prior runs.
That policy budget is not a measured RSS ceiling. Timing still spans native process
creation through complete CLI output and exit, with user/system CPU and peak RSS.
The existing supervisor now also records OS child page faults/block operations;
block counts are not byte counts and OS accounting varies. Capture host-wide VM
counters and load before/after each operation outside its measured clock. Do not
attribute unrelated host VM work to ShardLoom.

The host reports Apple M5, 10 logical CPUs, 16 GiB physical memory, macOS 27.0
build 26A428, AC power and no recorded thermal/performance warning. The native file
is larger than physical RAM; only requested columns are read, so file size alone
does not prove cache eviction. Compressed-memory and cumulative swap counters
motivate measuring VM deltas; they do not prove active swap during any query.
No system cache purge, power-policy change, unrelated process shutdown, engine
rebuild or concurrent heavy test/build is part of this comparison.

Report each role's sum of per-query minima, sum of per-query medians and all
samples, plus per-pair deltas and CPU/RSS. Investigate a query if its candidate
median exceeds control by both 10% and 0.15 s with at least two positive pair
deltas. Repeat flagged queries with reversed starting order before calling a
regression repeatable. Inspect route/stage evidence for repeatable differences;
fix or remove a regressing change rather than dismissing it as noise. A lower
aggregate score alone cannot hide a material persistent query regression.

The aim is a defensible local ship decision, not a significance or universal
latency claim from three samples. Historical numbers remain distinct. A same-day
paired comparison can separate patch effects from the historical mismatch without
identifying every contributor to host/cache variation.

## Guarded evidence

`scripts/run_clickbench_paired_query_uat.py` reuses the original native supervisor,
complete-value/no-fallback parser, timer and storage guards. It takes the same
exclusive UAT lock. The 100 GiB workspace, 12 GiB free-space and 256 MiB accumulated
log limits remain unchanged. Closed logs are losslessly bundled per query only
after execution/validation, with archive allocation admission, byte readback and
hashes before removing original copies. No old unrelated logs or data are deleted.

The initial frozen-binary comparison introduced no Rust runtime changes. Prior
complete workspace/native gates remain applicable to those binaries. New Python
harness/counter changes require focused tests and independent review before UAT.
Final results, provenance and the ship decision are appended after execution.

## Q17 investigation extension

The Full43 run completed 258/258 correct calls. Q11, Q12, Q17 and Q34 crossed
the registered follow-up threshold. The reversed-order block completed 24/24;
only Q17 crossed it again (control median 5.164006 s, candidate 5.733804 s).
This keeps Q17 open rather than accepting the faster aggregate score.

Before changing runtime code, collect two additional Q17-only blocks, one in each
order, using the same frozen binaries, clock, input and guards. Preserve every
sample from all four blocks, including the initial signal, and report each block
separately. This adds six pairs to the existing six and balances which binary
starts. Compare route, state size, partition work and OS counters alongside wall
time. The extension is diagnostic, not permission to discard the slower samples;
an unexplained repeatable material regression still blocks acceptance.

The two extra blocks pass 12/12 complete comparisons. Q17's normal-order medians
are 9.883091 / 13.927719 s (control/candidate), while the reversed block is
10.609329 / 6.583687 s. This sign reversal demonstrates instability, but does not
erase the three earlier signals. All 12 samples per binary remain in evidence.

Source inspection finds unchanged compound key counting/partition algorithms,
unchanged admitted routes and logical state, and no direct UTF8 accessor calls.
However, candidate D adds 64 bytes of fields to shared numeric work bookkeeping,
including 1,202 Q17 numeric owner observations per run. Its per-chunk zero-valued
counter accumulation is reachable. Actual compiled layout/optimization effects
are unmeasured; source inspection alone cannot acquit the instrumentation.

Next isolate D: build B/C with only the D instrumentation removed, then run Q17
against frozen `69ce65ac` in both starting orders. This is an ablation, not a claim
that D caused the signal. Follow any retained source change with the required
workspace/native checks and complete query acceptance. Do not trade away the
measured Q13/Q36 changes on the strength of uncontrolled historical totals.

The D ablation does not improve Q17: instrumented/D-off medians are 7.668345 /
7.859424 s in normal order and 5.056907 / 5.207413 s in reverse order; all 12
calls match complete references. Reject the removal and restore the useful
attribution. This does not prove zero instrumentation overhead, but supplies no
evidence that removing it resolves the observed signal.

An additional provenance check identifies the original frozen historical binary
`4f2c7b97007864d0396b10bdc5dc2bbfef52df38`, SHA-256
`3af1c45d90b6466a0af26ee205cfff4219bffce61880b444bfd30b28a5f6feeb`.
The intervening production hardening changes workspace publication, which the
successful non-spilling `collect` command bypasses; other ingest changes are test
only. Source reachability does not establish binary/code-layout equivalence.
Compare this original historical binary directly to `69ce65ac` on Q17 in both
orders (six pairs total) to test whether the signal also holds against the exact
historical executable. Keep this diagnostic distinct from the Full43 score.
