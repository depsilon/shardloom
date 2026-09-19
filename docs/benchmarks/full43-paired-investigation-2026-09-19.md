# Full43 matched performance investigation

Status: experiment registered before execution. This resolves the maintainer's
question about the 95.927383 s query acceptance sum versus the historical
91.825940 s, before further profiling or PR work. The
[implementation packet](../architecture/performance-ship-drop-2026-09-19.md)
preserves those observations and the accepted Q13/Q36 mechanisms.

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

The repository has no Rust runtime changes for this investigation. Prior complete
workspace/native gates remain applicable to the frozen binaries. New Python
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
