# Q34/Q35 reconciliation attribution

Status: growth/probing screen complete; temporary instrumentation removed.
No optimization or speedup claimed. Table-growth-only, arena-copy-only and
validation-only proposals remain dropped for lack of a demonstrated material gate.

This follows Q29 PR #1452 under the existing performance queue. The retained
runtime is `3e3b887f2e6ada7be88751931776fb03a1a55852`; current unpaired Q34/Q35
best calls are 5.383924 / 4.851808 seconds on the unchanged 99,997,497-row native
Vortex artifact. Full43 evidence is `full43_20260920T093549118609Z`.

Six complete-result-checked stack samples in `full43_20260920T094739754196Z`
show unresolved inlined reducer work alongside blocked partition waits. Their
worker observations cannot be added into wall time or treated as CPU time.
Disassembly identifies replacement-slot zeroing, occupancy probing, cached-hash
relocation and two distinct byte-copy sites, but sampled instruction offsets
were collapsed. UTF8 validation or byte-arena relocation alone does not yet
establish a one-second opportunity.

The temporary diagnostic build recorded partition-local lookup/insertion/
relocation probe counts, initialized slots and moved bytes, and coarse elapsed
spans around table allocation, initialization, relocation and arena growth.
It added no per-row clocks or shared per-row counters. Retained per-partition
diagnostics survive storage release and are reported after workers drain.
Diagnostic execution times are excluded from ship gates because counters can
change generated code and contention. The instrumentation was removed before
continuing to an unprofiled candidate comparison.

Vortex-first provider check: `implement_shardloom_kernel` instrumentation of the
existing ShardLoom-owned exact grouping directory. Reuse pinned Vortex 0.85 native
scan, VarBinView and owned partial providers. No new decode, storage encoding,
dependency, fallback, upstream API or user capability. Existing leases, complete
counts, full byte equality, cancellation and pressure transitions stay intact.

Only new evidence establishing at least one second of credible complete-query
savings, or at least 30% lower OS peak RSS with nonregressing complete time,
admits a runtime candidate. Preserve all comparable calls; use fastest valid
calls symmetrically. Winners require Full43, broad checks, PR and merge. Failed
prototypes are removed with evidence retained. Topology, coalescing, universal
compact state, codecs, PGO and native Python binding remain parked.

## Diagnostic result

Frozen diagnostic `6a17dbb2f6e280a8cab7c137018b3c597f011508`, SHA-256
`e2a9a3fb76de934ce3219c696a686b4cf61c61dd5d2f8b4380e9482c6d45f1cf`, passes
all six complete-value checks in `full43_20260920T102113139193Z`. Each call
preserves 99,997,497 committed rows and 18,342,019 complete groups, with no
native handoff or retry. All archives/results/binary identities are replayed by
external `audit-q34-diagnostic.py`; the full diagnostic patch and receipts remain
under `/Users/dylan/LocalData/shardloom/ship-drop-20260919`.

Lookup averages are 1.6639–1.6648 visited slots including the terminal slot;
insertion averages 1.7981–1.7984 and relocation averages 1.1668. Credit refill
rechecks are included. Each call initializes 134,216,704 slots cumulatively
(4,294,934,528 bytes at 32 bytes/slot) and copies 3,374,058,173 newly admitted
string bytes. Repeated arena relocation is measured separately.

| Query/run | Table initialization | Cached-hash relocation | Arena relocation |
| --- | ---: | ---: | ---: |
| Q34/1 | 0.805 s | 0.487 s | 1.998 s |
| Q34/2 | 1.026 s | 0.480 s | 1.117 s |
| Q34/3 | 1.199 s | 0.532 s | 1.062 s |
| Q35/1 | 1.017 s | 0.486 s | 1.066 s |
| Q35/2 | 0.719 s | 0.476 s | 0.877 s |
| Q35/3 | 0.746 s | 0.567 s | 1.026 s |

These are **summed worker elapsed spans**, not complete-query savings or CPU
time. They overlap across workers; instrumentation can alter instruction layout
and contention. No pathological probe count is observed, and this evidence does
not establish that a growth-only or copying-only replacement saves one second
of the complete call. It does not prove those improvements impossible.

The bounded screen is closed without shipping its counters or introducing a new
state representation. The next attribution moves to Q17/Q18's numeric/text
compound grouping against the latest retained runtime. Broader memory/serving
and PERF/CG obligations remain open.

Verification of the temporary build: 17 focused partition tests passed (one
existing benchmark fixture ignored), native Clippy and formatting passed,
independent review found no pre-run blocker, and all six full-size complete
values passed. The final runtime files are restored exactly to merged main;
this diagnostic screen is not a new Full43 or runtime speed claim.
