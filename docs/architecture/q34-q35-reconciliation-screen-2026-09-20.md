# Q34/Q35 reconciliation attribution

Status: diagnostic screen; no optimization or performance claim admitted.

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

The next temporary diagnostic build records partition-local lookup/insertion/
relocation probe counts, initialized slots and moved bytes, and coarse elapsed
spans around table allocation, initialization, relocation and arena growth.
It adds no per-row clocks or shared per-row counters. Retained per-partition
diagnostics survive storage release and are reported after workers drain.
Diagnostic execution times are excluded from ship gates because counters can
change generated code and contention. Remove the instrumentation before an
unprofiled candidate comparison unless separately justified as retained evidence.

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
