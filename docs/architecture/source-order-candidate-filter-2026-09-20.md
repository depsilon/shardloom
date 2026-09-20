# Retained source-order key filtering

Status: candidate admitted for implementation and paired validation; no speedup
claimed. This continues PERF-04/05/10/12 after the Q34/Q35 reconciliation screen.
Q17 already uses complete-key partitions and receives no duplicate repartition
implementation. Q18's retained best complete call is 3.289600 seconds on runtime
`3e3b887f`, including 2.682916 seconds of accessor work: 1.772460 seconds of UTF8
dictionary construction and 0.822092 seconds of native UTF8 provider work. These
are disjoint caller spans inside accessor work, not additional wall time.

The existing source-order COUNT route fixes its first K complete groups, then
continues counting only those groups. Once admission closes, a numeric component
absent from every retained key proves the complete key cannot contribute. Apply
that conservative membership test before constructing the UTF8 accessor. Keep
the existing complete numeric/text equality and checked counts for survivors.
The ten retained Q18 compound keys have counts of 1–26, totaling 44. Numeric
selectivity across other text values remains unmeasured; the candidate records
survivor counts. Dictionary construction alone provides the initial opportunity,
without assuming the native provider avoids full child canonicalization.

Scope: the existing non-null identity integer/UTF8 pair COUNT family, after the
existing direct source-order route has closed admission, with at most 64 retained
groups and no residual row selection or offset. Ordered, HAVING, DISTINCT, nullable,
transformed, unclosed and larger retained-key states retain existing execution.
The first chunk continues to establish groups in source order. Offset queries
retain existing execution and are not admitted by this candidate.

Vortex-first decision: `use_vortex_native_provider` for pinned Vortex 0.85
`FilterArray`/`Mask`, logical field access, native primitive ownership and selected
UTF8 execution. The new ShardLoom proof uses existing retained exact keys; it does
not create a scan provider, precomputed answer, sidecar, dependency, external
engine or syntax-specific route. No new global partition or scheduler is added.
This applies late materialization and proof-bound work avoidance within each
existing capillary chunk; broader PulseWeave scheduling remains unchanged.

Selected chunks reuse native accessors and the complete-key consumer. Source
generation, cancellation and outer resource handling remain in the existing scan
loop. A chunk-local mask and at most 64 numeric identities add bounded scratch;
provider allocations are not claimed to be fully reservation-owned or an RSS
bound. Errors from executed native providers propagate without retrying another
execution path. Rejected text need not execute; this query is not an integrity
scan of bytes excluded by the retained-key proof. Held-source generation checks
still cover the entire call. Nonempty selections may execute numeric data again;
that cost remains included in complete-call comparisons.
Counters distinguish source rows examined from survivor accessor rows; selection
does not establish avoided physical reads or zero decode.

Retain only for at least one second of complete-call savings or 30% lower OS peak
RSS with nonregressing complete time, using fastest valid paired calls
symmetrically and retaining every sample. Test late duplicate counts, equal
numeric/different text keys, signed/narrow integers, empty/dense selections,
offsets and excluded semantics. A winner requires complete Full43 UAT, workspace
and native checks, review, PR and merge. A failed candidate is removed with its
evidence preserved. Broad PERF/CG gates remain open.
