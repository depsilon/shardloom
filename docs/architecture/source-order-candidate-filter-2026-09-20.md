# Retained source-order key filtering

Status: retained after paired material gate, Full43, broad validation and review.
This continues PERF-04/05/10/12 after the Q34/Q35 reconciliation screen.
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
The ten retained Q18 compound keys have counts of 1–26, totaling 44. Before
implementation, numeric selectivity across other text values was unmeasured;
the paired and Full43 evidence below now establishes 27 survivors after the first
chunk. Dictionary construction supplied the initial opportunity without assuming
the native provider avoids full child canonicalization.

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

## Paired full-size result

Frozen candidate `b6b92497d7ec1fff26caa4411512320d9f0fc97f`, SHA-256
`e050bec18f77b9c7142bdf7a3d3d14951d0c9d01b7bb0e9c422cfeeb5b9ff205`, was built
with `cargo build --release -p shardloom-cli --features release-user-surfaces`.
Control is the retained `3e3b887f2e6ada7be88751931776fb03a1a55852`, SHA-256
`0deff1e7d58857aeb24cc32154ab81d0095defae843f6e152ea6a34c979a597e`.
Guarded run `paired43_20260920T103747961933Z` uses the unchanged 18,591,586,804-byte
native Vortex artifact with 99,997,497 rows, Apple M5 / 16 GiB physical RAM,
macOS 27, 24 GiB policy and requested P12 (host ceiling 10). Policy memory is not
an OS RSS bound. Each call includes fresh CLI startup, full output and exit.
Host load and cache are uncontrolled; no cold-cache or official ranking claim.

| Run | Control complete seconds | Candidate complete seconds |
| --- | ---: | ---: |
| 1 | 3.298828 | 0.932169 |
| 2 | 3.317587 | 0.266880 |
| 3 | 3.300233 | 0.267326 |

Fastest valid complete calls save **3.031948 seconds (91.9%)**. Their OS peak RSS
is 294,682,624 / 270,663,680 bytes (8.2% lower); the time gate determines retention.
Every candidate call returns the same ten complete groups and 44 total counts.
After the first 65,536-row chunk, the filter examines 99,931,961 rows across 1,549
chunks and retains 27 rows. Only eight later chunks need string access. UTF8
accessor rows fall from 99,997,497 to 65,563; dictionary entries from 8,571,186
to 2,517; copied dictionary bytes from 518,939,670 to 143,334. These are cumulative
work counts, not peak memory or physical read savings.

All six complete-value references pass. External `audit-q18-filter-paired.py`
replays archived outputs, binary/member/result hashes, timing receipts and the
symmetric fastest-call gate. Complete commands and all samples are retained under
`/Users/dylan/LocalData/shardloom/ship-drop-20260919`; references are previously
validated complete results, not a new independent oracle. Seven focused tests,
native Clippy and independent adversarial review pass.

## Full43 correctness and timing scope

Guarded `full43_20260920T103833464763Z` passes **129/129 complete-value checks**.
The filter activates only on Q18, which completes in 0.552738, 0.522003 and
0.551984 seconds. Every Q18 call preserves the paired selection and dictionary
work counts. Existing weighted integer partitions still activate on Q16/Q36.

The observed unpaired best-of-three sum is **103.104888 seconds**, versus the
earlier 71.393790 seconds. Twenty-three queries exceed the supplementary 0.15s
and 10% increase screen. These unmatched runs do not establish causal regressions
or overall suite gains; all raw samples and screen flags remain preserved. The
maintainer's symmetric fastest valid matched-call rule determines Q18 retention;
slower shared-host samples alone do not veto that gate. No new ingest, storage,
production fairness or whole-suite speedup is claimed.

`audit-q18-filter-full43.py` verifies all compressed and raw log hashes, timing
receipts, binary/source/query identities, full output references and score
recomputation. It checks that only Q18 activates the filter. The
[machine-readable packet](../benchmarks/source-order-candidate-filter-2026-09-20.json)
links both full-size runs, all validation receipts and the unpaired screen.

## Final validation

- `cargo fmt --all -- --check`: passed.
- `cargo clippy --workspace --all-targets -- -D warnings`: passed.
- `cargo test --workspace --all-targets`: 3,424 passed.
- Native `release-user-surfaces` Clippy for CLI/Vortex with all targets: passed.
- `cargo test -p shardloom-vortex --lib --features release-user-surfaces`:
  1,859 passed; nine existing benchmark fixtures ignored.
- Seven focused retained-key selection tests: passed.
- Public claim language, public status and workspace version validators: passed.
  Architecture tracker exits zero with `--allow-blocked`, retaining 116 unchecked
  phase items; this is not broad capability readiness.
- Independent adversarial review: no remaining correctness blocker.

The final runtime source matches the frozen candidate. No version bump or package
publication is included. Further compound grouping work requires new attribution
under the existing material gate.
