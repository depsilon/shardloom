# Material performance ship/drop implementation

This is the implementation follow-up to the approved
[domain-transfer proposals](performance-domain-transfer-2026-09-19.md), within
the existing PERF-04/05/07/10 operator and ownership work. It preserves the
Vortex-native, no-external-engine execution boundary. Query numbers identify
measurements; neither SQL text nor query numbers choose runtime behavior.

## Retained decisions

| Candidate | Decision | Complete-operation evidence |
| --- | --- | --- |
| A: Q29 owned weighted string partials | Drop implementation | Final control median 9.256 s; prototype 10.191 s. Source counting and order restoration replace much of the removed representation work. |
| B: Q36 physical-key proof through worker admission | Retain | Same-day control median 7.371 s; retained median 4.942 s: 2.428 s (32.95%) lower. |
| C: Q13 filtered complete-key exact counts | Retain | Same-day control median 6.242 s; retained median 0.666 s: 5.576 s (89.32%) lower. |
| D: Q23 accessor rewrite | Drop proposed rewrite; retain attribution | Of a 4.231 s first-run accessor span, 4.205 s is provider execution and 0.006 s is dictionary construction. Eliminating that dictionary construction cannot supply the one-second gate. |

The Q13/Q36 comparison medians use six control samples from two complete blocks and
three retained samples from the final complete candidate block, separately for
each query. Blocks alternate control/candidate; these are not randomized paired
trials. Every included result passed complete-value comparison. The interrupted
candidate block and exploratory screens remain separate evidence.

Q13's complete candidate block is 0.677 / 0.666 / 0.661 s. The earlier corrected
screen is 1.277 / 0.666 / 0.713 s. These are fresh processes through complete CLI
output and exit, with uncontrolled OS page cache and no answer cache. They show
subsecond warmed completion, not a guarantee of cold-storage or concurrent-load
latency. No resident-session result is substituted for the public CLI clock.

Q13 trades memory for removal of the second pass: peak RSS in the complete
comparison blocks rises from 1,086,947,328 to 1,707,261,952 bytes (about 1.01 to
1.59 GiB). Q36 peak RSS is effectively unchanged at 4,828,069,888 versus
4,827,643,904 bytes (about 4.50 GiB). All runs request a 24 GiB query budget.
OS RSS and accounted reservations remain different measurements; these results
do not close global allocator accounting or broad spill requirements.

## Shared runtime changes

**Q36 family:** preserve the existing functional-dependency reduction through
the source-shape worker precheck, then admit the actual nonnullable I32/I64/U64
physical key. The native numeric partial keeps I32 values at their native width.
Ordered weighted merges use the existing compact COUNT state and its renderer,
which reconstructs every logical output column. Required derived expressions are
validated for every observed key before Top-K can discard it; losing groups cannot
hide arithmetic overflow. Existing Dict and Constant routes remain valid. No new
worker pool or scheduler is added.

The complete P1/2/4/8/12 screen preserves results. Caller merge remains the dominant
span, so this is not a route to subsecond Q36 by increasing parallelism. The
per-setting medians are 6.174 / 4.841 / 4.859 / 5.152 / 4.942 s; these small blocks
do not establish an optimal worker count or justify changing scheduler policy. The
retention rationale is removal of measured work with no multiplied global state:
the same-day complete-query saving clears the gate despite a substantial remaining
serial merge. A further partition/reduction algorithm would need a separate
traffic and memory attribution gate; it is not implicitly approved by this result.

**Q13 family:** admit an original deterministic UTF8 comparison on the sole
grouping column only after native lowering leaves no residual predicate. Existing
nonnullable schema, row, memory and count-only gates still apply. Native lowering
may implement a nonempty-string comparison through embedded length metadata;
admission carries the original predicate proof across that rewrite. Existing
complete-key partitions count selected rows exactly and select Top-K at EOF,
avoiding the second exact recount. Pressure still uses the existing native
handoff/refinement contract rather than silently accepting incomplete counts.

The full-size route observes 13,172,392 selected rows across 1,548 chunks and
6,019,102 complete groups. It reports `complete_key_partition_topk`, exact selected
weight and no histogram recount. The initial source `42e6ec11` checked the lowered
predicate instead and did not activate this route; those Q13 measurements cannot
be claimed as evidence for the retained change.

**Q23 attribution:** the UTF8 accessor reports calls, accessor rows, dictionary
entries, copied UTF8 bytes, provider execution and dictionary-build elapsed time.
Accessor rows are after native scan selection and before residual selection;
they are per-column observations, not unique source rows. Provider time includes
deferred I/O, decompression, filtering and canonicalization. It is neither exclusive
CPU nor proof of duplicate reads. Further Q23 work needs attribution within that
provider span. The retained instrumentation does not itself claim a speedup.

**Q29 removal:** the rejected implementation and its tests/counters were removed
from the retained runtime. Its first run counted 25,771,910 source-value partial
entries, hashed about 6.66 GB and spent 4.110 s counting/restoring order, in addition
to 2.288 s canonicalization. The historical 9.300 s best is context, not a matched
same-day control. The rejection means the screen did not establish the required
gain; the later matched check also finds the prototype slower. This does not prove
that every owned-string design is slower. Source checkpoint
`42e6ec11` and complete results preserve the experiment without shipping its cost.

## Reproducibility and evidence ownership

The runtime control is `289fa42c2a8754840e18faa9c95fc4c551dce797`; its runtime is
the prior hardening source `b254bbce`. The initial candidate is
`42e6ec114b3f122a059191ddc08aa596abfb3576`. The retained source is
`69ce65acf7b127e34c31d5a13343f25e72e3fbd0`. Each binary was built from a clean
source checkpoint with `cargo build --release -p shardloom-cli --features
release-user-surfaces` and copied before changing source or rebuilding.

| Binary | SHA-256 |
| --- | --- |
| Control | `2fc973fac249216dc24f3b39a8b09946df734d1513f6cb945c9fca126038beda` |
| Initial candidate | `150631defe8714c04205151cce376556b83a5164d497ab27a458701e0f24f267` |
| Retained candidate | `3fa5b72e098b02d3f5c8803df259d6c65d54ce98e636cfdd6ada30a398a24bed` |

Local binaries, commands, comparison logs and extracted measurements live under
`/Users/dylan/LocalData/shardloom/ship-drop-20260919`. Source remains the retained
99,997,497-row, 18,591,586,804-byte native artifact
`clickbench-100m-uat/vortex/performance-pr-ingest-4f2c7b970078-r1.vortex`.
All runs use the canonical 43-statement query file, P12 unless explicitly part of
the parallelism matrix, and 24 GiB. Host grants can cap requested parallelism.
Builds and large runs are serial. No replacement ingest or new storage claim is
needed for these query changes; 95.923669 s ingest remains historical evidence.

References are all 43 complete saved native outputs from September 13, checked
against the raw hashes in the historical profiling inventory. Integer/string/order
values are exact; finite floats use the harness's 1e-12 absolute/relative tolerance.
This is regression evidence against retained ShardLoom results, not an independent
external-engine oracle. Native fixtures provide additional known-value correctness
checks for renamed columns, ties, OFFSET, Unicode, native dictionaries, empty
arrays, arithmetic errors, denial/refinement and owner release.

The second comparison block hit the unchanged 256 MiB accumulated-log guard after
five recorded successes. The sixth invocation is not accepted as a completed
comparison record. Its orphan output/timing and the guard exception are preserved.
Only this task's closed logs were losslessly archived; unrelated evidence and
native data were untouched. Each byte was checked before removing a plaintext or
intermediate compressed copy. `completed-run-xz-bundle.json` maps archived members
and hashes into `logs/ship-drop-20260919-completed.tar.xz`; per-run summaries remain
at their original paths. The harness now supports guarded, opt-in gzip compression
after native timing and validation, plus compressed-reference loading. Archive
allocation is admitted under the same storage limits. No limits were raised.

## Acceptance

The retained source passes **129/129 complete Full43 result comparisons**. The
new per-query best-of-three sum is **95.927383 s**, hot sum (best of runs 2/3)
96.232309 s, and all 129 native calls total 298.696499 s. Q13 is
0.992 / 0.872 / 0.749 s in this full run; Q36 is 4.631 / 4.514 / 4.923 s.
The frozen summary is
`/Users/dylan/LocalData/shardloom/clickbench-100m-uat/logs/full43_20260919T161449701280Z/summary.json`.
Its verified raw logs are in that directory's `completed-run-logs.tar.xz`, with
member/hash mappings in `completed-run-logs.json`.

The new sum is **4.47% higher** than the historical 91.825940 s. This is a complete
correctness acceptance run, not proof of a whole-suite performance improvement or
a replacement for the historical timing control. The individual retention claims
come from the same-day matched comparisons above. Q29/Q34/Q35 drift prompted a
further same-day comparison before final acceptance; its records are preserved
with the [machine-readable evidence](../benchmarks/performance-ship-drop-2026-09-19.json).
In that follow-up, Q29 control/prototype/retained medians are
9.256 / 10.191 / 9.311 s (three / six / three samples). Q34 control/retained medians
are 6.658 / 5.664 s, and Q35 is 5.607 / 4.973 s (three each). These checks confirm
Q29 removal and show no Q34/Q35 regression in the compared blocks; they do not
attribute a new Q34/Q35 speedup to this patch. All 24 follow-up results match.
OS cache, scheduling and host state were not controlled, and this experiment does
not diagnose which of those explains the historical difference.

Validation on the retained runtime:

| Check | Result |
| --- | --- |
| `cargo fmt --all -- --check` | Pass |
| `cargo clippy --workspace --all-targets -- -D warnings` | Pass |
| `cargo test --workspace --all-targets` | 3,424 passed |
| `cargo clippy -p shardloom-cli -p shardloom-vortex --all-targets --features shardloom-cli/release-user-surfaces -- -D warnings` | Pass |
| `cargo test -p shardloom-cli -p shardloom-vortex --all-targets --features shardloom-cli/release-user-surfaces` | 3,333 passed, nine existing manual cases ignored |
| `python3 -B -m unittest discover -s scripts -p test_clickbench_query_uat.py` | 11 passed |
| `python3 -B -m unittest discover -s scripts -p test_local_uat_storage.py` | 13 passed |
| Nine documentation/status/version validators | Pass |
| Architecture tracker with `--allow-blocked` | Validator passes; 116 broader phase items remain open |

Feature test counts overlap and must not be added as unique cases. The nine ignored
cases are manual release benchmarks/fixture regeneration, not failed tests. Initial
native validation found two candidate-test integration issues; both were corrected
before the final complete native gate. The Q29-specific tests were removed with
the rejected candidate. Independent review found two harness issues (compressed
reference loading and archive allocation admission) and stale execution-order
text; each was corrected. Exact commands, log hashes, run identities and individual
samples are retained in the machine-readable evidence. The harness source is
`f1e29694`; compression happens after the measured native operation.
The final evidence audit replays all 203 unique accepted native records from 18
summary directories against the saved complete references, verifies their result
hashes and one unchanged input generation, and excludes the interrupted orphan.

For replay, build the named native checkpoint, then use the query UAT harness with
`--binary` pointing to the frozen binary, `--input` to the retained native artifact,
`--reference-dir` to the verified complete references,
`--build-commit 69ce65acf7b127e34c31d5a13343f25e72e3fbd0`, `--memory-gb 24`,
`--max-parallelism 12`, and `--compress-logs`. Omit `--query-ids` for Full43;
targeted blocks explicitly name their query IDs and cannot become a suite score.
Storage admission must pass with the existing limits before replay.

This is local CG-5/CG-6 evidence within the active PERF work, not closure of those
competitive gates. The native input is a real Vortex payload; no placeholder output
artifact is promoted to real-output certification. No new ingest, release/package,
remote serving, cross-platform, production or competitive-superiority claim is
made. These changes are locally validated and remain unmerged/unpublished.

## Remaining decisions

The [phase plan](phased-execution-plan.md) remains the only active queue. Q19 triple
keys and Q33 near-unique duplicate reduction remain conditional on state/probe/
distribution attribution. Duplicate ingest traversal, serving queue policy and
large-payload delivery remain conditional on their own exclusive-work, queue-delay
and ownership measurements. The current evidence does not justify implementation
of those additional proposals. Codec sweeps, topology changes, speculative local
reductions, universal compact-state replacements, native Python binding and PGO
remain parked. Repackaging already-retained key elimination, heavy hitters, late
measures or DISTINCT preunion is not a new candidate.
