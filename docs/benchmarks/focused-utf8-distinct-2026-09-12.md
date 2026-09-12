# Focused UTF8-grouped integer DISTINCT acceptance

Status: retain the isolated operator on `codex/perf-focused-distinct-20260912`;
do not advance the whole-workload timing control. The paired target improves
materially and all required correctness gates pass. Full43 is 102.005509 seconds,
above both the earlier `2ad143da` observation of 96.692273 seconds and the
historical retained-runtime observation of 91.215296 seconds. Those Full43 runs
are sequential observations, not a paired causal comparison. No overall
throughput, stability, scaling or competitive superiority claim is established.

The maintainer's latest instruction pauses broad phase completion. This extraction
starts from accepted `2ad143dae444d3027fb07f11e5b605e991e79b61` and includes only
the shared nonnullable UTF8-group / integer COUNT DISTINCT workers and necessary
correctness and ownership helpers. The [operator contract](../reference/utf8-integer-distinct-workers.md)
describes complete pair equality, group partitioning, bounded exact selection,
source admission and explicit pressure behavior. No query or column-name switch,
external engine fallback, new dependency, codec or ingest policy is introduced.

## Frozen source and validation

The runtime is frozen at `4730003f9de3bb53446164c999b94c514fb41d10`.
The ordinary release CLI, built with `release-user-surfaces` and no PGO/RUSTFLAGS,
has SHA-256 `ca1005cfc1e482e7a451013e02c33d9974367c0213bcb923f8b495fa79615931`.
Baseline binary SHA-256 is
`dfa831817fdb681cfefbfac19d24c7525854ae5d10ce2052158beb72758e71b2`.
Documentation added after this checkpoint does not change the measured runtime.

| Validation | Result |
|---|---|
| `cargo fmt --all -- --check` | Passed |
| `cargo clippy --workspace --all-targets -- -D warnings` | Passed |
| `cargo test --workspace --all-targets -- --test-threads=1` | 3,417 passed |
| Native Vortex and CLI `release-user-surfaces`, all targets | 3,278 passed; nine existing manual benchmarks/fixtures ignored |
| Native release-surface and minimal-native Clippy | Passed |
| Focused new tests | 19 passed; included in the native suite, not additional to its total |
| Matched ClickBench screen | 48 complete exact results |
| Renamed independent fixture screen | 96 complete exact results and worker evidence |
| Full43 | 129/129 complete exact results; no guard failures |

The focused tests cover all eight integer widths, dictionary domains, forced hash
collisions, complete global ordering and offsets, schema/NULL denial, pressure,
cancellation, source generation, selected-string replacement and ownership.
Existing compound COUNT, integer DISTINCT and worker-job controls also pass.
The native Clippy/focused receipt anchors `0a4c282f` plus its formatter-only Rust
diff; that is the same Rust state frozen at `4730003f`. The broad suite runs on
the clean frozen commit. Test filters overlap and must not be summed.

## Paired feasibility screen

The threshold was written before measurement: Q14 median paired
candidate/baseline elapsed ratio at most 0.80, each control at most 1.05.
Both frozen binaries read the same protected 99,997,497-row Vortex artifact.
Each query has one excluded warmup per arm and three measured alternating pairs,
with first-arm order balanced across the six queries. An individual measured
query has a 2:1 first-arm imbalance. Requested limits are P12 and 24 GiB.

| Query | Baseline median, s | Candidate median, s | Median of paired candidate/baseline ratios |
|---|---:|---:|---:|
| Q11 control | 1.006696 | 0.919887 | 0.943984 |
| Q14 target | 8.815227 | 2.215635 | 0.251342 |
| Q17 control | 4.615139 | 4.821306 | 1.007469 |
| Q23 control | 6.307657 | 6.252688 | 1.003097 |
| Q34 control | 6.997566 | 7.600706 | 1.005274 |
| Q35 control | 7.176141 | 6.770252 | 0.928653 |

All predeclared gates pass. Q14 uses about 74.9% less elapsed time in the paired
median, approximately 4x faster. The median of paired ratios is not the ratio of
the two raw medians. Q34's raw-median ratio is 1.086193 and its worst measured
pair is 1.101219; retain that variation alongside its passing paired median.
Three samples establish feasibility, not confidence intervals or stability.

The unchanged renamed-fixture protocol uses 1,048,576 rows per profile,
`parcel_label` UTF8 and `inspection_token` I64, duplicate-heavy/mostly-unique/skew
data, all-groups and offset/tie output shapes, requested P1/P12 and 2 GiB. Each
cell has one excluded warmup and three alternating measured pairs. All twelve
cells improve against the baseline, with median paired baseline/candidate ratios
from 1.512764 to 3.713905. Complete ordered values match the independent integer-set
and UTF8-byte oracle, and all 48 candidate calls pass the worker/credit evidence.
P1 executes inline; requested P12 reports a ceiling of ten and nine actual count
workers. P12 is slower than P1 for duplicate-heavy and skew data and faster for
mostly-unique data. This is not a general parallel-scaling result. Scoped credits
exclude provider allocations bypassing HostAllocator, JSON output and process RSS.

## Complete workload and decision

Full43 packet `full43_20260912T224724814006Z` uses the existing frozen harness,
canonical SQL, complete-float comparison, P12/24 GiB, three fresh processes per
query and the protected native artifact. Native process creation through complete
stdout and exit is timed. OS page-cache and host state are uncontrolled.
These full-workload references are retained ShardLoom outputs; parity with them
is not an independent SQL correctness oracle. The renamed fixtures provide the
independent oracle for the changed operator family.
The source is 18,643,482,956 bytes, SHA-256
`93acc7b9bbabed1f6e15a91aeacda45637bd5d6c5fed26e9b2052bf9b77e84f2`.
This is query acceptance on the protected artifact, not a new ingest/storage run.

| Full43 measure | Result |
|---|---:|
| Sum of each query's best of three samples | 102.005509 s |
| Sum of each query's best of its last two samples | 102.764387 s |
| All 129 raw elapsed samples summed | 319.232074 s |
| Geometric mean of per-query best samples | 0.849766 s |
| Maximum recorded child RSS | 6,148,210,688 bytes |

Full43 Q14 samples are 1.848718, 1.726047 and 1.695600 seconds. The complete
workload score is 5.49% above the earlier `2ad143da` observation and 11.83% above
the historical 91.215296-second retained-runtime observation. Neither the matched
operator gain nor comparison with the slower broad `8ba36c76` total of
117.633288 seconds establishes a new overall timing control. RSS is an observed
process peak, not proof of a whole-process memory bound or an isolated memory gain.

Keep the focused operator as a measured, correctness-accepted branch change.
Keep the historical whole-workload control. Pause nullable COUNT, extra DISTINCT
spill, owned measure/result families, join/window/API expansion, codec portfolio
and native-Python work. Saved code is not shipping acceptance; these parked
families are not all measured rejections. The rejected allocation, topology and
prepared-minute variants remain dropped. Smaller text output remains a separately
tested, slower storage tradeoff. Ingest is unchanged; its historical 90.303309 and
93.945037-second observations are not rerun or relabeled as new measurements.

This finite experiment is complete. Any next experiment should select a measured
dominant cost and a bounded retain/drop gate, without automatically resuming the
capability inventory. Q29 regex grouping (10.336870 s), Q19 three-key aggregation
(9.386512 s), and Q13 string grouping (6.763428 s) are the largest per-query best
samples here. These identify investigation priorities, not approved fixes or
reasons to revive previously rejected implementations. Remaining PERF items and
CG-1 through CG-23 retain their existing open status.

## Reproduction and evidence

Local evidence root: `/Users/dylan/LocalData/shardloom/perf-all-20260906`.
UAT root: `/Users/dylan/LocalData/shardloom/clickbench-100m-uat`.
Receipts retain exact commands, frozen source/binary/harness identities, complete
output hashes, all raw timing samples and resource guards. Reproduction requires
the same resident artifacts and a fresh attempt name; preserve existing receipts.

| Receipt | SHA-256 |
|---|---|
| `phase-resume-4730003f-build.json` | `49edebfa5b14c277ace455772dcca0b0cf37641af8ad4f2499449e5dbfe1efdf` |
| `phase-resume-focused-distinct-r1.json` | `9fa85dbce06be0eb273f9820d4876ecf3349202ef514c7c0663cdba008ee5e28` |
| `phase-resume-broad-r10.json` | `149cbdd2cbac6d1a149bf207638ffb5c623434f0d7e0454aa23ed93e5f0b1674` |
| `phase-query-4730003f-p12-r1.json` | `e9cb7625283223d7fe8e79842601b5687896501f6eb6e81a7a7cb511936b418a` |
| `focused-query-gate-4730003f-p12-r1.json` | `70c78ef7d9bb8f50717786258fd904ed48785dc5875a379ed2373119b3396e7d` |
| `utf8-distinct-4730003f-r1.json` | `c19a220d314f883eb05d82dfb9bfbcb118c9c52fe27a1ebbaa48b0077d452ac4` |
| `phase-acceptance-4730003f-full43-r1.json` | `550f5e7a0b31ed9b6620238672e5f3b2754a5f7e5deba1d5e624c65fedcdd799` |

The Full43 summary SHA-256 is
`1bea87e0f4a262a9b101ec7268580fbab32aabab36aa45cd2577966de9321874`.
Independent reviews checked the operator source, paired calculations, archived
result bytes, generic oracle/worker evidence, all Full43 output/timing sidecars
and archive safety. Required checks
and this focused/full acceptance packet replace no unrun broader capability tests.

Log headroom was admitted before Full43. Completed and closed failed-attempt
transcripts were archived with complete byte verification, durable indexes and
exact original-file retirement. Failure summaries, terminal outputs, fixtures,
protected references and failure status remain preserved. The evidence notes
`phase-resume-transcript-headroom-20260912.json` and
`phase-resume-failed-archive-verification-20260912.md` link the archival receipts.
The 100-GiB workspace, 256-MiB log and 12-GiB free-space guards were unchanged.
