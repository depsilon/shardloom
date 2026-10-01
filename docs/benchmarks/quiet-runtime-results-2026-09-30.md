# Quiet-workstation runtime experiments — September 30

The [finite intake](../architecture/performance-quiet-intake-2026-09-30.md) is
complete. Retain the borrowed sort reads, concrete AVG/COUNT block loop, bounded
scalar UTF8 DISTINCT union, vacant-slot reuse and directory hash tags. Retain the
ingest grant measurements as configuration evidence; no default setting changed.
This closes this experiment batch under PERF-03/04/05/08/09/10/12, without closing
their broader obligations or changing CG-1 through CG-23.

The final paired Full43 sum of per-query best complete calls falls from
**64.048346 to 59.906465 seconds (6.4668%)**. The sum of query medians falls from
69.759710 to 65.340578 seconds (6.3348%). All 258 results match the retained
canonical JSON exactly. These are comparative aggregate statistics, not one
continuous end-to-end workflow. The earlier 55.251837-second single pass and
65.806017-second P4 ingest remain unchanged in their
[original observation](current-runtime-e2e-2026-09-30.md).

## Decisions and focused evidence

Each query screen uses three complete calls per role with alternating order.
Controls are the preceding retained stage, so these focused percentages must not
be added together. Every slower sample and unsuccessful prototype is preserved.

| Experiment | Decision and focused observation |
| --- | --- |
| Existing P4/P6/P8 ingest grants | Configuration evidence only. Best native ingest is 65.700169 / 52.893826 / 49.882496 s. Larger grants change provider drivers, prefetch and source batch size; they use more aggregate CPU. |
| Borrow native sort UTF8 bytes | Retain. Q26 best improves 6.248%; Q27 improves 4.649%. Validation still visits every required value, and retained output owns its strings. |
| Concrete compact AVG/COUNT loop | Retain a small implementation with modest focused evidence. Q28 best improves 0.254% initially and 0.533% in the one reversed-order follow-up. The latter median improves 1.37%; this does not establish a large or statistically certain gain. |
| Bounded scalar UTF8 COUNT(DISTINCT) | Retain. Q6 focused best falls 3.926549 → 1.180516 s (69.935%), using more CPU. Final Full43 includes the subsequent cancellation/accounting fixes. |
| Reuse a vacant directory slot | Retain the corrected implementation. The first version also added production probe counters and regressed Q34 by 13.6%. Removing those counters isolates slot reuse: the bounded follow-up improves Q34 best 3.856% and Q35 0.849%. This does not independently prove the cause of the first slowdown. |
| Reject mismatched directory hash tags | Retain separately after slot reuse. Q34 best improves 4.673%, Q35 0.576%; both medians improve slightly. The first Q34 candidate call is slower and remains recorded. |

The compact-loop release disassembly contains 40 row-driver instantiations and
40 nested measure bodies. Their repeated loops use direct calls; no `BLR`/`BR`
remains in those loops. Native-array access still has indirect calls before
the loop. The implementation preserves row visitation, original measure order,
floating additions, null behavior and partial state on error. There is no JIT,
parallel AVG or relaxed arithmetic.

Scalar DISTINCT uses the existing bounded chunk jobs and native Vortex owners.
Workers build or consume source-backed dictionaries, mark referenced nonnull
native codes, and union full hashes plus exact bytes into 64 partitions. Global
state copies key bytes only on an exact miss; it does not construct grouped
output or occurrence counts. Task and partition capacity, replacement peaks,
cancellation and final cardinality have explicit contracts. This is an in-memory
path. Provider allocations outside `HostAllocator` and caller scan metadata are
not a total-RSS bound. Copy counters exclude byte-arena resize copies, and summed
worker elapsed timers can overlap one another and caller preparation.

The original Q6 source review required a correction: provider execution and
dictionary construction already had separate timing fields. Scalar union was
incremental global-set insertion, not final merging of existing scalar worker
partials. The corrected attribution and original source inventories are retained.

Directory tags occupy the high 16 hash bits of an eight-byte entry; the low 48
bits encode ordinal-plus-one. Checked bounds preserve the empty sentinel and
keep tag bits separate from partition/bucket bits. Full hash and bytes remain
the equality proof. Actual entry width is charged, including the larger width
relative to `usize` on 32-bit targets; executed measurements are ARM64 only.

A separate debug/test-only Q34 diagnostic records 48,610,356 lookup probes,
10,763,268 dense-record reads and 19,406,352 tag rejections: 64.324% of occupied
lookups avoid a dense-record read. These counters cover `find()`, including
entry-credit rechecks, and exclude rehash/insertion searches. They are absent
from the production binary. The first diagnostic used an inconsistent memory
policy and failed its admission assertion after exact results passed. Its
corrected run uses the standard 24-GiB policy constructor and passes. Neither
debug duration is performance evidence.

## Final composition and negative observations

| Query | Control best (s) | Candidate best (s) | Control median (s) | Candidate median (s) |
| --- | ---: | ---: | ---: | ---: |
| Q6 | 3.515639 | 1.042027 | 3.549686 | 1.095878 |
| Q26 | 1.785244 | 1.663956 | 1.799923 | 1.679335 |
| Q27 | 1.790320 | 1.705658 | 1.811689 | 1.741510 |
| Q28 | 2.304366 | 2.153555 | 2.486947 | 2.214496 |
| Q34 | 4.716527 | 3.557598 | 5.822741 | 4.709998 |
| Q35 | 2.872998 | 2.762262 | 2.975171 | 2.992101 |

Sixteen queries have a slower candidate best. Q11 alone crosses the predeclared
10% **and** 150-ms regression flag: 0.839687 → 1.140509 s, while its medians are
1.179326 → 1.175489 s. The one allowed reversed-order follow-up gives best times
0.717665 → 0.727619 s, with all three candidate calls slightly slower. The large
regression did not repeat; the roughly 10-ms/1.4% difference remains. Q11 uses
the existing compound text/integer DISTINCT route. The follow-up does not replace
Q11 in the original Full43 total.

Timing spread is substantial in some calls: Q34 control includes 12.694275 s,
and its candidate includes 5.361199 s. The larger final-composition Q28/Q34
differences do not isolate the cost of the individual mechanisms. Source identity,
complete outputs, pair order, all samples and host snapshots remain available.
No particular desktop process is assigned causal responsibility.

Q6 final median accounted CPU rises from 3.621898 to 3.916525 s (about 8.1%) as
its wall time falls. Across all 129 calls per role, accounted CPU is 663.840257 s
for control and 642.780195 s for candidate; sums of per-query median CPU are
215.525015 and 213.121043 s. Maximum observed query RSS is 5,775,360,000 versus
5,723,815,936 bytes. These observations do not establish a general RSS improvement
or a throughput benefit under concurrent workloads.

## Ingest configuration and changed layout

The unchanged executable runs six ingests in P4/P6/P8/P8/P6/P4 order. Per-grant
native samples are:

| Grant | Both calls (s) | Median (s) | Output bytes |
| --- | --- | ---: | ---: |
| P4 | 65.700169, 77.272878 | 71.486523 | 15,682,956,489 |
| P6 | 52.893826, 59.626571 | 56.260198 | 15,663,942,481 |
| P8 | 49.882496, 52.619137 | 51.250817 | 15,663,942,481 |

The first P6 call stopped the original byte-identity check. Existing public
policy divides the source batch budget by the grant: P4 uses 131,072-row batches;
P6/P8 use 65,536. The changed artifact was preserved and compared completely:
all 99,997,497 rows and 112 columns match, as do exact schema, whole-file
statistics and embedded provenance bytes. Physical layout/directory bytes differ.
The continuation accepts that verified hash, retaining the interruption and failed
original check. It does not relabel the artifacts byte-identical.

The changed layout has its own paired Full43 on the unchanged runtime: best sums
57.663915 → 58.619419 s, with all 258 results exact and no regression flags.
P8 is fastest for ingestion in this screen, with a resource and layout tradeoff;
no default grant was changed. The final **new-runtime** Full43 uses the original
P4 artifact. A combined new-runtime P8 end-to-end workflow was not measured.

## Identity, validation and reproduction

- Hardware: Apple M5, 10 logical CPUs, 16 GiB physical RAM, macOS 27.0 ARM64.
  The 24-GiB setting is memory admission policy, not an RSS limit. Query requested
  parallelism is 12; the new scalar path admits 10 lanes including its caller.
- Control runtime source: `8385055894ba8c74bd4501ecbd833ae650a07895`;
  candidate runtime source: `744c9db041111a34592866b3f61f3203209d5bf6`.
  Candidate release binary SHA-256:
  `0493e101662d33004349df26e7c2ffa6a3c83adfdf0e36c576e221ebc7590987`.
  Both use the frozen `release-user-surfaces` build and recorded Cargo graph.
- Native Vortex query input: 99,997,497 rows, 112 columns, SHA-256
  `5a6e7d7b521b891e522f32398ef2582367e814791ab537f354003ecd2f7a488d`.
  Changed P6/P8 layout SHA-256:
  `dabc25feaeff6cda1d549ca56131fa99a4dc48ad70d0fa21f4d3833f0ea2b17f`.
- Full native-process durations include startup, complete output and exit.
  Input hashing precedes each cohort; cache state is uncontrolled. User-managed
  competing workstreams were paused; ordinary desktop activity remains visible.
- Shared workload/lock, local-only storage, disk/log ceilings and process-group
  deadlines remain active. Native builds, tests and timed calls are serial.
  All measured groups drained. Successful public calls report false fallback
  and external-engine execution flags.
- Exact canonical result hashes use retained regression references without float
  tolerance. This is not a fresh independent SQL oracle or CG-5 certification.

Final source passes:

```text
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace --all-targets
cargo clippy -p shardloom-cli -p shardloom-vortex --all-targets --features release-user-surfaces -- -D warnings
cargo test -p shardloom-vortex --lib --features release-user-surfaces
cargo test -p shardloom-cli --bin shardloom --test sql_local_source_runtime_smoke --test public_workflow_route --test resident_worker --features release-user-surfaces
```

The workspace run passes 3,436 tests; the native Vortex library passes 2,055 with
23 ignored; the selected native CLI suites pass 1,171. Twenty-two ignored native
experiments predate this batch. The new ignored real-input diagnostic was run
explicitly. Focused fixtures additionally cover native/sliced/multiple-buffer
sort ownership, invalid losing UTF8, arithmetic/error order, native dictionary
nulls/unused values, collision equality, cancellation, allocation denial, leases,
vacancy invalidation and packed-entry bounds.

To reproduce, check out each recorded runtime revision, resolve Cargo output to
local-only storage, and build `shardloom-cli --release --features
release-user-surfaces` with the pinned lockfile. Use
`scripts/run_clickbench_paired_query_uat.py` with those frozen binaries, the input
hash above, `benchmarks/clickbench/queries.sql`, retained references, 24 GiB,
parallelism 12 and a 120-second per-call deadline. The retained manifests contain
the exact commands, alternating order and original hashes. Reproduce ingest
through `scripts/run_clickbench_ingest_uat.sh` under the
[local operating procedure](../architecture/local-development-storage.md).

The [machine-readable index](quiet-runtime-results-2026-09-30.json) contains all
43 query sample sets, CPU/RSS, selected work profiles, checks, decisions and
negative observations. The [portable evidence](evidence/quiet-runtime-results-2026-09-30.json.xz)
contains 588 exact query results in 98 archived query groups, all 2,352 raw
stdout/stderr/timing/PID members, six ingest receipts, diagnostic/build/test
records and executed helper sources. It is 1,263,548 bytes, SHA-256
`d330bf80c1cb1cc25e05249def910050f8fe4d6b6c8b9b9c2c7e42a51e35c353`.
Paths are substituted with portable placeholders; full data and binaries are
excluded. Native Vortex input/output and no-fallback execution remain intact.
This batch does not publish a package, resume paused format work or establish
an official ranking, general superiority or production-serving certification.

After evidence verification, [guarded cleanup](quiet-runtime-cleanup-2026-09-30.json)
removed the run-owned P6/P8 comparison layout and six intermediate/comparator
executables: 16,075,311,297 logical bytes. Fresh hashes and file generations
matched the original manifests before removal. The original Parquet, P4 Vortex
reference, original control binary and final retained binary remain unchanged.
All raw evidence remains. Replaying the retired physical layout or intermediate
executables requires regeneration from the recorded commands and revisions.
