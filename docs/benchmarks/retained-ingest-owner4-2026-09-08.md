# Retained native ingest: four constructed CPU owners

Status: complete on September 8 at 20:53:01 UTC; recovered and documented on
September 12 after the task interruption. Both guarded ingests, complete-value
verification and owned-output retirement passed. This is one sequential
control/candidate pair after PR #1437 merged, not a scaling curve or a stable
speedup claim. The [machine-readable evidence](retained-ingest-owner4-2026-09-08.json)
records exact commands, measurements, proof and source identities.

## Question and disposition

Earlier requested-P1 observations compared a control that actually constructed
four CPU owners with a candidate that respected a true single-owner ceiling.
Their 95.84/187.60-second difference did not establish an equal-resource ingest
regression. This packet requests P2 from control `75fc09a0` and P4 from accepted
candidate `572bd52c` so both construct caller/source/conversion/provider owners
of 1/1/1/1. Native evidence confirms that premise. Control ownership also relies
on its frozen constructor implementation because it predates the explicit
candidate CPU-owner certificate.

| Observation | Control | Candidate |
|---|---:|---:|
| Requested public parallelism | 2 | 4 |
| Confirmed constructed CPU owners | 4 | 4 |
| Array-build prefetch slots | 1 | 3 |
| Native process seconds | 99.446032042 | 95.447305458 |
| Maximum observed native-child RSS, bytes | 3,314,712,576 | 2,811,117,568 |
| Native child user CPU seconds | 195.302454 | 199.473686 |
| Native child system CPU seconds | 12.906550 | 11.064397 |
| Artifact bytes | 18,643,482,956 | 18,591,586,804 |
| Final shared-native reserved bytes | 0 | 0 |

Candidate wall time is 4.02% lower, maximum observed child RSS 15.19% lower,
and artifact size 0.278% smaller in this pair. CPU work overlaps wall time; four
constructed owners neither means four continuously busy CPUs nor bounds every
thread in the process. The public requests and prefetch depths differ, and
other retained implementation changes separate the binaries. This does not
isolate prefetch, compression or scheduling as the cause of any difference.

Retain the existing explicit CPU/resource contract. This pair alone does not
justify another tuning round: the wall-time difference is modest, one pair
does not establish a stable effect, and no dominant new cost was identified.
The maintainer's September 12 request adds an explicit
[ingest implementation and test sequence](../architecture/ingest-performance-implementation-2026-09-12.md)
for CPU-stage balance, bounded writer overlap and duplicate representation work.
Start with bounded stage attribution; wider curves and implementation depth
require a concrete workload question or plausible material lifecycle benefit.
The rejected topology work, native Python prototype, universal state/codec
replacements and full-workspace PGO experiments remain deferred.

## Frozen inputs and execution

Local evidence root:
`/Users/dylan/LocalData/shardloom/perf-all-20260906/ingest-owner4-final-20260908`.
`matrix.json` records exact commands, sources, generations, binary/helper hashes,
observed ownership, storage admission and the merge/acceptance gate. The gate
requires PR #1437 merge `d51429e3702e5201646142a3d7252bbd72485c85` and the
[completed native/public acceptance receipts](retained-runtime-acceptance-2026-09-08.md).
Historical planning notes naming `e739edeb` are superseded by this pinned packet.

| Input | Identity |
|---|---|
| Resident official `hits.parquet` | 14,779,976,446 bytes; SHA-256 `a390f6cb782f6aaef278c72fc1dd86c4f30bc843ebab3c159e9bd4d45ddb079f` |
| Protected native reference `perf-current-c71a558e.vortex` | 18,643,482,956 bytes; SHA-256 `93acc7b9bbabed1f6e15a91aeacda45637bd5d6c5fed26e9b2052bf9b77e84f2` |
| Control `candidate-75fc09a0` | SHA-256 `e17690b5f9a1f84b692f90f4a7593f6be8c062346830205754685a8291f62392` |
| Candidate `candidate-572bd52c` | SHA-256 `9251e10babcfc235b984fd256bc67a123b0b4126b13b375b253b55558a8eef9e` |
| Candidate native source and ingest helpers | `572bd52c46307a7853c37fb6ca08b6a11d1b9b69`; ordinary release-user-surfaces build, no PGO or new build for this pair |

The candidate ran after merge. `572bd52c..d51429e3` changes only the four
acceptance harness/test scripts; native code, ingest helpers, Cargo inputs and
the public Python package are identical. This is evidence for the merged native
runtime using its frozen binary, not a separately rebuilt `d51429e3` executable.

Both arms use 99,997,497 rows, 112 columns, a 24 GiB memory request, native
Vortex output and the existing Parquet source/writer route. The host is the
same arm64 macOS machine as the final query acceptance. Runs are serial, control
then candidate; OS page cache is uncontrolled. Native process creation through
complete output and process exit is the timing boundary. Guard polling and the
independent post-run checksum/value verification are outside that clock.

The frozen `run_clickbench_ingest_uat.sh` enforces the existing 100 GiB workspace,
256 MiB log, 12 GiB free-headroom and 24 GiB candidate reservation limits. Only
one newly generated full-size output exists at a time. The local receipt wrapper
`run_retained_ingest_owner4.py` pins the gate, inputs, binaries and helpers and
fails closed on changed identities or missing owner evidence. Its seven small
ownership/comparison/cleanup tests passed in `retained-ingest-owner4-root-tests.log`.
No benchmark-time build, replacement ingest or guard relaxation is used.

## Complete values and cleanup

The control output matches the protected native reference SHA-256 exactly.
`control-p2-r1.validation.json` records that proof; its exact runner-owned output
was removed only after durable validation and retirement-intent receipts.

Candidate SHA-256 is
`7181c2e578659910da176ff6c0dcfe7ce563405337f3ae88cd44e7932d92a266`, so physical
byte identity is not claimed. Complete native value comparison passed before
its retirement. The frozen comparator is
`compare-native-artifacts-20260908`, SHA-256
`8673cd00615129265475404ad4b0e26b7c1850801effb95ac6a8f9f68bc21d25`, using
8,192-row windows and two lanes per source. It verified equal native schema,
all 99,997,497 rows and 11,199,719,664 column values, complete order/null/valid
primitive-bit agreement, unchanged source generations, no outstanding native
tasks, and zero remaining source/Arrow reservations. Native canonicalization and
Arrow conversion occur only inside this validation boundary. Layout statistics,
user metadata, null payload bits, dictionary codes and chunk boundaries are not
compared; logical equality does not prove physical metadata parity.

`candidate-p4-r1.validation.json` has SHA-256
`c58221888e485e0e0822a76df4d056d5f97dd62ce02c820294911a67822e295d`.
The complete matrix SHA-256 is
`98b2a7e680ec3275a6a653d912232801b86b849655ded843c2d95171a048359e`.

The wrapper retains full proof and only unlinks the exact generated regular
single-link file after rechecking path, inode and generation. It never removes
the original Parquet source, protected reference or unrelated output. Both
retirement receipts are complete; both generated output paths are absent.

The final Full43 packet continues to use the immutable protected native
reference, not this newly ingested candidate layout. Therefore this pair does
not establish an ingest-plus-query speedup or query-performance acceptance for
the changed physical bytes. Broader PERF-03/08/09/12 and competitive gates remain
open; native execution, complete-result correctness and explicit no-fallback
boundaries are preserved.
