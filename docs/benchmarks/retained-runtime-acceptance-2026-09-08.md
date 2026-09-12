# PR #1437 retained-runtime acceptance

September 12 continuation: frozen `2ad143da` now passes 129/129 protected-artifact
Full43 comparisons, 2,232 resident public calls, 380 independent held-out checks
and 80 selected required-worker checks. Its Full43 totals are 96.692272626 s
best, 96.825921333 s hot and 293.417033461 s across all runs; the protected query
timing control remains `572bd52c`. See the
[completed continuation packet](native-completion-boundaries-2026-09-12.md#completed-frozen-runtime-acceptance)
for resident improvements, the mixed 24-call COUNT-lane diagnosis, interrupted
attempts and lossless archive provenance. The original PR #1437 checkpoint below
retains its historical source, measurements and remaining-work scope.

Status: frozen runtime `572bd52c` passes Full43, scoped held-out acceptance and
the complete resident matrix. PR #1437 merged as
`d51429e3702e5201646142a3d7252bbd72485c85` on 2026-09-08 after all 40 checks
passed for head `5d2bf2b1fd7c1aa5c2a90d98c35af8095e51c783`.
The shipping extraction retains the prepared/native runtime and compatibility
export work while excluding the rejected topology runtime and unbuilt isolated
Python prototype. The scoped export retain decision stands. The final runtime's
Full43 best-sum is 2.88% higher than the fresh Existing-policy reference in an
uncontrolled sequential comparison. These results establish complete-value
agreement, not an overall throughput win or closure of the remaining PERF gates.

## Source and binary checkpoints

| Checkpoint | Exact identity | Scope |
|---|---|---|
| Extraction correctness | `15102a6af835e51bea2883c63e71be34b64e66b6` | Validation receipt records an empty runtime diff; docs/prototype extraction was separate |
| Extracted shipping source | `e739edeb59c7c3ae0184a671b776e1e30c854214` | Tree `745eafb84060734b10141c19382fbc813a4095b8`; no topology runtime or isolated Python prototype |
| Extracted release CLI | `0236e19c21ffd3e2806530347e730f56b7c58d76e6569b38b945f6aa02d13431` | Binary SHA-256 for `candidate-e739edeb`, release-user-surfaces, rustc 1.98.0 / LLVM 22.1.8 |
| Fresh existing-policy reference | `9152a92b7761bb9a124268f25e726a8094c637a7` | Binary SHA-256 `5fa36ae99ceca0782b228851c2e9b5dd8186f0ac1a07d4e4c86dc943a89932ab`; experimental branch with Existing policy |
| Frozen shipping runtime | `572bd52c46307a7853c37fb6ca08b6a11d1b9b69` | Tree `67d8061a7c84a0412a4acd89cc0161a7c5c465aa`; includes source-grant restoration and PGO lookup correction |
| Frozen release CLI | `9251e10babcfc235b984fd256bc67a123b0b4126b13b375b253b55558a8eef9e` | Binary SHA-256 for `candidate-572bd52c`; release-user-surfaces, rustc 1.98.0 / LLVM 22.1.8 |
| Frozen acceptance harness | `5d2bf2b1fd7c1aa5c2a90d98c35af8095e51c783` | Adds required distinct-worker evidence and bounded compressed resident logs; native crates, Cargo and public Python package unchanged from `572bd52c` |

Local evidence root is `/Users/dylan/LocalData/shardloom/perf-all-20260906`.
`retained-572bd52c-binary.json` pins the clean frozen source, toolchain, binary
and build log. `retained-runtime-reviewfix1.json` records the serial review-fix
checks; exact commands, source/diff identities and stage log hashes are retained.

| Check | Recorded outcome |
|---|---|
| Formatting; default workspace, native release-surface and minimal-native Clippy | All passed |
| Default workspace tests | 3,409 passed, zero failed |
| Native CLI/Vortex tests with release-user-surfaces | 3,219 passed, zero failed; nine manual benchmark/fixture tests ignored |
| Four Python harness modules at review-fix checkpoint | 47 passed |
| Final harness, including real PyArrow fixture coverage | 54 passed, zero skipped; `retained-runtime-harnessfix-final-python.json` and matching `.log` |

Formatting, native checks and the 47 Python tests ran on the recorded working
patch subsequently committed as `572bd52c`; default and minimal-native checks
ran on that clean commit. The final 54-test receipt pins harness `5d2bf2b1` and
unchanged native runtime `572bd52c`. Earlier extraction evidence remains in
`retained-runtime-extraction1.json` (3,216 native, 3,409 default and 40 Python
tests) and `retained-e739edeb-binary.json`; it is not relabeled as final evidence.

## Full43 and independent held-out evidence

The Full43 packets use the same 18,643,482,956-byte native artifact
`perf-current-c71a558e.vortex`, admitted SHA-256
`93acc7b9bbabed1f6e15a91aeacda45637bd5d6c5fed26e9b2052bf9b77e84f2`,
and SQL SHA-256 `4afa04814edf3a4c52ff26fd87ea3b5dd92c7264b2d8d69ee718709f3df6f09b`.
The host is arm64 macOS 26.6.2 with 10 logical CPUs. Each request supplies
24 GiB and parallelism 12; those are requested limits, not physical RAM or
proof of equal admitted worker counts. Each query runs in a fresh process,
including complete CLI output and process exit; OS cache state is uncontrolled.
UAT is `/Users/dylan/LocalData/shardloom/clickbench-100m-uat`; each packet below
retains its `summary.json` and raw query records under UAT `logs/`.
Full43 uses the frozen `9152a92b` complete-float/gzip harness with Existing
policy and no topology flags; its harness identity is separate from the tested
`572bd52c` native binary. The later held-out and resident packets use the frozen
`5d2bf2b1` harness.

| Packet | Runtime | Complete comparisons | Best-sum | Hot-sum | All raw samples | Maximum child RSS |
|---|---|---:|---:|---:|---:|---:|
| `full43_20260908T184428081909Z` | `9152a92b`, Existing | 129/129 | 88.661862 s | 88.791328 s | 269.051447 s | 7,237,746,688 bytes |
| `full43_20260908T191423419110Z` | `572bd52c` | 129/129 | 91.215296 s | 91.292520 s | 278.782517 s | 7,196,688,384 bytes |

Best-sum sums each query's best of three samples. Both packets complete without
guard failures. The frozen runtime's best-sum is 2.88% higher than the fresh
reference; uncontrolled cache/host state and sequential execution prevent
attributing the difference to one change. Do not substitute an older slower
reference to infer a gain. These are retained ShardLoom result comparisons,
not independent oracles; the timings and RSS peaks do not establish a stable
performance effect. Historical `e739edeb` packet
`full43_20260908T183709910205Z` passed 129/129 at 94.068430 s best-sum, 6.10%
higher than the same fresh reference; it predates the source-grant correction.

Independent held-out packets compare `48182c5a` and `572bd52c` against a Python
oracle at requested worker limits 1/2/4/8/12. All are explicitly not claim grade.

| Packet | Fixture and scope | Outcome |
|---|---|---|
| `heldout_operators_20260908T192912965178Z` | 4,096-row JSONL preparation; 19 cases; one warmup plus one sample per arm/limit | 380/380 passed; complete semantic matrix |
| `heldout_operators_20260908T193104530231Z` | 131,072-row JSONL preparation; two integer-distinct TopK cases; one warmup plus three samples | 80/80 passed; selected-case semantics |
| `heldout_operators_20260908T194120159296Z` | 131,072-row Parquet preparation; the same two cases and sample schedule; required candidate distinct-worker evidence | 80/80 passed; selected native worker paths established |

The JSONL-prepared schemas are all nullable and decline the intended integer
distinct worker paths. Their passing results prove semantics only. The Parquet
fixture is written by PyArrow 25.0.1 with required integer fields before native
preparation and query timing; PyArrow is not an execution fallback. Its native
artifact SHA-256 is
`dae23af24dc88544c159cd60c636263f167bf9424f12b6549714bd7ad8e104d8`.
Required evidence covers both `exact_integer_distinct_topk` and
`repeated_integer_distinct_topk`: the complete integer-distinct strategy,
completed chunks and committed rows, with no outstanding worker jobs. Requested
parallelism one can execute the worker family inline; this packet does not
claim simultaneous CPU utilization, scaling or throughput. It covers two of
19 cases, not the full operator or public resident matrix.

The earlier `heldout_operators_20260908T185727217270Z` packet retains 760/760
semantic comparisons for `48182c5a`/`e739edeb` at 4,096 rows. It remains earlier
checkpoint evidence and does not certify the final runtime's worker paths.

## Review correction and remaining acceptance

Review found that extraction omitted the ordinary aggregate's cap to the CPU
grant of its held native source. A request above host capacity could therefore
pass a wider policy to scan/aggregate admission than the source owner admitted.
The committed restoration caps ordinary work to that held-source grant and restores
typed provider-driver evidence for schema-declined workers. This is a resource
contract correction, not acceptance of the parked topology experiment. The
`572bd52c` receipts above include its broad and final-binary checks. Three
focused source-grant tests also pass in `retained-runtime-source-grant-tests2.log`;
the initial compile failure from a missing test import remains recorded.

The tested PGO helper correction resolves an explicit llvm-profdata override,
otherwise the selected compiler's sysroot component, then PATH, with no discovery
during print-only planning. It makes no PGO speedup claim. The
[native compatibility export packet](retained-native-export-2026-09-08.md)
remains unchanged: all 96 output checks pass, with material 65,536-row gains,
mixed small-case costs and separately reported source/publication guarantees.

The first resident attempt, `resident_call_paths_20260908T193128221001Z`, failed
the unchanged 256 MiB log budget at 268,509,184 bytes. Its evidence was archived
and byte-verified before owned-file retirement, recorded in
`interrupted-resident-20260908-archive.json`. It supplies no completed acceptance
or timing claim. The rerun `resident_call_paths_20260908T194141789969Z` passes
all 1,674 checks across nine cases, fresh CLI processes, persistent CLI workers
and the public Python client, both binaries, one warmup and 30 samples. Native
source reuse and fresh execution state pass on the candidate, with complete
results and explicit no-fallback evidence. Lossless output compression runs
after each timed call. Its 2,429,866-byte readable summary fits the 8,953,856-byte
reservation within the unchanged 256 MiB total log ceiling. The packet remains
bounded acceptance, not a production serving-throughput or scaling claim.

| Final-head acceptance item | Status |
|---|---|
| Freeze reviewed source, release binary and harness identities | Recorded above |
| Source-grant/driver regressions, PGO helper and broad checks | Passed; review-fix receipt and final 54-test harness receipt above |
| Final Full43 and bounded independent semantics | Passed; no overall throughput win established |
| Required candidate integer-distinct worker paths | 80/80 Parquet checks passed; selected-case scope |
| Complete public prepared-aggregate/resident matrix | 1,674/1,674 passed; prior interrupted attempt remains failed evidence |
| Exact-head PR checks and merge receipt | 40/40 passed; `pr1437-5d2bf2b1-premerge-checks.json` and `pr1437-merged-native-acceptance-gate.json`; merged `d51429e3`, no package publication |

Keep native Vortex execution/output, explicit compatibility boundaries, owned
temporary-run cleanup and no external-engine fallback. Broader ingest/resource
curves, multi-source results, production worker-to-spill transfer and independent
performance acceptance remain open. The post-merge
[ingest packet](retained-ingest-owner4-2026-09-08.md) is complete: control
`75fc09a0` P2 took 99.446032 seconds and frozen accepted candidate `572bd52c` P4
took 95.447305 seconds, with four confirmed constructed owners per arm. Complete
values match and both generated outputs were verified and retired. This is
neither an equal-public-setting comparison nor proof of simultaneous CPU use or
a stable speedup. Full43 used the protected reference artifact, so query acceptance
on this new physical output remains open. The September 12
[implementation/test sequence](../architecture/ingest-performance-implementation-2026-09-12.md)
prioritizes measured CPU-stage balance, bounded writer overlap and duplicate
representation work without reopening the rejected topology experiment.
