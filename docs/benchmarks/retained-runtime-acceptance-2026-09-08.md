# PR #1437 retained-runtime acceptance

Status: checkpoint evidence recorded; final-head acceptance remains pending.
The shipping extraction retains the prepared/native runtime and compatibility
export work while excluding the rejected topology runtime and unbuilt isolated
Python prototype. The scoped export retain decision stands. The Full43 records
below establish complete-value agreement for earlier binaries; they do not
establish a final-head throughput improvement or close the remaining PERF gates.

## Source and binary checkpoints

| Checkpoint | Exact identity | Scope |
|---|---|---|
| Extraction correctness | `15102a6af835e51bea2883c63e71be34b64e66b6` | Validation receipt records an empty runtime diff; docs/prototype extraction was separate |
| Extracted shipping source | `e739edeb59c7c3ae0184a671b776e1e30c854214` | Tree `745eafb84060734b10141c19382fbc813a4095b8`; no topology runtime or isolated Python prototype |
| Extracted release CLI | `0236e19c21ffd3e2806530347e730f56b7c58d76e6569b38b945f6aa02d13431` | Binary SHA-256 for `candidate-e739edeb`, release-user-surfaces, rustc 1.98.0 / LLVM 22.1.8 |
| Fresh existing-policy reference | `9152a92b7761bb9a124268f25e726a8094c637a7` | Binary SHA-256 `5fa36ae99ceca0782b228851c2e9b5dd8186f0ac1a07d4e4c86dc943a89932ab`; experimental branch with Existing policy |
| Final shipping source and CLI | Pending | Must include the reviewed source-grant restoration and PGO lookup correction; earlier binary results do not certify this head |

Local evidence root is `/Users/dylan/LocalData/shardloom/perf-all-20260906`.
`retained-e739edeb-binary.json` pins source, toolchain, binary and build log.
`retained-runtime-extraction1.json` records these completed serial checks:

| Check | Recorded outcome |
|---|---|
| Formatting; default workspace, native release-surface and minimal-native Clippy | All passed |
| Default workspace tests | 3,409 passed, zero failed |
| Native CLI/Vortex tests with release-user-surfaces | 3,216 passed, zero failed; nine manual benchmark/fixture tests ignored |
| Four Python harness modules | 40 passed |

These counts describe the extraction checkpoint, not tests added during PR
review. The exact commands and per-stage log identities remain in the receipt.

## Full43 and independent held-out evidence

Both Full43 packets use the same 18,643,482,956-byte native artifact
`perf-current-c71a558e.vortex`, admitted SHA-256
`93acc7b9bbabed1f6e15a91aeacda45637bd5d6c5fed26e9b2052bf9b77e84f2`,
and SQL SHA-256 `4afa04814edf3a4c52ff26fd87ea3b5dd92c7264b2d8d69ee718709f3df6f09b`.
The host is arm64 macOS 26.6.2 with 10 logical CPUs. Each request supplies
24 GiB and parallelism 12; those are requested limits, not physical RAM or
proof of equal admitted worker counts. Each query runs in a fresh process,
including complete CLI output and process exit; OS cache state is uncontrolled.

| Packet under UAT `logs/` | Runtime | Complete returned-value comparisons | Sum of per-query best of three | Maximum observed child RSS |
|---|---|---:|---:|---:|
| `full43_20260908T183709910205Z` | `e739edeb` | 129/129 | 94.068430 s | 7,186,300,928 bytes |
| `full43_20260908T184428081909Z` | `9152a92b`, Existing | 129/129 | 88.661862 s | 7,237,746,688 bytes |

The extracted binary's observed best-sum is 6.10% higher in this sequential
comparison. Do not replace the fresh control with an older slower run to infer
a gain, or attribute the entire difference to one change. These are retained
ShardLoom result comparisons, not independent oracles. Both packets complete
without guard failures. Their raw query distributions remain in `summary.json`;
neither the best-sum nor the two RSS peaks establish a stable performance effect.

The separate `heldout_operators_20260908T185727217270Z/summary.json` passes its
complete 760-record matrix at 4,096 rows: 19 cases, requested worker limits
1/2/4/8/12, one warmup plus three samples for both `48182c5a` and `e739edeb`.
It uses an independent Python oracle and a renamed native fixture with SHA-256
`e33225a1b499f709ab2648773a3eaa83508a07caa46e95093fccd93ad898a202`.
This supports bounded semantics at that checkpoint; it is explicitly not claim
grade and does not prove final-head worker utilization, scaling or throughput.

## Review correction and remaining acceptance

Review found that extraction omitted the ordinary aggregate's cap to the CPU
grant of its held native source. A request above host capacity could therefore
pass a wider policy to scan/aggregate admission than the source owner admitted.
The staged restoration caps ordinary work to that held-source grant and restores
typed provider-driver evidence for schema-declined workers. This is a resource
contract correction, not acceptance of the parked topology experiment. The
earlier broad tests and `e739edeb` measurements predate it. Source review found
no blocker. Three focused source-grant tests and 47 Python harness tests pass;
the first focused compile failure (a missing test import) remains recorded.
Broad and final-binary acceptance are still required. Subsequent acceptance
receipts and the exact merge gate are recorded in the PR before merge; this
document preserves the pre-freeze checkpoint rather than relabeling old results.

The PGO helper correction also remains subject to final-head checks: explicit
llvm-profdata override, otherwise the selected compiler's sysroot component,
then PATH, with no discovery during print-only planning. It makes no PGO speedup
claim. The [native compatibility export packet](retained-native-export-2026-09-08.md)
remains unchanged: all 96 output checks pass, with material 65,536-row gains,
mixed small-case costs and separately reported source/publication guarantees.

| Final-head acceptance item | Status |
|---|---|
| Freeze reviewed source and release binary identities | Pending |
| Source-grant/driver regressions and PGO helper checks | Three native and 47 Python harness tests passed; logs `retained-runtime-source-grant-tests2.log` and `retained-runtime-reviewfix-python.log` |
| Required formatting, feature checks and appropriate broad tests | Pending |
| Complete public prepared-aggregate/resident matrix | Pending |
| Final native query/correctness acceptance and measured performance disposition | Pending; do not reuse earlier binary identity |
| Exact-head PR checks and merge receipt | Pending; no package publication authorized |

Keep native Vortex execution/output, explicit compatibility boundaries, owned
temporary-run cleanup and no external-engine fallback. Broader ingest/resource
curves, multi-source results, production worker-to-spill transfer and independent
performance acceptance remain open. The next ingest packet is the bounded
control-P2/candidate-P4 comparison with four predicted constructed owners per
arm, conditional on merge and required public acceptance; wider curves or new
implementations require informative evidence and material benefit.
