<!-- SPDX-License-Identifier: Apache-2.0 -->

# Full43 comparison after native nested composition

The complete paired run passes all 258 retained-result comparisons. Candidate
`9f172abf` and retained control `e1133f69` are effectively unchanged at the
predeclared investigation thresholds: best-query sums differ by +0.31%, median
sums by −0.02%, and no individual timing or memory change triggers a repeat.
This is local regression evidence for the
[nested composition unit](../architecture/native-nested-composition-2026-10-02.md),
without a causal speedup, latency guarantee or official benchmark claim.

## Frozen workload and procedure

- Run: `paired43_20261003T025420873276Z`, October 3 UTC. All 43 statements in
  `benchmarks/clickbench/queries.sql` ran three times on each executable, with
  role order alternating by query and repetition. Every observation is retained.
- Dataset: the existing ClickBench hits Vortex artifact, 99,997,497 rows,
  15,682,956,489 bytes, prepared under profile 0.3.3. Its SHA-256 is
  `5a6e7d7b521b891e522f32398ef2582367e814791ab537f354003ecd2f7a488d`.
  No full-size input was regenerated in this comparison.
- Host: Apple M5, arm64, 10 logical CPUs, 16 GiB physical RAM, macOS 27.0.
  Both roles requested the same 24-GiB operation policy and maximum parallelism
  of 12. Those settings are separate from physical RAM and observed process RSS;
  they do not impose a total-process RSS ceiling.
- Candidate: clean revision `9f172abf8a087752aad8de280628ed889c885b7a`, optimized
  `release-user-surfaces` build using Rust 1.99.0. Executable SHA-256:
  `90cd3002521dad5cad27904939e8a22bd1b0aed55bde80586f721a4af13b0941`.
- Control: retained accepted revision `e1133f6981833b461e1dd6a131385a61285c6468`.
  Executable SHA-256:
  `7cfffec4f65ab2df146c2d567186d7bdc8b5a2c94abd29ec9aebdd9498cfdceb`.
- Timing covers each complete public CLI process, including startup, result
  output and exit. Input hashing, reference validation, host snapshots and log
  archiving are outside that process clock. The supervised comparison took
  390.303368 seconds, including harness work; query sums below are not elapsed
  end-to-end workflow time.
- The input was hashed before the run. Each call starts a fresh process; OS
  caches and ordinary host activity are uncontrolled. There is no answer cache
  or forced cache purge. Observed one-minute load averages span 1.356–9.392.
  Native builds, tests and other timed cohorts did not overlap this run.
- Complete results are checked against all 43 retained ShardLoom references,
  using exact structural values and the existing `1e-12` finite-float tolerance.
  This is a regression reference, with no fresh external correctness oracle.
  Every successful call proves no fallback and no external-engine invocation.

## Results

| Metric | Control | Candidate | Candidate change |
| --- | ---: | ---: | ---: |
| Sum of each query's fastest valid run | 50.739772 s | 50.895000 s | +0.3059% |
| Sum of each query's median | 51.688471 s | 51.678695 s | −0.0189% |
| Sum of all 129 native process times per role | 155.427660 s | 155.302692 s | −0.0804% |
| Geometric mean of fastest valid query times | 0.539185 s | 0.539872 s | +0.1274% |
| Maximum observed native-process peak RSS | 5,239,193,600 bytes | 5,265,850,368 bytes | +0.51% |
| Complete results matching retained references | 129/129 | 129/129 | All pass |

The largest absolute median timing changes were Q35 (−0.291802 s, −9.07%),
Q17 (+0.183669 s, +7.23%) and Q34 (+0.177750 s, +6.45%). All per-query timing,
CPU and RSS observations remain in the evidence, including smaller differences.
The largest absolute median RSS change was Q34, down 97,615,872 bytes (1.97%).

The frozen screen requires both 10% and 100 ms for a per-query timing change,
or both 10% and 32 MiB for a per-query median RSS change. Either aggregate timing
sum changing by both 5% and one second requires a complete reversed-order cohort;
individual flags require a targeted reversed-order cohort. No threshold was
crossed, so no repeat was required or substituted for these results.

The same retained control recorded a 73.215115-second fastest-query sum in its
earlier unpaired acceptance. Its 50.739772-second result here demonstrates that
identical code can have different absolute observations on this host. The paired
control is the comparator for this patch; the earlier measurement remains intact.

## Evidence and replay

The [portable acceptance packet](evidence/native-nested-composition-2026-10-02.json.xz)
contains the complete summaries, comparison calculations, host observations,
build/source identities, preflight, failed development observations and archive
manifests. It also records the independent public workflow matrix: 2,459 checks,
496 nested checks and 6,636,187 complete row comparisons.

Full43 summary SHA-256:
`0cb063b34ba56bafca30316ce814075844c461ee4b93a6509ad0b457c86c106f`.
Comparison JSON SHA-256:
`c109dce897dd44eb771408dbd9dd1ea11ff8b3e8aa2ea3f24d673baf1ccd1c87`.
All 43 per-query archives were read back and their member hashes verified.
Full results remain locally retained in those archives, with their original
byte identities in the portable packet.

Replay uses `scripts/run_clickbench_paired_query_uat.py` with the two frozen
executables, the exact resident input, all 43 references, the pinned query file,
`--memory-gb 24 --max-parallelism 12 --timeout 120`, and a fresh run directory.
Use the existing serial workload guard, UAT lock and storage ceilings. Preserve
all samples and apply the declared thresholds symmetrically. Wider workflow,
spill, adapter, production and competitive-gate obligations remain open.
