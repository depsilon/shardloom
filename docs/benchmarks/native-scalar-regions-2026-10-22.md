<!-- SPDX-License-Identifier: Apache-2.0 -->

# Bounded scalar-region decision — October 22, 2026

Status: **candidate dropped; accepted engine restored**. The finite region
prerequisite investigation under `PERF-03.regions` is decided. Neither the
mechanism nor its exact local reduction is retained. Both complete Full43
comparisons miss the original benefit gate, and the held-out metadata control
violates the required exclusion rule. The held-out experiment stops at that
failure; its remaining 41 calls are not run or credited.

`PERF-10.ordered_candidates`, `PERF-09.design` and `PERF-12.topology` remain
blocked by their missing retained region/reduction/cost prerequisites. These
are three explicit dependency outcomes, not three completed experiments.
The separate current-source attribution remains valid after exact restoration,
so `PERF-13.pgo` can proceed to its own frozen protocol. Optional breadth stays
deferred. No whole owner contract, CG gate, capability or release is completed.

## Candidate and frozen contract

The pre-implementation protocol binds the Vortex 0.85 provider review, source
`3d07013f`, immutable input and complete independent references. It compares
generation-bound natural-region scalar consumers with the existing scan path,
preserving original batches and the existing CPU/memory owner. A bounded
directory and existing jobs supply ordered completion, cancellation and drain.
No external query engine executes native work.

Local additive reduction admits only integer ranges whose worst-case total
remains exactly representable in F64; AVG divides after complete reduction.
Floating and large-integer additive evaluation keep their existing policy.
The candidate excludes metadata answers, unsupported shapes and unproved
layout/size conditions. The original protocol's complete-operation threshold
is at least 2% Full43 engine-time benefit in both opposite-order cohorts,
with unchanged per-query and resource controls. Held-out targets additionally
require actual activation, exact values and their own frozen cost gates.

The final measured candidate is `3e8bb1c2a76262454c0ae759766a9197517462c5`.
Before measurement, review caught missing public-report counters; the CLI
correction and its regression test were included in that build. Fifteen native
region regressions and the public propagation regression pass. Final validation
includes strict workspace/native lint, 3,240 workspace tests, 1,101 public CLI
tests and the source-matched native suite's 3,206 passes with 32 existing manual
ignores. The native-suite reuse excludes only the subsequent CLI reporting
change and is explicitly recorded. Earlier fixture, formatting and lint
failures remain in the packet.

## Observations and immediate deviation follow-up

Each Full43 row below contains exactly one execution of all 43 queries per
listed role, with all 381 independent result rows checked. The standard passes
use the isolated procedure; the two paired cohorts are the separately frozen
comparison, not routine repeats.

| Observation | Control engine seconds | Candidate engine seconds | Outcome |
| --- | ---: | ---: | --- |
| Prior accepted standard pass | 49.953164785 | — | Complete independent values |
| Candidate standard pass | — | 50.546043287 | Complete independent values |
| Comparison A | 47.974545789 | 48.532089507 | Benefit and per-query gates fail |
| Comparison B, reversed order | 49.222645578 | 49.143262923 | Benefit, per-query and one RSS control fail |
| Restored accepted engine, standard pass | 48.479745297 | — | Complete independent values |

The candidate standard-pass increase was concentrated in Q34 and Q3. Immediate
inspection found **no region activation in any of the 43 queries**. The
immutable input contains 876,721 physical layout nodes; the candidate's
whole-footer walk reaches its frozen 262,144-node bound before admission.
A bounded metadata-only diagnostic checks that geometry without rerunning a
query. The bound is not widened after observing the failure to activate.

Q34's unchanged grouping/equality counters and increased caller join wait track
its first increase, but the slowdown reverses in the second paired cohort.
This does not establish a persistent Q34 regression or identify host activity
or compiler layout as its cause. Q3 retains the same decode/accessor work and
also has no region activation. After restoration, Q3 is 1.184497875 engine
seconds, compared with 1.183572333 before the candidate; Q34 is 2.119595500,
compared with 2.148394000. The restored pass has no new per-query increase beyond
the comparison protocol's existing control tolerance. These single observations
do not establish a speedup.

## Mandatory held-out failure and removal

The first ten held-out calls pass complete exact values and their activation
contracts: four target pairs and the small-input control pair. Call eleven,
the candidate's metadata control, returns the correct `COUNT(*) AS n` value
for 4,194,304 rows but starts 64 region jobs. All 64 complete and drain; the
failure is the forbidden execution, not a wrong count or process crash.

The aliased public query enters the simple aggregate route, where the existing
footer aggregate declines the count-only shape and the candidate admission
accepts it. The native metadata regression paired COUNT(*) with a column
count and therefore missed this public count-only case. The frozen control
requires metadata completion, so the remaining held-out calls stop immediately.
No complete held-out score or performance acceptance is assigned.

An earlier paired-run launch rejected an unsupported `--compress-logs` argument
before any native query ran. The recorded amendment removes only that option
and uses fresh output directories; binaries, inputs, order, sample count and
gates stay fixed. The original failed launch, original driver and corrected
driver remain evidence. There are **226 executed candidate-experiment calls
out of 267 planned**, including the standard 43, paired 172 and held-out 11.
The later restoration pass is separate from those counts.

After the failed control, all fixture, binary, harness and source identities,
process-group drain and lock release pass closeout. The ten changed Rust files
are removed or restored in `da8225510a5612d5a0cddf5df2723c93b25e5c60`.
All 1,922 Rust/Cargo file hashes match the accepted baseline exactly. The
candidate source remains in local commits and the portable evidence packet.
Fresh formatting, workspace lint and workspace tests pass after restoration.
The required isolated Full43 uses the same immutable accepted executable,
whose entire build-source map matches; no replacement compiler build is claimed.

## Portable evidence

The [experiment packet](evidence/native-scalar-regions-2026-10-22/packet.tar.gz)
contains 414 members: raw calls and timing records, the frozen protocol,
candidate source, build/check evidence, controls and failure closeout. SHA-256:
`d79e24b01ee9d04efa0167895fce6d41f072c4053c14f2a8674bf9db21c81c9d`.
Its [verifier](evidence/native-scalar-regions-2026-10-22/verify_regions.py)
reconstructs all 226 complete values, engine clocks, role order, activation and
frozen gate decisions. A passing replay confirms the **drop decision**; it does
not turn the incomplete experiment into acceptance.

The separate [restoration packet](evidence/native-scalar-regions-2026-10-22/restoration.tar.gz)
contains 369 members, including the prior/restored raw Full43 observations and
source/check binding. SHA-256:
`b148481bdb72f250e3a4d9894ad264818da982ba4237372cd1d2c13dca676cf8`.
The [restoration verifier](evidence/native-scalar-regions-2026-10-22/verify_restoration.py)
replays both complete passes and the exact source restoration. These packets
retain binary/input identities without bundling their physical bytes.

```bash
python3 docs/benchmarks/evidence/native-scalar-regions-2026-10-22/verify_regions.py \
  docs/benchmarks/evidence/native-scalar-regions-2026-10-22/packet.tar.gz
python3 docs/benchmarks/evidence/native-scalar-regions-2026-10-22/verify_restoration.py \
  docs/benchmarks/evidence/native-scalar-regions-2026-10-22/restoration.tar.gz
python3 -B -m unittest discover -s python/tests -p test_native_region_evidence.py -v
```

The [index](evidence/native-scalar-regions-2026-10-22/manifest.json) binds the
archives, replay receipts and runnable corruption controls. Thirteen experiment
controls and seven restoration controls repair outer identities before checking
semantic rejection. CI also replays the separate
[current attribution packet](native-current-attribution-2026-10-22.md) and its
fifteen semantic controls from a relocated directory. Replay executes no native
engine, requires no dataset/compiler/network, and does not replace physical
reruns. ThinLTO, Vortex-native persistence and no-fallback boundaries stay intact.
