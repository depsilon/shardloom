<!-- SPDX-License-Identifier: Apache-2.0 -->

# Profile-guided compilation decision — October 23, 2026

Status: **finite investigation closed; PGO dropped**. The query comparisons,
held-out checks, pressure cases and installed-package checks pass, but the
unchanged control fails the frozen ingest byte-identity gate. Its historical
reference was prepared with different parallelism. The experiment therefore
cannot establish the required complete-workflow benefit or break-even reuse
count. The gate and failed observation remain unchanged; three planned ingest
roles are unrun. Portable ThinLTO remains the default.

This closes the finite `PERF-13.pgo` investigation, not an engine capability or
competitive gate. At the maintainer's direction, broader required, conditional
and optional work is paused. The next work is cleanup and a general review for
material improvements to the existing compute engine. The three
region-dependent investigations remain blocked, and the 38 optional rows stay
deferred.

## Frozen build and evaluation boundary

The protocol binds 2,243 source files, including 1,922 Rust/Cargo files matching
the restored accepted engine. Control, instrumented and profile-use builds use
fresh separate target directories and identical features and resource settings.
No runtime source change or external-engine execution is introduced.

A prior configuration crashed before successful training because native C
objects inherited incompatible profiling instrumentation. An eighteen-command
Rust/C probe reproduces the crash and verifies the correction: disable C/C++
profiling equally in all three roles while profiling Rust. Each fresh build
checks all 39 native objects for absent profiling symbols. The failed setup,
original protocol and pre-training amendment remain in the packet. A separate
control readback recovery reuses the completed build; it does not rebuild it.

Training uses sixteen cases at one, four and twelve requested workers, covering
nine selected strategy families: 48 CLI queries, two preparations, twenty native
Python operations with nineteen complete-value checks, and five pressure calls.
Full43 and the seventeen held-out cases are excluded from training. Two freshly
generated profiles are merged and transferred with identical hashes. The
candidate reports 3,922 missing-function-profile warnings and no profile hash
mismatch. This is partial training coverage, not whole-program coverage.

## Query observations

Each Full43 table entry contains one execution of all 43 queries and checks all
381 independent reference rows. The standard pass uses the isolated procedure;
the two paired orders are the prospective comparison specified by the frozen
protocol. No extra query repetitions were added after seeing results.

| Observation | Control engine seconds | PGO engine seconds | Outcome |
| --- | ---: | ---: | --- |
| Standard candidate pass | — | 53.109073585 | Complete values |
| Paired order A | 50.401856537 | 50.195823624 | 0.409% lower aggregate engine time |
| Paired order B | 50.835039255 | 50.652708911 | 0.359% lower aggregate engine time |

The immediate deviation review retains every threshold crossing. Q4 repeats in
both orders: control **0.495365249 / 0.492065791** engine seconds and candidate
**0.730426334 / 0.730968917**. The unchanged physical strategy and repeated native
CPU increase are consistent with a localized compiler tradeoff; the exact
generated-code cause is not isolated. Other crossings do not repeat in both
orders, and their individual causes remain unresolved. The deviation threshold
triggers investigation; it is not an automatic rejection rule.

All 204 held-out calls pass exact values across seventeen cases, three worker
settings and both role orders. Both aggregate held-out gates pass. Each build
also passes five custom ordering/spill/quota calls, sixteen public pressure and
failure/cleanup cases, and 121 installed native-binding tests without failures,
errors or skips. Package evidence is limited to this macOS arm64 host and Python
3.12 with a cp310-abi3 wheel; it is not a multi-platform acceptance claim.

## Ingest gate failure and attribution

The first ingest wrapper fails to import its helper under the selected Python
interpreter before starting the engine. A recorded amendment changes only that
wrapper interpreter. The recovered control ingest completes, but its Vortex
bytes differ from the historical reference. No candidate ingest runs.

The original reference requested twelve workers and admitted ten; the experiment
requests and admits two. With the same 24-GiB policy and source footer, existing
memory admission selects 32,768-row source batches for the reference and
262,144-row batches for the experiment. A metadata-only replay of all 226 source
row groups and the unchanged task-grouping algorithm reproduces **3,120 versus
423 batches** exactly. No source-policy change is needed to explain the layout
difference. The same-byte comparator was inappropriate for those settings.

A separately recorded diagnostic executes one isolated Full43 on the new
control output. All 381 reference rows pass at **43.268958791 engine seconds**.
This diagnoses correctness of the benchmark results; it is not a matched layout
performance comparison, an all-column equivalence proof, an accepted replacement
input, or a repair of the frozen retention gate. The planned candidate,
candidate and final-control ingests remain unrun. Complete-workflow savings and
break-even remain unavailable. The original failure is never replaced by the
diagnostic result.

## Portable evidence and verification

The [packet](evidence/native-pgo-2026-10-23/packet.tar.gz) contains 2,335 members,
including complete saved query results, clocks, commands, compiler records,
protocols, amendments, source identities and the
[closeout](evidence/native-pgo-2026-10-23/closeout.json). Its SHA-256 is
`76762a527aabceb972a2d1c9760f1748ed420f78ed610d6ee4bdc74f24f9cdbc`.
Physical binaries, profiles, datasets, build caches and environments are omitted;
their recorded identities and regeneration basis remain. Replay validates saved
observations and cannot regenerate compiler profiles or replace physical reruns.

The [verifier](evidence/native-pgo-2026-10-23/verify_pgo.py) authenticates the
complete archived Python import trees before importing helpers. It bounds outer
and nested archives, rejects symlinks and optimized Python, independently
recomputes result values and engine totals, and requires the explicit drop
closeout. Its [controls](evidence/native-pgo-2026-10-23/test_pgo.py) repair outer
identities before exercising twenty-one semantic mutations and one frozen
protocol mutation, plus four helper/symlink controls and 23 archive controls.
The [index](evidence/native-pgo-2026-10-23/manifest.json) binds these artifacts.

```bash
python3 -B docs/benchmarks/evidence/native-pgo-2026-10-23/verify_pgo.py \
  docs/benchmarks/evidence/native-pgo-2026-10-23/packet.tar.gz
python3 -B -m unittest python.tests.test_native_pgo_evidence -v
```

The CI wrapper relocates the packet and repeats replay with sequential floating
accumulation to cover Python 3.11's behavior. The engine is never executed during
replay. Native source is unchanged from the reviewed and tested restoration;
the new work changes evidence, replay tests and queue documentation only.
