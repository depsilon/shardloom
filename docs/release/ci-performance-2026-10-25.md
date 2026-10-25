# CI performance and coverage preservation

Status: accepted and merged in PR #1605 on October 25 at
`167ac248ba662ae7967a7801995e9b160b17bae5`.

The October 25 maintainer instruction places this CI change before further
engine optimization. After #1605's final native check exposed a regression-test
assertion race, the maintainer authorized folding this CI revision and test
correction into #1605 and rerunning it, superseding the earlier freeze and
separate-PR plan. This batch changes how validation is built, scheduled and
reused, not engine behavior or release scope. It extends the existing CI
work-shaping and release-gate contracts.

## Decision and acceptance

1. Evaluate `ci-native`, inheriting `dev` with optimization level 2, debug
   information level 1, debug assertions and overflow checks enabled, and no
   LTO. Retain the ordinary debug workspace and native public-surface checks.
   Keep native fixtures, assertions, grants and exclusions unchanged initially.
2. Build the native library test executable once. Discover its complete test
   and ignored-test inventories, assign every test exactly once, and execute
   duration-balanced partitions through the existing Rust test harness. Reject
   missing, duplicated or unexpected tests and expanded exclusions. Preserve
   child-process tests, fault hooks and distinct observation-feature coverage.
3. Move the eight patched-provider test/lint configurations out of the native
   suite's serial chain. Preserve every manifest, feature, patch, target and
   doctest obligation, with reproducible per-manifest resolution and deliberate
   compatible Cargo cache sharing.
4. Build each identical Linux CLI and abi3 wheel once. Bind artifacts to revision,
   operating system, architecture, Rust toolchain, Cargo lockfile, features,
   profile and ABI. Validate that same wheel under Python 3.10 and 3.13, keeping
   macOS, Windows, debug and observation-feature products distinct.
5. Connect the existing work-shaping plan to job-level selection. Treat unknown
   paths, manifests, dependencies, CI and executable/document fixtures
   conservatively. An always-running summary must reject failed, missing,
   cancelled or unexpectedly skipped selected jobs and explain every omission.
6. Attribute fixture preparation and comparison cost before changing either.
   Any later harness optimization must retain complete values, actual multi-run
   spilling, merge behavior and cleanup at the original memory grant.

Acceptance compares complete required-PR elapsed time and summed job execution
time, including build, transfer and verification overhead. Record runner and
cache context; do not present cross-platform or warm/cold differences as causal
proof. No reduced test inventory, new ignores, timeout increases, retries,
scheduled-only hard tests or extra in-run concurrency are part of this change.

The principal risks are silently omitted tests, reusing a differently configured
binary, stale dependency resolution and incorrect success after a skipped job.
Negative tests must exercise those boundaries. Local default/native checks and
remote platform/package checks remain required before acceptance.

## Source references

- [Cargo profiles](https://doc.rust-lang.org/cargo/reference/profiles.html)
  specify optimization, assertions and overflow checks independently.
- [Rust test harness](https://doc.rust-lang.org/rustc/tests/index.html)
  documents executable discovery, exact filters and ignored-test behavior.
- [GitHub job outputs](https://docs.github.com/en/actions/how-tos/write-workflows/choose-what-workflows-do/pass-job-outputs)
  connect an earlier plan to dependent jobs.

No external execution provider, package publication or competitive-gate closure
is introduced. CG-5/CG-6 claims still require their independent evidence.

## Baseline and local evidence

The completed [#1604 CI run](https://github.com/depsilon/shardloom/actions/runs/40167963537)
and [CodeQL run](https://github.com/depsilon/shardloom/actions/runs/40167963538)
at `b42df2870c0786dd8dce03475f7b53b9672aef22` provide the successful remote
baseline. All 44 check results complete in 6,918 seconds from the earliest
workflow creation; 42 actual CI/CodeQL runner jobs consume 19,709 summed job
seconds. The native job consumes 6,719 seconds, including 5,280.82 seconds in
its unoptimized library executable (3,269 passing tests and 32 existing ignores).
Runner consumption counts each actual job once, excluding CodeQL's summary and
the external website check; elapsed completion includes every check. These are
CI clocks, not query engine measurements or weighted billing estimates.

Initial local macOS/arm64 evidence with Rust 1.99.0:

| Check | Result |
| --- | --- |
| Cold `ci-native` compilation | 512.993 seconds; actual compiler metadata verifies optimization, assertions and overflow checks |
| Four partitions, executed serially on this host | 3,295 passed, 32 existing ignores; all 3,327 names assigned once; 404.706 summed partition seconds |
| Eight frozen provider configurations | 6,603 passing tests, two existing ignores; all eight strict Clippy runs pass; includes buffer integration tests and eight doctests |
| Real package producer and consumer | One Mac ABI3 wheel reused for isolated load, 140 binding tests and uninstall; separate extension passes all 13 observation tests; no skips |

The local suite contains the additions already present in #1605. Local build and
partition observations are not a cross-host speedup comparison against the
GitHub baseline. The completed remote acceptance is recorded below.
Native partitions each use one libtest thread; the old
native job used the harness default. This scheduling change is recorded
separately from profile optimization.

The previous [#1605 native job](https://github.com/depsilon/shardloom/actions/runs/40189897295/job/121158694023)
fails one cancellation/source-change test after 3,294 other passes and 32 ignores;
its other 43 checks pass. It is retained as a failed observation, not a successful
comparison or merge-ready result. That run remains tied to
`782c636c59a6686090bebcb07fe27b4641644e13`; its diagnosis and regression correction
belong to the explicitly authorized CI/test revision of #1605.

The failure is reproduced by validating the mutated source before the writer
starts. `SourceIdentity::validate` deliberately reports `changed` on detection
and `generation invalidated` on subsequent uses. A parallel reader can win that
detection race, while the old test accepts only `changed`. The corrected test
also forces the already-invalidated case, checks the exact generation diagnostic
family, and retains cancellation, existing-output preservation and zero-credit
assertions. It adds held-identity release and no-leftover-file checks. No reader,
writer, source-identity or publication implementation changes are needed.

Final local validation of the CI/test revision:

| Check | Result |
| --- | --- |
| Corrected optimized native suite | 3,295 passed, 32 unchanged ignores; complete ownership across four partitions; 394.997 summed partition seconds; all child processes drained |
| Ordinary workspace tests | 3,256 passed across 107 targets, no ignores |
| Source-change regression in ordinary debug | All three cancellation/changed/already-invalidated scenarios pass |
| Distinct Rust observation feature | 16 observation tests and nine credit-window tests pass |
| Formatting and strict Clippy | Workspace/all-target and native CLI/Vortex checks pass |
| Python validation | 170 release-script, 65 CI-helper, 17 native-wheel-helper and seven notice tests pass |
| Workflow and documentation | All 26 contract lanes, actionlint, public-status/docs, website readiness and ten link tests pass |

The Python release checks initially expose an outdated dependency-freshness
inventory: artifact reuse increases download steps from 14 to 24. The reviewed
inventory and its provenance note are updated together; all 170 checks then
pass. The failure remains retained alongside the successful observation. A
separate wiring inventory verifies that every download has a producer and the
final summary depends on exactly the other 25 jobs.

Local ordinary debug checks omit debug symbols for storage, retaining assertions
and checked arithmetic. The `ci-native` executable independently verifies its
explicit debug level 1 and optimization level 2 from Cargo compiler metadata.
The completed GitHub run establishes Linux artifact transfer, identical-wheel
proof across both Python interpreters, remaining platform coverage and total CI
cost. These CI/test changes do not require another Full43 engine observation.

## Completed remote acceptance

The [CI run](https://github.com/depsilon/shardloom/actions/runs/40198905498)
and [CodeQL run](https://github.com/depsilon/shardloom/actions/runs/40198905526)
pass at head `c9affef41210aa7dcc913cc18c6db6678978127b`, without retries.
All 57 checks succeed. The final summary selects all 26 job groups, omits none,
and verifies all 25 preceding job outcomes. Matrix expansion and aggregation
increase the check count without repeating the complete native suite.

| Complete observation | Successful #1604 baseline | Accepted #1605 revision |
| --- | ---: | ---: |
| All checks complete after workflow creation | 6,918 seconds (1h 55m 18s) | 2,425 seconds (40m 25s) |
| Actual CI/CodeQL runner jobs | 42 | 55 |
| Summed runner execution | 19,709 seconds | 14,536 seconds |

Observed elapsed completion is 64.9% shorter and runner execution is 26.2%
lower. These are two completed runs, including builds, artifact transfer and
verification. They are not repeated statistical evidence, weighted billing or
an isolated causal measurement of profile optimization. The native-test, debug
CLI and release CLI producers report cold caches. Preserve the failed earlier
#1605 run separately.

Remote native coverage verifies all 3,327 names exactly once: 3,295 pass and
the original 32 remain ignored. Linux Python 3.10 and 3.13 consume the identical
wheel SHA-256 `125c63c85221cd5f975fb7f9690a5386cb0110e01253b32630f3a98ed6a304d9`;
each passes 140 binding and 13 observation tests without skips. macOS verifies
the same test counts against its separate arm64 products. Windows, ordinary
debug checks and all provider configurations also pass. The automated review
reports no findings at the reviewed head, and #1605 merges without a source
revision after acceptance.

The [remote evidence packet](evidence/ci-performance-2026-10-25/manifest.json)
retains the successful baseline, complete final job/check snapshot, computed
comparison and coverage artifacts. The provider artifacts identify GitHub's
synthetic merge checkout `edb791cb05eb16f107643314c1cd96a5e9fb9e08`, distinct
from the PR head. The packet also retains the guarded local retirement result:
temporary binaries, package environments and completed compiler outputs are
removed after closed-handle and generation checks; current inputs, accepted
runtime/wheel and compact results remain. No engine-performance or publication
claim follows.

## Fixture-cost attribution

The existing 133,137-row aggregate pressure test can emit opt-in callback cost
observations with `SHARDLOOM_CI_FIXTURE_TIMING=1` and libtest `--nocapture`.
Input callback construction, expected-value preparation and complete output
validation are timed separately from the enclosing test execution. The remaining
time is unattributed execution work, not a newly defined engine clock.
The row count, 512-byte keys, 16-MiB constrained grant, ample-memory comparison,
denial, actual multi-run spill/merge, complete values and cleanup assertions stay
unchanged. No fixture reduction is admitted without measured evidence.

The local opt-in observation covers all six denied/ample/spilled scenarios across
grouped and ungrouped execution. Their combined enclosing time is 26.275627
seconds: input callbacks account for 0.115834 seconds, expected-value preparation
for 0.001104 and validation callbacks for 0.249186 (1.394% together). The grouped
spill still produces 2,600 runs and 1,299 merge passes; the ungrouped spill produces
259 runs and 129 merge passes, both with complete output and cleanup checks.
This provides no evidence that fixture/verifier rewriting is a material next
optimization, so those semantics and data sizes remain intact.

Vortex-first check: this is test-harness observation around the existing admitted
Vortex array and ShardLoom relational/spill providers (`baseline_or_oracle_only`).
It introduces no execution provider, layout, input/sink abstraction or residual
handler. Vortex-native execution and the existing result-validation boundary stay
unchanged; no external engine is invoked.
