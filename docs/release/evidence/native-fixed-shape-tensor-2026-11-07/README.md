<!-- SPDX-License-Identifier: Apache-2.0 -->

# Fixed-shape tensor lifecycle acceptance

This immutable local acceptance records checked tensor identity and native
lifecycle correctness at source base `2df6460cae18eb55b2a0d680a55721d438bc495d`. Hosted tensor
review/integration is pending at this observation. It does not complete item 19,
claim a performance gain, change an independent CG gate or publish a package.

`acceptance.json` binds 28 changed source files,
14 accepted check stages, the final CLI/library/wheel,
and all 230 members of `verification.tar.xz`.
The archive preserves raw outcomes, source snapshots, original failures,
provider review, test/harness corrections and the completed preceding binary
and dependency integrations. Both preceding PRs pass 58 checks; their exact
signed merges each pass all 55 required main checks and complete retirement.

Default/native Rust suites pass 3,319/6,955 cases; 37 existing opt-in native tests
remain ignored. The public Python shard reports 838
test runs and 49 optional/environment skip
records, separately from 1192 discovered
cases. The clean installed package passes all 350
tests with no skips, including 12 tensor and 19 context tests, then uninstalls.

Broad Rust suites precede only the Arrow constant's feature gate. Exact source
projections and resolved feature graphs establish equivalent enabled code; final
strict native lint and the minimal feature check cover both branches. The final
runtime source matches its build exactly. Later Python changes touch only a stale
test assertion and the expanded installed-package test selection. The original
mixed-core failure and first lint failure are retained, with explicit dispositions.

From the repository root, replay the saved proof with:

```sh
python3 docs/release/evidence/native-fixed-shape-tensor-2026-11-07/verify.py --repo-root .
```

Copy this packet directory elsewhere and run `python3 verify.py` for standalone
replay. Verification hashes and inspects saved evidence; it does not rebuild,
install, run queries or contact the network. Original and relocated replay pass.
Five controlled corruptions are rejected, including resealed failed-check and
skipped-test records. `corruption-controls.json` preserves exact mutations,
driver source, observations and logs.

Fixed shapes, supported primitive components, metadata fidelity and explicit
refusals are the current boundary. PyArrow canonical-type normalization and
zero-product export limits are documented in the
[implementation contract](../../../architecture/native-fixed-tensor-lifecycle-2026-11-07.md).
Variable shapes, tensor arithmetic, additional representations, devices and model
execution remain separately queued or explicitly unsupported.
