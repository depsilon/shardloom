<!-- SPDX-License-Identifier: Apache-2.0 -->

# Chrysalis exact partial native source coverage acceptance

`validation.json` records local acceptance across 15 passing source, native, artifact
and installed-package checks, five preserved original failures, and the frozen
three-role comparative evaluation for [RFC 0045](../../../rfcs/0045-partial-native-source-coverage.md).
The default workspace suite passes 3,351 tests across 108 targets; enabled-native
passes 4,449 tests with 42 declared ignored across 17 targets (`src/lib.rs` discovers
4,474 unit tests: 4,432 passing and 42 ignored, including 17 `columnar_coverage_`
unit tests across 4 partitions); public CLI passes 1,257 tests across 8 targets;
focused Python passes 3/3; and the fresh isolated installed wheel passes 468 tests with
zero errors, failures or skips and verified uninstall. These populations overlap and
must not be summed together.

`observations.json.gz` retains exact receipts, logs, release build manifests, the
installed-wheel proof, the frozen 18-request exposure manifest (`exposure-manifest-r2.json`),
candidate admission (`candidate-admission-r1.md`), prospective comparison protocol and
boundary review (`comparison-protocol-r1.md`, `comparison-boundary-review-r1.md`),
runtime bindings (`comparison-runtime-bindings-r1.json`), executable comparison harness
scripts, all three completed comparison receipts (`direct`, `full-preparation`, `partial`),
the five preserved original failures with exact pre-fix source snapshots, and the
42 candidate code/test/baseline files verified by the build/test receipts.

Preserved original failures retained without deletion:

- `exposure-r1`: failed (`returncode=1`, `AssertionError: query observation failed`) under
  `exposure-manifest-r1.json` due to the corrected SUM Float64 expectation before freezing `r2`.
- `coverage-native-full-r1`: failed (`returncode=101`) in cyclic reader tests before
  accounting for bounded `columnar_coverage_bytes` separately and testing warm/reclaim paths.
- `coverage-native-cli-lint-r3`: failed (`returncode=101`) with a `clippy::redundant-closure-for-method-calls`
  lint in `shardloom-vortex/src/resident_session.rs`.
- `coverage-public-cli-r3`: failed (`returncode=101`) when `resident_source_generation_validation`
  asserted `all_input_owners_released` before updating to `input_delivery_owners_released;bounded_optional_source_coverage_reported_separately`.
- `coverage-installed-wheel-r1`: failed (`returncode=1`, 465/468 passing, 3 test methods / 29
  subtests failing) because `NativeSession::close()` did not call `reclaim_optional_memory()`.

Comparative decision: **locally retained** (`partial` engine sum **149,794,248 ns** vs
`direct` **150,509,625 ns**, `-0.48%`; `full-preparation` query sum **95,469,499 ns** plus
**985,052,876 ns** explicit preparation and **1,171,740,248 ns** complete public workflow
vs `partial` **245,960,709 ns** and `direct` **282,764,791 ns**). All eight non-reusing
control groups regressed (`+4,901,207 ns` total), and `public_query_terminal_nanos` rose
from `169,037,209` to `170,560,665` ns (`+0.90%`). See the
[benchmark summary](../../../benchmarks/native-chrysalis-partial-coverage-2026-11-21.md)
for exact per-group and work-avoidance numbers. No large or general causal speedup,
repeatability, or competitive-gate closure is claimed.

Replay with only the Python standard library:

```sh
python3 -I -B docs/release/evidence/chrysalis-partial-coverage-2026-11-21/replay.py \
  docs/release/evidence/chrysalis-partial-coverage-2026-11-21 --repository .
```

Copy `replay.py`, `validation.json`, and `observations.json.gz` elsewhere and omit
`--repository` to verify integrity, preserved failures, and comparison decision gates
without a checkout or native runtime build. Replay executes no queries, imports no native
library, and spawns no subprocesses.

`verification.json` records a passing relocated standalone replay, repository-bound replay
binding the 42 candidate files and the
[shared-native incorporation predecessor archive](../shared-native-incorporation-2026-11-18/README.md),
and four rejected corruption controls (altered source bytes, flipped comparison decision,
erased preserved failure, and an installed-test skip). Hosted acceptance remains pending
required CI checks. `fallback_attempted=false`, `external_engine_invoked=false`,
`publication_attempted=false`, and `claim_gate_status=not_claim_grade` remain unchanged.
