<!-- SPDX-License-Identifier: Apache-2.0 -->

# Native as-of join acceptance — October 29, 2026

The [as-of contract](../architecture/native-time-series-completion-2026-10-29.md)
passes local implementation and conformance acceptance. SQL, DataFrame, typed
native relation, embedded workflow and CLI declarations use the same native
matching family. This completes the as-of join unit within active item 20;
calendar windows, resampling, gap filling, interpolation and combined item-20
acceptance remain open. This report contains no new benchmark observation or
performance claim. Installed-package and hosted acceptance are separate.

## Matching and ownership

Inputs may be unsorted. Every left row retains its input position and selects
at most one right row within optional equality-key partitions. Backward,
forward and nearest direction, exact-match exclusion and inclusive nonnegative
tick tolerance are explicit. Duplicate source order and nearest-distance ties
follow the contract; NULL keys do not match. Exact integers and matching temporal
domains are admitted. Temporal kind, unit and timezone conversions require an
explicit cast. Float, string and decimal ordering keys remain unsupported.

The resident path owns the native right batches and a charged stable row index.
Explicit spill reuses native ordering runs and bounded lookup/gather owners.
Both paths call the same selection algorithm, use exact widened distance
arithmetic and preserve the existing session grant. No external execution engine
or new runtime dependency participates. Certificates remain
`fallback_attempted=false` and `external_engine_invoked=false`.

## Complete correctness and failure checks

- Native tests compare against independent exhaustive references over unsorted,
  duplicate, NULL and multiple-batch populations, including fresh deterministic
  cases and signed/unsigned integer extremes. Fifteen temporal domains exercise
  seven matching policies; incompatible units, zones and kinds fail explicitly.
- The public matrix executes 120 positive declarations across all five entry
  points and compares all 30,840 result rows and exact schemas. It covers integer
  and zoned nanosecond ordering, two equality keys, decimal/list payloads, all
  directions, exact/excluded matches and tolerance. Source deletion, producer
  closure and later result reuse preserve values and owners.
- Six real Vortex, Parquet and Arrow IPC writes reopen after the original sources
  are removed and compare all 1,542 persisted rows and schemas. These are real
  payload checks. Three incompatible type pairs also fail through all five
  interfaces with the expected diagnostic and no-fallback evidence.
- A 32,771-row right side with 512-byte payloads fails resident admission at
  16 MiB, then completes explicit native spill under that same grant. An ample
  resident control agrees. The spill check requires real runs and merges,
  bounded reported reservations/quota, exact output and completed owned cleanup.
- Allocation denial, cancellation, corrupt runs, producer and consumer failures
  release state and permit a fresh retry. A zero output limit still drains
  producers and preserves late failures. Existing ordinary joins retain their
  six-kind public regression coverage.

These are executable complete-result assertions against independent reference
logic, not a replay of saved per-row public envelopes. Resource assertions cover
the existing lease and spill contracts; they do not establish a total process
RSS ceiling or measure upstream scratch/decode bytes.

## Validation and exact source linkage

| Gate | Result |
| --- | --- |
| Native Vortex and CLI library suites, `release-user-surfaces`, `ci-native` | 4,581 passed; 32 existing opt-in ignores |
| Default workspace, all targets | 3,277 passed; no ignores |
| Native Vortex/CLI/Python all-target strict Clippy | Passed |
| Default workspace all-target strict Clippy | Passed |
| `cargo fmt --all -- --check` | Passed |
| Python query-builder and generated-reference tests | 61 and 15 passed; no skips |
| Generated API, user-surface, public status/claims and contribution checks | Passed |
| Final native product public checks | Three methods passed; no skips |

Builds use Rust 1.99.0 and the unchanged locked dependency graph. The portable
packet records base revision `0eaad60c4d2daba240c92a7e798b951a25ddce55`, complete
changed Rust/Python sources, source identities for accepted gates and both public
attempts. Native library-suite linkage covers the exact Vortex/CLI sources;
final lint and build linkage also covers the Python binding. The only intervening
binding edit clarifies the public duplicate-rule documentation. Final public
tests include the added persistence and five-interface refusal assertions.

Final executable SHA-256:
`0e7a4003504889af0d95e0b20e768cd6483c0f9601adce29fac9cfbcdec8183c`.
Final native Python library SHA-256:
`904cfd8dc4251034763a0653207a2cbbfe42fa0de54aca940bc1af30323ab7f7`.
Pre/post identity checks bind public execution to those products and test sources.

The [portable evidence packet](evidence/native-asof-joins-2026-10-29.json.xz)
contains 84 size/hash-verified members. It is 457,692 compressed bytes, SHA-256
`4858b640e12cac90d99e8c744cc2fc77bd882c1a3a06c4819ad857ad6e860c4c`;
the 4,306,479 uncompressed bytes have SHA-256
`a969bdbac8cfce3617ecc434488298551eacf2889598209465ad0fd41bc1bdc2`.
All 20 build/check attempts remain represented, including five failed development
attempts: test-compilation corrections, strict-lint corrections and the compact
equality-key parser correction. Passing prefixes are not used as completed gates.

The packet includes guarded drivers, public checks, exact sources and raw logs.
To reproduce, bind its recorded local roots to a fresh checkout/output location,
use the recorded feature/profile/resource settings and build the native CLI and
Python binding before the public checks. Existing test modules remain the CI
entry points. Do not reuse a completed attempt's output path.

No Full43 or 347-query cohort was rerun for this new capability. The existing
performance observations remain unchanged. Package publication, whole-program
acceptance and competitive-gate completion are not implied by this unit.

The [staged-library cleanup](evidence/native-asof-joins-2026-10-29-cleanup.json)
records retirement of two task-owned test copies after exact hash/size and
closed-handle checks, recovering 230,021,472 bytes. The final built library,
build/source records and all failed-attempt evidence remain retained.
