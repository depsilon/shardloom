<!-- SPDX-License-Identifier: Apache-2.0 -->

# Native analytical aggregates and grouping acceptance

The shared native runtime now supports statistical and paired moments, exact
quantiles, distinct additive reductions, ordered selectors, list/string
collections, aggregate-local filters and grouping sets through all five public
interfaces. Native, SQL, installed-package and complete-result checks pass.
This accepts the aggregate/grouping work unit within
[item 20](../architecture/native-analytical-completion-2026-10-27.md), whose
remaining performance, temporal, recursive, numeric and workflow obligations
stay open. It does not establish that the cases losing to Polars are fixed.

## Implemented behavior

- Population/sample variance and standard deviation, covariance and correlation
  share compatible charged centered state. Scaling preserves representable
  results without raw squared/cross-product overflow; original-width integer
  differences and pairwise NULL domains remain explicit. Window summaries
  merge selected blocks without unstable subtraction of departed observations.
- Exact distinct SUM/AVG and median/continuous/discrete quantiles preserve
  original typed membership and ordering. Grouped ordered spill and moving
  window state have separate reservations and deterministic failure paths.
- FIRST/LAST, ARG_MIN/ARG_MAX, LIST/ARRAY_AGG and STRING_AGG retain declared
  local ordering, stable ties, NULL and output-type semantics. Native payload
  owners, compact ordinals and existing native runs carry their state; complete
  individual collection results still require an output reservation.
- Aggregate FILTER excludes false/NULL predicates before argument observation.
  SQL/DataFrame computed arguments are lazily guarded. Filters participate in
  source demand, aggregate identity, compatible state reuse and framed spill.
- GROUPING SETS/ROLLUP/CUBE expand native batches from one producer into the
  existing aggregate owner. Duplicate/distinct sets, genuine versus subtotal
  NULL keys, empty input and GROUPING/GROUPING_ID masks retain their explicit
  semantics through HAVING, ordering and post-aggregate expressions/windows.
- Completed identical pure expression results may be reused on the same input.
  Conditional branches keep their selected-input evaluation domains. No
  answer cache, alternate evaluator or per-benchmark branch is introduced.

The [public reference](../reference/native-analytical-aggregates.md) specifies
the names, aliases, examples, result types, resource limits and unsupported
combinations. CLI SQL, embedded workflows, Python SQL, DataFrame declarations
and typed native relations lower into these same owners. Vortex remains native
input/output; compatibility inputs use their declared adapters.

Vortex-first decision: implement the relational kernels over admitted Vortex
0.90.0 arrays, masks, native gathers, list/constant constructors and persistence.
The pinned registry has no replacement flat relational grouping/collection
owner. No dependency or query-engine integration is added, and execution retains
`fallback_attempted=false` and `external_engine_invoked=false`.

## Complete 347-case checkpoint

The new isolated observation validates all **347 cases and 12,565 complete result
rows**, including schema continuity, in **31.962394957 engine seconds**. Its
immediate [Parquet completion baseline](native-parquet-demand-completion-2026-10-27.md)
is **34.960427343 engine seconds**, an **8.6% observed reduction**. The earlier
50.877676708-second observation is preserved and is not the immediate baseline.

Both checkpoints use the same original case identities/order, five verified
Parquet files, independent expectations, Python SQL surface, 2-GiB native memory
policy and two admitted lanes. Each case executes once. Other task-owned jobs
remain idle during execution; standard storage, workload, deadline, complete
output and cleanup guards remain active. Full hashes precede/follow execution.
No cache manipulation, competitor run or Full43 repeat is performed.

| Family | Cases | Baseline engine seconds | Candidate engine seconds |
| --- | ---: | ---: | ---: |
| Projection | 26 | 6.154385 | 6.116541 |
| Predicates | 16 | 0.787894 | 0.762005 |
| Nullable | 8 | 0.130049 | 0.132712 |
| Numeric | 4 | 0.608927 | 0.593880 |
| Strings | 5 | 1.331113 | 1.330063 |
| Temporal | 6 | 0.879635 | 0.874553 |
| Global aggregation | 67 | 3.268936 | 2.532611 |
| Distinct | 4 | 0.328720 | 0.329950 |
| Ordering | 10 | 1.239656 | 1.235066 |
| Joins | 14 | 2.174269 | 2.143485 |
| Sets | 4 | 0.004348 | 0.004312 |
| Grouped aggregation | 180 | 16.535105 | 14.386201 |
| Multiway joins | 2 | 1.213723 | 1.217455 |
| Composite | 1 | 0.303665 | 0.303560 |
| **Total** | **347** | **34.960427** | **31.962395** |

Grouped aggregation improves by 13.0% and global aggregation by 22.5% in this
observation. No case exceeds both a 20% and 0.010-second regression; no family
exceeds both 20% and 0.050 seconds. These thresholds were frozen prospectively.
The inherited 10% benefit threshold passes for the declared grouped family.
Capability acceptance independently requires correctness and ownership evidence.

One observation does not establish statistical confidence, isolate a mechanism's
contribution, or prove an across-the-board win. The original Polars observations
use Arrow IPC and an API clock; they remain prioritization evidence and have
not been rerun. The 347 cases protect established behavior; they do not substitute
for the separate tests of new capabilities. Complete engine observations appear
in [case-results.csv](evidence/native-analytical-aggregates-2026-10-27/case-results.csv);
other clocks and supervision remain in the diagnostic packet.

## Correctness, resource and package gates

| Gate | Receipt | Result |
| --- | --- | --- |
| Native Vortex library, `release-user-surfaces`, `ci-native` | `r138` | 3,399 passed; 32 existing opt-in tests ignored |
| Native CLI library, same features/profile | `r139` | 1,030 passed |
| Default workspace, all targets | `r141` | 3,268 passed |
| Strict native/default workspace Clippy and formatting | `r131`, `r140`, `r142` | Passed |
| All-feature, no-default and minimal Parquet-reader checks | `r143`–`r145` | Passed |
| Frozen release CLI/native Python build | `r133`, `r134` | Passed |
| Nine new feature suites, five interfaces, Vortex and Parquet | `r135` | Passed; zero skips |
| Isolated installed wheel, load/integration/uninstall | `r147` | 194 passed; zero failures, errors or skips |
| Python source discovery | `r148` | 872 run; 28 existing optional/environment skips |
| Reference, parity, route and capability declaration validation | `r149` | Passed, including all three new lazy grouping methods |
| Portable complete-result replay and corruption controls | `r150`–`r153` | Passed; all 11 deliberate corruptions rejected, including from a clean source-only directory |
| Contribution governance | `r154` | Passed |

Required workspace commands are `cargo fmt --all -- --check`,
`cargo clippy --workspace --all-targets -- -D warnings`, and
`cargo test --workspace --all-targets`; receipts retain exact locked/offline
commands and feature/profile choices. Native tests exercise original-width
integer differences, extreme finite values, exact Decimal128 interpolation,
NULL pairs, frame units/exclusions, local order ties, native spill, low memory,
cancellation, failed delivery, source mutation and successful retry. Public
fixtures compare complete typed output to independent exact/reference results;
non-CLI interfaces prohibit subprocess execution.

All 52 failed intermediate development attempts remain in the packet alongside
their passing corrections. They include compiler/lint findings and semantic,
representation, memory and public-surface failures found during implementation.
The initial documentation generator invocation selected system Python 3.9 and
failed before generation; Python 3.13 corrected the environment without a runtime
change. No benchmark query was failed, omitted or repeated in this checkpoint.

## Identity, replay and scope

The measured runtime is based on `04e7d92a` plus the archived exact source overlay;
that parent commit alone does not identify the candidate. Final native lint and
test receipts match the compiled Rust source identities. Source-only capability
and reference updates follow the measured build and pass separate checks.

| Artifact | SHA-256 |
| --- | --- |
| Candidate CLI | `ec54c732c66f9c73f34c023df2515002d6d7be25f9c394824a5c9c70ac285c44` |
| Candidate native Python library | `014d54c9de04d1b4f1ae0ee43d76130059a952bbea7231426c4d46005b80783f` |
| Installed-tested macOS arm64 wheel | `67830e2b63cc7d435cfa4951111142236519bfd579a7dd1d2e546e0d7c04501b` |
| Prospective protocol | `425b37eaf2d2ef8b6d9ca03e8f0e4e2f6dff1aa62f771b66c6c3afb4ee463d86` |
| Portable archive | `c16f885c7bbaba82e945c7d4dbe077bb9af11efc7e49ada9ccccf4a0b1c58e02` |

The [manifest](evidence/native-analytical-aggregates-2026-10-27/manifest.json)
links source/runtime identities, complete results, references, guards, failures,
package proof and the unchanged preceding packet. The verifier reads bounded
content-addressed data and never executes archived code or a query. Hashes prove
internal consistency, not independently signed provenance. Large inputs and
binaries are represented by identity; the packet does not reconstruct them.

After verification, [retirement](evidence/native-analytical-aggregates-2026-10-27/cleanup.json)
removes 14,011 completed generated regular files and six environment links with
exact identity and closed-handle checks. Temporary package/environments, copied
verifier sources, duplicated raw benchmark evidence, archived source snapshots
and superseded runtimes/wheel are retired. The latest installed-tested runtime
and wheel, all five Parquet inputs and compact complete/failure evidence remain.
No payload backup or storage-limit change occurs. Post-retirement replay passes
in `r156`; [supplemental verification](evidence/native-analytical-aggregates-2026-10-27/verification.json)
retains the final replay, corruption, governance and cleanup receipts.

```sh
python3 -B scripts/verify_analytical_aggregate_packet.py \
  docs/benchmarks/evidence/native-analytical-aggregates-2026-10-27
python3 -B -m unittest discover -s scripts \
  -p test_analytical_aggregate_packet.py -v
```

PR #1614 merged at `690a671e155072f9afa7c5938412c94f173ef553` after all 57
hosted checks passed on `6e40c404bb0bb96c006d43bc1b9c9152f511fcf0`, with no
review threads or requested changes. The
[hosted receipt](evidence/native-analytical-aggregates-2026-10-27/hosted.json)
records every check. The conformance-count correction binds its gate to the
118-method Python inventory. No benchmark observation was replaced for this
metadata correction or merge.

General decimal rounding/widened integer sums, temporal fidelity, recursive and
materialized ownership, application batch functions, structural scaling, work
diagnostics and the six complete workflows remain open. Remaining file-format
and interface parity follows that work. No package publication, release,
competitive-gate closure or completed Polars-performance campaign is claimed.
