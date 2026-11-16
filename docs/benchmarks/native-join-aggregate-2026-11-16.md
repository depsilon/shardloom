<!-- SPDX-License-Identifier: Apache-2.0 -->

# Direct native join aggregation

The unique-dimension connection is locally retained. All 630 declared requests
pass, with complete engine time **94.178862655 → 91.576505630 seconds** (2.8%
lower). This is one scoped comparison using a retained baseline and a fresh
candidate. Most of the difference occurs on inactive paths; it does not
establish an isolated causal speedup. Hosted integration remains pending.

## Behavior and boundary

An admitted aggregate consumes exact source coordinates from completed unique
inner-join indexes. It preserves the original dimension-build order, every
intermediate validation barrier, logical group equality, batch phases and
ordered numeric updates. Native group state and finalization are shared with
ordinary aggregation. Joined payload arrays are absent at the admitted edges;
only bounded credited coordinates, observed group payloads and final results
are constructed.

Admission follows bound columns and pure aliases, including both star orders.
It initially supports one numeric fact measure or COUNT(*). Duplicate dimensions,
unborrowable representations, nullable-parent expressions, explicit spill and
other shapes keep ordinary ShardLoom-native execution. There is no dependency,
external executor, multiplicity-weighted SUM/AVG or execution fallback. The
[design](../architecture/native-join-aggregate-2026-11-16.md) records the native
Vortex provider review, semantic obligations and separately proved later scope.

## Complete observation

The selection was frozen before implementation: the same 600 join/neighbor
requests and 30 signed/sparse-key controls used for dense integer lookup. The
profiles contain 448 primary, 141 holdout, 28 smoke and 13 scale cases, across
five public interfaces and Parquet, Arrow IPC and Vortex. No case was selected
by timing or activation. All 630 prior candidate results are reused only after
exact request, input, reference, runtime, resource and engine-clock verification.
Every new candidate request executes once; there are no warmups or repeats.

The baseline runtime was built at `b21494e08858d219`; the candidate at
`1f80b27145fcbd212`. The later test-only commit `9ceeb5b493d767aa` changes no Rust
or package source and preserves the compiled runtime identities. Both use two
threads, a 2-GiB native grant and unchanged storage, deadline and result guards.
Full input/source inventories run before or after queries. The host is macOS
27 arm64 with Rust 1.99 and Python 3.13.13. Host/cache state is uncontrolled.

| Population | Requests | Baseline engine seconds | Candidate engine seconds |
| --- | ---: | ---: | ---: |
| Complete cohort | 630 | 94.178862655 | 91.576505630 |
| Direct aggregate active | 40 | 5.718050585 | 5.596205335 |
| Direct aggregate inactive | 590 | 88.460812070 | 85.980300295 |

There are 121 individual regressions totaling 0.376975212 engine seconds.
Twelve occur in the active subset. The original million-row `star_join` request
changes from 0.302577417 to 0.336076875 seconds; `star_join_products_first`
changes from 0.309911125 to 0.339888833 seconds. Both do activate direct execution
and deliver 960,700 aggregate contributions without joined payload construction.
Their regressions remain part of the decision. Reduced copying is proven work
elimination, not a per-request performance guarantee.

The 40 active requests include 35 primary cases, one scale case, three holdouts
and one smoke case; 23 use Parquet and 17 use IPC. None of this cohort's Vortex
cases activates the first borrowed-representation path. The tiny installed
Vortex fixtures do activate, so this is a physical-representation limit rather
than a Vortex format exclusion. Broader encoding admission is unproven.

All format and interface totals decrease. The frozen rule retains a complete,
correct, resource-admitted cohort with actual aggregate contributions and a
strictly lower total; it has no minimum percentage. This does not establish
repeatability, general superiority or closure of the wider item-20 roadmap.

## Validation and evidence

All nine source gates pass: formatting, strict default/native workspace lint,
full default/native workspace tests, all-features, no-default-features and
actual Rust 1.96 minimal/native checks. The suites pass 3,344 default tests and
7,333 native tests, with 42 classified native ignores. Seventeen focused tests
cover exact matches, both orders, aliases, dictionary/chunk equality, ordered
arithmetic, later-rejected invalid values, nulls, duplicates, denial,
cancellation, group growth and output lifetime.

All 456 isolated installed-wheel tests pass with no failures or skips. The
first run exposed one test assertion that incorrectly equated validation counts
across Vortex interface declarations; the correction checks each declaration's
exact fields. A separate focused check covers all 30 format/order/interface
combinations. Original failures remain retained. The earlier variable-key
footprint regression was repaired through separately credited metadata; the
full pressure suite passes without increasing its grant.

All 1,260 complete observations pass independent-reference, schema, resource,
ownership, clock and no-fallback replay. Standalone archive replay also passes
130 corruption controls and ten decision controls, with original-path reads,
execution-library imports, network and subprocesses forbidden. It executes no
queries. A first replay result-output path was correctly refused by that read
boundary; the same unchanged packet then passed with an external output path.

The checked-in [compact records](evidence/native-join-aggregate-2026-11-16/manifest.json)
contain the full [paired replay](evidence/native-join-aggregate-2026-11-16/replay.json.gz),
[protocol](evidence/native-join-aggregate-2026-11-16/protocol.json.gz),
[validation](evidence/native-join-aggregate-2026-11-16/validation.json.gz),
[decision](evidence/native-join-aggregate-2026-11-16/decision.json.gz),
[test correction](evidence/native-join-aggregate-2026-11-16/test-correction.json.gz)
and [portable verification](evidence/native-join-aggregate-2026-11-16/portable-replay.json.gz).
They are review records, not a standalone copy of every raw payload.

Raw results, scripts, source archives, inputs and runtimes remain in
`/Users/dylan/LocalData/shardloom/join-aggregate-2026-11-16`. Its retained
`join-aggregate-packet-r1` contains complete raw result stores and independent
references; run `python3 -I -B verify.py` with Python 3.13 or later and all packet
files together for offline verification. Runtime and input payloads remain
identity-only exclusions from that portable packet. No package was published.
