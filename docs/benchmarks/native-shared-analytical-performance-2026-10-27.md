<!-- SPDX-License-Identifier: Apache-2.0 -->

# Shared analytical performance acceptance

The native runtime retains two shared improvements: completed numeric aggregate
updates are reused under identical state/input conditions, and bounded streamed
ordering copies only its retained prefix after validating the complete input.
Both pass their prospective benefit gates, complete-result checks, resource and
ownership tests, and installed-package validation. A separate borrowed text/binary
output candidate misses its benefit gate and is removed.

This is a locally accepted work unit within
[item 20](../architecture/native-analytical-completion-2026-10-27.md).
Its remaining analytical and performance obligations stay open. Hosted PR
acceptance is separate; no new package release or external-engine victory is
claimed.

## Retained execution changes

Numeric SUM/AVG consumes a borrowed typed batch through one physical-type and
validity dispatch. Adjacent compatible measures may reuse a completed update
only when their input owner, filter domain and initial accumulator bits agree.
The copy occurs at the later measure's original position; an earlier failure
leaves later measures untouched. Grouped reuse is admitted only when its two
group-state passes cost no more iterations than the input pass they replace.
Exact native aliases share prepared input ownership, and identical adjacent
FILTER domains share their completed selection. Distinct/decimal states,
different starting states and different observation domains retain their paths.

An immediate bounded Sort/Limit over streamed input may retain an independently
owned prefix rather than the complete population. The existing fallible stable
ordering, gather and result builders implement this path. Every payload is
validated before ordering keys, and every source batch is released before the
next is requested. Stable ties preserve source order; zero demand still consumes
and validates the complete input. Small producer batches accumulate independently
owned pending rows up to four times the prefix before compaction. Actual bytes
remain subject to the native memory grant.

Initial admission requires ordinary value validation, no configured spill, a
prefix at most 16,384 rows and supported flat nondecimal payloads. Other plans
retain the existing native route. Ordinary nonstreamed error order is unchanged.
The ordering copy counter now records actual retained-row copies, including
repeated prefix compaction, and excludes validated rows never copied. In the
recorded 1M-row top-10 query it falls from **1,000,000 to 4,890**, while complete
input consumption and source detachment remain true.

Vortex-first review reuses admitted Vortex 0.90.0 primitive buffers, validity
masks, typed arrays, gathers and independent output ownership. Its grouped-array
reducers do not replace this flat relational grouping owner without additional
representation work. No dependency, evaluator, answer cache, scheduler or
external query-engine provider is added. Both changes serve the shared native
runtime behind all five public interfaces, preserving `fallback_attempted=false`
and `external_engine_invoked=false`.

## Complete observations and finite decisions

Each candidate runs exactly once through the same 347 cases, five verified
Parquet inputs, 1M-row fact-table corpus, complete independent expectations and
Python SQL surface. Every pass validates all **12,565 result rows and schemas**.
The native policy admits two lanes and 2 GiB. Task-owned builds, agents and other
work remain idle during query execution. Standard storage, concurrency, deadline,
result and process-cleanup guards remain active; full hashing and recursive
inventories occur before/after queries. There is no cache manipulation, artificial
pacing, new competitor observation or routine Full43 repeat.

| Candidate | Baseline engine seconds | Candidate engine seconds | Observed declared-family change | Decision |
| --- | ---: | ---: | --- | --- |
| Typed numeric update/reuse | 31.962394957 | 30.118456795 | Global aggregation 12.9% lower; grouped aggregation 7.5% lower | Retain |
| Borrowed variable-width output | 30.118456795 | 29.225110351 | Projection 8.9% lower; strings 6.9% lower | Drop |
| Bounded streamed ordering, after removing the dropped candidate | 30.118456795 | 28.017558877 | Ordering 40.5% lower; projection 14.8% lower; strings 15.8% lower | Retain |

Each frozen protocol requires a 10% reduction in the complete cohort or a
declared target family, correct complete output, ownership/resource proof and
disposition of every material regression. The borrowed-output candidate improves
the cohort by 3.0% and neither target family reaches 10%; its implementation and
call sites are removed. Its source overlay, tests, original failures and complete
observations remain in the evidence packet. Its result never becomes the
ordering candidate's baseline.

The retained numeric and ordering cohorts improve by 5.8% and 7.0% respectively
against their immediate baselines. These are single observations, not isolated
causal estimates or statistical confidence. Changes in unrelated families are
not attributed to the candidate. The earlier
[aggregate/grouping checkpoint](native-analytical-aggregates-2026-10-27.md)
and original Polars observations remain unchanged. Polars used Arrow IPC and
a complete API clock; those observations prioritize work and do not establish
a matched-format or engine-clock comparison.

Material regression means both 20% and 0.010 engine seconds for a case, or both
20% and 0.050 seconds for a family. No family is flagged. Two original case flags
remain in their original cohort totals:

| Cohort | Original case | Original baseline → candidate engine seconds | Bounded diagnostic disposition |
| --- | --- | --- | --- |
| Numeric reuse | `projection_limit_direct_31` | 0.117059333 → 0.150251250 | Not reproduced; role medians 0.117325541 → 0.109783958 |
| Bounded ordering | `aggregate_metric_scalar_mixed_arithmetic_all` | 0.065623334 → 0.081370208 | Not reproduced; role medians 0.080949125 → 0.067029125 |

Each separately frozen diagnostic executes the flagged case and two specified
neighbors in three pairs ordered AB, BA, AB: 18 executions per diagnostic.
All complete results pass, and no case reproduces the original material-regression
rule. The original causes remain unassigned. Diagnostics do not replace original
samples, erase flags or establish statistical confidence. The numeric diagnostic
uses LIMIT 11/31/100; the ordering diagnostic uses direct, arithmetic and COALESCE
scalar aggregate arguments.

[Case and family results](evidence/native-shared-analytical-performance-2026-10-27/case-results.json)
retain all three complete comparisons. The
[replay result](evidence/native-shared-analytical-performance-2026-10-27/replay.json)
retains both diagnostics and finite decisions. Other clocks and resource
observations remain in diagnostic evidence.

## Validation

| Gate | Receipt | Result |
| --- | --- | --- |
| Native Vortex library, `release-user-surfaces`, `ci-native` | `r21` | 3,407 passed; 32 existing opt-in ignores |
| Native CLI library, same features/profile | `r35` | 1,030 passed |
| Default workspace, all targets | `r23` | 3,268 passed |
| Strict native and workspace Clippy; formatting | `r20`, `r22`, `r24` | Passed |
| Minimal Parquet-reader feature build | `r31` | Passed |
| Frozen release CLI and native Python build | `r25`, `r26` | Passed |
| Public streamed-ordering suite | `r27` | 29 cases passed |
| Analytical feature suites, all five interfaces, Vortex/Parquet | `r32` | Nine suites passed; zero skips |
| Installed wheel, load/integration/uninstall | `r34` | 194 integration tests passed; zero skips |
| Complete result replay and corruption controls | `r36`–`r38` | Passed, including an isolated source-only copy and 18 rejected corruptions |
| Verified generated-payload retirement and subsequent replay | `r39`, `r40` | Passed |

Required workspace commands are `cargo fmt --all -- --check`,
`cargo clippy --workspace --all-targets -- -D warnings` and
`cargo test --workspace --all-targets`. Recorded commands add locked/offline
dependency resolution and bounded test concurrency. Native tests use
`cargo test --locked --offline -p shardloom-vortex --features release-user-surfaces
--profile ci-native --lib -- --test-threads=2`; the CLI gate uses the analogous
`-p shardloom-cli` target.

Focused tests cover encoded and nullable original-width numeric inputs,
divergent starting states, filter domains, exact aliases, high group cardinality,
cancellation and failure order. Ordering tests cover stable multikey ties,
1/17/257-row batches, every-row payload validation, source release, retained output
lifetimes, memory denial, consumer failure and successful retry. A 16-MiB payload
population is processed under a 4-MiB ordering grant. Existing nested, spill and
ordinary ordering paths remain covered by the native/public suites.

All five failed intermediate attempts (`r02`, `r04`, `r11`, `r17`, `r19`) remain
alongside their passing corrections. The packet checks exact build/test source
identities; no benchmark case is omitted or repeated to improve a cohort total.

## Identity, portable replay and cleanup

The retained numeric code is commit `42b5d2f5`; the measured ordering runtime is
based on `4aa53216` plus its exact archived source overlay. A parent revision alone
does not identify the measured candidate. Required native/workspace receipts bind
the same Rust source identities as the frozen release build. Later report,
verification and PR metadata changes do not replace that runtime observation.

| Artifact | SHA-256 |
| --- | --- |
| Retained CLI | `ac19b7bda03811c2945e8db5c42c01af6f6999f3f7e6346dd7ba07eab84f924e` |
| Retained native Python library | `f40be8041b7179d272c8603824082b9c11674c8a88882ab09557ca91ad2b553e` |
| Portable archive | `5564d2680d322a5e68a4dc2c2837212b82615bb25aec2f739f22906a124493da` |

The [manifest](evidence/native-shared-analytical-performance-2026-10-27/manifest.json)
links complete actual/expected results, typed contracts, runtime/source identities,
prospective protocols, all development receipts/logs and the unchanged accepted
aggregate/grouping packet. The verifier recomputes the complete comparisons and
diagnostics from bounded data, without executing archived code or queries. Its
positive test and 18 corruption controls pass again from isolated source-only
copies without datasets or binaries. Hashes establish internal consistency, not
signed provenance; large inputs and binaries are represented by identity.

After verification, [cleanup](evidence/native-shared-analytical-performance-2026-10-27/cleanup.json)
retires 18,514 completed generated regular files and six environment links after
exact identity, duplicate-inventory and closed-handle checks. The current tested
CLI/library/wheel and five original Parquet inputs remain. Archived raw logs and
source copies, superseded runtimes, temporary package environments and duplicate
packet copies are removed. Compact complete/failure evidence, user files, active
Cargo cache and unfinished item-18 work remain intact. Post-retirement replay
passes; [supplemental verification](evidence/native-shared-analytical-performance-2026-10-27/verification.json)
retains the final receipt/log and cleanup identities.

```sh
python3 -B scripts/verify_shared_analytical_performance_packet.py \
  docs/benchmarks/evidence/native-shared-analytical-performance-2026-10-27
python3 -B -m unittest discover -s scripts \
  -p test_shared_analytical_performance_packet.py -v
```

Temporal fidelity, recursive/materialized ownership, wider numeric policy,
application batch functions, structural scaling, meaningful work diagnostics,
six complete workflows and remaining measured performance gaps stay in item 20.
Format/rich-data parity follows under its existing owners. This work does not
close item 20, CG-1 through CG-23, platform readiness or package publication.
