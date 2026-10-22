<!-- SPDX-License-Identifier: Apache-2.0 -->

Historical design record. The [final decision](native-multiway-join-2026-10-20.md)
does not retain this experiment; production code is restored. The prospective
protocol and observations below remain unchanged as provenance.

# Three-input native constraint probe: frozen comparison

Status: prospective protocol `multiway-probe-817-v1`, fixed before candidate
implementation and candidate timing. This follows the
[independent intake](native-multiway-join-intake-2026-10-20.md), with the same
eight sources, equalities, complete output columns, grant and source order.
It is one finite `PERF-10.multiway_joins` experiment under RFC 0044.

## Observed opportunity

The actual prepared binary path passes all eight workflows and 16 calls,
including an independent check of every one of 124,308 result rows and exact
schemas. The separate regression passes empty inputs, nulls, duplicate products,
repeated calls and repeated source aliases. Six corrupted-result controls are
rejected by the verifier. The verifier's initial uppercase `I64` expectation
was corrected to the pinned provider's lowercase serde spelling; no engine
result or fixture changed and the failed attempt remains recorded.

| Cell | Actual inner output rows per call | Final rows per call |
| --- | ---: | ---: |
| cyclic_sparse | 524,288 | 4,096 |
| cyclic_empty | 524,288 | 0 |
| cyclic_skew | 1,198,372 | 9,363 |
| cyclic_duplicates | 31,188 | 3,575 |
| cyclic_dense | 32,768 | 32,768 |
| cyclic_small | 64 | 64 |
| star_control | 4,096 | 4,096 |
| binary_control | 8,192 | 8,192 |

All three large target cells pass the intake's materialized-work prerequisite.
For example, the sparse cell delivers 25,427,968 intermediate array bytes per
call according to Vortex `nbytes`, before the final output. This is delivered
representation traffic, not unique allocation, decode bytes or exclusive
operator time. No instrumentation-overhead or CPU-isolation claim is inferred.

The frozen input manifest is
`multiway-intake-build-817a-input.json`, SHA-256
`f2ecdc90b841e3a2880d8b77092108648743d987c331333255f4427a5be120b5`.
All 1,902 Rust/Cargo inputs match the freshly compiled optimized test artifact
SHA-256 `d6057de47bdb50757a672a7407b10d702ad2c0be8eb53f41a7dbc35c62ba425c`.
The complete result stream SHA-256 is
`4d9d2e9eeda010b5139c689c7cab71da6f8fbf52710a6902772ca173f4194f26`.
The independent oracle SHA-256 is
`7858a4f8b6ed53715ad46a123d1d5f7e2d97c4739ab282f0f297ada6418a35fa`.
All are retained in the eventual portable packet.

## One candidate and its reuse contract

Implement a three-input constraint probe inside the existing prepared native
execution path. Build T's compound `(a,c)` index first and S's b index second,
using the existing `RowIndex`, `Batch`/`Table` and original-width `KeyColumn`
owners. While probing R.b, borrow R.a and each matching S.c directly to probe
T's exact compound domain. Gather R/S/T payload only after all three
equalities pass. Every matching T duplicate must be emitted for every matching
R/S pair. This preserves R/S/T ordinal order through the existing duplicate
chains without a new ordered index or output reorder buffer.

This strategy still enumerates R/S candidate pairs. It targets intermediate
materialization and repeated key preparation; it makes no worst-case join-bound
claim. Sorted-domain intersection was considered at intake, but would add
index construction and an order-restoration owner. The first candidate reuses
the current exact hash-domain access paths. No earlier rejected mechanism is
being reopened.

Recognize exactly two left-deep inner equijoins: one lower equality and two
upper equalities, with one upper key originating from each lower input. The
three variable columns must be distinct within each relation. Allow key-order
permutations, output aliases/projections and signed/unsigned integer widths.
Both joins must have no additional condition. Each leaf must be a pure
immutable memory scan with no predicate or residual. Floating/noninteger
keys, source transformations, files, one-shot streams, explicit spill and other
join topologies keep the ordinary native strategy. This conservative admission
prevents newly evaluating an unmatched S.c floating error or replaying effects.
There is no data-dependent cardinality shortcut, fixture-specific branch or
cache of answers.

The public binder validates the original complete tree before strategy
selection. Reuse its field mapping and dtype contracts; no new public plan IR
or frontend syntax is introduced. A test-only selector compares strategies
inside one optimized binary. Production selection requires a separately
accepted integration after this finite gate.

Native key/table/index/pair containers retain the existing shared memory owner.
The output holds at most the caller's batch-size triples. Capacity growth must
be reserved before allocation, including replacement overlap. Propagate
cancellation and consumer failures and leave zero credits after the whole
prepared generation is dropped. There is no new spill permission. Source
payload stays in native arrays; the compatibility input and small JSON output
boundaries remain explicit.

The candidate constructs its access paths on every execution. The two lifecycle
columns are one call and four calls against the same immutable prepared
generation. Reuse means existing source/binding reuse, with all per-call index
construction included in engine time. No cross-generation index cache is
claimed.

## Correctness and resource preflight

Before scored timing, require all eight cells, both strategies and both call
counts to pass complete independent source/value/schema checks. Require actual
candidate activation for all six cyclic cells and no activation for star and
two-table controls. Separate observed executions must confirm zero delivered
R/S intermediate payload in the candidate, complete final rows, truthful
candidate-assignment/comparison counts and index capacities. Observations are
count evidence only and stay disabled during scored timing.

Focused regressions also cover empty/all-null sources, duplicates crossing
batch boundaries, source aliases, mixed signed/unsigned limits, key-order and
output-alias permutations, and native payload retention. Floating and other
ineligible controls must keep ordinary results/errors and remain unselected.
Test pre-cancellation, cancellation during duplicate output, consumer failure,
and memory denial. Require cleanup and fresh-call recovery after failure.

## Frozen quantitative gates

Use one CPU and a 64-MiB native grant for every timed cell. The same optimized
test binary contains both strategies, the byte-identical fixture generator and
the ordinary public prepare/collect path. After full preflight, run exactly two
scored cohorts, A and B, with three samples per `(cell, calls, strategy)` in
each. Calls are 1 and 4. Cohort A visits intake table order; B reverses it. Calls
visit 1 then 4 in A and 4 then 1 in B. Within each cell/call count, role order
alternates by sample and flips between cohorts. Before each cohort, run one
unscored four-call workflow per cell/role in that cohort's order. Retain every
warmup and failure. Do not rerun an unchanged failed candidate to obtain a pass.

For each cell/call count/cohort, compare medians of the three complete workflow
samples. Engine cost is the sum of the engine-owned execution duration for its
one or four calls. Complete cost is preparation plus all public collect calls,
result release and prepared-generation teardown. Keep result serialization
separate. Reference generation, source/result hashing, correctness comparison,
record persistence and supervision remain outside these component sums.
Component sums are not elapsed supervised workflow time.

Retention requires all of the following in **both** cohorts:

- Each sparse, empty and skewed target, at both call counts, reduces engine
  cost and complete cost by at least 10% (candidate/ordinary median <= 0.90).
- Duplicate/null, dense, small, star and binary controls, at both call counts,
  increase neither engine nor complete cost by more than 5% (ratio <= 1.05).
- Every individual successful workflow stays inside the native grant, has no
  denied reservation and returns zero native credits after teardown. Each
  candidate median native peak is <= 1.10 times the matched ordinary median
  plus 256 KiB. This bounds native credited state; it is not a process-RSS cap.
- Every complete value/schema, activation, no-fallback and cleanup gate passes.

Failure of any correctness/resource gate rejects admission. A repeated timing
or memory-control failure rejects this finite candidate. A missing/incomplete
or disturbed scored cohort is inconclusive and cannot establish retention.
Preserve the first cohort even when it already fails. No favorable subset,
individual engine improvement or small final output overrides the controls.

## Acceptance and disposition

Run the existing local storage/concurrency/process-drain guards, pin complete
source/delta/protocol/oracle/binary identities, and retain host/owned-process
observations. Do not pace, change thermal/security settings or raise ceilings.
Ordinary desktop activity remains an observation limitation. Routine timing
communication uses engine time only.

If retained, replace the test selector with explicit shared native admission
and execution evidence, then repeat the frozen gate on integrated behavior and
run focused and required workspace/native/public regression gates. Retained
code does not ship from a prototype result alone. If rejected, remove the
candidate and temporary observation hooks, retain useful public-path regression
tests, verify restored runtime source, run required gates and preserve a
portable result packet before retiring obsolete local artifacts.

No outcome closes broader multiway planning, resource ownership, spill,
SQL/DataFrame breadth, competitive gates, package publication or release hold.
