# Adaptive exact decimal accumulation

Status: **dropped after its first frozen screen** under PERF-04/10/12 and
CG-5/19/20/21. The [complete-operation report](../benchmarks/native-adaptive-decimal-2026-10-07.md)
records a 1.09% primary-score improvement, below the 3% requirement, with no
primary cell reaching 3%. The candidate has been removed; all 941 accepted
runtime assets match their control identities. The design below describes the
archived experiment, not active behavior. The
[conditional work campaign](native-conditional-work-campaign-2026-10-07.md)
continues with conservative membership filtering.
Builder PR #1529 is merged. Its accepted runtime is `53cd1582`, with 941 source
assets identified by
`a7308b726410569306bae14bf60ddd07f57bead73d44d423f5274b5b17c10c5f`.
The initial arithmetic control was preserved before candidate edits. No retained
performance claim, new output type or additional operator breadth follows.

## Contract

Change only the internal sum representation in
`shardloom-vortex/src/local_primitives/native_decimal_reduce.rs`. Preserve
`Total { sum: DecimalValue, count: u64 }`, its size, caller reservations and the
existing relational, rolling, pivot and analytic-frame owners. No memory saving
is claimed for an enum that still reserves its widest representation.

Start at I128 zero. Add/subtract and narrow/narrow merge use checked I128
arithmetic. On overflow, widen the unchanged operands and repeat in the existing
I256 domain. A wide operand keeps the result wide. Do not demote after
cancellation or emptying. The output dtype is determined by schema, never by the
observed internal width.

Preserve coefficient validation, count checks, arithmetic and assignment order.
Invalid metadata, count overflow, invalid removal and wide overflow leave both
fields unchanged. NULL handling stays with the existing callers. For AVG, widen
before scaling the numerator: a narrow accumulated sum can need wide arithmetic
even when its final quotient fits Decimal128. Preserve exact division and final
precision errors.

## Vortex-first decision

Classification: `implement_shardloom_kernel` inside the existing native reduction.
Retain pinned Vortex 0.85.0 `DecimalValue` and its I256 operations for the wide
path. No dependency, copied implementation, query-engine integration, decoder or
alternate executor is added.

The pinned `scalar/typed_view/decimal/dvalue.rs` widens checked operations to the
larger operand type and returns `None` on overflow; it does not automatically
retry with I256. The upstream decimal SUM accumulator selects storage from output
dtype and checks precision while accumulating. Its aggregate uses precision
`min(76, input + 10)` and saturated partials become NULL. Those contracts do not
replace ShardLoom's reversible count/state, exact cancellation, fixed Decimal128
output and explicit final errors.

Native input, output construction, certificates, source validation and
materialization boundaries stay on their existing paths. No external execution
fallback is introduced.

## Correctness obligations

- Ordinary positive/negative financial values, exact SUM/AVG, NULL and empty
  results with declared metadata.
- Positive/negative promotion, cancellation, removal after promotion, a real
  multiset whose opposite-signed removal itself triggers promotion, and final
  overflow versus representable oversized-intermediate means.
- Every narrow/wide merge combination, weighted count, and lifetime-wide state
  after cancellation/emptying.
- Two precision-32 scale-zero values `10^32 - 1`: their accumulated total fits
  I128 but the scaled AVG numerator does not; the exact final coefficient still
  fits precision 38 at scale 6.
- Inexact division, invalid scale/precision, count overflow, invalid emptying and
  fabricated I256-overflow states with unchanged diagnostics and atomic failure.
- Deterministic add/remove/merge sequences whose live multiset is recomputed
  independently in I256 after every operation; check state, count and each final
  result/error at scales zero, six and 38.

## Frozen performance screen

Compare identical native file-backed prepared relational operations in matched
old/new release test executables. The production difference is this one module;
the operation harness and test overlay are identical in both builds. Keep the
original executable, full source identities and build receipts. No runtime
strategy global or public flag is added.

Five primary cells cover dense grouped SUM/AVG, many groups with SUM/AVG, and
rolling SUM/MEAN. Controls cover sustained wide AVG, repeated independently
promoted/cancelled groups and unchanged Int64 aggregation. Use 262,144 grouped
input rows, 32,768 rolling rows, 8,192-row native chunks, one CPU and a 512-MiB
query grant. Retain exact formulas, prewritten fixture hashes, full reference
values, output dtype and source generations in the protocol packet.

Binding and fixture construction are recorded separately. Each operation starts
fresh operator state and consumes the complete result, including hashing and
destruction inside the clock. Use three unscored warmups per measurement process,
then five complete timed calls, with independent complete validation before and
after. Run 15 paired process rounds per cell with alternating build order.

The screen must improve the geometric mean of the five median paired ratios by
at least 3%, with a fixed-seed 10,000-resample paired bootstrap upper 95% bound
below one and at least two primary cells individually improving by 3%. Investigate
any primary/control regression exceeding both 3% and 100 microseconds per call.
A passing screen needs a fresh 15-pair confirmation with reversed initial order
and the same gates. Do not lower thresholds after observing results.

A separate same-binary component screen initializes the candidate `Total` at
I128 or I256. It records actual width transitions outside timed loops and checks
all result bytes against an independent reference. It supports mechanism
attribution, but cannot satisfy the complete-operation retention gate. Its
initial-I256 route is not claimed to be identical old machine code.

Preserve all failures and raw measurements. Native jobs remain serial under the
existing storage, workload, memory and process supervision. Full43 is broader
regression evidence, not the decimal speedup workload. Any retained change still
needs the required source, public/native, documentation and hosted gates. This
candidate does not trigger a package version bump or complete its wider PERF/CG
owners.
