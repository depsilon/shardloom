# Explicit decimal output — implementation contract

Prepared on October 29, 2026 against the temporal-grid implementation and
integrated on merged base `c1864a28`. The explicit decimal implementation passes
local source and installed-package acceptance; portable replay accompanies the
[acceptance report](../benchmarks/native-decimal-output-2026-10-29.md).
This is one bounded part of item 20, not completion of the numeric package.

## Scope and compatibility

The validated per-operation Decimal128 output declaration uses precision 1..38,
scale 0..precision, and one of `exact`, `half_even`, `half_away`, `toward_zero`,
`floor`, or `ceil`. Precision is total coefficient digits, not significant-digit
rounding. Scale fixes fractional places. `exact` refuses any discarded nonzero
fraction. Half modes use the exact mathematical remainder, with ties going to
an even coefficient or away from zero respectively. Overflow is checked after
the sole rounding step. Decimal128 has no signed-zero representation.

Existing arithmetic, casts, SUM/AVG and quantile declarations retain their
current behavior and result domains. No context/global default changes.
Nonfinite/signed-zero observation policy remains a separate numeric work unit.

Public scalar functions: `DECIMAL_RESCALE(value, precision, scale, rounding)`;
`DECIMAL_ADD`, `DECIMAL_SUBTRACT`, `DECIMAL_MULTIPLY`, and `DECIMAL_DIVIDE` take
two value operands followed by the same three literal options. These operations
admit validated Decimal128 and exact integer inputs; floats/text require an
explicit admitted conversion before the operation. Mixed input scales are
aligned mathematically. NULL propagates, while invalid option declarations fail
at binding even on empty/all-NULL data. Division by a selected nonnull zero fails.

SQL additive aggregates and continuous quantiles accept the same optional
trailing triple, e.g. `AVG(value, 18, 2, 'half_even')` or
`QUANTILE_CONT(value, 0.1, 18, 2, 'half_even')`. MEDIAN uses its existing q=0.5.
Python helpers expose keyword precision/scale/rounding; typed measures carry an
optional `decimal_output`. All three values are required when specifying the
option. The aggregate option is admitted only for Decimal128 SUM, AVG, their
DISTINCT forms, MEDIAN and QUANTILE_CONT. Ordinary integer SUM remains unchanged;
the exact widened-integer contract uses `SUM(CAST(value AS DECIMAL(38,0)))`.
Discrete quantiles and unsupported legacy/primitive routes must reject options,
never silently ignore them. Default pivot/rolling paths stay unchanged.

## State, arithmetic and ownership

Reuse the existing native decimal total (checked I128 -> Vortex I256), count,
distinct membership, rank selection, resident groups, windows and stored runs.
Rounding occurs at finalization, after merge/removal/filter/frame decisions.
Different result options may share exact input/accumulator state but cannot
reuse finalized values. All options participate in descriptor equality,
generated names and expression/aggregate semantic identity. Options are not
source-column dependencies. Stored paths retain the original values and exact
private state; never round spill records.

Use one exact integer-ratio rounding owner, shared by explicit scalar operations,
aggregate finalization and decimal interpolation. Quantile ranks and interpolation
retain the probability's original Float64 ratio, including subnormals. Avoid an
intermediate float or an early rounded endpoint. The native output remains an
ordinary Vortex Decimal128 array with its selected precision/scale and validity.

Implementation direction: a private bounded stack integer helper, with an
explicit capacity proof for the admitted Decimal128 domain and q's <=1074-bit
power-of-two denominator. Prefer checked narrow arithmetic for ordinary values;
no heap arbitrary-precision state, new dependency, upstream engine or row-table
execution. Review the fixed helper's complexity against a more compact dyadic
decomposition before implementation. The existing exact default owner does not
need to pay the new explicit-policy cost.

## Provider check and source evidence

The workspace pins Vortex 0.90.0 and patches `vendor/vortex-array`; an initial
0.85 registry lookup was stale and is not evidence for the decision.
Current provider sources are `vendor/vortex-array/src/scalar/typed_view/decimal/`:
`arithmetic.rs` derives its own result precision/scale and truncates division;
`dvalue.rs` has exact rescaling, with rescale/cast helpers crate-private. Neither
checked surface supplies the six caller-selected rounding/result-domain choices.
Decision: wrap existing Vortex DecimalArray/DecimalValue representation and
native total/rank owners, implement ShardLoom result semantics. Keep Vortex API
usage in shardloom-vortex; the dependency-free core semantic helper only handles
validated integers and declarations. No Arrow/external-engine execution.

The definition of half-even, half-away (Python ROUND_HALF_UP), toward-zero,
floor/ceiling and exact/inexact traps can be independently checked against the
[Python decimal reference](https://docs.python.org/3/library/decimal.html).
Python Decimal/Fraction are test oracles only. Policy propagation uses the
existing aggregate and window declarations, the shared decimal-total finalizer,
and the shared quantile position owner. The scalar binder removes the three
literal controls from input dependencies before creating a native expression.

## Acceptance

Use independent integer/Fraction/Decimal expected values, not the production
rounding implementation. Cover both signs, exact and inexact results, half ties,
near ties, zero/subnormal probabilities, maximum precision/scale, post-rounding
overflow, cancellation after wide totals, empty/all-NULL inputs and invalid
declarations. Include SQL, native typed Rust/Python, Python SQL and DataFrame
routes, DISTINCT/FILTER/HAVING/ORDER BY identity and ROWS/RANGE/calendar windows.

Prove actual stored and resident reductions/quantiles agree, with native spill,
denied growth, quota, cancellation, failed consumer and successful retry. Reopen
Vortex/Parquet/Arrow results after source disposal. Complete full-result
certificates, portable member/source/product validation and negative controls,
then required default/native workspace gates and installed-package acceptance.
No benchmark or performance claim is part of this capability change.

## Implemented acceptance boundary

The core checks 3,462 independently generated Fraction/Decimal cases. Six public
tests pass 250 executions and 2,695 complete rows across DataFrame, typed relation,
native SQL, embedded workflow and CLI entry points. Complete typed comparisons
and all five execution reports are retained. Additional streamed-input and
retained-result checks cover EOF, producer close, empty input, late errors,
zero-limit errors, lazy conditional evaluation and reuse after producer disposal.
Three output formats reopen after source deletion.

The composed conformance query, all three literal documentation examples, 131
source/interface tests and 223 installed-wheel integration tests pass with no
skips. Default workspace testing passes 3,285 tests. The full native attempt
passes 6,460 tests with 35 existing opt-in ignores and one stale document-count
assertion; correcting only its two expected counts then passes all 58 tests in
that native metadata target. Formatting, strict default/native lint and the
minimal native feature check pass. Original failures remain visible in evidence.

This closes explicit decimal precision/scale/rounding and proves the existing
explicit widened-integer SUM route for this unit. Non-finite and signed-zero
policy, broader composition, application batch functions, remaining diagnostics
and combined item-20 acceptance remain open. There is no release or new timing
measurement in this work unit.
