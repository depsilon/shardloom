<!-- SPDX-License-Identifier: Apache-2.0 -->

# Native typed expressions

Status: implemented and accepted under PERF-02/03/07/10/11/12 and CG-3/5/19/20/21;
merged in [PR #1510](https://github.com/depsilon/shardloom/pull/1510) on October 4
after all 37 hosted checks passed. The [completed ledger](phased-execution-completed-ledger.md)
records exact identities and review limitations. The [phase plan](phased-execution-plan.md) owns sequencing. This continues the
locally accepted [typed keys](native-typed-keys-2026-10-03.md) and
[universal workflow plan](universal-workflow-completion-2026-10-01.md). The
[acceptance report](../benchmarks/native-typed-expressions-full43-2026-10-03.md)
records this finite scope, not broader SQL/function parity.

## Decision and shared ownership

Extend the existing expression IR, native relational binder, native key-column
views and reserved result builder. SQL and Python/DataFrame declarations lower
to those same nodes, including ordinary source queries and composed results.
Do not add a frontend executor, decoded row-map interpreter, Arrow execution
boundary or fallback. Promote pure checked helpers from the existing core
decimal/calendar/binary semantics where both reference and native callers need
them; keep reference evaluation outside the native execution loop.

Vortex-first decision: `implement_shardloom_kernel` within the existing scalar
column kernels. Pinned Vortex 0.85 supplies native constants, decimal and binary
arrays, extension metadata, selections and canonical views. Its decimal cast
kernel supports exact rescaling and some buffer reuse, but materializing kernels
allocate through default buffers rather than this operation's fallible grant.
Its decimal numeric provider truncates division, whereas ShardLoom's existing
decimal contract requires an exact quotient. Extension casts operate on storage;
they do not implement the admitted date/timestamp calendar and unit conversions.
Use the existing native views and result allocator with shared checked semantics.
No dependency, query-engine integration or generic provider permission is added.

Native temporal literals wrap checked storage constants in the existing extension
array. Vortex 0.85's timestamp scalar validation passes extreme storage values
through a narrower `jiff::Span` constructor and can panic; constructing the native
extension array preserves the full admitted i64 microsecond domain without that
scalar conversion. Regression tests include both timestamp extremes.

| Existing owner | Extension |
| --- | --- |
| Core scalar helpers and native expression binder | Exact typed literals, explicit conversion matrix and output precision/scale fixed before execution, including empty input. |
| Native scalar/expression kernels | Checked decimal, binary and calendar operations over existing column owners; lazy CASE/COALESCE selection remains intact. |
| SQL expression lowering and Python declaration builders | Carry typed literal/function/cast declarations into the existing IR; encode Python Decimal values without external execution. |
| Native result builder and shared writers | Preserve computed logical types, validity and complete values under existing format admission and bounded delivery. |

## Admitted semantics

- Literals: Binary bytes, valid Decimal128 with precision 1–38 and scale
  0–precision, Date32 epoch days and timezone-free timestamp microseconds.
  Variable literal metadata is admitted before copying. Invalid decimal metadata
  or values fail before a provider constructor can panic.
- CAST/TRY_CAST: preserve admitted identity casts; extend explicit UTF8/Binary,
  numeric/decimal and date/timestamp conversions. UTF8-to-Binary uses UTF8 bytes;
  Binary-to-UTF8 requires valid UTF8. Numeric/calendar values cast to Binary use the same
  textual representation as their explicit UTF8 casts, not native machine bytes.
  Hexadecimal/base64 decoding remains an explicitly named function. Temporal
  types do not implicitly coerce to integer storage or to decimal.
- Decimal casts: integer and decimal conversions check precision and exact
  rescaling. Downscaling requires zero discarded digits. UTF8 and explicitly
  cast finite floats retain the existing decimal text/exponent policy and reject
  fractional digits beyond the target scale. Decimal-to-integer requires an
  exact integral value; explicit Decimal-to-F64 permits finite rounding.
- Decimal arithmetic: preserve exact add/subtract/multiply/divide and unary
  negation. Decimal operands require matching scales; integer operands use a
  precision derived from their declared signedness/width, never the current row.
  Addition/subtraction aligns integer scale and uses `max(p1,p2)+1`; multiplication
  uses `p1+p2` and `s1+s2`; division uses precision 38 and scale `max(s1,s2,6)`.
  Reject unrepresentable metadata at binding and overflow, zero divisors or
  inexact quotients during selected evaluation. No implicit float/decimal mix.
  Reference Int64/UInt64 operands likewise use declared precision 19/20 so result
  metadata does not vary with row values.
- Decimal ABS preserves precision/scale. FLOOR, CEIL and one-argument ROUND
  return scale zero, precision `p` when `s=0`, otherwise `p-s+1`; ROUND resolves
  half ties away from zero. All operate on exact unscaled integers.
- Int64/UInt64 casts check signedness and range. Float-to-integer casts require
  integral values and exclude the upper bounds `2^63` and `2^64`, respectively.
  The reference evaluator and native kernel share those admitted results and
  failures. Nonfinite source values fail admission for both CAST and TRY_CAST.
- Decimal CASE/COALESCE branches derive a lossless common type from their declared
  domains: scale is the larger input scale; precision is the larger integer-digit
  capacity plus that scale. Binding rejects precision above 38, including empty
  input and unselected branches. Only selected values are rescaled, using the
  existing checked decimal cast and reserved native result allocator. This
  permits Python Decimal null-fill values with smaller declared precision or a
  different scale without narrowing the source domain. Key compatibility and
  mixed-scale arithmetic keep their separate explicit-cast contracts.
- The decoded reference evaluator resolves branch result types without evaluating
  unused values. COALESCE accepts 1–128 arguments and stops at the first non-null
  result; CASE evaluates only its selected value. Both rescale selected decimals
  to the declared common domain. Nullable reference columns carry their declared
  dtype on the expression, since a null row value cannot supply schema metadata.
  UInt64 numeric functions/arithmetic, signed/unsigned comparisons and integer
  string offsets retain the native checked domains. Reference changes do not add
  a decoded evaluator to native execution.
- Binary functions: byte length on Binary/UTF8 and strict UNHEX/standard padded
  FROM_BASE64, with the existing aliases and invalid-padding/trailing-bit rules.
- Calendar functions: date and timestamp field extraction, checked day/second
  addition/subtraction and differences through the existing named functions.
  Timestamp-to-Date32 uses floor division at midnight, including negative epoch
  values. Date32-to-timestamp checks the microsecond range. Wider intermediates
  prevent overflow before a representable final value is narrowed.
- Calendar formatting must be safe over the admitted storage domains. Existing
  proleptic calendar output may contain years outside four digits; input parsing
  remains the explicit `0001..9999` ISO date and `Z`/fixed-offset timestamp scope.
  Named timezones, other temporal units and general interval arithmetic remain
  unadmitted.
  The named day/second helpers admit integer column operands and full signed or
  unsigned 64-bit literals; existing SQL INTERVAL spellings retain their scoped
  unit and magnitude checks.

Unsupported source/target/function combinations fail during binding, even for
empty input and TRY_CAST. TRY_CAST converts only admitted value-conversion errors
to typed NULL; cancellation, memory admission and invalid source ownership still
fail the operation. Expressions retain existing SQL NULL propagation and selected
branch evaluation. Comparison/key compatibility does not widen implicitly.

## Resources, acceptance and remaining work

Use one operation grant, existing native owners, cancellation and bounded result
delivery. Charge variable scratch before construction and hold it through native
buffer copy. Do not claim complete upstream scratch/RSS accounting or remove
collection, schema, queue or spill limits. No new state spill is implied.

The comparator is a columnar-engine maintainer reviewing precision/scale
derivation, logical identity, empty-plan binding, checked calendar boundaries,
null/error selection and live memory ownership. Independently specify expected
values at numeric/temporal boundaries, invalid bytes, exponents and scale loss.
Cover flat/dictionary/constant inputs, null/all-null/empty arrays, typed constants,
composed expressions, incompatible branches, TRY_CAST value errors, cancellation,
narrow grants and final credit release. Preserve every still-applicable typed-key
and payload test; replace denial tests with explicit positive and incompatible
cases rather than deleting their boundary coverage.

Complete SQL/DataFrame source/transform/consume/write/readback workflows on native
Vortex and admitted local inputs, with output above collection limits and every
representable writer. Freeze independent expected results, required local checks
and same-window paired Full43 under the existing serial storage/process guards.
Retain failed observations and investigate predeclared timing/RSS flags. Availability
requires correct complete workflows and resource proof, not a speedup.

Retained unary-state semantics, nested keys, richer aggregate/window semantics,
broader adapters and state spill remain with their existing phase owners. Hosted
acceptance depends on review and checks for the existing PR stack.
`fallback_attempted=false` and `external_engine_invoked=false` remain explicit.
This contract does not resume paused large format/text performance runs, native
Python binding experiments, package publication or broader competitive claims.

## Local acceptance

Frozen review source `895a45c95308552e5163738edb1942c153bdeebc` passes 6,600 complete
public checks and 14,120,333 row comparisons. The typed subset contains 3,295
checks/6,432,808 rows; 868 expression checks compare 1,837,436 rows against
independently frozen expectations. The separate direct-unary matrix passes 202
checks/131,734 rows. Computed 65,541-row inputs complete all representable writers
and preserve the existing collection and unsupported-format denials.

All 24 local gate categories pass on the unchanged compiled runtime. The
complete paired Full43 cohort passes 258/258 results, with no timing, RSS or
aggregate threshold crossed. The fastest-call sum changes by +0.29% and the
median sum by +1.10%; no repeat is prescribed. No speedup or total-RSS bound is
claimed. The
[report](../benchmarks/native-typed-expressions-full43-2026-10-03.md) and
[review packet](../benchmarks/evidence/native-typed-expressions-review-2026-10-03.json.xz)
retain the refreshed observations, source/binary identities, complete value/schema
evidence and local gate provenance. The original 6,510 cases and 40 complete
expression oracles remain unchanged; 90 additional checks cover Decimal branch
promotion. The original `3e507b97` acceptance and its Q26 investigation remain in
the report and [original packet](../benchmarks/evidence/native-typed-expressions-2026-10-03.json.xz).

The subsequent decoded-reference review is frozen at `7a97a0c5`. Seven core
regressions and four native/reference tables cover lazy branch types and values,
Decimal promotion, UInt64 numeric boundaries, integer string offsets and Boolean
TRY_CAST failures. Default workspace tests pass 3,467 cases, native Vortex tests
pass 2,296 with 23 existing ignored tests, and native CLI tests pass 1,592. These
counts overlap across configurations. The
[reference packet](../benchmarks/evidence/native-typed-expressions-reference-2026-10-03.json.xz)
retains the full gate logs and exact source delta. The explicit row-based
local-source diagnostic route also uses the corrected evaluator; its CLI tests
are included. Native kernels, binding, shared public helpers and public UAT
fixtures are unchanged. The 6,600 public checks and Full43 measurements above
remain observations of `895a45c9`, rather than new measurements of this follow-up.
