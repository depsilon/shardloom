# Independent Full43 correctness

This correction belongs to the RFC 0044 required comparative guarantees under
`PERF-12.comparisons`. It does not promote optional numeric types or operators.
An independently calculated, frozen DuckDB 2.0.0 reference over the original
Parquet input exposes two differences from the retained native results: Q4's
large-integer average exceeds the runner's existing 1e-12 relative tolerance,
and Q29 groups nonmatching URLs as though its regex were a URL parser. The
first reference was frozen before comparison and remains retained. The first
corrected native pass stops at Q4: source-only adjudication then establishes that
the reference's ordinary floating AVG also accumulates error. An exact total of
the original integers, an exact total after the specified F64 input conversion,
and independent compensated AVG all give the corrected native value. The new
reference therefore uses exact integer accumulation for the pinned integral
SUM/AVG operands, followed by F64 output conversion and count division. It
rejects fractional source types and fails on integer overflow. This changes the
oracle arithmetic, not the 1e-12 tolerance or any native result. Both references
and the stopped pass remain evidence. DuckDB is a separate test oracle, never
an execution dependency.

## Integer-input aggregation

Keep primitive SUM/AVG's F64 output and its original integer-to-F64 conversion.
Compensate additions for signed/unsigned integer inputs in the shared native
scalar and grouped consumers, including typed, materialized, fused and encoded
updates and partial merges. This corrects accumulation drift; it does not claim
exact integer arithmetic or recover bits lost in the initial conversion. Preserve
the original ordered additions for floating input, including its cancellation
behavior. Existing weighted UTF8-length arithmetic, decimal totals, pivot and
rolling policies are separate and unchanged.
The integer accumulation policy is an explicit amendment to the primitive policy
in the October 4 typed-reduction design, justified by this selected task's gap.

The accumulator is constant-sized, with no row buffer or allocation. Existing
byte reservations use its actual size; the legacy aggregate item estimate is
unchanged and remains an estimate, not a whole-process memory guarantee. Keep count overflow, nonfinite
failure, NULL omission, empty results and complete group keys. Validate large
and cancelling integers, native widths, nullable and selected rows, encoded
boundaries, fused measures, partial merges and unchanged floating results.

## Regex specialization

Only the complete pattern `^https?://(?:www\.)?([^/]+)/.*$` and replacement `\1`
admit the domain specialization. Preserve default regex semantics: case-sensitive
HTTP(S), a nonempty capture followed by a required slash, no newline in the
trailing dot expression, and unchanged nonmatching input. Ports, user information,
query and fragment bytes within the capture remain part of the key. NULL remains
NULL. The generic `url_domain` function retains its separate parsing contract.

Carry the transform identity through dictionary caches, local reduction,
worker recipes and ordered merge. Reject a changed transform before merging
prepared partials. Existing embedded URL-domain columns are not evidence of the
regex result and cannot satisfy this expression, even in an older Vortex file.

The first complete corrected pass and the maintainer-requested isolated repeat
expose a Q29 planning regression. The existing same-source dictionary preservation
check admits the regex transform, but then requires a corresponding embedded
group column. Regex intentionally has no such column. The check therefore fails,
and independently rewriting the length measure/predicate introduces a second
projected column, disabling the existing dense dictionary reduction. Preserve
the shared source dictionary when a regex group and its length measures satisfy
the existing fusion contract; do not require or consume an unrelated URL-domain
column as proof. Keep the same native dictionary consumers, CPU/memory owner,
ordered partial merge, cancellation and pressure behavior. This is a correction
to the rewrite admission rule, not a new execution provider or metadata format.

## Provider decision and acceptance

Use the existing Vortex-native numeric owners, validity, Constant/RunEnd and
UTF8 dictionary providers. ShardLoom owns aggregate evaluation policy and the
bounded borrowed specialization of its already-admitted regex expression. The
existing general native regex provider supplies an independent pattern check in
tests; no Vortex query-engine integration, Arrow execution substrate, new input
format, dependency, worker pool or side effect is introduced.

Acceptance requires focused regressions, the required workspace checks, public
SQL results, and one isolated standard Full43 pass after this coherent correction.
Run each query exactly once with complete frozen-reference comparison, the
unchanged storage/process guards and no overlapping task work. Full input hashes
and recursive inventories remain outside query execution. Preserve failed
observations. Required work and gated investigations continue afterward; the
maintainer's optional backlog remains deferred.

The [local acceptance and retained evidence](../benchmarks/native-independent-full43-2026-10-22.md)
now establish complete independent parity for this workload. The final isolated
43-query pass takes **49.953164785 engine seconds** and validates all 381 rows.
Q29 takes **3.079574458 engine seconds**, with the restored dictionary strategy
recorded in its counters. Both preceding slow passes and the initial stopped
attempt remain evidence. The same corrected executable now passes the refreshed
22-task matrix's 704 comparisons, 368 original collected-schema checks and 48
terminal-order checks. Its post-cohort log-retention interruption and five-cohort
continuation remain explicit; no completed cohort was repeated. Current PR
integration remains a separate acceptance step; no entire required owner or
competitive gate closes.
