<!-- SPDX-License-Identifier: Apache-2.0 -->

# Native binary, decimal and temporal payloads

Status: locally accepted under PERF-02/03/07/10/11/12 and CG-3/5/19/20/21;
hosted acceptance remains pending. The [phase plan](phased-execution-plan.md) owns sequencing.
This extends the [universal workflow plan](universal-workflow-completion-2026-10-01.md)
and [nested payload contract](native-nested-composition-2026-10-02.md); it does
not close their wider operator, adapter, resource or spill obligations.

## Contract and provider decision

Carry binary, Decimal128 with precision 1–38 and scale 0–precision, Date32
(signed days since the Unix epoch), and timezone-free microsecond timestamps
through the shared native payload path. Preserve exact values, logical dtype,
precision/scale, temporal units, nullability and row order. Admit those same
leaves inside the existing bounded static lists and structs. Unknown extensions,
other temporal units/timezones, larger decimals and invalid schemas remain
explicitly unsupported, including on empty input.

Vortex-first decision: `use_vortex_native_provider`. Pinned Vortex 0.85 supplies
native Binary and Decimal DTypes, `VarBinArray`, `DecimalArray`, `ExtensionArray`,
and the typed `Date`/`Timestamp` extension metadata. Decimal is a native DType,
not an extension. Reuse the existing scan, take and columnar providers and the
ShardLoom reserved host allocator; no dependency or execution-engine change is
needed. The [versioned DType reference](https://docs.rs/vortex-array/0.85.0/vortex_array/dtype/enum.DType.html)
and installed provider source distinguish logical type from physical encoding.

The existing owned result builder performs selected, terminal construction into
native buffers. Extend it rather than adding another row representation or
format-specific executor. Retain the existing resource grant, child-coordinate
reservations, cancellation checks, bounded batches and buffer lifetime credits.
Selection precedes ownership: small binary results must not retain unselected
source domains. Vortex's generic allocator-taking builder still discards the
allocator argument in this version; keep the established explicit buffer path.

At the original payload acceptance boundary, these types were payloads only;
that acceptance excluded them as keys and excluded arithmetic, casts, unary
state and decimal/temporal predicates. This describes that payload acceptance,
not the later expression scope. The subsequent [typed key contract](native-typed-keys-2026-10-03.md)
extends flat Binary, exact Decimal128 (matching precision and scale), Date32 and
timezone-free timestamp-microsecond equality, hashing and ordering across
relational joins, sets, groups, windows and subqueries. It also admits COUNT,
COUNT DISTINCT, MIN/MAX, same-type comparisons, IS NULL, CASE, COALESCE and
NULLIF. That key acceptance itself did not admit typed expressions. Current
source builds admit typed literals, explicit CAST/TRY_CAST, exact decimal
arithmetic/rounding and scoped binary/calendar functions through the shared
native expression binder. Decimal arithmetic output metadata binds before
execution; explicit decimal downscaling requires zero discarded digits. Key
compatibility still requires matching decimal precision/scale and preserves
distinct temporal types. Nested key equality, retained unary-state extensions,
richer aggregate/window semantics, broader adapters and state spill remain
separate. See the [typed expression contract](native-typed-expressions-2026-10-03.md).
Filtering/ordering/joining on already admitted scalar keys may carry these fields
as payloads through projection,
limits, UNION ALL, windows, subqueries, outer-join null extension and list
explosion.

## Shared components and delivery

| Existing owner | Extension |
| --- | --- |
| `native_payload_schema` and typed Arrow intake | Recognize the exact four type families, recursively; preserve the existing schema budgets and reject unrelated Arrow extensions. |
| `local_primitive_result_batch`, `native_payload` and relational gathering | Build compact owned native values and typed empty/null outputs without a JSON intermediate. Preserve exact dtype and selected validity. |
| Native JSON traversal, collect and text sink | Reuse the established typed export encoding and make metadata loss explicit. |
| Columnar compatibility schema/expansion policy | Map only representable logical types and count binary/decimal/temporal expansion before conversion. |
| Native writer and existing operation/spill lifetime | Reopen real Vortex output and retain existing cancellation, destination and cleanup contracts. |

JSON/JSONL and bounded collection reuse the existing typed export convention:
binary is lowercase hexadecimal text; decimals are strings of the form
`decimal128(precision,scale):unscaled_integer`; Date32 and microsecond timestamps
are exact signed integer units. CSV uses the existing typed cell convention.
These text formats do not persist native logical types. Native Vortex does;
Arrow IPC and Parquet must retain the mapped types. Avro fidelity is established
by actual readback, including its existing integer/list translations. ORC's
current writer admits binary but rejects decimals and temporal types. The
[shared native workflow](native-typed-reductions-2026-10-04.md) extends CSV to
nested JSON text cells, preserving values while reporting logical dtype loss.
Nested ORC remains unsupported. Unsupported output must fail without publishing
a destination, including for empty results. The original acceptance below
predates this CSV extension.

The expert comparator is a columnar-engine maintainer reviewing type identity,
precision, temporal storage, hidden null payloads, compact ownership and
format fidelity. Successful ingestion or file creation alone is insufficient.
All successful paths retain execution/Native I/O certificates and
`fallback_attempted=false` / `external_engine_invoked=false`. Provider scratch
and process RSS exclusions remain explicit; this is not complete allocator
coverage or broader spill admission.

## Acceptance

- Independently specified values cover binary zero/non-UTF8 bytes and empty
  values, decimal signs and precision boundaries, dates before/after epoch,
  timestamp microseconds, nulls, all-null/empty output, nested null parents,
  reordered/repeated selections and null-extended joins.
- Verify cloned/sliced results after producer/source/session drop, last-owner
  credit release, narrow-grant denial, cancellation and selected-only ownership.
- Exercise SQL and DataFrame public source/transform/write/reopen workflows
  through native Vortex and representable local formats, including complete
  multi-batch output above small collection bounds. Keep the collection bounds.
- Deny unimplemented keys/functions and unsupported dtype/format combinations
  before publication; retain inert inspection and unique report-field checks.
- Run focused tests, required workspace/native/Python/feature/doc/site gates,
  then freeze the complete public matrix and Full43 regression under the existing
  serial storage/process guards. Preserve failed observations and source hashes.

Availability requires correctness, ownership and output evidence, not a speedup.
Paused large format/text performance runs, native Python binding experiments,
package publication and hosted runtime acceptance stay separate.

The frozen `8237a900` acceptance passes 4,109 complete public checks, including
804 typed-payload checks, the separate 202-check direct-unary matrix, all 24
selected local gate categories and all 258 paired Full43 comparisons. The
[acceptance report](../benchmarks/native-typed-payloads-full43-2026-10-03.md)
records exact values, schemas, source/binary identities, failed development
observations and check provenance. No predeclared timing or memory investigation
threshold is crossed; no performance improvement is claimed.
