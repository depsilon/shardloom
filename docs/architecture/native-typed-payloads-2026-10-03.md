<!-- SPDX-License-Identifier: Apache-2.0 -->

# Native binary, decimal and temporal payloads

Status: implementation in progress under PERF-02/03/07/10/11/12 and
CG-3/5/19/20/21. The [phase plan](phased-execution-plan.md) owns sequencing.
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

Payload admission does not admit arithmetic, sorting/grouping/join keys, unary
state, casts or decimal/temporal predicates. Existing key/expression admission
must reject those uses before execution until their own semantics are implemented
and tested. Filtering/ordering/joining on already admitted scalar keys may carry
the new fields as payloads through projection, limits, UNION ALL, windows,
subqueries, outer-join null extension and list explosion.

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
current writer admits binary but rejects decimals and temporal types. Nested
CSV/ORC remain unsupported. Unsupported output must fail without publishing a
destination, including for empty results.

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
package publication and the inherited website advisory decision stay separate.
