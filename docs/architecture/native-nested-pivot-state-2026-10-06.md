<!-- SPDX-License-Identifier: Apache-2.0 -->

# Native nested pivot state

Status: complete local acceptance on `750b3783`; hosted integration completed in
[PR #1525](https://github.com/depsilon/shardloom/pull/1525) after all 39 checks passed.
The [acceptance report](../benchmarks/native-nested-pivot-state-full43-2026-10-06.md)
records complete public, resource, schema and Full43 regression evidence. Scalar-subquery hosted
integration completed in [PR #1524](https://github.com/depsilon/shardloom/pull/1524).
This is `NATIVE-NESTED-PIVOT-STATE` in the [phase plan](phased-execution-plan.md),
continuing the [universal workflow queue](universal-workflow-completion-2026-10-01.md) under
PERF-02/03/06/07/10/11/12 and CG-3/5/19/20/21. Broader adapter, accounting, spill
and recovery obligations remain in their existing owners.

## Contract

Extend the existing dynamic pivot owner to admitted static List, FixedSizeList
and Struct index fields, pivot-domain fields and selected cell values. Preserve
the recursive payload schema limits and exact declared child types. Variant,
Map, Union, unknown extensions and unsupported leaves remain explicit binding
errors, including on empty input. This adds no input format or execution mode.

Index and domain identity reuse unary exact-key serialization, including its
finite floating-bit identity. A single nested role uses the existing raw nested
key (`L`, `F`, `S` or the NULL marker), without composite-row length framing.
NULL parents form one identity without exposing
hidden children; empty lists, lists containing NULL and NULL lists differ.
Preserve the current serialized-key ordering of rows and domain columns.
Existing scalar names stay unchanged. Non-NULL nested domain names derive from
the type-tagged exact nested key through the existing field-name sanitizer and
collision suffix owner. NULL domains retain the existing NULL-name policy.
Freeze literal name fixtures before candidate acceptance; do not infer expected
names from candidate output. Keep the 128-column and field-name boundaries.

Examples of frozen domain labels are `pivot_l0` for an empty List, `pivot_l1_i1`
for List `[1]`, `pivot_l1_n` for List `[NULL]`, `pivot_f2_i1_i2` for FixedSizeList
`[1, 2]`, and `pivot_s1_1_ai1` for Struct `{a: 1}`. A NULL domain retains
`pivot_value`. List strings `["a-b"]` and `["a_b"]` share the sanitized base
`pivot_l1_s3_a_b`; the later-discovered distinct domain receives suffix `_2`.
Rows and columns retain serialized-key order rather than introducing logical
value sorting. Callers can request an explicit downstream order.

`first` retains the first selected complete value, including a NULL parent.
`first_unique` permits repeated equal complete values and rejects conflicting
ones; it must compare native values through the existing recursive comparator,
including logical signed-zero equality. Domain/index identity and duplicate-cell
value equality retain their distinct existing roles. Neither operation may
silently choose a later non-NULL value. COUNT continues to count rows in each
observed cell, including NULL value rows, and returns the existing U64 result.

Admit nested MIN/MAX through the existing recursive comparator and retain only
the selected complete native value. Skip NULL parents for those nested extrema;
all-NULL observed cells return typed NULL. Child NULLs follow the shared nested
ordering. Numeric SUM/MEAN and existing primitive/decimal extrema retain their
current contracts. Nested SUM/MEAN remain binding errors; no recursive arithmetic
or implicit list reduction is introduced.

Preserve existing Python aliases: `pivot()` and `pivot_table(aggfunc="first")`
both lower to `first_unique`. The raw SQL/native projection's explicit `first`
option has its separate first-row contract above. Verify parity for declarations
that lower to the same aggregate; do not silently change this established alias.

Nested cells preserve the value's logical dtype and child nullability, widening
only root nullability for missing cells. No fill, or explicit NULL fill, preserves
that dtype. Non-NULL scalar fill for nested cells is an explicit binding error;
structured scalar literal syntax is outside this unit. Both existing `dropna`
settings continue to retain observed domains and explicit NULL cells.

Margins with a nested index are rejected before reading because a string margin
label cannot inhabit the declared nested index type. With a scalar index,
COUNT/numeric margins keep their current behavior and type policy. Nested
MIN/MAX margins require a UTF8 index and select complete native values over the
same selected row scope. `first` and
`first_unique` margins keep their existing rejection. Preserve limit behavior,
domain/index/margin name collision diagnostics, and typed empty output.

Dynamic domain discovery still supplies the authoritative output schema once
per fresh execution. SQL, Python, direct prepared calls and composed relational
calls converge on the same owner. Scalar-subquery schemas cannot depend on
executing dynamic pivot discovery; their existing static-admission denial stays.

## Shared ownership and Vortex-first decision

The pinned provider remains Vortex 0.85.0. The local manifest, lockfile, provider
inventory, nested-key contract and selected-buffer implementation establish the
available boundary. Recheck the targeted RFCs and provider source before coding.

| Existing owner | Shared extension |
| --- | --- |
| `local_primitive_unary_pivot::Plan` | Admit nested roles and bind aggregate, fill and margins policy from DTypes before source execution. |
| `PivotRowExportState` and its domain-name helper | Keep sparse identity, name collisions and duplicate-cell decisions shared; make comparison fallible where native traversal requires it. |
| `NativeBatch`, `OwnedRow` and `native_payload` | Retain compact selected native values with their existing credits; do not build nested `ScalarValue` trees or retain unrelated child domains. |
| `native_relational_keys::KeyColumn` | Reuse exact nested keys, recursive comparison, parent validity and cancellation checks. |
| Pivot cell/margin owners | Retain selected nested extrema alongside existing primitive and exact decimal strategies. |
| `CompletedPivot` and `CompletedRows` | Coalesce retained values into bounded native output batches and deliver the actual schema through all existing consumers. |
| Dynamic relational binder and local writer staging | Preserve one discovery, fresh execution state, source-generation validation, provisional-output failure and commit cleanup. |

Classification: `implement_shardloom_kernel` in the existing sparse pivot owner,
using the admitted native Vortex array/validity and selected-output providers.
Upstream array operations do not own dynamic column naming, duplicate-cell
policy, selected-scope margins or ShardLoom grants. The existing shared native
key/payload owners already wrap the relevant provider APIs. No second pivot
executor, source-specific query route, Arrow execution middle or new dependency
is justified.

## Resources and failures

Reserve key bytes, sparse-map capacity, retained native payloads, recursive
metadata, replacement overlap and output batches before allocation. Avoid
copying a nested payload that cannot change the retained cell. Native output
credits must survive producer/session drop, clones and slices. Cancellation
checks apply inside child traversal and during discovery, completion and emission.

At this unit's original acceptance, sparse-state pressure meant deterministic
grant denial; no pivot spill adapter was added. The later
[October 8 pivot pressure contract](native-pivot-pressure-2026-10-08.md) admits
explicit relational spill for file/resident-memory sources. Direct prepared
unary pivots remain resident. Existing sort spill alone grants no pivot spill
permission.
Failure must release state and staged output without publishing a partial result.
Successful counters establish the named reservations, not complete reader/codec
accounting or a process-RSS ceiling. Preserve source-generation and repeated-call
checks, including changes between discovery and final writer publication.

Native Vortex output preserves complete logical dtype and validity. Existing
representable nested writers keep their fidelity boundaries; CSV is explicit
JSON-text translation and does not persist nested logical dtypes. The pinned ORC
nested writer remains an explicit denial before publication. No fallback engine
is permitted, and native certificates retain both false fallback/external-engine
fields.

## Acceptance

The expert comparator is a columnar-engine maintainer checking dynamic schema,
logical equality, selected native ownership and failure-safe publication.

The frozen release build passes all 27,373 public checks and 15,820,181 complete
row comparisons, including 3,587 nested-pivot checks and 468,657 rows. A separate
202-check direct matrix and all 129 Full43 runs pass. The new coverage verifies
complete SQL/DataFrame results, all five materializations, exact Vortex
and Arrow IPC schemas, all representable writers, correlated discovery, typed
empty output, deterministic denials and 65,537-row nested writer readback above
the small collection bound. Native tests additionally cover dictionary/chunked
inputs, signed zero, hidden NULL children, exact typed leaves, compact retention,
replacement overlap, constrained grants, cancellation, source changes and failed
publication. The [immutable acceptance packet](../benchmarks/evidence/native-nested-pivot-state-2026-10-06.json.xz)
retains the independent literal oracles, 910 frozen source assets, 27 source gates,
1,892 schema proofs, 2,871 complete value/resource proofs and all failed observations.
The accepted head `58c09499` merged at `4ba90532` with the same tree and all 910
runtime source hashes preserved. The [hosted receipt](../benchmarks/evidence/native-nested-pivot-state-hosted-2026-10-06.json)
records the exact checks, primary review and live website verification. Automated
hosted review was account-limited and supplied no approval. Published v0.4.0
predates this unit.

- Freeze independent complete SQL/Python result fixtures for each nested role,
  combinations of roles, all selected aggregate kinds, empty/null/duplicate
  cases, names/collisions, fill, margins, ordering and limits.
- Test cross-batch and cross-encoding identities; field order and metadata;
  finite signed zero; parent and child validity; hidden fixed children; and
  exact primitive/decimal/temporal leaves.
- Prove compact retention against large unused child domains, equality without
  unnecessary retained copies, narrow-grant denial, replacement overlap,
  cancellation, source changes, failed consumers and released credits.
- Exercise direct prepared and dynamic relational composition, repeated calls,
  downstream projection/aggregation/order, all representable local writers and
  explicit unsupported writer/type/policy shapes. Preserve the current scalar
  pivot regression cases and all existing public families.
- Run focused checks during implementation; then required workspace/native/
  Python/source gates and the complete public/Full43 regression matrices under
  serial storage/process guards. Freeze source, binary, oracle and complete
  evidence identities; preserve every failure. Availability requires correctness
  and resource proof, with no inferred performance claim.
- Update README/reference/support labels/Field Guide from actual admitted
  behavior, complete hosted integration and move the accepted unit to the ledger.

CG-1 through CG-23 retain their independent obligations. Real Vortex output
evidence remains distinct from placeholder artifacts. Paused large format/text
campaigns, native Python binding experiments, hardware work and new publication
remain outside this unit.
