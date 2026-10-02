<!-- SPDX-License-Identifier: Apache-2.0 -->

# Shared aggregate admission and null ordering

Status: implementation and acceptance in progress. This finite continuation belongs
to PERF-02/10/12 and CG-5/20/21; it closes neither their whole scope nor production
certification. The preceding [resource unit](native-relational-resources-2026-10-02.md)
identified a flat aggregate collection gap while its composed aggregate passed.

## Contract and reuse

A complete flat filter/group/aggregate/HAVING/order/limit chain must reach the same
public native aggregate admission for collection and all local writers. Python
renders the complete expression and carries source declarations and resources;
Rust remains responsible for admission and execution. An input limit or other
earlier transformation must retain its stage position through the existing native
relational renderer. Failed execution is never retried through another provider.

The existing `VortexAggregateOrderExpr` is shared by standalone native sort and
aggregate finalization. Extend it with optional explicit null placement using the
existing `VortexRelationalNullOrder` enum and one common comparison helper.
`NULLS FIRST`/`NULLS LAST` applies independently of ASC/DESC. Omission preserves
the primitive API's existing null-first ASC/null-last DESC behavior; general
relational binding keeps its existing explicit-null requirement. Existing `new`
callers keep their meaning, with a builder for the added option. The Rust struct
gains a field; internal struct-literal callers must supply it, and these internal
crates remain unpublished. No public type or command is renamed.

| Existing owner | Extension and required proof |
| --- | --- |
| Python aggregate SQL renderer and shared public workflow facade | One aggregate statement and source/resource declaration for collect/write/inspection, including declared compatibility schemas. |
| CLI flat aggregate/sort SQL lowering and typed primitive JSON parser | Preserve each explicit null modifier; reject malformed values rather than ignore them. |
| Native aggregate finalization | Reuse existing count, dictionary, numeric, string and general states; change only shared ordered comparison. |
| Native full sort, Top-K and decoded block candidate cutoff | Apply identical null policy before selection, pruning and final delivery; preserve source-row tie policy. |
| Specialized COUNT/DISTINCT/numeric spill and owned output | Verify nonnull order-value admission; keep the existing strategies when null placement cannot change values. |
| Native relational sort | Share the null/direction comparison rule while retaining native keys, stable ties, spill and resource ownership. |
| Native results and all eight writers | Consume the existing complete native result stream; do not reconstruct or rerun an aggregate in Python. |

Flat aggregate collection previously emitted only the native execution summary.
It now consumes the same `PreparedVortexAggregate::for_each_batch` output as the
writers through the existing `JsonRows` sink used by unary and relational
collection. The returned JSONL owns its memory reservation until response
emission and keeps the 65,536-row/8-MiB complete-result boundary. In-memory batch
delivery requires the native primitives feature; file writes and spill still
require their existing write feature. The report-only aggregate API is unchanged.
Public row consumers read `result_jsonl` or Python `report.result_rows`; diagnostic
summaries retain metrics without requiring duplicate row payloads for streaming.
The same JSON sink records its terminal row-materialization boundary and loss of
physical dtype, encoding, statistics and metadata for aggregate, unary and
relational collection, preserving each operation's source and execution proof.
Byte-bound denial must account for JSON escaping, release every failed output
reservation and return no completed result. A successfully returned payload must
remain charged after its preparation and session handles are dropped.

Streaming finalization must reserve the containers its selected strategy actually
retains. Completed partition cardinality is diagnostic evidence and can exceed
the remaining candidate map by many orders of magnitude. Compact numeric and
interned/Arc-based Top-K strategies reserve their retained candidates and sort
scratch; large-window numeric selection still reserves the complete candidate
vector, and string distinct finalization includes its complete numeric count map.
Payload buffers keep their separate leases. Compact minute keys must also preserve
the declared signed or unsigned result dtype at the native output boundary.

The Vortex-first decision is `implement_shardloom_kernel`: this extends existing
ShardLoom finalization/admission over its current Vortex 0.85 arrays, validity,
scan and owned result providers. It adds no competing aggregate, sort, source,
spill or sink implementation. Metadata-first and encoded strategy admission,
PulseWeave grants, capillary candidates, source generations and certificates remain
owned by those existing components. Provider decode/scratch exclusions remain
explicit. No Arrow middle, query-engine integration or new dependency is added.

## Acceptance

Freeze independently specified values before the public matrix: nullable grouping
keys, nullable and all-null measures, signed/unsigned boundaries, UTF8, ties,
ascending/descending keys with FIRST/LAST, multiple keys, offset/limit, empty
results, HAVING and transformed inputs. Include exact flat collection versus
complete write/reopen parity through SQL and DataFrame spellings, renamed schemas
and declared compatibility sources. Keep existing optimized COUNT/COUNT DISTINCT
and numeric spill selection visible in certificate/regression assertions.

Tests must expose candidate-cutoff mistakes, not only sort already retained rows.
Malformed JSON/SQL policies and conflicting or unused source declarations must
fail without fallback, a success certificate or a published output. Route
inspection remains inert. Existing small-result collection limits and explicit
spill-family admission remain in force.

Run focused request/parser/kernel/facade tests, required workspace formatting,
strict Clippy and tests, native provider/CLI suites, Python, lean/MSRV and affected
documentation gates. Freeze the final build for the complete public acceptance
matrix and Full43 regression under the existing sequential storage/process guards.
Record complete outputs and exact identities. A speedup is not an acceptance
requirement for this availability correction and no performance claim is made.

Broader unary composition, dynamic pivot schema, nested/extension types, other
operator-state spill, native Python binding, paused large text/format performance
runs and package publication remain outside this finite unit and with their
existing owners. All execution retains `fallback_attempted=false` and
`external_engine_invoked=false`.
