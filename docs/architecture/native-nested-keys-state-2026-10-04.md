<!-- SPDX-License-Identifier: Apache-2.0 -->

# Native nested keys and retained state

Status: implemented with local acceptance on frozen source `d65907f6`;
hosted acceptance remains open. See the
[acceptance report](../benchmarks/native-nested-keys-state-full43-2026-10-04.md)
and [immutable evidence packet](../benchmarks/evidence/native-nested-keys-state-2026-10-04.json.xz).
This continues the [universal workflow plan](universal-workflow-completion-2026-10-01.md)
after [typed unary state](native-typed-unary-2026-10-03.md), under
PERF-02/03/07/10/11/12 and CG-3/5/19/20/21. The expert comparator is a columnar
engine maintainer reviewing logical equality, null propagation, selected-buffer
ownership and complete source-to-output workflows.

## Contract

Extend the existing key and retained-state owners to admitted static lists,
fixed-size lists and structs. Reuse the existing payload schema bounds: depth
24, 4,096 recursive nodes, 8 MiB of schema metadata, and 1–1,024 distinct,
nonempty field names per struct. Leaves retain the admitted bool, integer,
finite F32/F64, UTF8, binary, Decimal128, Date32 and timezone-free microsecond
timestamp domains. Variant, Map, Union, unknown extensions and unsupported leaf
types remain deterministic binding errors, including on empty inputs.

Nested relational keys have matching logical structure and leaf metadata,
ignoring nullability at every level. Field names and order, list versus
fixed-size-list identity, fixed widths, primitive widths, decimal precision and
scale, and temporal units are part of that contract. No recursive numeric
widening, decimal rescaling, storage-integer reinterpretation or list-shape
conversion is implicit. Existing flat key compatibility is unchanged.
Output selection has a stricter existing common-type contract: set branches,
CASE/COALESCE branches and melt values require identical declared child types,
including child nullability. Only root nullability may be promoted. Key-pair
compatibility does not authorize recursive output-type coercion.

Lists compare lexicographically and then by length. Structs compare in declared
field order. Child NULLs compare equal and sort before non-NULL children; hidden
values under a NULL parent do not participate. Operators retain their existing
top-level NULL policies: ordinary equality predicates propagate NULL, joins and
membership follow their existing bound policies, and grouping/set identity may
match NULLs. Empty lists differ from NULL lists and lists containing NULL.
Relational finite floating signed zero remains normalized for equality/hash;
retained unary identity preserves its existing exact floating-bit policy,
including inside nested keys. Every hash match requires complete value equality.

## Shared implementation

| Existing owner | Extension and dependent callers |
| --- | --- |
| `native_relational_keys::KeyColumn` | Recursive native key owners, prepared list coordinates, parent validity, logical hash and arbitrary-row comparison for existing joins, sets, grouping, ordering, windows and membership. Preserve flat owners and dictionary-domain access. |
| Relational binder | Separate flat scalar operand admission from nested key/payload admission. Bind exact nested compatibility before reading rows. Keep arithmetic, text, calendar and conversion kernels scoped to their existing operands. |
| Retained source strategy selection | Include referenced nested field types when choosing the shared relational strategy for direct aggregate, order, filter and project declarations. Reuse one source generation and leave unreferenced nested columns out of strategy selection. |
| Public SQL source binding | Admit syntactic zero limits and offsets without source I/O. Select the native relational limit operator for `LIMIT 0`, preserving empty output even for metadata count; retain positive-limit primitive contracts. |
| Compatibility preparation handoff | Rebind declared source leaves to their prepared Vortex paths and authoritative schemas. Reuse the normalized request and matching preparation identities while validating both generations on every call. Attach original source identities to shared native SQL writers through final commit and alias checks; clear failed retained state. |
| Native expressions | Admit nested comparisons, NULL tests and selected CASE/COALESCE/NULLIF results through native arrays. Preserve lazy branches and typed empty output. |
| Native aggregation | COUNT uses parent validity; COUNT DISTINCT reuses the existing compact row set. Nested MIN/MAX retain only a selected native value and use the same key comparator. Additive measures retain numeric admission. |
| Unary retained rows and exact keys | Carry compact selected native nested values through tail, sampling, deduplication/uniqueness, value counts, selected rewrites and melt. Preserve source order, first/last/remove-all and sampling seeds/ties. |
| Native payload gathering and completed output | Reuse reserved recursive copying and bounded native-array emission. Avoid retaining an entire child domain for one selected value; avoid one permanent output chunk per retained row. |

Nested values carried alongside a supported scalar rewrite stay native. Nested
forward fill fills a NULL parent from the most recent valid complete value;
child NULLs do not trigger filling. Melt requires the same nested logical
shape and declared child types. Structured scalar literals and arbitrary
nested arithmetic/string operations are not implied by payload transport.
Dynamic pivot domain labels and its scalar aggregation state keep their
explicit existing type contract until a concrete native pivot-state extension
is verified under the broader aggregate owner. This finite unit does not
silently serialize nested pivot values into scalar text or JSON.

## Vortex-first provider check

The pinned provider is Vortex 0.85.0, isolated in `shardloom-vortex` under the
existing native feature gates. The provider inventory, alignment/evidence
contracts, RFCs 0031–0036 and RFC 0044 were checked before implementation.

| Subject | Classification | Source-grounded decision |
| --- | --- | --- |
| Logical nested row hash/comparison | `implement_shardloom_kernel` | Vortex's `scalar_fn/fns/binary/compare/nested.rs` supplies elementwise comparison, recursively canonicalizes both operands, and keeps its arbitrary-row comparator private. `ArrayHash`/`ArrayEq` describe physical array structure for caches, not logical row identity across encodings. Extend the existing ShardLoom key owner using Vortex native arrays; do not copy provider implementation or build a second operator engine. |
| Native structure and selected ownership | `use_vortex_native_provider` | Use DType, Struct/ListView/FixedSizeList arrays, validity, native leaf owners and native constructors through the existing compact payload boundary. ListView `take` retains the original element domain, so selection alone cannot prove compact retention. |

Native dictionaries and encoded leaf owners remain available; nested structure
and coordinate execution are explicit native partial materialization. Do not
build decoded `ScalarValue::List`/`Struct` trees or introduce an Arrow execution
middle. Generic provider builders do not establish reservation ownership; use
the existing reservation-owning constructors. Existing execution and Native I/O
certificates retain provider version, source generation, materialization and
`fallback_attempted=false`/`external_engine_invoked=false` evidence.

## Resource and failure obligations

Use one operation admission and memory pool. Reserve recursive owner metadata,
coordinate/state capacity, replacement overlap and output before allocation.
Native retained buffers keep their credits through source/producer/session
drop, clones and slices. Replacing a retained value releases the old owner only
after the new one is admitted. Cancellation must be checked within long child
traversals as well as between outer rows.

Preserve bounded result batches and collection limits. Native output retains
logical schema and validity. The six existing nested destinations retain their
exact support and fidelity contracts; CSV and the pinned ORC nested writer
remain explicit denials before publication. Existing native ordering spill may
carry nested keys after round-trip and merge proof. Other state families retain
their explicit grant-denial behavior until their own spill implementation is
verified. Reservation counters do not claim coverage of all provider scratch,
allocator metadata or process RSS.

Compatibility preparation rebinds declared source paths to the authoritative
Vortex schema. Retained relational reuse also compares original preparation
identities. Shared SQL writers retain those identities on the native source or
relational plan through final commit and output-alias validation; schema routing
preserves the existing optimized flat writer. Preparation metadata uses the same
memory grant and stays credited until its last owner drops. Opening a persisted
Vortex file directly does not imply a dependency on its historical raw input.

## Acceptance

- [x] Verify recursive equality/hash/order across different dictionaries,
  chunks and native layouts, NULL parents/children, empty lists, repeated and
  overlapping coordinates, fixed widths, field order and exact typed leaves.
- [x] Verify all affected relational/unary families, lazy/empty expressions,
  empty-plan operand denials and unchanged flat floating/seed/tie semantics.
- [x] Verify compact selected state against large unselected/hidden child
  domains, buffer lifetime, replacement overlap, narrow grants, cancellation,
  native sort spill/merge and failure cleanup.
- [x] Freeze independent complete public SQL/DataFrame results and every
  representable writer/readback path; retain all prior accepted cases.
- [x] Run required workspace/native/Python, lean/MSRV and affected documentation
  checks; freeze source/binary/oracle identity and portable evidence.
- [x] Run paired Full43 under the existing serial storage/process guards,
  using the existing symmetric timing/RSS/aggregate screens and prescribed
  reversed-order repeats. Preserve every observation and any inconclusive flags.
- [ ] Complete hosted review/gates after the inherited website advisory decision.

Availability requires complete correctness, resource and failure evidence; a
speedup is not required and must not be inferred. Broader aggregate/window,
adapter and resource work remains in the universal queue. CG-1 through CG-23
remain visible and unchanged; this unit alone closes none of those broad gates.
Real Vortex output proof remains distinct from placeholder artifacts. Paused
large format/text campaigns, native Python binding experiments and publication
remain outside this continuation.
