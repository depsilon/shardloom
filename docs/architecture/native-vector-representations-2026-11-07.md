<!-- SPDX-License-Identifier: Apache-2.0 -->

# Sparse and quantized vectors through native data workflows

Status: scoped integration complete, November 8, 2026. PR [#1669](https://github.com/depsilon/shardloom/pull/1669)
merged as `a157a5ff86c50ff253300848950fed207ce3d72d`; all 58 PR checks and all
55 exact-main checks passed with review clear. Verified retirement removed
10,226 regular files and three symlinks among 11,596 entries; current inputs and
the vector runtime-r2/wheel remained unchanged, and portable replay passed after
retirement. The preceding Float16 runtime/wheel were superseded and retired.

This is item 19 step 4's remaining representation work, after Float16 and vector
collections. It does not close item 19, item 20, RFC 0044 or CG-1 through CG-23.
Exact vector expressions, model execution, ANN indexes, additional logical
types and traversal beyond existing nesting limits retain their own contracts.

## Decision

Carry sparse coordinates and affine quantization codes as explicit logical
extensions over the existing native struct/list owners. Preserve supplied
primitive widths, visible values, parent/component nullability and interpretation
metadata. Ordinary-key filtering, reordering, joining, streaming, spill and
persistence carry these payloads through all five interfaces. No operation in
this unit sorts or merges sparse entries, densifies vectors, reconstructs floats
from codes or quantizes caller-supplied floating data.

The expert acceptance standard is complete value and metadata preservation at
real source/result/sink boundaries, including failure and retained-owner cases.
A schema label or same-process row count is insufficient. No dependency change,
fallback engine, benchmark campaign or package publication belongs to this unit.

ShardLoom technique review: metadata-first admission rejects invalid schemas and
unsupported destinations before payload work. Ordinary-column pruning and late
materialization remain with the shared source/operator owners. Existing capillary
work units, PulseWeave paths and dynamic admission/work shaping carry the newly
admitted payloads without a separate scheduler or representation-specific query
route. Explicit selected-validation grants extend the same resource contract.
Evidence tiers remain scoped: this is correctness/lifecycle acceptance, with
`fallback_attempted=false` and `external_engine_invoked=false`, not a performance
claim. Routine timing remains engine time only; no new timing campaign starts.

| Existing owner and callers | Remaining gap | Shared extension and proof |
| --- | --- | --- |
| native_extensions, native_payload_schema and native_spill_session | Two new logical identities and their metadata credits | Register/admit the two types with the same session and spill ownership; prove typed empty/null and separate-process native reopen. |
| native_payload and native_list, used by relational selection/join/stream/spill | Cross-field sparse/parameter relations | Validate observed row/range domains before the existing recursive copy; exercise all existing operator families without separate vector carriers. |
| NativeNumericOwner | Exact coordinate/scale inspection | Reuse original-width access over selected ranges; test wide integers, Float16 and cancellation/denial without an expanded numeric buffer. |
| Complete Arrow intake tensor traversal | New cross-field extension rules | Extend its compiled visitor and parent visibility, retaining old tensor controls. |
| native_input_schema, native_input_values and native_typed_input | Explicit typed declarations and row relations | Add the two versioned schemas and policy checks before the existing recursive construction. |
| Native Arrow conversion and ordinary Vortex/IPC/Parquet sinks | Extension metadata transfer and refusal | Reuse registered import/export plus preflight; independently read complete values and test refused fanout before publication. |
| Python types/input/result schema and Rust schema description | Inert construction and returned type interpretation | Add metadata-only constructors and exact schema parsing; validate ordinary SQL/DataFrame/typed-plan/CLI/embedded workflows. |

## Provider grounding

The pinned graph remains Vortex 0.90.0, half 2.7.1, Arrow array/IPC/Parquet 59.3.1
and the existing coherent Arrow schema/data patch versions. Vortex SparseArray
is a physical patch/fill encoding; it is not the logical sparse-vector type
described here. The inspected provider sources offer no existing sparse-vector
or quantized-vector logical extension to admit for this contract.

| Subject | Classification | Boundary |
| --- | --- | --- |
| Struct/list/primitive storage, selection, native files and spill | `use_vortex_native_provider` | Existing feature-gated Vortex providers in shardloom-vortex; unchanged source/result/sink certificates. |
| Versioned sparse and affine metadata and cross-field validation | `implement_shardloom_kernel` | Checked ShardLoom admission around the provider's extension-DType facility. |
| Arrow IPC and Parquet exchange | `use_vortex_native_provider` | Existing Arrow import/export registration and schema-bearing compatibility sinks. |
| Vector arithmetic and unsupported quantization schemes | `blocked_until_vortex_or_shardloom_evidence` | Explicit refusal; no residual delegation. |

Arrow's canonical tensor column types and its separate SparseTensor IPC message
are different surfaces. These two column extensions use ShardLoom names in both
Vortex and Arrow; they do not claim a canonical Arrow sparse/quantized standard.
The affine interpretation is informed by the ONNX DequantizeLinear specification,
without using ONNX execution or adding an ONNX dependency.

Sources already inspected: the pinned provider and native extension/Arrow/list/
numeric owners; [Arrow canonical extensions](https://arrow.apache.org/docs/format/CanonicalExtensions.html)
and [other IPC messages](https://arrow.apache.org/docs/format/Other.html), observed
as Arrow 26.0.1 documentation; [ONNX DequantizeLinear](https://onnx.ai/onnx/operators/onnx__DequantizeLinear.html),
observed as ONNX 1.24.0/operator 28. Current specification pages are design
references, not proof of the pinned PyArrow 25.0.1 implementation.

## Sparse wire contract

The versioned ID is `shardloom.sparse_vector.v1`. Storage is a struct with exactly
these fields, in this order:

| Field | Native storage | Meaning |
| --- | --- | --- |
| `size` | UInt64 | Logical dimension of this row. |
| `indices` | List of one signed/unsigned 8/16/32/64-bit integer type | Original coordinates, in supplied order. |
| `values` | List of Float16, Float32 or Float64 | Original component values in corresponding order. |

Every present row has a present size and both lists. Their list lengths match.
Indices are nonnull and satisfy `0 <= index < size`; validation uses exact
integer registers, including UInt64 extremes. A zero dimension requires empty
lists. Huge logical dimensions with tiny stored populations remain valid and
must never cause dimension-sized allocation.

Metadata is a canonical, duplicate-free JSON object requiring `version: 1` and
`index_policy: "preserve" | "strictly_increasing"`. `preserve` retains arbitrary
order and duplicates exactly. It defines no duplicate addition/overwrite rule.
`strictly_increasing` requires ascending unique coordinates; one previous index
is enough to validate it. Neither policy sorts, deduplicates, merges, drops
explicit zeros or normalizes signed zero.

Optional `uniform_size` is a UInt64 constraint on every present row. Omission
allows varying dimensions. Values may contain null components if the element
dtype is nullable; coordinate nulls remain invalid. Nonfinite value carriage
obeys the existing observed-value float policy and does not change that policy.

## Quantized wire contract

The versioned ID is `shardloom.quantized_vector.v1`. Storage is a struct with
exactly `codes`, `scale` and `zero_point`, in that order. `codes` is a list of any
one signed/unsigned 8/16/32/64-bit integer dtype. Its length is the dimension.
The scale primitive is Float16, Float32 or Float64. The zero-point primitive
matches the code primitive width and signedness.

Metadata requires `version: 1` and `parameter_mode: "per_vector" | "blocked"`.
The extension ID specifies affine interpretation `(code - zero_point) * scale`.
This is interpretation metadata, not a floating reconstruction precision promise.
No arithmetic is performed during carriage. Packed sub-byte codes, nonlinear or
codebook schemes and quantized tensors are not silently treated as this type.

For `per_vector`, scale and zero point are scalar fields; `block_size` must be
absent. For `blocked`, they are lists and metadata requires positive UInt64
`block_size`. Parameter count is `ceil(len(codes) / block_size)`, calculated
with quotient/remainder and checked conversions. Block size one describes
per-component parameters; a final partial block uses its corresponding entry.

Every present row has present code/parameter containers. Scales are nonnull,
finite and strictly positive, even with the extended float policy; zero points
are nonnull. Codes may contain null components when declared nullable, and their
positions still count toward dimension and block assignment. A per-vector empty
vector retains its scalar parameters. A blocked empty vector has empty parameter
lists. Optional UInt64 `uniform_size` constrains code count for present rows.

## Identity, schema and visibility

Both metadata objects may carry the seven existing optional embedding identity
fields: space_id, model_id, model_version, representation_version, normalization,
preprocessing_id and source_provenance. Reuse the existing field definitions and
nonempty-string-or-null rules. Preserve identity text, explicit nulls and
omissions. Matching dimensions or labels alone do not authorize comparison.

Unknown keys, unsupported versions, duplicate keys, wrong scalar JSON kinds,
negative/out-of-range sizes and inconsistent parameter-mode fields fail before
payload construction or output publication. Arrow declarations can use equivalent
JSON that canonicalizes at intake. Persisted native metadata requires canonical
UTF8 JSON under the existing bounded metadata admission. Metadata retains its
receiving allocator owner, including independent spill sessions.

Nullability of each supplied storage field is preserved. Convenience constructors
declare size, list containers and zero-point storage nonnullable; the item, code,
index and scale type declarations retain their supplied nullability. Explicit
storage declarations can retain other nullable fields. Required-value rules apply to visible present rows,
including otherwise-nullable index/parameter children. A null parent hides its
children and must not turn them into invalid observed values. Sparse and
quantized values can themselves appear within admitted list/struct collections.

Add a first-party vector edition declaring exactly the two new DTypes. Do not
alter the frozen Vortex core edition, other arrays/layouts or dependency pins.
Session registration, spill registration and independent file reopen must agree.

## Execution and validation ownership

Reuse the existing recursive payload copy/gather and original-width numeric
owners. At native observed-value validation, apply arbitrary row order,
duplicates and ancestor masks before resolving numeric children. Validate only
selected coordinate/parameter ranges, with cancellation and explicit admission.
Do not read sparse component values or quantization codes just to validate
structural lengths; ordinary payload validation still validates observed leaves.

Avoid a new retained-probe provider intake here. Existing numeric owners give
exact integer ordinals and per-value floating reads. Resolve selected numeric
ranges once rather than issuing a one-off scalar probe for every coordinate.
The existing provider headroom and selected-expansion contracts still apply:
not all pinned provider decode allocations are HostAllocator-owned, and this
unit must not claim an RSS bound or zero-decode validation. No dimension-sized
scratch, unordered duplicate set or reconstructed floating buffer is permitted.

At complete Arrow intake, extend the existing ancestor-aware tensor validation
traversal with sparse/quantized rules before masks can hide invalid source data.
Keep variable-tensor semantics and tests intact. The traversal checks slices,
nonzero offsets, nested parent validity and complete visible source rows with
bounded schema scratch. Typed JSON input checks cross-field relations before
native storage allocation, sharing representation policy rather than inventing
different boundary semantics.

Use ordinary scalar columns for comparisons, grouping, ordering and join keys.
Add explicit rich-key refusal before generic nested-key admission. Unsupported
vector arithmetic fails with existing no-fallback diagnostics. Extending storage
does not authorize structured rich values as generic comparison keys.

## Public declarations and persistence

Provide inert constructors following the existing types API:

```python
types.sparse_vector(item, *, index="uint32", index_policy="preserve",
                    uniform_size=None, metadata=None, nullable=True)
types.quantized_vector(code, *, scale="float32", block_size=None,
                       uniform_size=None, metadata=None, nullable=True)
```

Item/code declarations control component nullability; nullable controls the
whole vector. Constructors fill mandatory interpretation defaults. Explicit
storage plus metadata remains the exact schema round-trip form. Plain lists or
structs are not automatically inferred to be these logical types.

Vortex remains highest fidelity. Arrow IPC and Parquet preserve admitted logical
identity, primitive widths, nullability and visible data. Generated Arrow list
child names are not part of Vortex List DType identity; independent Parquet
readers may use `element` where IPC uses `item`. Keep all struct field names and
extension metadata exact. Admit Arrow List and LargeList storage through the
existing native List mapping. Offset width is a physical boundary choice, not
the vector's coordinate/code primitive width; native export uses the existing
checked Int32-offset Arrow List form. Existing Arrow list-offset export limits apply.
Text, Avro and ORC paths refuse these extensions before producer/staging work.
Rich dictionary ingestion remains explicitly unadmitted in this unit.

The independent preparation probe uses PyArrow 25.0.1 and checks 192 format
observations across integer/float widths, required-field nullabilities and both
parameter modes. It covers integer extrema, null parents/components, empty rows,
large sparse dimensions, duplicates, top-level slices and signed zero. The first
attempt's literal Arrow-schema comparison failed solely on Parquet's generated
list-element names. Preserve that failure and the corrected logical-schema
comparison. This probe is format grounding; it is not ShardLoom acceptance.

## Acceptance and review

Freeze a mixed scalar/Float16/dense/tensor/sparse/quantized fixture and complete
expected values before implementation verification. Exercise import, retained
owners, reordered/duplicated selections, scalar filters/joins, bounded streams,
forced native spill, native/IPC/Parquet write, source removal, separate-process
reopen and independent export readback through all five public interfaces.

Cover every code/index primitive width, all scale/value float widths, both sparse
policies and both quantization modes. Include explicit zeros, signed zero,
admitted nonfinite sparse values, nullable components, absent parents, variable
dimensions, empty/all-null schemas, zero dimension, huge dimension with tiny nnz,
partial blocks and repeated outer-join matches. Check exact schemas and complete
values, rather than counts or only a representative result prefix.

Negative controls cover malformed/unknown/duplicate metadata; dimension/range/
ordering/length errors; missing/null indices and parameters; nonpositive/nonfinite
scales; primitive-width mismatches; bad hidden versus observed children; source
changes; denied/shrunk budgets; cancellation; early stream close; retained-result
and Arrow-view lifetimes; source-free reopen; preflight/fanout publication failure;
and deterministic refusal of rich keys and limited formats. Preserve prior
variable/fixed tensor and Float16 regression coverage, especially empty intake.

Run focused native and Python tests first, then required default/native fmt,
strict lint and workspace tests; existing seven feature configurations; Python
core checks; generated public docs; a runnable public example; and an isolated
ordinary installed-wheel proof with zero skipped native tests. Bind exact source,
runtime and wheel identities in portable evidence, with positive replay and
targeted corruption controls. Preserve original failed attempts. PR #1669 passed
hosted review, CI and exact-main acceptance, and guarded artifact retirement
completed; no source-only result is described as a published package.

The adversarial review must challenge hidden-child validation, wide integer
rounding, metadata loss across spill/export, eager payload work, uncredited
retention and accidental admission of vector comparisons. Resolve those in the
shared owners before acceptance. Performance and broader type/operation claims
remain unavailable without their separate required evidence.

## November 7 local and portable acceptance

All 13 accepted stages passed against the same 39 source records,
including the one recorded deletion. Default Rust tests passed 3,321 cases;
native Rust tests passed 6,988 with 37 existing ignored tests. Formatting,
strict lint and all seven saved feature configurations passed. The Python core
shard reports 857 runs among 1,247 discovered cases, 56 optional/environment skip
records and no failures or errors. The clean installed wheel passed 386 tests with no failures,
errors or skips; isolated load and uninstall passed. This includes 11 vector,
37 existing tensor/Float16 regression and 19 context tests.

The [frozen evidence packet](../release/evidence/native-vector-representations-2026-11-07/README.md)
contains 532 members and preserves 10 original failed observations, including
two preparation attempts. Original and relocated replay pass; all nine negative
controls reject, alongside the two positive replay controls. The saved
acceptance binds source, runtime and installed-wheel identities. The separate
debug-validation retirement passed while leaving the retained runtimes, wheel,
input generations and release cache unchanged. This section records the local
and portable checkpoint; the subsequent merge and final retirement are recorded
in the status above. No benchmark, publication or arithmetic claim follows.

## Frozen independent mixed reference

Before any new engine implementation, `next_vector_fixture.py` fixes six complete
rows across eleven fields: ordinary identifiers and buckets, Float16 values,
dense embeddings, fixed/variable tensors, both sparse policies, per-vector and
blocked quantization, and a list of structs containing sparse/quantized vectors.
The source has a nonzero top-level slice offset. Cases include preserved duplicate
and unordered coordinates, explicit and signed zeros, nullable components and
parents, empty rows, UInt64 maximum dimensions/parameters, partial blocks and
nested absent parents. Dense/tensor parents in this mixed reference are present,
so the complete fixture can be read independently from Parquet; separate
existing tensor controls retain absent fixed-list parent coverage.

`next-vector-fixture-r2/report.json` records independent PyArrow 25.0.1 IPC and
Parquet readback of every value and the complete logical schema. JSON comparison
preserves signed zero and exact integer text. Built-in Arrow extension type
equality retains their metadata; generated list child names alone normalize.
The first checker attempted a custom-extension serializer unavailable on
PyArrow's built-in FixedShapeTensorType. Preserve its original source, receipt
and failure. No fixture values changed when correcting that checker.

These references are preparation evidence for this unit and must not be added
to Float16's committed failure counts or described as native support. Bind their
source and artifact identities when transferring them into the new task root.
