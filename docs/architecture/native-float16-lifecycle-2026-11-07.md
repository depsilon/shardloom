<!-- SPDX-License-Identifier: Apache-2.0 -->

# Float16 and collections of fixed-dimensional vectors

Status: local and portable acceptance passed, November 7. Hosted integration
and subsequent artifact retention remain pending.
This continues step 4 of the
[rich-data lifecycle](native-rich-data-lifecycle-2026-10-24.md) after fixed and
variable tensors. Variable-tensor PR #1662 merged as
`c47eeefa05012cce32b053f53af18583041c9f19` after all 58 PR checks passed; all 55
required checks then passed on that exact merge. Its cleanup removed 9,996
obsolete regular files and three environment links, retained the accepted
runtime, wheel and inputs, and passed evidence replay after removal.

The unit admits the existing binary16 primitive width through native sources,
scalar operators, nested payloads, dense embeddings, both tensor forms and
capable persistence boundaries. It also establishes complete lifecycle evidence
for multiple fixed-dimensional vectors in one row using existing
`List<FixedSizeList>` and `List<Embedding>` storage. Sparse and quantized logical
representations need separate interpretation contracts. Exact vector expressions
remain step 5; this unit does not add vector arithmetic or an index.

The expert acceptance standard is exact selected values and logical metadata
through real user workflows, with unchanged observation policy and bounded
ownership. Type recognition alone is insufficient. Item 19, item 20, RFC 0044
and CG-1 through CG-23 retain their separate completion requirements.

## Representation and scalar semantics

Binary16 has one sign bit, five exponent bits and ten fraction bits. Preserve
every one of its 65,536 representations during opaque carriage, including signed
zeros, subnormals, infinities, NaN signs, signaling state and payloads. Preserve
the original dtype and separate parent/component nullability. A null parent does
not authorize observation of hidden children; discarded rows remain unobserved.

Use the existing [floating policy](native-float-policy-2026-10-29.md). Strict
policy rejects an observed nonfinite scalar, while extended policy admits its
existing semantic domain. Rich payload carriage retains its existing policy.
Semantic keys may canonicalize NaNs and signed zero for identity without
rewriting the selected representative's stored bits. Newly computed arithmetic
results follow the existing result type and canonicalization rules.

Float16 uses the existing Float64 scalar registers and numeric owners. It does
not add a second scalar IR. Arithmetic, aggregation, ordering and mixed numeric
comparison retain their current semantic contracts; preserving reducers keep
the declared physical width when the existing operation requires that. There is
no implicit narrowing or newly introduced lossy cast.

The core boundary exposes bit-oriented widening and exact narrowing helpers,
without a new dependency on the provider's scalar type. Normal exponents are
rebased exactly; subnormals are normalized using integer bits; NaN fraction bits
are placed in corresponding high Float64 fraction positions. Exact narrowing
rejects overflow, underflow, rounding and NaN payload bits that cannot fit. It
must recover the original bits of every widened binary16 value.
Common-type promotion for sets and selected expression branches uses this same
bridge to write the declared Float64 result directly from selected Float16
storage. The provider's ordinary floating cast may quiet a signaling NaN before
the result owner sees it. Preserve policy checks, selected-row visibility and
allocator ownership at this promotion boundary; it does not introduce a widened
input-column cache.

Explicit Python `types.float16()` declarations accept only exactly representable
values under the caller's existing float policy. Inferred Python floats remain
Float64. The finite typed-JSON route retains its finite-only restriction.
In-process Python construction and row extraction use the bit-oriented bridge
so a carried signaling NaN is not silently quieted by a numeric conversion.
The existing scalar-literal API keeps its current result-domain contract.

## Vortex-first provider decisions

The resolved provider graph is Vortex 0.90.0, `half` 2.7.1, Arrow array/IPC
59.3.1 with schema 59.3.2, and Parquet 59.3.1. These are existing dependencies;
this work does not change their versions or introduce a query-engine integration.

| Subject | Decision | Native concept and admission |
| --- | --- | --- |
| Float16 storage and movement | `use_vortex_native_provider` | Existing `PType::F16`, primitive arrays, host buffers, validity, selection wrappers and source/sink APIs inside the admitted Vortex boundary. |
| Tensor, embedding and vector collections | `use_vortex_native_provider` | Existing fixed/variable tensor and embedding extensions over native fixed lists, lists and structs. Existing extension IDs and metadata versions remain unchanged. |
| Exact scalar register bridge | `implement_shardloom_kernel` | Small bit-oriented conversions extend the existing core Float32 policy helpers. The provider's ordinary numeric conversions do not guarantee preservation of all NaN representation bits. |
| Optional legacy predicate and reduction specializations | `blocked_until_vortex_or_shardloom_evidence` | Preserve their current explicit admission limits. Shared native relational execution supplies the public Float16 route. |

Public typed predicate lowering already excludes floating fields from optional
scan pushdown. Keep that rule: a threshold such as `0.1` must be compared in the
existing exact typed evaluator, without first rounding it to Float16. Extend
the existing type-based shared-relational selection for Float16 sources. Do not
copy the legacy Float32 predicate-literal narrowing into a Float16 path.

The relevant owners are `native_payload_schema`, `native_numeric_owner`,
`native_relational_keys`, the result builders, `native_payload_storage`, native
ingest and the recursive Arrow boundary. Admit Float16 through those owners and
their existing memory grants, cancellation and certificates. Numeric values
widen only in scalar registers; this does not authorize a column-sized Float64
copy or an Arrow execution substrate. `fallback_attempted=false` and
`external_engine_invoked=false` remain required.

The pinned [half API](https://docs.rs/half/2.7.1/half/struct.f16.html) supplies raw
bit constructors. Arrow defines
[half precision in its columnar format](https://arrow.apache.org/docs/25.0/format/Columnar.html),
and Parquet defines
[`FLOAT16` as two little-endian bytes](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#float16)
in a fixed-length byte array. Their format support is source grounding, not
proof of a ShardLoom lifecycle. Complete native and independent-reader checks
are required below.

## Ownership and persistence

Use the existing source/result/context ownership, selected-row copying, bounded
streaming and native spill paths. Preserve logical metadata through duplicate
join matches, reordering, retained Arrow views and source/context release.
Charge two bytes per primitive value plus existing validity and metadata
requirements. Empty and all-null columns retain their declared Float16 dtype.
Cancellation, denied allocation, source-generation changes, partial publication
and last-owner cleanup retain their existing deterministic behavior.

Vortex remains the highest-fidelity persistence target. Arrow IPC and Parquet
must preserve the admitted Float16 values and logical metadata deliberately.
Existing independent-reader limits for null-parent fixed lists and variable
tensor metadata remain explicit; verify capable primitive/present-parent cases
independently and verify complete native reopened results. Do not infer a
complete independent round trip from a schema label.

Text, Avro and ORC do not gain an exact Float16 schema mapping in this unit.
Unsupported writes must refuse before publishing an output. Existing lakehouse
logical schema restrictions remain in force. Scalar source hints and row
reconstruction must not silently replace a declared Float16 field with Float64.
The pinned Avro default writer widens Float16 to Float32. Reject scalar and
recursively nested Float16 in Avro/ORC schema preparation, before opening a
streaming producer or staging any output; do not wait for reopened-schema
validation to discover the loss.

## Required acceptance

1. Exhaust all 65,536 bit patterns through the core bridge. Independently compare
   finite numeric values with the pinned provider, and reject adjacent Float64
   values, excluded exponent regions and discarded NaN payload bits.
2. Carry a raw-buffer Float16 column containing every pattern through native,
   IPC and Parquet write/reopen, with complete valid-bit comparison and
   independent readers where supported. Exercise strict and extended policy,
   exact input construction, nulls and empty sources.
3. Carry a mixed dataset containing ordinary keys, Float16 lists, fixed vectors,
   embeddings, vector collections, fixed tensors, variable tensors and binary
   payloads through selection, filtering, duplicate-match joins, streaming,
   forced native spill and persistence. Compare full selected values, dtype,
   nullability and logical metadata, including sliced/nonzero-offset inputs.
4. Exercise DataFrame, Python SQL, typed native plan, embedded workflow and CLI
   SQL. Reopen native outputs in a separate process after source removal. Check
   retained owners, memory denial/shrink, cancellation, changed sources, failed
   publication and last-owner cleanup using existing conformance facilities.
5. Verify scalar filters, keys, ordering, arithmetic and reductions against the
   existing Float64-register semantics. Include nonrepresentable predicate
   thresholds on both file and memory sources. Preserve error-order tests whose
   previous invalid Float16 fixture becomes valid.
6. Pass focused tests, required default format/lint/workspace tests, native
   lint/workspace tests, the existing feature matrix, Python core tests, a clean
   installed-wheel suite and the runnable example. Preserve original failures
   and exact source/runtime identities. Verify portable evidence at its original
   and relocated locations and reject meaningful corruption controls.
7. Complete hosted review and PR checks, merge, verify the exact main commit,
   then apply verified task-owned artifact retention before the next unit.

This functional contract authorizes no performance claim, benchmark campaign,
package publication, external effects or competitive-gate completion.

## November 7 local and portable acceptance

The [portable packet](../release/evidence/native-float16-lifecycle-2026-11-07/README.md)
binds 71 changed runtime/test sources, 13 accepted validation stages and 442
archived evidence members to the tested native runtime and installed wheel.
Default workspace tests pass 3,321 cases; native workspace tests pass 6,976
with 37 existing opt-in tests ignored. Formatting, strict default/native lint
and all seven feature configurations pass. Dependencies, provider patches and
CI workflows are unchanged.

All 14 Float16 integration tests pass against the frozen runtime and again in
the clean installed package. The installed suite passes all 375 tests with no
failures, errors or skips; isolated loading and uninstall checks pass. The
Python core shard reports 850 test runs among 1,229 discovered cases, with 54
optional/environment skip records and no failures or errors. The runnable
example reopens both complete rows and preserves negative zero.

The checks exhaust every binary16 representation through the core bridge and
native Vortex, IPC and Parquet persistence. They exercise all five public
interfaces, mixed rich payloads, selected-row floating policy, exact predicate
thresholds, joins, streaming, native spill, retained owners and separate-process
reopen after source removal. The empty-input regression preserves Float16 dtype
and nullability for plain and dictionary-encoded sources across all three ingest
modes; a missing typed batch refuses instead of manufacturing a scalar schema.
Memory denial/shrink, cancellation, source changes and pre-publication refusal
retain explicit tests. Independent-reader limitations remain as described above.

Original and relocated packet replay pass. All nine corruption controls reject,
including a removed exhaustive test whose reported counts and log hashes were
adjusted to match. Exact control mutations are independently verified. The
archive preserves all 20 original failed or interrupted top-level observations,
their available logs and dispositions; successful reruns do not replace them.

This acceptance is local and portable. Hosted review, PR checks, exact-main
verification and post-integration retention must finish before the next
implementation unit. Sparse/quantized representations and exact vector
expressions remain separate work; item 19, item 20, RFC 0044 and CG-1 through
CG-23 retain their independent completion requirements.
