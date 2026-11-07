<!-- SPDX-License-Identifier: Apache-2.0 -->

# Fixed-shape tensor lifecycle

Status: implementation, local checks and portable evidence passed, November 7;
hosted integration pending. This is the
first tensor delivery in item 19 of the [active queue](phased-execution-plan.md),
after dense embeddings and managed binary objects. It implements the fixed-shape
part of [rich-data step 3](native-rich-data-lifecycle-2026-10-24.md). Variable
shapes follow through the same extension framework; their per-row shape/data
validation is a separate acceptance boundary. Sparse, quantized and lower
precision representations, tensor arithmetic, models and devices remain later
work. RFCs 0011, 0013, 0014, 0017, 0021, 0033 and 0044 govern this unit.

## Provider decision

The pinned Vortex 0.90.0 provider already has native `ExtensionArray`,
`FixedSizeListArray`, dtype registries, editions, structural gather, native
file/spill writers and Arrow import/export interfaces. Use those providers;
there is no new array layout, storage service or execution dependency.

The optional published `vortex-tensor` 0.90.0 source was checked against its
crates.io checksum before this decision. Its `FixedShapeTensor` vtable requires
nonnullable primitive components, multiplies shape dimensions without checked
arithmetic, and deserializes metadata without the receiving session's allocation
grant. It stores logical shape, while Arrow's canonical extension stores physical
shape. Its metadata/protobuf helpers also normalize empty optional arrays to
absence. Enabling that provider would not establish this contract's nullable
component, exact metadata and resource guarantees. Its implementation is not
copied into ShardLoom and its dependency is not enabled.

Classification: `implement_shardloom_kernel` for checked logical type admission
and metadata mapping; existing Vortex array/compute/file providers perform native
carriage under the current `vortex-local-primitives` and `universal-format-io`
boundaries. Residual tensor arithmetic is explicitly unsupported. Existing
execution and Native I/O certificates retain `fallback_attempted=false` and
`external_engine_invoked=false`. Arrow is an explicit import/export boundary.
No performance claim follows from lifecycle correctness.

## Persistent representation

The registered native dtype is `shardloom.fixed_shape_tensor.v1`, stored over a
native fixed-size list of Boolean, signed/unsigned 8/16/32/64-bit integers, or
Float32/Float64. Outer and component nullability are independent. Float16,
complex, decimal, string and nested tensor elements are not admitted by this
delivery. Values, including signed zero and non-finite floating bit patterns,
are preserved without conversion, transposition, padding or normalization.

Metadata is canonical UTF-8 JSON containing required `shape` and optional
`dim_names` and `permutation`. Unknown or duplicate fields fail. Each shape
dimension is a nonnegative signed 64-bit integer; the checked product must equal
the native uint32 fixed-list width. Rank zero has one element. Any zero dimension
has zero elements, including when the other dimensions would overflow a nonzero
product. Every dimension is validated before computing that product. No additional
rank/dimension ceiling is imposed; existing schema and allocation limits apply.

Shape and names describe physical row-major storage. Logical dimension `i`
refers to physical dimension `permutation[i]`; absence means identity. A supplied
permutation must contain each rank index exactly once. Names must be strings and
match rank; their bytes, order, empty strings and duplicates remain unchanged.
Optional arrays must be arrays, not null, and supplied empty arrays remain present
for rank zero. Canonical encoding sorts keys and omits whitespace. Native decode
requires canonical bytes; Arrow import accepts valid equivalent JSON and
canonicalizes it without changing its meaning. Parsing scratch and retained
metadata use the receiving session allocator, including independent spill sessions.

## Public and compatibility boundaries

Python declares `types.fixed_shape_tensor(item, shape, *, dim_names=None,
permutation=None, nullable=True)`. Values are flat physical-order lists; the type
records how to interpret them. Existing SQL, DataFrame, application and CLI
source/result/write paths share the native representation and diagnostics.
Result schemas preserve item type, shape, optional metadata and both nullability
levels. A tensor remains payload-only when filtering or joining ordinary keys;
comparison, grouping, distinct, sorting or arithmetic on tensor values fails
explicitly until a later semantic contract admits it.

Arrow uses the canonical `arrow.fixed_shape_tensor` name and metadata defined in
the [Arrow specification](https://arrow.apache.org/docs/format/CanonicalExtensions.html#fixed-shape-tensor).
Its signed 32-bit list-width limit is checked at that boundary. Native Vortex is
the highest-fidelity persistence target. Arrow IPC and admitted Parquet paths
must preserve the complete canonical extension metadata. Text, Avro and ORC
writers that cannot retain this identity refuse it before publication. Unknown
extensions are rejected rather than stripped to plain lists.

PyArrow 25.0.1's built-in canonical tensor deserializer makes component fields
nullable and normalizes present-empty optional arrays. ShardLoom's `to_arrow()`
helpers therefore reattach the authoritative native schema to shared physical
storage arrays, carrying canonical extension field metadata without changing
numeric buffers. Nested sliced struct validity may be rebuilt in caller-owned
memory at this explicit export boundary. Native files, Arrow IPC/Parquet files
and outgoing C schemas retain the original metadata. Third-party consumers that
normalize canonical tensor types have their own fidelity limits; bypassing the
ShardLoom helper with `pyarrow.table(result)` does not apply its restoration.
PyArrow also rejects a metadata shape whose intermediate nonzero product
overflows int64 before a later zero. Native zero-product validation is independent
of dimension order and those tensors remain storable; this is an explicit
PyArrow export limitation, not a native shape limit or a successful lossy export.

## Acceptance

Prove import, retain, arbitrary selection with duplicates, ordinary-column
filter/join, bounded stream, forced native spill, write, source/context drop,
separate-process reopen and export against independently constructed values.
Include rank zero, zero-sized dimensions, nullable rows/components, empty input,
nonzero slices, three-dimensional non-self-inverse permutations, Unicode names,
all admitted element widths and special float bits. Malformed shape, overflow,
width mismatch, duplicate/out-of-range permutations, unknown metadata, changed
extension identity and denied metadata grants must fail deterministically.
Verify metadata retention/release in parent and spill pools, no partial published
output, and no-fallback diagnostics. Complete focused and required workspace,
feature, Python and installed-package checks before hosted review and integration.
CG-1 through CG-23 retain their independent status; no benchmark, release or
package publication is authorized by this implementation contract.

## November 7 local verification

The final release CLI and native Python library pass 350 clean installed-package
tests with no skips, plus installation, identity and uninstall checks. All 12
tensor lifecycle tests and all 19 public-context tests execute in that package.
They cover ordinary SQL, DataFrame, Context, application and CLI write paths,
source/context drop, independent process reopen, native spill and shared numeric
buffers at Arrow export. Nested lists/structs, empty files, rank zero, zero-sized
dimensions, exact floating values, independent nullability, metadata grants and
unsupported roles/wrappers have explicit controls.

Default Rust tests pass 3,319 cases. Native/optional-graph Rust tests pass 6,955
cases, with 37 existing opt-in tests ignored. Strict default/native lint,
formatting, the seven-configuration feature matrix and generated public-doc/API
checks pass. The standard Python core shard passes with 838 reported test runs
and 49 optional/environment skip records; its 1,192 discovered cases are kept
separate from the executed count. Installed native coverage has no skips.

The original native lint failure came from a signed cast in a zero-width test
fixture; it was replaced with a checked conversion. The feature matrix then
identified an Arrow-only constant in minimal builds. Its declaration now has
the same feature gate as its consumers. Broad Rust suites precede only that
gate; exact source comparisons and resolved feature graphs establish unchanged
enabled code in those suites. Final strict native lint and the minimal compile
check cover both branches. Nine pre-existing minimal-build warnings remain.

The first mixed Python invocation injected the real CLI into four resolution
fixtures and a raw dynamic-library path into five suffix-based native loaders.
It also exposed one stale public-context assertion for older error wording.
The unchanged native diagnostic correctly refused a changed source. The test
now checks its stable code and current actionable message, and all public-context
tests are included in the isolated installed-package proof. The standard public
shard and installed native proof pass separately. Original failed observations
remain preserved; no runtime change or native rebuild was required for these
two test/harness corrections.

The [portable acceptance packet](../release/evidence/native-fixed-shape-tensor-2026-11-07/README.md)
binds all 28 changed source files, 14 accepted check stages, the tested runtime
and installed wheel to 230 archived evidence members. Original and relocated
offline replay pass. Five controlled corruptions are rejected, including failed
checks and skipped installed tests whose archive hashes were recomputed. Original
failed attempts and their corrections remain visible. Hosted review, integration
and post-merge retirement remain pending; this acceptance does not complete
item 19 or establish a performance result.

The first hosted Linux wheel jobs stopped in the package checker's isolated
test fixture: it lacked the newly required public-context suite. The
[hosted follow-up](../release/evidence/native-fixed-shape-tensor-2026-11-07/hosted-followup.json)
preserves both full job logs, a local reproduction and the corrected 19-test
harness pass. The fixture now includes context coverage and checks that missing
or skipped context/adapter suites remain fatal. Runtime and package contents
are unchanged; the original 350-test installed proof still applies. Hosted
validation of the corrected head remains required. Replay this supplement with
`python3 docs/release/evidence/native-fixed-shape-tensor-2026-11-07/verify_hosted_followup.py --repo-root .`.
