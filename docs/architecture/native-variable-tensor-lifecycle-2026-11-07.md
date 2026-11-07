<!-- SPDX-License-Identifier: Apache-2.0 -->

# Variable-shape tensor lifecycle

Status: local, portable and hosted acceptance passed, November 7, 2026; merged
in PR #1662 with exact-main verification and generated-artifact retirement complete.
This is the second tensor delivery in item 19 of the
[active queue](phased-execution-plan.md), following the
[fixed-shape contract](native-fixed-tensor-lifecycle-2026-11-07.md).
It completes variable-shape storage and carriage within rich-data step 3.
Sparse, quantized and lower-precision representations and exact vector expressions
retain their subsequent positions. RFCs 0011, 0013, 0014, 0017, 0021, 0033 and 0044
govern the extension, ownership, recovery and shared execution boundaries.
CG-1 through CG-23 retain their independent requirements.

## Provider decision

Use the pinned Vortex 0.90.0 extension, struct, list, fixed-size-list, selection,
native file and spill providers. The checksum-bound `vortex-tensor` 0.90.0
source reviewed for the first tensor delivery registers vector and fixed-shape
types only; it supplies no variable-shape dtype or row-shape validator.
There is no new array layout, storage service, dependency or execution engine.

Classification: `implement_shardloom_kernel` for checked metadata admission
and per-row shape validation around existing Vortex-native providers.
The canonical interoperability contract is
[Arrow's variable-shape tensor specification](https://arrow.apache.org/docs/format/CanonicalExtensions.html#variable-shape-tensor).
The pinned arrow-schema 59.3.2 helper serializes `permutations`, while the
canonical specification uses `permutation`; it also does not establish per-row
shape/data consistency. Its helper is therefore not the metadata authority.
PyArrow 25.0.1 has no public `variable_shape_tensor` constructor, although its
IPC reader recognizes the extension. Canonical struct fields and extension
field metadata remain ShardLoom's exchange form.

The first complete lifecycle run exposed three adapter defects in the existing
provider graph. The same `arrow-data` 59.3.2 and `arrow-array` 59.3.1 sources now
have narrow, provenance-recorded path patches: struct slicing applies parent
offsets once, and all fixed-list builder finish modes retain explicit row counts
at width zero. The existing Parquet patch preserves inherited null list masks
under absent structs. No package version changes. Subsequent local verification
passed; original failed runs remain in the evidence packet.

## Persistent and value contract

The native identity is `shardloom.variable_shape_tensor.v1`. Storage is a
struct with exactly two fields in order: `data`, a native variable list of
Boolean, signed/unsigned 8/16/32/64-bit integer or Float32/Float64 components;
and `shape`, a native fixed-size list of Int32 dimensions. Its fixed width is
the tensor rank, which is constant for the column. Rank may be zero.
Rows can have different dimension sizes, but not different ranks.

Whole-tensor and component nullability remain independent. All structural
nullability flags on the data list, shape list and shape components are also
retained. A present tensor must have present data and shape and no null shape
dimensions, even when its declared fields permit nulls. Children beneath an
absent tensor or containing null parent are unobservable. Validation must not
inspect those hidden values or turn them into successful present tensors.

Each visible dimension is a nonnegative Int32. Validate every dimension and
every uniform-dimension constraint before accepting a product. Rank zero
requires one component; any zero dimension requires zero components, including
when an earlier nonzero partial product would overflow. Otherwise the checked
product must exactly equal that row's data-list length. Validate offsets and
their bounds using the native list-coordinate provider. Never pad, truncate,
transpose, densify or infer shape from the flat payload. Preserve component
bits, including signed zero and non-finite floating representations, subject
to the existing caller-selected floating policy.

Metadata is canonical UTF-8 JSON with optional `dim_names`, `permutation`
and `uniform_shape`. No `shape` or `ndim` key is stored: rank belongs to
the shape field. Unknown and duplicate keys fail. Supplied arrays must match
rank, and supplied empty arrays remain present for rank zero. Names preserve
their strings and order. A permutation contains each rank index exactly once.
Uniform dimensions are nonnegative Int32 values or null; null means variable.
Names and uniform dimensions describe physical row-major storage; logical
dimension i refers to physical dimension `permutation[i]`. Absence means
identity order or fully variable dimensions as appropriate.

Arrow import accepts equivalent JSON, an empty metadata string, or absent
metadata for the empty optional object, then canonicalizes to native JSON.
Native decode requires canonical bytes. Schema parsing scratch and retained
metadata belong to the receiving allocator, including independent spill pools.
Existing resource/schema limits remain; no arbitrary tensor dimension cap or
new memory owner is added. The new dtype has a subsequent tensor edition;
the fixed-shape edition's admitted members remain unchanged.

## Public interfaces and validation placement

Python declares `types.variable_shape_tensor(item, ndim, *, dim_names=None,
permutation=None, uniform_shape=None, nullable=True)`. Present values are
`{"data": [...], "shape": [...]}`; null tensors are `None`.
The simple constructor uses nonnullable data/shape containers and dimensions.
Result schema descriptors retain complete native storage nullability so that
reusing an imported result schema does not normalize it.

SQL, DataFrame, Context, application and CLI paths use the same dtype and
payload owners. Tensor values remain payload-only: ordinary-key selection,
filtering and joins carry them; tensor comparison, grouping, ordering and
arithmetic fail explicitly. Unsupported wrappers and unknown extension
identities fail instead of becoming unannotated structs.

Validate public typed and Arrow intake before publishing the source, using
the complete containing schema and parent visibility. Do not validate an
isolated Arrow extension child without its parent's null mask. Reuse the
normal selected-payload validation boundary for native-file reads and output;
internal copies that preserve unobserved values must not validate discarded
rows early. Per-row shape checks read shape and list coordinates, not tensor
component values. Cancellation and existing allocator grants apply.

Vortex is the highest-fidelity persistence target. Arrow IPC and Parquet retain
the canonical `arrow.variable_shape_tensor` metadata and struct storage.
The Arrow boundary requires signed Int32 list offsets/rank representation;
a larger valid native representation fails explicitly on export. Text, Avro
and ORC paths that cannot preserve identity refuse before publication.
Arrow export may reconstruct wrappers or validity at the caller boundary;
there is no claim that every boundary is zero-copy.

## Acceptance and risks

Prove typed/Arrow intake, retain, arbitrary row order and duplicates,
ordinary-column filtering and duplicate-match joins, bounded stream, forced
native spill, all five public write interfaces, source/context drop,
separate-process reopen and export. Compare complete values, metadata, nullability
and floating bits through native Vortex, Arrow IPC and Parquet. Independently
read complete IPC results and all present-parent Parquet rows with PyArrow.
For the independent typed Parquet read, recover the file's `ARROW:schema`
footer with PyArrow IPC and pass that stored schema to the Parquet reader.
The default PyArrow reader drops the variable-tensor extension from its returned
schema; compare its complete present-parent values too and record that schema
limitation separately. Do not substitute an expected schema from the fixture.
Also attempt the complete independent Parquet read: PyArrow 25.0.1 rejects
fixed-size lists beneath null parents, so retain its exact failure separately
from native full-result equality. Do not claim that this external reader
validated rows it could not read.
Exercise nested lists/structs, nonzero slices, empty/all-null input, rank zero,
zero products, three-dimensional non-self-inverse permutation and partial
uniform shapes.

Controls cover malformed metadata, negative/null dimensions, wrong rank,
shape/data mismatch, zero after intermediate overflow, invalid offsets,
uniform-dimension violations, unsupported components/wrappers, changed sources,
denied metadata/data grants, cancellation, partial publication and last-owner
release. Include invalid hidden children and discarded rows to establish
visibility semantics; malformed visible values must fail.

The main risks are row/shape separation during gather, validation that ignores
parent masks, unchecked products, metadata loss in spill/export, and
unaccounted retained metadata. Local verification and remaining hosted work are
recorded below.

## November 7 local verification

Formatting, default/native lint, default workspace tests (3,319), native
workspace tests (6,972 passed; 37 existing ignored), the seven-configuration
feature matrix, provider tests (1,606 passed; one
pre-existing `Map` test ignored), static documentation checks and the example
passed. The installed native package passed 361 tests with zero failures,
errors or skips, including 11 variable-tensor and 19 public-context tests. The
Python core shard reported 845 test runs among 1,210 discovered cases, with 51
optional/environment skip records and no failures or errors. The example's SQL
predicate was corrected after the native build; that correction changed no
runtime code.

The [portable acceptance packet](../release/evidence/native-variable-shape-tensor-2026-11-07/README.md)
binds 162 source files, 13 accepted check stages and 497 archived evidence
members, including all 23 original failed observations. Original and relocated
replay pass, and nine controlled corruptions are rejected. PR #1662 merged as
`c47eeefa05012cce32b053f53af18583041c9f19` after all 58 PR checks passed. All 55
required checks passed on that exact main merge. Verified retirement removed
9,996 obsolete regular files and three environment links; inputs and retained
runtime/wheel identities stayed unchanged, and post-retirement replay passed.
PyArrow 25.0.1 has two documented limitations: its
default reader drops the tensor extension from the returned schema, and its
Parquet reader rejects fixed-size lists beneath null parents. The packet records
present-parent comparisons and native full-result equality separately; it does
not claim a complete independent PyArrow Parquet read. No performance campaign,
package publication or completion of all item 19 obligations follows from this
contract.
