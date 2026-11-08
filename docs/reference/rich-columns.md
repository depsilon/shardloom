<!-- SPDX-License-Identifier: Apache-2.0 -->

# Dense vectors, embeddings, tensors and binary objects

This capability is in current source builds after 1.1.0. It is not included in
the published 1.1.0 wheels. Use a matching native Python source build.
Files containing embeddings require a reader that registers
`shardloom.embedding.v1`; older wheels cannot read that logical type.
Managed binary objects additionally require `shardloom.binary-object.v1` and
the matching native object layout. The `native-binary-objects` feature is part
of the native Python and release user-surface build.
Fixed-shape tensors require a reader that registers `shardloom.fixed_shape_tensor.v1`.
Variable-shape tensors require `shardloom.variable_shape_tensor.v1`.

Store supplied dense Float16/Float32/Float64 vectors, inline bytes and nested values
alongside ordinary columns. An embedding adds caller-supplied identity to a
fixed-size vector. Filtering, ordering and joining on ordinary columns carry
these payloads through the same native engine, including explicit native spill.

```python
import shardloom as sl
from shardloom import types as t

client = sl.ShardLoomClient(
    transport="native", memory_bytes=128 << 20, max_parallelism=1
)
try:
    documents = sl.from_rows(
        [
            {"id": 1, "content": b"\x00\xff", "embedding": [0.5, -0.0, None]},
            {"id": 2, "content": b"", "embedding": None},
        ],
        schema={
            "id": t.int64(),
            "content": t.binary(),
            "embedding": t.embedding(
                t.float32(), 3,
                metadata={
                    "space_id": "documents-v1",
                    "model_id": "caller-supplied-model",
                    "representation_version": "dense-f32-v1",
                    "normalization": "none",
                },
            ),
        },
        client=client,
    )
    documents.select("id", "embedding", "content").write_vortex(
        "documents.vortex", check=True
    )
    result = sl.read_vortex("documents.vortex", client=client).collect(check=True)
    values = result.python_objects
    declared_types = result.result_schema
    arrow = result.to_arrow()
finally:
    client.close()
```

`t.embedding(item, size, metadata=..., nullable=...)` is a schema declaration.
It does not generate embeddings or execute a model. Its mapping equivalent is
`{"type": "embedding", "item": "float32", "size": 3, "metadata": {...}}`.
Use `t.fixed_size_list(t.float32(), size)` for a numeric vector without embedding
identity. `t.list_`, `t.struct` and `t.binary` describe ordinary rich columns.
Empty bytes, null bytes, an empty variable list and a null vector remain distinct.

## Shape, nullability and identity

Embedding dimensions are positive. The item must be Float16, Float32 or Float64. The
item's `nullable` flag governs components; the embedding's `nullable` flag
governs the complete vector. Values must have exactly the declared dimension.
No padding, truncation, rounding or normalization occurs. Size arithmetic,
the active memory allocation and the selected format's representation limits
govern admission. Native fixed-list widths use uint32; Arrow's width is signed
32-bit. There is no additional embedding dimension ceiling.

Metadata is versioned: `version` defaults to the integer `1` in Python
declarations. These optional fields accept nonempty strings or `None`:

| Field | Caller-supplied meaning |
| --- | --- |
| `space_id` | Identity of the embedding space |
| `model_id`, `model_version` | Model identity and version |
| `representation_version` | Version of the supplied representation |
| `normalization` | Description of its existing normalization state |
| `preprocessing_id` | Identity of the preprocessing applied |
| `source_provenance` | Column-level source/content provenance |

Unknown or missing identity remains unknown. Matching dimensions do not imply
compatible spaces. Keep row-specific provenance in companion columns. Unknown
metadata keys/versions and malformed dimensions fail explicitly. The existing
schema envelope and resource admission also cover this metadata.

The native logical type is `shardloom.embedding.v1`. Arrow fields carry
`ARROW:extension:name=shardloom.embedding.v1` and the canonical UTF-8 JSON
`ARROW:extension:metadata`. Arrow input may use ordinary JSON spacing/key order;
native persistence uses the canonical encoding. Struct field names, outer/component
nullability, exact floating width and declared identity survive nested fields.
Unrelated Arrow application metadata retains its existing adapter limitations.

## Values and output formats

The default `float_policy="strict"` rejects observed nonfinite input. Select
`float_policy="extended"` on the client to carry NaNs and infinities. Native
arrays, Arrow and Python-object delivery preserve floating bits, including
signed zero and representable NaN payloads. Explicit Python Float16 and Float32
declarations require an exact native representation; inferred Python floats
remain Float64. Raw typed-JSON numbers remain finite-only.

Vortex persistence, native spill, Arrow IPC and the native Parquet adapter
preserve the embedding declaration. CSV, JSON/JSONL files, Avro and ORC currently
reject declared embeddings before publication because those adapters do not
preserve this logical identity. Bounded CLI JSON **result envelopes** separately
carry a typed schema and finite values; that is distinct from an unannotated JSON
file export.

PyArrow 25.0.1 cannot read null fixed-size-list parents in Parquet, including
files it writes itself. The native Parquet reader preserves those rows. Use
Vortex or Arrow IPC when that PyArrow reader is the downstream consumer and
vectors may be null. Acceptance separately checks complete native Parquet
roundtrips, independently written Parquet input, independent readback without
null vector parents, and all nullable rows through Arrow IPC.

## Scope

This delivery carries rich payloads through projection, ordinary-column filters,
joins, bounded input/output, native spill, write and reopen. It does not admit
embedding or tensor values as comparison keys or arithmetic operands.
Fixed- and variable-shape tensors and Float16 components use the same payload
lifecycle. Sparse and quantized logical representations require separate
interpretation contracts. Exact vector functions remain in the
[rich-data lifecycle plan](../architecture/native-rich-data-lifecycle-2026-10-24.md).
Existing nested-schema depth/node/metadata limits remain; replacing them requires
the separately tracked resource-backed traversal work, not removal of checks.

See the [implementation and acceptance contract](../architecture/native-dense-embedding-lifecycle-2026-11-05.md)
for the native provider, ownership and verification boundaries.

## Float16 and multiple vectors per row

Use `t.float16()` for an explicit binary16 column or component type. Values such
as `0.5`, `1.5` and negative zero are exactly representable. A Python value such
as `0.1` is refused rather than silently rounded. Scalar arithmetic retains the
existing Float64 result domain; selectors such as MIN/MAX and discrete quantiles
retain Float16. Filters compare their thresholds without rounding them to the
column's width. Scalar Float16 casts are not added by this declaration.

Use existing lists to store several vectors in one row:

```python
from shardloom import types as t

vectors = t.list_(t.fixed_size_list(t.float16(), 3))
embeddings = t.list_(t.embedding(t.float16(), 3, metadata={"space_id": "passages"}))
# A row value: [[0.5, -0.0, None], [1.0, 2.0, 3.0]]
```

An empty collection, a null collection, a null vector and a null component remain
distinct. Native files, Arrow IPC and Parquet retain these declarations and raw
Float16 representations, subject to the independent-reader limitations above.
Avro and ORC refuse scalar or nested Float16 output before consuming a streaming
producer. Embedding and tensor text-output restrictions also remain unchanged.

The [runnable example](../../python/examples/float16_vectors.py) combines Float16
scalars, vector collections and both tensor forms, then writes and reopens a
native file. The [Float16 lifecycle contract](../architecture/native-float16-lifecycle-2026-11-07.md)
records the exact-bit, observation-policy and ownership requirements.

## Fixed-shape tensors

Declare tensor shape with `t.fixed_shape_tensor`. Supply each value as a flat
list in physical row-major order:

```python
from shardloom import types as t

tensor_type = t.fixed_shape_tensor(
    t.float32(nullable=False), [2, 3],
    dim_names=["height", "width"], permutation=[1, 0], nullable=True,
)
# In an ordinary from_rows schema: {"id": t.int64(), "tensor": tensor_type}
# Example tensor values: [1.0, 2.0, 3.0, 4.0, 5.0, 6.0] or None.
```

`shape` and `dim_names` describe physical storage. Logical dimension `i` refers
to physical dimension `permutation[i]`; the example has logical shape `[3, 2]`.
Omitting `permutation` means the same physical and logical dimension order.
The declaration never transposes values. Names retain their exact strings.
The component and whole-tensor `nullable` flags remain independent.

Elements may be Boolean, signed/unsigned 8/16/32/64-bit integers, Float16, Float32 or
Float64. Shape dimensions are nonnegative int64 values; their checked product
must fit the native uint32 list width. Arrow additionally requires a signed
32-bit list width. Shape `[]` denotes one scalar element, supplied as `[value]`;
a zero dimension denotes an empty tensor, supplied as `[]`, distinct from null.
Float policy and JSON number rules are the same as for vectors above.

Native results and files retain shape, optional names/permutation, element type
and nullability through selection, ordinary-key joins, streaming and spill.
Arrow IPC and Parquet use the canonical `arrow.fixed_shape_tensor` extension;
text, Avro and ORC file writers refuse tensor columns before publication.
Use `result.to_arrow()` or `batch.to_arrow()` for exact Python schema delivery.
These helpers retain physical Arrow fields with canonical extension metadata:
PyArrow's built-in tensor reader otherwise makes component fields nullable and
normalizes supplied empty optional arrays. Direct third-party C-stream consumers
apply their own extension rules. PyArrow 25.0.1 also rejects a shape when an
intermediate dimension product overflows int64 before a later zero dimension;
native storage accepts the valid zero-element tensor and Vortex remains available.

Variable-shape tensors keep one rank per column and store each row's physical
shape beside its flat data. See the [variable-shape tensor guide](variable-shape-tensors.md)
for its declaration and metadata rules. Tensor arithmetic remains separate
work. The [fixed tensor contract](../architecture/native-fixed-tensor-lifecycle-2026-11-07.md)
records the fixed representation, provider decision and verification requirements.

## Managed binary objects

Use `t.binary()` for ordinary inline bytes. Use `t.binary_object()` when rows
should retain a byte object while queries carry only its descriptor. Explicit
reads return complete bytes or a bounded range; selecting or inspecting a
handle does not read its payload. Null and a valid empty object are distinct.

```python
import tempfile
import shardloom as sl
from shardloom import types as t

with tempfile.TemporaryDirectory(prefix="shardloom-objects-") as directory:
    context = sl.context(transport="native", memory_bytes=64 << 20, max_parallelism=1)
    try:
        store = context.binary_object_store(directory, max_bytes=256 << 20)
        image = store.import_file("image.bin")
        empty = context.binary_object(b"")
        frame = context.from_rows(
            [{"id": 1, "payload": image}, {"id": 2, "payload": empty}],
            schema={"id": "int64", "payload": t.binary_object(nullable=False)},
        )
        frame.write_vortex("images.vortex", check=True)
        result = context.read_vortex("images.vortex").collect(check=True)
        handle = result.python_objects[0]["payload"]
        first_bytes = handle.read_range(0, min(128, handle.byte_length))
        ownership = handle.ownership  # "managed"
    finally:
        context.close()
        handle = result = frame = image = empty = store = None
```

The store copies a supplied file in bounded chunks into private temporary
storage under the existing directory. `store.put_bytes(data)` puts supplied
bytes there; `context.binary_object(data)` keeps a small managed object in
memory. `max_bytes` limits the store's retained payloads. Retained results,
readers and Arrow buffers can keep private files alive after the context closes;
keep their parent directory available until those owners are released. A store
never owns or deletes the original input. Its `snapshot` reports retained bytes
and cleanup failures. `context.binary_object_snapshot()` reports managed
identities and explicit payload reads; metadata/table reads have separate I/O
accounting.

`handle.read()` and `handle.read_range(start, end)` return Python `bytes`; ranges
use an inclusive start and exclusive end. These are explicit copies into Python
memory. The native operation first admits its buffers under the selected memory
limit. A large full read can be refused; iterate bounded ranges instead. Managed
file reads verify complete 64 KiB integrity chunks touched by the range. A short
range can therefore read more file bytes than it returns. Changed, missing or
damaged data raises an error, never a successful empty placeholder. Readers
remain usable after `context.close()`. Finish or close an execution stream
before reading its handles through the same context: the active stream owns
that context's execution operation.

`context.external_binary_object(path, start=0, end=length)` instead retains an
explicit caller-owned local file range without importing its bytes. Its
`ownership` is `"external"`. The file must retain the captured generation and
remain available. Native export preserves that external reference; it does not
copy it or make it portable.

Native Vortex export copies each distinct selected managed payload once into
the same file as its table. The resulting managed-only file can be moved and
reopened by a matching reader without its original sources or temporary store.
Queries and native spill retain the shared owner while carrying descriptors.
Joins, filters and ordering use ordinary columns; binary objects are not admitted
as comparison, grouping, ordering or arithmetic operands.

Arrow export preserves the versioned extension and retains object owners in its
buffers. Its Binary storage contains descriptors, not payload bytes. Bare Arrow
descriptors do not transfer ownership to a new context. Use `context.from_result`
for a native result, `context.import_binary_object` for an individual handle, or
reopen a complete native Vortex export. Cross-context file imports share the
immutable private file and its original cleanup quota; receiving arrays and
directories have their own memory admission. Parquet, Arrow IPC, Avro, ORC, CSV
and JSON/JSONL file exports currently reject object columns. Explicitly read
objects into inline Binary values when a compatibility output needs actual bytes.

See the [managed binary contract](../architecture/native-managed-binary-objects-2026-11-06.md)
for persistent format, integrity, ownership and verification boundaries.
