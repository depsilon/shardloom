<!-- SPDX-License-Identifier: Apache-2.0 -->

# Dense vectors, embeddings and binary columns

This capability is in current source builds after 1.1.0. It is not included in
the published 1.1.0 wheels. Use a matching native Python source build.
Files containing embeddings require a reader that registers
`shardloom.embedding.v1`; older wheels cannot read that logical type.

Store supplied dense Float32/Float64 vectors, inline bytes and nested values
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

Embedding dimensions are positive. The item must be Float32 or Float64. The
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
signed zero and representable NaN payloads. Python Float32 values must have an
exact native representation. Raw JSON numbers remain finite-only.

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
embedding values as comparison keys or vector arithmetic operands. Managed
large-object storage, tensors, Float16/sparse/quantized representations and exact
vector functions remain in the [rich-data lifecycle plan](../architecture/native-rich-data-lifecycle-2026-10-24.md).
Existing nested-schema depth/node/metadata limits remain; replacing them requires
the separately tracked resource-backed traversal work, not removal of checks.

See the [implementation and acceptance contract](../architecture/native-dense-embedding-lifecycle-2026-11-05.md)
for the native provider, ownership and verification boundaries.
