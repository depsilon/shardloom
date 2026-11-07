<!-- SPDX-License-Identifier: Apache-2.0 -->

# Variable-shape tensors

Variable-shape tensors keep a fixed rank for one column while allowing the
dimension sizes to vary by row. The declaration describes the component type,
rank and optional dimension metadata. Each present value supplies its own flat
data and physical shape; ShardLoom checks that the dimensions match the data
length. Null tensors remain distinct from present tensors with empty dimensions
or nullable components.

The Python constructor is `types.variable_shape_tensor(item, ndim, *,
dim_names=None, permutation=None, uniform_shape=None, nullable=True)`:

```python
from shardloom import types as t

measurement_type = t.variable_shape_tensor(
    t.float32(nullable=False),
    ndim=2,
    dim_names=["height", "width"],
    permutation=[1, 0],
    nullable=True,
)
```

The component type may be Boolean, signed or unsigned 8/16/32/64-bit integer,
Float16, Float32 or Float64. Its nullability controls individual components;
`nullable` controls whether the whole tensor may be `None`. The simple
constructor uses nonnullable data and shape containers, and nonnullable Int32
dimensions. A present value has exactly these keys:

```python
{"data": [1.0, 2.0, 3.0, 4.0], "shape": [2, 2]}
```

`data` is a flat physical row-major list. `shape` contains one nonnegative Int32
dimension per declared rank. A rank-zero tensor has one component; if any
dimension is zero, the data list must be empty. Otherwise the checked product
of the dimensions must equal the data length. ShardLoom does not reshape,
transpose, pad, truncate or infer dimensions from the data. A null tensor is
written as `None`.

## Dimension metadata

`dim_names`, `permutation` and `uniform_shape` are optional arrays and must match
the declared rank. Names describe physical dimensions and preserve their text
and order. `uniform_shape` describes physical dimensions too: a nonnegative
Int32 value fixes that dimension for every present row, while `None` leaves it
variable. A permutation contains each rank index exactly once. Unknown or
duplicate metadata keys and arrays of the wrong rank fail explicitly. Logical
dimension `i` refers to physical dimension `permutation[i]`; when omitted,
logical and physical order are the same. The permutation describes
interpretation and does not reorder the flat data. Omitted metadata means all
dimensions can vary; an explicit empty array remains present for rank zero.

For example, a value with physical shape `[2, 3]` and permutation `[1, 0]` has
logical shape `[3, 2]`. The six values remain in physical row-major order.

## Query and persistence boundaries

Tensor values are payload-only. Filter, sort, select and join using ordinary
columns to carry tensor payloads through a workflow. Tensor comparison,
grouping, ordering or arithmetic is unsupported and fails explicitly.

Vortex is the highest-fidelity persistence target. Arrow IPC and Parquet use the
canonical `arrow.variable_shape_tensor` extension name, canonical metadata and
struct storage containing `data` and `shape`. Arrow's signed Int32 list-offset
boundary applies during export. Text, Avro and ORC outputs that cannot preserve
the tensor identity are refused before publication. PyArrow 25.0.1 has no public
`variable_shape_tensor` constructor, although its IPC reader recognizes the
extension. Its default Parquet reader drops the extension from the returned
schema; a typed independent read must use the `ARROW:schema` stored in the file's
footer. Downstream libraries may apply their own extension and nullability
behavior. PyArrow 25.0.1 also rejects Parquet fixed-size
lists beneath null tensor parents. ShardLoom's native Parquet reader preserves
these values; independent PyArrow checks distinguish complete present-parent
results from that reader limitation.

For a Parquet file containing only present tensor parents, PyArrow can use its
stored schema explicitly:

```python
import base64
import pyarrow as pa
import pyarrow.parquet as pq

metadata = pq.read_metadata("tensors.parquet").metadata
schema = pa.ipc.read_schema(pa.BufferReader(base64.b64decode(metadata[b"ARROW:schema"])))
table = pq.read_table("tensors.parquet", schema=schema)
```

The [Python example](../../python/examples/variable_shape_tensor.py) creates,
filters, orders, collects, writes and reopens a small local Vortex file without
network access or an external query engine.
