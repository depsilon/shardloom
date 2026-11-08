"""Frozen independent mixed-vector reference; no ShardLoom imports or execution."""
from __future__ import annotations

import json


def tagged(pa, name, dtype, extension, metadata, *, nullable=True):
    return pa.field(name, dtype, nullable=nullable, metadata={
        b"ARROW:extension:name": extension.encode(),
        b"ARROW:extension:metadata": json.dumps(metadata, ensure_ascii=False,
            sort_keys=True, separators=(",", ":")).encode(),
    })


def fixture(pa, repetitions=1):
    """Six complete rows; repetition changes only the ordinary scalar row ID."""
    assert type(repetitions) is int and repetitions > 0
    sparse = tagged(pa, "sparse", pa.struct([
        pa.field("size", pa.uint64(), nullable=False),
        pa.field("indices", pa.list_(pa.field("item", pa.uint64(), nullable=False)), nullable=False),
        pa.field("values", pa.list_(pa.float16()), nullable=False),
    ]), "shardloom.sparse_vector.v1", {
        "version": 1, "index_policy": "preserve", "space_id": "source Ω",
        "model_version": None, "normalization": "caller supplied",
    })
    ordered = tagged(pa, "ordered", pa.struct([
        pa.field("size", pa.uint64()),
        pa.field("indices", pa.list_(pa.int16())),
        pa.field("values", pa.list_(pa.float32())),
    ]), "shardloom.sparse_vector.v1", {
        "version": 1, "index_policy": "strictly_increasing", "uniform_size": 8,
    })
    quantized = tagged(pa, "quantized", pa.struct([
        pa.field("codes", pa.list_(pa.int8()), nullable=False),
        pa.field("scale", pa.float16()),
        pa.field("zero_point", pa.int8(), nullable=False),
    ]), "shardloom.quantized_vector.v1", {
        "version": 1, "parameter_mode": "per_vector", "space_id": "source Ω",
        "representation_version": "affine-1",
    })
    blocked = tagged(pa, "blocked", pa.struct([
        pa.field("codes", pa.list_(pa.uint64())),
        pa.field("scale", pa.list_(pa.float64())),
        pa.field("zero_point", pa.list_(pa.uint64())),
    ]), "shardloom.quantized_vector.v1", {
        "version": 1, "parameter_mode": "blocked", "block_size": 2,
    })
    embedding = tagged(pa, "embedding", pa.list_(pa.float16(), 2),
        "shardloom.embedding.v1", {"version": 1, "space_id": "dense Ω"}, nullable=False)
    tensor = tagged(pa, "tensor", pa.list_(pa.float16(), 2),
        "arrow.fixed_shape_tensor", {"shape": [2], "dim_names": ["component"]}, nullable=False)
    variable = tagged(pa, "variable", pa.struct([
        pa.field("data", pa.list_(pa.float16()), nullable=False),
        pa.field("shape", pa.list_(pa.field("item", pa.int32(), nullable=False), 1), nullable=False),
    ]), "arrow.variable_shape_tensor", {"uniform_shape": [None]}, nullable=False)
    collection = pa.field("collection", pa.list_(pa.struct([
        sparse.with_name("s"), blocked.with_name("q"),
    ])))
    sparse_rows = [
        {"size": 10, "indices": [7, 2, 2, 0], "values": [1.5, -0.0, None, 0.0]},
        None,
        {"size": 0, "indices": [], "values": []},
        {"size": 1, "indices": [0], "values": [-2.5]},
        {"size": (1 << 64) - 1, "indices": [(1 << 64) - 2, 1 << 63, 1], "values": [0.0, 2.0, -1.5]},
        {"size": 6, "indices": [5, 1], "values": [None, 4.0]},
    ]
    ordered_rows = [
        {"size": 8, "indices": [0, 3, 7], "values": [0.0, -0.0, None]},
        None, {"size": 8, "indices": [], "values": []},
        {"size": 8, "indices": [7], "values": [1.25]},
        {"size": 8, "indices": [1, 6], "values": [-1.5, 2.0]},
        {"size": 8, "indices": [0], "values": [None]},
    ]
    quantized_rows = [
        {"codes": [0, -128, 127, None, -1], "scale": 0.25, "zero_point": -2},
        None, {"codes": [], "scale": 0.5, "zero_point": 127},
        {"codes": [None], "scale": 2.0, "zero_point": -128},
        {"codes": [1, 2], "scale": 0.125, "zero_point": 0},
        {"codes": [-1], "scale": 1.0, "zero_point": 1},
    ]
    blocked_rows = [
        {"codes": [0, (1 << 64) - 1, None, 1 << 63, 7],
         "scale": [0.5, 2.0, 0.25], "zero_point": [0, (1 << 64) - 1, 1 << 63]},
        None, {"codes": [], "scale": [], "zero_point": []},
        {"codes": [None], "scale": [1.0], "zero_point": [(1 << 64) - 1]},
        {"codes": [1, 2, 3, 4], "scale": [0.125, 4.0], "zero_point": [0, 1]},
        {"codes": [7, 8, 9], "scale": [1.0, 2.0], "zero_point": [2, 3]},
    ]
    fields = [pa.field("id", pa.int64(), nullable=False), pa.field("bucket", pa.int64(), nullable=False),
              pa.field("half", pa.float16()), embedding, tensor, variable,
              sparse, ordered, quantized, blocked, collection]
    rows = []
    for row in range(6 * repetitions):
        at = row % 6
        dense = [(-0.0 if at == 0 else 1.5), None if at == 1 else -2.0]
        data = [0.5, -0.0, None][:at % 4]
        collections = [
            [{"s": sparse_rows[0], "q": blocked_rows[0]}, None,
             {"s": sparse_rows[2], "q": blocked_rows[2]}],
            None, [], [{"s": None, "q": blocked_rows[3]}],
            [{"s": sparse_rows[4], "q": None}],
            [{"s": sparse_rows[5], "q": blocked_rows[5]}],
        ]
        rows.append({"id": row, "bucket": at // 2, "half": [0.0, -0.0, 1.5, None, -2.0, 0.125][at],
            "embedding": dense, "tensor": dense, "variable": {"data": data, "shape": [len(data)]},
            "sparse": sparse_rows[at], "ordered": ordered_rows[at],
            "quantized": quantized_rows[at], "blocked": blocked_rows[at], "collection": collections[at]})
    schema = pa.schema(fields)
    # A nonzero slice offset is part of the frozen source boundary.
    table = pa.Table.from_pylist([rows[-1], *rows], schema=schema).slice(1)
    return table, rows
