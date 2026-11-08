"""Carry dense, sparse and quantized vectors through native files exactly."""

import math
from pathlib import Path
import tempfile

import shardloom as sl
from shardloom import types as t


def main() -> None:
    half = t.float16()
    schema = {
        "id": t.int64(nullable=False),
        "score": half,
        "passages": t.list_(t.embedding(half, 3, metadata={"space_id": "supplied-vectors"})),
        "image": t.fixed_shape_tensor(half, [2, 2]),
        "samples": t.variable_shape_tensor(half, 2, uniform_shape=[None, 2]),
        "sparse": t.sparse_vector(t.float16(), index="uint64"),
        "quantized": t.quantized_vector(t.int8(), scale="float16"),
        "blocked": t.quantized_vector(t.uint64(), scale="float64", block_size=2),
    }
    rows = [
        {"id": 1, "score": 1.5, "passages": [[0.5, -0.0, None], [1.0, 2.0, 3.0]],
         "image": [0.5, 1.0, None, 2.0], "samples": {"data": [1.0, 2.0], "shape": [1, 2]},
         "sparse": {"size": 10, "indices": [7, 2, 2], "values": [1.5, -0.0, None]},
         "quantized": {"codes": [-128, None, 127], "scale": 0.25, "zero_point": -2},
         "blocked": {"codes": [0, (1 << 64) - 1, None], "scale": [0.5, 2.0],
                     "zero_point": [0, (1 << 64) - 1]}},
        {"id": 2, "score": None, "passages": [], "image": None,
         "samples": {"data": [], "shape": [0, 2]},
         "sparse": {"size": 0, "indices": [], "values": []},
         "quantized": {"codes": [], "scale": 0.25, "zero_point": -2},
         "blocked": {"codes": [], "scale": [], "zero_point": []}},
    ]
    with tempfile.TemporaryDirectory(prefix="shardloom-half-vectors-") as directory:
        output = Path(directory) / "vectors.vortex"
        with sl.context(transport="native", memory_bytes=64 << 20, max_parallelism=1) as context:
            # Vector and tensor components travel as payload; ordinary columns
            # supply the filter and ordering keys. No model or vector search runs.
            frame = context.from_rows(rows, schema=schema).where("id >= 1").sort("id", descending=True)
            frame.write_vortex(output, check=True)
            reopened = context.read_vortex(output).collect(check=True)
            assert reopened.python_objects == tuple(reversed(rows))
            assert dict(reopened.result_schema) == schema
            assert reopened.python_objects[1]["sparse"] == rows[0]["sparse"]
            assert reopened.python_objects[1]["quantized"] == rows[0]["quantized"]
            assert reopened.python_objects[1]["blocked"] == rows[0]["blocked"]
            assert reopened.python_objects[0]["blocked"] == rows[1]["blocked"]
            assert math.copysign(1.0, reopened.python_objects[1]["sparse"]["values"][1]) == -1.0
            print("Dense, sparse and quantized vectors reopened from Vortex unchanged.")
            for row in reopened.python_objects:
                print(row)


if __name__ == "__main__":
    main()
