"""Carry exact Float16 values and several vectors per row through native files."""

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
    }
    rows = [
        {"id": 1, "score": 1.5, "passages": [[0.5, -0.0, None], [1.0, 2.0, 3.0]],
         "image": [0.5, 1.0, None, 2.0], "samples": {"data": [1.0, 2.0], "shape": [1, 2]}},
        {"id": 2, "score": None, "passages": [], "image": None,
         "samples": {"data": [], "shape": [0, 2]}},
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
            print("Exact Float16 values and vector collections reopened from Vortex.")
            for row in reopened.python_objects:
                print(row)


if __name__ == "__main__":
    main()
