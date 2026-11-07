from __future__ import annotations

from pathlib import Path
import tempfile

import shardloom as sl
from shardloom import types as t


def main() -> None:
    tensor_type = t.variable_shape_tensor(
        t.float32(nullable=True),
        ndim=2,
        dim_names=["height", "width"],
        permutation=[1, 0],
        uniform_shape=[None, 3],
        nullable=True,
    )

    with tempfile.TemporaryDirectory(prefix="shardloom-variable-tensor-") as directory:
        output = Path(directory) / "measurements.vortex"
        context = sl.context(
            transport="native", memory_bytes=128 << 20, max_parallelism=1
        )
        frame = selected = result = reopened = None
        try:
            frame = context.from_rows(
                [
                    {
                        "id": 1,
                        "state": "archived",
                        "measurement": {
                            "data": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
                            "shape": [2, 3],
                        },
                    },
                    {"id": 2, "state": "active", "measurement": None},
                    {
                        "id": 3,
                        "state": "active",
                        "measurement": {
                            "data": [7.0, None, 9.0],
                            "shape": [1, 3],
                        },
                    },
                ],
                schema={
                    "id": t.int64(nullable=False),
                    "state": t.string(nullable=False),
                    "measurement": tensor_type,
                },
            )

            # Filtering and ordering use ordinary keys; tensor values travel as payload.
            selected = frame.filter("id >= 2").sort("id")
            result = selected.collect(check=True)
            selected.write_vortex(output, check=True)
            reopened = context.read_vortex(output).collect(check=True)

            restored_type = dict(reopened.result_schema)["measurement"]
            print(f"tensor schema: {restored_type.label}")
            for row in reopened.python_objects:
                print(row)
        finally:
            context.close()
            reopened = result = selected = frame = None


if __name__ == "__main__":
    main()
