from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from shardloom.client import ShardLoomClient
from shardloom.models import OutputEnvelope


class SourceBindingsTransportTests(unittest.TestCase):
    def setUp(self) -> None:
        self.client = ShardLoomClient(binary="unused", memory_gb=4, max_parallelism=2)
        self.commands: list[list[str]] = []

        def capture_run(args: list[str], *, check: bool = True) -> OutputEnvelope:
            self.commands.append(args)
            return OutputEnvelope.from_field_mapping({}, command=args[0])

        self.client.run = capture_run  # type: ignore[method-assign]

    def _bindings_arg(self, args: list[str]) -> str:
        self.assertIn("--source-bindings", args)
        return args[args.index("--source-bindings") + 1]

    def test_route_and_run_serialize_bindings_compactly(self) -> None:
        bindings = {
            "s3://bucket/a.csv?version=7": {
                "input_format": "csv",
                "source_schema": {"label": "utf8", "count": "int64"},
            },
            "local/path/data.jsonl": {
                "input_format": "jsonl",
                "source_schema": [("enabled", "bool"), ("value", "float64")],
            },
        }

        self.client.public_workflow_route("sql", source_bindings=bindings)
        self.client.public_workflow_run("sql", source_bindings=bindings)

        self.assertEqual(len(self.commands), 2)
        for args in self.commands:
            raw = self._bindings_arg(args)
            self.assertNotIn(" ", raw)
            self.assertEqual(
                json.loads(raw),
                {
                    "s3://bucket/a.csv?version=7": {
                        "input_format": "csv",
                        "source_schema": "label:utf8,count:int64",
                    },
                    "local/path/data.jsonl": {
                        "input_format": "jsonl",
                        "source_schema": "enabled:bool,value:float64",
                    },
                },
            )

    def test_route_and_run_omit_none_or_empty_bindings(self) -> None:
        for method in (self.client.public_workflow_route, self.client.public_workflow_run):
            with self.subTest(method=method.__name__, bindings=None):
                method("sql", source_bindings=None)
                self.assertNotIn("--source-bindings", self.commands[-1])
            with self.subTest(method=method.__name__, bindings={}):
                method("sql", source_bindings={})
                self.assertNotIn("--source-bindings", self.commands[-1])

    def test_invalid_binding_shapes_and_limits_are_rejected(self) -> None:
        invalid: tuple[tuple[str, Any, type[Exception]], ...] = (
            ("nonmapping", ["csv"], TypeError),
            ("nonstring URI", {1: {"input_format": "csv"}}, TypeError),
            ("missing format", {"path": {"source_schema": "id:int64"}}, ValueError),
            (
                "unknown field",
                {"path": {"input_format": "csv", "extra": True}},
                ValueError,
            ),
            ("empty format", {"path": {"input_format": "  "}}, ValueError),
            (
                "too many entries",
                {f"path-{index}": {"input_format": "csv"} for index in range(129)},
                ValueError,
            ),
            (
                "oversized serialized JSON",
                {"path": {"input_format": "x" * (256 * 1024)}},
                ValueError,
            ),
        )
        for label, bindings, error_type in invalid:
            with self.subTest(case=label), self.assertRaises(error_type):
                self.client.public_workflow_route("sql", source_bindings=bindings)


if __name__ == "__main__":
    unittest.main()
