"""Input declarations are immutable data; all operations execute in the native engine."""
from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from shardloom import LazyFrame, ShardLoomClient, ShardLoomContext, from_arrow_table, from_pandas


class NativeMemoryInputTests(unittest.TestCase):
    def setUp(self) -> None:
        self.client = ShardLoomClient(binary="unused")
        self.context = ShardLoomContext(self.client)

    def declaration(self, frame: LazyFrame) -> dict[str, object]:
        return json.loads(json.dumps(dict(frame.source.memory_input)))

    def test_rows_snapshot_preserves_nulls_exact_integers_text_and_name_order(self) -> None:
        rows = [{"n": None, "text": "λ,;\"\n%=null"}, {"text": None, "n": (1 << 63) - 1}]
        with mock.patch.object(self.client, "public_workflow_run") as execute:
            frame = self.context.from_rows(rows)
            rows[0]["n"] = 999
            rows[1]["text"] = "changed"
            self.assertEqual(self.declaration(frame), {
                "kind": "rows", "schema": [["n", "int64"], ["text", "utf8"]],
                "rows": [[None, "λ,;\"\n%=null"], ["9223372036854775807", None]],
            })
            execute.assert_not_called()

    def test_explicit_empty_and_all_null_schemas_are_retained(self) -> None:
        for rows in [[], [{"n": None, "text": None}]]:
            frame = self.context.from_rows(rows, schema={"n": "int64", "text": "utf8"})
            self.assertEqual(frame.source.schema, (("n", "int64"), ("text", "utf8")))
            self.assertEqual(self.declaration(frame)["rows"], [] if not rows else [[None, None]])
        self.assertEqual(self.context.from_rows([{"n": None}]).source.schema, (("n", "bool"),))
        with self.assertRaisesRegex(ValueError, "explicit schema"):
            self.context.from_rows([])

    def test_numeric_inference_is_order_independent_and_rejects_loss(self) -> None:
        for values in [(1, 1.5, None), (None, 1.5, 1)]:
            frame = self.context.from_rows([{"n": value} for value in values])
            self.assertEqual(frame.source.schema, (("n", "float64"),))
        for rows in [[{"n": (1 << 53) + 1}, {"n": 0.5}], [{"n": 1 << 63}], [{"n": float("nan")}]]:
            with self.subTest(rows=rows), self.assertRaises(ValueError):
                self.context.from_rows(rows)
        with self.assertRaises(TypeError):
            self.context.from_rows([{"n": True}, {"n": 1}])

    def test_input_boundaries_share_typed_row_declaration(self) -> None:
        rows = [{"n": None}, {"n": 7}]
        pandas = SimpleNamespace(to_dict=lambda orient: rows)
        arrow = SimpleNamespace(to_pylist=lambda: rows)
        expected = self.declaration(self.context.from_rows(rows))
        for frame in [from_pandas(pandas, client=self.client), from_arrow_table(arrow, client=self.client)]:
            self.assertIsInstance(frame, LazyFrame)
            self.assertEqual(self.declaration(frame), expected)
        for constructor, value in [(self.context.from_pandas, SimpleNamespace(to_dict=lambda orient: [])),
                                   (self.context.from_arrow_table, SimpleNamespace(to_pylist=lambda: []))]:
            frame = constructor(value, schema={"n": "int64"})
            self.assertEqual(self.declaration(frame), {"kind": "rows", "schema": [["n", "int64"]], "rows": []})

    def test_invalid_shape_and_payload_bounds_fail_before_execution(self) -> None:
        with mock.patch.object(self.client, "public_workflow_run") as execute:
            for rows in [[{"n": 1}, {"m": 2}], [{str(i): i for i in range(65)}],
                         [{"n": 1}] * 65_537, [{"n": "x" * (8 * 1024 * 1024)}]]:
                with self.subTest(size=len(rows)), self.assertRaises(ValueError):
                    self.context.from_rows(rows)
            execute.assert_not_called()


if __name__ == "__main__":
    unittest.main()
