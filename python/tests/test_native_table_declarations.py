# SPDX-License-Identifier: Apache-2.0
"""Table source declarations preserve selectors without reading during construction."""
from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
import shardloom as sl
from shardloom._native_backend import NativeBackend
from shardloom.models import OutputEnvelope


class NativeTableDeclarationTests(unittest.TestCase):
    def setUp(self):
        self.client = sl.ShardLoomClient(binary="unused-shardloom", memory_gb=4, max_parallelism=2)
        self.context = sl.ShardLoomContext(self.client)
        self.reply = SimpleNamespace(envelope=OutputEnvelope.from_field_mapping({
            "result_jsonl": "", "result_payload_complete": "true", "output_row_count": "0",
            "result_schema_format": "vortex.dtype.serde.v1",
            "result_schema_json": json.dumps({"Struct": [{"names": ["id"],
                "dtypes": [{"Primitive": ["i64", True]}]}, False]}),
            "fallback_attempted": "false", "external_engine_invoked": "false",
        }, command="run"))

    def test_selectors_reach_collect_bindings_and_defaults_are_empty_without_eager_work(self):
        cases = (
            ("iceberg", {"snapshot_id": 17}, {"snapshot_id": 17}),
            ("iceberg", {"snapshot_id": -(1 << 63)}, {"snapshot_id": -(1 << 63)}),
            ("iceberg", {"snapshot_id": (1 << 63) - 1}, {"snapshot_id": (1 << 63) - 1}),
            ("delta", {"version": 23}, {"version": 23}),
            ("delta", {"version": (1 << 64) - 1}, {"version": (1 << 64) - 1}),
            ("hudi", {"instant": "20261102093000"}, {"instant": "20261102093000"}),
            ("iceberg", {}, {}), ("delta", {}, {}), ("hudi", {}, {}),
        )
        with mock.patch.object(self.client, "public_workflow_run", return_value=self.reply) as execute, \
                mock.patch.object(self.client, "vortex_prepare", side_effect=AssertionError("eager preparation")), \
                mock.patch.object(Path, "exists", side_effect=AssertionError("declaration probed path")), \
                mock.patch.object(Path, "open", side_effect=AssertionError("declaration opened path")):
            for input_format, selectors, selection in cases:
                path = f"missing-{input_format}-table"
                with self.subTest(format=input_format, selectors=selectors):
                    frame = sl.read(path, input_format=input_format, client=self.client, **selectors)
                    self.assertIsInstance(frame, sl.LazyFrame)
                    execute.assert_not_called()
                    frame.select("id").collect()
                    execute.assert_called_once()
                    binding = execute.call_args.kwargs["source_bindings"][path]
                    self.assertEqual(binding, {"input_format": input_format, "table_snapshot": selection})
                    execute.reset_mock()

    def test_table_selector_validation_and_authoritative_schema(self):
        invalid = (
            ({"input_format": "iceberg", "snapshot_id": True}, TypeError),
            ({"input_format": "iceberg", "snapshot_id": 1.5}, TypeError),
            ({"input_format": "iceberg", "snapshot_id": -(1 << 63) - 1}, ValueError),
            ({"input_format": "iceberg", "snapshot_id": 1 << 63}, ValueError),
            ({"input_format": "delta", "version": True}, TypeError),
            ({"input_format": "delta", "version": "1"}, TypeError),
            ({"input_format": "delta", "version": -1}, ValueError),
            ({"input_format": "delta", "version": 1 << 64}, ValueError),
            ({"input_format": "hudi", "instant": 123}, TypeError),
            ({"input_format": "hudi", "instant": ""}, ValueError),
            ({"input_format": "hudi", "instant": "１２３"}, ValueError),
            ({"input_format": "hudi", "instant": "12x"}, ValueError),
            ({"input_format": "hudi", "instant": "1" * 33}, ValueError),
            ({"input_format": "iceberg", "schema": {"id": "int64"}}, ValueError),
            ({"input_format": "delta", "snapshot_id": 1}, ValueError),
            ({"input_format": "hudi", "version": 1}, ValueError),
            ({"input_format": "parquet", "version": 1}, ValueError),
        )
        for kwargs, error in invalid:
            with self.subTest(kwargs=kwargs), self.assertRaises(error):
                sl.read("table-or-file", client=self.client, **kwargs)

    @staticmethod
    def backend():
        backend = NativeBackend.__new__(NativeBackend)
        backend._plans = __import__("weakref").WeakSet()
        backend._plans_lock = __import__("threading").Lock()
        backend.session = SimpleNamespace(sql=mock.Mock(side_effect=AssertionError("native plan called")))
        return backend

    def test_raw_native_bindings_reject_invalid_table_snapshot_before_plan(self):
        invalid = (
            ("iceberg", {"other": 1}, ValueError),
            ("delta", {"snapshot_id": 1}, ValueError),
            ("hudi", "20261102", TypeError),
            ("parquet", {}, ValueError),
        )
        for input_format, selection, error in invalid:
            with self.subTest(format=input_format, selection=selection):
                backend = self.backend()
                request = {"sql_statement": "SELECT * FROM 'source'", "source_bindings": {
                    "source": {"input_format": input_format, "table_snapshot": selection},
                }}
                with self.assertRaises(error):
                    backend._plan(request, allocation=None)
                backend.session.sql.assert_not_called()


if __name__ == "__main__":
    unittest.main()
