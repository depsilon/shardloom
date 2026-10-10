"""Python object delivery preserves the entire native temporal storage domain."""
from __future__ import annotations

import datetime as dt
import importlib.util
import json
import unittest

from shardloom import ResultBatch
from shardloom._result_schema import arrow_table, python_rows, schema_fields
from shardloom.errors import ShardLoomProtocolError
from shardloom.models import OutputEnvelope
from shardloom.query import VortexWorkflowExecutionReport


FORMAT = "vortex.dtype.serde.v1"
TEMPORALS = {
    "day": {"Extension": {"id": "vortex.date", "metadata": [4],
                          "storage_dtype": {"Primitive": ["i32", True]}}},
    "stamp": {"Extension": {"id": "vortex.timestamp", "metadata": [1, 0, 0],
                            "storage_dtype": {"Primitive": ["i64", True]}}},
}


def struct(fields, nullable=False):
    return {"Struct": [{"names": list(fields), "dtypes": list(fields.values())}, nullable]}


def boundaries():
    days = [-(1 << 31), -719163, -719162, -1, 0, 1, 2932896, 2932897, (1 << 31) - 1, None]
    stamps = [-(1 << 63), -62135596800000001, -62135596800000000, -1, 0, 1,
              253402300799999999, 253402300800000000, (1 << 63) - 1, None]
    python_days = [-(1 << 31), -719163, dt.date.min, dt.date(1969, 12, 31),
                   dt.date(1970, 1, 1), dt.date(1970, 1, 2), dt.date.max, 2932897,
                   (1 << 31) - 1, None]
    python_stamps = [-(1 << 63), -62135596800000001, dt.datetime.min,
                     dt.datetime(1969, 12, 31, 23, 59, 59, 999999), dt.datetime(1970, 1, 1),
                     dt.datetime(1970, 1, 1, 0, 0, 0, 1), dt.datetime.max,
                     253402300800000000, (1 << 63) - 1, None]
    raw = [{"day": day, "stamp": stamp} for day, stamp in zip(days, stamps)]
    expected = [{"day": day, "stamp": stamp} for day, stamp in zip(python_days, python_stamps)]
    return raw, expected


class TemporalResultDeliveryTests(unittest.TestCase):
    def assert_exact_objects(self, actual, expected):
        self.assertIs(type(actual), type(expected))
        self.assertEqual(actual, expected)
        if isinstance(expected, dict):
            self.assertEqual(tuple(actual), tuple(expected))
            for name in expected:
                self.assert_exact_objects(actual[name], expected[name])
        elif isinstance(expected, (list, tuple)):
            for value, wanted in zip(actual, expected):
                self.assert_exact_objects(value, wanted)

    def test_calendar_edges_and_full_storage_endpoints_survive_report_and_batch_views(self):
        raw, expected = boundaries()
        wire = json.dumps(struct(TEMPORALS))
        fields = schema_fields(wire, FORMAT)
        batch = ResultBatch(0, tuple(raw), fields)
        self.assert_exact_objects(batch.python_objects, tuple(expected))
        self.assertEqual(batch.result_rows, tuple(raw))
        self.assertEqual(batch.result_schema, fields)
        for payload in ({"result_values_json": json.dumps(raw)},
                        {"result_jsonl": "".join(json.dumps(row) + "\n" for row in raw)}):
            with self.subTest(payload=next(iter(payload))):
                report = VortexWorkflowExecutionReport(None, "collect", OutputEnvelope.from_field_mapping({
                    **payload, "result_schema_json": wire, "result_schema_format": FORMAT,
                }))
                self.assert_exact_objects(report.python_objects, tuple(expected))
                self.assertEqual(report.result_rows, tuple(raw))
                self.assertEqual(report.result_schema, fields)
        self.assert_exact_objects(python_rows(raw, fields, temporal_objects=False), raw)

    def test_nested_temporal_views_preserve_units_and_parent_nullability(self):
        raw, expected = boundaries()
        declarations = {"record": struct(TEMPORALS, True),
                        "items": {"List": [struct(TEMPORALS, True), True]},
                        "fixed": {"FixedSizeList": [TEMPORALS["stamp"], 2, True]}}
        rows = [{"record": raw[0], "items": [raw[1], None, raw[2]],
                 "fixed": [raw[6]["stamp"], raw[7]["stamp"]]},
                {"record": raw[8], "items": [raw[6], raw[7]], "fixed": [None, raw[0]["stamp"]]},
                {"record": None, "items": [], "fixed": None},
                {"record": None, "items": None, "fixed": [None, None]}]
        wanted = [{"record": expected[0], "items": [expected[1], None, expected[2]],
                   "fixed": [expected[6]["stamp"], expected[7]["stamp"]]},
                  {"record": expected[8], "items": [expected[6], expected[7]],
                   "fixed": [None, expected[0]["stamp"]]},
                  {"record": None, "items": [], "fixed": None},
                  {"record": None, "items": None, "fixed": [None, None]}]
        fields = schema_fields(json.dumps(struct(declarations)), FORMAT)
        self.assert_exact_objects(ResultBatch(0, tuple(rows), fields).python_objects, tuple(wanted))
        self.assert_exact_objects(python_rows(rows, fields, temporal_objects=False), rows)

    def test_invalid_temporal_storage_still_raises_in_both_conversion_modes(self):
        for name, width in (("day", 32), ("stamp", 64)):
            fields = schema_fields(json.dumps(struct({name: TEMPORALS[name]})), FORMAT)
            for value in (-(1 << (width - 1)) - 1, 1 << (width - 1), True, 1.5, "0"):
                for temporal in (True, False):
                    with self.subTest(name=name, value=value, temporal=temporal):
                        with self.assertRaisesRegex(ShardLoomProtocolError, "declared domain"):
                            python_rows([{name: value}], fields, temporal_objects=temporal)
            nonnullable = json.loads(json.dumps(TEMPORALS[name]))
            nonnullable["Extension"]["storage_dtype"]["Primitive"][1] = False
            fields = schema_fields(json.dumps(struct({name: nonnullable})), FORMAT)
            with self.assertRaisesRegex(ShardLoomProtocolError, "nonnullable"):
                python_rows([{name: None}], fields)

    @unittest.skipUnless(importlib.util.find_spec("pyarrow"), "optional PyArrow conversion dependency")
    def test_arrow_retains_native_temporal_units_without_python_calendar_conversion(self):
        import pyarrow as pa

        raw, _ = boundaries()
        fields = schema_fields(json.dumps(struct(TEMPORALS)), FORMAT)
        table = arrow_table(raw, fields, pa)
        self.assertEqual(table.schema, pa.schema([pa.field("day", pa.date32()),
                                                pa.field("stamp", pa.timestamp("us"))]))
        self.assertEqual(table.column("day").cast(pa.int32()).to_pylist(), [row["day"] for row in raw])
        self.assertEqual(table.column("stamp").cast(pa.int64()).to_pylist(), [row["stamp"] for row in raw])


if __name__ == "__main__":
    unittest.main()
