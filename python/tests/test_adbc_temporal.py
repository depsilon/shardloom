"""Exact temporal tick preservation in ADBC scalar parameter binding."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from shardloom.adbc import _parameter_values


try:
    import pyarrow as pa
except ImportError:
    pa = None


@unittest.skipIf(pa is None, "PyArrow is required for ADBC Arrow parameter tests")
class AdbcTemporalParameterTests(unittest.TestCase):
    @staticmethod
    def _batch(fields):
        schema = pa.schema([pa.field(name, dtype, nullable=True) for name, dtype, _ in fields])
        arrays = []
        for _name, dtype, value in fields:
            if value is None:
                arrays.append(pa.array([None], type=dtype))
            else:
                storage = pa.int32() if pa.types.is_date32(dtype) or pa.types.is_time32(dtype) else pa.int64()
                arrays.append(pa.array([value], type=storage).cast(dtype))
        return pa.RecordBatch.from_arrays(arrays, schema=schema)

    def test_all_temporal_families_and_units_map_to_canonical_names_and_exact_ticks(self):
        fields = [
            ("date32", pa.date32(), 2),
            ("date64", pa.date64(), -86_400_000),
            ("time_s", pa.time32("s"), 12),
            ("time_ms", pa.time32("ms"), 12_345),
            ("time_us", pa.time64("us"), 12_345_678),
            ("time_ns", pa.time64("ns"), 12_345_678_901),
            ("timestamp_s", pa.timestamp("s"), -1),
            ("timestamp_ms", pa.timestamp("ms"), -1_001),
            ("timestamp_us", pa.timestamp("us"), 1_234_567),
            ("timestamp_ns", pa.timestamp("ns"), 1_234_567_891),
            ("timestamp_zone", pa.timestamp("ns", tz="America/Chicago"), -1),
            ("duration_s", pa.duration("s"), 2),
            ("duration_ms", pa.duration("ms"), 2_003),
            ("duration_us", pa.duration("us"), 2_000_003),
            ("duration_ns", pa.duration("ns"), 2_000_000_003),
        ]
        expected_names = {
            "date32": "date32", "date64": "date64",
            "time_s": "time(s)", "time_ms": "time(ms)",
            "time_us": "time(us)", "time_ns": "time(ns)",
            "timestamp_s": "timestamp(s)", "timestamp_ms": "timestamp(ms)",
            "timestamp_us": "timestamp_micros", "timestamp_ns": "timestamp(ns)",
            "timestamp_zone": "timestamp(ns,America/Chicago)",
            "duration_s": "duration(s)", "duration_ms": "duration(ms)",
            "duration_us": "duration(us)", "duration_ns": "duration(ns)",
        }
        values = _parameter_values(self._batch(fields))
        for name, _dtype, ticks in fields:
            with self.subTest(name=name):
                self.assertEqual(values[name].dtype.name, expected_names[name])
                self.assertEqual(values[name].value, ticks)

    def test_full_signed_64_bit_timestamp_and_duration_values_are_preserved(self):
        minimum, maximum = -(1 << 63), (1 << 63) - 1
        for dtype, ticks in ((pa.timestamp("ns"), minimum), (pa.timestamp("us"), maximum),
                             (pa.duration("ns"), minimum), (pa.duration("us"), maximum)):
            with self.subTest(dtype=dtype, ticks=ticks):
                result = _parameter_values(self._batch([("value", dtype, ticks)]))["value"]
                self.assertEqual(result.value, ticks)

    def test_typed_and_untyped_nulls_remain_null(self):
        typed = _parameter_values(self._batch([("stamp", pa.timestamp("ns"), None)]))["stamp"]
        untyped = _parameter_values(self._batch([("nothing", pa.null(), None)]))
        self.assertIsNone(typed.value)
        self.assertEqual(typed.dtype.name, "timestamp(ns)")
        self.assertIsNone(untyped["nothing"])

    def test_unsupported_arrow_type_keeps_explicit_diagnostic(self):
        batch = pa.record_batch([pa.array([1], type=pa.int32())], names=["value"])
        with self.assertRaisesRegex(ValueError, "ADBC parameter type int32 has no exact native scalar contract"):
            _parameter_values(batch)


if __name__ == "__main__":
    unittest.main()
