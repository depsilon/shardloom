"""Pure temporal name, storage metadata, and Arrow mapping contracts."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from shardloom._temporal import (
    arrow_temporal_type,
    canonical_temporal_name,
    decode_temporal_dtype,
    encode_temporal_dtype,
    temporal_parts,
    validate_temporal_ticks,
)


class TemporalMetadataTests(unittest.TestCase):
    def test_core_temporal_names_and_timestamp_us_alias(self):
        fixtures = {
            "date32": ("date32", None, None),
            "date64": ("date64", None, None),
            "time(s)": ("time", "s", None),
            "time(ms)": ("time", "ms", None),
            "time(us)": ("time", "us", None),
            "time(ns)": ("time", "ns", None),
            "timestamp(s)": ("timestamp", "s", None),
            "timestamp(ms)": ("timestamp", "ms", None),
            "timestamp(us)": ("timestamp", "us", None),
            "timestamp(ns)": ("timestamp", "ns", None),
            "timestamp(us,America/Chicago)": ("timestamp", "us", "America/Chicago"),
            "timestamp(ns,UTC+05:30)": ("timestamp", "ns", "UTC+05:30"),
            "duration(s)": ("duration", "s", None),
            "duration(ms)": ("duration", "ms", None),
            "duration(us)": ("duration", "us", None),
            "duration(ns)": ("duration", "ns", None),
        }
        for name, expected in fixtures.items():
            with self.subTest(name=name):
                self.assertEqual(temporal_parts(name), expected)
                canonical = "timestamp_micros" if name == "timestamp(us)" else name
                self.assertEqual(canonical_temporal_name(name), canonical)
        self.assertEqual(temporal_parts("timestamp_micros"), ("timestamp", "us", None))
        self.assertEqual(canonical_temporal_name("timestamp_micros"), "timestamp_micros")

    def test_rejects_noncanonical_names_and_malformed_zones(self):
        for name in (
            "Date32",
            "date32()",
            "timestamp()",
            "timestamp(microseconds)",
            "timestamp(us,)",
            "timestamp(us,/America/Chicago)",
            "timestamp(us,America//Chicago)",
            "timestamp(us,America/./Chicago)",
            "timestamp(us,America/../Chicago)",
            "timestamp(us,America\u00a0Chicago)",
            "timestamp(us," + "A" * 256 + ")",
            "time(s,UTC)",
            "duration(us,UTC)",
        ):
            with self.subTest(name=name):
                self.assertIsNone(temporal_parts(name))
                self.assertIsNone(canonical_temporal_name(name))
        self.assertIsNone(temporal_parts(None))
        self.assertEqual(
            canonical_temporal_name("timestamp(us,Not/In/Host/Database)"),
            "timestamp(us,Not/In/Host/Database)",
        )
        self.assertEqual(
            decode_temporal_dtype(
                {
                    "id": "vortex.timestamp",
                    "metadata": [1, 14, 0, *b"Not/In/Host/DB"],
                },
                "int64",
                True,
            ),
            "timestamp(us,Not/In/Host/DB)",
        )

    def test_literal_vortex_metadata_fixtures_decode_and_encode(self):
        fixtures = (
            ("date32", "vortex.date", "int32", [4]),
            ("date64", "vortex.date", "int64", [2]),
            ("time(s)", "vortex.time", "int32", [3]),
            ("time(ms)", "vortex.time", "int32", [2]),
            ("time(us)", "vortex.time", "int64", [1]),
            ("time(ns)", "vortex.time", "int64", [0]),
            ("timestamp(s)", "vortex.timestamp", "int64", [3, 0, 0]),
            ("timestamp(ms)", "vortex.timestamp", "int64", [2, 0, 0]),
            ("timestamp_micros", "vortex.timestamp", "int64", [1, 0, 0]),
            ("timestamp(ns)", "vortex.timestamp", "int64", [0, 0, 0]),
            (
                "timestamp(us,America/Chicago)",
                "vortex.timestamp",
                "int64",
                [1, 15, 0, *b"America/Chicago"],
            ),
            ("duration(s)", "shardloom.duration.v1", "int64", [0]),
            ("duration(ms)", "shardloom.duration.v1", "int64", [1]),
            ("duration(us)", "shardloom.duration.v1", "int64", [2]),
            ("duration(ns)", "shardloom.duration.v1", "int64", [3]),
        )
        for name, identifier, storage, metadata in fixtures:
            with self.subTest(name=name):
                expected = {
                    "Extension": {
                        "id": identifier,
                        "storage_dtype": {"Primitive": ["i32" if storage == "int32" else "i64", False]},
                        "metadata": metadata,
                    }
                }
                self.assertEqual(encode_temporal_dtype(name, False), expected)
                self.assertEqual(
                    decode_temporal_dtype(
                        {"id": identifier, "metadata": metadata}, storage, False
                    ),
                    name,
                )
                self.assertEqual(decode_temporal_dtype(expected, storage, False), name)

    def test_timestamp_zone_metadata_preserves_case_and_exact_bytes(self):
        name = "timestamp(ms,America/New_York)"
        encoded = encode_temporal_dtype(name, True)
        self.assertEqual(
            encoded,
            {
                "Extension": {
                    "id": "vortex.timestamp",
                    "storage_dtype": {"Primitive": ["i64", True]},
                    "metadata": [2, 16, 0, *b"America/New_York"],
                }
            },
        )
        self.assertEqual(
            decode_temporal_dtype(
                {"id": "vortex.timestamp", "metadata": [2, 16, 0, *b"America/New_York"]},
                "int64",
                True,
            ),
            name,
        )

    def test_metadata_decoder_rejects_wrong_ids_units_widths_and_shapes(self):
        bad = (
            ({"id": "vortex.date", "metadata": [4]}, "int64", False),
            ({"id": "vortex.date", "metadata": [2]}, "int32", False),
            ({"id": "vortex.time", "metadata": [3]}, "int64", False),
            ({"id": "vortex.time", "metadata": [1]}, "int32", False),
            ({"id": "vortex.timestamp", "metadata": [1, 0, 0, 65]}, "int64", False),
            ({"id": "vortex.timestamp", "metadata": [1, 1, 0]}, "int64", False),
            ({"id": "vortex.timestamp", "metadata": [1, 0, 0, 255]}, "int64", False),
            ({"id": "vortex.timestamp", "metadata": [1, 0, 0]}, "int32", False),
            ({"id": "shardloom.duration.v1", "metadata": [3, 0]}, "int64", False),
            ({"id": "vortex.unknown", "metadata": [4]}, "int32", False),
            ({"id": "vortex.date", "metadata": [True]}, "int32", False),
            ({"id": "vortex.date", "metadata": (4,)}, "int32", False),
            ({"id": "vortex.date", "metadata": [4], "extra": 1}, "int32", False),
        )
        for extension, storage, nullable in bad:
            with self.subTest(extension=extension, storage=storage):
                self.assertIsNone(decode_temporal_dtype(extension, storage, nullable))
        self.assertIsNone(decode_temporal_dtype({"id": "vortex.date", "metadata": [4]}, "int32", 0))
        self.assertIsNone(
            decode_temporal_dtype(
                {
                    "Extension": {
                        "id": "vortex.date",
                        "storage_dtype": {"Primitive": ["i32", True]},
                        "metadata": [4],
                    }
                },
                "int32",
                False,
            )
        )
        self.assertIsNone(encode_temporal_dtype("date32", 1))

    def test_tick_validation_preserves_full_domains_and_checks_calendar_ranges(self):
        for name, bounds in (
            ("date32", (-(1 << 31), (1 << 31) - 1)),
            (
                "date64",
                (
                    _I64_MIN_TEST + (-_I64_MIN_TEST) % 86_400_000,
                    _I64_MAX_TEST - _I64_MAX_TEST % 86_400_000,
                ),
            ),
            ("timestamp(s)", (-(1 << 63), (1 << 63) - 1)),
            ("timestamp(ns,Unlisted/Zone)", (-(1 << 63), (1 << 63) - 1)),
            ("duration(ns)", (-(1 << 63), (1 << 63) - 1)),
        ):
            for value in bounds:
                with self.subTest(name=name, value=value):
                    self.assertEqual(validate_temporal_ticks(name, value), value)
        for name in ("time(s)", "time(ms)", "time(us)", "time(ns)"):
            upper = 86_400 * _test_ticks_per_second(name)
            self.assertEqual(validate_temporal_ticks(name, 0), 0)
            self.assertEqual(validate_temporal_ticks(name, upper - 1), upper - 1)
            with self.subTest(name=name), self.assertRaises(ValueError):
                validate_temporal_ticks(name, upper)
            with self.subTest(name=name, negative=True), self.assertRaises(ValueError):
                validate_temporal_ticks(name, -1)
        for name, value in (
            ("date32", 1 << 31),
            ("date64", 1),
            ("date64", 1 << 63),
            ("date64", -(1 << 63)),
            ("timestamp(us)", 1 << 63),
            ("duration(s)", -(1 << 63) - 1),
        ):
            with self.subTest(name=name, value=value), self.assertRaises(ValueError):
                validate_temporal_ticks(name, value)
        for value in (True, False, 1.0, "1"):
            with self.subTest(value=value), self.assertRaises(TypeError):
                validate_temporal_ticks("timestamp_micros", value)
        with self.assertRaises(ValueError):
            validate_temporal_ticks("date128", 0)

    def test_arrow_type_mapping_uses_exact_standard_temporal_factories(self):
        calls = []

        def factory(kind):
            return lambda *args, **kwargs: (calls.append((kind, args, kwargs)) or (kind, args, kwargs))

        pa = SimpleNamespace(
            date32=factory("date32"),
            date64=factory("date64"),
            time32=factory("time32"),
            time64=factory("time64"),
            timestamp=factory("timestamp"),
            duration=factory("duration"),
        )
        expected = {
            "date32": ("date32", (), {}),
            "date64": ("date64", (), {}),
            "time(s)": ("time32", ("s",), {}),
            "time(ms)": ("time32", ("ms",), {}),
            "time(us)": ("time64", ("us",), {}),
            "time(ns)": ("time64", ("ns",), {}),
            "timestamp_micros": ("timestamp", ("us",), {"tz": None}),
            "timestamp(ns,America/Chicago)": (
                "timestamp", ("ns",), {"tz": "America/Chicago"}
            ),
            "duration(ms)": ("duration", ("ms",), {}),
        }
        for name, result in expected.items():
            with self.subTest(name=name):
                self.assertEqual(arrow_temporal_type(name, pa), result)
        self.assertIsNone(arrow_temporal_type("int64", pa))


class TemporalPublicBoundaryTests(unittest.TestCase):
    def test_declared_object_input_preserves_units_zone_case_and_exact_ticks(self):
        import datetime as dt
        from shardloom import types
        from shardloom._input_schema import normalize_schema, encode_cell, temporal_input_value
        from shardloom.query import _normalize_cast_dtype

        name = "timestamp(ns,America/New_York)"
        self.assertEqual(_normalize_cast_dtype("TIMESTAMP( NS, 'America/New_York' )"), name)
        fields = normalize_schema({"t": "TIMESTAMP( NS, America/New_York )"})
        self.assertEqual(fields[0][1].name, name)
        self.assertEqual(encode_cell(fields[0][1], -1), "-1")
        self.assertEqual(types.timestamp("ns", "America/New_York").label, name)
        self.assertEqual(types.timestamp("us").label, "timestamp_micros")
        self.assertEqual(types.time("ms").label, "time(ms)")
        self.assertEqual(types.duration("ns").label, "duration(ns)")
        self.assertEqual(temporal_input_value("date64", dt.date(1969, 12, 31)), -86_400_000)
        self.assertEqual(temporal_input_value("duration(ns)", dt.timedelta(microseconds=-1)), -1000)
        self.assertEqual(temporal_input_value("time(ns)", dt.time(1, 2, 3, 456789)), 3_723_456_789_000)
        aware = dt.datetime(1970, 1, 1, 5, 30, tzinfo=dt.timezone(dt.timedelta(hours=5, minutes=30)))
        self.assertEqual(temporal_input_value(name, aware), 0)
        for dtype, value in [(name, dt.datetime(1970, 1, 1)), ("timestamp(ns)", aware),
                             ("duration(ms)", dt.timedelta(microseconds=1)),
                             ("time(s)", dt.time(0, 0, 0, 1)), ("date64", 1)]:
            with self.subTest(dtype=dtype, value=value), self.assertRaises((ValueError, TypeError)):
                temporal_input_value(dtype, value)

    def test_result_schema_roundtrip_and_python_objects_never_lose_precision(self):
        import datetime as dt
        import json
        from shardloom._input_schema import normalize_schema, _wire_type
        from shardloom._result_schema import ResultType, schema_fields, python_rows

        fields = normalize_schema({"ns": "timestamp(ns)", "zoned": "timestamp(ns,America/New_York)",
                                   "elapsed": "duration(ns)", "day": "date64", "tod": "time(us)"})
        wire = _wire_type(ResultType("struct", False, fields))
        self.assertEqual(schema_fields(json.dumps(wire), "vortex.dtype.serde.v1"), fields)
        row = {"ns": -1, "zoned": 0, "elapsed": -1000, "day": -86_400_000, "tod": 3_600_000_001}
        self.assertEqual(python_rows([row], fields, temporal_objects=False), [row])
        self.assertEqual(python_rows([row], fields), [{
            "ns": -1, "zoned": 0, "elapsed": dt.timedelta(microseconds=-1),
            "day": dt.date(1969, 12, 31), "tod": dt.time(1, 0, 0, 1),
        }])


def _test_ticks_per_second(name: str) -> int:
    return {"s": 1, "ms": 1_000, "us": 1_000_000, "ns": 1_000_000_000}[
        temporal_parts(name)[1]
    ]


_I64_MIN_TEST = -(1 << 63)
_I64_MAX_TEST = (1 << 63) - 1


if __name__ == "__main__":
    unittest.main()
