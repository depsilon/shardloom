"""GeoArrow declarations and explicit JSON boundaries; no geometry execution."""

import copy
import json
import unittest

from shardloom import _spatial, functions as f, types as t
from shardloom._input_schema import _wire_type, encode_cell, normalize_schema
from shardloom._result_schema import schema_fields
from shardloom.errors import ShardLoomProtocolError


def decode(wire):
    return schema_fields(json.dumps({"Struct": [{"names": ["g"], "dtypes": [wire]}, False]}),
                         "vortex.dtype.serde.v1")[0][1]


class SpatialSchemaTests(unittest.TestCase):
    def test_geometry_identity_metadata_nullability_and_nested_wire_roundtrip(self):
        metadata = {"crs": {"name": "未知 Ω", "axis": ["x", "y"]}, "crs_type": "projjson"}
        for constructor in (t.point, t.linestring, t.polygon):
            for nullable in (False, True):
                dtype = constructor(metadata=metadata, nullable=nullable)
                self.assertEqual(decode(_wire_type(dtype)), dtype)
                self.assertEqual(normalize_schema({"g": dtype}), (("g", dtype),))
                self.assertEqual(dtype.parameters[0].nullable, nullable)
                self.assertEqual(json.loads(dtype.parameters[1]), metadata)
                nested = t.struct({"one": dtype, "many": t.list_(dtype)})
                self.assertEqual(decode(_wire_type(nested)), nested)
        retained = t.point(metadata=metadata)
        metadata["crs"]["name"] = "changed"
        self.assertEqual(json.loads(retained.parameters[1])["crs"]["name"], "未知 Ω")

    def test_spatial_metadata_refuses_ambiguity_and_unrecognized_interpretation(self):
        for metadata in ({"crs": None}, {"crs": ""}, {"crs_type": "projjson"},
                         {"crs": "x", "crs_type": "projjson"}, {"crs": {}, "crs_type": "srid"},
                         {"crs": "x", "crs_type": []}, {"edges": []}, {"edges": "planar"},
                         {"edges": None}, {"dimensions": 3}):
            with self.subTest(metadata=metadata), self.assertRaises(ValueError):
                t.point(metadata=metadata)
        for raw in ('[]', 'null', '{"crs":"a","crs":"b"}',
                    '{"crs":{"x":1,"x":2}}', '{"crs":{"x":NaN}}'):
            with self.subTest(raw=raw), self.assertRaises(ValueError):
                _spatial.decode_metadata(raw)
        for edge in ("spherical", "vincenty", "thomas", "andoyer", "karney"):
            self.assertEqual(json.loads(t.polygon(metadata={"edges": edge}).parameters[1]), {"edges": edge})
        with self.assertRaisesRegex(ValueError, "8 MiB"):
            t.point(metadata={"crs": "x" * _spatial.MAX_METADATA_BYTES})

    def test_geometry_json_requires_exact_finite_coordinates_and_closed_rings(self):
        point = {"x": -0.0, "y": 1.5}
        ring = [{"x": x, "y": y} for x, y in ((0, 0), (2, 0), (0, 2), (0, 0))]
        for dtype, values in ((t.point(), [point, None]),
                              (t.linestring(), [[point, {"x": 3.0, "y": 4.0}], [], None]),
                              (t.polygon(), [[ring], [], None])):
            for value in values:
                encoded = encode_cell(dtype, value)
                self.assertEqual(None if encoded is None else json.loads(encoded), value)
        self.assertIn('-0.0', encode_cell(t.point(), point))
        # Exact powers of two remain valid Float64 coordinates beyond integer widths.
        self.assertEqual(json.loads(encode_cell(t.point(), {"x": 1 << 64, "y": 1})),
                         {"x": 1 << 64, "y": 1})
        for value in ({"x": True, "y": 0}, {"x": None, "y": 0},
                      {"x": (1 << 53) + 1, "y": 1}, {"x": float("inf"), "y": 0},
                      {"x": float("nan"), "y": float("nan")}, {"x": 1, "y": 2, "z": 3}):
            with self.subTest(value=value), self.assertRaises((TypeError, ValueError)):
                encode_cell(t.point(), value)
        for dtype, value in ((t.linestring(), [point]), (t.linestring(), [point, None]),
                              (t.polygon(), [[]]), (t.polygon(), [ring[:-1]]),
                              (t.polygon(), [ring[:-1] + [point]])):
            with self.subTest(dtype=dtype, value=value), self.assertRaises((TypeError, ValueError)):
                encode_cell(dtype, value)

    def test_geometry_wire_rejects_wrong_dimensions_types_and_corrupt_metadata(self):
        valid = _wire_type(t.point())
        for raw in ([255], [True], list(b'{"edges":"unknown"}'), list(b'[]')):
            wire = copy.deepcopy(valid)
            wire["Extension"]["metadata"] = raw
            with self.assertRaises(ShardLoomProtocolError):
                decode(wire)
        for storage in (t.float64(), t.fixed_size_list(t.float64(), 2),
                        t.struct({"y": t.float64(), "x": t.float64()}),
                        t.struct({"x": t.float32(), "y": t.float64()}),
                        t.struct({"x": t.float64(), "y": t.float64(), "z": t.float64()})):
            with self.subTest(storage=storage), self.assertRaises(ValueError):
                normalize_schema({"g": {"type": "point", "storage": storage}})

    def test_public_functions_only_declare_shared_native_expressions(self):
        unary = ("st_x", "st_y", "st_isempty", "st_npoints", "st_xmin", "st_ymin",
                 "st_xmax", "st_ymax", "st_length")
        for name in unary:
            self.assertIn(name.upper(), str(getattr(f, name)("g")))
        for name in ("st_distance", "st_contains", "st_covers"):
            self.assertIn(name.upper(), str(getattr(f, name)("g", "p")))
        self.assertIn("ST_DWITHIN", str(f.st_dwithin("g", "p", 5.0)))
        self.assertIn("ST_POINT", str(f.st_point(3, None)))
        self.assertIn("ST_EMPTYPOINT", str(f.st_emptypoint()))


if __name__ == "__main__":
    unittest.main()
