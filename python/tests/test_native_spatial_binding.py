"""Complete geometry values and planar operations through public native routes."""

from __future__ import annotations

import gc
import hashlib
import json
import os
from pathlib import Path
import struct
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

if __package__:
    from .test_native_binding import _load_native
    from .test_native_vector_representations_binding import _schema_shape
else:
    from test_native_binding import _load_native
    from test_native_vector_representations_binding import _schema_shape


GRANT = 128 << 20


def _point(x, y):
    return {"x": float(x), "y": float(y)}


def _empty():
    return _point(float("nan"), float("nan"))


def _ring(points):
    return [_point(x, y) for x, y in points]


def _field(pa, name, kind, metadata=None, *, nullable=True, coordinate_nullable=False):
    coordinate = pa.struct([pa.field("x", pa.float64(), nullable=coordinate_nullable),
                            pa.field("y", pa.float64(), nullable=coordinate_nullable)])
    storage = coordinate
    if kind in {"linestring", "polygon"}:
        storage = pa.list_(pa.field("vertices", storage, nullable=False))
    if kind == "polygon":
        storage = pa.list_(pa.field("rings", storage, nullable=False))
    return pa.field(name, storage, nullable=nullable, metadata={
        b"ARROW:extension:name": ("geoarrow." + kind).encode(),
        b"ARROW:extension:metadata": json.dumps(metadata or {}, ensure_ascii=False,
            sort_keys=True, separators=(",", ":")).encode()})


def _fixture(repetitions=1, *, padding=0):
    import pyarrow as pa
    polygon = [_ring(((0, 0), (10, 0), (10, 10), (0, 10), (0, 0))),
               _ring(((3, 3), (7, 3), (7, 7), (3, 7), (3, 3)))]
    line = _ring(((0, 0), (3, 4), (6, 0)))
    points = [_point(1, 1), _point(3, 4), _empty(), None]
    others = [_point(4, 5), _point(6, 8), _point(0, 0), None]
    lines, polygons = [line, [], [], None], [polygon, polygon, [], None]
    detail = pa.struct([_field(pa, "place", "point", {"crs": "未知 Ω", "edges": "spherical"})])
    schema = pa.schema([pa.field("id", pa.int64()), _field(pa, "g", "point"),
        _field(pa, "other", "point"), _field(pa, "line", "linestring"),
        _field(pa, "poly", "polygon"), pa.field("detail", detail), pa.field("padding", pa.binary())])
    rows = [{"id": index, "g": points[index % 4], "other": others[index % 4],
             "line": lines[index % 4], "poly": polygons[index % 4],
             "detail": None if index % 4 == 3 else {"place": points[index % 4]},
             "padding": bytes([index % 251]) * padding} for index in range(4 * repetitions)]
    return pa.Table.from_pylist(rows, schema=schema)


def _digest(rows):
    def exact(value):
        if isinstance(value, float):
            return {"float64_bits": struct.pack("<d", value).hex()}
        if isinstance(value, bytes):
            return {"bytes": value.hex()}
        if isinstance(value, dict):
            return {key: exact(item) for key, item in value.items()}
        if isinstance(value, (list, tuple)):
            return [exact(item) for item in value]
        return value
    return hashlib.sha256(json.dumps(exact(rows), sort_keys=True, separators=(",", ":"),
                                     ensure_ascii=False, allow_nan=False).encode()).hexdigest()


def _projections(f):
    return [f.st_point(3, 4).alias("made"), f.st_emptypoint().alias("empty"),
        f.st_x("g").alias("x"), f.st_y("g").alias("y"), f.st_isempty("g").alias("isempty"),
        f.st_npoints("poly").alias("npoints"), f.st_xmin("poly").alias("xmin"),
        f.st_ymin("poly").alias("ymin"), f.st_xmax("poly").alias("xmax"),
        f.st_ymax("poly").alias("ymax"), f.st_distance("g", "other").alias("distance"),
        f.st_dwithin("g", "other", 5.0).alias("within"), f.st_length("line").alias("length"),
        f.st_contains("poly", "g").alias("contains"), f.st_covers("poly", "g").alias("covers")]


def _expected(table):
    # The four geometric cases are fixed independently of engine output.
    values = [
        dict(x=1.0, y=1.0, isempty=False, npoints=10, xmin=0.0, ymin=0.0, xmax=10.0,
             ymax=10.0, distance=5.0, within=True, length=10.0, contains=True, covers=True),
        dict(x=3.0, y=4.0, isempty=False, npoints=10, xmin=0.0, ymin=0.0, xmax=10.0,
             ymax=10.0, distance=5.0, within=True, length=0.0, contains=False, covers=True),
        dict(x=None, y=None, isempty=True, npoints=0, xmin=None, ymin=None, xmax=None,
             ymax=None, distance=None, within=False, length=0.0, contains=False, covers=False),
        dict(x=None, y=None, isempty=None, npoints=None, xmin=None, ymin=None, xmax=None,
             ymax=None, distance=None, within=None, length=None, contains=None, covers=None),
    ]
    return [{**{name: row[name] for name in ("id", "g", "line", "poly", "detail")},
             "made": _point(3, 4), "empty": _empty(), **values[row["id"] % 4]}
            for row in table.to_pylist()]


def _reopen(paths):
    _load_native()
    import pyarrow as pa
    import pyarrow.parquet as pq
    import shardloom as sl
    reports = []
    with sl.context(transport="native", memory_bytes=GRANT, max_parallelism=1) as context:
        for raw in paths:
            source = context.read_vortex(raw)
            result = source.collect(check=True)
            native = result.to_arrow()
            tables = {"vortex": native}
            for format_name in ("arrow_ipc", "parquet"):
                destination = Path(raw).with_suffix("." + format_name)
                getattr(source, "write_" + format_name)(destination, check=True)
                tables["native_" + format_name] = getattr(context, "read_" + format_name)(destination).collect(check=True).to_arrow()
                if format_name == "parquet":
                    tables["independent_parquet"] = pq.read_table(destination)
                else:
                    with pa.ipc.open_file(destination) as reader:
                        tables["independent_arrow"] = reader.read_all()
            assert not result.envelope.field_bool("fallback_attempted")
            assert not result.envelope.field_bool("external_engine_invoked")
            reports.append({"rows": native.num_rows,
                "digests": {name: _digest(table.to_pylist()) for name, table in tables.items()},
                "schemas_equal": {name: _schema_shape(table.schema) == _schema_shape(native.schema)
                                  for name, table in tables.items()}})
    return reports


class NativeSpatialBindingTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.native = _load_native()

    def context(self, memory=GRANT, policy="strict"):
        import shardloom as sl
        context = sl.context(transport="native", memory_bytes=memory, max_parallelism=1, float_policy=policy)
        self.addCleanup(context.close)
        return context

    def assert_values(self, result, expected):
        table = result.to_arrow()
        self.assertEqual(_digest(table.to_pylist()), _digest(expected))
        self.assertEqual(_digest(result.python_objects), _digest(expected))
        self.assertFalse(result.envelope.field_bool("fallback_attempted"))
        self.assertFalse(result.envelope.field_bool("external_engine_invoked"))
        return table

    def test_spatial_typed_rows_arrow_slices_empty_null_and_retained_results(self):
        from shardloom import types as t
        context = self.context()
        rows = [{"p": _point(-0.0, 2), "l": [_point(0, 0), _point(3, 4)],
                 "g": [_ring(((0, 0), (1, 0), (0, 1), (0, 0)))]},
                {"p": None, "l": [], "g": []}, {"p": None, "l": None, "g": None}]
        metadata = {"crs": {"name": "本地 Ω", "axis": [{"name": "x"}, {"name": "y"}]}, "crs_type": "projjson"}
        schema = {"p": t.point(metadata=metadata), "l": t.linestring(metadata=metadata),
                  "g": t.polygon(metadata=metadata)}
        with patch("shardloom._input_schema.encode_cell", side_effect=AssertionError("JSON row execution")):
            result = context.from_rows(rows, schema=schema).collect(check=True)
        table = self.assert_values(result, rows)
        self.assertEqual(result.result_schema, tuple(schema.items()))
        for selected in (table.slice(1, 2), table.slice(0, 0), table.slice(2, 1)):
            restored = context.from_arrow_table(selected).collect(check=True)
            actual = self.assert_values(restored, selected.to_pylist())
            self.assertEqual(_schema_shape(actual.schema), _schema_shape(table.schema))
        context.close()
        del result, restored
        self.assertEqual(_digest(table.to_pylist()), _digest(rows))

    def test_spatial_all_functions_sql_dataframe_typed_workflow_cli_and_fresh_reopen(self):
        import shardloom as sl
        from shardloom import functions as f
        context = self.context()
        table = _fixture()
        expected = _expected(table)
        projection = ["id", "g", "line", "poly", "detail", *_projections(f)]
        with tempfile.TemporaryDirectory(prefix="shardloom-spatial-routes-") as directory:
            root, outputs = Path(directory), []
            source = root / "input.vortex"
            context.from_arrow_table(table).write_vortex(source, check=True)
            statement = "SELECT " + ",".join(map(str, projection)) + " FROM '" + str(source).replace("'", "''") + "' ORDER BY id"
            frame = context.read_vortex(source).select(*projection).sort("id")
            self.assert_values(frame.collect(check=True), expected)
            self.assert_values(context.sql(statement).collect(check=True), expected)
            native = self.native.Session(self.native.ExecutionResources(GRANT, 1))
            cli = sl.ShardLoomClient(binary=os.environ.get("SHARDLOOM_SURFACE_TEST_BINARY"),
                transport="subprocess", use_persistent_worker=False, memory_bytes=GRANT, max_parallelism=1)
            e = self.native.Expression
            columns = [(name, e.column(name)) for name in ("id", "g", "line", "poly", "detail")]
            calls = [("made", "st_point", [e.literal(3), e.literal(4)]), ("empty", "st_emptypoint", []),
                ("x", "st_x", [e.column("g")]), ("y", "st_y", [e.column("g")]),
                ("isempty", "st_isempty", [e.column("g")]), ("npoints", "st_npoints", [e.column("poly")]),
                ("xmin", "st_xmin", [e.column("poly")]), ("ymin", "st_ymin", [e.column("poly")]),
                ("xmax", "st_xmax", [e.column("poly")]), ("ymax", "st_ymax", [e.column("poly")]),
                ("distance", "st_distance", [e.column("g"), e.column("other")]),
                ("within", "st_dwithin", [e.column("g"), e.column("other"), e.literal(5.0)]),
                ("length", "st_length", [e.column("line")]),
                ("contains", "st_contains", [e.column("poly"), e.column("g")]),
                ("covers", "st_covers", [e.column("poly"), e.column("g")])]
            relation = self.native.Relation.scan(str(source)).project(
                columns + [(name, e.call(function, args)) for name, function, args in calls]).sort([("id", False, None)])
            files = [(str(source), "vortex", None)]
            arguments = ["sql", "--sql", statement, "--request", "collect", "--bounded", "true",
                "--input", str(source), "--input-format", "vortex", "--memory-bytes", str(GRANT), "--max-parallelism", "1"]
            try:
                for route in ("dataframe", "sql", "typed_plan", "embedded_workflow", "cli_sql"):
                    destination = root / (route + ".vortex")
                    if route == "dataframe":
                        report = frame.write_vortex(destination, check=True)
                        field = report.envelope.field
                    elif route == "cli_sql":
                        report = sl.sql(statement, client=cli, input=str(source), input_format="vortex").write_vortex(destination, check=True)
                        field = report.envelope.field
                    else:
                        plan = (native.sql(statement, files=files) if route == "sql" else
                                native.plan(relation, files=files) if route == "typed_plan" else native.workflow(arguments))
                        with patch("subprocess.Popen", side_effect=AssertionError("external execution")):
                            report = plan.write(destination, "write_vortex")
                        field = report.get
                    self.assertEqual(field("fallback_attempted"), "false", route)
                    self.assertEqual(field("external_engine_invoked"), "false", route)
                    outputs.append(str(destination))
            finally:
                native.close()
                cli.close()
                context.close()
            source.unlink()
            child = subprocess.run([sys.executable, "-B", str(Path(__file__).resolve()), "--reopen", *outputs],
                env=os.environ.copy(), capture_output=True, text=True, timeout=180, check=False)
            self.assertEqual(child.returncode, 0, child.stderr)
            reports = json.loads(child.stdout)
            self.assertEqual(len(reports), 5)
            for report in reports:
                self.assertEqual(report["rows"], 4)
                self.assertEqual(set(report["digests"].values()), {_digest(expected)})
                self.assertTrue(all(report["schemas_equal"].values()), report)

    def test_spatial_ancestor_masks_hide_invalid_coordinates_but_visible_values_fail(self):
        import pyarrow as pa
        context = self.context()
        field = _field(pa, "g", "point", coordinate_nullable=True)
        values = pa.array([_point(-0.0, 1), _point(float("inf"), 0), {"x": None, "y": 1}], type=field.type)
        parent = pa.StructArray.from_arrays([values], fields=[field], mask=pa.array([False, True, True]))
        table = pa.Table.from_arrays([parent], names=["record"])
        self.assert_values(context.from_arrow_table(table).collect(check=True),
                           [{"record": {"g": _point(-0.0, 1)}}, {"record": None}, {"record": None}])
        for selected in (values.slice(1, 1), values.slice(2, 1)):
            with self.assertRaisesRegex(Exception, "finite|nonnull|null"):
                context.from_arrow_table(pa.Table.from_arrays([selected], schema=pa.schema([field]))).collect(check=True)
        # The geometry sentinel does not relax ordinary floating input policy.
        with self.assertRaisesRegex(Exception, "non.finite|finite"):
            context.from_arrow_table(pa.table({"ordinary": [float("nan")]})).collect(check=True)

    def test_spatial_interpretation_topology_radius_and_storage_refusals(self):
        import pyarrow as pa
        from shardloom import types as t
        context = self.context()
        for metadata in ({"edges": "spherical"}, {"crs": "another coordinate system"}):
            source = context.from_rows([{"g": _point(1, 1), "p": _point(0, 0)}],
                schema={"g": t.point(metadata=metadata), "p": t.point()})
            self.assertEqual(source.select("ST_X(g) AS x").collect(check=True).python_objects, ({"x": 1.0},))
            with self.assertRaisesRegex(Exception, "planar|matching CRS"):
                source.select("ST_DISTANCE(g,p) AS distance").schema()
        source = context.from_rows([{"g": _point(0, 0)}], schema={"g": t.point()})
        with self.assertRaisesRegex(Exception, "nonnegative"):
            source.select("ST_DWITHIN(g,g,-1) AS within").collect(check=True)
        with self.assertRaisesRegex(Exception, "geometry kind"):
            source.select("ST_LENGTH(g) AS length").schema()
        bowtie = [_ring(((0, 0), (4, 4), (0, 4), (4, 0), (0, 0)))]
        source = context.from_rows([{"g": bowtie}], schema={"g": t.polygon()})
        self.assertEqual(source.select("ST_XMIN(g) AS x").collect(check=True).python_objects, ({"x": 0.0},))
        with self.assertRaisesRegex(Exception, "simple|intersect"):
            source.select("ST_CONTAINS(g,ST_POINT(100,100)) AS contains").collect(check=True)
        for field in (pa.field("g", pa.list_(pa.float64(), 2), metadata={b"ARROW:extension:name": b"geoarrow.point"}),
                      _field(pa, "g", "point", {"edges": "unknown"})):
            with self.assertRaisesRegex(Exception, "spatial|GeoArrow|geoarrow|extension"):
                context.from_arrow_table(pa.Table.from_arrays([pa.array([], type=field.type)], schema=pa.schema([field]))).collect(check=True)

    def test_spatial_join_spill_stream_cancellation_and_last_owner_release(self):
        import pyarrow as pa
        context = self.context()
        table = _fixture(600, padding=1024)
        frame = context.from_arrow_table(table)
        tags = context.from_arrow_table(pa.table({"id": [0, 0, 2, 2399], "tag": [10, 11, 20, 30]}))
        projection = ["f." + name + " AS " + name for name in table.schema.names] + ["d.tag AS tag"]
        query = frame.join(tags, on="id", how="left").select(*projection).sort("id", "tag", nulls="last")
        expected = [{**row, "tag": tag} for row in table.to_pylist()
                    for tag in ({0: [10, 11], 2: [20], 2399: [30]}.get(row["id"], [None]))]
        session = context.client._native_backend.session
        with tempfile.TemporaryDirectory(prefix="shardloom-spatial-spill-") as directory:
            workspace = Path(directory) / "spill"
            workspace.mkdir()
            spill = {"workspace": str(workspace), "quota_bytes": 64 << 20, "buffer_bytes": 1 << 20}
            result = query.collect(spill=spill, check=True)
            self.assert_values(result, expected)
            self.assertGreater(result.envelope.field_int("relational_spill_runs_written"), 0)
            self.assertTrue(result.envelope.field_bool("relational_spill_owned_cleanup_completed"))
            self.assertEqual(list(workspace.iterdir()), [])
            with query.iter_batches(batch_rows=17, spill=spill) as early:
                retained = next(early).to_arrow()
            self.assertIsNone(early.report)
            self.assertEqual(_digest(retained.to_pylist()), _digest(expected[:17]))
            cancelled = query.iter_batches(batch_rows=17, spill=spill)
            next(cancelled)
            context.client.cancel()
            with self.assertRaisesRegex(Exception, "cancelled"):
                next(cancelled)
            cancelled.close()
            self.assertIsNone(cancelled.report)
            self.assertEqual(list(workspace.iterdir()), [])
            context.close()
            del result, query, tags, frame, early, cancelled
            gc.collect()
            self.assertGreater(session.snapshot()["reserved_bytes"], 0)
            self.assertEqual(_digest(retained.to_pylist()), _digest(expected[:17]))
            del retained
            gc.collect()
            self.assertEqual(session.snapshot()["reserved_bytes"], 0)

    def test_spatial_resource_denial_does_not_invalidate_retained_results(self):
        context = self.context(1 << 20)
        table = _fixture()
        source = context.from_arrow_table(table)
        result = source.collect(check=True)
        session = context.client._native_backend.session
        retained = result.to_arrow()
        before = session.snapshot()["reserved_bytes"]
        with self.assertRaisesRegex(Exception, "reservation denied"):
            context.from_arrow_table(_fixture(128, padding=8192)).collect(check=True)
        gc.collect()
        self.assertEqual(session.snapshot()["reserved_bytes"], before)
        self.assert_values(result, table.to_pylist())
        context.close()
        del source, result
        gc.collect()
        self.assertGreater(session.snapshot()["reserved_bytes"], 0)
        self.assertEqual(_digest(retained.to_pylist()), _digest(table.to_pylist()))
        del retained
        gc.collect()
        self.assertEqual(session.snapshot()["reserved_bytes"], 0)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--reopen":
        print(json.dumps(_reopen(sys.argv[2:]), allow_nan=False))
    else:
        unittest.main()
