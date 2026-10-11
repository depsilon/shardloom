"""Integration coverage for the typed native expression and relation API."""

from __future__ import annotations

import importlib
import importlib.util
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch


_REQUIRE_NATIVE = os.environ.get("SHARDLOOM_REQUIRE_NATIVE_PYTHON") == "1"


def _load_native():
    try:
        importlib.import_module("shardloom")
    except ModuleNotFoundError as error:
        if error.name != "shardloom":
            raise
        if _REQUIRE_NATIVE:
            raise AssertionError("optional shardloom Python package is not installed") from error
        raise unittest.SkipTest("optional shardloom Python package is not installed") from error
    library = os.environ.get("SHARDLOOM_NATIVE_TEST_LIBRARY")
    if library:
        path = Path(library)
        if not path.is_absolute() or not path.is_file():
            raise AssertionError("SHARDLOOM_NATIVE_TEST_LIBRARY must name an existing absolute file")
        spec = importlib.util.spec_from_file_location("shardloom._native", path)
        if spec is None or spec.loader is None:
            raise AssertionError(f"cannot load native binding from {path}")
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)
        return module
    try:
        return importlib.import_module("shardloom._native")
    except ModuleNotFoundError as error:
        if error.name != "shardloom._native":
            raise
        if _REQUIRE_NATIVE:
            raise AssertionError("optional shardloom._native extension is not installed") from error
        raise unittest.SkipTest("optional shardloom._native extension is not installed") from error


class NativeExpressionBindingTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.native = _load_native()
        try:
            import pyarrow as pa
        except ImportError as error:
            if _REQUIRE_NATIVE:
                raise AssertionError("optional PyArrow dependency is not installed") from error
            raise unittest.SkipTest("optional PyArrow dependency is not installed") from error
        cls.pa = pa

    def setUp(self):
        process = patch(
            "shardloom.client.subprocess.run", side_effect=AssertionError("process launch")
        )
        worker = patch(
            "shardloom.client.subprocess.Popen", side_effect=AssertionError("worker launch")
        )
        process.start()
        worker.start()
        self.addCleanup(worker.stop)
        self.addCleanup(process.stop)

    def session(self, memory_bytes=128 << 20, parallelism=2):
        session = self.native.Session(self.native.ExecutionResources(memory_bytes, parallelism))
        self.addCleanup(session.close)
        return session

    def arrow_rows(self, result):
        return self.pa.RecordBatchReader.from_stream(result).read_all().to_pylist()

    def test_range_filter_project_reuse_vortex_roundtrip_and_stream(self):
        native = self.native
        expr = native.Expression
        session = self.session(512 << 20, 2)
        source = session.from_range("value", 0, 1, 65_537)
        relation = (
            native.Relation.scan("memory://values")
            .filter((expr.column("value") % expr.literal(2)) == expr.literal(0))
            .project(
                [
                    ("value", expr.column("value")),
                    ("plus", expr.column("value") + expr.literal(7)),
                ]
            )
        )
        plan = session.plan(relation, sources=[("memory://values", source)])
        expected = [
            {"value": value, "plus": value + 7}
            for value in range(0, 65_537, 2)
        ]
        with (
            patch("shardloom.client.subprocess.run", side_effect=AssertionError("process launch")),
            patch("shardloom.client.subprocess.Popen", side_effect=AssertionError("worker launch")),
        ):
            first = plan.collect()
            self.assertEqual(first.report["resident_relational_declaration_reused"], "false")
            self.assertEqual(self.arrow_rows(first), expected)
            second = plan.collect()
            self.assertEqual(second.report["resident_relational_declaration_reused"], "true")
            self.assertEqual(self.arrow_rows(second), expected)

            with tempfile.TemporaryDirectory() as directory:
                path = Path(directory) / "native-expression.vortex"
                plan.write(path, "write_vortex")
                read_plan = session.plan(native.Relation.scan(str(path)))
                self.assertEqual(self.arrow_rows(read_plan.collect()), expected)
                streamed = read_plan.iter_batches()
                stream_rows = []
                for batch in streamed:
                    stream_rows.extend(self.arrow_rows(batch))
                self.assertIsNotNone(streamed.report)
                self.assertEqual(stream_rows, expected)

        # Result buffers carry their own Vortex ownership beyond the plan/session.
        session.close()
        self.assertEqual(self.arrow_rows(first), expected)

    def test_exact_and_rich_literals_list_struct_call_and_cast(self):
        expr = self.native.Expression
        session = self.session()
        source = session.from_range("value", 0, 1, 1)
        relation = self.native.Relation.scan("memory://one").project(
            [
                ("value", expr.column("value")),
                ("uint_max", expr.uint64((1 << 64) - 1)),
                ("binary", expr.literal(b"\x00\xff")),
                ("decimal", expr.decimal128(12345, 8, 2)),
                ("day", expr.date32(-1)),
                ("day_min", expr.date32(-(1 << 31))),
                ("day_max", expr.date32((1 << 31) - 1)),
                ("instant", expr.timestamp_micros((1 << 63) - 1)),
                ("instant_min", expr.timestamp_micros(-(1 << 63))),
                ("list", expr.list([expr.literal(1), expr.literal(2)])),
                ("record", expr.struct_([("left", expr.literal("x")), ("right", expr.literal(9))])),
                ("upper", expr.call("upper", [expr.literal("native")])),
                ("text", expr.column("value").cast("utf8")),
            ]
        )
        plan = session.plan(relation, sources=[("memory://one", source)])
        with (
            patch("shardloom.client.subprocess.run", side_effect=AssertionError("process launch")),
            patch("shardloom.client.subprocess.Popen", side_effect=AssertionError("worker launch")),
        ):
            result = plan.collect()
            batch = self.pa.RecordBatchReader.from_stream(result).read_next_batch()
            self.assertEqual(batch.column("value").to_pylist(), [0])
            self.assertEqual(batch.column("uint_max").to_pylist(), [(1 << 64) - 1])
            self.assertEqual(batch.column("binary").to_pylist(), [b"\x00\xff"])
            self.assertEqual(str(batch.column("decimal")[0].as_py()), "123.45")
            self.assertEqual(batch.column("day").cast(self.pa.int32()).to_pylist(), [-1])
            self.assertEqual(batch.column("day_min").cast(self.pa.int32()).to_pylist(), [-(1 << 31)])
            self.assertEqual(batch.column("day_max").cast(self.pa.int32()).to_pylist(), [(1 << 31) - 1])
            self.assertEqual(
                batch.column("instant").cast(self.pa.int64()).to_pylist(), [(1 << 63) - 1]
            )
            self.assertEqual(batch.column("instant_min").cast(self.pa.int64()).to_pylist(), [-(1 << 63)])
            self.assertEqual(batch.column("list").to_pylist(), [[1, 2]])
            self.assertEqual(batch.column("record").to_pylist(), [{"left": "x", "right": 9}])
            self.assertEqual(batch.column("upper").to_pylist(), ["NATIVE"])
            self.assertEqual(batch.column("text").to_pylist(), ["0"])

    def test_typed_set_join_aggregate_sort_matches_native_sql(self):
        native = self.native
        expr = native.Expression
        session = self.session()
        left = session.from_rows(
            [{"key": 1, "score": 10}, {"key": 2, "score": 20}, {"key": 3, "score": 30}]
        )
        right = session.from_rows(
            [{"key": 2, "label": "b"}, {"key": 3, "label": "c"}, {"key": 4, "label": "d"}]
        )
        left_scan = native.Relation.scan("memory://left")
        right_scan = native.Relation.scan("memory://right")
        joined = left_scan.join(
            right_scan,
            [("key", "key")],
            [("left", "key", "key"), ("left", "score", "score"), ("right", "label", "label")],
        )
        relation = joined.aggregate(
            ["label"], [("sum", "score", "total"), ("count", None, "rows")]
        ).sort([("label", False, None)])
        sources = [("memory://left", left), ("memory://right", right)]
        plan = session.plan(relation, sources=sources)

        set_relation = native.Relation.scan("memory://left").project(
            [("key", expr.column("key"))]
        ).set_operation(
            native.Relation.scan("memory://right").project([("key", expr.column("key"))])
        )
        set_result = self.arrow_rows(session.plan(set_relation, sources=sources).collect())
        self.assertEqual(set_result, [{"key": 1}, {"key": 2}, {"key": 3}, {"key": 2}, {"key": 3}, {"key": 4}])
        expected = [{"label": "b", "total": 20, "rows": 1}, {"label": "c", "total": 30, "rows": 1}]
        with (
            patch("shardloom.client.subprocess.run", side_effect=AssertionError("process launch")),
            patch("shardloom.client.subprocess.Popen", side_effect=AssertionError("worker launch")),
        ):
            self.assertEqual(self.arrow_rows(plan.collect()), expected)
            sql = session.sql(
                "SELECT r.label AS label, SUM(l.score) AS total, COUNT(*) AS rows "
                "FROM 'memory://left' AS l INNER JOIN 'memory://right' AS r "
                "ON l.key = r.key GROUP BY r.label ORDER BY r.label",
                sources=sources,
            )
            self.assertEqual(self.arrow_rows(sql.collect()), expected)

    def test_balanced_large_relation_tree_and_projection_width(self):
        native = self.native
        session = self.session(512 << 20, 2)
        source = session.from_range("value", 0, 1, 1)
        leaves = [native.Relation.scan("memory://single") for _ in range(128)]
        while len(leaves) > 1:
            leaves = [leaves[index].set_operation(leaves[index + 1]) for index in range(0, len(leaves), 2)]
        repeated = session.plan(leaves[0], sources=[("memory://single", source)])
        self.assertEqual(self.arrow_rows(repeated.collect()), [{"value": 0}] * 128)

        wide_source = session.from_range("value", 0, 1, 1)
        expr = native.Expression
        projection = native.Relation.scan("memory://wide").project(
            [(f"column_{index}", expr.literal(index)) for index in range(4097)]
        )
        wide_result = session.plan(projection, sources=[("memory://wide", wide_source)]).collect()
        wide_row = self.arrow_rows(wide_result)[0]
        self.assertEqual(
            wide_row, {f"column_{index}": index for index in range(4097)}
        )

    def test_preflight_failures_are_side_effect_free_and_same_plan_can_retry(self):
        native = self.native
        expr = native.Expression
        with self.assertRaisesRegex(ValueError, "recursive scalar expression"):
            nested = expr.literal(0)
            for _ in range(25):
                nested = nested + expr.literal(1)

        relation = native.Relation.scan("memory://retry").project(
            [("value", expr.column("value"))]
        )
        session = native.Session()
        resources = native.ExecutionResources(64 << 20, 1)
        source = session.from_range("value", 0, 1, 3, resources=resources)
        plan = session.plan(relation, sources=[("memory://retry", source)])
        with self.assertRaisesRegex(ValueError, "SL_CONFIGURATION_ERROR.*resources"):
            plan.collect()
        with self.assertRaisesRegex(RuntimeError, "deadline|cancel"):
            plan.collect(resources=resources, timeout=0)
        with self.assertRaises(RuntimeError):
            plan.collect(resources=native.ExecutionResources(1, 1))
        self.assertEqual(self.arrow_rows(plan.collect(resources=resources)), [
            {"value": 0}, {"value": 1}, {"value": 2}
        ])

        # Unknown functions fail before destination creation; the declaration
        # itself remained inert and did not open its memory source.
        invalid = native.Relation.scan("memory://invalid").project(
            [("bad", expr.call("not_a_native_function", [expr.column("value")]))]
        )
        invalid_plan = session.plan(invalid, sources=[("memory://invalid", source)])
        destination = Path(tempfile.gettempdir()) / f"absent-native-expression-{os.getpid()}.vortex"
        self.assertFalse(destination.exists())
        with self.assertRaisesRegex(RuntimeError, "not_a_native_function"):
            invalid_plan.write(destination, "write_vortex", resources=resources)
        self.assertFalse(destination.exists())
        invalid_schema = native.Relation.scan("memory://invalid-schema").project(
            [("missing", expr.column("missing_column"))]
        )
        with self.assertRaises(RuntimeError):
            session.plan(
                invalid_schema, sources=[("memory://invalid-schema", source)]
            ).collect(resources=resources)
        self.assertFalse(destination.exists())

        with self.assertRaises(ValueError):
            recursive = native.Relation.scan("memory://recursive")
            for _ in range(25):
                recursive = recursive.limit(1)
        session.close()

    def test_directory_membership_rebinds_plan_after_adding_a_vortex_part(self):
        native = self.native
        session = self.session(128 << 20, 2)
        with tempfile.TemporaryDirectory() as directory:
            parts = Path(directory) / "parts"
            parts.mkdir()
            for name, start, rows in (("a.vortex", 0, 2),):
                source = session.from_range("value", start, 1, rows)
                session.plan(native.Relation.scan("memory://part"), sources=[("memory://part", source)]).write(
                    parts / name, "write_vortex"
                )
            plan = session.plan(
                native.Relation.scan(str(parts)), files=[(str(parts), "vortex", None)]
            )
            self.assertEqual(self.arrow_rows(plan.collect()), [{"value": 0}, {"value": 1}])
            source = session.from_range("value", 2, 1, 3)
            session.plan(native.Relation.scan("memory://part"), sources=[("memory://part", source)]).write(
                parts / "b.vortex", "write_vortex"
            )
            changed = plan.collect()
            self.assertEqual(self.arrow_rows(changed), [{"value": 0}, {"value": 1}, {"value": 2}, {"value": 3}, {"value": 4}])
            self.assertEqual(changed.report["resident_relational_declaration_reused"], "false")

            manifest = Path(directory) / "parts.json"
            manifest.write_text(
                json.dumps({"inputs": ["parts/a.vortex"]}), encoding="utf-8"
            )
            manifest_plan = session.plan(
                native.Relation.scan(str(manifest)),
                files=[(str(manifest), "vortex", None)],
            )
            self.assertEqual(
                self.arrow_rows(manifest_plan.collect()), [{"value": 0}, {"value": 1}]
            )
            manifest.write_text(
                json.dumps({"inputs": ["parts/b.vortex", "parts/a.vortex"]}),
                encoding="utf-8",
            )
            reordered = manifest_plan.collect()
            self.assertEqual(
                self.arrow_rows(reordered),
                [{"value": 2}, {"value": 3}, {"value": 4}, {"value": 0}, {"value": 1}],
            )
            self.assertEqual(
                reordered.report["resident_relational_declaration_reused"], "false"
            )

    def test_window_analytics_scalar_subquery_and_public_aliases(self):
        import shardloom.native as public

        native = self.native
        expr = native.Expression
        session = self.session()
        source = session.from_range("value", 0, 1, 4)
        scan = public.Relation.scan("memory://window")

        # The module-level short names are direct aliases for the typed IR
        # constructors, and compose into the same native relation declaration.
        self.assertIs(public.Expression, native.Expression)
        alias_filter = scan.filter(public.col("value") >= public.lit(0))

        frame = public.WindowFrame(
            "rows",
            ("preceding", public.FrameOffset.number(expr.literal(1))),
            ("current_row", None),
        )
        windows = [
            public.WindowExpression(
                "sum", "rolling", source_column="value",
                order_by=[("value", False, None)], frame=frame,
            ),
            public.WindowExpression(
                "row_number", "rank", order_by=[("value", False, None)]
            ),
            public.WindowExpression(
                "lag", "previous", source_column="value", argument=1,
                order_by=[("value", False, None)],
            ),
        ]
        window_plan = session.plan(
            scan.window(["value"], windows), sources=[("memory://window", source)]
        )
        expected = [
            {"value": 0, "rolling": 0, "rank": 1, "previous": None},
            {"value": 1, "rolling": 1, "rank": 2, "previous": 0},
            {"value": 2, "rolling": 3, "rank": 3, "previous": 1},
            {"value": 3, "rolling": 5, "rank": 4, "previous": 2},
        ]
        self.assertEqual(
            self.arrow_rows(window_plan.collect()), expected
        )
        self.assertEqual(
            self.arrow_rows(
                session.plan(alias_filter, sources=[("memory://window", source)]).collect()
            ),
            [{"value": 0}, {"value": 1}, {"value": 2}, {"value": 3}],
        )

        scalar = public.Relation.scan("memory://window").aggregate(
            [], [("sum", "value", "total")]
        )
        with_subquery = public.Relation.scan("memory://window").subquery(
            scalar, "total"
        )
        self.assertEqual(
            self.arrow_rows(
                session.plan(
                    with_subquery, sources=[("memory://window", source)]
                ).collect()
            ),
            [
                {"value": 0, "total": 6},
                {"value": 1, "total": 6},
                {"value": 2, "total": 6},
                {"value": 3, "total": 6},
            ],
        )


if __name__ == "__main__":
    unittest.main()
