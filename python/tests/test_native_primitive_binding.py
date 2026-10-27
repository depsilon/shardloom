"""Extension integration tests for shared native Vortex primitive workflows."""

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


def _unavailable(message: str) -> None:
    if _REQUIRE_NATIVE:
        raise AssertionError(message)
    raise unittest.SkipTest(message)


def _load_native():
    try:
        importlib.import_module("shardloom")
    except ModuleNotFoundError as error:
        if error.name != "shardloom":
            raise
        _unavailable("optional shardloom Python package is not installed")
    library = os.environ.get("SHARDLOOM_NATIVE_TEST_LIBRARY")
    if library:
        path = Path(library)
        if not path.is_absolute() or not path.is_file():
            raise AssertionError(
                "SHARDLOOM_NATIVE_TEST_LIBRARY must name an existing absolute file"
            )
        spec = importlib.util.spec_from_file_location("shardloom._native", path)
        if spec is None or spec.loader is None:
            raise AssertionError(f"cannot load native binding from {path}")
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        try:
            spec.loader.exec_module(module)
        except BaseException:
            sys.modules.pop(spec.name, None)
            raise
        return module
    try:
        return importlib.import_module("shardloom._native")
    except ModuleNotFoundError as error:
        if error.name != "shardloom._native":
            raise
        _unavailable("optional shardloom._native extension is not installed")


class NativePrimitiveBindingTests(unittest.TestCase):
    rows = 65_537
    memory_bytes = 512 << 20
    max_parallelism = 2

    @classmethod
    def setUpClass(cls) -> None:
        cls.native = _load_native()
        try:
            import pyarrow as pa
        except ImportError:
            _unavailable("optional PyArrow dependency is not installed")
        cls.pa = pa
        cls._directory = tempfile.TemporaryDirectory(prefix="shardloom-native-primitives-")
        cls.root = Path(cls._directory.name)
        cls.large_path = cls.root / "shipments.vortex"
        cls._write_fixture(cls.large_path, cls.rows)

    @classmethod
    def tearDownClass(cls) -> None:
        cls._directory.cleanup()

    @classmethod
    def _write_fixture(
        cls,
        path: Path,
        count: int,
        *,
        offset: int = 0,
        descending: bool = True,
    ) -> None:
        keys = list(range(offset, offset + count))
        if descending:
            keys.reverse()
        payloads = [None if key % 11 == 0 else key * 10 for key in keys]
        schema = cls.pa.schema(
            [
                cls.pa.field("shipment_id", cls.pa.int64(), nullable=False),
                cls.pa.field("nullable_payload", cls.pa.int64(), nullable=True),
            ]
        )
        table = cls.pa.Table.from_arrays(
            [cls.pa.array(keys, type=cls.pa.int64()), cls.pa.array(payloads, type=cls.pa.int64())],
            schema=schema,
        )
        resources = cls.native.ExecutionResources(cls.memory_bytes, cls.max_parallelism)
        session = cls.native.Session(resources)
        try:
            source = session.from_arrow(table)
            plan = session.sql(
                "SELECT shipment_id, nullable_payload "
                "FROM 'memory://native-primitive-fixture'",
                sources=[("memory://native-primitive-fixture", source)],
            )
            plan.write(path, "write_vortex")
        finally:
            session.close()

    @classmethod
    def _write_table_fixture(cls, path: Path, table) -> None:
        resources = cls.native.ExecutionResources(cls.memory_bytes, cls.max_parallelism)
        session = cls.native.Session(resources)
        try:
            source = session.from_arrow(table)
            plan = session.sql(
                "SELECT * FROM 'memory://native-primitive-table'",
                sources=[("memory://native-primitive-table", source)],
            )
            plan.write(path, "write_vortex")
        finally:
            session.close()

    def _write_manifest(self, path: Path, parts: list[Path]) -> None:
        path.write_text(
            json.dumps({"inputs": [part.relative_to(self.root).as_posix() for part in parts]}),
            encoding="utf-8",
        )

    def _run_manifest_primitive(self, path: Path, primitive: str, **options):
        return self._run(path, primitive, **options)

    def setUp(self) -> None:
        import shardloom as sl

        self.client = sl.ShardLoomClient(
            transport="native",
            memory_bytes=self.memory_bytes,
            max_parallelism=self.max_parallelism,
        )
        self.addCleanup(self.client.close)

    def _run(self, path: Path, primitive: str, **options):
        execution = self.client.public_workflow_run(
            "cli",
            input_uri=path,
            input_format="vortex",
            requested_output="collect",
            execution_policy="native_vortex",
            materialization_policy="bounded",
            bounded=True,
            memory_bytes=self.memory_bytes,
            max_parallelism=self.max_parallelism,
            vortex_primitive=primitive,
            **options,
        )
        envelope = execution.envelope
        self.assertEqual(envelope.status, "success", envelope.raw)
        self.assertFalse(envelope.fallback.attempted, envelope.raw)
        self.assertFalse(envelope.fallback.allowed, envelope.raw)
        self.assertEqual(envelope.field("python_execution_transport"), "native")
        result = envelope._native_result
        self.assertIsNotNone(result)
        table = self.pa.RecordBatchReader.from_stream(result).read_all()
        return envelope, result, table

    @staticmethod
    def _expected(keys):
        return [
            {
                "shipment_id": key,
                "nullable_payload": None if key % 11 == 0 else key * 10,
            }
            for key in keys
        ]

    def _primitive_args(
        self,
        path: Path,
        primitive: str,
        *,
        memory_bytes: int | None = None,
        max_parallelism: int | None = None,
        options: dict[str, object] | None = None,
    ) -> list[str]:
        memory_bytes = self.memory_bytes if memory_bytes is None else memory_bytes
        max_parallelism = self.max_parallelism if max_parallelism is None else max_parallelism
        args = [
            "cli",
            "--input",
            str(path),
            "--input-format",
            "vortex",
            "--request",
            "collect",
            "--bounded",
            "true",
            "--execution-policy",
            "native_vortex",
            "--vortex-primitive",
            primitive,
            "--memory-bytes",
            str(memory_bytes),
            "--max-parallelism",
            str(max_parallelism),
        ]
        for key, value in (options or {}).items():
            args.extend((key, value))
        return args

    def test_public_native_projection_filter_sort_rewrite_and_counts_keep_complete_values(self):
        full_rows = self._expected(range(self.rows - 1, -1, -1))
        with (
            patch("shardloom.client.subprocess.run", side_effect=AssertionError("process launch")),
            patch("shardloom.client.subprocess.Popen", side_effect=AssertionError("worker launch")),
        ):
            _, projected, projection = self._run(
                self.large_path, "project", vortex_columns=("shipment_id", "nullable_payload")
            )
            self.assertEqual(projection.schema.names, ["shipment_id", "nullable_payload"])
            self.assertEqual(projection.schema.field("shipment_id").type, self.pa.int64())
            self.assertFalse(projection.schema.field("shipment_id").nullable)
            self.assertTrue(projection.schema.field("nullable_payload").nullable)
            self.assertEqual(projection.to_pylist(), full_rows)

            _, filtered, filter_projection = self._run(
                self.large_path,
                "filter_project",
                vortex_predicate="gte:shipment_id:0",
                vortex_columns=("shipment_id", "nullable_payload"),
            )
            self.assertEqual(filter_projection.to_pylist(), full_rows)
            self.assertGreater(filtered.row_count, 65_536)

            order = json.dumps(
                {"order_by": [{"column": "shipment_id", "descending": False}]},
                separators=(",", ":"),
            )
            _, sorted_result, sorted_table = self._run(
                self.large_path,
                "sort_rows",
                vortex_sort_rows=order,
                vortex_source_order_limit=257,
            )
            self.assertEqual(
                sorted_table.to_pylist(),
                self._expected(range(257)),
            )
            del sorted_result

            rewrite = json.dumps(
                {
                    "columns": ["shipment_id", "nullable_payload"],
                    "rewrites": [
                        {
                            "kind": "numeric_scalar_arithmetic",
                            "target_column": "shipment_id",
                            "operator": "+",
                            "operand": {"type": "int64", "value": 0},
                        }
                    ],
                },
                separators=(",", ":"),
            )
            _, rewritten, rewritten_table = self._run(
                self.large_path,
                "expression_project",
                vortex_expression_projection=rewrite,
                vortex_columns=("shipment_id", "nullable_payload"),
            )
            self.assertEqual(rewritten_table.to_pylist(), full_rows)
            del projected, filtered, rewritten

            output = self.root / "rewritten.vortex"
            written = self.client.public_workflow_run(
                "cli",
                input_uri=self.large_path,
                input_format="vortex",
                requested_output="write_vortex",
                output_ref=output,
                execution_policy="native_vortex",
                materialization_policy="bounded",
                bounded=True,
                allow_overwrite=False,
                memory_bytes=self.memory_bytes,
                max_parallelism=self.max_parallelism,
                vortex_primitive="expression_project",
                vortex_expression_projection=rewrite,
                vortex_columns=("shipment_id", "nullable_payload"),
            )
            self.assertEqual(written.envelope.status, "success", written.envelope.raw)
            self.assertTrue(output.is_file())
            _, reloaded, reloaded_table = self._run(
                output, "project", vortex_columns=("shipment_id", "nullable_payload")
            )
            self.assertEqual(reloaded_table.to_pylist(), full_rows)
            del reloaded

            for primitive, predicate in (("count", None), ("count_where", "gte:shipment_id:0")):
                options = {} if predicate is None else {"vortex_predicate": predicate}
                _, count_result, count_table = self._run(self.large_path, primitive, **options)
                self.assertEqual(count_table.schema.names, ["count_all"])
                self.assertEqual(count_table.schema.field("count_all").type, self.pa.uint64())
                self.assertFalse(count_table.schema.field("count_all").nullable)
                self.assertEqual(count_table.to_pylist(), [{"count_all": self.rows}])
                del count_result

    def test_native_sort_plan_stream_has_global_order_and_completion_report(self):
        order = json.dumps(
            {"order_by": [{"column": "shipment_id", "descending": False}]},
            separators=(",", ":"),
        )
        session = self.native.Session(
            self.native.ExecutionResources(self.memory_bytes, self.max_parallelism)
        )
        try:
            plan = session.workflow(
                self._primitive_args(
                    self.large_path,
                    "sort_rows",
                    options={
                        "--vortex-sort-rows": order,
                        "--vortex-source-order-limit": "257",
                    },
                )
            )
            stream = plan.iter_batches()
            with stream:
                tables = [self.pa.RecordBatchReader.from_stream(batch).read_all() for batch in stream]
            self.assertTrue(tables)
            actual = [row for table in tables for row in table.to_pylist()]
            self.assertEqual(actual, self._expected(range(257)))
            report = stream.report
            self.assertIsNotNone(report)
            self.assertEqual(report["resident_native_preparation_reused"], "false")
            self.assertEqual(report["columnar_export"]["python_rows_constructed"], False)
            self.assertEqual(report["native_result_payload"], "retained_native_arrays")
        finally:
            session.close()

    def test_empty_native_projection_rewrite_and_sort_keep_typed_schemas(self):
        empty_path = self.root / "empty.vortex"
        self._write_fixture(empty_path, 0)
        rewrite = json.dumps(
            {
                "columns": ["shipment_id", "nullable_payload"],
                "rewrites": [
                    {
                        "kind": "numeric_scalar_arithmetic",
                        "target_column": "shipment_id",
                        "operator": "+",
                        "operand": {"type": "int64", "value": 0},
                    }
                ],
            },
            separators=(",", ":"),
        )
        with (
            patch("shardloom.client.subprocess.run", side_effect=AssertionError("process launch")),
            patch("shardloom.client.subprocess.Popen", side_effect=AssertionError("worker launch")),
        ):
            for primitive, options in (
                ("project", {"vortex_columns": ("shipment_id", "nullable_payload")}),
                (
                    "expression_project",
                    {
                        "vortex_expression_projection": rewrite,
                        "vortex_columns": ("shipment_id", "nullable_payload"),
                    },
                ),
                (
                    "sort_rows",
                    {
                        "vortex_sort_rows": json.dumps(
                            {"order_by": [{"column": "shipment_id", "descending": False}]}
                        ),
                        "vortex_source_order_limit": 1,
                    },
                ),
            ):
                with self.subTest(primitive=primitive):
                    _, result, table = self._run(empty_path, primitive, **options)
                    self.assertEqual(table.num_rows, 0)
                    self.assertEqual(table.schema.names, ["shipment_id", "nullable_payload"])
                    self.assertEqual(table.schema.field("shipment_id").type, self.pa.int64())
                    self.assertFalse(table.schema.field("shipment_id").nullable)
                    self.assertTrue(table.schema.field("nullable_payload").nullable)
                    del result

    def test_missing_source_admission_and_cancellation_are_side_effect_free_then_recover(self):
        session = self.native.Session(
            self.native.ExecutionResources(self.memory_bytes, self.max_parallelism)
        )
        missing = self.root / "not-created.vortex"
        destination = self.root / "must-not-exist.vortex"
        before = session.snapshot()
        try:
            zero_decode = self._primitive_args(missing, "sort_rows", options={
                "--vortex-sort-rows": '{"order_by":[{"column":"shipment_id"}]}',
                "--vortex-source-order-limit": "1",
                "--materialization-policy": "zero_decode",
            })
            with self.assertRaisesRegex(RuntimeError, "requires admitted materialization"):
                session.workflow(zero_decode)
            self.assertEqual(session.snapshot()["prepared_source_opens"], before["prepared_source_opens"])
            invalid = session.workflow(
                self._primitive_args(
                    missing,
                    "count",
                    memory_bytes=1,
                    max_parallelism=1,
                )
            )
            with self.assertRaises(RuntimeError):
                invalid.collect(resources=self.native.ExecutionResources(1, 1))
            self.assertFalse(destination.exists())
            self.assertEqual(session.snapshot()["prepared_source_opens"], before["prepared_source_opens"])

            cancelled = session.workflow(
                self._primitive_args(missing, "count")
            )
            with self.assertRaises(RuntimeError):
                cancelled.collect(timeout=0)
            self.assertEqual(session.snapshot()["prepared_source_opens"], before["prepared_source_opens"])

            write_args = self._primitive_args(
                missing, "project", options={"--vortex-columns": "shipment_id"}
            )
            write_args[write_args.index("--request") + 1] = "write_vortex"
            write_args.extend(("--output", str(destination)))
            write_plan = session.workflow(write_args)
            with self.assertRaises(RuntimeError):
                write_plan.write(destination, "write_vortex", timeout=0)
            self.assertFalse(destination.exists())

            valid = session.workflow(self._primitive_args(self.large_path, "count"))
            result = valid.collect()
            del valid
            session.close()
            self.assertEqual(
                self.pa.RecordBatchReader.from_stream(result).read_all().to_pylist(),
                [{"count_all": self.rows}],
            )
            del result
        finally:
            session.close()

    def test_vortex_directory_and_explicit_manifest_preserve_part_order_and_values(self):
        parts = self.root / "shipment-parts.vortex"
        parts.mkdir()
        self._write_fixture(parts / "a.vortex", 5, descending=False)
        self._write_fixture(parts / "b.vortex", 7, offset=5, descending=False)
        manifest = self.root / "shipment-parts.vortex-manifest"
        manifest.write_text(
            json.dumps({"inputs": [f"{parts.name}/a.vortex", f"{parts.name}/b.vortex"]}),
            encoding="utf-8",
        )
        expected = self._expected(range(12))
        with (
            patch("shardloom.client.subprocess.run", side_effect=AssertionError("process launch")),
            patch("shardloom.client.subprocess.Popen", side_effect=AssertionError("worker launch")),
        ):
            for source in (parts, manifest):
                with self.subTest(source=source.name):
                    _, count, count_table = self._run(source, "count")
                    self.assertEqual(count_table.to_pylist(), [{"count_all": 12}])
                    del count
                    _, projected, table = self._run(
                        source,
                        "project",
                        vortex_columns=("shipment_id", "nullable_payload"),
                    )
                    self.assertEqual(table.to_pylist(), expected)
                    del projected

            session = self.native.Session(
                self.native.ExecutionResources(self.memory_bytes, self.max_parallelism)
            )
            try:
                plan = session.workflow(
                    self._primitive_args(
                        parts,
                        "project",
                        options={"--vortex-columns": "shipment_id,nullable_payload"},
                    )
                )
                stream = plan.iter_batches()
                with stream:
                    batches = [
                        self.pa.RecordBatchReader.from_stream(batch).read_all()
                        for batch in stream
                    ]
                self.assertEqual(
                    [row for batch in batches for row in batch.to_pylist()],
                    expected,
                )
                self.assertIsNotNone(stream.report)
                self.assertEqual(stream.report["columnar_export"]["python_rows_constructed"], False)
            finally:
                session.close()

    def test_sql_file_declarations_reuse_the_shared_primitive_providers(self):
        path = self.root / "sql-provider-selection.vortex"
        self._write_table_fixture(path, self.pa.table({
            "key": ["a", "b", "a", "b", None],
            "value": self.pa.array([2, None, 4, -3, 0], type=self.pa.int64()),
        }))
        cases = [
            ("SUM(value) AS total, COUNT(value) AS present, COUNT(*) AS rows", "",
             [{"total": 3.0, "present": 4, "rows": 5}]),
            ("MIN(value) AS minimum, MAX(value) AS maximum, AVG(value) AS mean", "",
             [{"minimum": -3, "maximum": 4, "mean": 0.75}]),
            ("SUM(value + 2) AS total", "", [{"total": 11.0}]),
            ("SUM(value) AS total", " WHERE value > 100", [{"total": None}]),
            ("key, SUM(value) AS total", " GROUP BY key HAVING SUM(value) > 0 ORDER BY total DESC",
             [{"key": "a", "total": 6.0}]),
            ("value", " WHERE value >= 0 ORDER BY value DESC LIMIT 2", [{"value": 4}, {"value": 2}]),
            ("value", " LIMIT 3", [{"value": 2}, {"value": None}, {"value": 4}]),
        ]
        session = self.native.Session(
            self.native.ExecutionResources(self.memory_bytes, self.max_parallelism)
        )
        try:
            for projection, suffix, expected in cases:
                with self.subTest(projection=projection, suffix=suffix):
                    plan = session.sql(f"SELECT {projection} FROM '{path}'{suffix}")
                    first = plan.collect()
                    self.assertEqual(first.report.get("resident_native_preparation_reused"), "false")
                    self.assertEqual(self.pa.RecordBatchReader.from_stream(first).read_all().to_pylist(), expected)
                    second = plan.collect()
                    self.assertEqual(second.report["resident_native_preparation_reused"], "true")
                    self.assertEqual(self.pa.RecordBatchReader.from_stream(second).read_all().to_pylist(), expected)
                    self.assertEqual(second.report["fallback_attempted"], "false")
                    self.assertEqual(second.report["external_engine_invoked"], "false")
            session.close()
            self.assertEqual(self.pa.RecordBatchReader.from_stream(first).read_all().to_pylist(), cases[-1][2])
        finally:
            session.close()

    def test_sql_collection_and_compatibility_writes_preserve_complete_rows(self):
        source_path = self.root / "schema-regression.vortex"
        table = self.pa.table({
            "key": self.pa.array(["a", "b", "a", "c", "b"], type=self.pa.string()),
            "value": self.pa.array([2, None, 4, -3, 0], type=self.pa.int64()),
        })
        self._write_table_fixture(source_path, table)
        ipc_path = self.root / "schema-regression.arrow"
        with ipc_path.open("wb") as sink:
            with self.pa.ipc.new_file(sink, table.schema) as writer:
                writer.write_table(table)

        formats = ("csv", "json", "jsonl", "arrow_ipc", "parquet", "avro", "orc")
        text_schema = "key:utf8,value:int64"
        queries = (
            ("COUNT(*)", "", [{"count_all": 5}]),
            ("SUM(value) AS total,COUNT(value) AS present,COUNT(*) AS rows", "",
             [{"total": 3.0, "present": 4, "rows": 5}]),
            ("key,SUM(value) AS total", " GROUP BY key ORDER BY total DESC",
             [{"key": "a", "total": 6.0}, {"key": "b", "total": 0.0}, {"key": "c", "total": -3.0}]),
            ("value", " WHERE value >= 0 ORDER BY value DESC LIMIT 2",
             [{"value": 4}, {"value": 2}]),
            ("SUM(value) AS total", " WHERE value > 100", [{"total": None}]),
        )
        resources = self.native.ExecutionResources(self.memory_bytes, self.max_parallelism)
        session = self.native.Session(resources)
        try:
            written = {}
            for output_format in formats:
                destination = self.root / f"schema-regression.{output_format}"
                write_plan = session.sql(
                    f"SELECT key,value FROM '{ipc_path}'",
                    files=[(str(ipc_path), "arrow-ipc", None)],
                )
                write_plan.write(destination, f"write_{output_format}")
                written[output_format] = destination

            sources = [(source_path, "vortex", None)] + [
                (path, output_format.replace("_", "-"), text_schema if output_format in {"csv", "json", "jsonl"} else None)
                for output_format, path in written.items()
            ]
            control_reports = {}
            for source_index, (source_path_for_query, input_format, schema) in enumerate(sources):
                for query_index, (projection, suffix, expected) in enumerate(queries):
                    statement = f"SELECT {projection} FROM '{source_path_for_query}'{suffix}"
                    plan = session.sql(statement, files=[(str(source_path_for_query), input_format, schema)])
                    result = plan.collect()
                    reuse_field = ("resident_relational_declaration_reused" if input_format == "parquet"
                                   else "resident_native_preparation_reused")
                    self.assertTrue(reuse_field in result.report,
                                    (input_format, projection, suffix, result.report.get("mode")))
                    actual_rows = self.pa.RecordBatchReader.from_stream(result).read_all().to_pylist()
                    self.assertEqual(actual_rows, expected)
                    if source_index == 0:
                        control_reports[projection, suffix] = result.report
                    elif input_format == "parquet":
                        self.assertEqual(result.report["native_source_demand_sources"], "1")
                        self.assertEqual(result.report["native_source_demand_prepared_input_bytes_written"], "0")
                        self.assertEqual(result.report.get("fallback_attempted"), "false")
                        self.assertEqual(result.report.get("external_engine_invoked"), "false")
                    else:
                        control_report = control_reports[projection, suffix]
                        for field in ("mode", "primitive", "vortex_primitive_arg"):
                            self.assertEqual(result.report.get(field), control_report.get(field), field)
                        self.assertEqual(result.report.get("fallback_attempted"), "false")
                        self.assertEqual(result.report.get("external_engine_invoked"), "false")
                        if query_index > 0:
                            self.assertEqual(result.report["public_workflow_preparation_prepared_state_reuse_hit"], "true")
                            self.assertEqual(result.report["public_workflow_local_source_vortex_ingest_performed"], "false")

            csv_path = written["csv"]
            for count_source, input_format, source_schema in (
                (source_path, "vortex", None),
                (csv_path, "csv", text_schema),
            ):
                for threshold, expected_count in ((0, 2), (100, 0)):
                    counted = session.sql(
                        f"SELECT COUNT(*) FROM '{count_source}' WHERE value > {threshold}",
                        files=[(str(count_source), input_format, source_schema)],
                    )
                    for output_format in ("vortex",) + formats:
                        destination = self.root / f"count-{input_format}-{threshold}.{output_format}"
                        writer_report = counted.write(destination, f"write_{output_format}")
                        self.assertEqual(
                            writer_report["local_primitive_physical_policy_writer_coalescing_enabled"],
                            "true",
                        )
                        self.assertEqual(writer_report["fallback_attempted"], "false")
                        self.assertEqual(writer_report["external_engine_invoked"], "false")
                        schema = "count_all:uint64" if output_format in {"csv", "json", "jsonl"} else None
                        reopened = session.sql(
                            f"SELECT count_all FROM '{destination}'",
                            files=[(str(destination), output_format.replace("_", "-"), schema)],
                        ).collect()
                        self.assertEqual(
                            self.pa.RecordBatchReader.from_stream(reopened).read_all().to_pylist(),
                            [{"count_all": expected_count}],
                        )
            grouped = session.sql(
                f"SELECT key,SUM(value) AS total FROM '{csv_path}' GROUP BY key ORDER BY total DESC",
                files=[(str(csv_path), "csv", text_schema)],
            )
            for output_format in ("vortex",) + formats:
                destination = self.root / f"grouped.{output_format}"
                writer_report = grouped.write(destination, f"write_{output_format}")
                self.assertEqual(writer_report.get("fallback_attempted"), "false")
                self.assertEqual(writer_report.get("external_engine_invoked"), "false")
                schema = "key:utf8,total:float64" if output_format in {"csv", "json", "jsonl"} else None
                reopened = session.sql(
                    f"SELECT key,total FROM '{destination}'",
                    files=[(str(destination), output_format.replace("_", "-"), schema)],
                ).collect()
                self.assertEqual(
                    self.pa.RecordBatchReader.from_stream(reopened).read_all().to_pylist(),
                    [{"key": "a", "total": 6.0}, {"key": "b", "total": 0.0}, {"key": "c", "total": -3.0}],
                )
                self.assertEqual(reopened.report.get("fallback_attempted"), "false")
                self.assertEqual(reopened.report.get("external_engine_invoked"), "false")
        finally:
            session.close()

    def test_retained_parquet_and_orc_sql_plans_reject_changed_source_generation(self):
        table = self.pa.table({"value": self.pa.array([2, None, 4], type=self.pa.int64())})
        replacement = self.pa.table({"value": self.pa.array([40, None, 20], type=self.pa.int64())})
        resources = self.native.ExecutionResources(self.memory_bytes, self.max_parallelism)
        session = self.native.Session(resources)
        try:
            ipc_path = self.root / "generation-source.arrow"
            with ipc_path.open("wb") as sink:
                with self.pa.ipc.new_file(sink, table.schema) as writer:
                    writer.write_table(table)
            replacement_ipc = self.root / "generation-replacement.arrow"
            with replacement_ipc.open("wb") as sink:
                with self.pa.ipc.new_file(sink, replacement.schema) as writer:
                    writer.write_table(replacement)
            for input_format in ("parquet", "orc"):
                path = self.root / f"generation.{input_format}"
                session.sql(
                    f"SELECT value FROM '{ipc_path}'",
                    files=[(str(ipc_path), "arrow-ipc", None)],
                ).write(path, f"write_{input_format}")
                replacement_path = self.root / f"replacement.{input_format}"
                session.sql(
                    f"SELECT value FROM '{replacement_ipc}'",
                    files=[(str(replacement_ipc), "arrow-ipc", None)],
                ).write(replacement_path, f"write_{input_format}")
                plan = session.sql(
                    f"SELECT SUM(value) AS total FROM '{path}'",
                    files=[(str(path), input_format, None)],
                )
                retained = plan.collect()
                retained_table = self.pa.RecordBatchReader.from_stream(retained).read_all()
                self.assertEqual(retained_table.to_pylist(), [{"total": 6.0}])
                if input_format == "parquet":
                    self.assertEqual(retained.report.get("native_source_demand_sources"), "1")
                    self.assertEqual(retained.report.get("native_source_demand_prepared_input_bytes_written"), "0")
                path.write_bytes(replacement_path.read_bytes())
                with self.assertRaises(RuntimeError) as raised:
                    plan.collect()
                # Direct Parquet holds SourceIdentity; ORC retains the complete
                # preparation binding. Both must reject the changed generation.
                message = "prepared source changed" if input_format == "parquet" else "source generation changed"
                self.assertIn(message, str(raised.exception).lower())
                self.assertEqual(retained_table.to_pylist(), [{"total": 6.0}])

            primary = self.root / "declaration-primary.parquet"
            extra = self.root / "unused-extra.parquet"
            session.sql(
                f"SELECT value FROM '{ipc_path}'",
                files=[(str(ipc_path), "arrow-ipc", None)],
            ).write(primary, "write_parquet")
            before = session.snapshot()["prepared_source_opens"]
            plan = session.sql(
                f"SELECT SUM(value) AS total FROM '{primary}'",
                files=[(str(primary), "parquet", None), (str(extra), "parquet", None)],
            )
            self.assertEqual(session.snapshot()["prepared_source_opens"], before)
            with self.assertRaises(RuntimeError):
                plan.collect()
            self.assertEqual(session.snapshot()["prepared_source_opens"], before)
        finally:
            session.close()

    def test_context_read_demand_and_prepared_sources_across_queries_and_writers(self):
        import shardloom as sl
        import pyarrow.orc as orc
        import pyarrow.parquet as parquet

        table = self.pa.table({
            "key": ["a", "b", "a", "c", "b"],
            "value": self.pa.array([2, None, 4, -3, 0], type=self.pa.int64()),
        })
        expected = [{"key": "a", "total": 6.0}, {"key": "b", "total": 0.0},
                    {"key": "c", "total": -3.0}]
        ctx = sl.context(transport="native", memory_bytes=self.memory_bytes,
                         max_parallelism=self.max_parallelism)
        self.addCleanup(ctx.client.close)

        def generation(path):
            stat = path.stat()
            return stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns

        for input_format, write_fixture in (("parquet", parquet.write_table), ("orc", orc.write_table)):
            source = self.root / f"context-reuse.{input_format}"
            write_fixture(table, source)

            def grouped(path):
                return ctx.read(path).group_by("key").agg(total="sum(value)").sort("key")

            first = grouped(source).collect(check=True)
            self.assertEqual(first.to_arrow().to_pylist(), expected)
            if input_format == "parquet":
                self.assertEqual(first.envelope.field("native_source_demand_sources"), "1")
                self.assertEqual(first.envelope.field("native_source_demand_prepared_input_bytes_written"), "0")
                self.assertIsNone(first.envelope.field("public_workflow_local_source_prepared_vortex_path"))
                prepared = self.root / "context-demand-control.vortex"
                ctx.prepare_vortex(source, prepared, input_format="parquet", check=True)
            else:
                prepared = Path(first.envelope.field("public_workflow_local_source_prepared_vortex_path"))
            original_generation = generation(prepared)
            def filtered(path):
                return ctx.read(path).filter(sl.col("value") >= 0).select("value").sort("value")

            altered = filtered(source).collect(check=True)
            altered_control = filtered(prepared).collect(check=True)
            for result in (altered, altered_control):
                self.assertEqual(result.to_arrow().to_pylist(), [{"value": 0}, {"value": 2}, {"value": 4}])
                self.assertFalse(result.envelope.fallback.attempted)
            for field in ("mode", "primitive", "vortex_primitive_arg", "resident_relational_declaration_reused"):
                self.assertEqual(altered.envelope.field(field), altered_control.envelope.field(field), field)
            # Composed relational plans retain preparation identities internally;
            # their report does not carry the single-source primitive attachments.
            # The same persisted generation proves the changed query did not rewrite it.
            self.assertEqual(generation(prepared), original_generation)
            repeated = grouped(source).collect(check=True)
            self.assertEqual(repeated.to_arrow().to_pylist(), expected)
            if input_format == "parquet":
                self.assertEqual(repeated.envelope.field("native_source_demand_sources"), "1")
                self.assertEqual(repeated.envelope.field("native_source_demand_prepared_input_bytes_written"), "0")
            else:
                self.assertEqual(repeated.envelope.field("public_workflow_preparation_prepared_state_reuse_hit"), "true")
                self.assertEqual(repeated.envelope.field("public_workflow_local_source_vortex_ingest_performed"), "false")
            native_control = grouped(prepared).collect(check=True)
            self.assertEqual(native_control.to_arrow().to_pylist(), expected)
            if input_format != "parquet":
                for field in ("mode", "primitive", "vortex_primitive_arg"):
                    self.assertEqual(first.envelope.field(field), native_control.envelope.field(field), field)
            for output_format in ("parquet", "orc"):
                destination = self.root / f"context-{input_format}-to-{output_format}.{output_format}"
                written = grouped(source).write(destination, output_format=output_format, check=True)
                self.assertFalse(written.envelope.fallback.attempted)
                self.assertEqual(ctx.read(destination).sort("key").collect(check=True).to_arrow().to_pylist(), expected)
                self.assertEqual(generation(prepared), original_generation)
            self.assertEqual(first.to_arrow().to_pylist(), expected)

    def test_sql_provider_selection_preserves_memory_bindings_and_relational_shapes(self):
        path = self.root / "sql-provider-selection-boundaries.vortex"
        self._write_table_fixture(path, self.pa.table({"value": [2, 4, -3, 0]}))
        session = self.native.Session(
            self.native.ExecutionResources(self.memory_bytes, self.max_parallelism)
        )
        try:
            for statement, expected in [
                (f"SELECT SUM(value * value) AS total FROM '{path}'", [{"total": 29.0}]),
                (f"SELECT SUM(value) AS total FROM '{path}' LIMIT 0", []),
                (f"SELECT value FROM '{path}' UNION ALL SELECT value FROM '{path}'",
                 [{"value": n} for n in [2, 4, -3, 0, 2, 4, -3, 0]]),
            ]:
                with self.subTest(statement=statement):
                    result = session.sql(statement).collect()
                    self.assertIn("resident_relational_declaration_reused", result.report)
                    self.assertEqual(self.pa.RecordBatchReader.from_stream(result).read_all().to_pylist(), expected)
            source = session.from_arrow(self.pa.table({"value": [100, 200]}))
            result = session.sql("SELECT SUM(value) AS total FROM 'memory://value.vortex'",
                                 sources=[("memory://value.vortex", source)]).collect()
            self.assertIn("resident_relational_declaration_reused", result.report)
            self.assertEqual(self.pa.RecordBatchReader.from_stream(result).read_all().to_pylist(), [{"total": 300.0}])
            # Invalid memory references must retain their diagnostic rather than
            # accidentally opening a file with the same reference.
            before = session.snapshot()["prepared_source_opens"]
            with self.assertRaisesRegex(RuntimeError, "memory source requires"):
                session.sql(f"SELECT SUM(value) AS total FROM '{path}'",
                            sources=[(str(path), source)]).collect()
            self.assertEqual(session.snapshot()["prepared_source_opens"], before)
        finally:
            session.close()

    def test_retained_collection_declarations_rebind_directory_and_manifest_membership(self):
        parts = self.root / "changing-parts.vortex"
        parts.mkdir()
        self._write_fixture(parts / "a.vortex", 2, descending=False)
        manifest = self.root / "changing-parts.vortex-manifest"
        manifest.write_text(
            json.dumps({"inputs": [f"{parts.name}/a.vortex"]}),
            encoding="utf-8",
        )
        session = self.native.Session(
            self.native.ExecutionResources(self.memory_bytes, self.max_parallelism)
        )
        try:
            count_plan = session.workflow(self._primitive_args(parts, "count"))
            sql_plan = session.sql(
                f"SELECT COUNT(*) AS total FROM '{parts}'",
                files=[(str(parts), "vortex", None)],
            )
            count_first = count_plan.collect()
            self.assertEqual(
                self.pa.RecordBatchReader.from_stream(count_first)
                .read_all()
                .to_pylist(),
                [{"count_all": 2}],
            )
            self.assertEqual(
                count_first.report["resident_native_preparation_reused"], "false"
            )
            sql_first = sql_plan.collect()
            self.assertEqual(
                self.pa.RecordBatchReader.from_stream(sql_first).read_all().to_pylist(),
                [{"total": 2}],
            )
            self.assertEqual(
                sql_first.report["resident_native_preparation_reused"], "false"
            )

            self._write_fixture(parts / "b.vortex", 3, offset=2, descending=False)
            count_after_add = count_plan.collect()
            self.assertEqual(
                self.pa.RecordBatchReader.from_stream(count_after_add).read_all().to_pylist(),
                [{"count_all": 5}],
            )
            self.assertEqual(
                count_after_add.report["resident_native_preparation_reused"], "false"
            )
            sql_after_add = sql_plan.collect()
            self.assertEqual(
                self.pa.RecordBatchReader.from_stream(sql_after_add).read_all().to_pylist(),
                [{"total": 5}],
            )
            self.assertEqual(
                sql_after_add.report["resident_native_preparation_reused"], "false"
            )
        finally:
            session.close()

        manifest_session = self.native.Session(
            self.native.ExecutionResources(self.memory_bytes, self.max_parallelism)
        )
        try:
            manifest_plan = manifest_session.workflow(
                self._primitive_args(
                    manifest,
                    "project",
                    options={"--vortex-columns": "shipment_id,nullable_payload"},
                )
            )
            first = manifest_plan.collect()
            self.assertEqual(
                self.pa.RecordBatchReader.from_stream(first).read_all().to_pylist(),
                self._expected([0, 1]),
            )
            self.assertEqual(first.report["resident_native_preparation_reused"], "false")

            manifest.write_text(
                json.dumps(
                    {"inputs": [f"{parts.name}/a.vortex", f"{parts.name}/b.vortex"]}
                ),
                encoding="utf-8",
            )
            appended = manifest_plan.collect()
            self.assertEqual(
                self.pa.RecordBatchReader.from_stream(appended).read_all().to_pylist(),
                self._expected(range(5)),
            )
            self.assertEqual(
                appended.report["resident_native_preparation_reused"], "false"
            )

            manifest.write_text(
                json.dumps(
                    {"inputs": [f"{parts.name}/b.vortex", f"{parts.name}/a.vortex"]}
                ),
                encoding="utf-8",
            )
            reordered = manifest_plan.collect()
            self.assertEqual(
                self.pa.RecordBatchReader.from_stream(reordered).read_all().to_pylist(),
                self._expected([2, 3, 4, 0, 1]),
            )
            self.assertEqual(
                reordered.report["resident_native_preparation_reused"], "false"
            )
        finally:
            manifest_session.close()

    def test_declared_csv_source_options_filter_rows_and_write_vortex_readback(self):
        csv_path = self.root / "compatibility-input.csv"
        csv_path.write_text(
            "shipment_id,nullable_payload\n1,10\n2,\n3,30\n",
            encoding="utf-8",
        )
        schema = {"shipment_id": "int64", "nullable_payload": "int64"}
        statement = (
            f"SELECT shipment_id, nullable_payload FROM '{csv_path}'"
        )
        expected = [
            {"shipment_id": 2, "nullable_payload": None},
            {"shipment_id": 3, "nullable_payload": 30},
        ]
        common = dict(
            input_uri=csv_path,
            input_format="csv",
            source_schema=schema,
            sql_statement=statement,
            execution_policy="vortex_middle",
            materialization_policy="bounded",
            bounded=True,
            memory_bytes=self.memory_bytes,
            max_parallelism=self.max_parallelism,
            vortex_primitive="filter_project",
            vortex_predicate="gte:shipment_id:2",
            vortex_columns=("shipment_id", "nullable_payload"),
        )
        with (
            patch("shardloom.client.subprocess.run", side_effect=AssertionError("process launch")),
            patch("shardloom.client.subprocess.Popen", side_effect=AssertionError("worker launch")),
        ):
            execution = self.client.public_workflow_run(
                "cli", requested_output="collect", **common
            )
            self.assertEqual(execution.envelope.status, "success", execution.envelope.raw)
            self.assertFalse(execution.envelope.fallback.attempted, execution.envelope.raw)
            self.assertEqual(
                self.pa.RecordBatchReader.from_stream(execution.envelope._native_result)
                .read_all()
                .to_pylist(),
                expected,
            )

            output = self.root / "compatibility-filtered.vortex"
            written = self.client.public_workflow_run(
                "cli",
                requested_output="write_vortex",
                output_ref=output,
                **common,
            )
            self.assertEqual(written.envelope.status, "success", written.envelope.raw)
            self.assertTrue(output.is_file())
            _, reloaded, table = self._run(
                output,
                "project",
                vortex_columns=("shipment_id", "nullable_payload"),
            )
            self.assertEqual(table.to_pylist(), expected)
            del reloaded

    def test_compatibility_csv_generation_change_fails_without_invalidating_retained_result(self):
        csv_path = self.root / "generation-change.csv"
        csv_path.write_text(
            "shipment_id,nullable_payload\n1,10\n2,\n3,30\n",
            encoding="utf-8",
        )
        statement = f"SELECT shipment_id, nullable_payload FROM '{csv_path}'"
        args = [
            "cli",
            "--input",
            str(csv_path),
            "--input-format",
            "csv",
            "--source-schema",
            "shipment_id:int64,nullable_payload:int64",
            "--sql",
            statement,
            "--request",
            "collect",
            "--bounded",
            "true",
            "--execution-policy",
            "vortex_middle",
            "--materialization-policy",
            "bounded",
            "--vortex-primitive",
            "filter_project",
            "--vortex-predicate",
            "gte:shipment_id:2",
            "--vortex-columns",
            "shipment_id,nullable_payload",
            "--memory-bytes",
            str(self.memory_bytes),
            "--max-parallelism",
            str(self.max_parallelism),
        ]
        session = self.native.Session(
            self.native.ExecutionResources(self.memory_bytes, self.max_parallelism)
        )
        try:
            plan = session.workflow(args)
            retained = plan.collect()
            expected = [
                {"shipment_id": 2, "nullable_payload": None},
                {"shipment_id": 3, "nullable_payload": 30},
            ]
            retained_table = self.pa.RecordBatchReader.from_stream(retained).read_all()
            self.assertEqual(retained_table.to_pylist(), expected)

            csv_path.write_text(
                "shipment_id,nullable_payload\n4,40\n5,50\n",
                encoding="utf-8",
            )
            with self.assertRaises(RuntimeError) as raised:
                plan.collect()
            self.assertIn("source generation changed", str(raised.exception).lower())
            self.assertEqual(retained_table.to_pylist(), expected)
        finally:
            session.close()

    def test_retained_result_denies_small_grant_then_releases_for_retry_and_rejects_stale_file(self):
        source_path = self.root / "replaceable.vortex"
        replacement_path = self.root / "replacement.vortex"
        self._write_fixture(source_path, 5)
        self._write_fixture(replacement_path, 5, offset=10)
        session = self.native.Session(
            self.native.ExecutionResources(self.memory_bytes, self.max_parallelism)
        )
        try:
            plan = session.workflow(
                self._primitive_args(
                    source_path,
                    "project",
                    options={"--vortex-columns": "shipment_id,nullable_payload"},
                )
            )
            first = plan.collect()
            first_table = self.pa.RecordBatchReader.from_stream(first).read_all()
            self.assertEqual(first_table.to_pylist(), self._expected(range(4, -1, -1)))
            self.assertEqual(first.report["resident_native_preparation_reused"], "false")
            second = plan.collect()
            self.assertEqual(second.report["resident_native_preparation_reused"], "true")
            del second

            retained = session.snapshot()["reserved_bytes"]
            self.assertGreater(retained, 1)
            too_small = self.native.ExecutionResources(retained - 1, 1)
            opens = session.snapshot()["prepared_source_opens"]
            with self.assertRaises(RuntimeError):
                plan.collect(resources=too_small)
            self.assertEqual(session.snapshot()["prepared_source_opens"], opens)
            del first, first_table
            plan.release_prepared()
            self.assertEqual(session.snapshot()["reserved_bytes"], 0)
            retried = plan.collect()
            self.assertEqual(
                self.pa.RecordBatchReader.from_stream(retried).read_all().to_pylist(),
                self._expected(range(4, -1, -1)),
            )
            del retried

            os.replace(replacement_path, source_path)
            with self.assertRaises(RuntimeError) as raised:
                plan.collect()
            self.assertIn("prepared source changed", str(raised.exception).lower())
        finally:
            session.close()

    def test_partitioned_native_aggregate_matches_single_source_with_offsets_transforms_and_having(self):
        rows = [
            (0, None), (1, 1), (2, 1), (3, 2), (4, None), (5, 3),
            (6, 3), (7, 3), (8, 4), (9, 4), (10, None), (11, 2),
        ]
        table = self.pa.table(
            {
                "shipment_id": self.pa.array([row[0] for row in rows], type=self.pa.int64()),
                "nullable_payload": self.pa.array([row[1] for row in rows], type=self.pa.int64()),
                "tag": self.pa.array(
                    ["none0", "a", "bb", "ccc", "none4", "dd", "eeeee", "f", "gg", "hhh", "none10", "iiii"],
                    type=self.pa.string(),
                ),
            }
        )
        parts_dir = self.root / "aggregate-parts.vortex"
        parts_dir.mkdir()
        part_paths = [parts_dir / "a.vortex", parts_dir / "empty.vortex", parts_dir / "b.vortex"]
        self._write_table_fixture(part_paths[0], table.slice(0, 5))
        self._write_table_fixture(part_paths[1], table.slice(5, 0))
        self._write_table_fixture(part_paths[2], table.slice(5))
        single = self.root / "aggregate-single.vortex"
        self._write_table_fixture(single, table)
        manifest = self.root / "aggregate-parts.vortex-manifest"
        self._write_manifest(manifest, part_paths)

        aggregate = json.dumps(
            {
                "group_by": ["nullable_payload"],
                "group_expressions": [
                    {
                        "alias": "shifted_group",
                        "column": "shipment_id",
                        "function": "constant_int",
                        "argument_offset": 7,
                    }
                ],
                "measures": [
                    {"function": "count", "alias": "rows"},
                    {"function": "count", "column": "shipment_id", "alias": "present"},
                    {"function": "count_distinct", "column": "shipment_id", "alias": "distinct_ids"},
                    {"function": "sum", "column": "shipment_id", "alias": "shifted_sum", "argument_offset": 2},
                    {"function": "min", "column": "shipment_id", "alias": "min_id"},
                    {"function": "max", "column": "shipment_id", "alias": "max_id"},
                    {"function": "min", "column": "tag", "alias": "minimum_tag_length", "value_transform": "length"},
                ],
                "having": [{"column": "rows", "op": "gte", "value": 2}],
                "order_by": [{"column": "nullable_payload", "nulls": "first"}],
                "offset": 1,
            },
            separators=(",", ":"),
        )
        with (
            patch("shardloom.client.subprocess.run", side_effect=AssertionError("process launch")),
            patch("shardloom.client.subprocess.Popen", side_effect=AssertionError("worker launch")),
        ):
            _, partitioned, partitioned_table = self._run_manifest_primitive(
                manifest, "aggregate", vortex_aggregate=aggregate, vortex_source_order_limit=20
            )
            _, unpartitioned, unpartitioned_table = self._run_manifest_primitive(
                single, "aggregate", vortex_aggregate=aggregate, vortex_source_order_limit=20
            )
            self.assertEqual(partitioned_table.schema, unpartitioned_table.schema)
            self.assertEqual(partitioned_table.to_pylist(), unpartitioned_table.to_pylist())
            self.assertEqual(
                partitioned_table.to_pylist(),
                [{
                    "nullable_payload": 1,
                    "shifted_group": 7,
                    "rows": 2,
                    "present": 2,
                    "distinct_ids": 2,
                    "shifted_sum": 7.0,
                    "min_id": 1,
                    "max_id": 2,
                    "minimum_tag_length": 1,
                }, {
                    "nullable_payload": 2,
                    "shifted_group": 7,
                    "rows": 2,
                    "present": 2,
                    "distinct_ids": 2,
                    "shifted_sum": 18.0,
                    "min_id": 3,
                    "max_id": 11,
                    "minimum_tag_length": 3,
                }, {
                    "nullable_payload": 3,
                    "shifted_group": 7,
                    "rows": 3,
                    "present": 3,
                    "distinct_ids": 3,
                    "shifted_sum": 24.0,
                    "min_id": 5,
                    "max_id": 7,
                    "minimum_tag_length": 1,
                }, {
                    "nullable_payload": 4,
                    "shifted_group": 7,
                    "rows": 2,
                    "present": 2,
                    "distinct_ids": 2,
                    "shifted_sum": 21.0,
                    "min_id": 8,
                    "max_id": 9,
                    "minimum_tag_length": 2,
                }],
            )
            self.assertEqual(
                partitioned.report["local_primitive_aggregate_result_serialized_json_round_trip"], "false"
            )
            session = self.native.Session(self.native.ExecutionResources(self.memory_bytes, self.max_parallelism))
            try:
                plan = session.workflow(self._primitive_args(manifest, "aggregate", options={
                    "--vortex-aggregate": aggregate, "--vortex-source-order-limit": "20",
                }))
                with plan.iter_batches() as stream:
                    rows = [row for batch in stream
                            for row in self.pa.RecordBatchReader.from_stream(batch).read_all().to_pylist()]
                self.assertEqual(rows, partitioned_table.to_pylist())
                output = self.root / "partitioned-aggregate-output.vortex"
                plan.write(output, "write_vortex")
                _, reopened, written = self._run(output, "project", vortex_columns="*")
                self.assertEqual(written.schema, partitioned_table.schema)
                self.assertEqual(written.to_pylist(), partitioned_table.to_pylist())
                del reopened
            finally:
                session.close()
            del partitioned, unpartitioned

            scalar = json.dumps(
                {
                    "measures": [{"function": "count", "alias": "rows"}],
                    "having": [{"column": "rows", "op": "lt", "value": 0}],
                },
                separators=(",", ":"),
            )
            _, rejected, rejected_table = self._run_manifest_primitive(
                manifest, "aggregate", vortex_aggregate=scalar, vortex_source_order_limit=1
            )
            self.assertEqual(rejected_table.num_rows, 0)
            self.assertEqual(rejected_table.schema.names, ["rows"])
            del rejected

    def test_partitioned_sort_preserves_global_ties_nulls_projection_predicate_and_streamed_output(self):
        table = self.pa.table(
            {
                "shipment_id": self.pa.array([0, 1, 2, 3, 4, 5, 6, 7, 8, 9], type=self.pa.int64()),
                "nullable_payload": self.pa.array([2, None, 1, 2, 2, 3, None, 2, 3, 1], type=self.pa.int64()),
            }
        )
        parts_dir = self.root / "sort-parts.vortex"
        parts_dir.mkdir()
        part_paths = [parts_dir / "a.vortex", parts_dir / "b.vortex"]
        self._write_table_fixture(part_paths[0], table.slice(0, 5))
        self._write_table_fixture(part_paths[1], table.slice(5))
        single = self.root / "sort-single.vortex"
        self._write_table_fixture(single, table)
        manifest = self.root / "sort-parts.vortex-manifest"
        self._write_manifest(manifest, part_paths)

        with (
            patch("shardloom.client.subprocess.run", side_effect=AssertionError("process launch")),
            patch("shardloom.client.subprocess.Popen", side_effect=AssertionError("worker launch")),
        ):
            for tie_policy in ("first", "last", "all"):
                order = json.dumps(
                    {
                        "order_by": [{"column": "nullable_payload", "descending": True, "nulls": "last"}],
                        "tie_policy": tie_policy,
                        "offset": 1,
                        "limit": 3,
                    },
                    separators=(",", ":"),
                )
                options = {
                    "vortex_sort_rows": order,
                    "vortex_columns": ("shipment_id",),
                    "vortex_predicate": "gte:shipment_id:1",
                    "vortex_source_order_limit": 3,
                }
                _, partitioned, part_table = self._run(manifest, "sort_rows", **options)
                _, unpartitioned, single_table = self._run(single, "sort_rows", **options)
                self.assertEqual(part_table.schema, single_table.schema)
                self.assertEqual(part_table.to_pylist(), single_table.to_pylist())
                self.assertEqual(part_table.schema.names, ["shipment_id"])
                del partitioned, unpartitioned

            order = json.dumps(
                {
                    "order_by": [{"column": "nullable_payload", "descending": False, "nulls": "first"}],
                    "offset": 2,
                    "limit": 8,
                    "tie_policy": "first",
                },
                separators=(",", ":"),
            )
            session = self.native.Session(
                self.native.ExecutionResources(self.memory_bytes, self.max_parallelism)
            )
            try:
                args = self._primitive_args(
                    manifest,
                    "sort_rows",
                    options={
                        "--vortex-sort-rows": order,
                        "--vortex-predicate": "gte:shipment_id:0",
                        "--vortex-columns": "shipment_id,nullable_payload",
                        "--vortex-source-order-limit": "8",
                    },
                )
                plan = session.workflow(args)
                opens_before_denial = session.snapshot()["prepared_source_opens"]
                with self.assertRaises(RuntimeError):
                    plan.collect(resources=self.native.ExecutionResources(1, 1))
                self.assertEqual(
                    session.snapshot()["prepared_source_opens"], opens_before_denial
                )
                retried = plan.collect()
                self.assertEqual(
                    self.pa.RecordBatchReader.from_stream(retried).read_all().to_pylist(),
                    self._run(
                        manifest,
                        "sort_rows",
                        vortex_sort_rows=order,
                        vortex_predicate="gte:shipment_id:0",
                        vortex_columns=("shipment_id", "nullable_payload"),
                        vortex_source_order_limit=8,
                    )[2].to_pylist(),
                )
                del retried
                opens_after_retry = session.snapshot()["prepared_source_opens"]
                cancelled = session.workflow(args)
                with self.assertRaises(RuntimeError):
                    cancelled.collect(timeout=0)
                self.assertEqual(
                    session.snapshot()["prepared_source_opens"], opens_after_retry
                )
                expected = self._run(manifest, "sort_rows", vortex_sort_rows=order,
                                     vortex_predicate="gte:shipment_id:0",
                                     vortex_columns=("shipment_id", "nullable_payload"),
                                     vortex_source_order_limit=8)[2].to_pylist()
                stream = plan.iter_batches()
                with stream:
                    actual = [
                        row
                        for batch in stream
                        for row in self.pa.RecordBatchReader.from_stream(batch).read_all().to_pylist()
                    ]
                self.assertEqual(actual, expected)
                self.assertTrue(actual)
                output = self.root / "partitioned-sort-output.vortex"
                plan.write(output, "write_vortex")
                _, result, written_table = self._run(
                    output,
                    "project",
                    vortex_columns=("shipment_id", "nullable_payload"),
                )
                self.assertEqual(written_table.to_pylist(), expected)
                del result
            finally:
                session.close()

    def test_partitioned_structured_projection_delivers_all_rows_past_collection_threshold(self):
        count = 4_193
        ids = list(range(count))
        table = self.pa.table(
            {
                "shipment_id": self.pa.array(ids, type=self.pa.int64()),
                "items": self.pa.array([[value, value + 1] for value in ids], type=self.pa.list_(self.pa.int64())),
                "metadata": self.pa.array(
                    [{"code": str(value), "rank": value % 13} for value in ids],
                    type=self.pa.struct([("code", self.pa.string()), ("rank", self.pa.int64())]),
                ),
            }
        )
        parts_dir = self.root / "nested-parts.vortex"
        parts_dir.mkdir()
        part_paths = [parts_dir / "a.vortex", parts_dir / "b.vortex"]
        self._write_table_fixture(part_paths[0], table.slice(0, 2_101))
        self._write_table_fixture(part_paths[1], table.slice(2_101))
        single = self.root / "nested-single.vortex"
        self._write_table_fixture(single, table)
        manifest = self.root / "nested-parts.vortex-manifest"
        self._write_manifest(manifest, part_paths)
        with (
            patch("shardloom.client.subprocess.run", side_effect=AssertionError("process launch")),
            patch("shardloom.client.subprocess.Popen", side_effect=AssertionError("worker launch")),
        ):
            options = {
                "vortex_expression_projection": json.dumps({"structured_columns": [
                    {"name": "shipment_id", "source": "shipment_id"},
                    {"name": "original_items", "source": "items"},
                    {"name": "items", "array": [1, None, 2]},
                    {"name": "record", "struct": ["shipment_id", "metadata"]},
                ]}),
                "vortex_columns": ("shipment_id", "items", "metadata"),
                "vortex_predicate": "gte:shipment_id:7",
                "vortex_source_order_limit": 4_097,
            }
            _, partitioned, result = self._run(manifest, "expression_project", **options)
            _, unpartitioned, expected = self._run(single, "expression_project", **options)
            self.assertEqual(result.schema, expected.schema)
            self.assertEqual(result.num_rows, 4_097)
            self.assertEqual(result.to_pylist(), expected.to_pylist())
            exact = [{
                "shipment_id": value,
                "original_items": [value, value + 1],
                "items": [1, None, 2],
                "record": {"shipment_id": value, "metadata": {
                    "code": str(value), "rank": value % 13,
                }},
            } for value in range(7, 4_104)]
            self.assertEqual(result.to_pylist(), exact)
            session = self.native.Session(self.native.ExecutionResources(self.memory_bytes, self.max_parallelism))
            try:
                plan = session.workflow(self._primitive_args(manifest, "expression_project", options={
                    "--vortex-expression-projection": options["vortex_expression_projection"],
                    "--vortex-columns": "shipment_id,items,metadata",
                    "--vortex-predicate": options["vortex_predicate"],
                    "--vortex-source-order-limit": "4097",
                }))
                with plan.iter_batches() as stream:
                    actual = [row for batch in stream
                              for row in self.pa.RecordBatchReader.from_stream(batch).read_all().to_pylist()]
                self.assertEqual(actual, exact)
                output = self.root / "partitioned-structured-output.vortex"
                plan.write(output, "write_vortex")
                _, reopened, values = self._run(output, "project", vortex_columns="*")
                self.assertEqual(values.to_pylist(), exact)
                del reopened
            finally:
                session.close()
            del partitioned, unpartitioned

    def test_partitioned_sort_streams_selected_payload_in_global_order(self):
        row_ids = list(range(8_192, -1, -1))
        arrays = {"shipment_id": self.pa.array(row_ids, type=self.pa.int64())}
        for index in range(9):
            arrays[f"p{index}"] = self.pa.array(
                [None if row_id % 11 == 0 else row_id * 10 + index for row_id in row_ids],
                type=self.pa.int64(),
            )
        table = self.pa.table(arrays)

        parts_dir = self.root / "selected-sort-parts.vortex"
        parts_dir.mkdir()
        part_paths = [parts_dir / "a.vortex", parts_dir / "b.vortex"]
        self._write_table_fixture(part_paths[0], table.slice(0, 4_001))
        self._write_table_fixture(part_paths[1], table.slice(4_001))
        manifest = self.root / "selected-sort-parts.vortex-manifest"
        self._write_manifest(manifest, part_paths)

        fields = [f"p{index}" for index in range(9)]
        order = json.dumps(
            {
                "order_by": [{"column": "shipment_id", "descending": False}],
                "offset": 17,
                "limit": 4_097,
                "tie_policy": "first",
            },
            separators=(",", ":"),
        )
        options = {
            "--vortex-sort-rows": order,
            "--vortex-predicate": "gte:shipment_id:3",
            "--vortex-columns": ",".join(fields),
            "--vortex-source-order-limit": "4097",
        }
        expected = [
            {
                field: None if row_id % 11 == 0 else row_id * 10 + index
                for index, field in enumerate(fields)
            }
            for row_id in range(20, 4_117)
        ]

        with (
            patch("shardloom.client.subprocess.run", side_effect=AssertionError("process launch")),
            patch("shardloom.client.subprocess.Popen", side_effect=AssertionError("worker launch")),
        ):
            session = self.native.Session(
                self.native.ExecutionResources(self.memory_bytes, self.max_parallelism)
            )
            try:
                plan = session.workflow(
                    self._primitive_args(manifest, "sort_rows", options=options)
                )
                collected = plan.collect()
                collected_table = self.pa.RecordBatchReader.from_stream(collected).read_all()
                self.assertEqual(collected_table.schema.names, fields)
                self.assertEqual(
                    [field.type for field in collected_table.schema],
                    [self.pa.int64()] * len(fields),
                )
                self.assertEqual(collected_table.to_pylist(), expected)
                self.assertIn(
                    "partitioned_row_ref_final_k_materialization;reason=wide_output_projection",
                    collected.report["local_primitive_late_materialization_status"],
                )
                del collected, collected_table

                with plan.iter_batches() as stream:
                    streamed = [
                        row
                        for batch in stream
                        for row in self.pa.RecordBatchReader.from_stream(batch)
                        .read_all()
                        .to_pylist()
                    ]
                self.assertEqual(len(streamed), 4_097)
                self.assertEqual(streamed, expected)

                output = self.root / "selected-sort-output.vortex"
                plan.write(output, "write_vortex")
                _, reopened, persisted = self._run(output, "project", vortex_columns="*")
                self.assertEqual(persisted.schema.names, fields)
                self.assertEqual(persisted.to_pylist(), expected)
                del reopened
            finally:
                session.close()


if __name__ == "__main__":
    unittest.main()
