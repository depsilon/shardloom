"""Integration contract for the optional in-process native Python binding."""

from __future__ import annotations

import importlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
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
    # Import the package so the explicitly named extension has its normal parent.
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


def _persisted_native_job(mode: str, profile: str, destination: str) -> dict:
    """One real process in the durable write/reopen integration contract."""
    native = _load_native()
    import shardloom as sl
    import pyarrow as pa

    count = 24_009 if profile == "spill" else 129
    ids = list(range(count))
    table = pa.table({
        "id": pa.array(ids, type=pa.int64()),
        "label": pa.array([None if n % 7 == 0 else f"港-{n}" for n in ids]),
    })
    if profile == "typed":
        from decimal import Decimal
        table = table.append_column("items", pa.array(
            [None if n % 5 == 0 else [n, None, -n] for n in ids], pa.list_(pa.int32())))
        table = table.append_column("record", pa.array(
            [None if n % 3 == 0 else {"value": n} for n in ids],
            pa.struct([pa.field("value", pa.int32())])))
        table = table.append_column("instant", pa.array(
            [(-(1 << 63), None, (1 << 63) - 1)[n % 3] for n in ids],
            pa.int64()).cast(pa.timestamp("us")))
        table = table.append_column("amount", pa.array(
            [None if n % 5 == 0 else Decimal(n).scaleb(-2) for n in ids],
            pa.decimal128(12, 2)))
    length = 24_001 if profile == "spill" else 33
    expected = table.slice(7, length)
    if profile == "empty":
        expected = table.slice(0, 0)
    elif profile == "spill":
        expected = expected.take(pa.array(list(reversed(range(length)))))
    path = Path(destination)
    client = sl.ShardLoomClient(transport="native", memory_bytes=64 << 20, max_parallelism=1)
    try:
        # Child processes are test job boundaries. Execution inside each job must
        # use the shared native runtime, including the external-input adapter.
        with patch("subprocess.Popen", side_effect=AssertionError("execution child process")):
            if mode == "write":
                if profile in {"streamed", "spill"}:
                    frame = sl.from_batches(
                        lambda: iter(table.to_batches(max_chunksize=257)),
                        schema={"id": "int64", "label": "utf8"},
                        streaming=True, client=client,
                    )
                elif profile == "external":
                    import pyarrow.parquet as parquet
                    external = path.with_suffix(".parquet")
                    parquet.write_table(table, external)
                    frame = sl.read_parquet(external, client=client)
                else:
                    frame = sl.from_arrow_table(table, client=client)
                    if profile == "source":
                        source_path = path.with_name("input.vortex")
                        frame.write_vortex(source_path, check=True)
                        frame = sl.read_vortex(source_path, client=client)
                    elif profile == "completed":
                        frame = sl.from_arrow_table(frame.collect(check=True), client=client)
                frame = frame.where("id < 0" if profile == "empty" else "id >= 7").limit(length)
                options = {}
                if profile == "spill":
                    frame = frame.sort("id", descending=True)
                    options["spill"] = {"workspace": str(path.parent),
                                        "quota_bytes": 16 << 20, "buffer_bytes": 1 << 20}
                result = frame.write_vortex(path, check=True, **options)
            elif mode == "aggregate":
                result = sl.sql(
                    f"SELECT COUNT(*) AS n, MIN(id) AS lo, MAX(id) AS hi, "
                    f"COUNT(id) AS ids FROM '{path}'", client=client,
                ).collect(check=True)
                expected_ids = expected["id"].to_pylist()
                actual = result.to_arrow().to_pylist()
                wanted = [{"n": len(expected_ids), "lo": min(expected_ids, default=None),
                           "hi": max(expected_ids, default=None),
                           "ids": len(expected_ids)}]
                if actual != wanted:
                    raise AssertionError((actual, wanted))
            elif mode == "filter":
                threshold = 7 + length - 3
                result = sl.sql(
                    f"SELECT label, id FROM '{path}' WHERE id >= {threshold} ORDER BY id DESC",
                    client=client,
                ).collect(check=True)
                wanted = sorted(
                    [row for row in expected.select(["label", "id"]).to_pylist()
                     if row["id"] >= threshold], key=lambda row: row["id"], reverse=True,
                )
                actual = result.to_arrow().to_pylist()
                if actual != wanted:
                    raise AssertionError((actual, wanted))
            elif mode == "values":
                result = sl.read_vortex(path, client=client).collect(check=True)
                actual = result.to_arrow()
                if not actual.equals(expected, check_metadata=True):
                    raise AssertionError((actual.schema, expected.schema, actual.num_rows))
            else:
                raise AssertionError(f"unknown persisted test job: {mode}")
            raw = result.envelope.raw
            if raw["fallback"]["attempted"] or raw["fallback"]["allowed"]:
                raise AssertionError("native persisted job attempted fallback")
            fields = {field["key"]: field["value"] for field in
                      [*raw.get("fields", []), *raw.get("result", {}).get("fields", [])]}
            return {"pid": os.getpid(), "mode": mode, "profile": profile,
                    "expected_rows": expected.num_rows, "fields": fields,
                    "library": str(Path(native.__file__).resolve()),
                    "schema": str(expected.schema)}
    finally:
        client.close()


class NativeBindingTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.native = _load_native()

    def resources(self, memory_bytes: int = 32 << 20, lanes: int = 2, **kwargs):
        return self.native.ExecutionResources(memory_bytes, lanes, **kwargs)

    def session(self, memory_bytes: int = 32 << 20, lanes: int = 2):
        return self.native.Session(self.resources(memory_bytes, lanes))

    def command_args(
        self,
        path: Path,
        primitive: str,
        *,
        columns: str | None = None,
        memory_bytes: int | None = None,
        max_parallelism: int | None = None,
    ) -> list[str]:
        family = "count" if primitive == "count" else "filter_project_limit"
        args = [
            "run",
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
            "--native-vortex-operation-family",
            family,
            "--vortex-primitive",
            primitive,
        ]
        if columns is not None:
            args.extend(["--vortex-columns", columns])
        if memory_bytes is not None:
            args.extend(["--memory-bytes", str(memory_bytes)])
        if max_parallelism is not None:
            args.extend(["--max-parallelism", str(max_parallelism)])
        return args

    @staticmethod
    def command_fields(envelope: dict) -> dict[str, str]:
        result = envelope.get("result", {})
        fields = [*envelope.get("fields", []), *result.get("fields", [])]
        return {field["key"]: field["value"] for field in fields}

    def command_envelope(self, session, args: list[str], *, timeout: float | None = None) -> dict:
        return json.loads(session.command(args, timeout=timeout))

    def assert_no_command_fallback(self, envelope: dict) -> None:
        self.assertFalse(envelope["fallback"]["attempted"], envelope)
        self.assertFalse(envelope["fallback"]["allowed"], envelope)
        self.assertIsNone(envelope["fallback"]["engine"], envelope)

    def write_vortex_fixture(self, session, directory: str, rows: list[dict]) -> Path:
        path = Path(directory) / "command-input.vortex"
        source = session.from_rows(rows)
        plan = session.sql(
            "SELECT value, metric FROM 'memory://command-fixture' ORDER BY value",
            sources=[("memory://command-fixture", source)],
        )
        plan.write(path, "write_vortex")
        return path

    def arrow(self):
        try:
            import pyarrow as pa
        except ImportError:
            _unavailable("optional PyArrow dependency is not installed")
        return pa

    def test_extension_exports_expected_binding_types(self):
        for name in ("ExecutionResources", "Session", "Source", "Plan", "Result", "ColumnarStream"):
            with self.subTest(name=name):
                self.assertTrue(hasattr(self.native, name))
        self.assertEqual(self.native.execution_provider, "shared_shardloom_native")

    def test_execution_resources_require_exact_positive_integer_declarations(self):
        resources = self.resources((1 << 64) - 1, 1)
        self.assertEqual(resources.memory_bytes, (1 << 64) - 1)
        self.assertEqual(resources.max_parallelism, 1)

        for args in (
            (),
            (1024,),
            (True, 1),
            (1024, True),
            (1.5, 1),
            (1024, 1.5),
            ("1024", 1),
            (1024, "1"),
            (0, 1),
            (1024, 0),
            (-1, 1),
            (1024, -1),
            (1 << 64, 1),
            (1024, 1 << 64),
        ):
            with self.subTest(args=args), self.assertRaises((TypeError, ValueError)):
                self.native.ExecutionResources(*args)

        for kwargs in (
            {"memory_origin": "unknown"},
            {"parallelism_origin": "unknown"},
            {"memory_limit_bytes": 0},
            {"memory_limit_bytes": True},
            {"memory_limit_bytes": 1.5},
            {"memory_limit_bytes": "1024"},
            {"memory_limit_bytes": 1024, "parallelism_limit": 0},
            {"memory_limit_bytes": 1024, "parallelism_limit": True},
            {"memory_limit_bytes": 8, "parallelism_limit": 1},
        ):
            with self.subTest(kwargs=kwargs), self.assertRaises((TypeError, ValueError)):
                self.resources(1024, 1, **kwargs)

        declared = self.resources(
            1024,
            1,
            memory_origin="context",
            parallelism_origin="session",
            memory_limit_bytes=2048,
            parallelism_limit=2,
        )
        self.assertEqual(declared.memory_bytes, 1024)
        self.assertEqual(declared.max_parallelism, 1)

    def test_sql_declaration_is_lazy_and_missing_source_fails_without_external_engine(self):
        session = self.session()
        plan = session.sql(
            "SELECT * FROM missing_native_file",
            files=[("missing_native_file", "parquet", None)],
        )
        self.assertEqual(session.snapshot()["prepared_source_opens"], 0)
        with self.assertRaises(RuntimeError) as raised:
            plan.collect()
        self.assertTrue(
            any(token in str(raised.exception).lower() for token in ("source", "file", "path", "open")),
            str(raised.exception),
        )
        self.assertIn("no fallback execution was attempted", str(raised.exception).lower())
        self.assertFalse(raised.exception.external_engine_invoked)

    def test_range_result_export_and_prepared_declaration_reuse(self):
        pa = self.arrow()
        session = self.session(64 << 20, 2)
        source = session.from_range("id", 3, 2, 17_003)
        plan = session.sql(
            "SELECT id FROM 'memory://range' WHERE id >= 9 ORDER BY id",
            sources=[("memory://range", source)],
        )

        first = plan.collect()
        self.assertEqual(first.report["resident_relational_declaration_reused"], "false")
        first_rows = pa.RecordBatchReader.from_stream(first).read_all().to_pylist()
        expected = [{"id": 3 + 2 * index} for index in range(17_003) if 3 + 2 * index >= 9]
        self.assertEqual(first_rows, expected)

        second = plan.collect()
        self.assertEqual(second.report["resident_relational_declaration_reused"], "true")
        self.assertEqual(pa.RecordBatchReader.from_stream(second).read_all().to_pylist(), expected)

    def test_public_native_columnar_workflow_retains_results_and_preparation(self):
        import shardloom as sl

        pa = self.arrow()
        table = pa.table({"id": [1, 3, 5, 7], "label": ["one", None, "five", "seven"]})
        ctx = sl.context(transport="native", memory_bytes=64 << 20, max_parallelism=2)
        expected = [{"id": 3, "label": None}, {"id": 5, "label": "five"}, {"id": 7, "label": "seven"}]
        with (
            patch("shardloom.client.subprocess.run", side_effect=AssertionError("process launch")),
            patch("shardloom.client.subprocess.Popen", side_effect=AssertionError("worker launch")),
            patch("shardloom.query._arrow_table_like_records", side_effect=AssertionError("input rows")),
            patch("shardloom.query.arrow_table", side_effect=AssertionError("output row bridge")),
            patch("shardloom.query.python_rows", side_effect=AssertionError("Python row conversion")),
        ):
            frame = ctx.from_arrow_table(table).filter(sl.col("id") >= 3).sort("id")
            first = frame.collect(check=True)
            self.assertIsNotNone(first.native_result)
            self.assertEqual(first.to_arrow().to_pylist(), expected)
            self.assertEqual(first.envelope.field("python_execution_transport"), "native")
            self.assertIsNone(first.envelope.field("result_values_json"))
            self.assertEqual(first.envelope.field("resident_relational_declaration_reused"), "false")
            second = frame.collect(check=True)
            self.assertEqual(second.envelope.field("resident_relational_declaration_reused"), "true")
            self.assertEqual(pa.RecordBatchReader.from_stream(second).read_all().to_pylist(), expected)
            composed = ctx.from_arrow_table(second)
            self.assertEqual(composed.source.native_input.input_columnar_buffer_bytes_copied, 0)
            self.assertEqual(composed.select("id").to_arrow().to_pylist(), [{"id": 3}, {"id": 5}, {"id": 7}])
            ctx.client.close()
            self.assertEqual(first.to_arrow().to_pylist(), expected)
        self.assertEqual(first.python_objects, tuple(expected))
        self.assertEqual(first.result_rows, tuple(expected))

    def test_public_native_resource_refusal_precedes_loading_and_producer(self):
        import shardloom as sl

        class Producer:
            def __arrow_c_stream__(self, requested_schema=None):
                raise AssertionError("producer was inspected before resource admission")

        with patch("shardloom._native_backend.load_native", side_effect=AssertionError("extension load")):
            ctx = sl.context(transport="native")
            lazy = ctx.range(0, 3)
            self.assertIsNone(ctx.resources)
            with self.assertRaises(sl.ShardLoomResourceConfigurationError):
                lazy.collect()
            with self.assertRaises(sl.ShardLoomResourceConfigurationError):
                ctx.from_arrow_table(Producer())
            limited = sl.context(transport="native", resource_limits=sl.ExecutionResourceLimits(memory_bytes=4096))
            with self.assertRaises(sl.ShardLoomResourceConfigurationError):
                limited.from_arrow_table(Producer(), resources=sl.ExecutionResources(8192, 1))

    def test_public_native_frames_compose_across_clients(self):
        import shardloom as sl

        left = sl.context(transport="native", memory_bytes=64 << 20, max_parallelism=2)
        right = sl.context(transport="native", memory_bytes=64 << 20, max_parallelism=2)
        local = left.from_rows([{"id": 1}, {"id": 3}])
        foreign = right.from_rows([{"id": 3}, {"id": 5}])
        right.client.close()
        cases = [
            (local.union_all(foreign).sort("id"), [{"id": 1}, {"id": 3}, {"id": 3}, {"id": 5}]),
            (local.join(foreign, on="id").select("f.id AS id"), [{"id": 3}]),
            (local.filter(sl.col("id").isin_source(foreign, "id")), [{"id": 3}]),
        ]
        with (
            patch("shardloom.client.subprocess.run", side_effect=AssertionError("process launch")),
            patch("shardloom.client.subprocess.Popen", side_effect=AssertionError("worker launch")),
            patch("shardloom.query._arrow_table_like_records", side_effect=AssertionError("input rows")),
            patch("shardloom.query.arrow_table", side_effect=AssertionError("output row bridge")),
            patch("shardloom.query.python_rows", side_effect=AssertionError("Python row conversion")),
        ):
            for workflow, expected in cases:
                with self.subTest(operation=workflow.operation_summary):
                    first = workflow.collect(check=True)
                    self.assertEqual(first.to_arrow().to_pylist(), expected)
                    repeated = workflow.collect(check=True)
                    self.assertEqual(repeated.to_arrow().to_pylist(), expected)
                    self.assertEqual(repeated.envelope.field("resident_relational_declaration_reused"), "true")
        limited = sl.context(
            transport="native", memory_bytes=32 << 20, max_parallelism=1,
            resource_limits=sl.ExecutionResourceLimits(memory_bytes=32 << 20, max_parallelism=1),
        )
        limited_source = limited.from_rows([{"id": 9}])
        with self.assertRaises(sl.ShardLoomResourceConfigurationError):
            local.union_all(limited_source)
        constrained = limited_source.union_all(local).sort("id")
        with self.assertRaises(sl.ShardLoomResourceConfigurationError):
            constrained.collect(memory_bytes=64 << 20, max_parallelism=2, check=True)
        admitted = constrained.collect(memory_bytes=32 << 20, max_parallelism=1, check=True)
        self.assertEqual(admitted.to_arrow().to_pylist(), [{"id": 1}, {"id": 3}, {"id": 9}])
        limited.client.close()
        left.client.close()

    def test_native_source_transfer_preserves_rich_values_and_lifetimes(self):
        pa = self.arrow()
        producer = self.session(64 << 20)
        receiver = self.session(64 << 20)
        schema = pa.schema([
            pa.field("id", pa.int64()),
            pa.field("binary", pa.binary()),
            pa.field("items", pa.list_(pa.int16())),
            pa.field("record", pa.struct([pa.field("text", pa.string())])),
            pa.field("instant", pa.timestamp("us")),
        ])
        table = pa.Table.from_arrays([
            pa.array([-(1 << 63), None, (1 << 63) - 1], type=pa.int64()),
            pa.array([b"\x00\xff", None, b"last"], type=pa.binary()),
            pa.array([[-32768, None, 32767], None, []], type=pa.list_(pa.int16())),
            pa.array([{"text": "λ\x00猫"}, None, {"text": None}], type=schema.field("record").type),
            pa.array([-(1 << 63), None, (1 << 63) - 1], type=pa.int64()).cast(pa.timestamp("us")),
        ], schema=schema)
        source = producer.from_arrow(table)
        imported = receiver.from_arrow(source)
        self.assertGreater(imported.input_columnar_buffer_bytes_copied, 0)
        self.assertEqual(imported.schema_json, source.schema_json)
        self.assertEqual(imported.row_count, 3)
        producer.close()
        del source, producer
        plan = receiver.plan(self.native.Relation.scan("memory://transferred"), sources=[("memory://transferred", imported)])
        result = plan.collect()
        del plan, imported
        receiver.close()
        restored = pa.RecordBatchReader.from_stream(result).read_all()
        self.assertTrue(restored.equals(table), (restored.schema, table.schema))

    def test_native_ipc_public_frontdoors_preserve_all_values_and_reuse_the_source(self):
        import gc
        import shardloom as sl

        fixtures = Path(__file__).resolve().parents[2] / "shardloom-vortex/tests/fixtures/independent-vortex-0.85"
        expected = [{
            "key": (1 << 52) + row,
            "amount": None if row % 7 == 0 else -10.5 + row / 4.0,
            "flag": None if row % 11 == 0 else row % 2 == 0,
            "text": None if row % 17 == 0 else "" if row % 13 == 0 else f"row-{row}-λ\0猫",
        } for row in range(4099)]
        for frontdoor in ("module", "context"):
            with self.subTest(frontdoor=frontdoor):
                client = sl.ShardLoomClient(transport="native", memory_bytes=64 << 20, max_parallelism=1)
                self.addCleanup(client.close)
                ctx = sl.ShardLoomContext(client)
                array = (fixtures / "mixed.array.vxipc").read_bytes()
                dtype = (fixtures / "mixed.dtype.vxipc").read_bytes()
                if frontdoor == "module":
                    frame = sl.from_vortex_ipc(array, dtype_message=dtype, client=client)
                else:
                    frame = ctx.from_vortex_ipc(array, dtype_message=dtype)
                receipt = frame.source.native_input.intake_transfer_report
                self.assertEqual(receipt["ipc_transport_bytes"], len(array) + len(dtype))
                self.assertGreater(receipt["ipc_buffer_bytes_copied"], 0)
                self.assertGreater(receipt["encoded_buffer_bytes_copied"], 0)
                self.assertEqual(receipt["materialized_rows"], 0)
                del array, dtype
                gc.collect()
                session = client._native_backend.session
                before = session.snapshot()["completed_executions"]
                complete = frame.collect(check=True)
                self.assertEqual(complete.result_rows, tuple(expected))
                self.assertGreater(session.snapshot()["completed_executions"], before)
                first = frame.select("key").limit(7).collect(check=True)
                second = frame.filter(sl.col("key") >= (1 << 52) + 4094).select("key", "text").collect(check=True)
                self.assertEqual(first.result_rows, tuple({"key": row["key"]} for row in expected[:7]))
                self.assertEqual(second.result_rows, tuple({"key": row["key"], "text": row["text"]} for row in expected[-5:]))
                self.assertEqual(frame.source.native_input.intake_transfer_report, receipt)
                self.assertEqual(frame.source.native_input.input_columnar_buffer_bytes_copied,
                                 receipt["ipc_buffer_bytes_copied"] + receipt["encoded_buffer_bytes_copied"])
                for result in (complete, first, second):
                    self.assertEqual(result.envelope.field("fallback_attempted"), "false")
                    self.assertEqual(result.envelope.field("external_engine_invoked"), "false")

    def test_native_ipc_denial_corruption_and_cancellation_release_the_receiving_grant(self):
        import gc

        fixtures = Path(__file__).resolve().parents[2] / "shardloom-vortex/tests/fixtures/independent-vortex-0.85"
        array = (fixtures / "encoded.array.vxipc").read_bytes()
        dtype = (fixtures / "encoded.dtype.vxipc").read_bytes()
        session = self.session(64 << 20, 1)
        self.addCleanup(session.close)
        for action in (
            lambda: session.from_vortex_ipc(array, dtype_message=dtype, resources=self.resources(1, 1)),
            lambda: session.from_vortex_ipc(array, dtype_message=dtype, timeout=0),
            lambda: session.from_vortex_ipc(array[:-1], dtype_message=dtype),
            lambda: session.from_vortex_ipc(array, dtype_message=dtype[:-1]),
        ):
            with self.assertRaises(RuntimeError):
                action()
            self.assertEqual(session.snapshot()["reserved_bytes"], 0)
        source = session.from_vortex_ipc(array, dtype_message=dtype)
        receipt = source.intake_transfer_report
        self.assertEqual(source.row_count, 65539)
        self.assertEqual(receipt["materialized_batches"], 0)
        self.assertLess(receipt["encoded_buffer_bytes_copied"], 256 << 10)
        plan = session.sql("SELECT constant, run_end, packed FROM 'memory://ipc' LIMIT 33",
                           sources=[("memory://ipc", source)])
        result = plan.collect()
        self.assertEqual(result.row_export()[0],
                         [{"constant": 11, "run_end": 0, "packed": row % 31} for row in range(33)])
        del array, dtype, plan, source
        session.close()
        self.assertGreater(session.snapshot()["reserved_bytes"], 0)
        self.assertEqual(result.row_export()[0][-1], {"constant": 11, "run_end": 0, "packed": 1})
        del result
        gc.collect()
        self.assertEqual(session.snapshot()["reserved_bytes"], 0)

    def test_native_ipc_never_invokes_implicit_message_conversions(self):
        import shardloom as sl

        class Untouched:
            def __bytes__(self):
                raise AssertionError("implicit bytes conversion")

            def __len__(self):
                raise AssertionError("producer inspection")

        client = sl.ShardLoomClient(transport="native", memory_bytes=32 << 20, max_parallelism=1)
        self.addCleanup(client.close)
        session = self.session()
        self.addCleanup(session.close)
        for value in (Untouched(), bytearray(b"invalid"), memoryview(b"invalid")):
            with self.assertRaises(TypeError):
                sl.from_vortex_ipc(value, dtype_message=b"invalid", client=client)
            with self.assertRaises(TypeError):
                session.from_vortex_ipc(value, dtype_message=b"invalid")
        self.assertEqual(session.snapshot()["reserved_bytes"], 0)

    def test_cross_client_binding_refusal_releases_target_copy_and_keeps_source(self):
        producer = self.session(64 << 20)
        source = producer.from_rows([{"id": n} for n in range(4099)])
        producer_before = producer.snapshot()["reserved_bytes"]
        receiver = self.session(64 << 20)
        receiver_before = receiver.snapshot()["reserved_bytes"]
        plan = receiver.sql("SELECT id FROM 'memory://foreign'", sources=[("memory://foreign", source)])
        with self.assertRaises(RuntimeError):
            plan.collect(timeout=0)
        self.assertEqual(receiver.snapshot()["reserved_bytes"], receiver_before)
        with self.assertRaises(RuntimeError):
            plan.collect(resources=self.resources(32768, 1))
        self.assertEqual(receiver.snapshot()["reserved_bytes"], 0)
        self.assertEqual(producer.snapshot()["reserved_bytes"], producer_before)
        result = plan.collect()
        self.assertEqual(result.row_count, 4099)
        self.assertEqual(result.row_export()[0], [{"id": n} for n in range(4099)])
        producer.close()
        self.assertEqual(plan.collect().row_export()[0], [{"id": n} for n in range(4099)])

    def test_cross_client_native_result_import_does_not_reenter_producer(self):
        producer = self.session(64 << 20)
        source = producer.from_range("id", 0, 1, 4099)
        plan = producer.sql("SELECT id FROM 'memory://original'", sources=[("memory://original", source)])
        completed = plan.collect()
        receiver = self.session(64 << 20)
        with plan.iter_batches() as busy:
            self.assertGreater(next(busy).row_count, 0)
            # The producer's grant is held by the unfinished stream. Transfer
            # must read immutable native arrays without opening its Arrow stream.
            imported = receiver.from_arrow(completed, timeout=2)
            self.assertGreater(imported.input_columnar_buffer_bytes_copied, 0)
            received = receiver.plan(self.native.Relation.scan("memory://received"), sources=[("memory://received", imported)])
            self.assertEqual(received.collect().row_export()[0], [{"id": n} for n in range(4099)])
        compact = receiver.from_arrow(producer.from_range("id", -(1 << 63), 1, 1_000_017))
        self.assertEqual(compact.row_count, 1_000_017)
        self.assertEqual(compact.input_columnar_buffer_bytes_copied, 0)
        producer.close()
        del source, plan, completed, producer
        self.assertEqual(received.collect().row_export()[0], [{"id": n} for n in range(4099)])

    def test_public_native_columnar_refusal_does_not_probe_producer_attributes(self):
        import shardloom as sl

        inspected = []

        class ProducerType(type):
            def __eq__(cls, other):
                inspected.append("type equality")
                raise AssertionError("unadmitted producer type comparison ran")

        class Producer(metaclass=ProducerType):
            @property
            def native_result(self):
                inspected.append("native_result")
                raise AssertionError("unadmitted producer attribute was inspected")

            @property
            def __class__(self):
                inspected.append("__class__")
                raise AssertionError("unadmitted producer class was inspected")

            def __arrow_c_stream__(self, requested_schema=None):
                inspected.append("__arrow_c_stream__")
                raise AssertionError("unadmitted producer was opened")

        expired = sl.ShardLoomClient(transport="native", timeout=0,
                                    memory_bytes=32 << 20, max_parallelism=1)
        self.addCleanup(expired.close)
        with self.assertRaisesRegex(RuntimeError, "deadline exceeded"):
            sl.from_arrow_table(Producer(), client=expired)

        ctx = sl.context(transport="native", memory_bytes=32 << 20, max_parallelism=1)
        self.addCleanup(ctx.client.close)
        retained = ctx.from_rows([{"id": 1}])
        self.assertIsNotNone(retained.source.native_input)
        self.assertGreater(ctx.client._native_backend.session.snapshot()["reserved_bytes"], 1)
        with self.assertRaisesRegex(RuntimeError, "memory allocation cannot cover retained owners"):
            ctx.from_arrow_table(Producer(), resources=sl.ExecutionResources(1, 1))
        self.assertEqual(inspected, [])

    def test_public_native_columnar_import_ignores_unrelated_native_result(self):
        import shardloom as sl

        pa = self.arrow()
        expected = [{"id": 2}, {"id": 7}, {"id": 11}]
        table = pa.table({"id": [2, 7, 11]})
        opened = []

        class ProducerWithAttribute:
            native_result = object()

            def __arrow_c_stream__(self, requested_schema=None):
                opened.append(type(self).__name__)
                return table.__arrow_c_stream__(requested_schema)

        class ProducerWithProperty(ProducerWithAttribute):
            @property
            def native_result(self):
                raise AssertionError("unrelated producer property was inspected")

        ctx = sl.context(transport="native", memory_bytes=32 << 20, max_parallelism=1)
        self.addCleanup(ctx.client.close)
        for producer_type in (ProducerWithAttribute, ProducerWithProperty):
            with self.subTest(producer=producer_type.__name__):
                frame = ctx.from_arrow_table(producer_type())
                self.assertEqual(frame.collect(check=True).to_arrow().to_pylist(), expected)
        self.assertEqual(opened, ["ProducerWithAttribute", "ProducerWithProperty"])

    def test_public_native_incremental_results_keep_owners_after_close(self):
        import shardloom as sl

        ctx = sl.context(transport="native", memory_bytes=64 << 20, max_parallelism=2)
        frame = ctx.range(0, 17, column="id")
        with patch("shardloom.client.subprocess.Popen", side_effect=AssertionError("worker launch")):
            with frame.iter_batches(batch_rows=3) as batches:
                self.assertIsNone(batches.report)
                retained = list(batches)
                self.assertEqual([len(batch.result_rows) for batch in retained], [3, 3, 3, 3, 3, 2])
                self.assertIsNotNone(batches.report)
                self.assertEqual(batches.report.envelope.status, "success")
                self.assertEqual(retained[0].result_schema[0][1].name, "int64")
            with frame.iter_batches(batch_rows=4) as partial:
                first = next(partial)
                self.assertIsNone(partial.report)
            self.assertIsNone(partial.report)
            composed = ctx.from_arrow_table(first).collect(check=True)
            self.assertEqual(composed.to_arrow().to_pylist(), [{"id": value} for value in range(4)])
        ctx.client.close()
        self.assertEqual(first.to_arrow().to_pylist(), [{"id": value} for value in range(4)])
        actual = [row for batch in retained for row in batch.to_arrow().to_pylist()]
        self.assertEqual(actual, [{"id": value} for value in range(17)])

    def test_public_native_writer_and_metadata_commands_never_launch_processes(self):
        import shardloom as sl

        ctx = sl.context(transport="native", memory_bytes=64 << 20, max_parallelism=2)
        with (
            tempfile.TemporaryDirectory(prefix="shardloom-native-public-") as directory,
            patch("shardloom.client.subprocess.run", side_effect=AssertionError("process launch")),
            patch("shardloom.client.subprocess.Popen", side_effect=AssertionError("worker launch")),
        ):
            self.assertEqual(ctx.client.status().status, "success")
            path = Path(directory) / "native.jsonl"
            report = ctx.range(2, 11, step=2, column="id").write_jsonl(path)
            self.assertEqual(report.envelope.status, "success")
            result = ctx.read_json(path, schema={"id": "int64"}).collect(check=True)
            self.assertEqual(result.python_objects, tuple({"id": value} for value in (2, 4, 6, 8, 10)))
            self.assertEqual(result.envelope.field("python_execution_transport"), "native")
        ctx.client.close()

    def test_retained_results_compose_as_sources_without_copying(self):
        pa = self.arrow()
        session = self.session(64 << 20, 2)
        source = session.from_range("id", 3, 2, 129)
        result = session.sql(
            "SELECT id FROM 'memory://range_result'",
            sources=[("memory://range_result", source)],
        ).collect()
        plan = session.sql(
            "SELECT id FROM 'memory://range' WHERE id >= 9 ORDER BY id",
            sources=[("memory://range", result.as_source())],
        )
        composed = plan.collect()
        expected = [{"id": value} for value in range(9, 261, 2)]
        self.assertEqual(pa.RecordBatchReader.from_stream(composed).read_all().to_pylist(), expected)

        same_session_source = session.from_arrow(result)
        self.assertEqual(same_session_source.input_columnar_buffer_bytes_copied, 0)
        copied_again = session.sql(
            "SELECT id FROM 'memory://retained' ORDER BY id",
            sources=[("memory://retained", same_session_source)],
        ).collect()
        self.assertEqual(
            pa.RecordBatchReader.from_stream(copied_again).read_all().to_pylist(),
            [{"id": 3 + 2 * index} for index in range(129)],
        )

        session.close()
        with self.assertRaises(RuntimeError):
            session.from_range("later", 0, 1, 1)
        # A retained result owns its buffers independently of the closed session.
        self.assertEqual(
            pa.RecordBatchReader.from_stream(result).read_all().to_pylist(),
            [{"id": 3 + 2 * index} for index in range(129)],
        )

    def test_arrow_import_copies_source_buffers_and_accepts_empty_batches(self):
        pa = self.arrow()
        import struct

        session = self.session(64 << 20, 2)
        backing = bytearray(struct.pack("<3q", 2, 7, 11))
        batch = pa.record_batch(
            [
                pa.Array.from_buffers(pa.int64(), 3, [None, pa.py_buffer(backing)]),
                pa.array([True, None, False], type=pa.bool_()),
                pa.array(["alpha", None, "λ"], type=pa.string()),
            ],
            names=["id", "enabled", "label"],
        )
        producer = pa.RecordBatchReader.from_batches(batch.schema, [batch])
        source = session.from_arrow(producer)
        struct.pack_into("<3q", backing, 0, -99, -99, -99)
        self.assertEqual(batch.column(0).to_pylist(), [-99, -99, -99])
        self.assertGreater(source.input_columnar_buffer_bytes_copied, 0)
        result = session.sql(
            "SELECT id, enabled, label FROM 'memory://input' ORDER BY id",
            sources=[("memory://input", source)],
        ).collect()
        self.assertEqual(
            pa.RecordBatchReader.from_stream(result).read_all().to_pylist(),
            [
                {"id": 2, "enabled": True, "label": "alpha"},
                {"id": 7, "enabled": None, "label": None},
                {"id": 11, "enabled": False, "label": "λ"},
            ],
        )

        empty_schema = pa.schema([("id", pa.int64()), ("label", pa.string())])
        empty_batch = pa.RecordBatch.from_arrays(
            [pa.array([], type=pa.int64()), pa.array([], type=pa.string())],
            schema=empty_schema,
        )
        empty_source = session.from_arrow(
            pa.RecordBatchReader.from_batches(empty_schema, [empty_batch])
        )
        self.assertEqual(empty_source.row_count, 0)

    def test_arrow_import_rejects_malformed_capsule_names(self):
        import ctypes

        session = self.session(64 << 20, 2)
        self.addCleanup(session.close)
        capsule_new = ctypes.pythonapi.PyCapsule_New
        capsule_new.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_void_p]
        capsule_new.restype = ctypes.py_object
        expected_error = (
            "Expected __arrow_c_stream__ to return a arrow_array_stream capsule."
        )

        for capsule_name in (None, b"wrong_stream", b"\xff"):
            with self.subTest(capsule_name=capsule_name):
                backing = ctypes.create_string_buffer(b"live payload")
                # PyCapsule keeps the name pointer, not a Python reference to the bytes.
                # Keep both the backing memory and the name alive across repeated calls.
                name_bytes = capsule_name
                capsule = capsule_new(
                    ctypes.cast(backing, ctypes.c_void_p), name_bytes, None
                )

                class Producer:
                    def __arrow_c_stream__(self, requested_schema=None):
                        return capsule

                producer = Producer()
                for _ in range(2):
                    reserved_before = session.snapshot()["reserved_bytes"]
                    with self.assertRaises(TypeError) as raised:
                        session.from_arrow(producer)
                    self.assertEqual(str(raised.exception), expected_error)
                    self.assertEqual(
                        session.snapshot()["reserved_bytes"], reserved_before
                    )

        later = session.from_range("after_malformed_capsules", 0, 1, 1)
        self.assertEqual(later.row_count, 1)

    def test_resource_override_admits_before_producer_and_does_not_replace_session_default(self):
        pa = self.arrow()
        session = self.session(32 << 20, 2)
        table = pa.table({"id": [1, 2, 3]})
        retained = session.from_range("held", 0, 1, 16)
        self.assertEqual(retained.row_count, 16)

        class Spy:
            def __init__(self):
                self.calls = 0

            def __arrow_c_stream__(self, requested_schema=None):
                self.calls += 1
                return table.__arrow_c_stream__(requested_schema)

        denied = Spy()
        with self.assertRaises(RuntimeError):
            session.from_arrow(denied, resources=self.resources(1, 2))
        self.assertEqual(denied.calls, 0)

        authorized = Spy()
        source = session.from_arrow(authorized, resources=self.resources(64 << 20, 1))
        self.assertEqual(authorized.calls, 1)
        self.assertEqual(source.row_count, 3)
        later = session.from_range("later", 0, 1, 4)
        self.assertEqual(later.row_count, 4)
        snapshot = session.snapshot()
        self.assertEqual(snapshot["memory_limit_bytes"], 32 << 20)
        self.assertEqual(snapshot["admitted_max_parallelism"], 2)

    def test_columnar_stream_is_one_shot_reports_completion_and_preserves_result(self):
        pa = self.arrow()
        session = self.session(64 << 20, 2)
        source = session.from_range("id", 1, 1, 257)
        result = session.sql(
            "SELECT id FROM 'memory://range_result'",
            sources=[("memory://range_result", source)],
        ).collect()

        stream = result.columnar_stream()
        before = stream.report
        self.assertFalse(before["complete"])
        self.assertEqual(before["output_rows"], 0)
        self.assertFalse(before["exported"])
        self.assertFalse(before["python_rows_constructed"])
        with self.assertRaises(ValueError):
            stream.__arrow_c_stream__(object())
        self.assertFalse(stream.report["exported"])

        rows = pa.RecordBatchReader.from_stream(stream).read_all().to_pylist()
        self.assertEqual(rows, [{"id": index} for index in range(1, 258)])
        after = stream.report
        self.assertTrue(after["complete"])
        self.assertTrue(after["exported"])
        self.assertFalse(after["python_rows_constructed"])
        self.assertEqual(after["output_rows"], 257)
        with self.assertRaises(ValueError):
            stream.__arrow_c_stream__()

        closed_stream = result.columnar_stream()
        closed_stream.close()
        with self.assertRaises(ValueError):
            closed_stream.__arrow_c_stream__()
        # Closing one export does not consume or invalidate the retained result.
        self.assertEqual(pa.RecordBatchReader.from_stream(result).read_all().num_rows, 257)

    def test_recursive_input_callback_is_rejected_without_poisoning_later_work(self):
        # A timeout is necessary because a regression here could wait forever on
        # the session's non-reentrant operation admission lock.
        library = os.environ.get("SHARDLOOM_NATIVE_TEST_LIBRARY")
        if not library:
            library = str(Path(self.native.__file__).resolve())
        env = os.environ.copy()
        env["SHARDLOOM_NATIVE_TEST_LIBRARY"] = library
        env["SHARDLOOM_REQUIRE_NATIVE_PYTHON"] = "1"
        script = r'''
import importlib, importlib.util, os, sys
from pathlib import Path
import pyarrow as pa
import shardloom
path = Path(os.environ["SHARDLOOM_NATIVE_TEST_LIBRARY"])
spec = importlib.util.spec_from_file_location("shardloom._native", path)
module = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = module
spec.loader.exec_module(module)
resources = module.ExecutionResources(64 << 20, 1)
session = module.Session(resources)
table = pa.table({"inner": [1, 2]})
observed = []
class RecursiveProducer:
    def __arrow_c_stream__(self, requested_schema=None):
        try:
            session.from_arrow(table)
        except RuntimeError as error:
            observed.append(str(error))
        else:
            raise AssertionError("recursive same-session call unexpectedly succeeded")
        return table.__arrow_c_stream__(requested_schema)
session.from_arrow(RecursiveProducer())
assert len(observed) == 1, observed
assert "recursive" in observed[0].lower(), observed[0]
later = session.from_arrow(table)
assert later.row_count == 2
'''
        completed = subprocess.run(
            [sys.executable, "-c", script],
            check=False,
            capture_output=True,
            text=True,
            timeout=20,
            env=env,
        )
        self.assertEqual(
            completed.returncode,
            0,
            msg=f"recursive callback subprocess failed:\n{completed.stdout}\n{completed.stderr}",
        )

    def test_rich_columnar_values_and_explicit_python_rows_preserve_exact_domains(self):
        import datetime as dt
        from decimal import Decimal, localcontext

        pa = self.arrow()
        session = self.session(64 << 20, 2)
        dates = [-(1 << 31), 0, (1 << 31) - 1]
        timestamps = [-(1 << 63), 0, (1 << 63) - 1]
        decimals = [Decimal("999999999999999999999999999999999999.99"), None, Decimal("-0.01")]
        nested = [{"n": 1, "s": "α"}, None, {"n": None, "s": ""}]
        batch = pa.record_batch([
            pa.array([-128, None, 127], type=pa.int8()),
            pa.array([0, None, (1 << 64) - 1], type=pa.uint64()),
            pa.array([b"\x00\xff", None, b""], type=pa.binary()),
            pa.array(decimals, type=pa.decimal128(38, 2)),
            pa.array(dates, type=pa.int32()).cast(pa.date32()),
            pa.array(timestamps, type=pa.int64()).cast(pa.timestamp("us")),
            pa.array([[1, None], None, []], type=pa.list_(pa.int16())),
            pa.array(nested, type=pa.struct([("n", pa.int16()), ("s", pa.string())])),
        ], names=["small", "wide", "payload", "amount", "date", "stamp", "items", "nested"])
        source = session.from_arrow(pa.RecordBatchReader.from_batches(batch.schema, [batch]))
        result = session.sql("SELECT * FROM 'memory://rich'", sources=[("memory://rich", source)]).collect()
        exported = pa.RecordBatchReader.from_stream(result).read_all()
        self.assertTrue(exported.equals(pa.Table.from_batches([batch])))
        receiver = self.session(64 << 20, 2)
        imported = receiver.from_arrow(result)
        transferred = receiver.sql("SELECT * FROM 'memory://rich-transfer'", sources=[("memory://rich-transfer", imported)]).collect()
        self.assertTrue(pa.RecordBatchReader.from_stream(transferred).read_all().equals(exported))
        with localcontext() as context:
            context.prec = 3  # Row conversion must not round exact Decimal128.
            rows, report = result.row_export()
        self.assertEqual([row["small"] for row in rows], [-128, None, 127])
        self.assertEqual([row["wide"] for row in rows], [0, None, (1 << 64) - 1])
        self.assertEqual([row["payload"] for row in rows], [b"\x00\xff", None, b""])
        self.assertEqual([row["amount"] for row in rows], decimals)
        self.assertEqual([row["date"] for row in rows], [dates[0], dt.date(1970, 1, 1), dates[2]])
        self.assertEqual([row["stamp"] for row in rows], [timestamps[0], dt.datetime(1970, 1, 1), timestamps[2]])
        self.assertEqual([row["items"] for row in rows], [[1, None], None, []])
        self.assertEqual([row["nested"] for row in rows], nested)
        self.assertTrue(report["complete"])
        self.assertTrue(report["python_rows_constructed"])
        self.assertEqual(report["output_rows"], 3)
        epoch_rows, _ = result.row_export(temporal_objects=False)
        self.assertEqual([row["date"] for row in epoch_rows], dates)
        self.assertEqual([row["stamp"] for row in epoch_rows], timestamps)
        from shardloom._result_schema import python_rows, schema_fields
        fields = schema_fields(result.schema_json, "vortex.dtype.serde.v1")
        self.assertEqual([name for name, _ in fields], batch.schema.names)
        self.assertEqual(dict(fields)["amount"].parameters, (38, 2))
        wire_rows, wire_report = result.row_export(wire_values=True)
        self.assertEqual([row["payload"] for row in wire_rows], ["00ff", None, ""])
        self.assertEqual([row["amount"] for row in wire_rows], [
            "decimal128(38,2):99999999999999999999999999999999999999",
            None, "decimal128(38,2):-1",
        ])
        self.assertEqual([row["date"] for row in wire_rows], dates)
        self.assertEqual([row["stamp"] for row in wire_rows], timestamps)
        self.assertEqual(python_rows(wire_rows, fields), rows)
        self.assertTrue(wire_report["complete"])

    def test_incremental_execution_waits_for_demand_and_reports_only_complete_results(self):
        pa = self.arrow()
        session = self.session(64 << 20, 2)
        source = session.from_range("id", 0, 1, 131_073)
        plan = session.sql("SELECT id FROM 'memory://stream'", sources=[("memory://stream", source)])
        before = session.snapshot()["completed_executions"]
        stream = plan.iter_batches()
        self.assertEqual(session.snapshot()["completed_executions"], before)
        self.assertIsNone(stream.report)
        first = next(stream)
        self.assertGreater(first.row_count, 0)
        self.assertLess(first.row_count, 131_073)
        self.assertEqual(first.schema, [("id", {"type": "int64", "nullable": False})])
        sliced = first.slice(2, 3)
        self.assertEqual(sliced.row_count, 3)
        self.assertEqual(sliced.row_export(wire_values=True), [{"id": i} for i in range(2, 5)])
        self.assertEqual(sliced.schema, first.schema)
        self.assertEqual(first.slice(first.row_count, 0).row_count, 0)
        for offset, length in ((first.row_count, 1), (first.row_count + 1, 0)):
            with self.assertRaises(ValueError):
                first.slice(offset, length)
        self.assertIsNone(stream.report)
        with self.assertRaisesRegex(RuntimeError, "incremental consumer"):
            session.from_range("interleaved", 0, 1, 1)
        with self.assertRaisesRegex(RuntimeError, "prepared plan is executing"):
            plan.release_prepared()
        observed = pa.RecordBatchReader.from_stream(first).read_all()["id"].to_pylist()
        for batch in stream:
            observed.extend(pa.RecordBatchReader.from_stream(batch).read_all()["id"].to_pylist())
        self.assertEqual(observed, list(range(131_073)))
        report = stream.report
        self.assertIsNotNone(report)
        self.assertTrue(report["columnar_export"]["complete"])
        self.assertEqual(report["columnar_export"]["output_rows"], 131_073)
        self.assertFalse(report["columnar_export"]["python_rows_constructed"])
        self.assertIsNone(next(stream, None))
        stream.close()
        self.assertEqual(session.from_range("later", 0, 1, 1).row_count, 1)

    def test_incremental_close_and_session_cancel_keep_retained_batches_valid(self):
        pa = self.arrow()
        session = self.session(64 << 20, 2)
        source = session.from_range("id", 0, 1, 131_073)
        plan = session.sql("SELECT id FROM 'memory://stream'", sources=[("memory://stream", source)])
        stream = plan.iter_batches()
        first = next(stream)
        stream.close()
        self.assertIsNone(stream.report)
        self.assertIsNone(next(stream, None))
        self.assertEqual(first.row_export(), [{"id": i} for i in range(first.row_count)])
        self.assertEqual(session.from_range("later", 0, 1, 1).row_count, 1)

        cancelled = plan.iter_batches()
        batch = next(cancelled)
        session.cancel()
        with self.assertRaisesRegex(RuntimeError, "cancelled"):
            next(cancelled)
        self.assertIsNone(cancelled.report)
        self.assertEqual(pa.RecordBatchReader.from_stream(batch).read_all().num_rows, batch.row_count)
        self.assertEqual(session.from_range("after_cancel", 0, 1, 1).row_count, 1)

        unopened = plan.iter_batches()
        session.close()
        with self.assertRaisesRegex(RuntimeError, "cancelled"):
            next(unopened)
        unopened.close()
        self.assertIsNone(unopened.report)

    def test_native_deadlines_reject_before_input_or_output_and_allow_session_reuse(self):
        session = self.session(64 << 20, 2)

        class Untouched:
            def __iter__(self):
                raise AssertionError("timed-out input was consumed")

            def __arrow_c_stream__(self, requested_schema=None):
                raise AssertionError("timed-out columnar producer was called")

        for operation in (lambda: session.from_rows(Untouched(), timeout=0),
                          lambda: session.from_arrow(Untouched(), timeout=0),
                          lambda: session.from_range("id", 0, 1, 3, timeout=0)):
            with self.assertRaisesRegex(RuntimeError, "deadline exceeded"):
                operation()
        self.assertEqual(session.snapshot()["reserved_bytes"], 0)
        missing = session.sql("SELECT * FROM missing", files=[("missing", "parquet", None)])
        for operation in (missing.prepare, missing.collect):
            with self.assertRaisesRegex(RuntimeError, "deadline exceeded"):
                operation(timeout=0)
        self.assertEqual(session.snapshot()["prepared_source_opens"], 0)

        source = session.from_range("id", 1, 1, 3)
        plan = session.sql("SELECT id FROM 'memory://deadline'", sources=[("memory://deadline", source)])
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / "uncreated" / "output.vortex"
            with self.assertRaisesRegex(RuntimeError, "deadline exceeded"):
                plan.write(target, "write_vortex", timeout=0)
            self.assertFalse(target.parent.exists())
        with plan.iter_batches(timeout=0) as stream:
            with self.assertRaisesRegex(RuntimeError, "deadline exceeded"):
                next(stream)
            self.assertIsNone(stream.report)
        rows, _ = plan.collect().row_export()
        self.assertEqual(rows, [{"id": 1}, {"id": 2}, {"id": 3}])

    def test_native_stream_deadline_starts_on_demand_and_expires_during_consumer_wait(self):
        import time

        session = self.session(64 << 20, 2)
        source = session.from_range("id", 0, 1, 4097)
        plan = session.sql("SELECT id FROM 'memory://deadline'", sources=[("memory://deadline", source)])
        plan.prepare()
        with plan.iter_batches(timeout=0.25) as stream:
            time.sleep(0.3)  # A declaration has not started the operation clock.
            retained = next(stream)
            expected = [{"id": index} for index in range(retained.row_count)]
            time.sleep(0.3)  # An admitted operation keeps its deadline while idle.
            with self.assertRaisesRegex(RuntimeError, "deadline exceeded"):
                next(stream)
            self.assertIsNone(stream.report)
        self.assertEqual(retained.row_export(), expected)
        self.assertEqual(session.from_range("after_timeout", 0, 1, 1).row_count, 1)

    def test_public_native_timeout_validation_and_no_consumption(self):
        import shardloom as sl
        from shardloom.errors import ShardLoomCommandError

        invalid = (True, "1", -1, float("nan"), float("inf"))
        session = self.session()
        plan = session.sql("SELECT * FROM missing", files=[("missing", "parquet", None)])
        for timeout in invalid:
            with self.subTest(timeout=timeout):
                with self.assertRaises((TypeError, ValueError)):
                    sl.ShardLoomClient(transport="native", timeout=timeout)
                with self.assertRaises((TypeError, ValueError)):
                    plan.collect(timeout=timeout)
        client = sl.ShardLoomClient(transport="native", timeout=0,
                                   memory_bytes=32 << 20, max_parallelism=1)
        self.addCleanup(client.close)
        opened = []

        def producer():
            opened.append(True)
            yield [{"id": 1}]

        frame = sl.from_batches(producer, schema={"id": "int64"}, streaming=True, client=client)
        with self.assertRaisesRegex(ShardLoomCommandError, "deadline exceeded"):
            frame.collect(check=True)
        with frame.iter_batches() as stream:
            with self.assertRaisesRegex(TimeoutError, "deadline exceeded"):
                next(stream)
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / "uncreated.vortex"
            with self.assertRaisesRegex(ShardLoomCommandError, "deadline exceeded"):
                frame.write(target, output_format="vortex")
            self.assertFalse(target.exists())
        self.assertEqual(opened, [])

    def test_public_native_cancel_drains_producer_and_preserves_results_and_reuse(self):
        import shardloom as sl
        import threading

        client = sl.ShardLoomClient(transport="native", memory_bytes=64 << 20, max_parallelism=2)
        self.addCleanup(client.close)
        retained = sl.from_rows([{"id": 7}], schema={"id": "int64"}, client=client).collect(check=True)
        entered, release = threading.Event(), threading.Event()
        closed, outcomes = [], []

        def producer():
            try:
                entered.set()
                if not release.wait(5):
                    raise AssertionError("test did not release its producer")
                yield [{"id": 1}]
                raise AssertionError("cancelled operation requested another batch")
            finally:
                closed.append(True)

        frame = sl.from_batches(producer, schema={"id": "int64"}, streaming=True, client=client)

        def execute():
            try:
                outcomes.append(frame.collect(check=True))
            except BaseException as error:
                outcomes.append(error)

        worker = threading.Thread(target=execute)
        worker.start()
        try:
            self.assertTrue(entered.wait(5), "producer was not opened")
            client.cancel()
        finally:
            release.set()
            worker.join(10)
        self.assertFalse(worker.is_alive(), "cancelled operation did not drain")
        self.assertEqual(closed, [True])
        self.assertEqual(len(outcomes), 1)
        self.assertIsInstance(outcomes[0], Exception)
        self.assertIn("cancelled", str(outcomes[0]))
        self.assertEqual(retained.to_arrow()["id"].to_pylist(), [7])
        later = sl.from_rows([{"id": 9}], schema={"id": "int64"}, client=client).collect(check=True)
        self.assertEqual(later.to_arrow()["id"].to_pylist(), [9])

    def test_native_writer_outputs_reopen_all_complete_values(self):
        session = self.session(64 << 20, 2)
        source = session.from_range("id", -4, 3, 129)
        plan = session.sql("SELECT id FROM 'memory://writers'", sources=[("memory://writers", source)])
        expected = [{"id": -4 + 3 * index} for index in range(129)]
        with tempfile.TemporaryDirectory(prefix="shardloom-native-writers-") as directory:
            for output in ("vortex", "parquet", "arrow_ipc", "csv", "json", "jsonl", "avro", "orc"):
                with self.subTest(format=output):
                    path = Path(directory) / ("values." + output)
                    report = plan.write(path, "write_" + output)
                    self.assertTrue(path.is_file())
                    self.assertIsInstance(report, dict)
                    reader = session.sql(
                        f"SELECT id FROM '{path}'",
                        files=[(str(path), output.replace("_", "-"),
                                "id:int64" if output in {"csv", "json", "jsonl"} else None)],
                    )
                    reopened = reader.collect()
                    rows, observation = reopened.row_export()
                    self.assertEqual(rows, expected)
                    self.assertTrue(observation["complete"])
                    with self.assertRaises(RuntimeError):
                        plan.write(path, "write_" + output)

    def test_public_native_persisted_files_reopen_in_independent_jobs(self):
        import shardloom as sl
        import shutil

        self.arrow()
        test_path = str(Path(__file__).resolve())
        package_root = str(Path(sl.__file__).resolve().parent.parent)
        library = str(Path(self.native.__file__).resolve())
        command = (
            "import json, os, runpy, sys; sys.path.insert(0, sys.argv[1]); "
            "os.environ['SHARDLOOM_NATIVE_TEST_LIBRARY'] = sys.argv[3]; "
            "os.environ['SHARDLOOM_REQUIRE_NATIVE_PYTHON'] = '1'; "
            "job = runpy.run_path(sys.argv[2], run_name='native_persisted_job'); "
            "print(json.dumps(job['_persisted_native_job'](*sys.argv[4:])))"
        )

        def run(mode, profile, path):
            process = subprocess.run(
                [sys.executable, "-I", "-c", command, package_root, test_path,
                 library, mode, profile, str(path)],
                capture_output=True, text=True, timeout=120, check=False,
            )
            self.assertEqual(process.returncode, 0, process.stderr)
            report = json.loads(process.stdout)
            self.assertEqual(report["library"], library)
            self.assertNotEqual(report["pid"], os.getpid())
            return report

        for profile in ("produced", "completed", "source", "external", "streamed", "spill", "empty", "typed"):
            with self.subTest(profile=profile), tempfile.TemporaryDirectory(
                prefix="shardloom-persisted-jobs-"
            ) as directory:
                root = Path(directory)
                output = root / "produced.vortex"
                writer = run("write", profile, output)
                moved = root / "reopened.vortex"
                if profile in {"completed", "external", "typed"}:
                    shutil.copyfile(output, moved)
                    output.unlink()
                else:
                    output.rename(moved)
                for source in (root / "input.vortex", root / "produced.parquet"):
                    source.unlink(missing_ok=True)
                if profile == "external":
                    # The input adapter's reusable prepared file is distinct
                    # from output state. Remove it before either later job.
                    self.assertTrue(list((root / ".shardloom" / "prepared").glob("*.vortex")))
                    shutil.rmtree(root / ".shardloom")
                aggregate = run("aggregate", profile, moved)
                filtered = run("filter", profile, moved)
                values = run("values", profile, moved)
                self.assertEqual(aggregate["expected_rows"], writer["expected_rows"])
                self.assertEqual(values["schema"], writer["schema"])
                self.assertEqual(len({writer["pid"], aggregate["pid"], filtered["pid"], values["pid"]}), 4)
                self.assertEqual(aggregate["fields"]["local_primitive_arrays_read_count"], "0")
                self.assertEqual(aggregate["fields"]["local_primitive_aggregate_update_strategy"],
                                 "exact_file_footer_scalar_completion")
                if profile == "spill":
                    self.assertGreater(int(writer["fields"]["relational_spill_runs_written"]), 0)
                    self.assertEqual(writer["fields"]["relational_spill_owned_cleanup_completed"], "true")
                self.assertEqual([path for path in root.rglob("*") if path.is_file()], [moved])

    def test_native_fanout_reopens_complete_values_and_preserves_empty_outputs(self):
        session = self.session(64 << 20, 2)
        source = session.from_range("id", -4, 3, 129)
        formats = ("vortex", "parquet", "arrow_ipc", "csv", "json", "jsonl", "avro", "orc")
        with tempfile.TemporaryDirectory(prefix="shardloom-native-fanout-") as directory:
            for empty in (False, True):
                statement = "SELECT id FROM 'memory://fanout'"
                if empty:
                    statement += " WHERE id < -100"
                plan = session.sql(statement, sources=[("memory://fanout", source)])
                expected = [] if empty else [{"id": -4 + 3 * index} for index in range(129)]
                retained = plan.collect()
                targets = [(Path(directory) / f"{empty}.{name}", "write_" + name) for name in formats]
                report = plan.write_many(targets)
                self.assertEqual(report["native_vortex_result_export_target_count"], "8")
                self.assertEqual(report["native_vortex_result_export_fanout_count"], "7")
                self.assertEqual(report["native_vortex_result_export_execution_count"], "8")
                self.assertEqual(report["native_vortex_result_export_execution_model"],
                                 "native_query_replayed_once_per_output_adapter")
                self.assertEqual(report["execution_resource_declared_memory_bytes"], str(64 << 20))
                self.assertEqual(report["native_vortex_result_export_target_roles"],
                                 "primary," + ",".join(["fanout"] * 7))
                self.assertEqual(retained.row_export()[0], expected)
                for (path, _), name in zip(targets, formats):
                    with self.subTest(empty=empty, format=name):
                        if empty and name in {"json", "jsonl"}:
                            # Empty JSON has no embedded schema. The shared input
                            # adapter currently requires a row even with a schema.
                            if name == "json":
                                self.assertEqual(json.loads(path.read_text()), [])
                            else:
                                self.assertEqual(path.read_text(), "")
                            continue
                        reader = session.sql(
                            f"SELECT id FROM '{path}'",
                            files=[(str(path), name.replace("_", "-"),
                                    "id:int64" if name in {"csv", "json", "jsonl"} else None)],
                        )
                        reopened = reader.collect()
                        self.assertEqual(reopened.row_export()[0], expected)
                        self.assertEqual(self.arrow().RecordBatchReader.from_stream(reopened)
                                         .read_all().schema.field("id").type, self.arrow().int64())
            self.assertEqual(len(list(Path(directory).glob("False.*"))), 8)
            self.assertEqual(len(list(Path(directory).glob("True.*"))), 8)
            self.assertFalse(list(Path(directory).glob(".shardloom-fanout-*")))

    def test_public_native_fanout_validates_before_factory_and_preserves_shared_owners(self):
        import shardloom as sl
        from shardloom.errors import ShardLoomCommandError

        client = sl.ShardLoomClient(transport="native", memory_bytes=64 << 20, max_parallelism=2)
        self.addCleanup(client.close)
        events = []

        def producer():
            events.append("opened")
            try:
                yield [{"id": value} for value in range(257)]
            finally:
                events.append("closed")

        frame = sl.from_batches(producer, schema={"id": "int64"}, streaming=False, client=client)
        retained = sl.from_rows([{"id": 999}], schema={"id": "int64"}, client=client).collect(check=True)
        with tempfile.TemporaryDirectory(prefix="shardloom-native-fanout-public-") as directory:
            root = Path(directory)
            existing = root / "existing.jsonl"
            existing.write_text("keep me")
            for overwrite in (False, True):
                with self.assertRaises(ShardLoomCommandError):
                    frame.fanout([("vortex", root / "uncreated/out.vortex"), ("jsonl", existing)],
                                 allow_overwrite=overwrite)
                self.assertEqual(existing.read_text(), "keep me")
                self.assertFalse((root / "uncreated").exists())
                self.assertEqual(events, [])
            with self.assertRaises(ShardLoomCommandError):
                frame.fanout([("vortex", root / "same"), ("jsonl", root / "same")])
            with self.assertRaises(ValueError):
                frame.fanout([])
            with self.assertRaises(ShardLoomCommandError):
                frame.fanout([("vortex", root / "uncreated/a"), ("jsonl", root / "uncreated/b")],
                             memory_bytes=1)
            streaming = sl.from_batches(producer, schema={"id": "int64"}, streaming=True, client=client)
            with self.assertRaises(ShardLoomCommandError):
                streaming.fanout([("vortex", root / "stream"), ("jsonl", existing)])
            self.assertEqual(events, [])
            self.assertEqual(list(root.iterdir()), [existing])
            outputs = {"vortex": root / "complete.vortex", "jsonl": root / "complete.jsonl"}
            with patch("subprocess.Popen", side_effect=AssertionError("child process")), \
                 patch("shardloom._batches.BatchInput.encode", side_effect=AssertionError("JSON input")):
                report = frame.fanout(outputs)
                self.assertEqual(report.envelope.field_int("native_vortex_result_export_target_count"), 2)
                self.assertEqual(report.envelope.field_int("native_vortex_result_export_fanout_count"), 1)
                self.assertEqual(sl.read_vortex(outputs["vortex"], client=client).collect(check=True)
                                 .to_arrow()["id"].to_pylist(), list(range(257)))
                self.assertEqual([json.loads(line) for line in outputs["jsonl"].read_text().splitlines()],
                                 [{"id": value} for value in range(257)])
            self.assertEqual(events, ["opened", "closed"])
            self.assertEqual(set(root.iterdir()), {existing, *outputs.values()})
        client.close()
        self.assertEqual(retained.to_arrow()["id"].to_pylist(), [999])

    def test_public_native_stream_destinations_consume_once_and_preserve_empty_schema(self):
        import csv
        import shardloom as sl

        client = sl.ShardLoomClient(transport="native", memory_bytes=64 << 20, max_parallelism=2)
        self.addCleanup(client.close)
        formats = ("vortex", "parquet", "arrow_ipc", "avro", "orc", "csv", "json", "jsonl")
        for empty in (False, True):
            wanted = [] if empty else [{"id": -1, "label": 'λ"\n'}, {"id": None, "label": None},
                                      {"id": (1 << 63) - 1, "label": ""}]
            events = []
            with tempfile.TemporaryDirectory(prefix="shardloom-stream-destinations-") as directory:
                root = Path(directory)
                targets = {name: root / ("output." + name) for name in formats}

                def producer():
                    events.append("opened")
                    try:
                        for batch in ([], wanted[:2], [], wanted[2:]):
                            self.assertFalse(any(path.exists() for path in targets.values()))
                            yield batch
                        events.append("ended")
                    finally:
                        events.append("closed")

                frame = sl.from_batches(producer, schema={"id": "int64", "label": "utf8"},
                                        streaming=True, client=client)
                with patch("subprocess.Popen", side_effect=AssertionError("child process")), \
                     patch("shardloom._batches.BatchInput.encode", side_effect=AssertionError("JSON input")):
                    report = frame.fanout(targets)
                    fields = report.envelope
                    self.assertEqual(events, ["opened", "ended", "closed"])
                    self.assertEqual(fields.field_int("native_input_batches"), 4)
                    self.assertEqual(fields.field_int("native_input_batch_rows"), len(wanted))
                    self.assertTrue(fields.field_bool("native_input_end_observed"))
                    self.assertEqual(fields.field_int("native_vortex_result_export_execution_count"), 1)
                    self.assertEqual(fields.field("native_vortex_result_export_execution_model"),
                                     "native_query_once_with_credited_result_replay")
                    self.assertEqual(fields.field_int("native_vortex_result_export_replay_count"), 8)
                    self.assertEqual(fields.field_int("native_vortex_result_export_replay_rows_detached"), len(wanted))
                    self.assertEqual(fields.field_int("native_input_replay_sources"), 0)
                    self.assertFalse(fields.fallback.attempted)
                    self.assertEqual(fields.field("external_engine_invoked"), "false")
                    self.assertEqual(set(root.iterdir()), set(targets.values()))
                    for name, path in targets.items():
                        with self.subTest(empty=empty, format=name):
                            if name == "csv":
                                with path.open(newline="") as stream:
                                    reader = csv.DictReader(stream)
                                    self.assertEqual(reader.fieldnames, ["id", "label"])
                                    self.assertEqual(list(reader), [
                                        {key: "" if value is None else str(value) for key, value in row.items()}
                                        for row in wanted])
                            elif name == "json":
                                self.assertEqual(json.loads(path.read_text()), wanted)
                            elif name == "jsonl":
                                self.assertEqual([json.loads(line) for line in path.read_text().splitlines()], wanted)
                            else:
                                reopened = getattr(sl, "read_" + name)(path, client=client).collect(check=True).to_arrow()
                                self.assertEqual(reopened.to_pylist(), wanted)
                                self.assertEqual(reopened.column_names, ["id", "label"])
                                self.assertEqual(reopened.schema.field("id").type, self.arrow().int64())
                    self.assertFalse(list(root.glob(".shardloom-fanout-*")))

    def test_public_native_stream_destinations_keep_exact_recursive_values_and_admit_schema_first(self):
        from decimal import Decimal
        import shardloom as sl
        from shardloom.errors import ShardLoomCommandError

        pa = self.arrow()
        table = pa.table({
            "id": pa.array([1, None, 3], pa.int64()),
            "items": pa.array([[1, None, -2], None, []], pa.list_(pa.int32())),
            "record": pa.array([{"name": "λ", "value": 4}, None, {"name": "", "value": None}],
                               pa.struct([("name", pa.string()), ("value", pa.int64())])),
            "amount": pa.array([Decimal("-9999999999.99"), None, Decimal("0.01")], pa.decimal128(12, 2)),
            "payload": pa.array([b"\x00\xff", None, b""], pa.binary()),
        })
        schema = {"id": "int64", "items": {"type": "list", "item": "int32"},
                  "record": {"type": "struct", "fields": {"name": "utf8", "value": "int64"}},
                  "amount": "decimal128(12,2)", "payload": "binary"}
        client = sl.ShardLoomClient(transport="native", memory_bytes=64 << 20, max_parallelism=1)
        self.addCleanup(client.close)
        with tempfile.TemporaryDirectory(prefix="shardloom-stream-rich-destinations-") as directory:
            root = Path(directory)
            events = []

            def producer():
                events.append("opened")
                try:
                    yield from table.to_batches(max_chunksize=1)
                    events.append("ended")
                finally:
                    events.append("closed")

            frame = sl.from_batches(producer, schema=schema, streaming=True, client=client)
            with self.assertRaises(ShardLoomCommandError) as rejected:
                frame.fanout({"vortex": root / "denied.vortex", "orc": root / "denied.orc"})
            self.assertIn("ORC does not admit nested output", str(rejected.exception))
            self.assertEqual(events, [])
            self.assertEqual(list(root.iterdir()), [])
            targets = {name: root / ("rich." + name) for name in ("vortex", "parquet", "arrow_ipc", "avro")}
            with patch("subprocess.Popen", side_effect=AssertionError("child process")), \
                 patch("shardloom._batches.BatchInput.encode", side_effect=AssertionError("JSON input")):
                report = frame.fanout(targets)
                self.assertEqual(events, ["opened", "ended", "closed"])
                self.assertEqual(report.envelope.field_int("native_vortex_result_export_execution_count"), 1)
                self.assertEqual(set(root.iterdir()), set(targets.values()))
                for name, path in targets.items():
                    with self.subTest(format=name):
                        actual = getattr(sl, "read_" + name)(path, client=client).collect(check=True).to_arrow()
                        self.assertTrue(actual.equals(table), (actual.schema, table.schema, actual.to_pylist()))
            self.assertFalse(list(root.glob(".shardloom-fanout-*")))

    def test_public_native_stream_composition_preserves_each_producer_across_terminals(self):
        import shardloom as sl
        from shardloom.query import SqlWorkflow

        client = sl.ShardLoomClient(transport="native", memory_bytes=32 << 20, max_parallelism=1)
        self.addCleanup(client.close)
        for shape in ("multiple", "repeated", "dynamic"):
            for surface in ("dataframe", "sql"):
                for terminal in ("collect", "batches", "single", "fanout"):
                    with self.subTest(shape=shape, surface=surface, terminal=terminal), \
                         tempfile.TemporaryDirectory(prefix="shardloom-native-composed-stream-") as directory:
                        root, traces = Path(directory), []

                        def source(rows, schema, columnar=False):
                            events = []
                            traces.append(events)

                            def producer():
                                events.append("opened")
                                try:
                                    for batch in ([], rows[:1], [], rows[1:]):
                                        yield self.arrow().Table.from_pylist(batch, schema=self.arrow().schema([
                                            (key, self.arrow().int64() if kind == "int64" else self.arrow().string())
                                            for key, kind in schema.items()])) if columnar else batch
                                    events.append("ended")
                                finally:
                                    events.append("closed")

                            return sl.from_batches(producer, schema=schema, streaming=True, client=client)

                        if shape == "dynamic":
                            frame = source([{"entity": 1, "category": "a", "amount": 3},
                                            {"entity": 1, "category": "b", "amount": 5},
                                            {"entity": 2, "category": "a", "amount": 7}],
                                           {"entity": "int64", "category": "utf8", "amount": "int64"})
                            query = frame.pivot_table(index="entity", columns="category", values="amount", aggfunc="sum")
                            expected = [{"entity": 1, "pivot_a": 3.0, "pivot_b": 5.0},
                                        {"entity": 2, "pivot_a": 7.0, "pivot_b": None}]
                        else:
                            frame = source([{"n": 1, "label": "a"}, {"n": 2, "label": "b"}],
                                           {"n": "int64", "label": "utf8"})
                            if shape == "multiple":
                                other = source([{"n": 1, "metric": 11}, {"n": 3, "metric": 33}],
                                               {"n": "int64", "metric": "int64"}, columnar=True)
                                query = frame.join(other, on="n", how="full").select("f.label AS label", "d.metric AS metric")
                                expected = [{"label": "a", "metric": 11}, {"label": "b", "metric": None},
                                            {"label": None, "metric": 33}]
                            else:
                                query = frame.join(frame, on="n").select("f.label AS label", "d.label AS copy")
                                expected = [{"label": "a", "copy": "a"}, {"label": "b", "copy": "b"}]
                        if surface == "sql":
                            query = SqlWorkflow(query._relation_statement(), client, source_bindings=query._declared_sources())
                        with patch("subprocess.Popen", side_effect=AssertionError("child process")), \
                             patch("shardloom._batches.BatchInput.encode", side_effect=AssertionError("JSON input")):
                            if terminal == "collect":
                                report = query.collect(check=True)
                                actual = report.to_arrow().to_pylist()
                            elif terminal == "batches":
                                with query.iter_batches(batch_rows=1) as stream:
                                    actual = [row for batch in stream for row in batch.result_rows]
                                    report = stream.report
                            else:
                                targets = {name: root / ("output." + name) for name in
                                           (("vortex", "parquet", "jsonl") if terminal == "fanout" else ("parquet",))}
                                report = query.fanout(targets) if terminal == "fanout" else query.write_parquet(targets["parquet"])
                                self.assertEqual(set(root.iterdir()), set(targets.values()))
                                for name, path in targets.items():
                                    actual = ([json.loads(line) for line in path.read_text().splitlines()] if name == "jsonl"
                                              else getattr(sl, "read_" + name)(path, client=client).collect(check=True).to_arrow().to_pylist())
                                    self.assertCountEqual(actual, expected)
                            self.assertCountEqual(actual, expected)
                            self.assertEqual(traces, [["opened", "ended", "closed"]] * len(traces))
                            envelope = report.envelope
                            self.assertEqual(envelope.field_int("native_input_batch_sources"), len(traces))
                            self.assertTrue(envelope.field_bool("native_input_end_observed"))
                            self.assertEqual(envelope.field_int("native_input_replay_sources"), int(shape == "repeated"))
                            if shape == "dynamic":
                                self.assertEqual(envelope.field("relational_schema_binding"), "during_execution")
                            if terminal == "fanout":
                                self.assertEqual(envelope.field_int("native_vortex_result_export_execution_count"), 1)
                                self.assertEqual(envelope.field_int("native_vortex_result_export_replay_count"), 3)
                            self.assertFalse(envelope.fallback.attempted)
                            self.assertEqual(envelope.field("external_engine_invoked"), "false")

    def test_public_native_multiple_producers_drain_fail_and_close_without_publication(self):
        import shardloom as sl
        from shardloom.errors import ShardLoomCommandError

        pa = self.arrow()
        client = sl.ShardLoomClient(transport="native", memory_bytes=32 << 20, max_parallelism=1)
        self.addCleanup(client.close)
        retained = sl.from_rows([{"n": 99}], schema={"n": "int64"}, client=client).collect(check=True)
        for mode in ("empty", "zero", "denial", "producer", "close", "schema", "cancel", "consumer-close"):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory(prefix="shardloom-native-multiple-fault-") as directory:
                root = Path(directory)
                trace, opened = [[], []], []
                sentinel = LookupError("second producer sentinel")

                def source(index):
                    def producer():
                        trace[index].append("opened")
                        opened.append(index)
                        second = len(opened) == 2
                        try:
                            if not (mode == "empty" and index == 1):
                                yield [{"n": n} for n in (1, 2, 3)]
                            if second:
                                if mode == "producer":
                                    raise sentinel
                                if mode == "schema":
                                    yield pa.table({"changed": pa.array([1], pa.int64())})
                                if mode == "cancel":
                                    client.cancel()
                            trace[index].append("ended")
                        finally:
                            trace[index].append("closed")
                            if mode == "close" and second:
                                raise sentinel
                    return sl.from_batches(producer, schema={"n": "int64"}, streaming=True, client=client)

                first, second = source(0), source(1)
                query = first.join(second, on="n").select("f.n AS n")
                if mode == "zero":
                    query = query.limit(0)
                targets = {name: root / ("output." + name) for name in ("vortex", "jsonl")}
                if mode == "consumer-close":
                    with query.iter_batches(batch_rows=1) as stream:
                        batch = next(stream)
                        self.assertIsNone(stream.report)
                    self.assertIsNone(stream.report)
                    self.assertEqual(batch.to_arrow().to_pylist(), [{"n": 1}])
                    self.assertEqual(len(opened), 2)
                    self.assertTrue(all(events[-1] == "closed" for events in trace))
                    self.assertTrue(any("ended" not in events for events in trace))
                elif mode in {"empty", "zero"}:
                    report = query.fanout(targets)
                    self.assertEqual(trace, [["opened", "ended", "closed"]] * 2)
                    self.assertEqual(report.envelope.field_int("native_input_batch_sources"), 2)
                    self.assertTrue(report.envelope.field_bool("native_input_end_observed"))
                    self.assertEqual(sl.read_vortex(targets["vortex"], client=client).collect(check=True).to_arrow().to_pylist(), [])
                    self.assertEqual(targets["jsonl"].read_bytes(), b"")
                    self.assertEqual(set(root.iterdir()), set(targets.values()))
                else:
                    expected_error = LookupError if mode in {"producer", "close"} else ValueError if mode == "schema" else ShardLoomCommandError
                    with self.assertRaises(expected_error) as raised:
                        query.fanout(targets, **({"memory_bytes": 1} if mode == "denial" else {}))
                    if mode in {"producer", "close"}:
                        self.assertIs(raised.exception, sentinel)
                    elif mode == "schema":
                        self.assertIn("declared schema", str(raised.exception))
                    elif mode == "cancel":
                        self.assertIn("cancelled", str(raised.exception))
                    if mode == "denial":
                        self.assertEqual(trace, [[], []])
                    else:
                        self.assertEqual(len(opened), 2)
                        self.assertEqual(trace[opened[0]], ["opened", "ended", "closed"])
                        self.assertEqual(trace[opened[1]][-1], "closed")
                    self.assertEqual(list(root.iterdir()), [])
                self.assertEqual(retained.to_arrow().to_pylist(), [{"n": 99}])
                self.assertEqual(sl.from_rows([{"n": 7}], schema={"n": "int64"}, client=client)
                                 .collect(check=True).to_arrow().to_pylist(), [{"n": 7}])

    def test_public_native_fanout_producer_failure_publishes_no_prefix(self):
        import shardloom as sl

        class ProducerFailure(Exception):
            pass

        events = []

        def producer():
            try:
                yield [{"id": 1}]
                raise ProducerFailure("fanout input failed")
            finally:
                events.append("closed")

        client = sl.ShardLoomClient(transport="native", memory_bytes=32 << 20, max_parallelism=1)
        self.addCleanup(client.close)
        for streaming in (False, True):
            events.clear()
            frame = sl.from_batches(producer, schema={"id": "int64"}, streaming=streaming, client=client)
            with tempfile.TemporaryDirectory(prefix="shardloom-native-fanout-failure-") as directory:
                root = Path(directory)
                with self.assertRaisesRegex(ProducerFailure, "fanout input failed"):
                    frame.fanout({"vortex": root / "new/one.vortex", "jsonl": root / "new/two.jsonl"})
                self.assertEqual(list(root.iterdir()), [])
                self.assertEqual(events, ["closed"])

    def test_public_native_object_intake_infers_and_converts_without_json_cells(self):
        import shardloom as sl
        from collections import UserDict, UserList

        client = sl.ShardLoomClient(transport="native", memory_bytes=64 << 20, max_parallelism=2)
        rows = UserList([UserDict(id=1, value=2.5, active=True, name="α"),
                         UserDict(id=2, value=None, active=False, name=""),
                         UserDict(id=3, value=4, active=None, name=None)])
        expected = [dict(row) for row in rows]
        with patch("shardloom._input_schema.encode_cell", side_effect=AssertionError("JSON cell bridge")), \
             patch("shardloom.query._infer_memory_column_type", side_effect=AssertionError("Python per-cell inference")), \
             patch("shardloom.query._native_memory_frame", side_effect=AssertionError("JSON memory frame")), \
             patch("subprocess.Popen", side_effect=AssertionError("child process")):
            frame = sl.from_rows(rows, client=client)
            rows[0]["id"] = 999
            result = frame.collect()
            self.assertEqual(result.python_objects, tuple(expected))
            self.assertEqual(result.to_arrow().to_pylist(), expected)
            self.assertGreater(frame.source.native_input.input_columnar_buffer_bytes_copied, 0)
            client.close()
            self.assertEqual(result.python_objects, tuple(expected))

    def test_public_native_object_intake_preserves_rich_exact_values_and_null_masks(self):
        import shardloom as sl
        import datetime as dt
        from decimal import Decimal, localcontext
        from shardloom._input_schema import normalize_schema

        schema = {
            "id": {"type": "uint64", "nullable": False},
            "small": "int8", "single": "float32", "payload": "binary",
            "amount": "decimal128(38,6)", "day": "date32", "instant": "timestamp_micros",
            "items": {"type": "list", "item": "uint16"},
            "fixed": {"type": "fixed_size_list", "size": 2,
                      "item": {"type": "int32", "nullable": False}},
            "record": {"type": "struct", "fields": {
                "key": {"type": "int16", "nullable": False},
                "value": {"type": "list", "item": "utf8"},
            }},
        }
        rows = [
            {"id": (1 << 64) - 1, "small": -128, "single": -0.0,
             "payload": bytearray(b"\x00\xff"),
             "amount": Decimal("99999999999999999999999999999999.999999"),
             "day": -(1 << 31), "instant": -(1 << 63), "items": [0, None, 65535],
             "fixed": [-(1 << 31), (1 << 31) - 1], "record": {"key": -32768, "value": ["é", None]}},
            {"id": 0, "small": None, "single": 1.5, "payload": memoryview(b"abc")[::2],
             "amount": Decimal("-1.2300000"), "day": (1 << 31) - 1, "instant": (1 << 63) - 1,
             "items": [], "fixed": None, "record": None},
            {"id": 1, "small": 127, "single": None, "payload": None, "amount": Decimal("0E-1000000"),
             "day": dt.date(2024, 2, 29), "instant": dt.datetime(2024, 2, 29, 1, 2, 3, 456789),
             "items": None, "fixed": [0, 1], "record": {"key": 1, "value": []}},
        ]
        expected = [dict(row) for row in rows]
        expected[0]["payload"], expected[1]["payload"] = b"\x00\xff", b"ac"
        context = sl.context(transport="native", memory_bytes=64 << 20, max_parallelism=2)
        self.addCleanup(context.client.close)
        with localcontext() as decimal_context, \
             patch("shardloom._input_schema.encode_cell", side_effect=AssertionError("JSON cells")):
            decimal_context.prec = 3
            result = context.from_rows(rows, schema=schema).collect()
            self.assertEqual(result.result_schema, normalize_schema(schema))
            self.assertEqual(result.python_objects, tuple(expected))
            self.assertEqual(result.to_arrow().num_rows, len(rows))
            self.assertEqual(context.from_rows([], schema=schema).collect().result_schema, normalize_schema(schema))

    def test_native_object_refusals_preserve_input_and_release_partial_construction(self):
        from collections.abc import Sequence
        from decimal import Decimal
        import datetime as dt
        from shardloom._input_schema import _wire_type, normalize_schema
        import json

        session = self.session()
        touched = []
        class Rows(Sequence):
            def __len__(self):
                touched.append("length")
                return 1
            def __getitem__(self, index):
                touched.append(index)
                return {"n": 1}

        with self.assertRaisesRegex(RuntimeError, "memory"):
            session.from_rows(Rows(), resources=self.resources(1))
        self.assertEqual(touched, [])
        baseline = session.snapshot()["reserved_bytes"]
        invalid = [
            ("int8", 128), ("uint64", -1), ("int64", True), ("float32", 0.1),
            ("float64", (1 << 53) + 1), ("float64", float("inf")), ("bool", 1),
            ("binary", "text"), ("decimal128(3,2)", Decimal("1.234")),
            ("decimal128(3,2)", Decimal("10.00")), ("decimal128(3,2)", Decimal("NaN")),
            ("date32", dt.datetime(2024, 1, 1)), ("date32", 1 << 31),
            ("timestamp_micros", dt.datetime(2024, 1, 1, tzinfo=dt.timezone.utc)),
            ({"type": "int64", "nullable": False}, None),
            ({"type": "list", "item": {"type": "int8", "nullable": False}}, [1, None]),
            ({"type": "fixed_size_list", "item": "int8", "size": 2}, [1]),
            ({"type": "struct", "fields": {"x": "int8"}}, {"y": 1}),
        ]
        for specification, value in invalid:
            with self.subTest(schema=specification, value=value):
                fields = [(name, json.dumps(_wire_type(dtype)))
                          for name, dtype in normalize_schema({"n": specification})]
                with self.assertRaises((ValueError, TypeError)):
                    session.from_rows([{"n": value}], fields=fields)
                self.assertEqual(session.snapshot()["reserved_bytes"], baseline)
        source = session.from_rows([{"n": 7}, {"n": None}])
        plan = session.sql("SELECT n FROM 'memory://objects'", sources=[("memory://objects", source)])
        self.assertEqual(plan.collect().row_export()[0], [{"n": 7}, {"n": None}])

    def test_native_object_input_crosses_old_total_row_limit_with_complete_values(self):
        import shardloom as sl

        count = 65_537
        client = sl.ShardLoomClient(transport="native", memory_bytes=96 << 20, max_parallelism=2)
        with patch("shardloom._input_schema.encode_cell", side_effect=AssertionError("JSON cells")):
            frame = sl.from_rows([{"n": n} for n in range(count)], client=client, schema={"n": "int64"})
            result = frame.collect()
            self.assertEqual(result.to_arrow()["n"].to_pylist(), list(range(count)))
        client.close()

    def test_public_native_batch_factories_use_fresh_values_without_json_cells(self):
        import shardloom as sl

        for streaming in (False, True):
            with self.subTest(streaming=streaming):
                calls = []

                def producer():
                    offset = len(calls) * 10_000
                    calls.append(offset)
                    yield []
                    # The native adapter subdivides construction work; the
                    # caller's batch is not constrained by the JSON frame cap.
                    yield [{"n": offset + n, "b": bytes([n % 256])} for n in range(3001)]
                    yield []

                client = sl.ShardLoomClient(transport="native", memory_bytes=64 << 20, max_parallelism=2)
                self.addCleanup(client.close)
                frame = sl.from_batches(producer, schema={"n": "int64", "b": "binary"},
                                        streaming=streaming, client=client)
                self.assertEqual(calls, [])
                with patch("shardloom._batches.BatchInput.encode", side_effect=AssertionError("JSON cells")), \
                     patch("subprocess.Popen", side_effect=AssertionError("child process")):
                    first = frame.collect(check=True)
                    second = frame.collect(check=True)
                self.assertEqual(calls, [0, 10_000])
                self.assertEqual(first.to_arrow()["n"].to_pylist(), list(range(3001)))
                self.assertEqual(second.to_arrow()["n"].to_pylist(), list(range(10_000, 13_001)))
                self.assertEqual(first.to_arrow()["b"].to_pylist(), [bytes([n % 256]) for n in range(3001)])
                if streaming:
                    self.assertTrue(second.envelope.field_bool("native_input_end_observed"))
                    self.assertEqual(second.envelope.field_int("native_input_batch_rows"), 3001)
                    self.assertEqual(second.envelope.field_int("native_input_max_retained_batches"), 1)

    def test_native_spill_plan_preserves_values_owners_and_cleanup(self):
        pa = self.arrow()
        session = self.session(64 << 20, 1)
        self.addCleanup(session.close)
        rows = 24_001
        source = session.from_range("id", 0, 1, rows)
        with tempfile.TemporaryDirectory(prefix="shardloom-native-spill-") as directory:
            spill = json.dumps({"workspace": directory, "quota_bytes": 64 << 20,
                                "buffer_bytes": 1 << 20})
            plan = session.sql("SELECT id FROM 'memory://spill' ORDER BY id DESC",
                               sources=[("memory://spill", source)], spill=spill)
            self.assertEqual(list(Path(directory).iterdir()), [])
            result = plan.collect()
            self.assertEqual(result.report["relational_spill_requested"], "true")
            self.assertGreater(int(result.report["relational_spill_runs_written"]), 0)
            self.assertEqual(result.report["relational_spill_owned_cleanup_completed"], "true")
            self.assertEqual(list(Path(directory).iterdir()), [])
            self.assertEqual(pa.RecordBatchReader.from_stream(result).read_all()["id"].to_pylist(),
                             list(reversed(range(rows))))
            session.close()
            self.assertEqual(pa.RecordBatchReader.from_stream(result).read_all()["id"].to_pylist(),
                             list(reversed(range(rows))))

    def test_public_native_spill_collect_stream_write_and_invalid_options(self):
        import shardloom as sl
        from shardloom.errors import ShardLoomCommandError

        client = sl.ShardLoomClient(transport="native", memory_bytes=32 << 20, max_parallelism=1)
        self.addCleanup(client.close)
        events = []

        def producer():
            events.append("opened")
            yield [{"id": value} for value in range(4097)]

        frame = sl.from_batches(producer, schema={"id": "int64"}, streaming=True,
                                client=client).sort("id", descending=True)
        with tempfile.TemporaryDirectory(prefix="shardloom-native-public-spill-") as directory:
            root = Path(directory)
            options = {"workspace": directory, "quota_bytes": 64 << 20, "buffer_bytes": 1 << 20}
            for invalid in ("not-json", {**options, "buffer_bytes": 64 << 20},
                            {**options, "quota_bytes": 0}, {**options, "unknown": True}):
                with self.subTest(spill=invalid), self.assertRaises(ShardLoomCommandError):
                    frame.collect(spill=invalid, check=True)
                self.assertEqual(events, [])
                self.assertEqual(list(root.iterdir()), [])
            with self.assertRaises(ShardLoomCommandError):
                frame.write_jsonl(root / "uncreated" / "out.jsonl", spill="not-json", check=True)
            self.assertEqual(events, [])
            self.assertFalse((root / "uncreated").exists())
            expected = list(reversed(range(4097)))
            with patch("subprocess.Popen", side_effect=AssertionError("child process")), \
                 patch("shardloom._batches.BatchInput.encode", side_effect=AssertionError("JSON input")):
                result = frame.collect(spill=options, check=True)
                self.assertEqual(result.to_arrow()["id"].to_pylist(), expected)
                self.assertTrue(result.envelope.field_bool("relational_spill_requested"))
                with frame.iter_batches(batch_rows=257, spill=options) as batches:
                    actual = [value for batch in batches for value in batch.to_arrow()["id"].to_pylist()]
                    self.assertEqual(actual, expected)
                    self.assertTrue(batches.report.envelope.field_bool("relational_spill_requested"))
                self.assertEqual(list(root.iterdir()), [])
                target = root / "out.vortex"
                report = frame.write_vortex(target, spill=options, check=True)
                self.assertTrue(report.envelope.field_bool("relational_spill_requested"))
                reopened = sl.read_vortex(target, client=client).collect(check=True)
                self.assertEqual(reopened.to_arrow()["id"].to_pylist(), expected)
                self.assertEqual(list(root.iterdir()), [target])
            self.assertEqual(events, ["opened"] * 3)

    def test_public_native_columnar_batches_preserve_large_batches_and_empty_input(self):
        import shardloom as sl
        pa = self.arrow()
        values = list(range(4097))
        batch = pa.record_batch([pa.array(values, type=pa.int32())], names=["n"])
        empty = pa.record_batch([pa.array([], type=pa.int32())], names=["n"])
        client = sl.ShardLoomClient(transport="native", memory_bytes=32 << 20, max_parallelism=2)
        self.addCleanup(client.close)
        for streaming in (False, True):
            with self.subTest(streaming=streaming), \
                 patch("shardloom._batches.BatchInput.encode", side_effect=AssertionError("row bridge")):
                frame = sl.from_batches(lambda: iter((empty, batch, empty)), schema={"n": "int32"},
                                        streaming=streaming, client=client)
                result = frame.collect(check=True)
                self.assertEqual(result.to_arrow()["n"].to_pylist(), values)
                if streaming:
                    self.assertEqual(result.envelope.field_int("native_input_max_batch_rows"), 4097)
                empty_result = sl.from_batches(lambda: iter(()), schema={"n": "int32"},
                                              streaming=streaming, client=client).collect(check=True)
                self.assertEqual(empty_result.to_arrow().num_rows, 0)
                self.assertEqual(empty_result.to_arrow().schema.field("n").type, pa.int32())

    def test_public_native_batch_admission_precedes_factory_and_stream_close_cleans_producer(self):
        import shardloom as sl

        events = []

        def producer():
            events.append("open")
            try:
                yield [{"n": n} for n in range(4097)]
                events.append("next input")
                yield [{"n": 9999}]
            finally:
                events.append("closed")

        client = sl.ShardLoomClient(transport="native")
        self.addCleanup(client.close)
        frame = sl.from_batches(producer, schema={"n": "int64"}, streaming=True, client=client)
        with self.assertRaises(ValueError):
            frame.collect(check=True)
        self.assertEqual(events, [])
        with self.assertRaises(Exception) as refused:
            frame.collect(memory_bytes=1, max_parallelism=1, check=True)
        self.assertIn("memory", str(refused.exception).lower())
        self.assertEqual(events, [])
        with frame.iter_batches(memory_bytes=32 << 20, max_parallelism=2, batch_rows=32) as stream:
            self.assertEqual(events, [])
            retained = next(stream)
            self.assertIsNone(stream.report)
        self.assertEqual(events, ["open", "closed"])
        self.assertIsNone(stream.report)
        client.close()
        self.assertEqual(retained.to_arrow()["n"].to_pylist(), list(range(32)))

    def test_public_native_batch_late_errors_preserve_exception_and_existing_output(self):
        import shardloom as sl
        from shardloom.errors import ShardLoomCommandError

        client = sl.ShardLoomClient(transport="native", memory_bytes=32 << 20, max_parallelism=1)
        self.addCleanup(client.close)
        failure = LookupError("late native producer failure")
        closed = []

        def producer():
            try:
                yield [{"n": 1}, {"n": 2}]
                raise failure
            finally:
                closed.append(True)

        for streaming in (False, True):
            with self.subTest(streaming=streaming):
                frame = sl.from_batches(producer, schema={"n": "int64"}, streaming=streaming, client=client)
                with self.assertRaises(LookupError) as raised:
                    frame.collect(check=True)
                self.assertIs(raised.exception, failure)
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / "existing.vortex"
            target.write_bytes(b"preserve this artifact")
            frame = sl.from_batches(producer, schema={"n": "int64"}, streaming=True, client=client)
            # The shared native writer refuses replacement before demanding
            # input when generation-conditional publication is unavailable.
            with self.assertRaisesRegex(ShardLoomCommandError, "existing destination"):
                frame.write(target, output_format="vortex", allow_overwrite=True)
            self.assertEqual(len(closed), 2)
            failed_target = Path(directory) / "failed.vortex"
            with self.assertRaises(LookupError) as raised:
                frame.write(failed_target, output_format="vortex")
            self.assertIs(raised.exception, failure)
            self.assertFalse(failed_target.exists())
            self.assertEqual(target.read_bytes(), b"preserve this artifact")
            self.assertEqual(list(Path(directory).iterdir()), [target])
        self.assertEqual(len(closed), 3)

    def test_public_native_batch_intake_crosses_former_total_batch_limits(self):
        import shardloom as sl

        def producer():
            for _ in range(8193):
                yield []
            for value in range(4099):
                yield [{"n": value}]

        for streaming in (False, True):
            with self.subTest(streaming=streaming):
                client = sl.ShardLoomClient(transport="native", memory_bytes=128 << 20, max_parallelism=2)
                self.addCleanup(client.close)
                frame = sl.from_batches(producer, schema={"n": "int64"}, streaming=streaming, client=client)
                with patch("shardloom._batches.BatchInput.encode", side_effect=AssertionError("JSON cells")):
                    result = frame.collect(check=True)
                self.assertEqual(result.to_arrow()["n"].to_pylist(), list(range(4099)))
                client.close()

    def test_public_native_wide_object_and_columnar_inputs_preserve_every_field(self):
        import shardloom as sl
        pa = self.arrow()

        names = [f"field_{index}" for index in range(4097)]
        expected = {name: [index, None if index % 17 == 0 else -index]
                    for index, name in enumerate(names)}
        rows = [{name: values[row] for name, values in expected.items()} for row in range(2)]
        schema = dict.fromkeys(names, "int32")
        # Both complete wide results remain retained under this one grant.
        client = sl.ShardLoomClient(transport="native", memory_bytes=512 << 20, max_parallelism=2)
        self.addCleanup(client.close)
        with patch("shardloom._input_schema.encode_cell", side_effect=AssertionError("JSON cells")), \
             patch("shardloom.query.arrow_table", side_effect=AssertionError("row bridge")):
            objects = sl.from_rows(rows, schema=schema, client=client).collect(check=True)
            self.assertEqual(objects.to_arrow().to_pydict(), expected)
            columns = pa.table({name: pa.array(values, type=pa.int32()) for name, values in expected.items()})
            columnar = sl.from_arrow_table(columns, client=client).collect(check=True)
            self.assertEqual(columnar.to_arrow().to_pydict(), expected)
            self.assertEqual(columnar.to_arrow().schema.names, names)
        client.close()
        self.assertEqual(objects.to_arrow().to_pydict(), expected)

    def test_public_native_file_sql_selects_typed_decimal_providers(self):
        from decimal import Decimal
        import shardloom as sl
        import pyarrow.parquet as parquet

        pa = self.arrow()
        client = sl.ShardLoomClient(transport="native", memory_bytes=32 << 20, max_parallelism=1)
        self.addCleanup(client.close)
        table = pa.table({
            "group_id": pa.array([1, 1, 2, 2], pa.int64()),
            "amount": pa.array([Decimal("1.00"), Decimal("2.00"), None, Decimal("3.00")],
                               pa.decimal128(10, 2)),
        })

        def check(operation, expected, schema=None):
            result = operation.collect(check=True)
            arrow = result.to_arrow()
            self.assertEqual(arrow.to_pylist(), expected)
            if schema is not None:
                self.assertEqual(arrow.schema, schema)
            self.assertFalse(result.envelope.fallback.attempted)
            self.assertFalse(result.envelope.fallback.allowed)
            fields = result.envelope.field_map
            self.assertEqual(fields["execution_resource_declared_memory_bytes"], str(32 << 20))
            self.assertEqual(fields["execution_resource_declared_max_parallelism"], "1")

        with tempfile.TemporaryDirectory(prefix="shardloom-native-decimal-file-") as directory:
            root = Path(directory)
            compatibility = root / "compatibility-fixtures"
            compatibility.mkdir()
            with patch("subprocess.Popen", side_effect=AssertionError("execution child process")):
                for index in range(2):
                    sl.from_arrow_table(table, client=client).write_vortex(root / f"part-{index}.vortex", check=True)
                parquet_source = compatibility / "decimal.parquet"
                parquet.write_table(table, parquet_source)
                ipc_source = compatibility / "decimal.arrow"
                with ipc_source.open("wb") as sink:
                    with pa.ipc.new_file(sink, table.schema) as writer:
                        writer.write_table(table)
                for source, copies, input_format in (
                    (root / "part-0.vortex", 1, "vortex"),
                    (root, 2, "vortex"),
                    (parquet_source, 1, "parquet"),
                    (ipc_source, 1, "arrow-ipc"),
                ):
                    with self.subTest(source=source.name):
                        base = (sl.read_vortex(source, client=client) if input_format == "vortex"
                                else sl.read(source, client=client))
                        grouped = base.group_by("group_id").agg(total="sum(amount)", mean="avg(amount)").sort("group_id")
                        expected = [
                            {"group_id": 1, "total": Decimal(3 * copies), "mean": Decimal("1.500000")},
                            {"group_id": 2, "total": Decimal(3 * copies), "mean": Decimal("3.000000")},
                        ]
                        check(grouped, expected, pa.schema([
                            pa.field("group_id", pa.int64(), nullable=True),
                            pa.field("total", pa.decimal128(38, 2), nullable=True),
                            pa.field("mean", pa.decimal128(38, 6), nullable=True),
                        ]))
                        # Repeat on one declaration so preparation reuse cannot
                        # turn a correct first result into a different provider.
                        check(grouped, expected)
                        check(sl.sql(f"SELECT amount FROM '{source}' WHERE amount IS NOT NULL ORDER BY amount DESC",
                                     input=source, input_format=input_format, client=client),
                              [{"amount": Decimal(value)} for value in ("3.00", "2.00", "1.00") for _ in range(copies)])
                        check(sl.sql(f"SELECT COUNT(*) FROM '{source}' WHERE amount IS NOT NULL",
                                     input=source, input_format=input_format, client=client),
                              [{"count_all": 3 * copies}])
                        check(sl.sql(f"SELECT COUNT(*) AS n FROM '{source}' WHERE amount IS NOT NULL",
                                     input=source, input_format=input_format, client=client),
                              [{"n": 3 * copies}])
                        # Unreferenced decimal fields do not require the typed
                        # strategy for an otherwise ordinary primitive sort.
                        check(base.select("group_id").sort("group_id"),
                              [{"group_id": value} for value in (1, 2) for _ in range(2 * copies)])
                        del grouped, base
        native_session = client._native_backend.session
        client.close()
        self.assertEqual(native_session.snapshot()["reserved_bytes"], 0)

    def test_native_workflow_rejects_unbound_options_before_execution(self):
        import shardloom as sl

        client = sl.ShardLoomClient(transport="native", memory_bytes=32 << 20, max_parallelism=1)
        cases = ({"fanout_outputs": {"jsonl": "uncreated.jsonl"}}, {"bounded": False})
        for options in cases:
            with self.subTest(options=options), self.assertRaises(ValueError):
                client.public_workflow_run("sql", sql_statement="SELECT 1", **options)
        # Complete workflow declarations now use the same Rust validation and
        # structured diagnostics as the CLI, before preparing any operation.
        for options, message in (
            ({"vortex_sample_fraction": 0.25}, "matching complete primitive declaration"),
            ({"execution_policy": "invented"}, "unsupported route execution policy"),
            ({"materialization_policy": "unbounded"}, "unsupported route materialization"),
        ):
            with self.subTest(options=options), self.assertRaises(sl.ShardLoomCommandError) as raised:
                client.public_workflow_run("sql", sql_statement="SELECT 1", **options)
            self.assertIn(message, str(raised.exception))
            self.assertFalse(raised.exception.envelope.fallback.attempted)
            self.assertFalse(raised.exception.envelope.fallback.allowed)
        self.assertEqual(client._native_backend.session.snapshot()["completed_executions"], 0)
        client.close()

    def assert_failed_native_resources(self, fields, memory_bytes, snapshot):
        self.assertEqual(fields["execution_resource_declared_memory_bytes"], str(memory_bytes))
        self.assertEqual(fields["execution_resource_admission_status"], "admitted")
        self.assertEqual(fields["execution_resource_admitted_memory_bytes"], str(memory_bytes))
        self.assertEqual(fields["execution_resource_admitted_max_parallelism"],
                         str(snapshot["admitted_max_parallelism"]))
        self.assertEqual(fields["execution_resource_observed_native_reserved_bytes"],
                         str(snapshot["reserved_bytes"]))
        self.assertEqual(fields["execution_resource_observed_native_peak_reserved_bytes"],
                         str(snapshot["peak_reserved_bytes"]))
        self.assertEqual(fields["execution_resource_observed_native_allocation_peak_reserved_bytes"],
                         str(snapshot["allocation_peak_reserved_bytes"]))
        self.assertLessEqual(snapshot["reserved_bytes"], snapshot["allocation_peak_reserved_bytes"])
        self.assertLessEqual(snapshot["allocation_peak_reserved_bytes"], memory_bytes)
        self.assertIn("current_allocation", fields["execution_resource_allocation_memory_observation_scope"])
        self.assertEqual(fields["execution_resource_observed_native_denied_reservations"],
                         str(snapshot["denied_reservations"]))
        self.assertIn("session_pool_lifetime", fields["execution_resource_memory_observation_scope"])
        self.assertEqual(fields["execution_resource_observed_process_peak_rss_bytes"], "unavailable")
        self.assertEqual(fields["execution_resource_observed_spill_bytes"], "unavailable")

    def test_native_plan_failures_retain_admitted_resources_for_every_terminal(self):
        allocation = 32 << 20
        resources = self.resources(allocation, 2, memory_origin="platform", parallelism_origin="context")
        session = self.native.Session(resources)
        source = session.from_rows([{"n": 1}, {"n": 2}])
        plan = session.sql("SELECT missing FROM 'memory://failure'", sources=[("memory://failure", source)])
        with tempfile.TemporaryDirectory(prefix="shardloom-native-failure-fields-") as directory:
            target = Path(directory) / "result.jsonl"
            fanout = Path(directory) / "result.vortex"
            operations = (
                ("prepare", plan.prepare),
                ("collect", plan.collect),
                ("write", lambda: plan.write(target, "write_jsonl")),
                ("fanout", lambda: plan.write_many([(target, "write_jsonl"), (fanout, "write_vortex")])),
                ("stream", lambda: next(plan.iter_batches())),
            )
            for name, operation in operations:
                with self.subTest(operation=name):
                    with self.assertRaises(RuntimeError) as raised:
                        operation()
                    error = raised.exception
                    self.assertFalse(error.diagnostic["fallback"]["attempted"])
                    self.assert_failed_native_resources(error.resource_fields, allocation, session.snapshot())
                    self.assertEqual(error.resource_fields["execution_resource_memory_origin"], "platform")
                    self.assertEqual(error.resource_fields["execution_resource_parallelism_origin"], "context")
                    self.assertFalse(target.exists())
                    self.assertFalse(fanout.exists())
        session.close()

    def test_public_native_failure_envelopes_keep_observations_and_stream_errors(self):
        import shardloom as sl

        allocation = 32 << 20
        client = sl.ShardLoomClient(transport="native", memory_bytes=allocation, max_parallelism=2)
        self.addCleanup(client.close)
        frame = sl.from_rows([{"n": 1}, {"n": 2}], client=client).select("missing")
        with tempfile.TemporaryDirectory(prefix="shardloom-public-native-failure-") as directory:
            target = Path(directory) / "result.jsonl"
            operations = (frame.collect, lambda **kwargs: frame.write_jsonl(target, **kwargs))
            for operation in operations:
                report = operation(check=False)
                self.assertEqual(report.envelope.status, "error")
                fields = self.command_fields(report.envelope.raw)
                self.assert_failed_native_resources(fields, allocation, client._native_backend.session.snapshot())
                self.assertEqual(fields["python_execution_transport"], "native")
                self.assertFalse(report.envelope.fallback.attempted)
                with self.assertRaises(sl.ShardLoomCommandError) as raised:
                    operation(check=True)
                self.assertEqual(raised.exception.envelope.status, "error")
                fields = self.command_fields(raised.exception.envelope.raw)
                self.assert_failed_native_resources(fields, allocation, client._native_backend.session.snapshot())
            with frame.iter_batches() as stream:
                with self.assertRaises(sl.ShardLoomCommandError) as raised:
                    next(stream)
                self.assertIsNone(stream.report)
                fields = self.command_fields(raised.exception.envelope.raw)
                self.assert_failed_native_resources(fields, allocation, client._native_backend.session.snapshot())
            self.assertFalse(target.exists())

    def test_native_memory_denial_evidence_is_frozen_before_later_success(self):
        session = self.native.Session()
        plan = session.sql("SELECT 1 AS n")
        with self.assertRaises(RuntimeError) as raised:
            plan.collect(resources=self.resources(1, 1))
        fields = raised.exception.resource_fields
        frozen = dict(fields)
        self.assert_failed_native_resources(fields, 1, session.snapshot())
        self.assertGreater(int(fields["execution_resource_observed_native_denied_reservations"]), 0)
        result = plan.collect(resources=self.resources(32 << 20, 2))
        self.assertEqual(result.row_export()[0], [{"n": 1}])
        self.assertEqual(fields, frozen)
        session.close()

    def test_native_failed_admission_does_not_borrow_previous_operation_observations(self):
        session = self.session(32 << 20, 2)
        source = session.from_rows([{"n": 1}, {"n": 2}])
        plan = session.sql("SELECT n FROM 'memory://retained'", sources=[("memory://retained", source)])
        self.assertGreater(session.snapshot()["reserved_bytes"], 1)
        with self.assertRaises(RuntimeError) as raised:
            plan.collect(resources=self.resources(1, 1))
        fields = raised.exception.resource_fields
        self.assertEqual(fields["execution_resource_declared_memory_bytes"], "1")
        self.assertEqual(fields["execution_resource_admission_status"], "not_observed")
        for name in ("admitted_memory_bytes", "admitted_max_parallelism", "observed_native_reserved_bytes",
                     "observed_native_peak_reserved_bytes", "observed_native_denied_reservations"):
            self.assertEqual(fields[f"execution_resource_{name}"], "unavailable")
        self.assertEqual(session.snapshot()["memory_limit_bytes"], 32 << 20)
        self.assertEqual(plan.collect().row_export()[0], [{"n": 1}, {"n": 2}])
        session.close()

    def test_native_input_failures_keep_original_exception_and_resource_observations(self):
        session = self.session(32 << 20, 2)
        failure = LookupError("original columnar producer failure")

        class BrokenProducer:
            def __arrow_c_stream__(self, requested_schema=None):
                raise failure

        with self.assertRaises(LookupError) as raised:
            session.from_arrow(BrokenProducer())
        self.assertIs(raised.exception, failure)
        self.assert_failed_native_resources(failure.resource_fields, 32 << 20, session.snapshot())
        with self.assertRaises(TypeError) as raised:
            session.from_rows(42)
        self.assert_failed_native_resources(raised.exception.resource_fields, 32 << 20, session.snapshot())
        session.close()

    def test_native_failure_observations_preserve_producer_with_custom_attribute_assignment(self):
        session = self.session(32 << 20, 2)

        class ProducerError(Exception):
            def __setattr__(self, name, value):
                raise AssertionError("producer exception attributes are managed by the producer")

        failure = ProducerError("original producer error")

        class BrokenProducer:
            def __arrow_c_stream__(self, requested_schema=None):
                raise failure

        with self.assertRaises(ProducerError) as raised:
            session.from_arrow(BrokenProducer())
        self.assertIs(raised.exception, failure)
        self.assert_failed_native_resources(failure.resource_fields, 32 << 20, session.snapshot())
        session.close()

    def test_session_command_uses_the_configured_native_owner_for_generated_vortex_inputs(self):
        rows = [{"value": value, "metric": value * 10} for value in range(1, 6)]
        session = self.session(64 << 20, 2)
        with (
            tempfile.TemporaryDirectory(prefix="shardloom-session-command-owner-") as directory,
            patch("shardloom.client.subprocess.run", side_effect=AssertionError("process launch")),
            patch("shardloom.client.subprocess.Popen", side_effect=AssertionError("worker launch")),
        ):
            path = self.write_vortex_fixture(session, directory, rows)

            count = self.command_envelope(session, self.command_args(path, "count"))
            self.assertEqual(count["status"], "success", count)
            self.assert_no_command_fallback(count)
            count_fields = self.command_fields(count)
            self.assertEqual(count_fields["result_jsonl"], '{"count_all":5}\n')
            self.assertEqual(count_fields["result_known"], "true")
            self.assertEqual(count_fields["execution_resource_admission_status"], "admitted")
            self.assertEqual(count_fields["execution_resource_declared_memory_bytes"], str(64 << 20))
            self.assertEqual(count_fields["execution_resource_admitted_memory_bytes"], str(64 << 20))
            self.assertEqual(count_fields["execution_resource_declared_max_parallelism"], "2")
            self.assertEqual(count_fields["execution_resource_admitted_max_parallelism"], "2")

            projected = self.command_envelope(
                session,
                self.command_args(path, "project", columns="metric"),
            )
            self.assertEqual(projected["status"], "success", projected)
            self.assert_no_command_fallback(projected)
            projected_fields = self.command_fields(projected)
            self.assertEqual(
                json.loads(projected_fields["result_values_json"]),
                [{"metric": value * 10} for value in range(1, 6)],
            )
            self.assertEqual(projected_fields["execution_resource_admission_status"], "admitted")
            self.assertEqual(projected_fields["execution_resource_admitted_memory_bytes"], str(64 << 20))
            self.assertEqual(projected_fields["execution_resource_admitted_max_parallelism"], "2")

    def test_session_command_refuses_smaller_grant_while_native_sources_and_results_are_retained(self):
        rows = [{"value": value, "metric": value * 10} for value in range(1, 6)]
        session = self.session(64 << 20, 2)
        with (
            tempfile.TemporaryDirectory(prefix="shardloom-session-command-retained-") as directory,
            patch("shardloom.client.subprocess.run", side_effect=AssertionError("process launch")),
            patch("shardloom.client.subprocess.Popen", side_effect=AssertionError("worker launch")),
        ):
            path = self.write_vortex_fixture(session, directory, rows)
            retained_source = session.from_rows(rows)
            retained_plan = session.sql(
                "SELECT value, metric FROM 'memory://retained-command-source' ORDER BY value",
                sources=[("memory://retained-command-source", retained_source)],
            )
            retained_result = retained_plan.collect()
            self.assertEqual(retained_result.row_export()[0], rows)
            retained_plan.release_prepared()

            before = session.snapshot()
            retained_bytes = before["reserved_bytes"]
            self.assertIsInstance(retained_bytes, int)
            self.assertGreater(retained_bytes, 1, before)
            insufficient = retained_bytes - 1
            self.assertGreater(insufficient, 0)

            denied = self.command_envelope(
                session,
                self.command_args(
                    path,
                    "count",
                    memory_bytes=insufficient,
                    max_parallelism=1,
                ),
            )
            self.assertEqual(denied["status"], "error", denied)
            self.assert_no_command_fallback(denied)
            denial_reasons = [
                str(item.get("reason", "")).lower()
                for item in denied.get("diagnostics", [])
            ]
            self.assertTrue(
                any("memory allocation cannot cover retained owners" in reason for reason in denial_reasons),
                denied,
            )
            after_denial = session.snapshot()
            self.assertEqual(after_denial["reserved_bytes"], retained_bytes)
            self.assertEqual(after_denial["prepared_source_opens"], before["prepared_source_opens"])

            del retained_result, retained_plan, retained_source
            self.assertEqual(session.snapshot()["reserved_bytes"], 0)

            completed = self.command_envelope(session, self.command_args(path, "count"))
            self.assertEqual(completed["status"], "success", completed)
            self.assert_no_command_fallback(completed)
            self.assertEqual(self.command_fields(completed)["result_jsonl"], '{"count_all":5}\n')

    def test_resource_free_session_command_does_not_turn_a_call_override_into_a_default(self):
        session = self.native.Session()
        self.assertFalse(session.snapshot()["native_owner_initialized"])
        self.assertIsNone(session.snapshot()["memory_limit_bytes"])
        with (
            tempfile.TemporaryDirectory(prefix="shardloom-session-command-explicit-") as directory,
            patch("shardloom.client.subprocess.run", side_effect=AssertionError("process launch")),
            patch("shardloom.client.subprocess.Popen", side_effect=AssertionError("worker launch")),
        ):
            discovery = self.command_envelope(session, ["capabilities"])
            self.assertEqual(discovery["status"], "success", discovery)
            self.assert_no_command_fallback(discovery)
            self.assertFalse(session.snapshot()["native_owner_initialized"])
            self.assertIsNone(session.snapshot()["memory_limit_bytes"])

            fixture_session = self.session(32 << 20, 1)
            path = self.write_vortex_fixture(
                fixture_session,
                directory,
                [{"value": value, "metric": value * 10} for value in range(1, 6)],
            )
            explicit = self.command_envelope(
                session,
                self.command_args(
                    path,
                    "count",
                    memory_bytes=16 << 20,
                    max_parallelism=1,
                ),
            )
            self.assertEqual(explicit["status"], "success", explicit)
            self.assert_no_command_fallback(explicit)
            self.assertEqual(
                self.command_fields(explicit)["execution_resource_admitted_memory_bytes"],
                str(16 << 20),
            )
            self.assertEqual(session.snapshot()["memory_limit_bytes"], 16 << 20)

            opens_before_missing_grant = session.snapshot()["prepared_source_opens"]
            missing_grant = self.command_envelope(session, self.command_args(path, "count"))
            self.assertEqual(missing_grant["status"], "error", missing_grant)
            self.assert_no_command_fallback(missing_grant)
            reasons = [
                str(item.get("reason", "")).lower()
                for item in missing_grant.get("diagnostics", [])
            ]
            self.assertTrue(
                any("missing required execution resources" in reason for reason in reasons),
                missing_grant,
            )
            self.assertEqual(session.snapshot()["prepared_source_opens"], opens_before_missing_grant)

    def test_session_command_inside_columnar_producer_preserves_join_dependency(self):
        pa = self.arrow()
        session = self.session(64 << 20, 1)
        table = pa.table({"id": [1, 2, 3]})
        observed = []
        with (
            tempfile.TemporaryDirectory(prefix="shardloom-session-command-callback-") as directory,
            patch("shardloom.client.subprocess.run", side_effect=AssertionError("process launch")),
            patch("shardloom.client.subprocess.Popen", side_effect=AssertionError("worker launch")),
        ):
            path = self.write_vortex_fixture(
                session,
                directory,
                [{"value": value, "metric": value * 10} for value in range(1, 6)],
            )

            class Producer:
                def __arrow_c_stream__(producer_self, requested_schema=None):
                    discovery = self.command_envelope(session, ["capabilities"])
                    nested_data = self.command_envelope(
                        session,
                        self.command_args(path, "count"),
                        timeout=2,
                    )
                    observed.append((discovery, nested_data))
                    return table.__arrow_c_stream__(requested_schema)

            imported = session.from_arrow(Producer())
            self.assertEqual(imported.row_count, 3)
            self.assertEqual(len(observed), 1)
            discovery, nested_data = observed[0]
            self.assertEqual(discovery["status"], "success", discovery)
            self.assert_no_command_fallback(discovery)
            self.assertEqual(nested_data["status"], "error", nested_data)
            self.assert_no_command_fallback(nested_data)
            reasons = [
                str(item.get("reason", "")).lower()
                for item in nested_data.get("diagnostics", [])
            ]
            self.assertTrue(
                any("nested operation cannot wait" in reason for reason in reasons),
                nested_data,
            )

            imported_plan = session.sql(
                "SELECT id FROM 'memory://callback-input' ORDER BY id",
                sources=[("memory://callback-input", imported)],
            )
            imported_result = imported_plan.collect()
            self.assertEqual(imported_result.row_export()[0], [{"id": 1}, {"id": 2}, {"id": 3}])

            completed = self.command_envelope(
                session,
                self.command_args(path, "project", columns="metric"),
            )
            self.assertEqual(completed["status"], "success", completed)
            self.assert_no_command_fallback(completed)
            self.assertEqual(
                json.loads(self.command_fields(completed)["result_values_json"]),
                [{"metric": value * 10} for value in range(1, 6)],
            )


if __name__ == "__main__":
    unittest.main()
