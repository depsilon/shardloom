"""Integration contract for the optional in-process native Python binding."""

from __future__ import annotations

import importlib
import importlib.util
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


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


class NativeBindingTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.native = _load_native()

    def resources(self, memory_bytes: int = 32 << 20, lanes: int = 2, **kwargs):
        return self.native.ExecutionResources(memory_bytes, lanes, **kwargs)

    def session(self, memory_bytes: int = 32 << 20, lanes: int = 2):
        return self.native.Session(self.resources(memory_bytes, lanes))

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
        try:
            import numpy as np
        except ImportError:
            _unavailable("NumPy is required for the Arrow source-buffer ownership check")

        session = self.session(64 << 20, 2)
        backing = np.array([2, 7, 11], dtype=np.int64)
        batch = pa.record_batch(
            [
                pa.array(backing),
                pa.array([True, None, False], type=pa.bool_()),
                pa.array(["alpha", None, "λ"], type=pa.string()),
            ],
            names=["id", "enabled", "label"],
        )
        producer = pa.RecordBatchReader.from_batches(batch.schema, [batch])
        source = session.from_arrow(producer)
        backing[:] = -99
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


if __name__ == "__main__":
    unittest.main()
