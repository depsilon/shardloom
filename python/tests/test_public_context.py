"""Application context/result contracts, including real native ownership."""

from __future__ import annotations

import os
from pathlib import Path
import runpy
import sys
import tempfile
import threading
import time
import unittest
from unittest import mock

import shardloom as sl
from shardloom import _native_backend
from shardloom.client import ShardLoomClient
from shardloom.errors import (
    ShardLoomClosedError, ShardLoomCommandError, ShardLoomError,
    ShardLoomInstallationError, ShardLoomProtocolError,
)
from shardloom.query import Result, VortexWorkflowExecutionReport
from shardloom.models import OutputEnvelope
try:
    from .test_native_binding import _load_native
except ImportError:
    from test_native_binding import _load_native


GRANT = {"memory_bytes": 64 << 20, "max_parallelism": 2}


class PublicContextContractTests(unittest.TestCase):
    def test_installation_selects_once_without_loading_native_or_allocating(self):
        with mock.patch.dict(os.environ, {}, clear=True), mock.patch.object(
            _native_backend, "load_native", side_effect=AssertionError("eager extension load")
        ), mock.patch("shardloom.client.subprocess.Popen", side_effect=AssertionError("child")):
            for platform, expected in [("linux", "native"), ("darwin", "native"), ("win32", "subprocess")]:
                with self.subTest(platform=platform), mock.patch.object(sys, "platform", platform):
                    with sl.context() as ctx:
                        self.assertEqual(ctx.transport, expected)
                        self.assertIsNone(ctx.resources)
                        with mock.patch.dict(os.environ, {"SHARDLOOM_BIN": "later"}):
                            self.assertEqual(ctx.transport, expected)
            for configuration in ({"binary": "declared-cli"}, {"repo_root": "/declared/source"},
                                  {"env": {"SHARDLOOM_BIN": "declared-cli"}},
                                  {"transport": "subprocess"}):
                with sl.context(**configuration) as ctx:
                    self.assertEqual(ctx.transport, "subprocess")
            with mock.patch.object(sys, "platform", "unadmitted-os"):
                with self.assertRaises(ShardLoomInstallationError) as failure:
                    sl.context()
                self.assertEqual(failure.exception.diagnostics[0].code, "SL_NATIVE_UNAVAILABLE")
            with mock.patch.object(sys, "platform", "linux"):
                self.assertEqual(sl.read_csv("unopened.csv").client.transport, "native")

    def test_missing_and_malformed_resources_precede_extension_or_dataset_access(self):
        with mock.patch.object(_native_backend, "load_native", side_effect=AssertionError("native load")):
            with sl.context(transport="native") as ctx:
                plan = ctx.read_csv("must-not-be-opened.csv").select("id")
                for resources in ({}, {"memory_bytes": 0, "max_parallelism": 1},
                                  {"memory_bytes": 1 << 20, "max_parallelism": True}):
                    with self.subTest(resources=resources), self.assertRaises(sl.ShardLoomResourceConfigurationError):
                        plan.collect(**resources)
                with self.assertRaises(sl.ShardLoomResourceConfigurationError):
                    ctx.from_result(object())

    def test_missing_selected_native_backend_is_typed_and_never_starts_cli(self):
        with sl.context(transport="native", **GRANT) as ctx:
            with mock.patch("shardloom.client.subprocess.Popen", side_effect=AssertionError("backend replay")), \
                 mock.patch.object(_native_backend.importlib, "import_module", side_effect=ImportError("missing")):
                with self.assertRaises(ShardLoomInstallationError) as failure:
                    ctx.sql("SELECT 1 AS id").collect()
                self.assertIsInstance(failure.exception, (ShardLoomError, ImportError))
                self.assertFalse(failure.exception.diagnostics[0].fallback.attempted)

    def test_context_session_close_revokes_shared_work_for_both_backends(self):
        for transport in ("native", "subprocess"):
            with self.subTest(transport=transport):
                ctx = sl.context(transport=transport, **GRANT)
                session = ctx.session()
                self.assertIs(ctx.session(), session)
                plan = ctx.sql("SELECT 1 AS id")
                session.close()
                self.assertTrue(ctx.closed)
                self.assertTrue(session.closed)
                with self.assertRaises(ShardLoomClosedError):
                    plan.collect()
                with self.assertRaises(ShardLoomClosedError):
                    ctx.sql("SELECT 2 AS id")
                with self.assertRaises(ShardLoomClosedError):
                    ctx.cancel()
                ctx.close()

    def test_result_legacy_name_is_the_same_owner_type(self):
        self.assertIs(Result, VortexWorkflowExecutionReport)
        self.assertIs(sl.Result, sl.VortexWorkflowExecutionReport)

    def test_worker_request_exit_is_never_replayed(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            trace, peer = root / "requests", root / "peer.py"
            peer.write_text(
                "import sys\nfrom pathlib import Path\n"
                f"trace = Path({str(trace)!r})\n"
                "if sys.argv[1] == 'python-worker':\n"
                "    request = sys.stdin.readline()\n"
                "    trace.write_text('admitted: ' + request)\n"
                "    raise SystemExit(5)\n"
                "with trace.open('a') as output: output.write('REPLAYED')\n"
            )
            client = ShardLoomClient(binary=[sys.executable, str(peer)], use_persistent_worker=True, timeout=3)
            self.addCleanup(client.close)
            with mock.patch.object(client, "_run_subprocess", side_effect=AssertionError("replayed")):
                with self.assertRaisesRegex(ShardLoomProtocolError, "No request was replayed"):
                    client.run(["run", "sql", "--sql", "SELECT 1"])
            self.assertEqual(trace.read_text().count("admitted:"), 1)
            self.assertNotIn("REPLAYED", trace.read_text())
            self.assertIsNone(client._worker_process)

    def test_cancelling_process_calls_reports_uncertain_publication_and_allows_reuse(self):
        for persistent in (False, True):
            with self.subTest(persistent=persistent), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                entered, peer = root / "entered", root / "peer.py"
                peer.write_text(
                    "import json, sys, time\nfrom pathlib import Path\n"
                    "if sys.argv[1] == 'python-worker': sys.stdin.readline()\n"
                    f"entered = Path({str(entered)!r})\n"
                    "if entered.exists():\n"
                    f"    print(json.dumps({dict(OutputEnvelope.from_field_mapping({'test_reused': 'true'}, command='status').raw)!r}), flush=True)\n"
                    "    raise SystemExit(0)\n"
                    "entered.write_text('entered')\n"
                    "while True: time.sleep(0.1)\n"
                )
                client = ShardLoomClient(binary=[sys.executable, str(peer)], use_persistent_worker=persistent)
                self.addCleanup(client.close)
                outcomes = []

                def run():
                    try:
                        outcomes.append(client.run(["run"]))
                    except BaseException as error:
                        outcomes.append(error)

                thread = threading.Thread(target=run, daemon=True)
                thread.start()
                try:
                    deadline = time.monotonic() + 5
                    while not entered.exists() and thread.is_alive() and time.monotonic() < deadline:
                        time.sleep(0.01)
                    self.assertTrue(entered.exists())
                    client.cancel()
                    thread.join(5)
                    self.assertFalse(thread.is_alive())
                    self.assertEqual(len(outcomes), 1)
                    self.assertIsInstance(outcomes[0], ShardLoomCommandError)
                    self.assertEqual(outcomes[0].diagnostics[0].code, "SL_EXECUTION_CANCELLED")
                    self.assertIn("cannot be confirmed", str(outcomes[0]))
                    self.assertEqual(client._active_processes, set())
                    self.assertFalse(client.closed)
                    self.assertEqual(client.status().field("test_reused"), "true")
                finally:
                    client.close()
                    thread.join(5)


class PublicNativeContextTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.native = _load_native()

    def context(self):
        with mock.patch.dict(os.environ, {}, clear=True):
            context = sl.context(**GRANT)
        self.assertEqual(context.transport, "native")
        self.addCleanup(context.close)
        return context

    def test_sql_dataframe_use_actual_shared_native_plan_and_values(self):
        ctx = self.context()
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "orders.csv"
            source.write_text("id,total,status\n1,12,paid\n2,8,open\n3,25,paid\n")
            frame = ctx.read_csv(source).filter(sl.col("status") == "paid").select("id", "total").sort("id")
            sql = ctx.sql(f"SELECT id,total FROM '{source}' WHERE status = 'paid' ORDER BY id")
            plans = []
            original = _native_backend.NativeBackend._plan

            def track(backend, *args, **kwargs):
                plan = original(backend, *args, **kwargs)
                plans.append(plan)
                return plan

            with mock.patch.object(_native_backend.NativeBackend, "_plan", track), \
                 mock.patch("shardloom.client.subprocess.Popen", side_effect=AssertionError("worker launch")):
                results = [frame.collect(), sql.collect()]
            self.assertEqual(len(plans), 2)
            self.assertTrue(all(type(plan) is self.native.Plan for plan in plans))
            for result in results:
                self.assertIsInstance(result, Result)
                self.assertEqual(result.to_rows(), ({"id": 1, "total": 12}, {"id": 3, "total": 25}))
                self.assertIs(result.details, result.envelope)
                self.assertEqual(result.details.field("python_result_ownership"), "retained_native")
                self.assertEqual(result.details.field("resident_provider_crate"), "vortex")
                self.assertEqual(result.details.field("resident_provider_version"), "0.85")
                self.assertEqual(tuple(name for name, _ in result.schema), ("id", "total"))

    def test_native_chaining_keeps_exact_values_after_context_close_without_export(self):
        ctx = self.context()
        schema = {"id": "int64", "items": {"type": "list", "item": "int64"},
                  "record": {"type": "struct", "fields": {"label": "utf8"}}}
        rows = [{"id": 1, "items": [7, None, -2], "record": {"label": "港"}},
                {"id": 2, "items": None, "record": None}]
        result = ctx.from_rows(rows, schema=schema).collect()
        empty = ctx.from_rows([], schema=schema).collect()
        ctx.close()
        receiver = self.context()
        with mock.patch.object(Result, "to_rows", side_effect=AssertionError("row conversion")), \
             mock.patch.object(Result, "to_arrow", side_effect=AssertionError("Arrow conversion")), \
             mock.patch.object(Result, "python_objects", new_callable=mock.PropertyMock, side_effect=AssertionError("row export")):
            chained = receiver.from_result(result).select("id", "items", "record").collect()
            empty_chain = receiver.from_result(empty).collect()
        receiver.close()
        self.assertEqual(chained.to_rows(), tuple(rows))
        self.assertEqual(empty_chain.to_rows(), ())
        self.assertEqual(chained.schema, result.schema)
        self.assertEqual(empty_chain.schema, empty.schema)

    def test_development_example_reuses_completed_data_after_close(self):
        path = Path(__file__).resolve().parents[2] / "examples/paid-orders/context-result.py"
        example = runpy.run_path(str(path))
        result = example["reuse_completed_result"]("native")
        self.assertEqual(result.to_rows(), ({"id": 1}, {"id": 3}))
        self.assertIsNotNone(result.native_result)

    def test_execution_conveniences_raise_typed_errors_by_default(self):
        ctx = self.context()
        with tempfile.TemporaryDirectory() as directory:
            missing = Path(directory) / "missing.csv"
            frame = ctx.read_csv(missing)
            sql = ctx.sql(f"SELECT * FROM '{missing}'")
            for plan in (frame, sql):
                for name, arguments in (
                    ("collect", ()), ("preview", ()), ("head", ()),
                    ("take", (2,)), ("profile", ()), ("to_python_objects", ()),
                    ("to_arrow", ()), ("to_arrow_table", ()), ("to_arrow_ipc", ()),
                ):
                    with self.subTest(plan=type(plan).__name__, terminal=name):
                        with self.assertRaises(ShardLoomCommandError) as failure:
                            getattr(plan, name)(*arguments)
                        self.assertTrue(failure.exception.diagnostics)
                        self.assertFalse(failure.exception.envelope.fallback.attempted)
                self.assertTrue(plan.collect(check=False).is_error)
            with self.assertRaises(ShardLoomCommandError):
                frame.count()

    def test_file_plan_rejects_changed_generation_without_replaying_completed_data(self):
        ctx = self.context()
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "changing.csv"
            source.write_text("id\n1\n2\n", encoding="utf-8")
            plan = ctx.read_csv(source).sort("id")
            retained = plan.collect()
            self.assertEqual(plan.collect().to_rows(), ({"id": 1}, {"id": 2}))
            source.write_text("id\n7\n8\n9\n", encoding="utf-8")
            with self.assertRaises(ShardLoomCommandError) as changed:
                plan.collect()
            self.assertIn("source generation changed", str(changed.exception))
            self.assertFalse(changed.exception.envelope.fallback.attempted)
            prepared = Path(directory) / "generation-2.vortex"
            ctx.prepare_vortex(source, prepared)
            fresh = ctx.read_vortex(prepared).sort("id").collect()
            self.assertEqual(fresh.to_rows(), ({"id": 7}, {"id": 8}, {"id": 9}))
            self.assertEqual(retained.to_rows(), ({"id": 1}, {"id": 2}))

    def test_session_reuse_false_releases_preparation_without_closing_context(self):
        ctx = self.context()
        session = ctx.session()
        for _ in range(2):
            result = session.sql("SELECT 7 AS id").collect(reuse=False)
            self.assertIsInstance(result, Result)
            self.assertEqual(result.to_rows(), ({"id": 7},))
            self.assertFalse(ctx.closed)
            self.assertEqual(ctx.from_result(result).collect().to_rows(), ({"id": 7},))
        ctx.close()
        self.assertEqual(result.to_rows(), ({"id": 7},))

    def test_session_python_conversion_preserves_logical_binary_values(self):
        from shardloom.diagnostics import SessionLazyFrame
        ctx = self.context()
        rows = ({"id": 1, "payload": b"\x00\xff"}, {"id": 2, "payload": None})
        frame = ctx.from_rows(rows, schema={"id": "int64", "payload": "binary"})
        session_frame = SessionLazyFrame(ctx.session(), frame)
        self.assertEqual(session_frame.to_python_objects(), rows)

    def test_context_cancel_closes_paused_stream_and_preserves_delivered_values(self):
        ctx = self.context()
        with ctx.range(0, 120_013, column="id").iter_batches(batch_rows=2048) as stream:
            batches = list(stream)
            self.assertIsNotNone(stream.report)
        self.assertEqual(sum(len(batch.to_rows()) for batch in batches), 120_013)
        self.assertEqual(batches[-1].to_rows()[-1], {"id": 120_012})
        stream = ctx.range(0, 50, column="id").iter_batches(batch_rows=3)
        retained = next(stream)
        ctx.cancel()
        self.assertIsNone(stream.report)
        with self.assertRaises(sl.ShardLoomCommandError) as cancelled:
            next(stream)
        self.assertEqual(cancelled.exception.diagnostics[0].code, "SL_EXECUTION_CANCELLED")
        with self.assertRaises(StopIteration):
            next(stream)
        self.assertEqual(ctx.sql("SELECT 9 AS id").collect().to_rows(), ({"id": 9},))
        unopened = ctx.range(0, 10, column="id").iter_batches()
        ctx.close()
        with self.assertRaises(sl.ShardLoomCommandError):
            next(unopened)
        self.assertEqual(retained.to_rows(), ({"id": 0}, {"id": 1}, {"id": 2}))

    def test_close_and_cancel_do_not_wait_for_a_running_native_producer(self):
        for action in ("cancel", "close"):
            with self.subTest(action=action):
                ctx = self.context()
                entered, release = threading.Event(), threading.Event()
                outcomes, stops = [], []

                def producer():
                    entered.set()
                    if not release.wait(5):
                        raise RuntimeError("test producer was not released")
                    yield [{"id": 7}]

                stream = ctx.from_batches(
                    producer, schema={"id": "int64"}, streaming=True,
                ).iter_batches()

                def pull():
                    try:
                        outcomes.append(next(stream))
                    except BaseException as error:
                        outcomes.append(error)

                def stop():
                    try:
                        getattr(ctx, action)()
                        stops.append("returned")
                    except BaseException as error:
                        stops.append(error)

                worker = threading.Thread(target=pull, daemon=True)
                stopper = threading.Thread(target=stop, daemon=True)
                worker.start()
                try:
                    self.assertTrue(entered.wait(5))
                    stopper.start()
                    stopper.join(2)
                    self.assertEqual(stops, ["returned"])
                finally:
                    release.set()
                    worker.join(5)
                    if stopper.ident is not None:
                        stopper.join(5)
                    ctx.close()
                self.assertFalse(worker.is_alive())
                self.assertEqual(len(outcomes), 1)
                self.assertIsInstance(outcomes[0], ShardLoomCommandError)
                self.assertIsNone(stream.report)
                self.assertEqual(ctx.closed, True)

    def test_write_receipt_and_reopen_report_real_publication(self):
        ctx = self.context()
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / "result.vortex"
            frame = ctx.from_rows([{"id": 1}, {"id": 3}], schema={"id": "int64"})
            receipt = frame.write_vortex(target)
            self.assertIsInstance(receipt, Result)
            self.assertEqual(receipt.output_commit_status, "committed")
            self.assertEqual(ctx.read_vortex(target).collect().to_rows(), ({"id": 1}, {"id": 3}))
            before = target.read_bytes()
            with self.assertRaises(ShardLoomCommandError):
                frame.write_vortex(target)
            self.assertEqual(target.read_bytes(), before)
            with self.assertRaises(ValueError):
                ctx.from_result(receipt)


@unittest.skipUnless(os.environ.get("SHARDLOOM_TEST_INSTALLED_QUICKSTART") == "1",
                     "requires an explicitly selected clean installed package")
class InstalledProcessResultTests(unittest.TestCase):
    def test_process_result_chaining_preserves_empty_nested_nullable_and_exact_types(self):
        schema = {
            "id": "uint64", "items": {"type": "list", "item": "int64"},
            "record": {"type": "struct", "fields": {"label": "utf8"}},
            "payload": "binary",
        }
        rows = ({"id": (1 << 64) - 1, "items": [7, None, -2],
                 "record": {"label": "港"}, "payload": b"\x00\xff"},
                {"id": 2, "items": None, "record": None, "payload": None})
        for source_transport in ("native", "subprocess"):
            with self.subTest(source_transport=source_transport):
                with sl.context(transport=source_transport, **GRANT) as source:
                    result = source.from_rows(rows, schema=schema).collect()
                    empty = source.from_rows([], schema=schema).collect()
                with sl.context(transport="subprocess", **GRANT) as receiver:
                    chained = receiver.from_result(result).select("id", "items", "record", "payload").collect()
                    empty_chain = receiver.from_result(empty).collect()
                self.assertEqual(chained.to_rows(), rows)
                self.assertEqual(empty_chain.to_rows(), ())
                self.assertEqual(chained.schema, result.schema)
                self.assertEqual(empty_chain.schema, empty.schema)
                for completed in (chained, empty_chain):
                    self.assertIsNone(completed.native_result)
                    self.assertFalse(completed.fallback_attempted)
                    self.assertFalse(completed.external_engine_invoked)


if __name__ == "__main__":
    unittest.main()
