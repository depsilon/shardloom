"""Python admission rejects before data access and preserves explicit ownership.

The recording transport stops at dispatch; native conformance is tested through
the real CLI separately. These checks do not claim native memory enforcement.
"""

from collections.abc import Sequence
import importlib
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import shardloom as sl
from shardloom.execution_resources import BYTES_PER_GIB


class Dispatched(Exception):
    pass


class RecordingClient(sl.ShardLoomClient):
    def __init__(self, **kwargs):
        super().__init__(binary="unused-shardloom-test-transport", **kwargs)
        self.calls = []

    def run(self, args, **kwargs):
        self.calls.append(tuple(args))
        raise Dispatched()

    def _binary_parts(self):
        return ["unused-shardloom-test-transport"]


class UnopenedRows(Sequence):
    def __init__(self):
        self.accesses = 0

    def __len__(self):
        self.accesses += 1
        raise AssertionError("rows inspected before resource admission")

    def __getitem__(self, index):
        self.accesses += 1
        raise AssertionError("rows inspected before resource admission")


class UnopenedColumnar:
    def __init__(self):
        self.accesses = 0

    def to_dict(self, *args, **kwargs):
        self.accesses += 1
        raise AssertionError("pandas conversion before resource admission")

    def to_pylist(self, *args, **kwargs):
        self.accesses += 1
        raise AssertionError("Arrow conversion before resource admission")


class RequiredResourceBoundaryTests(unittest.TestCase):
    def assert_allocation(self, command, memory_bytes, parallelism,
                          memory_origin="context", parallelism_origin="context"):
        args = list(command)
        for name, expected in (
            ("--memory-bytes", memory_bytes), ("--max-parallelism", parallelism),
            ("--memory-origin", memory_origin), ("--parallelism-origin", parallelism_origin),
        ):
            self.assertEqual(args.count(name), 1, command)
            self.assertEqual(args[args.index(name) + 1], str(expected), command)
        self.assertNotIn("--memory-gb", args)

    def test_unconfigured_lazy_sources_and_routes_remain_inert(self):
        client = RecordingClient()
        ctx = sl.context(client=client)
        frames = [ctx.read("must-not-open.parquet"), ctx.range(0, 100),
                  ctx.sql("SELECT x FROM 'must-not-open.vortex'").limit(3)]
        self.assertEqual(client.calls, [])
        for frame in frames:
            self.assertIsNone(frame.resources)
            with self.assertRaises(Dispatched):
                frame.route()
            self.assertNotIn("--memory-bytes", client.calls[-1])
        self.assertIsNone(client.resources)

    def test_plan_transformations_keep_context_allocation_without_mutating_client(self):
        client = RecordingClient()
        first = sl.context(client=client, memory_bytes=1_500_000_001, max_parallelism=3)
        second = sl.context(client=client, memory_gb=2, max_parallelism=4)
        base = first.read_vortex("must-not-open.vortex")
        plans = [base.filter("x > 0").select("x").limit(5),
                 base.with_engine("batch"),
                 first.sql("SELECT x FROM 'must-not-open.vortex'").select("x").limit(2),
                 first.range(0, 10).with_column("other", sl.col("value") + 1).limit(4),
                 first.from_rows([{"x": 1}]).select("x")]
        self.assertEqual(client.calls, [])
        self.assertIs(first.client, second.client)
        self.assertIsNone(client.resources)
        for plan in plans:
            with self.subTest(plan=type(plan).__name__):
                self.assertEqual(plan.resources, first.resources)
                with self.assertRaises(Dispatched):
                    plan.collect(max_parallelism=2)
                self.assert_allocation(client.calls[-1], 1_500_000_001, 2,
                                       parallelism_origin="execution_call")
        self.assertEqual(first.resources.max_parallelism, 3)
        self.assertEqual(second.resources.memory_bytes, 2 * BYTES_PER_GIB)

    def test_missing_or_invalid_eager_input_resources_never_inspect_rows(self):
        for options in ({}, {"memory_gb": 1}, {"max_parallelism": 1},
                        {"memory_gb": True, "max_parallelism": 1},
                        {"memory_bytes": 1.5, "max_parallelism": 1}):
            for make in (sl.from_rows, sl.literal_table):
                rows = UnopenedRows()
                with self.subTest(options=options, make=make.__name__):
                    with self.assertRaises(sl.ShardLoomResourceConfigurationError):
                        make(rows, **options)
                    self.assertEqual(rows.accesses, 0)
        ctx = sl.context(memory_gb=1, max_parallelism=1)
        rows = UnopenedRows()
        with self.assertRaises(sl.ShardLoomResourceConfigurationError):
            ctx.from_rows(rows, max_parallelism=False)
        self.assertEqual(rows.accesses, 0)
        frame = sl.context().from_rows([{"x": 1}], memory_gb=1, max_parallelism=1)
        self.assertEqual(frame.resources.memory_origin, "execution_call")

    def test_columnar_ipc_and_calendar_reject_before_conversion(self):
        for make in (sl.from_pandas, sl.from_arrow_table):
            value = UnopenedColumnar()
            with self.assertRaises(sl.ShardLoomResourceConfigurationError):
                make(value)
            self.assertEqual(value.accesses, 0)
        with patch("shardloom.query._read_arrow_ipc_table", side_effect=AssertionError("opened")) as opened:
            with self.assertRaises(sl.ShardLoomResourceConfigurationError):
                sl.from_arrow_ipc("must-not-open.arrow")
            opened.assert_not_called()
        with patch("shardloom.query._normalize_date", side_effect=AssertionError("generated")) as generated:
            with self.assertRaises(sl.ShardLoomResourceConfigurationError):
                sl.calendar("2020-01-01", "2020-02-01")
            generated.assert_not_called()

    def test_collect_inspection_and_writers_reject_before_dispatch_or_output(self):
        client = RecordingClient()
        frame = sl.context(client=client).read_vortex("must-not-open.vortex")
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / "must-not-exist"
            terminals = [frame.collect, frame.run, frame.count, frame.schema,
                         lambda: frame.prepare(target), lambda: frame.write_jsonl(target),
                         lambda: frame.write_vortex(target), lambda: frame.fanout({"jsonl": target})]
            for terminal in terminals:
                with self.subTest(terminal=terminal):
                    with self.assertRaises(sl.ShardLoomResourceConfigurationError):
                        terminal()
            self.assertEqual(list(Path(directory).iterdir()), [])
        self.assertEqual(client.calls, [])
        with self.assertRaises(Dispatched):
            frame.collect(memory_gb=1, max_parallelism=1)
        self.assert_allocation(client.calls[-1], BYTES_PER_GIB, 1, "execution_call", "execution_call")

    def test_every_writer_and_sql_terminal_transports_exact_bytes_and_origins(self):
        client = RecordingClient()
        ctx = sl.context(client=client, memory_bytes=1_500_000_001, max_parallelism=3)
        for frame in (ctx.read_vortex("source.vortex"), ctx.sql("SELECT * FROM 'source.vortex'")):
            for name in ("write_json", "write_jsonl", "write_csv", "write_vortex", "write_parquet",
                         "write_arrow_ipc", "write_avro", "write_orc"):
                with self.subTest(frame=type(frame).__name__, writer=name):
                    with self.assertRaises(Dispatched):
                        getattr(frame, name)("never-created", max_parallelism=2)
                    self.assert_allocation(client.calls[-1], 1_500_000_001, 2,
                                           parallelism_origin="execution_call")

    def test_batch_rejection_does_not_open_producer_or_start_transport(self):
        client = RecordingClient()
        calls = []
        def producer():
            calls.append("opened")
            yield [{"x": 1}]
        frame = sl.from_batches(producer, schema={"x": "int64"}, streaming=True, client=client)
        for options in ({}, {"memory_gb": 1}, {"memory_gb": 1, "max_parallelism": True}):
            with self.subTest(options=options):
                with self.assertRaises(sl.ShardLoomResourceConfigurationError):
                    frame.iter_batches(**options)
        self.assertEqual(calls, [])
        with patch("shardloom._batches.subprocess.Popen", side_effect=AssertionError("spawned")) as spawn:
            iterator = frame.iter_batches(memory_bytes=1_500_000_001, max_parallelism=3)
            self.assert_allocation(iterator._args, 1_500_000_001, 3, "execution_call", "execution_call")
            iterator.close()
            spawn.assert_not_called()
        self.assertEqual(calls, [])

    def test_preparation_rejects_before_source_fingerprint_or_native_dispatch(self):
        client = RecordingClient()
        session = sl.session(client=client)
        with patch("shardloom.session._fingerprint_file_metadata", side_effect=AssertionError("opened")) as opened:
            for options in ({}, {"memory_gb": 1}, {"memory_gb": 1, "max_parallelism": 1.5}):
                with self.assertRaises(sl.ShardLoomResourceConfigurationError):
                    session.prepare_vortex("source.parquet", "target.vortex", **options)
            opened.assert_not_called()
        self.assertEqual(client.calls, [])
        configured = sl.session(client=client, memory_bytes=1_500_000_001, max_parallelism=3)
        with self.assertRaises(Dispatched):
            configured.read_vortex("not-opened.vortex").collect()
        self.assert_allocation(client.calls[-1], 1_500_000_001, 3, "session", "session")

    def test_direct_clients_require_resources_before_payload_inspection(self):
        client = RecordingClient()
        routes = [lambda: client.public_workflow_run("sql", sql_statement="SELECT 1"),
                  lambda: client.public_workflow_prepare("cli", input_uri="a.csv", output_ref="a.vortex"),
                  lambda: client.vortex_prepare("a.csv", "a.vortex"),
                  lambda: client.vortex_run("a.vortex", "count"),
                  lambda: client.vortex_count("a.vortex", execute_local_encoded_count=True),
                  lambda: client.public_workflow_batches("sql", sql_statement="SELECT 1")]
        for run in routes:
            with self.assertRaises(sl.ShardLoomResourceConfigurationError):
                run()
        self.assertEqual(client.calls, [])

    def test_platform_ceilings_survive_context_and_object_overrides(self):
        limits = sl.ExecutionResourceLimits(memory_bytes=2 * BYTES_PER_GIB, max_parallelism=4)
        platform = sl.ExecutionResources(BYTES_PER_GIB, 2, "platform", "platform", limits)
        client = RecordingClient(resources=platform)
        ctx = sl.context(client=client)
        frame = ctx.read_vortex("not-opened.vortex").limit(1)
        for options in ({"memory_gb": 3}, {"max_parallelism": 5},
                        {"resources": sl.ExecutionResources.from_gib(3, 2)}):
            with self.assertRaises(sl.ShardLoomResourceConfigurationError):
                frame.collect(**options)
        with self.assertRaises(Dispatched):
            frame.collect(max_parallelism=4)
        self.assert_allocation(client.calls[-1], BYTES_PER_GIB, 4, "platform", "execution_call")
        self.assertIn("--memory-limit-bytes", client.calls[-1])
        self.assertIn("--parallelism-limit", client.calls[-1])
        self.assertEqual(client.resources, platform)

    def test_legacy_data_and_fixture_helpers_require_and_forward_the_same_allocation(self):
        routes = (
            ("local_table_metadata_read_smoke", (), {}),
            ("object_store_read_smoke", ("never-read",), {}),
            ("object_store_partition_discovery_smoke", ("never-list",), {}),
            ("object_store_write_smoke", ("never-read", "never-create"), {}),
            ("object_store_write_recovery_smoke", ("never-read",), {}),
            ("local_table_append_commit_rehearsal_smoke", ("never-create",), {}),
            ("local_table_commit_recovery_smoke", ("never-read",), {}),
            ("sqlite_local_import_export_smoke", ("never-read",),
             {"table": "rows", "export_jsonl": "never-create", "roundtrip_db": "never-create-db"}),
            ("live_fixture_run", (), {}), ("hybrid_overlay_run", (), {}),
            ("live_hybrid_state_transition_smoke", (), {}),
            ("live_hybrid_durable_checkpoint_smoke", ("never-create",), {}),
            ("distributed_local_fixture_run", (), {}),
            ("udf_local_scalar_fixture_smoke", ([1, None],), {}),
            ("embedding_vector_local_fixture_smoke", (["a", "b"],), {}),
        )
        client = RecordingClient()
        unconfigured = sl.context(client=client)
        configured = sl.context(client=client, memory_bytes=1_500_000_001, max_parallelism=3)
        for name, args, kwargs in routes:
            with self.subTest(method=name):
                for surface in (client, unconfigured):
                    with self.assertRaises(sl.ShardLoomResourceConfigurationError):
                        getattr(surface, name)(*args, **kwargs)
                    with self.assertRaises(sl.ShardLoomResourceConfigurationError):
                        getattr(surface, name)(*args, memory_gb=1, max_parallelism=False, **kwargs)
                with self.assertRaises(Dispatched):
                    getattr(configured, name)(*args, max_parallelism=2, **kwargs)
                self.assert_allocation(client.calls[-1], 1_500_000_001, 2,
                                       parallelism_origin="execution_call")
        self.assertEqual(len(client.calls), len(routes))
        self.assertIsNone(client.resources)

    def test_fixture_helpers_reject_before_consuming_caller_sequences(self):
        client = RecordingClient()
        for surface in (client, sl.context(client=client)):
            for name in ("udf_local_scalar_fixture_smoke", "embedding_vector_local_fixture_smoke"):
                values = UnopenedRows()
                with self.subTest(surface=type(surface).__name__, method=name):
                    with self.assertRaises(sl.ShardLoomResourceConfigurationError):
                        getattr(surface, name)(values)
                    self.assertEqual(values.accesses, 0)
        self.assertEqual(client.calls, [])

    def test_generated_storage_helpers_reject_before_consuming_rows_or_creating_output(self):
        client = RecordingClient()
        ctx = sl.context(client=client)
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / "never-create"
            for name, kwargs in (
                ("generated_output_to_object_store", {}),
                ("generated_output_to_partitioned_object_store", {"partition_values": {"region": "west"}}),
                ("foundry_generated_output", {}),
            ):
                rows = UnopenedRows()
                with self.subTest(method=name):
                    with self.assertRaises(sl.ShardLoomResourceConfigurationError):
                        getattr(ctx, name)(target, rows=rows, **kwargs)
                    self.assertEqual(rows.accesses, 0)
                    self.assertEqual(list(Path(directory).iterdir()), [])
            self.assertEqual(client.calls, [])

    def test_import_has_no_ambient_allocation_and_explicit_loading_rejects_invalid_env(self):
        with patch.dict(os.environ, {"SHARDLOOM_MEMORY_GB": "bad", "SHARDLOOM_MAX_PARALLELISM": "eight"}):
            defaults = importlib.import_module("shardloom.runtime_defaults")
            importlib.reload(defaults)
            self.assertFalse(hasattr(defaults, "DEFAULT_LOCAL_RUNTIME_MEMORY_GB"))
            self.assertIsNone(sl.context().resources)
            with self.assertRaises(sl.ShardLoomResourceConfigurationError):
                sl.ExecutionResources.from_env()
            self.assertEqual(sl.context(memory_gb=1, max_parallelism=1).resources.memory_bytes, BYTES_PER_GIB)

    def test_inspection_and_conversion_helpers_admit_resources_before_dispatch(self):
        calls = (
            ("schema", ()), ("describe_schema", ()),
            ("validate_schema", ({"n": "int64"},)),
            ("schema_contract", ({"n": "int64"},)),
            ("data_quality_check", ("not_null:n",)),
            ("data_quality", ("not_null:n",)), ("data_quality_summary", ()),
            ("profile", ()), ("quarantine", ()), ("preview", ()),
            ("head", ()), ("take", (1,)), ("display", ()),
            ("to_python_objects", ()), ("to_pandas", ()), ("to_arrow", ()),
            ("to_arrow_table", ()), ("to_arrow_ipc", ()), ("to_numpy", ()),
        )
        client = RecordingClient()
        ctx = sl.context(client=client)
        frames = (ctx.read_vortex("never-open.vortex"),
                  ctx.sql("SELECT n FROM 'never-open.vortex'"))
        with patch("shardloom.query._optional_module", return_value=object()) as dependency:
            for frame in frames:
                for name, args in calls:
                    with self.subTest(surface=type(frame).__name__, method=name):
                        with self.assertRaises(sl.ShardLoomResourceConfigurationError):
                            getattr(frame, name)(*args)
            self.assertEqual(client.calls, [])
            dependency.assert_not_called()
            for frame in frames:
                for name, args in calls:
                    with self.subTest(surface=type(frame).__name__, method=name):
                        with self.assertRaises(Dispatched):
                            getattr(frame, name)(*args, memory_bytes=1_500_000_001,
                                                 max_parallelism=3)
                        self.assert_allocation(client.calls[-1], 1_500_000_001, 3,
                                               "execution_call", "execution_call")

    def test_context_session_partial_overrides_keep_the_other_origin_and_ceilings(self):
        limits = sl.ExecutionResourceLimits(memory_bytes=2 * BYTES_PER_GIB, max_parallelism=4)
        client = RecordingClient()
        ctx = sl.context(client=client, memory_bytes=BYTES_PER_GIB + 1,
                         max_parallelism=2, resource_limits=limits)
        session = ctx.session(max_parallelism=3)
        self.assertEqual(session.resources.memory_origin, "context")
        self.assertEqual(session.resources.parallelism_origin, "session")
        self.assertEqual(session.resources.limits, limits)
        with self.assertRaises(sl.ShardLoomResourceConfigurationError):
            ctx.session(max_parallelism=5)
        with self.assertRaises(Dispatched):
            session.read_vortex("never-open.vortex").to_python_objects(max_parallelism=4)
        self.assert_allocation(client.calls[-1], BYTES_PER_GIB + 1, 4,
                               "context", "execution_call")
        self.assertEqual(ctx.resources.max_parallelism, 2)
        self.assertEqual(session.resources.max_parallelism, 3)
        self.assertIsNone(client.resources)

    def test_direct_context_calls_inherit_override_and_validate_before_dispatch(self):
        client = RecordingClient()
        ctx = sl.context(client=client, memory_bytes=1500000001, max_parallelism=3)
        operations = (
            lambda **kwargs: ctx.run("sql", sql_statement="SELECT 1", **kwargs),
            lambda **kwargs: ctx.prepare("python", input_uri="never-open.csv",
                                         output_ref="never-create.vortex", **kwargs),
            lambda **kwargs: ctx.route("sql", sql_statement="SELECT 1", **kwargs),
        )
        for operation in operations:
            with self.assertRaises(Dispatched):
                operation()
            self.assert_allocation(client.calls[-1], 1500000001, 3)
            with self.assertRaises(Dispatched):
                operation(max_parallelism=2)
            self.assert_allocation(client.calls[-1], 1500000001, 2,
                                   parallelism_origin="execution_call")
            before = len(client.calls)
            with self.assertRaises(sl.ShardLoomResourceConfigurationError):
                operation(memory_bytes=False)
            self.assertEqual(len(client.calls), before)
        self.assertEqual(ctx.resources.memory_bytes, 1500000001)
        self.assertEqual(ctx.resources.max_parallelism, 3)
        self.assertIsNone(client.resources)

    def test_ceiling_only_client_context_and_session_keep_lazy_grants_unset(self):
        limits = sl.ExecutionResourceLimits(memory_bytes=1024, max_parallelism=2)
        client = RecordingClient(resource_limits=limits)
        owners = (
            sl.context(client=client), sl.session(client=client),
            sl.context(resource_limits=limits), sl.session(resource_limits=limits),
            sl.ShardLoomContext.from_env(env={}, resource_limits=limits),
            sl.ShardLoomContext.from_repo("never-open", resource_limits=limits),
        )
        for owner in owners:
            self.assertIsNone(owner.resources)
            self.assertEqual(owner.resource_limits, limits)
        self.assertIsNone(client.resources)
        self.assertEqual(client.resource_limits, limits)
        self.assertEqual(client.calls, [])
        for options in ({}, {"memory_bytes": 1025, "max_parallelism": 1},
                        {"resources": sl.ExecutionResources(512, 3)}):
            with self.assertRaises(sl.ShardLoomResourceConfigurationError):
                client.public_workflow_run("sql", sql_statement="SELECT 1", **options)
        with self.assertRaises(Dispatched):
            client.public_workflow_run("sql", sql_statement="SELECT 1",
                                       memory_bytes=1024, max_parallelism=2)
        self.assert_allocation(client.calls[-1], 1024, 2, "execution_call", "execution_call")
        self.assertIn("--memory-limit-bytes", client.calls[-1])
        self.assertIn("--parallelism-limit", client.calls[-1])

    def test_context_only_ceilings_survive_descendants_without_mutating_shared_client(self):
        client = RecordingClient()
        limits = sl.ExecutionResourceLimits(memory_bytes=2048, max_parallelism=2)
        ctx = sl.context(client=client, resource_limits=limits)
        session = ctx.session(resource_limits=sl.ExecutionResourceLimits(4096, 1))
        self.assertEqual(session.resource_limits, sl.ExecutionResourceLimits(2048, 1))
        frames = (
            ctx.read_vortex("never-open.vortex").filter("n > 0").select("n").limit(1),
            ctx.range(0, 3).with_column("other", sl.col("value") + 1),
            ctx.sql("SELECT n FROM 'never-open.vortex'").select("n").limit(1),
            session.read_vortex("never-open.vortex").select("n"),
            session.sql("SELECT n FROM 'never-open.vortex'").select("n"),
            sl.read_vortex("never-open.vortex", client=client, resource_limits=limits).limit(1),
        )
        for frame in frames:
            with self.subTest(frame=type(frame).__name__):
                for options in ({}, {"memory_bytes": 2049, "max_parallelism": 1},
                                {"resources": sl.ExecutionResources(2049, 1)}):
                    with self.assertRaises(sl.ShardLoomResourceConfigurationError):
                        frame.collect(**options)
                with self.assertRaises(Dispatched):
                    frame.collect(memory_bytes=2048, max_parallelism=1)
                self.assert_allocation(client.calls[-1], 2048, 1, "execution_call", "execution_call")
                self.assertIn("--memory-limit-bytes", client.calls[-1])
        self.assertIsNone(client.resources)
        self.assertIsNone(client.resource_limits)
        self.assertIsNone(ctx.resources)
        self.assertIsNone(session.resources)

    def test_ceiling_only_input_and_writers_refuse_before_access_or_creation(self):
        client = RecordingClient()
        ctx = sl.context(client=client, resource_limits=sl.ExecutionResourceLimits(1024, 1))
        rows = UnopenedRows()
        columnar = UnopenedColumnar()
        for call in (
            lambda: ctx.from_rows(rows, memory_bytes=1025, max_parallelism=1),
            lambda: ctx.from_arrow_table(columnar, memory_bytes=1025, max_parallelism=1),
            lambda: ctx.from_pandas(columnar, memory_bytes=1025, max_parallelism=1),
        ):
            with self.assertRaises(sl.ShardLoomResourceConfigurationError):
                call()
        demanded = []
        def batches():
            demanded.append(True)
            yield [{"n": 1}]
        frame = ctx.from_batches(batches, schema={"n": "int64"}).select("n")
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / "never-create.vortex"
            for call in (
                lambda: frame.iter_batches(memory_bytes=1025, max_parallelism=1),
                lambda: frame.write_vortex(target, memory_bytes=1025, max_parallelism=1),
                lambda: ctx.prepare("python", input_uri="never-open.csv", output_ref=target,
                                    memory_bytes=1025, max_parallelism=1),
            ):
                with self.assertRaises(sl.ShardLoomResourceConfigurationError):
                    call()
            self.assertEqual(list(Path(directory).iterdir()), [])
        self.assertEqual(rows.accesses, 0)
        self.assertEqual(columnar.accesses, 0)
        self.assertEqual(demanded, [])
        self.assertEqual(client.calls, [])


if __name__ == "__main__":
    unittest.main()
