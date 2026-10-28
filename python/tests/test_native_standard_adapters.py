"""Independent ADBC/DB-API/IPython clients over the shared native context."""

from datetime import date
from decimal import Decimal
import gc
import unittest

try:
    from .test_native_binding import _load_native
except ImportError:
    from test_native_binding import _load_native


class NativeStandardAdapterTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.native = _load_native()
        import shardloom as sl
        import pyarrow as pa
        try:
            import adbc_driver_manager as manager
        except ImportError as error:
            raise unittest.SkipTest("optional independent ADBC manager is not installed") from error
        cls.sl, cls.pa, cls.manager = sl, pa, manager

    def context(self):
        context = self.sl.context(transport="native", memory_bytes=128 << 20, max_parallelism=2)
        self.addCleanup(context.close)
        return context

    def open(self, context):
        from shardloom.adbc import database
        db = database(context)
        self.addCleanup(db.close)
        connection = self.manager.AdbcConnection(db)
        self.addCleanup(connection.close)
        return connection

    def statement(self, connection, query):
        statement = self.manager.AdbcStatement(connection)
        self.addCleanup(statement.close)
        statement.set_sql_query(query)
        statement.prepare()
        return statement

    def bind(self, statement, batch):
        schema, array = batch.__arrow_c_array__()
        statement.bind(array, schema)

    def reader(self, statement):
        handle, count = statement.execute_query()
        self.assertEqual(count, -1)
        return self.pa.RecordBatchReader.from_stream(handle)

    def test_independent_manager_typed_parameters_snapshot_and_no_stale_binding(self):
        ctx, pa = self.context(), self.pa
        ctx.register("numbers", ctx.range(0, 5, column="value"))
        statement = self.statement(self.open(ctx),
            "WITH selected(x) AS (SELECT value FROM numbers WHERE value>=:minimum) "
            "SELECT x+:add AS answer,:money AS money,:day AS day,:missing AS missing FROM selected ORDER BY answer")
        schema = pa.schema(statement.get_parameter_schema())
        self.assertEqual(schema.names, ["add", "day", "minimum", "missing", "money"])
        self.assertFalse(ctx.client._native_backend.session.snapshot()["native_owner_initialized"])
        ctx.register("numbers", ctx.range(90, 92, column="value"), replace=True)
        ctx.drop_view("numbers")
        for minimum in (3, 4):
            batch = pa.record_batch({"add": [10], "minimum": [minimum], "money": [Decimal("12.340")],
                "day": [date(1970, 1, 2)], "missing": pa.array([None], type=pa.int64())})
            self.bind(statement, batch)
            schema = pa.schema(statement.execute_schema())
            self.assertEqual(schema.field("money").type, pa.decimal128(5, 3))
            result = self.reader(statement).read_all()
            self.assertEqual(result.to_pylist(), [{"answer": value + 10, "money": Decimal("12.340"),
                "day": date(1970, 1, 2), "missing": None} for value in range(minimum, 5)])
            with self.assertRaisesRegex(self.manager.Error, "missing named scalar"):
                statement.execute_query()
        self.bind(statement, batch)
        with self.assertRaisesRegex(self.manager.Error, "exactly"):
            self.bind(statement, pa.record_batch({"wrong": [1]}))
        with self.assertRaisesRegex(self.manager.Error, "missing named scalar"):
            statement.execute_query()

    def test_independent_dbapi_fetches_connected_plan_with_exact_values(self):
        import adbc_driver_manager.dbapi as independent
        from shardloom.adbc import driver_options
        from shardloom import functions as f
        ctx = self.context()
        base = ctx.range(0, 5, column="id").filter(self.sl.col("id") >= 1)
        ctx.register("facts", base)
        query = ctx.sql("WITH a(x) AS (SELECT id FROM facts) SELECT x AS id,x+:amount AS total FROM a",
                        parameters={"amount": 10})
        dimension = ctx.range(1, 5, column="id").select("id", "id+100 AS label")
        combined = query.join(dimension, on="id").select("f.id AS id", "f.total AS total", "d.label AS label")
        combined = combined.select("id", "total", "label", f.row_number().over(self.sl.Window.order_by("id")).alias("position"))
        ctx.register("joined", combined.observe("matched", rows=f.count()))
        with driver_options(ctx) as options:
            connection = independent.connect(db_kwargs=options, autocommit=True)
        with connection, connection.cursor() as cursor:
            cursor.execute("SELECT * FROM joined WHERE id>=:start ORDER BY id", {"start": 2})
            self.assertEqual([column[0] for column in cursor.description], ["id", "total", "label", "position"])
            self.assertEqual(cursor.fetchone(), (2, 12, 102, 2))
            self.assertEqual(cursor.fetchmany(1), [(3, 13, 103, 3)])
            self.assertEqual(cursor.fetchall(), [(4, 14, 104, 4)])
            self.assertIsNone(cursor.fetchone())
            cursor.execute("SELECT * FROM joined WHERE id>=:start ORDER BY id", {"start": 4})
            self.assertEqual(cursor.fetchall(), [(4, 14, 104, 4)])
            with self.assertRaises(independent.Error):
                cursor.execute("SELECT * FROM joined WHERE id>=:start ORDER BY id")
        self.assertFalse(ctx.closed)
        self.assertEqual(ctx.table("facts").count().to_rows(), ({"count": 4},))

    def test_metadata_and_prepare_never_open_a_producer(self):
        ctx, pa = self.context(), self.pa
        events = []
        def producer():
            events.append("opened")
            yield [{"id": 1, "text": "é"}]
        ctx.register("facts", ctx.from_batches(producer, schema={"id": "int64", "text": "utf8"}, streaming=True))
        connection = self.open(ctx)
        statement = self.statement(connection, "SELECT id FROM facts WHERE id>=:minimum")
        self.bind(statement, pa.record_batch({"minimum": [0]}))
        self.assertEqual(pa.schema(statement.execute_schema()).names, ["id"])
        self.assertEqual(pa.schema(connection.get_table_schema(None, None, "facts")).names, ["id", "text"])
        tables = pa.RecordBatchReader.from_stream(connection.get_table_types()).read_all().to_pylist()
        self.assertEqual(tables, [{"table_type": "VIEW"}])
        info = pa.RecordBatchReader.from_stream(connection.get_info([0, 3, 4, 103])).read_all().to_pylist()
        self.assertEqual({row["info_name"]: row["info_value"] for row in info}, {0: "ShardLoom", 3: True, 4: False, 103: 1_001_000})
        objects = pa.RecordBatchReader.from_stream(connection.get_objects(
            self.manager.GetObjectsDepth.ALL, table_name="f%", column_name="i_")).read_all().to_pylist()
        table = objects[0]["catalog_db_schemas"][0]["db_schema_tables"][0]
        self.assertEqual(table["table_name"], "facts")
        self.assertEqual([column["column_name"] for column in table["table_columns"]], ["id"])
        self.assertEqual(events, [])
        with self.assertRaisesRegex(self.manager.Error, "exactly one row"):
            self.bind(statement, pa.record_batch({"minimum": [0, 1]}))
        for query in ("SELECT ? AS value", "DELETE FROM facts", "SELECT * FROM missing"):
            with self.subTest(query=query), self.assertRaises(self.manager.Error):
                statement.set_sql_query(query)
                statement.prepare()
        self.assertEqual(events, [])

    def test_parameter_schema_preserves_unsigned_decimal_and_typed_null(self):
        from shardloom import types
        ctx, pa = self.context(), self.pa
        statement = self.statement(self.open(ctx), "SELECT :small AS small,:money AS money,:empty AS empty")
        batch = pa.record_batch([pa.array([1], type=pa.uint64()),
            pa.array([Decimal("1.20")], type=pa.decimal128(12, 2)), pa.array([None], type=pa.int64())],
            names=["small", "money", "empty"])
        self.bind(statement, batch)
        result = self.reader(statement).read_all()
        self.assertEqual(result.schema.types, [pa.uint64(), pa.decimal128(12, 2), pa.int64()])
        self.assertEqual(result.to_pylist(), [{"small": 1, "money": Decimal("1.20"), "empty": None}])
        equivalent = ctx.sql("SELECT :small AS small,:money AS money,:empty AS empty", parameters={
            "small": types.scalar(1, "uint64"), "money": types.scalar(Decimal("1.20"), "decimal128(12,2)"),
            "empty": types.scalar(None, "int64")}).collect()
        self.assertEqual(equivalent.to_arrow().schema, result.schema)
        self.assertEqual(equivalent.to_rows(), tuple(result.to_pylist()))
        rejected = self.statement(self.open(ctx), "SELECT :value AS value")
        for dtype in (pa.int32(), pa.float32()):
            with self.subTest(dtype=dtype), self.assertRaisesRegex(self.manager.Error, "exact native scalar"):
                self.bind(rejected, pa.record_batch([pa.array([None], type=dtype)], names=["value"]))

        for dtype in (pa.timestamp("ns"), pa.timestamp("us", tz="UTC")):
            for ticks in (None, -1, (1 << 63) - 1):
                with self.subTest(dtype=dtype, ticks=ticks):
                    statement = self.statement(self.open(ctx), "SELECT :value AS value")
                    values = pa.array([ticks], type=pa.int64()).cast(dtype)
                    self.bind(statement, pa.record_batch([values], names=["value"]))
                    result = self.reader(statement).read_all()
                    self.assertEqual(result.schema.field("value").type, dtype)
                    self.assertEqual(result.column("value").cast(pa.int64()).to_pylist(), [ticks])

    def test_completion_report_contains_native_observations_only_after_eof(self):
        import json
        from shardloom import functions as f
        ctx = self.context()
        ctx.register("observed", ctx.range(0, 4, column="id").observe("quality", rows=f.count(), total=f.sum("id")))
        statement = self.statement(self.open(ctx), "SELECT * FROM observed")
        reader = self.reader(statement)
        with self.assertRaises(self.manager.Error):
            statement.get_option("shardloom.execution_report")
        self.assertEqual(reader.read_all().to_pylist(), [{"id": value} for value in range(4)])
        report = self.sl.OutputEnvelope.from_json(json.loads(statement.get_option("shardloom.execution_report")))
        self.assertFalse(report.fallback.attempted)
        self.assertEqual(report.field("external_engine_invoked"), "false")
        self.assertTrue(report.field_bool("relational_observation_data_completed"))
        self.assertEqual(json.loads(report.field("relational_observation_0_result_jsonl")), {"rows": 4, "total": 6})
        reader = self.reader(statement)
        reader.read_next_batch()
        statement.cancel()
        with self.assertRaises(self.manager.Error):
            statement.get_option("shardloom.execution_report")
        reader.close()

    def test_partial_fetch_cancellation_and_retained_columnar_owner(self):
        ctx, pa = self.context(), self.pa
        events = []
        def producer():
            events.append("opened")
            try:
                for start in range(0, 12_000, 3000):
                    events.append(start)
                    yield [{"id": value} for value in range(start, start + 3000)]
            finally:
                events.append("closed")
        ctx.register("streamed", ctx.from_batches(producer, schema={"id": "int64"}, streaming=True))
        connection = self.open(ctx)
        statement = self.statement(connection, "SELECT id FROM streamed")
        reader = self.reader(statement)
        self.assertEqual(events, [])
        retained = reader.read_next_batch()
        self.assertEqual(retained.column(0)[0].as_py(), 0)
        statement.cancel()
        with self.assertRaises(pa.ArrowException):
            reader.read_all()
        self.assertEqual(events[-1], "closed")
        self.assertNotIn(9000, events)
        other = self.statement(self.open(ctx), "SELECT 9 AS value")
        self.assertEqual(self.reader(other).read_all().to_pylist(), [{"value": 9}])
        ctx.close()
        gc.collect()
        self.assertEqual(retained.column(0)[0].as_py(), 0)
        with self.assertRaises(self.manager.Error):
            other.execute_query()

    def test_late_failure_and_early_reader_close_release_producer(self):
        ctx = self.context()
        events = []
        def failing():
            try:
                yield [{"id": 1}]
                raise LookupError("late adapter producer failure")
            finally:
                events.append("closed")
        ctx.register("failing", ctx.from_batches(failing, schema={"id": "int64"}, streaming=True))
        statement = self.statement(self.open(ctx), "SELECT * FROM failing")
        reader = self.reader(statement)
        self.assertEqual(reader.read_next_batch().to_pylist(), [{"id": 1}])
        with self.assertRaisesRegex(self.pa.ArrowException, "late adapter producer failure"):
            reader.read_all()
        self.assertEqual(events, ["closed"])
        reader = self.reader(statement)
        reader.read_next_batch()
        reader.close()
        self.assertEqual(events, ["closed", "closed"])

    def test_one_use_bootstrap_and_public_dbapi_connection_contract(self):
        from shardloom import dbapi
        from shardloom.adbc import driver_options
        ctx = self.context()
        with driver_options(ctx) as options:
            stale = dict(options)
        with self.assertRaisesRegex(self.manager.Error, "expired"):
            self.manager.AdbcDatabase(**stale)
        with driver_options(ctx) as options:
            options.pop("init_func")  # Independent dynamic loading of the real C entrypoint.
            database = self.manager.AdbcDatabase(**options)
            self.addCleanup(database.close)
            with self.assertRaisesRegex(self.manager.Error, "consumed"):
                self.manager.AdbcDatabase(**options)
        with self.assertRaises(dbapi.NotSupportedError):
            dbapi.connect(ctx, autocommit=False)
        with dbapi.connect(ctx) as connection, connection.cursor() as cursor:
            cursor.execute("SELECT :amount AS amount", {"amount": Decimal("1.200")})
            self.assertEqual(cursor.fetchone(), (Decimal("1.200"),))
            # The independent DB-API layer makes commit/rollback no-ops in
            # autocommit; its underlying driver never claims transactions.
            connection.commit()
            connection.rollback()
        low_level = self.open(ctx)
        for method in (low_level.commit, low_level.rollback):
            with self.assertRaises(self.manager.NotSupportedError):
                method()
        self.assertFalse(ctx.closed)

    def test_native_invalid_input_preserves_standard_driver_status(self):
        connection = self.open(self.context())
        for sql in ("SELECT * FROM missing", "SELECT unavailable_function(1) AS x"):
            with self.subTest(sql=sql):
                with self.assertRaises(self.manager.Error) as failure:
                    self.statement(connection, sql)
                self.assertEqual(failure.exception.status_code, self.manager.AdbcStatusCode.INVALID_ARGUMENT)
                self.assertIn("SL_INVALID_INPUT", str(failure.exception))
                self.assertIn("fallback_attempted=False", str(failure.exception))

    def test_notebook_magic_binds_existing_context_without_execution(self):
        try:
            from IPython.core.interactiveshell import InteractiveShell
        except ImportError as error:
            raise unittest.SkipTest("optional IPython consumer is not installed") from error
        from shardloom.notebook import install
        ctx, shell = self.context(), InteractiveShell()
        self.addCleanup(shell.cleanup)
        ctx.register("facts", ctx.range(0, 3, column="value"))
        shell.user_ns["bindings"] = {"increment": 10}
        with install(ctx, shell) as handle:
            frame = shell.run_cell_magic("shardloom", "--params bindings --into answer",
                "WITH a(x) AS (SELECT value FROM facts) SELECT x+:increment AS total FROM a")
            self.assertIs(frame, shell.user_ns["answer"])
            self.assertFalse(ctx.client._native_backend.session.snapshot()["native_owner_initialized"])
            self.assertEqual(frame.filter(self.sl.col("total") >= 11).collect().to_rows(), ({"total": 11}, {"total": 12}))
            self.assertEqual(shell.run_line_magic("shardloom", "SELECT 7 AS value").collect().to_rows(), ({"value": 7},))
            with self.assertRaisesRegex(ValueError, "registered"):
                install(ctx, shell)
        self.assertNotIn("shardloom", shell.magics_manager.magics["cell"])
        handle.close()
