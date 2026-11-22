# SPDX-License-Identifier: Apache-2.0
"""Exact ordinary query reuse with detached output and source-generation checks."""

from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

if __package__:
    from .test_native_binding import _load_native
else:
    from test_native_binding import _load_native


class NativeColumnarCoverageTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.native = _load_native()
        import pyarrow as pa
        import pyarrow.parquet as parquet
        import shardloom as sl
        cls.pa, cls.parquet, cls.sl = pa, parquet, sl

    def setUp(self):
        process = patch("subprocess.Popen", side_effect=AssertionError("external execution"))
        process.start()
        self.addCleanup(process.stop)
        directory = tempfile.TemporaryDirectory(prefix="shardloom-column-coverage-")
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        self.path = self.root / "source.parquet"
        self.rows = [dict(a=row, b=None if row % 7 == 0 else -row,
                          text=None if row % 3 == 0 else f"row-{row}-λ") for row in range(257)]
        self.schema = self.pa.schema([self.pa.field("a", self.pa.int64(), nullable=False),
                                     self.pa.field("b", self.pa.int64()),
                                     self.pa.field("text", self.pa.string())])
        self.parquet.write_table(self.pa.Table.from_pylist(self.rows, schema=self.schema),
                                 self.path, row_group_size=97)
        self.context = self.sl.context(transport="native", memory_bytes=64 << 20, max_parallelism=1)
        self.addCleanup(self.context.close)

    def checked(self, result, rows, names, decoded, reused):
        fields = result.envelope.field_map
        self.assertEqual(fields["fallback_attempted"], "false")
        self.assertEqual(fields["external_engine_invoked"], "false")
        self.assertEqual(fields["native_input_end_observed"], "true")
        self.assertEqual(fields["native_input_output_ownership_detached"], "true")
        self.assertEqual(int(fields["native_source_demand_rows_decoded"]), decoded)
        self.assertEqual(int(fields["native_source_coverage_field_rows"]), reused)
        self.assertLessEqual(int(fields["native_source_coverage_retained_bytes"]), (64 << 20) // 16)
        self.assertIn("prior_successful_operation_only", fields["native_source_coverage_scope"])
        guarantee = fields["local_primitive_native_io_pushdown_guarantee"]
        self.assertIn("input_delivery_owners_released_before_next_demand", guarantee)
        self.assertIn("bounded_optional_native_source_coverage_may_retain_credited_backing", guarantee)
        self.assertNotIn("all_input_buffer_owners_released", guarantee)
        proof = fields["local_primitive_native_io_pushdown_proof_basis"]
        self.assertIn(f"native_source_coverage_field_rows={reused} ", proof)
        self.assertIn("retained_native_source_coverage_is_not_new_arrow_intake", proof)
        if decoded == 0:
            for name in ("native_source_demand_data_read_bytes",
                         "native_input_columnar_buffer_bytes_copied",
                         "native_input_columnar_buffer_bytes_transferred"):
                self.assertEqual(int(fields[name]), 0)
        expected = self.pa.Table.from_pylist([{name: row[name] for name in names} for row in rows],
                                            schema=self.pa.schema([self.schema.field(name) for name in names]))
        actual = result.to_arrow()
        self.assertTrue(actual.schema.equals(expected.schema, check_metadata=True), (actual.schema, expected.schema))
        self.assertEqual(actual.to_pylist(), expected.to_pylist())

    def test_fresh_sql_and_dataframe_bindings_share_columns_and_detach_complete_results(self):
        cold = self.context.sql(f"SELECT a,b FROM '{self.path}'").collect(check=True)
        self.checked(cold, self.rows, ["a", "b"], 257, 0)
        partial = self.context.read_parquet(self.path).select("b", "text").collect(check=True)
        self.checked(partial, self.rows, ["b", "text"], 257, 257)
        reused = self.context.sql(f"SELECT text,b,a FROM '{self.path}' WHERE a>=3 AND a<251 ORDER BY a DESC").collect(check=True)
        self.checked(reused, list(reversed(self.rows[3:251])), ["text", "b", "a"], 0, 257 * 3)
        output = self.root / "complete.vortex"
        written = self.context.read_parquet(self.path).write_vortex(output, check=True)
        self.assertEqual(written.output_commit_status, "committed")
        self.assertEqual(written.envelope.field("fallback_attempted"), "false")
        native_session = self.context.client._native_backend.session
        self.context.close()
        self.path.unlink()
        # Both retained results and the explicit complete artifact survive the
        # source/session. Ordinary reads created no durable partial files.
        self.assertGreater(native_session.snapshot()["reserved_bytes"], 0)
        self.assertEqual(reused.to_arrow().to_pylist(), [
            {name: row[name] for name in ("text", "b", "a")} for row in reversed(self.rows[3:251])])
        self.assertEqual(sorted(path.name for path in self.root.iterdir()), ["complete.vortex"])
        with self.sl.context(transport="native", memory_bytes=64 << 20, max_parallelism=1) as reader:
            self.assertEqual(reader.read_vortex(output).collect(check=True).to_arrow().to_pylist(), self.rows)
        del cold, partial, reused, written
        self.assertEqual(native_session.snapshot()["reserved_bytes"], 0)

    def test_replacement_and_new_session_never_reuse_old_values(self):
        self.context.read_parquet(self.path).select("a").collect(check=True)
        with self.sl.context(transport="native", memory_bytes=64 << 20, max_parallelism=1) as other:
            fresh = other.read_parquet(self.path).select("a").collect(check=True)
            self.checked(fresh, self.rows, ["a"], 257, 0)
        replacement = self.root / "replacement.parquet"
        changed = [{**row, "a": row["a"] + 1000} for row in self.rows]
        self.parquet.write_table(self.pa.Table.from_pylist(changed, schema=self.schema), replacement, row_group_size=97)
        replacement.replace(self.path)
        result = self.context.read_parquet(self.path).select("a").collect(check=True)
        self.checked(result, changed, ["a"], 257, 0)


if __name__ == "__main__":
    unittest.main()
