# SPDX-License-Identifier: Apache-2.0
"""Native integration: one intent, complete results through five real APIs."""
from __future__ import annotations

import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "benchmarks" / "traditional_analytics"))
sys.path.insert(0, str(ROOT / "scripts"))
if __package__:
    from .test_native_expression_binding import _load_native
else:
    from test_native_expression_binding import _load_native

from query_surface.adapters import execute
from query_surface.corpus import aggregate_case, composition_neighbors, reviewed_cases
from query_surface.datasets import DatasetSpec, RowGenerator, SCHEMAS
from query_surface.reference import expected
from query_surface.semantics import ResultContract, TypedResult, compare
from query_surface.specification import Relation, SURFACES


class NativeQuerySurfaceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.native = _load_native()
        try:
            import pyarrow as pa
        except ImportError as error:
            if os.environ.get("SHARDLOOM_REQUIRE_NATIVE_PYTHON") == "1":
                raise AssertionError("required fixture dependency is unavailable") from error
            raise unittest.SkipTest("optional PyArrow is unavailable") from error
        cls.pa = pa

    def test_parquet_directory_prefix_and_grouping_through_five_surfaces(self):
        import pyarrow.parquet as pq
        import shardloom as sl
        from shardloom._result_schema import _arrow_type
        binary = os.environ.get("SHARDLOOM_SURFACE_TEST_BINARY")
        if not binary:
            client = sl.ShardLoomClient()
            try:
                binary = client.binary_command()[0]
            finally:
                client.close()
        spec = DatasetSpec(fact_rows=127, entity_rows=23, product_rows=7, relationship_rows=53, calendar_rows=11)
        schema = self.pa.schema([self.pa.field(name, _arrow_type(dtype, self.pa), dtype.nullable)
                                for name, dtype in SCHEMAS["fact"]])
        rows = list(RowGenerator(spec, "fact").rows())
        data = self.pa.Table.from_pylist(rows, schema=schema)
        scan = Relation.scan("fact")
        prefix = scan.project(("id", scan.col("row_id"))).take(7, offset=61)
        grouped = aggregate_case({"argument": "optional_value", "keys": "group_key_category",
                                  "measures": "mixed", "expression": "coalesce", "filter": "all"})
        cases = [
            ("prefix", prefix, TypedResult(prefix.schema, [{"id": row["row_id"]} for row in rows[61:68]]),
             ResultContract(prefix.schema, ordered=True), 68),
            ("grouped", grouped.relation, expected(grouped, spec), grouped.contract, 127),
        ]
        with tempfile.TemporaryDirectory(prefix="shardloom-parquet-directory-surfaces-") as directory:
            for name, relation, wanted, contract, decoded in cases:
                for surface in SURFACES:
                    with self.subTest(case=name, surface=surface):
                        path = Path(directory) / f"{name}-{surface}.parquet"
                        path.mkdir()
                        pq.write_table(data.slice(0, 64), path / "a.PARQUET", row_group_size=17)
                        pq.write_table(data.slice(64), path / "b.parquet", row_group_size=17)
                        (path / "ignored.txt").write_text("not a source")
                        if surface == "cli_sql":
                            actual, report = execute(relation, {"fact": [path]}, surface,
                                native=self.native, binary=binary, memory_bytes=256 << 20)
                        else:
                            with patch.object(subprocess, "Popen", side_effect=AssertionError("native surface launched a process")):
                                actual, report = execute(relation, {"fact": [path]}, surface,
                                    native=self.native, binary=binary, memory_bytes=256 << 20)
                        compare(wanted, actual, contract)
                        fields = report["fields"]
                        self.assertTrue(report["complete_consumption"])
                        self.assertEqual(fields["native_source_demand_sources"], "1")
                        self.assertEqual(fields["native_source_demand_files"], "2")
                        self.assertEqual(int(fields["native_source_demand_rows_decoded"]), decoded)
                        self.assertEqual(fields["native_source_demand_prefix_sources"], "1" if name == "prefix" else "0")
                        self.assertEqual(fields["native_source_demand_prepared_input_bytes_written"], "0")
                        self.assertFalse((path / ".shardloom").exists())
                        self.assertFalse((path.parent / ".shardloom").exists())

    def test_parquet_five_surface_requests_preserve_strict_complete_results(self):
        import pyarrow.parquet as pq
        import shardloom as sl
        from shardloom._result_schema import _arrow_type
        binary = os.environ.get("SHARDLOOM_SURFACE_TEST_BINARY")
        if not binary:
            client = sl.ShardLoomClient()
            try:
                binary = client.binary_command()[0]
            finally:
                client.close()
        spec = DatasetSpec(fact_rows=127, entity_rows=23, product_rows=7, relationship_rows=53, calendar_rows=11)
        names = {"predicate_one_row", "projection_alias_filter", "projection_rich", "projection_wide",
                 "selectivity_10000", "set_union_all", "topk_11_offset_7", "global_count_star",
                 "global_sum", "numeric_add"}
        cases = [item for item in reviewed_cases() if item.id in names]
        self.assertEqual(len(cases), len(names))
        cases += [aggregate_case({"argument": "signed_value", "keys": "group_key", "measures": "mixed",
                                  "expression": "direct", "filter": "all"}),
                  aggregate_case({"argument": "optional_value", "keys": "group_key_category", "measures": "mixed",
                                  "expression": "coalesce", "filter": "one_percent"})]
        maximum_columns = {"global_count_star": 1, "global_sum": 1, "numeric_add": 2,
                           cases[-2].id: 2, cases[-1].id: 4}
        schema = self.pa.schema([self.pa.field(name, _arrow_type(dtype, self.pa), dtype.nullable)
                                for name, dtype in SCHEMAS["fact"]])
        data = self.pa.Table.from_pylist(list(RowGenerator(spec, "fact").rows()), schema=schema)
        with tempfile.TemporaryDirectory(prefix="shardloom-parquet-surfaces-") as directory:
            for item in cases:
                wanted = expected(item, spec)
                for surface in SURFACES:
                    with self.subTest(case=item.id, surface=surface):
                        # Each interface must admit a fresh original source;
                        # an earlier interface's durable cache cannot mask it.
                        path = Path(directory) / f"{item.id}-{surface}.parquet"
                        pq.write_table(data, path, row_group_size=17)
                        if surface == "cli_sql":
                            actual, report = execute(item.relation, {"fact": [path]}, surface,
                                native=self.native, binary=binary, memory_bytes=256 << 20)
                        else:
                            with patch.object(subprocess, "Popen", side_effect=AssertionError("native surface launched a process")):
                                actual, report = execute(item.relation, {"fact": [path]}, surface,
                                    native=self.native, binary=binary, memory_bytes=256 << 20)
                        compare(wanted, actual, item.contract)
                        self.assertTrue(report["complete_consumption"])
                        self.assertEqual(report["surface"], surface)
                        fields = report["fields"]
                        self.assertEqual(fields["native_source_demand_sources"], "1")
                        self.assertEqual(fields["native_source_demand_prepared_input_bytes_written"], "0")
                        self.assertIn("not_full_ingest_certification", fields["native_source_demand_validation_scope"])
                        self.assertFalse((path.parent / ".shardloom").exists())
                        if item.id in {"predicate_one_row", "projection_alias_filter"}:
                            self.assertLess(int(fields["native_source_demand_projected_columns"]), len(schema))
                        if item.id in maximum_columns:
                            self.assertLessEqual(int(fields["native_source_demand_projected_columns"]),
                                                 maximum_columns[item.id])
                        if item.id == "predicate_one_row":
                            self.assertLess(int(fields["native_source_demand_row_groups_read"]),
                                            int(fields["native_source_demand_row_groups_total"]))

    def test_real_five_surface_requests_preserve_strict_complete_results(self):
        from shardloom._result_schema import _arrow_type
        import shardloom as sl
        binary = os.environ.get("SHARDLOOM_SURFACE_TEST_BINARY")
        if not binary:
            client = sl.ShardLoomClient()
            try:
                binary = client.binary_command()[0]
            finally:
                client.close()
        spec = DatasetSpec(fact_rows=127, entity_rows=23, product_rows=7, relationship_rows=53, calendar_rows=11)
        names = {"global_count_star", "projection_rich", "predicate_eq", "predicate_and", "numeric_mod", "string_length",
                 "temporal_date_year", "join_left", "join_semi", "join_anti", "set_union_all"}
        cases = [item for item in reviewed_cases() if item.id in names]
        cases += [aggregate_case({"argument": "optional_value", "keys": "group_key_category",
                                  "measures": "mixed", "expression": "coalesce", "filter": "all"})]
        cases += composition_neighbors()
        with tempfile.TemporaryDirectory(prefix="shardloom-query-surface-") as directory:
            sources = {}
            session = self.native.Session(self.native.ExecutionResources(256 << 20, 1))
            try:
                for table in sorted(set().union(*(item.relation.sources() for item in cases))):
                    schema = self.pa.schema([self.pa.field(name, _arrow_type(dtype, self.pa), dtype.nullable)
                                            for name, dtype in SCHEMAS[table]])
                    data = self.pa.Table.from_pylist(list(RowGenerator(spec, table).rows()), schema=schema)
                    source = session.from_arrow(data)
                    path = Path(directory) / f"{table}.vortex"
                    session.plan(self.native.Relation.scan("memory://fixture"), sources=[("memory://fixture", source)]).write(path, "write_vortex")
                    sources[table] = (path,)
            finally:
                session.close()
            for item in cases:
                wanted = expected(item, spec)
                for surface in SURFACES:
                    with self.subTest(case=item.id, surface=surface):
                        if surface == "cli_sql":
                            actual, report = execute(item.relation, sources, surface, native=self.native, binary=binary, memory_bytes=256 << 20)
                        else:
                            with patch.object(subprocess, "Popen", side_effect=AssertionError("native surface launched a process")):
                                actual, report = execute(item.relation, sources, surface, native=self.native, binary=binary, memory_bytes=256 << 20)
                        compare(wanted, actual, item.contract)
                        self.assertTrue(report["complete_consumption"])
                        self.assertEqual(report["surface"], surface)
                        self.assertIn("engine_execution_seconds", report["clocks"])


if __name__ == "__main__":
    unittest.main()
