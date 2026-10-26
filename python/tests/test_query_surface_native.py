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
from query_surface.semantics import compare
from query_surface.specification import SURFACES


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
