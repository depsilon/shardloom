# SPDX-License-Identifier: Apache-2.0
"""Fresh prepared-expression combinations agree across native formats and APIs."""

import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "benchmarks/traditional_analytics"))
sys.path.insert(0, str(ROOT / "scripts"))
if __package__:
    from . import test_native_join_demand_binding as fixture
else:
    import test_native_join_demand_binding as fixture

from query_surface.adapters import execute
from query_surface.corpus import case, corpus
from query_surface.datasets import DatasetSpec
from query_surface.reference import expected
from query_surface.semantics import ResultType, compare
from query_surface.specification import Expression as E, Order, Relation, SURFACES


def prepared_cases():
    cases = [item for item in corpus(interactions="all", include_pending=False)
             if item.family in {"strings", "temporal", "numeric"}]
    fact = Relation.scan("fact")
    text = fact.col("optional_text")
    lower = E.call("lower", (fact.col("unicode_text"),), ResultType("utf8", False))
    normalized = E.call("trim", (E.call("upper", (fact.col("unicode_text"),),
                                       ResultType("utf8", False)),), ResultType("utf8", False))
    expressions = (
        ("nullable_left_sub", E.literal(7).binary("sub", fact.col("optional_value"))),
        ("nullable_literal_mod", fact.col("optional_value").binary("mod", 7)),
        ("float_left_mul", E.literal(-2.5).binary("mul", fact.col("metric"))),
        ("float_right_sub", fact.col("metric").binary("sub", 3.25)),
        ("nullable_lower", E.call("lower", (text,), ResultType("utf8", True))),
        ("nullable_trim", E.call("trim", (text,), ResultType("utf8", True))),
        ("nullable_length", E.call("length", (text,), ResultType("int64", True))),
        ("normalized_length", E.call("length", (normalized,), ResultType("int64", False))),
    )
    for name, expression in expressions:
        plan = fact.project(("row_id", fact.col("row_id")), ("value", expression))
        cases.append(case("prepared_" + name, "projection", plan.order(Order("row_id")).take(43, 5), ordered=True))
    plan = fact.project(("row_id", fact.col("row_id")), ("lower", lower), ("again", lower),
                        ("length", E.call("length", (lower,), ResultType("int64", False))))
    cases.append(case("prepared_completed_text_child", "projection", plan.order(Order("row_id")).take(43, 5), ordered=True))
    return cases


class NativePreparedKernelsTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.native = fixture._load_native()
        import pyarrow as pa
        cls.pa = pa

    def write_sources(self, root, spec, tables, extension):
        return fixture.NativeJoinDemandTests.write_sources(self, root, spec, tables,
                                                           extension, group_rows=13)

    def checked(self, wanted, actual, contract, report, *, label, extension, surface):
        compare(wanted, actual, contract)
        self.assertTrue(report["complete_consumption"])
        self.assertEqual(report["surface"], surface)
        fields = report["fields"]
        self.assertEqual(fields["fallback_attempted"], "false")
        self.assertEqual(fields["external_engine_invoked"], "false")
        if extension == "parquet":
            self.assertGreater(int(fields["native_source_demand_sources"]), 0)
            self.assertEqual(fields["native_source_demand_prepared_input_bytes_written"], "0")
            self.assertEqual(fields["native_input_end_observed"], "true")
            self.assertEqual(fields["native_input_output_ownership_detached"], "true")

    def test_fresh_prepared_strings_temporal_and_scalar_operands_match_all_surfaces(self):
        spec = DatasetSpec(seed=0x30_2026_6B7, fact_rows=541, entity_rows=37,
                           product_rows=19, relationship_rows=89, calendar_rows=11, profile="null_heavy")
        cases = prepared_cases()
        self.assertEqual(len(cases), 24)
        self.assertEqual(len({item.id for item in cases}), 24)
        tables = set().union(*(item.relation.sources() for item in cases))
        from shardloom import ShardLoomClient
        cli = ShardLoomClient(binary=os.environ.get("SHARDLOOM_SURFACE_TEST_BINARY"),
                             transport="subprocess", use_persistent_worker=False)
        try:
            binary = cli.binary_command()[0]
        finally:
            cli.close()
        with tempfile.TemporaryDirectory(prefix="shardloom-prepared-kernels-") as directory:
            root = Path(directory)
            for extension in ("parquet", "arrow", "vortex"):
                format_root = root / extension
                format_root.mkdir()
                sources = self.write_sources(format_root, spec, tables, extension)
                for item in cases:
                    wanted = expected(item, spec)
                    for surface in SURFACES:
                        with self.subTest(format=extension, case=item.id, surface=surface):
                            kwargs = dict(native=self.native, binary=binary, memory_bytes=256 << 20, parallelism=2)
                            if surface == "cli_sql":
                                actual, report = execute(item.relation, sources, surface, **kwargs)
                            else:
                                with patch.object(subprocess, "Popen", side_effect=AssertionError("external execution")):
                                    actual, report = execute(item.relation, sources, surface, **kwargs)
                            self.checked(wanted, actual, item.contract, report,
                                         label=item.id, extension=extension, surface=surface)
                            if extension == "parquet":
                                self.assertFalse((format_root / ".shardloom").exists())


if __name__ == "__main__":
    unittest.main()
