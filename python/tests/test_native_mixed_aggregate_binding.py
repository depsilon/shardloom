# SPDX-License-Identifier: Apache-2.0
"""Fresh mixed grouped reductions agree across native formats and public APIs."""

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
from query_surface.corpus import case, measure
from query_surface.datasets import DatasetSpec
from query_surface.reference import expected
from query_surface.semantics import ResultType, compare
from query_surface.specification import Expression as E, Order, Relation, SURFACES


def mixed_cases():
    cases = []

    def add(name, source, keys, argument):
        measures = [measure(source, function, argument, alias) for function, alias in (
            ("sum", "total"), ("avg", "mean"), ("min", "low"), ("max", "high"), ("count", "present"))]
        measures.append(measure(source, "count", alias="rows"))
        plan = source.aggregate(keys, measures)
        if keys:
            plan = plan.order(*(Order(key) for key in keys))
        cases.append(case("mixed_state_" + name,
                          "grouped_aggregation" if keys else "global_aggregation",
                          plan, ordered=bool(keys)))

    fact = Relation.scan("fact")
    for column in ("small_value", "signed_value", "unsigned_value", "metric", "optional_value", "all_null"):
        for key in ("group_key", "high_key", "skew_key"):
            add(column + "_" + key, fact, (key,), column)
    filtered = fact.where(fact.col("metric").binary("gt", 0.5))
    add("filtered_category", filtered, ("category",), "optional_value")
    entities = Relation.scan("entities")
    joined = fact.join(entities, (("entity_id", "entity_id"),),
                       (("left", "metric", "metric"), ("right", "region", "region")), "left")
    add("joined_nullable_region", joined, ("region",), "metric")
    add("nullable_text_key", fact, ("optional_text",), "unsigned_value")
    coalesced = E.call("coalesce", (fact.col("optional_value"), E.literal(-3)), ResultType("int64", False))
    add("completed_expression", fact, ("medium_key",), coalesced)
    add("scalar_control", fact, (), "metric")
    empty = fact.where(fact.col("row_id").binary("lt", 0))
    add("empty_grouped", empty, ("group_key",), "signed_value")
    return cases


class NativeMixedAggregateTests(unittest.TestCase):
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

    def test_fresh_mixed_reductions_and_compositions_match_all_surfaces(self):
        spec = DatasetSpec(seed=0x30_2026_C7, fact_rows=1069, entity_rows=41,
                           product_rows=23, relationship_rows=97, calendar_rows=13, profile="null_heavy")
        cases = mixed_cases()
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
        with tempfile.TemporaryDirectory(prefix="shardloom-mixed-aggregate-") as directory:
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
