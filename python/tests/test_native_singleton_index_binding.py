# SPDX-License-Identifier: Apache-2.0
"""Fresh membership consumers preserve complete results across formats and APIs."""

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
from query_surface.corpus import case, composition_neighbors, measure
from query_surface.datasets import DatasetSpec
from query_surface.reference import expected
from query_surface.semantics import compare
from query_surface.specification import Order, Relation, SURFACES


def singleton_cases():
    fact, entities, relationships = (Relation.scan(name) for name in ("fact", "entities", "relationships"))
    cases = []
    for kind in ("inner", "left", "right", "full", "semi", "anti"):
        columns = [("left", "row_id", "row_id"), ("left", "entity_id", "entity_id")]
        if kind not in {"semi", "anti"}:
            columns.append(("right", "region", "region"))
        cases.append(case(f"singleton_join_{kind}", "joins",
                          fact.join(entities, (("entity_id", "entity_id"),), columns, kind)))
    duplicates = fact.join(relationships, (("entity_id", "entity_id"),),
        (("left", "row_id", "row_id"), ("right", "relationship_id", "relationship_id")), "full")
    cases.append(case("singleton_duplicate_full_join", "joins", duplicates))
    distinct = fact.select("row_id", "optional_text").distinct().order(Order("row_id"))
    cases.append(case("singleton_distinct_rows", "distinct", distinct, ordered=True))
    grouped = fact.aggregate(("optional_text", "category"),
        (measure(fact, "sum", "signed_value", "total"), measure(fact, "count", alias="rows")))
    cases.append(case("singleton_grouped_nullable_text", "grouped_aggregation",
                      grouped.order(Order("optional_text"), Order("category")), ordered=True))
    left = fact.where(fact.col("row_id").binary("lt", 211)).select("row_id", "optional_text")
    right = fact.where(fact.col("row_id").binary("ge", 101)).select("row_id", "optional_text")
    for kind in ("union_distinct", "intersect", "except"):
        cases.append(case(f"singleton_set_{kind}", "sets",
                          left.set(right, kind).order(Order("row_id")), ordered=True))
    names = {"star_join", "star_join_products_first", "composite_active_region_report"}
    cases.extend(item for item in composition_neighbors() if item.id in names)
    return cases


class NativeSingletonIndexTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.native = fixture._load_native()
        import pyarrow as pa
        cls.pa = pa

    def write_sources(self, root, spec, tables, extension):
        return fixture.NativeJoinDemandTests.write_sources(self, root, spec, tables,
                                                           extension, group_rows=11)

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

    def test_fresh_singleton_duplicate_and_shared_membership_consumers_match_all_surfaces(self):
        spec = DatasetSpec(seed=0x30_2026_517, fact_rows=307, entity_rows=43,
                           product_rows=17, relationship_rows=97, calendar_rows=5, profile="null_heavy")
        cases = singleton_cases()
        self.assertEqual(len(cases), 15)
        self.assertEqual(len({item.id for item in cases}), 15)
        tables = set().union(*(item.relation.sources() for item in cases))
        from shardloom import ShardLoomClient
        cli = ShardLoomClient(binary=os.environ.get("SHARDLOOM_SURFACE_TEST_BINARY"),
                             transport="subprocess", use_persistent_worker=False)
        try:
            binary = cli.binary_command()[0]
        finally:
            cli.close()
        with tempfile.TemporaryDirectory(prefix="shardloom-singleton-index-") as directory:
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
