# SPDX-License-Identifier: Apache-2.0
"""Complete grouped results span independent typed-state page boundaries."""

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
from query_surface.semantics import compare
from query_surface.specification import Order, Relation, SURFACES


GROUP_COUNTS = (1, 16, 512, 513, 1024, 1025, 4097)


def grouped_cases(groups):
    cases = []
    fact = Relation.scan("fact")

    def add(name, column, *, filtered=False, extrema_only=False, count_only=False):
        source = fact.where(fact.col("metric").binary("gt", 0.5)) if filtered else fact
        # Exactly the declared group population on unfiltered nonempty input,
        # independent of the generator's random/skewed key cardinalities.
        projected = source.project(
            ("g", source.col("row_id").binary("mod", groups)),
            ("renamed_value", source.col(column)),
        )
        functions = (("max", "high"), ("min", "low"), ("count", "present")) if extrema_only else (
            ("sum", "total"), ("avg", "mean"), ("min", "low"), ("max", "high"), ("count", "present"))
        measures = [measure(projected, "count", alias="rows")] if count_only else [
            measure(projected, function, "renamed_value", alias) for function, alias in functions]
        plan = projected.aggregate(("g",), measures).order(Order("g"))
        cases.append(case(f"grouped_state_g{groups}_{name}", "grouped_aggregation", plan, ordered=True))

    if groups in (1, 16):
        for column in ("small_value", "signed_value", "unsigned_value", "metric", "optional_value", "all_null"):
            add("mixed_" + column, column)
        add("filtered_optional", "optional_value", filtered=True)
        add("renamed_reverse_extrema", "signed_value", extrema_only=True)
    elif groups == 4097:
        add("mixed_optional_value", "optional_value")
        add("mixed_unsigned_value", "unsigned_value")
    else:
        add("mixed_signed_value", "signed_value")
        add("mixed_metric", "metric")
        add("count_rows", "signed_value", count_only=True)
    return cases


class NativeGroupedStateTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.native = fixture._load_native()
        import pyarrow as pa
        cls.pa = pa

    def write_sources(self, root, spec, tables, extension):
        return fixture.NativeJoinDemandTests.write_sources(self, root, spec, tables,
                                                           extension, group_rows=2048)

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

    def test_grouped_state_page_boundaries_and_compositions_match_all_surfaces(self):
        from shardloom import ShardLoomClient
        cli = ShardLoomClient(binary=os.environ.get("SHARDLOOM_SURFACE_TEST_BINARY"),
                             transport="subprocess", use_persistent_worker=False)
        try:
            binary = cli.binary_command()[0]
        finally:
            cli.close()
        count = 0
        with tempfile.TemporaryDirectory(prefix="shardloom-grouped-state-") as directory:
            root = Path(directory)
            for groups in GROUP_COUNTS:
                spec = DatasetSpec(seed=0x31_2026_C7, fact_rows=groups * 3 + 11,
                                   entity_rows=11, product_rows=7, relationship_rows=13,
                                   calendar_rows=5, profile="null_heavy")
                cases = grouped_cases(groups)
                self.assertEqual(len(cases), 8 if groups in (1, 16) else 2 if groups == 4097 else 3)
                self.assertEqual(len({item.id for item in cases}), len(cases))
                references = {item.id: expected(item, spec) for item in cases}
                for item in cases:
                    if "filtered" not in item.id:
                        self.assertEqual(len(references[item.id].rows), groups)
                for extension in ("parquet", "arrow", "vortex"):
                    format_root = root / f"{groups}-{extension}"
                    format_root.mkdir()
                    sources = self.write_sources(format_root, spec, {"fact"}, extension)
                    for item in cases:
                        for surface in SURFACES:
                            with self.subTest(groups=groups, format=extension, case=item.id, surface=surface):
                                kwargs = dict(native=self.native, binary=binary, memory_bytes=256 << 20, parallelism=2)
                                if surface == "cli_sql":
                                    actual, report = execute(item.relation, sources, surface, **kwargs)
                                else:
                                    with patch.object(subprocess, "Popen", side_effect=AssertionError("external execution")):
                                        actual, report = execute(item.relation, sources, surface, **kwargs)
                                self.checked(references[item.id], actual, item.contract, report,
                                             label=item.id, extension=extension, surface=surface)
                                count += 1
                                if extension == "parquet":
                                    self.assertFalse((format_root / ".shardloom").exists())
        self.assertEqual(count, 450)


if __name__ == "__main__":
    unittest.main()
