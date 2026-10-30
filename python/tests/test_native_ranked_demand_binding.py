# SPDX-License-Identifier: Apache-2.0
"""Fresh ranked demand and refusal controls share one native query engine."""

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
    from .test_native_binding import _load_native
    from . import test_native_join_demand_binding as fixtures
else:
    from test_native_binding import _load_native
    import test_native_join_demand_binding as fixtures

from query_surface.adapters import execute
from query_surface.corpus import case, reviewed_cases
from query_surface.datasets import DatasetSpec
from query_surface.reference import expected
from query_surface.semantics import compare
from query_surface.specification import Order, Relation, SURFACES


def ranked_cases(rows):
    names = {"projection_wide", "projection_rich", "projection_reordered", "projection_narrow"}
    cases = [item for item in reviewed_cases() if item.id in names]
    assert {item.id for item in cases} == names
    fact = Relation.scan("fact")
    rich = fact.select("row_id", "optional_value", "long_text", "amount", "items", "detail", "event_time")
    # The shared DataFrame adapter admits one direction and null placement for
    # all keys. Mixed-key ordering is covered by the native ranking tests.
    cases.append(case("rank_nullable", "ordering", rich.order(
        Order("optional_value", True, "first"), Order("row_id", True, "first")).take(9, offset=3), ordered=True))
    alias = fact.project(("position", fact.col("row_id")), ("payload", fact.col("long_text")),
                         ("repeated", fact.col("long_text")))
    alias = alias.order(Order("position", True)).take(7, offset=2).select("payload", "repeated")
    cases.append(case("rank_alias", "projection", alias, ordered=True))
    predicate = fact.col("row_id").binary("ge", 73).binary("and", fact.col("row_id").binary("lt", rows - 42))
    filtered = fact.where(predicate).select("row_id", "amount", "detail", "items", "long_text")
    cases.append(case("rank_predicate", "ordering", filtered.order(Order("row_id", True)).take(11, offset=5), ordered=True))
    cases.append(case("rank_zero", "ordering", rich.order(Order("row_id")).take(0, offset=19), ordered=True))
    cases.append(case("rank_large_prefix", "projection", fact.order(Order("row_id")).take(rows), ordered=True))
    computed = fact.project(("row_id", fact.col("row_id")),
                            ("value", fact.col("signed_value").binary("add", 1)),
                            ("text", fact.col("long_text")))
    cases.append(case("rank_computed_barrier", "projection", computed.order(Order("row_id")).take(7), ordered=True))
    # A physical source prefix is layout-dependent. Sort before the inner LIMIT
    # so the independent reference and the shuffled input agree on its domain.
    ranged = rich.order(Order("row_id")).take(200).order(Order("row_id", True)).take(7)
    cases.append(case("rank_range_barrier", "ordering", ranged, ordered=True))
    keys = fact.select("row_id", "signed_value")
    cases.append(case("rank_all_keys", "ordering", keys.order(Order("row_id"), Order("signed_value")).take(7), ordered=True))
    assert len(cases) == 12
    return cases


class NativeRankedDemandTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.native = _load_native()
        import pyarrow as pa
        cls.pa = pa

    def write_sources(self, root, spec, tables, extension):
        return fixtures.NativeJoinDemandTests.write_sources(self, root, spec, tables, extension, group_rows=7)

    def checked(self, wanted, actual, contract, report, *, label, extension, surface):
        compare(wanted, actual, contract)
        self.assertTrue(report["complete_consumption"])
        self.assertEqual(report["surface"], surface)
        fields = report["fields"]
        self.assertEqual(fields["fallback_attempted"], "false")
        self.assertEqual(fields["external_engine_invoked"], "false")
        active = int(fields.get("native_source_ranked_sources", "0"))
        enabling = {"projection_wide", "projection_rich", "projection_reordered", "rank_nullable",
                    "rank_alias", "rank_predicate", "rank_zero"}
        if extension == "parquet":
            self.assertEqual(active, int(label in enabling), label)
            self.assertEqual(fields["native_input_end_observed"], "true")
            self.assertEqual(fields["native_input_output_ownership_detached"], "true")
            self.assertEqual(fields["native_source_demand_prepared_input_bytes_written"], "0")
            if active:
                self.activated_surfaces.add(surface)
                rank_bytes = int(fields["native_source_ranked_rank_read_bytes"])
                payload_bytes = int(fields["native_source_ranked_payload_read_bytes"])
                self.assertEqual(int(fields["native_source_demand_data_read_bytes"]), rank_bytes + payload_bytes)
                self.assertLess(int(fields["native_source_ranked_payload_group_visits"]),
                                int(fields["native_source_ranked_rank_group_visits"]))
                self.assertIn("rank_failure_precedes_payload_failure", fields["native_source_ranked_validation_scope"])
                if label == "rank_zero":
                    self.assertEqual((payload_bytes, int(fields["native_source_ranked_selected_rows"])), (0, 0))
        else:
            self.assertEqual(active, 0)

    def test_ranked_values_and_refusals_match_all_interfaces_and_formats(self):
        spec = DatasetSpec(seed=0x30_2026_941, fact_rows=659, entity_rows=31,
                           product_rows=13, relationship_rows=47, calendar_rows=11, profile="null_heavy")
        cases = ranked_cases(spec.fact_rows)
        self.activated_surfaces = set()
        tables = set().union(*(item.relation.sources() for item in cases))
        from shardloom import ShardLoomClient
        cli = ShardLoomClient(binary=os.environ.get("SHARDLOOM_SURFACE_TEST_BINARY"),
                             transport="subprocess", use_persistent_worker=False)
        try:
            binary = cli.binary_command()[0]
        finally:
            cli.close()
        with tempfile.TemporaryDirectory(prefix="shardloom-ranked-demand-") as directory:
            for extension in ("parquet", "arrow", "vortex"):
                root = Path(directory) / extension
                root.mkdir()
                sources = self.write_sources(root, spec, tables, extension)
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
                    self.assertFalse((root / ".shardloom").exists())
        self.assertEqual(self.activated_surfaces, set(SURFACES))


if __name__ == "__main__":
    unittest.main()
