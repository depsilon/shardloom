# SPDX-License-Identifier: Apache-2.0
"""Private reader ownership reaches every public query surface without changing values."""

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
    from . import test_native_join_demand_binding as join_fixtures
else:
    from test_native_binding import _load_native
    import test_native_join_demand_binding as join_fixtures

from query_surface.adapters import execute
from query_surface.corpus import reviewed_cases
from query_surface.datasets import DatasetSpec
from query_surface.reference import expected
from query_surface.semantics import compare
from query_surface.specification import SURFACES


class NativeReaderTransferTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.native = _load_native()
        import pyarrow as pa
        cls.pa = pa

    def write_sources(self, root, spec, tables, extension):
        return join_fixtures.NativeJoinDemandTests.write_sources(
            self, root, spec, tables, extension, group_rows=128)

    def checked(self, wanted, actual, contract, report, *, label, extension, surface):
        compare(wanted, actual, contract)
        self.assertTrue(report["complete_consumption"])
        self.assertEqual(report["surface"], surface)
        fields = report["fields"]
        self.assertEqual(fields["fallback_attempted"], "false")
        self.assertEqual(fields["external_engine_invoked"], "false")
        transferred = int(fields.get("native_input_columnar_buffer_bytes_transferred", "0"))
        regions = int(fields.get("native_input_columnar_buffer_regions_transferred", "0"))
        capacity = int(fields.get("native_input_columnar_allocation_bytes_transferred", "0"))
        if extension == "parquet":
            self.assertGreaterEqual(transferred, 0)
            self.assertEqual(transferred > 0, regions > 0)
            self.assertEqual(transferred > 0, capacity > 0)
            self.assertGreaterEqual(capacity, transferred)
            if transferred:
                self.activated_surfaces.add(surface)
            self.assertEqual(fields["native_input_end_observed"], "true")
            self.assertEqual(fields["native_input_output_ownership_detached"], "true")
            self.assertEqual(fields["native_source_demand_prepared_input_bytes_written"], "0")
            self.assertIn("not_rss_or_whole_query_zero_copy", fields["native_input_columnar_ownership_scope"])
        else:
            self.assertEqual((transferred, regions, capacity), (0, 0, 0))

    def test_owned_reader_results_and_observations_match_all_interfaces_and_formats(self):
        spec = DatasetSpec(seed=0x29_2026_731, fact_rows=131, entity_rows=29,
                           product_rows=11, relationship_rows=43, calendar_rows=7, profile="null_heavy")
        names = {"projection_narrow", "projection_reordered", "projection_rich", "projection_wide",
                 "predicate_one_row", "global_count_star", "global_sum", "distinct_group_key",
                 "topk_10_offset_7", "join_left", "set_union_all", "string_length"}
        cases = [item for item in reviewed_cases() if item.id in names]
        self.assertEqual({item.id for item in cases}, names)
        self.activated_surfaces = set()
        tables = set().union(*(item.relation.sources() for item in cases))
        from shardloom import ShardLoomClient
        cli = ShardLoomClient(binary=os.environ.get("SHARDLOOM_SURFACE_TEST_BINARY"),
                             transport="subprocess", use_persistent_worker=False)
        try:
            binary = cli.binary_command()[0]
        finally:
            cli.close()
        with tempfile.TemporaryDirectory(prefix="shardloom-query-reader-transfer-") as directory:
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
