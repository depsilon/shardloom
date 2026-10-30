# SPDX-License-Identifier: Apache-2.0
"""Fresh join consumers share complete results across public interfaces and formats."""

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
else:
    from test_native_binding import _load_native

from query_surface.adapters import execute
from query_surface.corpus import case, composition_neighbors
from query_surface.datasets import DatasetSpec, RowGenerator, SCHEMAS
from query_surface.reference import expected
from query_surface.semantics import compare
from query_surface.specification import Order, Relation, SURFACES


class NativeJoinDemandTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.native = _load_native()
        import pyarrow as pa
        cls.pa = pa

    def checked(self, wanted, actual, contract, report, *, label, extension, surface):
        compare(wanted, actual, contract)
        self.assertTrue(report["complete_consumption"])
        self.assertEqual(report["surface"], surface)
        fields = report["fields"]
        self.assertEqual(fields["fallback_attempted"], "false")
        self.assertEqual(fields["external_engine_invoked"], "false")
        if extension == "parquet":
            self.assertGreater(int(fields["native_source_demand_sources"]), 0)
            self.assertLess(int(fields["native_source_demand_projected_columns"]),
                            int(fields["native_source_demand_available_columns"]))
            self.assertEqual(fields["native_source_demand_prepared_input_bytes_written"], "0")
            self.assertEqual(fields["native_input_end_observed"], "true")
            self.assertEqual(fields["native_input_output_ownership_detached"], "true")
            self.assertIn("not_full_ingest_certification", fields["native_source_demand_validation_scope"])
            if label in {"join_population_inner_count", "join_population_left_count"}:
                self.assertEqual(int(fields["native_source_demand_projected_columns"]), 3)

    def write_sources(self, root, spec, tables, extension):
        import pyarrow.parquet as parquet
        from shardloom._result_schema import _arrow_type
        sources = {}
        session = self.native.Session(self.native.ExecutionResources(256 << 20, 2))
        try:
            for table in sorted(tables):
                path = root / f"{table}.{extension}"
                schema = self.pa.schema([self.pa.field(name, _arrow_type(dtype, self.pa), dtype.nullable)
                                        for name, dtype in SCHEMAS[table]])
                rows = list(RowGenerator(spec, table).rows())
                # A fresh layout breaks discovery-set row adjacency. Expectations
                # compare complete multisets or explicitly ordered results.
                rows = rows[1::2] + list(reversed(rows[::2]))
                data = self.pa.Table.from_pylist(rows, schema=schema)
                if extension == "parquet":
                    parquet.write_table(data, path, row_group_size=7, use_dictionary=True)
                elif extension == "vortex":
                    source = session.from_arrow(data)
                    session.plan(self.native.Relation.scan("memory://fixture"),
                        sources=[("memory://fixture", source)]).write(path, "write_vortex")
                else:
                    with self.pa.OSFile(str(path), "wb") as stream, self.pa.ipc.new_file(stream, schema) as writer:
                        writer.write_table(data, max_chunksize=7)
                sources[table] = (path,)
        finally:
            session.close()
        return sources

    def test_fresh_count_mixed_multiway_and_rich_consumers_match_all_interfaces_and_formats(self):
        spec = DatasetSpec(seed=0x2910_2029, fact_rows=129, entity_rows=23,
                           product_rows=7, relationship_rows=37, calendar_rows=5, profile="null_heavy")
        wanted_names = {"join_population_inner_count", "join_population_left_count",
            "join_population_inner_mixed", "join_population_left_mixed", "join_population_semi",
            "join_population_anti", "join_population_duplicate_fanout", "star_join",
            "star_join_products_first", "composite_active_region_report"}
        cases = [item for item in composition_neighbors() if item.id in wanted_names]
        self.assertEqual({item.id for item in cases}, wanted_names)
        fact, entities = Relation.scan("fact"), Relation.scan("entities")
        columns = tuple(("left", name, name) for name in ("row_id", "items", "detail", "amount", "event_time", "long_text"))
        joined = fact.join(entities, (("entity_id", "entity_id"),), columns, "left")
        projected = joined.project(*[(name, joined.col(name)) for name in ("row_id", "detail", "amount", "event_time")])
        cases.append(case("join_parent_rich_projection", "joins",
                          projected.order(Order("row_id")).take(17, offset=3), ordered=True))
        tables = set().union(*(item.relation.sources() for item in cases))
        from shardloom import ShardLoomClient
        cli = ShardLoomClient(binary=os.environ.get("SHARDLOOM_SURFACE_TEST_BINARY"),
                             transport="subprocess", use_persistent_worker=False)
        try:
            binary = cli.binary_command()[0]
        finally:
            cli.close()
        with tempfile.TemporaryDirectory(prefix="shardloom-join-demand-") as directory:
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
