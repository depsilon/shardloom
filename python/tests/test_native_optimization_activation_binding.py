# SPDX-License-Identifier: Apache-2.0
"""Completed values and actual optimization work through five public surfaces."""
import json
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

from query_surface.activation_cases import SCHEMAS, declarations, tables
from query_surface.adapters import execute
from query_surface.semantics import compare
from query_surface.specification import SURFACES


def evidence(fields, suffix):
    for prefix in ("local_primitive_native_io", "local_count_native_io"):
        key = prefix + "_" + suffix + "_json"
        if key in fields:
            return json.loads(fields[key])
    raise AssertionError(f"missing completed native evidence: {suffix}")


def write_sources(native, root, extension):
    import pyarrow as pa
    import pyarrow.parquet as parquet
    from shardloom._result_schema import _arrow_type

    result = {}
    session = native.Session(native.ExecutionResources(64 << 20, 1))
    try:
        for name, rows in tables().items():
            schema = pa.schema([pa.field(field, _arrow_type(dtype, pa), dtype.nullable)
                                for field, dtype in SCHEMAS[name]])
            table = pa.Table.from_pylist(rows, schema=schema)
            path = root / f"{name}.{extension}"
            if extension == "parquet":
                parquet.write_table(table, path, row_group_size=257, use_dictionary=True)
            elif extension == "arrow":
                with pa.OSFile(str(path), "wb") as stream, pa.ipc.new_file(stream, schema) as writer:
                    writer.write_table(table, max_chunksize=257)
            else:
                source = session.from_arrow(table)
                session.plan(native.Relation.scan("memory://fixture"),
                    sources=[("memory://fixture", source)]).write(path, "write_vortex")
                del source
            result[name] = (path,)
    finally:
        session.close()
    return result


class NativeOptimizationActivationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.native = _load_native()

    def check_work(self, fields, requirement, extension):
        self.assertEqual(fields["fallback_attempted"], "false")
        self.assertEqual(fields["external_engine_invoked"], "false")
        if requirement.startswith("count_"):
            work = evidence(fields, "count_partial_work")
            if requirement == "count_partial":
                self.assertGreater(work["admitted_batches"], 0)
                self.assertEqual(work["input_rows"], 4096)
                self.assertLess(work["reduced_groups"], work["input_rows"])
            elif requirement == "count_unique":
                self.assertGreater(work["considered_batches"], 0)
                self.assertGreater(work["declined_unique_batches"], 0)
                self.assertEqual(work["admitted_batches"], 0)
            else:
                self.assertEqual(work["considered_batches"], 0)
                self.assertEqual(work["admitted_batches"], 0)
        if requirement.startswith("filter_"):
            work = evidence(fields, "join_work")
            runtime_filter, skew = work["runtime_filter"], work["skew"]
            self.assertEqual(runtime_filter["considered"], 1)
            if requirement == "filter_small":
                self.assertEqual(runtime_filter["admitted"], 0)
                self.assertEqual(runtime_filter["declined_small"], 1)
                self.assertEqual(skew["admitted_groups"], 0)
            else:
                self.assertEqual(runtime_filter["admitted"], 1)
                self.assertEqual(runtime_filter["build_keys"], 128)
                self.assertGreater(runtime_filter["rejected_rows"], 450)
                self.assertEqual(runtime_filter["index_lookups_avoided"], runtime_filter["rejected_rows"])
                if requirement == "filter_and_skew":
                    self.assertEqual(skew["admitted_groups"], 1)
                    self.assertEqual(skew["admitted_rows"], 70)
                    self.assertEqual(skew["range_visits"], 1)
                    self.assertEqual(skew["rows_visited"], 70)
                else:
                    self.assertEqual(skew["range_visits"], 0)
        if requirement == "cyclic":
            work = evidence(fields, "join_work")["cyclic"]
            self.assertEqual(work["executions"], 1)
            self.assertEqual(work["outer_rows"], 132)
            self.assertEqual(work["witness_exits"], 44)
            self.assertGreater(work["last_probes"], 0)
        elif requirement == "cyclic_excluded":
            self.assertEqual(evidence(fields, "join_work")["cyclic"]["executions"], 0)
        scans = evidence(fields, "file_scans")
        if extension != "vortex":
            self.assertEqual(scans["sources"], [])
            return
        self.assertGreater(len(scans["sources"]), 0)
        for source in scans["sources"]:
            self.assertEqual(len(source["generation_fingerprint"]), 64)
            self.assertEqual(source["segment_reuse"]["cross_call_hits"], 0)
        if requirement == "narrow_scan":
            contracts = scans["contracts"]
            projected = set().union(*(set(row["projected_columns"] or []) for row in contracts))
            predicates = set().union(*(set(row["pushed_predicate_columns"] or []) for row in contracts))
            self.assertEqual(projected | predicates, {"id", "key"})
            self.assertNotIn("payload", projected)
            self.assertGreater(sum(row["completed_read_bytes"] for row in scans["sources"]), 0)
        elif requirement in {"metadata_count", "pruned_count"}:
            self.assertEqual(sum(row["provider_scan_invocations"] for row in scans["contracts"]), 0)
            self.assertEqual(sum(row["completed_read_bytes"] for row in scans["sources"]), 0)
            self.assertEqual(sum(row["segment_requests"] for row in scans["sources"]), 0)
            if requirement == "pruned_count":
                self.assertEqual(sum(row["whole_file_exclusions"] for row in scans["contracts"]), 1)

    def test_complete_values_and_actual_activation_across_all_surfaces_and_formats(self):
        from shardloom import ShardLoomClient

        cli = ShardLoomClient(binary=os.environ.get("SHARDLOOM_SURFACE_TEST_BINARY"),
                             transport="subprocess", use_persistent_worker=False)
        try:
            binary = cli.binary_command()[0]
        finally:
            cli.close()
        cases = declarations()
        self.assertEqual(len(cases), 17)
        self.assertEqual(len({item.id for item, _, _ in cases}), len(cases))
        with tempfile.TemporaryDirectory(prefix="shardloom-optimization-activation-") as directory:
            for extension in ("vortex", "parquet", "arrow"):
                root = Path(directory) / extension
                root.mkdir()
                sources = write_sources(self.native, root, extension)
                for item, expected, requirement in cases:
                    for surface in SURFACES:
                        with self.subTest(format=extension, case=item.id, surface=surface):
                            kwargs = dict(native=self.native, binary=binary,
                                          memory_bytes=128 << 20, parallelism=2)
                            if surface == "cli_sql":
                                actual, report = execute(item.relation, sources, surface, **kwargs)
                            else:
                                with patch.object(subprocess, "Popen", side_effect=AssertionError("external execution")):
                                    actual, report = execute(item.relation, sources, surface, **kwargs)
                            compare(expected, actual, item.contract)
                            self.assertTrue(report["complete_consumption"])
                            self.check_work(report["fields"], requirement, extension)
                            if (requirement, extension, surface) == ("cyclic", "vortex", "dataframe"):
                                work = evidence(report["fields"], "join_work")["cyclic"]
                                self.assertGreater(work["middle_payload_rows_validated"], 0)
                                self.assertGreater(work["last_payload_rows_validated"], 0)
                                self.assertGreater(work["payload_cells_validated"], 0)


if __name__ == "__main__":
    unittest.main()
