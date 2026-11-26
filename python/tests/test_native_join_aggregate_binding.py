# SPDX-License-Identifier: Apache-2.0
"""Exact join matches feed the shared aggregate through every public surface."""
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

from query_surface.adapters import execute
from query_surface.corpus import case
from query_surface.semantics import ResultType, TypedResult, compare
from query_surface.specification import Measure, Relation, SURFACES


I64 = ResultType("int64", False)
TEXT = ResultType("utf8", False)
SCHEMAS = {
    "fact": (("a", ResultType("int64", True)), ("b", I64), ("value", I64)),
    "a": (("id", I64), ("label", TEXT)),
    "b": (("id", I64), ("label", TEXT)),
}
TABLES = {
    "fact": [dict(a=a, b=b, value=value) for a, b, value in (
        (1, 1, 2), (2, 1, 3), (3, 2, 4), (1, 8, 6),
        (None, 1, 7), (8, 1, 8), (2, 2, 9))],
    "a": [dict(id=key, label=label) for key, label in (
        (1, "shared long label"), (2, "shared long label"), (3, "other long label"))],
    "b": [dict(id=key, label=label) for key, label in ((1, ""), (2, "é\0東京"))],
}


def declaration(reverse):
    def scan(name):
        return Relation("scan", SCHEMAS[name], args=(name,))

    relation = scan("fact")
    order = ("b", "a") if reverse else ("a", "b")
    for index, name in enumerate(order):
        key = "next_key" if index else name
        columns = tuple(("left", field, field) for field, _ in relation.schema)
        relation = relation.join(scan(name), ((key, "id"),),
            (*columns, ("right", "label", "g" + name)))
        if index == 0:
            relation = relation.project(*(
                ("next_key" if field == order[1] else field, relation.col(field))
                for field, _ in relation.schema))
    relation = relation.project(("left_group", relation.col("ga")),
        ("right_group", relation.col("gb")), ("amount", relation.col("value")))
    relation = relation.aggregate(("left_group", "right_group"),
        (Measure("sum", relation.col("amount"), "total", ResultType("float64", True)),))
    return case("direct_join_aggregate_" + str(reverse).lower(), "multiway_joins", relation)


def write_sources(native, root, extension):
    import pyarrow as pa
    import pyarrow.parquet as parquet
    from shardloom._result_schema import _arrow_type

    result = {}
    session = native.Session(native.ExecutionResources(64 << 20, 1))
    try:
        for name, rows in TABLES.items():
            schema = pa.schema([pa.field(field, _arrow_type(dtype, pa), dtype.nullable)
                                for field, dtype in SCHEMAS[name]])
            table = pa.Table.from_pylist(rows, schema=schema)
            path = root / f"{name}.{extension}"
            if extension == "parquet":
                parquet.write_table(table, path, row_group_size=2, use_dictionary=True)
            elif extension == "arrow":
                with pa.OSFile(str(path), "wb") as stream, pa.ipc.new_file(stream, schema) as writer:
                    writer.write_table(table, max_chunksize=2)
            else:
                source = session.from_arrow(table)
                session.plan(native.Relation.scan("memory://fixture"),
                    sources=[("memory://fixture", source)]).write(path, "write_vortex")
                del source
            result[name] = (path,)
    finally:
        session.close()
    return result


class NativeJoinAggregateTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.native = _load_native()

    def test_both_join_orders_and_pure_aliases_converge_on_shared_execution(self):
        from shardloom import ShardLoomClient

        cli = ShardLoomClient(binary=os.environ.get("SHARDLOOM_SURFACE_TEST_BINARY"),
                             transport="subprocess", use_persistent_worker=False)
        try:
            binary = cli.binary_command()[0]
        finally:
            cli.close()
        expected = [
            {"left_group": "shared long label", "right_group": "", "total": 5.0},
            {"left_group": "other long label", "right_group": "é\0東京", "total": 4.0},
            {"left_group": "shared long label", "right_group": "é\0東京", "total": 9.0},
        ]
        with tempfile.TemporaryDirectory(prefix="shardloom-direct-join-aggregate-") as directory:
            for extension in ("parquet", "arrow", "vortex"):
                root = Path(directory) / extension
                root.mkdir()
                sources = write_sources(self.native, root, extension)
                for reverse in (False, True):
                    item = declaration(reverse)
                    observed = []
                    for surface in SURFACES:
                        with self.subTest(format=extension, reverse=reverse, surface=surface):
                            kwargs = dict(native=self.native, binary=binary,
                                          memory_bytes=128 << 20, parallelism=2)
                            if surface == "cli_sql":
                                actual, report = execute(item.relation, sources, surface, **kwargs)
                            else:
                                with patch.object(subprocess, "Popen", side_effect=AssertionError("external execution")):
                                    actual, report = execute(item.relation, sources, surface, **kwargs)
                            compare(TypedResult(item.relation.schema, expected), actual, item.contract)
                            self.assertTrue(report["complete_consumption"])
                            fields = report["fields"]
                            self.assertEqual(fields["fallback_attempted"], "false")
                            self.assertEqual(fields["external_engine_invoked"], "false")
                            work = json.loads(fields["local_primitive_native_io_join_work_json"])["direct_aggregate"]
                            self.assertEqual(work["strategy"], "exact_unique_inner_matches_in_original_update_order")
                            self.assertEqual(work["chains_considered"], 1)
                            self.assertEqual(work["chains_declined_duplicates"], 0)
                            self.assertEqual(work["probe_filters_pushed"], 2)
                            if extension != "vortex":
                                self.assertGreater(work["source_batches_direct"], 0)
                                self.assertEqual(work["source_batches_declined"], 0)
                                self.assertEqual(work["aggregate_rows"], 4)
                                self.assertEqual(work["match_rows"], 8)
                                self.assertEqual(work["validated_cells"], 24)
                            else:
                                # The normal writer chooses encodings. Admission
                                # follows those owners without forcing a codec.
                                self.assertGreater(work["source_batches_direct"] + work["source_batches_declined"], 0)
                                if work["aggregate_rows"]:
                                    stage0_matches = 4 if work["probe_filters_pushed"] >= 2 else (6 if reverse else 5)
                                    self.assertEqual(work["aggregate_rows"], 4)
                                    self.assertEqual(work["match_rows"], stage0_matches + 4)
                                    # The public declarations retain different
                                    # intermediate fields before the final pure
                                    # projection. Each still owes its own checks.
                                    first, final = {"dataframe": (5, 6),
                                                    "typed_relation": (4, 3)}.get(surface, (4, 5))
                                    self.assertEqual(work["validated_cells"],
                                                     stage0_matches * first + 4 * final)
                            observed.append({key: value for key, value in work.items()
                                             if key != "validated_cells"})
                    self.assertEqual(len(observed), len(SURFACES))
                    self.assertTrue(all(work == observed[0] for work in observed))


if __name__ == "__main__":
    unittest.main()
