# SPDX-License-Identifier: Apache-2.0
"""Fixture identities, independent seeds, observed distributions and layouts."""
from __future__ import annotations

from collections import Counter
from dataclasses import replace
from pathlib import Path
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "benchmarks" / "traditional_analytics"))
sys.path.insert(0, str(ROOT / "scripts"))

from query_surface.datasets import (
    ColumnObservation, DatasetSpec, FACT_SCHEMA, LAYOUTS, RowGenerator, SCHEMAS,
    layout_indices, stream_seed, write_dataset,
)
from query_surface.semantics import ResultContract, TypedResult, arrow_schema, compare


class QueryDatasetTests(unittest.TestCase):
    def setUp(self):
        self.spec = DatasetSpec(fact_rows=513, entity_rows=53, product_rows=11,
                                relationship_rows=107, calendar_rows=7)

    def test_independent_named_streams_and_declared_correlations(self):
        generator = RowGenerator(self.spec, "fact")
        self.assertEqual(len(generator.seeds), len(set(generator.seeds.values())))
        self.assertFalse(set(generator.seeds.values()) & set(generator.null_seeds.values()))
        self.assertNotEqual(stream_seed(1, "fact", "group_key"), stream_seed(1, "entities", "group_key"))
        for i in range(100):
            row = generator.row(i)
            self.assertEqual(row["correlated_key"], row["selection_key"])
            self.assertEqual(row["anti_correlated_key"] + row["selection_key"], 999999)
        values = [generator.row(i, ("selection_key", "independent_key")) for i in range(100)]
        self.assertTrue(any(row["selection_key"] != row["independent_key"] for row in values))

    def test_seed_changes_logical_data_but_layout_preserves_row_ids(self):
        self.assertNotEqual(self.spec.identity, replace(self.spec, seed=9).identity)
        self.assertEqual(self.spec.identity, replace(self.spec).identity)
        for table, count in self.spec.counts().items():
            for layout in LAYOUTS:
                with self.subTest(table=table, layout=layout):
                    self.assertEqual(sorted(layout_indices(self.spec, table, layout)), list(range(count)))
        self.assertNotEqual(list(layout_indices(self.spec, "fact", "sorted")),
                            list(layout_indices(self.spec, "fact", "shuffled")))

    def test_nullability_and_domain_profiles_are_not_inferred_from_values(self):
        schema = dict(FACT_SCHEMA)
        rows = list(RowGenerator(self.spec, "fact").rows())
        self.assertEqual(len(schema), 38)
        self.assertTrue(schema["nullable_no_null"].nullable)
        self.assertTrue(all(row["nullable_no_null"] is not None for row in rows))
        self.assertTrue(all(row["all_null"] is None for row in rows))
        self.assertTrue(any(row["optional_value"] is None for row in rows))
        self.assertTrue(any(row["unicode_text"] == "🙂" for row in rows))
        self.assertTrue(any(row["unicode_text"] == "" for row in rows))
        self.assertTrue(any(row["event_time"] < 0 for row in rows))
        heavy = list(RowGenerator(replace(self.spec, profile="null_heavy"), "fact").rows(("optional_value",)))
        self.assertGreater(sum(row["optional_value"] is None for row in heavy),
                           sum(row["optional_value"] is None for row in rows))

    def test_observation_is_exact_until_explicitly_labelled_approximate(self):
        observation = ColumnObservation()
        for value in [None, None, 1, 1, 2, "", "2"]:
            observation.add(value)
        record = observation.record()
        self.assertEqual(record["nulls"], 2)
        self.assertEqual(record["distinct_nonnull"], 4)
        self.assertEqual(record["distinct_method"], "exact_value_set")
        large = ColumnObservation()
        for i in range(20000):
            large.add(i)
        record = large.record()
        self.assertEqual(record["distinct_method"], "hll_4096_registers")
        self.assertLess(abs(record["distinct_nonnull"] - 20000) / 20000, 0.08)
        self.assertGreater(record["distinct_relative_standard_error"], 0)

    def test_empty_fact_is_admitted_but_dimensions_and_profiles_are_validated(self):
        self.assertEqual(list(RowGenerator(replace(self.spec, fact_rows=0), "fact").rows()), [])
        for changes in ({"fact_rows": -1}, {"entity_rows": 0}, {"seed": True}, {"profile": "guessed"}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                replace(self.spec, **changes)

    def test_all_layouts_read_back_with_complete_typed_identity(self):
        try:
            import pyarrow as pa
        except ImportError:
            self.skipTest("optional PyArrow fixture writer is unavailable")
        guard_calls = []
        with tempfile.TemporaryDirectory(prefix="shardloom-query-layouts-") as directory:
            fingerprints = {}
            for layout in LAYOUTS:
                root = Path(directory) / layout
                manifest = write_dataset(root, self.spec, layout, guard=lambda: guard_calls.append(layout), batch_rows=64)
                for table, record in manifest["tables"].items():
                    tables = []
                    for file in record["files"]:
                        with pa.memory_map(str(root / file["path"]), "r") as handle:
                            table_data = pa.ipc.open_file(handle).read_all()
                        self.assertEqual(table_data.num_rows, file["rows"])
                        if layout == "partitioned":
                            self.assertTrue(all(value % 8 == file["partition_value"]
                                                for value in table_data[file["partition_column"]].to_pylist()))
                        tables.append(table_data)
                    actual = pa.concat_tables(tables)
                    compare(TypedResult(SCHEMAS[table], list(RowGenerator(self.spec, table).rows())),
                            TypedResult(arrow_schema(actual.schema), actual.to_pylist()), ResultContract(SCHEMAS[table]))
                    token = record["multiset_sha256_sum"], record["multiset_sha256_square_sum"]
                    if table in fingerprints:
                        self.assertEqual(token, fingerprints[table])
                    fingerprints[table] = token
                fact = manifest["tables"]["fact"]
                rows = list(RowGenerator(self.spec, "fact").rows())
                for cutoff, count in fact["selection_key_less_than_counts"].items():
                    self.assertEqual(count, sum(row["selection_key"] < int(cutoff) for row in rows))
                counts = Counter(row["entity_id"] for row in rows)
                self.assertEqual(fact["entity_key_observation"]["null_keys"], counts[None])
                self.assertEqual(fact["observed_columns"]["all_null"]["nulls"], len(rows))
            with self.assertRaises(FileExistsError):
                write_dataset(Path(directory) / "sorted", self.spec, "sorted", guard=lambda: None)
        self.assertGreater(len(guard_calls), 50)

    def test_empty_input_keeps_typed_file_and_preflight_precedes_writes(self):
        try:
            import pyarrow as pa
        except ImportError:
            self.skipTest("optional PyArrow fixture writer is unavailable")
        with tempfile.TemporaryDirectory(prefix="shardloom-query-empty-") as directory:
            root = Path(directory) / "empty"
            def reject():
                raise RuntimeError("storage admission rejected")
            with self.assertRaisesRegex(RuntimeError, "storage admission"):
                write_dataset(root, self.spec, "sorted", guard=reject)
            self.assertFalse(root.exists())
            manifest = write_dataset(root, replace(self.spec, fact_rows=0), "sorted", guard=lambda: None)
            path = root / manifest["tables"]["fact"]["files"][0]["path"]
            with pa.memory_map(str(path), "r") as handle:
                table = pa.ipc.open_file(handle).read_all()
            self.assertEqual(table.num_rows, 0)
            self.assertEqual(arrow_schema(table.schema), FACT_SCHEMA)


if __name__ == "__main__":
    unittest.main()
