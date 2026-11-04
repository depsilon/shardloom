# SPDX-License-Identifier: Apache-2.0
"""Lossless bounded SQLite transport checks for the independent query oracle."""

from __future__ import annotations

from datetime import datetime, timezone
from contextlib import closing
from decimal import Decimal
import tempfile
from pathlib import Path
import sqlite3
import sys
import unittest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "benchmarks/traditional_analytics"))
sys.path.insert(0, str(ROOT / "scripts"))

try:
    import pyarrow as pa
    import pyarrow.ipc as ipc
except ImportError:
    pa = ipc = None

from query_surface.datasets import file_hash
from query_surface.expanded_reference_input import build_database


class Guard:
    def check(self):
        pass


@unittest.skipIf(pa is None, "IPC fixture controls require PyArrow")
class ExpandedReferenceInputTests(unittest.TestCase):
    def make_input(self, root, rows):
        dataset = root / "dataset"
        dataset.mkdir()
        schema = pa.schema(
            [
                pa.field("row_id", pa.int64(), nullable=False),
                pa.field("event_time", pa.timestamp("us")),
                pa.field("amount", pa.decimal128(18, 2)),
                pa.field("items", pa.list_(pa.int64())),
                pa.field(
                    "detail", pa.struct([pa.field("score", pa.int64()), pa.field("label", pa.string())])
                ),
            ]
        )
        table = pa.Table.from_pylist(rows, schema=schema)
        source = dataset / "fact-00000.arrow"
        with source.open("wb") as handle:
            with ipc.new_file(handle, schema) as writer:
                writer.write_table(table, max_chunksize=1)
        manifest = {
            "tables": {
                "fact": {
                    "rows": len(rows),
                    "files": [
                        {
                            "path": source.name,
                            "rows": len(rows),
                            "bytes": source.stat().st_size,
                            "sha256": file_hash(source),
                        }
                    ],
                }
            }
        }
        return dataset, manifest

    def test_null_negative_timestamp_decimal_and_nested_values_survive(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            rows = [
                {
                    "row_id": 1,
                    "event_time": datetime(1969, 12, 31, 23, 59, 59, 999999, tzinfo=timezone.utc),
                    "amount": Decimal("-123.40"),
                    "items": [1, None, 3],
                    "detail": {"score": None, "label": "東京"},
                },
                {"row_id": 2, "event_time": None, "amount": None, "items": None, "detail": None},
            ]
            dataset, manifest = self.make_input(root, rows)
            db = root / "reference.sqlite"
            record = build_database(
                db, dataset, manifest, {"fact": {"event_time", "amount", "items", "detail"}}, Guard()
            )
            with closing(sqlite3.connect(db)) as connection:
                fetched = connection.execute(
                    "SELECT row_id,event_time,amount,items,detail FROM fact ORDER BY row_id"
                ).fetchall()
            self.assertEqual(fetched[0], (1, -1, "-123.40", "[1,null,3]", '{"label":"東京","score":null}'))
            self.assertEqual(fetched[1], (2, None, None, None, None))
            self.assertEqual(record["row_counts"], {"fact": 2})
            self.assertEqual(record["database_sha256"], file_hash(db))

    def test_duplicate_primary_key_fails_and_keeps_partial_unadmitted(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            dataset, manifest = self.make_input(
                root,
                [
                    {"row_id": 4, "event_time": None, "amount": None, "items": None, "detail": None},
                    {"row_id": 4, "event_time": None, "amount": None, "items": None, "detail": None},
                ],
            )
            db = root / "reference.sqlite"
            with self.assertRaisesRegex(ValueError, "duplicate or invalid primary key"):
                build_database(db, dataset, manifest, {"fact": set()}, Guard())
            self.assertFalse(db.exists())
            self.assertTrue(db.with_name(db.name + ".partial").exists())


if __name__ == "__main__":
    unittest.main()
