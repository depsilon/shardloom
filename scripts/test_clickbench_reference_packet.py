#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Validation tests for frozen full-set ClickBench reference packets."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
import tempfile
import unittest

from clickbench_reference_packet import (
    SCHEMA_VERSION,
    canonical_values_sha256,
    load_reference_packet,
)


class ClickBenchReferencePacketTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.queries_path = self.root / "queries.sql"
        self.queries = [f"SELECT {query_id} AS value" for query_id in range(1, 44)]
        self.query_bytes = ("\n".join(f"{statement};" for statement in self.queries) + "\n").encode()
        self.queries_path.write_bytes(self.query_bytes)
        self.packet_path = self.root / "reference.json"

    def tearDown(self):
        self.temporary.cleanup()

    def packet(self, *, kind="retained_native_regression"):
        records = []
        for query_id, statement in enumerate(self.queries, 1):
            values = [{"value": query_id}]
            records.append({
                "query_id": query_id,
                "statement": statement,
                "values": values,
                "values_sha256": canonical_values_sha256(values),
            })
        return {
            "schema_version": SCHEMA_VERSION,
            "queries_sha256": hashlib.sha256(self.query_bytes).hexdigest(),
            "reference_kind": kind,
            "records": records,
        }

    def write_packet(self, packet):
        self.packet_path.write_text(json.dumps(packet, allow_nan=False), encoding="utf-8")
        return self.packet_path

    def test_complete_43_query_packet_returns_values_and_frozen_receipt(self):
        for kind in ("retained_native_regression", "independent_reference"):
            with self.subTest(kind=kind):
                packet = self.packet(kind=kind)
                path = self.write_packet(packet)
                loaded = load_reference_packet(path, self.queries_path)
                self.assertEqual(loaded["values"], {
                    query_id: [{"value": query_id}] for query_id in range(1, 44)
                })
                self.assertEqual(loaded["packet"], {
                    "path": str(path.resolve()),
                    "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                    "reference_kind": kind,
                    "queries_sha256": packet["queries_sha256"],
                })

    def test_incomplete_duplicate_and_extra_query_records_fail(self):
        base = self.packet()
        missing = dict(base, records=base["records"][:-1])
        duplicate = dict(base, records=[*base["records"][:-1], dict(base["records"][0])])
        extra = dict(base, records=[*base["records"], {
            "query_id": 44,
            "statement": "SELECT 44 AS value",
            "values": [{"value": 44}],
            "values_sha256": canonical_values_sha256([{"value": 44}]),
        }])
        for packet in (missing, duplicate, extra):
            with self.subTest(count=len(packet["records"])), self.assertRaises(ValueError):
                load_reference_packet(self.write_packet(packet), self.queries_path)

    def test_statement_query_file_and_values_hashes_are_pinned(self):
        base = self.packet()
        mismatched_statement = json.loads(json.dumps(base))
        mismatched_statement["records"][0]["statement"] += " "
        wrong_query_hash = dict(base, queries_sha256="0" * 64)
        wrong_values_hash = json.loads(json.dumps(base))
        wrong_values_hash["records"][0]["values_sha256"] = "f" * 64
        for packet in (mismatched_statement, wrong_query_hash, wrong_values_hash):
            with self.subTest(packet=packet["records"][0]), self.assertRaises(ValueError):
                load_reference_packet(self.write_packet(packet), self.queries_path)

    def test_row_shape_and_reference_kind_are_validated(self):
        base = self.packet()
        non_object_rows = json.loads(json.dumps(base))
        non_object_rows["records"][0]["values"] = [3]
        non_object_rows["records"][0]["values_sha256"] = canonical_values_sha256([{"value": 1}])
        not_a_list = json.loads(json.dumps(base))
        not_a_list["records"][0]["values"] = {"value": 1}
        wrong_kind = dict(base, reference_kind="historical-ish")
        for packet in (non_object_rows, not_a_list, wrong_kind):
            with self.subTest(record=packet["records"][0]), self.assertRaises(ValueError):
                load_reference_packet(self.write_packet(packet), self.queries_path)

    def test_duplicate_json_keys_and_nonfinite_numbers_are_rejected(self):
        packet_text = json.dumps(self.packet(), allow_nan=False)
        duplicate_key_text = packet_text[:-1] + ',"schema_version":"' + SCHEMA_VERSION + '"}'
        self.packet_path.write_text(duplicate_key_text, encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "duplicate JSON object key"):
            load_reference_packet(self.packet_path, self.queries_path)

        nonfinite_text = packet_text.replace('"values": [{"value": 1}]', '"values": [{"value": NaN}]', 1)
        self.packet_path.write_text(nonfinite_text, encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "nonfinite JSON value"):
            load_reference_packet(self.packet_path, self.queries_path)


if __name__ == "__main__":
    unittest.main()
