# SPDX-License-Identifier: Apache-2.0
"""Portable evidence controls use local bytes and never execute a query."""

from pathlib import Path
import hashlib
import lzma
import sys
import tempfile
import unittest
import zlib

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "benchmarks/traditional_analytics"))
from query_surface.expanded_packet import bounded_unpack, canonical_blob_bytes, write_part
from query_surface.expanded_replay import lines, source_json


class ExpandedPacketTests(unittest.TestCase):
    def test_ordered_chunk_and_original_blob_round_trip(self):
        rows = [{"id": "first", "text": "é"}, {"id": "last", "value": None}]
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            part = write_part(root, 0, iter(rows))
            self.assertEqual(list(lines(root, "cases-0000.jsonl.xz", part, 2)), rows)
            with self.assertRaises(FileExistsError):
                write_part(root, 0, [])
        source = canonical_blob_bytes(rows)
        for level in (0, 3, 9):
            encoded = zlib.compress(source, level)
            actual, hashes = bounded_unpack(encoded, "control")
            self.assertEqual(hashes["blob_sha256"], hashlib.sha256(encoded).hexdigest())
            self.assertEqual(source_json({"value": actual, **hashes}, "value"), source)
            with self.assertRaisesRegex(ValueError, "original SQLite JSON"):
                source_json({"value": actual[::-1], **hashes}, "value")

    def test_changed_path_count_expansion_duplicate_key_and_trailing_stream_are_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            raw = b'{"n":1}\n'
            name = "cases-0000.jsonl.xz"

            def verify(data, limit, count=1, path=name):
                (root / name).write_bytes(data)
                info = dict(
                    path=path,
                    compressed_bytes=len(data),
                    raw_bytes=limit,
                    sha256=hashlib.sha256(data).hexdigest(),
                )
                return list(lines(root, name, info, count))

            for data, limit, count, path in [
                (lzma.compress(raw), len(raw), 2, name),
                (lzma.compress(raw), len(raw), 1, "../elsewhere"),
                (lzma.compress(raw), len(raw) - 1, 1, name),
                (lzma.compress(raw) * 2, len(raw), 1, name),
                (lzma.compress(raw)[:-1], len(raw), 1, name),
                (lzma.compress(b'{"n":1,"n":2}\n'), 14, 1, name),
            ]:
                with self.subTest(limit=limit, count=count, path=path), self.assertRaises(ValueError):
                    verify(data, limit, count, path)
