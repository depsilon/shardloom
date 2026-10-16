"""Complete-byte and failure-retention checks for streaming UAT JSON logs."""
from __future__ import annotations

import gzip
import hashlib
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))

import run_native_streaming_input_uat as uat


VALUE = {"unicode": "é\n", "integer": 9223372036854775815,
         "nested": [None, {"b": True}]}
RAW = ('{"unicode":"é\\n","integer":9223372036854775815,'
       '"nested":[null,{"b":true}]}\n').encode()


class StreamingUatCompressionTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()
        self.harness = uat.Harness.__new__(uat.Harness)
        self.harness.root = self.root
        self.harness.logs = self.root / "logs"
        self.harness.logs.mkdir()
        self.harness.compress_logs = True
        self.harness.compressed_json_files = []

    def test_default_bytes_and_root_summary_remain_plain_json(self):
        self.harness.compress_logs = False
        path = self.harness.logs / "plain.json"
        self.harness.json(path, VALUE)
        self.assertEqual(path.read_bytes(), RAW)
        self.assertFalse(path.with_suffix(".json.gz").exists())
        self.harness.compress_logs = True
        summary = self.root / "summary.json"
        self.harness.json(summary, VALUE)
        self.assertEqual(summary.read_bytes(), RAW)
        self.assertFalse(summary.with_suffix(".json.gz").exists())
        self.assertEqual(self.harness.compressed_json_files, [])

    def test_compressed_log_preserves_complete_bytes_and_both_identities(self):
        path = self.harness.logs / "complete.json"
        self.harness.json(path, VALUE)
        packed_path = path.with_suffix(".json.gz")
        packed = packed_path.read_bytes()
        self.assertEqual(gzip.decompress(packed), RAW)
        self.assertFalse(path.exists())
        self.assertEqual(self.harness.compressed_json_files, [{
            "path": "logs/complete.json.gz", "raw_path": "logs/complete.json",
            "bytes": len(packed), "sha256": hashlib.sha256(packed).hexdigest(),
            "raw_bytes": len(RAW), "raw_sha256": hashlib.sha256(RAW).hexdigest(),
        }])

    def test_existing_raw_or_compressed_files_are_never_replaced(self):
        existing = self.harness.logs / "existing.json"
        existing.write_bytes(b"foreign raw evidence")
        with self.assertRaises(FileExistsError):
            self.harness.json(existing, VALUE)
        self.assertEqual(existing.read_bytes(), b"foreign raw evidence")
        self.assertFalse(existing.with_suffix(".json.gz").exists())
        companion = self.harness.logs / "companion.json"
        companion.with_suffix(".json.gz").write_bytes(b"foreign gzip evidence")
        with self.assertRaises(FileExistsError):
            self.harness.json(companion, VALUE)
        self.assertEqual(companion.read_bytes(), RAW)
        self.assertEqual(companion.with_suffix(".json.gz").read_bytes(), b"foreign gzip evidence")
        self.assertEqual(self.harness.compressed_json_files, [])

    def test_corrupt_and_different_content_readback_keep_originals(self):
        for number, packed in enumerate((b"invalid gzip", gzip.compress(b"different JSON", mtime=0))):
            with self.subTest(number=number):
                path = self.harness.logs / f"failed-{number}.json"
                with patch.object(uat.gzip, "compress", return_value=packed):
                    with self.assertRaises(ValueError):
                        self.harness.json(path, VALUE)
                self.assertEqual(path.read_bytes(), RAW)
                self.assertEqual(path.with_suffix(".json.gz").read_bytes(), packed)
        self.assertEqual(self.harness.compressed_json_files, [])

    def test_raw_replacement_before_cleanup_is_preserved_and_rejected(self):
        path = self.harness.logs / "changed.json"
        compress = gzip.compress

        def replace(raw, **kwargs):
            path.unlink()
            path.write_bytes(b"replacement must survive")
            return compress(raw, **kwargs)

        with patch.object(uat.gzip, "compress", side_effect=replace):
            with self.assertRaises(ValueError):
                self.harness.json(path, VALUE)
        self.assertEqual(path.read_bytes(), b"replacement must survive")
        self.assertEqual(gzip.decompress(path.with_suffix(".json.gz").read_bytes()), RAW)
        self.assertEqual(self.harness.compressed_json_files, [])


if __name__ == "__main__":
    unittest.main()
