# SPDX-License-Identifier: Apache-2.0
"""Evidence retention and failure behavior of closed-envelope compaction."""

import gzip
import hashlib
import json
from pathlib import Path
import tarfile
import tempfile
import unittest
from unittest.mock import patch

from native_uat_envelope_archive import archive_envelopes


class EnvelopeArchiveTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()

    def envelopes(self, count=3):
        entries, originals = [], {}
        for index in range(count):
            raw = gzip.compress((f'{index}: exact envelope with NULL and binary 00ff\n' * 8).encode(), mtime=0)
            path = self.root / f"case-{index}.envelope.json.gz"
            path.write_bytes(raw)
            originals[path.name] = raw
            entries.append({"path": str(path), "stored_bytes": len(raw),
                            "stored_sha256": hashlib.sha256(raw).hexdigest()})
        return entries, originals

    def test_full_batch_retains_every_original_byte_and_lookup(self):
        entries, originals = self.envelopes(128)
        result = archive_envelopes(self.root, entries, 1)
        archive_path = Path(result["path"])
        stored = archive_path.read_bytes()
        self.assertEqual(result["bytes"], len(stored))
        self.assertEqual(result["sha256"], hashlib.sha256(stored).hexdigest())
        manifest = Path(result["manifest_path"]).read_bytes()
        self.assertEqual(result["manifest_bytes"], len(manifest))
        self.assertEqual(result["manifest_sha256"], hashlib.sha256(manifest).hexdigest())
        self.assertEqual(json.loads(manifest)["members"], json.loads(json.dumps(result["members"])))
        with tarfile.open(archive_path, "r:xz") as archive:
            self.assertEqual(set(archive.getnames()), set(originals))
            for entry, member in zip(entries, result["members"]):
                self.assertEqual(entry["archive_path"], str(archive_path))
                raw = archive.extractfile(entry["archive_member"]).read()
                self.assertEqual(raw, originals[member["name"]])
                self.assertEqual(member["sha256"], hashlib.sha256(raw).hexdigest())
                self.assertEqual(member["bytes"], len(raw))
                self.assertFalse(Path(entry["path"]).exists())
        self.assertTrue(result["original_bytes_verified_before_removal"])

    def test_changed_original_prevents_any_removal(self):
        entries, originals = self.envelopes()
        Path(entries[-1]["path"]).write_bytes(b"changed")
        with self.assertRaisesRegex(ValueError, "changed before archival"):
            archive_envelopes(self.root, entries, 1)
        self.assertTrue(all(Path(entry["path"]).exists() for entry in entries))
        self.assertEqual(Path(entries[0]["path"]).read_bytes(), originals[Path(entries[0]["path"]).name])
        self.assertFalse((self.root / "envelopes-000001.tar.xz").exists())

    def test_failed_readback_preserves_all_originals(self):
        entries, originals = self.envelopes()
        real_open = tarfile.open

        def fail_readback(*args, **kwargs):
            if len(args) > 1 and args[1] == "r:xz":
                raise OSError("injected archive readback failure")
            return real_open(*args, **kwargs)

        with patch("native_uat_envelope_archive.tarfile.open", side_effect=fail_readback):
            with self.assertRaisesRegex(OSError, "injected archive readback failure"):
                archive_envelopes(self.root, entries, 1)
        for entry in entries:
            path = Path(entry["path"])
            self.assertEqual(path.read_bytes(), originals[path.name])
            self.assertNotIn("archive_path", entry)

    def test_existing_archive_is_not_replaced(self):
        entries, _ = self.envelopes()
        target = self.root / "envelopes-000001.tar.xz"
        target.write_bytes(b"existing archive")
        with self.assertRaises(FileExistsError):
            archive_envelopes(self.root, entries, 1)
        self.assertEqual(target.read_bytes(), b"existing archive")
        self.assertTrue(all(Path(entry["path"]).exists() for entry in entries))

    def test_rejects_symlink_and_duplicate_inputs(self):
        entries, _ = self.envelopes()
        alias = self.root / "alias.envelope.json.gz"
        alias.symlink_to(entries[0]["path"])
        with self.assertRaisesRegex(ValueError, "not a regular file"):
            archive_envelopes(self.root, [{**entries[0], "path": str(alias)}], 1)
        with self.assertRaisesRegex(ValueError, "duplicate"):
            archive_envelopes(self.root, [entries[0], entries[0]], 1)
        self.assertTrue(all(Path(entry["path"]).exists() for entry in entries))

    def test_tail_batch_and_empty_or_oversized_batches(self):
        entries, _ = self.envelopes(1)
        with self.assertRaisesRegex(ValueError, "1..128"):
            archive_envelopes(self.root, [], 1)
        with self.assertRaisesRegex(ValueError, "1..128"):
            archive_envelopes(self.root, entries * 129, 1)
        result = archive_envelopes(self.root, entries, 1)
        self.assertEqual(len(result["members"]), 1)


if __name__ == "__main__":
    unittest.main()
