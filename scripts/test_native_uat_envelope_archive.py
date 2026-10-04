# SPDX-License-Identifier: Apache-2.0
"""Evidence retention and failure behavior of closed-envelope compaction."""

import gzip
import hashlib
import io
import json
from pathlib import Path
import random
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
            raw = (json.dumps({"case": index, "values": [None, "00ff", "é\n"] * 8}) + "\n").encode()
            stored = gzip.compress(raw, mtime=0)
            path = self.root / f"case-{index}.envelope.json.gz"
            path.write_bytes(stored)
            originals[path.name] = stored
            entries.append({"path": str(path), "raw_bytes": len(raw),
                            "raw_sha256": hashlib.sha256(raw).hexdigest(),
                            "stored_bytes": len(stored),
                            "stored_sha256": hashlib.sha256(stored).hexdigest()})
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
        self.assertEqual(result["member_encoding"], "raw_json")
        with tarfile.open(archive_path, "r:xz") as archive:
            self.assertEqual(set(archive.getnames()), {name.removesuffix(".gz") for name in originals})
            for entry, member in zip(entries, result["members"]):
                self.assertEqual(entry["archive_path"], str(archive_path))
                self.assertEqual(entry["archive_member_encoding"], "raw_json")
                self.assertEqual(entry["archive_member"], Path(entry["path"]).name.removesuffix(".gz"))
                raw = archive.extractfile(entry["archive_member"]).read()
                original = originals[member["source_name"]]
                self.assertEqual(raw, gzip.decompress(original))
                self.assertEqual(member["source_stored_bytes"], len(original))
                self.assertEqual(member["source_stored_sha256"], hashlib.sha256(original).hexdigest())
                self.assertEqual(member["sha256"], hashlib.sha256(raw).hexdigest())
                self.assertEqual(member["bytes"], len(raw))
                self.assertFalse(Path(entry["path"]).exists())
        self.assertTrue(result["source_gzip_bytes_verified_before_removal"])
        self.assertTrue(result["original_bytes_verified_before_removal"])

    def test_common_report_content_is_compressed_across_envelopes(self):
        entries, originals = self.envelopes(128)
        random_source = random.Random(57)
        shared = random_source.randbytes(4096).hex()
        old_layout = io.BytesIO()
        with tarfile.open(fileobj=old_layout, mode="w:xz", preset=3) as archive:
            for index, entry in enumerate(entries):
                raw = (json.dumps({"request": random_source.randbytes(17 + index).hex(),
                                   "common_report": shared}) + "\n").encode()
                stored = gzip.compress(raw, mtime=0)
                path = Path(entry["path"])
                path.write_bytes(stored)
                originals[path.name] = stored
                entry.update(raw_bytes=len(raw), raw_sha256=hashlib.sha256(raw).hexdigest(),
                             stored_bytes=len(stored), stored_sha256=hashlib.sha256(stored).hexdigest())
                item = tarfile.TarInfo(path.name)
                item.size = len(stored)
                archive.addfile(item, io.BytesIO(stored))
        result = archive_envelopes(self.root, entries, 1)
        self.assertLess(result["bytes"], len(old_layout.getvalue()) // 3)
        with tarfile.open(result["path"], "r:xz") as archive:
            for member in result["members"]:
                self.assertEqual(archive.extractfile(member["name"]).read(),
                                 gzip.decompress(originals[member["source_name"]]))

    def test_invalid_gzip_or_raw_hash_prevents_any_removal(self):
        for mismatch in ("gzip", "raw_bytes", "raw_sha256"):
            with self.subTest(mismatch=mismatch):
                entries, _ = self.envelopes()
                if mismatch == "gzip":
                    stored = b"not gzip"
                    Path(entries[-1]["path"]).write_bytes(stored)
                    entries[-1].update(stored_bytes=len(stored),
                                       stored_sha256=hashlib.sha256(stored).hexdigest())
                elif mismatch == "raw_bytes":
                    entries[-1]["raw_bytes"] += 1
                else:
                    entries[-1]["raw_sha256"] = "0" * 64
                with self.assertRaisesRegex(ValueError, "invalid gzip|raw envelope differs"):
                    archive_envelopes(self.root, entries, 1)
                self.assertTrue(all(Path(entry["path"]).exists() for entry in entries))
                self.assertFalse((self.root / "envelopes-000001.tar.xz").exists())

    def test_change_during_readback_preserves_all_sources(self):
        entries, originals = self.envelopes()
        real_open = tarfile.open

        def mutate_during_readback(*args, **kwargs):
            if len(args) > 1 and args[1] == "r:xz":
                Path(entries[-1]["path"]).write_bytes(b"changed during readback")
            return real_open(*args, **kwargs)

        with patch("native_uat_envelope_archive.tarfile.open", side_effect=mutate_during_readback):
            with self.assertRaisesRegex(ValueError, "changed during archival"):
                archive_envelopes(self.root, entries, 1)
        self.assertTrue(all(Path(entry["path"]).exists() for entry in entries))
        self.assertEqual(Path(entries[0]["path"]).read_bytes(), originals[Path(entries[0]["path"]).name])
        self.assertTrue(all("archive_path" not in entry for entry in entries))

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

    def test_failed_manifest_readback_preserves_all_originals(self):
        entries, originals = self.envelopes()
        real_read = Path.read_bytes

        def corrupt_manifest(path):
            raw = real_read(path)
            return raw + b"changed" if path.name.endswith(".manifest.json") else raw

        with patch("native_uat_envelope_archive.Path.read_bytes", new=corrupt_manifest):
            with self.assertRaisesRegex(ValueError, "manifest readback differs"):
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
