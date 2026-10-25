"""Offline corruption controls; no engine execution or local evidence paths required."""
import copy
import gzip
import io
import json
import lzma
from pathlib import Path
import tarfile
import unittest
from unittest.mock import patch

import verify as v


ROOT = Path(__file__).resolve().parent


class PacketTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.manifest = (ROOT / "manifest.json").read_bytes()
        cls.archive = (ROOT / "packet.tar.xz").read_bytes()
        cls.verifier = (ROOT / "verify.py").read_bytes()
        cls.files = v.object_files(cls.manifest, cls.archive, cls.verifier)

    def test_complete_packet(self):
        self.assertEqual(v.verify_evidence(self.files)["status"], "passed")

    def test_strict_json(self):
        for raw in ('{"a":1,"a":2}', 'NaN', 'Infinity', '-Infinity', '1e999'):
            with self.subTest(raw=raw), self.assertRaises(ValueError):
                v.strict_json(raw)

    def test_value_types_and_float_tolerance(self):
        for a, b in ((True, 1), (1, 1.0), ({"a": 1}, {"b": 1}), ([1], [1, 2]), (1.0, 1.001)):
            self.assertFalse(v.equivalent(a, b))
        self.assertTrue(v.equivalent([1.0], [1.0 + 1e-13]))
        for value in (True, -1, "-1", "1.0", "１", None):
            with self.subTest(value=value), self.assertRaises(ValueError):
                v.unsigned(value)

    def test_manifest_and_archive_identities(self):
        for changes in ({"expanded_file_bytes": 0}, {"unique_objects": 0}, {"files": []}):
            meta = v.strict_json(self.manifest)
            meta.update(changes)
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                v.object_files(json.dumps(meta), self.archive, self.verifier)
        with self.assertRaises(ValueError):
            v.object_files(self.manifest, self.archive + b"x", self.verifier)
        with self.assertRaises(ValueError):
            v.object_files(self.manifest, self.archive, b"different verifier")

    def test_archive_safety(self):
        def reject(names, symlink=False):
            stream = io.BytesIO()
            with tarfile.open(fileobj=stream, mode="w:xz") as archive:
                for name in names:
                    entry = tarfile.TarInfo(name)
                    if symlink:
                        entry.type = tarfile.SYMTYPE
                        entry.linkname = "/tmp/escape"
                    archive.addfile(entry)
            packed = stream.getvalue()
            meta = v.strict_json(self.manifest)
            meta["archive"].update(bytes=len(packed), sha256=v.sha(packed))
            with self.assertRaises(ValueError):
                v.object_files(json.dumps(meta), packed, self.verifier)
        valid_empty = "objects/sha256/" + v.sha(b"")
        reject([valid_empty, valid_empty])
        reject(["../escape"])
        reject([valid_empty], symlink=True)
        # A small configured ceiling tests the same bounded decoder without a huge fixture.
        with patch.object(v, "MAX_TAR", 1024), self.assertRaises(ValueError):
            v.object_files(self.manifest, self.archive, self.verifier)

    def test_gzip_bounds(self):
        with patch.object(v, "MAX_FILE", 32), self.assertRaises(ValueError):
            v.bounded_gunzip(gzip.compress(b"x" * 33))
        for raw in (gzip.compress(b"x") + b"trailing", gzip.compress(b"x")[:-1]):
            with self.assertRaises(ValueError):
                v.bounded_gunzip(raw)

    def test_summary_corruption_after_rebinding(self):
        stem = v.RUNS["candidate"]
        driver_key = stem + ".driver.json"
        accept_key = stem + "-acceptance.json"
        original_driver = v.read_json(self.files, driver_key)
        summary_key = v.normalized(original_driver["summaries"][0]["path"])
        mutations = (
            lambda s: s["records"].pop(),
            lambda s: s["records"][1].update(query=1),
            lambda s: s.update(complete=False),
            lambda s: s.update(completed_identity_check=False),
            lambda s: s.update(reference_packet_identity_verified=False),
            lambda s: s.update(harness_identity_verified=False),
            lambda s: s.update(repeats=2),
            lambda s: s.update(binary_sha256="0" * 64),
            lambda s: s["records"][0].update(engine_execution_seconds=9),
            lambda s: s["records"][0].update(result_sha256="0" * 64),
        )
        for index, mutate in enumerate(mutations):
            files = dict(self.files)
            driver = copy.deepcopy(original_driver)
            summary = v.read_json(files, summary_key)
            accept = v.read_json(files, accept_key)
            mutate(summary)
            files[summary_key] = json.dumps(summary).encode()
            driver["summaries"][0].update(bytes=len(files[summary_key]), sha256=v.sha(files[summary_key]))
            files[driver_key] = json.dumps(driver).encode()
            accept["outer"].update(bytes=len(files[driver_key]), sha256=v.sha(files[driver_key]))
            accept["summary"] = driver["summaries"][0]
            files[accept_key] = json.dumps(accept).encode()
            with self.subTest(index=index), self.assertRaises(ValueError):
                v.verify_evidence(files)

    def test_result_payload_contract(self):
        driver = v.read_json(self.files, v.RUNS["candidate"] + ".driver.json")
        summary = v.read_json(self.files, driver["summaries"][0]["path"])
        path = v.normalized(summary["records"][0]["completed_log_archives"][0]["path"])
        original = v.strict_json(v.bounded_gunzip(self.files[path]))
        for key, value in (("output_row_count", "2"), ("native_query_execution_nanos", "-1"),
                           ("result_payload_complete", "false"),
                           ("public_workflow_external_engine_invoked", "true")):
            output = copy.deepcopy(original)
            for field in output["fields"]:
                if field["key"] == key:
                    field["value"] = value
            with self.subTest(key=key), self.assertRaises(ValueError):
                v.report_fields(output)
        original["fallback"]["attempted"] = True
        with self.assertRaises(ValueError):
            v.report_fields(original)

    def test_receipt_corruption(self):
        for key, changes in (
            ("checkpoint-full43-r1-acceptance.json", {"more_than_10_percent_regression": False}),
            ("runtime-restoration-r1.json", {"no_candidate_runtime_changes_remain": False}),
            ("restored-main-full43-r1-acceptance.json", {"fresh_latest_main_executable": False}),
        ):
            files = dict(self.files)
            record = v.read_json(files, key)
            record.update(changes)
            files[key] = json.dumps(record).encode()
            with self.subTest(key=key), self.assertRaises(ValueError):
                v.verify_evidence(files)


if __name__ == "__main__":
    unittest.main()
