"""Reject stale or altered payloads in an otherwise well-formed native wheel."""

import base64
import csv
import hashlib
import io
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import zipfile

from check_native_python_wheel import verify, verify_record
from release_dry_run_proof import bundled_cli_platform_tag


class NativePythonWheelIntegrityTests(unittest.TestCase):
    def fixture(self, root, mutation=None):
        (root / "Cargo.toml").write_text('[workspace.package]\nversion = "0.5.1"\n')
        sources = root / "python/src/shardloom"
        sources.mkdir(parents=True)
        (sources / "__init__.py").write_text('__version__ = "0.5.1"\n')
        legal = root / "python/THIRD_PARTY_LICENSES/texts"
        legal.mkdir(parents=True)
        for relative, content in (("LICENSE", "project license"), ("NOTICE", "project notice"),
                                  ("THIRD_PARTY_LICENSES/manifest.json", "{}"),
                                  ("THIRD_PARTY_LICENSES/texts/fixture.txt", "dependency license")):
            (root / "python" / relative).write_text(content)
        library, binary = root / "built.so", root / "shardloom"
        library.write_bytes(b"native library fixture")
        binary.write_bytes(b"native CLI fixture")
        dist = "shardloom-0.5.1.dist-info"
        files = {"shardloom/__init__.py": (sources / "__init__.py").read_bytes(),
                 "shardloom/_native.abi3.so": library.read_bytes(),
                 f"shardloom/bin/{bundled_cli_platform_tag()}/shardloom": binary.read_bytes(),
                 f"{dist}/WHEEL": b"Wheel-Version: 1.0\nRoot-Is-Purelib: false\nTag: cp310-abi3-linux_x86_64\n"}
        metadata = "Metadata-Version: 2.4\nName: shardloom\nVersion: 0.5.1\n"
        for name in ("LICENSE", "NOTICE", "THIRD_PARTY_LICENSES/manifest.json",
                     "THIRD_PARTY_LICENSES/texts/fixture.txt"):
            files[f"{dist}/licenses/{name}"] = (root / "python" / name).read_bytes()
            metadata += f"License-File: {name}\n"
        files[f"{dist}/METADATA"] = metadata.encode()
        if mutation is not None:
            mutation(files)
        record = f"{dist}/RECORD"
        lines = io.StringIO()
        writer = csv.writer(lines, lineterminator="\n")
        for name, value in files.items():
            encoded = base64.urlsafe_b64encode(hashlib.sha256(value).digest()).decode().rstrip("=")
            writer.writerow([name, "sha256=" + encoded, len(value)])
        writer.writerow([record, "", ""])
        files[record] = lines.getvalue().encode()
        wheel = root / "shardloom-0.5.1-cp310-abi3-linux_x86_64.whl"
        with zipfile.ZipFile(wheel, "w") as archive:
            for name, value in files.items():
                archive.writestr(name, value)
        return wheel, library, binary

    def check(self, root, artifacts):
        # Notice content admission has its own real-bundle tests. Here the trust
        # boundary is transfer of those admitted bytes into a wheel archive.
        with patch("check_native_python_wheel.verify_notices", return_value={"status": "passed"}):
            return verify(root, *artifacts)

    def test_complete_archive_matches_exact_artifacts_sources_and_notices(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            report = self.check(root, self.fixture(root))
            self.assertEqual(report["status"], "passed")
            self.assertFalse(report["load_verified"])
            self.assertFalse(report["published"])

    def test_valid_records_do_not_certify_stale_artifacts_sources_or_notices(self):
        paths = ("shardloom/_native.abi3.so",
                 f"shardloom/bin/{bundled_cli_platform_tag()}/shardloom",
                 "shardloom/__init__.py", "shardloom-0.5.1.dist-info/licenses/NOTICE")
        for name in paths:
            with self.subTest(payload=name), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                artifacts = self.fixture(root, lambda files: files.__setitem__(name, b"stale payload"))
                with self.assertRaises(ValueError):
                    self.check(root, artifacts)

    def test_record_rejects_unrecorded_modified_and_duplicate_payload_declarations(self):
        for record in ("RECORD,,\n", "data,sha256=wrong,5\nRECORD,,\n",
                       "data,sha256=wrong,5\ndata,sha256=wrong,5\nRECORD,,\n"):
            with self.subTest(record=record), tempfile.TemporaryDirectory() as directory:
                path = Path(directory) / "fixture.whl"
                with zipfile.ZipFile(path, "w") as archive:
                    archive.writestr("data", b"value")
                    archive.writestr("RECORD", record)
                with zipfile.ZipFile(path) as archive, self.assertRaises(ValueError):
                    verify_record(archive, "RECORD")


if __name__ == "__main__":
    unittest.main()
