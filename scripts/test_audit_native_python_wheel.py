"""Exercise the Linux audit's payload and failed-process boundaries."""

import base64
import csv
import hashlib
import io
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import zipfile

import audit_native_python_wheel as subject
from check_native_python_wheel import AUDITED_LINUX_PLATFORM, AUDITWHEEL_VERSION, verify
import test_check_native_python_wheel as fixtures


def retag(original, directory, mutation=None):
    with zipfile.ZipFile(original) as archive:
        files = {name: archive.read(name) for name in archive.namelist()}
    record = next(name for name in files if name.endswith(".dist-info/RECORD"))
    metadata = record.removesuffix("RECORD") + "WHEEL"
    files[metadata] = files[metadata].replace(b"linux_x86_64", AUDITED_LINUX_PLATFORM.encode())
    if mutation:
        mutation(files)
    lines = io.StringIO()
    writer = csv.writer(lines, lineterminator="\n")
    for name, data in files.items():
        if name != record:
            digest = base64.urlsafe_b64encode(hashlib.sha256(data).digest()).decode().rstrip("=")
            writer.writerow([name, "sha256=" + digest, len(data)])
    writer.writerow([record, "", ""])
    files[record] = lines.getvalue().encode()
    output = directory / (original.name.rsplit("-", 1)[0] + f"-{AUDITED_LINUX_PLATFORM}.whl")
    with zipfile.ZipFile(output, "w") as archive:
        for name, data in files.items():
            archive.writestr(name, data)
    return output


class NativePythonLinuxAuditTests(unittest.TestCase):
    def fixture(self, root):
        return fixtures.NativePythonWheelIntegrityTests().fixture(root)

    def test_success_requires_pinned_auditor_and_preserves_every_payload(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            original, library, binary = self.fixture(root)
            commands = []

            def run(name, command):
                commands.append((name, command))
                retag(original, root / "audited")

            with patch.object(subject.sys, "platform", "linux"), patch.object(
                    subject, "version", return_value=AUDITWHEEL_VERSION):
                wheel, receipt = subject.audit(original, root / "audited", run)
            command = [str(part) for part in commands[0][1]]
            self.assertEqual(command[1:9], ["-m", "auditwheel", "repair", "--only-plat",
                             "--plat", AUDITED_LINUX_PLATFORM, "--patcher", "none"])
            self.assertEqual(len(commands), 1)
            self.assertTrue(receipt["payloads_unchanged"])
            self.assertGreater(receipt["unchanged_payload_count"], 2)
            with patch("check_native_python_wheel.verify_notices", return_value={"status": "passed"}):
                self.assertTrue(verify(root, wheel, library, binary,
                                       linux_audit=receipt)["linux_audit_verified"])
                for invalid in (None, {**receipt, "status": "failed"},
                                {**receipt, "wheel_sha256": "wrong"},
                                {**receipt, "payloads_unchanged": False}):
                    with self.subTest(receipt=invalid), self.assertRaises(ValueError):
                        verify(root, wheel, library, binary, linux_audit=invalid)

    def test_valid_records_cannot_hide_changed_or_added_payloads(self):
        mutations = {
            "library": lambda files: files.__setitem__("shardloom/_native.abi3.so", b"changed"),
            "source": lambda files: files.__setitem__("shardloom/__init__.py", b"changed"),
            "new library": lambda files: files.__setitem__("shardloom.libs/extra.so", b"new"),
            "removed source": lambda files: files.pop("shardloom/__init__.py"),
        }
        for label, mutation in mutations.items():
            with self.subTest(mutation=label), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                original, _, _ = self.fixture(root)
                audited = retag(original, root, mutation)
                with self.assertRaises(ValueError):
                    subject.verify_payload_preservation(original, audited)

    def test_changed_package_or_policy_tag_is_rejected(self):
        for old, new in (("0.5.1", "0.6.0"), ("manylinux_2_39", "manylinux_2_28"),
                         ("cp310", "cp313")):
            with self.subTest(change=new), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                original, _, _ = self.fixture(root)
                audited = retag(original, root)
                changed = audited.with_name(audited.name.replace(old, new))
                audited.rename(changed)
                with self.assertRaises(ValueError):
                    subject.verify_payload_preservation(original, changed)

    def test_audit_failure_cannot_produce_passed_receipt(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            original, _, _ = self.fixture(root)
            with patch.object(subject.sys, "platform", "linux"), patch.object(
                    subject, "version", return_value=AUDITWHEEL_VERSION):
                def failed(name, command):
                    retag(original, root / "audited")
                    raise RuntimeError("audit rejected linked libraries")
                with self.assertRaisesRegex(RuntimeError, "audit rejected"):
                    subject.audit(original, root / "audited", failed)

    def test_wrong_auditor_version_is_rejected_before_running(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            original, _, _ = self.fixture(root)
            with patch.object(subject.sys, "platform", "linux"), patch.object(
                    subject, "version", return_value="0.0.0"):
                with self.assertRaisesRegex(ValueError, "hash-pinned"):
                    subject.audit(original, root / "audited", lambda *args: self.fail("ran audit"))

    def test_zero_or_multiple_outputs_and_mutated_input_are_rejected(self):
        for action in ("none", "extra", "mutate"):
            with self.subTest(action=action), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                original, _, _ = self.fixture(root)
                def run(name, command):
                    if action != "none":
                        retag(original, root / "audited")
                    if action == "extra":
                        (root / "audited/extra.whl").write_bytes(b"unexpected")
                    if action == "mutate":
                        original.write_bytes(b"changed input")
                with patch.object(subject.sys, "platform", "linux"), patch.object(
                        subject, "version", return_value=AUDITWHEEL_VERSION):
                    with self.assertRaises(ValueError):
                        subject.audit(original, root / "audited", run)


if __name__ == "__main__":
    unittest.main()
