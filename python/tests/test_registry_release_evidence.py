# SPDX-License-Identifier: Apache-2.0
"""Pure fixture checks; no network, native binaries or package execution."""
import io
import copy
import json
import os
from pathlib import Path
import shutil
import sys
import tempfile
import unittest
from unittest.mock import patch
import zipfile

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))
import registry_release_evidence as evidence


def native_fixture(linux=False, package_version=evidence.VERSION):
    """Inert report/log fixture. No command or module in this fixture runs."""
    if package_version not in evidence.NATIVE_PROOF_EXPECTATIONS:
        raise ValueError("unsupported native proof fixture version")
    expected = evidence.NATIVE_PROOF_EXPECTATIONS[package_version]
    kind = "python-dist-linux" if linux else "python-dist-macos"
    filename, platform, _ = evidence.KINDS[kind]
    filename = filename.replace(evidence.VERSION, package_version)
    native_sha, cli_sha = "b" * 64, "c" * 64
    distribution = {"filename": filename, "sha256": "a" * 64, "size_bytes": 900,
        "wheel_tag": filename[len("shardloom-" + package_version + "-"):-4], "url": "https://example.invalid/" + filename,
        "native_library": {"member": "shardloom/_native.abi3.so", "sha256": native_sha,
                           "size_bytes": 64, "python_abi": "cp310-abi3"},
        "bundled_cli": {"member": "shardloom/bin/" + platform + "/shardloom",
                        "sha256": cli_sha, "size_bytes": 80, "platform": platform}}
    integration = {"tests": expected["tests"], "failures": 0, "errors": 0, "skipped": 0, "status": "passed"}
    installed = {"version": package_version, "isolated_install": True,
                 "native_library": "/env/site/shardloom/_native.abi3.so",
                 "cli": "/env/site/shardloom/bin/" + platform + "/shardloom"}
    programs = {"IMPORT_CHECK": "inert import program", "RUN_TESTS": "inert test program"}
    commands = {name: ["/env/bin/python", name] for name in (
        "build", "create_environment", "install_optional_consumer")}
    commands.update({
        "install": ["/env/bin/python", "-I", "-m", "pip", "install", "--no-index", "--no-deps", "/dist/" + filename],
        "load": ["/env/bin/python", "-I", "-c", programs["IMPORT_CHECK"], native_sha, cli_sha],
        "integration": ["/env/bin/python", "-I", "-c", programs["RUN_TESTS"], "/source/python/tests", "/proof/integration.json"],
        "uninstall": ["/env/bin/python", "-I", "-m", "pip", "uninstall", "--yes", "shardloom"],
        "verify_uninstalled": ["/env/bin/python", "-I", "-c", "import importlib.util, pathlib, sys; "
            "assert importlib.util.find_spec('shardloom') is None; "
            "assert all(not pathlib.Path(path).exists() for path in sys.argv[1:])",
            installed["native_library"], installed["cli"]],
    })
    if linux:
        commands["linux_compatibility_audit"] = ["python", "-m", "auditwheel", "repair", "--only-plat", "--plat",
            "manylinux_2_39_x86_64", "--patcher", "none", "--wheel-dir", "/dist", "/unaudited/wheel.whl"]
    files = {"proof/" + name + ".log": (name + " inert fixture\n").encode() for name in commands}
    files["proof/load.log"] = evidence.json_bytes(installed)
    proof = {"schema_version": "shardloom.native_python_wheel_proof.v1", "status": "passed",
        "uninstall_status": "passed", "publication_attempted": False, "performance_claim": False,
        "integration": integration, "installed_load": installed,
        "stage": {"status": "passed", "native_library_sha256": native_sha, "cli_sha256": cli_sha},
        "integrity": {"status": "passed", "wheel": filename, "wheel_sha256": distribution["sha256"],
            "wheel_bytes": 900, "tag": distribution["wheel_tag"], "native_library_sha256": native_sha,
            "cli_sha256": cli_sha, "linux_audit_verified": linux},
        "steps": [{"name": name, "command": command, "log": name + ".log", "returncode": 0,
                   "log_sha256": evidence.sha_bytes(files["proof/" + name + ".log"])}
                  for name, command in commands.items()]}
    if linux:
        proof["linux_audit"] = {"status": "passed", "auditwheel_version": "6.8.2",
            "platform": "manylinux_2_39_x86_64", "payloads_unchanged": True,
            "unchanged_payload_count": expected["linux_payloads"], "wheel_sha256": distribution["sha256"]}
    build = {"schema_version": "shardloom.native_python_build.v1", "status": "passed",
        "package_version": package_version, "python_abi": "cp310-abi3", "library_sha256": native_sha,
        "library_bytes": 64, "command": ["cargo", "build", "--locked", "-p", "shardloom-python",
            "--features", "python-native", "--message-format=json-render-diagnostics", "--release"]}
    files.update({"native-build.json": evidence.json_bytes(build),
                  "proof/report.json": evidence.json_bytes(proof),
                  "proof/integration.json": evidence.json_bytes(integration)})
    return files, distribution, programs


class RegistryReleaseEvidenceTests(unittest.TestCase):
    @unittest.skipUnless(os.name == "posix" and shutil.which("cargo"), "POSIX Cargo metadata fixture")
    def test_observer_uses_configured_external_cargo_target(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory).resolve()
            root, target = base / "Documents/repo", base / "local-cache"
            (root / "src").mkdir(parents=True)
            (root / "src/lib.rs").write_text("")
            (root / "Cargo.toml").write_text('[package]\nname="metadata-fixture"\nversion="0.0.0"\nedition="2021"\n')
            with patch.dict(os.environ, {"CARGO_TARGET_DIR": str(target)}), \
                    patch.object(evidence, "available_bytes", return_value=100 << 30):
                observer = evidence.Observer(root)
            self.assertEqual(observer.target, target)
            self.assertEqual(observer.directory.parent, target)
            self.assertFalse((root / "target").exists())
            self.assertEqual(observer.local_ref(observer.directory / "capture.json"), str(observer.directory / "capture.json"))
            self.assertEqual(observer.local_ref(root / "docs/proof.json"), "docs/proof.json")

    @unittest.skipUnless(os.name == "posix" and shutil.which("cargo"), "POSIX Cargo metadata fixture")
    def test_observer_rejects_synced_cargo_target_before_workspace_creation(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory).resolve()
            root, target = base / "repo", base / "Documents/cargo-target"
            (root / "src").mkdir(parents=True)
            (root / "src/lib.rs").write_text("")
            (root / "Cargo.toml").write_text('[package]\nname="metadata-fixture"\nversion="0.0.0"\nedition="2021"\n')
            with patch.dict(os.environ, {"CARGO_TARGET_DIR": str(target)}), \
                    patch.object(evidence.Path, "home", return_value=base), \
                    patch.object(evidence.sys, "platform", "darwin"):
                with self.assertRaisesRegex(ValueError, "cloud-managed"):
                    evidence.Observer(root)
            self.assertFalse(target.exists())

    @unittest.skipUnless(os.name == "posix" and shutil.which("cargo"), "POSIX Cargo metadata fixture")
    def test_failed_metadata_does_not_start_observation(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            with self.assertRaisesRegex(ValueError, "offline Cargo metadata failed"):
                evidence.Observer(root)
            self.assertFalse((root / "target").exists())

    def test_stream_digest_and_bound(self):
        self.assertEqual(evidence.stream_sha(io.BytesIO(b"fixture"), 7),
                         (evidence.sha_bytes(b"fixture"), 7))
        with self.assertRaises(ValueError):
            evidence.stream_sha(io.BytesIO(b"oversized"), 3)

    def test_archive_path_and_metadata_rejection(self):
        for path in ("../escape", "/absolute", "x/../escape", "x\\escape"):
            with self.subTest(path=path), self.assertRaises(ValueError):
                evidence.safe_member(path)
        self.assertEqual(evidence.package_metadata(f"Name: shardloom\nVersion: {evidence.VERSION}\n\n".encode())["version"], evidence.VERSION)
        for raw in (f"Name: other\nVersion: {evidence.VERSION}\n".encode(), b"Name: shardloom\nVersion: 0.2.3\n"):
            with self.assertRaises(ValueError):
                evidence.package_metadata(raw)

    def test_complete_registry_inventory_rejects_digest_url_and_size_drift(self):
        rows = [{"filename": item[0], "sha256": "a" * 64, "size": 8, "url": "https://example.invalid/" + item[0]}
                for item in evidence.KINDS.values()]
        proof = {"schema_version": "shardloom.python_registry_package_proof.v1", "proof_status": "passed",
                 "package_version": evidence.VERSION, "channel_id": "testpypi", "fallback_attempted": False,
                 "external_engine_invoked": False, "registry_release_artifacts": rows}
        def live():
            return {"urls": [dict(row, digests={"sha256": row["sha256"]}) for row in rows]}
        self.assertEqual(len(evidence.validate_inventory(proof, live(), "testpypi")), 4)
        for key, value in (("size", 9), ("url", "different"), ("digests", {"sha256": "b" * 64})):
            changed = live(); changed["urls"][0][key] = value
            with self.subTest(key=key), self.assertRaises(ValueError):
                evidence.validate_inventory(proof, changed, "testpypi")

    def test_wheel_exact_platform_cli_and_metadata(self):
        filename, platform, binary = evidence.KINDS["python-dist-macos"]
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / filename
            def write(extra=False, version=evidence.VERSION, relocated=False, native=True, extra_native=False):
                with zipfile.ZipFile(path, "w") as archive:
                    archive.writestr(f"shardloom-{evidence.VERSION}.dist-info/METADATA", f"Name: shardloom\nVersion: {version}\n\n")
                    archive.writestr(f"shardloom-{evidence.VERSION}.dist-info/WHEEL", "Root-Is-Purelib: false\nTag: cp310-abi3-macosx_26_0_arm64\n\n")
                    prefix = f"shardloom-{evidence.VERSION}.data/purelib/" if relocated else ""
                    archive.writestr(prefix + f"shardloom/bin/{platform}/{binary}", b"inert fixture, never executable")
                    if native:
                        archive.writestr("shardloom/_native.abi3.so", b"inert native module fixture")
                    if extra_native:
                        archive.writestr("shardloom/_other.so", b"inert extra native module")
                    if extra:
                        archive.writestr("shardloom/bin/other/shardloom", b"unexpected")
            write()
            result = evidence.inspect_distribution(path, "python-dist-macos", b"")
            self.assertEqual(result["bundled_cli"]["sha256"], evidence.sha_bytes(b"inert fixture, never executable"))
            self.assertEqual(result["native_library"]["sha256"], evidence.sha_bytes(b"inert native module fixture"))
            write(relocated=True)
            self.assertTrue(evidence.inspect_distribution(path, "python-dist-macos", b"")["bundled_cli"]["member"].startswith(f"shardloom-{evidence.VERSION}.data/purelib/"))
            write(extra=True)
            with self.assertRaises(ValueError):
                evidence.inspect_distribution(path, "python-dist-macos", b"")
            for changes in ({"version": "0.2.3"}, {"native": False}, {"extra_native": True}):
                write(**changes)
                with self.subTest(changes=changes), self.assertRaises(ValueError):
                    evidence.inspect_distribution(path, "python-dist-macos", b"")

    def test_windows_remains_cli_only(self):
        filename, platform, binary = evidence.KINDS["python-dist-windows"]
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / filename
            for with_native in (False, True):
                with zipfile.ZipFile(path, "w") as archive:
                    archive.writestr(f"shardloom-{evidence.VERSION}.dist-info/METADATA", f"Name: shardloom\nVersion: {evidence.VERSION}\n\n")
                    archive.writestr(f"shardloom-{evidence.VERSION}.dist-info/WHEEL", "Root-Is-Purelib: false\nTag: cp313-cp313-win_amd64\n\n")
                    archive.writestr(f"shardloom/bin/{platform}/{binary}", b"inert Windows CLI fixture")
                    if with_native:
                        archive.writestr("shardloom/_native.pyd", b"inert unexpected module")
                if with_native:
                    with self.assertRaisesRegex(ValueError, "Windows CLI package"):
                        evidence.inspect_distribution(path, "python-dist-windows", b"")
                else:
                    self.assertIsNone(evidence.inspect_distribution(path, "python-dist-windows", b"")["native_library"])

    def test_native_install_evidence_requires_exact_wheel_module_suite_logs_and_programs(self):
        for linux in (False, True):
            files, distribution, programs = native_fixture(linux)
            result = evidence.verify_native_proof(files, distribution, programs)
            self.assertEqual(result["integration"]["tests"], evidence.NATIVE_PROOF_EXPECTATIONS[evidence.VERSION]["tests"])
            self.assertEqual(result["native_library_sha256"], distribution["native_library"]["sha256"])
            self.assertFalse(result["fresh_execution_performed_by_observer"])
            self.assertEqual({row["name"] for row in result["original_files"]}, set(files))
            for mutation in ("wheel", "native", "cli", "suite", "skip", "failed_step", "log", "load_code",
                             "suite_code", "install_wheel", "uninstall_code", "missing", "extra", "build", "load_output",
                             "bool_returncode", "bool_summary_count", "bool_result_count"):
                changed = copy.deepcopy(files)
                proof = json.loads(changed["proof/report.json"])
                steps = {row["name"]: row for row in proof["steps"]}
                if mutation == "wheel": proof["integrity"]["wheel_sha256"] = "d" * 64
                elif mutation == "native": proof["integrity"]["native_library_sha256"] = "d" * 64
                elif mutation == "cli": proof["integrity"]["cli_sha256"] = "d" * 64
                elif mutation == "suite": proof["integration"]["tests"] = 1
                elif mutation == "skip": proof["integration"]["skipped"] = 1
                elif mutation == "failed_step": steps["uninstall"]["returncode"] = 1
                elif mutation == "bool_returncode": steps["uninstall"]["returncode"] = False
                elif mutation == "bool_summary_count": proof["integration"]["failures"] = False
                elif mutation == "bool_result_count":
                    integration = json.loads(changed["proof/integration.json"])
                    integration["skipped"] = False
                    changed["proof/integration.json"] = evidence.json_bytes(integration)
                elif mutation == "log": changed["proof/integration.log"] += b"changed"
                elif mutation == "load_code": steps["load"]["command"][3] = "changed"
                elif mutation == "suite_code": steps["integration"]["command"][3] = "changed"
                elif mutation == "install_wheel": steps["install"]["command"][-1] = "/other.whl"
                elif mutation == "uninstall_code": steps["verify_uninstalled"]["command"][3] = "pass"
                elif mutation == "missing": del changed["proof/integration.json"]
                elif mutation == "extra": changed["proof/unknown.log"] = b"unexpected"
                elif mutation == "build":
                    build = json.loads(changed["native-build.json"]); build["command"].remove("--release")
                    changed["native-build.json"] = evidence.json_bytes(build)
                elif mutation == "load_output":
                    changed["proof/load.log"] = evidence.json_bytes(dict(proof["installed_load"], isolated_install=False))
                    steps["load"]["log_sha256"] = evidence.sha_bytes(changed["proof/load.log"])
                changed["proof/report.json"] = evidence.json_bytes(proof)
                with self.subTest(linux=linux, mutation=mutation), self.assertRaises(ValueError):
                    evidence.verify_native_proof(changed, distribution, programs)
        files, distribution, programs = native_fixture(True)
        proof = json.loads(files["proof/report.json"])
        proof["linux_audit"]["payloads_unchanged"] = False
        files["proof/report.json"] = evidence.json_bytes(proof)
        with self.assertRaisesRegex(ValueError, "portability"):
            evidence.verify_native_proof(files, distribution, programs)

    def test_native_module_sbom_has_distinct_hash_and_dependency_edge(self):
        _, distribution, _ = native_fixture()
        bom = evidence.cyclonedx("testpypi", [distribution], [], "2026-10-15T00:00:00Z")
        components = {row["bom-ref"]: row for row in bom["components"]}
        ref = "sha256:" + distribution["sha256"]
        self.assertEqual(set(components), {ref, ref + ":bundled-cli", ref + ":native-library"})
        self.assertEqual(components[ref + ":native-library"]["hashes"][0]["content"], distribution["native_library"]["sha256"])
        self.assertEqual(bom["dependencies"], [{"ref": ref, "dependsOn": [ref + ":bundled-cli", ref + ":native-library"]}])

    def test_native_proof_programs_require_complete_literal_program_set(self):
        self.assertEqual(evidence.native_proof_programs('IMPORT_CHECK="load"\nRUN_TESTS="tests"\n'),
                         {"IMPORT_CHECK": "load", "RUN_TESTS": "tests"})
        for source in ('IMPORT_CHECK="load"\n', 'IMPORT_CHECK="a"\nIMPORT_CHECK="b"\nRUN_TESTS="c"\n'):
            with self.assertRaises(ValueError):
                evidence.native_proof_programs(source)


if __name__ == "__main__":
    unittest.main()
