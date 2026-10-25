"""Tests for sealed native package production, consumption, and proof aggregation."""
from __future__ import annotations

from contextlib import ExitStack
import json
from pathlib import Path
import platform
import sys
import tempfile
import unittest
from unittest.mock import patch

import ci_build_artifact as artifact
import ci_native_package as package


class NativePackageTests(unittest.TestCase):
    revision = "a" * 40
    rustc = "rustc 1.99.0 (fixture)"
    cargo = "cargo 1.99.0 (fixture)"

    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name) / "checkout"
        self.root.mkdir()
        (self.root / "Cargo.lock").write_text("fixture lock\n")
        self.system = platform.system()
        self.architecture = platform.machine()
        self.identity = {
            "revision": self.revision,
            "cargo_lock_sha256": artifact.sha(self.root / "Cargo.lock"),
            "os": self.system,
            "architecture": self.architecture,
            "workspace_root": str(self.root.resolve()),
            "rustc": self.rustc,
            "cargo": self.cargo,
        }
        self.consumer_case = 0
        self.last_consumer_output: Path | None = None
        self.sources = self.root / "sources"
        self.sources.mkdir()

    def current_command(self, command: list[str], root: Path = artifact.ROOT) -> str:
        if command[:3] == ["git", "rev-parse", "HEAD"]:
            return self.revision
        if command == ["rustc", "-vV"]:
            return self.rustc
        if command == ["cargo", "--version"]:
            return self.cargo
        raise AssertionError(f"unexpected command in fixture: {command!r}")

    def fixture_patches(self) -> ExitStack:
        stack = ExitStack()
        identity_for = lambda root=artifact.ROOT: {
            **self.identity, "workspace_root": str(Path(root).resolve())
        }
        stack.enter_context(patch.object(artifact, "identity", side_effect=identity_for))
        stack.enter_context(patch.object(package, "identity", side_effect=identity_for))
        stack.enter_context(patch.object(artifact, "command_output", side_effect=self.current_command))
        stack.enter_context(patch.object(artifact.platform, "system", return_value=self.system))
        stack.enter_context(patch.object(artifact.platform, "machine", return_value=self.architecture))
        return stack

    def make_bundle(self, product: str, destination: Path) -> dict:
        destination.parent.mkdir(parents=True, exist_ok=True)
        name = "_native.abi3.so" if "extension" in product else "shardloom"
        source = self.sources / f"{product}-{len(list(self.sources.iterdir()))}"
        source.write_bytes(f"fixture payload for {product}".encode())
        if product in {"cli-release", "cli-debug"}:
            profile = {"opt_level": "3" if product == "cli-release" else "0",
                       "debug_assertions": product == "cli-debug",
                       "overflow_checks": product == "cli-debug", "test": False}
        else:
            profile = {"opt_level": "3", "debug_assertions": False,
                       "overflow_checks": False, "test": False}
        with patch.object(artifact, "identity", return_value=self.identity.copy()):
            return artifact.record_files(
                destination, product, {name: source}, root=self.root,
                build={"command": ["fixture", product], "compiler_features": artifact.PRODUCTS[product][3],
                       "compiler_profile": profile},
            )

    def make_package(self, destination: Path) -> dict:
        bundle_manifests = {}
        for product, directory in (("cli-release", "cli"), ("native-extension", "extension"),
                                   ("observation-extension", "observation")):
            self.make_bundle(product, destination / directory)
            bundle_manifests[directory] = artifact.sha(destination / directory / "manifest.json")
        wheel_dir = destination / "wheel-build/dist"
        wheel_dir.mkdir(parents=True)
        wheel = wheel_dir / "shardloom_fixture_cp310-abi3-manylinux.whl"
        wheel.write_bytes(b"one canonical wheel")
        report = destination / "wheel-build/report.json"
        report.write_text(json.dumps({"status": "built", "installation_performed": False}))
        manifest = {
            "schema_version": "shardloom.ci_native_package.v1",
            "status": "built",
            "identity": self.identity.copy(),
            "wheel": wheel.name,
            "wheel_sha256": artifact.sha(wheel),
            "installed": False,
            "build_report_sha256": artifact.sha(report),
            "bundle_manifests": bundle_manifests,
        }
        artifact.write_json(destination / "manifest.json", manifest)
        return manifest

    def test_producer_reuses_release_cli_and_seals_distinct_products(self) -> None:
        cli = self.root / "existing-cli"
        with self.fixture_patches():
            self.make_bundle("cli-release", cli)
            events: list[str] = []

            def build_mock(product: str, destination: Path, *, root: Path) -> dict:
                events.append(f"build:{product}")
                return self.make_bundle(product, destination)

            def prove_mock(root: Path, output: Path, library: Path, binary: Path,
                           wheelhouse: Path, **kwargs: object) -> dict:
                events.append("prove:build-only")
                self.assertTrue(kwargs["build_only"])
                self.assertEqual(library, output.parent / "extension/_native.abi3.so")
                self.assertEqual(binary, output.parent / "cli/shardloom")
                dist = output / "dist"
                dist.mkdir(parents=True)
                (dist / "shardloom_fixture_cp310-abi3-manylinux.whl").write_bytes(b"wheel")
                (output / "report.json").write_text(json.dumps(
                    {"status": "built", "installation_performed": False}))
                return {"status": "built", "installation_performed": False}

            with patch.object(package, "build", side_effect=build_mock), \
                    patch.object(package, "prove", side_effect=prove_mock):
                result = package.build_package(self.root / "package", cli_bundle=cli, root=self.root)

        self.assertEqual(events, ["build:native-extension", "prove:build-only",
                                  "build:observation-extension"])
        self.assertEqual(result["status"], "built")
        self.assertFalse(result["installed"])
        self.assertEqual(result["identity"], self.identity)
        self.assertEqual(result["wheel"], "shardloom_fixture_cp310-abi3-manylinux.whl")
        self.assertEqual(result["wheel_sha256"], artifact.sha(
            self.root / "package/wheel-build/dist" / result["wheel"]))
        self.assertEqual(result["build_report_sha256"],
                         artifact.sha(self.root / "package/wheel-build/report.json"))
        self.assertEqual(len(list((self.root / "package/wheel-build/dist").glob("*.whl"))), 1)
        self.assertEqual(set(result["bundle_manifests"]), {"cli", "extension", "observation"})
        for directory, digest in result["bundle_manifests"].items():
            self.assertEqual(digest, artifact.sha(self.root / "package" / directory / "manifest.json"))
        self.assertTrue((self.root / "package/observation/_native.abi3.so").is_file())

    def test_consumer_passes_exact_canonical_wheel_and_report_and_emits_provenance(self) -> None:
        output = self.root / "proof-output"
        package_root = self.root / "package"
        self.make_package(package_root)
        wheel = next((package_root / "wheel-build/dist").glob("*.whl"))
        report = package_root / "wheel-build/report.json"
        wheelhouse = self.root / "wheelhouse"
        wheelhouse.mkdir()
        def prove_mock(root: Path, proof_dir: Path, library: Path, binary: Path,
                       passed_wheelhouse: Path, **kwargs: object) -> dict:
            self.assertEqual(root, self.root)
            self.assertEqual(library, package_root / "extension/_native.abi3.so")
            self.assertEqual(binary, package_root / "cli/shardloom")
            self.assertEqual(passed_wheelhouse, wheelhouse)
            self.assertEqual(kwargs, {"audit_linux": True, "existing_wheel": wheel,
                                      "build_report": report})
            return {"status": "passed", "installation_performed": True,
                    "uninstall_status": "passed",
                    "integrity": {"wheel_sha256": artifact.sha(wheel)},
                    "integration": {"status": "passed", "tests": 4, "failures": 0,
                                    "errors": 0, "skipped": 0}}

        def run_mock(command: list[str], *, cwd: Path, stdout: object, stderr: object,
                     check: bool, timeout: int) -> None:
            self.assertEqual(command, ["fake-observation-runner"])
            self.assertEqual(cwd, self.root)
            self.assertTrue(check)
            (output / "output-window-tests.json").write_text(json.dumps(
                {"status": "passed", "tests": 13, "failures": 0, "errors": 0, "skipped": 0}))

        with self.fixture_patches(), patch.object(package, "prove", side_effect=prove_mock), \
                patch.object(package, "observation_command", return_value=["fake-observation-runner"]), \
                patch.object(package.subprocess, "run", side_effect=run_mock):
            result = package.test_package(package_root, output, wheelhouse, root=self.root)

        self.assertEqual(result["status"], "passed")
        self.assertEqual(result["wheel_sha256"], artifact.sha(wheel))
        self.assertEqual(result["os"], self.system)
        self.assertEqual(result["architecture"], self.architecture)
        self.assertEqual(result["python_version"], f"{sys_version_major()}.{sys_version_minor()}")
        self.assertEqual(result["package_manifest_sha256"], artifact.sha(package_root / "manifest.json"))
        self.assertEqual(result["ordinary_extension_sha256"],
                         artifact.sha(package_root / "extension/_native.abi3.so"))
        self.assertEqual(result["observation_extension_sha256"],
                         artifact.sha(package_root / "observation/_native.abi3.so"))
        self.assertEqual(result["source_revision"], self.revision)

    def run_consumer(self, mutate: object | None = None, *, preexisting_output: bool = False,
                     prove_result: dict | None = None, observation: dict | None = None) -> None:
        self.consumer_case += 1
        case_root = self.root / f"consumer-{self.consumer_case}"
        package_root = case_root / "package"
        manifest = self.make_package(package_root)
        if mutate:
            mutate(package_root, manifest)
            artifact.write_json(package_root / "manifest.json", manifest)
        output = case_root / "proof-output"
        self.last_consumer_output = output
        if preexisting_output:
            output.mkdir()
            (output / "keep.txt").write_text("keep")
        wheelhouse = case_root / "wheelhouse"
        wheelhouse.mkdir()

        def prove_mock(root: Path, proof_dir: Path, library: Path, binary: Path,
                       passed_wheelhouse: Path, **kwargs: object) -> dict:
            wheel = kwargs.get("existing_wheel")
            return prove_result or {
                "status": "passed", "installation_performed": True,
                "uninstall_status": "passed", "integrity": {"wheel_sha256": artifact.sha(wheel)},
                "integration": {"status": "passed", "tests": 4, "failures": 0, "errors": 0,
                                "skipped": 0},
            }

        def run_mock(command: list[str], *, cwd: Path, stdout: object, stderr: object,
                     check: bool, timeout: int) -> None:
            (output / "output-window-tests.json").write_text(json.dumps(
                observation or {"status": "passed", "tests": 13, "failures": 0,
                                "errors": 0, "skipped": 0}))

        with self.fixture_patches(), patch.object(package, "prove", side_effect=prove_mock), \
                patch.object(package, "observation_command", return_value=["fake"]), \
                patch.object(package.subprocess, "run", side_effect=run_mock):
            package.test_package(package_root, output, wheelhouse, root=self.root)

    def test_consumer_rejects_wrong_package_schema_and_modified_seals(self) -> None:
        with self.fixture_patches(), self.assertRaisesRegex(ValueError, "producer evidence"):
            self.run_consumer(lambda root, manifest: manifest.update(schema_version="wrong"))

        def alter_bundle(root: Path, manifest: dict) -> None:
            bundle = root / "extension/manifest.json"
            value = json.loads(bundle.read_text())
            value["fixture_tampered"] = True
            bundle.write_text(json.dumps(value))

        with self.fixture_patches(), self.assertRaisesRegex(ValueError, "mismatched producer artifacts"):
            self.run_consumer(alter_bundle)

        def alter_build_report(root: Path, manifest: dict) -> None:
            (root / "wheel-build/report.json").write_text("changed build report")

        with self.fixture_patches(), self.assertRaisesRegex(ValueError, "build report changed"):
            self.run_consumer(alter_build_report)

    def test_consumer_rejects_bundle_revision_platform_and_toolchain_mismatch(self) -> None:
        for key, value, message in (("revision", "b" * 40, "revision, platform"),
                                    ("os", "OtherOS", "revision, platform")):
            def mutate(root: Path, manifest: dict, field: str = key, new_value: str = value) -> None:
                path = root / "extension/manifest.json"
                bundle = json.loads(path.read_text())
                bundle["identity"][field] = new_value
                path.write_text(json.dumps(bundle))
            with self.subTest(key=key), self.fixture_patches(), \
                    self.assertRaisesRegex(ValueError, message):
                self.run_consumer(mutate)

        def change_toolchain(root: Path, manifest: dict) -> None:
            path = root / "extension/manifest.json"
            bundle = json.loads(path.read_text())
            bundle["identity"]["rustc"] = "different rustc"
            path.write_text(json.dumps(bundle))
            manifest["bundle_manifests"]["extension"] = artifact.sha(path)

        with self.fixture_patches(), self.assertRaisesRegex(ValueError, "mismatched producer artifacts"):
            self.run_consumer(change_toolchain)

    def test_consumer_rejects_missing_multiple_and_changed_wheels(self) -> None:
        def remove_wheel(root: Path, manifest: dict) -> None:
            next((root / "wheel-build/dist").glob("*.whl")).unlink()
        with self.fixture_patches(), self.assertRaisesRegex(ValueError, "exactly one wheel"):
            self.run_consumer(remove_wheel)

        def add_wheel(root: Path, manifest: dict) -> None:
            (root / "wheel-build/dist/second.whl").write_bytes(b"second")
        with self.fixture_patches(), self.assertRaisesRegex(ValueError, "exactly one wheel"):
            self.run_consumer(add_wheel)

        def edit_wheel(root: Path, manifest: dict) -> None:
            next((root / "wheel-build/dist").glob("*.whl")).write_bytes(b"changed")
        with self.fixture_patches(), self.assertRaisesRegex(ValueError, "exactly one wheel"):
            self.run_consumer(edit_wheel)

    def test_consumer_rejects_preexisting_proof_output(self) -> None:
        with self.fixture_patches(), self.assertRaisesRegex(ValueError, "proof output must be new"):
            self.run_consumer(preexisting_output=True)
        assert self.last_consumer_output is not None
        self.assertEqual((self.last_consumer_output / "keep.txt").read_text(), "keep")

    def test_consumer_requires_installed_canonical_proof_and_full_observation_coverage(self) -> None:
        invalid_proofs = [
            {"status": "failed", "installation_performed": True, "uninstall_status": "passed",
             "integrity": {"wheel_sha256": "unused"}, "integration": {}},
            {"status": "passed", "installation_performed": False, "uninstall_status": "passed",
             "integrity": {"wheel_sha256": "unused"}, "integration": {}},
            {"status": "passed", "installation_performed": True, "uninstall_status": "failed",
             "integrity": {"wheel_sha256": "unused"}, "integration": {}},
            {"status": "passed", "installation_performed": True, "uninstall_status": "passed",
             "integrity": {"wheel_sha256": "wrong"}, "integration": {}},
        ]
        for proof in invalid_proofs:
            with self.subTest(proof=proof), self.fixture_patches(), \
                    self.assertRaisesRegex(ValueError, "canonical wheel"):
                self.run_consumer(prove_result=proof)
        invalid_observations = [
            {"status": "failed", "tests": 13, "failures": 1, "errors": 0, "skipped": 0},
            {"status": "passed", "tests": 12, "failures": 0, "errors": 0, "skipped": 0},
            {"status": "passed", "tests": 13, "failures": 0, "errors": 0, "skipped": 1},
        ]
        for result in invalid_observations:
            with self.subTest(observation=result), self.fixture_patches(), \
                    self.assertRaisesRegex(ValueError, "observation feature coverage incomplete"):
                self.run_consumer(observation=result)

    def proof_row(self, lane: str, *, os_name: str | None = None,
                  python_version: str | None = None) -> dict:
        expected_os, expected_python = {
            "native-python-wheel-py310_ubuntu": ("Linux", "3.10"),
            "native-python-wheel-py313_ubuntu": ("Linux", "3.13"),
            "native-python-wheel-py313_macos": ("Darwin", "3.13"),
        }[lane]
        return {
            "status": "passed",
            "source_revision": self.revision,
            "os": os_name or expected_os,
            "python_version": python_version or expected_python,
            "wheel_sha256": "wheel-linux" if expected_os == "Linux" else "wheel-mac",
            "package_manifest_sha256": "manifest-linux" if expected_os == "Linux" else "manifest-mac",
            "ordinary_extension_sha256": "ordinary-linux" if expected_os == "Linux" else "ordinary-mac",
            "observation_extension_sha256": "observation-linux" if expected_os == "Linux" else "observation-mac",
            "architecture": "x86_64" if expected_os == "Linux" else "arm64",
            "binding_tests": {"status": "passed", "tests": 5, "failures": 0, "errors": 0, "skipped": 0},
            "observation_tests": {"status": "passed", "tests": 13, "failures": 0, "errors": 0, "skipped": 0},
        }

    def write_reports(self, reports: Path) -> dict[str, dict]:
        reports.mkdir(parents=True, exist_ok=True)
        rows = {}
        for lane in ("native-python-wheel-py310_ubuntu", "native-python-wheel-py313_ubuntu",
                     "native-python-wheel-py313_macos"):
            path = reports / lane / "ci-package-proof.json"
            path.parent.mkdir()
            rows[lane] = self.proof_row(lane)
            path.write_text(json.dumps(rows[lane]))
        return rows

    def run_aggregate(self, reports: Path) -> dict:
        with patch.object(package, "command_output", side_effect=self.current_command):
            return package.aggregate(reports, root=self.root)

    def test_aggregate_accepts_exact_three_proofs_and_distinct_macos_package(self) -> None:
        reports = self.root / "reports"
        rows = self.write_reports(reports)
        rows["native-python-wheel-py310_ubuntu"]["architecture"] = "x86_64"
        rows["native-python-wheel-py313_ubuntu"]["architecture"] = "x86_64"
        rows["native-python-wheel-py313_macos"].update({
            "wheel_sha256": "different-macos-wheel",
            "package_manifest_sha256": "different-macos-manifest",
            "ordinary_extension_sha256": "different-macos-extension",
            "observation_extension_sha256": "different-macos-observation",
            "architecture": "arm64",
        })
        for lane, row in rows.items():
            (reports / lane / "ci-package-proof.json").write_text(json.dumps(row))
        result = self.run_aggregate(reports)
        self.assertEqual(result["status"], "passed")
        self.assertTrue(result["same_linux_wheel"])
        self.assertEqual(set(result["proofs"]), set(rows))

    def test_aggregate_rejects_missing_or_extra_proof_paths(self) -> None:
        reports = self.root / "reports"
        self.write_reports(reports)
        (reports / "native-python-wheel-py313_macos").rename(reports / "missing-macos")
        with self.assertRaisesRegex(ValueError, "omit or invent"):
            self.run_aggregate(reports)
        (reports / "missing-macos").rename(reports / "native-python-wheel-py313_macos")
        extra = reports / "unexpected"
        extra.mkdir()
        (extra / "ci-package-proof.json").write_text("{}")
        with self.assertRaisesRegex(ValueError, "omit or invent"):
            self.run_aggregate(reports)

    def test_aggregate_rejects_wrong_revision_platform_and_interpreter(self) -> None:
        mutations = [
            ("revision", lambda row: row.update(source_revision="b" * 40)),
            ("platform", lambda row: row.update(os="Windows")),
            ("interpreter", lambda row: row.update(python_version="3.12")),
        ]
        for label, mutate in mutations:
            with self.subTest(label=label):
                reports = self.root / f"reports-{label}"
                self.write_reports(reports)
                path = reports / "native-python-wheel-py310_ubuntu/ci-package-proof.json"
                row = json.loads(path.read_text())
                mutate(row)
                path.write_text(json.dumps(row))
                with self.assertRaisesRegex(ValueError, "stale revision or wrong platform/interpreter"):
                    self.run_aggregate(reports)

    def test_aggregate_rejects_zero_failed_or_skipped_binding_and_observation_tests(self) -> None:
        mutations = [
            ("binding-zero", "binding_tests", lambda row: row.update(tests=0)),
            ("binding-failed", "binding_tests", lambda row: row.update(status="failed")),
            ("binding-failures", "binding_tests", lambda row: row.update(failures=1)),
            ("binding-errors", "binding_tests", lambda row: row.update(errors=1)),
            ("binding-skipped", "binding_tests", lambda row: row.update(skipped=1)),
            ("observation-zero", "observation_tests", lambda row: row.update(tests=0)),
            ("observation-failed", "observation_tests", lambda row: row.update(status="failed")),
            ("observation-failures", "observation_tests", lambda row: row.update(failures=1)),
            ("observation-errors", "observation_tests", lambda row: row.update(errors=1)),
            ("observation-skipped", "observation_tests", lambda row: row.update(skipped=1)),
            ("observation-short", "observation_tests", lambda row: row.update(tests=12)),
        ]
        for label, key, mutate_test in mutations:
            with self.subTest(label=label):
                reports = self.root / f"reports-{label}"
                self.write_reports(reports)
                path = reports / "native-python-wheel-py310_ubuntu/ci-package-proof.json"
                row = json.loads(path.read_text())
                mutate_test(row[key])
                path.write_text(json.dumps(row))
                with self.assertRaisesRegex(ValueError, "test coverage failed or was skipped"):
                    self.run_aggregate(reports)

    def test_aggregate_rejects_each_linux_artifact_identity_mismatch(self) -> None:
        for key in ("wheel_sha256", "package_manifest_sha256", "ordinary_extension_sha256",
                    "observation_extension_sha256", "architecture"):
            with self.subTest(key=key):
                reports = self.root / f"reports-{key}"
                self.write_reports(reports)
                path = reports / "native-python-wheel-py313_ubuntu/ci-package-proof.json"
                row = json.loads(path.read_text())
                row[key] = "different-linux-" + key
                path.write_text(json.dumps(row))
                with self.assertRaisesRegex(ValueError, "same package artifacts"):
                    self.run_aggregate(reports)


def sys_version_major() -> int:
    return sys.version_info.major


def sys_version_minor() -> int:
    return sys.version_info.minor


if __name__ == "__main__":
    unittest.main()
