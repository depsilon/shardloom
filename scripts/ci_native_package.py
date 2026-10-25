#!/usr/bin/env python3
"""Build platform-native CI products once and test the exact wheel per interpreter."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import platform
import shutil
import subprocess
import sys

from ci_build_artifact import ROOT, build, command_output, identity, sha, verify_bundle, write_json
from prove_native_python_wheel import prove


def build_package(output: Path, cli_bundle: Path | None, *, root: Path = ROOT) -> dict:
    if output.exists() or output.is_symlink():
        raise ValueError("native package build output must be new")
    output.mkdir(parents=True)
    if cli_bundle is None:
        build("cli-release", output / "cli", root=root)
    else:
        verify_bundle(cli_bundle, "cli-release", root=root)
        shutil.copytree(cli_bundle, output / "cli")
    binary = output / "cli/shardloom"
    binary.chmod(0o755)
    build("native-extension", output / "extension", root=root)
    library = output / "extension/_native.abi3.so"
    wheelhouse = output / "empty-wheelhouse"
    wheelhouse.mkdir()
    report = prove(root, output / "wheel-build", library, binary, wheelhouse,
                   audit_linux=True, build_only=True)
    if report["status"] != "built" or report["installation_performed"]:
        raise ValueError("package producer must report build-only evidence")
    # The observation extension is a distinct product. It never replaces the
    # ordinary extension already sealed inside the distributed wheel.
    build("observation-extension", output / "observation", root=root)
    wheels = list((output / "wheel-build/dist").glob("*.whl"))
    if len(wheels) != 1:
        raise ValueError("native package producer must seal exactly one wheel")
    result = {"schema_version": "shardloom.ci_native_package.v1", "status": "built",
              "identity": identity(root), "wheel": wheels[0].name,
              "wheel_sha256": sha(wheels[0]), "installed": False,
              "build_report_sha256": sha(output / "wheel-build/report.json"),
              "bundle_manifests": {name: sha(output / name / "manifest.json")
                                   for name in ("cli", "extension", "observation")}}
    write_json(output / "manifest.json", result)
    return result


def observation_command(root: Path, library: Path, output: Path) -> list[str]:
    code = """
import json, os, pathlib, sys, unittest
root, library, report = map(pathlib.Path, sys.argv[1:])
sys.path[:0] = [str(root / 'scripts'), str(root / 'python/src')]
os.environ.update(SHARDLOOM_NATIVE_TEST_LIBRARY=str(library), SHARDLOOM_REQUIRE_NATIVE_PYTHON='1',
                  SHARDLOOM_REQUIRE_OUTPUT_WINDOW_OBSERVATION='1')
suite = unittest.defaultTestLoader.discover(str(root / 'python/tests'), pattern='test_native_output_window_observation.py')
expected = suite.countTestCases()
assert expected == 13
result = unittest.TextTestRunner(verbosity=2).run(suite)
passed = result.wasSuccessful() and result.testsRun == expected and not result.skipped
report.write_text(json.dumps({'status': 'passed' if passed else 'failed', 'tests': result.testsRun,
    'failures': len(result.failures), 'errors': len(result.errors), 'skipped': len(result.skipped)}, indent=2) + '\\n')
raise SystemExit(0 if passed else 1)
"""
    return [str(output / "proof/venv/bin/python"), "-I", "-c", code, str(root), str(library),
            str(output / "output-window-tests.json")]


def test_package(package: Path, output: Path, wheelhouse: Path, *, root: Path = ROOT) -> dict:
    manifest = json.loads((package / "manifest.json").read_text())
    if (manifest.get("schema_version") != "shardloom.ci_native_package.v1"
            or manifest.get("status") != "built" or manifest.get("installed") is not False):
        raise ValueError("native package producer evidence is invalid")
    for product, directory in (("cli-release", "cli"), ("native-extension", "extension"),
                               ("observation-extension", "observation")):
        bundle = verify_bundle(package / directory, product, root=root, require_toolchain=False)
        if (sha(package / directory / "manifest.json") != manifest["bundle_manifests"][directory]
                or any(bundle["identity"][key] != manifest["identity"][key]
                       for key in ("revision", "cargo_lock_sha256", "os", "architecture", "rustc", "cargo"))):
            raise ValueError("native package contains mismatched producer artifacts")
    if output.exists() or output.is_symlink():
        raise ValueError("native package proof output must be new")
    output.mkdir(parents=True)
    binary = package / "cli/shardloom"
    binary.chmod(0o755)
    library = package / "extension/_native.abi3.so"
    wheels = sorted((package / "wheel-build/dist").glob("*.whl"))
    if (len(wheels) != 1 or wheels[0].name != manifest["wheel"]
            or sha(wheels[0]) != manifest["wheel_sha256"]):
        raise ValueError("native package must contain exactly one wheel")
    build_report = package / "wheel-build/report.json"
    if sha(build_report) != manifest["build_report_sha256"]:
        raise ValueError("native package build report changed")
    report = prove(root, output / "proof", library, binary, wheelhouse, audit_linux=True,
                   existing_wheel=wheels[0], build_report=build_report)
    if (report.get("status") != "passed" or report.get("installation_performed") is not True
            or report.get("uninstall_status") != "passed"
            or report["integrity"]["wheel_sha256"] != sha(wheels[0])):
        raise ValueError("installed proof did not use the canonical wheel")
    observation = package / "observation/_native.abi3.so"
    command = observation_command(root, observation, output)
    with (output / "output-window-tests.log").open("xb") as stream:
        subprocess.run(command, cwd=root, stdout=stream, stderr=subprocess.STDOUT, check=True, timeout=600)
    diagnostics = json.loads((output / "output-window-tests.json").read_text())
    if diagnostics["status"] != "passed" or diagnostics["tests"] != 13 or diagnostics["skipped"]:
        raise ValueError("observation feature coverage incomplete")
    result = {"status": "passed", "python": sys.version, "wheel_sha256": sha(wheels[0]),
              "os": platform.system(), "architecture": platform.machine(),
              "python_version": f"{sys.version_info.major}.{sys.version_info.minor}",
              "package_manifest_sha256": sha(package / "manifest.json"),
              "ordinary_extension_sha256": sha(library), "observation_extension_sha256": sha(observation),
              "binding_tests": report["integration"], "observation_tests": diagnostics,
              "source_revision": json.loads((package / "cli/manifest.json").read_text())["identity"]["revision"]}
    write_json(output / "ci-package-proof.json", result)
    return result


def aggregate(reports: Path, *, root: Path = ROOT) -> dict:
    expected = {"native-python-wheel-py310_ubuntu": ("Linux", "3.10"),
                "native-python-wheel-py313_ubuntu": ("Linux", "3.13"),
                "native-python-wheel-py313_macos": ("Darwin", "3.13")}
    paths = sorted(reports.glob("*/ci-package-proof.json"))
    if {path.parent.name for path in paths} != set(expected) or len(paths) != len(expected):
        raise ValueError("native package proofs omit or invent an interpreter/platform")
    revision = command_output(["git", "rev-parse", "HEAD"], root)
    rows = {path.parent.name: json.loads(path.read_text()) for path in paths}
    for lane, row in rows.items():
        if (row["status"] != "passed" or row["source_revision"] != revision
                or (row["os"], row["python_version"]) != expected[lane]):
            raise ValueError("native package proof has stale revision or wrong platform/interpreter")
        for key in ("binding_tests", "observation_tests"):
            tests = row[key]
            if (tests["status"] != "passed" or tests["tests"] <= 0
                    or any(tests[field] != 0 for field in ("failures", "errors", "skipped"))
                    or (key == "observation_tests" and tests["tests"] != 13)):
                raise ValueError("native package test coverage failed or was skipped")
    linux = [rows[f"native-python-wheel-py{version}_ubuntu"] for version in ("310", "313")]
    for key in ("wheel_sha256", "package_manifest_sha256", "ordinary_extension_sha256",
                "observation_extension_sha256", "architecture"):
        if not linux[0][key] or linux[0][key] != linux[1][key]:
            raise ValueError("Linux interpreters did not validate the same package artifacts")
    return {"schema_version": "shardloom.ci_native_package_coverage.v1", "status": "passed",
            "revision": revision, "proofs": rows, "same_linux_wheel": True}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("build", "test", "aggregate"))
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--cli-bundle", type=Path)
    parser.add_argument("--package", type=Path)
    parser.add_argument("--wheelhouse", type=Path)
    parser.add_argument("--reports", type=Path)
    args = parser.parse_args()
    if args.action == "build":
        result = build_package(args.output, args.cli_bundle)
    elif args.action == "test":
        if args.package is None or args.wheelhouse is None:
            parser.error("test requires --package and --wheelhouse")
        result = test_package(args.package, args.output, args.wheelhouse)
    else:
        if args.reports is None:
            parser.error("aggregate requires --reports")
        result = aggregate(args.reports)
        args.output.parent.mkdir(parents=True, exist_ok=True)
        write_json(args.output, result)
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
