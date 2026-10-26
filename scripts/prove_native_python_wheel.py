"""Build, verify, install and exercise a local native wheel without publication.

Supply already built native/CLI artifacts and a hash-verified optional dependency
wheelhouse. All generated files go into a new explicit local directory. On a
developer workstation run this under the existing workload/storage supervisor.
"""

from __future__ import annotations

import argparse
from importlib.metadata import version
import json
import os
from pathlib import Path
import platform
import shutil
import subprocess
import sys
import time

from check_native_python_wheel import verify
from audit_native_python_wheel import audit
from local_uat_storage import require_local_path
from stage_native_python_package import sha, stage


IMPORT_CHECK = """
import hashlib, json, pathlib, subprocess, sys
import shardloom
from shardloom import _native, ShardLoomClient
root = pathlib.Path(sys.prefix).resolve()
package = pathlib.Path(shardloom.__file__).resolve()
library = pathlib.Path(_native.__file__).resolve()
assert root in package.parents and root in library.parents, (root, package, library)
assert _native.__version__ == shardloom.__version__
assert _native.execution_provider == "shared_shardloom_native"
def digest(path):
    value = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1 << 20), b""): value.update(block)
    return value.hexdigest()
assert digest(library) == sys.argv[1]
client = ShardLoomClient()
binary = pathlib.Path(client.binary_command()[0]).resolve()
assert root in binary.parents and digest(binary) == sys.argv[2]
cli_version = subprocess.check_output([str(binary), "--version"], text=True).strip()
assert cli_version == "shardloom " + shardloom.__version__, cli_version
client.close()
print(json.dumps({"package": str(package), "native_library": str(library),
                  "cli": str(binary), "version": shardloom.__version__,
                  "python": sys.version, "isolated_install": True}))
"""

RUN_TESTS = """
import json, pathlib, sys, unittest
patterns = ("test_native*binding.py", "test_connected_programming.py",
            "test_native_function_catalog.py", "test_native_observations.py",
            "test_native_standard_adapters.py")
suite = unittest.TestSuite()
for pattern in patterns:
    selected = unittest.defaultTestLoader.discover(sys.argv[1], pattern=pattern)
    if selected.countTestCases() == 0:
        raise RuntimeError("required native integration suite is absent: " + pattern)
    suite.addTests(selected)
result = unittest.TextTestRunner(verbosity=2).run(suite)
report = {"tests": result.testsRun, "failures": len(result.failures),
          "errors": len(result.errors), "skipped": len(result.skipped)}
passed = result.wasSuccessful() and result.testsRun > 0 and not result.skipped
report["status"] = "passed" if passed else "failed"
pathlib.Path(sys.argv[2]).write_text(json.dumps(report, indent=2) + "\\n")
raise SystemExit(0 if passed else 1)
"""


def prove(root: Path, output: Path, library: Path, binary: Path, wheelhouse: Path, *,
          audit_linux: bool = False, build_only: bool = False,
          existing_wheel: Path | None = None, build_report: Path | None = None) -> dict:
    if sys.platform not in {"darwin", "linux"}:
        raise ValueError("native wheel proof currently requires macOS or Linux")
    if output.is_symlink() or library.is_symlink() or binary.is_symlink():
        raise ValueError("native wheel proof requires non-symlink artifacts and destination")
    if (existing_wheel is None) != (build_report is None) or (build_only and existing_wheel):
        raise ValueError("wheel reuse requires its build report and cannot request another build")
    if existing_wheel and (existing_wheel.is_symlink() or build_report.is_symlink()):
        raise ValueError("wheel reuse requires regular artifacts")
    root, library, binary, wheelhouse = (path.resolve(strict=True)
                                        for path in (root, library, binary, wheelhouse))
    output = require_local_path(output.resolve(), Path.home(), sys.platform)
    if output.exists():
        raise ValueError("native wheel proof requires a new output directory")
    build_tools = {name: version(name) for name in ("setuptools", "wheel", "pip")}
    output.mkdir(parents=True)
    report = {"schema_version": "shardloom.native_python_wheel_proof.v1", "status": "running",
              "python": sys.version, "platform": platform.platform(), "steps": [],
              "build_tools": build_tools,
              "publication_attempted": False, "performance_claim": False,
              "scope": ("current_host_native_wheel_build_integrity" if build_only else
                        "current_host_native_wheel_install_and_binding_integration"),
              "installation_performed": False}
    env = dict(os.environ, PYTHONNOUSERSITE="1", PYTHONDONTWRITEBYTECODE="1",
               PIP_DISABLE_PIP_VERSION_CHECK="1", SHARDLOOM_REQUIRE_NATIVE_PYTHON="1")
    for key in ("PYTHONPATH", "PYTHONHOME", "SHARDLOOM_NATIVE_TEST_LIBRARY", "SHARDLOOM_BIN",
                "SHARDLOOM_REPO_ROOT", "SHARDLOOM_PROFILE_ORDER", "SHARDLOOM_WHEEL_PLAT_NAME"):
        env.pop(key, None)
    for key in list(env):
        if key.startswith("AUDITWHEEL_"):
            env.pop(key)

    def run(name, command):
        log = output / f"{name}.log"
        started = time.monotonic()
        entry = {"name": name, "command": [str(arg) for arg in command], "log": log.name}
        report["steps"].append(entry)
        try:
            with log.open("xb") as stream:
                result = subprocess.run(command, cwd=output, env=env, stdout=stream,
                                        stderr=subprocess.STDOUT, timeout=600, check=False)
            entry["returncode"] = result.returncode
            if result.returncode:
                raise RuntimeError(f"{name} failed; retained log: {log}")
        finally:
            entry["seconds"] = time.monotonic() - started
            if log.exists():
                entry["log_sha256"] = sha(log)

    try:
        needs_linux_audit = audit_linux and sys.platform == "linux"
        if existing_wheel is not None:
            source_report = json.loads(build_report.read_text())
            if (source_report.get("schema_version") != "shardloom.native_python_wheel_proof.v1"
                    or source_report.get("status") not in {"built", "passed"}
                    or source_report["integrity"]["wheel_sha256"] != sha(existing_wheel)):
                raise ValueError("reused wheel does not match a successful build report")
            dist = output / "dist"
            dist.mkdir()
            wheel = dist / existing_wheel.name
            shutil.copyfile(existing_wheel, wheel)
            report["artifact_reuse"] = {"wheel_sha256": sha(wheel),
                                        "build_report_sha256": sha(build_report)}
            if needs_linux_audit:
                report["linux_audit"] = source_report.get("linux_audit")
            report["integrity"] = verify(root, wheel, library, binary,
                                         linux_audit=report.get("linux_audit"))
        else:
            staged = output / "package"
            report["stage"] = stage(root, staged, library, binary)
            if report["stage"]["wheel_platform_tag"]:
                env["SHARDLOOM_WHEEL_PLAT_NAME"] = report["stage"]["wheel_platform_tag"]
            dist = output / ("unaudited" if needs_linux_audit else "dist")
            run("build", [sys.executable, "-m", "pip", "wheel", "--no-build-isolation",
                          "--no-deps", "--no-index", "--wheel-dir", dist, staged])
            wheels = list(dist.glob("*.whl"))
            if len(wheels) != 1:
                raise ValueError("native proof requires exactly one built wheel")
            wheel = wheels[0]
            report["integrity"] = verify(root, wheel, library, binary)
            if needs_linux_audit:
                report["unaudited_integrity"] = report["integrity"]
                wheel, report["linux_audit"] = audit(wheel, output / "dist", run)
                report["integrity"] = verify(root, wheel, library, binary,
                                             linux_audit=report["linux_audit"])
        if needs_linux_audit and not report["integrity"]["linux_audit_verified"]:
            raise ValueError("Linux wheel reuse requires the reviewed compatibility audit")
        if build_only:
            report["status"] = "built"
            return report
        virtualenv = output / "venv"
        run("create_environment", [sys.executable, "-m", "venv", virtualenv])
        python = virtualenv / "bin/python"
        run("install_optional_consumer", [python, "-I", "-m", "pip", "install", "--no-index",
            "--find-links", wheelhouse, "--only-binary=:all:", "--require-hashes", "-r",
            root / ".github/requirements/ci-native-python.txt"])
        run("install", [python, "-I", "-m", "pip", "install", "--no-index", "--no-deps", wheel])
        run("load", [python, "-I", "-c", IMPORT_CHECK,
                     report["integrity"]["native_library_sha256"], report["integrity"]["cli_sha256"]])
        run("integration", [python, "-I", "-c", RUN_TESTS, root / "python/tests",
                            output / "integration.json"])
        report["integration"] = json.loads((output / "integration.json").read_text())
        report["installed_load"] = json.loads((output / "load.log").read_text())
        report["installation_performed"] = True
        if sha(wheel) != report["integrity"]["wheel_sha256"]:
            raise ValueError("native wheel changed during installation proof")
        run("uninstall", [python, "-I", "-m", "pip", "uninstall", "--yes", "shardloom"])
        run("verify_uninstalled", [python, "-I", "-c",
            "import importlib.util, pathlib, sys; "
            "assert importlib.util.find_spec('shardloom') is None; "
            "assert all(not pathlib.Path(path).exists() for path in sys.argv[1:])",
            report["installed_load"]["native_library"], report["installed_load"]["cli"]])
        report["uninstall_status"] = "passed"
        report["status"] = "passed"
    except BaseException as error:
        report.update(status="failed", error=f"{type(error).__name__}: {error}")
        raise
    finally:
        (output / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--native-library", type=Path, required=True)
    parser.add_argument("--cli-binary", type=Path, required=True)
    parser.add_argument("--wheelhouse", type=Path, required=True)
    parser.add_argument("--audit-linux", action="store_true",
                        help="Require the reviewed Linux compatibility audit before installation.")
    parser.add_argument("--build-only", action="store_true",
                        help="Produce build/integrity evidence; installation remains unproven.")
    parser.add_argument("--existing-wheel", type=Path,
                        help="Install this exact previously built wheel without rebuilding it.")
    parser.add_argument("--build-report", type=Path,
                        help="Exact build/integrity report required with --existing-wheel.")
    args = parser.parse_args()
    report = prove(args.repo_root, args.output_dir, args.native_library, args.cli_binary, args.wheelhouse,
                   audit_linux=args.audit_linux, build_only=args.build_only,
                   existing_wheel=args.existing_wheel, build_report=args.build_report)
    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
