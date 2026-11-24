"""Build and stage the optional native Python module without installing it.

Cargo selects the actual artifact location; the output directory is explicit.
On macOS both locations must pass the existing local-only storage guard.
Run substantial local builds under the repository's workload/storage supervisor.
This command does not build a wheel, publish a package, or select a query engine.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile

from local_uat_storage import require_local_path
from maintain_shared_cargo_target import prune_superseded_artifacts, resolve_cargo_target_dir


def native_artifact(messages: list[dict[str, object]]) -> Path:
    candidates = set()
    for message in messages:
        target = message.get("target", {})
        if (message.get("reason") != "compiler-artifact"
                or target.get("name") != "_native"
                or "cdylib" not in target.get("crate_types", [])):
            continue
        for filename in message.get("filenames", []):
            path = Path(filename)
            if path.suffix in {".so", ".dylib"}:
                candidates.add(path)
    if len(candidates) != 1:
        raise ValueError("Cargo must report exactly one _native cdylib artifact")
    return candidates.pop()


def stage_library(source: Path, output_dir: Path) -> Path:
    if not source.is_file() or source.is_symlink():
        raise ValueError("native library must be a regular built artifact")
    output_dir.mkdir(parents=True, exist_ok=True)
    destination = output_dir / "_native.abi3.so"
    if destination.is_symlink():
        raise ValueError("native library destination must not be a symlink")
    # Atomic replacement leaves any earlier loaded module's inode intact.
    descriptor, temporary = tempfile.mkstemp(prefix=".native-", suffix=".so", dir=output_dir)
    temporary = Path(temporary)
    try:
        with os.fdopen(descriptor, "wb") as target, source.open("rb") as origin:
            shutil.copyfileobj(origin, target)
            target.flush()
            os.fsync(target.fileno())
        shutil.copymode(source, temporary)
        os.replace(temporary, destination)
    finally:
        temporary.unlink(missing_ok=True)
    return destination


def build_native_binding(repo_root: Path, output_dir: Path | None = None, *, release: bool,
                         offline: bool = False, cargo: str = "cargo") -> dict[str, object]:
    if sys.platform not in {"darwin", "linux"}:
        raise ValueError("the native Python binding currently requires macOS or Linux")
    repo_root = repo_root.resolve(strict=True)
    env = dict(os.environ, PYO3_PYTHON=sys.executable, PYO3_BUILD_EXTENSION_MODULE="1")
    metadata_command = [cargo, "metadata", "--locked", "--no-deps", "--format-version", "1"]
    if offline:
        metadata_command.append("--offline")
    metadata = json.loads(subprocess.check_output(metadata_command, cwd=repo_root, env=env))
    target = require_local_path(Path(metadata["target_directory"]), Path.home(), sys.platform)
    resolved_output = output_dir if output_dir is not None else (target / "native-python-dev")
    output_dir = require_local_path(resolved_output.resolve(), Path.home(), sys.platform)
    command = [cargo, "build", "--locked", "-p", "shardloom-python", "--features", "python-native",
               "--message-format=json-render-diagnostics"]
    if release:
        command.append("--release")
    if offline:
        command.append("--offline")
    messages = []
    with subprocess.Popen(command, cwd=repo_root, env=env, stdout=subprocess.PIPE,
                          text=True, encoding="utf-8") as process:
        for line in process.stdout:
            message = json.loads(line)
            if message.get("reason") == "compiler-artifact":
                messages.append(message)
            elif message.get("reason") == "compiler-message":
                rendered = message.get("message", {}).get("rendered")
                if rendered:
                    print(rendered, file=sys.stderr, end="")
        code = process.wait()
    if code:
        raise subprocess.CalledProcessError(code, command)
    artifact = native_artifact(messages)
    if target.resolve() not in artifact.resolve().parents:
        raise ValueError("Cargo artifact is outside its resolved target directory")
    destination = stage_library(artifact, output_dir)
    prune_report = prune_superseded_artifacts(target, keep_workspace_hashes=2, keep_dep_hashes=3)
    digest = hashlib.sha256()
    with destination.open("rb") as stream:
        for block in iter(lambda: stream.read(1 << 20), b""):
            digest.update(block)
    return {
        "schema_version": "shardloom.native_python_build.v1", "status": "passed",
        "command": command, "target_directory": str(target), "cargo_artifact": str(artifact),
        "library": str(destination), "library_bytes": destination.stat().st_size,
        "library_sha256": digest.hexdigest(), "python_abi": "cp310-abi3",
        "package_version": next(package["version"] for package in metadata["packages"]
                                if package["name"] == "shardloom-python"),
        "pruned_superseded_artifacts": prune_report,
        "installed": False, "published": False,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Defaults to <resolved Cargo target_directory>/native-python-dev",
    )
    parser.add_argument("--release", action="store_true")
    parser.add_argument("--offline", action="store_true")
    parser.add_argument("--cargo", default="cargo")
    arguments = parser.parse_args()
    output_dir = arguments.output_dir
    if output_dir is None:
        output_dir = resolve_cargo_target_dir(arguments.repo_root, arguments.cargo) / "native-python-dev"
    report = build_native_binding(arguments.repo_root, output_dir,
                                  release=arguments.release, offline=arguments.offline,
                                  cargo=arguments.cargo)
    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
