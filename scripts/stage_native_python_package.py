"""Stage an explicit native extension and CLI for a local Python wheel build.

The destination must be new and outside macOS synced storage. Neither compilation
nor package publication occurs here. Actual build/load evidence remains required.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import sys

from build_native_python import stage_library
from check_python_license_bundle import verify
from local_uat_storage import require_local_path
from release_dry_run_proof import stage_python_package_with_bundled_cli


def sha(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def stage(repo_root: Path, destination: Path, library: Path, binary: Path) -> dict:
    if sys.platform not in {"darwin", "linux"}:
        raise ValueError("the native extension currently requires macOS or Linux")
    if destination.is_symlink():
        raise ValueError("native package staging requires a non-symlink destination")
    destination = require_local_path(destination.resolve(), Path.home(), sys.platform)
    if destination.exists():
        raise ValueError("native package staging requires a new destination")
    for path in (library, binary):
        if path.is_symlink() or not path.is_file():
            raise ValueError("native artifacts must be regular built files")
    licenses = verify(repo_root)
    source_identity = {"native_library": sha(library), "cli_binary": sha(binary)}
    result = stage_python_package_with_bundled_cli(repo_root, destination, binary)
    if result["returncode"] != 0:
        raise ValueError(result["stderr"])
    staged = stage_library(library, destination / "src/shardloom")
    platform = result["bundled_cli_platform_tag"]
    staged_binary = destination / "src/shardloom/bin" / platform / "shardloom"
    if sha(staged) != source_identity["native_library"] or sha(staged_binary) != source_identity["cli_binary"]:
        raise ValueError("native artifact identity changed during package staging")
    return {"status": "passed", "destination": str(destination),
            "native_library_sha256": source_identity["native_library"],
            "cli_sha256": source_identity["cli_binary"], "python_abi": "cp310-abi3",
            "wheel_platform_tag": result["wheel_platform_tag"],
            "licenses": licenses, "published": False}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--stage-dir", type=Path, required=True)
    parser.add_argument("--native-library", type=Path, required=True)
    parser.add_argument("--cli-binary", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(stage(args.repo_root, args.stage_dir, args.native_library, args.cli_binary), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
