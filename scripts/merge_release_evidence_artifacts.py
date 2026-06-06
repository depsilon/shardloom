#!/usr/bin/env python
# SPDX-License-Identifier: Apache-2.0
"""Normalize downloaded release evidence artifacts into repo-relative paths.

GitHub upload-artifact can expose different roots depending on the uploaded path
set. Release validators, however, read fixed repo-relative paths such as
``target/release-dry-run-proof`` and ``python/dist``. This script makes that
merge explicit so downstream jobs do not depend on artifact path-shaping quirks.
"""

from __future__ import annotations

import argparse
import json
import shutil
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_VERSION = "shardloom.release_evidence_artifact_merge.v1"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=ROOT)
    parser.add_argument(
        "--artifact",
        type=Path,
        action="append",
        required=True,
        help="Downloaded artifact directory. May be provided more than once.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("target/release-evidence-artifact-merge-report.json"),
    )
    return parser.parse_args()


def resolve(repo_root: Path, path: Path) -> Path:
    return path if path.is_absolute() else repo_root / path


def copy_path(source: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    if source.is_dir():
        shutil.copytree(source, destination, dirs_exist_ok=True)
    else:
        shutil.copy2(source, destination)


def copy_contents(source_dir: Path, destination_dir: Path) -> list[str]:
    copied: list[str] = []
    destination_dir.mkdir(parents=True, exist_ok=True)
    for child in sorted(source_dir.iterdir(), key=lambda path: path.name):
        destination = destination_dir / child.name
        copy_path(child, destination)
        copied.append(destination.as_posix())
    return copied


def merge_artifact(repo_root: Path, artifact_dir: Path) -> dict[str, Any]:
    artifact_dir = artifact_dir.resolve()
    copied: list[str] = []
    blockers: list[str] = []

    if not artifact_dir.is_dir():
        return {
            "artifact_dir": artifact_dir.as_posix(),
            "status": "failed",
            "copied_paths": [],
            "blockers": [f"missing artifact directory: {artifact_dir.as_posix()}"],
        }

    for child in sorted(artifact_dir.iterdir(), key=lambda path: path.name):
        if child.name == "target":
            copied.extend(copy_contents(child, repo_root / "target"))
        elif child.name == "python":
            copied.extend(copy_contents(child, repo_root / "python"))
        elif child.name == "dist":
            copy_path(child, repo_root / "python" / "dist")
            copied.append((repo_root / "python" / "dist").as_posix())
        elif child.name == "debug":
            copy_path(child, repo_root / "target" / "debug")
            copied.append((repo_root / "target" / "debug").as_posix())
        else:
            destination = repo_root / "target" / child.name
            copy_path(child, destination)
            copied.append(destination.as_posix())

    return {
        "artifact_dir": artifact_dir.as_posix(),
        "status": "passed" if not blockers else "failed",
        "copied_paths": copied,
        "blockers": blockers,
    }


def main() -> int:
    args = parse_args()
    repo_root = args.repo_root.resolve()
    rows = [
        merge_artifact(repo_root, resolve(repo_root, artifact))
        for artifact in args.artifact
    ]
    blockers = [blocker for row in rows for blocker in row["blockers"]]
    report = {
        "schema_version": SCHEMA_VERSION,
        "status": "passed" if not blockers else "failed",
        "artifact_count": len(rows),
        "artifacts": rows,
        "blockers": blockers,
        "publication_attempted": False,
        "tag_created": False,
        "secrets_required": False,
        "fallback_attempted": False,
        "external_engine_invoked": False,
    }
    output = resolve(repo_root, args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(output)
    return 0 if not blockers else 1


if __name__ == "__main__":
    raise SystemExit(main())
