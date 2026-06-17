#!/usr/bin/env python
# SPDX-License-Identifier: Apache-2.0
"""Normalize downloaded release evidence artifacts into repo-relative paths.

GitHub upload-artifact can expose different roots depending on the uploaded path
set. Release validators, however, read fixed repo-relative paths such as
``target/release-dry-run-proof/transcript.json`` and release evidence reports.
This script makes that merge explicit so downstream jobs do not depend on
artifact path-shaping quirks.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import stat
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_VERSION = "shardloom.release_evidence_artifact_merge.v1"
KNOWN_EXECUTABLE_REFS = (
    "target/debug/shardloom",
    "target/release/shardloom",
)


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


def path_ref(repo_root: Path, path: Path) -> str:
    resolved_root = repo_root.resolve()
    resolved_path = path.resolve()
    try:
        return resolved_path.relative_to(resolved_root).as_posix()
    except ValueError:
        return f"external-artifact:{path.name}"


def artifact_tree_manifest(artifact_dir: Path) -> tuple[dict[str, Any], list[str]]:
    rows: list[dict[str, Any]] = []
    blockers: list[str] = []
    total_bytes = 0

    for path in sorted(artifact_dir.rglob("*"), key=lambda item: item.as_posix()):
        relative_path = path.relative_to(artifact_dir).as_posix()
        if path.is_symlink():
            blockers.append(f"artifact contains unsupported symlink: {relative_path}")
            continue
        if path.is_dir():
            continue
        if not path.is_file():
            blockers.append(f"artifact contains unsupported non-file entry: {relative_path}")
            continue
        content = path.read_bytes()
        total_bytes += len(content)
        rows.append(
            {
                "path": relative_path,
                "size_bytes": len(content),
                "sha256": hashlib.sha256(content).hexdigest(),
            }
        )

    digest_payload = json.dumps(rows, sort_keys=True, separators=(",", ":")).encode("utf-8")
    manifest = {
        "producer_artifact_name": artifact_dir.name,
        "artifact_tree_digest": "sha256:" + hashlib.sha256(digest_payload).hexdigest(),
        "artifact_file_count": len(rows),
        "artifact_total_bytes": total_bytes,
        "artifact_files": rows,
    }
    return manifest, blockers


def copy_path(source: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    if source.is_dir():
        shutil.copytree(source, destination, dirs_exist_ok=True)
    else:
        shutil.copy2(source, destination)


def copy_contents(repo_root: Path, source_dir: Path, destination_dir: Path) -> list[str]:
    copied: list[str] = []
    destination_dir.mkdir(parents=True, exist_ok=True)
    for child in sorted(source_dir.iterdir(), key=lambda path: path.name):
        destination = destination_dir / child.name
        copy_path(child, destination)
        copied.append(path_ref(repo_root, destination))
    return copied


def normalize_known_executables(repo_root: Path) -> tuple[list[dict[str, Any]], list[str]]:
    rows: list[dict[str, Any]] = []
    blockers: list[str] = []
    if os.name == "nt":
        return rows, blockers

    for ref in KNOWN_EXECUTABLE_REFS:
        path = repo_root / ref
        if not path.is_file():
            continue
        before = stat.S_IMODE(path.stat().st_mode)
        repair_attempted = not bool(before & stat.S_IXUSR)
        try:
            if repair_attempted:
                path.chmod(path.stat().st_mode | stat.S_IXUSR)
            after = stat.S_IMODE(path.stat().st_mode)
        except OSError as error:
            after = before
            blockers.append(f"failed to restore executable permission for {ref}: {error}")
        rows.append(
            {
                "path": ref,
                "before_mode": oct(before),
                "after_mode": oct(after),
                "permission_repair_attempted": repair_attempted,
                "owner_executable": bool(after & stat.S_IXUSR),
            }
        )
    return rows, blockers


def merge_artifact(repo_root: Path, artifact_dir: Path) -> dict[str, Any]:
    artifact_dir = artifact_dir.resolve()
    copied: list[str] = []
    blockers: list[str] = []

    if not artifact_dir.is_dir():
        return {
            "artifact_dir": path_ref(repo_root, artifact_dir),
            "producer_artifact_name": artifact_dir.name,
            "artifact_tree_digest": "not_available_missing_artifact",
            "artifact_file_count": 0,
            "artifact_total_bytes": 0,
            "artifact_files": [],
            "downloaded_artifact_digest_bound": False,
            "status": "failed",
            "copied_paths": [],
            "blockers": [f"missing artifact directory: {artifact_dir.as_posix()}"],
        }

    manifest, manifest_blockers = artifact_tree_manifest(artifact_dir)
    blockers.extend(manifest_blockers)
    if blockers:
        return {
            "artifact_dir": path_ref(repo_root, artifact_dir),
            **manifest,
            "downloaded_artifact_digest_bound": False,
            "status": "failed",
            "copied_paths": [],
            "blockers": blockers,
        }

    for child in sorted(artifact_dir.iterdir(), key=lambda path: path.name):
        if child.name == "target":
            copied.extend(copy_contents(repo_root, child, repo_root / "target"))
        elif child.name == "python":
            copied.extend(copy_contents(repo_root, child, repo_root / "python"))
        elif child.name == "dist":
            copy_path(child, repo_root / "python" / "dist")
            copied.append(path_ref(repo_root, repo_root / "python" / "dist"))
        elif child.name == "debug":
            copy_path(child, repo_root / "target" / "debug")
            copied.append(path_ref(repo_root, repo_root / "target" / "debug"))
        else:
            destination = repo_root / "target" / child.name
            copy_path(child, destination)
            copied.append(path_ref(repo_root, destination))

    executable_rows, executable_blockers = normalize_known_executables(repo_root)
    blockers.extend(executable_blockers)

    return {
        "artifact_dir": path_ref(repo_root, artifact_dir),
        **manifest,
        "downloaded_artifact_digest_bound": True,
        "status": "passed" if not blockers else "failed",
        "copied_paths": copied,
        "normalized_executable_paths": executable_rows,
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
        "artifact_digests": {
            row["producer_artifact_name"]: row["artifact_tree_digest"] for row in rows
        },
        "downloaded_artifact_digest_binding_status": (
            "bound" if rows and all(row["downloaded_artifact_digest_bound"] for row in rows) else "blocked"
        ),
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
