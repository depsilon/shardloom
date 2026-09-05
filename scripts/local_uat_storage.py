#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Read-only disk and destination guard for local large-artifact UAT."""

from __future__ import annotations

import argparse
import json
import math
import os
from pathlib import Path
import shutil
import stat
import sys

GIB = 1024**3
MIB = 1024**2


class StorageGuardError(ValueError):
    """A destination or disk budget is unsafe for this run."""


def beneath(path: Path, parent: Path) -> bool:
    # macOS normally uses a case-insensitive filesystem. Over-rejecting a
    # differently cased synced path is safer than accidentally admitting it.
    child_parts = tuple(part.casefold() for part in path.parts)
    parent_parts = tuple(part.casefold() for part in parent.parts)
    return child_parts[:len(parent_parts)] == parent_parts


def require_local_path(path: Path, home: Path, platform: str) -> Path:
    lexical = Path(os.path.abspath(path.expanduser()))
    resolved = lexical.resolve()
    if platform == "darwin":
        roots = [
            home / "Desktop",
            home / "Documents",
            home / "Library/Mobile Documents",
            home / "Library/CloudStorage",
            home / "Library/Application Support/CloudDocs",
        ]
        for root in roots:
            for candidate in (lexical, resolved):
                if beneath(candidate, root) or beneath(candidate, root.resolve()):
                    raise StorageGuardError(
                        f"refusing iCloud/cloud-managed destination or input: {path}; "
                        "use a local folder outside Desktop, Documents and cloud storage"
                    )
    return resolved


def validate_paths(
    root: Path, source: Path, target: Path, logs: Path,
    *, home: Path | None = None, platform: str | None = None,
) -> tuple[Path, Path, Path, Path]:
    home = Path.home() if home is None else home
    platform = sys.platform if platform is None else platform
    paths = tuple(require_local_path(path, home, platform)
                  for path in (root, source, target, logs))
    root, source, target, logs = paths
    if target == root or not target.is_relative_to(root):
        raise StorageGuardError("target must be inside the declared UAT root")
    if logs == root or not logs.is_relative_to(root):
        raise StorageGuardError("logs must be inside the declared UAT root")
    if source == target or target.is_relative_to(logs) or source.is_relative_to(logs):
        raise StorageGuardError("source, target and log destinations must not overlap")
    return paths


def accounted_bytes(root: Path) -> int:
    """Conservative logical/allocated byte count, without following symlinks."""
    total = 0
    pending = [root]
    seen: set[tuple[int, int]] = set()
    while pending:
        path = pending.pop()
        try:
            info = path.lstat()
        except FileNotFoundError:
            continue
        identity = (info.st_dev, info.st_ino)
        if identity in seen:
            continue
        seen.add(identity)
        if stat.S_ISDIR(info.st_mode):
            with os.scandir(path) as entries:
                pending.extend(Path(entry.path) for entry in entries)
        else:
            total += max(info.st_size, getattr(info, "st_blocks", 0) * 512)
    return total


def available_bytes(path: Path) -> int:
    while not path.exists():
        path = path.parent
    return shutil.disk_usage(path).free


def check_budgets(
    root: Path, target: Path, logs: Path, *,
    min_free_bytes: int, reserve_bytes: int,
    max_workspace_bytes: int, max_log_bytes: int,
) -> dict[str, int]:
    workspace_bytes = accounted_bytes(root)
    log_bytes = accounted_bytes(root / "logs")
    free_bytes = min(available_bytes(root), available_bytes(target.parent),
                     available_bytes(logs))
    if workspace_bytes + reserve_bytes > max_workspace_bytes:
        raise StorageGuardError(
            f"UAT workspace budget exceeded: {workspace_bytes} existing + "
            f"{reserve_bytes} reserved > {max_workspace_bytes} bytes; "
            "review old run artifacts before retrying"
        )
    if log_bytes > max_log_bytes:
        raise StorageGuardError(f"UAT log budget exceeded: {log_bytes} > {max_log_bytes} bytes")
    if free_bytes < min_free_bytes + reserve_bytes:
        raise StorageGuardError(
            f"insufficient free disk: {free_bytes} bytes available; "
            f"{reserve_bytes} for the candidate plus {min_free_bytes} headroom required"
        )
    return {
        "workspace_bytes": workspace_bytes,
        "log_bytes": log_bytes,
        "free_bytes": free_bytes,
        "reserved_candidate_bytes": reserve_bytes,
    }


def nonnegative_number(value: str) -> float:
    number = float(value)
    if not math.isfinite(number) or number < 0:
        raise argparse.ArgumentTypeError("expected a finite nonnegative number")
    return number


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("root", "source", "target", "logs"):
        parser.add_argument(f"--{name}", type=Path, required=True)
    parser.add_argument("--paths-only", action="store_true")
    parser.add_argument("--min-free-gib", type=nonnegative_number, default=12)
    parser.add_argument("--reserve-gib", type=nonnegative_number, default=0)
    parser.add_argument("--max-workspace-gib", type=nonnegative_number, default=100)
    parser.add_argument("--max-log-mib", type=nonnegative_number, default=256)
    args = parser.parse_args()
    try:
        root, _, target, logs = validate_paths(args.root, args.source, args.target, args.logs)
        if not args.paths_only:
            snapshot = check_budgets(
                root, target, logs,
                min_free_bytes=math.ceil(args.min_free_gib * GIB),
                reserve_bytes=math.ceil(args.reserve_gib * GIB),
                max_workspace_bytes=math.floor(args.max_workspace_gib * GIB),
                max_log_bytes=math.floor(args.max_log_mib * MIB),
            )
            print(json.dumps(snapshot, sort_keys=True))
    except (StorageGuardError, OSError, RuntimeError) as error:
        print(f"storage guard: {error}", file=sys.stderr)
        return 78
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
