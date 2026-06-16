#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Synchronize package-manager version files from root Cargo.toml.

Root `Cargo.toml` `[workspace.package].version` is ShardLoom's package-version
source of truth. PyPI, npm/Astro tooling, and Cargo.lock still need literal
versions for their package managers, so this script updates or checks those
derived files instead of requiring hand edits in every place.
"""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

from release_report_utils import read_text, workspace_members, workspace_package_version


ROOT = Path(__file__).resolve().parents[1]
DERIVED_VERSION_SOURCES = (
    "python/src/shardloom/_version.py",
    "website-src/package.json",
    "website-src/package-lock.json",
    "Cargo.lock",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=ROOT)
    parser.add_argument(
        "--check",
        action="store_true",
        help="Fail when derived version files are stale instead of rewriting them.",
    )
    return parser.parse_args()


def replace_python_version(text: str, version: str) -> str:
    expected = (
        '"""Package version for the ShardLoom Python client.\n\n'
        'This file is derived from root Cargo.toml by '
        'scripts/sync_workspace_package_versions.py.\n'
        '"""\n\n'
        f'__version__ = "{version}"\n'
    )
    if "__version__" not in text:
        return expected
    return re.sub(
        r'\A.*?__version__\s*=\s*"[^"\\]*(?:\\.[^"\\]*)*"\s*\Z',
        expected.rstrip("\n"),
        text,
        flags=re.DOTALL,
    ) + "\n"


def update_json_version(path: Path, version: str, *, package_lock: bool) -> str:
    payload = json.loads(read_text(path, missing_ok=False))
    if not isinstance(payload, dict):
        raise ValueError(f"{path} must contain a JSON object")
    payload["version"] = version
    if package_lock:
        packages = payload.get("packages")
        if not isinstance(packages, dict) or not isinstance(packages.get(""), dict):
            raise ValueError(f"{path} must contain packages[''] metadata")
        packages[""]["version"] = version
    return json.dumps(payload, indent=2, ensure_ascii=True) + "\n"


def update_cargo_lock(text: str, workspace_crate_names: set[str], version: str) -> str:
    lines = text.splitlines(keepends=True)
    current_name: str | None = None
    for index, line in enumerate(lines):
        stripped = line.strip()
        if stripped == "[[package]]":
            current_name = None
            continue
        if stripped.startswith("name = "):
            match = re.match(r'name\s*=\s*"([^"]+)"', stripped)
            current_name = match.group(1) if match else None
            continue
        if current_name in workspace_crate_names and stripped.startswith("version = "):
            newline = "\n" if line.endswith("\n") else ""
            lines[index] = f'version = "{version}"{newline}'
    return "".join(lines)


def planned_updates(repo_root: Path) -> dict[Path, str]:
    version = workspace_package_version(repo_root)
    workspace_crate_names = {
        (repo_root / member / "Cargo.toml")
        for member in workspace_members(repo_root)
    }
    crate_names: set[str] = set()
    for manifest in workspace_crate_names:
        text = read_text(manifest, missing_ok=True)
        match = re.search(r'(?m)^\s*name\s*=\s*"([^"]+)"', text)
        if match:
            crate_names.add(match.group(1))

    updates = {
        repo_root / "python/src/shardloom/_version.py": replace_python_version(
            read_text(repo_root / "python/src/shardloom/_version.py", missing_ok=True),
            version,
        ),
        repo_root / "website-src/package.json": update_json_version(
            repo_root / "website-src/package.json", version, package_lock=False
        ),
        repo_root / "website-src/package-lock.json": update_json_version(
            repo_root / "website-src/package-lock.json", version, package_lock=True
        ),
        repo_root / "Cargo.lock": update_cargo_lock(
            read_text(repo_root / "Cargo.lock", missing_ok=False), crate_names, version
        ),
    }
    return updates


def stale_paths(repo_root: Path, updates: dict[Path, str]) -> list[str]:
    stale: list[str] = []
    for path in updates:
        current = read_text(path, missing_ok=True)
        if current != updates[path]:
            stale.append(path.relative_to(repo_root).as_posix())
    return stale


def main() -> int:
    args = parse_args()
    repo_root = args.repo_root.resolve()
    updates = planned_updates(repo_root)
    stale = stale_paths(repo_root, updates)
    if args.check:
        if stale:
            print("stale derived package version files:")
            for path in stale:
                print(f"- {path}")
            print("run: python3 scripts/sync_workspace_package_versions.py")
            return 1
        print("passed: derived package version files match Cargo.toml")
        return 0

    for path, text in updates.items():
        path.write_text(text, encoding="utf-8")
    print(
        "synced package versions from Cargo.toml to "
        + ", ".join(DERIVED_VERSION_SOURCES)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
