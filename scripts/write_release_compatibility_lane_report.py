#!/usr/bin/env python
# SPDX-License-Identifier: Apache-2.0
"""Write a small release-compatibility matrix lane report.

This is CI evidence only. It records the matrix lane after the lane's own
commands have already run successfully. It does not publish packages, create
tags, use secrets, invoke external engines, or authorize fallback execution.
"""

from __future__ import annotations

import argparse
import json
import platform
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_VERSION = "shardloom.release_compatibility_lane_report.v1"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=ROOT)
    parser.add_argument("--lane", required=True)
    parser.add_argument("--surface", choices=("python", "rust"), required=True)
    parser.add_argument("--python-version")
    parser.add_argument("--rust-toolchain")
    parser.add_argument("--os-name")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("target/release-compatibility"),
    )
    return parser.parse_args()


def resolve(repo_root: Path, path: Path) -> Path:
    return path if path.is_absolute() else repo_root / path


def main() -> int:
    args = parse_args()
    repo_root = args.repo_root.resolve()
    output_dir = resolve(repo_root, args.output_dir)
    output = output_dir / f"{args.lane}.json"
    payload: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "lane": args.lane,
        "surface": args.surface,
        "status": "passed",
        "os_name": args.os_name or platform.system(),
        "python_version": args.python_version,
        "runtime_python_version": platform.python_version(),
        "rust_toolchain": args.rust_toolchain,
        "package_publication_performed": False,
        "publication_attempted": False,
        "tag_created": False,
        "secrets_required": False,
        "fallback_attempted": False,
        "external_engine_invoked": False,
        "argv_python": sys.executable,
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
