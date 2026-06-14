#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Write manifest-derived version variables for GitHub Actions jobs."""

from __future__ import annotations

import argparse
import os
from pathlib import Path

from release_report_utils import (
    rust_msrv_lane_id,
    rust_toolchain_version,
    upstream_vortex_lock_version,
    upstream_vortex_manifest_version,
    upstream_vortex_provider_version,
)


ROOT = Path(__file__).resolve().parents[1]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=ROOT)
    parser.add_argument(
        "--github-env",
        type=Path,
        default=None,
        help="Append KEY=VALUE lines to this GitHub Actions env file.",
    )
    return parser.parse_args()


def version_env(repo_root: Path) -> dict[str, str]:
    return {
        "SHARDLOOM_RUST_MSRV_TOOLCHAIN": rust_toolchain_version(repo_root),
        "SHARDLOOM_RUST_MSRV_LANE": rust_msrv_lane_id(repo_root),
        "SHARDLOOM_UPSTREAM_VORTEX_MANIFEST_VERSION": upstream_vortex_manifest_version(
            repo_root
        ),
        "SHARDLOOM_UPSTREAM_VORTEX_LOCK_VERSION": upstream_vortex_lock_version(
            repo_root
        ),
        "SHARDLOOM_UPSTREAM_VORTEX_PROVIDER_VERSION": upstream_vortex_provider_version(
            repo_root
        ),
    }


def main() -> int:
    args = parse_args()
    repo_root = args.repo_root.resolve()
    env = version_env(repo_root)
    github_env = args.github_env or (
        Path(os.environ["GITHUB_ENV"]) if os.environ.get("GITHUB_ENV") else None
    )
    lines = [f"{key}={value}" for key, value in env.items()]
    if github_env is not None:
        with github_env.open("a", encoding="utf-8") as handle:
            handle.write("\n".join(lines) + "\n")
    print("\n".join(lines))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
