#!/usr/bin/env python
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import argparse
import sys
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run ShardLoom's local Python smoke.")
    parser.add_argument("--repo-root", type=Path, default=Path.cwd())
    parser.add_argument("--shardloom-bin")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    repo_root = args.repo_root.resolve()
    sys.path.insert(0, str(repo_root / "python" / "src"))

    from shardloom import ShardLoomClient

    client = (
        ShardLoomClient(binary=args.shardloom_bin)
        if args.shardloom_bin
        else ShardLoomClient.from_repo(repo_root)
    )
    status = client.status()
    smoke = client.smoke_check()
    capabilities = client.capabilities()

    print(f"status: {status.status}")
    print(f"protocol: {smoke.protocol_version}")
    print(f"cli: {smoke.resolved_cli_path}")
    print(f"capabilities command: {capabilities.command}")
    print(f"fallback attempted: {smoke.fallback_attempted}")
    return 1 if smoke.fallback_attempted else 0


if __name__ == "__main__":
    raise SystemExit(main())
