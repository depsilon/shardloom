from __future__ import annotations

import argparse
from pathlib import Path

from shardloom import ShardLoomClient


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run ShardLoom's current live ETL smoke surface through the Python client."
    )
    parser.add_argument("--repo-root", type=Path, default=Path.cwd())
    parser.add_argument("--shardloom-bin")
    parser.add_argument("--mode", choices=("csv", "vortex"), default="csv")
    parser.add_argument("--scenario", default="csv/file ingest")
    parser.add_argument("--fact", required=True, help="Fact CSV or Vortex input path.")
    parser.add_argument("--dim", required=True, help="Dimension CSV or Vortex input path.")
    parser.add_argument("--workspace", help="CSV mode workspace for temporary Vortex files.")
    parser.add_argument(
        "--dynamic-profile",
        choices=("balanced", "memory-pressure", "object-store-throttled", "small-tasks"),
        default="balanced",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.shardloom_bin:
        client = ShardLoomClient(binary=args.shardloom_bin)
    else:
        client = ShardLoomClient.from_repo(args.repo_root)

    capabilities = client.capabilities("python")
    dynamic = client.dynamic_work_shaping_plan(args.dynamic_profile)
    etl = client.live_etl_smoke(
        args.scenario,
        args.fact,
        args.dim,
        input_format=args.mode,
        workspace=args.workspace,
    )

    print(f"python capability status: {capabilities.status}")
    print(f"dynamic profile: {dynamic.field('profile', args.dynamic_profile)}")
    print(f"etl command: {etl.command}")
    print(f"etl status: {etl.status}")
    print(f"fallback attempted: {etl.fallback.attempted}")
    print(f"rows scanned: {etl.field('rows_scanned', 'unknown')}")
    print(f"rows materialized: {etl.field('rows_materialized', 'unknown')}")
    print(
        "materialization boundary reported: "
        f"{etl.field('materialization_boundary_reported', 'unknown')}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
