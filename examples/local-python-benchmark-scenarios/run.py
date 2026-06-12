#!/usr/bin/env python
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Sequence

from scenario_support import build_run_paths, run_scenarios, write_json


DEFAULT_RUN_ROOT = Path("target/local-python-benchmark-scenarios")


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the documented ShardLoom Python benchmark scenario snippets locally."
    )
    parser.add_argument("--repo-root", type=Path, default=Path.cwd())
    parser.add_argument("--run-root", type=Path, default=DEFAULT_RUN_ROOT)
    parser.add_argument("--run-id")
    parser.add_argument("--shardloom-bin")
    parser.add_argument(
        "--profile-order",
        default="release,debug",
        help="Comma-separated source-checkout build profiles to try. Defaults to release,debug.",
    )
    return parser.parse_args(argv)


def profile_order(value: str) -> tuple[str, ...]:
    values = tuple(part.strip() for part in value.split(",") if part.strip())
    if not values:
        raise ValueError("--profile-order must include at least one profile")
    return values


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    repo_root = args.repo_root.resolve()
    paths = build_run_paths(
        repo_root,
        run_root=args.run_root,
        run_id=args.run_id,
    )
    payload = run_scenarios(
        repo_root=repo_root,
        run_dir=paths["run_dir"],  # type: ignore[arg-type]
        binary=args.shardloom_bin,
        profile_order=profile_order(args.profile_order),
    )
    summary_json = paths["summary_json"]
    write_json(summary_json, payload)  # type: ignore[arg-type]

    print(f"scenario_summary_json={summary_json}")
    print(f"scenario_run_dir={paths['run_dir']}")
    for result in payload["results"]:
        status = "ok" if result["ok"] else "check"
        expected = "expected_error" if result["expected_error"] else "success"
        print(
            "scenario={name} status={status} expected={expected} "
            "command_status={command_status} rows={rows} fallback_attempted={fallback} "
            "external_engine_invoked={external}".format(
                name=result["name"],
                status=status,
                expected=expected,
                command_status=result["status"],
                rows=result.get("output_row_count"),
                fallback=str(result.get("fallback_attempted")).lower(),
                external=str(result.get("external_engine_invoked")).lower(),
            )
        )
    return 0 if payload["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
