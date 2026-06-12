#!/usr/bin/env python
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Sequence

from scenario_support import (
    build_run_paths,
    run_scenarios,
    write_json,
    write_timing_markdown,
)


DEFAULT_RUN_ROOT = Path("target/local-python-benchmark-scenarios")


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run ShardLoom Python benchmark scenarios and emit timing components."
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
    parser.add_argument("--output-json", type=Path)
    parser.add_argument("--output-md", type=Path)
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
    output_json = (
        args.output_json.resolve()
        if args.output_json is not None
        else paths["timing_json"]
    )
    output_md = (
        args.output_md.resolve()
        if args.output_md is not None
        else paths["timing_markdown"]
    )
    write_json(output_json, payload)  # type: ignore[arg-type]
    write_timing_markdown(output_md, payload)  # type: ignore[arg-type]

    print(f"timing_components_json={output_json}")
    print(f"timing_components_markdown={output_md}")
    print(f"scenario_run_dir={paths['run_dir']}")
    for result in payload["results"]:
        components = result.get("timing_components", {})
        print(
            "scenario={name} status={status} python_wall_millis={wall} "
            "timing_scope={scope} output_format={output}".format(
                name=result["name"],
                status="ok" if result["ok"] else "check",
                wall=components.get("python_wall_millis"),
                scope=result.get("timing_scope") or "n/a",
                output=result.get("output_format") or "n/a",
            )
        )
    return 0 if payload["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
