#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Write manifest-derived version variables for GitHub Actions jobs."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from release_report_utils import (
    workspace_version_env,
)


ROOT = Path(__file__).resolve().parents[1]
OUTPUT_FORMATS = ("env", "json", "posix", "powershell")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=ROOT)
    parser.add_argument(
        "--github-env",
        type=Path,
        default=None,
        help="Append KEY=VALUE lines to this GitHub Actions env file.",
    )
    parser.add_argument(
        "--format",
        choices=OUTPUT_FORMATS,
        default="env",
        help=(
            "Output format for stdout. The GitHub env file is always written as "
            "KEY=VALUE lines."
        ),
    )
    return parser.parse_args()


def version_env(repo_root: Path) -> dict[str, str]:
    return workspace_version_env(repo_root)


def posix_quote(value: str) -> str:
    return "'" + value.replace("'", "'\"'\"'") + "'"


def powershell_quote(value: str) -> str:
    return '"' + value.replace("`", "``").replace('"', '`"') + '"'


def env_lines(env: dict[str, str]) -> list[str]:
    return [f"{key}={value}" for key, value in env.items()]


def format_env(env: dict[str, str], output_format: str) -> str:
    if output_format == "env":
        return "\n".join(env_lines(env))
    if output_format == "json":
        return json.dumps(env, indent=2, sort_keys=True)
    if output_format == "posix":
        return "\n".join(f"export {key}={posix_quote(value)}" for key, value in env.items())
    if output_format == "powershell":
        return "\n".join(
            f"$env:{key} = {powershell_quote(value)}" for key, value in env.items()
        )
    raise ValueError(f"unsupported output format: {output_format}")


def main() -> int:
    args = parse_args()
    repo_root = args.repo_root.resolve()
    env = version_env(repo_root)
    github_env = args.github_env or (
        Path(os.environ["GITHUB_ENV"]) if os.environ.get("GITHUB_ENV") else None
    )
    if github_env is not None:
        with github_env.open("a", encoding="utf-8") as handle:
            handle.write("\n".join(env_lines(env)) + "\n")
    print(format_env(env, args.format))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
