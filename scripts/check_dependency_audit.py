#!/usr/bin/env python
# SPDX-License-Identifier: Apache-2.0
"""Run ShardLoom dependency audit tools when they are installed.

This script is release/check tooling only. It does not add runtime
dependencies, publish packages, or authorize fallback engines.
"""

from __future__ import annotations

import argparse
import importlib.util
import shutil
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
# Required cargo-deny release check: cargo deny check licenses advisories bans sources.


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--strict-missing",
        action="store_true",
        help="Fail when an optional audit tool is not installed.",
    )
    parser.add_argument(
        "--include-cargo-audit",
        action="store_true",
        help="Run cargo audit when cargo-audit is installed.",
    )
    parser.add_argument(
        "--include-python-packaging",
        action="store_true",
        help="Run pip-audit for the current packaging/dev Python environment.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    failures = 0

    failures += run_external_tool(
        label="cargo-deny",
        executable="cargo-deny",
        command=["cargo", "deny", "check", "licenses", "advisories", "bans", "sources"],
        install_hint="cargo install cargo-deny --locked",
        strict_missing=args.strict_missing,
    )

    if args.include_cargo_audit:
        failures += run_external_tool(
            label="cargo-audit",
            executable="cargo-audit",
            command=["cargo", "audit"],
            install_hint="cargo install cargo-audit --locked",
            strict_missing=args.strict_missing,
        )
    else:
        print("SKIP cargo-audit: optional until the maintainer adds it as a release gate")

    if args.include_python_packaging:
        failures += run_pip_audit(strict_missing=args.strict_missing)
    else:
        print(
            "SKIP pip-audit: use --include-python-packaging only in packaging/dev "
            "environments, not as a ShardLoom runtime dependency assumption"
        )

    return 1 if failures else 0


def run_external_tool(
    *,
    label: str,
    executable: str,
    command: list[str],
    install_hint: str,
    strict_missing: bool,
) -> int:
    if shutil.which(executable) is None:
        print(f"SKIP {label}: install with `{install_hint}`")
        return 1 if strict_missing else 0

    print(f"RUN {' '.join(command)}")
    return subprocess.run(command, cwd=ROOT, check=False).returncode


def run_pip_audit(*, strict_missing: bool) -> int:
    if importlib.util.find_spec("pip_audit") is None:
        print("SKIP pip-audit: install in a packaging/dev env with `python -m pip install pip-audit`")
        return 1 if strict_missing else 0

    command = [sys.executable, "-m", "pip_audit"]
    print(f"RUN {' '.join(command)}")
    return subprocess.run(command, cwd=ROOT, check=False).returncode


if __name__ == "__main__":
    raise SystemExit(main())
