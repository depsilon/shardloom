#!/usr/bin/env python
# SPDX-License-Identifier: Apache-2.0
"""Validate non-expert Use Case Field Guide glossary coverage."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
GLOSSARY = REPO_ROOT / "docs" / "use-cases" / "field-guide" / "README.md"
REQUIRED_TERMS = [
    "execution mode",
    "engine mode",
    "Vortex-native",
    "compatibility import",
    "prepared Vortex",
    "native Vortex",
    "direct transient",
    "no fallback",
    "materialization boundary",
    "Native I/O certificate",
    "result-sink replay",
    "claim gate",
    "fixture smoke",
    "report-only",
    "external baseline",
    "residual-native",
    "encoded-native",
    "source-state reuse",
    "output-plan reuse",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=REPO_ROOT)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    repo_root = args.repo_root.resolve()
    glossary = repo_root / GLOSSARY.relative_to(REPO_ROOT)
    blockers: list[str] = []
    if not glossary.exists():
        blockers.append("missing docs/use-cases/field-guide/README.md")
    else:
        text = glossary.read_text(encoding="utf-8")
        for term in REQUIRED_TERMS:
            if term not in text:
                blockers.append(f"missing glossary term: {term}")
        required_headers = [
            "One-Sentence Explanation",
            "Why It Matters",
            "How To Inspect It",
            "Related Use Cases",
            "Reference Files",
        ]
        for header in required_headers:
            if header not in text:
                blockers.append(f"missing glossary column: {header}")

    if blockers:
        print("use-case glossary validation failed:", file=sys.stderr)
        for blocker in blockers:
            print(f"- {blocker}", file=sys.stderr)
        return 1
    print("use-case glossary ok")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
