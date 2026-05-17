#!/usr/bin/env python
# SPDX-License-Identifier: Apache-2.0
"""Validate Use Case Atlas generated pages and reference backlink ledger."""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

from check_use_case_index import INDEX_PATH, REPO_ROOT, load_index, validate_index


BACKLINKS = REPO_ROOT / "docs" / "use-cases" / "reference-backlinks.md"
GENERATED = REPO_ROOT / "docs" / "use-cases" / "generated"


def values(use_case: dict[str, object], field: str) -> list[str]:
    value = use_case.get(field)
    if isinstance(value, list):
        return [str(item) for item in value]
    if value is None:
        return []
    return [str(value)]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=REPO_ROOT)
    parser.add_argument("--index", type=Path, default=INDEX_PATH)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    repo_root = args.repo_root.resolve()
    index_path = args.index if args.index.is_absolute() else repo_root / args.index
    data = load_index(index_path)
    blockers = validate_index(data, repo_root)

    backlink_text = BACKLINKS.read_text(encoding="utf-8") if BACKLINKS.exists() else ""
    if not backlink_text:
        blockers.append("missing docs/use-cases/reference-backlinks.md")

    for use_case in data.get("use_cases", []):
        if not isinstance(use_case, dict):
            continue
        use_case_id = str(use_case["id"])
        page = GENERATED / f"{use_case_id}.md"
        if not page.exists():
            blockers.append(f"missing generated use-case page: {page.relative_to(repo_root).as_posix()}")
            continue
        text = page.read_text(encoding="utf-8")
        if "## Reference Files" not in text:
            blockers.append(f"generated page missing Reference Files block: {use_case_id}")
        for reference in values(use_case, "references"):
            if f"`{reference}`" not in text:
                blockers.append(f"generated page {use_case_id} missing reference: {reference}")
            if reference not in backlink_text:
                blockers.append(f"backlink ledger missing reference: {reference}")
        if not re.search(rf"\b{re.escape(use_case_id)}\b", backlink_text):
            blockers.append(f"backlink ledger missing use case id: {use_case_id}")

    if blockers:
        print("use-case backlink validation failed:", file=sys.stderr)
        for blocker in blockers:
            print(f"- {blocker}", file=sys.stderr)
        return 1
    print("use-case backlinks ok")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
