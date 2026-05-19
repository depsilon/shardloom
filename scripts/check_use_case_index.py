#!/usr/bin/env python
# SPDX-License-Identifier: Apache-2.0
"""Validate docs/use-cases/use-case-index.yml without adding a YAML dependency."""

from __future__ import annotations

import argparse
import ast
import re
import sys
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
INDEX_PATH = REPO_ROOT / "docs" / "use-cases" / "use-case-index.yml"
ALLOWED_STATUSES = {
    "ready_local",
    "smoke_supported",
    "report_only",
    "planned",
    "blocked",
    "unsupported",
}
SUPPORTED_STATUSES = {"ready_local", "smoke_supported"}
EXPLANATION_STATUSES = {"report_only", "planned", "blocked", "unsupported"}
REQUIRED_USE_CASE_FIELDS = {
    "id",
    "title",
    "capability_family",
    "audience",
    "status",
    "execution_mode",
    "engine_mode",
    "inputs",
    "outputs",
    "evidence_fields",
    "claim_boundary",
    "expected_output_evidence",
    "common_mistakes",
    "references",
    "related_use_cases",
}
REQUIRED_FAMILY_FIELDS = {"id", "title"}
FORBIDDEN_CLAIM_PATTERNS = [
    re.compile(pattern, re.IGNORECASE)
    for pattern in [
        r"\bShardLoom is faster\b",
        r"\bShardLoom is better\b",
        r"\bproduction ready\b",
        r"\bproduction-ready\b",
        r"\bShardLoom (?:is|as) (?:a )?Spark replacement\b",
        r"\bPolars cannot\b",
        r"\bDuckDB cannot\b",
        r"\bDataFusion cannot\b",
    ]
]
TEXT_FIELD_LIMITS = {
    "title": 160,
    "claim_boundary": 1200,
    "expected_output_evidence": 1000,
    "blocked_explanation": 1100,
}


def split_inline_list(value: str) -> list[str]:
    parts: list[str] = []
    current: list[str] = []
    quote: str | None = None
    escaped = False
    for char in value:
        if escaped:
            current.append(char)
            escaped = False
            continue
        if char == "\\" and quote == '"':
            current.append(char)
            escaped = True
            continue
        if char in {"'", '"'}:
            current.append(char)
            if quote == char:
                quote = None
            elif quote is None:
                quote = char
            continue
        if char == "," and quote is None:
            parts.append("".join(current).strip())
            current = []
            continue
        current.append(char)
    if current or value.endswith(","):
        parts.append("".join(current).strip())
    return [part for part in parts if part]


def parse_value(raw: str) -> Any:
    value = raw.strip()
    if value.startswith("[") and value.endswith("]"):
        inner = value[1:-1].strip()
        if not inner:
            return []
        return [parse_value(part) for part in split_inline_list(inner)]
    if (value.startswith('"') and value.endswith('"')) or (
        value.startswith("'") and value.endswith("'")
    ):
        try:
            return ast.literal_eval(value)
        except (SyntaxError, ValueError):
            return value[1:-1]
    if value.isdigit():
        return int(value)
    return value


def parse_key_value(line: str, line_number: int) -> tuple[str, Any]:
    if ":" not in line:
        raise ValueError(f"line {line_number}: expected key/value pair")
    key, value = line.split(":", 1)
    key = key.strip()
    if not key:
        raise ValueError(f"line {line_number}: empty key")
    return key, parse_value(value)


def load_index(path: Path = INDEX_PATH) -> dict[str, Any]:
    data: dict[str, Any] = {"capability_families": [], "use_cases": []}
    current_section: str | None = None
    current_item: dict[str, Any] | None = None

    for line_number, raw_line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if not raw_line.strip() or raw_line.lstrip().startswith("#"):
            continue
        indent = len(raw_line) - len(raw_line.lstrip(" "))
        line = raw_line.strip()

        if indent == 0:
            current_item = None
            if line.endswith(":"):
                current_section = line[:-1]
                data.setdefault(current_section, [])
                continue
            key, value = parse_key_value(line, line_number)
            data[key] = value
            current_section = None
            continue

        if current_section not in {"capability_families", "use_cases"}:
            raise ValueError(f"line {line_number}: nested data outside a supported list")

        if indent == 2 and line.startswith("- "):
            current_item = {}
            data[current_section].append(current_item)
            rest = line[2:].strip()
            if rest:
                key, value = parse_key_value(rest, line_number)
                current_item[key] = value
            continue

        if indent == 4 and current_item is not None:
            key, value = parse_key_value(line, line_number)
            current_item[key] = value
            continue

        raise ValueError(f"line {line_number}: unsupported indentation or structure")

    return data


def as_list(value: Any, label: str, blockers: list[str]) -> list[Any]:
    if not isinstance(value, list):
        blockers.append(f"{label} must be an inline list")
        return []
    return value


def validate_index(data: dict[str, Any], repo_root: Path) -> list[str]:
    blockers: list[str] = []
    families = as_list(data.get("capability_families"), "capability_families", blockers)
    use_cases = as_list(data.get("use_cases"), "use_cases", blockers)
    declared_statuses = set(as_list(data.get("statuses"), "statuses", blockers))

    if declared_statuses != ALLOWED_STATUSES:
        blockers.append(f"statuses must be exactly {sorted(ALLOWED_STATUSES)}")

    family_ids: set[str] = set()
    for family in families:
        if not isinstance(family, dict):
            blockers.append("capability family entry must be a mapping")
            continue
        missing = REQUIRED_FAMILY_FIELDS - family.keys()
        if missing:
            blockers.append(f"family {family.get('id', '<missing-id>')} missing {sorted(missing)}")
        family_id = family.get("id")
        if not isinstance(family_id, str):
            blockers.append("family id must be a string")
            continue
        if family_id in family_ids:
            blockers.append(f"duplicate family id: {family_id}")
        family_ids.add(family_id)

    use_case_ids: set[str] = set()
    for use_case in use_cases:
        if not isinstance(use_case, dict):
            blockers.append("use case entry must be a mapping")
            continue
        use_case_id = str(use_case.get("id", "<missing-id>"))
        missing = REQUIRED_USE_CASE_FIELDS - use_case.keys()
        if missing:
            blockers.append(f"use case {use_case_id} missing {sorted(missing)}")
        if use_case_id in use_case_ids:
            blockers.append(f"duplicate use case id: {use_case_id}")
        use_case_ids.add(use_case_id)

        family = use_case.get("capability_family")
        if family not in family_ids:
            blockers.append(f"use case {use_case_id} maps to unknown family {family!r}")

        status = use_case.get("status")
        if status not in ALLOWED_STATUSES:
            blockers.append(f"use case {use_case_id} has unsupported status {status!r}")
        if status in SUPPORTED_STATUSES and not use_case.get("runnable_example"):
            blockers.append(f"use case {use_case_id} status {status} requires runnable_example")
        if status in EXPLANATION_STATUSES and not use_case.get("blocked_explanation"):
            blockers.append(f"use case {use_case_id} status {status} requires blocked_explanation")

        for field in ("inputs", "outputs", "evidence_fields", "common_mistakes", "references", "related_use_cases"):
            values = as_list(use_case.get(field), f"{use_case_id}.{field}", blockers)
            if field != "related_use_cases" and not values:
                blockers.append(f"use case {use_case_id} field {field} must not be empty")

        if not str(use_case.get("claim_boundary", "")).strip():
            blockers.append(f"use case {use_case_id} requires claim_boundary")
        for field, limit in TEXT_FIELD_LIMITS.items():
            text = str(use_case.get(field, ""))
            if text and len(text) > limit:
                blockers.append(
                    f"use case {use_case_id} field {field} is too long: {len(text)} > {limit}"
                )
        if not as_list(
            use_case.get("related_use_cases"),
            f"{use_case_id}.related_use_cases",
            blockers,
        ):
            blockers.append(f"use case {use_case_id} must link at least one related use case")

        searchable_text = " ".join(str(value) for value in use_case.values())
        for pattern in FORBIDDEN_CLAIM_PATTERNS:
            if pattern.search(searchable_text):
                blockers.append(f"use case {use_case_id} contains forbidden claim phrase: {pattern.pattern}")

        for reference in as_list(use_case.get("references"), f"{use_case_id}.references", blockers):
            if not isinstance(reference, str):
                blockers.append(f"use case {use_case_id} reference must be a string")
                continue
            if "*" in reference:
                blockers.append(f"use case {use_case_id} reference must be exact, not a glob: {reference}")
                continue
            if not (repo_root / reference).exists():
                blockers.append(f"use case {use_case_id} reference does not exist: {reference}")

    for use_case in use_cases:
        if not isinstance(use_case, dict):
            continue
        use_case_id = str(use_case.get("id", "<missing-id>"))
        for related in as_list(use_case.get("related_use_cases"), f"{use_case_id}.related_use_cases", blockers):
            if related not in use_case_ids:
                blockers.append(f"use case {use_case_id} has unknown related use case {related!r}")

    return blockers


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=REPO_ROOT)
    parser.add_argument("--index", type=Path, default=INDEX_PATH)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    repo_root = args.repo_root.resolve()
    index_path = args.index if args.index.is_absolute() else repo_root / args.index
    try:
        data = load_index(index_path)
    except ValueError as error:
        print(f"use-case index parse failed: {error}", file=sys.stderr)
        return 1

    blockers = validate_index(data, repo_root)
    if blockers:
        print("use-case index validation failed:", file=sys.stderr)
        for blocker in blockers:
            print(f"- {blocker}", file=sys.stderr)
        return 1

    print(f"use-case index ok: {len(data['use_cases'])} use cases, {len(data['capability_families'])} families")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
