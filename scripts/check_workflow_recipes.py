#!/usr/bin/env python
# SPDX-License-Identifier: Apache-2.0
"""Validate the claim-safe workflow recipe library."""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
INDEX_PATH = ROOT / "docs" / "use-cases" / "recipes" / "recipe-index.json"
README_PATH = ROOT / "docs" / "use-cases" / "recipes" / "README.md"
SCHEMA_VERSION = "shardloom.workflow_recipe_library.v1"
REPORT_SCHEMA_VERSION = "shardloom.workflow_recipe_library_report.v1"

REQUIRED_RECIPE_IDS = {
    "no-dataset-smoke",
    "local-csv-certified-result",
    "local-parquet-certified-result",
    "prepared-vortex-batch-run",
    "native-vortex-input",
    "source-free-generated-reference-table",
    "dirty-csv-cleanup",
    "nested-json-scan",
    "cdc-overlay",
    "output-fanout",
    "object-store-blocked-diagnostic",
    "foundry-dev-stack-smoke",
    "benchmark-evidence-interpretation",
}
SUPPORTED_STATUSES = {"ready_local", "smoke_supported"}
EXPLANATION_STATUSES = {"report_only", "planned", "blocked", "unsupported"}
REQUIRED_FALSE_FIELDS = ["fallback_attempted", "external_engine_invoked"]
REQUIRED_RECIPE_FIELDS = {
    "id",
    "title",
    "status",
    "use_case_id",
    "user_goal",
    "command",
    "expected_output",
    "evidence_fields",
    "claim_boundary",
    "references",
}
FORBIDDEN_CLAIM_PATTERNS = [
    re.compile(pattern, re.IGNORECASE)
    for pattern in [
        r"\bShardLoom is faster\b",
        r"\bShardLoom is better\b",
        r"\bproduction ready\b",
        r"\bproduction-ready\b",
        r"\bSpark replacement\b",
        r"\bPolars cannot\b",
        r"\bDuckDB cannot\b",
        r"\bDataFusion cannot\b",
    ]
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=ROOT)
    parser.add_argument("--index", type=Path, default=INDEX_PATH)
    parser.add_argument("--readme", type=Path, default=README_PATH)
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("target/workflow-recipe-library-report.json"),
    )
    return parser.parse_args()


def resolve(repo_root: Path, path: Path) -> Path:
    return path if path.is_absolute() else repo_root / path


def as_non_empty_list(value: Any, label: str, blockers: list[str]) -> list[Any]:
    if not isinstance(value, list) or not value:
        blockers.append(f"{label} must be a non-empty list")
        return []
    return value


def parse_use_case_ids(path: Path) -> set[str]:
    ids: set[str] = set()
    if not path.exists():
        return ids
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if stripped.startswith("- id:"):
            ids.add(stripped.split(":", 1)[1].strip())
    return ids


def validate_index(data: dict[str, Any], repo_root: Path) -> list[str]:
    blockers: list[str] = []
    if data.get("schema_version") != SCHEMA_VERSION:
        blockers.append(f"schema_version={data.get('schema_version')}")
    if data.get("gar_id") != "GAR-COMMERCIAL-1F":
        blockers.append(f"gar_id={data.get('gar_id')}")
    if data.get("status") != "report_only_documentation_surface":
        blockers.append(f"status={data.get('status')}")
    if data.get("claim_gate_status") != "not_claim_grade":
        blockers.append(f"claim_gate_status={data.get('claim_gate_status')}")
    for field in REQUIRED_FALSE_FIELDS:
        if data.get(field) is not False:
            blockers.append(f"{field} must be false")

    use_case_ids = parse_use_case_ids(repo_root / "docs" / "use-cases" / "use-case-index.yml")
    recipes = as_non_empty_list(data.get("recipes"), "recipes", blockers)
    recipe_ids: set[str] = set()

    for recipe in recipes:
        if not isinstance(recipe, dict):
            blockers.append("recipe entries must be objects")
            continue
        recipe_id = str(recipe.get("id", "<missing-id>"))
        missing = REQUIRED_RECIPE_FIELDS - set(recipe)
        if missing:
            blockers.append(f"{recipe_id}: missing {sorted(missing)}")
        if recipe_id in recipe_ids:
            blockers.append(f"duplicate recipe id: {recipe_id}")
        recipe_ids.add(recipe_id)

        status = recipe.get("status")
        if status in SUPPORTED_STATUSES and not str(recipe.get("command", "")).strip():
            blockers.append(f"{recipe_id}: supported/smoke recipe requires command")
        if status in EXPLANATION_STATUSES and not str(recipe.get("blocked_explanation", "")).strip():
            blockers.append(f"{recipe_id}: {status} recipe requires blocked_explanation")
        if recipe.get("use_case_id") not in use_case_ids:
            blockers.append(f"{recipe_id}: unknown use_case_id {recipe.get('use_case_id')!r}")

        evidence = as_non_empty_list(recipe.get("evidence_fields"), f"{recipe_id}.evidence_fields", blockers)
        if not any(str(field) == "fallback_attempted=false" for field in evidence):
            blockers.append(f"{recipe_id}: missing fallback_attempted=false evidence field")
        if not any(str(field) == "external_engine_invoked=false" for field in evidence):
            blockers.append(f"{recipe_id}: missing external_engine_invoked=false evidence field")

        if not str(recipe.get("claim_boundary", "")).strip():
            blockers.append(f"{recipe_id}: missing claim_boundary")
        references = as_non_empty_list(recipe.get("references"), f"{recipe_id}.references", blockers)
        for reference in references:
            if not isinstance(reference, str):
                blockers.append(f"{recipe_id}: reference must be a string")
                continue
            if "*" in reference:
                blockers.append(f"{recipe_id}: reference must be exact, not a glob: {reference}")
                continue
            if not (repo_root / reference).exists():
                blockers.append(f"{recipe_id}: missing reference file {reference}")

        searchable_text = json.dumps(recipe, sort_keys=True)
        for pattern in FORBIDDEN_CLAIM_PATTERNS:
            if pattern.search(searchable_text):
                blockers.append(f"{recipe_id}: forbidden claim phrase {pattern.pattern}")

    missing = REQUIRED_RECIPE_IDS - recipe_ids
    extra = recipe_ids - REQUIRED_RECIPE_IDS
    if missing or extra:
        blockers.append(f"recipe id set mismatch: missing={sorted(missing)} extra={sorted(extra)}")
    return blockers


def validate_readme(readme: str, data: dict[str, Any]) -> list[str]:
    blockers: list[str] = []
    if not readme:
        return ["missing recipe README"]
    for required in [
        "shardloom.workflow_recipe_library.v1",
        "python scripts\\check_workflow_recipes.py",
        "fallback_attempted=false",
        "external_engine_invoked=false",
    ]:
        if required not in readme:
            blockers.append(f"README missing {required}")
    for recipe in data.get("recipes", []):
        if not isinstance(recipe, dict):
            continue
        heading = f"## {recipe.get('title')}"
        if heading not in readme:
            blockers.append(f"README missing recipe heading {heading}")
    return blockers


def main() -> int:
    args = parse_args()
    repo_root = args.repo_root.resolve()
    index_path = resolve(repo_root, args.index)
    readme_path = resolve(repo_root, args.readme)
    output_path = resolve(repo_root, args.output)
    data = json.loads(index_path.read_text(encoding="utf-8")) if index_path.exists() else {}
    readme = readme_path.read_text(encoding="utf-8") if readme_path.exists() else ""
    blockers = validate_index(data, repo_root)
    blockers.extend(validate_readme(readme, data))

    report = {
        "schema_version": REPORT_SCHEMA_VERSION,
        "recipe_index": str(args.index).replace("\\", "/"),
        "recipe_count": len(data.get("recipes", [])) if isinstance(data.get("recipes"), list) else 0,
        "status": "passed" if not blockers else "failed",
        "claim_gate_status": data.get("claim_gate_status", "missing"),
        "fallback_attempted": data.get("fallback_attempted", False),
        "external_engine_invoked": data.get("external_engine_invoked", False),
        "blockers": blockers,
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(output_path)
    return 0 if not blockers else 1


if __name__ == "__main__":
    raise SystemExit(main())
