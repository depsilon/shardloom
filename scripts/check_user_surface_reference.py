#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Validate the agent-facing ShardLoom user-surface index."""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_VERSION = "shardloom.user_surface_index.v1"
MD_PATH = Path("docs/reference/shardloom-user-surface-index.md")
JSON_PATH = Path("docs/reference/shardloom-user-surface-index.json")
COMMAND_REGISTRY_PATH = Path("shardloom-cli/src/command_registry.rs")

REQUIRED_MD_MARKERS = (
    "Schema marker: `shardloom.user_surface_index.v1`",
    "shardloom --version",
    "shardloom command-metadata --format json",
    "shardloom agent-contract-pack --format json",
    "ctx.front_door_semantic_surface_matrix()",
    "ctx.read(path)",
    "ctx.sql(\"SELECT ...\")",
    "fallback_attempted=false",
    "external_engine_invoked=false",
    "Hidden fallback execution in DuckDB, DataFusion, Spark, Polars, pandas",
)

REQUIRED_BACKLINKS = {
    "README.md": (
        "docs/reference/shardloom-user-surface-index.md",
        "docs/reference/shardloom-user-surface-index.json",
    ),
    "python/README.md": (
        "docs/reference/shardloom-user-surface-index.md",
        "docs/reference/shardloom-user-surface-index.json",
    ),
    "docs/architecture/agent-contract-pack.md": (
        "docs/reference/shardloom-user-surface-index.md",
        "docs/reference/shardloom-user-surface-index.json",
    ),
    "docs/architecture/v1-front-door-runtime-scope.md": (
        "docs/reference/shardloom-user-surface-index.md",
        "docs/reference/shardloom-user-surface-index.json",
    ),
    "docs/skills/developer-agent-experience.md": (
        "docs/reference/shardloom-user-surface-index.md",
        "docs/reference/shardloom-user-surface-index.json",
    ),
}

REQUIRED_JSON_POINTERS = (
    "cli.version_command",
    "cli.exhaustive_inventory_command",
    "cli.agent_contract_pack_command",
    "python.context_readers",
    "python.query_builder_methods",
    "semantic_claim_surface.agent_source",
    "semantic_claim_surface.disallowed_broad_claims",
    "sql.entrypoints",
    "guardrails.no_fallback_policy",
)

REQUIRED_COMMANDS = (
    "command-metadata",
    "help",
    "agent-contract-pack",
    "capabilities",
    "route",
    "run",
    "prepare",
    "local-source-runtime",
    "generated-source-sql",
    "vortex-prepare",
)

REQUIRED_PYTHON_METHODS = (
    "ctx.read",
    "ctx.read_csv",
    "ctx.read_json",
    "ctx.read_parquet",
    "ctx.read_arrow_ipc",
    "ctx.read_avro",
    "ctx.read_orc",
    "ctx.read_vortex",
    "filter",
    "select",
    "group_by",
    "join",
    "collect",
    "write_jsonl",
    "write_vortex",
)

REQUIRED_SQL_ENTRYPOINTS = (
    "ctx.sql",
    "sl.sql",
    "shardloom local-source-runtime --format json",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=ROOT)
    return parser.parse_args()


def read_text(repo_root: Path, path: Path) -> str:
    return (repo_root / path).read_text(encoding="utf-8")


def load_json(repo_root: Path, path: Path) -> dict[str, Any]:
    return json.loads(read_text(repo_root, path))


def registry_commands(repo_root: Path) -> list[str]:
    source = read_text(repo_root, COMMAND_REGISTRY_PATH)
    match = re.search(
        r"REGISTERED_COMMANDS:\s*&\[[^\]]+\]\s*=\s*&\[(?P<body>.*?)\];",
        source,
        flags=re.S,
    )
    if match is None:
        raise ValueError("could not locate REGISTERED_COMMANDS in command registry")
    return re.findall(r'"([^"]+)"', match.group("body"))


def nested_get(payload: dict[str, Any], pointer: str) -> Any:
    current: Any = payload
    for part in pointer.split("."):
        if not isinstance(current, dict) or part not in current:
            raise KeyError(pointer)
        current = current[part]
    return current


def validate(repo_root: Path) -> tuple[dict[str, Any], list[str]]:
    blockers: list[str] = []
    md = read_text(repo_root, MD_PATH)
    payload = load_json(repo_root, JSON_PATH)
    commands = registry_commands(repo_root)

    if payload.get("schema_version") != SCHEMA_VERSION:
        blockers.append(f"{JSON_PATH}: schema_version must be {SCHEMA_VERSION}")
    if payload.get("canonical_human_reference") != MD_PATH.as_posix():
        blockers.append(f"{JSON_PATH}: canonical_human_reference must point at {MD_PATH}")
    if payload.get("fallback_attempted") is not False:
        blockers.append(f"{JSON_PATH}: fallback_attempted must be false")
    if payload.get("external_engine_invoked") is not False:
        blockers.append(f"{JSON_PATH}: external_engine_invoked must be false")

    for marker in REQUIRED_MD_MARKERS:
        if marker not in md:
            blockers.append(f"{MD_PATH}: missing marker {marker!r}")

    for path_raw, markers in REQUIRED_BACKLINKS.items():
        path = Path(path_raw)
        text = read_text(repo_root, path)
        for marker in markers:
            if marker not in text:
                blockers.append(f"{path}: missing user-surface index backlink {marker}")

    for pointer in REQUIRED_JSON_POINTERS:
        try:
            value = nested_get(payload, pointer)
        except KeyError:
            blockers.append(f"{JSON_PATH}: missing {pointer}")
            continue
        if value in ("", [], {}, None):
            blockers.append(f"{JSON_PATH}: {pointer} must not be empty")

    reported_count = nested_get(payload, "cli.registered_command_count")
    if reported_count != len(commands):
        blockers.append(
            f"{JSON_PATH}: cli.registered_command_count={reported_count} "
            f"does not match registry count {len(commands)}"
        )

    command_set = set(commands)
    for command in REQUIRED_COMMANDS:
        if command not in command_set:
            blockers.append(f"{COMMAND_REGISTRY_PATH}: missing registered command {command}")

    if payload["dynamic_sources"]["cli_command_registry_source"] != COMMAND_REGISTRY_PATH.as_posix():
        blockers.append(f"{JSON_PATH}: CLI registry source path drifted")

    python_surface = payload["python"]
    all_python_values = {
        value
        for key in (
            "context_readers",
            "query_builder_methods",
            "bounded_inspection_and_materialization",
            "expression_helpers",
            "source_free_helpers",
            "capability_and_diagnostic_methods",
        )
        for value in python_surface.get(key, [])
    }
    for method in REQUIRED_PYTHON_METHODS:
        if method not in all_python_values:
            blockers.append(f"{JSON_PATH}: missing Python surface {method}")

    sql_entrypoints = set(payload["sql"].get("entrypoints", []))
    for entrypoint in REQUIRED_SQL_ENTRYPOINTS:
        if entrypoint not in sql_entrypoints:
            blockers.append(f"{JSON_PATH}: missing SQL entrypoint {entrypoint}")

    guardrails = payload["guardrails"]
    for field in (
        "no_fallback_policy",
        "metadata_first_discovery",
        "unsupported_paths_fail_closed",
        "public_claims_require_dynamic_evidence",
    ):
        if guardrails.get(field) is not True:
            blockers.append(f"{JSON_PATH}: guardrails.{field} must be true")
    for field in (
        "production_claim_allowed",
        "performance_claim_allowed",
        "broad_sql_dataframe_parity_claim_allowed",
    ):
        if guardrails.get(field) is not False:
            blockers.append(f"{JSON_PATH}: guardrails.{field} must be false")

    report = {
        "schema_version": "shardloom.user_surface_index_gate.v1",
        "checked_files": [
            MD_PATH.as_posix(),
            JSON_PATH.as_posix(),
            COMMAND_REGISTRY_PATH.as_posix(),
            *REQUIRED_BACKLINKS.keys(),
        ],
        "registered_command_count": len(commands),
        "fallback_attempted": False,
        "external_engine_invoked": False,
        "blockers": blockers,
        "status": "passed" if not blockers else "failed",
    }
    return report, blockers


def main() -> int:
    args = parse_args()
    report, blockers = validate(args.repo_root)
    print(json.dumps(report, indent=2, sort_keys=True))
    if blockers:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
