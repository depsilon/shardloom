#!/usr/bin/env python
# SPDX-License-Identifier: Apache-2.0
"""Validate the GAR-RUNTIME-IMPL-6C user-surface graduation matrix.

The matrix is a release-readiness guardrail, not a runtime expansion by itself.
It fails when a public CLI command or Python context/client method appears
without a deliberate posture:

``high_level_context``, ``client_only``, ``diagnostic_only``,
``feature_gated``, or ``not_user_facing``.
"""

from __future__ import annotations

import argparse
import ast
import json
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_VERSION = "shardloom.user_surface_graduation_matrix.v1"
GATE_ID = "gar-runtime-impl-6c.user_surface_graduation_matrix"
POSTURE_VOCABULARY = (
    "high_level_context",
    "client_only",
    "diagnostic_only",
    "feature_gated",
    "not_user_facing",
)
SUPPORT_STATE_VOCABULARY = (
    "executable",
    "feature_gated",
    "diagnostic_only",
    "report_only",
    "blocked",
    "future",
)
HIGH_LEVEL_CONTEXT_COMMANDS = {
    "route",
    "generated-source-user-rows-smoke",
    "generated-source-range-smoke",
    "generated-source-sequence-smoke",
    "generated-source-sql-smoke",
    "sql-local-source-smoke",
    "vortex-ingest-smoke",
    "sqlite-local-import-export-smoke",
    "udf-local-scalar-fixture-smoke",
    "object-store-read-smoke",
    "object-store-write-smoke",
    "local-table-metadata-read-smoke",
    "local-table-append-commit-rehearsal-smoke",
    "live-fixture-run",
    "hybrid-overlay-run",
    "session-cache-smoke",
    "traditional-analytics-vortex-run",
    "traditional-analytics-vortex-batch-run",
    "traditional-analytics-prepare-batch-run",
    "vortex-count",
    "vortex-count-where",
    "vortex-project",
    "vortex-filter",
    "vortex-filter-project",
    "vortex-local-exec",
    "vortex-bounded-local-exec",
    "vortex-run",
    "vortex-query-trace",
}
EXECUTABLE_METADATA_COMMANDS = {
    "help",
    "command-metadata",
    "evidence-schema",
    "status",
    "runs-today",
    "capabilities",
    "doctor",
    "explain",
    "estimate",
    "spill-payload-roundtrip",
    "cleanup-synthetic-payload",
    "vortex-encoded-read-spike",
    "vortex-count",
    "vortex-count-where",
    "vortex-project",
    "vortex-filter",
    "vortex-filter-project",
    "vortex-local-exec",
    "vortex-bounded-local-exec",
    "vortex-run",
    "vortex-query-trace",
}
REST_API_COMMANDS = {
    "api-compat-plan",
    "rest-api-contract-plan",
    "rest-api-plan-preview",
    "rest-api-local-lifecycle",
    "rest-api-event-stream",
    "rest-api-security-governance",
    "rest-api-data-plane",
    "serve",
}
COMMAND_FAMILY_FUNCTIONS = (
    ("is_status_capabilities_command", "status_capabilities"),
    ("is_vortex_primitive_command", "vortex_primitive_execution"),
    ("is_prepared_source_backed_command", "prepared_source_backed_execution"),
    ("is_vortex_output_commit_command", "vortex_output_commit"),
    ("is_vortex_runtime_planning_command", "vortex_runtime_planning"),
    ("is_vortex_planning_command", "vortex_planning"),
    ("is_evidence_certificate_command", "evidence_certificates"),
    ("is_benchmark_command", "benchmarks"),
    ("is_packaging_deployment_command", "packaging_deployment"),
    ("is_foundry_command", "foundry"),
    ("is_object_store_planning_command", "object_store_planning"),
    ("is_operational_hardening_command", "operational_hardening"),
    ("is_diagnostics_command", "diagnostics"),
    ("is_input_planning_command", "input_planning"),
    ("is_workflow_planning_command", "workflow_planning"),
    ("is_engine_runtime_planning_command", "engine_runtime_planning"),
    ("is_optimizer_planning_command", "optimizer_planning"),
    ("is_extension_planning_command", "extension_planning"),
)
REQUIRED_DOC_MARKERS = {
    "README.md": [
        "user surface graduation",
        "high_level_context",
        "client_only",
        "diagnostic_only",
        "feature_gated",
    ],
    "python/README.md": [
        "user_surface_graduation_matrix",
        "high_level_context",
        "client_only",
    ],
    "docs/status/cli-command-registry.md": [
        "user_surface_graduation_posture",
        "high_level_context",
    ],
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=ROOT)
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("target/user-surface-graduation-matrix.json"),
    )
    return parser.parse_args()


def resolve(repo_root: Path, path: Path) -> Path:
    return path if path.is_absolute() else repo_root / path


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8") if path.exists() else ""


def public_methods(repo_root: Path, relative: str, class_name: str) -> list[str]:
    tree = ast.parse(read_text(repo_root / relative))
    for node in tree.body:
        if isinstance(node, ast.ClassDef) and node.name == class_name:
            return [
                item.name
                for item in node.body
                if isinstance(item, ast.FunctionDef) and not item.name.startswith("_")
            ]
    return []


def load_python_matrix(repo_root: Path) -> Any:
    src = repo_root / "python" / "src"
    if str(src) not in sys.path:
        sys.path.insert(0, str(src))
    from shardloom import ShardLoomContext

    return ShardLoomContext(client=None).user_surface_graduation_matrix()


def row_payload(row: Any) -> dict[str, Any]:
    return {
        "row_id": row.row_id,
        "surface_kind": row.surface_kind,
        "surface": row.surface,
        "graduation_posture": row.graduation_posture,
        "support_state": row.support_state,
        "cli_commands": list(row.cli_commands),
        "context_methods": list(row.context_methods),
        "client_methods": list(row.client_methods),
        "runtime_route": row.runtime_route,
        "promotion_criteria": row.promotion_criteria,
        "evidence_refs": list(row.evidence_refs),
        "claim_boundary": row.claim_boundary,
        "fallback_attempted": row.fallback_attempted,
        "external_engine_invoked": row.external_engine_invoked,
    }


def parse_registered_commands(repo_root: Path) -> list[str]:
    source = read_text(repo_root / "shardloom-cli" / "src" / "command_registry.rs")
    marker = "pub(crate) const REGISTERED_COMMANDS: &[&str] = &["
    if marker not in source:
        return []
    body = source.split(marker, 1)[1].split("];", 1)[0]
    return re.findall(r'"([^"]+)"', body)


def command_groups(repo_root: Path) -> list[tuple[set[str], str]]:
    source = read_text(repo_root / "shardloom-cli" / "src" / "command_family.rs")
    groups: list[tuple[set[str], str]] = []
    for fn_name, family in COMMAND_FAMILY_FUNCTIONS:
        marker = f"fn {fn_name}"
        if marker not in source:
            groups.append((set(), family))
            continue
        body = source.split(marker, 1)[1].split("\n}", 1)[0]
        groups.append((set(re.findall(r'"([^"]+)"', body)), family))
    return groups


def classify_command(command: str, groups: list[tuple[set[str], str]]) -> str:
    for commands, family in groups:
        if command in commands:
            return family
    if command in REST_API_COMMANDS:
        return "rest_api_planning"
    if command.startswith("cg9-"):
        return "workflow_planning"
    if command.startswith("object-store-") or command.startswith("cg10-"):
        return "object_store_planning"
    return "other"


def command_support_state(command: str, family: str) -> str:
    if family == "diagnostics" or command in {
        "help",
        "command-metadata",
        "evidence-schema",
        "status",
        "runs-today",
        "capabilities",
    }:
        return "diagnostic_only"
    if (
        command in EXECUTABLE_METADATA_COMMANDS
        or command.endswith("-smoke")
        or command.endswith("-run")
    ):
        return "executable"
    if "write" in command or "execute" in command:
        return "feature_gated"
    return "report_only"


def command_graduation_posture(command: str, support_state: str) -> str:
    if command in HIGH_LEVEL_CONTEXT_COMMANDS:
        return "high_level_context"
    if support_state == "feature_gated":
        return "feature_gated"
    if support_state in {"diagnostic_only", "report_only", "blocked"}:
        return "diagnostic_only"
    if support_state == "future":
        return "not_user_facing"
    return "client_only"


def build_cli_rows(repo_root: Path) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    groups = command_groups(repo_root)
    command_rows: list[dict[str, Any]] = []
    for command in parse_registered_commands(repo_root):
        family = classify_command(command, groups)
        support_state = command_support_state(command, family)
        posture = command_graduation_posture(command, support_state)
        command_rows.append(
            {
                "command": command,
                "family": family,
                "support_state": support_state,
                "graduation_posture": posture,
            }
        )

    by_family: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in command_rows:
        by_family[str(row["family"])].append(row)
    family_rows: list[dict[str, Any]] = []
    for family, rows in sorted(by_family.items()):
        posture_counts = Counter(str(row["graduation_posture"]) for row in rows)
        support_counts = Counter(str(row["support_state"]) for row in rows)
        if posture_counts.get("feature_gated"):
            family_posture = "feature_gated"
        elif posture_counts.get("high_level_context") and len(posture_counts) == 1:
            family_posture = "high_level_context"
        elif posture_counts.get("client_only") and len(posture_counts) == 1:
            family_posture = "client_only"
        elif support_counts.get("future"):
            family_posture = "not_user_facing"
        else:
            family_posture = "diagnostic_only"
        family_rows.append(
            {
                "family": family,
                "graduation_posture": family_posture,
                "command_count": len(rows),
                "support_state_counts": dict(sorted(support_counts.items())),
                "graduation_posture_counts": dict(sorted(posture_counts.items())),
                "commands": [str(row["command"]) for row in rows],
            }
        )
    return command_rows, family_rows


def validate_python_matrix(
    repo_root: Path,
    matrix: Any,
    rows: list[dict[str, Any]],
) -> list[str]:
    blockers: list[str] = []
    if matrix.schema_version != SCHEMA_VERSION:
        blockers.append(f"schema_version={matrix.schema_version!r}")
    if tuple(matrix.posture_vocabulary) != POSTURE_VOCABULARY:
        blockers.append("unexpected user-surface graduation posture vocabulary")
    if not matrix.all_rows_have_allowed_posture:
        blockers.append("matrix contains row outside posture vocabulary")
    if not matrix.all_high_level_rows_have_runtime_evidence:
        blockers.append("high-level context rows must name runtime_route and evidence_refs")
    if not matrix.all_no_fallback_no_external_engine:
        blockers.append("all matrix rows must preserve no fallback and no external engine")

    allowed = set(POSTURE_VOCABULARY)
    seen_rows: set[str] = set()
    for row in rows:
        row_id = str(row["row_id"])
        if row_id in seen_rows:
            blockers.append(f"duplicate matrix row_id: {row_id}")
        seen_rows.add(row_id)
        posture = str(row["graduation_posture"])
        if posture not in allowed:
            blockers.append(f"{row_id}: invalid graduation_posture={posture}")
        if not str(row["claim_boundary"]).strip():
            blockers.append(f"{row_id}: claim_boundary is required")
        if not row["evidence_refs"]:
            blockers.append(f"{row_id}: evidence_refs are required")
        if row["fallback_attempted"] is not False:
            blockers.append(f"{row_id}: fallback_attempted must be false")
        if row["external_engine_invoked"] is not False:
            blockers.append(f"{row_id}: external_engine_invoked must be false")
        if posture == "high_level_context" and not row["context_methods"]:
            blockers.append(f"{row_id}: high_level_context row must list context_methods")
        if posture == "feature_gated" and "feature" not in str(row["support_state"]):
            blockers.append(f"{row_id}: feature_gated row must carry feature-gated support_state")

    for field, label in [
        ("cli_commands", "CLI commands"),
        ("context_methods", "context methods"),
        ("client_methods", "client methods"),
    ]:
        owners: dict[str, list[str]] = defaultdict(list)
        for row in rows:
            for value in row[field]:
                owners[str(value)].append(str(row["row_id"]))
        duplicates = {
            value: row_ids for value, row_ids in owners.items() if len(row_ids) > 1
        }
        if duplicates:
            formatted = ";".join(
                f"{value}={'|'.join(row_ids)}"
                for value, row_ids in sorted(duplicates.items())
            )
            blockers.append(f"{label} have multiple graduation postures: {formatted}")

    context_methods = public_methods(repo_root, "python/src/shardloom/context.py", "ShardLoomContext")
    client_methods = public_methods(repo_root, "python/src/shardloom/client.py", "ShardLoomClient")
    covered_context = {
        method
        for row in rows
        for method in row["context_methods"]
    }
    covered_client = {
        method
        for row in rows
        for method in row["client_methods"]
    }
    missing_context = sorted(set(context_methods) - covered_context)
    extra_context = sorted(covered_context - set(context_methods))
    missing_client = sorted(set(client_methods) - covered_client)
    extra_client = sorted(covered_client - set(client_methods))
    if missing_context:
        blockers.append("context methods lack graduation posture: " + ",".join(missing_context))
    if extra_context:
        blockers.append("graduation matrix references missing context methods: " + ",".join(extra_context))
    if missing_client:
        blockers.append("client methods lack graduation posture: " + ",".join(missing_client))
    if extra_client:
        blockers.append("graduation matrix references missing client methods: " + ",".join(extra_client))
    return blockers


def validate_cli_rows(
    rows: list[dict[str, Any]],
    family_rows: list[dict[str, Any]],
    matrix_rows: list[dict[str, Any]],
) -> list[str]:
    blockers: list[str] = []
    matrix_high_level_commands = {
        command
        for row in matrix_rows
        for command in row["cli_commands"]
        if row["graduation_posture"] == "high_level_context"
    }
    if not rows:
        blockers.append("registered CLI command list is empty")
    command_names = [str(row["command"]) for row in rows]
    duplicates = sorted(name for name, count in Counter(command_names).items() if count > 1)
    if duplicates:
        blockers.append("duplicate registered CLI commands: " + ",".join(duplicates))
    for row in rows:
        command = str(row["command"])
        support_state = str(row["support_state"])
        posture = str(row["graduation_posture"])
        if support_state not in SUPPORT_STATE_VOCABULARY:
            blockers.append(f"{command}: invalid support_state={support_state}")
        if posture not in POSTURE_VOCABULARY:
            blockers.append(f"{command}: invalid graduation_posture={posture}")
        if support_state == "feature_gated" and posture != "feature_gated":
            blockers.append(f"{command}: feature-gated command must use feature_gated posture")
        if support_state == "executable" and posture not in {"high_level_context", "client_only"}:
            blockers.append(
                f"{command}: executable command must be high_level_context or client_only"
            )
        if posture == "high_level_context" and command not in matrix_high_level_commands:
            blockers.append(
                f"{command}: high-level CLI command must be referenced by Python matrix"
            )
    if not family_rows:
        blockers.append("registered CLI command family list is empty")
    for row in family_rows:
        posture = str(row["graduation_posture"])
        if posture not in POSTURE_VOCABULARY:
            blockers.append(f"{row['family']}: invalid family graduation_posture={posture}")
    return blockers


def doc_marker_blockers(repo_root: Path) -> list[str]:
    blockers: list[str] = []
    for relative, markers in REQUIRED_DOC_MARKERS.items():
        text = read_text(repo_root / relative)
        if not text:
            blockers.append(f"missing required user-surface graduation doc: {relative}")
            continue
        for marker in markers:
            if marker not in text:
                blockers.append(f"{relative} missing user-surface graduation marker: {marker}")
    return blockers


def build_report(repo_root: Path) -> dict[str, Any]:
    matrix = load_python_matrix(repo_root)
    matrix_rows = [row_payload(row) for row in matrix.rows]
    cli_command_rows, cli_family_rows = build_cli_rows(repo_root)
    blockers = [
        *validate_python_matrix(repo_root, matrix, matrix_rows),
        *validate_cli_rows(cli_command_rows, cli_family_rows, matrix_rows),
        *doc_marker_blockers(repo_root),
    ]
    posture_counts = Counter(str(row["graduation_posture"]) for row in matrix_rows)
    cli_posture_counts = Counter(str(row["graduation_posture"]) for row in cli_command_rows)
    return {
        "schema_version": SCHEMA_VERSION,
        "gate_id": GATE_ID,
        "status": "passed" if not blockers else "blocked",
        "blockers": blockers,
        "posture_vocabulary": list(POSTURE_VOCABULARY),
        "matrix_row_count": len(matrix_rows),
        "cli_command_count": len(cli_command_rows),
        "cli_family_count": len(cli_family_rows),
        "context_method_count": len(matrix.context_method_order),
        "client_method_count": len(matrix.client_method_order),
        "matrix_posture_counts": {key: posture_counts.get(key, 0) for key in POSTURE_VOCABULARY},
        "cli_command_posture_counts": {
            key: cli_posture_counts.get(key, 0) for key in POSTURE_VOCABULARY
        },
        "high_level_context_command_count": cli_posture_counts.get("high_level_context", 0),
        "fallback_attempted": False,
        "external_engine_invoked": False,
        "claim_gate_status": "not_claim_grade",
        "runtime_support_claim_allowed": False,
        "performance_claim_allowed": False,
        "production_claim_allowed": False,
        "acceptance_summary": {
            "all_python_context_methods_classified": not any(
                blocker.startswith("context methods lack") for blocker in blockers
            ),
            "all_python_client_methods_classified": not any(
                blocker.startswith("client methods lack") for blocker in blockers
            ),
            "all_cli_commands_classified": not any(
                blocker.startswith("registered CLI") or "invalid graduation_posture" in blocker
                for blocker in blockers
            ),
            "all_high_level_cli_commands_have_context_matrix_refs": not any(
                "high-level CLI command must be referenced" in blocker
                for blocker in blockers
            ),
            "all_no_fallback_no_external_engine": not any(
                "fallback_attempted" in blocker or "external_engine_invoked" in blocker
                for blocker in blockers
            ),
        },
        "rows": matrix_rows,
        "cli_command_rows": cli_command_rows,
        "cli_family_rows": cli_family_rows,
    }


def main() -> int:
    args = parse_args()
    repo_root = args.repo_root.resolve()
    report = build_report(repo_root)
    output = resolve(repo_root, args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    for blocker in report["blockers"]:
        print(f"user-surface graduation matrix blocker: {blocker}", file=sys.stderr)
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
