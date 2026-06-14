#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Generate the v1 supported/unsupported surface page from checked-in matrices."""

from __future__ import annotations

import argparse
import json
from collections import defaultdict
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_VERSION = "shardloom.v1_supported_unsupported_surface.v1"
DEFAULT_RUNS_TODAY = Path("docs/status/runs-today-support-matrix.json")
DEFAULT_PACKAGE_MATRIX = Path("docs/release/package-channel-readiness-matrix.json")
DEFAULT_OUTPUT = Path("docs/getting-started/v1-supported-unsupported.md")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=ROOT)
    parser.add_argument("--runs-today-matrix", type=Path, default=DEFAULT_RUNS_TODAY)
    parser.add_argument("--package-channel-matrix", type=Path, default=DEFAULT_PACKAGE_MATRIX)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--check", action="store_true")
    return parser.parse_args()


def resolve(repo_root: Path, path: Path) -> Path:
    return path if path.is_absolute() else repo_root / path


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def bool_text(value: Any) -> str:
    if value is True:
        return "true"
    if value is False:
        return "false"
    return "unknown"


def inverse_bool_text(value: Any) -> str:
    if value is True:
        return "false"
    if value is False:
        return "true"
    return "unknown"


def cell(value: Any) -> str:
    text = ", ".join(str(item) for item in value) if isinstance(value, list) else str(value)
    return text.replace("|", "\\|").replace("\n", " ").strip()


def table(headers: tuple[str, ...], rows: list[tuple[Any, ...]]) -> list[str]:
    lines = ["| " + " | ".join(headers) + " |"]
    lines.append("| " + " | ".join("---" for _ in headers) + " |")
    for row in rows:
        lines.append("| " + " | ".join(cell(value) for value in row) + " |")
    return lines


def render_runs_today(runs_today: dict[str, Any]) -> list[str]:
    rows = [row for row in runs_today.get("rows", []) if isinstance(row, dict)]
    by_family: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        by_family[str(row.get("family", "unknown"))].append(row)

    lines: list[str] = []
    family_order = runs_today.get("family_order", [])
    ordered_families = [str(family) for family in family_order if str(family) in by_family]
    ordered_families.extend(sorted(set(by_family) - set(ordered_families)))
    for family in ordered_families:
        lines.append(f"## {family.replace('_', ' ').title()}")
        lines.append("")
        family_rows = sorted(by_family[family], key=lambda row: str(row.get("id", "")))
        lines.extend(
            table(
                (
                    "Surface",
                    "State",
                    "Feature gate",
                    "Runtime",
                    "Write",
                    "Claim gate",
                    "Boundary",
                ),
                [
                    (
                        row.get("surface", row.get("id", "")),
                        row.get("support_state", "unknown"),
                        row.get("feature_gate", "default"),
                        bool_text(row.get("runtime_execution")),
                        bool_text(row.get("write_io")),
                        row.get("claim_gate_status", "not_claim_grade"),
                        row.get("claim_boundary", ""),
                    )
                    for row in family_rows
                ],
            )
        )
        lines.append("")
    return lines


def render_package_channels(package_matrix: dict[str, Any]) -> list[str]:
    channels = [row for row in package_matrix.get("channels", []) if isinstance(row, dict)]
    ready_channels = [row for row in channels if row.get("ready") is True]
    lines = [
        "## Package Channels",
        "",
        "Package channel rows are generated from `docs/release/package-channel-readiness-matrix.json`.",
        "Package install commands are intentionally withheld while channel status is blocked.",
        "",
        "```text",
        f"package_install_commands_visible={str(bool(ready_channels)).lower()}",
        f"public_package_release_claim_allowed={bool_text(package_matrix.get('public_package_release_claim_allowed'))}",
        f"publication_attempted={bool_text(package_matrix.get('publication_attempted'))}",
        f"tag_created={bool_text(package_matrix.get('tag_created'))}",
        f"package_upload_attempted={bool_text(package_matrix.get('package_channel_submission_attempted'))}",
        "```",
        "",
    ]
    lines.extend(
        table(
            ("Channel", "Status", "Ready", "Current blockers"),
            [
                (
                    row.get("display_name", row.get("channel_id", "")),
                    row.get("status", "unknown"),
                    bool_text(row.get("ready")),
                    row.get("current_blockers", []),
                )
                for row in channels
            ],
        )
    )
    lines.append("")
    return lines


def render(runs_today: dict[str, Any], package_matrix: dict[str, Any]) -> str:
    lines = [
        "<!-- SPDX-License-Identifier: Apache-2.0 -->",
        "<!-- This file is generated by scripts/write_v1_supported_unsupported_docs.py. -->",
        "",
        "# V1 Supported And Unsupported Surface",
        "",
        f"Schema marker: `{SCHEMA_VERSION}`.",
        "",
        "Status: generated public support boundary from machine-readable matrices.",
        "",
        "This page is generated from `docs/status/runs-today-support-matrix.json` and "
        "`docs/release/package-channel-readiness-matrix.json`. Edit those source matrices or the "
        "generator instead of hand-editing this file.",
        "",
        "```text",
        f"runs_today_schema_version={runs_today.get('schema_version', 'missing')}",
        f"runs_today_row_count={runs_today.get('row_count', 'missing')}",
        f"package_channel_schema_version={package_matrix.get('schema_version', 'missing')}",
        f"fallback_attempted={inverse_bool_text(runs_today.get('all_rows_fallback_attempted_false'))}",
        f"external_engine_invoked={inverse_bool_text(runs_today.get('all_rows_external_engine_invoked_false'))}",
        f"performance_claim_allowed={bool_text(runs_today.get('performance_claim_allowed'))}",
        f"package_publication_allowed={bool_text(runs_today.get('package_publication_allowed'))}",
        "```",
        "",
        "Use this page to decide what can be run locally today and what must return a deterministic "
        "unsupported or blocked diagnostic. It is not a performance, production, package-publication, "
        "or Spark-replacement claim.",
        "",
    ]
    lines.extend(render_runs_today(runs_today))
    lines.extend(render_package_channels(package_matrix))
    lines.extend(
        [
            "## Unsupported Example Boundaries",
            "",
            "The generated support rows above must be read with these fail-closed examples:",
            "",
            "- `unsupported_example_broad_sql`: arbitrary SQL outside admitted local-source/source-free shapes must return an unsupported diagnostic.",
            "- `unsupported_example_unbounded_collect`: unbounded materialization helpers must expose a blocker instead of invoking another engine.",
            "- `unsupported_example_object_store`: object-store read/write paths are not v1 local runtime support unless a future gate closes them.",
            "- `unsupported_example_foundry`: Foundry platform use is not v1 production support or endorsement.",
            "- `unsupported_example_udf_effect`: UDFs, API calls, LLM calls, embeddings, and other effectful operations require explicit admission before execution.",
            "",
            "Every unsupported example must preserve:",
            "",
            "```text",
            "fallback_attempted=false",
            "external_engine_invoked=false",
            "```",
            "",
        ]
    )
    return "\n".join(lines)


def main() -> int:
    args = parse_args()
    repo_root = args.repo_root.resolve()
    runs_today = load_json(resolve(repo_root, args.runs_today_matrix))
    package_matrix = load_json(resolve(repo_root, args.package_channel_matrix))
    expected = render(runs_today, package_matrix)
    output = resolve(repo_root, args.output)
    if args.check:
        actual = output.read_text(encoding="utf-8") if output.exists() else ""
        if actual != expected:
            print(f"{output}: generated docs are stale")
            return 1
        print(f"{output}: generated docs are current")
        return 0
    output.write_text(expected, encoding="utf-8")
    print(output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
