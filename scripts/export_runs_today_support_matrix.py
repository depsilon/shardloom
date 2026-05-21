#!/usr/bin/env python
# SPDX-License-Identifier: Apache-2.0
"""Export the generated `runs-today` current-support matrix."""

from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT = Path("docs/status/runs-today-support-matrix.json")
DEFAULT_WEBSITE_OUTPUT = Path("website-src/src/data/runs-today-support-matrix.json")
SUPPORT_STATES = (
    "executable",
    "feature_gated",
    "diagnostic_only",
    "report_only",
    "blocked",
    "future",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=ROOT)
    parser.add_argument("--binary", type=Path)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--website-output", type=Path, default=DEFAULT_WEBSITE_OUTPUT)
    parser.add_argument(
        "--check",
        action="store_true",
        help="fail if generated output differs from committed artifacts",
    )
    return parser.parse_args()


def csv_values(value: str | None) -> list[str]:
    if not value:
        return []
    return [part.strip() for part in value.split(",") if part.strip()]


def field_bool(fields: dict[str, str], key: str, default: bool = False) -> bool:
    value = fields.get(key)
    if value is None:
        return default
    return value.lower() == "true"


def field_int(fields: dict[str, str], key: str) -> int:
    return int(fields[key])


def resolve(path: Path, repo_root: Path) -> Path:
    return path if path.is_absolute() else repo_root / path


def run_runs_today(repo_root: Path, binary: Path | None) -> dict[str, Any]:
    if binary is not None:
        command = [str(binary), "runs-today", "--format", "json"]
    else:
        command = [
            "cargo",
            "run",
            "--quiet",
            "-p",
            "shardloom-cli",
            "--",
            "runs-today",
            "--format",
            "json",
        ]
    completed = subprocess.run(
        command,
        cwd=repo_root,
        check=True,
        capture_output=True,
        text=True,
    )
    return json.loads(completed.stdout)


def envelope_fields(envelope: dict[str, Any]) -> dict[str, str]:
    fields: dict[str, str] = {}
    for source in (
        envelope.get("fields", []),
        envelope.get("result", {}).get("fields", []),
        envelope.get("policy", {}).get("fields", []),
        envelope.get("lifecycle", {}).get("fields", []),
        envelope.get("capability_snapshot", {}).get("fields", []),
    ):
        for field in source:
            key = str(field["key"])
            fields[key] = str(field["value"])
    return fields


def normalize(envelope: dict[str, Any]) -> dict[str, Any]:
    fields = envelope_fields(envelope)
    row_ids = csv_values(fields["runs_today_row_order"])
    rows = []
    for row_id in row_ids:
        prefix = f"runs_today_row_{row_id}_"
        rows.append(
            {
                "id": row_id,
                "family": fields[f"{prefix}family"],
                "surface": csv_values(fields[f"{prefix}surface"]),
                "support_state": fields[f"{prefix}support_state"],
                "feature_gate": fields[f"{prefix}feature_gate"],
                "evidence_refs": csv_values(fields[f"{prefix}evidence_refs"]),
                "blocker_id": fields[f"{prefix}blocker_id"],
                "claim_gate_status": fields[f"{prefix}claim_gate_status"],
                "claim_boundary": fields[f"{prefix}claim_boundary"],
                "runtime_execution": field_bool(fields, f"{prefix}runtime_execution"),
                "data_read": field_bool(fields, f"{prefix}data_read"),
                "write_io": field_bool(fields, f"{prefix}write_io"),
                "fallback_attempted": field_bool(fields, f"{prefix}fallback_attempted"),
                "external_engine_invoked": field_bool(
                    fields,
                    f"{prefix}external_engine_invoked",
                ),
            }
        )
    support_state_vocabulary = csv_values(fields["runs_today_support_state_vocabulary"])
    if tuple(support_state_vocabulary) != SUPPORT_STATES:
        raise ValueError(f"unexpected support-state vocabulary: {support_state_vocabulary}")
    return {
        "schema_version": fields["runs_today_schema_version"],
        "matrix_id": fields["runs_today_matrix_id"],
        "command": envelope.get("command"),
        "docs_ref": fields["runs_today_docs_ref"],
        "website_data_ref": fields["runs_today_website_data_ref"],
        "support_state_vocabulary": support_state_vocabulary,
        "family_order": csv_values(fields["runs_today_family_order"]),
        "row_order": row_ids,
        "row_count": field_int(fields, "runs_today_row_count"),
        "support_state_counts": {
            state: field_int(fields, f"runs_today_{state}_row_count")
            for state in support_state_vocabulary
        },
        "family_counts": {
            family: field_int(fields, f"runs_today_{family}_row_count")
            for family in csv_values(fields["runs_today_family_order"])
        },
        "blocker_ids": csv_values(fields.get("runs_today_blocker_ids")),
        "evidence_refs": csv_values(fields.get("runs_today_evidence_refs")),
        "all_rows_fallback_attempted_false": field_bool(
            fields,
            "runs_today_all_rows_fallback_attempted_false",
        ),
        "all_rows_external_engine_invoked_false": field_bool(
            fields,
            "runs_today_all_rows_external_engine_invoked_false",
        ),
        "all_rows_no_fallback_no_external_engine": field_bool(
            fields,
            "runs_today_all_rows_no_fallback_no_external_engine",
        ),
        "runtime_expansion_allowed": field_bool(
            fields,
            "runs_today_runtime_expansion_allowed",
            True,
        ),
        "package_publication_allowed": field_bool(
            fields,
            "runs_today_package_publication_allowed",
            True,
        ),
        "performance_claim_allowed": field_bool(
            fields,
            "runs_today_performance_claim_allowed",
            True,
        ),
        "claim_gate_status": fields["runs_today_claim_gate_status"],
        "claim_boundary": fields["runs_today_claim_boundary"],
        "rows": rows,
    }


def render(matrix: dict[str, Any]) -> str:
    return json.dumps(matrix, indent=2) + "\n"


def write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def check_matches(path: Path, content: str) -> list[str]:
    if not path.exists():
        return [f"missing generated artifact: {path}"]
    existing = path.read_text(encoding="utf-8")
    if existing != content:
        return [f"generated artifact is stale: {path}"]
    return []


def main() -> int:
    args = parse_args()
    repo_root = args.repo_root.resolve()
    docs_output = resolve(args.output, repo_root)
    website_output = resolve(args.website_output, repo_root)
    envelope = run_runs_today(repo_root, args.binary)
    matrix = normalize(envelope)
    content = render(matrix)

    if args.check:
        blockers = [
            *check_matches(docs_output, content),
            *check_matches(website_output, content),
        ]
        if blockers:
            for blocker in blockers:
                print(f"runs-today matrix blocker: {blocker}")
            return 1
        print("runs-today support matrix is current")
        return 0

    write(docs_output, content)
    write(website_output, content)
    print(f"wrote {docs_output}")
    print(f"wrote {website_output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
