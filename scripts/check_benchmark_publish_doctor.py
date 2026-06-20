#!/usr/bin/env python
# SPDX-License-Identifier: Apache-2.0
"""Static benchmark publication doctor and compact agent route packet.

This script reads committed benchmark artifacts only. It does not execute benchmarks, import
external engines, mutate benchmark rows, or publish the website.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT))

from check_benchmark_artifact_completeness import (  # noqa: E402
    CLICKBENCH_URL,
    PUBLIC_BENCHMARK_SURFACE,
    default_public_benchmark_manifest_retired,
    load_json,
    repo_path,
    retired_public_benchmark_report,
    result_rows,
    validate_manifest,
)
from check_benchmark_publication_claim_gate import (  # noqa: E402
    DEFAULT_MAX_AGE_DAYS,
    DEFAULT_PRE_5J_DEPENDENCY_REPORT,
    retired_public_benchmark_publication_report,
    validate_publication_claim_gate,
)


SCHEMA_VERSION = "shardloom.benchmark_publish_doctor.v1"
ROUTE_PACKET_SCHEMA_VERSION = "shardloom.benchmark_route_packet.v1"
DEFAULT_MANIFEST = ROOT / "website" / "assets" / "benchmarks" / "latest" / "manifest.json"
DEFAULT_OUTPUT = ROOT / "target" / "benchmark-publish-doctor.json"
DEFAULT_PACKET_JSON = ROOT / "target" / "benchmark-route-packet.json"
DEFAULT_PACKET_MD = ROOT / "target" / "benchmark-route-packet.md"
MIRROR_GROUPS = (
    (
        "benchmark_results",
        (
            "website/assets/benchmarks/latest/benchmark-results.json",
            "website-public/assets/benchmarks/latest/benchmark-results.json",
            "website/assets/data/benchmark-evidence.json",
            "website-public/assets/data/benchmark-evidence.json",
            "website-src/src/data/benchmark-evidence.json",
        ),
    ),
    (
        "benchmark_manifest",
        (
            "website/assets/benchmarks/latest/manifest.json",
            "website-public/assets/benchmarks/latest/manifest.json",
            "website-src/src/data/benchmark-manifest.json",
        ),
    ),
)
REQUIRED_VALIDATORS = (
    "python3 scripts/check_benchmark_artifact_completeness.py --manifest website/assets/benchmarks/latest/manifest.json",
    "python3 scripts/check_benchmark_publication_claim_gate.py --manifest website/assets/benchmarks/latest/manifest.json --allow-stale-git --allow-dirty-worktree",
    "PATH=/Users/dylan/.cache/codex-runtimes/codex-primary-runtime/dependencies/node/bin:$PATH node website/validate_static_assets.js",
    "python3 scripts/check_website_readiness.py --output target/website-readiness-report.json",
    "git diff --check",
)
FORBIDDEN_CLAIMS = (
    "performance superiority",
    "production readiness",
    "Spark displacement",
    "package publication readiness",
    "object-store/lakehouse/Foundry production support",
    "encoded-native operator support unless operator_encoded_native_claim_allowed=true",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument("--repo-root", type=Path, default=ROOT)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--packet-json", type=Path, default=DEFAULT_PACKET_JSON)
    parser.add_argument("--packet-md", type=Path, default=DEFAULT_PACKET_MD)
    parser.add_argument(
        "--pre-5j-dependency-report",
        type=Path,
        default=DEFAULT_PRE_5J_DEPENDENCY_REPORT,
    )
    parser.add_argument("--allow-incomplete", action="store_true")
    parser.add_argument("--allow-stale-git", action="store_true")
    parser.add_argument("--allow-dirty-worktree", action="store_true")
    parser.add_argument("--max-age-days", type=int, default=DEFAULT_MAX_AGE_DAYS)
    return parser.parse_args()


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def file_sha256(path: Path) -> str | None:
    if not path.exists() or not path.is_file():
        return None
    return hashlib.sha256(path.read_bytes()).hexdigest()


def artifact_payload(manifest: dict[str, Any], manifest_path: Path) -> dict[str, Any]:
    artifact_paths = manifest.get("artifact_paths")
    if not isinstance(artifact_paths, dict):
        return {}
    json_path_text = artifact_paths.get("json")
    if not isinstance(json_path_text, str) or not json_path_text:
        return {}
    path = repo_path(json_path_text, manifest_path)
    if not path.exists():
        return {}
    payload = load_json(path)
    return payload if isinstance(payload, dict) else {}


def mirror_status(repo_root: Path) -> dict[str, Any]:
    groups: list[dict[str, Any]] = []
    blockers: list[str] = []
    for label, refs in MIRROR_GROUPS:
        digests: dict[str, str | None] = {}
        missing: list[str] = []
        for ref in refs:
            path = repo_root / ref
            digest = file_sha256(path)
            digests[ref] = digest
            if digest is None:
                missing.append(ref)
        unique = {digest for digest in digests.values() if digest is not None}
        status = "passed" if not missing and len(unique) <= 1 else "blocked"
        if status != "passed":
            blockers.append(f"{label} mirror drift or missing refs: {missing or sorted(digests)}")
        groups.append(
            {
                "label": label,
                "status": status,
                "refs": list(refs),
                "sha256_by_ref": digests,
            }
        )
    return {
        "status": "passed" if not blockers else "blocked",
        "groups": groups,
        "blockers": blockers,
    }


def retired_static_benchmark_artifact_status(repo_root: Path) -> dict[str, Any]:
    stale_refs: list[str] = []
    groups: list[dict[str, Any]] = []
    for label, refs in MIRROR_GROUPS:
        present = [ref for ref in refs if (repo_root / ref).exists()]
        if present:
            stale_refs.extend(present)
        groups.append(
            {
                "label": label,
                "status": "passed" if not present else "blocked",
                "refs": list(refs),
                "present_retired_refs": present,
            }
        )
    blockers = [
        "retired public benchmark dashboard artifacts are still present: "
        + ",".join(sorted(stale_refs))
    ] if stale_refs else []
    return {
        "status": "passed" if not blockers else "blocked",
        "groups": groups,
        "blockers": blockers,
    }


def counter_dict(rows: list[dict[str, Any]], key: str) -> dict[str, int]:
    return dict(sorted(Counter(str(row.get(key) or "missing") for row in rows).items()))


def dashboard_table(payload: dict[str, Any], key: str) -> dict[str, Any]:
    dashboard = payload.get("comparative_dashboard")
    if not isinstance(dashboard, dict):
        return {}
    table = dashboard.get(key)
    return table if isinstance(table, dict) else {}


def primary_bottleneck(payload: dict[str, Any]) -> str:
    table = dashboard_table(payload, "cold_lane_attribution")
    headers = table.get("headers")
    rows = table.get("rows")
    if not isinstance(headers, list) or not isinstance(rows, list):
        return "not_reported"
    try:
        primary_index = headers.index("Primary bottleneck")
        rows_index = headers.index("Rows")
    except ValueError:
        return "not_reported"
    counts: Counter[str] = Counter()
    for row in rows:
        if not isinstance(row, list) or len(row) <= max(primary_index, rows_index):
            continue
        primary = str(row[primary_index])
        if primary in {"external_baseline_only", "not_applicable", "missing"}:
            continue
        try:
            count = int(float(str(row[rows_index])))
        except ValueError:
            count = 0
        counts[primary] += count
    return counts.most_common(1)[0][0] if counts else "not_reported"


def first_unchecked_phase_item(repo_root: Path) -> str:
    plan = repo_root / "docs" / "architecture" / "phased-execution-plan.md"
    if not plan.exists():
        return "not_reported"
    in_planned = False
    for line in plan.read_text(encoding="utf-8").splitlines():
        if line.strip() == "## Planned":
            in_planned = True
            continue
        if in_planned and line.startswith("## "):
            break
        if not in_planned:
            continue
        stripped = line.strip()
        match = re.match(r"(?:[-*]|\d+\.) \[ \] (.+)", stripped)
        if match:
            return match.group(1).strip()
    return "none"


def route_packet(
    *,
    manifest: dict[str, Any],
    payload: dict[str, Any],
    rows: list[dict[str, Any]],
    report_status: str,
    repo_root: Path,
) -> dict[str, Any]:
    shardloom_rows = [row for row in rows if str(row.get("engine") or "").startswith("shardloom")]
    external_rows = [row for row in rows if not str(row.get("engine") or "").startswith("shardloom")]
    operator_table = dashboard_table(payload, "operator_mode_inventory")
    candidate_table = dashboard_table(payload, "operator_hot_path_candidates")
    return {
        "schema_version": ROUTE_PACKET_SCHEMA_VERSION,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "status": report_status,
        "benchmark_profile": manifest.get("benchmark_profile"),
        "artifact_status": manifest.get("artifact_status"),
        "route_runtime_status_counts": counter_dict(rows, "route_runtime_status"),
        "operator_execution_mode_counts": counter_dict(rows, "operator_execution_mode"),
        "shardloom_claim_grade_rows": sum(
            1 for row in shardloom_rows if row.get("claim_gate_status") == "claim_grade"
        ),
        "shardloom_unsupported_rows": sum(
            1
            for row in shardloom_rows
            if row.get("status") == "unsupported"
            or row.get("route_runtime_status") == "unsupported"
        ),
        "external_baseline_rows": len(external_rows),
        "external_unsupported_rows": sum(1 for row in external_rows if row.get("status") == "unsupported"),
        "primary_bottleneck": primary_bottleneck(payload),
        "operator_inventory_status": operator_table.get("status", "not_reported"),
        "operator_hot_path_candidates": candidate_table.get("rows", [])[:5],
        "relevant_files": [
            "website/assets/benchmarks/latest/manifest.json",
            "website/assets/benchmarks/latest/benchmark-results.json",
            "website/assets/benchmarks/latest/published-benchmark-rows-*.json.gz",
            "scripts/check_benchmark_artifact_completeness.py",
            "scripts/check_benchmark_publication_claim_gate.py",
            "scripts/check_benchmark_publish_doctor.py",
            "docs/architecture/phased-execution-plan.md",
        ],
        "required_validators": list(REQUIRED_VALIDATORS),
        "forbidden_claims": list(FORBIDDEN_CLAIMS),
        "next_implementation_slice": first_unchecked_phase_item(repo_root),
        "claim_boundary": (
            "route packet summarizes publication readiness only; it does not authorize "
            "performance, production, replacement, package, object-store, or broad encoded-native claims"
        ),
        "fallback_boundary": (
            "external engines are baseline context only; ShardLoom rows must preserve "
            "fallback_attempted=false and external_engine_invoked=false"
        ),
    }


def render_packet_markdown(packet: dict[str, Any]) -> str:
    lines = [
        "# Benchmark Route Packet",
        "",
        f"- Status: `{packet['status']}`",
        f"- Profile: `{packet.get('benchmark_profile')}`",
        f"- Artifact status: `{packet.get('artifact_status')}`",
        f"- Route runtime status counts: `{packet.get('route_runtime_status_counts')}`",
        f"- Operator execution mode counts: `{packet.get('operator_execution_mode_counts')}`",
        f"- ShardLoom claim-grade rows: `{packet.get('shardloom_claim_grade_rows')}`",
        f"- ShardLoom unsupported rows: `{packet.get('shardloom_unsupported_rows')}`",
        f"- External baseline rows: `{packet.get('external_baseline_rows')}`",
        f"- External unsupported rows: `{packet.get('external_unsupported_rows')}`",
        f"- Primary bottleneck: `{packet.get('primary_bottleneck')}`",
        f"- Operator inventory status: `{packet.get('operator_inventory_status')}`",
        f"- Next implementation slice: `{packet.get('next_implementation_slice')}`",
        "",
        "## Required Validators",
        "",
        *[f"- `{command}`" for command in packet.get("required_validators", [])],
        "",
        "## Forbidden Claims",
        "",
        *[f"- {claim}" for claim in packet.get("forbidden_claims", [])],
        "",
        "## Claim Boundary",
        "",
        str(packet.get("claim_boundary")),
        "",
        "## Fallback Boundary",
        "",
        str(packet.get("fallback_boundary")),
        "",
    ]
    return "\n".join(lines)


def source_command(payload: dict[str, Any], profile: str) -> str:
    metadata = payload.get("published_benchmark_artifact")
    source = metadata.get("source") if isinstance(metadata, dict) else None
    if not source:
        return "not_reported"
    return f"python3 scripts/promote_benchmark_artifact.py --input {source} --profile {profile}"


def next_command(
    *,
    completeness_blockers: list[str],
    claim_gate_blockers: list[str],
    mirror_blockers: list[str],
) -> str:
    if completeness_blockers:
        return "python3 scripts/check_benchmark_artifact_completeness.py --manifest website/assets/benchmarks/latest/manifest.json"
    if claim_gate_blockers:
        return "python3 scripts/check_benchmark_publication_claim_gate.py --manifest website/assets/benchmarks/latest/manifest.json --allow-stale-git --allow-dirty-worktree"
    if mirror_blockers:
        return "cd website-src && PATH=/Users/dylan/.cache/codex-runtimes/codex-primary-runtime/dependencies/node/bin:$PATH node scripts/prebuild-static.mjs --reset-output && ./node_modules/.bin/astro build && node scripts/postbuild-static.mjs"
    return "python3 scripts/check_release_readiness.py --allow-blocked"


def build_report(
    *,
    manifest_path: Path,
    repo_root: Path = ROOT,
    pre_5j_dependency_report_path: Path = DEFAULT_PRE_5J_DEPENDENCY_REPORT,
    allow_incomplete: bool = False,
    require_current_git: bool = True,
    allow_dirty_worktree: bool = False,
    max_age_days: int = DEFAULT_MAX_AGE_DAYS,
) -> tuple[dict[str, Any], dict[str, Any]]:
    if default_public_benchmark_manifest_retired(manifest_path):
        completeness_report = retired_public_benchmark_report(manifest_path)
        claim_gate = retired_public_benchmark_publication_report(manifest_path)
        mirror = retired_static_benchmark_artifact_status(repo_root)
        manifest = {
            "benchmark_profile": "public_site_retired",
            "artifact_status": "retired_from_public_website",
            "performance_claim_allowed": False,
        }
        blockers = [f"retired public benchmark surface: {blocker}" for blocker in mirror["blockers"]]
        status = "passed" if not blockers else "blocked"
        packet = route_packet(
            manifest=manifest,
            payload={},
            rows=[],
            report_status=status,
            repo_root=repo_root,
        )
        packet.update(
            {
                "public_benchmark_surface": PUBLIC_BENCHMARK_SURFACE,
                "public_benchmark_url": CLICKBENCH_URL,
                "artifact_status": "retired_from_public_website",
                "required_validators": [
                    "python3 scripts/check_benchmark_publish_doctor.py",
                    "python3 scripts/check_website_readiness.py --output target/website-readiness-report.json",
                    "node website/validate_static_assets.js",
                    "git diff --check",
                ],
                "relevant_files": [
                    "website-src/src/pages/benchmarks.astro",
                    "scripts/check_website_readiness.py",
                    "website-public/validate_static_assets.js",
                    "scripts/check_benchmark_publish_doctor.py",
                ],
            }
        )
        report = {
            "schema_version": SCHEMA_VERSION,
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "status": status,
            "manifest": str(manifest_path),
            "benchmark_profile": "public_site_retired",
            "artifact_status": "retired_from_public_website",
            "public_benchmark_surface": PUBLIC_BENCHMARK_SURFACE,
            "public_benchmark_url": CLICKBENCH_URL,
            "artifact_generated_at_utc": None,
            "benchmark_git_sha": None,
            "shardloom_git_sha": None,
            "artifact_json": None,
            "artifact_json_sha256": None,
            "source_command": "not_applicable_public_site_dashboard_retired",
            "row_count": 0,
            "shardloom_row_count": 0,
            "external_baseline_only_row_count": 0,
            "shardloom_unsupported_row_count": 0,
            "external_unsupported_row_count": 0,
            "claim_grade_row_count": 0,
            "route_runtime_status_counts": {},
            "operator_execution_mode_counts": {},
            "route_timing_ledger_status_counts": {},
            "timing_ledger_valid_row_count": 0,
            "artifact_completeness_status": completeness_report["status"],
            "artifact_completeness_blockers": completeness_report["blockers"],
            "publication_claim_gate_status": claim_gate["status"],
            "publication_claim_gate_blockers": claim_gate["blockers"],
            "mirror_status": mirror,
            "nearest_next_validation_command": "python3 scripts/check_website_readiness.py --output target/website-readiness-report.json",
            "route_packet_ref": str(DEFAULT_PACKET_JSON),
            "route_packet_markdown_ref": str(DEFAULT_PACKET_MD),
            "benchmark_run_performed": False,
            "fallback_attempted": False,
            "external_engine_invoked": False,
            "retired_static_artifact_contract": (
                "The public website no longer publishes the internal ShardLoom benchmark "
                "dashboard bundle; it links to ClickBench as the public comparison surface."
            ),
            "blockers": blockers,
        }
        return report, packet
    completeness_blockers, manifest = validate_manifest(manifest_path, allow_incomplete)
    claim_gate = validate_publication_claim_gate(
        manifest_path,
        repo_root=repo_root,
        pre_5j_dependency_report_path=pre_5j_dependency_report_path,
        allow_incomplete=allow_incomplete,
        require_current_git=require_current_git,
        allow_dirty_worktree=allow_dirty_worktree,
        max_age_days=max_age_days,
    )
    payload = artifact_payload(manifest, manifest_path)
    rows = result_rows(payload)
    mirror = mirror_status(repo_root)
    artifact_paths = manifest.get("artifact_paths") if isinstance(manifest, dict) else {}
    json_ref = artifact_paths.get("json") if isinstance(artifact_paths, dict) else None
    json_path = repo_path(str(json_ref), manifest_path) if json_ref else None

    blockers = [
        *[f"artifact completeness: {blocker}" for blocker in completeness_blockers],
        *[
            f"publication claim gate: {blocker}"
            for blocker in claim_gate.get("blockers", [])
        ],
        *[f"website artifact mirror: {blocker}" for blocker in mirror["blockers"]],
    ]
    status = "passed" if not blockers else "blocked"
    packet = route_packet(
        manifest=manifest,
        payload=payload,
        rows=rows,
        report_status=status,
        repo_root=repo_root,
    )
    shardloom_rows = [row for row in rows if str(row.get("engine") or "").startswith("shardloom")]
    external_rows = [row for row in rows if not str(row.get("engine") or "").startswith("shardloom")]
    report = {
        "schema_version": SCHEMA_VERSION,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "status": status,
        "manifest": str(manifest_path),
        "benchmark_profile": manifest.get("benchmark_profile"),
        "artifact_status": manifest.get("artifact_status"),
        "artifact_generated_at_utc": manifest.get("generated_at_utc"),
        "benchmark_git_sha": manifest.get("benchmark_git_sha"),
        "shardloom_git_sha": manifest.get("shardloom_git_sha"),
        "artifact_json": str(json_path) if json_path else None,
        "artifact_json_sha256": file_sha256(json_path) if json_path else None,
        "source_command": source_command(payload, str(manifest.get("benchmark_profile") or "")),
        "row_count": len(rows),
        "shardloom_row_count": len(shardloom_rows),
        "external_baseline_only_row_count": len(external_rows),
        "shardloom_unsupported_row_count": packet["shardloom_unsupported_rows"],
        "external_unsupported_row_count": packet["external_unsupported_rows"],
        "claim_grade_row_count": sum(
            1 for row in rows if row.get("claim_gate_status") == "claim_grade"
        ),
        "route_runtime_status_counts": counter_dict(rows, "route_runtime_status"),
        "operator_execution_mode_counts": counter_dict(rows, "operator_execution_mode"),
        "route_timing_ledger_status_counts": counter_dict(
            rows, "route_timing_ledger_status"
        ),
        "timing_ledger_valid_row_count": sum(
            1 for row in rows if row.get("route_timing_ledger_status") == "valid"
        ),
        "artifact_completeness_status": "passed" if not completeness_blockers else "blocked",
        "artifact_completeness_blockers": completeness_blockers,
        "publication_claim_gate_status": claim_gate.get("status"),
        "publication_claim_gate_blockers": claim_gate.get("blockers", []),
        "mirror_status": mirror,
        "nearest_next_validation_command": next_command(
            completeness_blockers=completeness_blockers,
            claim_gate_blockers=list(claim_gate.get("blockers", [])),
            mirror_blockers=mirror["blockers"],
        ),
        "route_packet_ref": str(DEFAULT_PACKET_JSON),
        "route_packet_markdown_ref": str(DEFAULT_PACKET_MD),
        "benchmark_run_performed": False,
        "fallback_attempted": False,
        "external_engine_invoked": False,
        "blockers": blockers,
    }
    return report, packet


def main() -> int:
    args = parse_args()
    repo_root = args.repo_root.resolve()
    manifest_path = args.manifest if args.manifest.is_absolute() else repo_root / args.manifest
    report, packet = build_report(
        manifest_path=manifest_path,
        repo_root=repo_root,
        pre_5j_dependency_report_path=args.pre_5j_dependency_report,
        allow_incomplete=args.allow_incomplete,
        require_current_git=not args.allow_stale_git,
        allow_dirty_worktree=args.allow_dirty_worktree,
        max_age_days=args.max_age_days,
    )
    output = args.output if args.output.is_absolute() else repo_root / args.output
    packet_json = args.packet_json if args.packet_json.is_absolute() else repo_root / args.packet_json
    packet_md = args.packet_md if args.packet_md.is_absolute() else repo_root / args.packet_md
    report["route_packet_ref"] = str(packet_json)
    report["route_packet_markdown_ref"] = str(packet_md)
    write_json(output, report)
    write_json(packet_json, packet)
    packet_md.parent.mkdir(parents=True, exist_ok=True)
    packet_md.write_text(render_packet_markdown(packet), encoding="utf-8")
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
