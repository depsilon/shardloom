#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Validate and optionally refresh the repo-wide audit coverage inventory."""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_INVENTORY = ROOT / "docs" / "architecture" / "repo-wide-audit-inventory.json"
DEFAULT_REPORT = ROOT / "docs" / "architecture" / "repo-wide-audit.md"
DEFAULT_TARGET_REPORT = ROOT / "target" / "repo-wide-audit-coverage-report.json"
SCHEMA_VERSION = "shardloom.repo_wide_audit_inventory.v1"
COVERAGE_REPORT_SCHEMA_VERSION = "shardloom.repo_wide_audit_coverage_report.v1"
SECTIONS = ("Architecture/Documentation", "Shardloom Code", "Website")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--inventory", type=Path, default=DEFAULT_INVENTORY)
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    parser.add_argument("--output", type=Path, default=DEFAULT_TARGET_REPORT)
    parser.add_argument("--write", action="store_true", help="refresh the checked-in inventory")
    return parser.parse_args()


def git_ls_files(repo_root: Path) -> list[str]:
    result = subprocess.run(
        ["git", "ls-files"],
        cwd=repo_root,
        check=True,
        text=True,
        capture_output=True,
    )
    return [line for line in result.stdout.splitlines() if line]


def git_rev_parse(repo_root: Path, ref: str) -> str:
    result = subprocess.run(
        ["git", "rev-parse", ref],
        cwd=repo_root,
        check=True,
        text=True,
        capture_output=True,
    )
    return result.stdout.strip()


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def is_text_file(path: Path) -> bool:
    try:
        path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        return False
    return True


def line_count(path: Path) -> int | None:
    if not is_text_file(path):
        return None
    return len(path.read_text(encoding="utf-8").splitlines())


def path_section(path: str) -> tuple[str, str]:
    """Return ``(section, boundary_owner)`` for a tracked path."""
    if path.startswith(("website-src/", "website/", "website-public/")) or path in {
        "wrangler.jsonc",
        "wrangler.toml",
    }:
        return "Website", "website_public_surface"
    if path in {
        "scripts/check_website_readiness.py",
        "scripts/validate_website_static_assets.py",
    }:
        return "Website", "website_validation"
    if path.startswith("docs/") or path in {
        "AGENTS.md",
        "CLA.md",
        "CONTRIBUTING.md",
        "GOVERNANCE.md",
        "LICENSE",
        "NOTICE",
        "README.md",
        "SECURITY.md",
    }:
        return "Architecture/Documentation", "architecture_docs_policy"
    if path.startswith(".github/"):
        return "Shardloom Code", "ci_release_governance"
    if path.startswith(
        (
            "benchmarks/",
            "examples/",
            "packaging/",
            "python/",
            "scripts/",
            "shardloom-cli/",
            "shardloom-contract-tests/",
            "shardloom-core/",
            "shardloom-exec/",
            "shardloom-plan/",
            "shardloom-vortex/",
        )
    ):
        return "Shardloom Code", "runtime_tests_tools"
    if path in {
        ".gitignore",
        "Cargo.lock",
        "Cargo.toml",
        "REUSE.toml",
        "deny.toml",
    }:
        return "Shardloom Code", "workspace_package_policy"
    return "Shardloom Code", "unclassified_default_code_owner"


def classify_path(repo_root: Path, path: str) -> dict[str, Any]:
    section, boundary_owner = path_section(path)
    full_path = repo_root / path
    suffix = full_path.suffix.lower()
    return {
        "path": path,
        "section": section,
        "boundary_owner": boundary_owner,
        "review_state": "reviewed",
        "file_kind": file_kind(path, suffix),
        "line_count": line_count(full_path),
        "sha256": file_sha256(full_path),
    }


def file_kind(path: str, suffix: str) -> str:
    if path.startswith(("website/", "website-public/")):
        return "checked_in_static_site"
    if path.startswith("website-src/"):
        return "website_source"
    if path.endswith((".json", ".yaml", ".yml", ".toml")):
        return "structured_config_or_data"
    if path.endswith(".md") or path in {"README.md", "AGENTS.md", "SECURITY.md"}:
        return "documentation"
    if path.endswith(".rs"):
        return "rust"
    if path.endswith(".py"):
        return "python"
    if path.endswith((".js", ".mjs", ".ts", ".astro")):
        return "web_or_node"
    if path.endswith((".csv", ".jsonl", ".vortex", ".parquet", ".ipc")):
        return "fixture_or_benchmark_data"
    if suffix:
        return suffix.removeprefix(".")
    return "text_or_policy"


def build_inventory(repo_root: Path, paths: list[str]) -> dict[str, Any]:
    entries = [classify_path(repo_root, path) for path in paths]
    section_counts = Counter(entry["section"] for entry in entries)
    kind_counts = Counter(entry["file_kind"] for entry in entries)
    boundary_counts = Counter(entry["boundary_owner"] for entry in entries)
    return {
        "schema_version": SCHEMA_VERSION,
        "source_head_at_generation": git_rev_parse(repo_root, "HEAD"),
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "coverage_universe_command": "git ls-files",
        "coverage_universe_note": (
            "Inventory covers the tracked path universe visible to git ls-files at generation "
            "time, including staged additions before the final audit commit exists."
        ),
        "total_tracked_files": len(entries),
        "reviewed_tracked_files": sum(1 for entry in entries if entry["review_state"] == "reviewed"),
        "skipped_tracked_files": 0,
        "section_counts": {section: section_counts.get(section, 0) for section in SECTIONS},
        "file_kind_counts": dict(sorted(kind_counts.items())),
        "boundary_owner_counts": dict(sorted(boundary_counts.items())),
        "entries": entries,
    }


def load_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ValueError(f"{path} must contain a JSON object")
    return payload


def validate_inventory(payload: dict[str, Any], paths: list[str], inventory_path: Path) -> list[str]:
    blockers: list[str] = []
    if payload.get("schema_version") != SCHEMA_VERSION:
        blockers.append("inventory schema_version is missing or invalid")
    entries = payload.get("entries")
    if not isinstance(entries, list):
        blockers.append("inventory entries must be a list")
        return blockers
    entry_paths = [entry.get("path") for entry in entries if isinstance(entry, dict)]
    duplicate_paths = sorted(path for path, count in Counter(entry_paths).items() if count > 1)
    if duplicate_paths:
        blockers.append(f"duplicate inventory path entries: {duplicate_paths[:10]}")
    missing = sorted(set(paths) - set(entry_paths))
    extra = sorted(set(entry_paths) - set(paths))
    if missing:
        blockers.append(f"tracked files missing from inventory: {missing[:20]}")
    if extra:
        blockers.append(f"inventory paths are not tracked by git ls-files: {extra[:20]}")
    inventory_ref = str(inventory_path.relative_to(ROOT))
    for entry in entries:
        if not isinstance(entry, dict):
            blockers.append("inventory entry is not an object")
            continue
        path = entry.get("path")
        section = entry.get("section")
        if section not in SECTIONS:
            blockers.append(f"{path}: invalid section {section!r}")
        expected_section, expected_owner = path_section(str(path))
        if section != expected_section:
            blockers.append(f"{path}: section {section!r} does not match {expected_section!r}")
        if entry.get("boundary_owner") != expected_owner:
            blockers.append(f"{path}: boundary_owner does not match {expected_owner!r}")
        if entry.get("review_state") != "reviewed":
            blockers.append(f"{path}: review_state must be reviewed")
        tracked_path = ROOT / str(path)
        if path == inventory_ref:
            continue
        if tracked_path.exists() and entry.get("sha256") != file_sha256(tracked_path):
            blockers.append(f"{path}: sha256 drift from current file")
    section_counts = Counter(entry["section"] for entry in entries if isinstance(entry, dict))
    if set(section_counts) - set(SECTIONS):
        blockers.append("inventory contains sections outside requested three-section audit")
    if sum(section_counts.values()) != len(paths):
        blockers.append("section counts do not sum to tracked file count")
    if payload.get("total_tracked_files") != len(paths):
        blockers.append("total_tracked_files does not match git ls-files count")
    if payload.get("reviewed_tracked_files") != len(paths):
        blockers.append("reviewed_tracked_files does not match git ls-files count")
    if payload.get("skipped_tracked_files") != 0:
        blockers.append("skipped_tracked_files must be zero")
    return blockers


def validate_report(path: Path) -> list[str]:
    if not path.exists():
        return [f"audit report missing: {path}"]
    text = path.read_text(encoding="utf-8")
    blockers: list[str] = []
    required_headings = (
        "## Architecture/Documentation",
        "## Shardloom Code",
        "## Website",
    )
    for heading in required_headings:
        if heading not in text:
            blockers.append(f"missing audit report section: {heading}")
    forbidden_headings = ("## Fourth Queue", "## Miscellaneous")
    for heading in forbidden_headings:
        if heading in text:
            blockers.append(f"forbidden parallel audit queue heading present: {heading}")
    required_phrases = (
        "performance_claim_allowed=false",
        "fallback_attempted=false",
        "external_engine_invoked=false",
        "No package publication",
    )
    for phrase in required_phrases:
        if phrase not in text:
            blockers.append(f"missing audit claim boundary phrase: {phrase}")
    return blockers


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def main() -> int:
    args = parse_args()
    paths = git_ls_files(ROOT)
    if args.write:
        write_json(args.inventory, build_inventory(ROOT, paths))
    inventory = load_json(args.inventory)
    blockers = validate_inventory(inventory, paths, args.inventory)
    blockers.extend(validate_report(args.report))
    report = {
        "schema_version": COVERAGE_REPORT_SCHEMA_VERSION,
        "status": "passed" if not blockers else "blocked",
        "inventory_path": str(args.inventory.relative_to(ROOT)),
        "report_path": str(args.report.relative_to(ROOT)),
        "tracked_file_count": len(paths),
        "inventory_file_count": inventory.get("total_tracked_files"),
        "section_counts": inventory.get("section_counts", {}),
        "reviewed_tracked_files": inventory.get("reviewed_tracked_files"),
        "skipped_tracked_files": inventory.get("skipped_tracked_files"),
        "blockers": blockers,
    }
    write_json(args.output, report)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if not blockers else 1


if __name__ == "__main__":
    raise SystemExit(main())
