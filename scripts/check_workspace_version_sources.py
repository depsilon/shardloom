#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Validate manifest-derived Rust and Vortex version source contracts.

This gate keeps ShardLoom's active Rust/Vortex version surfaces centralized:

- Rust MSRV is read from root `Cargo.toml` `[workspace.package].rust-version`.
- The upstream Vortex crate line is read from root `Cargo.toml`
  `[workspace.dependencies].vortex`.
- Workspace crates inherit `rust-version.workspace = true`.
- `shardloom-vortex` inherits `vortex = { workspace = true, optional = true }`.
- CI and release evidence use `scripts/release_report_utils.py` instead of
  duplicating current-version literals.
"""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any

from release_report_utils import (
    fail_closed_fields,
    read_text,
    workspace_rust_version,
    workspace_version_env,
)


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_VERSION = "shardloom.workspace_version_source_report.v1"
DEFAULT_OUTPUT = Path("target/workspace-version-source-report.json")
INTERNAL_WORKSPACE_DEPENDENCIES = (
    "shardloom-core",
    "shardloom-exec",
    "shardloom-plan",
    "shardloom-vortex",
)
ACTIVE_DOC_VERSION_CONSUMERS = (
    "docs/architecture/effectful-operation-admission-matrix.md",
    "docs/architecture/pulseweave-runtime-control.md",
    "docs/architecture/wrapper-connector-implementation-registry.md",
    "docs/dependencies/vortex-upstream-release-intake-runbook.md",
    "docs/release/ci-gate-matrix.md",
    "docs/release/hard-release-readiness-gate.md",
)
PINNED_RUST_TOOLCHAIN_PATTERNS = (
    re.compile(r"\bcargo \+\d+\.\d+(?:\.\d+)?\b"),
    re.compile(r"RUSTUP_TOOLCHAIN\s*=\s*['\"]\d+\.\d+(?:\.\d+)?['\"]"),
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=ROOT)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    return parser.parse_args()


def resolve(repo_root: Path, path: Path | str) -> Path:
    candidate = Path(path)
    return candidate if candidate.is_absolute() else repo_root / candidate


def quoted_values(text: str) -> list[str]:
    return [match.group(1) for match in re.finditer(r'"([^"\\]*(?:\\.[^"\\]*)*)"', text)]


def workspace_members(repo_root: Path) -> list[str]:
    text = read_text(repo_root / "Cargo.toml", missing_ok=False)
    members: list[str] = []
    in_members = False
    for line in text.splitlines():
        stripped = line.split("#", 1)[0].strip()
        if not in_members and stripped.startswith("members"):
            in_members = "[" in stripped
        if in_members:
            members.extend(quoted_values(stripped))
            if "]" in stripped:
                break
    return members


def require_marker(blockers: list[str], label: str, text: str, marker: str) -> None:
    if marker not in text:
        blockers.append(f"{label}: missing marker {marker!r}")


def forbid_marker(blockers: list[str], label: str, text: str, marker: str) -> None:
    if marker in text:
        blockers.append(f"{label}: forbidden marker {marker!r}")


def toml_dotted_key_value(text: str, key: str) -> str | None:
    for line in text.splitlines():
        stripped = line.split("#", 1)[0].strip()
        if not stripped or stripped.startswith("["):
            continue
        candidate, separator, value = stripped.partition("=")
        if not separator:
            continue
        if candidate.strip() == key:
            return value.strip()
    return None


def require_toml_dotted_key_value(
    blockers: list[str],
    label: str,
    text: str,
    key: str,
    expected: str,
    message: str,
) -> None:
    actual = toml_dotted_key_value(text, key)
    if actual != expected:
        blockers.append(f"{label}: {message}")


def cargo_member_inheritance_checks(repo_root: Path, blockers: list[str]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    members = workspace_members(repo_root)
    if not members:
        blockers.append("root Cargo.toml workspace.members is empty or missing")
        return rows
    for member in members:
        manifest = repo_root / member / "Cargo.toml"
        row_blockers: list[str] = []
        text = read_text(manifest, missing_ok=True)
        if not text:
            row_blockers.append(f"{member}/Cargo.toml missing")
        else:
            require_toml_dotted_key_value(
                row_blockers,
                member,
                text,
                "version.workspace",
                "true",
                "package version must inherit workspace version",
            )
            require_toml_dotted_key_value(
                row_blockers,
                member,
                text,
                "rust-version.workspace",
                "true",
                "rust-version must inherit workspace rust-version",
            )
            for dependency in INTERNAL_WORKSPACE_DEPENDENCIES:
                actual = toml_dotted_key_value(text, dependency)
                if actual is not None and actual != "{ workspace = true }":
                    row_blockers.append(
                        f"internal dependency {dependency} must inherit workspace dependency"
                    )
        blockers.extend(f"{member}: {blocker}" for blocker in row_blockers)
        rows.append(
            {
                "member": member,
                "manifest": f"{member}/Cargo.toml",
                "status": "passed" if not row_blockers else "blocked",
                "blockers": row_blockers,
            }
        )
    return rows


def build_report(repo_root: Path) -> dict[str, Any]:
    repo_root = repo_root.resolve()
    blockers: list[str] = []

    try:
        version_env = workspace_version_env(repo_root)
        rust_version = workspace_rust_version(repo_root)
    except Exception as exc:  # pragma: no cover - exercised through script failure path.
        version_env = {}
        rust_version = ""
        blockers.append(f"workspace version env derivation failed: {exc}")

    root_cargo = read_text(repo_root / "Cargo.toml", missing_ok=True)
    require_marker(blockers, "root Cargo.toml", root_cargo, "[workspace.package]")
    require_marker(blockers, "root Cargo.toml", root_cargo, "rust-version =")
    require_marker(blockers, "root Cargo.toml", root_cargo, "[workspace.dependencies]")
    require_marker(blockers, "root Cargo.toml", root_cargo, "vortex =")

    member_rows = cargo_member_inheritance_checks(repo_root, blockers)

    vortex_manifest = read_text(repo_root / "shardloom-vortex/Cargo.toml", missing_ok=True)
    require_marker(
        blockers,
        "shardloom-vortex/Cargo.toml",
        vortex_manifest,
        "vortex = { workspace = true, optional = true }",
    )
    forbid_marker(blockers, "shardloom-vortex/Cargo.toml", vortex_manifest, 'vortex = "')
    forbid_marker(
        blockers,
        "shardloom-vortex/Cargo.toml",
        vortex_manifest,
        "vortex = { version =",
    )

    vortex_build = read_text(repo_root / "shardloom-vortex/build.rs", missing_ok=True)
    for marker in [
        'workspace_dependency_version(&workspace_manifest_text, "vortex")',
        "cargo:rustc-env=SHARDLOOM_UPSTREAM_VORTEX_PROVIDER_VERSION={vortex_version}",
    ]:
        require_marker(blockers, "shardloom-vortex/build.rs", vortex_build, marker)
    forbidden_build_literals = {
        '"0.72"',
        '"0.73"',
        '"0.74"',
        f'"{version_env.get("SHARDLOOM_UPSTREAM_VORTEX_MANIFEST_VERSION", "")}"',
    }
    for marker in sorted(item for item in forbidden_build_literals if item != '""'):
        forbid_marker(blockers, "shardloom-vortex/build.rs", vortex_build, marker)

    release_utils = read_text(repo_root / "scripts/release_report_utils.py", missing_ok=True)
    for marker in [
        "def workspace_rust_version",
        "def upstream_vortex_manifest_version",
        "def upstream_vortex_lock_version",
        "def upstream_vortex_provider_version",
        "def workspace_version_env",
    ]:
        require_marker(blockers, "scripts/release_report_utils.py", release_utils, marker)

    ci_env_writer = read_text(repo_root / "scripts/write_ci_version_env.py", missing_ok=True)
    require_marker(
        blockers,
        "scripts/write_ci_version_env.py",
        ci_env_writer,
        "workspace_version_env",
    )
    for marker in [
        "rust_toolchain_version",
        "rust_msrv_lane_id",
        "upstream_vortex_manifest_version",
        "upstream_vortex_lock_version",
        "upstream_vortex_provider_version",
    ]:
        forbid_marker(blockers, "scripts/write_ci_version_env.py", ci_env_writer, marker)

    workflow = read_text(repo_root / ".github/workflows/ci.yml", missing_ok=True)
    for marker in [
        'python scripts/write_ci_version_env.py --github-env "$GITHUB_ENV"',
        "$SHARDLOOM_RUST_MSRV_TOOLCHAIN",
        "$SHARDLOOM_RUST_MSRV_LANE",
    ]:
        require_marker(blockers, ".github/workflows/ci.yml", workflow, marker)

    active_version_consumers = [
        ".github/workflows/ci.yml",
        ".github/workflows/pypi-publish-draft.yml",
        "scripts/write_ci_version_env.py",
        "scripts/check_release_readiness.py",
        "scripts/check_package_channel_readiness.py",
        "scripts/python_registry_package_proof.py",
        "scripts/run_release_validation_evidence.py",
        "scripts/check_ci_gate_matrix.py",
        "scripts/check_v1_security_ci_hardening.py",
        "benchmarks/traditional_analytics/run.py",
    ]
    current_literals = {
        rust_version,
        str(version_env.get("SHARDLOOM_RUST_MSRV_TOOLCHAIN", "")),
        str(version_env.get("SHARDLOOM_UPSTREAM_VORTEX_MANIFEST_VERSION", "")),
        str(version_env.get("SHARDLOOM_UPSTREAM_VORTEX_LOCK_VERSION", "")),
        str(version_env.get("SHARDLOOM_UPSTREAM_VORTEX_PROVIDER_VERSION", "")),
    }
    for relative_path in active_version_consumers:
        text = read_text(repo_root / relative_path, missing_ok=True)
        for literal in sorted(item for item in current_literals if item):
            if literal in text:
                blockers.append(
                    f"{relative_path}: duplicate active version literal {literal!r}; "
                    "derive it from scripts/release_report_utils.py"
                )
    for relative_path in ACTIVE_DOC_VERSION_CONSUMERS:
        text = read_text(repo_root / relative_path, missing_ok=True)
        for pattern in PINNED_RUST_TOOLCHAIN_PATTERNS:
            for match in pattern.finditer(text):
                blockers.append(
                    f"{relative_path}: pinned Rust toolchain command {match.group(0)!r}; "
                    "derive it with scripts/write_ci_version_env.py"
                )

    benchmark_harness = read_text(
        repo_root / "benchmarks/traditional_analytics/run.py",
        missing_ok=True,
    )
    for marker in [
        "from release_report_utils import upstream_vortex_provider_version",
        "UPSTREAM_VORTEX_PROVIDER_VERSION = upstream_vortex_provider_version(REPO_ROOT)",
    ]:
        require_marker(blockers, "benchmarks/traditional_analytics/run.py", benchmark_harness, marker)

    freshness_gate = read_text(
        repo_root / "scripts/check_pre_5j_dependency_freshness.py",
        missing_ok=True,
    )
    for marker in [
        "$CURRENT_VORTEX_MANIFEST_VERSION",
        "$CURRENT_VORTEX_LOCK_VERSION",
        "$CURRENT_VORTEX_PROVIDER_VERSION",
        "upstream_vortex_provider_version(repo_root)",
    ]:
        require_marker(blockers, "scripts/check_pre_5j_dependency_freshness.py", freshness_gate, marker)

    report: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "status": "passed" if not blockers else "blocked",
        "workspace_version_sources_status": "passed" if not blockers else "blocked",
        "blockers": blockers,
        "version_env": version_env,
        "rust_msrv_source": "Cargo.toml#[workspace.package].rust-version",
        "upstream_vortex_manifest_source": "Cargo.toml#[workspace.dependencies].vortex",
        "upstream_vortex_lock_source": "Cargo.lock#package:vortex.version",
        "upstream_vortex_provider_source": "Cargo.toml#[workspace.dependencies].vortex via shardloom-vortex/build.rs",
        "cargo_member_count": len(member_rows),
        "cargo_member_inheritance": member_rows,
        "active_version_literal_audit_paths": active_version_consumers,
        "active_doc_version_literal_audit_paths": list(ACTIVE_DOC_VERSION_CONSUMERS),
        "publication_attempted": False,
        "tag_created": False,
        "secrets_required": False,
        "fallback_attempted": False,
        "external_engine_invoked": False,
        "claim_gate_status": "not_claim_grade",
    }
    report.update(fail_closed_fields())
    return report


def main() -> int:
    args = parse_args()
    repo_root = args.repo_root.resolve()
    output = resolve(repo_root, args.output)
    report = build_report(repo_root)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(f"{report['status']}: {output}")
    if report["blockers"]:
        for blocker in report["blockers"]:
            print(f"- {blocker}")
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
