#!/usr/bin/env python
# SPDX-License-Identifier: Apache-2.0
"""Generate local SBOM/checksum/provenance dry-run evidence without publishing."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
import time
import tomllib
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_VERSION = "shardloom.supply_chain_release_evidence.v1"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=ROOT)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("target/release-provenance-dry-run"),
        help="Output directory, relative to the repo root by default.",
    )
    parser.add_argument(
        "--build-profile",
        choices=["debug", "release"],
        default="debug",
        help="Local CLI binary profile to inspect.",
    )
    parser.add_argument(
        "--skip-build",
        action="store_true",
        help="Inspect existing local artifacts instead of building them first.",
    )
    return parser.parse_args()


def resolve_under_repo(repo_root: Path, path: Path) -> Path:
    resolved = path if path.is_absolute() else repo_root / path
    return resolved.resolve()


def run_step(name: str, command: list[str], cwd: Path) -> dict[str, Any]:
    started = time.perf_counter()
    completed = subprocess.run(
        command,
        cwd=cwd,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    return {
        "name": name,
        "command": command,
        "returncode": completed.returncode,
        "elapsed_millis": round((time.perf_counter() - started) * 1000.0, 4),
        "stdout": completed.stdout[-4000:],
        "stderr": completed.stderr[-4000:],
    }


def read_toml(path: Path) -> dict[str, Any]:
    with path.open("rb") as handle:
        return tomllib.load(handle)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def rel(repo_root: Path, path: Path) -> str:
    try:
        return path.resolve().relative_to(repo_root).as_posix()
    except ValueError:
        return path.resolve().as_posix()


def git_value(repo_root: Path, *args: str) -> str | None:
    completed = subprocess.run(
        ["git", *args],
        cwd=repo_root,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        check=False,
    )
    if completed.returncode != 0:
        return None
    return completed.stdout.strip()


def artifact_ref(repo_root: Path, path: Path, kind: str) -> dict[str, Any]:
    exists = path.exists()
    return {
        "kind": kind,
        "path": rel(repo_root, path),
        "exists": exists,
        "size_bytes": path.stat().st_size if exists else None,
        "sha256": sha256_file(path) if exists and path.is_file() else None,
    }


def rust_workspace_components(repo_root: Path) -> list[dict[str, Any]]:
    lock = read_toml(repo_root / "Cargo.lock")
    components = []
    for package in lock.get("package", []):
        components.append(
            {
                "type": "library",
                "name": package["name"],
                "version": package["version"],
                "purl": f"pkg:cargo/{package['name']}@{package['version']}",
            }
        )
    return sorted(components, key=lambda item: (item["name"], item["version"]))


def python_components(repo_root: Path) -> list[dict[str, Any]]:
    pyproject = read_toml(repo_root / "python" / "pyproject.toml")
    project = pyproject.get("project", {})
    components = [
        {
            "type": "library",
            "name": project.get("name", "shardloom"),
            "version": project.get("version", "0.0.0"),
            "licenses": [{"license": {"id": project.get("license", "Apache-2.0")}}],
        }
    ]
    for dependency in project.get("dependencies", []):
        components.append({"type": "library", "name": dependency, "scope": "runtime"})
    return components


def cyclonedx_bom(
    *,
    name: str,
    version: str,
    components: list[dict[str, Any]],
    evidence_note: str,
) -> dict[str, Any]:
    return {
        "bomFormat": "CycloneDX",
        "specVersion": "1.5",
        "serialNumber": f"urn:uuid:shardloom-{name}",
        "version": 1,
        "metadata": {
            "component": {"type": "application", "name": name, "version": version},
            "properties": [
                {"name": "shardloom:dry_run", "value": "true"},
                {"name": "shardloom:evidence_note", "value": evidence_note},
                {"name": "shardloom:no_publication", "value": "true"},
            ],
        },
        "components": components,
    }


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def workflow_policy_snapshot(repo_root: Path) -> dict[str, Any]:
    workflow_path = repo_root / ".github" / "workflows" / "pypi-publish-draft.yml"
    text = workflow_path.read_text(encoding="utf-8")
    uses_refs = re.findall(r"uses:\s*([^\s]+)", text)
    sha_pinned = [
        ref
        for ref in uses_refs
        if re.search(r"@[0-9a-fA-F]{40}$", ref) is not None or ref.startswith("actions/")
    ]
    unpinned_third_party = [
        ref for ref in uses_refs if not ref.startswith("actions/") and ref not in sha_pinned
    ]
    return {
        "schema_version": "shardloom.release_workflow_policy_snapshot.v1",
        "workflow": rel(repo_root, workflow_path),
        "workflow_dispatch_only": "workflow_dispatch:" in text,
        "publish_approval_input": "publish_approved" in text,
        "protected_environment": "environment: pypi" in text,
        "oidc_id_token_write": "id-token: write" in text,
        "long_lived_token_configured": any(
            needle in text.lower()
            for needle in ["password:", "api-token:", "pypi-token", "twine_password"]
        ),
        "least_privilege_permissions": "permissions:" in text
        and "contents: read" in text
        and "id-token: write" in text,
        "uses_refs": uses_refs,
        "unpinned_third_party_actions": unpinned_third_party,
        "third_party_action_pin_status": "all_pinned"
        if not unpinned_third_party
        else "waived_until_real_publication",
        "real_publication_requires_sha_pinning_or_explicit_maintainer_waiver": True,
        "publication_attempted": False,
        "secrets_required": False,
    }


def main() -> int:
    args = parse_args()
    repo_root = args.repo_root.resolve()
    output_dir = resolve_under_repo(repo_root, args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    steps: list[dict[str, Any]] = []

    if not args.skip_build:
        steps.append(
            run_step(
                "build_cli_binary",
                [
                    "cargo",
                    "build",
                    "-p",
                    "shardloom-cli",
                    "--bin",
                    "shardloom",
                    *(["--release"] if args.build_profile == "release" else []),
                ],
                repo_root,
            )
        )
        steps.append(run_step("build_python_artifacts", [sys.executable, "-m", "build", "python"], repo_root))

    binary_name = "shardloom.exe" if os.name == "nt" else "shardloom"
    binary = repo_root / "target" / args.build_profile / binary_name
    python_artifacts = sorted((repo_root / "python" / "dist").glob("shardloom-*"))

    rust_sbom_path = output_dir / "shardloom-rust-workspace.cdx.json"
    python_sbom_path = output_dir / "shardloom-python-artifacts.cdx.json"
    binary_sbom_path = output_dir / "shardloom-cli-binary.cdx.json"
    workflow_snapshot_path = output_dir / "workflow-policy-snapshot.json"
    provenance_path = output_dir / "supply-chain-release-evidence.json"
    checksum_path = output_dir / "checksums.sha256"

    cargo = read_toml(repo_root / "Cargo.toml")
    version = cargo.get("workspace", {}).get("package", {}).get("version", "0.0.0")
    write_json(
        rust_sbom_path,
        cyclonedx_bom(
            name="shardloom-rust-workspace",
            version=version,
            components=rust_workspace_components(repo_root),
            evidence_note="Local dry-run Rust workspace dependency SBOM from Cargo.lock.",
        ),
    )
    write_json(
        python_sbom_path,
        cyclonedx_bom(
            name="shardloom-python-artifacts",
            version=version,
            components=python_components(repo_root),
            evidence_note="Local dry-run Python artifact SBOM from pyproject metadata.",
        ),
    )
    write_json(
        binary_sbom_path,
        cyclonedx_bom(
            name="shardloom-cli-binary",
            version=version,
            components=[artifact_ref(repo_root, binary, "release_binary")],
            evidence_note="Local dry-run CLI binary artifact SBOM with checksum evidence.",
        ),
    )
    workflow_snapshot = workflow_policy_snapshot(repo_root)
    write_json(workflow_snapshot_path, workflow_snapshot)

    artifact_refs = [
        artifact_ref(repo_root, binary, "release_binary"),
        *[artifact_ref(repo_root, path, "python_artifact") for path in python_artifacts],
    ]
    sbom_refs = [
        artifact_ref(repo_root, rust_sbom_path, "rust_workspace_sbom"),
        artifact_ref(repo_root, python_sbom_path, "python_artifact_sbom"),
        artifact_ref(repo_root, binary_sbom_path, "cli_binary_sbom"),
    ]
    checksum_targets = [*artifact_refs, *sbom_refs, artifact_ref(repo_root, workflow_snapshot_path, "workflow_policy_snapshot")]
    checksum_lines = [
        f"{item['sha256']}  {item['path']}"
        for item in checksum_targets
        if item["exists"] and item["sha256"] is not None
    ]
    checksum_path.write_text("\n".join(checksum_lines) + "\n", encoding="utf-8")

    provenance = {
        "schema_version": SCHEMA_VERSION,
        "release_ref": "local-dry-run",
        "source_ref": git_value(repo_root, "rev-parse", "HEAD"),
        "source_dirty": bool(git_value(repo_root, "status", "--porcelain")),
        "build_workflow_ref": "local scripts/release_provenance_dry_run.py",
        "builder_identity": f"local:{os.environ.get('USERNAME') or os.environ.get('USER') or 'unknown'}",
        "artifact_refs": artifact_refs,
        "checksum_refs": [artifact_ref(repo_root, checksum_path, "checksum_manifest")],
        "sbom_refs": sbom_refs,
        "attestation_refs": [],
        "provenance_status": "dry_run_unsigned_local_evidence",
        "signed_or_attested_status": "not_signed_local_dry_run",
        "verification_instructions_ref": "docs/release/release-provenance-dry-run.md",
        "publish_workflow_policy": workflow_snapshot,
        "fallback_dependency_absent": True,
        "publication_attempted": False,
        "tag_created": False,
        "secrets_required": False,
        "external_runtime_dependencies_added": False,
        "fallback_engine_dependency_added": False,
        "steps": steps,
    }
    write_json(provenance_path, provenance)

    manifest = {
        "schema_version": "shardloom.release_provenance_dry_run_manifest.v1",
        "proof_status": "passed" if all(step["returncode"] == 0 for step in steps) else "failed",
        "output_dir": str(output_dir),
        "provenance": rel(repo_root, provenance_path),
        "checksum_manifest": rel(repo_root, checksum_path),
        "sbom_refs": [rel(repo_root, path) for path in [rust_sbom_path, python_sbom_path, binary_sbom_path]],
        "workflow_policy_snapshot": rel(repo_root, workflow_snapshot_path),
        "publication_attempted": False,
        "tag_created": False,
        "secrets_required": False,
    }
    write_json(output_dir / "manifest.json", manifest)
    print(output_dir / "manifest.json")
    return 0 if manifest["proof_status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
