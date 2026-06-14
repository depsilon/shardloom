#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Small shared helpers for release, docs, and benchmark evidence scripts."""

from __future__ import annotations

import gzip
import json
import re
from pathlib import Path
from typing import Any


def resolve_path(repo_root: Path, path: Path | str) -> Path:
    candidate = Path(path)
    return candidate if candidate.is_absolute() else repo_root / candidate


def read_text(path: Path, *, missing_ok: bool = True) -> str:
    if not path.exists():
        if missing_ok:
            return ""
        raise FileNotFoundError(path)
    return path.read_text(encoding="utf-8")


def load_json(path: Path, *, missing_ok: bool = False) -> Any:
    if not path.exists():
        if missing_ok:
            return None
        raise FileNotFoundError(path)
    if path.name.endswith(".gz"):
        with gzip.open(path, "rt", encoding="utf-8") as handle:
            return json.load(handle)
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def fail_closed_fields() -> dict[str, bool]:
    return {
        "public_release_claim_allowed": False,
        "public_package_claim_allowed": False,
        "performance_claim_allowed": False,
        "production_claim_allowed": False,
        "spark_replacement_claim_allowed": False,
        "publication_attempted": False,
        "tag_created": False,
        "package_upload_attempted": False,
        "fallback_attempted": False,
        "external_engine_invoked": False,
    }


def require_markers(label: str, text: str, markers: tuple[str, ...]) -> list[str]:
    if not text:
        return [f"{label}: missing file or empty text"]
    return [f"{label}: missing marker {marker!r}" for marker in markers if marker not in text]


def _current_section_key(line: str) -> str | None:
    stripped = line.strip()
    if not stripped.startswith("[") or not stripped.endswith("]"):
        return None
    return stripped.strip("[]")


def _quoted_value(raw: str) -> str | None:
    match = re.search(r'"([^"\\]*(?:\\.[^"\\]*)*)"', raw)
    if match is None:
        return None
    try:
        return json.loads(match.group(0))
    except json.JSONDecodeError:
        return match.group(1)


def workspace_rust_version(repo_root: Path) -> str:
    """Return `[workspace.package] rust-version` from the root Cargo manifest."""

    text = read_text(repo_root / "Cargo.toml", missing_ok=False)
    section: str | None = None
    for line in text.splitlines():
        section_key = _current_section_key(line)
        if section_key is not None:
            section = section_key
            continue
        if section != "workspace.package":
            continue
        stripped = line.split("#", 1)[0].strip()
        if stripped.startswith("rust-version"):
            value = _quoted_value(stripped)
            if value:
                return value
    raise ValueError("root Cargo.toml is missing [workspace.package] rust-version")


def rust_toolchain_version(repo_root: Path) -> str:
    """Return the concrete rustup toolchain version for the workspace MSRV."""

    version = workspace_rust_version(repo_root)
    parts = version.split(".")
    return f"{version}.0" if len(parts) == 2 else version


def rust_msrv_lane_id(repo_root: Path) -> str:
    return "rust_msrv_" + "_".join(workspace_rust_version(repo_root).split("."))


def _manifest_dependency_raw(
    text: str,
    *,
    section_name: str,
    dependency: str,
) -> str | None:
    section: str | None = None
    dependency_key = f"{dependency} ="
    for line in text.splitlines():
        section_key = _current_section_key(line)
        if section_key is not None:
            section = section_key
            continue
        if section != section_name:
            continue
        stripped = line.split("#", 1)[0].strip()
        if not stripped.startswith(dependency_key):
            continue
        _, raw_value = stripped.split("=", 1)
        return raw_value.strip()
    return None


def _dependency_version_from_raw(raw_value: str) -> str | None:
    value = _quoted_value(raw_value)
    if value:
        return value
    match = re.search(r'\bversion\s*=\s*"([^"\\]*(?:\\.[^"\\]*)*)"', raw_value)
    if match is None:
        return None
    try:
        return json.loads(f'"{match.group(1)}"')
    except json.JSONDecodeError:
        return match.group(1)


def workspace_manifest_dependency_version(repo_root: Path, dependency: str) -> str:
    text = read_text(repo_root / "Cargo.toml", missing_ok=False)
    raw_value = _manifest_dependency_raw(
        text,
        section_name="workspace.dependencies",
        dependency=dependency,
    )
    if raw_value is None:
        raise ValueError(f"root Cargo.toml is missing workspace dependency {dependency!r}")
    version = _dependency_version_from_raw(raw_value)
    if version:
        return version
    raise ValueError(f"root Cargo.toml workspace dependency {dependency!r} is missing a version")


def cargo_manifest_dependency_version(
    repo_root: Path,
    manifest: Path | str,
    dependency: str,
) -> str:
    text = read_text(resolve_path(repo_root, manifest), missing_ok=False)
    raw_value = _manifest_dependency_raw(
        text,
        section_name="dependencies",
        dependency=dependency,
    )
    if raw_value is None:
        raise ValueError(f"{manifest} is missing dependency {dependency!r}")
    if "workspace = true" in raw_value:
        return workspace_manifest_dependency_version(repo_root, dependency)
    value = _dependency_version_from_raw(raw_value)
    if value:
        return value
    raise ValueError(f"{manifest} is missing dependency {dependency!r}")


def cargo_lock_package_version(repo_root: Path, package_name: str) -> str:
    text = read_text(repo_root / "Cargo.lock", missing_ok=False)
    name: str | None = None
    version: str | None = None
    for line in text.splitlines():
        stripped = line.strip()
        if stripped == "[[package]]":
            if name == package_name and version is not None:
                return version
            name = None
            version = None
            continue
        if stripped.startswith("name = "):
            name = _quoted_value(stripped)
        elif stripped.startswith("version = "):
            version = _quoted_value(stripped)
    if name == package_name and version is not None:
        return version
    raise ValueError(f"Cargo.lock is missing package {package_name!r}")


def upstream_vortex_manifest_version(repo_root: Path) -> str:
    return cargo_manifest_dependency_version(
        repo_root,
        Path("shardloom-vortex/Cargo.toml"),
        "vortex",
    )


def upstream_vortex_lock_version(repo_root: Path) -> str:
    return cargo_lock_package_version(repo_root, "vortex")


def upstream_vortex_provider_version(repo_root: Path) -> str:
    """Return the upstream Vortex provider line used by Rust evidence surfaces.

    `shardloom-vortex/build.rs` exports this value to Rust from the root
    workspace dependency, so Python release tooling should use the same source
    instead of parsing a duplicated Rust string literal.
    """

    return upstream_vortex_manifest_version(repo_root)


def workspace_version_env(repo_root: Path) -> dict[str, str]:
    """Return the manifest-derived version variables shared by CI and evidence tools."""

    return {
        "SHARDLOOM_RUST_MSRV_TOOLCHAIN": rust_toolchain_version(repo_root),
        "SHARDLOOM_RUST_MSRV_LANE": rust_msrv_lane_id(repo_root),
        "SHARDLOOM_UPSTREAM_VORTEX_MANIFEST_VERSION": upstream_vortex_manifest_version(
            repo_root
        ),
        "SHARDLOOM_UPSTREAM_VORTEX_LOCK_VERSION": upstream_vortex_lock_version(
            repo_root
        ),
        "SHARDLOOM_UPSTREAM_VORTEX_PROVIDER_VERSION": upstream_vortex_provider_version(
            repo_root
        ),
    }
