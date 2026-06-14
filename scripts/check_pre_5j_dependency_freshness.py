#!/usr/bin/env python
# SPDX-License-Identifier: Apache-2.0
"""Validate dependency freshness before any GAR-RUNTIME-IMPL-5J benchmark refresh.

This is a preflight gate only. It does not run benchmarks, publish benchmark
artifacts, publish packages, create tags, add secrets, or authorize fallback
execution. Use `--require-live-github` immediately before a 5J benchmark
publication refresh so open Dependabot PRs cannot be skipped accidentally.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - Python 3.10 fallback.
    try:
        import tomli as tomllib  # type: ignore[no-redef]
    except ModuleNotFoundError:
        tomllib = None  # type: ignore[assignment]

from check_dependency_audit import check_runtime_dependency_scope
from release_report_utils import (
    upstream_vortex_lock_version,
    upstream_vortex_manifest_version,
    upstream_vortex_provider_version,
)


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_VERSION = "shardloom.pre_5j_dependency_freshness_gate.v1"
DEFAULT_OUTPUT = Path("target/pre-5j-dependency-freshness-gate.json")
GITHUB_PULLS_URL = "https://api.github.com/repos/depsilon/shardloom/pulls?state=open&per_page=100"
GITHUB_PULLS_HOST = "api.github.com"
GITHUB_PULLS_PATH = "/repos/depsilon/shardloom/pulls"

ADMITTED_DEPENDABOT_PRS: dict[int, dict[str, Any]] = {
    1149: {
        "kind": "github_action",
        "dependency": "actions/download-artifact",
        "expected_workflow": ".github/workflows/ci.yml",
        "required_markers": ["actions/download-artifact@v8"],
        "forbidden_markers": ["actions/download-artifact@v7"],
        "expected_marker_counts": {"actions/download-artifact@v8": 14},
        "review_doc": "docs/dependencies/github-actions-dependency-review.md",
        "review_markers": [
            "Dependabot PR <https://github.com/depsilon/shardloom/pull/1149>",
            "actions/download-artifact@v8",
            "digest-mismatch default is error",
            "fallback execution",
        ],
    },
    1223: {
        "dependency": "vortex",
        "expected_manifest": "shardloom-vortex/Cargo.toml",
        "expected_manifest_version": "$CURRENT_VORTEX_MANIFEST_VERSION",
        "expected_lock_versions": {"vortex": "$CURRENT_VORTEX_LOCK_VERSION"},
        "review_doc": "docs/dependencies/vortex-upstream-review.md",
        "review_markers": [
            "Dependabot PR: <https://github.com/depsilon/shardloom/pull/1223>.",
            "vortex = $CURRENT_VORTEX_MANIFEST_VERSION",
            "`Cargo.lock` records the upstream Vortex crate family at `$CURRENT_VORTEX_LOCK_VERSION`",
            "Vortex query-engine integrations remain prohibited",
        ],
    },
    1226: {
        "dependency": "regex",
        "expected_manifest": "shardloom-core/Cargo.toml",
        "expected_manifest_version": "1.12.4",
        "expected_lock_versions": {"regex": "1.12.4"},
        "review_doc": "docs/dependencies/structured-format-dependency-review.md",
        "review_markers": [
            "Dependabot PR <https://github.com/depsilon/shardloom/pull/1226>",
            "regex = 1.12.4",
            "MIT OR Apache-2.0",
            "fallback execution",
        ],
    },
    1151: {
        "dependency": "serde_json",
        "expected_manifest": "shardloom-vortex/Cargo.toml",
        "expected_manifest_version": "1.0",
        "expected_lock_versions": {"serde_json": "1.0.150"},
        "review_doc": "docs/dependencies/json-digest-dependency-review.md",
        "review_markers": [
            "Dependabot PR <https://github.com/depsilon/shardloom/pull/1151>",
            "serde_json = 1.0.150",
            "MIT OR Apache-2.0",
            "fallback execution",
        ],
    },
    1152: {
        "dependency": "sha2",
        "expected_manifest": "shardloom-vortex/Cargo.toml",
        "expected_manifest_version": "0.11",
        "expected_lock_versions": {"sha2": "0.11.0", "digest": "0.11.3"},
        "review_doc": "docs/dependencies/json-digest-dependency-review.md",
        "review_markers": [
            "Dependabot PR <https://github.com/depsilon/shardloom/pull/1152>",
            "sha2 = 0.11.0",
            "MIT OR Apache-2.0",
            "fallback execution",
        ],
    },
    1153: {
        "dependency": "rusqlite",
        "expected_manifest": "shardloom-cli/Cargo.toml",
        "expected_manifest_version": "0.40.1",
        "expected_lock_versions": {
            "rusqlite": "0.40.1",
            "libsqlite3-sys": "0.38.1",
        },
        "review_doc": "docs/dependencies/sqlite-rusqlite-dependency-review.md",
        "review_markers": [
            "Dependabot PR <https://github.com/depsilon/shardloom/pull/1153>",
            "rusqlite = 0.40.1",
            "default-features = false",
            "features = [\"bundled\"]",
            "fallback execution",
        ],
    },
}

VORTEX_PROVIDER_SURFACE_EXPECTATIONS: tuple[dict[str, Any], ...] = (
    {
        "path": "benchmarks/traditional_analytics/run.py",
        "required_markers": [
            "UPSTREAM_VORTEX_PROVIDER_VERSION = _read_upstream_vortex_provider_version()",
            'SHARDLOOM_VORTEX_PROVIDER_VERSION = (',
        ],
        "forbidden_markers": [
            'UPSTREAM_VORTEX_PROVIDER_VERSION = "0.73"',
            'UPSTREAM_VORTEX_PROVIDER_VERSION = "0.74"',
            '"0.72" if admitted',
            "vortex=0.72",
            "provider_version, \"0.72\"",
        ],
    },
    {
        "path": "python/tests/test_cli_client.py",
        "required_markers": [
            "UPSTREAM_VORTEX_PROVIDER_VERSION = _current_upstream_vortex_provider_version()",
            '"value": UPSTREAM_VORTEX_PROVIDER_VERSION',
            "provider_version, UPSTREAM_VORTEX_PROVIDER_VERSION",
        ],
        "forbidden_markers": [
            '"provider_version", "value": "0.73"',
            'provider_version, "0.73"',
            '"evidence_slot_provider_version_refs", "value": "0.73"',
            '"provider_version", "value": "0.74"',
            'provider_version, "0.74"',
            '"evidence_slot_provider_version_refs", "value": "0.74"',
            '"provider_version", "value": "0.72"',
            'provider_version, "0.72"',
            '"evidence_slot_provider_version_refs", "value": "0.72"',
        ],
    },
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=ROOT)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument(
        "--open-prs-json",
        type=Path,
        help="Use a saved GitHub pulls API JSON response instead of live network access.",
    )
    parser.add_argument(
        "--require-live-github",
        action="store_true",
        help="Query GitHub and block if the live open Dependabot set cannot be checked.",
    )
    parser.add_argument("--github-url", default=GITHUB_PULLS_URL)
    parser.add_argument(
        "--github-token-env",
        action="append",
        default=None,
        help=(
            "Environment variable to read for GitHub API authentication. May be "
            "passed multiple times. Defaults to GITHUB_TOKEN then GH_TOKEN."
        ),
    )
    parser.add_argument("--timeout-seconds", type=float, default=15.0)
    return parser.parse_args()


def current_version_tokens(repo_root: Path) -> dict[str, str]:
    return {
        "$CURRENT_VORTEX_MANIFEST_VERSION": upstream_vortex_manifest_version(repo_root),
        "$CURRENT_VORTEX_LOCK_VERSION": upstream_vortex_lock_version(repo_root),
        "$CURRENT_VORTEX_PROVIDER_VERSION": upstream_vortex_provider_version(repo_root),
    }


def resolve_current_version_token(value: Any, tokens: dict[str, str]) -> Any:
    if not isinstance(value, str):
        return value
    resolved = value
    for token, replacement in tokens.items():
        resolved = resolved.replace(token, replacement)
    return resolved


def resolve(repo_root: Path, path: Path) -> Path:
    return path if path.is_absolute() else repo_root / path


def load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8") if path.exists() else ""


def load_toml(path: Path) -> dict[str, Any]:
    text = read_text(path)
    if not text:
        return {}
    if tomllib is None:
        return {}
    return tomllib.loads(text)


def _unquote_toml_string(value: str) -> str | None:
    match = re.fullmatch(r'"([^"\\]*(?:\\.[^"\\]*)*)"', value.strip())
    if not match:
        return None
    try:
        return json.loads(value.strip())
    except json.JSONDecodeError:
        return match.group(1)


def _split_top_level_commas(value: str) -> list[str]:
    parts: list[str] = []
    start = 0
    depth = 0
    in_string = False
    escape = False
    for index, char in enumerate(value):
        if in_string:
            if escape:
                escape = False
            elif char == "\\":
                escape = True
            elif char == '"':
                in_string = False
            continue
        if char == '"':
            in_string = True
        elif char in "[{":
            depth += 1
        elif char in "]}":
            depth -= 1
        elif char == "," and depth == 0:
            parts.append(value[start:index].strip())
            start = index + 1
    tail = value[start:].strip()
    if tail:
        parts.append(tail)
    return parts


def _parse_simple_toml_value(value: str) -> Any:
    stripped = value.strip()
    if stripped == "true":
        return True
    if stripped == "false":
        return False
    quoted = _unquote_toml_string(stripped)
    if quoted is not None:
        return quoted
    if stripped.startswith("[") and stripped.endswith("]"):
        inner = stripped[1:-1].strip()
        if not inner:
            return []
        return [_parse_simple_toml_value(part) for part in _split_top_level_commas(inner)]
    return stripped


def parse_inline_dependency_table(value: str) -> dict[str, Any] | None:
    stripped = value.strip()
    quoted = _unquote_toml_string(stripped)
    if quoted is not None:
        return {"version": quoted}
    if not (stripped.startswith("{") and stripped.endswith("}")):
        return None
    row: dict[str, Any] = {}
    for field in _split_top_level_commas(stripped[1:-1]):
        if "=" not in field:
            return None
        key, raw_value = field.split("=", 1)
        row[key.strip()] = _parse_simple_toml_value(raw_value)
    return row


def cargo_lock_versions_from_text(text: str) -> dict[str, str]:
    versions: dict[str, str] = {}
    name: str | None = None
    version: str | None = None

    def flush() -> None:
        if name is not None and version is not None:
            versions[name] = version

    for line in text.splitlines():
        stripped = line.strip()
        if stripped == "[[package]]":
            flush()
            name = None
            version = None
            continue
        if stripped.startswith("name = "):
            name = _unquote_toml_string(stripped.split("=", 1)[1].strip())
        elif stripped.startswith("version = "):
            version = _unquote_toml_string(stripped.split("=", 1)[1].strip())
    flush()
    return versions


def manifest_dependency_from_text(
    text: str,
    dependency: str,
    *,
    section_name: str = "dependencies",
) -> dict[str, Any] | None:
    in_dependencies = False
    dependency_key = f"{dependency} ="
    for line in text.splitlines():
        stripped = line.split("#", 1)[0].strip()
        if not stripped:
            continue
        if stripped.startswith("[") and stripped.endswith("]"):
            in_dependencies = stripped == f"[{section_name}]"
            continue
        if not in_dependencies or not stripped.startswith(dependency_key):
            continue
        _, raw_value = stripped.split("=", 1)
        return parse_inline_dependency_table(raw_value)
    return None


def cargo_lock_versions(repo_root: Path) -> dict[str, str]:
    path = repo_root / "Cargo.lock"
    data = load_toml(path)
    packages = data.get("package", [])
    versions: dict[str, str] = {}
    if isinstance(packages, list):
        for package in packages:
            if isinstance(package, dict):
                name = package.get("name")
                version = package.get("version")
                if isinstance(name, str) and isinstance(version, str):
                    versions[name] = version
    if not versions:
        versions = cargo_lock_versions_from_text(read_text(path))
    return versions


def workspace_manifest_dependency(repo_root: Path, dependency: str) -> dict[str, Any] | None:
    path = repo_root / "Cargo.toml"
    data = load_toml(path)
    workspace = data.get("workspace", {})
    if isinstance(workspace, dict):
        dependencies = workspace.get("dependencies", {})
        if isinstance(dependencies, dict):
            row = dependencies.get(dependency)
            if isinstance(row, dict):
                return row
            if isinstance(row, str):
                return {"version": row}
    return manifest_dependency_from_text(
        read_text(path),
        dependency,
        section_name="workspace.dependencies",
    )


def resolve_workspace_dependency(
    repo_root: Path,
    dependency: str,
    row: dict[str, Any],
) -> dict[str, Any]:
    if row.get("workspace") is not True:
        return row
    workspace_row = workspace_manifest_dependency(repo_root, dependency)
    if workspace_row is None:
        return row
    resolved = dict(workspace_row)
    resolved.update({key: value for key, value in row.items() if key != "workspace"})
    return resolved


def manifest_dependency(repo_root: Path, manifest: str, dependency: str) -> dict[str, Any] | None:
    path = repo_root / manifest
    data = load_toml(path)
    dependencies = data.get("dependencies", {})
    if not isinstance(dependencies, dict):
        row = manifest_dependency_from_text(read_text(path), dependency)
        return resolve_workspace_dependency(repo_root, dependency, row) if row is not None else None
    row = dependencies.get(dependency)
    if isinstance(row, dict):
        return resolve_workspace_dependency(repo_root, dependency, row)
    if isinstance(row, str):
        return {"version": row}
    row = manifest_dependency_from_text(read_text(path), dependency)
    return resolve_workspace_dependency(repo_root, dependency, row) if row is not None else None


def dependabot_prs(open_prs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for pr in open_prs:
        if not isinstance(pr, dict):
            continue
        user = pr.get("user")
        login = user.get("login") if isinstance(user, dict) else ""
        title = str(pr.get("title", ""))
        if login == "dependabot[bot]" or "dependabot" in title.lower():
            rows.append(pr)
    return rows


def github_token_from_env(
    env: Mapping[str, str], names: Sequence[str] | None = None
) -> str | None:
    for name in names or ("GITHUB_TOKEN", "GH_TOKEN"):
        token = env.get(name)
        if token:
            return token
    return None


def github_request_headers(github_token: str | None = None) -> dict[str, str]:
    headers = {
        "Accept": "application/vnd.github+json",
        "User-Agent": "shardloom-pre-5j-dependency-freshness-gate",
    }
    if github_token:
        headers["Authorization"] = f"Bearer {github_token}"
    return headers


def validate_live_github_pulls_url(url: str) -> str | None:
    try:
        parsed = urllib.parse.urlparse(url)
        port = parsed.port
    except ValueError as exc:
        return f"invalid live GitHub URL: {exc}"
    if parsed.scheme != "https":
        return "live GitHub dependency check URL must use https"
    if parsed.username or parsed.password:
        return "live GitHub dependency check URL must not include userinfo"
    if parsed.hostname != GITHUB_PULLS_HOST:
        return (
            "refusing live GitHub dependency check URL host "
            f"{parsed.hostname!r}; expected {GITHUB_PULLS_HOST}"
        )
    if port is not None:
        return "live GitHub dependency check URL must not specify a custom port"
    if parsed.path != GITHUB_PULLS_PATH:
        return (
            "refusing live GitHub dependency check URL path "
            f"{parsed.path!r}; expected {GITHUB_PULLS_PATH}"
        )
    if parsed.fragment:
        return "live GitHub dependency check URL must not include a fragment"
    return None


def fetch_open_prs(
    url: str,
    timeout_seconds: float,
    github_token: str | None = None,
) -> tuple[list[dict[str, Any]] | None, str, str | None]:
    url_error = validate_live_github_pulls_url(url)
    if url_error is not None:
        return None, "failed", url_error
    request = urllib.request.Request(
        url,
        headers=github_request_headers(github_token),
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
        return None, "failed", str(exc)
    if not isinstance(payload, list):
        return None, "failed", "GitHub pulls API did not return a list"
    return payload, "passed", None


def load_open_prs(
    *,
    repo_root: Path,
    open_prs_json: Path | None,
    require_live_github: bool,
    github_url: str,
    timeout_seconds: float,
    github_token_env: Sequence[str] | None = None,
) -> tuple[list[dict[str, Any]] | None, str, str | None]:
    if open_prs_json is not None:
        path = resolve(repo_root, open_prs_json)
        try:
            payload = load_json(path)
        except (FileNotFoundError, json.JSONDecodeError) as exc:
            return None, "failed", str(exc)
        if not isinstance(payload, list):
            return None, "failed", f"{path} must contain a GitHub pulls API list"
        return payload, "loaded_from_file", None
    if require_live_github:
        github_token = github_token_from_env(os.environ, github_token_env)
        return fetch_open_prs(github_url, timeout_seconds, github_token)
    return None, "skipped_not_requested", None


def validate_admitted_pr(repo_root: Path, pr_number: int, spec: dict[str, Any]) -> list[str]:
    blockers: list[str] = []
    kind = str(spec.get("kind", "cargo_dependency"))
    tokens = current_version_tokens(repo_root)
    if kind == "github_action":
        workflow = str(spec["expected_workflow"])
        workflow_text = read_text(repo_root / workflow)
        if not workflow_text:
            blockers.append(f"PR #{pr_number}: missing workflow {workflow}")
        for marker in spec.get("required_markers", []):
            if str(marker) not in workflow_text:
                blockers.append(f"PR #{pr_number}: {workflow} missing marker: {marker}")
        for marker in spec.get("forbidden_markers", []):
            if str(marker) in workflow_text:
                blockers.append(f"PR #{pr_number}: {workflow} contains stale marker: {marker}")
        for marker, expected_count in dict(spec.get("expected_marker_counts", {})).items():
            observed_count = workflow_text.count(str(marker))
            if observed_count != expected_count:
                blockers.append(
                    f"PR #{pr_number}: {workflow} marker {marker!r} count={observed_count}, "
                    f"expected {expected_count}"
                )
        review_doc = str(spec["review_doc"])
        review_text = read_text(repo_root / review_doc)
        if not review_text:
            blockers.append(f"PR #{pr_number}: missing dependency review doc {review_doc}")
        for marker in spec["review_markers"]:
            expected_marker = str(resolve_current_version_token(marker, tokens))
            if expected_marker not in review_text:
                blockers.append(
                    f"PR #{pr_number}: {review_doc} missing marker: {expected_marker}"
                )
        return blockers
    if kind != "cargo_dependency":
        return [f"PR #{pr_number}: unknown admitted dependency spec kind {kind!r}"]

    dependency = str(spec["dependency"])
    manifest = str(spec["expected_manifest"])
    row = manifest_dependency(repo_root, manifest, dependency)
    if row is None:
        blockers.append(f"PR #{pr_number}: {manifest} missing dependency {dependency}")
    else:
        expected_manifest_version = str(
            resolve_current_version_token(spec["expected_manifest_version"], tokens)
        )
        if str(row.get("version")) != expected_manifest_version:
            blockers.append(
                f"PR #{pr_number}: {dependency} manifest version={row.get('version')!r}"
            )
        if dependency == "vortex" and row.get("optional") is not True:
            blockers.append(f"PR #{pr_number}: vortex dependency must remain optional")
        if dependency == "rusqlite":
            if row.get("default-features") is not False:
                blockers.append(f"PR #{pr_number}: rusqlite default-features must be false")
            if row.get("features") != ["bundled"]:
                blockers.append(f"PR #{pr_number}: rusqlite features must be [\"bundled\"]")

    lock_versions = cargo_lock_versions(repo_root)
    for package, expected in dict(spec["expected_lock_versions"]).items():
        expected_version = str(resolve_current_version_token(expected, tokens))
        if lock_versions.get(package) != expected_version:
            blockers.append(
                f"PR #{pr_number}: Cargo.lock {package}={lock_versions.get(package)!r}"
            )

    review_doc = str(spec["review_doc"])
    review_text = read_text(repo_root / review_doc)
    if not review_text:
        blockers.append(f"PR #{pr_number}: missing dependency review doc {review_doc}")
    for marker in spec["review_markers"]:
        expected_marker = str(resolve_current_version_token(marker, tokens))
        if expected_marker not in review_text:
            blockers.append(f"PR #{pr_number}: {review_doc} missing marker: {expected_marker}")
    return blockers


def validate_vortex_provider_version_surfaces(repo_root: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    expected_provider_version = upstream_vortex_provider_version(repo_root)
    for spec in VORTEX_PROVIDER_SURFACE_EXPECTATIONS:
        path = Path(str(spec["path"]))
        text = read_text(repo_root / path)
        blockers: list[str] = []
        if not text:
            blockers.append(f"missing provider-version evidence surface {path.as_posix()}")
        for marker in spec["required_markers"]:
            if str(marker) not in text:
                blockers.append(f"{path.as_posix()} missing marker: {marker}")
        for marker in spec["forbidden_markers"]:
            if str(marker) in text:
                blockers.append(f"{path.as_posix()} contains stale marker: {marker}")
        rows.append(
            {
                "path": path.as_posix(),
                "status": "passed" if not blockers else "blocked",
                "expected_provider_version": expected_provider_version,
                "blockers": blockers,
            }
        )
    return rows


def build_report(
    *,
    repo_root: Path,
    open_prs: list[dict[str, Any]] | None,
    open_prs_status: str,
    open_prs_error: str | None = None,
    require_live_github: bool = False,
) -> dict[str, Any]:
    blockers: list[str] = []
    admitted_rows: list[dict[str, Any]] = []

    for pr_number, spec in ADMITTED_DEPENDABOT_PRS.items():
        pr_blockers = validate_admitted_pr(repo_root, pr_number, spec)
        blockers.extend(pr_blockers)
        admitted_rows.append(
            {
                "pr_number": pr_number,
                "dependency": spec["dependency"],
                "status": "admitted" if not pr_blockers else "blocked",
                "blockers": pr_blockers,
                "review_doc": spec["review_doc"],
            }
        )

    provider_surface_rows = validate_vortex_provider_version_surfaces(repo_root)
    for row in provider_surface_rows:
        blockers.extend(row["blockers"])

    dependency_scope = check_runtime_dependency_scope()
    if dependency_scope.get("fallback_dependency_absent") is not True:
        blockers.append("runtime dependency scope contains forbidden fallback dependencies")

    live_rows: list[dict[str, Any]] = []
    unknown_open_dependabot: list[dict[str, Any]] = []
    admitted_open_dependabot: list[int] = []
    if open_prs is not None:
        for pr in dependabot_prs(open_prs):
            number = int(pr.get("number", 0))
            row = {
                "number": number,
                "title": pr.get("title"),
                "user": (pr.get("user") or {}).get("login") if isinstance(pr.get("user"), dict) else None,
                "html_url": pr.get("html_url"),
            }
            live_rows.append(row)
            if number in ADMITTED_DEPENDABOT_PRS:
                admitted_open_dependabot.append(number)
            else:
                unknown_open_dependabot.append(row)
                blockers.append(
                    f"unincorporated open Dependabot PR before 5J: #{number} {row['title']}"
                )
    elif require_live_github:
        blockers.append("live GitHub Dependabot PR check is required before 5J")

    if open_prs_error:
        blockers.append(f"open Dependabot PR check failed: {open_prs_error}")

    live_check_sufficient = open_prs_status in {"passed", "loaded_from_file"} and not unknown_open_dependabot
    benchmark_refresh_allowed = not blockers and live_check_sufficient
    benchmark_status = (
        "passed"
        if benchmark_refresh_allowed
        else "blocked_live_github_check_required"
        if open_prs_status == "skipped_not_requested"
        else "blocked"
    )

    return {
        "schema_version": SCHEMA_VERSION,
        "status": "passed" if not blockers else "blocked",
        "gate_id": "gar-runtime-impl-5j.pre_5j_dependency_freshness",
        "require_live_github": require_live_github,
        "open_dependabot_check_status": open_prs_status,
        "open_dependabot_check_error": open_prs_error,
        "open_dependabot_prs": live_rows,
        "open_dependabot_pr_count": len(live_rows),
        "admitted_open_dependabot_prs": sorted(admitted_open_dependabot),
        "unknown_open_dependabot_prs": unknown_open_dependabot,
        "admitted_dependabot_prs": admitted_rows,
        "vortex_provider_version_surfaces": provider_surface_rows,
        "runtime_dependency_scope": dependency_scope,
        "benchmark_refresh_dependency_gate_status": benchmark_status,
        "benchmark_refresh_allowed": benchmark_refresh_allowed,
        "benchmark_run_performed": False,
        "publication_attempted": False,
        "tag_created": False,
        "secrets_required": False,
        "fallback_attempted": False,
        "external_engine_invoked": False,
        "claim_boundary": (
            "Dependency freshness and no-fallback preflight only. This does not run benchmarks, "
            "publish artifacts, approve packages, or create performance claims."
        ),
        "blockers": blockers,
    }


def main() -> int:
    args = parse_args()
    repo_root = args.repo_root.resolve()
    open_prs, open_prs_status, open_prs_error = load_open_prs(
        repo_root=repo_root,
        open_prs_json=args.open_prs_json,
        require_live_github=args.require_live_github,
        github_url=args.github_url,
        timeout_seconds=args.timeout_seconds,
        github_token_env=args.github_token_env,
    )
    report = build_report(
        repo_root=repo_root,
        open_prs=open_prs,
        open_prs_status=open_prs_status,
        open_prs_error=open_prs_error,
        require_live_github=args.require_live_github,
    )
    output = resolve(repo_root, args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    if report["blockers"]:
        for blocker in report["blockers"]:
            print(f"pre-5J dependency freshness blocker: {blocker}")
        return 1
    print(f"pre-5J dependency freshness gate passed: {output}")
    if not report["benchmark_refresh_allowed"]:
        print(
            "pre-5J dependency freshness note: run with --require-live-github immediately "
            "before any 5J benchmark refresh"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
