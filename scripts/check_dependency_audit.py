#!/usr/bin/env python
# SPDX-License-Identifier: Apache-2.0
"""Run ShardLoom dependency audit tools and emit a DependencyAuditReport.

This script is release/check tooling only. It does not add runtime dependencies,
publish packages, or authorize fallback engines.
"""

from __future__ import annotations

import argparse
import os
import json
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - exercised only on Python 3.10.
    tomllib = None  # type: ignore[assignment]


ROOT = Path(__file__).resolve().parents[1]
REPORT_SCHEMA_VERSION = "shardloom.dependency_audit_report.v1"
DEFAULT_REPORT_PATH = ROOT / "target" / "dependency-audit-report.json"
PYTHON_PROJECT = ROOT / "python"
PYTHON_RUNTIME_REQUIREMENTS = ROOT / "target" / "dependency-audit" / "python-runtime-requirements.txt"
PIP_AUDIT_MODULE = "pip_audit"
PIP_AUDIT_ENV_VARS = ("SHARDLOOM_PIP_AUDIT_PYTHON", "PIP_AUDIT_PYTHON")
CODEX_BUNDLED_PYTHON = (
    Path.home()
    / ".cache"
    / "codex-runtimes"
    / "codex-primary-runtime"
    / "dependencies"
    / "python"
    / "bin"
    / "python3"
)
# Required release command: cargo deny check licenses advisories bans sources.
FORBIDDEN_FALLBACK_DEPENDENCIES = {
    "bigquery",
    "dask",
    "databricks",
    "datafusion",
    "duckdb",
    "pandas",
    "polars",
    "pyspark",
    "ray",
    "snowflake",
    "spark",
    "trino",
    "velox",
}
RUNTIME_CARGO_MANIFESTS = (
    "Cargo.toml",
    "shardloom-core/Cargo.toml",
    "shardloom-plan/Cargo.toml",
    "shardloom-exec/Cargo.toml",
    "shardloom-vortex/Cargo.toml",
    "shardloom-cli/Cargo.toml",
)
BENCHMARK_REQUIREMENTS_FILES = (
    ROOT / "benchmarks" / "traditional_analytics" / "requirements.txt",
    ROOT / "benchmarks" / "traditional_analytics" / "requirements-extended-local.txt",
    ROOT / "benchmarks" / "traditional_analytics" / "requirements-spark.txt",
    ROOT / "benchmarks" / "traditional_analytics" / "requirements-gpu-optional.txt",
)


@dataclass
class ToolResult:
    label: str
    command: list[str]
    status: str
    returncode: int | None = None
    install_hint: str | None = None
    diagnostics: str | None = None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--strict-missing",
        action="store_true",
        help="Fail when a requested audit tool is not installed.",
    )
    parser.add_argument(
        "--release-gate",
        action="store_true",
        help=(
            "Run the hard release gate: strict missing tools, cargo-deny, cargo-audit, "
            "packaging/dev pip-audit, and no-fallback dependency checks."
        ),
    )
    parser.add_argument(
        "--include-cargo-audit",
        action="store_true",
        help="Run cargo audit when cargo-audit is installed.",
    )
    parser.add_argument(
        "--include-python-packaging",
        action="store_true",
        help="Run pip-audit for the current packaging/dev Python environment.",
    )
    parser.add_argument(
        "--json-output",
        type=Path,
        default=DEFAULT_REPORT_PATH,
        help="Write a machine-readable DependencyAuditReport JSON file.",
    )
    parser.add_argument(
        "--no-json",
        action="store_true",
        help="Do not write the JSON report.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    strict_missing = args.strict_missing or args.release_gate
    include_cargo_audit = args.include_cargo_audit or args.release_gate
    include_python_packaging = args.include_python_packaging or args.release_gate
    tool_results: list[ToolResult] = []

    tool_results.append(
        run_external_tool(
            label="cargo-deny",
            executable="cargo-deny",
            command=["cargo", "deny", "check", "licenses", "advisories", "bans", "sources"],
            install_hint="cargo install cargo-deny --locked",
            strict_missing=strict_missing,
        )
    )

    if include_cargo_audit:
        tool_results.append(
            run_external_tool(
                label="cargo-audit",
                executable="cargo-audit",
                command=["cargo", "audit"],
                install_hint="cargo install cargo-audit --locked",
                strict_missing=strict_missing,
            )
        )
    else:
        print("SKIP cargo-audit: optional until --release-gate or --include-cargo-audit")
        tool_results.append(
            ToolResult(
                label="cargo-audit",
                command=["cargo", "audit"],
                status="skipped_not_requested",
                diagnostics="release gate requires cargo-audit or explicit maintainer waiver",
            )
        )

    if include_python_packaging:
        tool_results.append(run_pip_audit(strict_missing=strict_missing))
    else:
        print(
            "SKIP pip-audit: use --include-python-packaging only in packaging/dev "
            "environments, not as a ShardLoom runtime dependency assumption"
        )
        tool_results.append(
            ToolResult(
                label="pip-audit",
                command=[
                    sys.executable,
                    "-m",
                    "pip_audit",
                    "--requirement",
                    str(PYTHON_RUNTIME_REQUIREMENTS),
                ],
                status="skipped_not_requested",
                diagnostics=(
                    "pip-audit is packaging/dev evidence only; the Python runtime package has "
                    "no dependencies"
                ),
            )
        )

    runtime_dependency_report = check_runtime_dependency_scope()
    benchmark_dependency_report = check_benchmark_dependency_scope()
    report = build_report(
        tool_results=tool_results,
        runtime_dependency_report=runtime_dependency_report,
        benchmark_dependency_report=benchmark_dependency_report,
        release_gate=args.release_gate,
    )

    if not args.no_json:
        output = args.json_output
        if not output.is_absolute():
            output = ROOT / output
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        print(f"WROTE {output}")

    failed = any(result.status in {"failed", "missing"} for result in tool_results)
    failed = failed or not runtime_dependency_report["fallback_dependency_absent"]
    if args.release_gate:
        failed = failed or any(
            result.status in {"skipped_not_requested", "skipped_missing"}
            for result in tool_results
        )
        failed = failed or report["cargo_audit_status"] != "passed"
        failed = failed or report["pip_audit_status"] != "passed"
    return 1 if failed else 0


def run_external_tool(
    *,
    label: str,
    executable: str,
    command: list[str],
    install_hint: str,
    strict_missing: bool,
) -> ToolResult:
    if shutil.which(executable) is None:
        status = "missing" if strict_missing else "skipped_missing"
        print(f"{status.upper()} {label}: install with `{install_hint}`")
        return ToolResult(
            label=label,
            command=command,
            status=status,
            install_hint=install_hint,
            diagnostics="tool not installed",
        )

    print(f"RUN {' '.join(command)}")
    completed = subprocess.run(command, cwd=ROOT, check=False)
    return ToolResult(
        label=label,
        command=command,
        status="passed" if completed.returncode == 0 else "failed",
        returncode=completed.returncode,
    )


def run_pip_audit(*, strict_missing: bool) -> ToolResult:
    runtime_requirements = read_python_runtime_requirement_specs(
        PYTHON_PROJECT / "pyproject.toml"
    )
    write_python_runtime_requirements(PYTHON_RUNTIME_REQUIREMENTS, runtime_requirements)
    pip_audit_prefix = resolve_pip_audit_command()
    command = [
        *(pip_audit_prefix or [sys.executable, "-m", PIP_AUDIT_MODULE]),
        "--requirement",
        str(PYTHON_RUNTIME_REQUIREMENTS),
        "--progress-spinner",
        "off",
    ]
    diagnostics = None
    if not runtime_requirements:
        command.extend(["--disable-pip", "--no-deps"])
        diagnostics = (
            "Python runtime declares no dependencies; pip-audit checked a generated empty "
            "runtime requirements file without invoking pip dependency resolution."
        )
    if pip_audit_prefix is None:
        status = "missing" if strict_missing else "skipped_missing"
        print(
            f"{status.upper()} pip-audit: install in a packaging/dev env with "
            "`python -m pip install pip-audit`, put `pip-audit` on PATH, or set "
            "`SHARDLOOM_PIP_AUDIT_PYTHON` to a Python executable that has pip-audit"
        )
        return ToolResult(
            label="pip-audit",
            command=command,
            status=status,
            install_hint="python -m pip install pip-audit",
            diagnostics="tool not installed in current Python environment, PATH, or configured packaging Python",
        )

    print(f"RUN {' '.join(command)}")
    completed = subprocess.run(command, cwd=ROOT, check=False)
    return ToolResult(
        label="pip-audit",
        command=command,
        status="passed" if completed.returncode == 0 else "failed",
        returncode=completed.returncode,
        diagnostics=diagnostics,
    )


def resolve_pip_audit_command(
    *,
    module_available: Any | None = None,
    executable_lookup: Any | None = None,
    home: Path | None = None,
) -> list[str] | None:
    """Return a command prefix for packaging/dev pip-audit evidence.

    Release scripts may be launched with system Python while packaging tools live
    in a managed dev runtime. Treat `pip-audit` as an external audit tool: prefer
    explicit configured Python, then the invoking interpreter, then a PATH
    executable, then common local/Codex packaging Python environments.
    """

    module_available = module_available or pip_audit_module_available
    executable_lookup = executable_lookup or shutil.which
    home = home or Path.home()

    for env_var in PIP_AUDIT_ENV_VARS:
        configured = os.environ.get(env_var)
        if configured:
            prefix = pip_audit_python_prefix(configured, module_available=module_available)
            if prefix is not None:
                return prefix

    current = pip_audit_python_prefix(sys.executable, module_available=module_available)
    if current is not None:
        return current

    pip_audit_executable = executable_lookup("pip-audit")
    if pip_audit_executable:
        return [pip_audit_executable]

    for candidate in pip_audit_python_candidates(home):
        prefix = pip_audit_python_prefix(candidate, module_available=module_available)
        if prefix is not None:
            return prefix
    return None


def pip_audit_python_candidates(home: Path) -> list[Path]:
    return [
        ROOT / ".venv" / "bin" / "python",
        PYTHON_PROJECT / ".venv" / "bin" / "python",
        ROOT
        / "target"
        / "release-readiness-audit"
        / "pip-audit-venv"
        / "bin"
        / "python",
        ROOT / "target" / "release-dry-run-proof" / "venv" / "bin" / "python",
        home
        / ".cache"
        / "codex-runtimes"
        / "codex-primary-runtime"
        / "dependencies"
        / "python"
        / "bin"
        / "python3",
        CODEX_BUNDLED_PYTHON,
    ]


def pip_audit_python_prefix(
    python_executable: str | Path,
    *,
    module_available: Any | None = None,
) -> list[str] | None:
    python_path = str(python_executable)
    module_available = module_available or pip_audit_module_available
    return [python_path, "-m", PIP_AUDIT_MODULE] if module_available(python_path) else None


def pip_audit_module_available(python_executable: str) -> bool:
    python_path = Path(python_executable)
    if not python_path.exists():
        resolved_executable = shutil.which(python_executable)
        if resolved_executable is None:
            return False
        python_executable = resolved_executable

    # Always execute the requested interpreter. macOS venvs commonly symlink
    # bin/python to the base interpreter, but invoking the venv path is what
    # activates its pyvenv.cfg and site-packages.
    if not Path(python_executable).exists():
        return False
    completed = subprocess.run(
        [
            python_executable,
            "-c",
            (
                "import importlib.util, sys; "
                f"sys.exit(0 if importlib.util.find_spec({PIP_AUDIT_MODULE!r}) else 1)"
            ),
        ],
        cwd=ROOT,
        check=False,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    return completed.returncode == 0


def check_runtime_dependency_scope() -> dict[str, Any]:
    diagnostics: list[str] = []
    cargo_dependency_names: set[str] = set()
    for manifest in RUNTIME_CARGO_MANIFESTS:
        path = ROOT / manifest
        if path.exists():
            cargo_dependency_names.update(read_cargo_dependency_names(path))
    python_dependencies = read_python_runtime_dependencies(ROOT / "python" / "pyproject.toml")
    forbidden_runtime = sorted(
        dependency
        for dependency in cargo_dependency_names.union(python_dependencies)
        if canonical_dependency_name(dependency) in FORBIDDEN_FALLBACK_DEPENDENCIES
    )
    if forbidden_runtime:
        diagnostics.append(
            "forbidden runtime fallback dependencies present: " + ", ".join(forbidden_runtime)
        )
    else:
        diagnostics.append("runtime package metadata contains no forbidden fallback-engine dependencies")
    return {
        "cargo_manifest_count": len(RUNTIME_CARGO_MANIFESTS),
        "python_runtime_dependency_count": len(python_dependencies),
        "forbidden_runtime_dependencies": forbidden_runtime,
        "fallback_dependency_absent": not forbidden_runtime,
        "diagnostics": diagnostics,
    }


def check_benchmark_dependency_scope() -> dict[str, Any]:
    profiles: list[dict[str, Any]] = []
    all_dependencies: set[str] = set()
    all_external_baselines: set[str] = set()
    for requirements_file in BENCHMARK_REQUIREMENTS_FILES:
        benchmark_dependencies = read_requirements(requirements_file)
        external_baselines = sorted(
            dependency
            for dependency in benchmark_dependencies
            if canonical_dependency_name(dependency) in FORBIDDEN_FALLBACK_DEPENDENCIES
        )
        all_dependencies.update(benchmark_dependencies)
        all_external_baselines.update(external_baselines)
        profiles.append(
            {
                "requirements_file": str(requirements_file.relative_to(ROOT)),
                "benchmark_dependency_count": len(benchmark_dependencies),
                "external_baseline_dependencies": external_baselines,
                "scope": "benchmark_only_external_baselines",
            }
        )
    return {
        "profile_count": len(profiles),
        "profiles": profiles,
        "benchmark_dependency_count": len(all_dependencies),
        "external_baseline_dependencies": sorted(all_external_baselines),
        "scope": "benchmark_only_external_baselines",
        "diagnostics": [
            "benchmark external engines are comparison baselines only, never runtime fallback",
            "benchmark profile requirements are audited separately from release runtime package metadata",
        ],
    }


def read_cargo_dependency_names(path: Path) -> set[str]:
    if tomllib is None:
        return read_cargo_dependency_names_text(path)
    data = tomllib.loads(path.read_text(encoding="utf-8"))
    names: set[str] = set()
    sections = ("dependencies", "dev-dependencies", "build-dependencies")
    for section in sections:
        table = data.get(section, {})
        if isinstance(table, dict):
            names.update(str(key) for key in table)
    target_tables = data.get("target", {})
    if isinstance(target_tables, dict):
        for target_table in target_tables.values():
            if not isinstance(target_table, dict):
                continue
            for section in sections:
                table = target_table.get(section, {})
                if isinstance(table, dict):
                    names.update(str(key) for key in table)
    return names


def read_cargo_dependency_names_text(path: Path) -> set[str]:
    names: set[str] = set()
    active = False
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.split("#", 1)[0].strip()
        if not line:
            continue
        if line.startswith("["):
            section = line.strip("[]")
            active = section in {
                "dependencies",
                "dev-dependencies",
                "build-dependencies",
            } or section.endswith(
                (
                    ".dependencies",
                    ".dev-dependencies",
                    ".build-dependencies",
                )
            )
            continue
        if active and "=" in line:
            names.add(line.split("=", 1)[0].strip().strip('"'))
    return names


def read_python_runtime_dependencies(path: Path) -> set[str]:
    return {
        dependency_name_from_requirement(spec)
        for spec in read_python_runtime_requirement_specs(path)
    }


def read_python_runtime_requirement_specs(path: Path) -> set[str]:
    if tomllib is None:
        return read_python_runtime_requirement_specs_text(path)
    data = tomllib.loads(path.read_text(encoding="utf-8"))
    project = data.get("project", {})
    dependencies = project.get("dependencies", [])
    if not isinstance(dependencies, list):
        return set()
    return {str(dependency).strip() for dependency in dependencies if str(dependency).strip()}


def read_python_runtime_dependencies_text(path: Path) -> set[str]:
    return {
        dependency_name_from_requirement(spec)
        for spec in read_python_runtime_requirement_specs_text(path)
    }


def read_python_runtime_requirement_specs_text(path: Path) -> set[str]:
    dependencies: set[str] = set()
    collecting = False
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.split("#", 1)[0].strip()
        if line == "dependencies = []":
            return set()
        if line.startswith("dependencies = ["):
            collecting = True
            remainder = line.split("[", 1)[1]
            if "]" in remainder:
                collecting = False
                remainder = remainder.split("]", 1)[0]
            dependencies.update(extract_quoted_dependency_specs(remainder))
            continue
        if collecting:
            if "]" in line:
                collecting = False
                line = line.split("]", 1)[0]
            dependencies.update(extract_quoted_dependency_specs(line))
    return dependencies


def extract_quoted_dependencies(text: str) -> set[str]:
    return {
        dependency_name_from_requirement(spec)
        for spec in extract_quoted_dependency_specs(text)
    }


def extract_quoted_dependency_specs(text: str) -> set[str]:
    dependencies: set[str] = set()
    for part in text.split(","):
        token = part.strip().strip('"').strip("'")
        if token:
            dependencies.add(token)
    return dependencies


def write_python_runtime_requirements(path: Path, requirements: set[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    body = "".join(f"{requirement}\n" for requirement in sorted(requirements))
    path.write_text(body, encoding="utf-8")


def read_requirements(path: Path) -> set[str]:
    if not path.exists():
        return set()
    dependencies: set[str] = set()
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.split("#", 1)[0].strip()
        if not line or line.startswith(("-", "http:", "https:", "git+")):
            continue
        dependencies.add(dependency_name_from_requirement(line))
    return dependencies


def dependency_name_from_requirement(requirement: str) -> str:
    token = requirement.split(";", 1)[0].strip()
    if " @ " in token:
        token = token.split(" @ ", 1)[0].strip()
    token = token.split("[", 1)[0]
    for separator in ("==", ">=", "<=", "~=", "!=", ">", "<"):
        token = token.split(separator, 1)[0]
    return token.strip()


def canonical_dependency_name(name: str) -> str:
    return name.lower().replace("_", "-")


def build_report(
    *,
    tool_results: list[ToolResult],
    runtime_dependency_report: dict[str, Any],
    benchmark_dependency_report: dict[str, Any],
    release_gate: bool,
) -> dict[str, Any]:
    by_label = {result.label: result for result in tool_results}
    cargo_deny = by_label["cargo-deny"].status
    cargo_audit = by_label["cargo-audit"].status
    pip_audit = by_label["pip-audit"].status
    diagnostics = [diagnostic for result in tool_results if (diagnostic := result.diagnostics)]
    diagnostics.extend(runtime_dependency_report["diagnostics"])
    diagnostics.extend(benchmark_dependency_report["diagnostics"])
    return {
        "schema_version": REPORT_SCHEMA_VERSION,
        "release_gate": release_gate,
        "cargo_deny_status": cargo_deny,
        "cargo_audit_status": cargo_audit,
        "pip_audit_status": pip_audit,
        "license_policy_status": "passed" if cargo_deny == "passed" else cargo_deny,
        "advisory_status": derive_advisory_status(cargo_deny, cargo_audit, pip_audit, release_gate),
        "yanked_dependency_status": "covered_by_cargo_deny",
        "unknown_source_status": "covered_by_cargo_deny",
        "runtime_dependency_scope": runtime_dependency_report,
        "benchmark_dependency_scope": benchmark_dependency_report,
        "fallback_dependency_absent": runtime_dependency_report["fallback_dependency_absent"],
        "tool_results": [
            {
                "label": result.label,
                "command": result.command,
                "status": result.status,
                "returncode": result.returncode,
                "install_hint": result.install_hint,
                "diagnostics": result.diagnostics,
            }
            for result in tool_results
        ],
        "diagnostics": diagnostics,
    }


def derive_advisory_status(
    cargo_deny_status: str, cargo_audit_status: str, pip_audit_status: str, release_gate: bool
) -> str:
    statuses = [cargo_deny_status, cargo_audit_status]
    if release_gate:
        statuses.append(pip_audit_status)
    if any(status in {"failed", "missing"} for status in statuses):
        return "failed"
    if any(status.startswith("skipped") for status in statuses):
        return "skipped_until_release_gate"
    return "passed"


if __name__ == "__main__":
    raise SystemExit(main())
