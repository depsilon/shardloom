#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Run ShardLoom `scripts/check_*.py` validators in-process with shared document caching.

Preserves every existing `scripts/check_*.py` script as an untouched standalone CLI entrypoint
while eliminating redundant process startups and repeated reads of shared plan/matrix/Cargo
documents during local and CI validation.
"""

from __future__ import annotations

import argparse
import contextlib
import importlib.util
import io
import json
import sys
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

SCRIPTS_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPTS_DIR.parents[0]
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from release_report_utils import cached_document_reads, fail_closed_fields, write_json


SCHEMA_VERSION = "shardloom.unified_check_runner_report.v1"


@dataclass(frozen=True)
class CheckSpec:
    """Metadata describing one discovered `scripts/check_*.py` module."""

    name: str
    suite: str  # "docs" | "gated" | "library_helper"
    reason: str
    uses_repo_root: bool = True
    output_flag: str | None = "--output"  # "--output" | "--json-output" | None
    no_json_flag: bool = False
    has_main: bool = True
    default_extra_args: tuple[str, ...] = ()


# Clean, offline, side-effect-free pure-Python checks verified to pass on a bare source checkout
# without pre-built binaries, pre-generated `target/*.json` sibling reports, or network access.
DOCS_SUITE_NAMES: tuple[str, ...] = (
    "check_benchmark_constitution",
    "check_benchmark_optimization_targets",
    "check_benchmark_publication_claim_gate",
    "check_benchmark_publish_doctor",
    "check_ci_gate_matrix",
    "check_clickbench_olap_runtime_coverage",
    "check_contribution_governance",
    "check_enterprise_evidence_export_pack",
    "check_foundry_package_proof_boundary",
    "check_front_door_benchmark_publication",
    "check_local_format_production_profiles",
    "check_pre_5j_dependency_freshness",
    "check_public_claim_language",
    "check_public_status_docs",
    "check_python_license_bundle",
    "check_runtime_execution_envelopes",
    "check_runtime_gap_family_burn_down",
    "check_security_posture",
    "check_sql_python_dataframe_parity",
    "check_universal_ingress_routes",
    "check_use_case_backlinks",
    "check_use_case_index",
    "check_user_route_capability_report",
    "check_user_surface_graduation_matrix",
    "check_user_surface_reference",
    "check_user_surface_runtime_gap_inventory",
    "check_v1_api_schema_stability",
    "check_v1_docs_productization",
    "check_v1_front_door_runtime_scope",
    "check_v1_inclusion_scope",
    "check_v1_local_output_sink_scope",
    "check_v1_local_source_package_release",
    "check_v1_source_prepared_state_scope",
    "check_v1_vortex_runtime_scope",
    "check_website_readiness",
    "check_workflow_recipes",
    "check_workspace_version_sources",
)

GATED_CHECK_REASONS: dict[str, str] = {
    "check_admitted_semantics_matrix": "requires compiled shardloom CLI binary / feature matrix",
    "check_benchmark_artifact_completeness": "requires explicit --manifest benchmark packet path",
    "check_compatibility_output_translation_reports": "requires generated compatibility translation report in target/",
    "check_compute_engine_completion_gate": "blocks by design while phase plan contains open items",
    "check_dependency_audit": "invokes external cargo-deny / cargo-audit / pip-audit tooling",
    "check_evidence_schema_registry": "writes hardcoded target/evidence-schema-registry-report.json without --output",
    "check_final_release_approval": "aggregates pre-generated release rehearsal and approval artifacts",
    "check_finished_product_readiness": "aggregates pre-generated CI/release reports from target/",
    "check_foundry_dev_stack_starter": "invokes shardloom CLI binary / writes starter artifacts",
    "check_golden_workflows": "requires compiled shardloom CLI binary to execute golden workflows",
    "check_local_format_edge_case_fixtures": "requires generated edge-case fixture report in target/",
    "check_local_format_pushdown_fidelity": "requires generated pushdown fidelity report in target/",
    "check_native_python_wheel": "requires built --wheel, --cli-binary, and --native-library artifacts",
    "check_package_channel_readiness": "requires pre-generated dependency-audit and provenance reports",
    "check_production_certification_gate": "aggregates pre-generated v1 conformance/resource reports",
    "check_production_usability_gate": "aggregates pre-generated usability and dry-run reports",
    "check_python_user_surface_completion": "requires pre-generated release dry-run transcript",
    "check_release_architecture_tracker": "blocks without --allow-blocked until release items close",
    "check_release_readiness": "aggregates all pre-generated release readiness reports in target/",
    "check_release_security_gate": "aggregates pre-generated security posture and provenance reports",
    "check_repo_wide_audit": "validates/refreshes external repo-wide audit inventory outputs",
    "check_use_case_coverage": "tracks known open Use Case Atlas execution_mode coverage gap",
    "check_use_case_glossary": "tracks known open Use Case Field Guide glossary gap",
    "check_v1_correctness_conformance": "aggregates pre-generated correctness and replay reports",
    "check_v1_example_replay": "requires compiled shardloom CLI binary to replay examples",
    "check_v1_local_resource_safety": "requires compiled shardloom CLI binary and local output reports",
    "check_v1_observability_support": "requires compiled shardloom CLI binary for doctor/support-bundle checks",
    "check_v1_release_boundary": "requires pre-generated package-channel and dry-run reports",
    "check_v1_security_ci_hardening": "aggregates pre-generated CI/security/provenance reports",
}

LIBRARY_HELPER_REASONS: dict[str, str] = {
    "check_published_channel_proofs": "library helper module imported by check_package_channel_readiness",
    "check_registry_bundled_proof": "library helper module imported by check_package_channel_readiness",
}


def _detect_spec(script_path: Path) -> CheckSpec:
    name = script_path.stem
    src = script_path.read_text(encoding="utf-8")
    uses_repo_root = "--repo-root" in src
    if "--no-json" in src and "--json-output" in src:
        output_flag: str | None = "--json-output"
        no_json_flag = True
    elif "--no-json" in src and "--output" in src:
        output_flag = "--output"
        no_json_flag = True
    elif "--output" in src:
        output_flag = "--output"
        no_json_flag = False
    elif "--json-output" in src:
        output_flag = "--json-output"
        no_json_flag = False
    else:
        output_flag = None
        no_json_flag = False

    has_main = "def main(" in src

    if name in LIBRARY_HELPER_REASONS:
        return CheckSpec(
            name=name,
            suite="library_helper",
            reason=LIBRARY_HELPER_REASONS[name],
            uses_repo_root=uses_repo_root,
            output_flag=output_flag,
            no_json_flag=no_json_flag,
            has_main=has_main,
        )
    if name in DOCS_SUITE_NAMES:
        return CheckSpec(
            name=name,
            suite="docs",
            reason="pure-Python clean-checkout validator",
            uses_repo_root=uses_repo_root,
            output_flag=output_flag,
            no_json_flag=no_json_flag,
            has_main=has_main,
        )
    if name in GATED_CHECK_REASONS:
        return CheckSpec(
            name=name,
            suite="gated",
            reason=GATED_CHECK_REASONS[name],
            uses_repo_root=uses_repo_root,
            output_flag=output_flag,
            no_json_flag=no_json_flag,
            has_main=has_main,
        )
    return CheckSpec(
        name=name,
        suite="gated",
        reason="unclassified check script; classify explicitly before admitting to docs suite",
        uses_repo_root=uses_repo_root,
        output_flag=output_flag,
        no_json_flag=no_json_flag,
        has_main=has_main,
    )


def discover_checks(scripts_dir: Path | None = None) -> dict[str, CheckSpec]:
    """Discover all `scripts/check_*.py` files in deterministic sorted order."""

    target_dir = scripts_dir or SCRIPTS_DIR
    specs: dict[str, CheckSpec] = {}
    for path in sorted(target_dir.glob("check_*.py")):
        specs[path.stem] = _detect_spec(path)
    return specs


def normalize_check_name(raw: str) -> str:
    name = raw.strip()
    if name.endswith(".py"):
        name = name[:-3]
    if "/" in name or "\\" in name:
        name = Path(name).name
        if name.endswith(".py"):
            name = name[:-3]
    if not name.startswith("check_"):
        name = f"check_{name}"
    return name


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=REPO_ROOT)
    parser.add_argument(
        "--suite",
        choices=("docs", "all"),
        default="docs",
        help="Named check suite to execute (default: docs).",
    )
    parser.add_argument(
        "--checks",
        nargs="+",
        default=None,
        help="Explicit check names (e.g. check_public_status_docs or public_status_docs).",
    )
    parser.add_argument(
        "--include-gated",
        action="store_true",
        help="Include gated checks when running --suite all (fails unless required artifacts exist).",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="List all discovered check_*.py scripts and their classifications as JSON.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("target/unified-check-runner-report.json"),
    )
    parser.add_argument(
        "--no-json",
        action="store_true",
        help="Print the summary JSON to stdout without writing --output to disk.",
    )
    parser.add_argument(
        "--keep-reports-dir",
        type=Path,
        default=None,
        help="Optional directory to retain individual per-check JSON outputs.",
    )
    parser.add_argument(
        "--fail-fast",
        action="store_true",
        help="Stop executing remaining checks after the first non-zero exit code.",
    )
    return parser.parse_args(argv)


def _tail_lines(text: str, limit: int = 8) -> list[str]:
    lines = [line.rstrip() for line in text.splitlines() if line.strip()]
    return lines[-limit:] if len(lines) > limit else lines


def _invoke_check_in_process(
    *,
    spec: CheckSpec,
    script_path: Path,
    repo_root: Path,
    reports_dir: Path | None,
) -> dict[str, Any]:
    if not spec.has_main:
        return {
            "name": spec.name,
            "script": f"scripts/{spec.name}.py",
            "suite": spec.suite,
            "status": "failed",
            "exit_code": 1,
            "duration_ms": 0,
            "stdout_tail": [],
            "stderr_tail": [f"{spec.name}.py is a library helper without def main()"],
        }

    argv = [str(script_path)]
    if spec.uses_repo_root:
        argv.extend(["--repo-root", str(repo_root)])
    if reports_dir is not None and spec.output_flag is not None:
        argv.extend([spec.output_flag, str(reports_dir / f"{spec.name}.json")])
    elif spec.no_json_flag:
        argv.append("--no-json")
    elif spec.output_flag is not None and reports_dir is None:
        # Temp report directory is always provided by caller unless user opted out; defensive fallback.
        pass
    argv.extend(spec.default_extra_args)

    scripts_parent = str(script_path.parent)
    if scripts_parent not in sys.path:
        sys.path.insert(0, scripts_parent)

    stdout_buf = io.StringIO()
    stderr_buf = io.StringIO()
    prev_argv = sys.argv
    started = time.monotonic()
    exit_code = 0
    try:
        module_spec = importlib.util.spec_from_file_location(
            f"_shardloom_check_{spec.name}", script_path
        )
        if module_spec is None or module_spec.loader is None:
            raise ImportError(f"cannot load spec for {script_path}")
        module = importlib.util.module_from_spec(module_spec)
        # Do not leave stale check module entries in sys.modules, while registering during exec
        # so dataclasses / annotations resolve cleanly under `from __future__ import annotations`.
        sys.modules[module_spec.name] = module
        try:
            module_spec.loader.exec_module(module)
            sys.argv = argv
            with contextlib.redirect_stdout(stdout_buf), contextlib.redirect_stderr(stderr_buf):
                try:
                    raw_rc = module.main()
                    exit_code = 0 if raw_rc is None else int(raw_rc)
                except SystemExit as exc:
                    if exc.code is None:
                        exit_code = 0
                    elif isinstance(exc.code, int):
                        exit_code = exc.code
                    else:
                        print(exc.code, file=sys.stderr)
                        exit_code = 1
        finally:
            sys.modules.pop(module_spec.name, None)
    except Exception as exc:
        print(f"in-process runner exception in {spec.name}: {exc}", file=stderr_buf)
        exit_code = 1
    finally:
        sys.argv = prev_argv

    duration_ms = int(round((time.monotonic() - started) * 1000.0))
    return {
        "name": spec.name,
        "script": f"scripts/{spec.name}.py",
        "suite": spec.suite,
        "status": "passed" if exit_code == 0 else "failed",
        "exit_code": exit_code,
        "duration_ms": duration_ms,
        "stdout_tail": _tail_lines(stdout_buf.getvalue()),
        "stderr_tail": _tail_lines(stderr_buf.getvalue()),
    }


def build_report(
    *,
    repo_root: Path,
    suite: str = "docs",
    checks: list[str] | None = None,
    include_gated: bool = False,
    keep_reports_dir: Path | None = None,
    fail_fast: bool = False,
) -> dict[str, Any]:
    repo_root = repo_root.resolve()
    scripts_dir = repo_root / "scripts"
    discovered = discover_checks(scripts_dir)

    selection_errors: list[str] = []
    if checks is not None:
        selected_names: list[str] = []
        for raw_name in checks:
            normalized = normalize_check_name(raw_name)
            if normalized not in discovered:
                selection_errors.append(f"unknown check script: {raw_name} ({normalized})")
            elif discovered[normalized].suite == "library_helper":
                selection_errors.append(
                    f"check script is a non-executable library helper: {normalized}"
                )
            elif normalized not in selected_names:
                selected_names.append(normalized)
    elif suite == "docs":
        selected_names = [name for name, spec in discovered.items() if spec.suite == "docs"]
    else:
        selected_names = [
            name
            for name, spec in discovered.items()
            if spec.suite == "docs" or (include_gated and spec.suite == "gated")
        ]

    if selection_errors:
        return {
            "schema_version": SCHEMA_VERSION,
            "status": "failed",
            "repo_root": str(repo_root),
            "suite": suite,
            "discovered_script_count": len(discovered),
            "executed_count": 0,
            "passed_count": 0,
            "failed_count": len(selection_errors),
            "total_duration_ms": 0,
            "document_cache": {"hits": 0, "misses": 0, "unique_paths": 0},
            "checks": [],
            "blockers": selection_errors,
            "claim_gate_status": "not_claim_grade",
            **fail_closed_fields(),
        }

    results: list[dict[str, Any]] = []
    blockers: list[str] = []
    total_started = time.monotonic()

    with contextlib.ExitStack() as stack:
        if keep_reports_dir is not None:
            keep_dir = (
                keep_reports_dir
                if keep_reports_dir.is_absolute()
                else repo_root / keep_reports_dir
            )
            keep_dir.mkdir(parents=True, exist_ok=True)
            reports_dir = keep_dir
        else:
            reports_dir = Path(stack.enter_context(tempfile.TemporaryDirectory()))

        with cached_document_reads() as doc_cache:
            for name in selected_names:
                spec = discovered[name]
                row = _invoke_check_in_process(
                    spec=spec,
                    script_path=scripts_dir / f"{name}.py",
                    repo_root=repo_root,
                    reports_dir=reports_dir,
                )
                results.append(row)
                if row["exit_code"] != 0:
                    tail = row["stderr_tail"] or row["stdout_tail"]
                    detail = tail[-1] if tail else f"exit_code={row['exit_code']}"
                    blockers.append(f"{name}: {detail}")
                    if fail_fast:
                        break

    total_duration_ms = int(round((time.monotonic() - total_started) * 1000.0))
    passed_count = sum(1 for row in results if row["exit_code"] == 0)
    failed_count = len(results) - passed_count
    return {
        "schema_version": SCHEMA_VERSION,
        "status": "passed" if failed_count == 0 and not blockers else "failed",
        "repo_root": str(repo_root),
        "suite": suite,
        "discovered_script_count": len(discovered),
        "executed_count": len(results),
        "passed_count": passed_count,
        "failed_count": failed_count,
        "total_duration_ms": total_duration_ms,
        "document_cache": doc_cache.stats(),
        "checks": results,
        "blockers": blockers,
        "claim_gate_status": "not_claim_grade",
        **fail_closed_fields(),
    }


def list_payload(repo_root: Path) -> dict[str, Any]:
    discovered = discover_checks(repo_root / "scripts")
    counts: dict[str, int] = {}
    rows = []
    for name, spec in discovered.items():
        counts[spec.suite] = counts.get(spec.suite, 0) + 1
        rows.append(
            {
                "name": name,
                "script": f"scripts/{name}.py",
                "suite": spec.suite,
                "reason": spec.reason,
                "uses_repo_root": spec.uses_repo_root,
                "output_flag": spec.output_flag,
                "no_json_flag": spec.no_json_flag,
                "has_main": spec.has_main,
            }
        )
    return {
        "schema_version": SCHEMA_VERSION,
        "discovered_script_count": len(discovered),
        "suite_counts": counts,
        "checks": rows,
        "claim_gate_status": "not_claim_grade",
        **fail_closed_fields(),
    }


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    repo_root = args.repo_root.resolve()
    if args.list:
        print(json.dumps(list_payload(repo_root), indent=2, sort_keys=True))
        return 0

    report = build_report(
        repo_root=repo_root,
        suite=args.suite,
        checks=args.checks,
        include_gated=args.include_gated,
        keep_reports_dir=args.keep_reports_dir,
        fail_fast=args.fail_fast,
    )
    if not args.no_json:
        output = args.output if args.output.is_absolute() else repo_root / args.output
        write_json(output, report)

    summary = {
        key: value
        for key, value in report.items()
        if key != "checks"
    }
    summary["failed_checks"] = [
        {
            "name": row["name"],
            "exit_code": row["exit_code"],
            "stderr_tail": row["stderr_tail"],
            "stdout_tail": row["stdout_tail"],
        }
        for row in report["checks"]
        if row["exit_code"] != 0
    ]
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
