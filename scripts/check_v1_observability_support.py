#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Validate v1 observability, supportability, and troubleshooting evidence.

This gate is intentionally local and side-effect-free. It validates existing
doctor, support-bundle, capability, explain/estimate, runtime-report, route
capability, diagnostic-code, issue-template, and benchmark-field surfaces
without claiming production observability, telemetry export, package release,
or performance superiority.
"""

from __future__ import annotations

import argparse
import gzip
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Mapping, Sequence

from release_report_utils import fail_closed_fields, load_json, read_text, resolve_path, write_json


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_VERSION = "shardloom.v1_observability_support_report.v1"
DEFAULT_FEATURES = "vortex-write,vortex-local-primitives"
SCOPE_DOC = Path("docs/architecture/v1-observability-support.md")
TROUBLESHOOTING_DOC = Path("docs/release/troubleshooting-diagnostics.md")
DIAGNOSTIC_CODE_DOC = Path("docs/release/diagnostic-code-stability.md")
USER_ROUTE_REPORT = Path("target/user-route-capability-report.json")
API_SCHEMA_REPORT = Path("target/v1-api-schema-stability-report.json")
BENCHMARK_ARTIFACT = Path("website/assets/benchmarks/latest/benchmark-results.json")
ISSUE_TEMPLATES = (
    Path(".github/ISSUE_TEMPLATE/shardloom-diagnostic.yml"),
    Path(".github/ISSUE_TEMPLATE/shardloom-support-bundle.yml"),
)

COMMAND_SPECS: tuple[dict[str, Any], ...] = (
    {
        "label": "doctor",
        "argv": ("doctor", "--format", "json"),
        "expected_returncodes": (0,),
    },
    {
        "label": "support_bundle",
        "argv": (
            "support-bundle",
            "--note",
            "token=abc123 Authorization: Bearer secret-value",
            "--include-defaults",
            "--format",
            "json",
        ),
        "expected_returncodes": (0,),
    },
    {
        "label": "agent_contract_pack",
        "argv": ("agent-contract-pack", "--format", "json"),
        "expected_returncodes": (0,),
    },
    {
        "label": "capabilities_certification",
        "argv": ("capabilities", "certification", "--format", "json"),
        "expected_returncodes": (0,),
    },
    {
        "label": "runtime_report",
        "argv": ("runtime-report", "--format", "json"),
        "expected_returncodes": (0,),
    },
    {
        "label": "observability_schema_coverage",
        "argv": ("observability-schema-coverage", "--format", "json"),
        "expected_returncodes": (0,),
    },
    {
        "label": "explain_plan_only",
        "argv": ("explain", "local-file-query", "--format", "json"),
        "expected_returncodes": (1,),
    },
    {
        "label": "estimate_plan_only",
        "argv": ("estimate", "local-file-query", "--format", "json"),
        "expected_returncodes": (1,),
    },
)

DOC_MARKERS = (
    "shardloom.v1_observability_support.v1",
    "python scripts/check_v1_observability_support.py",
    "target/v1-observability-support-report.json",
    "doctor --format json",
    "support-bundle --format json",
    "agent-contract-pack --format json",
    "capabilities certification --format json",
    "runtime-report --format json",
    "observability-schema-coverage --format json",
    "explain local-file-query --format json",
    "estimate local-file-query --format json",
    "no OpenTelemetry exporter claim",
    "no remote support upload claim",
)

TROUBLESHOOTING_MARKERS = (
    "docs/release/diagnostic-code-stability.md",
    "SL_INVALID_INPUT",
    "SL_UNSUPPORTED_SQL",
    "SL_RESOURCE_BUDGET_EXCEEDED",
    "fallback_attempted=false",
    "external_engine_invoked=false",
    "support-bundle --format json",
    "doctor --format json",
)

ISSUE_TEMPLATE_MARKERS = (
    "command",
    "JSON envelope",
    "diagnostic code",
    "route id",
    "fallback_attempted",
    "external_engine_invoked",
    "CLI version",
    "Python version",
    "Rust version",
    "OS",
)

BENCHMARK_REQUIRED_FIELDS = (
    "route_lane_id",
    "route_runtime_status",
    "timing_surface",
    "timing_surface_evidence_tier",
    "route_timing_surface_schema_version",
    "route_timing_stage_inclusion_schema_version",
    "route_timing_stage_inclusion_classes",
    "route_timing_stage_inclusion_stage_ids",
    "route_timing_stage_inclusion_stage_owners",
    "route_timing_included_stage_ids",
    "route_total_formula",
    "actual_evidence_tier",
    "sink_timing_included_in_route_total",
    "fallback_attempted",
    "external_engine_invoked",
    "native_io_certificate_status",
    "runtime_execution_certificate_status",
    "source_native_io_certificate_status",
    "output_native_io_certificate_status",
    "certificate_link_status",
)

EXPECTED_ROUTE_LANES = {
    "cold_certified_route",
    "native_vortex_query",
    "prepare_once_batch",
    "prepare_once_first_query",
    "warm_prepared_query",
}


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=ROOT)
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("target/v1-observability-support-report.json"),
    )
    parser.add_argument("--binary", type=Path)
    parser.add_argument("--features", default=DEFAULT_FEATURES)
    parser.add_argument("--skip-build", action="store_true")
    return parser.parse_args(argv)


def rel(repo_root: Path, path: Path) -> str:
    try:
        return path.resolve().relative_to(repo_root).as_posix()
    except ValueError:
        return path.resolve().as_posix()


def tail(text: str, limit: int = 4000) -> str:
    return text if len(text) <= limit else text[-limit:]


def redact_report_text(text: str) -> str:
    """Keep synthetic redaction probes out of generated evidence reports."""
    redacted = text
    for raw, replacement in (
        ("token=abc123", "token=<redacted>"),
        ("Bearer secret-value", "Bearer <redacted>"),
        ("abc123", "<redacted>"),
        ("secret-value", "<redacted>"),
    ):
        redacted = redacted.replace(raw, replacement)
    return redacted


def command_text(command: Sequence[str]) -> str:
    return redact_report_text(" ".join(command).replace(str(sys.executable), "python"))


def command_env(repo_root: Path) -> dict[str, str]:
    env = os.environ.copy()
    python_path = str(repo_root / "python" / "src")
    env["PYTHONPATH"] = (
        python_path
        if not env.get("PYTHONPATH")
        else python_path + os.pathsep + env["PYTHONPATH"]
    )
    return env


def run_plain_command(
    repo_root: Path,
    command: list[str],
    *,
    include_raw_stdout: bool = False,
) -> dict[str, Any]:
    started = time.perf_counter()
    completed = subprocess.run(
        command,
        cwd=repo_root,
        env=command_env(repo_root),
        text=True,
        capture_output=True,
        check=False,
    )
    elapsed = (time.perf_counter() - started) * 1000.0
    result: dict[str, Any] = {
        "command": command_text(command),
        "argv": [redact_report_text(part) for part in command],
        "returncode": completed.returncode,
        "status": "passed" if completed.returncode == 0 else "failed",
        "elapsed_millis": round(elapsed, 4),
        "stdout_tail": redact_report_text(tail(completed.stdout)),
        "stderr_tail": redact_report_text(tail(completed.stderr)),
    }
    if include_raw_stdout:
        result["_stdout"] = completed.stdout
    return result


def run_json_command(repo_root: Path, command: list[str]) -> dict[str, Any]:
    result = run_plain_command(repo_root, command, include_raw_stdout=True)
    stdout = str(result.pop("_stdout", ""))
    payload: Any = None
    parse_error = None
    if stdout.strip():
        try:
            import json

            payload = json.loads(stdout)
        except json.JSONDecodeError as error:
            parse_error = str(error)
    result["json_parse_error"] = parse_error
    result["payload"] = payload
    return result


def locate_binary(repo_root: Path, explicit: Path | None) -> Path:
    if explicit is not None:
        return resolve_path(repo_root, explicit).resolve()
    target_root = Path(os.environ.get("CARGO_TARGET_DIR", repo_root / "target"))
    if not target_root.is_absolute():
        target_root = repo_root / target_root
    suffix = ".exe" if os.name == "nt" else ""
    return (target_root / "debug" / f"shardloom{suffix}").resolve()


def ensure_binary(
    repo_root: Path,
    *,
    binary: Path,
    features: str,
    skip_build: bool,
    explicit_binary: bool,
) -> tuple[dict[str, Any], list[str]]:
    if binary.exists() and (skip_build or explicit_binary):
        return {
            "command": "reused explicit/existing shardloom binary",
            "status": "passed",
            "binary_ref": rel(repo_root, binary),
            "blockers": [],
        }, []
    if skip_build:
        blocker = f"binary missing and --skip-build was set: {rel(repo_root, binary)}"
        return {
            "command": "skipped",
            "status": "failed",
            "binary_ref": rel(repo_root, binary),
            "blockers": [blocker],
        }, [blocker]
    command = [
        "cargo",
        "build",
        "-q",
        "-p",
        "shardloom-cli",
        "--features",
        features,
    ]
    result = run_plain_command(repo_root, command)
    blockers: list[str] = []
    if result["returncode"] != 0:
        blockers.append("failed to build shardloom CLI for v1 observability support")
    if not binary.exists():
        blockers.append(f"built binary missing: {rel(repo_root, binary)}")
    result.update(
        {
            "status": "passed" if not blockers else "failed",
            "binary_ref": rel(repo_root, binary),
            "blockers": blockers,
        }
    )
    return result, blockers


def collect_field_rows(payload: Any) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if isinstance(payload, dict):
        direct = payload.get("fields")
        if isinstance(direct, list):
            rows.extend(row for row in direct if isinstance(row, dict))
        for value in payload.values():
            rows.extend(collect_field_rows(value))
    elif isinstance(payload, list):
        for value in payload:
            rows.extend(collect_field_rows(value))
    return rows


def fields(payload: Any) -> dict[str, str]:
    return {
        str(row.get("key")): str(row.get("value"))
        for row in collect_field_rows(payload)
        if isinstance(row.get("key"), str)
    }


def bool_value(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered == "true":
            return True
        if lowered == "false":
            return False
    return None


def check_envelope(
    label: str,
    result: Mapping[str, Any],
    expected_returncodes: Sequence[int],
) -> tuple[dict[str, Any], dict[str, str], list[str]]:
    blockers: list[str] = []
    payload = result.get("payload")
    if result.get("returncode") not in expected_returncodes:
        blockers.append(f"{label}: returncode={result.get('returncode')}")
    if result.get("json_parse_error"):
        blockers.append(f"{label}: JSON parse error {result.get('json_parse_error')}")
    if not isinstance(payload, dict):
        blockers.append(f"{label}: command output was not a JSON object")
        return {"status": "failed", "command": result.get("command")}, {}, blockers
    if payload.get("schema_version") != "shardloom.output.v2":
        blockers.append(f"{label}: schema_version={payload.get('schema_version', 'missing')}")
    fallback = payload.get("fallback")
    if not isinstance(fallback, dict):
        blockers.append(f"{label}: missing fallback object")
    else:
        if fallback.get("attempted") is not False:
            blockers.append(f"{label}: fallback.attempted must be false")
        if fallback.get("allowed") is not False:
            blockers.append(f"{label}: fallback.allowed must be false")
    observed = fields(payload)
    if bool_value(observed.get("fallback_attempted")) is True:
        blockers.append(f"{label}: field fallback_attempted must not be true")
    if bool_value(observed.get("external_engine_invoked")) is True:
        blockers.append(f"{label}: field external_engine_invoked must not be true")
    return {
        "status": "passed" if not blockers else "failed",
        "command": result.get("command"),
        "returncode": result.get("returncode"),
        "output_status": payload.get("status"),
        "field_count": len(observed),
        "diagnostic_count": len(payload.get("diagnostics", [])),
    }, observed, blockers


def expect_fields(
    label: str,
    observed: Mapping[str, str],
    *,
    equals: Mapping[str, str] | None = None,
    true_fields: Sequence[str] = (),
    false_fields: Sequence[str] = (),
    contains: Mapping[str, str] | None = None,
) -> list[str]:
    blockers: list[str] = []
    for key, expected in (equals or {}).items():
        if observed.get(key) != expected:
            blockers.append(f"{label}: {key}={observed.get(key, 'missing')}, expected {expected}")
    for key in true_fields:
        if bool_value(observed.get(key)) is not True:
            blockers.append(f"{label}: {key} must be true")
    for key in false_fields:
        if bool_value(observed.get(key)) is not False:
            blockers.append(f"{label}: {key} must be false")
    for key, needle in (contains or {}).items():
        if needle not in str(observed.get(key, "")):
            blockers.append(f"{label}: {key} must contain {needle!r}")
    return blockers


def validate_command(
    label: str,
    payload: Mapping[str, Any],
    observed: Mapping[str, str],
) -> tuple[dict[str, Any], list[str]]:
    blockers: list[str] = []
    if label == "doctor":
        blockers.extend(
            expect_fields(
                label,
                observed,
                equals={
                    "doctor_schema_version": "shardloom.doctor.v1",
                    "doctor_check_count": "8",
                    "doctor_check_no_fallback_invariant_status": "verified",
                },
                true_fields=("support_bundle_available",),
                false_fields=(
                    "environment_probe_performed",
                    "filesystem_probe_performed",
                    "network_probe_performed",
                    "runtime_execution",
                    "fallback_attempted",
                    "external_engine_invoked",
                ),
                contains={"support_bundle_command": "support-bundle --format json"},
            )
        )
    elif label == "support_bundle":
        blockers.extend(
            expect_fields(
                label,
                observed,
                equals={
                    "schema_version": "shardloom.support_bundle.v1",
                    "redaction_status": "redacted",
                    "doctor_schema_version": "shardloom.doctor.v1",
                },
                true_fields=("support_bundle_generated", "input_contains_redacted_tokens"),
                false_fields=(
                    "support_bundle_written",
                    "raw_secret_values_present",
                    "secret_values_included",
                    "filesystem_write_performed",
                    "filesystem_probe_performed",
                    "network_probe_performed",
                    "external_effects_executed",
                    "runtime_execution",
                    "fallback_attempted",
                    "external_engine_invoked",
                ),
                contains={
                    "redacted_note_preview": "token=<redacted>",
                    "included_report_refs": "doctor",
                },
            )
        )
        preview = observed.get("redacted_note_preview", "")
        if "abc123" in preview or "secret-value" in preview:
            blockers.append(f"{label}: redacted_note_preview leaked raw secret value")
    elif label == "agent_contract_pack":
        blockers.extend(
            expect_fields(
                label,
                observed,
                equals={
                    "available_surface_count": "14",
                    "side_effect_free_surface_count": "14",
                    "fallback_allowed_surface_count": "0",
                },
                true_fields=("side_effect_free",),
                false_fields=("fallback_attempted", "fallback_execution_allowed"),
                contains={
                    "recommended_sequence": "doctor --format json",
                    "surface_order": "support_bundle",
                },
            )
        )
    elif label == "capabilities_certification":
        blockers.extend(
            expect_fields(
                label,
                observed,
                equals={
                    "best_default_certification_gate_support_status": "blocked",
                    "best_default_certification_gate_claim_gate_status": "not_claim_grade",
                },
                true_fields=("best_default_certification_gate_no_fallback_policy_required",),
                false_fields=(
                    "fallback_execution_allowed",
                    "fallback_attempted",
                    "runtime_execution",
                    "best_default_certification_gate_best_default_claim_allowed",
                    "best_default_certification_gate_performance_claim_allowed",
                    "best_default_certification_gate_superiority_claim_allowed",
                    "best_default_certification_gate_spark_replacement_claim_allowed",
                    "best_default_certification_gate_production_claim_allowed",
                    "best_default_certification_gate_runtime_execution",
                    "best_default_certification_gate_fallback_attempted",
                    "best_default_certification_gate_external_engine_invoked",
                ),
            )
        )
    elif label == "runtime_report":
        blockers.extend(
            expect_fields(
                label,
                observed,
                equals={
                    "schema_version": "shardloom.runtime_observability_report.v1",
                    "local_benchmark_stage_timing_field_count": "10",
                    "support_status": "report_only",
                    "claim_gate_status": "not_claim_grade",
                },
                true_fields=("no_runtime_collection_or_external_effects",),
                false_fields=(
                    "trace_backend_enabled",
                    "runtime_collection_enabled",
                    "debug_bundle_generated",
                    "external_engine_invoked",
                    "fallback_attempted",
                    "fallback_execution_allowed",
                ),
                contains={
                    "local_benchmark_stage_timing_field_order": "total_runtime_millis",
                    "runtime_blocker_order": "live_profiling",
                },
            )
        )
    elif label == "observability_schema_coverage":
        blockers.extend(
            expect_fields(
                label,
                observed,
                true_fields=(
                    "schema_coverage_complete",
                    "debug_bundle_schema_present",
                    "redaction_required",
                    "certificate_link_required",
                ),
                false_fields=(
                    "exporter_integration_enabled",
                    "runtime_collection_enabled",
                    "fallback_attempted",
                ),
            )
        )
    elif label in {"explain_plan_only", "estimate_plan_only"}:
        if payload.get("status") != "unsupported":
            blockers.append(f"{label}: output status must be unsupported")
        diagnostics = payload.get("diagnostics")
        if not isinstance(diagnostics, list) or not diagnostics:
            blockers.append(f"{label}: missing deterministic diagnostic")
        else:
            diagnostic = diagnostics[0]
            if diagnostic.get("code") != "SL_UNSUPPORTED_SQL":
                blockers.append(f"{label}: diagnostic code={diagnostic.get('code')}")
            if diagnostic.get("category") != "unsupported_feature":
                blockers.append(f"{label}: diagnostic category={diagnostic.get('category')}")
            fallback = diagnostic.get("fallback")
            if not isinstance(fallback, dict) or fallback.get("attempted") is not False:
                blockers.append(f"{label}: diagnostic fallback.attempted must be false")
        blockers.extend(
            expect_fields(
                label,
                observed,
                equals={"mode": "plan_only", "execution": "not_performed"},
                true_fields=("plan_only",),
                false_fields=(
                    "fallback_execution_allowed",
                    "data_read",
                    "data_materialized",
                    "object_store_io",
                    "write_io",
                    "external_effects_executed",
                ),
            )
        )
    else:
        blockers.append(f"{label}: unknown command validation")
    return {
        "status": "passed" if not blockers else "failed",
        "field_count": len(observed),
        "selected_fields": {
            key: observed.get(key)
            for key in sorted(
                {
                    "schema_version",
                    "doctor_schema_version",
                    "redaction_status",
                    "available_surface_count",
                    "support_status",
                    "claim_gate_status",
                    "fallback_attempted",
                    "external_engine_invoked",
                    "runtime_execution",
                    "network_probe_performed",
                }
            )
            if key in observed
        },
    }, blockers


def ensure_report(
    repo_root: Path,
    path: Path,
    command: list[str],
    label: str,
) -> tuple[dict[str, Any] | None, dict[str, Any], list[str]]:
    resolved = resolve_path(repo_root, path)
    command_summary: dict[str, Any] = {
        "command": command_text(command),
        "status": "skipped_existing_report" if resolved.exists() else "not_run",
        "report_ref": path.as_posix(),
    }
    blockers: list[str] = []
    if not resolved.exists():
        command_summary = run_plain_command(repo_root, command)
        command_summary["report_ref"] = path.as_posix()
        if command_summary["returncode"] != 0:
            blockers.append(f"{label}: prerequisite generator failed")
    payload = load_json(resolved, missing_ok=True)
    if payload is None:
        blockers.append(f"{label}: missing report {path}")
        return None, command_summary, blockers
    if not isinstance(payload, dict):
        blockers.append(f"{label}: report must be an object")
        return None, command_summary, blockers
    return payload, command_summary, blockers


def validate_user_route_report(payload: Mapping[str, Any]) -> tuple[dict[str, Any], list[str]]:
    blockers: list[str] = []
    expected = {
        "schema_version": "shardloom.user_route_capability_report.v1",
        "status": "passed",
        "claim_gate_status": "not_claim_grade",
    }
    for key, value in expected.items():
        if payload.get(key) != value:
            blockers.append(f"user_route_capability: {key}={payload.get(key, 'missing')}")
    if payload.get("all_no_fallback_no_external_engine") is not True:
        blockers.append("user_route_capability: all_no_fallback_no_external_engine must be true")
    if payload.get("unsupported_local_benchmark_route_ids"):
        blockers.append("user_route_capability: unsupported local benchmark routes must be empty")
    if payload.get("local_file_benchmark_unsupported_scenario_ids"):
        blockers.append("user_route_capability: unsupported benchmark scenarios must be empty")
    acceptance = payload.get("acceptance_summary")
    required_acceptance = (
        "all_required_local_file_benchmark_scenarios_mapped",
        "all_admitted_benchmark_routes_have_clear_output_options",
        "all_admitted_local_file_benchmark_routes_have_clear_output_options",
        "public_front_door_routes_preserve_no_fallback",
    )
    if not isinstance(acceptance, dict):
        blockers.append("user_route_capability: missing acceptance_summary")
    else:
        for field in required_acceptance:
            if acceptance.get(field) is not True:
                blockers.append(f"user_route_capability: acceptance {field} must be true")
    return {
        "status": "passed" if not blockers else "failed",
        "route_count": payload.get("route_count"),
        "local_file_benchmark_route_count": payload.get("local_file_benchmark_route_count"),
        "public_front_door_route_count": payload.get("public_front_door_route_count"),
        "unsupported_local_benchmark_route_count": len(
            payload.get("unsupported_local_benchmark_route_ids", [])
        ),
    }, blockers


def validate_api_schema_report(payload: Mapping[str, Any]) -> tuple[dict[str, Any], list[str]]:
    blockers: list[str] = []
    if payload.get("schema_version") != "shardloom.v1_api_schema_stability_report.v1":
        blockers.append("api_schema_stability: schema_version mismatch")
    if payload.get("status") != "passed":
        blockers.extend(payload.get("blockers", ["api schema stability report blocked"]))
    if payload.get("diagnostic_code_count") != 22:
        blockers.append(
            "api_schema_stability: diagnostic_code_count="
            + str(payload.get("diagnostic_code_count", "missing"))
        )
    if payload.get("diagnostic_code_doc_ref") != DIAGNOSTIC_CODE_DOC.as_posix():
        blockers.append("api_schema_stability: diagnostic_code_doc_ref mismatch")
    codes = payload.get("diagnostic_code_order")
    for code in ("SL_INVALID_INPUT", "SL_UNSUPPORTED_SQL", "SL_RESOURCE_BUDGET_EXCEEDED"):
        if not isinstance(codes, list) or code not in codes:
            blockers.append(f"api_schema_stability: missing diagnostic code {code}")
    return {
        "status": "passed" if not blockers else "failed",
        "diagnostic_code_count": payload.get("diagnostic_code_count"),
        "diagnostic_code_doc_ref": payload.get("diagnostic_code_doc_ref"),
    }, blockers


def validate_docs(repo_root: Path) -> tuple[dict[str, Any], list[str]]:
    blockers: list[str] = []
    scope_text = read_text(resolve_path(repo_root, SCOPE_DOC), missing_ok=True)
    troubleshooting_text = read_text(
        resolve_path(repo_root, TROUBLESHOOTING_DOC),
        missing_ok=True,
    )
    diagnostic_text = read_text(
        resolve_path(repo_root, DIAGNOSTIC_CODE_DOC),
        missing_ok=True,
    )
    blockers.extend(
        f"{SCOPE_DOC}: missing marker {marker!r}" for marker in DOC_MARKERS if marker not in scope_text
    )
    blockers.extend(
        f"{TROUBLESHOOTING_DOC}: missing marker {marker!r}"
        for marker in TROUBLESHOOTING_MARKERS
        if marker not in troubleshooting_text
    )
    for code in ("SL_INVALID_INPUT", "SL_UNSUPPORTED_SQL", "SL_RESOURCE_BUDGET_EXCEEDED"):
        if code not in diagnostic_text:
            blockers.append(f"{DIAGNOSTIC_CODE_DOC}: missing diagnostic code {code}")
    return {
        "status": "passed" if not blockers else "failed",
        "scope_doc_ref": SCOPE_DOC.as_posix(),
        "troubleshooting_doc_ref": TROUBLESHOOTING_DOC.as_posix(),
        "diagnostic_code_doc_ref": DIAGNOSTIC_CODE_DOC.as_posix(),
        "scope_marker_count": len(DOC_MARKERS),
        "troubleshooting_marker_count": len(TROUBLESHOOTING_MARKERS),
    }, blockers


def validate_issue_templates(repo_root: Path) -> tuple[dict[str, Any], list[str]]:
    blockers: list[str] = []
    for template in ISSUE_TEMPLATES:
        text = read_text(resolve_path(repo_root, template), missing_ok=True)
        if not text:
            blockers.append(f"{template}: missing template")
            continue
        for marker in ISSUE_TEMPLATE_MARKERS:
            if marker not in text:
                blockers.append(f"{template}: missing marker {marker!r}")
    return {
        "status": "passed" if not blockers else "failed",
        "issue_template_count": len(ISSUE_TEMPLATES),
        "issue_template_refs": [template.as_posix() for template in ISSUE_TEMPLATES],
    }, blockers


def load_benchmark_rows(repo_root: Path) -> list[dict[str, Any]]:
    payload = load_json(resolve_path(repo_root, BENCHMARK_ARTIFACT))
    if not isinstance(payload, dict):
        return []
    rows: list[dict[str, Any]] = []
    for ref in payload.get("published_benchmark_row_chunks", []):
        if not isinstance(ref, dict) or not isinstance(ref.get("path"), str):
            continue
        path = resolve_path(repo_root, Path(ref["path"]))
        if path.name.endswith(".gz"):
            with gzip.open(path, "rt", encoding="utf-8") as handle:
                chunk = load_json_from_handle(handle)
        else:
            chunk = load_json(path, missing_ok=True)
        if isinstance(chunk, dict) and isinstance(chunk.get("rows"), list):
            rows.extend(row for row in chunk["rows"] if isinstance(row, dict))
    if not rows and isinstance(payload.get("published_benchmark_rows"), list):
        rows.extend(row for row in payload["published_benchmark_rows"] if isinstance(row, dict))
    return rows


def load_json_from_handle(handle: Any) -> Any:
    import json

    return json.load(handle)


def validate_benchmark_rows(repo_root: Path) -> tuple[dict[str, Any], list[str]]:
    rows = load_benchmark_rows(repo_root)
    shardloom_rows = [
        row for row in rows if str(row.get("engine", "")).startswith("shardloom")
    ]
    blockers: list[str] = []
    if not shardloom_rows:
        blockers.append("benchmark observability: no ShardLoom rows found")

    missing_counts = {
        field: sum(
            1
            for row in shardloom_rows
            if field not in row or row.get(field) in (None, "")
        )
        for field in BENCHMARK_REQUIRED_FIELDS
    }
    for field, count in missing_counts.items():
        if count:
            blockers.append(f"benchmark observability: {field} missing on {count} rows")

    route_lanes = {str(row.get("route_lane_id")) for row in shardloom_rows}
    missing_lanes = sorted(EXPECTED_ROUTE_LANES - route_lanes)
    if missing_lanes:
        blockers.append("benchmark observability: missing route lanes " + ",".join(missing_lanes))

    unsupported_rows = [
        row
        for row in shardloom_rows
        if str(row.get("route_runtime_status")) in {"unsupported", "blocked"}
        or str(row.get("status")) in {"unsupported", "blocked"}
    ]
    if unsupported_rows:
        blockers.append(
            "benchmark observability: ShardLoom blocked/unsupported rows present: "
            + str(len(unsupported_rows))
        )

    bad_fallback = [
        row
        for row in shardloom_rows
        if row.get("fallback_attempted") is not False
        or row.get("external_engine_invoked") is not False
    ]
    if bad_fallback:
        blockers.append(
            "benchmark observability: fallback/external engine flags are not false on "
            + str(len(bad_fallback))
            + " rows"
        )

    hot_rows = [row for row in shardloom_rows if row.get("timing_surface") == "hot_runtime"]
    publication_rows = [
        row for row in shardloom_rows if row.get("timing_surface") == "publication_proof"
    ]
    if not hot_rows:
        blockers.append("benchmark observability: missing hot_runtime rows")
    if not publication_rows:
        blockers.append("benchmark observability: missing publication_proof rows")

    for row in hot_rows:
        if row.get("actual_evidence_tier") != "metadata_sink":
            blockers.append("benchmark observability: hot_runtime row without metadata_sink tier")
            break
        if row.get("sink_timing_included_in_route_total") is not False:
            blockers.append("benchmark observability: hot_runtime row includes sink timing")
            break
        if "timing_surface=hot_runtime" not in str(row.get("route_total_formula", "")):
            blockers.append("benchmark observability: hot_runtime formula missing timing surface")
            break
    for row in publication_rows:
        if row.get("actual_evidence_tier") != "publication_full":
            blockers.append(
                "benchmark observability: publication_proof row without publication_full tier"
            )
            break
        if row.get("sink_timing_included_in_route_total") is not True:
            blockers.append(
                "benchmark observability: publication_proof row does not include sink timing"
            )
            break
        if "timing_surface=publication_proof" not in str(row.get("route_total_formula", "")):
            blockers.append(
                "benchmark observability: publication_proof formula missing timing surface"
            )
            break

    return {
        "status": "passed" if not blockers else "failed",
        "benchmark_artifact_ref": BENCHMARK_ARTIFACT.as_posix(),
        "benchmark_row_count": len(rows),
        "shardloom_row_count": len(shardloom_rows),
        "hot_runtime_row_count": len(hot_rows),
        "publication_proof_row_count": len(publication_rows),
        "route_lane_ids": sorted(route_lanes),
        "missing_required_field_counts": missing_counts,
        "unsupported_or_blocked_shardloom_row_count": len(unsupported_rows),
        "fallback_or_external_engine_row_count": len(bad_fallback),
    }, blockers


def build_report(
    *,
    repo_root: Path,
    binary: Path,
    explicit_binary: bool,
    features: str,
    skip_build: bool,
) -> dict[str, Any]:
    blockers: list[str] = []
    build, build_blockers = ensure_binary(
        repo_root,
        binary=binary,
        features=features,
        skip_build=skip_build,
        explicit_binary=explicit_binary,
    )
    blockers.extend(build_blockers)

    command_summaries: dict[str, dict[str, Any]] = {}
    command_results: dict[str, dict[str, Any]] = {}
    command_pass_count = 0
    for spec in COMMAND_SPECS:
        label = str(spec["label"])
        command = [str(binary), *spec["argv"]]
        result = run_json_command(repo_root, command)
        command_results[label] = {
            key: value for key, value in result.items() if key != "payload"
        }
        envelope_summary, observed, envelope_blockers = check_envelope(
            label,
            result,
            spec["expected_returncodes"],
        )
        command_blockers: list[str] = []
        if isinstance(result.get("payload"), dict):
            summary, command_blockers = validate_command(label, result["payload"], observed)
            envelope_summary.update(summary)
        command_summaries[label] = envelope_summary
        combined = envelope_blockers + command_blockers
        if combined:
            blockers.extend(combined)
        else:
            command_pass_count += 1

    user_payload, user_command, user_blockers = ensure_report(
        repo_root,
        USER_ROUTE_REPORT,
        [sys.executable, "scripts/check_user_route_capability_report.py"],
        "user_route_capability",
    )
    blockers.extend(user_blockers)
    api_payload, api_command, api_blockers = ensure_report(
        repo_root,
        API_SCHEMA_REPORT,
        [sys.executable, "scripts/check_v1_api_schema_stability.py"],
        "api_schema_stability",
    )
    blockers.extend(api_blockers)

    user_summary: dict[str, Any] = {"status": "failed"}
    api_summary: dict[str, Any] = {"status": "failed"}
    if isinstance(user_payload, dict):
        user_summary, user_validate_blockers = validate_user_route_report(user_payload)
        blockers.extend(user_validate_blockers)
    if isinstance(api_payload, dict):
        api_summary, api_validate_blockers = validate_api_schema_report(api_payload)
        blockers.extend(api_validate_blockers)

    docs_summary, docs_blockers = validate_docs(repo_root)
    blockers.extend(docs_blockers)
    issue_summary, issue_blockers = validate_issue_templates(repo_root)
    blockers.extend(issue_blockers)
    benchmark_summary, benchmark_blockers = validate_benchmark_rows(repo_root)
    blockers.extend(benchmark_blockers)

    passed = not blockers
    no_fallback = passed and command_pass_count == len(COMMAND_SPECS)
    return {
        "schema_version": SCHEMA_VERSION,
        "status": "passed" if passed else "failed",
        "blockers": blockers,
        "build": build,
        "binary_ref": rel(repo_root, binary),
        "scope_document": SCOPE_DOC.as_posix(),
        "troubleshooting_doc": TROUBLESHOOTING_DOC.as_posix(),
        "runtime_command_count": len(COMMAND_SPECS),
        "runtime_command_pass_count": command_pass_count,
        "command_results": command_results,
        "command_summaries": command_summaries,
        "user_route_capability_report_ref": USER_ROUTE_REPORT.as_posix(),
        "user_route_capability_command": user_command,
        "user_route_capability_summary": user_summary,
        "api_schema_stability_report_ref": API_SCHEMA_REPORT.as_posix(),
        "api_schema_stability_command": api_command,
        "api_schema_stability_summary": api_summary,
        "docs_summary": docs_summary,
        "issue_template_summary": issue_summary,
        "benchmark_observability_summary": benchmark_summary,
        "doctor_status": command_summaries.get("doctor", {}).get("status", "failed"),
        "support_bundle_status": command_summaries.get("support_bundle", {}).get(
            "status",
            "failed",
        ),
        "agent_contract_status": command_summaries.get("agent_contract_pack", {}).get(
            "status",
            "failed",
        ),
        "capability_discovery_status": command_summaries.get(
            "capabilities_certification",
            {},
        ).get("status", "failed"),
        "runtime_observability_status": command_summaries.get("runtime_report", {}).get(
            "status",
            "failed",
        ),
        "observability_schema_status": command_summaries.get(
            "observability_schema_coverage",
            {},
        ).get("status", "failed"),
        "explain_plan_only_status": command_summaries.get("explain_plan_only", {}).get(
            "status",
            "failed",
        ),
        "estimate_plan_only_status": command_summaries.get("estimate_plan_only", {}).get(
            "status",
            "failed",
        ),
        "route_capability_status": user_summary.get("status", "failed"),
        "api_schema_stability_status": api_summary.get("status", "failed"),
        "docs_status": docs_summary.get("status", "failed"),
        "issue_template_status": issue_summary.get("status", "failed"),
        "benchmark_observability_status": benchmark_summary.get("status", "failed"),
        "v1_scope_ready": passed,
        "observability_support_evidence_ready": passed,
        "side_effect_free_support_surfaces": no_fallback,
        "support_bundle_redaction_ready": command_summaries.get("support_bundle", {}).get(
            "status"
        )
        == "passed",
        "all_no_fallback_no_external_engine": no_fallback,
        "telemetry_exporter_enabled": False,
        "remote_support_upload_enabled": False,
        "runtime_profile_collection_enabled": False,
        "claim_gate_status": "not_claim_grade",
        "claim_boundary": "local_v1_observability_supportability_only_no_production_telemetry_or_remote_upload_claim",
        **fail_closed_fields(),
    }


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    repo_root = args.repo_root.resolve()
    binary = locate_binary(repo_root, args.binary)
    report = build_report(
        repo_root=repo_root,
        binary=binary,
        explicit_binary=args.binary is not None,
        features=args.features,
        skip_build=args.skip_build,
    )
    output = resolve_path(repo_root, args.output)
    write_json(output, report)
    print(output)
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
