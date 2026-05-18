#!/usr/bin/env python
# SPDX-License-Identifier: Apache-2.0
"""Validate the report-only enterprise evidence export-pack contract."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_VERSION = "shardloom.enterprise_evidence_export_pack.v1"
REPORT_SCHEMA_VERSION = "shardloom.enterprise_evidence_export_pack_report.v1"

EXPECTED_COMPONENT_IDS = [
    "shardloom_json_evidence_bundle",
    "openlineage_custom_facets",
    "opentelemetry_spans_metrics",
    "markdown_summary",
]

REQUIRED_FALSE_FIELDS = [
    "export_pack_runtime_supported",
    "export_pack_enabled_by_default",
    "network_calls_by_default",
    "backend_integration_configured",
    "lineage_event_emitted",
    "telemetry_trace_emitted",
    "telemetry_metric_emitted",
    "telemetry_log_emitted",
    "markdown_summary_generated_by_default",
    "external_engine_invoked",
    "fallback_attempted",
    "object_store_io_performed",
    "credential_resolution_performed",
    "foundry_runtime_invoked",
]

REQUIRED_REDACTED_FIELDS = [
    "secrets",
    "credentials",
    "access_tokens",
    "environment_variables",
    "full_local_paths",
    "query_text",
    "schema_names",
    "sample_values",
    "endpoint_urls",
    "object_store_credentials",
    "platform_dataset_identifiers",
]

REQUIRED_DOC_SNIPPETS = [
    "shardloom.enterprise_evidence_export_pack.v1",
    "python scripts\\check_enterprise_evidence_export_pack.py",
    "shardloom.openlineage_facet_mapping.v1",
    "shardloom.opentelemetry_trace_export_contract.v1",
    "strict_local_enterprise_redaction",
    "fallback_attempted=false",
    "external_engine_invoked=false",
    "network_calls_by_default=false",
    "backend_integration_configured=false",
    "claim_gate_status=not_claim_grade",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=ROOT)
    parser.add_argument(
        "--manifest",
        type=Path,
        default=Path("docs/release/enterprise-evidence-export-pack.json"),
    )
    parser.add_argument(
        "--doc",
        type=Path,
        default=Path("docs/release/enterprise-evidence-export-pack.md"),
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("target/enterprise-evidence-export-pack-report.json"),
    )
    return parser.parse_args()


def resolve(repo_root: Path, path: Path) -> Path:
    return path if path.is_absolute() else repo_root / path


def load_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def _non_empty_string(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip())


def validate_manifest(manifest: dict[str, Any] | None) -> list[str]:
    blockers: list[str] = []
    if manifest is None:
        return ["missing enterprise evidence export-pack manifest"]

    if manifest.get("schema_version") != SCHEMA_VERSION:
        blockers.append(f"schema_version={manifest.get('schema_version')}")
    if manifest.get("gar_id") != "GAR-COMMERCIAL-1D":
        blockers.append(f"gar_id={manifest.get('gar_id')}")
    if manifest.get("status") != "report-only":
        blockers.append(f"status={manifest.get('status')}")
    if manifest.get("claim_gate_status") != "not_claim_grade":
        blockers.append(f"claim_gate_status={manifest.get('claim_gate_status')}")
    if manifest.get("opt_in_required") is not True:
        blockers.append("opt_in_required must be true")
    for field in REQUIRED_FALSE_FIELDS:
        if manifest.get(field) is not False:
            blockers.append(f"{field} must be false")
    for field in ["claim_boundary", "fallback_boundary"]:
        if not _non_empty_string(manifest.get(field)):
            blockers.append(f"missing {field}")

    refs = manifest.get("source_refs")
    if not isinstance(refs, list) or not refs:
        blockers.append("source_refs must be a non-empty list")
    else:
        for required in [
            "docs/architecture/evidence-native-generated-execution-observability-confidence.md",
            "docs/architecture/adoption-commercial-readiness-friction-reduction.md",
        ]:
            if required not in refs:
                blockers.append(f"missing source ref {required}")

    layout = manifest.get("local_artifact_layout")
    if not isinstance(layout, dict):
        blockers.append("local_artifact_layout must be an object")
    else:
        if layout.get("root_directory") != "target/enterprise-evidence-export-pack/<run-id>/":
            blockers.append("local_artifact_layout.root_directory mismatch")
        layout_files = layout.get("files")
        if not isinstance(layout_files, list) or not layout_files:
            blockers.append("local_artifact_layout.files must be non-empty")
        else:
            file_components = [
                row.get("component_id")
                for row in layout_files
                if isinstance(row, dict) and row.get("component_id") != "redaction_report"
            ]
            for component_id in EXPECTED_COMPONENT_IDS:
                if component_id not in file_components:
                    blockers.append(f"missing layout file for {component_id}")

    components = manifest.get("artifact_components")
    if not isinstance(components, list):
        blockers.append("artifact_components must be a list")
    else:
        seen = [row.get("component_id") for row in components if isinstance(row, dict)]
        if seen != EXPECTED_COMPONENT_IDS:
            blockers.append(f"component order/ids mismatch: {seen}")
        for row in components:
            if not isinstance(row, dict):
                blockers.append("artifact component rows must be objects")
                continue
            component_id = row.get("component_id", "<missing>")
            prefix = f"{component_id}: "
            for field in ["display_name", "source_schema_ref", "network_effect_status", "backend_integration_status", "redaction_status"]:
                if not _non_empty_string(row.get(field)):
                    blockers.append(prefix + f"missing {field}")
            if row.get("default_enabled") is not False:
                blockers.append(prefix + "default_enabled must be false")
            if row.get("network_effect_status") != "none":
                blockers.append(prefix + "network_effect_status must be none")
            if row.get("backend_integration_status") != "none":
                blockers.append(prefix + "backend_integration_status must be none")
            if row.get("redaction_status") != "required_before_export":
                blockers.append(prefix + "redaction_status must be required_before_export")

    redaction = manifest.get("redaction_policy")
    if not isinstance(redaction, dict):
        blockers.append("redaction_policy must be an object")
    else:
        if redaction.get("policy_id") != "strict_local_enterprise_redaction":
            blockers.append(f"redaction policy mismatch: {redaction.get('policy_id')}")
        if redaction.get("required") is not True:
            blockers.append("redaction_policy.required must be true")
        redacted_fields = redaction.get("redacted_fields")
        if not isinstance(redacted_fields, list):
            blockers.append("redaction_policy.redacted_fields must be a list")
        else:
            for field in REQUIRED_REDACTED_FIELDS:
                if field not in redacted_fields:
                    blockers.append(f"redaction_policy missing {field}")
        allowed = redaction.get("allowed_field_classes")
        if not isinstance(allowed, list) or "claim_gate_status" not in allowed:
            blockers.append("redaction_policy.allowed_field_classes must preserve claim_gate_status")
        if not _non_empty_string(redaction.get("path_policy")):
            blockers.append("redaction_policy.path_policy missing")
        if not _non_empty_string(redaction.get("sample_policy")):
            blockers.append("redaction_policy.sample_policy missing")

    retention = manifest.get("retention_policy")
    if not isinstance(retention, dict):
        blockers.append("retention_policy must be an object")
    else:
        if retention.get("upload_by_default") is not False:
            blockers.append("retention_policy.upload_by_default must be false")
        if retention.get("backend_retention_configured") is not False:
            blockers.append("retention_policy.backend_retention_configured must be false")

    controls = manifest.get("opt_in_controls")
    if not isinstance(controls, dict):
        blockers.append("opt_in_controls must be an object")
    else:
        if controls.get("future_cli_local_only_default") is not True:
            blockers.append("future_cli_local_only_default must be true")
        if controls.get("requires_explicit_output_directory") is not True:
            blockers.append("requires_explicit_output_directory must be true")
        if controls.get("requires_redaction_report") is not True:
            blockers.append("requires_redaction_report must be true")

    return blockers


def validate_doc(doc_text: str) -> list[str]:
    blockers: list[str] = []
    if not doc_text:
        return ["missing enterprise evidence export-pack documentation"]
    for required in REQUIRED_DOC_SNIPPETS:
        if required not in doc_text:
            blockers.append(f"doc missing {required}")
    return blockers


def main() -> int:
    args = parse_args()
    repo_root = args.repo_root.resolve()
    manifest_path = resolve(repo_root, args.manifest)
    doc_path = resolve(repo_root, args.doc)
    output_path = resolve(repo_root, args.output)
    manifest = load_json(manifest_path)
    blockers = validate_manifest(manifest)
    blockers.extend(validate_doc(doc_path.read_text(encoding="utf-8") if doc_path.exists() else ""))
    report = {
        "schema_version": REPORT_SCHEMA_VERSION,
        "manifest_ref": str(args.manifest).replace("\\", "/"),
        "doc_ref": str(args.doc).replace("\\", "/"),
        "status": "passed" if not blockers else "failed",
        "claim_gate_status": (manifest or {}).get("claim_gate_status", "missing"),
        "export_pack_runtime_supported": (manifest or {}).get("export_pack_runtime_supported", False),
        "network_calls_by_default": (manifest or {}).get("network_calls_by_default", False),
        "backend_integration_configured": (manifest or {}).get("backend_integration_configured", False),
        "fallback_attempted": (manifest or {}).get("fallback_attempted", False),
        "external_engine_invoked": (manifest or {}).get("external_engine_invoked", False),
        "blockers": blockers,
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(output_path)
    return 0 if not blockers else 1


if __name__ == "__main__":
    raise SystemExit(main())
