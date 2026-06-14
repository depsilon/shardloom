#!/usr/bin/env python3
"""Validate the evidence-field schema registry docs and contract anchors."""

from __future__ import annotations

import json
from pathlib import Path


SCHEMA_VERSION = "shardloom.evidence_field_schema_registry.v1"
REGISTRY_SOURCE = "shardloom-cli/src/evidence_schema_registry.rs"
REGISTRY_DOC = "docs/status/evidence-field-schema-registry.md"
REGISTRY_COMMAND = "shardloom evidence-schema [surface] --format json"
SURFACE_IDS = (
    "execution_mode_selection_report",
    "compute_flow_evidence",
    "execution_certificate_report",
    "native_io_report",
    "benchmark_plan_report",
    "benchmark_constitution_report",
    "benchmark_claim_evidence_report",
    "compute_capability_matrix_report",
)


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8") if path.exists() else ""


def main() -> int:
    repo_root = Path(__file__).resolve().parents[1]
    source_path = repo_root / REGISTRY_SOURCE
    docs_path = repo_root / REGISTRY_DOC
    typed_envelope_path = repo_root / "shardloom-cli/src/typed_envelope.rs"
    client_path = repo_root / "python/src/shardloom/client.py"

    source = read_text(source_path)
    docs = read_text(docs_path)
    typed_envelope = read_text(typed_envelope_path)
    client = read_text(client_path)

    blockers: list[str] = []
    for path, label in [
        (source_path, "registry source"),
        (docs_path, "registry docs"),
        (typed_envelope_path, "typed envelope source"),
        (client_path, "Python client"),
    ]:
        if not path.exists():
            blockers.append(f"missing {label}: {path}")

    for required in [
        SCHEMA_VERSION,
        REGISTRY_COMMAND,
        "fallback_attempted=false",
        "external_engine_invoked=false",
        "Surface count: 8",
        "Field count: 297",
    ]:
        if required not in docs:
            blockers.append(f"docs missing {required}")

    for required in [
        SCHEMA_VERSION,
        "REGISTRY_REPORT_ID",
        "append_evidence_schema_registry_capability_fields",
        "typed_envelope_artifact_payload_keys",
        "must_remain_false",
        "schema_declared",
    ]:
        if required not in source:
            blockers.append(f"registry source missing {required}")

    for surface_id in SURFACE_IDS:
        if surface_id not in source:
            blockers.append(f"registry source missing surface {surface_id}")
        if surface_id not in docs:
            blockers.append(f"docs missing surface {surface_id}")
        if surface_id not in typed_envelope:
            blockers.append(f"typed envelope missing payload surface {surface_id}")

    for required in [
        "EvidenceSchemaRegistryReport",
        "def evidence_schema(",
        "dtype_for",
        "cardinality_for",
        "no_fallback_semantics_for",
    ]:
        if required not in client:
            blockers.append(f"Python client missing {required}")

    report = {
        "schema_version": "shardloom.evidence_field_schema_registry_validation.v1",
        "status": "blocked" if blockers else "passed",
        "registry_schema_version": SCHEMA_VERSION,
        "registry_source": REGISTRY_SOURCE,
        "registry_docs": REGISTRY_DOC,
        "surface_count": len(SURFACE_IDS),
        "surface_order": list(SURFACE_IDS),
        "fallback_attempted": False,
        "external_engine_invoked": False,
        "blockers": blockers,
    }
    output_path = repo_root / "target/evidence-schema-registry-report.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2, sort_keys=True))
    return 1 if blockers else 0


if __name__ == "__main__":
    raise SystemExit(main())
