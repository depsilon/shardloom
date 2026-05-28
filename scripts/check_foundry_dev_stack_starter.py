#!/usr/bin/env python
# SPDX-License-Identifier: Apache-2.0
"""Validate the local Foundry dev-stack starter contract."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_VERSION = "shardloom.foundry_dev_stack_starter_kit.v1"
REPORT_SCHEMA_VERSION = "shardloom.foundry_dev_stack_starter_kit_report.v1"

REQUIRED_FALSE_FIELDS = [
    "real_foundry_runtime_supported",
    "foundry_runtime_invoked",
    "foundry_compute_invoked",
    "foundry_spark_invoked",
    "foundry_output_api_invoked",
    "foundry_result_dataset_written",
    "foundry_evidence_dataset_written",
    "direct_s3_read_invoked",
    "direct_s3_write_invoked",
    "object_store_read_invoked",
    "object_store_write_invoked",
    "object_store_commit_invoked",
    "credential_resolution_performed",
    "network_probe_performed",
    "external_compute_invoked",
    "external_engine_invoked",
    "fallback_attempted",
    "public_foundry_claim_allowed",
    "foundry_marketplace_claim_allowed",
]

REQUIRED_TRUE_FIELDS = [
    "starter_runtime_supported",
    "foundry_style_output_api_invoked",
    "foundry_style_result_dataset_written",
    "foundry_style_evidence_dataset_written",
    "output_evidence_dataset_written",
]

EXPECTED_COMMAND_IDS = [
    "build_cli",
    "run_foundry_style_transform",
    "run_foundry_proof_of_use",
]

REQUIRED_DOC_SNIPPETS = [
    "shardloom.foundry_dev_stack_starter_kit.v1",
    "python scripts\\check_foundry_dev_stack_starter.py",
    "cargo build -p shardloom-cli --bin shardloom",
    "python examples\\foundry-lightweight-transform\\run.py --repo-root .",
    "python scripts\\foundry_proof_of_use.py --rows 64 --iterations 1",
    "no_dataset_smoke_separate_from_generated_output=true",
    "generated_output_execution_performed=true",
    "staged_input_transform_execution_performed=true",
    "foundry_style_output_api_invoked=true",
    "foundry_style_result_dataset_written=true",
    "foundry_style_evidence_dataset_written=true",
    "output_evidence_dataset_written=true",
    "foundry_runtime_invoked=false",
    "foundry_compute_invoked=false",
    "foundry_spark_invoked=false",
    "fallback_attempted=false",
    "external_engine_invoked=false",
    "local_foundry_style_generated_output_and_staged_transform_smoke_only",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=ROOT)
    parser.add_argument(
        "--manifest",
        type=Path,
        default=Path("docs/foundry/dev-stack-starter-kit.json"),
    )
    parser.add_argument(
        "--doc",
        type=Path,
        default=Path("docs/foundry/dev-stack-starter-kit.md"),
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("target/foundry-dev-stack-starter-kit-report.json"),
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
        return ["missing Foundry dev-stack starter manifest"]

    if manifest.get("schema_version") != SCHEMA_VERSION:
        blockers.append(f"schema_version={manifest.get('schema_version')}")
    if manifest.get("gar_id") != "GAR-COMMERCIAL-1E":
        blockers.append(f"gar_id={manifest.get('gar_id')}")
    if manifest.get("status") != "local_style_runtime_proof":
        blockers.append(f"status={manifest.get('status')}")
    if manifest.get("claim_gate_status") != "not_claim_grade":
        blockers.append(f"claim_gate_status={manifest.get('claim_gate_status')}")
    for field in REQUIRED_FALSE_FIELDS:
        if manifest.get(field) is not False:
            blockers.append(f"{field} must be false")
    for field in REQUIRED_TRUE_FIELDS:
        if manifest.get(field) is not True:
            blockers.append(f"{field} must be true")
    for field in ["claim_boundary", "fallback_boundary"]:
        if not _non_empty_string(manifest.get(field)):
            blockers.append(f"missing {field}")

    commands = manifest.get("proof_commands")
    if not isinstance(commands, list):
        blockers.append("proof_commands must be a list")
    else:
        seen = [row.get("command_id") for row in commands if isinstance(row, dict)]
        if seen != EXPECTED_COMMAND_IDS:
            blockers.append(f"proof command ids mismatch: {seen}")
        for row in commands:
            if not isinstance(row, dict):
                blockers.append("proof command rows must be objects")
                continue
            for field in ["command_id", "command", "purpose"]:
                if not _non_empty_string(row.get(field)):
                    blockers.append(f"{row.get('command_id', '<missing>')}: missing {field}")

    outputs = manifest.get("local_outputs")
    if not isinstance(outputs, list) or len(outputs) < 2:
        blockers.append("local_outputs must list starter artifacts")
    else:
        for required in [
            "target/foundry-lightweight-transform/certificate-output.json",
            "target/foundry-lightweight-transform/generated-output.jsonl",
            "target/foundry-lightweight-transform/staged-transform-output.jsonl",
            "target/foundry-lightweight-transform/result-dataset",
            "target/foundry-lightweight-transform/evidence-dataset",
            "target/foundry-proof-of-use/report.json",
            "target/foundry-proof-of-use/result-dataset",
            "target/foundry-proof-of-use/evidence-dataset",
        ]:
            if required not in outputs:
                blockers.append(f"missing local output {required}")

    generated = manifest.get("source_free_generated_output_posture")
    if not isinstance(generated, dict):
        blockers.append("source_free_generated_output_posture must be an object")
    else:
        expected = {
            "no_dataset_smoke_separate_from_generated_output": True,
            "generated_output_execution_performed": True,
            "generated_source_created": True,
            "generated_source_certificate_status": "present",
            "output_native_io_certificate_status": "certified_local_file_sink",
            "foundry_output_api_required": True,
            "foundry_style_output_api_invoked": True,
            "claim_gate_status": "fixture_smoke_only",
        }
        for field, value in expected.items():
            if generated.get(field) != value:
                blockers.append(f"source_free_generated_output_posture.{field}={generated.get(field)}")

    staged = manifest.get("staged_input_example")
    if not isinstance(staged, dict):
        blockers.append("staged_input_example must be an object")
    else:
        if staged.get("path") != "examples/foundry-lightweight-transform/fixtures/staged_input.csv":
            blockers.append("staged input path mismatch")
        if staged.get("staged_dataset_path_explicit") is not True:
            blockers.append("staged_dataset_path_explicit must be true")
        if staged.get("staged_input_execution_claimed") is not True:
            blockers.append("staged_input_execution_claimed must be true")
        if staged.get("staged_input_transform_execution_performed") is not True:
            blockers.append("staged_input_transform_execution_performed must be true")

    boundary = manifest.get("evidence_dataset_output_boundary")
    if not isinstance(boundary, dict):
        blockers.append("evidence_dataset_output_boundary must be an object")
    else:
        if boundary.get("local_certificate_json_written") is not True:
            blockers.append("local_certificate_json_written must be true")
        for field in [
            "foundry_style_evidence_dataset_written",
            "foundry_style_result_dataset_written",
            "output_evidence_dataset_written",
        ]:
            if boundary.get(field) is not True:
                blockers.append(f"evidence_dataset_output_boundary.{field} must be true")
        for field in [
            "foundry_evidence_dataset_written",
            "foundry_result_dataset_written",
        ]:
            if boundary.get(field) is not False:
                blockers.append(f"evidence_dataset_output_boundary.{field} must be false")
        if boundary.get("deterministic_blocker") != "blocked_until_real_foundry_output_api_evidence":
            blockers.append("missing Foundry output API deterministic blocker")

    refs = manifest.get("reference_files")
    if not isinstance(refs, list) or not refs:
        blockers.append("reference_files must be a non-empty list")
    else:
        for required in [
            "docs/foundry/dev-stack-starter-kit.md",
            "docs/foundry/proof-of-use-certification.md",
            "examples/foundry-lightweight-transform/run.py",
            "scripts/foundry_proof_of_use.py",
        ]:
            if required not in refs:
                blockers.append(f"missing reference {required}")

    return blockers


def validate_doc(doc_text: str) -> list[str]:
    blockers: list[str] = []
    if not doc_text:
        return ["missing Foundry dev-stack starter documentation"]
    for required in REQUIRED_DOC_SNIPPETS:
        if required not in doc_text:
            blockers.append(f"doc missing {required}")
    return blockers


def validate_example_files(repo_root: Path) -> list[str]:
    blockers: list[str] = []
    run_py = (repo_root / "examples/foundry-lightweight-transform/run.py").read_text(
        encoding="utf-8"
    )
    expected = (repo_root / "examples/foundry-lightweight-transform/expected-output.json").read_text(
        encoding="utf-8"
    )
    certificate = (
        repo_root / "examples/foundry-lightweight-transform/expected-certificate-fields.json"
    ).read_text(encoding="utf-8")
    limitations = (repo_root / "examples/foundry-lightweight-transform/known-limitations.md").read_text(
        encoding="utf-8"
    )
    for field in [
        "foundry_spark_invoked",
        "foundry_output_api_invoked",
        "foundry_result_dataset_written",
        "foundry_evidence_dataset_written",
        "foundry_style_output_api_invoked",
        "foundry_style_result_dataset_written",
        "foundry_style_evidence_dataset_written",
        "generated_output_execution_performed",
        "staged_input_transform_execution_performed",
        "output_evidence_dataset_written",
        "no_dataset_smoke_separate_from_generated_output",
        "generated_source_certificate_status",
        "external_engine_invoked",
        "claim_gate_status",
    ]:
        if field not in run_py:
            blockers.append(f"run.py missing {field}")
        if field not in expected and field not in certificate:
            blockers.append(f"expected outputs missing {field}")
    for required in [
        "does not run in Foundry",
        "does not invoke real Foundry output APIs",
        "does not use Foundry Spark",
        "does not write direct S3/object-store outputs",
    ]:
        if required not in limitations:
            blockers.append(f"known limitations missing {required}")
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
    blockers.extend(validate_example_files(repo_root))
    report = {
        "schema_version": REPORT_SCHEMA_VERSION,
        "manifest_ref": str(args.manifest).replace("\\", "/"),
        "doc_ref": str(args.doc).replace("\\", "/"),
        "status": "passed" if not blockers else "failed",
        "claim_gate_status": (manifest or {}).get("claim_gate_status", "missing"),
        "foundry_runtime_invoked": (manifest or {}).get("foundry_runtime_invoked", False),
        "foundry_compute_invoked": (manifest or {}).get("foundry_compute_invoked", False),
        "foundry_spark_invoked": (manifest or {}).get("foundry_spark_invoked", False),
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
