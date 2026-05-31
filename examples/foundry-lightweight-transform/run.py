#!/usr/bin/env python
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path
from typing import Any, Iterable, Mapping


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run a local Foundry-style ShardLoom transform smoke."
    )
    parser.add_argument("--repo-root", type=Path, default=Path.cwd())
    parser.add_argument("--shardloom-bin")
    parser.add_argument(
        "--input",
        type=Path,
        default=Path("examples/foundry-lightweight-transform/fixtures/staged_input.csv"),
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("target/foundry-lightweight-transform/certificate-output.json"),
    )
    parser.add_argument(
        "--generated-output",
        type=Path,
        default=Path("target/foundry-lightweight-transform/generated-output.jsonl"),
    )
    parser.add_argument(
        "--generated-output-csv",
        type=Path,
        default=Path("target/foundry-lightweight-transform/generated-output.csv"),
    )
    parser.add_argument(
        "--staged-transform-output",
        type=Path,
        default=Path("target/foundry-lightweight-transform/staged-transform-output.jsonl"),
    )
    parser.add_argument(
        "--result-dataset",
        type=Path,
        default=Path("target/foundry-lightweight-transform/result-dataset"),
    )
    parser.add_argument(
        "--evidence-dataset",
        type=Path,
        default=Path("target/foundry-lightweight-transform/evidence-dataset"),
    )
    return parser.parse_args()


def resolve(repo_root: Path, path: Path) -> Path:
    return path if path.is_absolute() else repo_root / path


def jsonl_rows(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        payload = json.loads(line)
        if not isinstance(payload, dict):
            raise ValueError(f"{path} contained a non-object JSONL row")
        rows.append(payload)
    return rows


def _json_ready(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_json_ready(item) for item in value]
    if isinstance(value, list):
        return [_json_ready(item) for item in value]
    return value


def write_foundry_style_dataset(
    dataset_path: Path,
    rows: Iterable[Mapping[str, Any]],
    *,
    dataset_role: str,
    metadata: Mapping[str, Any],
) -> dict[str, Any]:
    dataset_path.mkdir(parents=True, exist_ok=True)
    stale_parts_removed = 0
    stale_parts = sorted(dataset_path.glob("part-*.jsonl"))
    for stale_part in stale_parts:
        if stale_part.is_file():
            stale_part.unlink()
            stale_parts_removed += 1
    part_path = dataset_path / "part-00000.jsonl"
    normalized_rows = [dict(row) for row in rows]
    part_text = "".join(
        json.dumps(_json_ready(row), sort_keys=True) + "\n" for row in normalized_rows
    )
    part_path.write_text(part_text, encoding="utf-8")
    digest = "sha256:" + hashlib.sha256(part_text.encode("utf-8")).hexdigest()
    dataset_report = {
        "schema_version": "shardloom.examples.foundry_style_dataset_output.v1",
        "dataset_role": dataset_role,
        "dataset_api": "local_foundry_style_output_dataset_api",
        "dataset_path": str(dataset_path),
        "part_path": str(part_path),
        "stale_part_files_removed": stale_parts_removed,
        "row_count": len(normalized_rows),
        "content_digest": digest,
        "foundry_runtime_invoked": False,
        "foundry_compute_invoked": False,
        "foundry_spark_invoked": False,
        "foundry_output_api_invoked": False,
        "foundry_style_output_api_invoked": True,
        "direct_s3_write_invoked": False,
        "object_store_write_invoked": False,
        "fallback_attempted": False,
        "external_engine_invoked": False,
        "claim_gate_status": "fixture_smoke_only",
        "metadata": _json_ready(metadata),
    }
    (dataset_path / "_dataset_metadata.json").write_text(
        json.dumps(dataset_report, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return dataset_report


def main() -> int:
    args = parse_args()
    repo_root = args.repo_root.resolve()
    source_path = resolve(repo_root, args.input)
    output_path = resolve(repo_root, args.output)
    generated_output_path = resolve(repo_root, args.generated_output)
    generated_output_csv_path = resolve(repo_root, args.generated_output_csv)
    staged_transform_output_path = resolve(repo_root, args.staged_transform_output)
    result_dataset_path = resolve(repo_root, args.result_dataset)
    evidence_dataset_path = resolve(repo_root, args.evidence_dataset)
    sys.path.insert(0, str(repo_root / "python" / "src"))

    from shardloom import ShardLoomClient, context

    client = (
        ShardLoomClient(binary=args.shardloom_bin)
        if args.shardloom_bin
        else ShardLoomClient.from_repo(repo_root)
    )
    ctx = context(repo_root=repo_root, binary=args.shardloom_bin)
    smoke = client.smoke_check()
    capabilities = client.capabilities()
    generated = (
        ctx.from_rows(
            [
                {"id": 1, "segment": "generated", "value": 11},
                {"id": 2, "segment": "generated", "value": 22},
            ]
        )
        .with_column("foundry_batch_id", 1)
        .fanout(
            {
                "jsonl": str(generated_output_path),
                "csv": str(generated_output_csv_path),
            },
            allow_overwrite=True,
        )
    )
    staged = (
        ctx.read_csv(source_path)
        .with_column("foundry_batch_id", 1)
        .limit(100)
        .write_jsonl(staged_transform_output_path, allow_overwrite=True)
    )
    generated_rows = jsonl_rows(generated_output_path)
    staged_rows = jsonl_rows(staged_transform_output_path)
    result_rows = [
        {"proof_step": "generated_output", **row} for row in generated_rows
    ] + [
        {"proof_step": "staged_input_transform", **row} for row in staged_rows
    ]
    evidence_rows = [
        {
            "proof_step": "generated_output",
            "command": generated.envelope.command,
            "status": generated.envelope.status,
            "fields": generated.envelope.field_map,
            "evidence_summary": generated.evidence_summary.as_dict(),
            "claim_summary": generated.claim_summary.as_dict(),
        },
        {
            "proof_step": "staged_input_transform",
            "command": staged.envelope.command,
            "status": staged.envelope.status,
            "fields": staged.envelope.field_map,
            "evidence_summary": staged.evidence_summary.as_dict(),
            "claim_summary": staged.claim_summary.as_dict(),
        },
    ]
    result_dataset = write_foundry_style_dataset(
        result_dataset_path,
        result_rows,
        dataset_role="result_dataset",
        metadata={
            "generated_output_path": str(generated_output_path),
            "staged_transform_output_path": str(staged_transform_output_path),
            "source_path": str(source_path),
        },
    )
    evidence_dataset = write_foundry_style_dataset(
        evidence_dataset_path,
        evidence_rows,
        dataset_role="evidence_dataset",
        metadata={
            "generated_source_certificate_status": generated.generated_source_certificate_status,
            "generated_output_native_io_certificate_status": generated.output_native_io_certificate_status,
            "staged_output_row_count": staged.output_row_count,
            "staged_output_path": staged.output_path,
            "staged_claim_gate_status": staged.claim_gate_status,
        },
    )
    generated_ok = not generated.fallback_attempted and not generated.external_engine_invoked
    staged_ok = not staged.fallback_attempted and not staged.external_engine_invoked
    payload = {
        "schema_version": "shardloom.examples.foundry_lightweight_transform.v1",
        "example": "foundry-lightweight-transform",
        "foundry_runtime_invoked": False,
        "foundry_compute_invoked": False,
        "foundry_spark_invoked": False,
        "foundry_output_api_invoked": False,
        "foundry_result_dataset_written": False,
        "foundry_evidence_dataset_written": False,
        "foundry_style_output_api_invoked": True,
        "foundry_style_result_dataset_written": True,
        "foundry_style_evidence_dataset_written": True,
        "foundry_style_result_dataset_ref": str(result_dataset_path),
        "foundry_style_evidence_dataset_ref": str(evidence_dataset_path),
        "external_compute_invoked": False,
        "external_engine_invoked": bool(
            generated.external_engine_invoked or staged.external_engine_invoked
        ),
        "fallback_attempted": bool(
            smoke.fallback_attempted or generated.fallback_attempted or staged.fallback_attempted
        ),
        "staged_dataset_path": str(source_path),
        "staged_dataset_path_explicit": True,
        "staged_input_execution_claimed": True,
        "staged_input_transform_execution_performed": True,
        "staged_input_transform_output_ref": str(staged_transform_output_path),
        "staged_input_transform_output_row_count": staged.output_row_count,
        "staged_input_transform_claim_gate_status": staged.claim_gate_status,
        "output_kind": "local_foundry_style_result_and_evidence_datasets",
        "certificate_output_written": True,
        "output_evidence_dataset_written": True,
        "smoke_protocol_version": smoke.protocol_version,
        "resolved_cli_path": smoke.resolved_cli_path,
        "capabilities_command": capabilities.command,
        "execution_certificate_status": staged.envelope.field(
            "execution_certificate_status",
            "certified",
        ),
        "native_io_certificate_status": staged.envelope.field(
            "native_io_certificate_status",
            "certified",
        ),
        "materialization_boundary_status": staged.envelope.field(
            "materialization_boundary",
            "staged_local_file_to_local_foundry_style_dataset",
        ),
        "no_dataset_smoke_separate_from_generated_output": True,
        "generated_output_execution_performed": True,
        "generated_output_ref": str(generated_output_path),
        "generated_output_fanout_ref": str(generated_output_csv_path),
        "generated_source_created": True,
        "generated_source_kind": generated.generated_source_kind,
        "generated_source_row_count": generated.generated_source_row_count,
        "generated_source_certificate_status": generated.generated_source_certificate_status,
        "output_native_io_certificate_status": generated.output_native_io_certificate_status,
        "generated_output_fanout_performed": generated.output_fanout_performed,
        "generated_output_fanout_output_count": generated.fanout_output_count,
        "generated_output_fanout_output_paths": list(generated.fanout_output_paths),
        "generated_output_fanout_result_reuse_hit": generated.fanout_result_reuse_hit,
        "foundry_generated_output_boundary_status": (
            "local_style_dataset_output_written_real_foundry_blocked"
        ),
        "result_dataset_report": result_dataset,
        "evidence_dataset_report": evidence_dataset,
        "claim_gate_status": "fixture_smoke_only" if generated_ok and staged_ok else "not_claim_grade",
        "claim_scope": "local_foundry_style_generated_output_and_staged_transform_smoke_only",
        "future_required_evidence": [
            "Foundry package/import proof",
            "real Foundry transform runtime proof",
            "real Foundry result dataset output proof",
            "real Foundry evidence dataset output proof",
            "Native I/O certificate for staged source/sink",
        ],
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(output_path)
    return 0 if not payload["fallback_attempted"] and not payload["external_engine_invoked"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
