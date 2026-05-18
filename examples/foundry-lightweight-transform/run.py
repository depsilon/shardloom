#!/usr/bin/env python
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


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
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    repo_root = args.repo_root.resolve()
    source_path = args.input if args.input.is_absolute() else repo_root / args.input
    output_path = args.output if args.output.is_absolute() else repo_root / args.output
    sys.path.insert(0, str(repo_root / "python" / "src"))

    from shardloom import ShardLoomClient

    client = (
        ShardLoomClient(binary=args.shardloom_bin)
        if args.shardloom_bin
        else ShardLoomClient.from_repo(repo_root)
    )
    smoke = client.smoke_check()
    capabilities = client.capabilities()
    payload = {
        "schema_version": "shardloom.examples.foundry_lightweight_transform.v1",
        "example": "foundry-lightweight-transform",
        "foundry_runtime_invoked": False,
        "foundry_compute_invoked": False,
        "foundry_spark_invoked": False,
        "foundry_output_api_invoked": False,
        "foundry_result_dataset_written": False,
        "foundry_evidence_dataset_written": False,
        "external_compute_invoked": False,
        "external_engine_invoked": False,
        "fallback_attempted": bool(smoke.fallback_attempted),
        "staged_dataset_path": str(source_path),
        "staged_dataset_path_explicit": True,
        "staged_input_execution_claimed": False,
        "output_kind": "local_certificate_smoke",
        "certificate_output_written": True,
        "output_evidence_dataset_written": False,
        "smoke_protocol_version": smoke.protocol_version,
        "resolved_cli_path": smoke.resolved_cli_path,
        "capabilities_command": capabilities.command,
        "execution_certificate_status": "not_emitted_no_dataset_smoke",
        "native_io_certificate_status": "not_emitted_no_dataset_smoke",
        "materialization_boundary_status": "staged_path_declared_only",
        "no_dataset_smoke_separate_from_generated_output": True,
        "generated_output_execution_performed": False,
        "generated_source_created": False,
        "generated_source_certificate_status": "not_emitted_report_only",
        "output_native_io_certificate_status": "not_emitted_report_only",
        "foundry_generated_output_boundary_status": "blocked_until_real_foundry_output_api_evidence",
        "claim_gate_status": "not_claim_grade",
        "future_required_evidence": [
            "Foundry package/import proof",
            "staged dataset execution proof",
            "certificate dataset output proof",
            "Native I/O certificate for staged source/sink",
        ],
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(output_path)
    return 1 if smoke.fallback_attempted else 0


if __name__ == "__main__":
    raise SystemExit(main())
