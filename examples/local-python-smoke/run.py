#!/usr/bin/env python
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Sequence


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run ShardLoom's local Python smoke.")
    parser.add_argument("--repo-root", type=Path, default=Path.cwd())
    parser.add_argument("--shardloom-bin")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    repo_root = args.repo_root.resolve()
    sys.path.insert(0, str(repo_root / "python" / "src"))

    import shardloom as sl
    from shardloom import ShardLoomClient

    client = (
        ShardLoomClient(binary=args.shardloom_bin)
        if args.shardloom_bin
        else ShardLoomClient.from_repo(repo_root)
    )
    ctx = sl.context(client=client)
    status = client.status()
    smoke = client.smoke_check()
    capabilities = client.capabilities()

    quickstart_dir = repo_root / "target" / "local-python-smoke"
    quickstart_dir.mkdir(parents=True, exist_ok=True)
    source_path = quickstart_dir / "orders.csv"
    output_path = quickstart_dir / "orders-out.jsonl"
    generated_output_path = quickstart_dir / "generated-reference.jsonl"
    source_path.write_text(
        "id,label,amount\n"
        "1,alpha,8\n"
        "2,beta,15\n"
        "3,gamma,27\n",
        encoding="utf-8",
    )

    workflow = (
        ctx.read(source_path)
        .filter(sl.col("amount") >= 10)
        .select("id", "label", "amount")
        .limit(2)
    )
    result = workflow.write_jsonl(output_path, allow_overwrite=True)
    generated = (
        ctx.from_rows([{"id": 1, "label": "alpha"}])
        .with_column("batch_id", 1)
        .write_jsonl(generated_output_path, allow_overwrite=True)
    )
    unsupported = workflow.to_pandas()
    first_row = result.first_result_row or {}
    result_evidence = result.evidence_summary
    result_claim = result.claim_summary
    generated_evidence = generated.evidence_summary
    generated_claim = generated.claim_summary

    print(f"status: {status.status}")
    print(f"protocol: {smoke.protocol_version}")
    print(f"cli: {smoke.resolved_cli_path}")
    print(f"capabilities command: {capabilities.command}")
    print(f"fallback attempted: {smoke.fallback_attempted}")
    print("quickstart_user_surface_status=passed")
    print(f"quickstart_result_row_id={first_row.get('id')}")
    print(f"quickstart_output_row_count={result.output_row_count}")
    print(f"quickstart_output_path={result.output_path}")
    print(f"quickstart_evidence_output_row_count={result_evidence.output_row_count}")
    print(f"quickstart_evidence_output_path={result_evidence.output_path}")
    print(
        "quickstart_evidence_fallback_attempted="
        f"{str(result_evidence.fallback_attempted).lower()}"
    )
    print(
        "quickstart_evidence_external_engine_invoked="
        f"{str(result_evidence.external_engine_invoked).lower()}"
    )
    print(f"quickstart_claim_gate_status={result_claim.claim_gate_status}")
    print(f"quickstart_generated_source_kind={generated.generated_source_kind}")
    print(f"quickstart_generated_source_row_count={generated.generated_source_row_count}")
    print(
        "quickstart_generated_evidence_output_row_count="
        f"{generated_evidence.output_row_count}"
    )
    print(
        "quickstart_generated_evidence_fallback_attempted="
        f"{str(generated_evidence.fallback_attempted).lower()}"
    )
    print(
        "quickstart_generated_evidence_external_engine_invoked="
        f"{str(generated_evidence.external_engine_invoked).lower()}"
    )
    print(f"quickstart_generated_claim_gate_status={generated_claim.claim_gate_status}")
    print(f"quickstart_unsupported_blocker_id={unsupported.blocker_id}")
    print(
        "quickstart_unsupported_runtime_execution="
        f"{str(unsupported.runtime_execution).lower()}"
    )
    print(f"quickstart_unsupported_data_read={str(unsupported.data_read).lower()}")
    print(f"quickstart_unsupported_write_io={str(unsupported.write_io).lower()}")
    print(
        "quickstart_unsupported_fallback_attempted="
        f"{str(unsupported.fallback_attempted).lower()}"
    )
    print(
        "quickstart_unsupported_external_engine_invoked="
        f"{str(unsupported.external_engine_invoked).lower()}"
    )
    failed = (
        smoke.fallback_attempted
        or result.fallback_attempted
        or result.external_engine_invoked
        or generated.fallback_attempted
        or generated.external_engine_invoked
        or unsupported.fallback_attempted
        or unsupported.external_engine_invoked
        or unsupported.runtime_execution
        or unsupported.data_read
        or unsupported.write_io
        or result.output_row_count <= 0
        or first_row.get("id") is None
        or generated.generated_source_row_count <= 0
        or unsupported.blocker_id is None
    )
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
