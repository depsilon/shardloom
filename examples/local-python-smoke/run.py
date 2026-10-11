#!/usr/bin/env python
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import argparse
import json
import sys
import tempfile
from pathlib import Path
from typing import Sequence


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run ShardLoom's local Python smoke.")
    parser.add_argument("--repo-root", type=Path, default=Path.cwd())
    parser.add_argument("--shardloom-bin")
    parser.add_argument("--memory-gb", type=int, required=True, help="Caller allocation in GiB.")
    parser.add_argument("--max-parallelism", type=int, required=True, help="Caller execution-lane ceiling.")
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
    ctx = sl.context(client=client, memory_gb=args.memory_gb, max_parallelism=args.max_parallelism)
    status = client.status()
    smoke = client.smoke_check()
    capabilities = client.capabilities()

    quickstart_root = repo_root / "target" / "local-python-smoke"
    quickstart_root.mkdir(parents=True, exist_ok=True)
    quickstart_dir = Path(tempfile.mkdtemp(prefix="run-", dir=quickstart_root))
    source_path = quickstart_dir / "orders.csv"
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
    local_file = workflow.collect(check=True)

    generated = (
        ctx.from_rows([{"id": 1, "label": "alpha"}])
        .with_column("batch_id", 1)
        .write_jsonl(generated_output_path)
    )
    expected_generated_rows = [{"id": 1, "label": "alpha", "batch_id": 1}]
    generated_result_verified = False
    if generated.output_commit_status == "committed":
        try:
            committed_output_path = Path(generated.output_path)
            generated_output_rows = [
                json.loads(line)
                for line in committed_output_path.read_text(encoding="utf-8").splitlines()
                if line.strip()
            ]
        except (OSError, json.JSONDecodeError):
            pass
        else:
            generated_result_verified = generated_output_rows == expected_generated_rows
    unsupported = ctx.read(source_path).select("id").apply("row_udf")
    expected_local_file_rows = (
        {"id": 2, "label": "beta", "amount": 15},
        {"id": 3, "label": "gamma", "amount": 27},
    )
    local_file_result_rows = tuple(local_file.result_rows)
    local_file_blocker_id = getattr(local_file, "blocker_id", None)
    local_file_runtime_execution = bool(getattr(local_file, "runtime_execution", False))
    local_file_fallback_attempted = bool(getattr(local_file, "fallback_attempted", False))
    local_file_external_engine_invoked = bool(
        getattr(local_file, "external_engine_invoked", False)
    )
    local_file_native_plan_family = local_file.envelope.field(
        "public_workflow_native_vortex_plan_route_family"
    )
    local_file_source_opens = local_file.envelope.field_int("resident_source_opens")
    generated_native_plan_family = generated.envelope.field(
        "public_workflow_native_vortex_plan_route_family"
    )
    generated_source_opens = generated.envelope.field_int("resident_source_opens")
    local_file_route_passed = (
        local_file_blocker_id is None
        and local_file_runtime_execution
        and local_file_native_plan_family == "native_vortex_unified_plan"
        and local_file_source_opens == 1
        and not local_file_fallback_attempted
        and not local_file_external_engine_invoked
        and local_file.output_row_count == 2
        and local_file.result_columns == ("id", "label", "amount")
        and local_file_result_rows == expected_local_file_rows
    )
    failed = (
        smoke.fallback_attempted
        or generated.fallback_attempted
        or generated.external_engine_invoked
        or generated_native_plan_family != "native_vortex_unified_plan"
        or generated_source_opens != 0
        or not local_file_route_passed
        or unsupported.fallback_attempted
        or unsupported.external_engine_invoked
        or unsupported.runtime_execution
        or unsupported.data_read
        or unsupported.write_io
        or generated.rows_written != 1
        or generated.output_row_count != 1
        or generated.output_commit_status != "committed"
        or not generated_result_verified
        or unsupported.blocker_id is None
    )

    print(f"status: {status.status}")
    print(f"protocol: {smoke.protocol_version}")
    print(f"cli: {smoke.resolved_cli_path}")
    print(f"capabilities command: {capabilities.command}")
    print(f"fallback attempted: {smoke.fallback_attempted}")
    print("quickstart_user_surface_status=failed" if failed else "quickstart_user_surface_status=passed")
    print(f"quickstart_local_file_blocker_id={local_file_blocker_id or 'none'}")
    print("quickstart_local_file_route_status=passed" if local_file_route_passed
          else "quickstart_local_file_route_status=failed")
    print(
        "quickstart_local_file_runtime_execution="
        f"{str(local_file_runtime_execution).lower()}"
    )
    print(
        "quickstart_local_file_native_plan_family="
        f"{local_file_native_plan_family}"
    )
    print(f"quickstart_local_file_source_opens={local_file_source_opens}")
    print(f"quickstart_local_file_output_row_count={local_file.output_row_count}")
    print(f"quickstart_local_file_result_rows={local_file_result_rows}")
    print(
        "quickstart_local_file_fallback_attempted="
        f"{str(local_file_fallback_attempted).lower()}"
    )
    print(
        "quickstart_local_file_external_engine_invoked="
        f"{str(local_file_external_engine_invoked).lower()}"
    )
    print("quickstart_generated_input_row_count=1")
    print(f"quickstart_generated_native_plan_family={generated_native_plan_family}")
    print(f"quickstart_generated_source_opens={generated_source_opens}")
    print(
        "quickstart_generated_result_verified="
        f"{str(generated_result_verified).lower()}"
    )
    print(f"quickstart_generated_output_path={generated.output_path}")
    print(f"quickstart_generated_rows_written={generated.rows_written}")
    print(
        "quickstart_generated_output_row_count="
        f"{generated.output_row_count}"
    )
    print(
        "quickstart_generated_output_commit_status="
        f"{generated.output_commit_status}"
    )
    print(
        "quickstart_generated_fallback_attempted="
        f"{str(generated.fallback_attempted).lower()}"
    )
    print(
        "quickstart_generated_external_engine_invoked="
        f"{str(generated.external_engine_invoked).lower()}"
    )
    print(f"quickstart_generated_claim_gate_status={generated.claim_gate_status}")
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
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
