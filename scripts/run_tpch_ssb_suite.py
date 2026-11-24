# SPDX-License-Identifier: Apache-2.0
"""Multi-table TPC-H and SSB suite runner over public ShardLoom CLI workflows."""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import importlib.util
import json
import math
import os
from pathlib import Path
import platform
import statistics
import subprocess
import sys
from typing import Any

SCRIPTS_DIR = Path(__file__).resolve().parent
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from local_uat_storage import (
    GIB,
    MIB,
    add_log_budget_arguments,
    check_budgets,
    require_local_path,
)
from native_workflow_protocol import (
    extract_result,
    public_workflow_command,
    report_fields,
    strict_json,
)
from query_timing_report import query_phase_timings
from run_clickbench_query_uat import (
    PersistentResidentWorker,
    execution_report,
    file_sha256,
    run_profiled_command,
    verify_zero_answer_cache,
)
from tpch_ssb_workloads import (
    SSB_SCHEMAS,
    TPCH_SCHEMAS,
    QuerySpec,
    build_source_bindings,
    generate_ssb_tables,
    generate_tpch_tables,
    results_equivalent,
    select_queries,
    verify_dual_oracles,
)


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _file_generation(path: Path) -> dict[str, int]:
    st = path.stat()
    return {
        "device": st.st_dev,
        "inode": st.st_ino,
        "bytes": st.st_size,
        "mtime_ns": st.st_mtime_ns,
        "ctime_ns": st.st_ctime_ns,
    }


def _git_head(repo_root: Path) -> str:
    res = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=repo_root,
        capture_output=True,
        text=True,
        check=False,
    )
    return res.stdout.strip() if res.returncode == 0 and res.stdout.strip() else "unknown"


def _write_parquet_table(
    rows: list[dict[str, Any]],
    cols: list[tuple[str, str]],
    dest: Path,
    *,
    binary: Path | None = None,
    tmp_dir: Path | None = None,
    timeout: float = 120.0,
) -> None:
    if importlib.util.find_spec("pyarrow") is not None:
        import pyarrow as pa
        import pyarrow.parquet as pq

        schema = pa.schema(
            [
                pa.field(
                    name,
                    pa.int64()
                    if dt_name == "int64"
                    else (pa.float64() if dt_name == "float64" else pa.string()),
                    nullable=False,
                )
                for name, dt_name in cols
            ]
        )
        table = pa.Table.from_pylist(rows, schema=schema)
        with dest.open("xb") as fh:
            pq.write_table(table, fh)
        return
    if binary is None or tmp_dir is None:
        raise RuntimeError("pyarrow is required unless --binary is supplied for CLI JSONL->Parquet export")
    jsonl_path = tmp_dir / f"{dest.stem}.jsonl"
    with jsonl_path.open("x", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row, ensure_ascii=False, allow_nan=False) + "\n")
    schema_arg = ",".join(f"{name}:{dt_name}" for name, dt_name in cols)
    bindings = {str(jsonl_path): {"input_format": "jsonl", "source_schema": schema_arg}}
    cmd = public_workflow_command(
        binary,
        f"SELECT * FROM {dest.stem}",
        source_bindings=bindings,
        memory_gb=1,
        max_parallelism=2,
        requested_output="write_parquet",
        output=dest,
    )
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, check=False)
    if proc.returncode != 0 or not dest.is_file():
        raise RuntimeError(f"CLI parquet fixture conversion failed for {dest}: {proc.stderr}")


def _prepare_vortex_table(
    parquet_path: Path,
    vortex_path: Path,
    binary: Path,
    timeout: float,
) -> dict[str, Any]:
    cmd = [
        str(binary),
        "vortex-prepare",
        str(parquet_path),
        str(vortex_path),
        "--input-format",
        "parquet",
        "--memory-gb",
        "1",
        "--max-parallelism",
        "2",
        "--format",
        "json",
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, check=False)
    if proc.returncode != 0 or not vortex_path.is_file():
        raise RuntimeError(f"vortex-prepare failed for {parquet_path}: {proc.stderr} {proc.stdout}")
    envelope = strict_json(proc.stdout)
    fields = {entry["key"]: entry["value"] for entry in envelope.get("fields", [])}
    if (
        envelope.get("status") != "success"
        or envelope.get("fallback", {}).get("attempted") is not False
        or fields.get("vortex_ingest_performed") != "true"
        or fields.get("external_engine_invoked") != "false"
    ):
        raise RuntimeError(f"vortex-prepare lacked ingest confirmation for {vortex_path}")
    return {"bytes": vortex_path.stat().st_size, "sha256": file_sha256(vortex_path)}


def prepare_fixtures(
    fixtures_root: Path,
    suites: list[str],
    scale_rows: int,
    seed: int,
    *,
    need_vortex: bool,
    prepare_binary: Path | None,
    timeout: float,
) -> tuple[dict[str, dict[str, dict[str, Path]]], dict[str, dict[str, list[dict[str, Any]]]], dict[str, Any]]:
    fixtures_root.mkdir(parents=True, exist_ok=True)
    tmp_dir = fixtures_root / ".jsonl-tmp"
    tmp_dir.mkdir(exist_ok=True)
    raw_tables: dict[str, dict[str, list[dict[str, Any]]]] = {}
    table_paths: dict[str, dict[str, dict[str, Path]]] = {"parquet": {}, "vortex": {}}
    identities: dict[str, dict[str, Any]] = {"parquet": {}, "vortex": {}}
    manifest_path = fixtures_root / "fixtures_manifest.json"
    existing_manifest = strict_json(manifest_path.read_text()) if manifest_path.is_file() else {}
    if existing_manifest and (
        existing_manifest.get("scale_rows") != scale_rows
        or existing_manifest.get("seed") != seed
    ):
        raise ValueError(
            f"existing fixtures directory {fixtures_root} has scale/seed mismatch"
        )
    for suite_name in suites:
        suite_rows = (
            generate_tpch_tables(scale_rows, seed=seed)
            if suite_name == "tpch"
            else generate_ssb_tables(scale_rows, seed=seed)
        )
        raw_tables[suite_name] = suite_rows
        schemas = TPCH_SCHEMAS if suite_name == "tpch" else SSB_SCHEMAS
        suite_dir = fixtures_root / suite_name
        pq_dir = suite_dir / "parquet"
        vx_dir = suite_dir / "vortex"
        pq_dir.mkdir(parents=True, exist_ok=True)
        if need_vortex:
            vx_dir.mkdir(parents=True, exist_ok=True)
        table_paths["parquet"][suite_name] = {}
        table_paths["vortex"][suite_name] = {}
        identities["parquet"][suite_name] = {}
        identities["vortex"][suite_name] = {}
        for tname, cols in schemas.items():
            pq_path = pq_dir / f"{tname}.parquet"
            if not pq_path.is_file():
                _write_parquet_table(
                    suite_rows[tname],
                    cols,
                    pq_path,
                    binary=prepare_binary,
                    tmp_dir=tmp_dir,
                    timeout=timeout,
                )
            table_paths["parquet"][suite_name][tname] = pq_path
            identities["parquet"][suite_name][tname] = {
                "path": str(pq_path),
                "sha256": file_sha256(pq_path),
                "bytes": pq_path.stat().st_size,
                "generation": _file_generation(pq_path),
            }
            if need_vortex:
                vx_path = vx_dir / f"{tname}.vortex"
                if not vx_path.is_file():
                    if prepare_binary is None:
                        raise RuntimeError("--binary (or --prepare-binary) required to create Vortex fixtures")
                    _prepare_vortex_table(pq_path, vx_path, prepare_binary, timeout)
                table_paths["vortex"][suite_name][tname] = vx_path
                identities["vortex"][suite_name][tname] = {
                    "path": str(vx_path),
                    "sha256": file_sha256(vx_path),
                    "bytes": vx_path.stat().st_size,
                    "generation": _file_generation(vx_path),
                }
    manifest_payload = {
        "schema_version": "shardloom.tpch_ssb.fixtures.v1",
        "scale_rows": scale_rows,
        "seed": seed,
        "suites": suites,
        "identities": identities,
    }
    manifest_path.write_text(json.dumps(manifest_payload, indent=2) + "\n")
    return table_paths, raw_tables, identities


def _score_records(
    records: list[dict[str, Any]],
    selected_ids: list[int],
    repeats: int,
    *,
    timing_key: str,
) -> dict[str, Any]:
    runs_requested = len(selected_ids) * repeats
    runs_completed = len(records)
    passed = [r for r in records if r.get("passed")]
    failed = runs_completed - len(passed)
    timings = [r[timing_key] for r in passed if r.get(timing_key) is not None]
    complete = (
        failed == 0
        and runs_completed == runs_requested
        and len(passed) == runs_requested
        and len(timings) == runs_requested
        and all(isinstance(v, (int, float)) and math.isfinite(v) and v >= 0 for v in timings)
    )
    if timings:
        sorted_t = sorted(timings)
        return {
            "runs_requested": runs_requested,
            "runs_completed": runs_completed,
            "runs_passed": len(passed),
            "runs_failed": failed,
            "complete": complete,
            "timing_available": True,
            "total_seconds": sum(timings),
            "mean_seconds": statistics.fmean(timings),
            "median_seconds": statistics.median(sorted_t),
            "min_seconds": sorted_t[0],
            "max_seconds": sorted_t[-1],
        }
    return {
        "runs_requested": runs_requested,
        "runs_completed": runs_completed,
        "runs_passed": len(passed),
        "runs_failed": failed,
        "complete": False,
        "timing_available": False,
        "total_seconds": None,
        "mean_seconds": None,
        "median_seconds": None,
        "min_seconds": None,
        "max_seconds": None,
    }


def _verify_no_fallback_envelope(envelope: dict[str, Any], fields: dict[str, Any]) -> None:
    if envelope.get("status") != "success":
        raise ValueError(f"native status is {envelope.get('status')!r}: {envelope.get('summary')}")
    if envelope.get("fallback", {}).get("attempted") is not False:
        raise ValueError("envelope lacks fallback.attempted == false")
    if fields.get("public_workflow_fallback_attempted") != "false":
        raise ValueError("missing public_workflow_fallback_attempted=false")
    if fields.get("public_workflow_external_engine_invoked") != "false":
        raise ValueError("missing public_workflow_external_engine_invoked=false")
    verify_zero_answer_cache(fields)


def _execute_lane(
    *,
    lane_name: str,
    specs: list[QuerySpec],
    expected_map: dict[str, list[dict[str, Any]]],
    table_paths: dict[str, dict[str, Path]],
    identities: dict[str, dict[str, Any]],
    args: argparse.Namespace,
    build_commit: str,
    binary_sha256: str,
    logs: Path,
    guard,
) -> dict[str, Any]:
    logs.mkdir(parents=True, exist_ok=True)
    records: list[dict[str, Any]] = []
    selected_ids = [s.query_id for s in specs]
    worker = (
        PersistentResidentWorker(args.binary, logs / "warm_worker.stderr.txt")
        if lane_name == "warm_resident_session"
        else None
    )
    summary: dict[str, Any] = {
        "schema_version": "shardloom.tpch_ssb.multi_table.v1",
        "build_commit": build_commit,
        "binary": str(args.binary),
        "binary_sha256": binary_sha256,
        "input_format": args.input_format,
        "suite": args.suite,
        "scale_rows": args.scale_rows,
        "seed": args.seed,
        "timing_surface": lane_name,
        "timing_metric": "engine_execution_seconds",
        "memory_gb": args.memory_gb,
        "max_parallelism": args.max_parallelism,
        "repeats": args.repeats,
        "selected_query_ids": selected_ids,
        "selected_query_codes": [s.code for s in specs],
        "worker_startup_seconds": [worker.start_seconds] if worker is not None else [],
        "fallback_attempted": False,
        "external_engine_invoked": False,
        "zero_answer_cache_verified": False,
        "claim_gate_status": "not_claim_grade",
        "performance_claim_allowed": False,
        "production_claim_allowed": False,
        "platform": platform.platform(),
        "machine": platform.machine(),
        "records": records,
    }
    try:
        for spec in specs:
            suite_paths = table_paths[spec.suite]
            bindings = build_source_bindings(spec, suite_paths, args.input_format)
            for run in range(1, args.repeats + 1):
                prefix = logs / f"q{spec.query_id:02d}_{spec.code}_run{run}"
                if len(spec.tables) == 1:
                    command = public_workflow_command(
                        args.binary,
                        spec.sql,
                        input_path=suite_paths[spec.tables[0]],
                        input_format=args.input_format,
                        memory_gb=args.memory_gb,
                        max_parallelism=args.max_parallelism,
                    )
                else:
                    command = public_workflow_command(
                        args.binary,
                        spec.sql,
                        source_bindings=bindings,
                        memory_gb=args.memory_gb,
                        max_parallelism=args.max_parallelism,
                    )
                if worker is None:
                    raw = run_profiled_command(command, prefix, args.timeout, guard)
                else:
                    prefix.with_suffix(".stderr.txt").write_text("")
                    raw = worker.run_query(command, prefix.with_suffix(".stdout.json"), args.timeout, guard)
                rec: dict[str, Any] = {
                    **raw,
                    "query": spec.query_id,
                    "code": spec.code,
                    "suite": spec.suite,
                    "tables": list(spec.tables),
                    "run": run,
                    "timing_surface": lane_name,
                    "zero_answer_cache_verified": False,
                    "passed": False,
                }
                try:
                    if raw["returncode"] != 0 or raw["guard_failures"]:
                        raise ValueError(f"native command failed rc={raw['returncode']} guards={raw['guard_failures']}")
                    envelope = strict_json(prefix.with_suffix(".stdout.json").read_text())
                    fields = report_fields(envelope)
                    _verify_no_fallback_envelope(envelope, fields)
                    rec["zero_answer_cache_verified"] = True
                    rec["public_workflow_fallback_attempted"] = fields["public_workflow_fallback_attempted"]
                    rec["public_workflow_external_engine_invoked"] = fields["public_workflow_external_engine_invoked"]
                    rec["execution_report"] = execution_report(fields)
                    rec.update(query_phase_timings(fields, raw["seconds"], allow_missing=False))
                    actual = extract_result(envelope)
                    if not results_equivalent(actual, expected_map[spec.code], ordered=spec.ordered):
                        raise ValueError(
                            f"{spec.code} mismatch vs verified offline oracle: "
                            f"actual={actual[:2]} expected={expected_map[spec.code][:2]}"
                        )
                    for tname in spec.tables:
                        tpath = suite_paths[tname]
                        if _file_generation(tpath) != identities[spec.suite][tname]["generation"]:
                            raise ValueError(f"source table {tpath} changed during execution")
                    rec["result_sha256"] = hashlib.sha256(_canonical_json(actual).encode()).hexdigest()
                    rec["row_count"] = len(actual)
                    rec["passed"] = True
                except (OSError, ValueError, KeyError, TypeError) as err:
                    rec["failure"] = str(err)
                records.append(rec)
    finally:
        if worker is not None:
            worker.close()

    summary.update(_score_records(records, selected_ids, args.repeats, timing_key="engine_execution_seconds"))
    summary["secondary_process_wall"] = _score_records(records, selected_ids, args.repeats, timing_key="seconds")
    summary["zero_answer_cache_verified"] = bool(records) and all(
        r.get("zero_answer_cache_verified") for r in records
    )
    summary["full_result_validation"] = bool(summary["complete"] and summary["zero_answer_cache_verified"])
    (logs / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    return summary


def execute_suite(args: argparse.Namespace) -> tuple[dict[str, Any], Path]:
    repo_root = Path(__file__).resolve().parents[1]
    uat_root = require_local_path(args.uat_root, Path.home(), sys.platform)
    binary = args.binary.resolve(strict=True)
    prepare_binary = (args.prepare_binary or binary).resolve(strict=True)
    if args.fixtures_dir is not None:
        fixtures_root = require_local_path(args.fixtures_dir, Path.home(), sys.platform)
    else:
        stamp_fx = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
        fixtures_root = uat_root / "fixtures" / f"tpch_ssb_s{args.scale_rows}_{args.seed}_{stamp_fx}"

    stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    logs = uat_root / "logs" / f"tpch_ssb_{args.suite}_{args.input_format}_{stamp}"

    def guard(reserve_log_bytes: int = 0) -> dict[str, Any]:
        return check_budgets(
            uat_root,
            fixtures_root,
            logs,
            min_free_bytes=12 * GIB,
            reserve_bytes=reserve_log_bytes,
            max_workspace_bytes=100 * GIB,
            max_log_bytes=math.floor(args.max_log_mib * MIB),
            max_run_log_bytes=math.floor(args.max_run_log_mib * MIB),
            reserve_log_bytes=reserve_log_bytes,
        )

    storage_preflight = guard()
    uat_root.mkdir(parents=True, exist_ok=True)
    logs.mkdir(parents=True, exist_ok=False)
    specs = select_queries(args.suite, args.queries.split(",") if args.queries else None)
    suites = sorted({s.suite for s in specs})
    table_paths, raw_tables, identities = prepare_fixtures(
        fixtures_root,
        suites,
        args.scale_rows,
        args.seed,
        need_vortex=(args.input_format == "vortex"),
        prepare_binary=prepare_binary,
        timeout=args.timeout,
    )
    oracle_verified: dict[str, dict[str, Any]] = {}
    for suite_name in suites:
        suite_specs = [s for s in specs if s.suite == suite_name]
        oracle_verified.update(
            verify_dual_oracles(
                suite_name,
                suite_specs,
                raw_tables[suite_name],
                duckdb_oracle=args.duckdb_oracle,
            )
        )
    expected_map = {code: payload["expected"] for code, payload in oracle_verified.items()}
    build_commit = args.build_commit or _git_head(repo_root)
    binary_sha = file_sha256(binary)
    format_paths = table_paths[args.input_format]
    format_identities = identities[args.input_format]

    if args.lane == "paired":
        cold = _execute_lane(
            lane_name="cold_process_cli",
            specs=specs,
            expected_map=expected_map,
            table_paths=format_paths,
            identities=format_identities,
            args=args,
            build_commit=build_commit,
            binary_sha256=binary_sha,
            logs=logs / "cold_process_cli",
            guard=guard,
        )
        warm = _execute_lane(
            lane_name="warm_resident_session",
            specs=specs,
            expected_map=expected_map,
            table_paths=format_paths,
            identities=format_identities,
            args=args,
            build_commit=build_commit,
            binary_sha256=binary_sha,
            logs=logs / "warm_resident_session",
            guard=guard,
        )
        summary: dict[str, Any] = {
            "schema_version": "shardloom.tpch_ssb.multi_table.v1",
            "build_commit": build_commit,
            "binary": str(binary),
            "binary_sha256": binary_sha,
            "input_format": args.input_format,
            "suite": args.suite,
            "scale_rows": args.scale_rows,
            "seed": args.seed,
            "lane": "paired",
            "timing_surface": "paired_separate_lanes",
            "timing_metric": "engine_execution_seconds",
            "selected_query_ids": [s.query_id for s in specs],
            "selected_query_codes": [s.code for s in specs],
            "complete": bool(cold["complete"] and warm["complete"]),
            "full_result_validation": bool(cold["full_result_validation"] and warm["full_result_validation"]),
            "zero_answer_cache_verified": bool(cold["zero_answer_cache_verified"] and warm["zero_answer_cache_verified"]),
            "fallback_attempted": False,
            "external_engine_invoked": False,
            "claim_gate_status": "not_claim_grade",
            "performance_claim_allowed": False,
            "production_claim_allowed": False,
            "oracle_verification": {code: payload["oracles"] for code, payload in oracle_verified.items()},
            "storage_preflight": storage_preflight,
            "lanes": {"cold_process_cli": cold, "warm_resident_session": warm},
        }
    else:
        lane_summary = _execute_lane(
            lane_name=args.lane,
            specs=specs,
            expected_map=expected_map,
            table_paths=format_paths,
            identities=format_identities,
            args=args,
            build_commit=build_commit,
            binary_sha256=binary_sha,
            logs=logs,
            guard=guard,
        )
        lane_summary["oracle_verification"] = {
            code: payload["oracles"] for code, payload in oracle_verified.items()
        }
        lane_summary["storage_preflight"] = storage_preflight
        summary = lane_summary

    summary_path = logs / "summary.json"
    summary_path.write_text(json.dumps(summary, indent=2) + "\n")
    if args.summary_out is not None:
        args.summary_out.parent.mkdir(parents=True, exist_ok=True)
        args.summary_out.write_text(json.dumps(summary, indent=2) + "\n")
    return summary, summary_path


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--binary", required=True, type=Path)
    p.add_argument("--prepare-binary", type=Path)
    p.add_argument("--uat-root", required=True, type=Path)
    p.add_argument("--fixtures-dir", type=Path)
    p.add_argument("--suite", choices=("tpch", "ssb", "all"), default="all")
    p.add_argument("--queries", help="comma-separated query codes, e.g. TPCH-Q1,SSB-Q4.1")
    p.add_argument("--input-format", choices=("parquet", "vortex"), default="vortex")
    p.add_argument("--lane", choices=("cold_process_cli", "warm_resident_session", "paired"), default="paired")
    p.add_argument("--scale-rows", type=int, default=12000)
    p.add_argument("--seed", type=int, default=20261124)
    p.add_argument("--repeats", type=int, default=1)
    p.add_argument("--memory-gb", type=int, default=4)
    p.add_argument("--max-parallelism", type=int, default=4)
    p.add_argument("--timeout", type=float, default=120.0)
    p.add_argument("--build-commit")
    p.add_argument("--duckdb-oracle", action="store_true")
    p.add_argument("--summary-out", type=Path)
    add_log_budget_arguments(p)
    args = p.parse_args(argv)
    if args.scale_rows < 60 or args.repeats < 1 or args.memory_gb <= 0 or args.max_parallelism <= 0:
        p.error("scale-rows >= 60 and repeats/resources > 0 required")
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    summary, summary_path = execute_suite(args)
    print(json.dumps({"summary_path": str(summary_path), "complete": summary["complete"], "input_format": summary["input_format"]}))
    return 0 if summary["complete"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
