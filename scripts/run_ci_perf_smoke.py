#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Bounded deterministic CI performance-smoke and E2E-FIX-01 parity harness.

Runs against small freshly generated fixtures under a verified non-synced directory.
Produces machine-readable timings and exact independent Python oracle checks; never
attempts external engine execution or claim-grade superiority assertions.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
from pathlib import Path
import sys
import tempfile
import time
from typing import Any, Callable

from local_uat_storage import require_local_path
from native_workflow_protocol import public_workflow_command, report_fields, strict_json
from run_clickbench_query_uat import equivalent, extract_result, run_profiled_command


SCHEMA_VERSION = "shardloom.ci_perf_smoke_report.v1"


def build_smoke_rows(rows: int) -> list[dict[str, Any]]:
    if rows < 8:
        raise ValueError("CI perf-smoke fixture requires at least 8 rows")
    # Include > 2^53 cancellation pairs alongside ordinary negative, null, and skewed rows.
    special = [
        {"id": 0, "category": "alpha", "wide_val": (1 << 62) + 7, "score": 10},
        {"id": 1, "category": "alpha", "wide_val": -(1 << 62), "score": 20},
        {"id": 2, "category": "beta", "wide_val": (1 << 54) + 5, "score": None},
        {"id": 3, "category": "beta", "wide_val": -(1 << 54), "score": 30},
    ]
    generated = list(special)
    for index in range(len(special), rows):
        generated.append({
            "id": index,
            "category": "alpha" if index % 4 != 0 else "gamma",
            "wide_val": None if index % 7 == 0 else ((index % 17) - 5),
            "score": (index * 3) % 19,
        })
    return generated


def write_smoke_csv(target: Path, rows: list[dict[str, Any]]) -> None:
    with target.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=["id", "category", "wide_val", "score"])
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def smoke_queries(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    alpha_wide = [r["wide_val"] for r in rows if r["category"] == "alpha" and r["wide_val"] is not None]
    beta_vals = [r["wide_val"] for r in rows if r["category"] == "beta" and r["wide_val"] is not None]
    grouped: dict[str, tuple[int, int]] = {}
    for row in rows:
        cat = row["category"]
        cnt, total = grouped.get(cat, (0, 0))
        grouped[cat] = (cnt + 1, total + (row["score"] or 0))
    grouped_rows = sorted(
        [{"category": k, "cnt": c, "score_sum": float(s)} for k, (c, s) in grouped.items()],
        key=lambda item: (-item["cnt"], item["category"]),
    )
    return [
        {
            "name": "wide_i64_cancellation_alpha",
            "sql": "SELECT SUM(wide_val) AS sum_wide, AVG(wide_val) AS avg_wide FROM hits WHERE category = 'alpha'",
            "expected": [{
                "sum_wide": float(sum(alpha_wide)),
                "avg_wide": float(sum(alpha_wide)) / len(alpha_wide),
            }],
        },
        {
            "name": "filtered_beta_wide_count",
            "sql": "SELECT SUM(wide_val) AS sum_beta, COUNT(*) AS cnt_beta FROM hits WHERE category = 'beta'",
            "expected": [{"sum_beta": float(sum(beta_vals)), "cnt_beta": sum(r["category"] == "beta" for r in rows)}],
        },
        {
            "name": "grouped_skew_topk",
            "sql": "SELECT category, COUNT(*) AS cnt, SUM(score) AS score_sum FROM hits GROUP BY category ORDER BY cnt DESC, category ASC",
            "expected": grouped_rows,
        },
    ]


def run_smoke_suite(
    *,
    binary: Path | None,
    work_dir: Path,
    rows: int = 256,
    max_wall_seconds: float = 30.0,
    max_peak_rss_bytes: int = 1024 * 1024 * 1024,
    executor: Callable[[list[str], Path], dict[str, Any]] | None = None,
) -> dict[str, Any]:
    checked_dir = require_local_path(work_dir, Path.home(), sys.platform)
    fixture_rows = build_smoke_rows(rows)
    fixture_path = checked_dir / "smoke_fixture.csv"
    write_smoke_csv(fixture_path, fixture_rows)
    cases = smoke_queries(fixture_rows)

    results = []
    passed = True
    failures: list[str] = []
    for index, case in enumerate(cases, 1):
        prefix = checked_dir / f"smoke_{index:02d}"
        command = public_workflow_command(
            binary or Path("shardloom-dry-run"),
            case["sql"],
            input_path=fixture_path,
            input_format="csv",
            memory_gb=1,
            max_parallelism=2,
        )
        if binary is None and executor is None:
            results.append({
                "name": case["name"],
                "mode": "oracle_only",
                "passed": True,
                "expected": case["expected"],
            })
            continue

        t0 = time.perf_counter()
        exec_res = (
            executor(command, prefix)
            if executor is not None
            else run_profiled_command(command, prefix, max_wall_seconds, lambda: None)
        )
        elapsed = exec_res.get("seconds", time.perf_counter() - t0)
        peak_rss = int(exec_res.get("native_peak_rss_bytes", 0))
        if exec_res.get("returncode") != 0 or exec_res.get("guard_failures"):
            passed = False
            failures.append(f"{case['name']}: process failed")
            results.append({"name": case["name"], "passed": False, "seconds": elapsed, "native_peak_rss_bytes": peak_rss})
            continue
        envelope = strict_json(prefix.with_suffix(".stdout.json").read_text(encoding="utf-8"))
        fields = report_fields(envelope)
        if fields.get("public_workflow_fallback_attempted") not in (None, False, "false"):
            passed = False
            failures.append(f"{case['name']}: fallback_attempted was not false")
        if fields.get("public_workflow_external_engine_invoked") not in (None, False, "false"):
            passed = False
            failures.append(f"{case['name']}: external_engine_invoked was not false")
        actual = extract_result(envelope)
        if not equivalent(actual, case["expected"]):
            passed = False
            failures.append(f"{case['name']}: oracle mismatch")
        if elapsed > max_wall_seconds or peak_rss > max_peak_rss_bytes:
            passed = False
            failures.append(f"{case['name']}: exceeded smoke budget (wall={elapsed:.3f}s, rss={peak_rss})")
        results.append({
            "name": case["name"],
            "passed": not failures,
            "seconds": elapsed,
            "native_peak_rss_bytes": peak_rss,
        })

    return {
        "schema_version": SCHEMA_VERSION,
        "status": "passed" if passed else "failed",
        "rows": rows,
        "cases": results,
        "failures": failures,
        "claim_gate_status": "not_claim_grade",
        "fallback_attempted": False,
        "external_engine_invoked": False,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path)
    parser.add_argument("--work-dir", type=Path)
    parser.add_argument("--rows", type=int, default=256)
    parser.add_argument("--max-wall-seconds", type=float, default=30.0)
    parser.add_argument("--max-peak-rss-mib", type=int, default=1024)
    args = parser.parse_args()

    if args.work_dir is not None:
        args.work_dir.mkdir(parents=True, exist_ok=True)
        report = run_smoke_suite(
            binary=args.binary,
            work_dir=args.work_dir,
            rows=args.rows,
            max_wall_seconds=args.max_wall_seconds,
            max_peak_rss_bytes=args.max_peak_rss_mib * 1024 * 1024,
        )
    else:
        with tempfile.TemporaryDirectory(prefix="shardloom-ci-smoke-") as tmp:
            report = run_smoke_suite(
                binary=args.binary,
                work_dir=Path(tmp),
                rows=args.rows,
                max_wall_seconds=args.max_wall_seconds,
                max_peak_rss_bytes=args.max_peak_rss_mib * 1024 * 1024,
            )
    print(json.dumps(report, indent=2, allow_nan=False))
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
