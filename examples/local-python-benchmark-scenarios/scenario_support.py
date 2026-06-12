#!/usr/bin/env python
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence


FACT_SCHEMA = {
    "id": "int64",
    "group_key": "int64",
    "dim_key": "int64",
    "value": "int64",
    "metric": "float64",
    "flag": "boolean",
    "category": "utf8",
    "event_date": "utf8",
    "nullable_metric_00": "float64",
    "raw_event_time": "utf8",
    "dirty_numeric": "utf8",
}
DIM_SCHEMA = {
    "dim_key": "int64",
    "dim_label": "utf8",
    "weight": "float64",
}
EVENTS_SCHEMA = {
    "id": "int64",
    "nested_payload": "utf8",
}
EXPECTED_ERROR_SCENARIOS = frozenset({"malformed_timestamp_cast"})
TIMING_FIELD_TOKENS = (
    "millis",
    "timing",
    "source_state",
    "vortex_output",
    "output_commit",
    "row_count",
)


def default_run_id() -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{timestamp}-pid{os.getpid()}"


def resolve_under_repo(repo_root: Path, path: Path) -> Path:
    candidate = path if path.is_absolute() else repo_root / path
    return candidate.resolve()


def validate_run_id(run_id: str) -> None:
    if not run_id or Path(run_id).name != run_id or run_id in {".", ".."}:
        raise ValueError("--run-id must be a single path segment")


def build_run_paths(
    repo_root: Path,
    *,
    run_root: Path,
    run_id: str | None,
) -> dict[str, Path | str]:
    resolved_run_id = run_id or default_run_id()
    validate_run_id(resolved_run_id)
    resolved_run_root = resolve_under_repo(repo_root, run_root)
    run_dir = resolved_run_root / resolved_run_id
    return {
        "repo_root": repo_root,
        "run_root": resolved_run_root,
        "run_id": resolved_run_id,
        "run_dir": run_dir,
        "data_dir": run_dir / "data",
        "target_dir": run_dir / "target",
        "summary_json": run_dir / "scenario-summary.json",
        "timing_json": run_dir / "timing-components.json",
        "timing_markdown": run_dir / "timing-components.md",
    }


def write_fixture_data(run_dir: Path) -> None:
    data_dir = run_dir / "data"
    target_dir = run_dir / "target"
    data_dir.mkdir(parents=True, exist_ok=True)
    target_dir.mkdir(parents=True, exist_ok=True)
    (data_dir / "fact.csv").write_text(
        "id,group_key,dim_key,value,metric,flag,category,event_date,nullable_metric_00,raw_event_time,dirty_numeric\n"
        "1,10,100,90,1.5,true,A,2026-01-01,4.0,2026-01-01,12.5\n"
        "2,10,101,120,2.5,true,B,2026-01-02,,2026-01-02,-3.0\n"
        "3,20,100,150,3.0,false,A,2026-01-03,7.5,not-a-timestamp,5.25\n"
        "4,20,102,210,-1.0,true,C,2026-01-04,9.0,2026-01-04,0.0\n"
        "5,30,101,300,8.5,true,D,2026-01-05,11.0,2026-01-05,42.0\n",
        encoding="utf-8",
    )
    (data_dir / "dim.csv").write_text(
        "dim_key,dim_label,weight\n"
        "100,alpha,1.0\n"
        "101,beta,2.0\n"
        "102,gamma,3.0\n",
        encoding="utf-8",
    )
    (data_dir / "events.jsonl").write_text(
        '{"id":1,"nested_payload":"alpha target payload"}\n'
        '{"id":2,"nested_payload":"ordinary payload"}\n'
        '{"id":3,"nested_payload":"target nested value"}\n',
        encoding="utf-8",
    )


def load_local_shardloom(repo_root: Path) -> tuple[Any, Callable[..., Any]]:
    source_path = str(repo_root / "python" / "src")
    if source_path not in sys.path:
        sys.path.insert(0, source_path)
    import shardloom as sl
    from shardloom import context

    return sl, context


def scenario_actions(ctx: Any, sl: Any) -> list[tuple[str, Callable[[], Any]]]:
    fact = ctx.read_csv("data/fact.csv", schema=FACT_SCHEMA)
    dim = ctx.read_csv("data/dim.csv", schema=DIM_SCHEMA)
    events = ctx.read_json("data/events.jsonl", schema=EVENTS_SCHEMA)
    return [
        (
            "selective_filter",
            lambda: fact.filter(sl.col("flag") == True)
            .select("id", "group_key", "value")
            .limit(1000)
            .collect(),
        ),
        (
            "filter_projection_limit",
            lambda: fact.filter(sl.col("value") >= 100)
            .select("id", "group_key", "metric")
            .limit(100)
            .collect(),
        ),
        (
            "group_by_aggregation",
            lambda: fact.filter(sl.col("metric") >= 0)
            .group_by("group_key")
            .agg(rows="count(*)", total_metric="sum(metric)")
            .limit(100)
            .collect(),
        ),
        (
            "hash_join",
            lambda: fact.join(dim, on="dim_key", how="inner")
            .select("f.id", "d.dim_label", "f.metric")
            .limit(100)
            .collect(),
        ),
        (
            "global_top_n",
            lambda: fact.select("id", "group_key", "metric")
            .nlargest(10, "metric")
            .collect(),
        ),
        (
            "clean_cast_filter_write",
            lambda: fact.with_column(
                "amount_float",
                sl.col("dirty_numeric").cast("float64"),
            )
            .filter(sl.col("amount_float") >= 0)
            .limit(1000)
            .write_vortex(
                "target/clean-cast-filter-write.vortex",
                allow_overwrite=True,
            ),
        ),
        (
            "malformed_timestamp_cast",
            lambda: fact.with_column(
                "event_day",
                sl.col("raw_event_time").cast("date32"),
            )
            .limit(1000)
            .collect(),
        ),
        (
            "null_heavy_aggregate",
            lambda: fact.dropna(subset=["nullable_metric_00"])
            .group_by("group_key")
            .agg(
                rows="count(*)",
                total_nullable_metric="sum(nullable_metric_00)",
            )
            .limit(100)
            .collect(),
        ),
        (
            "nested_json_field_scan",
            lambda: events.filter(sl.col("nested_payload").contains("target"))
            .select("id", "nested_payload")
            .limit(100)
            .collect(),
        ),
    ]


def run_scenarios(
    *,
    repo_root: Path,
    run_dir: Path,
    binary: str | None = None,
    profile_order: Sequence[str] = ("release", "debug"),
) -> dict[str, Any]:
    write_fixture_data(run_dir)
    sl, context = load_local_shardloom(repo_root)
    previous_cwd = Path.cwd()
    os.chdir(run_dir)
    try:
        ctx = context(
            repo_root=str(repo_root),
            binary=binary,
            profile_order=tuple(profile_order),
        )
        scenario_results = []
        for name, action in scenario_actions(ctx, sl):
            started = time.perf_counter()
            try:
                report = action()
                elapsed = (time.perf_counter() - started) * 1000.0
                scenario_results.append(
                    summarize_report(
                        name,
                        report,
                        python_wall_millis=round(elapsed, 4),
                    )
                )
            except Exception as exc:  # noqa: BLE001 - surfaced in JSON for local diagnosis.
                elapsed = (time.perf_counter() - started) * 1000.0
                scenario_results.append(
                    summarize_exception(
                        name,
                        exc,
                        python_wall_millis=round(elapsed, 4),
                    )
                )
    finally:
        os.chdir(previous_cwd)
    return {
        "schema_version": "shardloom.local_python_benchmark_scenarios.v1",
        "run_dir": str(run_dir),
        "data_dir": str(run_dir / "data"),
        "target_dir": str(run_dir / "target"),
        "scenario_count": len(scenario_results),
        "passed": all(result["ok"] for result in scenario_results),
        "results": scenario_results,
    }


def summarize_report(
    name: str,
    report: Any,
    *,
    python_wall_millis: float,
) -> dict[str, Any]:
    envelope = getattr(report, "envelope", None)
    fields = envelope_fields(envelope)
    status = safe_attr(report, "status", getattr(envelope, "status", "unknown"))
    diagnostics = [
        {
            "code": diagnostic.code,
            "severity": diagnostic.severity,
            "reason": diagnostic.reason,
            "message": diagnostic.message,
        }
        for diagnostic in getattr(report, "diagnostics", ())
    ]
    rows = result_sample(report)
    expected_error = name in EXPECTED_ERROR_SCENARIOS
    fallback_attempted = bool(safe_attr(report, "fallback_attempted", False))
    external_engine_invoked = bool(safe_attr(report, "external_engine_invoked", False))
    is_error = bool(safe_attr(report, "is_error", status != "success"))
    ok = (
        not fallback_attempted
        and not external_engine_invoked
        and ((is_error and expected_error) or (not is_error and not expected_error))
    )
    return {
        "name": name,
        "ok": ok,
        "expected_error": expected_error,
        "report_type": type(report).__name__,
        "command": getattr(envelope, "command", None),
        "status": status,
        "is_error": is_error,
        "python_wall_millis": python_wall_millis,
        "output_row_count": safe_attr(report, "output_row_count"),
        "fallback_attempted": fallback_attempted,
        "external_engine_invoked": external_engine_invoked,
        "claim_gate_status": safe_attr(
            report,
            "claim_gate_status",
            fields.get("claim_gate_status"),
        ),
        "timing_scope": fields.get("timing_scope"),
        "source_format": fields.get("source_format"),
        "output_format": fields.get("output_format"),
        "output_path": fields.get("output_path"),
        "vortex_output_row_count": fields.get("vortex_output_row_count"),
        "diagnostics": diagnostics,
        "result_sample": rows,
        "fields": fields,
        "timing_components": timing_components(fields, python_wall_millis),
    }


def summarize_exception(
    name: str,
    exc: Exception,
    *,
    python_wall_millis: float,
) -> dict[str, Any]:
    return {
        "name": name,
        "ok": False,
        "expected_error": name in EXPECTED_ERROR_SCENARIOS,
        "report_type": "exception",
        "command": None,
        "status": "exception",
        "is_error": True,
        "python_wall_millis": python_wall_millis,
        "exception_type": type(exc).__name__,
        "exception_message": str(exc),
        "fallback_attempted": None,
        "external_engine_invoked": None,
        "diagnostics": [],
        "result_sample": [],
        "fields": {},
        "timing_components": {"python_wall_millis": python_wall_millis},
    }


def envelope_fields(envelope: Any) -> dict[str, str]:
    if envelope is None:
        return {}
    return {entry.key: entry.value for entry in getattr(envelope, "fields", ())}


def safe_attr(value: Any, name: str, default: Any = None) -> Any:
    try:
        return getattr(value, name)
    except Exception:  # noqa: BLE001 - typed accessors may require success-only fields.
        return default


def result_sample(report: Any, limit: int = 3) -> list[Any]:
    try:
        rows = getattr(report, "result_rows")
        return list(rows[:limit])
    except Exception:  # noqa: BLE001 - absent result_jsonl on expected error reports.
        return []


def timing_components(fields: Mapping[str, str], python_wall_millis: float) -> dict[str, Any]:
    components: dict[str, Any] = {"python_wall_millis": python_wall_millis}
    for key, value in fields.items():
        if any(token in key for token in TIMING_FIELD_TOKENS):
            components[key] = value
    return components


def write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def write_timing_markdown(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = payload.get("results", [])
    lines = [
        "# ShardLoom Python Benchmark Scenario Timing Review",
        "",
        f"Run directory: `{payload.get('run_dir')}`",
        "",
        "| Scenario | Status | Expected | Python wall | Timing scope | Rows | Output | Key components |",
        "| --- | --- | --- | ---: | --- | ---: | --- | --- |",
    ]
    for row in rows if isinstance(rows, list) else []:
        components = row.get("timing_components", {})
        component_text = compact_component_text(components if isinstance(components, dict) else {})
        expected = "error" if row.get("expected_error") else "success"
        lines.append(
            "| {name} | {status} | {expected} | {wall:.4f} ms | {scope} | {count} | {output} | {components} |".format(
                name=row.get("name"),
                status="ok" if row.get("ok") else "check",
                expected=expected,
                wall=float(row.get("python_wall_millis") or 0.0),
                scope=row.get("timing_scope") or "n/a",
                count=row.get("output_row_count") or 0,
                output=row.get("output_format") or "n/a",
                components=component_text,
            )
        )
    lines.append("")
    path.write_text("\n".join(lines), encoding="utf-8")


def compact_component_text(components: Mapping[str, Any]) -> str:
    keys = [
        "source_to_columnar_millis",
        "source_read_millis",
        "compatibility_parse_millis",
        "vortex_output_timing_scope",
        "vortex_output_row_count",
        "output_commit_status",
    ]
    pairs = [
        f"{key}={components[key]}"
        for key in keys
        if components.get(key) not in {None, "", "0", "not_applicable"}
    ]
    return "<br>".join(pairs) if pairs else "n/a"
