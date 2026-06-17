#!/usr/bin/env python
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import csv
import json
import os
import shutil
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
    "flag": "int64",
    "category": "utf8",
    "event_date": "utf8",
    "nullable_metric_00": "float64",
    "nested_payload": "utf8",
    "nested_group": "utf8",
    "nested_score": "float64",
    "raw_event_time": "utf8",
    "dirty_numeric": "utf8",
    "dirty_flag": "utf8",
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
SCENARIO_ROUTES: tuple[tuple[str, str, str], ...] = (
    ("selective_filter", "selective filter", "selective-filter"),
    ("filter_projection_limit", "filter + projection + limit", "filter---projection---limit"),
    ("group_by_aggregation", "group by aggregation", "group-by-aggregation"),
    ("hash_join", "hash join", "hash-join"),
    ("global_top_n", "sort and top-k", "sort-and-top-k"),
    ("clean_cast_filter_write", "clean/cast/filter/write", "clean-cast-filter-write"),
    (
        "malformed_timestamp_cast",
        "malformed timestamp / dirty CSV",
        "malformed-timestamp---dirty-CSV",
    ),
    ("null_heavy_aggregate", "null-heavy aggregate", "null-heavy-aggregate"),
    ("nested_json_field_scan", "nested JSON field scan", "nested-JSON-field-scan"),
)
EXPECTED_ERROR_SCENARIOS = frozenset()
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
    if run_dir.exists():
        shutil.rmtree(run_dir)
    data_dir = run_dir / "data"
    target_dir = run_dir / "target"
    data_dir.mkdir(parents=True, exist_ok=True)
    target_dir.mkdir(parents=True, exist_ok=True)
    fact_columns = tuple(FACT_SCHEMA)
    with (data_dir / "fact.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(fact_columns)
        for idx in range(1, 21):
            group_key = (idx % 3 + 1) * 10
            dim_key = 100 + (idx % 3)
            value = 4_800 + idx * 250
            metric = float(idx) * 1.5
            flag = 1 if idx % 2 else 0
            category = chr(ord("A") + (idx % 4))
            event_date = f"2026-01-{(idx % 28) + 1:02d}"
            nested_payload = json.dumps(
                {
                    "event": {"date": event_date, "flag": bool(flag)},
                    "metrics": {"value": value, "score": round(metric / 10.0, 4)},
                    "labels": [category, f"g{group_key % 5}"],
                },
                separators=(",", ":"),
            )
            writer.writerow(
                [
                    idx,
                    group_key,
                    dim_key,
                    value,
                    f"{metric:.2f}",
                    flag,
                    category,
                    event_date,
                    "" if idx % 4 == 0 else f"{metric + 2.0:.2f}",
                    nested_payload,
                    f"g{group_key % 5}",
                    f"{metric / 10.0:.4f}",
                    "not-a-timestamp"
                    if idx % 7 == 0
                    else f"{event_date}T00:00:00Z",
                    "bad-number" if idx % 9 == 0 else str(value),
                    "Y" if flag else "N",
                ]
            )
    with (data_dir / "dim.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(tuple(DIM_SCHEMA))
        writer.writerows(
            [
                [100, "alpha", 1.0],
                [101, "beta", 2.0],
                [102, "gamma", 3.0],
            ]
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
    del ctx, sl
    return [(scenario_id, lambda: None) for scenario_id, _, _ in SCENARIO_ROUTES]


def run_scenarios(
    *,
    repo_root: Path,
    run_dir: Path,
    binary: str | os.PathLike[str] | Sequence[str] | None = None,
    profile_order: Sequence[str] = ("release", "debug"),
) -> dict[str, Any]:
    write_fixture_data(run_dir)
    _, context = load_local_shardloom(repo_root)
    resolved_binary = binary
    if isinstance(binary, (str, os.PathLike)):
        resolved_binary = str(resolve_under_repo(repo_root, Path(binary)))
    previous_cwd = Path.cwd()
    os.chdir(run_dir)
    try:
        ctx = context(
            repo_root=str(repo_root),
            binary=resolved_binary,
            profile_order=tuple(profile_order),
        )
        route = ctx.prepare_vortex(
            "data/fact.csv",
            dim="data/dim.csv",
            workspace="target/prepared-vortex",
            input_format="csv",
            result_workspace="target/prepared-vortex-results",
            evidence_level="certified",
            max_parallelism=1,
        )
        started = time.perf_counter()
        try:
            report = route.run_batch(
                [scenario for _, scenario, _ in SCENARIO_ROUTES],
                result_workspace="target/prepared-vortex-batch",
                evidence_level="certified",
                max_parallelism=1,
                check=False,
            )
            elapsed = (time.perf_counter() - started) * 1000.0
            scenario_results = summarize_prepared_batch(
                report,
                python_wall_millis=round(elapsed, 4),
            )
        except Exception as exc:  # noqa: BLE001 - surfaced in JSON for local diagnosis.
            elapsed = (time.perf_counter() - started) * 1000.0
            scenario_results = [
                summarize_exception(
                    scenario_id,
                    exc,
                    python_wall_millis=round(elapsed, 4),
                )
                for scenario_id, _, _ in SCENARIO_ROUTES
            ]
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


def summarize_prepared_batch(
    report: Any,
    *,
    python_wall_millis: float,
) -> list[dict[str, Any]]:
    envelope = getattr(report, "batch", getattr(report, "envelope", report))
    fields = envelope_fields(envelope)
    return [
        summarize_prepared_scenario(
            scenario_id,
            slug,
            report,
            envelope,
            fields,
            python_wall_millis=python_wall_millis,
        )
        for scenario_id, _, slug in SCENARIO_ROUTES
    ]


def summarize_prepared_scenario(
    name: str,
    slug: str,
    report: Any,
    envelope: Any,
    fields: Mapping[str, str],
    *,
    python_wall_millis: float,
) -> dict[str, Any]:
    scenario_fields = scenario_field_subset(fields, slug)
    status = safe_attr(envelope, "status", "unknown")
    support_status = scenario_fields.get(f"scenario_{slug}_support_status")
    lifecycle_status = scenario_fields.get(
        f"scenario_{slug}_prepared_native_vortex_lifecycle_status"
    )
    expected_error = name in EXPECTED_ERROR_SCENARIOS
    fallback_attempted = bool_field(
        scenario_fields,
        f"scenario_{slug}_fallback_attempted",
        "fallback_attempted",
        default=False,
    )
    external_engine_invoked = bool_field(
        scenario_fields,
        f"scenario_{slug}_external_engine_invoked",
        "external_engine_invoked",
        default=False,
    )
    is_error = bool(safe_attr(envelope, "is_error", status != "success"))
    scenario_supported = support_status in {None, "supported"}
    ok = (
        not fallback_attempted
        and not external_engine_invoked
        and scenario_supported
        and ((is_error and expected_error) or (not is_error and not expected_error))
    )
    output_row_count = prepared_output_row_count(slug, scenario_fields)
    diagnostics = [
        {
            "code": diagnostic.code,
            "severity": diagnostic.severity,
            "reason": diagnostic.reason,
            "message": diagnostic.message,
        }
        for diagnostic in getattr(envelope, "diagnostics", ())
    ]
    return {
        "name": name,
        "ok": ok,
        "expected_error": expected_error,
        "report_type": type(report).__name__,
        "command": getattr(envelope, "command", None),
        "status": support_status or status,
        "is_error": is_error,
        "python_wall_millis": python_wall_millis,
        "output_row_count": output_row_count,
        "fallback_attempted": fallback_attempted,
        "external_engine_invoked": external_engine_invoked,
        "claim_gate_status": scenario_fields.get(
            f"scenario_{slug}_claim_gate_status",
            fields.get("claim_gate_status"),
        ),
        "timing_scope": scenario_fields.get(f"scenario_{slug}_timing_scope")
        or fields.get("timing_scope")
        or "prepared_vortex_batch",
        "source_format": fields.get("source_format") or "csv",
        "output_format": scenario_fields.get(f"scenario_{slug}_output_format")
        or fields.get("output_format"),
        "output_path": scenario_fields.get(f"scenario_{slug}_output_path")
        or fields.get("output_path"),
        "vortex_output_row_count": scenario_fields.get(
            f"scenario_{slug}_vortex_output_row_count"
        ),
        "lifecycle_status": lifecycle_status,
        "execution_mode": scenario_fields.get(f"scenario_{slug}_execution_mode"),
        "diagnostics": diagnostics,
        "result_sample": prepared_result_sample(slug, scenario_fields),
        "fields": scenario_fields,
        "timing_components": timing_components(scenario_fields, python_wall_millis),
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
    field_map = getattr(envelope, "field_map", None)
    if isinstance(field_map, Mapping):
        return {str(key): str(value) for key, value in field_map.items()}
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


def scenario_field_subset(fields: Mapping[str, str], slug: str) -> dict[str, str]:
    prefix = f"scenario_{slug}_"
    shared_keys = {
        "runner_kind",
        "scenario_order",
        "prepare_batch_schema_version",
        "prepare_batch_lifecycle_schema_version",
        "prepare_batch_lifecycle_status",
        "prepare_batch_lifecycle_no_standalone_lane",
        "prepare_batch_scale_runtime_status",
        "prepare_batch_scale_route",
        "source_format",
        "fallback_attempted",
        "external_engine_invoked",
    }
    return {
        key: value
        for key, value in fields.items()
        if key.startswith(prefix) or key in shared_keys
    }


def bool_field(
    fields: Mapping[str, str],
    *keys: str,
    default: bool,
) -> bool:
    for key in keys:
        value = fields.get(key)
        if value is None:
            continue
        lowered = str(value).strip().lower()
        if lowered == "true":
            return True
        if lowered == "false":
            return False
    return default


def prepared_output_row_count(slug: str, fields: Mapping[str, str]) -> int | None:
    result_json = fields.get(f"scenario_{slug}_result_json")
    if result_json:
        try:
            decoded = json.loads(result_json)
        except json.JSONDecodeError:
            decoded = None
        if isinstance(decoded, list):
            return len(decoded)
        if isinstance(decoded, dict):
            for key in ("row_count", "rows", "count"):
                value = decoded.get(key)
                if isinstance(value, int):
                    return value
    for key in (
        f"scenario_{slug}_streaming_result_row_count",
        f"scenario_{slug}_computed_result_sink_rows",
        "output_row_count",
    ):
        value = fields.get(key)
        if value not in {None, "", "none"}:
            try:
                return int(str(value))
            except ValueError:
                continue
    return None


def prepared_result_sample(slug: str, fields: Mapping[str, str]) -> list[Any]:
    result_json = fields.get(f"scenario_{slug}_result_json")
    if not result_json:
        return []
    try:
        decoded = json.loads(result_json)
    except json.JSONDecodeError:
        return []
    if isinstance(decoded, list):
        return decoded[:3]
    if isinstance(decoded, dict):
        return [decoded]
    return []


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
