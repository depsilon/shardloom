#!/usr/bin/env python3
"""Validate the ShardLoom ClickBench OLAP runtime coverage manifest."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
PYTHON_SRC = ROOT / "python" / "src"
if str(PYTHON_SRC) not in sys.path:
    sys.path.insert(0, str(PYTHON_SRC))

from shardloom import DEFAULT_LOCAL_RUNTIME_MAX_PARALLELISM

DEFAULT_QUERY_MANIFEST = ROOT / "benchmarks" / "clickbench" / "queries.sql"
DEFAULT_OUTPUT = ROOT / "target" / "clickbench-olap-runtime-coverage.json"
SCHEMA_VERSION = "shardloom.clickbench_olap_runtime_coverage.v1"
STATE_BUDGET_SCHEMA_VERSION = "shardloom.clickbench_olap_state_budget.v2"
SCALE_FIXTURE_STRATEGY_SCHEMA_VERSION = "shardloom.clickbench_scale_fixture_strategy.v1"
CANONICAL_SOURCE_URL = (
    "https://raw.githubusercontent.com/ClickHouse/ClickBench/main/clickhouse/queries.sql"
)
EXPECTED_QUERY_COUNT = 43

SQL_KEYWORDS = {
    "SELECT",
    "FROM",
    "WHERE",
    "GROUP",
    "BY",
    "ORDER",
    "DESC",
    "ASC",
    "LIMIT",
    "OFFSET",
    "AS",
    "AND",
    "OR",
    "NOT",
    "LIKE",
    "IN",
    "CASE",
    "WHEN",
    "THEN",
    "ELSE",
    "END",
    "HAVING",
    "DISTINCT",
    "COUNT",
    "SUM",
    "AVG",
    "MIN",
    "MAX",
    "REGEXP_REPLACE",
    "DATE_TRUNC",
    "EXTRACT",
    "LENGTH",
    "MINUTE",
    "HITS",
    "TRUE",
    "FALSE",
    "NULL",
}

ALIASES = {
    "c",
    "u",
    "l",
    "k",
    "m",
    "src",
    "dst",
    "pageviews",
}


def split_sql_statements(text: str) -> list[str]:
    statements: list[str] = []
    current: list[str] = []
    in_string = False
    index = 0
    while index < len(text):
        char = text[index]
        next_char = text[index + 1] if index + 1 < len(text) else ""
        if not in_string and char == "-" and next_char == "-":
            while index < len(text) and text[index] != "\n":
                index += 1
            current.append(" ")
            continue
        if not in_string and char == "/" and next_char == "*":
            index += 2
            while index < len(text):
                if text[index] == "*" and index + 1 < len(text) and text[index + 1] == "/":
                    index += 2
                    break
                index += 1
            current.append(" ")
            continue
        if char == "'":
            current.append(char)
            if in_string and next_char == "'":
                current.append(next_char)
                index += 2
                continue
            in_string = not in_string
            index += 1
            continue
        if char == ";" and not in_string:
            statement = " ".join("".join(current).split())
            if statement:
                statements.append(statement)
            current = []
            index += 1
            continue
        current.append(char)
        index += 1
    tail = " ".join("".join(current).split())
    if tail:
        statements.append(tail)
    return statements


def compact_sql(statement: str) -> str:
    return re.sub(r"\s+", " ", statement).strip()


def lowered(statement: str) -> str:
    return compact_sql(statement).lower()


def operator_tags(statement: str) -> list[str]:
    sql = lowered(statement)
    tags = ["scan", "single_table_hits"]
    tags.append("filtered_scan" if " where " in sql else "full_scan")
    if "count(*)" in sql:
        tags.append("aggregate_count")
    if "sum(" in sql:
        tags.append("aggregate_sum")
    if "avg(" in sql:
        tags.append("aggregate_avg")
    if "min(" in sql:
        tags.append("aggregate_min")
    if "max(" in sql:
        tags.append("aggregate_max")
    if any(tag.startswith("aggregate_") for tag in tags):
        tags.append("aggregate")
    if "count(distinct" in sql:
        tags.append("distinct_aggregate")
    if " group by " in sql:
        tags.append("group_by")
        group_clause = sql.split(" group by ", 1)[1].split(" order by ", 1)[0].split(" limit ", 1)[0]
        if "," in group_clause:
            tags.append("multi_key_group_by")
        if re.search(r"(?:^|,)\s*\d+\s*(?:,|$)", group_clause):
            tags.append("ordinal_group_by")
    if " order by " in sql:
        tags.append("order_by")
    if " limit " in sql:
        tags.append("limit")
    if " offset " in sql:
        tags.append("offset")
    if " order by " in sql and " limit " in sql:
        tags.append("top_k")
    if " having " in sql:
        tags.append("having")
    if " like " in sql:
        tags.append("like_predicate")
    if " not like " in sql:
        tags.append("not_like_predicate")
    if "regexp_replace(" in sql:
        tags.append("regex_replace")
    if "length(" in sql:
        tags.append("length_function")
    if "extract(" in sql:
        tags.append("date_time_extract")
    if "date_trunc(" in sql:
        tags.append("date_time_trunc")
    if " case " in f" {sql} ":
        tags.append("case_expression")
    if " in (" in sql:
        tags.append("in_list_predicate")
    if re.search(r"\b\w+\s*(?:<>|!=|=|>=|<=|>|<)\s*-?\d+\b", sql):
        tags.append("integer_comparison_predicate")
    if re.search(r"\b\w+\s*(?:<>|!=)\s*''", sql):
        tags.append("string_non_empty_predicate")
    if re.search(r"\beventdate\s*(?:>=|<=|=)\s*'", sql):
        tags.append("date_range_predicate")
    if " and " in sql:
        tags.append("conjunctive_predicate")
    if "select *" in sql:
        tags.append("star_projection")
    string_scrubbed_sql = re.sub(r"'(?:''|[^'])*'", " ", sql)
    if re.search(r"\b\w+\s*[-+*/]\s*\d+\b", string_scrubbed_sql):
        tags.append("arithmetic_projection")
    if sql.count("sum(") >= 20:
        tags.append("wide_repeated_aggregate_projection")
    if "where userid = " in sql and " group by " not in sql:
        tags.append("point_lookup")
    return sorted(set(tags), key=tags.index)


def result_shape(tags: list[str], statement: str) -> str:
    sql = lowered(statement)
    if "wide_repeated_aggregate_projection" in tags:
        return "wide_scalar_row"
    if "group_by" in tags:
        return "grouped_rows"
    if "top_k" in tags:
        return "ordered_bounded_rows"
    if "star_projection" in tags:
        return "raw_bounded_rows"
    if "aggregate" in tags:
        return "scalar_row"
    if "select userid from" in sql:
        return "single_column_rows"
    return "rows"


def input_columns(statement: str) -> list[str]:
    scrubbed = re.sub(r"'(?:''|[^'])*'", " ", statement)
    tokens = re.findall(r"\b[A-Za-z_][A-Za-z0-9_]*\b", scrubbed)
    columns = []
    for token in tokens:
        if token.upper() in SQL_KEYWORDS or token.lower() in ALIASES:
            continue
        if token not in columns:
            columns.append(token)
    return columns


def unique(values: list[str]) -> list[str]:
    out: list[str] = []
    for value in values:
        if value not in out:
            out.append(value)
    return out


def state_budget_profile(tags: list[str], route_family: str) -> dict[str, Any]:
    capillary_work_units = ["vortex_scan"]
    pulseweave_pressure_signals: list[str] = []
    state_family = "stateless_scan_or_metadata"
    required = False

    if "aggregate" in tags:
        required = True
        state_family = "scalar_aggregate_state"
        capillary_work_units.append("aggregate_state")
        pulseweave_pressure_signals.extend(["aggregate_input_rows", "aggregate_measure_count"])
    if "group_by" in tags:
        required = True
        state_family = "grouped_aggregate_state"
        capillary_work_units.extend(["group_key_state", "aggregate_state"])
        pulseweave_pressure_signals.extend(["group_cardinality", "group_state_rows"])
    if "distinct_aggregate" in tags:
        required = True
        state_family = (
            f"{state_family}+count_distinct"
            if state_family != "stateless_scan_or_metadata"
            else "count_distinct_state"
        )
        capillary_work_units.append("count_distinct_set")
        pulseweave_pressure_signals.append("distinct_value_cardinality")
    if "top_k" in tags or route_family.endswith("_topk"):
        required = True
        state_family = (
            f"{state_family}+topk"
            if state_family != "stateless_scan_or_metadata"
            else "raw_row_topk_sort_state"
        )
        capillary_work_units.append("topk_heap_state")
        pulseweave_pressure_signals.append("topk_heap_rows")
    if "offset" in tags:
        required = True
        state_family = (
            f"{state_family}+offset"
            if state_family != "stateless_scan_or_metadata"
            else "offset_drain_state"
        )
        capillary_work_units.append("offset_drain")
        pulseweave_pressure_signals.append("offset_drain_rows")
    if "like_predicate" in tags or "not_like_predicate" in tags or "string_non_empty_predicate" in tags:
        capillary_work_units.append("utf8_predicate_scan")
        pulseweave_pressure_signals.append("string_scan_selectivity")
    if "wide_repeated_aggregate_projection" in tags:
        required = True
        capillary_work_units.append("wide_aggregate_projection")
        pulseweave_pressure_signals.append("aggregate_projection_width")
    if "having" in tags:
        capillary_work_units.append("having_filter")
        pulseweave_pressure_signals.append("having_selectivity")

    pressure_class = state_pressure_class(tags, route_family, required)
    if not required and len(capillary_work_units) == 1:
        status = "not_required"
        spill_policy = "not_applicable"
        fail_closed = False
    else:
        status = state_budget_status_for_pressure(pressure_class)
        spill_policy = "fail_closed_before_uncertified_spill"
        fail_closed = True

    return {
        "state_budget_schema_version": STATE_BUDGET_SCHEMA_VERSION,
        "state_budget_required": required,
        "state_budget_status": status,
        "state_pressure_class": pressure_class,
        "state_family": state_family,
        "capillary_work_units": unique(capillary_work_units),
        "pulseweave_pressure_signals": unique(pulseweave_pressure_signals),
        "spill_required": False,
        "spill_supported": False,
        "spill_io_performed": False,
        "spill_policy": spill_policy,
        "fail_closed_if_spill_required": fail_closed,
        "state_budget_diagnostic_code": state_budget_diagnostic_code_for_pressure(pressure_class),
        "state_budget_next_action": state_budget_next_action_for_pressure(pressure_class),
    }


def state_pressure_class(
    tags: list[str], route_family: str, state_budget_required: bool
) -> str:
    if not state_budget_required:
        return "none"
    if "group_by" in tags and (
        "multi_key_group_by" in tags
        or "distinct_aggregate" in tags
        or "like_predicate" in tags
        or "not_like_predicate" in tags
        or route_family.endswith("_topk")
    ):
        return "near_input_cardinality_high_pressure"
    if "group_by" in tags or "top_k" in tags or "offset" in tags:
        return "high_cardinality_pressure"
    if "wide_repeated_aggregate_projection" in tags or "distinct_aggregate" in tags:
        return "moderate_cardinality_pressure"
    return "low_cardinality_pressure"


def state_budget_status_for_pressure(pressure_class: str) -> str:
    return {
        "none": "not_required",
        "low_cardinality_pressure": "bounded_in_memory_low_pressure_spill_not_required",
        "moderate_cardinality_pressure": "bounded_in_memory_moderate_pressure_spill_not_certified",
        "high_cardinality_pressure": "bounded_in_memory_high_pressure_spill_not_certified",
        "near_input_cardinality_high_pressure": (
            "bounded_in_memory_near_input_cardinality_spill_not_certified"
        ),
    }.get(pressure_class, "bounded_in_memory_route_budget_declared_spill_not_certified")


def state_budget_diagnostic_code_for_pressure(pressure_class: str) -> str:
    if pressure_class == "near_input_cardinality_high_pressure":
        return "SL_STATE_BUDGET_HIGH_PRESSURE_NATIVE_SPILL_PENDING"
    if pressure_class == "high_cardinality_pressure":
        return "SL_STATE_BUDGET_HIGH_CARDINALITY_REVIEW"
    return "none"


def state_budget_next_action_for_pressure(pressure_class: str) -> str:
    if pressure_class == "near_input_cardinality_high_pressure":
        return (
            "prefer embedded layout pruning, partitioned exact merge, or certified native spill "
            "before broad scale or sub-second performance claims"
        )
    if pressure_class == "high_cardinality_pressure":
        return (
            "review layout/state evidence and retain only reusable exact optimizations that reduce "
            "dominant state work"
        )
    if pressure_class == "moderate_cardinality_pressure":
        return "continue with in-memory exact route and monitor pressure evidence in targeted UAT"
    return "none"


def scale_fixture_strategy() -> dict[str, Any]:
    return {
        "schema_version": SCALE_FIXTURE_STRATEGY_SCHEMA_VERSION,
        "strategy_id": "clickbench_scale_fixture_strategy_v1",
        "default_pr_fast_lane_tier": "small_deterministic_local",
        "performance_claim_allowed": False,
        "max_parallelism_default": DEFAULT_LOCAL_RUNTIME_MAX_PARALLELISM,
        "tiers": [
            {
                "tier_id": "small_deterministic_local",
                "purpose": "correctness and route-readiness coverage for all 43 query families",
                "required_for_pr_fast_lane": True,
                "sequential_default": True,
                "full_clickbench_performance_claim_allowed": False,
            },
            {
                "tier_id": "medium_sequential_uat",
                "purpose": "optional local stress/UAT over larger generated hits-like data",
                "required_for_pr_fast_lane": False,
                "sequential_default": True,
                "full_clickbench_performance_claim_allowed": False,
            },
            {
                "tier_id": "full_100m_artifact_runner",
                "purpose": "manual/offline full-scale artifact production after maintainer approval",
                "required_for_pr_fast_lane": False,
                "sequential_default": True,
                "full_clickbench_performance_claim_allowed": False,
            },
        ],
    }


def runtime_status(query_id: str, tags: list[str], statement: str) -> dict[str, str]:
    sql = lowered(statement)
    if query_id == "CB-Q01":
        return {
            "runtime_status": "admitted_current_runtime",
            "route_family": "native_vortex_count_all",
            "route_id": "native_vortex_count_all",
            "blocker_id": "none",
            "next_action": "none",
        }
    if query_id == "CB-Q02":
        return {
            "runtime_status": "admitted_current_runtime",
            "route_family": "native_vortex_count_where",
            "route_id": "native_vortex_count_where",
            "blocker_id": "none",
            "next_action": "none",
        }
    if "point_lookup" in tags and "select userid" in sql:
        return {
            "runtime_status": "admitted_current_runtime",
            "route_family": "native_vortex_filter_project",
            "route_id": "native_vortex_filter_project",
            "blocker_id": "none",
            "next_action": "none",
        }
    if "group_by" in tags:
        route_family = "native_vortex_grouped_aggregate"
        if "order_by" in tags or "top_k" in tags or "offset" in tags:
            route_family = "native_vortex_grouped_aggregate_topk"
        return {
            "runtime_status": "admitted_current_runtime",
            "route_family": route_family,
            "route_id": "native_vortex_aggregate",
            "blocker_id": "none",
            "next_action": "none",
        }
    elif "distinct_aggregate" in tags and "group_by" not in tags:
        return {
            "runtime_status": "admitted_current_runtime",
            "route_family": "native_vortex_scalar_aggregate",
            "route_id": "native_vortex_aggregate",
            "blocker_id": "none",
            "next_action": "none",
        }
    elif "distinct_aggregate" in tags:
        blocker = "clickbench.olap_runtime.distinct_expression_required"
        action = "finish remaining expression lowering around count-distinct without adding an external engine fallback"
    elif "group_by" in tags:
        blocker = "clickbench.olap_runtime.group_by_expression_required"
        action = "lower remaining grouped SQL expressions into native projection/predicate kernels before grouped aggregate execution"
    elif "wide_repeated_aggregate_projection" in tags:
        return {
            "runtime_status": "admitted_current_runtime",
            "route_family": "native_vortex_scalar_aggregate_wide_sum_offsets",
            "route_id": "native_vortex_aggregate",
            "blocker_id": "none",
            "next_action": "none",
        }
    elif "top_k" in tags or "order_by" in tags:
        return {
            "runtime_status": "admitted_current_runtime",
            "route_family": "native_vortex_sorted_rows_topk",
            "route_id": "native_vortex_sort_rows",
            "blocker_id": "none",
            "next_action": "none",
        }
    elif "aggregate" in tags:
        return {
            "runtime_status": "admitted_current_runtime",
            "route_family": "native_vortex_scalar_aggregate",
            "route_id": "native_vortex_aggregate",
            "blocker_id": "none",
            "next_action": "none",
        }
    elif "like_predicate" in tags or "string_non_empty_predicate" in tags:
        blocker = "clickbench.olap_runtime.string_filter_rows_required"
        action = "add native UTF-8 LIKE/not-LIKE row-returning filter/project route with no fallback"
    else:
        blocker = "clickbench.olap_runtime.general_sql_lowering_required"
        action = "lower this SQL shape into a declared native Vortex operator family"
    return {
        "runtime_status": "implementation_required",
        "route_family": "implementation_track",
        "route_id": "not_admitted_yet",
        "blocker_id": blocker,
        "next_action": action,
    }


def coverage_report(queries: list[str]) -> dict[str, Any]:
    rows = []
    for index, statement in enumerate(queries, start=1):
        query_id = f"CB-Q{index:02d}"
        tags = operator_tags(statement)
        status = runtime_status(query_id, tags, statement)
        state_budget = state_budget_profile(tags, status["route_family"])
        rows.append(
            {
                "query_id": query_id,
                "query_text": statement,
                "query_text_sha256": hashlib.sha256(statement.encode("utf-8")).hexdigest(),
                "input_table": "hits",
                "input_columns": input_columns(statement),
                "operator_tags": tags,
                "result_shape": result_shape(tags, statement),
                "runtime_status": status["runtime_status"],
                "route_family": status["route_family"],
                "route_id": status["route_id"],
                "blocker_id": status["blocker_id"],
                "next_action": status["next_action"],
                "benchmark_site_readiness_status": "ready_route_readiness_not_performance",
                "readiness_surface": "clickbench_olap_route_readiness",
                "timing_surface": "route_readiness_no_timing",
                "scale_fixture_strategy_id": "clickbench_scale_fixture_strategy_v1",
                "scale_fixture_default_tier": "small_deterministic_local",
                "max_parallelism_default": DEFAULT_LOCAL_RUNTIME_MAX_PARALLELISM,
                **state_budget,
                "fallback_attempted": False,
                "external_engine_invoked": False,
                "performance_claim_allowed": False,
            }
        )
    status_counts: dict[str, int] = {}
    blocker_counts: dict[str, int] = {}
    tag_counts: dict[str, int] = {}
    state_family_counts: dict[str, int] = {}
    state_pressure_class_counts: dict[str, int] = {}
    route_family_counts: dict[str, int] = {}
    capillary_work_unit_counts: dict[str, int] = {}
    pulseweave_pressure_signal_counts: dict[str, int] = {}
    spill_policy_counts: dict[str, int] = {}
    for row in rows:
        status_counts[row["runtime_status"]] = status_counts.get(row["runtime_status"], 0) + 1
        route_family_counts[row["route_family"]] = route_family_counts.get(row["route_family"], 0) + 1
        if row["blocker_id"] != "none":
            blocker_counts[row["blocker_id"]] = blocker_counts.get(row["blocker_id"], 0) + 1
        for tag in row["operator_tags"]:
            tag_counts[tag] = tag_counts.get(tag, 0) + 1
        state_family_counts[row["state_family"]] = state_family_counts.get(row["state_family"], 0) + 1
        state_pressure_class_counts[row["state_pressure_class"]] = (
            state_pressure_class_counts.get(row["state_pressure_class"], 0) + 1
        )
        spill_policy_counts[row["spill_policy"]] = spill_policy_counts.get(row["spill_policy"], 0) + 1
        for unit in row["capillary_work_units"]:
            capillary_work_unit_counts[unit] = capillary_work_unit_counts.get(unit, 0) + 1
        for signal in row["pulseweave_pressure_signals"]:
            pulseweave_pressure_signal_counts[signal] = (
                pulseweave_pressure_signal_counts.get(signal, 0) + 1
            )
    return {
        "schema_version": SCHEMA_VERSION,
        "canonical_source_url": CANONICAL_SOURCE_URL,
        "query_manifest": "benchmarks/clickbench/queries.sql",
        "snapshot_date": "2026-06-18",
        "query_count": len(rows),
        "expected_query_count": EXPECTED_QUERY_COUNT,
        "runtime_status_counts": status_counts,
        "admitted_query_count": status_counts.get("admitted_current_runtime", 0),
        "implementation_required_count": status_counts.get("implementation_required", 0),
        "feature_gated_query_count": status_counts.get("feature_gated", 0),
        "blocker_counts": blocker_counts,
        "route_family_counts": route_family_counts,
        "operator_tag_counts": tag_counts,
        "state_budget_schema_version": STATE_BUDGET_SCHEMA_VERSION,
        "state_budget_required_count": sum(1 for row in rows if row["state_budget_required"]),
        "state_family_counts": state_family_counts,
        "state_pressure_class_counts": state_pressure_class_counts,
        "capillary_work_unit_counts": capillary_work_unit_counts,
        "pulseweave_pressure_signal_counts": pulseweave_pressure_signal_counts,
        "spill_policy_counts": spill_policy_counts,
        "spill_required_count": sum(1 for row in rows if row["spill_required"]),
        "spill_supported_count": sum(1 for row in rows if row["spill_supported"]),
        "fail_closed_if_spill_required_count": sum(
            1 for row in rows if row["fail_closed_if_spill_required"]
        ),
        "scale_fixture_strategy": scale_fixture_strategy(),
        "clickbench_olap_readiness_status": "all_queries_admitted_route_readiness",
        "benchmark_site_readiness_status": "ready_route_readiness_not_performance",
        "benchmark_site_readiness_fields_present": True,
        "memory_spill_diagnostic_status": "state_budget_declared_spill_fail_closed_no_spill_io",
        "site_readiness_claim_boundary": (
            "route readiness only; not a timing, performance, production, spill-runtime, "
            "larger-than-memory, or superiority claim"
        ),
        "claim_gate_status": "not_claim_grade",
        "performance_claim_allowed": False,
        "fallback_attempted": False,
        "external_engine_invoked": False,
        "coverage_boundary": (
            "local ClickBench query-family coverage map only; not a benchmark rerun, "
            "performance claim, or external-engine fallback"
        ),
        "rows": rows,
    }


def validate(report: dict[str, Any]) -> list[str]:
    blockers: list[str] = []
    rows = report["rows"]
    if report["query_count"] != EXPECTED_QUERY_COUNT:
        blockers.append(
            f"expected {EXPECTED_QUERY_COUNT} ClickBench queries, found {report['query_count']}"
        )
    if report.get("admitted_query_count") != EXPECTED_QUERY_COUNT:
        blockers.append("ClickBench route-readiness report must admit every canonical query")
    if report.get("implementation_required_count") != 0:
        blockers.append("ClickBench route-readiness report must have zero implementation-required rows")
    if report.get("feature_gated_query_count") != 0:
        blockers.append("ClickBench route-readiness report must have zero feature-gated rows")
    if not report.get("route_family_counts"):
        blockers.append("ClickBench report missing route_family_counts")
    if report.get("clickbench_olap_readiness_status") != "all_queries_admitted_route_readiness":
        blockers.append("ClickBench report missing all-query route-readiness status")
    if not report.get("state_pressure_class_counts"):
        blockers.append("ClickBench report missing state pressure class counts")
    if (
        report.get("memory_spill_diagnostic_status")
        != "state_budget_declared_spill_fail_closed_no_spill_io"
    ):
        blockers.append("ClickBench report missing memory/spill diagnostic status")
    if report.get("benchmark_site_readiness_fields_present") is not True:
        blockers.append("ClickBench report missing benchmark/site readiness fields")
    ids = [row["query_id"] for row in rows]
    if ids != [f"CB-Q{index:02d}" for index in range(1, EXPECTED_QUERY_COUNT + 1)]:
        blockers.append("ClickBench query IDs are not contiguous CB-Q01..CB-Q43")
    for row in rows:
        if not row["operator_tags"]:
            blockers.append(f"{row['query_id']} has no operator tags")
        if row["runtime_status"] not in {
            "admitted_current_runtime",
            "implementation_required",
            "feature_gated",
        }:
            blockers.append(f"{row['query_id']} has invalid runtime status {row['runtime_status']}")
        if row["fallback_attempted"] or row["external_engine_invoked"]:
            blockers.append(f"{row['query_id']} violates no-fallback/no-external-engine boundary")
        if row["runtime_status"] != "admitted_current_runtime" and row["blocker_id"] == "none":
            blockers.append(f"{row['query_id']} missing stable blocker/implementation ID")
        if row["runtime_status"] == "admitted_current_runtime" and row["blocker_id"] != "none":
            blockers.append(f"{row['query_id']} admitted row still has blocker_id")
        if row["state_budget_schema_version"] != STATE_BUDGET_SCHEMA_VERSION:
            blockers.append(f"{row['query_id']} has stale state-budget schema")
        if not row.get("state_pressure_class"):
            blockers.append(f"{row['query_id']} missing state pressure classification")
        if row["state_budget_required"] and row["state_pressure_class"] == "none":
            blockers.append(f"{row['query_id']} requires state but reports no pressure class")
        if not row["state_budget_required"] and row["state_pressure_class"] != "none":
            blockers.append(f"{row['query_id']} reports pressure class without state budget")
        if row["benchmark_site_readiness_status"] != "ready_route_readiness_not_performance":
            blockers.append(f"{row['query_id']} has invalid benchmark site readiness status")
        if row["timing_surface"] != "route_readiness_no_timing":
            blockers.append(f"{row['query_id']} has invalid ClickBench timing surface")
        if row["scale_fixture_strategy_id"] != "clickbench_scale_fixture_strategy_v1":
            blockers.append(f"{row['query_id']} missing scale fixture strategy linkage")
        if row["spill_required"] or row["spill_io_performed"]:
            blockers.append(f"{row['query_id']} unexpectedly reports spill execution")
        if row["spill_required"] and not row["fail_closed_if_spill_required"]:
            blockers.append(f"{row['query_id']} lacks fail-closed spill policy")
        if row["performance_claim_allowed"]:
            blockers.append(f"{row['query_id']} incorrectly allows a performance claim")
        if row["state_budget_required"] and not row["capillary_work_units"]:
            blockers.append(f"{row['query_id']} missing capillary work units")
        if row["state_budget_required"] and not row["pulseweave_pressure_signals"]:
            blockers.append(f"{row['query_id']} missing PulseWeave pressure signals")
    strategy = report.get("scale_fixture_strategy", {})
    if strategy.get("schema_version") != SCALE_FIXTURE_STRATEGY_SCHEMA_VERSION:
        blockers.append("ClickBench scale fixture strategy has invalid schema version")
    if strategy.get("default_pr_fast_lane_tier") != "small_deterministic_local":
        blockers.append("ClickBench scale fixture strategy must keep PR fast lane small/local")
    if strategy.get("max_parallelism_default") != DEFAULT_LOCAL_RUNTIME_MAX_PARALLELISM:
        blockers.append(
            "ClickBench scale fixture strategy must use the shared public local runtime default"
        )
    return blockers


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--queries", type=Path, default=DEFAULT_QUERY_MANIFEST)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()

    queries = split_sql_statements(args.queries.read_text(encoding="utf-8"))
    report = coverage_report(queries)
    blockers = validate(report)
    report["validation_passed"] = not blockers
    report["validation_blockers"] = blockers
    report["status"] = "passed" if not blockers else "blocked"
    report["blockers"] = blockers
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    if blockers:
        for blocker in blockers:
            print(f"clickbench coverage blocker: {blocker}")
        return 1
    print(
        "ClickBench OLAP runtime coverage validated: "
        f"{report['query_count']} queries, "
        f"{report['runtime_status_counts'].get('admitted_current_runtime', 0)} admitted, "
        f"{report['runtime_status_counts'].get('implementation_required', 0)} implementation-required"
    )
    output_path = args.output.resolve()
    try:
        output_ref = output_path.relative_to(ROOT)
    except ValueError:
        output_ref = output_path
    print(f"wrote {output_ref}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
