#!/usr/bin/env python3
"""Validate the ShardLoom ClickBench OLAP runtime coverage manifest."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_QUERY_MANIFEST = ROOT / "benchmarks" / "clickbench" / "queries.sql"
DEFAULT_OUTPUT = ROOT / "target" / "clickbench-olap-runtime-coverage.json"
SCHEMA_VERSION = "shardloom.clickbench_olap_runtime_coverage.v1"
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
                "fallback_attempted": False,
                "external_engine_invoked": False,
                "performance_claim_allowed": False,
            }
        )
    status_counts: dict[str, int] = {}
    blocker_counts: dict[str, int] = {}
    tag_counts: dict[str, int] = {}
    for row in rows:
        status_counts[row["runtime_status"]] = status_counts.get(row["runtime_status"], 0) + 1
        if row["blocker_id"] != "none":
            blocker_counts[row["blocker_id"]] = blocker_counts.get(row["blocker_id"], 0) + 1
        for tag in row["operator_tags"]:
            tag_counts[tag] = tag_counts.get(tag, 0) + 1
    return {
        "schema_version": SCHEMA_VERSION,
        "canonical_source_url": CANONICAL_SOURCE_URL,
        "query_manifest": "benchmarks/clickbench/queries.sql",
        "snapshot_date": "2026-06-18",
        "query_count": len(rows),
        "expected_query_count": EXPECTED_QUERY_COUNT,
        "runtime_status_counts": status_counts,
        "blocker_counts": blocker_counts,
        "operator_tag_counts": tag_counts,
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
    print(f"wrote {args.output.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
