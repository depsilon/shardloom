# SPDX-License-Identifier: Apache-2.0
"""Traditional workloads as SQL declarations, independent of engine lifecycle.

The query semantics originate in the existing benchmark's comparison workload.
An engine must admit each complete declaration or report its diagnostic. A
workload name never selects a private runtime implementation.
"""
from __future__ import annotations

from dataclasses import dataclass
from string import Formatter
from typing import Any, Callable, Mapping


@dataclass(frozen=True)
class Workload:
    statements: tuple[str, ...]
    result_kind: str = "scalar"
    keys: tuple[str, ...] = ()
    write_statement: str | None = None

    @property
    def source_roles(self) -> tuple[str, ...]:
        declarations = self.statements + ((self.write_statement,) if self.write_statement else ())
        return tuple(sorted({field for sql in declarations
                             for _, field, _, _ in Formatter().parse(sql) if field}))

    def bind(self, sources: Mapping[str, str]) -> tuple[tuple[str, ...], str | None]:
        missing = set(self.source_roles) - sources.keys()
        if missing:
            raise ValueError(f"workload source declarations are missing: {sorted(missing)}")
        return (tuple(sql.format_map(sources) for sql in self.statements),
                self.write_statement.format_map(sources) if self.write_statement else None)

    def result(self, batches: list[list[dict[str, Any]]], round_float: Callable) -> Any:
        """Normalize already-computed result cells for the comparison contract."""
        if len(batches) != len(self.statements):
            raise ValueError("workload omitted a complete statement result")
        rows = batches[0]
        if self.result_kind in ("scalar", "distinct", "nested"):
            if len(rows) != 1:
                raise ValueError("scalar benchmark declaration must produce exactly one row")
            row = rows[0]
            if self.result_kind == "distinct":
                return {"distinct_category_count": int(row["distinct_category_count"])}
            result = {"row_count": int(row["row_count"]), "metric_sum": round_float(row["metric_sum"])}
            if self.result_kind == "nested":
                result["flagged"] = int(row["flagged"])
            return result
        if self.result_kind == "high_cardinality":
            if len(batches[1]) != 1:
                raise ValueError("distinct statement must produce exactly one row")
            return {"distinct_category_count": int(batches[1][0]["distinct_category_count"]),
                    "groups": self._groups(rows, round_float)}
        if self.result_kind == "groups":
            return self._groups(rows, round_float)
        if self.result_kind in ("top", "rank"):
            result = [{"id": int(row["id"]), "metric": round_float(row["metric"]),
                       **({"group_key": int(row["group_key"]), "rank": int(row["rank"])}
                          if self.result_kind == "rank" else {})} for row in rows]
            return sorted(result, key=(lambda row: (row["group_key"], row["rank"], row["id"]))
                          if self.result_kind == "rank" else (lambda row: (-row["metric"], row["id"])))
        if self.result_kind == "etl":
            result = [{"dim_label": str(row["dim_label"]), "bucket": int(row["bucket"]),
                       "row_count": int(row["row_count"]), "metric_sum": round_float(row["metric_sum"]),
                       "weighted_sum": round_float(row["weighted_sum"])} for row in rows]
            return sorted(result, key=lambda row: (-row["weighted_sum"], row["dim_label"], row["bucket"]))
        raise ValueError(f"unknown comparison result shape: {self.result_kind}")

    def _groups(self, rows, round_float):
        result = [{**{key: str(row[key]) if key in ("dim_label", "category") else int(row[key])
                      for key in self.keys}, "row_count": int(row["row_count"]),
                   "metric_sum": round_float(row["metric_sum"])} for row in rows]
        return sorted(result, key=lambda row: tuple(row[key] for key in self.keys))


_CLEAN = (
    "SELECT id,raw_event_time,TRY_CAST(dirty_numeric AS double) AS clean_numeric,category "
    "FROM {fact} WHERE TRY_STRPTIME(raw_event_time, '%Y-%m-%dT%H:%M:%SZ') IS NOT NULL "
    "AND TRY_CAST(dirty_numeric AS double) IS NOT NULL AND CAST(dirty_flag AS varchar) = 'Y' "
    "AND TRY_CAST(dirty_numeric AS double) >= 500"
)
_RANK = ("SELECT group_key,id,metric,ROW_NUMBER() OVER "
         "(PARTITION BY group_key ORDER BY metric DESC,id ASC) AS rank FROM {fact}")

WORKLOADS = {
    "csv/file ingest": Workload(("SELECT COUNT(*) AS row_count,SUM(metric) AS metric_sum FROM {fact}",)),
    "selective filter": Workload(("SELECT COUNT(*) AS row_count,SUM(metric) AS metric_sum FROM {fact} WHERE flag = 1 AND value >= 5000",)),
    "group by aggregation": Workload(("SELECT group_key,COUNT(*) AS row_count,SUM(metric) AS metric_sum FROM {fact} GROUP BY group_key",), "groups", ("group_key",)),
    "sort and top-k": Workload(("SELECT id,metric FROM {fact} ORDER BY metric DESC,id ASC LIMIT 10",), "top"),
    "hash join": Workload(("SELECT d.dim_label AS dim_label,COUNT(*) AS row_count,SUM(f.metric) AS metric_sum FROM {fact} AS f JOIN {dim} AS d ON f.dim_key = d.dim_key GROUP BY d.dim_label",), "groups", ("dim_label",)),
    "wide projection": Workload(("SELECT COUNT(*) AS row_count,SUM(group_key) AS metric_sum FROM (SELECT id,group_key,category FROM {fact}) AS projected",)),
    "distinct count": Workload(("SELECT COUNT(DISTINCT category) AS distinct_category_count FROM {fact}",), "distinct"),
    "filter + projection + limit": Workload(("SELECT COUNT(*) AS row_count,SUM(value) AS metric_sum FROM (SELECT id,value,category FROM {fact} WHERE flag = 1 AND value >= 5000 ORDER BY id ASC LIMIT 100) AS selected",)),
    "multi-key group by": Workload(("SELECT group_key,category,COUNT(*) AS row_count,SUM(metric) AS metric_sum FROM {fact} GROUP BY group_key,category",), "groups", ("group_key", "category")),
    "join + aggregate": Workload(("SELECT d.dim_label AS dim_label,f.category AS category,COUNT(*) AS row_count,SUM(f.metric) AS metric_sum FROM {fact} AS f JOIN {dim} AS d ON f.dim_key = d.dim_key WHERE f.value >= 2500 GROUP BY d.dim_label,f.category",), "groups", ("dim_label", "category")),
    "row number window": Workload(("SELECT group_key,id,metric,rank FROM (" + _RANK + ") AS ranked WHERE rank = 1",), "rank"),
    "partition pruning": Workload(("SELECT COUNT(*) AS row_count,SUM(metric) AS metric_sum FROM {fact} WHERE event_date >= '2024-03-01' AND event_date < '2024-06-01'",)),
    "many-small-files scan": Workload(("SELECT COUNT(*) AS row_count,SUM(metric) AS metric_sum FROM {parts}",)),
    "null-heavy aggregate": Workload(("SELECT COUNT(TRY_CAST(nullable_metric_00 AS double)) AS row_count,SUM(TRY_CAST(nullable_metric_00 AS double)) AS metric_sum FROM {fact}",)),
    "high-cardinality string group/distinct": Workload((
        "SELECT category,COUNT(*) AS row_count,SUM(metric) AS metric_sum FROM {fact} GROUP BY category ORDER BY category LIMIT 100",
        "SELECT COUNT(DISTINCT category) AS distinct_category_count FROM {fact}",
    ), "high_cardinality", ("category",)),
    "top-N per group": Workload(("SELECT group_key,id,metric,rank FROM (" + _RANK + ") AS ranked WHERE rank <= 3",), "rank"),
    "clean/cast/filter/write": Workload(("SELECT COUNT(*) AS row_count,SUM(clean_numeric) AS metric_sum FROM (" + _CLEAN + ") AS cleaned",), write_statement=_CLEAN),
    "malformed timestamp / dirty CSV": Workload(("SELECT COUNT(*) AS row_count,SUM(TRY_CAST(dirty_numeric AS double)) AS metric_sum FROM {fact} WHERE TRY_STRPTIME(raw_event_time, '%Y-%m-%dT%H:%M:%SZ') IS NOT NULL AND TRY_CAST(dirty_numeric AS double) IS NOT NULL",)),
    "small change over large base": Workload((
        "WITH overlay AS (SELECT id,op,TRY_CAST(metric AS double) AS metric FROM {delta}), "
        "base_kept AS (SELECT f.id,f.metric FROM {fact} AS f LEFT JOIN overlay AS o ON f.id = o.id WHERE o.id IS NULL), "
        "merged AS (SELECT id,metric FROM base_kept UNION ALL SELECT id,metric FROM overlay WHERE op <> 'delete') "
        "SELECT COUNT(*) AS row_count,SUM(metric) AS metric_sum FROM merged",
    )),
    "nested JSON field scan": Workload(("SELECT COUNT(*) AS row_count,SUM(CAST(JSON_EXTRACT(nested_payload, '$.metrics.score') AS double)) AS metric_sum,SUM(CASE WHEN CAST(JSON_EXTRACT(nested_payload, '$.event.flag') AS boolean) THEN 1 ELSE 0 END) AS flagged FROM {fact}",), "nested"),
    "scale stress skewed join aggregation": Workload(("SELECT f.group_key % 10 AS skew_key,COUNT(*) AS row_count,SUM(f.metric) AS metric_sum FROM {fact} AS f JOIN {dim} AS d ON f.dim_key = d.dim_key GROUP BY skew_key",), "groups", ("skew_key",)),
    "scale stress multi-stage etl": Workload(("SELECT d.dim_label AS dim_label,f.group_key % 10 AS bucket,COUNT(*) AS row_count,SUM(f.metric) AS metric_sum,SUM(f.metric * (d.weight + 1)) AS weighted_sum FROM {fact} AS f JOIN {dim} AS d ON f.dim_key = d.dim_key WHERE f.value >= 2500 GROUP BY d.dim_label,bucket ORDER BY weighted_sum DESC,dim_label ASC,bucket ASC LIMIT 20",), "etl"),
}
