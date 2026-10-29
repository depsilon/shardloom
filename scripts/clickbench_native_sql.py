# SPDX-License-Identifier: Apache-2.0
"""Explicit native dialect spelling for the immutable ClickBench fixture.

ClickHouse LENGTH counts bytes; ShardLoom's typed LENGTH counts Unicode code
points. The independent reference already declares byte-length semantics.
Implicit expression names differ between the specialized file provider and
shared relational binding. Explicit aliases preserve the frozen result names.
Keep the original workload and reference identities, and record the submitted
SQL separately. This finite benchmark adapter is not part of engine execution.
Custom query files retain exactly their supplied SQL.
"""

from __future__ import annotations

import hashlib
import json
from datetime import date


PINNED_QUERIES_SHA256 = "4afa04814edf3a4c52ff26fd87ea3b5dd92c7264b2d8d69ee718709f3df6f09b"
_PINNED_STATEMENTS_SHA256 = "5bd0df79d5b88c61331895d41de66b8a07ec8dc78f1c4ec5e30dba7b4ce0e95d"
_BYTE_LENGTH_QUERIES = {
    28: (
        "SELECT CounterID, AVG(length(URL)) AS l, COUNT(*) AS c FROM hits WHERE URL <> '' "
        "GROUP BY CounterID HAVING COUNT(*) > 100000 ORDER BY l DESC LIMIT 25",
        "SELECT CounterID, AVG(octet_length(URL)) AS l, COUNT(*) AS c FROM hits WHERE URL <> '' "
        "GROUP BY CounterID HAVING COUNT(*) > 100000 ORDER BY l DESC LIMIT 25",
    ),
    29: (
        r"SELECT REGEXP_REPLACE(Referer, '^https?://(?:www\.)?([^/]+)/.*$', '\1') AS k, "
        "AVG(length(Referer)) AS l, COUNT(*) AS c, MIN(Referer) FROM hits WHERE Referer <> '' "
        "GROUP BY k HAVING COUNT(*) > 100000 ORDER BY l DESC LIMIT 25",
        r"SELECT REGEXP_REPLACE(Referer, '^https?://(?:www\.)?([^/]+)/.*$', '\1') AS k, "
        "AVG(octet_length(Referer)) AS l, COUNT(*) AS c, MIN(Referer) AS min_Referer_3 "
        "FROM hits WHERE Referer <> '' GROUP BY k HAVING COUNT(*) > 100000 ORDER BY l DESC LIMIT 25",
    ),
}

# Only SELECT names change. In particular, ORDER BY and GROUP BY expressions
# retain their original bytes. This map is admitted only for the complete pinned
# statement vector, never by inspecting arbitrary user SQL or observed results.
_OUTPUT_ALIASES = {
    3: [("SUM(AdvEngineID)", "sum_AdvEngineID_0"), ("COUNT(*)", "count_all_1"),
        ("AVG(ResolutionWidth)", "avg_ResolutionWidth_2")],
    4: [("AVG(UserID)", "avg_UserID_0")],
    5: [("COUNT(DISTINCT UserID)", "count_distinct_UserID_0")],
    6: [("COUNT(DISTINCT SearchPhrase)", "count_distinct_SearchPhrase_0")],
    7: [("MIN(EventDate)", "min_EventDate_0"), ("MAX(EventDate)", "max_EventDate_1")],
    8: [("COUNT(*)", "count_all_1")],
    10: [("SUM(AdvEngineID)", "sum_AdvEngineID_1"),
         ("AVG(ResolutionWidth)", "avg_ResolutionWidth_3"),
         ("COUNT(DISTINCT UserID)", "count_distinct_UserID_4")],
    16: [("COUNT(*)", "count_all_1")],
    17: [("COUNT(*)", "count_all_2")],
    18: [("COUNT(*)", "count_all_2")],
    19: [("COUNT(*)", "count_all_3")],
    22: [("MIN(URL)", "min_URL_1")],
    23: [("MIN(URL)", "min_URL_1"), ("MIN(Title)", "min_Title_2"),
         ("COUNT(DISTINCT UserID)", "count_distinct_UserID_4")],
    30: [("SUM(ResolutionWidth)" if index == 0 else f"SUM(ResolutionWidth + {index})",
          f"sum_ResolutionWidth_{index}") for index in range(90)],
    31: [("SUM(IsRefresh)", "sum_IsRefresh_3"), ("AVG(ResolutionWidth)", "avg_ResolutionWidth_4")],
    32: [("SUM(IsRefresh)", "sum_IsRefresh_3"), ("AVG(ResolutionWidth)", "avg_ResolutionWidth_4")],
    33: [("SUM(IsRefresh)", "sum_IsRefresh_3"), ("AVG(ResolutionWidth)", "avg_ResolutionWidth_4")],
    35: [("1", "literal_1")],
    36: [(f"ClientIP - {index}", f"ClientIP_minus{index}") for index in range(1, 4)],
}

# The pinned dataset stores EventTime as epoch seconds and EventDate as epoch
# days. Use the existing explicit native integer-epoch functions and date bounds;
# do not ask typed binding to treat these integers as timestamp/date values.
_EPOCH_FUNCTIONS = {
    19: [("extract(minute FROM EventTime)", "extract_minute(EventTime)")],
    43: [("DATE_TRUNC('minute', EventTime)", "date_trunc_minute(EventTime)")],
}
_EPOCH_DATES = {literal: str((date.fromisoformat(literal) - date(1970, 1, 1)).days)
                for literal in ("2013-07-01", "2013-07-31", "2013-07-14", "2013-07-15")}

# The immutable independent reference specifies ascending complete group keys
# for aggregate-order ties. Declare that contract in SQL for every provider.
# Q18 retains first-occurrence group order; Q43 already orders by its sole key.
_GROUP_TIES = {
    8: ["AdvEngineID"], 9: ["RegionID"], 10: ["RegionID"],
    11: ["MobilePhoneModel"], 12: ["MobilePhone", "MobilePhoneModel"],
    13: ["SearchPhrase"], 14: ["SearchPhrase"],
    15: ["SearchEngineID", "SearchPhrase"], 16: ["UserID"],
    17: ["UserID", "SearchPhrase"], 19: ["UserID", "m", "SearchPhrase"],
    22: ["SearchPhrase"], 23: ["SearchPhrase"], 28: ["CounterID"], 29: ["k"],
    31: ["SearchEngineID", "ClientIP"], 32: ["WatchID", "ClientIP"],
    33: ["WatchID", "ClientIP"], 34: ["URL"], 35: ["literal_1", "URL"],
    36: ["ClientIP", "ClientIP_minus1", "ClientIP_minus2", "ClientIP_minus3"],
    37: ["URL"], 38: ["Title"], 39: ["URL"],
    40: ["TraficSourceID", "SearchEngineID", "AdvEngineID", "Src", "Dst"],
    41: ["URLHash", "EventDate"], 42: ["WindowClientWidth", "WindowClientHeight"],
}


def _statements_sha256(statements: list[str]) -> str:
    return hashlib.sha256(
        json.dumps(statements, ensure_ascii=True, separators=(",", ":")).encode()
    ).hexdigest()


def native_query_statements(
    statements: list[str], queries_sha256: str, *, input_format: str = "vortex",
) -> tuple[list[str], dict]:
    """Bind pinned byte lengths, integer epochs and explicit result names.

    Parquet uses shared relational binding, whose SQL declares the reference's
    output names, integer epoch domains and group tie keys explicitly. Existing
    Vortex callers retain their byte-length adapter. Values, thresholds, grouping
    and the independent reference's ordering contract remain unchanged.
    The source file hash and complete original statements must match before
    applying this mapping; no name-based rewriting of arbitrary SQL occurs.
    """
    if input_format not in ("vortex", "parquet"):
        raise ValueError("native ClickBench input format must be vortex or parquet")
    submitted = list(statements)
    byte_length_changes, alias_changes, epoch_changes = [], [], []
    if queries_sha256 == PINNED_QUERIES_SHA256:
        if len(statements) != 43:
            raise ValueError("pinned native ClickBench dialect requires 43 statements")
        for query_id, (original, equivalent) in _BYTE_LENGTH_QUERIES.items():
            if statements[query_id - 1] != original:
                raise ValueError(f"pinned native ClickBench statement {query_id} differs")
            submitted[query_id - 1] = equivalent
            byte_length_changes.append(query_id)
        if _statements_sha256(statements) != _PINNED_STATEMENTS_SHA256:
            raise ValueError("pinned native ClickBench statement vector differs")
    if queries_sha256 == PINNED_QUERIES_SHA256 and input_format == "parquet":
        for query_id, aliases in _OUTPUT_ALIASES.items():
            projection, separator, tail = submitted[query_id - 1].partition(" FROM hits")
            if not separator:
                raise ValueError(f"pinned native ClickBench query {query_id} has no source boundary")
            for expression, alias in aliases:
                if projection.count(expression) != 1:
                    raise ValueError(f"pinned native ClickBench query {query_id} has an ambiguous output")
                projection = projection.replace(expression, f"{expression} AS {alias}", 1)
            submitted[query_id - 1] = projection + separator + tail
            alias_changes.append(query_id)
        for query_id, replacements in _EPOCH_FUNCTIONS.items():
            for original, native in replacements:
                submitted[query_id - 1] = submitted[query_id - 1].replace(original, native)
            epoch_changes.append(query_id)
        for query_id in range(37, 44):
            for literal, days in _EPOCH_DATES.items():
                submitted[query_id - 1] = submitted[query_id - 1].replace(f"'{literal}'", days)
            epoch_changes.append(query_id)
        submitted[42] = submitted[42].replace(
            "GROUP BY date_trunc_minute(EventTime) ORDER BY date_trunc_minute(EventTime)",
            "GROUP BY M ORDER BY M")
        for query_id, keys in _GROUP_TIES.items():
            before, separator, limit = submitted[query_id - 1].partition(" LIMIT ")
            before += ", " + ", ".join(key + " ASC NULLS FIRST" for key in keys)
            submitted[query_id - 1] = before + separator + limit
    group_tie_changes = sorted(_GROUP_TIES) if alias_changes else []
    changed = sorted(set(byte_length_changes + alias_changes + epoch_changes + group_tie_changes))
    receipt = {
        "schema_version": "shardloom.clickbench.native_sql.v1",
        "source_queries_sha256": queries_sha256,
        "input_format": input_format,
        "translation": ("explicit_byte_length_epoch_types_output_names_and_group_ties" if alias_changes
                        else "explicit_utf8_byte_length" if changed else "as_supplied"),
        "changed_query_ids": changed,
        "byte_length_query_ids": byte_length_changes,
        "output_alias_query_ids": sorted(set(alias_changes + ([29] if byte_length_changes else []))),
        "integer_epoch_query_ids": sorted(set(epoch_changes)),
        "projected_alias_reference_query_ids": [43] if epoch_changes else [],
        "group_tie_query_ids": group_tie_changes,
        "submitted_statements_sha256": _statements_sha256(submitted),
    }
    return submitted, receipt
