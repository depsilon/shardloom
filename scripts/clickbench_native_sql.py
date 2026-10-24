# SPDX-License-Identifier: Apache-2.0
"""Explicit native dialect spelling for the immutable ClickBench fixture.

ClickHouse LENGTH counts bytes; ShardLoom's typed LENGTH counts Unicode code
points. The independent reference already declares byte-length semantics.
Keep the original workload and reference identities, and record the submitted
SQL separately. This finite benchmark adapter is not part of engine execution.
Custom query files retain exactly their supplied SQL.
"""

from __future__ import annotations

import hashlib
import json


PINNED_QUERIES_SHA256 = "4afa04814edf3a4c52ff26fd87ea3b5dd92c7264b2d8d69ee718709f3df6f09b"
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


def native_query_statements(statements: list[str], queries_sha256: str) -> tuple[list[str], dict]:
    """Bind only the pinned byte-length equivalents and keep result names exact.

    Q29's previously implicit MIN name becomes explicit when the complete SQL
    enters typed binding. No values, thresholds, grouping or ordering change.
    The source file hash and complete original statements must match before
    applying this mapping; no name-based rewriting of arbitrary SQL occurs.
    """
    submitted = list(statements)
    changed = []
    if queries_sha256 == PINNED_QUERIES_SHA256:
        if len(statements) != 43:
            raise ValueError("pinned native ClickBench dialect requires 43 statements")
        for query_id, (original, equivalent) in _BYTE_LENGTH_QUERIES.items():
            if statements[query_id - 1] != original:
                raise ValueError(f"pinned native ClickBench statement {query_id} differs")
            submitted[query_id - 1] = equivalent
            changed.append(query_id)
    receipt = {
        "schema_version": "shardloom.clickbench.native_sql.v1",
        "source_queries_sha256": queries_sha256,
        "translation": "explicit_utf8_byte_length" if changed else "as_supplied",
        "changed_query_ids": changed,
        "submitted_statements_sha256": hashlib.sha256(
            json.dumps(submitted, ensure_ascii=True, separators=(",", ":")).encode()
        ).hexdigest(),
    }
    return submitted, receipt
