# SPDX-License-Identifier: Apache-2.0
"""General aggregation over one complete public incremental source."""

from __future__ import annotations

import json

import shardloom as sl
from shardloom.errors import ShardLoomCommandError
from native_streaming_input_cases import declaration

SCHEMA = {"g": "utf8", "n": "int64", "s": "utf8", "f": "float64", "b": "bool"}
ROWS = [
    {"g": g, "n": n, "s": s, "f": f, "b": b}
    for g, n, s, f, b in [
        ("B", -2, "Z", 1e16, True), ("A", 5, "é", 3.5, False),
        ("B", None, "Z", -1e16, None), (None, 9, None, -0.0, True),
        ("A", 5, "é", None, False), ("B", 4, "ß", 1.0, True),
        (None, None, "", 0.0, None),
    ]
]
BATCHES = [[], ROWS[:2], ROWS[2:3], [], ROWS[3:6], ROWS[6:], []]


def oracle(rows, groups):
    buckets = {} if groups else {(): []}
    for row in rows:
        buckets.setdefault(tuple(row[name] for name in groups), []).append(row)
    result = []
    for key, members in buckets.items():
        numbers = [row["n"] for row in members if row["n"] is not None]
        floats = [row["f"] for row in members if row["f"] is not None]
        words = [row["s"] for row in members if row["s"] is not None]
        total, floating = 0.0, 0.0
        # Explicit sequential additions preserve this engine's floating contract.
        for value in numbers:
            total += float(value)
        for value in floats:
            floating += value
        result.append(dict(zip(groups, key), rows=len(members), present=len(numbers),
                           unique=len(set(numbers)), total=total if numbers else None,
                           mean=total / len(numbers) if numbers else None,
                           smallest=min(numbers) if numbers else None,
                           largest=max(numbers) if numbers else None,
                           words=len(set(words)), first=min(words) if words else None,
                           last=max(words) if words else None, floating=floating if floats else None,
                           flags=len({row["b"] for row in members if row["b"] is not None})))
    return result


def aggregate(query, groups):
    target = query.group_by(*groups) if groups else query
    return target.agg(rows="count(*)", present="count(n)", unique=sl.count_distinct("n"),
                      total="sum(n)", mean="avg(n)", smallest="min(n)", largest="max(n)",
                      words=sl.count_distinct("s"), first="min(s)", last="max(s)",
                      floating="sum(f)", flags=sl.count_distinct("b"))


def run(harness):
    policy = {"memory_gb": 1, "max_parallelism": 1}
    workspace = harness.output / "aggregate-small-spill"
    workspace.mkdir()
    spill = {"workspace": str(workspace), "quota_bytes": 64 << 20, "buffer_bytes": 1 << 20}
    harness.json(harness.logs / "aggregate-input-oracles.json", {
        "schema": SCHEMA, "batches": BATCHES,
        "grouped": oracle(ROWS, ["g"]), "scalar": oracle(ROWS, []),
        "compound": oracle(ROWS, ["g", "b"]), "native_grant_bytes": 1 << 30,
    })

    def frame(batches=BATCHES):
        trace = {"opened": 0, "batches": 0, "rows": 0, "ended": False, "closed": False}
        def produce():
            trace["opened"] += 1
            try:
                for batch in batches:
                    trace["batches"] += 1
                    trace["rows"] += len(batch)
                    yield batch
                trace["ended"] = True
            finally:
                trace["closed"] = True
        return harness.context.from_batches(produce, schema=SCHEMA, streaming=True), trace

    def completed(name, envelope, trace, stages):
        assert trace["opened"] == 1 and trace["ended"] and trace["closed"], trace
        harness.completed(name, envelope, batches=trace["batches"], rows=trace["rows"])
        assert envelope.field_int("relational_ordered_aggregate_stages") == stages
        assert list(workspace.iterdir()) == []

    def consume(name, query, trace, expected, spilled, stages=1, collect=False):
        requested = dict(policy, spill=spill) if spilled else policy
        if collect:
            result = query.collect(check=True, **requested)
            harness.values(name, result.result_rows, expected)
            envelope = result.envelope
        else:
            actual, schema = [], None
            with query.iter_batches(batch_rows=2, **requested) as iterator:
                for batch in iterator:
                    assert trace["ended"] and iterator.report is None
                    assert len(batch.result_rows) <= 2
                    if schema is None:
                        schema = batch.result_schema
                    assert batch.result_schema == schema
                    actual.extend(batch.result_rows)
                assert iterator.report is not None and iterator._process.poll() == 0
                envelope = iterator.report.envelope
            harness.values(name, actual, expected)
        completed(name, envelope, trace, stages if spilled else 0)

    for spilled in (False, True):
        for label, groups in [("grouped", ["g"]), ("scalar", []), ("compound", ["g", "b"])]:
            name = f"aggregate-{label}-spill-{spilled}"
            def check(name=name, groups=groups, spilled=spilled):
                source, trace = frame()
                query = aggregate(source, groups)
                assert trace["opened"] == 0
                consume(name, query, trace, oracle(ROWS, groups), spilled)
            harness.case(name, check)
        for label, batches, groups in [
            ("empty-scalar", [], []), ("empty-grouped", [[], []], ["g"]),
            ("null-scalar", [[dict.fromkeys(SCHEMA)], [], [dict.fromkeys(SCHEMA)]], []),
            ("null-grouped", [[dict.fromkeys(SCHEMA)], [], [dict.fromkeys(SCHEMA)]], ["g"]),
        ]:
            name = f"aggregate-{label}-spill-{spilled}"
            def check(name=name, batches=batches, groups=groups, spilled=spilled):
                source, trace = frame(batches)
                consume(name, aggregate(source, groups), trace,
                        oracle([row for batch in batches for row in batch], groups), spilled, collect=True)
            harness.case(name, check)

        name = f"aggregate-nested-spill-{spilled}"
        def nested(name=name, spilled=spilled):
            source, trace = frame()
            query = aggregate(source, ["g"]).aggregate("count(*) AS groups", "sum(rows) AS total")
            consume(name, query, trace, [{"groups": 3, "total": 7.0}], spilled, stages=2)
        harness.case(name, nested)

        name = f"aggregate-composed-spill-{spilled}"
        def composed(name=name, spilled=spilled):
            source, trace = frame()
            eligible = [row for row in ROWS if row["n"] is not None and row["n"] > 0]
            expected = [row for row in oracle(eligible, ["g"]) if row["total"] > 4]
            expected.sort(key=lambda row: row["total"], reverse=True)
            expected = [{"label": row["g"], "amount": row["total"]} for row in expected[:2]]
            query = aggregate(source.filter(sl.col("n") > 0), ["g"])
            query = query.filter(sl.col("total") > 4).sort("total", descending=True)
            query = query.select("g AS label", "total AS amount").limit(2)
            consume(name, query, trace, expected, spilled)
        harness.case(name, composed)

        name = f"aggregate-sql-spill-{spilled}"
        def sql(name=name, spilled=spilled):
            source, trace = frame()
            statement = f"SELECT g,COUNT(*) AS rows,COUNT(DISTINCT n) AS unique FROM '{source.source.uri}' GROUP BY g"
            requested = dict(declaration(source), spill=spill) if spilled else declaration(source)
            with harness.client.public_workflow_batches("sql", sql_statement=statement,
                                                        batch_rows=1, **requested) as iterator:
                actual = []
                for batch in iterator:
                    assert trace["ended"] and iterator.report is None
                    actual.extend(batch.result_rows)
                expected = [{key: row[key] for key in ("g", "rows", "unique")} for row in oracle(ROWS, ["g"])]
                harness.values(name, actual, expected)
                completed(name, iterator.report.envelope, trace, int(spilled))
        harness.case(name, sql)

    def writer():
        source, trace = frame()
        target = harness.output / "aggregate-small.vortex"
        result = aggregate(source, ["g"]).write_vortex(target, **dict(policy, spill=spill))
        completed("aggregate-small-write", result.envelope, trace, 1)
        reopened = harness.context.read_vortex(target).collect(check=True, **policy)
        harness.envelope("aggregate-small-reopen", reopened.envelope)
        harness.values("aggregate-small-reopen", reopened.result_rows, oracle(ROWS, ["g"]))
        return {"native_output": harness.artifact(target)}
    harness.case("aggregate-small-write", writer)

    for failure in ("producer", "schema", "overflow"):
        for limit in (False, True):
            name = f"aggregate-late-{failure}-zero-{limit}"
            def fail(name=name, failure=failure, limit=limit):
                trace = {"pulled": 0, "closed": False}
                def produce():
                    try:
                        trace["pulled"] += 1
                        yield [{"n": 1}]
                        trace["pulled"] += 1
                        if failure == "producer":
                            raise RuntimeError("late aggregate producer")
                        yield [{"wrong": 2}] if failure == "schema" else [{"n": (1 << 63) - 1}]
                    finally:
                        trace["closed"] = True
                query = harness.context.from_batches(produce(), schema={"n": "int64"}, streaming=True)
                if failure == "overflow":
                    query = query.select("n + 1 AS n")
                query = query.agg(rows="count(*)", unique=sl.count_distinct("n"))
                if limit:
                    query = query.limit(0)
                iterator = query.iter_batches(**dict(policy, spill=spill))
                error_type = {"producer": RuntimeError, "schema": ValueError, "overflow": ShardLoomCommandError}[failure]
                try:
                    with iterator:
                        assert list(iterator) == []
                except error_type as error:
                    if isinstance(error, ShardLoomCommandError):
                        harness.envelope(name, error.envelope, success=False)
                        assert "overflow" in json.dumps(error.envelope.raw)
                else:
                    raise AssertionError("aggregation hid a late input failure")
                assert trace == {"pulled": 2, "closed": True}
                assert iterator.report is None and iterator._process.poll() is not None
                assert list(workspace.iterdir()) == []
                return {"successful_prefix": False, "source_closed": True, "child_exited": True}
            harness.case(name, fail)
