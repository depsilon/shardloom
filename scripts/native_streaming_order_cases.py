# SPDX-License-Identifier: Apache-2.0
"""Complete-source ordering/range checks through real public terminals."""

from __future__ import annotations

from functools import cmp_to_key
import json

import shardloom as sl
from shardloom.errors import ShardLoomCommandError
from native_streaming_input_cases import declaration

SCHEMA = {"n": "int64", "s": "utf8", "id": "int64"}
ROWS = [
    {"n": [None, -(1 << 63), (1 << 63) - 1, -1, 0, (1 << 53) + 1][row % 6],
     "s": ["東京", None, "", 'λ"\n', "a\0z"][row % 5], "id": row}
    for row in range(257)
]
BATCHES = [[], *[ROWS[start:start + 17] for start in range(0, len(ROWS), 17)], []]


def ordered(rows, keys):
    def compare(left, right):
        for field, descending, first in keys:
            a, b = left[field], right[field]
            if a is None or b is None:
                result = 0 if a is b else (-1 if (a is None) == first else 1)
            else:
                result = (a > b) - (a < b)
                if descending:
                    result = -result
            if result:
                return result
        return 0
    return sorted(rows, key=cmp_to_key(compare))


def run(harness):
    context = harness.context
    resources = {"memory_gb": 1, "max_parallelism": 1}
    # Freeze every input value and the independent stable oracle before demand.
    harness.json(harness.logs / "ordering-input-oracle.json", {
        "batches": BATCHES,
        "multikey": ordered(ROWS, [("n", True, False), ("s", False, True)]),
        "native_public_memory_grant_bytes": 1 << 30,
        "larger_than_grant_public_claim": False,
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
        return context.from_batches(produce, schema=SCHEMA, streaming=True), trace

    def completed(name, envelope, trace, *, ordering_rows):
        assert trace["opened"] == 1 and trace["ended"] and trace["closed"], trace
        harness.completed(name, envelope, batches=trace["batches"], rows=trace["rows"])
        assert envelope.field_int("native_input_ordering_rows_detached") == ordering_rows

    def collect(name, transform, expected, *, ordering_rows, batches=BATCHES):
        source, trace = frame(batches)
        query = transform(source)
        assert trace["opened"] == 0
        result = query.collect(check=True, **resources)
        harness.values(name, result.result_rows, expected)
        completed(name, result.envelope, trace, ordering_rows=ordering_rows)

    primary = [("n", True, False)]
    secondary = [("s", False, True)]
    for name, transform, expected, copied in [
        ("order-dataframe", lambda q: q.sort("n", descending=True, nulls="last"),
         ordered(ROWS, primary), len(ROWS)),
        ("order-nested", lambda q: q.sort("s", nulls="first").sort("n", descending=True, nulls="last"),
         ordered(ordered(ROWS, secondary), primary), 2 * len(ROWS)),
        ("order-limit-before", lambda q: q.limit(41).sort("n", descending=True, nulls="last"),
         ordered(ROWS[:41], primary), 41),
        ("order-limit-after", lambda q: q.sort("n", descending=True, nulls="last").limit(41),
         ordered(ROWS, primary)[:41], len(ROWS)),
        ("order-limit-zero", lambda q: q.sort("n", nulls="first").limit(0), [], len(ROWS)),
        ("limit-zero-before-order", lambda q: q.limit(0).sort("n", nulls="first"), [], 0),
        ("global-range", lambda q: q.limit(41), ROWS[:41], 0),
    ]:
        harness.case(name, lambda name=name, transform=transform, expected=expected, copied=copied:
                     collect(name, transform, expected, ordering_rows=copied))

    def composed():
        eligible = [row for row in ROWS if row["id"] > 7]
        expected = [{"value": row["n"], "label": row["s"], "ordinal": row["id"]}
                    for row in ordered(eligible, [("n", False, True)])
                    if row["n"] is not None and row["n"] > 0]
        collect("order-filter-project", lambda q: q.filter(sl.col("id") > 7)
                .select("n AS value", "s AS label", "id AS ordinal").sort("value", nulls="first")
                .filter(sl.col("value") > 0), expected, ordering_rows=len(eligible))
    harness.case("order-filter-project", composed)

    for offset, count in [(16, 23), (256, 3), (300, 1), (0, 0)]:
        name = f"order-sql-range-{offset}-{count}"
        def sql_range(name=name, offset=offset, count=count):
            source, trace = frame()
            statement = (f"SELECT * FROM '{source.source.uri}' "
                         f"ORDER BY n DESC NULLS LAST,s ASC NULLS FIRST LIMIT {count} OFFSET {offset}")
            with harness.client.public_workflow_batches("sql", sql_statement=statement,
                                                       batch_rows=7, **declaration(source)) as iterator:
                actual = []
                for batch in iterator:
                    assert len(batch.result_rows) <= 7 and trace["ended"]
                    assert iterator.report is None
                    actual.extend(batch.result_rows)
                assert iterator.report is not None and iterator._process.poll() == 0
                harness.values(name, actual, ordered(ROWS, primary + secondary)[offset:offset + count])
                completed(name, iterator.report.envelope, trace, ordering_rows=len(ROWS))
        harness.case(name, sql_range)

    for mode, batches, transform, expected, copied in [
        ("empty", [], lambda q: q.sort("n", nulls="last"), [], 0),
        ("empty-batches", [[], [], []], lambda q: q.sort("n", nulls="last"), [], 0),
        ("all-null", [[dict.fromkeys(SCHEMA)], [], [dict.fromkeys(SCHEMA)]],
         lambda q: q.sort("n", nulls="last"), [dict.fromkeys(SCHEMA)] * 2, 2),
        ("filtered", BATCHES, lambda q: q.filter("FALSE").sort("n", nulls="last"), [], 0),
    ]:
        name = "order-" + mode
        harness.case(name, lambda name=name, batches=batches, transform=transform, expected=expected, copied=copied:
                     collect(name, transform, expected, ordering_rows=copied, batches=batches))

    def writer():
        source, trace = frame()
        target = harness.output / "ordered.vortex"
        result = source.sort("s", nulls="first").sort("n", descending=True, nulls="last").write_vortex(target, **resources)
        completed("order-write", result.envelope, trace, ordering_rows=2 * len(ROWS))
        reopened = context.read_vortex(target).collect(check=True, **resources)
        harness.envelope("order-reopen", reopened.envelope)
        harness.values("order-reopen", reopened.result_rows, ordered(ordered(ROWS, secondary), primary))
        before = harness.artifact(target)
        source, trace = frame()
        try:
            source.sort("n", nulls="last").write_vortex(target, allow_overwrite=True, **resources)
        except ShardLoomCommandError as error:
            harness.envelope("order-existing-destination", error.envelope, success=False)
            assert "atomic generation-conditional replacement is unavailable" in json.dumps(error.envelope.raw)
        else:
            raise AssertionError("existing output replacement unexpectedly succeeded")
        assert trace["opened"] == 0 and harness.artifact(target) == before
        return {"artifact": before, "existing_destination_preserved": True,
                "replacement_support": "rejected_before_input_demand"}
    harness.case("order-write", writer)

    def late_failure(name, mode, count, sort):
        trace = {"pulled": 0, "closed": False}
        def produce():
            try:
                trace["pulled"] += 1
                yield [{"n": 1}]
                trace["pulled"] += 1
                if mode == "producer":
                    raise RuntimeError("late range sentinel")
                yield [{"wrong": 2}] if mode == "schema" else [{"n": (1 << 63) - 1}]
            finally:
                trace["closed"] = True
        query = context.from_batches(produce(), schema={"n": "int64"}, streaming=True)
        if mode == "expression":
            query = query.select("n + 1 AS n")
        if sort:
            query = query.sort("n")
        query = query.limit(count)
        error_type = {"producer": RuntimeError, "schema": ValueError, "expression": ShardLoomCommandError}[mode]
        iterator = query.iter_batches(**resources)
        actual = []
        try:
            with iterator:
                for batch in iterator:
                    actual.extend(batch.result_rows)
        except error_type as error:
            if isinstance(error, ShardLoomCommandError):
                harness.envelope(name, error.envelope, success=False)
                assert "overflow" in json.dumps(error.envelope.raw)
        else:
            raise AssertionError("range hid a late upstream failure")
        assert trace == {"pulled": 2, "closed": True}
        expected = [] if sort or count == 0 else [{"n": 2 if mode == "expression" else 1}]
        harness.values(name, actual, expected)
        assert iterator.report is None and iterator._process.poll() is not None
        return {"successful_prefix": False, "source_closed": True, "child_exited": True}

    for mode in ("producer", "schema", "expression"):
        for count in (0, 1):
            for sort in (False, True):
                name = f"range-late-{mode}-{count}-sort-{sort}"
                harness.case(name, lambda name=name, mode=mode, count=count, sort=sort:
                             late_failure(name, mode, count, sort))
