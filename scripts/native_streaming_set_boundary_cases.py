# SPDX-License-Identifier: Apache-2.0
"""Type alignment, dynamic consumers and interrupted finite set boundaries."""

import json

from shardloom.query import LazyFrame
from native_streaming_dynamic_cases import frame, ROWS as PIVOT_ROWS
from native_streaming_set_cases import KINDS, LEFT, RIGHT, POLICY, combine, complete, oracle


def run(harness, spill, clean):
    alignment(harness)
    dynamic_consumers(harness, spill, clean)
    interrupted(harness, spill, clean)


def alignment(harness):
    for kind in KINDS:
        for surface in ("dataframe", "sql"):
            for terminal in ("collect", "batches"):
                name = f"set-positional-{kind}-{surface}-{terminal}"
                def check(name=name, kind=kind, surface=surface, terminal=terminal):
                    left, a = frame(harness, LEFT, schema={"n": "int16", "s": "utf8"})
                    renamed = [{"other": row["n"], "label": row["s"]} for row in RIGHT]
                    right, b = frame(harness, renamed, schema={"other": "int32", "label": "utf8"})
                    query = combine(left, right, kind, surface)
                    if terminal == "collect":
                        result = query.collect(check=True, **POLICY)
                        actual, schema, envelope = result.result_rows, result.result_schema, result.envelope
                    else:
                        with query.iter_batches(batch_rows=1, **POLICY) as iterator:
                            actual = [row for batch in iterator for row in batch.result_rows]
                            assert iterator.report is not None and iterator._process.poll() == 0
                            schema, envelope = iterator._schema, iterator.report.envelope
                    assert [(key, dtype.name, dtype.nullable) for key, dtype in schema] == [
                        ("n", "int32", True), ("s", "utf8", True)]
                    harness.values(name, actual, oracle(LEFT, RIGHT, kind))
                    complete(harness, name, envelope, [a, b], [True, True])
                harness.case(name, check)


def dynamic_consumers(harness, spill, clean):
    payload = json.dumps({"aggregate": "sum", "index_column": "entity",
                          "pivot_column": "category", "value_column": "amount"}, separators=(",", ":"))
    for kind in KINDS:
        for stored in (False, True):
            name = f"set-before-pivot-{kind}-spill-{stored}"
            def check(name=name, kind=kind, stored=stored):
                left, a = frame(harness)
                right, b = frame(harness)
                sets = combine(left, right, kind)
                query = LazyFrame.from_sql(f"SELECT * FROM PIVOT(({sets._relation_statement()}), '{payload}') AS p",
                                    harness.client, source_bindings=sets._declared_sources())
                rows = oracle(PIVOT_ROWS, PIVOT_ROWS, kind)
                categories = sorted({row["category"] for row in rows})
                expected = []
                for entity in sorted({row["entity"] for row in rows}):
                    expected.append({"entity": entity, **{
                        "pivot_" + category: (float(sum(values)) if values else None)
                        for category in categories
                        for values in [[row["amount"] for row in rows
                                        if row["entity"] == entity and row["category"] == category]]}})
                result = query.collect(check=True, **(dict(POLICY, spill=spill) if stored else POLICY))
                harness.values(name, result.result_rows, expected)
                assert list(result.result_columns) == ["entity", *("pivot_" + key for key in categories)]
                assert result.envelope.field("relational_schema_binding") == "during_execution"
                assert result.envelope.field_int("relational_dynamic_schema_stages") == 1
                complete(harness, name, result.envelope, [a, b], [True, True])
                clean()
            harness.case(name, check)


def interrupted(harness, spill, clean):
    for kind in KINDS:
        for stored in (False, True):
            name = f"set-early-close-{kind}-spill-{stored}"
            def close(name=name, kind=kind, stored=stored):
                events = [[], []]
                def source(side):
                    def producer():
                        events[side].append("opened")
                        try:
                            for number in range(20):
                                yield [{"n": number + side * 10}]
                            events[side].append("ended")
                        finally:
                            events[side].append("closed")
                    return harness.context.from_batches(producer, schema={"n": "int64"}, streaming=True)
                query = combine(source(0), source(1), kind)
                requested = dict(POLICY, spill=spill) if stored else POLICY
                with query.iter_batches(batch_rows=1, **requested) as iterator:
                    retained = next(iterator)
                    assert iterator.report is None
                assert iterator.report is None and iterator._process.poll() is not None
                harness.values(name, retained.result_rows, [{"n": 10 if kind == "intersect" else 0}])
                # Ordered membership consumes both sources before output; an
                # abandoned output stream is still provisional after input ends.
                if stored and kind != "union_all":
                    assert events == [["opened", "ended", "closed"]] * 2, events
                else:
                    assert any("opened" in trace and "ended" not in trace for trace in events), events
                assert all(not trace or trace[-1] == "closed" for trace in events), events
                clean()
                return {"producer_events": events, "completion_report": None,
                        "ordered_membership_strategy": stored and kind != "union_all"}
            harness.case(name, close)

        for side in (0, 1):
            for terminal in ("collect", "zero", "write"):
                name = f"set-schema-failure-{kind}-side-{side}-{terminal}"
                def schema_failure(name=name, kind=kind, side=side, terminal=terminal):
                    events = []
                    def broken():
                        events.append("opened")
                        try:
                            yield LEFT[:2]
                            events.append("invalid")
                            yield [{"wrong": 1}]
                        finally:
                            events.append("closed")
                    source = harness.context.from_batches(broken, schema={"n": "int64", "s": "utf8"}, streaming=True)
                    other, _ = frame(harness, RIGHT, schema={"n": "int64", "s": "utf8"})
                    query = combine(source, other, kind) if side == 0 else combine(other, source, kind)
                    target = harness.output / (name + ".vortex")
                    before = set(harness.output.iterdir())
                    try:
                        if terminal == "collect":
                            query.collect(check=True, spill=spill, **POLICY)
                        elif terminal == "write":
                            query.write_vortex(target, spill=spill, **POLICY)
                        else:
                            with query.limit(0).iter_batches(spill=spill, **POLICY) as iterator:
                                assert list(iterator) == []
                    except ValueError as error:
                        assert "schema" in str(error) or "field" in str(error), str(error)
                    else:
                        raise AssertionError("set operation certified invalid late input")
                    assert events == ["opened", "invalid", "closed"]
                    assert set(harness.output.iterdir()) == before and not target.exists()
                    clean()
                    return {"producer_events": events, "no_final_publication": True}
                harness.case(name, schema_failure)
