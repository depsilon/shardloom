# SPDX-License-Identifier: Apache-2.0
"""Public proof separates dynamic pivot state from native input replay and cleanup."""

from __future__ import annotations

import json

import shardloom as sl
from shardloom._result_schema import ResultType as T
from shardloom.errors import ShardLoomCommandError
from native_streaming_dynamic_subquery_cases import consume
from native_streaming_unary_cases import frame

ROWS, WIDTH, BATCH = 1025, 2048, 127
SCHEMA = {"entity": "utf8", "category": "utf8", "amount": "int64"}
FIELDS = (("n", T("int64", True)), ("answer", T("bool", True)))


def run(harness):
    workspace = harness.output / "dynamic-subquery-pressure-spill"
    workspace.mkdir()
    foreign = workspace / "foreign"
    foreign.write_bytes(b"preserve")
    spill = {"workspace": str(workspace), "quota_bytes": 128 << 20, "buffer_bytes": 1 << 20}
    harness.json(harness.logs / "dynamic-subquery-pressure-fixtures.json", {
        "rows": ROWS, "width": WIDTH, "batch_rows": BATCH, "schema": SCHEMA,
        "entity": "eight_digit_row_ordinal_then_2048_p", "category": "a", "amount": 1,
        "outer_parameters": [ROWS, 0], "expected": [{"n": ROWS, "answer": True}, {"n": 0, "answer": False}],
        "small_grant": 32 << 20, "ample_grant": 128 << 20, "spill": spill,
    })

    def clean():
        assert foreign.read_bytes() == b"preserve" and list(workspace.iterdir()) == [foreign]

    def declaration(failure=None, count=ROWS, width=WIDTH):
        left, a = frame(harness, [{"n": count}, {"n": 0}], {"n": "int64"}, width=2)
        b = dict(opened=0, ended=False, closed=False, batches=0, rows=0, actual_spill_seen=False)

        def produce():
            b["opened"] += 1
            try:
                for start in range(0, count, BATCH):
                    runs = list(workspace.glob("shardloom-query-*/*.vortex"))
                    b["actual_spill_seen"] |= bool(runs)
                    if b["batches"] == 6 and failure == "producer":
                        assert runs, "the injected producer failure must follow actual native spill"
                        raise LookupError("dynamic subquery spill producer sentinel")
                    rows = [{"entity": f"{index:08}" + "p" * width, "category": "a", "amount": 1}
                            for index in range(start, min(start + BATCH, count))]
                    b["batches"] += 1
                    b["rows"] += len(rows)
                    yield rows
                b["ended"] = True
            finally:
                b["closed"] = True

        right = harness.context.from_batches(produce, schema=SCHEMA, streaming=True)
        dynamic = right.filter(sl.outer("n") >= 0).pivot_table(
            index="entity", columns="category", values="amount", aggfunc="sum")
        query = left.with_column("answer", sl.col("n").isin_source(dynamic.agg(n="COUNT(*)"), "n"))
        return query, [a, b], [{"n": count, "answer": True}, {"n": 0, "answer": False}]

    for mode in ("resident-denied", "resident-ample", "spilled"):
        name = "dynamic-subquery-pressure-" + mode

        def pressure(name=name, mode=mode):
            requested = dict(memory_bytes=(128 if mode == "resident-ample" else 32) << 20, max_parallelism=1)
            if mode == "spilled":
                requested["spill"] = spill
            query, traces, expected = declaration()
            if mode == "resident-denied":
                target = harness.output / (name + ".vortex")
                before = set(harness.output.iterdir())
                try:
                    query.write_vortex(target, **requested)
                except ShardLoomCommandError as error:
                    harness.envelope(name, error.envelope, success=False)
                    assert "reservation denied" in json.dumps(error.envelope.raw), error.envelope.raw
                else:
                    raise AssertionError("dynamic pivot/replay exceeded the predeclared resident grant")
                assert all(trace["closed"] for trace in traces)
                assert set(harness.output.iterdir()) == before and not target.exists()
                clean()
                fresh, fresh_traces, wanted = declaration(count=2, width=8)
                consume(harness, name + "-fresh", fresh, wanted, FIELDS, fresh_traces, requested=requested)
            else:
                report = consume(harness, name, query, expected, FIELDS, traces, terminal="batches", requested=requested)
                envelope = report.envelope
                for suffix, expected_value in (("sources", 1), ("passes", 1), ("rows", ROWS),
                                               ("prefilled_sources", 0), ("spilled_sources", int(mode == "spilled"))):
                    assert envelope.field_int("native_input_replay_" + suffix) == expected_value
                assert envelope.field_int("relational_dynamic_schema_stages") == 2
                assert envelope.field_int("relational_spilled_pivot_stages", 0) == (2 if mode == "spilled" else 0)
                if mode == "spilled":
                    assert traces[1]["actual_spill_seen"]
                    assert envelope.field_int("relational_spilled_pivot_input_rows") == 2 * ROWS
                    assert envelope.field_int("relational_spilled_pivot_index_rows") == 2 * ROWS
                    assert envelope.field_bool("relational_spill_owned_cleanup_completed") is True
            clean()
            return {"traces": traces, "grant": requested["memory_bytes"], "resident_denial": mode == "resident-denied"}
        harness.case(name, pressure)

    for failure in ("producer", "quota"):
        for terminal in ("collect", "fanout"):
            name = f"dynamic-subquery-pressure-{failure}-{terminal}"

            def fail(name=name, failure=failure, terminal=terminal):
                requested = dict(memory_bytes=32 << 20, max_parallelism=1,
                                 spill=dict(spill, quota_bytes=1 << 20) if failure == "quota" else spill)
                query, traces, _ = declaration(failure)
                before = set(harness.output.iterdir())
                targets = {ext: harness.output / (name + "." + ext) for ext in ("vortex", "jsonl")}
                try:
                    if terminal == "collect":
                        query.collect(check=True, **requested)
                    else:
                        query.fanout(targets, **requested)
                except (LookupError, ShardLoomCommandError) as error:
                    if failure == "producer":
                        assert isinstance(error, LookupError) and str(error) == "dynamic subquery spill producer sentinel"
                        assert traces[1]["actual_spill_seen"]
                    else:
                        assert isinstance(error, ShardLoomCommandError)
                        harness.envelope(name, error.envelope, success=False)
                        assert "quota" in json.dumps(error.envelope.raw).lower()
                else:
                    raise AssertionError("dynamic subquery published after a spill failure")
                assert all(trace["closed"] for trace in traces)
                assert set(harness.output.iterdir()) == before
                assert not any(path.exists() for path in targets.values())
                clean()
                fresh, fresh_traces, wanted = declaration(count=2, width=8)
                consume(harness, name + "-fresh", fresh, wanted, FIELDS, fresh_traces, requested=requested)
                clean()
                return {"traces": traces, "fresh_completed": True, "final_publication": False}
            harness.case(name, fail)
