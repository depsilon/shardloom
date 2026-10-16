# SPDX-License-Identifier: Apache-2.0
"""Bounded inner scans, shared native replay and credited resident-state denial."""

from __future__ import annotations

import json

import shardloom as sl
from shardloom.errors import ShardLoomCommandError
from native_streaming_subquery_cases import SCHEMA, complete_inputs
from native_streaming_unary_cases import frame


def large_frame(harness, count, width):
    trace = dict(opened=0, ended=False, closed=False, batches=0, rows=0)

    def produce():
        trace["opened"] += 1
        try:
            for start in range(0, count, 31):
                rows = [dict(n=index, s=f"{index:08}" + "p" * width) for index in range(start, min(start + 31, count))]
                trace["batches"] += 1
                trace["rows"] += len(rows)
                yield rows
            trace["ended"] = True
        finally:
            trace["closed"] = True

    return harness.context.from_batches(produce, schema=SCHEMA, streaming=True), trace


def run(harness):
    workspace = harness.output / "subquery-pressure-spill"
    workspace.mkdir()
    foreign = workspace / "foreign"
    foreign.write_bytes(b"preserve")
    spill = {"workspace": str(workspace), "quota_bytes": 128 << 20, "buffer_bytes": 1 << 20}

    def clean():
        assert foreign.read_bytes() == b"preserve" and list(workspace.iterdir()) == [foreign]

    for skipped in (False, True):
        name = f"subquery-large-bounded-inner-skipped-{skipped}"

        def bounded(name=name, skipped=skipped):
            grant, count = 8 << 20, 8193
            left, a = frame(harness, [dict(n=1, s="outer")], SCHEMA, width=1)
            right, b = large_frame(harness, count, 4096)
            expression = sl.scalar_subquery(right.select("1/0 AS bad")) if skipped else sl.scalar_subquery(right.agg(answer="COUNT(*)"))
            if skipped:
                expression = sl.case_when(sl.col("n") < 0, expression, 7)
            query = left.with_column("answer", expression)
            expected = [dict(n=1, s="outer", answer=7 if skipped else count)]
            harness.json(harness.logs / (name + "-predeclared.json"), {"expected": expected, "count": count, "width": 4096, "grant": grant})
            report = query.collect(check=True, memory_bytes=grant, max_parallelism=1)
            harness.values(name, report.result_rows, expected)
            complete_inputs(harness, name, report.envelope, [a, b], grant=grant)
            assert report.envelope.field_int("native_input_logical_bytes") > 4 * grant
            assert report.envelope.field_int("native_input_replay_sources") == 0
            assert report.envelope.field_int("native_input_subquery_validation_sources") == int(skipped)
            assert report.envelope.field_int("native_input_subquery_build_rows_detached") == int(not skipped)
            clean()
            return {"traces": [a, b], "grant": grant, "input_greater_than_four_grants": True}
        harness.case(name, bounded)

    for spilling in (False, True):
        name = f"subquery-shared-replay-spilled-{spilling}"

        def shared(name=name, spilling=spilling):
            grant, count = 16 << 20, 2049
            source, trace = large_frame(harness, count, 2048)
            query = source.select("n").with_column("answer", sl.scalar_subquery(source.select("n").limit(1)))
            expected = [dict(n=index, answer=0) for index in range(count)]
            harness.json(harness.logs / (name + "-predeclared.json"), expected)
            requested = dict(memory_bytes=grant, max_parallelism=1)
            if spilling:
                requested["spill"] = spill
            with query.iter_batches(batch_rows=29, **requested) as iterator:
                held, actual = [], []
                for batch in iterator:
                    assert iterator.report is None
                    assert trace["ended"] and trace["closed"]
                    actual.extend(batch.result_rows)
                    held.append(batch)
                report = iterator.report
                assert report is not None and iterator._process.poll() == 0
            harness.values(name, actual, expected)
            assert [row for batch in held for row in batch.result_rows] == expected
            complete_inputs(harness, name, report.envelope, [trace], grant=grant)
            for field, value in {"sources": 1, "passes": 2, "rows": 2 * count,
                                 "prefilled_sources": 1, "spilled_sources": int(spilling)}.items():
                assert report.envelope.field_int("native_input_replay_" + field) == value
            if spilling:
                assert report.envelope.field_int("relational_spill_runs_written") > 0
                assert report.envelope.field_bool("relational_spill_owned_cleanup_completed") is True
            clean()
            return {"trace": trace, "grant": grant, "actual_spilled_replay": spilling}
        harness.case(name, shared)

        name = f"subquery-membership-resident-denial-spill-{spilling}"

        def denied(name=name, spilling=spilling):
            grant, count = 8 << 20, 8193
            left, a = frame(harness, [dict(n=0, s="00000000" + "p" * 4096)], SCHEMA, width=1)
            right, b = large_frame(harness, count, 4096)
            query = left.with_column("answer", sl.col("s").isin_source(right, "s"))
            requested = dict(memory_bytes=grant, max_parallelism=1)
            if spilling:
                requested["spill"] = spill
            target = harness.output / (name + ".vortex")
            before = set(harness.output.iterdir())
            try:
                query.write_vortex(target, **requested)
            except ShardLoomCommandError as error:
                harness.envelope(name, error.envelope, success=False)
                assert "reservation denied" in json.dumps(error.envelope.raw), error.envelope.raw
            else:
                raise AssertionError("source replay spill silently spilled resident subquery state")
            assert a["opened"] == 0
            assert 0 < b["rows"] < count and b["closed"] and not b["ended"]
            assert set(harness.output.iterdir()) == before and not target.exists()
            clean()
            # A fresh finite subquery on the same client proves failure cleanup.
            outer, a2 = frame(harness, [dict(n=1, s="small")], SCHEMA)
            inner, b2 = frame(harness, [dict(n=1, s="small")], SCHEMA)
            report = outer.with_column("answer", sl.col("s").isin_source(inner, "s")).collect(check=True, **requested)
            harness.values(name + "-fresh", report.result_rows, [dict(n=1, s="small", answer=True)])
            complete_inputs(harness, name + "-fresh", report.envelope, [a2, b2], grant=grant)
            clean()
            return {"traces": [a, b], "grant": grant, "resident_state_denied": True, "fresh_completed": True}
        harness.case(name, denied)

    name = "subquery-prefill-quota-denial"

    def quota():
        source, trace = large_frame(harness, 2049, 2048)
        query = source.select("n").with_column("answer", sl.scalar_subquery(source.select("n").limit(1)))
        target = harness.output / (name + ".vortex")
        before = set(harness.output.iterdir())
        try:
            query.write_vortex(target, memory_bytes=16 << 20, max_parallelism=1,
                               spill=dict(spill, quota_bytes=1 << 20))
        except ShardLoomCommandError as error:
            harness.envelope(name, error.envelope, success=False)
            assert "quota" in json.dumps(error.envelope.raw).lower(), error.envelope.raw
        else:
            raise AssertionError("shared prefill exceeded its explicit disk quota")
        assert trace["opened"] == 1 and trace["closed"]
        assert set(harness.output.iterdir()) == before and not target.exists()
        clean()
        return {"trace": trace, "final_publication": False}
    harness.case(name, quota)
