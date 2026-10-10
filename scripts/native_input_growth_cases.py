# SPDX-License-Identifier: Apache-2.0
"""Complete public workflows beyond retired input-count, width and range limits."""

from __future__ import annotations

import hashlib
import time

from native_workflow_outputs import LOCAL_FORMATS, write_outputs
from shardloom.errors import ShardLoomCommandError


def run(harness):
    context = harness.context
    policy = {"memory_gb": 1, "max_parallelism": 1}
    schema = {f"c{index}": ("int64", "float64", "bool", "utf8")[index % 4]
              for index in range(4097)}
    rows = [
        {name: (-(1 << 63), -1.25, True, f'東京 λ "\n {index}')[index % 4]
         for index, name in enumerate(schema)},
        dict.fromkeys(schema),
        {name: ((1 << 63) - 1, 1.25, False, f"space,;%=null {index}")[index % 4]
         for index, name in enumerate(schema)},
    ]
    columns = list(reversed(schema))
    expected = [{name: row[name] for name in columns} for row in rows]

    def wide(mode):
        if mode == "rows":
            source = context.from_rows(rows, schema=schema)
        else:
            source = context.from_batches(lambda: iter([[], rows[:2], [], rows[2:]]),
                                          schema=schema, streaming=mode == "streaming")
        return source.select(*columns)

    def check_schema(result):
        assert result.result_columns == tuple(columns)
        assert [(name, dtype.name, dtype.nullable) for name, dtype in result.result_schema] == [
            (name, schema[name], True) for name in columns]

    def accepted(name, report):
        harness.envelope(name, report.envelope)
        return report.envelope

    def complete(name, actual, wanted, _destination=None):
        harness.values(name, actual, wanted)

    for mode in ("rows", "resident", "streaming"):
        def collect(mode=mode):
            name = f"growth-wide-{mode}-collect"
            result = wide(mode).collect(check=True, **policy)
            harness.values(name, result.result_rows, expected)
            check_schema(result)
            if mode == "streaming":
                harness.completed(name, result.envelope, batches=4, rows=3)
            else:
                harness.envelope(name, result.envelope)
        harness.case(f"growth-wide-{mode}-collect", collect)

        def incremental(mode=mode):
            name = f"growth-wide-{mode}-iter"
            with wide(mode).iter_batches(batch_rows=1, **policy) as iterator:
                actual = []
                for batch in iterator:
                    check_schema(batch)
                    actual.extend(batch.result_rows)
                    assert iterator.report is None
                assert iterator.report is not None and iterator._process.poll() == 0
                harness.values(name, actual, expected)
                if mode == "streaming":
                    harness.completed(name, iterator.report.envelope, batches=4, rows=3)
                else:
                    harness.envelope(name, iterator.report.envelope)
        harness.case(f"growth-wide-{mode}-iter", incremental)

        def outputs(mode=mode):
            formats = ("vortex",) if mode == "streaming" else LOCAL_FORMATS
            destinations = write_outputs(
                context, harness.output, wide(mode), expected, columns,
                name=f"growth-wide-{mode}", guard=harness.guard,
                accepted=accepted, complete=complete, formats=formats, execution=policy)
            return {"artifacts": [harness.artifact(path) for path in destinations.values()],
                    "complete_columns_verified": len(columns)}
        harness.case(f"growth-wide-{mode}-outputs", outputs)

    def cumulative(mode, empty, output):
        count = 8193 if empty else 4099
        name = f"growth-{mode}-{'empty' if empty else 'nonempty'}-{output}"
        state = {"opened": 0, "produced": 0, "ended": False, "closed": False}

        def produce():
            state["opened"] += 1
            try:
                for index in range(count):
                    state["produced"] += 1
                    yield [] if empty else [{"n": index, "s": f"λ {index}"}]
                state["ended"] = True
            finally:
                state["closed"] = True

        wanted = [] if empty else [{"n": index, "s": f"λ {index}"} for index in range(count)]
        query = context.from_batches(produce(), schema={"n": "int64", "s": "utf8"},
                                     streaming=mode == "streaming")
        if output == "collect":
            report = query.collect(check=True, **policy)
            actual = report.result_rows
        else:
            target = harness.output / f"{name}.vortex"
            report = query.write_vortex(target, **policy)
            reopened = context.read_vortex(target).collect(check=True, **policy)
            harness.envelope(f"{name}-reopened", reopened.envelope)
            assert reopened.result_columns == ("n", "s")
            actual = reopened.result_rows
        assert state == {"opened": 1, "produced": count, "ended": True, "closed": True}
        harness.values(name, actual, wanted)
        if mode == "streaming":
            harness.completed(name, report.envelope, batches=count, rows=len(wanted))
        else:
            harness.envelope(name, report.envelope)
            assert report.envelope.field_int("native_input_batches") == count
            assert report.envelope.field_int("native_input_batch_rows") == len(wanted)
        scratch = report.envelope.field_int("native_batch_input_peak_scratch_reservation_bytes")
        assert 0 < scratch <= 1 << 30
        return {"complete_input_batches": count, "complete_output_rows_verified": len(wanted),
                "input_scratch_peak_bytes": scratch, "producer": state}

    for mode in ("resident", "streaming"):
        for empty in (False, True):
            for output in ("collect", "vortex"):
                name = f"growth-{mode}-{'empty' if empty else 'nonempty'}-{output}"
                harness.case(name, lambda mode=mode, empty=empty, output=output:
                             cumulative(mode, empty, output))

    def range_output():
        count = 1_000_017
        target = harness.output / "growth-range.vortex"
        written = context.range(count - 1, -1, step=-1, column="n").write_vortex(target, **policy)
        harness.envelope("growth-range-write", written.envelope)
        seen = 0
        digest = hashlib.sha256()
        with context.read_vortex(target).iter_batches(batch_rows=2048, **policy) as iterator:
            for batch in iterator:
                assert batch.result_columns == ("n",)
                for row in batch.result_rows:
                    assert row == {"n": count - 1 - seen}, (seen, row)
                    digest.update(row["n"].to_bytes(8, "little", signed=True))
                    seen += 1
                assert iterator.report is None
            assert iterator.report is not None and iterator._process.poll() == 0
            harness.envelope("growth-range-reopened", iterator.report.envelope)
        assert seen == count
        result = context.read_vortex(target).aggregate(
            "COUNT(*) AS count", "SUM(n) AS total", "MIN(n) AS low", "MAX(n) AS high"
        ).collect(check=True, **policy)
        harness.envelope("growth-range-aggregate", result.envelope)
        harness.values("growth-range-aggregate", result.result_rows,
                       [{"count": count, "total": count * (count - 1) // 2, "low": 0, "high": count - 1}])
        return {"artifact": harness.artifact(target), "every_value_verified": seen,
                "complete_i64_le_sha256": digest.hexdigest()}
    harness.case("growth-range-write-reopen", range_output)

    def late_failure():
        closed = []
        produced = 0

        def produce():
            nonlocal produced
            try:
                for index in range(4099):
                    produced += 1
                    yield [{"n": index}]
                produced += 1
                yield [{"n": (1 << 63) - 1}]
            finally:
                closed.append(True)

        target = harness.output / "growth-late-failure.vortex"
        prior = set(harness.output.iterdir())
        query = context.from_batches(produce(), schema={"n": "int64"}, streaming=True)
        try:
            query.select("n + 1 AS n").limit(7).write_vortex(target, **policy)
        except ShardLoomCommandError as error:
            harness.envelope("growth-late-failure", error.envelope, success=False)
        else:
            raise AssertionError("late overflow after the retired batch bound was ignored")
        assert produced == 4100 and closed == [True]
        assert not target.exists() and set(harness.output.iterdir()) == prior
        return {"input_batches_before_failure": produced, "owned_output_removed": True}
    harness.case("growth-late-failure-cleans-output", late_failure)

    def cancel():
        produced = 0
        closed = []

        def produce():
            nonlocal produced
            try:
                for index in range(8193):
                    produced += 1
                    yield [{"n": index}]
            finally:
                closed.append(True)

        query = context.from_batches(produce(), schema={"n": "int64"}, streaming=True)
        with query.iter_batches(batch_rows=1, **policy) as iterator:
            for index in range(4097):
                batch = next(iterator)
                assert batch.result_rows == ({"n": index},)
                assert produced == index + 1 and iterator.report is None
                if index in (0, 4096):
                    time.sleep(0.01)
                    assert produced == index + 1
        assert produced == 4097 and closed == [True]
        assert iterator.report is None and iterator._process.poll() is not None
        return {"consumed_before_cancel": produced, "producer_closed": True,
                "successful_completion_reported": False}
    harness.case("growth-cancel-after-former-bound", cancel)
