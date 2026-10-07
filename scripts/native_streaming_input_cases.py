# SPDX-License-Identifier: Apache-2.0
"""Complete public input-streaming cases against the actual native executable."""

from __future__ import annotations

import json
import time

import shardloom as sl
from shardloom.errors import ShardLoomCommandError

SCHEMA = {"n": "int64", "f": "float64", "b": "bool", "s": "utf8"}
ROWS = [
    {"n": -(1 << 63), "f": -1.25, "b": True, "s": 'λ"\n'},
    {"n": (1 << 63) - 1, "f": 1.25, "b": False, "s": ""},
    {"n": None, "f": None, "b": None, "s": None},
    {"n": 0, "f": -0.0, "b": False, "s": "東京"},
    {"n": 2, "f": 2.5, "b": True, "s": "space,;%=null"},
]


def declaration(frame):
    """Use the documented source declaration on the public SQL client surface."""
    uri = frame.source.uri
    return {
        "input_uri": uri, "input_format": "memory",
        "source_bindings": {uri: {"input_format": "memory", "memory_input": dict(frame.source.memory_input)}},
        "input_batches": {uri: frame.source.batch_input},
        "memory_gb": 1, "max_parallelism": 1,
    }


def run(harness):
    context = harness.context
    policy = {"memory_gb": 1, "max_parallelism": 1}

    def factory():
        return iter([[], ROWS[:2], ROWS[2:4], [], ROWS[4:]])

    def frame():
        return context.from_batches(factory, schema=SCHEMA, streaming=True)

    def collect(name, query, expected, batches=5, rows=len(ROWS)):
        result = query.collect(check=True, **policy)
        harness.values(name, result.result_rows, expected)
        harness.completed(name, result.envelope, batches=batches, rows=rows)
        return result

    def typed_collection():
        result = collect("typed-collection", frame(), ROWS)
        assert [(name, dtype.name, dtype.nullable) for name, dtype in result.result_schema] == [
            (name, dtype, True) for name, dtype in SCHEMA.items()]
    harness.case("typed-collection", typed_collection)

    expected = [{"id": row["n"], "text": row["s"], "twice": row["f"] * 2}
                for row in ROWS if row["b"] is True]
    def pipeline():
        return frame().filter(sl.col("b") == True).select("n AS id", "s AS text", "f AS amount") \
            .with_column("twice", sl.col("amount") * 2).select("id", "text", "twice")
    harness.case("composed-row-local", lambda: collect("composed-row-local", pipeline(), expected))

    def nested():
        expected_nested = [{"details": {"n": row["n"], "s": row["s"]}, "values": [1, 2, None]}
                           for row in ROWS]
        query = frame().select("STRUCT(n,s) AS details", "ARRAY[1,2,NULL] AS values")
        collect("nested-output", query, expected_nested)
    harness.case("nested-output", nested)

    def sql_pipeline():
        source = frame()
        statement = f"SELECT n AS id,s AS text,f*2 AS twice FROM '{source.source.uri}' WHERE b = TRUE"
        with harness.client.public_workflow_batches("sql", sql_statement=statement, **declaration(source)) as iterator:
            observed = [row for batch in iterator for row in batch.result_rows]
            assert iterator.report is not None
            harness.values("sql-pipeline", observed, expected)
            harness.completed("sql-pipeline", iterator.report.envelope, batches=5, rows=len(ROWS))
            assert iterator._process.poll() == 0
    harness.case("sql-pipeline", sql_pipeline)

    def incremental():
        pulls, closed = [], []
        def produce():
            try:
                for index, row in enumerate(ROWS):
                    pulls.append(index)
                    yield [row]
            finally:
                closed.append(True)
        query = context.from_batches(produce(), schema=SCHEMA, streaming=True)
        retained, before = [], time.monotonic()
        with query.iter_batches(batch_rows=1, **policy) as iterator:
            for index, batch in enumerate(iterator):
                if index == 0:
                    first = time.monotonic() - before
                retained.append(batch)
                assert pulls == list(range(index + 1)), pulls
                assert iterator.report is None
                time.sleep(0.01)
                assert pulls == list(range(index + 1))
            assert closed == [True] and iterator.report is not None
            assert iterator._process.poll() == 0
            harness.values("incremental-slow-retained", [row for batch in retained for row in batch.result_rows], ROWS)
            harness.completed("incremental-slow-retained", iterator.report.envelope, batches=len(ROWS), rows=len(ROWS))
        return {"first_provisional_seconds": first, "complete_delivery_seconds": time.monotonic() - before,
                "retained_python_batches": len(retained), "performance_claim": False}
    harness.case("incremental-slow-retained", incremental)

    for name, inputs, filtered in [
        ("empty-source", [], False), ("empty-batches", [[], [], []], False),
        ("all-filtered", [ROWS[:2], [], ROWS[2:]], True),
        ("all-null", [[dict.fromkeys(SCHEMA)]], False),
    ]:
        def empty_case(name=name, inputs=inputs, filtered=filtered):
            consumed, ended = [], []
            def produce():
                for index, batch in enumerate(inputs):
                    consumed.append(index)
                    yield batch
                ended.append(True)
            query = context.from_batches(produce(), schema=SCHEMA, streaming=True)
            if filtered:
                query = query.filter("FALSE")
            expected_empty = [] if name != "all-null" else [dict.fromkeys(SCHEMA)]
            collect(name, query, expected_empty, len(inputs), sum(map(len, inputs)))
            assert consumed == list(range(len(inputs))) and ended == [True]
        harness.case(name, empty_case)

    def writer():
        target = harness.output / "complete.vortex"
        result = pipeline().write_vortex(target, **policy)
        harness.completed("native-write", result.envelope, batches=5, rows=len(ROWS))
        reopened = context.read_vortex(target).collect(check=True, **policy)
        harness.envelope("native-reopen", reopened.envelope)
        harness.values("native-reopen", reopened.result_rows, expected)
        return {"artifact": harness.artifact(target)}
    harness.case("native-write", writer)

    def reject(name, action):
        opened = []
        def produce():
            opened.append(True)
            return iter([[{"n": 1}]])
        query = context.from_batches(produce, schema={"n": "int64"}, streaming=True)
        try:
            action(query)
        except ShardLoomCommandError as error:
            harness.envelope(name, error.envelope, success=False)
            assert "SL-NATIVE-BATCH" in json.dumps(error.envelope.raw)
        else:
            raise AssertionError(name + " unexpectedly succeeded")
        assert opened == [], opened
        assert not list(harness.output.glob(name + "*")), "denial left output artifacts"

    for name, operation in [
        ("deny-limit", lambda q: q.limit(1).collect(check=True, **policy)),
        ("deny-sort", lambda q: q.sort("n").collect(check=True, **policy)),
        ("deny-aggregate", lambda q: q.count(check=True, **policy)),
        ("deny-self-join", lambda q: q.join(q, on="n").collect(check=True, **policy)),
        ("deny-union", lambda q: q.union_all(q).collect(check=True, **policy)),
        ("deny-window", lambda q: q.window("ROW_NUMBER() OVER (ORDER BY n) AS rn").collect(check=True, **policy)),
        ("deny-unary", lambda q: q.drop_duplicates(["n"]).collect(check=True, **policy)),
        ("deny-dynamic", lambda q: q.select("n AS entity", "'a' AS category", "n AS amount")
         .pivot(index="entity", columns="category", values="amount").collect(check=True, **policy)),
        ("deny-parquet", lambda q: q.write_parquet(harness.output / "deny-parquet.parquet", **policy)),
        ("deny-fanout", lambda q: q.fanout([
            ("vortex", harness.output / "deny-fanout-a.vortex"),
            ("vortex", harness.output / "deny-fanout-b.vortex")], **policy)),
    ]:
        harness.case(name, lambda name=name, operation=operation: reject(name, operation))

    def producer_failure(name, mode, write=False):
        closed, pulls = [], []
        def produce():
            try:
                pulls.append(0)
                yield [{"n": 1}]
                pulls.append(1)
                if mode == "producer":
                    raise RuntimeError("late source sentinel")
                if mode == "shape":
                    yield [{"wrong": 2}]
                if mode == "overflow":
                    yield [{"n": (1 << 63) - 1}]
            finally:
                closed.append(True)
        query = context.from_batches(produce(), schema={"n": "int64"}, streaming=True)
        if mode == "overflow":
            query = query.select("n + 1 AS n")
        expected_type = {"producer": RuntimeError, "shape": ValueError, "overflow": ShardLoomCommandError}[mode]
        target = harness.output / (name + ".vortex")
        before = set(harness.output.iterdir())
        iterator = None
        try:
            if write:
                query.write_vortex(target, **policy)
            else:
                iterator = query.iter_batches(**policy)
                with iterator:
                    first = next(iterator)
                    assert first.result_rows == ({"n": 2 if mode == "overflow" else 1},)
                    assert iterator.report is None and pulls == [0]
                    next(iterator)
        except expected_type as error:
            if isinstance(error, ShardLoomCommandError):
                harness.envelope(name, error.envelope, success=False)
            if mode == "producer":
                assert str(error) == "late source sentinel"
        else:
            raise AssertionError(name + " unexpectedly succeeded")
        assert pulls == [0, 1] and closed == [True]
        assert not target.exists() and set(harness.output.iterdir()) == before
        if iterator is not None:
            assert iterator.report is None and iterator._process.poll() is not None
        return {"provisional_rows": 0 if write else 1, "successful_prefix": False, "staging_remaining": False}

    for mode in ("producer", "shape", "overflow"):
        for write in (False, True):
            name = "late-" + mode + ("-write" if write else "-delivery")
            harness.case(name, lambda name=name, mode=mode, write=write: producer_failure(name, mode, write))

    for failed_consumer in (False, True):
        name = "failed-consumer" if failed_consumer else "early-cancel"
        def cancel_case(failed_consumer=failed_consumer):
            pulls, closed = [], []
            def produce():
                try:
                    for n in range(3):
                        pulls.append(n)
                        yield [{"n": n}]
                finally:
                    closed.append(True)
            query = context.from_batches(produce(), schema={"n": "int64"}, streaming=True)
            iterator = query.iter_batches(**policy)
            before = time.monotonic()
            try:
                with iterator:
                    assert next(iterator).result_rows == ({"n": 0},)
                    if failed_consumer:
                        raise RuntimeError("consumer sentinel")
            except RuntimeError as error:
                assert failed_consumer and str(error) == "consumer sentinel"
            assert iterator.report is None and iterator._process.poll() is not None
            assert pulls == [0] and closed == [True]
            return {"elapsed_seconds": time.monotonic() - before, "pulled_batches": 1,
                    "successful_prefix": False, "child_exited": True}
        harness.case(name, cancel_case)

    def inert_discovery():
        opened = []
        def produce():
            opened.append(True)
            return iter([])
        query = context.from_batches(produce, schema={"n": "int64"}, streaming=True)
        for method in (query.explain, query.estimate):
            envelope = method(check=False)
            assert envelope.status in {"success", "unsupported"}
            harness.envelope("inert-" + method.__name__, envelope, success=envelope.status == "success")
        assert opened == []
    harness.case("inert-discovery", inert_discovery)

    def no_replay():
        query = context.from_batches([[{"n": 7}]], schema={"n": "int64"}, streaming=True)
        collect("one-shot-first", query, [{"n": 7}], 1, 1)
        try:
            query.collect(check=True, **policy)
        except ValueError as error:
            assert "already been consumed" in str(error)
        else:
            raise AssertionError("one-shot producer was silently replayed")
    harness.case("no-input-replay", no_replay)
