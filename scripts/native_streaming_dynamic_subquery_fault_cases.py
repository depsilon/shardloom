# SPDX-License-Identifier: Apache-2.0
"""Late source failures, selected evaluation and dynamic writer/cancellation cleanup."""

from __future__ import annotations

import shardloom as sl
from native_streaming_dynamic_subquery_cases import apply, consume, declaration, fields, inner, predicate
from native_streaming_subquery_cases import LEFT, RIGHT, SCHEMA, POLICY, complete_inputs
from native_streaming_unary_cases import frame
from native_streaming_protocol_cases import Peer


def selected(left, right, kind, selection):
    if selection == "empty":
        left = left.limit(0)
    expression = predicate(inner(right), kind)
    if selection in ("false", "null"):
        expression = sl.case_when("FALSE" if selection == "false" else "CAST(NULL AS BOOLEAN)", expression, True)
    return left.with_column("answer", expression)


def run(harness):
    workspace = harness.output / "dynamic-subquery-fault-spill"
    workspace.mkdir()
    foreign = workspace / "foreign"
    foreign.write_bytes(b"preserve")
    requested = dict(POLICY, spill={"workspace": str(workspace), "quota_bytes": 128 << 20, "buffer_bytes": 1 << 20})

    def clean():
        assert foreign.read_bytes() == b"preserve" and list(workspace.iterdir()) == [foreign]

    for kind in ("in", "row-not-in", "exists", "all-ge"):
        for selection in ("selected", "empty", "false", "null", "shared"):
            for failure in ("producer", "schema"):
                for terminal in ("collect", "zero", "vortex", "fanout"):
                    name = f"dynamic-subquery-fault-{kind}-{selection}-{failure}-{terminal}"

                    def fail(name=name, kind=kind, selection=selection, failure=failure, terminal=terminal):
                        events, sentinel = [], LookupError(name + " sentinel")

                        def broken():
                            events.append("opened")
                            try:
                                yield RIGHT[:1]
                                events.append("fault")
                                if failure == "producer":
                                    raise sentinel
                                yield [{"changed": 1}]
                            finally:
                                events.append("closed")

                        right = harness.context.from_batches(broken, schema=SCHEMA, streaming=True)
                        left, trace = frame(harness, LEFT, SCHEMA, width=1)
                        query = selected(right if selection == "shared" else left, right, kind, selection)
                        targets = {ext: harness.output / (name + "." + ext) for ext in ("vortex", "jsonl")}
                        before, iterator = set(harness.output.iterdir()), None
                        try:
                            if terminal == "collect":
                                query.collect(check=True, **requested)
                            elif terminal == "zero":
                                iterator = query.limit(0).iter_batches(batch_rows=1, **requested)
                                with iterator:
                                    assert list(iterator) == []
                            elif terminal == "vortex":
                                query.write_vortex(targets["vortex"], **requested)
                            else:
                                query.fanout(targets, **requested)
                        except (LookupError, ValueError) as error:
                            if failure == "producer":
                                assert error is sentinel
                            else:
                                assert isinstance(error, ValueError)
                                assert "schema" in str(error) or "field" in str(error), str(error)
                        else:
                            raise AssertionError("dynamic subquery certified a failed finite producer")
                        assert events == ["opened", "fault", "closed"]
                        assert not trace["opened"] or trace["closed"]
                        if iterator is not None:
                            assert iterator.report is None and iterator._process.poll() is not None
                        assert set(harness.output.iterdir()) == before
                        assert not any(path.exists() for path in targets.values())
                        clean()
                        return {"producer_events": events, "other_trace": trace, "final_publication": False}
                    harness.case(name, fail)

    for selection in ("empty", "false", "null"):
        name = "dynamic-subquery-unused-invalid-pivot-" + selection

        def unused(name=name, selection=selection):
            left, a = frame(harness, LEFT, SCHEMA, width=1)
            right, b = frame(harness, RIGHT, SCHEMA, width=1)
            # Division is in a dynamic inner plan, and would fail if its callback ran.
            invalid = right.filter(sl.col("n") <= sl.outer("n")).select(
                "n AS entity", "'a' AS category", "1/0 AS amount").pivot_table(
                    index="entity", columns="category", values="amount", aggfunc="sum")
            expression = sl.exists_source(invalid)
            if selection == "empty":
                left, expected = left.limit(0), []
            else:
                expression = sl.case_when("FALSE" if selection == "false" else "CAST(NULL AS BOOLEAN)", expression, True)
                expected = [dict(row, answer=True) for row in LEFT]
            query = left.with_column("answer", expression)
            report = consume(harness, name, query, expected, fields("exists"), [a, b], requested=requested)
            assert report.envelope.field_int("relational_dynamic_schema_stages", 0) == 0
            assert report.envelope.field_int("native_input_replay_sources") == 0
            assert report.envelope.field_int("native_input_subquery_validation_sources") == 1
            clean()
            return {"traces": [a, b], "unused_pivot_evaluated": False}
        harness.case(name, unused)

    for shared in (False, True):
        for kind in ("in", "exists", "any-gt"):
            for failure in ("close", "consumer"):
                name = f"dynamic-subquery-output-{kind}-{failure}-shared-{shared}"

                def interrupt(name=name, kind=kind, failure=failure, shared=shared):
                    query, expected, traces, _ = declaration(harness, kind, shared=shared)
                    sentinel = LookupError(name + " sentinel")
                    try:
                        with query.iter_batches(batch_rows=1, **requested) as iterator:
                            retained = next(iterator)
                            assert iterator.report is None
                            if failure == "consumer":
                                raise sentinel
                    except LookupError as error:
                        assert error is sentinel
                    assert iterator.report is None and iterator._process.poll() is not None
                    assert all(not trace["opened"] or trace["closed"] for trace in traces)
                    harness.values(name, retained.result_rows, expected[:1])
                    clean()
                    fresh, wanted, fresh_traces, flags = declaration(harness, kind, shared=shared)
                    consume(harness, name + "-fresh", fresh, wanted, fields(kind), fresh_traces, flags, requested=requested)
                    harness.values(name + "-retained-after-fresh", retained.result_rows, expected[:1])
                    clean()
                    return {"traces": traces, "completion_report": None, "fresh_completed": True}
                harness.case(name, interrupt)
    protocol(harness, requested, clean)


def protocol(harness, requested, clean):
    for empty_outer in (False, True):
        for write in (False, True):
            for failure in ("eof", "cancel", "wrong-end", "wrong-uri", "malformed"):
                name = f"dynamic-subquery-unused-protocol-empty-{empty_outer}-write-{write}-{failure}"

                def check(name=name, empty_outer=empty_outer, write=write, failure=failure):
                    left, _ = frame(harness, LEFT, SCHEMA)
                    right, _ = frame(harness, RIGHT, SCHEMA)
                    query = selected(left, right, "exists", "false")
                    workflow = dict(requested, input_uri=left.source.uri, input_format="memory",
                        source_bindings={source.source.uri: {"input_format": "memory", "memory_input": dict(source.source.memory_input)}
                                         for source in (left, right)}, sql_statement=query._relation_statement())
                    before = set(harness.output.iterdir())
                    with Peer(harness, name, write=write, workflow=workflow) as peer:
                        peer.demand(0)
                        if not empty_outer:
                            peer.rows(0, [["1", "a"]])
                            if not write:
                                batch = peer.read()
                                assert batch["kind"] == "batch" and batch["rows"] == [dict(n=1, s="a", answer=True)]
                                peer.send({"kind": "ack", "index": batch["index"]})
                            peer.demand(1)
                        peer.send({"kind": "end", "uri": left.source.uri, "index": int(not empty_outer)})
                        peer.uri = right.source.uri
                        peer.demand(0)
                        peer.rows(0, [["2", "b"]])
                        peer.demand(1)
                        if failure == "eof":
                            peer.process.stdin.close()
                        elif failure == "cancel":
                            peer.send({"kind": "cancel"})
                        elif failure == "wrong-end":
                            peer.send({"kind": "end", "uri": right.source.uri, "index": 0})
                        elif failure == "wrong-uri":
                            peer.send({"kind": "rows", "uri": left.source.uri, "index": 1, "rows": [["3", "c"]]})
                        else:
                            peer.rows(1, [["invalid-integer", "c"]])
                        result = peer.failed()
                    assert set(harness.output.iterdir()) == before
                    clean()
                    return dict(result, provisional_rows=int(not empty_outer and not write), unused_inner_validated=True)
                harness.case(name, check)
