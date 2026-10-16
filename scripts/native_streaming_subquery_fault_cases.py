# SPDX-License-Identifier: Apache-2.0
"""Selected scalar errors and unused-source failures must prevent final publication."""

from __future__ import annotations

import json

import shardloom as sl
from shardloom.errors import ShardLoomCommandError
from native_streaming_subquery_cases import LEFT, RIGHT, SCHEMA, POLICY, apply, declaration
from native_streaming_unary_cases import frame
from native_streaming_protocol_cases import Peer


def run(harness):
    workspace = harness.output / "subquery-fault-spill"
    workspace.mkdir()
    foreign = workspace / "foreign"
    foreign.write_bytes(b"preserve")
    spill = {"workspace": str(workspace), "quota_bytes": 128 << 20, "buffer_bytes": 1 << 20}
    requested = dict(POLICY, spill=spill)

    def clean():
        assert foreign.read_bytes() == b"preserve" and list(workspace.iterdir()) == [foreign]

    for kind in ("in", "scalar-one", "parameterized-scalar", "guarded-scalar", "coalesce", "shared-scalar"):
        for side in (("outer",) if kind == "shared-scalar" else ("outer", "inner", "unused-inner")):
            if side == "unused-inner" and kind == "in":
                continue  # Eager membership evaluates its inner even for an empty outer.
            for failure in ("producer", "schema"):
                for terminal in ("collect", "zero", "vortex", "fanout"):
                    name = f"subquery-fault-{kind}-{side}-{failure}-{terminal}"

                    def fail(name=name, kind=kind, side=side, failure=failure, terminal=terminal):
                        events, sentinel = [], LookupError(name + " sentinel")

                        def broken():
                            events.append("opened")
                            try:
                                yield (LEFT if side == "outer" else RIGHT)[:2]
                                events.append("fault")
                                if failure == "producer":
                                    raise sentinel
                                yield [{"changed": 1}]
                            finally:
                                events.append("closed")

                        source = harness.context.from_batches(broken, schema=SCHEMA, streaming=True)
                        other, trace = frame(harness, [] if side == "unused-inner" else LEFT if side != "outer" else RIGHT, SCHEMA)
                        left, right = (source, source) if kind == "shared-scalar" else (
                            (source, other) if side == "outer" else (other, source))
                        query = apply(left, right, kind)
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
                            raise AssertionError("subquery certified a failed finite source")
                        assert events == ["opened", "fault", "closed"]
                        assert not trace["opened"] or trace["closed"]
                        if iterator is not None:
                            assert iterator.report is None and iterator._process.poll() is not None
                        assert set(harness.output.iterdir()) == before
                        assert not any(path.exists() for path in targets.values())
                        clean()
                        return {"producer_events": events, "other_trace": trace,
                                "final_publication": False, "original_exception_preserved": failure == "producer"}
                    harness.case(name, fail)

    for failure in ("cardinality", "duplicate-cardinality", "expression", "late-cardinality"):
        for terminal in ("collect", "zero", "vortex", "fanout"):
            name = f"subquery-selected-{failure}-{terminal}"

            def selected(name=name, failure=failure, terminal=terminal):
                left, a = frame(harness, LEFT, SCHEMA, width=1)
                rows = [dict(n=2, s="b")] * 2 if failure == "duplicate-cardinality" else RIGHT
                right, b = frame(harness, rows, SCHEMA, width=1)
                inner = right.select("1/0 AS bad" if failure == "expression" else "n")
                expression = sl.scalar_subquery(inner)
                if failure == "late-cardinality":
                    expression = sl.case_when(sl.col("n") == 3, expression, 7)
                query = left.with_column("answer", expression)
                if terminal == "zero":
                    query = query.limit(0)
                targets = {ext: harness.output / (name + "." + ext) for ext in ("vortex", "jsonl")}
                before = set(harness.output.iterdir())
                try:
                    if terminal in ("collect", "zero"):
                        query.collect(check=True, **requested)
                    elif terminal == "vortex":
                        query.write_vortex(targets["vortex"], **requested)
                    else:
                        query.fanout(targets, **requested)
                except ShardLoomCommandError as error:
                    harness.envelope(name, error.envelope, success=False)
                    diagnostic = "division by zero" if failure == "expression" else "scalar subquery cardinality"
                    assert diagnostic in json.dumps(error.envelope.raw), error.envelope.raw
                else:
                    raise AssertionError("selected invalid scalar evaluation was skipped")
                assert a["closed"] and b["closed"]
                assert set(harness.output.iterdir()) == before
                clean()
                return {"outer_trace": a, "inner_trace": b, "final_publication": False}
            harness.case(name, selected)

    for kind in ("in", "parameterized-scalar", "guarded-scalar", "shared-scalar"):
        for failure in ("close", "consumer"):
            name = f"subquery-output-{kind}-{failure}"

            def interrupt(name=name, kind=kind, failure=failure):
                query, expected, traces, _ = declaration(harness, kind, "mixed", (True, True), width=1)
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
                assert all(not trace["opened"] or trace["closed"] for trace in traces), traces
                harness.values(name, retained.result_rows, expected[:1])
                clean()
                fresh, wanted, fresh_traces, flags = declaration(harness, kind, "mixed", (True, True))
                report = fresh.collect(check=True, **requested)
                from native_streaming_subquery_cases import completed
                harness.values(name + "-fresh", report.result_rows, wanted)
                completed(harness, name + "-fresh", report.envelope, fresh_traces, flags, kind, "mixed")
                clean()
                return {"traces": traces, "completion_report": None, "fresh_completed": True}
            harness.case(name, interrupt)
    protocol(harness, requested, clean)


def protocol(harness, requested, clean):
    for empty_outer in (False, True):
        for write in (False, True):
            for failure in ("eof", "cancel", "wrong-end", "wrong-uri", "malformed"):
                name = f"subquery-unused-protocol-empty-{empty_outer}-write-{write}-{failure}"

                def check(name=name, empty_outer=empty_outer, write=write, failure=failure):
                    left, _ = frame(harness, LEFT, SCHEMA)
                    right, _ = frame(harness, RIGHT, SCHEMA)
                    query = apply(left, right, "guarded-scalar")
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
                                assert batch["kind"] == "batch" and batch["rows"] == [dict(n=1, s="a", answer=7)]
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
