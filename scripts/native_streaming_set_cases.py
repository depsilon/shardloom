# SPDX-License-Identifier: Apache-2.0
"""Complete finite set operations through the public SQL and DataFrame surfaces."""

from __future__ import annotations

from shardloom.query import SqlWorkflow
from native_report_evidence import require_native_allocation_peak
from native_streaming_join_cases import SCHEMA, frame
from native_workflow_outputs import LOCAL_FORMATS, verify_output

KINDS = ("union_all", "union", "intersect", "except_")
OPERATORS = {"union_all": "UNION ALL", "union": "UNION", "intersect": "INTERSECT", "except_": "EXCEPT"}
LEFT = [{"n": 3, "s": "港"}, {"n": None, "s": None}, {"n": 3, "s": "港"},
        {"n": 2, "s": "β"}, {"n": 1, "s": ""}]
RIGHT = [{"n": None, "s": None}, {"n": 2, "s": "β"}, {"n": 9, "s": " "}, {"n": 9, "s": " "}]
POLICY = {"memory_bytes": 32 << 20, "max_parallelism": 1}


def oracle(left, right, kind):
    """Small independent full-value reference; Python is only the test oracle."""
    selected = list(left) + list(right) if kind in ("union_all", "union") else [
        row for row in left if (row in right) == (kind == "intersect")]
    if kind == "union_all":
        return selected
    result = []
    for row in selected:
        if row not in result:
            result.append(row)
    return result


def batch_frame(harness, rows, *, streaming=True):
    return frame(harness, [[], rows[:2], [], rows[2:], []], streaming=streaming)


def combine(left, right, kind, surface="dataframe"):
    if surface == "dataframe":
        return getattr(left, kind)(right)
    bindings = left._declared_sources()
    if left is not right:
        bindings += right._declared_sources()
    return SqlWorkflow(f"SELECT * FROM ({left._relation_statement()}) AS set_left {OPERATORS[kind]} "
                       f"SELECT * FROM ({right._relation_statement()}) AS set_right",
                       left.client, source_bindings=bindings)


def complete(harness, name, envelope, traces, flags, *, replay=0, grant=None):
    for trace in traces:
        assert trace["opened"] == 1 and trace["ended"] and trace["closed"], trace
    streaming = [trace for trace, enabled in zip(traces, flags, strict=True) if enabled]
    if streaming:
        harness.completed(name, envelope, sources=len(streaming),
                          batches=sum(trace["batches"] for trace in streaming),
                          rows=sum(trace["rows"] for trace in streaming))
        assert envelope.field_int("native_input_replay_sources") == replay
        assert envelope.field_int("native_input_replay_passes") == replay
    else:
        harness.envelope(name, envelope)
    require_native_allocation_peak(name, envelope, grant or POLICY["memory_bytes"])


def consume(harness, name, query, expected, traces, flags, terminal, *, replay=0, policy=None):
    assert all(trace["opened"] == 0 for trace in traces)
    requested = policy or POLICY
    if terminal == "collect":
        result = query.collect(check=True, **requested)
        actual, envelope = result.result_rows, result.envelope
        assert list(result.result_columns) == ["n", "s"]
    else:
        actual, retained, schema = [], [], None
        with query.iter_batches(batch_rows=2, **requested) as iterator:
            for batch in iterator:
                assert iterator.report is None and len(batch.result_rows) <= 2
                assert list(batch.result_columns) == ["n", "s"]
                if schema is None:
                    schema = batch.result_schema
                assert batch.result_schema == schema
                actual.extend(batch.result_rows)
                retained.append(batch)
            assert iterator.report is not None and iterator._process.poll() == 0
            envelope = iterator.report.envelope
        assert [row for batch in retained for row in batch.result_rows] == actual
    harness.values(name, actual, expected)
    complete(harness, name, envelope, traces, flags, replay=replay)


def run(harness):
    harness.json(harness.logs / "set-fixtures.json", {
        "schema": SCHEMA, "left": LEFT, "right": RIGHT, "policy": POLICY,
        "oracles": {kind: oracle(LEFT, RIGHT, kind) for kind in KINDS},
        "membership_state": "credited resident state by default; explicit spill admits bounded native distinct-set ordering",
    })
    for kind in KINDS:
        for surface in ("dataframe", "sql"):
            for terminal in ("collect", "batches"):
                for flags in ((True, True), (True, False), (False, True), (False, False)):
                    name = f"set-{kind}-{surface}-{terminal}-stream-{int(flags[0])}{int(flags[1])}"
                    def check(name=name, kind=kind, surface=surface, terminal=terminal, flags=flags):
                        left, a = batch_frame(harness, LEFT, streaming=flags[0])
                        right, b = batch_frame(harness, RIGHT, streaming=flags[1])
                        consume(harness, name, combine(left, right, kind, surface), oracle(LEFT, RIGHT, kind),
                                [a, b], flags, terminal)
                    harness.case(name, check)

                name = f"set-{kind}-{surface}-{terminal}-repeated"
                def repeated(name=name, kind=kind, surface=surface, terminal=terminal):
                    left, trace = batch_frame(harness, LEFT)
                    consume(harness, name, combine(left, left, kind, surface), oracle(LEFT, LEFT, kind),
                            [trace], [True], terminal, replay=1)
                harness.case(name, repeated)

        for shape, a_rows, b_rows in (("empty-left", [], RIGHT), ("empty-right", LEFT, []),
                                      ("empty-both", [], []), ("all-null", [{"n": None, "s": None}] * 3,
                                                                       [{"n": None, "s": None}] * 2)):
            for terminal in ("collect", "batches"):
                name = f"set-{kind}-{shape}-{terminal}"
                def empty(name=name, kind=kind, a_rows=a_rows, b_rows=b_rows, terminal=terminal):
                    left, a = batch_frame(harness, a_rows)
                    right, b = batch_frame(harness, b_rows)
                    consume(harness, name, combine(left, right, kind), oracle(a_rows, b_rows, kind),
                            [a, b], [True, True], terminal)
                harness.case(name, empty)

    native = harness.output / "set-ordinary.vortex"
    result = harness.context.from_rows(RIGHT, schema=SCHEMA).write_vortex(native, **POLICY)
    harness.envelope("set-ordinary-fixture", result.envelope)
    for kind in KINDS:
        for stream_right in (False, True):
            name = f"set-{kind}-native-file-stream-right-{stream_right}"
            def mixed(name=name, kind=kind, stream_right=stream_right):
                source, trace = batch_frame(harness, LEFT)
                ordinary = harness.context.read_vortex(native)
                left, right = (ordinary, source) if stream_right else (source, ordinary)
                expected = oracle(RIGHT, LEFT, kind) if stream_right else oracle(LEFT, RIGHT, kind)
                consume(harness, name, combine(left, right, kind), expected, [trace], [True], "batches")
            harness.case(name, mixed)

    destinations(harness)
    from native_streaming_set_composition_cases import run as composition
    composition(harness)


def destinations(harness):
    def accepted(name, report):
        harness.envelope(name, report.envelope)
        return report.envelope

    def values(name, actual, expected, _destination):
        harness.values(name, actual, expected)

    for kind in KINDS:
        for destination in (*LOCAL_FORMATS, "fanout"):
            name = f"set-{kind}-write-{destination}"
            def write(name=name, kind=kind, destination=destination):
                left, a = batch_frame(harness, LEFT)
                right, b = batch_frame(harness, RIGHT)
                query = combine(left, right, kind)
                expected = oracle(LEFT, RIGHT, kind)
                formats = LOCAL_FORMATS if destination == "fanout" else [destination]
                targets = {format: harness.output / f"{name}.{format}" for format in formats}
                assert a["opened"] == b["opened"] == 0
                report = (query.fanout(targets, **POLICY) if destination == "fanout" else
                          getattr(query, "write_" + destination)(targets[destination], **POLICY))
                complete(harness, name, report.envelope, [a, b], [True, True])
                if destination == "fanout":
                    assert report.envelope.field_int("native_vortex_result_export_execution_count") == 1
                    assert report.envelope.field_int("native_vortex_result_export_target_count") == len(formats)
                    assert report.envelope.field_int("native_vortex_result_export_replay_count") == len(formats)
                for format, path in targets.items():
                    verify_output(harness.context, path, format, expected, ("n", "s"),
                                  name=name + "-" + format, accepted=accepted, complete=values)
                return {"artifacts": [harness.artifact(path) for path in targets.values()],
                        "complete_rows_per_output": len(expected), "producer_executions": [a["opened"], b["opened"]]}
            harness.case(name, write)

    for format in LOCAL_FORMATS:
        name = "set-empty-write-" + format
        def empty(name=name, format=format):
            left, trace = batch_frame(harness, LEFT)
            query = combine(left, left, "except_")
            path = harness.output / f"{name}.{format}"
            report = getattr(query, "write_" + format)(path, **POLICY)
            complete(harness, name, report.envelope, [trace], [True], replay=1)
            verify_output(harness.context, path, format, [], ("n", "s"),
                          name=name + "-reopen", accepted=accepted, complete=values)
            return {"artifact": harness.artifact(path), "complete_rows": 0}
        harness.case(name, empty)
