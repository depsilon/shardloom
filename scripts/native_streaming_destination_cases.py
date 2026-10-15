# SPDX-License-Identifier: Apache-2.0
"""Complete finite inputs through the shared local destination adapters."""

from __future__ import annotations

import json

from shardloom.errors import ShardLoomCommandError
from shardloom.query import SqlWorkflow
from native_report_evidence import require_native_allocation_peak
from native_streaming_join_cases import ORDINARY, STREAM, frame, joined, oracle
from native_streaming_dynamic_cases import EXPECTED, frame as dynamic_frame, pivot
from native_workflow_outputs import LOCAL_FORMATS, verify_output


def run(harness):
    policy = {"memory_bytes": 32 << 20, "max_parallelism": 1}
    workspace = harness.output / "destination-spill"
    workspace.mkdir()
    foreign = workspace / "foreign"
    foreign.write_bytes(b"preserve")
    spill = {"workspace": str(workspace), "quota_bytes": 128 << 20, "buffer_bytes": 1 << 20}
    harness.json(harness.logs / "destination-fixtures.json", {
        "stream": STREAM, "ordinary": ORDINARY, "dynamic_expected": EXPECTED,
        "policy": policy, "spill": spill, "formats": LOCAL_FORMATS,
    })

    def clean():
        assert foreign.read_bytes() == b"preserve" and list(workspace.iterdir()) == [foreign]
        assert not list(harness.output.glob(".shardloom-fanout-*"))

    def accepted(name, report):
        harness.envelope(name, report.envelope)
        return report.envelope

    def values(name, actual, expected, _destination):
        harness.values(name, actual, expected)

    for stored in (False, True):
        requested = dict(policy, spill=spill) if stored else policy
        for surface in ("dataframe", "sql"):
            for shape in ("multiple", "repeated", "dynamic", "zero"):
                name = f"destination-{shape}-{surface}-spill-{stored}"

                def composed(name=name, shape=shape, surface=surface, requested=requested):
                    first, trace = dynamic_frame(harness) if shape == "dynamic" else frame(harness)
                    traces = [trace]
                    if shape == "multiple":
                        other, other_trace = frame(harness, [ORDINARY])
                        traces.append(other_trace)
                        query = joined(first, other, False, "full")
                        expected = oracle(STREAM, ORDINARY, "full")
                        columns = ("left_value", "right_value")
                    elif shape == "repeated":
                        query = joined(first, first, False, "inner")
                        expected = oracle(STREAM, STREAM, "inner")
                        columns = ("left_value", "right_value")
                    elif shape == "dynamic":
                        query = pivot(first)
                        expected, columns = EXPECTED, ("entity", "pivot_a", "pivot_b")
                    else:
                        query = first.sort("n", nulls="last").limit(0)
                        expected, columns = [], ("n", "s")
                    if surface == "sql":
                        query = SqlWorkflow(query._relation_statement(), harness.client,
                                            source_bindings=query._declared_sources())
                    targets = {format: harness.output / f"{name}.{format}" for format in LOCAL_FORMATS}
                    assert all(trace["opened"] == 0 for trace in traces)
                    report = query.fanout(targets, **requested)
                    envelope = report.envelope
                    assert all(trace["opened"] == 1 and trace["ended"] and trace["closed"] for trace in traces)
                    harness.completed(name, envelope, sources=len(traces),
                        batches=sum(trace["batches"] for trace in traces), rows=sum(trace["rows"] for trace in traces))
                    for key, value in {"execution_count": 1, "target_count": 8, "replay_count": 8,
                                       "replay_rows_detached": len(expected)}.items():
                        assert envelope.field_int("native_vortex_result_export_" + key) == value, (key, envelope.raw)
                    assert envelope.field("native_vortex_result_export_execution_model") == "native_query_once_with_credited_result_replay"
                    assert envelope.field_bool("native_vortex_result_export_replay_performed") is True
                    assert envelope.field_int("native_input_replay_sources") == int(shape == "repeated")
                    if shape == "dynamic":
                        assert envelope.field("relational_schema_binding") == "during_execution"
                    require_native_allocation_peak(name, envelope, policy["memory_bytes"])
                    for format, path in targets.items():
                        verify_output(harness.context, path, format, expected, columns,
                            name=name + "-" + format, accepted=accepted, complete=values)
                    clean()
                    return {"artifacts": [harness.artifact(path) for path in targets.values()],
                            "input_producers": len(traces), "executions": 1,
                            "complete_rows_per_output": len(expected)}
                harness.case(name, composed)

    for fault in ("existing", "duplicate", "static-schema", "producer", "late-adapter", "dynamic-schema"):
        name = "destination-failure-" + fault

        def failed(name=name, fault=fault):
            trace = {"opened": 0, "ended": False, "closed": False}
            paths = [harness.output / (name + suffix) for suffix in (".vortex", ".parquet")]
            schema = {"n": "int64", "s": "utf8"}
            supplied = [{"n": 1, "s": "x" * 65_537 if fault == "late-adapter" else "value"}]
            if fault == "static-schema":
                schema = {"n": "int64", "s": {"type": "list", "item": "int64"}}
                supplied = [{"n": 1, "s": [1, None]}]

            def producer():
                trace["opened"] += 1
                try:
                    yield supplied
                    if fault == "producer":
                        raise RuntimeError("destination producer sentinel")
                    trace["ended"] = True
                finally:
                    trace["closed"] = True

            query = harness.context.from_batches(producer, schema=schema, streaming=True)
            if fault == "dynamic-schema":
                query, trace = dynamic_frame(harness)
                query = pivot(query).select("pivot_missing")
            targets = [("vortex", paths[0]), ("orc" if fault == "static-schema" else "parquet", paths[1])]
            if fault == "existing":
                paths[1].write_bytes(b"preserve existing")
            if fault == "duplicate":
                targets[1] = ("parquet", paths[0])
            before = set(harness.output.iterdir())
            try:
                query.fanout(targets, allow_overwrite=fault == "existing", spill=spill, **policy)
            except (ShardLoomCommandError, RuntimeError) as error:
                if fault == "producer":
                    assert type(error) is RuntimeError and str(error) == "destination producer sentinel"
                else:
                    assert isinstance(error, ShardLoomCommandError)
                    harness.envelope(name, error.envelope, success=False)
                    diagnostic = {"existing": "existing destination", "duplicate": "distinct",
                                  "static-schema": "ORC does not admit nested output",
                                  "late-adapter": "string", "dynamic-schema": "not present"}[fault]
                    assert diagnostic in json.dumps(error.envelope.raw), error.envelope.raw
            else:
                raise AssertionError("failed destination transaction published outputs")
            if fault in {"existing", "duplicate", "static-schema"}:
                assert trace["opened"] == 0
            else:
                assert trace["opened"] == 1 and trace["closed"]
                assert trace["ended"] is (fault != "producer")
            assert set(harness.output.iterdir()) == before
            assert not paths[0].exists()
            if fault == "existing":
                assert paths[1].read_bytes() == b"preserve existing"
            clean()
            return {"producer_opened": trace["opened"], "no_published_prefix": True}
        harness.case(name, failed)
