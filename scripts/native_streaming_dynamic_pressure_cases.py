# SPDX-License-Identifier: Apache-2.0
"""Streaming pivot pressure, complete persisted values and real spill failures."""

from __future__ import annotations

import gzip
import hashlib
import json

from shardloom.errors import ShardLoomCommandError
from native_streaming_dynamic_cases import pivot
from native_streaming_order_pressure_cases import encoded
from native_report_evidence import require_native_allocation_peak

GROUPS = 6145
VALUES = (1e16, 1.0, -1e16, 3.0)
ROWS = GROUPS * len(VALUES)
BATCH_ROWS = 251
SCHEMA = {"entity": "utf8", "category": "utf8", "amount": "float64"}


def text(index):
    return f"{index:08d}" + "p" * 760


def expected():
    # The native contract updates each cell in source order; pairwise summation
    # would change this result. Every group sees 1e16, 1, -1e16, 3 in that order.
    for index in range(GROUPS):
        yield {"entity": text(index), "pivot_a": 3.0}


def run(harness):
    grant = 16 << 20
    policy = {"memory_bytes": grant, "max_parallelism": 1}
    workspace = harness.output / "dynamic-pressure-spill"
    workspace.mkdir()
    foreign = workspace / "foreign"
    foreign.write_bytes(b"preserve")
    spill = {"workspace": str(workspace), "quota_bytes": 256 << 20, "buffer_bytes": 1 << 20}
    digest = hashlib.sha256()
    for row in expected():
        digest.update(encoded(row))
    expected_sha = digest.hexdigest()
    harness.json(harness.logs / "dynamic-pressure-fixtures.json", {
        "rows": ROWS, "groups": GROUPS, "batch_rows": BATCH_ROWS, "values": VALUES,
        "entity": "eight_digit_group_ordinal_then_760_p", "group": "row_ordinal % 6145",
        "value": "values[row_ordinal // 6145]", "schema": SCHEMA, "grant": grant,
        "spill": spill, "expected_rows": GROUPS, "expected_sha256": expected_sha,
    })

    def clean():
        assert foreign.read_bytes() == b"preserve" and list(workspace.iterdir()) == [foreign]

    def live_runs():
        runs = sorted(workspace.glob("shardloom-query-*/*.vortex"))
        assert runs, "fault must follow actual native pivot spill"
        return runs

    def query(failure=None):
        trace = {"opened": 0, "batches": 0, "rows": 0, "ended": False, "closed": False, "fault": False}
        def produce():
            trace["opened"] += 1
            try:
                for start in range(0, ROWS, BATCH_ROWS):
                    if trace["batches"] == 32 and failure == "producer":
                        live_runs()
                        trace["fault"] = True
                        raise RuntimeError("dynamic pressure producer sentinel")
                    batch = [{"entity": text(row % GROUPS), "category": "a", "amount": VALUES[row // GROUPS]}
                             for row in range(start, min(start + BATCH_ROWS, ROWS))]
                    trace["batches"] += 1
                    trace["rows"] += len(batch)
                    yield batch
                if failure == "corrupt":
                    run = live_runs()[0]
                    assert not run.is_symlink() and run.parent.parent == workspace
                    with run.open("r+b") as stream:
                        stream.truncate(1)
                    trace["fault"] = True
                trace["ended"] = True
            finally:
                trace["closed"] = True
        source = harness.context.from_batches(produce, schema=SCHEMA, streaming=True)
        return pivot(source), trace

    def capture(name, iterator):
        path = harness.logs / (name + "-values.jsonl.gz")
        reference, count, digest = iter(expected()), 0, hashlib.sha256()
        with path.open("xb") as raw, gzip.GzipFile(fileobj=raw, mode="wb", mtime=0) as stream:
            with iterator:
                for batch in iterator:
                    assert iterator.report is None and len(batch.result_rows) <= 257
                    for row in batch.result_rows:
                        assert row == next(reference), (name, count)
                        data = encoded(row)
                        digest.update(data)
                        stream.write(data)
                        count += 1
                assert iterator.report is not None and iterator._process.poll() == 0
                envelope = iterator.report.envelope
        assert next(reference, None) is None and count == GROUPS and digest.hexdigest() == expected_sha
        reference, count, digest = iter(expected()), 0, hashlib.sha256()
        with gzip.open(path, "rb") as stream:
            for line in stream:
                assert json.loads(line) == next(reference)
                digest.update(line)
                count += 1
        assert next(reference, None) is None and count == GROUPS and digest.hexdigest() == expected_sha
        return envelope, {"values": harness.artifact(path), "complete_rows": count, "expected_sha256": expected_sha}

    def verify(name, envelope, trace):
        assert trace["opened"] == 1 and trace["ended"] and trace["closed"]
        harness.completed(name, envelope, batches=trace["batches"], rows=ROWS)
        require_native_allocation_peak(name, envelope, grant)
        assert envelope.field_int("native_input_logical_bytes") > grant
        assert envelope.field("relational_schema_binding") == "during_execution"
        for suffix, value in (("stages", 1), ("input_rows", ROWS), ("index_rows", GROUPS), ("cells", GROUPS), ("domains", 1)):
            assert envelope.field_int("relational_spilled_pivot_" + suffix) == value
        assert envelope.field_int("relational_spill_runs_written") > 8
        assert envelope.field_int("relational_spill_merge_passes") > 4
        assert envelope.field_int("relational_spilled_pivot_lookup_blocks") > 0
        assert envelope.field_bool("relational_spill_owned_cleanup_completed") is True
        assert envelope.field_int("relational_spill_peak_disk_bytes") <= spill["quota_bytes"]
        clean()

    def denied():
        frame, trace = query()
        with frame.iter_batches(batch_rows=257, **policy) as iterator:
            try:
                list(iterator)
            except ShardLoomCommandError as error:
                harness.envelope("dynamic-pressure-resident-denied", error.envelope, success=False)
                assert "reservation denied" in json.dumps(error.envelope.raw)
            else:
                raise AssertionError("resident sparse pivot unexpectedly fit the grant")
            assert iterator.report is None and iterator._process.poll() is not None
        assert trace["closed"] and not trace["ended"]
        clean()
    harness.case("dynamic-pressure-resident-denied", denied)

    def streamed():
        frame, trace = query()
        envelope, record = capture("dynamic-pressure-streamed", frame.iter_batches(batch_rows=257, spill=spill, **policy))
        verify("dynamic-pressure-streamed", envelope, trace)
        return record
    harness.case("dynamic-pressure-streamed", streamed)

    def written():
        frame, trace = query()
        target = harness.output / "dynamic-pressure.vortex"
        result = frame.write_vortex(target, spill=spill, **policy)
        verify("dynamic-pressure-written", result.envelope, trace)
        envelope, record = capture("dynamic-pressure-reopened", harness.context.read_vortex(target).iter_batches(batch_rows=257, **policy))
        harness.envelope("dynamic-pressure-reopened", envelope)
        record["output"] = harness.artifact(target)
        return record
    harness.case("dynamic-pressure-write-reopen", written)

    for failure in ("producer", "corrupt", "quota"):
        for write in (False, True):
            name = f"dynamic-pressure-{failure}-write-{write}"
            def fail(name=name, failure=failure, write=write):
                frame, trace = query(failure)
                requested = dict(spill, quota_bytes=2 << 20) if failure == "quota" else spill
                before = set(harness.output.iterdir())
                iterator = None
                try:
                    if write:
                        frame.write_vortex(harness.output / (name + ".vortex"), spill=requested, **policy)
                    else:
                        iterator = frame.iter_batches(batch_rows=257, spill=requested, **policy)
                        with iterator:
                            for _ in iterator:
                                assert iterator.report is None
                except (RuntimeError, ShardLoomCommandError) as error:
                    assert trace["fault"] or failure == "quota", (name, error)
                    if isinstance(error, ShardLoomCommandError):
                        harness.envelope(name, error.envelope, success=False)
                        assert failure != "producer"
                        if failure == "quota":
                            assert "quota" in json.dumps(error.envelope.raw).lower()
                    else:
                        assert str(error) == "dynamic pressure producer sentinel"
                else:
                    raise AssertionError("failed streaming pivot spill was certified")
                assert trace["closed"]
                if iterator is not None:
                    assert iterator.report is None and iterator._process.poll() is not None
                assert set(harness.output.iterdir()) == before
                clean()
            harness.case(name, fail)

    def stopped():
        frame, trace = query()
        iterator = frame.iter_batches(batch_rows=257, spill=spill, **policy)
        with iterator:
            first = next(iterator)
            reference = iter(expected())
            assert first.result_rows == tuple(next(reference) for _ in first.result_rows)
            assert trace["ended"] and trace["closed"]
            live_runs()
            assert iterator.report is None
        assert iterator.report is None and iterator._process.poll() is not None
        clean()
    harness.case("dynamic-pressure-early-consumer-close", stopped)
