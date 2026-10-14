# SPDX-License-Identifier: Apache-2.0
"""Two producers share a constrained grant, real spill, and failure cleanup."""

from __future__ import annotations

import gzip
import hashlib
import json

from shardloom.errors import ShardLoomCommandError
from native_streaming_join_cases import ORDINARY, SCHEMA, joined
from native_streaming_join_pressure_cases import BATCH_ROWS, ROWS, expected, key, text
from native_streaming_order_pressure_cases import encoded
from native_report_evidence import require_native_allocation_peak


def run(harness):
    grant = 16 << 20
    policy = {"memory_bytes": grant, "max_parallelism": 1}
    workspace = harness.output / "multiple-pressure-spill"
    workspace.mkdir()
    foreign = workspace / "foreign"
    foreign.write_bytes(b"preserve")
    spill = {"workspace": str(workspace), "quota_bytes": 256 << 20, "buffer_bytes": 1 << 20}
    reference_digest = hashlib.sha256()
    for row in expected():
        reference_digest.update(encoded(row))
    expected_sha = reference_digest.hexdigest()
    harness.json(harness.logs / "multiple-pressure-fixtures.json", {
        "rows": ROWS, "batch_rows": BATCH_ROWS, "build_key_formula": "ordinal * 17 % 65537",
        "build_null_rule": "ordinal % 31 == 0", "build_text": "nine_digit_ordinal_then_503_x",
        "probe_batches": [ORDINARY[:1], ORDINARY[1:]], "schema": SCHEMA,
        "grant": grant, "spill": spill, "expected_rows": ROWS + 1, "expected_sha256": expected_sha,
    })

    def clean():
        assert foreign.read_bytes() == b"preserve"
        assert list(workspace.iterdir()) == [foreign]

    def live_runs():
        runs = sorted(workspace.glob("shardloom-query-*/*.vortex"))
        assert runs, "this fault must follow actual native spill"
        return runs

    def query(failure=None):
        traces = [{"batches": 0, "rows": 0, "ended": False, "closed": False, "opened": False} for _ in range(2)]
        fault = {"reached": False}
        def build():
            trace = traces[1]
            trace["opened"] = True
            try:
                for start in range(0, ROWS, BATCH_ROWS):
                    if trace["batches"] == 8 and failure == "build-producer":
                        live_runs()
                        fault["reached"] = True
                        raise RuntimeError("multiple build producer sentinel")
                    batch = [{"n": key(row), "s": text(row)}
                             for row in range(start, min(start + BATCH_ROWS, ROWS))]
                    trace["batches"] += 1
                    trace["rows"] += len(batch)
                    yield batch
                trace["ended"] = True
            finally:
                trace["closed"] = True
        def probe():
            trace = traces[0]
            trace["opened"] = True
            try:
                assert traces[1]["ended"] and traces[1]["closed"]
                if failure == "corrupt":
                    run = live_runs()[0]
                    assert not run.is_symlink() and run.parent.parent == workspace
                    with run.open("r+b") as stream:
                        stream.truncate(1)
                    fault["reached"] = True
                for index, batch in enumerate((ORDINARY[:1], ORDINARY[1:])):
                    if index == 1 and failure == "probe-producer":
                        live_runs()
                        fault["reached"] = True
                        raise RuntimeError("multiple probe producer sentinel")
                    trace["batches"] += 1
                    trace["rows"] += len(batch)
                    yield batch
                trace["ended"] = True
            finally:
                trace["closed"] = True
        first = harness.context.from_batches(probe, schema=SCHEMA, streaming=True)
        second = harness.context.from_batches(build, schema=SCHEMA, streaming=True)
        return joined(second, first, True, "full"), traces, fault

    def capture(name, iterator):
        reference = iter(expected())
        path = harness.logs / (name + "-values.jsonl.gz")
        digest, count = hashlib.sha256(), 0
        with path.open("xb") as raw, gzip.GzipFile(fileobj=raw, mode="wb", mtime=0) as stream:
            with iterator:
                for batch in iterator:
                    assert iterator.report is None
                    for row in batch.result_rows:
                        assert row == next(reference), (name, count)
                        data = encoded(row)
                        digest.update(data)
                        stream.write(data)
                        count += 1
                assert iterator.report is not None and iterator._process.poll() == 0
                envelope = iterator.report.envelope
        assert next(reference, None) is None and count == ROWS + 1 and digest.hexdigest() == expected_sha
        reference, count, digest = iter(expected()), 0, hashlib.sha256()
        with gzip.open(path, "rb") as stream:
            for line in stream:
                assert json.loads(line) == next(reference)
                count += 1
                digest.update(line)
        assert next(reference, None) is None and count == ROWS + 1 and digest.hexdigest() == expected_sha
        return envelope, {"values": harness.artifact(path), "complete_rows": count, "expected_sha256": expected_sha}

    def verify(name, envelope, traces):
        assert all(trace["ended"] and trace["closed"] for trace in traces)
        harness.completed(name, envelope, sources=2, batches=sum(trace["batches"] for trace in traces), rows=ROWS + 3)
        require_native_allocation_peak(name, envelope, grant)
        assert envelope.field_int("native_input_logical_bytes") > 2 * grant
        assert envelope.field_int("relational_ordered_join_build_rows") == ROWS
        assert envelope.field_int("relational_ordered_join_probe_rows") == 3
        assert envelope.field_int("relational_spill_runs_written") > 3
        assert envelope.field_int("relational_spill_merge_passes") > 1
        assert envelope.field_bool("relational_spill_owned_cleanup_completed") is True
        assert envelope.field_int("relational_spill_peak_disk_bytes") <= spill["quota_bytes"]
        clean()

    def denied():
        frame, traces, _ = query()
        with frame.iter_batches(batch_rows=257, **policy) as iterator:
            try:
                list(iterator)
            except ShardLoomCommandError as error:
                harness.envelope("multiple-pressure-resident-denied", error.envelope, success=False)
                assert "reservation denied" in json.dumps(error.envelope.raw)
            else:
                raise AssertionError("resident build unexpectedly fit the constrained grant")
            assert iterator.report is None and iterator._process.poll() is not None
        assert traces[1]["closed"] and not traces[1]["ended"]
        assert not traces[0]["opened"]
        clean()
    harness.case("multiple-pressure-resident-denied", denied)

    def streamed():
        frame, traces, _ = query()
        envelope, record = capture("multiple-pressure-streamed", frame.iter_batches(batch_rows=257, spill=spill, **policy))
        verify("multiple-pressure-streamed", envelope, traces)
        return record
    harness.case("multiple-pressure-streamed", streamed)

    def written():
        frame, traces, _ = query()
        target = harness.output / "multiple-pressure.vortex"
        result = frame.write_vortex(target, spill=spill, **policy)
        verify("multiple-pressure-written", result.envelope, traces)
        envelope, record = capture("multiple-pressure-reopened",
            harness.context.read_vortex(target).iter_batches(batch_rows=257, **policy))
        harness.envelope("multiple-pressure-reopened", envelope)
        record["output"] = harness.artifact(target)
        return record
    harness.case("multiple-pressure-write-reopen", written)

    for failure in ("build-producer", "probe-producer", "corrupt"):
        for write in (False, True):
            name = f"multiple-pressure-{failure}-write-{write}"
            def fail(name=name, failure=failure, write=write):
                frame, traces, fault = query(failure)
                before = set(harness.output.iterdir())
                iterator = None
                try:
                    if write:
                        frame.write_vortex(harness.output / (name + ".vortex"), spill=spill, **policy)
                    else:
                        iterator = frame.iter_batches(batch_rows=257, spill=spill, **policy)
                        with iterator:
                            for _ in iterator:
                                assert iterator.report is None
                except (RuntimeError, ShardLoomCommandError) as error:
                    assert fault["reached"], (name, error)
                    if isinstance(error, ShardLoomCommandError):
                        assert failure == "corrupt"
                        harness.envelope(name, error.envelope, success=False)
                    else:
                        assert str(error) == "multiple " + failure.replace("-", " ") + " sentinel"
                else:
                    raise AssertionError("two-source spill failure was certified")
                assert all(not trace["opened"] or trace["closed"] for trace in traces)
                if iterator is not None:
                    assert iterator.report is None and iterator._process.poll() is not None
                assert set(harness.output.iterdir()) == before
                clean()
            harness.case(name, fail)

    def stopped():
        frame, traces, _ = query()
        iterator = frame.iter_batches(batch_rows=257, spill=spill, **policy)
        with iterator:
            first = next(iterator)
            reference = iter(expected())
            assert first.result_rows == tuple(next(reference) for _ in first.result_rows)
            live_runs()
            assert iterator.report is None
        assert iterator.report is None and iterator._process.poll() is not None
        assert all(not trace["opened"] or trace["closed"] for trace in traces)
        clean()
    harness.case("multiple-pressure-early-consumer-close", stopped)
