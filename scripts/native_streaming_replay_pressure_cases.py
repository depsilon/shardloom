# SPDX-License-Identifier: Apache-2.0
"""Full replay values beyond the grant, native persistence and real run faults."""

from __future__ import annotations

import gzip
import hashlib
import json

from shardloom.errors import ShardLoomCommandError
from native_streaming_order_pressure_cases import encoded
from native_report_evidence import require_native_allocation_peak

ROWS = 8193
BATCH_ROWS = 251
WIDTH = 2048
SCHEMA = {"n": "int64", "s": "utf8"}


def row(index):
    return {"n": index * 17 % ROWS, "s": f"{index:08d}" + "p" * (WIDTH - 8)}


def expected():
    for index in range(ROWS):
        yield row(index)


def run(harness):
    grant = 16 << 20
    policy = {"memory_bytes": grant, "max_parallelism": 1}
    workspace = harness.output / "replay-pressure-spill"
    workspace.mkdir()
    foreign = workspace / "foreign"
    foreign.write_bytes(b"preserve")
    spill = {"workspace": str(workspace), "quota_bytes": 128 << 20, "buffer_bytes": 1 << 20}
    digest = hashlib.sha256()
    for value in expected():
        digest.update(encoded(value))
    expected_sha = digest.hexdigest()
    harness.json(harness.logs / "replay-pressure-fixtures.json", {
        "rows": ROWS, "batch_rows": BATCH_ROWS, "text_width": WIDTH,
        "key": "row_ordinal * 17 % 8193", "text": "eight_digit_row_ordinal_then_2040_p",
        "schema": SCHEMA, "grant": grant, "spill": spill, "expected_sha256": expected_sha,
        "query": "self_join;first_build_scan_projects_only_n;replayed_probe_delivers_original_n_and_s",
    })

    def clean():
        assert foreign.read_bytes() == b"preserve" and list(workspace.iterdir()) == [foreign]

    def live_runs():
        files = sorted(workspace.glob("shardloom-query-*/*.vortex"))
        assert files, "fault must follow actual native replay spill"
        return files

    def query(failure=None):
        trace = {"opened": 0, "batches": 0, "rows": 0, "ended": False, "closed": False, "fault": False}
        def produce():
            trace["opened"] += 1
            try:
                for start in range(0, ROWS, BATCH_ROWS):
                    if trace["batches"] == 6 and failure in ("producer", "corrupt"):
                        run = live_runs()[0]
                        trace["fault"] = True
                        if failure == "producer":
                            raise RuntimeError("replay pressure producer sentinel")
                        assert not run.is_symlink() and run.parent.parent == workspace
                        with run.open("r+b") as stream:
                            stream.truncate(1)
                    batch = [row(index) for index in range(start, min(start + BATCH_ROWS, ROWS))]
                    trace["batches"] += 1
                    trace["rows"] += len(batch)
                    yield batch
                trace["ended"] = True
            finally:
                trace["closed"] = True
        source = harness.context.from_batches(produce, schema=SCHEMA, streaming=True)
        frame = source.join(source.select("n"), on="n").select("f.n AS n", "f.s AS s")
        return frame, trace

    def capture(name, iterator):
        path = harness.logs / (name + "-values.jsonl.gz")
        reference, count, digest = iter(expected()), 0, hashlib.sha256()
        with path.open("xb") as raw, gzip.GzipFile(fileobj=raw, mode="wb", mtime=0) as stream:
            with iterator:
                for batch in iterator:
                    assert iterator.report is None and len(batch.result_rows) <= 257
                    for value in batch.result_rows:
                        assert value == next(reference), (name, count)
                        data = encoded(value)
                        digest.update(data)
                        stream.write(data)
                        count += 1
                assert iterator.report is not None and iterator._process.poll() == 0
                envelope = iterator.report.envelope
        assert next(reference, None) is None and count == ROWS and digest.hexdigest() == expected_sha
        reference, count, digest = iter(expected()), 0, hashlib.sha256()
        with gzip.open(path, "rb") as stream:
            for line in stream:
                assert json.loads(line) == next(reference)
                digest.update(line)
                count += 1
        assert next(reference, None) is None and count == ROWS and digest.hexdigest() == expected_sha
        return envelope, {"values": harness.artifact(path), "complete_rows": count, "expected_sha256": expected_sha}

    def verify(name, envelope, trace):
        assert trace["opened"] == 1 and trace["ended"] and trace["closed"]
        harness.completed(name, envelope, batches=trace["batches"], rows=ROWS)
        require_native_allocation_peak(name, envelope, grant)
        assert envelope.field_int("native_input_logical_bytes") > grant
        assert envelope.field_int("native_input_replay_buffer_bytes_copied") > grant
        for key, value in (("sources", 1), ("passes", 1), ("rows", ROWS),
                           ("rows_detached", ROWS), ("spilled_sources", 1)):
            assert envelope.field_int("native_input_replay_" + key) == value
        assert envelope.field_int("relational_spill_runs_written") > 3
        assert envelope.field_int("relational_spill_merge_passes") > 1
        assert envelope.field_bool("relational_spill_owned_cleanup_completed") is True
        assert envelope.field_int("relational_spill_peak_disk_bytes") <= spill["quota_bytes"]
        clean()

    def denied():
        frame, trace = query()
        with frame.iter_batches(batch_rows=257, **policy) as iterator:
            try:
                list(iterator)
            except ShardLoomCommandError as error:
                harness.envelope("replay-pressure-resident-denied", error.envelope, success=False)
                assert "reservation denied" in json.dumps(error.envelope.raw)
            else:
                raise AssertionError("resident replay unexpectedly fit the grant")
            assert iterator.report is None and iterator._process.poll() is not None
        assert trace["closed"] and not trace["ended"]
        clean()
    harness.case("replay-pressure-resident-denied", denied)

    def streamed():
        frame, trace = query()
        envelope, record = capture("replay-pressure-streamed", frame.iter_batches(batch_rows=257, spill=spill, **policy))
        verify("replay-pressure-streamed", envelope, trace)
        return record
    harness.case("replay-pressure-streamed", streamed)

    def written():
        frame, trace = query()
        target = harness.output / "replay-pressure.vortex"
        result = frame.write_vortex(target, spill=spill, **policy)
        verify("replay-pressure-written", result.envelope, trace)
        assert result.envelope.field_int("native_vortex_array_sink_scalar_values_materialized") > ROWS
        assert result.envelope.field("decode_materialization_boundary") == (
            "native_scan_arrays_to_native_flat_writer;bounded_adapter_scalar_copy;"
            "no_arrow_conversion;provider_decode_may_occur"
        )
        envelope, record = capture("replay-pressure-reopened", harness.context.read_vortex(target).iter_batches(batch_rows=257, **policy))
        harness.envelope("replay-pressure-reopened", envelope)
        record["output"] = harness.artifact(target)
        return record
    harness.case("replay-pressure-write-reopen", written)

    for failure in ("producer", "corrupt", "quota"):
        for write in (False, True):
            name = f"replay-pressure-{failure}-write-{write}"
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
                        message = json.dumps(error.envelope.raw)
                        assert failure != "producer"
                        if failure == "quota":
                            assert "quota" in message.lower()
                        else:
                            assert "byte length changed" in message or "prepared source changed" in message
                    else:
                        assert str(error) == "replay pressure producer sentinel"
                else:
                    raise AssertionError("failed native replay was certified")
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
    harness.case("replay-pressure-early-consumer-close", stopped)
