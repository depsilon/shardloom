# SPDX-License-Identifier: Apache-2.0
"""Complete set values beyond the grant; distinct state and source replay differ."""

from __future__ import annotations

import gzip
import hashlib
import json

from shardloom.errors import ShardLoomCommandError
from native_streaming_order_pressure_cases import encoded
from native_streaming_set_cases import KINDS, combine, complete
from native_report_evidence import require_native_allocation_peak

ROWS, BATCH_ROWS, WIDTH, GRANT = 8193, 251, 2048, 16 << 20
SCHEMA = {"n": "int64", "s": "utf8"}
POLICY = {"memory_bytes": GRANT, "max_parallelism": 1}


def row(value):
    return {"n": value, "s": f"{value:08d}" + "p" * (WIDTH - 8)}


def expected(kind, *, repeated=False):
    if repeated:
        for _ in range(2):
            yield from (row(index) for index in range(ROWS))
    elif kind == "union_all":
        for side in (0, 1):
            yield from (row(side * 64 + index % 128) for index in range(ROWS))
    else:
        selected = range(192) if kind == "union" else range(64, 128) if kind == "intersect" else range(64)
        yield from (row(value) for value in selected)


def capture(harness, name, iterator, reference_factory):
    path = harness.logs / (name + "-values.jsonl.gz")
    reference, count, digest = iter(reference_factory()), 0, hashlib.sha256()
    with path.open("xb") as raw, gzip.GzipFile(fileobj=raw, mode="wb", mtime=0) as stream:
        with iterator:
            for batch in iterator:
                assert iterator.report is None and len(batch.result_rows) <= 257
                for value in batch.result_rows:
                    assert value == next(reference, None), (name, count)
                    data = encoded(value)
                    digest.update(data)
                    stream.write(data)
                    count += 1
            assert iterator.report is not None and iterator._process.poll() == 0
            envelope = iterator.report.envelope
    assert next(reference, None) is None
    written_count, written_sha = count, digest.hexdigest()
    reference, count, digest = iter(reference_factory()), 0, hashlib.sha256()
    with gzip.open(path, "rb") as stream:
        for line in stream:
            assert json.loads(line) == next(reference, None)
            digest.update(line)
            count += 1
    assert next(reference, None) is None and count == written_count and digest.hexdigest() == written_sha
    return envelope, {"values": harness.artifact(path), "complete_rows": count, "expected_sha256": written_sha}


def run(harness):
    workspace = harness.output / "set-pressure-spill"
    workspace.mkdir()
    foreign = workspace / "foreign"
    foreign.write_bytes(b"preserve")
    spill = {"workspace": str(workspace), "quota_bytes": 128 << 20, "buffer_bytes": 1 << 20}
    oracles = {}
    for kind in KINDS:
        digest, count = hashlib.sha256(), 0
        for value in expected(kind):
            digest.update(encoded(value))
            count += 1
        oracles[kind] = {"rows": count, "sha256": digest.hexdigest()}
    harness.json(harness.logs / "set-pressure-fixtures.json", {
        "input_rows_per_source": ROWS, "batch_rows": BATCH_ROWS, "text_width": WIDTH,
        "schema": SCHEMA, "grant": GRANT, "spill": spill,
        "low_cardinality_key": "source_index * 64 + row_index % 128",
        "unique_key": "row_index", "text": "eight_digit_key_then_2040_p", "oracles": oracles,
        "set_membership_spill": True,
    })

    def clean():
        assert foreign.read_bytes() == b"preserve" and list(workspace.iterdir()) == [foreign]

    def source(side, unique=False, failure=None, offset=0, reverse=False):
        trace = {"opened": 0, "batches": 0, "rows": 0, "ended": False, "closed": False, "fault": False}
        def produce():
            trace["opened"] += 1
            try:
                for start in range(0, ROWS, BATCH_ROWS):
                    if trace["batches"] == 6 and failure in ("producer", "corrupt"):
                        runs = sorted(workspace.glob("shardloom-query-*/*.vortex"))
                        assert runs, "failure must follow actual native spill"
                        trace["fault"] = True
                        if failure == "producer":
                            raise RuntimeError("set replay producer sentinel")
                        damaged = runs[0]
                        assert not damaged.is_symlink() and damaged.parent.parent == workspace
                        with damaged.open("r+b") as stream:
                            stream.truncate(1)
                    batch = [row(offset + (ROWS - 1 - index if reverse else index)
                                 if unique else side * 64 + index % 128)
                             for index in range(start, min(start + BATCH_ROWS, ROWS))]
                    trace["batches"] += 1
                    trace["rows"] += len(batch)
                    yield batch
                trace["ended"] = True
            finally:
                trace["closed"] = True
        return harness.context.from_batches(produce, schema=SCHEMA, streaming=True), trace

    for kind in KINDS:
        name = "set-duplicate-pressure-" + kind
        def duplicate(name=name, kind=kind):
            left, a = source(0)
            right, b = source(1)
            query = combine(left, right, kind)
            envelope, record = capture(harness, name, query.iter_batches(batch_rows=257, **POLICY), lambda: expected(kind))
            complete(harness, name, envelope, [a, b], [True, True], grant=GRANT)
            assert record["complete_rows"] == oracles[kind]["rows"]
            assert record["expected_sha256"] == oracles[kind]["sha256"]
            assert envelope.field_int("native_input_logical_bytes") > GRANT * 2
            clean()
            return record
        harness.case(name, duplicate)

    for kind in ("union", "intersect", "except_"):
        name = f"set-membership-pressure-denied-{kind}-spill-False"
        def denied(name=name, kind=kind):
            left, a = source(0, unique=True)
            right, b = source(1, unique=True)
            query = combine(left, right, kind)
            iterator = query.iter_batches(batch_rows=257, **POLICY)
            try:
                with iterator:
                    for _ in iterator:
                        assert iterator.report is None
            except ShardLoomCommandError as error:
                harness.envelope(name, error.envelope, success=False)
                assert "reservation denied" in json.dumps(error.envelope.raw)
            else:
                raise AssertionError("unbounded distinct membership was admitted")
            assert iterator.report is None and iterator._process.poll() is not None
            assert a["rows"] + b["rows"] < ROWS * 2
            assert all(not trace["opened"] or trace["closed"] for trace in (a, b))
            clean()
        harness.case(name, denied)

    def verify_replay(name, envelope, trace):
        complete(harness, name, envelope, [trace], [True], replay=1, grant=GRANT)
        assert envelope.field_int("native_input_replay_spilled_sources") == 1
        assert envelope.field_int("native_input_replay_rows") == ROWS
        assert envelope.field_int("native_input_replay_rows_detached") == ROWS
        assert envelope.field_int("native_input_replay_buffer_bytes_copied") > GRANT
        assert envelope.field_int("relational_spill_runs_written") > 3
        assert envelope.field_int("relational_spill_merge_passes") > 1
        assert envelope.field_bool("relational_spill_owned_cleanup_completed") is True
        assert envelope.field_int("relational_spill_peak_disk_bytes") <= spill["quota_bytes"]
        clean()

    def repeated_denied():
        left, trace = source(0, unique=True)
        iterator = combine(left, left, "union_all").iter_batches(batch_rows=257, **POLICY)
        try:
            with iterator:
                for _ in iterator:
                    assert iterator.report is None
        except ShardLoomCommandError as error:
            harness.envelope("set-replay-resident-denied", error.envelope, success=False)
            assert "reservation denied" in json.dumps(error.envelope.raw)
        else:
            raise AssertionError("oversized resident source replay was admitted")
        assert iterator.report is None and iterator._process.poll() is not None
        assert trace["closed"] and not trace["ended"]
        clean()
    harness.case("set-replay-resident-denied", repeated_denied)

    for destination in ("batches", "vortex"):
        name = "set-replay-spilled-" + destination
        def repeated(name=name, destination=destination):
            left, trace = source(0, unique=True)
            query = combine(left, left, "union_all")
            reference = lambda: expected("union_all", repeated=True)
            if destination == "batches":
                envelope, record = capture(harness, name, query.iter_batches(batch_rows=257, spill=spill, **POLICY), reference)
            else:
                target = harness.output / (name + ".vortex")
                report = query.write_vortex(target, spill=spill, **POLICY)
                envelope = report.envelope
                reopened, record = capture(harness, name + "-reopen",
                    harness.context.read_vortex(target).iter_batches(batch_rows=257, **POLICY), reference)
                harness.envelope(name + "-reopen", reopened)
                require_native_allocation_peak(name + "-reopen", reopened, GRANT)
                record["output"] = harness.artifact(target)
            verify_replay(name, envelope, trace)
            return record
        harness.case(name, repeated)

    for failure in ("producer", "corrupt", "quota"):
        for write in (False, True):
            name = f"set-replay-{failure}-write-{write}"
            def fail(name=name, failure=failure, write=write):
                left, trace = source(0, unique=True, failure=failure)
                query = combine(left, left, "union_all")
                requested = dict(spill, quota_bytes=2 << 20) if failure == "quota" else spill
                before = set(harness.output.iterdir())
                iterator = None
                try:
                    if write:
                        query.write_vortex(harness.output / (name + ".vortex"), spill=requested, **POLICY)
                    else:
                        iterator = query.iter_batches(batch_rows=257, spill=requested, **POLICY)
                        with iterator:
                            for _ in iterator:
                                assert iterator.report is None
                except (RuntimeError, ShardLoomCommandError) as error:
                    assert trace["fault"] or failure == "quota"
                    if failure == "producer":
                        assert type(error) is RuntimeError and str(error) == "set replay producer sentinel"
                    else:
                        assert isinstance(error, ShardLoomCommandError)
                        harness.envelope(name, error.envelope, success=False)
                        assert ("quota" if failure == "quota" else "byte length changed") in json.dumps(error.envelope.raw)
                else:
                    raise AssertionError("failed source replay returned complete set output")
                assert trace["closed"] and set(harness.output.iterdir()) == before
                if iterator is not None:
                    assert iterator.report is None and iterator._process.poll() is not None
                clean()
            harness.case(name, fail)

    from native_streaming_set_spill_cases import run as membership
    membership(harness, source, spill, clean)
