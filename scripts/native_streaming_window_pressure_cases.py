# SPDX-License-Identifier: Apache-2.0
"""Complete public window spill and publication under a declared local grant.

Native tests establish larger-than-grant completion at 16 MiB. This public path
uses the existing 1 GiB minimum and a 1 MiB flush threshold; it does not bound RSS.
"""

from __future__ import annotations

import gzip
import hashlib
import json
import time

from shardloom.errors import ShardLoomCommandError
from native_streaming_order_pressure_cases import encoded

ROWS = 24_013
BATCH_ROWS = 997
GRANT = 1 << 30
SCHEMA = {"n": "int64", "s": "utf8"}
FRAME = "ORDER BY n ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"
EXPRESSIONS = [f"{function} OVER ({FRAME}) AS {name}" for name, function in (
    ("distinct_values", "COUNT(DISTINCT n)"), ("minimum", "MIN(n)"),
    ("maximum", "MAX(n)"), ("total", "SUM(n)"),
)]


def text(value):
    return f"{value:09}" + "x" * 759


def expected():
    for n in range(ROWS - 1, -1, -1):
        yield {"n": n, "s": text(n), "distinct_values": n + 1, "minimum": 0,
               "maximum": n, "total": float(n * (n + 1) // 2)}


def run(harness):
    workspace = harness.output / "window-pressure-spill"
    workspace.mkdir()
    spill = {"workspace": str(workspace), "quota_bytes": 256 << 20, "buffer_bytes": 1 << 20}
    policy = {"memory_gb": 1, "max_parallelism": 1}
    digest = hashlib.sha256()
    for row in expected():
        digest.update(encoded(row))
    expected_sha = digest.hexdigest()
    harness.json(harness.logs / "window-pressure-frozen-oracle.json", {
        "rows": ROWS, "input_batch_rows": BATCH_ROWS, "payload_bytes": 768,
        "input_and_output_order": "descending n", "expressions": EXPRESSIONS,
        "oracle": {"distinct_values": "n + 1", "minimum": 0, "maximum": "n", "total": "n * (n + 1) / 2"},
        "all_output_rows_sha256": expected_sha, "native_grant_bytes": GRANT, "spill": spill,
        "larger_than_grant_public_claim": False, "whole_process_memory_ceiling": False,
    })

    def clean():
        assert list(workspace.iterdir()) == [], "window retained owned spill state"

    def frame(failure=None):
        trace = {"batches": 0, "rows": 0, "ended": False, "closed": False, "damaged_run": False}
        def produce():
            try:
                for start in range(0, ROWS, BATCH_ROWS):
                    if trace["batches"] == 8 and failure in ("producer", "corrupt"):
                        runs = sorted(workspace.glob("shardloom-query-*/*.vortex"))
                        assert runs, "failure must follow actual window run creation"
                        if failure == "producer":
                            raise RuntimeError("late window spill producer")
                        run = runs[0]
                        assert not run.is_symlink() and run.parent.parent == workspace
                        with run.open("r+b") as stream:
                            stream.truncate(1)
                        trace["damaged_run"] = True
                    batch = [{"n": ROWS - row - 1, "s": text(ROWS - row - 1)}
                             for row in range(start, min(start + BATCH_ROWS, ROWS))]
                    trace["batches"] += 1
                    trace["rows"] += len(batch)
                    yield batch
                trace["ended"] = True
            finally:
                trace["closed"] = True
        source = harness.context.from_batches(produce(), schema=SCHEMA, streaming=True)
        return source.window(*EXPRESSIONS), trace

    def verified(name, envelope, trace, spilled):
        assert trace["ended"] and trace["closed"] and trace["rows"] == ROWS
        harness.completed(name, envelope, batches=(ROWS + BATCH_ROWS - 1) // BATCH_ROWS, rows=ROWS)
        assert envelope.field_int("resident_memory_limit_bytes") == GRANT
        assert envelope.field_int("resident_peak_reserved_buffer_bytes") <= GRANT
        assert envelope.field_int("native_input_window_rows_detached") == ROWS
        assert envelope.field_int("native_input_ordering_rows_detached") == 0
        assert envelope.field_int("relational_ordered_window_stages") == int(spilled)
        assert envelope.field_bool("spill_io_performed") is spilled
        if spilled:
            for field, value in {"input_rows": ROWS, "groups": 1, "partitions": 1,
                                 "bounds_rows": ROWS * 3, "distinct_intervals": ROWS,
                                 "distinct_events": ROWS * 2}.items():
                assert envelope.field_int("relational_ordered_window_" + field) == value
            assert envelope.field_int("relational_ordered_window_extrema_summary_rows") > ROWS
            assert envelope.field_int("relational_ordered_window_lookup_blocks") > 0
            assert envelope.field_int("relational_spill_runs_written") > 3
            assert envelope.field_int("relational_spill_merge_passes") > 1
            assert envelope.field_int("relational_spill_peak_disk_bytes") <= spill["quota_bytes"]
            assert envelope.field_bool("relational_spill_owned_cleanup_completed") is True
        clean()

    def capture(name, iterator, trace=None, *, slow=False):
        path = harness.logs / (name + "-complete-rows.jsonl.gz")
        digest, count, schema = hashlib.sha256(), 0, None
        reference = iter(expected())
        with path.open("xb") as raw, gzip.GzipFile(fileobj=raw, mode="wb", mtime=0) as stream:
            with iterator:
                for batch in iterator:
                    assert iterator.report is None and len(batch.result_rows) <= 257
                    if trace is not None:
                        assert trace["ended"], "window must consume its complete source before delivery"
                    if schema is None:
                        schema = batch.result_schema
                        assert [(name, dtype.name) for name, dtype in schema] == [
                            ("n", "int64"), ("s", "utf8"), ("distinct_values", "uint64"),
                            ("minimum", "int64"), ("maximum", "int64"), ("total", "float64")]
                    assert batch.result_schema == schema
                    if slow:
                        time.sleep(0.001)
                    for row in batch.result_rows:
                        assert row == next(reference), (name, count)
                        data = encoded(row)
                        stream.write(data)
                        digest.update(data)
                        count += 1
                assert next(reference, None) is None
                assert iterator.report is not None and iterator._process.poll() == 0
                envelope = iterator.report.envelope
        assert count == ROWS and digest.hexdigest() == expected_sha
        digest, reference, reopened = hashlib.sha256(), iter(expected()), 0
        with gzip.open(path, "rb") as stream:
            for line in stream:
                assert json.loads(line) == next(reference)
                digest.update(line)
                reopened += 1
        assert next(reference, None) is None and reopened == count and digest.hexdigest() == expected_sha
        return envelope, {"complete_output_rows": count, "complete_rows": harness.artifact(path),
                          "all_output_rows_sha256": expected_sha, "native_grant_bytes": GRANT,
                          "larger_than_grant_public_claim": False, "performance_claim": False}

    for spilled in (False, True):
        name = f"window-pressure-spill-{spilled}"
        def check(name=name, spilled=spilled):
            query, trace = frame()
            requested = dict(policy, spill=spill) if spilled else policy
            envelope, observation = capture(name, query.iter_batches(batch_rows=257, **requested), trace, slow=spilled)
            verified(name, envelope, trace, spilled)
            observation["peak_reserved_bytes"] = envelope.field_int("resident_peak_reserved_buffer_bytes")
            return observation
        harness.case(name, check)

    def writer():
        query, trace = frame()
        target = harness.output / "window-pressure.vortex"
        result = query.write_vortex(target, **dict(policy, spill=spill))
        verified("window-pressure-write", result.envelope, trace, True)
        envelope, observation = capture("window-pressure-reopen",
            harness.context.read_vortex(target).iter_batches(batch_rows=257, **policy))
        harness.envelope("window-pressure-reopen", envelope)
        observation["native_output"] = harness.artifact(target)
        return observation
    harness.case("window-pressure-write", writer)

    for failure in ("producer", "quota", "corrupt"):
        name = "window-pressure-failed-" + failure
        def fail(name=name, failure=failure):
            query, trace = frame(failure)
            requested = dict(spill, quota_bytes=32 << 10) if failure == "quota" else spill
            target = harness.output / (name + ".vortex")
            before = set(harness.output.iterdir())
            try:
                query.write_vortex(target, **dict(policy, spill=requested))
            except RuntimeError as error:
                if isinstance(error, ShardLoomCommandError):
                    harness.envelope(name, error.envelope, success=False)
                    assert failure != "producer"
                    assert ("quota" if failure == "quota" else "changed") in json.dumps(error.envelope.raw)
                else:
                    assert failure == "producer" and str(error) == "late window spill producer"
            else:
                raise AssertionError("failed window published output")
            assert trace["closed"] and not target.exists() and set(harness.output.iterdir()) == before
            if failure == "corrupt":
                assert trace["damaged_run"]
            clean()
            return {"source_closed": True, "published": False, "spill_cleaned": True,
                    "damaged_run": trace["damaged_run"], "source_batches_pulled": trace["batches"]}
        harness.case(name, fail)

    for failure in (False, True):
        name = "window-pressure-consumer-failure" if failure else "window-pressure-consumer-close"
        def consumer(name=name, failure=failure):
            query, trace = frame()
            iterator = query.iter_batches(batch_rows=257, **dict(policy, spill=spill))
            try:
                with iterator:
                    batch = next(iterator)
                    reference = iter(expected())
                    assert list(batch.result_rows) == [next(reference) for _ in batch.result_rows]
                    assert trace["ended"] and iterator.report is None
                    if failure:
                        raise RuntimeError("window downstream consumer")
            except RuntimeError as error:
                assert failure and str(error) == "window downstream consumer"
            assert iterator.report is None and iterator._process.poll() is not None
            assert trace["closed"]
            clean()
            return {"successful_prefix": False, "source_closed": True, "child_exited": True}
        harness.case(name, consumer)
