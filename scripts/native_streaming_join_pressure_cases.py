# SPDX-License-Identifier: Apache-2.0
"""Complete public join spill, native write/reopen and interrupted consumers.

This public workflow uses the existing 1 GiB minimum grant and an explicit
1 MiB flush threshold. Native tests separately establish completion under a
16 MiB grant with input larger than that grant. Neither boundary limits RSS.
"""

from __future__ import annotations

import gzip
import hashlib
import json
import time

from shardloom.errors import ShardLoomCommandError
from native_streaming_join_cases import ORDINARY, SCHEMA, joined
from native_streaming_order_pressure_cases import encoded

ROWS = 65_537
BATCH_ROWS = 1024
GRANT = 1 << 30


def key(ordinal):
    return None if ordinal % 31 == 0 else ordinal * 17 % ROWS


def text(ordinal):
    return f"{ordinal:09}" + "x" * 503


MATCHED = [next(row for row in range(ROWS) if key(row) == value) for value in (1, 3)]


def expected():
    yield {"left_value": "r1", "right_value": text(MATCHED[0])}
    yield {"left_value": "r3", "right_value": text(MATCHED[1])}
    yield {"left_value": "rn", "right_value": None}
    for row in range(ROWS):
        if row not in MATCHED:
            yield {"left_value": None, "right_value": text(row)}


def run(harness):
    workspace = harness.output / "join-pressure-spill"
    workspace.mkdir()
    spill = {"workspace": str(workspace), "quota_bytes": 256 << 20, "buffer_bytes": 1 << 20}
    policy = {"memory_gb": 1, "max_parallelism": 1}
    ordinary = harness.context.from_rows(ORDINARY, schema=SCHEMA)
    digest = hashlib.sha256()
    for row in expected():
        digest.update(encoded(row))
    expected_sha = digest.hexdigest()
    harness.json(harness.logs / "join-pressure-frozen-oracle.json", {
        "rows": ROWS, "input_batch_rows": BATCH_ROWS, "key_formula": "ordinal * 17 % 65537",
        "null_rule": "ordinal % 31 == 0", "payload_bytes": 512, "matched_ordinals": MATCHED,
        "output_order": "probe order then original unmatched build ordinals", "complete_output_rows": ROWS + 1,
        "all_output_rows_sha256": expected_sha, "native_grant_bytes": GRANT, "spill": spill,
        "larger_than_grant_public_claim": False, "whole_process_memory_ceiling": False,
    })

    def clean():
        assert list(workspace.iterdir()) == [], "join retained owned spill state"

    def frame(failure=None):
        trace = {"batches": 0, "rows": 0, "ended": False, "closed": False, "damaged_run": False}
        def produce():
            try:
                for start in range(0, ROWS, BATCH_ROWS):
                    if trace["batches"] == 8 and failure in ("producer", "corrupt"):
                        runs = sorted(workspace.glob("shardloom-query-*/*.vortex"))
                        assert runs, "failure must follow actual join run creation"
                        if failure == "producer":
                            raise RuntimeError("late join spill producer")
                        run = runs[0]
                        assert not run.is_symlink() and run.parent.parent == workspace
                        with run.open("r+b") as stream:
                            stream.truncate(1)
                        trace["damaged_run"] = True
                    batch = [{"n": key(row), "s": text(row)}
                             for row in range(start, min(start + BATCH_ROWS, ROWS))]
                    trace["batches"] += 1
                    trace["rows"] += len(batch)
                    yield batch
                trace["ended"] = True
            finally:
                trace["closed"] = True
        source = harness.context.from_batches(produce(), schema=SCHEMA, streaming=True)
        return joined(source, ordinary, True, "full"), trace

    def verified(name, envelope, trace, spilled):
        assert trace["ended"] and trace["closed"] and trace["rows"] == ROWS
        harness.completed(name, envelope, batches=ROWS // BATCH_ROWS + 1, rows=ROWS)
        assert envelope.field_int("resident_memory_limit_bytes") == GRANT
        assert envelope.field_int("resident_peak_reserved_buffer_bytes") <= GRANT
        assert envelope.field_int("native_input_join_build_rows_detached") == ROWS
        assert envelope.field_int("native_input_ordering_rows_detached") == 0
        assert envelope.field_int("relational_ordered_join_stages") == int(spilled)
        assert envelope.field_bool("spill_io_performed") is spilled
        if spilled:
            assert envelope.field_int("relational_ordered_join_build_rows") == ROWS
            assert envelope.field_int("relational_ordered_join_probe_rows") == 3
            assert envelope.field_int("relational_ordered_join_match_records") == 2
            assert envelope.field_int("relational_ordered_join_lookup_blocks") > 0
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
                        assert trace["ended"], "build source must end before join delivery"
                    if schema is None:
                        schema = batch.result_schema
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
        assert count == ROWS + 1 and digest.hexdigest() == expected_sha
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
        name = f"join-pressure-spill-{spilled}"
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
        target = harness.output / "join-pressure.vortex"
        result = query.write_vortex(target, **dict(policy, spill=spill))
        verified("join-pressure-write", result.envelope, trace, True)
        envelope, observation = capture("join-pressure-reopen",
            harness.context.read_vortex(target).iter_batches(batch_rows=257, **policy))
        harness.envelope("join-pressure-reopen", envelope)
        observation["native_output"] = harness.artifact(target)
        return observation
    harness.case("join-pressure-write", writer)

    for failure in ("producer", "quota", "corrupt"):
        name = "join-pressure-failed-" + failure
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
                    assert failure == "producer" and str(error) == "late join spill producer"
            else:
                raise AssertionError("failed join published output")
            assert trace["closed"] and not target.exists() and set(harness.output.iterdir()) == before
            if failure == "corrupt":
                assert trace["damaged_run"]
            clean()
            return {"source_closed": True, "published": False, "spill_cleaned": True,
                    "damaged_run": trace["damaged_run"], "source_batches_pulled": trace["batches"]}
        harness.case(name, fail)

    for failure in (False, True):
        name = "join-pressure-consumer-failure" if failure else "join-pressure-consumer-close"
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
                        raise RuntimeError("join downstream consumer")
            except RuntimeError as error:
                assert failure and str(error) == "join downstream consumer"
            assert iterator.report is None and iterator._process.poll() is not None
            assert trace["closed"]
            clean()
            return {"successful_prefix": False, "source_closed": True, "child_exited": True}
        harness.case(name, consumer)
