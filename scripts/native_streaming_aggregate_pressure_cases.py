# SPDX-License-Identifier: Apache-2.0
"""Complete public group/DISTINCT execution and failure after native spill.

The public grant is 1 GiB. Independent native tests prove the larger-than-grant
condition at 16 MiB. Here the existing explicit flush policy forces real runs;
the reports keep these two resource conditions separate.
"""

from __future__ import annotations

import gzip
import hashlib
import json
import time

import shardloom as sl
from shardloom.errors import ShardLoomCommandError
from native_streaming_order_pressure_cases import encoded

GROUPS = 16_381
ROWS = 2 * GROUPS + 7
BATCH_ROWS = 1024
GRANT = 1 << 30


def key(ordinal):
    return ordinal * 17 % GROUPS


def text(value):
    return f"{value:09}" + "x" * 503


def number(value):
    return value if value % 31 else None


def expected(grouped):
    if grouped:
        for ordinal in range(GROUPS):
            value = key(ordinal)
            count = 2 + int(ordinal < 7)
            present = number(value) is not None
            yield {"s": text(value), "rows": count, "present": count if present else 0,
                   "unique": int(present), "total": float(value * count) if present else None,
                   "mean": float(value) if present else None,
                   "smallest": number(value), "largest": number(value)}
    else:
        total, count = 0.0, 0
        for ordinal in range(ROWS):
            value = number(key(ordinal))
            if value is not None:
                total += float(value)
                count += 1
        yield {"rows": ROWS, "present": count, "unique": GROUPS, "total": total,
               "mean": total / count, "smallest": 1, "largest": GROUPS - 1}


def run(harness):
    workspace = harness.output / "aggregate-pressure-spill"
    workspace.mkdir()
    spill = {"workspace": str(workspace), "quota_bytes": 256 << 20, "buffer_bytes": 1 << 20}
    policy = {"memory_gb": 1, "max_parallelism": 1}
    expected_hashes = {}
    for grouped in (False, True):
        digest = hashlib.sha256()
        for row in expected(grouped):
            digest.update(encoded(row))
        expected_hashes[grouped] = digest.hexdigest()
    harness.json(harness.logs / "aggregate-pressure-frozen-oracles.json", {
        "rows": ROWS, "groups": GROUPS, "input_batch_rows": BATCH_ROWS,
        "group_id_formula": "ordinal * 17 % 16381", "string_bytes": 512,
        "number_null_rule": "group_id % 31 == 0", "first_seven_groups_repeat_three_times": True,
        "all_output_rows_sha256": {str(grouped): digest for grouped, digest in expected_hashes.items()},
        "native_grant_bytes": GRANT, "spill": spill,
        "larger_than_grant_public_claim": False, "whole_process_memory_ceiling": False,
    })

    def clean():
        assert list(workspace.iterdir()) == [], "aggregate left owned native runs"

    def frame(grouped, failure=None):
        trace = {"batches": 0, "rows": 0, "ended": False, "closed": False, "damaged_run": False}
        def produce():
            try:
                for start in range(0, ROWS, BATCH_ROWS):
                    if trace["batches"] == 8 and failure in {"producer", "corrupt"}:
                        runs = sorted(workspace.glob("shardloom-query-*/*.vortex"))
                        assert runs, "failure must follow actual aggregate run creation"
                        if failure == "producer":
                            raise RuntimeError("late aggregate spill producer")
                        run = runs[0]
                        assert not run.is_symlink() and run.parent.parent == workspace
                        with run.open("r+b") as stream:
                            stream.truncate(1)
                        trace["damaged_run"] = True
                    batch = [{"s": text(key(ordinal)), "n": number(key(ordinal))}
                             for ordinal in range(start, min(start + BATCH_ROWS, ROWS))]
                    trace["batches"] += 1
                    trace["rows"] += len(batch)
                    yield batch
                trace["ended"] = True
            finally:
                trace["closed"] = True
        source = harness.context.from_batches(produce(), schema={"s": "utf8", "n": "int64"}, streaming=True)
        query = source.group_by("s") if grouped else source
        query = query.agg(rows="count(*)", present="count(n)",
                          unique=sl.count_distinct("n" if grouped else "s"),
                          total="sum(n)", mean="avg(n)", smallest="min(n)", largest="max(n)")
        return query, trace

    def verified(name, envelope, trace, grouped, spilled):
        assert trace["ended"] and trace["closed"] and trace["rows"] == ROWS
        harness.completed(name, envelope, batches=ROWS // BATCH_ROWS + 1, rows=ROWS)
        assert envelope.field_int("resident_memory_limit_bytes") == GRANT
        assert envelope.field_int("resident_peak_reserved_buffer_bytes") <= GRANT
        assert envelope.field_int("relational_ordered_aggregate_stages") == int(spilled)
        assert envelope.field_bool("spill_io_performed") is spilled
        if spilled:
            assert envelope.field_int("relational_ordered_aggregate_input_rows") == ROWS
            distinct_rows = sum(number(key(row)) is not None for row in range(ROWS)) if grouped else ROWS
            assert envelope.field_int("relational_ordered_aggregate_distinct_rows") == distinct_rows
            assert envelope.field_int("relational_spill_runs_written") > 3
            assert envelope.field_int("relational_spill_merge_passes") > 1
            assert envelope.field_int("relational_spill_peak_disk_bytes") <= spill["quota_bytes"]
            assert envelope.field_bool("relational_spill_owned_cleanup_completed") is True
        clean()

    def capture(name, iterator, grouped, trace=None, *, slow=False):
        path = harness.logs / (name + "-complete-rows.jsonl.gz")
        digest, count, schema = hashlib.sha256(), 0, None
        reference = iter(expected(grouped))
        with path.open("xb") as raw, gzip.GzipFile(fileobj=raw, mode="wb", mtime=0) as stream:
            with iterator:
                for batch in iterator:
                    assert iterator.report is None and len(batch.result_rows) <= 127
                    if trace is not None:
                        assert trace["ended"], "aggregate delivered before complete input"
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
        assert count == (GROUPS if grouped else 1) and digest.hexdigest() == expected_hashes[grouped]
        digest, reference, reopened = hashlib.sha256(), iter(expected(grouped)), 0
        with gzip.open(path, "rb") as stream:
            for line in stream:
                assert json.loads(line) == next(reference)
                digest.update(line)
                reopened += 1
        assert next(reference, None) is None and reopened == count
        assert digest.hexdigest() == expected_hashes[grouped]
        return envelope, {"complete_output_rows": count, "complete_rows": harness.artifact(path),
                          "all_output_rows_sha256": digest.hexdigest(), "native_grant_bytes": GRANT,
                          "larger_than_grant_public_claim": False, "performance_claim": False}

    for grouped in (False, True):
        for spilled in (False, True):
            name = f"aggregate-pressure-grouped-{grouped}-spill-{spilled}"
            def check(name=name, grouped=grouped, spilled=spilled):
                query, trace = frame(grouped)
                requested = dict(policy, spill=spill) if spilled else policy
                envelope, result = capture(name, query.iter_batches(batch_rows=127, **requested),
                                           grouped, trace, slow=grouped and spilled)
                verified(name, envelope, trace, grouped, spilled)
                result["peak_reserved_bytes"] = envelope.field_int("resident_peak_reserved_buffer_bytes")
                return result
            harness.case(name, check)

    def writer():
        query, trace = frame(True)
        target = harness.output / "aggregate-pressure.vortex"
        result = query.write_vortex(target, **dict(policy, spill=spill))
        verified("aggregate-pressure-write", result.envelope, trace, True, True)
        envelope, observation = capture("aggregate-pressure-reopen",
            harness.context.read_vortex(target).iter_batches(batch_rows=127, **policy), True)
        harness.envelope("aggregate-pressure-reopen", envelope)
        observation["native_output"] = harness.artifact(target)
        return observation
    harness.case("aggregate-pressure-write", writer)

    for failure in ("producer", "quota", "corrupt"):
        name = "aggregate-pressure-failed-" + failure
        def fail(name=name, failure=failure):
            query, trace = frame(True, failure)
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
                    assert failure == "producer" and str(error) == "late aggregate spill producer"
            else:
                raise AssertionError("failed aggregate published output")
            assert trace["closed"] and not target.exists()
            assert set(harness.output.iterdir()) == before
            if failure == "corrupt":
                assert trace["damaged_run"]
            clean()
            return {"source_closed": True, "published": False, "spill_cleaned": True,
                    "damaged_run": trace["damaged_run"], "source_batches_pulled": trace["batches"]}
        harness.case(name, fail)

    for failure in (False, True):
        name = "aggregate-pressure-consumer-failure" if failure else "aggregate-pressure-consumer-close"
        def consumer(name=name, failure=failure):
            query, trace = frame(True)
            iterator = query.iter_batches(batch_rows=127, **dict(policy, spill=spill))
            try:
                with iterator:
                    batch = next(iterator)
                    reference = iter(expected(True))
                    assert list(batch.result_rows) == [next(reference) for _ in batch.result_rows]
                    assert trace["ended"] and iterator.report is None
                    if failure:
                        raise RuntimeError("aggregate downstream consumer")
            except RuntimeError as error:
                assert failure and str(error) == "aggregate downstream consumer"
            assert iterator.report is None and iterator._process.poll() is not None
            assert trace["closed"]
            clean()
            return {"successful_prefix": False, "source_closed": True, "child_exited": True}
        harness.case(name, consumer)
