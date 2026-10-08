# SPDX-License-Identifier: Apache-2.0
"""Public streamed ordering with complete values, native runs and failure cleanup.

The public grant is the existing 1 GiB minimum. Native Rust tests separately
exercise a 4x-larger-than-grant input at 8 MiB; this module makes no such public
claim. It uses an explicit 1 MiB flush threshold to force native ordering runs.
"""

from __future__ import annotations

import gzip
import hashlib
import json
import time

from shardloom.errors import ShardLoomCommandError

ROWS = 65 * 1024 + 7
BATCH_ROWS = 1024
TEXT = "λ" * 256
GRANT = 1 << 30
SCHEMA = {"n": "int64", "s": "utf8"}


def encoded(row):
    return (json.dumps(row, sort_keys=True, ensure_ascii=False, allow_nan=False,
                       separators=(",", ":")) + "\n").encode("utf-8")


def run(harness):
    workspace = harness.output / "ordering-spill"
    workspace.mkdir()
    spill = {"workspace": str(workspace), "quota_bytes": 128 << 20, "buffer_bytes": 1 << 20}
    policy = {"memory_gb": 1, "max_parallelism": 1}
    oracle = hashlib.sha256()
    for n in range(ROWS):
        oracle.update(encoded({"n": n, "s": TEXT}))
    expected_sha = oracle.hexdigest()
    harness.json(harness.logs / "ordering-pressure-frozen-oracle.json", {
        "rows": ROWS, "input_order": "descending n", "text": TEXT,
        "input_batch_rows": BATCH_ROWS, "output_order": "ascending n",
        "all_output_rows_sha256": expected_sha, "native_grant_bytes": GRANT,
        "spill": spill, "larger_than_grant_public_claim": False,
        "whole_process_memory_ceiling": False, "performance_claim": False,
    })

    def clean():
        assert list(workspace.iterdir()) == [], "native order runs were not cleaned"

    def frame(failure=None):
        trace = {"batches": 0, "rows": 0, "ended": False, "closed": False, "damaged_run": False}
        def produce():
            try:
                for start in range(0, ROWS, BATCH_ROWS):
                    if trace["batches"] == 8 and failure == "producer":
                        raise RuntimeError("late spilled source sentinel")
                    if trace["batches"] == 8 and failure == "corrupt":
                        runs = list(workspace.glob("shardloom-query-*/*.vortex"))
                        assert runs, "corruption case did not reach a real native run"
                        run = sorted(runs)[0]
                        assert not run.is_symlink() and run.parent.parent == workspace
                        # Keep the owned file identity; the next read must detect damage.
                        with run.open("r+b") as stream:
                            stream.truncate(1)
                        trace["damaged_run"] = True
                    batch = [{"n": ROWS - row - 1, "s": TEXT}
                             for row in range(start, min(start + BATCH_ROWS, ROWS))]
                    trace["batches"] += 1
                    trace["rows"] += len(batch)
                    yield batch
                trace["ended"] = True
            finally:
                trace["closed"] = True
        return harness.context.from_batches(produce(), schema=SCHEMA, streaming=True).sort("n"), trace

    def verified(name, envelope, trace, *, spilled):
        assert trace["ended"] and trace["closed"] and trace["rows"] == ROWS
        harness.completed(name, envelope, batches=ROWS // BATCH_ROWS + 1, rows=ROWS)
        assert envelope.field_int("native_input_ordering_rows_detached") == ROWS
        assert envelope.field_int("resident_memory_limit_bytes") == GRANT
        assert envelope.field_int("resident_peak_reserved_buffer_bytes") <= GRANT
        assert envelope.field_bool("spill_io_performed") is spilled
        if spilled:
            assert envelope.field_int("relational_spill_merge_passes") > 1
            assert envelope.field_int("relational_spill_runs_written") > 3
            assert envelope.field_int("relational_spill_peak_disk_bytes") <= spill["quota_bytes"]
            assert envelope.field_bool("relational_spill_owned_cleanup_completed") is True
        clean()

    def capture(name, iterator, trace=None, *, slow=False):
        path = harness.logs / (name + "-complete-rows.jsonl.gz")
        actual_hash = hashlib.sha256()
        count, first = 0, None
        started = time.monotonic()
        with path.open("xb") as raw, gzip.GzipFile(fileobj=raw, mode="wb", mtime=0) as stream:
            with iterator:
                for batch in iterator:
                    if first is None:
                        first = time.monotonic() - started
                    if trace is not None:
                        assert trace["ended"], "ordering delivered before source completion"
                    assert iterator.report is None
                    if slow:
                        time.sleep(0.001)
                    for row in batch.result_rows:
                        assert row == {"n": count, "s": TEXT}, (name, count)
                        data = encoded(row)
                        stream.write(data)
                        actual_hash.update(data)
                        count += 1
                assert iterator.report is not None and iterator._process.poll() == 0
                report = iterator.report.envelope
        assert count == ROWS and actual_hash.hexdigest() == expected_sha
        copied_hash, reopened = hashlib.sha256(), 0
        with gzip.open(path, "rb") as stream:
            for line in stream:
                assert json.loads(line) == {"n": reopened, "s": TEXT}
                copied_hash.update(line)
                reopened += 1
        assert reopened == ROWS and copied_hash.hexdigest() == expected_sha
        return report, {"rows_verified": count, "complete_rows": harness.artifact(path),
                        "all_output_rows_sha256": actual_hash.hexdigest(),
                        "first_provisional_seconds": first,
                        "complete_delivery_and_artifact_verification_seconds": time.monotonic() - started,
                        "performance_claim": False}

    for spilled in (False, True):
        name = "order-pressure-spill" if spilled else "order-pressure-resident"
        def consume(name=name, spilled=spilled):
            query, trace = frame()
            requested = dict(policy, spill=spill) if spilled else policy
            envelope, observation = capture(name, query.iter_batches(batch_rows=511, **requested),
                                            trace, slow=spilled)
            verified(name, envelope, trace, spilled=spilled)
            observation.update(native_grant_bytes=GRANT,
                               peak_reserved_bytes=envelope.field_int("resident_peak_reserved_buffer_bytes"),
                               larger_than_grant_public_claim=False)
            return observation
        harness.case(name, consume)

    def write():
        query, trace = frame()
        target = harness.output / "ordered-pressure.vortex"
        written = query.write_vortex(target, **dict(policy, spill=spill))
        verified("order-pressure-write", written.envelope, trace, spilled=True)
        envelope, observation = capture("order-pressure-reopen",
            harness.context.read_vortex(target).iter_batches(batch_rows=511, **policy))
        harness.envelope("order-pressure-reopen", envelope)
        observation["native_output"] = harness.artifact(target)
        return observation
    harness.case("order-pressure-write", write)

    for failure in ("producer", "quota", "corrupt"):
        name = "order-pressure-failed-" + failure
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
                    message = json.dumps(error.envelope.raw)
                    assert ("quota" if failure == "quota" else "changed") in message
                else:
                    assert failure == "producer" and str(error) == "late spilled source sentinel"
            else:
                raise AssertionError("failed streamed ordering published output")
            assert trace["closed"] and not target.exists()
            assert set(harness.output.iterdir()) == before
            if failure == "corrupt":
                assert trace["damaged_run"]
            clean()
            return {"source_closed": True, "published": False, "spill_cleaned": True,
                    "source_batches_pulled": trace["batches"], "damaged_run": trace["damaged_run"]}
        harness.case(name, fail)
