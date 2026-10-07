#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""One frozen finite input workflow, with streaming/resident and grant controls.

Run under a serial workload/deadline supervisor. The default source carries
4.5 GiB of UTF8 payload in 1,152 batches; only one exact row per batch survives.
Every complete result is compared with a Python value oracle. No performance,
unbounded-source, input-spill or whole-process memory guarantee is inferred.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import sys
import time

from local_uat_storage import GIB, require_local_path
from run_native_streaming_input_uat import Harness, sha
from shardloom.errors import ShardLoomCommandError

ROWS_PER_BATCH = 1024
VALUE_BYTES = 4096


def row_at(index):
    return {"n": index, "s": f"{index:012d}|" + chr(97 + index % 26) * (VALUE_BYTES - 13)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--uat-root", type=Path, required=True)
    parser.add_argument("--input-mode", choices=("streaming", "resident"), required=True)
    parser.add_argument("--output-mode", choices=("batches", "vortex"), required=True)
    parser.add_argument("--memory-gb", type=int, required=True)
    parser.add_argument("--batches", type=int, default=1152)
    parser.add_argument("--expect-denial", action="store_true")
    parser.add_argument("--require-fourfold-input", action="store_true")
    args = parser.parse_args()
    assert 1 <= args.batches <= 4096 and 1 <= args.memory_gb <= 6
    binary = args.binary.resolve(strict=True)
    root = require_local_path(args.uat_root, Path.home(), sys.platform)
    assert not root.exists(), "use a new output directory"
    harness = Harness(binary, root)
    harness.guard()
    harness.logs.mkdir(parents=True)
    harness.output.mkdir()
    expected = [row_at(index * ROWS_PER_BATCH) for index in range(args.batches)]
    oracle = json.dumps(expected, ensure_ascii=False, separators=(",", ":")).encode()
    # Native logical accounting includes values, offsets, validity and names.
    batch_logical = (ROWS_PER_BATCH * (8 + VALUE_BYTES) + (ROWS_PER_BATCH + 1) * 8
                     + 2 * ((ROWS_PER_BATCH + 7) // 8) + 2)
    logical_bytes = args.batches * batch_logical
    if args.require_fourfold_input:
        assert logical_bytes >= 4 * args.memory_gb * GIB
    code = Path(__file__).resolve()
    root_source = code.parents[1]
    sources = [code, code.with_name("run_native_streaming_input_uat.py"),
               *[root_source / "python/src/shardloom" / name for name in
                 ("query.py", "client.py", "context.py", "_batches.py", "_result_schema.py", "models.py")]]
    source_hashes = {str(path): sha(path) for path in sources}
    for path in sources:
        with (harness.logs / path.name).open("xb") as copy:
            copy.write(path.read_bytes())
    protocol = {"binary": str(binary), "binary_sha256": sha(binary), "source_hashes": source_hashes,
                "input_mode": args.input_mode, "output_mode": args.output_mode,
                "memory_grant_bytes": args.memory_gb * GIB, "max_parallelism": 1,
                "batches": args.batches, "rows_per_batch": ROWS_PER_BATCH,
                "input_rows": args.batches * ROWS_PER_BATCH, "input_logical_bytes": logical_bytes,
                "one_batch_logical_bytes": batch_logical, "payload_bytes_each_value": VALUE_BYTES,
                "expected_output_rows": len(expected), "oracle_sha256": hashlib.sha256(oracle).hexdigest(),
                "expect_denial": args.expect_denial, "require_fourfold_input": args.require_fourfold_input,
                "performance_claim": False, "whole_process_memory_ceiling": False,
                "timing_scope": "producer generation + typed intake + query + complete output and child exit; excludes oracle generation and output readback"}
    harness.json(root / "protocol.json", protocol)
    harness.json(harness.logs / "expected.json", expected)
    generated = 0
    ended = False
    closed = False
    def produce():
        nonlocal generated, ended, closed
        try:
            for batch in range(args.batches):
                if batch % 64 == 0:
                    harness.guard()
                start = batch * ROWS_PER_BATCH
                values = [row_at(index) for index in range(start, start + ROWS_PER_BATCH)]
                generated += 1
                yield values
            ended = True
        finally:
            closed = True
    query = harness.context.from_batches(produce(), schema={"n": "int64", "s": "utf8"},
                                         streaming=args.input_mode == "streaming")
    query = query.filter(f"n % {ROWS_PER_BATCH} = 0").select("n", "s")
    grant = {"memory_gb": args.memory_gb, "max_parallelism": 1}
    target = harness.output / "complete.vortex"
    status, first, iterator, result = "failed", None, None, None
    observed = []
    started = time.monotonic()
    report = {"status": "running", "protocol_sha256": sha(root / "protocol.json")}
    try:
        try:
            if args.output_mode == "vortex":
                result = query.write_vortex(target, **grant)
            else:
                iterator = query.iter_batches(batch_rows=1024, **grant)
                with iterator:
                    for batch in iterator:
                        if first is None:
                            first = time.monotonic() - started
                        observed.extend(batch.result_rows)
                        if args.input_mode == "streaming":
                            assert generated == len(observed), "producer pulled ahead of acknowledgement"
                        assert iterator.report is None
                    result = iterator.report
                    assert iterator._process.poll() == 0
        except ShardLoomCommandError as error:
            report["complete_operation_seconds"] = time.monotonic() - started
            harness.envelope("denied", error.envelope, success=False)
            assert args.expect_denial, error.envelope.raw
            assert "memory reservation denied" in json.dumps(error.envelope.raw).lower()
            assert not observed and result is None and not ended and closed
            assert 0 < generated < args.batches
            if iterator is not None:
                assert iterator.report is None and iterator._process.poll() is not None
            assert not list(harness.output.iterdir()), "denied operation retained staging/output"
            report["expected_memory_denial"] = True
        else:
            report["complete_operation_seconds"] = time.monotonic() - started
            assert not args.expect_denial and result is not None
            assert generated == args.batches and ended and closed
            envelope = result.envelope
            if args.input_mode == "streaming":
                harness.completed("complete", envelope, batches=args.batches, rows=args.batches * ROWS_PER_BATCH)
                assert envelope.field_int("native_input_logical_bytes") == logical_bytes
                assert envelope.field_int("native_input_max_retained_logical_bytes") == batch_logical
                assert envelope.field_int("native_input_max_retained_batches") == 1
            else:
                harness.envelope("complete", envelope)
                assert envelope.field_int("native_input_batches") == args.batches
                assert envelope.field_int("native_input_batch_rows") == args.batches * ROWS_PER_BATCH
            assert envelope.field_int("resident_memory_limit_bytes") == args.memory_gb * GIB
            assert envelope.field_int("resident_peak_reserved_buffer_bytes") <= args.memory_gb * GIB
            if args.output_mode == "vortex":
                reopened = harness.context.read_vortex(target).collect(check=True, **grant)
                harness.envelope("reopened", reopened.envelope)
                observed = list(reopened.result_rows)
                report["output_artifact"] = harness.artifact(target)
            harness.values("complete", observed, expected)
            report["complete_output_rows_verified"] = len(observed)
            report["peak_reserved_bytes"] = envelope.field_int("resident_peak_reserved_buffer_bytes")
        assert all(sha(Path(path)) == digest for path, digest in source_hashes.items())
        assert sha(binary) == protocol["binary_sha256"]
        harness.guard()
        status = "passed"
    finally:
        harness.client.close()
        report.update(status=status, generated_input_batches=generated, producer_ended=ended,
                      producer_closed=closed, first_provisional_seconds=first,
                      total_check_seconds=time.monotonic() - started,
                      performance_claim=False, whole_process_memory_ceiling=False)
        harness.json(root / "summary.json", report)
        print(json.dumps(report), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
