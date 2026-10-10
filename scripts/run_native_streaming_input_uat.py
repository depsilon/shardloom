#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Verify completion-aware input through the real Python/native boundary.

Run under the serial workload/deadline supervisor, with a frozen --binary and a
new local-only --uat-root. Keeps exact results, native reports, raw adversarial
frames and failures. This is capability evidence, not a speedup or RSS claim.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import sys
import time

from local_uat_storage import GIB, MIB, check_budgets, require_local_path
from native_streaming_input_cases import run as public_cases
from native_input_growth_cases import run as growth_cases
from native_streaming_order_cases import run as ordering_cases
from native_streaming_order_pressure_cases import run as ordering_pressure_cases
from native_streaming_aggregate_cases import run as aggregate_cases
from native_streaming_aggregate_pressure_cases import run as aggregate_pressure_cases
from native_streaming_join_cases import run as join_cases
from native_streaming_join_pressure_cases import run as join_pressure_cases
from native_streaming_window_cases import run as window_cases
from native_streaming_window_pressure_cases import run as window_pressure_cases
from native_streaming_protocol_cases import run as protocol_cases
import shardloom as sl

FAMILIES = {"input": public_cases, "growth": growth_cases, "ordering": ordering_cases, "ordering-pressure": ordering_pressure_cases,
            "aggregate": aggregate_cases, "aggregate-pressure": aggregate_pressure_cases,
            "join": join_cases, "join-pressure": join_pressure_cases,
            "window": window_cases, "window-pressure": window_pressure_cases, "protocol": protocol_cases}


def sha(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


class Harness:
    def __init__(self, binary, root, operation_timeout):
        self.binary, self.root = binary, root
        self.logs, self.output = root / "logs", root / "outputs"
        self.results = []
        self.client = sl.ShardLoomClient(binary=binary, timeout=operation_timeout)
        self.context = sl.ShardLoomContext(self.client)

    def guard(self):
        check_budgets(self.root, self.output / "reserved", self.logs,
                      min_free_bytes=12 * GIB, reserve_bytes=128 * MIB,
                      max_workspace_bytes=100 * GIB, max_log_bytes=192 * MIB)

    def json(self, path, value):
        with path.open("x") as stream:
            json.dump(value, stream, ensure_ascii=False, indent=2, allow_nan=False)
            stream.write("\n")

    def artifact(self, path):
        return {"path": str(path.relative_to(self.root)), "bytes": path.stat().st_size, "sha256": sha(path)}

    def envelope(self, name, envelope, *, success=True):
        self.json(self.logs / (name + "-envelope.json"), envelope.raw)
        assert len({entry.key for entry in envelope.fields}) == len(envelope.fields)
        assert envelope.fallback.attempted is False
        if success:
            assert envelope.status == "success", envelope.raw
            assert envelope.field("public_workflow_external_engine_invoked") == "false"
        else:
            assert envelope.status != "success" and envelope.diagnostics
            assert envelope.field("result_payload_complete") != "true"

    def completed(self, name, envelope, *, batches, rows):
        self.envelope(name, envelope)
        for field, value in {"native_input_batches": batches, "native_input_batch_rows": rows,
                             "native_input_batch_sources": 1, "resident_completed_executions": 1,
                             "native_input_max_retained_batches": int(batches != 0)}.items():
            assert envelope.field_int(field) == value, (field, envelope.field(field), value)
        assert envelope.field_bool("native_input_end_observed") is True
        assert envelope.field_bool("native_input_output_ownership_detached") is True
        assert "released_before_next_demand" in envelope.field("native_batch_input_storage")

    def values(self, name, actual, expected):
        actual = list(actual)
        self.json(self.logs / (name + "-values.json"), {"actual": actual, "expected": expected})
        assert actual == expected, (name, actual, expected)

    def case(self, name, call):
        self.guard()
        started = time.monotonic()
        record = {"name": name, "status": "running"}
        self.results.append(record)
        try:
            result = call()
            if isinstance(result, dict):
                record.update(result)
            elif result is not None:
                assert result.envelope.status == "success"
            record["status"] = "passed"
        except BaseException as error:
            record.update(status="failed", error=type(error).__name__ + ": " + str(error))
            if getattr(error, "envelope", None) is not None:
                self.json(self.logs / (name + "-unexpected-error.json"), error.envelope.raw)
            raise
        finally:
            record["seconds"] = time.monotonic() - started
            print(json.dumps(record), flush=True)
        self.guard()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--uat-root", type=Path, required=True)
    parser.add_argument("--operation-timeout", type=float, default=120)
    parser.add_argument("--families", nargs="+", choices=tuple(FAMILIES), default=list(FAMILIES))
    args = parser.parse_args()
    binary = args.binary.resolve(strict=True)
    root = require_local_path(args.uat_root, Path.home(), sys.platform)
    assert not root.exists(), "use a fresh output directory; prior evidence is immutable"
    assert args.operation_timeout > 0
    harness = Harness(binary, root, args.operation_timeout)
    harness.guard()
    harness.logs.mkdir(parents=True)
    harness.output.mkdir()
    inputs = [binary, Path(__file__).resolve(),
              Path(__file__).with_name("native_streaming_input_cases.py"),
              Path(__file__).with_name("native_input_growth_cases.py"),
              Path(__file__).with_name("native_streaming_order_cases.py"),
              Path(__file__).with_name("native_streaming_order_pressure_cases.py"),
              Path(__file__).with_name("native_streaming_aggregate_cases.py"),
              Path(__file__).with_name("native_streaming_aggregate_pressure_cases.py"),
              Path(__file__).with_name("native_streaming_join_cases.py"),
              Path(__file__).with_name("native_streaming_join_pressure_cases.py"),
              Path(__file__).with_name("native_streaming_window_cases.py"),
              Path(__file__).with_name("native_streaming_window_pressure_cases.py"),
              Path(__file__).with_name("native_window_frame_reference.py"),
              Path(__file__).with_name("native_streaming_protocol_cases.py")]
    sources = {str(path): sha(path) for path in inputs}
    for path in inputs[1:]:
        with (harness.logs / path.name).open("xb") as snapshot:
            snapshot.write(path.read_bytes())
    status = "failed"
    try:
        for family in args.families:
            FAMILIES[family](harness)
        assert all(sha(Path(path)) == digest for path, digest in sources.items())
        status = "passed"
    finally:
        harness.client.close()
        harness.json(root / "summary.json", {"status": status, "sources": sources,
                     "cases": harness.results, "families": args.families, "operation_timeout_seconds": args.operation_timeout,
                     "performance_claim": False,
                     "whole_process_memory_ceiling": False})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
