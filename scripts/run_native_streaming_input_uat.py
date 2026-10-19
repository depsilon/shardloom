#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Verify completion-aware input through the real Python/native boundary.

Run under the serial workload/deadline supervisor, with a frozen --binary and a
new local-only --uat-root. Keeps exact results, native reports, raw adversarial
frames and failures. This is capability evidence, not a speedup or RSS claim.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
from pathlib import Path
import stat
import sys
import time
import zlib

from local_uat_storage import GIB, MIB, check_budgets, require_local_path
from native_streaming_input_cases import run as public_cases
from native_input_growth_cases import run as growth_cases
from native_typed_input_cases import run as typed_cases
from native_streaming_order_cases import run as ordering_cases
from native_streaming_order_pressure_cases import run as ordering_pressure_cases
from native_streaming_aggregate_cases import run as aggregate_cases
from native_streaming_aggregate_pressure_cases import run as aggregate_pressure_cases
from native_streaming_join_cases import run as join_cases
from native_streaming_join_pressure_cases import run as join_pressure_cases
from native_streaming_window_cases import run as window_cases
from native_streaming_window_pressure_cases import run as window_pressure_cases
from native_streaming_protocol_cases import run as protocol_cases
from native_batch_preparation_cases import run as preparation_cases
from native_streaming_multiple_cases import run as multiple_cases
from native_streaming_multiple_pressure_cases import run as multiple_pressure_cases
from native_streaming_dynamic_cases import run as dynamic_cases
from native_streaming_dynamic_pressure_cases import run as dynamic_pressure_cases
from native_streaming_replay_cases import run as replay_cases
from native_streaming_replay_pressure_cases import run as replay_pressure_cases
from native_streaming_destination_cases import run as destination_cases
from native_streaming_set_cases import run as set_cases
from native_streaming_set_pressure_cases import run as set_pressure_cases
from native_streaming_unary_cases import run as unary_cases
from native_streaming_unary_reference_cases import run as unary_reference_cases
from native_streaming_unary_composition_cases import run as unary_composition_cases
from native_streaming_unary_fault_cases import run as unary_fault_cases
from native_streaming_unary_pressure_cases import run as unary_pressure_cases
from native_streaming_unary_membership_cases import run as unary_membership_cases
from native_streaming_subquery_cases import run as subquery_cases
from native_streaming_subquery_composition_cases import run as subquery_composition_cases
from native_streaming_subquery_fault_cases import run as subquery_fault_cases
from native_streaming_subquery_pressure_cases import run as subquery_pressure_cases
from native_streaming_dynamic_subquery_cases import run as dynamic_subquery_cases
from native_streaming_dynamic_subquery_reference_cases import run as dynamic_subquery_reference_cases
from native_streaming_dynamic_subquery_composition_cases import run as dynamic_subquery_composition_cases
from native_streaming_dynamic_subquery_fault_cases import run as dynamic_subquery_fault_cases
from native_streaming_dynamic_subquery_pressure_cases import run as dynamic_subquery_pressure_cases
import shardloom as sl

FAMILIES = {"input": public_cases, "growth": growth_cases, "typed-input": typed_cases, "ordering": ordering_cases, "ordering-pressure": ordering_pressure_cases,
            "aggregate": aggregate_cases, "aggregate-pressure": aggregate_pressure_cases,
            "join": join_cases, "join-pressure": join_pressure_cases,
            "window": window_cases, "window-pressure": window_pressure_cases, "protocol": protocol_cases,
            "preparation": preparation_cases, "multiple": multiple_cases,
            "multiple-pressure": multiple_pressure_cases, "dynamic": dynamic_cases,
            "dynamic-pressure": dynamic_pressure_cases, "replay": replay_cases,
            "replay-pressure": replay_pressure_cases, "destinations": destination_cases,
            "sets": set_cases, "sets-pressure": set_pressure_cases,
            "unary": unary_cases, "unary-reference": unary_reference_cases,
            "unary-composition": unary_composition_cases, "unary-fault": unary_fault_cases,
            "unary-pressure": unary_pressure_cases, "unary-membership": unary_membership_cases,
            "subqueries": subquery_cases,
            "subqueries-composition": subquery_composition_cases,
            "subqueries-fault": subquery_fault_cases, "subqueries-pressure": subquery_pressure_cases,
            "dynamic-subqueries": dynamic_subquery_cases,
            "dynamic-subqueries-reference": dynamic_subquery_reference_cases,
            "dynamic-subqueries-composition": dynamic_subquery_composition_cases,
            "dynamic-subqueries-fault": dynamic_subquery_fault_cases,
            "dynamic-subqueries-pressure": dynamic_subquery_pressure_cases}


def sha(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def json_file_identity(path):
    info = path.lstat()
    if not stat.S_ISREG(info.st_mode):
        raise ValueError(f"JSON evidence is not a regular file: {path}")
    return info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns


class Harness:
    def __init__(self, binary, root, operation_timeout, *, compress_logs=False):
        self.binary, self.root = binary, root
        self.logs, self.output = root / "logs", root / "outputs"
        self.results = []
        self.compress_logs = compress_logs
        self.compressed_json_files = []
        self.client = sl.ShardLoomClient(
            binary=binary, timeout=operation_timeout, memory_gb=4, max_parallelism=2,
        )
        self.context = sl.ShardLoomContext(self.client)

    def guard(self):
        check_budgets(self.root, self.output / "reserved", self.logs,
                      min_free_bytes=12 * GIB, reserve_bytes=128 * MIB,
                      max_workspace_bytes=100 * GIB, max_log_bytes=192 * MIB)

    def json(self, path, value):
        raw = (json.dumps(value, ensure_ascii=False, separators=(",", ":"),
                          allow_nan=False) + "\n").encode()
        with path.open("xb") as stream:
            stream.write(raw)
        if not self.compress_logs or path.parent != self.logs:
            return
        # Keep the original until the complete compressed bytes are verified.
        # Failed compression or readback preserves both files for inspection.
        identity = json_file_identity(path)
        if path.read_bytes() != raw:
            raise ValueError(f"persisted JSON evidence differs: {path}")
        destination = path.with_suffix(path.suffix + ".gz")
        with destination.open("xb") as stream:
            stream.write(gzip.compress(raw, compresslevel=6, mtime=0))
        packed = destination.read_bytes()
        try:
            decoded = gzip.decompress(packed)
        except (OSError, EOFError, zlib.error) as error:
            raise ValueError(f"compressed JSON evidence is invalid: {destination}") from error
        if decoded != raw:
            raise ValueError(f"compressed JSON evidence differs: {destination}")
        if json_file_identity(path) != identity or path.read_bytes() != raw:
            raise ValueError(f"original JSON evidence changed before cleanup: {path}")
        record = {"path": str(destination.relative_to(self.root)),
                  "raw_path": str(path.relative_to(self.root)),
                  "bytes": len(packed), "sha256": hashlib.sha256(packed).hexdigest(),
                  "raw_bytes": len(raw), "raw_sha256": hashlib.sha256(raw).hexdigest()}
        path.unlink()
        self.compressed_json_files.append(record)

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

    def completed(self, name, envelope, *, batches, rows, sources=1):
        self.envelope(name, envelope)
        for field, value in {"native_input_batches": batches, "native_input_batch_rows": rows,
                             "native_input_batch_sources": sources, "resident_completed_executions": 1,
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
    parser.add_argument("--compress-logs", action="store_true",
                        help="losslessly compress JSON logs within the existing log budget")
    parser.add_argument("--families", nargs="+", choices=tuple(FAMILIES), default=list(FAMILIES))
    args = parser.parse_args()
    binary = args.binary.resolve(strict=True)
    root = require_local_path(args.uat_root, Path.home(), sys.platform)
    assert not root.exists(), "use a fresh output directory; prior evidence is immutable"
    assert args.operation_timeout > 0
    harness = Harness(binary, root, args.operation_timeout, compress_logs=args.compress_logs)
    harness.guard()
    harness.logs.mkdir(parents=True)
    harness.output.mkdir()
    inputs = [binary, Path(__file__).resolve(),
              Path(__file__).with_name("native_streaming_input_cases.py"),
              Path(__file__).with_name("native_input_growth_cases.py"),
              Path(__file__).with_name("native_typed_input_cases.py"),
              Path(__file__).with_name("native_typed_input_fixtures.py"),
              Path(sl.__file__).with_name("_input_schema.py"),
              Path(__file__).with_name("native_streaming_order_cases.py"),
              Path(__file__).with_name("native_streaming_order_pressure_cases.py"),
              Path(__file__).with_name("native_streaming_aggregate_cases.py"),
              Path(__file__).with_name("native_streaming_aggregate_pressure_cases.py"),
              Path(__file__).with_name("native_streaming_join_cases.py"),
              Path(__file__).with_name("native_streaming_join_pressure_cases.py"),
              Path(__file__).with_name("native_streaming_window_cases.py"),
              Path(__file__).with_name("native_streaming_window_pressure_cases.py"),
              Path(__file__).with_name("native_window_frame_reference.py"),
              Path(__file__).with_name("native_streaming_protocol_cases.py"),
              Path(__file__).with_name("native_batch_preparation_cases.py"),
              Path(__file__).with_name("native_streaming_multiple_cases.py"),
              Path(__file__).with_name("native_streaming_multiple_pressure_cases.py"),
              Path(__file__).with_name("native_streaming_dynamic_cases.py"),
              Path(__file__).with_name("native_streaming_dynamic_pressure_cases.py"),
              Path(__file__).with_name("native_streaming_replay_cases.py"),
              Path(__file__).with_name("native_streaming_replay_pressure_cases.py"),
              Path(__file__).with_name("native_streaming_destination_cases.py"),
              Path(__file__).with_name("native_streaming_set_cases.py"),
              Path(__file__).with_name("native_streaming_set_composition_cases.py"),
              Path(__file__).with_name("native_streaming_set_boundary_cases.py"),
              Path(__file__).with_name("native_streaming_set_pressure_cases.py"),
              Path(__file__).with_name("native_streaming_set_spill_cases.py"),
              Path(__file__).with_name("native_streaming_unary_cases.py"),
              Path(__file__).with_name("native_streaming_unary_reference_cases.py"),
              Path(__file__).with_name("native_streaming_unary_composition_cases.py"),
              Path(__file__).with_name("native_streaming_unary_fault_cases.py"),
              Path(__file__).with_name("native_streaming_unary_pressure_cases.py"),
              Path(__file__).with_name("native_streaming_unary_membership_cases.py"),
              Path(__file__).with_name("native_streaming_subquery_cases.py"),
              Path(__file__).with_name("native_streaming_subquery_composition_cases.py"),
              Path(__file__).with_name("native_streaming_subquery_fault_cases.py"),
              Path(__file__).with_name("native_streaming_subquery_pressure_cases.py"),
              Path(__file__).with_name("native_streaming_dynamic_subquery_cases.py"),
              Path(__file__).with_name("native_streaming_dynamic_subquery_reference_cases.py"),
              Path(__file__).with_name("native_streaming_dynamic_subquery_composition_cases.py"),
              Path(__file__).with_name("native_streaming_dynamic_subquery_fault_cases.py"),
              Path(__file__).with_name("native_streaming_dynamic_subquery_pressure_cases.py"),
              Path(__file__).with_name("native_unary_reference.py"),
              Path(__file__).with_name("native_streaming_unary_reference_fixtures.json")]
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
                     "compress_logs": args.compress_logs,
                     "compressed_json_files": harness.compressed_json_files,
                     "performance_claim": False,
                     "whole_process_memory_ceiling": False})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
