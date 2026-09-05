#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Paired, bounded native collection acceptance across three public call paths.

This measures a deterministic 32-row held-out fixture, not ClickBench, broad SQL,
native Python bindings, process-wide memory enforcement, or a production workload.
"""

from __future__ import annotations

import argparse
import contextlib
import datetime as dt
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import selectors
import signal
import subprocess
import sys
import time

from local_uat_storage import GIB, MIB, check_budgets, require_local_path
from run_clickbench_query_uat import (
    equivalent, extract_result, file_sha256, run_command, stop_process, strict_json,
)

SURFACES = {
    "fresh_cli_process": "native process creation through complete stdout and process exit",
    "persistent_worker": "request JSON encoding/write through complete response line; worker startup separate",
    "python_client": "public_workflow_run argument construction, worker roundtrip and typed envelope parsing; import excluded",
}


def fixture_rows() -> list[dict]:
    return [{"cohort_key": i, "exact_identifier": (1 if i % 2 else -1) * (2**60 + i),
             "nullable_label": None if i % 5 == 0 else ["λ\"\n", "東京", "", "plain"][i % 4]}
            for i in range(32)]


def cases(rows: list[dict]) -> list[dict]:
    columns = ["nullable_label", "exact_identifier", "cohort_key"]
    return [
        {"name": "metadata_count", "primitive": "count", "expected": len(rows)},
        {"name": "renamed_nullable_projection", "primitive": "project", "columns": columns,
         "expected": rows},
        {"name": "filtered_large_integer_rows", "primitive": "filter_project", "columns": columns,
         "predicate": "gte:cohort_key:24", "expected": rows[24:]},
        {"name": "empty_filtered_rows", "primitive": "filter_project", "columns": columns,
         "predicate": "gte:cohort_key:99", "expected": []},
    ]


def request_options(source: Path, case: dict) -> dict:
    options = dict(input_uri=str(source), input_format="vortex", requested_output="collect",
                   execution_policy="native_vortex", materialization_policy="bounded", bounded=True,
                   vortex_primitive=case["primitive"], memory_gb=1, max_parallelism=2)
    if "columns" in case:
        options["vortex_columns"] = case["columns"]
    if "predicate" in case:
        options["vortex_predicate"] = case["predicate"]
    return options


def command_args(source: Path, case: dict) -> list[str]:
    args = ["run", "dataframe", "--input", str(source), "--input-format", "vortex",
            "--request", "collect", "--execution-policy", "native_vortex", "--bounded", "true",
            "--materialization-policy", "bounded", "--vortex-primitive", case["primitive"],
            "--memory-gb", "1", "--max-parallelism", "2", "--format", "json"]
    if "columns" in case:
        args.extend(["--vortex-columns", ",".join(case["columns"])])
    if "predicate" in case:
        args.extend(["--vortex-predicate", case["predicate"]])
    return args


def percentiles(samples: list[float]) -> dict:
    if not samples or any(not math.isfinite(value) or value < 0 for value in samples):
        raise ValueError("latency samples must be nonempty, finite and nonnegative")
    ordered = sorted(samples)
    return {"sample_count": len(samples), "method": "nearest_rank",
            **{f"p{p}_seconds": ordered[max(0, math.ceil(len(ordered) * p / 100) - 1)]
               for p in (50, 95, 99)}, "min_seconds": ordered[0], "max_seconds": ordered[-1]}


def paired_order(sample: int) -> tuple[str, str]:
    return ("baseline", "candidate") if sample % 2 == 0 else ("candidate", "baseline")


def generation(path: Path) -> tuple[int, ...]:
    value = path.stat()
    return value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns, value.st_ctime_ns


def python_source_hash(root: Path) -> str:
    digest = hashlib.sha256()
    for path in sorted(root.rglob("*.py")):
        digest.update(path.relative_to(root).as_posix().encode())
        digest.update(bytes.fromhex(file_sha256(path)))
    return digest.hexdigest()


class Worker:
    def __init__(self, binary: Path, stderr: Path):
        self.error_file = stderr.open("xb")
        started = time.perf_counter()
        try:
            self.process = subprocess.Popen([str(binary), "python-worker"], stdin=subprocess.PIPE,
                                            stdout=subprocess.PIPE, stderr=self.error_file,
                                            start_new_session=True, bufsize=0)
        except BaseException:
            self.error_file.close()
            raise
        self.start_seconds = time.perf_counter() - started
        self.buffer = b""
        self.stderr = stderr

    def request(self, args: list[str], timeout: float, guard) -> tuple[dict, float, bytes]:
        started = time.perf_counter()
        payload = (json.dumps({"args": args}, separators=(",", ":")) + "\n").encode()
        if len(payload) > 4096:
            raise ValueError("bounded worker request exceeds 4096 bytes")
        self.process.stdin.write(payload)
        self.process.stdin.flush()
        deadline = started + timeout
        next_guard = started + 0.25
        with selectors.DefaultSelector() as selector:
            selector.register(self.process.stdout, selectors.EVENT_READ)
            while b"\n" not in self.buffer:
                if time.perf_counter() >= deadline:
                    raise TimeoutError("persistent worker response timed out")
                if len(self.buffer) > 8 * MIB or self.stderr.stat().st_size > 8 * MIB:
                    raise ValueError("persistent worker output exceeds 8 MiB")
                if time.perf_counter() >= next_guard:
                    guard()
                    next_guard = time.perf_counter() + 0.25
                if not selector.select(min(0.1, max(0, deadline - time.perf_counter()))):
                    continue
                chunk = os.read(self.process.stdout.fileno(), 65536)
                if not chunk:
                    raise ValueError("persistent worker exited without a complete response")
                self.buffer += chunk
        line, self.buffer = self.buffer.split(b"\n", 1)
        elapsed = time.perf_counter() - started
        if len(line) > 8 * MIB or self.buffer:
            raise ValueError("worker emitted oversized or unsolicited extra output")
        return strict_json(line.decode()), elapsed, line + b"\n"

    def close(self):
        stop_process(self.process)
        for pipe in (self.process.stdin, self.process.stdout):
            pipe.close()
        self.error_file.close()


def validate(envelope: dict, expected) -> str:
    actual = extract_result(envelope)
    if not equivalent(actual, expected):
        raise ValueError("complete typed values disagree with deterministic fixture expectation")
    return hashlib.sha256(json.dumps(actual, sort_keys=True, ensure_ascii=False,
                                     separators=(",", ":")).encode()).hexdigest()


def validate_preparation(envelope: dict) -> None:
    if envelope.get("status") != "success":
        raise ValueError("fixture preparation did not report success")
    fields = envelope.get("fields", [])
    for key in ("public_workflow_fallback_attempted", "public_workflow_external_engine_invoked"):
        values = [field.get("value") for field in fields if field.get("key") == key]
        if not values or any(not (value is False or value == "false") for value in values):
            raise ValueError(f"missing or unsafe preparation evidence: {key}")
    for field in fields:
        if field.get("key", "").endswith(("fallback_attempted", "external_engine_invoked")):
            if not (field.get("value") is False or field.get("value") == "false"):
                raise ValueError("unsafe fixture preparation evidence")


def execute(args) -> Path:
    if os.name != "posix":
        raise ValueError("this local process-group/selector harness requires a POSIX host")
    root = require_local_path(args.uat_root, Path.home(), sys.platform)
    binaries = {name: getattr(args, f"{name}_binary").resolve(strict=True)
                for name in ("baseline", "candidate")}
    python_root = args.python_source.resolve(strict=True)
    stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    output = root / "logs" / f"resident_call_paths_{stamp}"

    def guard():
        check_budgets(root, output / "fixture.vortex", output, min_free_bytes=GIB,
                      reserve_bytes=0, max_workspace_bytes=100 * GIB, max_log_bytes=256 * MIB)

    guard()
    root.mkdir(parents=True, exist_ok=True)
    lock = root / ".ingest-uat.lock"
    summary = {"schema_version": "shardloom.resident_public_call_paths.v1", "status": "running",
               "claim_gate_status": "not_claim_grade", "scope": __doc__.strip(),
               "surfaces": SURFACES, "samples_per_case": args.samples,
               "warmups_per_case": 1, "ordering": "alternating sequential baseline/candidate pairs",
               "cache_policy": "OS page cache uncontrolled; one source artifact; no query-answer reuse",
               "python_source": str(python_root), "python_source_sha256": python_source_hash(python_root / "shardloom"),
               "platform": platform.platform(), "python_version": platform.python_version(),
               "logical_cpus": os.cpu_count(), "resource_request": {"memory_gb": 1, "max_parallelism": 2},
               "unmeasured": ["process_RSS", "copied_bytes", "decoded_bytes", "CPU_kernel_time"],
               "binaries": {name: {"path": str(binary), "sha256": file_sha256(binary)}
                            for name, binary in binaries.items()}, "records": [], "latencies": {}}
    lock.mkdir()  # Shared with replacement ingest/full43; never remove someone else's lock.
    try:
        output.mkdir(parents=True)
        rows = fixture_rows()
        raw_source = output / "fixture.jsonl"
        raw_source.write_text("".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows))
        source = output / "fixture.vortex"
        prepare = [str(binaries["candidate"]), "prepare", "dataframe", "--input", str(raw_source),
                   "--input-format", "jsonl", "--output", str(source), "--memory-gb", "1",
                   "--max-parallelism", "2", "--format", "json"]
        summary["fixture_prepare_command"] = prepare
        preparation = run_command(prepare, output / "prepare.stdout.json", output / "prepare.stderr.txt", args.timeout, guard)
        if preparation["returncode"] or preparation["guard_failures"] or not source.is_file():
            raise ValueError("fixture native preparation failed")
        prepared_envelope = strict_json((output / "prepare.stdout.json").read_text())
        validate_preparation(prepared_envelope)
        identity = generation(source)
        source_hash = file_sha256(source)
        summary["fixture"] = {"rows": len(rows), "source": str(source), "sha256": source_hash,
                              "generation": identity, "bytes": source.stat().st_size,
                              "input_sha256": file_sha256(raw_source), "prepare": preparation,
                              "expected_rows": rows}
        sys.path.insert(0, str(python_root))
        from shardloom import ShardLoomClient
        for surface in SURFACES:
            for case in cases(rows):
                command = command_args(source, case)
                options = request_options(source, case)
                with contextlib.ExitStack() as stack:
                    transports = {}
                    for name in binaries:
                        if surface == "persistent_worker":
                            transport = Worker(binaries[name], output / f"{surface}_{case['name']}_{name}.stderr.txt")
                            stack.callback(transport.close)
                            summary.setdefault("worker_process_spawn_seconds", {})[f"{case['name']}.{name}"] = transport.start_seconds
                            transports[name] = transport
                        elif surface == "python_client":
                            client = ShardLoomClient(binary=str(binaries[name]), timeout=args.timeout,
                                                    use_persistent_worker=True,
                                                    env={"SHARDLOOM_PERSISTENT_WORKER": "true"})
                            stack.callback(client.close)
                            transports[name] = client
                    for sample in range(args.samples + 1):
                        for name in paired_order(sample):
                            guard()
                            if generation(source) != identity:
                                raise ValueError("source generation changed during acceptance")
                            prefix = output / f"{surface}_{case['name']}_{name}_{sample:04d}"
                            envelope_path = prefix.with_suffix(".stdout.json")
                            record = {"surface": surface, "case": case["name"], "variant": name,
                                      "sample": sample, "warmup": sample == 0, "command_args": command,
                                      "envelope": envelope_path.name, "passed": False}
                            summary["records"].append(record)
                            if surface == "fresh_cli_process":
                                result = run_command([str(binaries[name]), *command], envelope_path,
                                                     prefix.with_suffix(".stderr.txt"), args.timeout, guard)
                                record.update(result)
                                if result["returncode"] or result["guard_failures"]:
                                    raise ValueError("fresh public call failed")
                                envelope = strict_json(envelope_path.read_text())
                            elif surface == "persistent_worker":
                                envelope, seconds, raw = transports[name].request(command, args.timeout, guard)
                                envelope_path.write_bytes(raw)
                                record["seconds"] = seconds
                            else:
                                started = time.perf_counter()
                                envelope = transports[name].public_workflow_run("dataframe", **options).envelope.raw
                                record["seconds"] = time.perf_counter() - started
                                client = transports[name]
                                if client._worker_disabled or client._worker_process is None or client._worker_process.poll() is not None:
                                    raise ValueError("Python client did not retain the requested worker transport")
                                envelope_path.write_text(json.dumps(dict(envelope), ensure_ascii=False) + "\n")
                                record["capture"] = "typed_envelope_reserialized"
                            record["result_sha256"] = validate(envelope, case["expected"])
                            fields = {field["key"]: field["value"] for field in envelope.get("fields", [])}
                            record["resident_source_opens"] = fields.get("resident_source_opens")
                            record["resident_completed_executions"] = fields.get("resident_completed_executions")
                            if name == "candidate" and (case["primitive"] != "count"
                                                        or record["resident_source_opens"] is not None):
                                expected_executions = 1 if surface == "fresh_cli_process" else sample + 1
                                if (str(record["resident_source_opens"]) != "1"
                                        or str(record["resident_completed_executions"]) != str(expected_executions)):
                                    raise ValueError("candidate prepared reader/operation reuse evidence disagrees with call path")
                            record["passed"] = True
                            guard()
                for name in binaries:
                    samples = [record["seconds"] for record in summary["records"]
                               if record["surface"] == surface and record["case"] == case["name"]
                               and record["variant"] == name and not record["warmup"]]
                    summary["latencies"][f"{surface}.{case['name']}.{name}"] = percentiles(samples)
        if generation(source) != identity or file_sha256(source) != source_hash:
            raise ValueError("source was not immutable through the paired run")
        for name, binary in binaries.items():
            if file_sha256(binary) != summary["binaries"][name]["sha256"]:
                raise ValueError(f"{name} binary changed during the paired run")
        summary["status"] = "passed"
        summary["fallback_attempted"] = False
        summary["external_engine_invoked"] = False
    except BaseException as error:
        summary["status"] = "failed"
        summary["error"] = str(error) or type(error).__name__
        raise
    finally:
        try:
            if output.is_dir():
                (output / "summary.json").write_text(json.dumps(summary, indent=2, ensure_ascii=False) + "\n")
        finally:
            lock.rmdir()
    return output / "summary.json"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline-binary", type=Path, required=True)
    parser.add_argument("--candidate-binary", type=Path, required=True)
    parser.add_argument("--uat-root", type=Path, required=True)
    parser.add_argument("--python-source", type=Path, default=Path(__file__).resolve().parents[1] / "python/src")
    parser.add_argument("--samples", type=int, default=30)
    parser.add_argument("--timeout", type=float, default=30)
    args = parser.parse_args()
    if not 3 <= args.samples <= 1000 or not math.isfinite(args.timeout) or args.timeout <= 0:
        parser.error("samples must be 3..=1000 and timeout finite and positive")
    def interrupted(_signal, _frame):
        raise KeyboardInterrupt("acceptance interrupted")
    signal.signal(signal.SIGTERM, interrupted)
    print(execute(args))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
