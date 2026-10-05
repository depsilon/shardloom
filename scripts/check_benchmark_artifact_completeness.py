#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Verify a complete public-native benchmark packet without executing workloads.

The runner emits final evidence directly. There is no promotion, lane backfill,
inferred timing, query-answer cache or claim-grade conversion.
"""
from __future__ import annotations

import argparse
from dataclasses import asdict
import gzip
import hashlib
import json
import math
from pathlib import Path
import re
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT))
from benchmarks.traditional_analytics.comparison import round_float
from benchmarks.traditional_analytics.run import harness_source_inventory
from benchmarks.traditional_analytics.workloads import WORKLOADS
from native_workflow_protocol import extract_result, read_json_output, report_fields, strict_json
from run_clickbench_query_uat import equivalent

REPORT_SCHEMA_VERSION = "shardloom.benchmark_artifact_completeness_report.v2"
ARTIFACT_SCHEMA_VERSION = "shardloom.public_native_benchmark.v1"
DEFAULT_PUBLIC_BENCHMARK_MANIFEST = ROOT / "website/assets/benchmarks/latest/manifest.json"
PUBLIC_BENCHMARK_SURFACE = "clickbench_handoff"
CLICKBENCH_URL = "https://benchmark.clickhouse.com/"
OUTPUT_FORMATS = {"collect", "vortex", "json", "jsonl", "csv", "parquet", "arrow_ipc", "avro", "orc"}


def load_json(path):
    path = Path(path)
    if path.suffix == ".gz":
        with gzip.open(path, "rt", encoding="utf-8") as stream:
            return strict_json(stream.read())
    return strict_json(path.read_text(encoding="utf-8"))


def file_sha256(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def result_rows(payload):
    rows = payload.get("records", []) if isinstance(payload, dict) else []
    return rows if isinstance(rows, list) else []


def default_public_benchmark_manifest_retired(path):
    path = Path(path)
    if not path.is_absolute():
        path = ROOT / path
    return path.resolve() == DEFAULT_PUBLIC_BENCHMARK_MANIFEST.resolve() and not path.exists()


def retired_public_benchmark_report(manifest_path):
    return {
        "schema_version": REPORT_SCHEMA_VERSION, "status": "passed",
        "manifest": str(manifest_path), "manifest_sha256": None,
        "artifact_status": "retired_from_public_website", "benchmark_profile": "public_site_retired",
        "public_benchmark_surface": PUBLIC_BENCHMARK_SURFACE, "public_benchmark_url": CLICKBENCH_URL,
        "evidence_class": "public_surface_absence_check", "available_lane_count": 0,
        "missing_lane_count": 0, "performance_claim_allowed": False,
        "runtime_execution_performed": False, "benchmark_run_performed": False,
        "fallback_attempted": False, "external_engine_invoked": False, "blockers": [],
    }


def _digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, allow_nan=False).encode()).hexdigest()


def _strings(value, label):
    if (not isinstance(value, list) or not value or any(not isinstance(item, str) or not item for item in value)
            or len(set(value)) != len(value)):
        raise ValueError(f"{label} must be a nonempty unique string array")
    return value


def _seconds(value):
    if type(value) not in (int, float) or not math.isfinite(value) or value < 0:
        raise ValueError("timing must be a finite nonnegative number")
    return value


def _option(command, name):
    if command.count(name) != 1 or command.index(name) + 1 >= len(command):
        raise ValueError(f"native command requires exactly one {name} value")
    return command[command.index(name) + 1]


def _bindings(command):
    bindings = strict_json(_option(command, "--source-bindings"))
    if (not isinstance(bindings, dict) or not bindings
            or any(not isinstance(value, dict) for value in bindings.values())):
        raise ValueError("native command requires explicit source bindings")
    sql = _option(command, "--sql")
    for path in bindings:
        if "'" + path.replace("'", "''") + "'" not in sql:
            raise ValueError("native SQL does not reference its declared source")
    return bindings


class EvidenceReader:
    def __init__(self):
        self.files = {}
        self.candidate = None

    def verified(self, path, expected):
        path = Path(path)
        if not isinstance(expected, str) or re.fullmatch(r"[a-f0-9]{64}", expected) is None:
            raise ValueError("evidence requires a SHA-256 identity")
        actual = self.files.get(str(path))
        if actual is None:
            actual = file_sha256(path)
            self.files[str(path)] = actual
        if actual != expected:
            raise ValueError(f"evidence content changed: {path}")
        return path

    def receipt(self, receipt, *, native=False, binary_sha=None, preparation=False):
        if (not isinstance(receipt, dict) or type(receipt.get("returncode")) is not int
                or receipt["returncode"] != 0 or receipt.get("guard_failures") != []):
            raise ValueError("successful process and guard receipt is required")
        _seconds(receipt.get("seconds"))
        envelope = load_json(self.verified(receipt["stdout"], receipt["stdout_sha256"]))
        if not isinstance(envelope, dict):
            raise ValueError("process evidence must be an object")
        if not native:
            if envelope.get("status") != "passed":
                raise ValueError("independent reference process did not pass")
            return envelope
        if receipt.get("binary_sha256") != binary_sha:
            raise ValueError("native call binary differs from frozen candidate")
        command = receipt.get("command")
        if (not isinstance(command, list) or not command
                or any(not isinstance(item, str) for item in command)):
            raise ValueError("native command must be a string array")
        if Path(command[0]).resolve() != self.candidate:
            raise ValueError("native command does not identify the frozen executable")
        fallback = envelope.get("fallback", {})
        if fallback.get("attempted") is not False or fallback.get("engine") is not None:
            raise ValueError("native envelope has missing or unsafe fallback evidence")
        if preparation:
            if len(command) < 4 or command[1] != "vortex-prepare":
                raise ValueError("prepared inputs require public vortex-prepare receipts")
            if envelope.get("status") != "success":
                raise ValueError("native preparation did not report success")
            return envelope
        if (command[1:3] != ["run", "sql"]
                or any(item.startswith("--vortex-") for item in command)):
            raise ValueError("native calls must use the complete public SQL declaration")
        fields = report_fields(envelope)
        requested = _option(command, "--request")
        if requested == "collect":
            extract_result(envelope)
        elif requested.startswith("write_") and requested.removeprefix("write_") in OUTPUT_FORMATS - {"collect"}:
            output = receipt.get("output", {})
            if (fields.get("native_vortex_result_export_all_targets_committed") != "true"
                    or fields.get("native_vortex_result_export_path") != output.get("path")
                    or _option(command, "--output") != output.get("path")):
                raise ValueError("native writer lacks committed output evidence")
            path = self.verified(output["path"], output["sha256"])
            if path.stat().st_size != output.get("bytes"):
                raise ValueError("native output byte count differs")
        else:
            raise ValueError("unknown native requested output")
        return envelope

    def prepared_inputs(self, receipts, inputs, binary_sha):
        targets = set()
        for receipt in receipts:
            envelope = self.receipt(receipt, native=True, binary_sha=binary_sha, preparation=True)
            command = receipt["command"]
            source, target = Path(command[2]).resolve(), Path(command[3]).resolve()
            if source not in inputs or target in inputs or target in targets:
                raise ValueError("preparation must bind frozen inputs to unique new targets")
            fields = {}
            if not isinstance(envelope.get("fields"), list):
                raise ValueError("native preparation fields are absent")
            for field in envelope["fields"]:
                if (not isinstance(field, dict) or not isinstance(field.get("key"), str)
                        or (field["key"] in fields and (
                            type(fields[field["key"]]) is not type(field.get("value"))
                            or fields[field["key"]] != field.get("value")))):
                    raise ValueError("native preparation fields must be unambiguous")
                key, value = field["key"], field.get("value")
                if key.endswith(("fallback_attempted", "external_engine_invoked")) and not (value is False or value == "false"):
                    raise ValueError("native preparation has unsafe execution evidence")
                fields[key] = value
            if (fields.get("vortex_ingest_performed") != "true"
                    or fields.get("external_engine_invoked") != "false"
                    or fields.get("vortex_ingest_output_commit_status") != "committed"
                    or fields.get("vortex_ingest_output_canonical_output_path") != str(target)):
                raise ValueError("native preparation lacks committed Vortex output evidence")
            digest = fields.get("vortex_ingest_output_output_digest")
            if not isinstance(digest, str) or not digest.startswith("sha256:"):
                raise ValueError("native preparation requires a retained output digest")
            path = self.verified(target, digest.removeprefix("sha256:"))
            if fields.get("vortex_ingest_output_bytes_written") != str(path.stat().st_size):
                raise ValueError("prepared Vortex byte count differs")
            targets.add(target)
        return targets


def validate_manifest(manifest_path, allow_incomplete=False):
    """Validate exact case coverage and retained values; incomplete runs fail."""
    payload = load_json(manifest_path)
    blockers = []
    try:
        if allow_incomplete:
            raise ValueError("incomplete benchmark evidence cannot be admitted")
        if not isinstance(payload, dict) or payload.get("schema_version") != ARTIFACT_SCHEMA_VERSION:
            raise ValueError("expected a public_native_benchmark.v1 report")
        if (payload.get("status") != "passed" or payload.get("performance_claim") is not False
                or payload.get("independent_reference") is not True
                or payload.get("query_answers_cached_by_native_adapter") is not False):
            raise ValueError("a complete independently checked, non-claim benchmark is required")
        configuration = payload["configuration"]
        input_state, output_format = configuration["input_state"], configuration["output_format"]
        if input_state not in ("raw", "prepared") or output_format not in OUTPUT_FORMATS:
            raise ValueError("unknown benchmark input state or requested output")
        engines = _strings(payload["engines"], "engines")
        formats = _strings(configuration["formats"], "formats")
        scenarios = _strings(configuration["scenarios"], "scenarios")
        for scenario in scenarios:
            if scenario not in WORKLOADS or _digest(payload["workload_declarations"].get(scenario)) != _digest(asdict(WORKLOADS[scenario])):
                raise ValueError("workload declaration differs from the frozen harness")
        reference = payload["reference_engine"]
        if "shardloom" not in engines or reference not in engines or reference == "shardloom":
            raise ValueError("one native candidate and an independent comparison engine are required")
        if any(engine.startswith("shardloom") and engine != "shardloom" for engine in engines):
            raise ValueError("multiple ShardLoom execution providers are not admitted")
        repeats = configuration["repeats"]
        if type(repeats) is not int or repeats < 1:
            raise ValueError("repeats must be a positive integer")
        expected = {(engine, fmt, scenario, repeat) for engine in engines for fmt in formats
                    for scenario in scenarios for repeat in range(1, repeats + 1)}
        rows = result_rows(payload)
        if any(not isinstance(row, dict) or type(row.get("repeat")) is not int for row in rows):
            raise ValueError("benchmark rows require integer repeat identities")
        keys = [(row["engine"], row["format"], row["scenario"], row["repeat"]) for row in rows]
        if len(keys) != len(expected) or set(keys) != expected:
            raise ValueError("case coverage is missing, duplicated or undeclared")
        reader = EvidenceReader()
        binary = payload["binary"]
        reader.candidate = reader.verified(binary["path"], binary["sha256"]).resolve()
        if not payload.get("inputs") or not payload.get("harness_sources"):
            raise ValueError("frozen inputs and harness sources are required")
        if payload["harness_sources"] != harness_source_inventory():
            raise ValueError("harness source inventory is incomplete or changed")
        inputs = set()
        for item in payload["inputs"]:
            path = reader.verified(item["path"], item["sha256"])
            if path.stat().st_size != item["bytes"]:
                raise ValueError("input byte count differs")
            if path.resolve() in inputs:
                raise ValueError("frozen input inventory contains duplicate paths")
            inputs.add(path.resolve())
        for path, expected_sha in payload["harness_sources"].items():
            reader.verified(ROOT / path, expected_sha)
        references = {(row["format"], row["scenario"]): row["result"] for row in rows
                      if row["engine"] == reference and row["repeat"] == 1}
        preparation_identity, prepared_inputs = None, set()
        for row in rows:
            if row.get("status") != "passed" or row.get("matches_reference") is not True:
                raise ValueError("every requested case must pass an independent complete-value comparison")
            _seconds(row.get("seconds"))
            if row.get("result_sha256") != _digest(row["result"]):
                raise ValueError("stored complete result differs from its identity")
            if not equivalent(row["result"], references[(row["format"], row["scenario"])]):
                raise ValueError("stored result differs from the independent reference")
            if row["engine"] != "shardloom":
                observed = reader.receipt(row["receipt"])
                if _digest(observed["result"]) != row["result_sha256"]:
                    raise ValueError("comparison process output differs from the recorded result")
                continue
            evidence = row["evidence"]
            if (evidence.get("benchmark_request_protocol") != "public_native_workflow"
                    or evidence.get("benchmark_query_answer_cached") != "false"
                    or evidence.get("public_workflow_fallback_attempted") != "false"
                    or evidence.get("public_workflow_external_engine_invoked") != "false"):
                raise ValueError("native case lacks shared-engine and no-fallback evidence")
            if (evidence.get("benchmark_input_state") != input_state
                    or evidence.get("benchmark_output_format") != output_format):
                raise ValueError("native case input/output modes differ from the requested configuration")
            preparations = strict_json(evidence["benchmark_input_preparation_calls"])
            if (not isinstance(preparations, list)
                    or bool(preparations) != (input_state == "prepared")):
                raise ValueError("preparation receipts differ from the requested input state")
            identity = _digest(preparations)
            if preparation_identity is None:
                prepared_inputs = reader.prepared_inputs(preparations, inputs, binary["sha256"])
                preparation_identity = identity
            elif identity != preparation_identity:
                raise ValueError("native cases disagree about their prepared input inventory")
            calls = strict_json(evidence["benchmark_native_calls"])
            readbacks = strict_json(evidence["benchmark_output_validation_calls"])
            if not isinstance(calls, list) or not calls or not isinstance(readbacks, list):
                raise ValueError("native execution receipts are absent")
            workload = WORKLOADS[row["scenario"]]
            first_result = int(workload.write_statement is not None)
            if len(calls) != len(workload.statements) + first_result:
                raise ValueError("native call count differs from the complete workload")
            expected_sql = strict_json(evidence["benchmark_sql_declarations"])
            if expected_sql != [_option(call["command"], "--sql") for call in calls[first_result:]]:
                raise ValueError("native calls differ from recorded SQL declarations")
            writer_count = 0
            batches = []
            for index, call in enumerate(calls):
                envelope = reader.receipt(call, native=True, binary_sha=binary["sha256"])
                requested_format = "csv" if index < first_result else output_format
                requested = "collect" if requested_format == "collect" else f"write_{requested_format}"
                if _option(call["command"], "--request") != requested:
                    raise ValueError("native call does not request the declared output format")
                bindings = _bindings(call["command"])
                admitted_inputs = prepared_inputs if input_state == "prepared" else inputs
                if any(Path(path).resolve() not in admitted_inputs for path in bindings):
                    raise ValueError("native source does not belong to the verified input inventory")
                if input_state == "prepared" and any(binding.get("input_format") != "vortex" for binding in bindings.values()):
                    raise ValueError("prepared workloads must bind Vortex inputs")
                if "output" in call:
                    if writer_count >= len(readbacks):
                        raise ValueError("native writer lacks readback evidence")
                    readback = readbacks[writer_count]
                    writer_count += 1
                    if requested_format in ("json", "jsonl"):
                        if (readback.get("kind") != "complete_json_file_readback"
                                or readback.get("format") != requested_format
                                or any(readback.get(key) != value for key, value in call["output"].items())):
                            raise ValueError("JSON readback does not identify the committed output")
                        _seconds(readback.get("seconds"))
                        path = reader.verified(readback["path"], readback["sha256"])
                        rows_from_execution = read_json_output(path, requested_format)
                    else:
                        envelope = reader.receipt(readback, native=True, binary_sha=binary["sha256"])
                        command = readback["command"]
                        bindings = _bindings(command)
                        if (set(bindings) != {call["output"]["path"]}
                                or bindings[call["output"]["path"]].get("input_format") != requested_format
                                or _option(command, "--request") != "collect"):
                            raise ValueError("readback does not bind the committed output")
                        rows_from_execution = extract_result(envelope)
                else:
                    rows_from_execution = extract_result(envelope)
                if index >= first_result:
                    batches.append(rows_from_execution)
            if writer_count != len(readbacks):
                raise ValueError("unexpected native readback evidence")
            if not equivalent(workload.result(batches, round_float), row["result"]):
                raise ValueError("native complete payload differs from the recorded comparison result")
            if not math.isclose(row["seconds"], sum(call["seconds"] for call in calls), rel_tol=1e-12, abs_tol=1e-12):
                raise ValueError("native timing differs from retained process durations")
        summary = payload.get("summary", {})
        if (summary.get("complete") is not True or summary.get("expected_cases") != len(expected)
                or summary.get("recorded_cases") != len(rows) or summary.get("passed_cases") != len(rows)):
            raise ValueError("summary differs from the complete observed case set")
    except (KeyError, IndexError, TypeError, ValueError, OSError) as error:
        blockers.append(str(error))
    return blockers, payload


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True, help="Current public-native runner report")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if default_public_benchmark_manifest_retired(args.manifest):
        report = retired_public_benchmark_report(args.manifest)
    else:
        try:
            blockers, payload = validate_manifest(args.manifest)
        except (OSError, ValueError) as error:
            blockers, payload = [str(error)], {}
        report = {"schema_version": REPORT_SCHEMA_VERSION, "status": "blocked" if blockers else "passed",
                  "manifest": str(args.manifest), "recorded_cases": len(result_rows(payload)),
                  "performance_claim_allowed": False, "benchmark_run_performed": False,
                  "fallback_attempted": False, "external_engine_invoked": False, "blockers": blockers}
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))
    return int(report["status"] != "passed")


if __name__ == "__main__":
    raise SystemExit(main())
