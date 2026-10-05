"""Contract tests for retained native benchmark evidence packets."""
from __future__ import annotations

import copy
import hashlib
import json
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))

import check_benchmark_artifact_completeness as validator
from benchmarks.traditional_analytics.comparison import round_float
from benchmarks.traditional_analytics.workloads import WORKLOADS


SCENARIO = "csv/file ingest"
FORMAT = "vortex.dtype.serde.v1"
RESULT = {"row_count": 3, "metric_sum": 7.0}


def digest_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def digest_value(value) -> str:
    return digest_bytes(json.dumps(value, sort_keys=True, allow_nan=False).encode())


def write_json(path: Path, value) -> str:
    content = json.dumps(value, ensure_ascii=False, allow_nan=False).encode() + b"\n"
    path.write_bytes(content)
    return digest_bytes(content)


def schema(names, dtypes):
    return {"Struct": [{"names": names, "dtypes": dtypes}, False]}


class NativeBenchmarkEvidenceTests(unittest.TestCase):
    def make_packet(self, root: Path):
        binary = root / "shardloom"
        binary.write_bytes(b"frozen test executable bytes")
        source = root / "fact.csv"
        source.write_bytes(b"id,metric\n1,2\n2,5\n3,0\n")
        binary_sha = validator.file_sha256(binary)
        source_sha = validator.file_sha256(source)

        source_sql_path = str(source.resolve())
        sql = WORKLOADS[SCENARIO].statements[0].format(fact=f"'{source_sql_path}'")
        native_rows = [{"row_count": 3, "metric_sum": 7.0}]
        native_schema = schema(
            ["row_count", "metric_sum"],
            [{"Primitive": ["u64", False]}, {"Primitive": ["f64", True]}],
        )
        native_envelope = {
            "status": "success",
            "fallback": {"attempted": False, "engine": None},
            "human_text": "collected result",
            "fields": [
                {"key": "public_workflow_fallback_attempted", "value": "false"},
                {"key": "public_workflow_external_engine_invoked", "value": "false"},
                {"key": "result_jsonl", "value": "".join(json.dumps(row) + "\n" for row in native_rows)},
                {"key": "result_payload_complete", "value": True},
                {"key": "output_row_count", "value": "1"},
                {"key": "result_schema_json", "value": json.dumps(native_schema)},
                {"key": "result_schema_format", "value": FORMAT},
            ],
        }
        native_log = root / "native.stdout.json"
        native_log_sha = write_json(native_log, native_envelope)
        native_call = {
            "returncode": 0, "guard_failures": [], "seconds": 1.25,
            "stdout": str(native_log), "stdout_sha256": native_log_sha,
            "binary_sha256": binary_sha,
            "command": [
                str(binary), "run", "sql", "--sql", sql, "--request", "collect",
                "--source-bindings", json.dumps({source_sql_path: {"input_format": "csv"}}),
            ],
        }

        reference_envelope = {"status": "passed", "result": RESULT}
        reference_log = root / "reference.stdout.json"
        reference_log_sha = write_json(reference_log, reference_envelope)
        reference_receipt = {
            "returncode": 0, "guard_failures": [], "seconds": 0.75,
            "stdout": str(reference_log), "stdout_sha256": reference_log_sha,
        }

        native_evidence = {
            "benchmark_request_protocol": "public_native_workflow",
            "benchmark_input_state": "raw",
            "benchmark_output_format": "collect",
            "benchmark_query_answer_cached": "false",
            "public_workflow_fallback_attempted": "false",
            "public_workflow_external_engine_invoked": "false",
            "benchmark_native_calls": json.dumps([native_call]),
            "benchmark_output_validation_calls": "[]",
            "benchmark_input_preparation_calls": "[]",
            "benchmark_sql_declarations": json.dumps([sql]),
        }
        rows = [
            {"engine": "pandas", "format": "csv", "scenario": SCENARIO, "repeat": 1,
             "status": "passed", "matches_reference": True, "seconds": 0.75,
             "result": copy.deepcopy(RESULT), "result_sha256": digest_value(RESULT),
             "receipt": reference_receipt},
            {"engine": "shardloom", "format": "csv", "scenario": SCENARIO, "repeat": 1,
             "status": "passed", "matches_reference": True, "seconds": 1.25,
             "result": copy.deepcopy(RESULT), "result_sha256": digest_value(RESULT),
             "evidence": native_evidence},
        ]
        payload = {
            "schema_version": validator.ARTIFACT_SCHEMA_VERSION,
            "status": "passed", "performance_claim": False,
            "independent_reference": True,
            "query_answers_cached_by_native_adapter": False,
            "configuration": {
                "formats": ["csv"], "scenarios": [SCENARIO], "repeats": 1,
                "input_state": "raw", "output_format": "collect",
            },
            "engines": ["pandas", "shardloom"], "reference_engine": "pandas",
            "workload_declarations": {SCENARIO: validator.asdict(WORKLOADS[SCENARIO])},
            "binary": {"path": str(binary), "sha256": binary_sha},
            "inputs": [{"path": str(source), "sha256": source_sha, "bytes": source.stat().st_size}],
            "harness_sources": validator.harness_source_inventory(),
            "records": rows,
            "summary": {"complete": True, "expected_cases": 2,
                        "recorded_cases": 2, "passed_cases": 2},
        }
        manifest = root / "manifest.json"
        write_json(manifest, payload)
        return manifest, payload, native_log, reference_log, binary, source

    def make_prepared_packet(self, root: Path):
        root = root.resolve()
        manifest, payload, native_log, reference_log, binary, source = self.make_packet(root)
        target = root / "fact.prepared.vortex"
        target_bytes = b"retained prepared Vortex artifact bytes"
        target.write_bytes(target_bytes)
        target_sha = validator.file_sha256(target)
        preparation_envelope = {
            "status": "success",
            "fallback": {"attempted": False, "engine": None},
            "fields": [
                {"key": "vortex_ingest_performed", "value": "true"},
                {"key": "external_engine_invoked", "value": "false"},
                {"key": "vortex_ingest_output_commit_status", "value": "committed"},
                {"key": "vortex_ingest_output_canonical_output_path", "value": str(target)},
                {"key": "vortex_ingest_output_bytes_written", "value": str(len(target_bytes))},
                {"key": "vortex_ingest_output_output_digest", "value": f"sha256:{target_sha}"},
                {"key": "source_read_metadata_scout_millis", "value": "0"},
                {"key": "source_read_metadata_scout_millis", "value": "0"},
            ],
        }
        prep_log = root / "preparation.stdout.json"
        prep_log_sha = write_json(prep_log, preparation_envelope)
        prep_receipt = {
            "returncode": 0,
            "guard_failures": [],
            "seconds": 0.5,
            "stdout": str(prep_log),
            "stdout_sha256": prep_log_sha,
            "binary_sha256": payload["binary"]["sha256"],
            "command": [
                str(binary), "vortex-prepare", str(source), str(target),
                "--input-format", "csv", "--memory-gb", "1",
                "--max-parallelism", "1", "--format", "json",
            ],
        }
        native_row = payload["records"][1]
        evidence = native_row["evidence"]
        old_source_sql = f"'{source.resolve()}'"
        prepared_source_sql = f"'{target.resolve()}'"
        prepared_sql = evidence["benchmark_sql_declarations"].replace(
            old_source_sql, prepared_source_sql
        )
        evidence["benchmark_input_state"] = "prepared"
        evidence["benchmark_input_preparation_calls"] = json.dumps([prep_receipt])
        evidence["benchmark_sql_declarations"] = prepared_sql
        calls = json.loads(evidence["benchmark_native_calls"])
        for call in calls:
            command = call["command"]
            command[command.index("--sql") + 1] = command[command.index("--sql") + 1].replace(
                old_source_sql, prepared_source_sql
            )
            command[command.index("--source-bindings") + 1] = json.dumps(
                {str(target): {"input_format": "vortex"}}
            )
        evidence["benchmark_native_calls"] = json.dumps(calls)
        payload["configuration"]["input_state"] = "prepared"
        write_json(manifest, payload)
        return manifest, payload, native_log, reference_log, binary, source, target, prep_log

    def make_json_writer_packet(self, root: Path, output_format: str):
        if output_format not in ("json", "jsonl"):
            raise ValueError("synthetic writer packet requires JSON or JSONL")
        manifest, payload, native_log, reference_log, binary, source = self.make_packet(root)
        output = root / f"result.{output_format}"
        output_rows = [copy.deepcopy(RESULT)]
        content = (json.dumps(output_rows, ensure_ascii=False, allow_nan=False)
                   if output_format == "json"
                   else "".join(json.dumps(row, ensure_ascii=False, allow_nan=False) + "\n"
                                for row in output_rows))
        output.write_text(content, encoding="utf-8")
        output_sha = validator.file_sha256(output)
        writer_envelope = {
            "status": "success",
            "fallback": {"attempted": False, "engine": None},
            "fields": [
                {"key": "public_workflow_fallback_attempted", "value": "false"},
                {"key": "public_workflow_external_engine_invoked", "value": "false"},
                {"key": "native_vortex_result_export_all_targets_committed", "value": "true"},
                {"key": "native_vortex_result_export_path", "value": str(output)},
            ],
        }
        writer_log_sha = write_json(native_log, writer_envelope)
        evidence = payload["records"][1]["evidence"]
        evidence["benchmark_output_format"] = output_format
        writer_call = json.loads(evidence["benchmark_native_calls"])[0]
        writer_call["stdout_sha256"] = writer_log_sha
        writer_call["command"][writer_call["command"].index("--request") + 1] = f"write_{output_format}"
        writer_call["command"].extend(["--output", str(output)])
        writer_call["output"] = {
            "path": str(output), "sha256": output_sha, "bytes": output.stat().st_size,
        }
        evidence["benchmark_native_calls"] = json.dumps([writer_call])
        evidence["benchmark_output_validation_calls"] = json.dumps([{
            "kind": "complete_json_file_readback", "format": output_format,
            "path": str(output), "sha256": output_sha, "bytes": output.stat().st_size,
            "seconds": 0.05,
        }])
        payload["configuration"]["output_format"] = output_format
        write_json(manifest, payload)
        return manifest, payload, output, native_log

    def validate(self, payload, root, *, allow_incomplete=False):
        manifest = root / "manifest.json"
        write_json(manifest, payload)
        return validator.validate_manifest(manifest, allow_incomplete=allow_incomplete)[0]

    def test_complete_public_native_packet_passes_using_retained_rows(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest, payload, *_ = self.make_packet(root)
            blockers, observed = validator.validate_manifest(manifest)
            self.assertEqual(blockers, [])
            self.assertEqual(observed["records"][1]["result"], RESULT)
            self.assertEqual(WORKLOADS[SCENARIO].result(
                [[{"row_count": 3, "metric_sum": 7.0}]], round_float), RESULT)

    def test_complete_prepared_public_native_packet_passes_with_retained_prepare_receipt(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest, payload, *_ = self.make_prepared_packet(root)
            blockers, observed = validator.validate_manifest(manifest)
            self.assertEqual(blockers, [])
            self.assertEqual(observed["records"][1]["result"], RESULT)

    def test_complete_json_and_jsonl_writer_packets_use_hashed_file_readbacks(self):
        for output_format in ("json", "jsonl"):
            with self.subTest(output_format=output_format), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                manifest, payload, output, _native_log = self.make_json_writer_packet(
                    root, output_format
                )
                blockers, observed = validator.validate_manifest(manifest)
                self.assertEqual(blockers, [])
                self.assertEqual(observed["records"][1]["result"], RESULT)
                readback = json.loads(
                    payload["records"][1]["evidence"]["benchmark_output_validation_calls"]
                )[0]
                self.assertEqual(readback["path"], str(output))
                self.assertEqual(readback["sha256"], validator.file_sha256(output))

    def test_json_writer_readback_evidence_fails_closed(self):
        mutations = ("changed_artifact_hash", "changed_rows_with_new_hash", "missing_readback",
                     "missing_path", "wrong_path", "wrong_format")
        for mutation in mutations:
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                _, original, output, _ = self.make_json_writer_packet(root, "json")
                payload = copy.deepcopy(original)
                evidence = payload["records"][1]["evidence"]
                call = json.loads(evidence["benchmark_native_calls"])[0]
                readbacks = json.loads(evidence["benchmark_output_validation_calls"])
                readback = readbacks[0]
                if mutation == "changed_artifact_hash":
                    output.write_text('[{"row_count":3,"metric_sum":8.0}]', encoding="utf-8")
                elif mutation == "changed_rows_with_new_hash":
                    output.write_text('[{"row_count":3,"metric_sum":8.0}]', encoding="utf-8")
                    changed_identity = {
                        "path": str(output), "sha256": validator.file_sha256(output),
                        "bytes": output.stat().st_size,
                    }
                    call["output"] = changed_identity
                    readback.update(changed_identity)
                    evidence["benchmark_native_calls"] = json.dumps([call])
                    evidence["benchmark_output_validation_calls"] = json.dumps(readbacks)
                elif mutation == "missing_readback":
                    evidence["benchmark_output_validation_calls"] = "[]"
                elif mutation == "missing_path":
                    readback.pop("path")
                    evidence["benchmark_output_validation_calls"] = json.dumps(readbacks)
                elif mutation == "wrong_path":
                    readback["path"] = str(root / "other.json")
                    evidence["benchmark_output_validation_calls"] = json.dumps(readbacks)
                elif mutation == "wrong_format":
                    readback["format"] = "jsonl"
                    evidence["benchmark_output_validation_calls"] = json.dumps(readbacks)
                self.assertTrue(self.validate(payload, root), mutation)

    def test_case_coverage_and_engine_identity_are_exact(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            _, original, *_ = self.make_packet(root)
            cases = []
            missing = copy.deepcopy(original)
            missing["records"].pop()
            cases.append(("missing case", missing))
            duplicate = copy.deepcopy(original)
            duplicate["records"].append(copy.deepcopy(duplicate["records"][-1]))
            cases.append(("duplicate case", duplicate))
            undeclared = copy.deepcopy(original)
            undeclared["records"][-1]["repeat"] = 2
            cases.append(("undeclared case", undeclared))
            alias = copy.deepcopy(original)
            alias["engines"].append("shardloom-vortex")
            alias["records"].append({"engine": "shardloom-vortex", "format": "csv",
                                     "scenario": SCENARIO, "repeat": 1})
            cases.append(("engine alias", alias))
            missing_reference = copy.deepcopy(original)
            missing_reference["records"] = [missing_reference["records"][-1]]
            cases.append(("missing reference", missing_reference))
            for label, payload in cases:
                with self.subTest(label=label):
                    self.assertTrue(self.validate(payload, root))

    def test_frozen_binary_input_logs_and_harness_hashes_cannot_drift(self):
        mutations = ("binary", "input", "native_log", "harness_hash")
        for mutation in mutations:
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                _, payload, native_log, _, binary, source = self.make_packet(root)
                if mutation == "binary":
                    binary.write_bytes(b"changed executable")
                elif mutation == "input":
                    source.write_bytes(b"changed source")
                elif mutation == "native_log":
                    native_log.write_bytes(native_log.read_bytes() + b" \n")
                else:
                    first = next(iter(payload["harness_sources"]))
                    payload["harness_sources"][first] = "0" * 64
                self.assertTrue(self.validate(payload, root))

    def test_native_rows_require_full_schema_and_must_match_independent_result(self):
        for mutation in ("missing_schema", "poisoned_payload"):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                _, payload, native_log, _, _, _ = self.make_packet(root)
                call = json.loads(payload["records"][1]["evidence"]["benchmark_native_calls"])[0]
                envelope = json.loads(native_log.read_text())
                if mutation == "missing_schema":
                    envelope["fields"] = [field for field in envelope["fields"]
                                          if field["key"] != "result_schema_format"]
                else:
                    field = next(item for item in envelope["fields"] if item["key"] == "result_jsonl")
                    field["value"] = '{"row_count":3,"metric_sum":999.0}\n'
                call["stdout_sha256"] = write_json(native_log, envelope)
                payload["records"][1]["evidence"]["benchmark_native_calls"] = json.dumps([call])
                self.assertTrue(self.validate(payload, root))

    def test_process_status_guards_summary_timing_and_incomplete_override_fail_closed(self):
        mutations = ("nonzero", "guard", "summary", "timing", "allow_incomplete")
        for mutation in mutations:
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                _, payload, *_ = self.make_packet(root)
                changed = copy.deepcopy(payload)
                if mutation == "nonzero":
                    call = json.loads(changed["records"][1]["evidence"]["benchmark_native_calls"])
                    call[0]["returncode"] = 3
                    changed["records"][1]["evidence"]["benchmark_native_calls"] = json.dumps(call)
                elif mutation == "guard":
                    call = json.loads(changed["records"][1]["evidence"]["benchmark_native_calls"])
                    call[0]["guard_failures"] = ["memory budget exceeded"]
                    changed["records"][1]["evidence"]["benchmark_native_calls"] = json.dumps(call)
                elif mutation == "summary":
                    changed["summary"]["expected_cases"] = 3
                elif mutation == "timing":
                    changed["records"][1]["seconds"] = 1.5
                if mutation == "allow_incomplete":
                    self.assertTrue(self.validate(changed, root, allow_incomplete=True))
                else:
                    self.assertTrue(self.validate(changed, root))

    def test_prepared_input_contract_fails_closed(self):
        mutations = (
            "missing_receipt", "changed_target", "nonzero_receipt", "guard_receipt",
            "wrong_binary", "source_not_in_input_inventory", "unsafe_fallback",
            "unprepared_binding", "raw_prepared_mismatch", "output_format_mismatch",
            "unsuccessful_envelope", "wrong_committed_path", "wrong_byte_count",
            "missing_output_digest", "incorrect_output_digest", "conflicting_duplicate",
            "executable_path_mismatch",
        )
        for mutation in mutations:
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                _, original, _, _, binary, source, target, prep_log = self.make_prepared_packet(root)
                payload = copy.deepcopy(original)
                evidence = payload["records"][1]["evidence"]
                prep = json.loads(evidence["benchmark_input_preparation_calls"])
                if mutation == "missing_receipt":
                    prep.clear()
                    evidence["benchmark_input_preparation_calls"] = json.dumps(prep)
                elif mutation == "changed_target":
                    target.write_bytes(target.read_bytes() + b" changed")
                elif mutation == "nonzero_receipt":
                    prep[0]["returncode"] = 2
                    evidence["benchmark_input_preparation_calls"] = json.dumps(prep)
                elif mutation == "guard_receipt":
                    prep[0]["guard_failures"] = ["fixture guard failure"]
                    evidence["benchmark_input_preparation_calls"] = json.dumps(prep)
                elif mutation == "wrong_binary":
                    prep[0]["binary_sha256"] = "0" * 64
                    evidence["benchmark_input_preparation_calls"] = json.dumps(prep)
                elif mutation == "source_not_in_input_inventory":
                    prep[0]["command"][2] = str(root / "unadmitted.csv")
                    evidence["benchmark_input_preparation_calls"] = json.dumps(prep)
                elif mutation == "unsafe_fallback":
                    envelope = json.loads(prep_log.read_text())
                    envelope["fallback"] = {"attempted": True, "engine": "duckdb"}
                    prep[0]["stdout_sha256"] = write_json(prep_log, envelope)
                    evidence["benchmark_input_preparation_calls"] = json.dumps(prep)
                elif mutation == "unprepared_binding":
                    other = root / "other.vortex"
                    other.write_bytes(b"not prepared by this receipt")
                    call = json.loads(evidence["benchmark_native_calls"])[0]
                    call["command"][call["command"].index("--source-bindings") + 1] = json.dumps(
                        {str(other): {"input_format": "vortex"}}
                    )
                    evidence["benchmark_native_calls"] = json.dumps([call])
                elif mutation == "raw_prepared_mismatch":
                    payload["configuration"]["input_state"] = "raw"
                elif mutation == "output_format_mismatch":
                    evidence["benchmark_output_format"] = "vortex"
                elif mutation == "unsuccessful_envelope":
                    envelope = json.loads(prep_log.read_text())
                    envelope["status"] = "failed"
                    prep[0]["stdout_sha256"] = write_json(prep_log, envelope)
                    evidence["benchmark_input_preparation_calls"] = json.dumps(prep)
                elif mutation == "wrong_committed_path":
                    envelope = json.loads(prep_log.read_text())
                    field = next(item for item in envelope["fields"]
                                 if item["key"] == "vortex_ingest_output_canonical_output_path")
                    field["value"] = str(root / "different.vortex")
                    prep[0]["stdout_sha256"] = write_json(prep_log, envelope)
                    evidence["benchmark_input_preparation_calls"] = json.dumps(prep)
                elif mutation == "wrong_byte_count":
                    envelope = json.loads(prep_log.read_text())
                    field = next(item for item in envelope["fields"]
                                 if item["key"] == "vortex_ingest_output_bytes_written")
                    field["value"] = str(int(field["value"]) + 1)
                    prep[0]["stdout_sha256"] = write_json(prep_log, envelope)
                    evidence["benchmark_input_preparation_calls"] = json.dumps(prep)
                elif mutation in ("missing_output_digest", "incorrect_output_digest"):
                    envelope = json.loads(prep_log.read_text())
                    if mutation == "missing_output_digest":
                        envelope["fields"] = [item for item in envelope["fields"]
                                              if item["key"] != "vortex_ingest_output_output_digest"]
                    else:
                        field = next(item for item in envelope["fields"]
                                     if item["key"] == "vortex_ingest_output_output_digest")
                        field["value"] = "sha256:" + "0" * 64
                    prep[0]["stdout_sha256"] = write_json(prep_log, envelope)
                    evidence["benchmark_input_preparation_calls"] = json.dumps(prep)
                elif mutation == "conflicting_duplicate":
                    envelope = json.loads(prep_log.read_text())
                    envelope["fields"].append(
                        {"key": "source_read_metadata_scout_millis", "value": "1"}
                    )
                    prep[0]["stdout_sha256"] = write_json(prep_log, envelope)
                    evidence["benchmark_input_preparation_calls"] = json.dumps(prep)
                elif mutation == "executable_path_mismatch":
                    prep[0]["command"][0] = str(root / "other-shardloom")
                    evidence["benchmark_input_preparation_calls"] = json.dumps(prep)
                self.assertTrue(self.validate(payload, root), mutation)

    def test_preparation_receipts_must_match_across_native_cases(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            _, original, *_ = self.make_prepared_packet(root)
            payload = copy.deepcopy(original)
            payload["configuration"]["repeats"] = 2
            reference = copy.deepcopy(payload["records"][0])
            reference["repeat"] = 2
            native = copy.deepcopy(payload["records"][1])
            native["repeat"] = 2
            preparation = json.loads(
                native["evidence"]["benchmark_input_preparation_calls"]
            )
            preparation[0]["seconds"] += 0.1
            native["evidence"]["benchmark_input_preparation_calls"] = json.dumps(preparation)
            payload["records"].extend((reference, native))
            payload["summary"] = {
                "complete": True, "expected_cases": 4,
                "recorded_cases": 4, "passed_cases": 4,
            }
            self.assertTrue(self.validate(payload, root))

    def test_native_request_must_match_configured_output_mode(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            _, payload, *_ = self.make_packet(root)
            payload["configuration"]["output_format"] = "vortex"
            payload["records"][1]["evidence"]["benchmark_output_format"] = "vortex"
            # The retained collect receipt is otherwise valid. Only the mismatch
            # with the requested configuration must cause rejection.
            self.assertEqual(self.validate(payload, root), [
                "native call does not request the declared output format"
            ])


if __name__ == "__main__":
    unittest.main()
