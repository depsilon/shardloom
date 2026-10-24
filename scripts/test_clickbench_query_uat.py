# SPDX-License-Identifier: Apache-2.0
import json
import csv
import gzip
import hashlib
import os
from pathlib import Path
import sys
import tempfile
import time
import unittest
from unittest.mock import patch
import contextlib
import io

from local_uat_storage import StorageGuardError, accounted_bytes, check_budgets
import run_clickbench_query_uat as query_uat
import run_clickbench_paired_query_uat as paired_uat
from run_clickbench_query_uat import compress_completed_log, correctness_boundary, equivalent, extract_reference_result, extract_result, read_json_log, run_command, run_profiled_command, score, strict_json
from native_workflow_protocol import public_workflow_command
from clickbench_reference_packet import canonical_values_sha256
from clickbench_native_sql import native_query_statements
from native_workflow_test_support import schema_payload
from query_timing_report import TIMING_BOUNDARY, TIMING_CLOCK


def envelope(summary):
    return {"status": "success", "human_text": summary, "fields": [
        {"key": "public_workflow_fallback_attempted", "value": "false"},
        {"key": "public_workflow_external_engine_invoked", "value": "false"},
    ]}


class ClickBenchUatTests(unittest.TestCase):
    def _runner_fixture(self, root, reference_kind="independent_reference", *, pinned_queries=False):
        query_path = root / "queries.sql"
        statements = [f"SELECT {query_id} AS value" for query_id in range(1, 44)]
        query_bytes = ("\n".join(f"{statement};" for statement in statements) + "\n").encode()
        if pinned_queries:
            query_bytes, statements = query_uat.query_statements(
                Path(__file__).resolve().parents[1] / "benchmarks/clickbench/queries.sql")
        query_path.write_bytes(query_bytes)
        packet_path = root / "reference.json"
        records = []
        for query_id, statement in enumerate(statements, 1):
            values = [{"value": query_id}]
            records.append({"query_id": query_id, "statement": statement, "values": values,
                            "values_sha256": canonical_values_sha256(values)})
        packet_path.write_text(json.dumps({
            "schema_version": "shardloom.clickbench.reference_values.v1",
            "queries_sha256": hashlib.sha256(query_bytes).hexdigest(),
            "reference_kind": reference_kind,
            "records": records,
        }, allow_nan=False))
        binary = root / "shardloom"
        binary.write_bytes(b"test binary")
        source = root / "source.vortex"
        source.write_bytes(b"test Vortex source")
        uat_root = root / "uat"
        uat_root.mkdir()
        return query_path, packet_path, binary, source, uat_root

    def _run_mocked_single(self, root, *, identity_mutation=None, log_options=(),
                           historical_bytes=0, check_storage=False, budget_probe=None,
                           query_ids="1", repeats=None, paired=False, engine_timing=True,
                           pinned_queries=False):
        runner = paired_uat if paired else query_uat
        query_path, packet_path, binary, source, uat_root = self._runner_fixture(
            root, pinned_queries=pinned_queries)
        if historical_bytes:
            history = uat_root / "logs/completed/retained.json"
            history.parent.mkdir(parents=True)
            history.write_bytes(b"h" * historical_bytes)
        schema = {"Struct": [{"names": ["value"],
                             "dtypes": [{"Primitive": ["i64", False]}]}, False]}

        def fake_profiled_command(command, prefix, timeout, guard):
            result_rows = [{"value": int(prefix.name[1:3])}]
            envelope = {
                "status": "success",
                "fallback": {"attempted": False, "allowed": False},
                "fields": [
                    {"key": "public_workflow_fallback_attempted", "value": "false"},
                    {"key": "public_workflow_external_engine_invoked", "value": "false"},
                    {"key": "result_values_json", "value": json.dumps(result_rows)},
                    {"key": "result_payload_complete", "value": "true"},
                    {"key": "output_row_count", "value": "1"},
                    {"key": "result_schema_json", "value": json.dumps(schema)},
                    {"key": "result_schema_format", "value": "vortex.dtype.serde.v1"},
                    {"key": "native_query_timing_reported", "value": "true"},
                    {"key": "native_query_execution_nanos", "value": "60000000"},
                    {"key": "native_result_serialization_nanos", "value": "5000000"},
                    {"key": "native_query_timing_clock", "value": TIMING_CLOCK},
                    {"key": "native_query_timing_boundary", "value": TIMING_BOUNDARY},
                ],
            }
            if not engine_timing:
                envelope["fields"] = [field for field in envelope["fields"]
                                      if not field["key"].startswith(("native_query_", "native_result_serialization_"))]
            prefix.with_suffix(".stdout.json").write_text(json.dumps(envelope))
            prefix.with_suffix(".stderr.txt").write_text("")
            prefix.with_suffix(".timing.json").write_text("{}")
            prefix.with_suffix(".pid").write_text("1")
            guard()
            return {"returncode": 0, "guard_failures": [], "seconds": 0.1}

        original_hash = query_uat.file_sha256
        identity_calls = {}

        def maybe_changed_hash(path):
            path = Path(path).resolve()
            if identity_mutation == "packet" and path == packet_path.resolve():
                packet_path.write_bytes(packet_path.read_bytes() + b" ")
            if identity_mutation == "harness" and path.name == "_result_schema.py":
                identity_calls[path] = identity_calls.get(path, 0) + 1
                if identity_calls[path] == 2:
                    return "0" * 64
            return original_hash(path)

        binary_args = (["--control-binary", str(binary), "--candidate-binary", str(binary),
                        "--control-commit", "test-commit", "--candidate-commit", "test-commit"]
                       if paired else ["--binary", str(binary), "--build-commit", "test-commit"])
        argv = [
            str(runner.__file__), *binary_args,
            "--input", str(source),
            "--uat-root", str(uat_root),
            "--queries", str(query_path),
            "--reference-packet", str(packet_path),
            *(["--query-ids", query_ids] if query_ids is not None else []),
            *(["--repeats", str(repeats)] if repeats is not None else []),
            *log_options,
        ]
        with (
            patch.object(sys, "argv", argv),
            patch("local_uat_storage.available_bytes", return_value=1 << 40),
            patch.object(runner, "check_budgets",
                         side_effect=budget_probe or (check_budgets if check_storage else lambda *a, **kw: {})),
            patch.object(runner, "run_profiled_command", side_effect=fake_profiled_command),
            patch.object(runner, "file_sha256", side_effect=maybe_changed_hash),
            contextlib.redirect_stdout(io.StringIO()),
        ):
            result = runner.main()
        pattern = "paired43_*/summary.json" if paired else "full43_*/summary.json"
        summaries = list((uat_root / "logs").glob(pattern))
        self.assertEqual(len(summaries), 1)
        return result, json.loads(summaries[0].read_text())

    def test_single_packet_run_persists_successful_identity_and_harness_receipt(self):
        with tempfile.TemporaryDirectory() as directory:
            result, summary = self._run_mocked_single(Path(directory))
        self.assertEqual(result, 0)
        self.assertTrue(summary["complete"])
        self.assertTrue(summary["completed_identity_check"])
        self.assertTrue(summary["reference_packet_identity_verified"])
        self.assertTrue(summary["harness_identity_verified"])
        self.assertEqual(summary["reference_kind"], "independent_reference")
        self.assertIn("independent reference", summary["correctness_boundary"])
        self.assertEqual(len(summary["records"]), 1)
        self.assertEqual(summary["repeats"], 1)
        self.assertFalse(summary["full_result_validation"])
        hashed_names = {Path(path).name for path in summary["harness_sha256"]}
        self.assertTrue({
            "run_clickbench_query_uat.py", "clickbench_reference_packet.py",
            "native_workflow_protocol.py", "timed_native_command.py", "local_uat_storage.py",
            "_result_schema.py",
        } <= hashed_names)
        package_sources = (Path(__file__).resolve().parents[1] / "python" / "src" / "shardloom").rglob("*.py")
        self.assertTrue({str(path.resolve()) for path in package_sources} <= set(summary["harness_sha256"]))

    def test_single_and_paired_runners_record_and_submit_the_same_explicit_byte_sql(self):
        _, original = query_uat.query_statements(
            Path(__file__).resolve().parents[1] / "benchmarks/clickbench/queries.sql")
        for paired in (False, True):
            with self.subTest(paired=paired), tempfile.TemporaryDirectory() as directory:
                result, summary = self._run_mocked_single(
                    Path(directory), pinned_queries=True, query_ids="28,29", paired=paired)
                self.assertEqual(result, 0)
                submitted, receipt = native_query_statements(original, summary["queries_sha256"])
                self.assertEqual(summary["query_dialect"], receipt)
                self.assertIn("clickbench_native_sql.py",
                              {Path(path).name for path in summary["harness_sha256"]})
                self.assertEqual(len(summary["records"]), 4 if paired else 2)
                for record in summary["records"]:
                    index = record["query"] - 1
                    self.assertEqual(record["query_statement"], original[index])
                    self.assertEqual(record["executed_query_statement"], submitted[index])
                    self.assertEqual(record["command"][record["command"].index("--sql") + 1],
                                     submitted[index])

    def test_query_history_larger_than_active_run_budget_is_admitted(self):
        with tempfile.TemporaryDirectory() as directory:
            result, summary = self._run_mocked_single(
                Path(directory), historical_bytes=65536, check_storage=True,
                log_options=("--max-log-mib", "1", "--max-run-log-mib", "0.05"),
            )
            retained = Path(directory) / "uat/logs/completed/retained.json"
            self.assertEqual(retained.read_bytes(), b"h" * 65536)
        self.assertEqual(result, 0)
        self.assertTrue(summary["complete"])
        self.assertEqual(summary["log_budget_bytes"], {"retained": 1048576, "active_run": 52428})
        self.assertEqual(summary["storage_preflight"]["run_log_bytes"], 0)
        self.assertGreater(summary["storage_preflight"]["log_bytes"], 52428)

    def test_retained_log_denial_happens_before_queries_or_new_log_directory(self):
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(StorageGuardError, "retained log budget"):
                self._run_mocked_single(
                    Path(directory), historical_bytes=65536, check_storage=True,
                    log_options=("--max-log-mib", "0.05", "--max-run-log-mib", "1"),
                )
            self.assertEqual(list((Path(directory) / "uat/logs").iterdir()),
                             [Path(directory) / "uat/logs/completed"])
            self.assertFalse((Path(directory) / "uat/.ingest-uat.lock").exists())

    def test_final_query_summary_budget_failure_cannot_return_success(self):
        for label, guard_failure in (
            ("retained log ceiling", "UAT retained log budget exceeded after final summary"),
            ("active run ceiling", "UAT active run log budget exceeded after final summary"),
        ):
            with self.subTest(ceiling=label), tempfile.TemporaryDirectory() as directory:
                def deny_completed_summary(root, target, logs, **limits):
                    summary_path = logs / "summary.json"
                    if (summary_path.is_file()
                            and json.loads(summary_path.read_text()).get("completed_identity_check")):
                        raise StorageGuardError(guard_failure)
                    return check_budgets(root, target, logs, **limits)

                with self.assertRaisesRegex(StorageGuardError, "after final summary"):
                    self._run_mocked_single(Path(directory), budget_probe=deny_completed_summary)
                summary = json.loads(
                    next((Path(directory) / "uat/logs").glob("*/summary.json")).read_text()
                )
                self.assertFalse(summary["complete"])
                self.assertFalse(summary["full_result_validation"])
                self.assertEqual(summary["status"], "failed")
                self.assertEqual(summary["storage_error"], guard_failure)
                self.assertEqual(summary["failure"], "storage budget rejected: " + guard_failure)
                self.assertEqual(len(summary["records"]), 1)
                self.assertTrue(all(record["passed"] for record in summary["records"]))
                self.assertTrue(summary["completed_identity_check"])
                self.assertTrue(summary["reference_packet_identity_verified"])
                self.assertTrue(summary["harness_identity_verified"])
                self.assertIn("source_generation", summary)
                self.assertIn("binary_sha256", summary)
                self.assertFalse((Path(directory) / "uat/.ingest-uat.lock").exists())

    def test_single_packet_or_harness_identity_change_fails_and_keeps_records(self):
        for mutation in ("packet", "harness"):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as directory:
                result, summary = self._run_mocked_single(
                    Path(directory), identity_mutation=mutation
                )
                self.assertEqual(result, 1)
                self.assertFalse(summary["complete"])
                self.assertFalse(summary["full_result_validation"])
                self.assertFalse(summary["completed_identity_check"])
                self.assertEqual(len(summary["records"]), 1)
                self.assertIn("identity verification failed", summary["failure"])
                self.assertEqual(
                    summary["reference_packet_identity_verified"], mutation != "packet"
                )
                self.assertEqual(summary["harness_identity_verified"], mutation != "harness")

    def test_correctness_boundary_does_not_promote_legacy_or_partial_overrides(self):
        self.assertIn("not an independent oracle", correctness_boundary("retained_native_regression"))
        self.assertIn(
            "not a full independent oracle",
            correctness_boundary("retained_native_regression", reference_override=True),
        )
        self.assertIn("independent reference", correctness_boundary("independent_reference"))

    def test_completed_log_archive_preserves_bytes_and_rejects_clobber(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "q01_run1.stdout.json"
            raw = ("東京 complete values\n" * 100).encode()
            path.write_bytes(raw)
            evidence = compress_completed_log(path, lambda reserved: None)
            self.assertEqual(gzip.decompress(Path(evidence["path"]).read_bytes()), raw)
            self.assertEqual(evidence["raw_sha256"], hashlib.sha256(raw).hexdigest())
            self.assertFalse(path.exists())
            path.write_bytes(b"competing log")
            with self.assertRaises(FileExistsError):
                compress_completed_log(path, lambda reserved: None)
            self.assertEqual(path.read_bytes(), b"competing log")
            self.assertEqual(gzip.decompress(Path(evidence["path"]).read_bytes()), raw)

    def test_completed_archive_remains_a_complete_result_reference(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "q01_run1.stdout.json"
            expected = envelope("value summary: 99997497")
            path.write_text(json.dumps(expected))
            self.assertEqual(read_json_log(path), expected)
            compress_completed_log(path, lambda reserved: None)
            self.assertEqual(extract_reference_result(read_json_log(path)), [{"count_all": 99997497}])

    def test_archive_budget_rejection_preserves_raw_evidence(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            logs = root / "logs"
            logs.mkdir()
            path = logs / "q01_run1.stdout.json"
            raw = b"complete result" * 100
            path.write_bytes(raw)
            budget = accounted_bytes(logs)
            def guard(reserved):
                check_budgets(root, path, logs, min_free_bytes=0, reserve_bytes=reserved,
                              max_workspace_bytes=10**9, max_log_bytes=budget - reserved)
            with self.assertRaises(StorageGuardError):
                compress_completed_log(path, guard)
            self.assertEqual(path.read_bytes(), raw)
            self.assertFalse(path.with_suffix(".json.gz").exists())

    def test_active_archive_reservation_denial_preserves_original(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            logs = root / "logs/active"
            logs.mkdir(parents=True)
            path = logs / "q01_run1.stdout.json"
            raw = b"complete result" * 100
            path.write_bytes(raw)
            limit = accounted_bytes(logs)
            def guard(reserved):
                check_budgets(root, path, logs, min_free_bytes=0, reserve_bytes=reserved,
                              reserve_log_bytes=reserved, max_workspace_bytes=10**9,
                              max_log_bytes=10**8, max_run_log_bytes=limit)
            with self.assertRaisesRegex(StorageGuardError, "active run log budget"):
                compress_completed_log(path, guard)
            self.assertEqual(path.read_bytes(), raw)
            self.assertFalse(path.with_suffix(".json.gz").exists())

    def test_historical_reference_results_preserve_integer_precision(self):
        self.assertEqual(extract_reference_result(envelope("value summary: 99997497")), [{"count_all": 99997497}])
        legacy = envelope("value summary: old scalar")
        legacy["fields"].extend([{"key": "result_known", "value": "true"},
                                 {"key": "count", "value": "18446744073709551615"}])
        self.assertEqual(extract_reference_result(legacy), [{"count_all": (1 << 64) - 1}])
        rows = [{"UserID": 435090932899640449, "label": "\u03bb", "absent": None}]
        result = envelope("result summary: native_collect values=" + json.dumps({"rows": 1, "values": rows}))
        self.assertEqual(extract_reference_result(result), rows)
        self.assertEqual(extract_reference_result(envelope('result summary: aggregate values={"rows":1,"values":{"x":3}}')), [{"x": 3}])

    def test_live_scalar_and_array_results_use_only_structured_fields(self):
        scalar = envelope("value summary: 999")
        scalar["fields"].extend([
            {"key": "result_known", "value": "true"},
            {"key": "count", "value": str((1 << 63) + 7)},
        ])
        with self.assertRaises(ValueError):
            extract_result(scalar)
        for rows in ([], [{"id": (1 << 63) + 7, "label": "東京\nλ", "missing": None}]):
            result = envelope("value summary: 999")
            result["fields"].extend([
                {"key": "result_values_json", "value": json.dumps(rows)},
                {"key": "result_payload_complete", "value": "true"},
                {"key": "output_row_count", "value": str(len(rows))},
            ] + schema_payload(rows))
            self.assertEqual(extract_result(result), rows)
        for summary in ("value summary: 4", 'result summary: aggregate values={"rows":1,"values":{"n":4}}'):
            with self.assertRaises(ValueError):
                extract_result(envelope(summary))

    def test_historical_repeated_certificates_require_identical_values(self):
        historical = envelope("value summary: 4")
        historical["fields"].append(dict(historical["fields"][0]))
        self.assertEqual(extract_reference_result(historical), [{"count_all": 4}])
        with self.assertRaisesRegex(ValueError, "duplicate report field"):
            extract_result(historical)
        for unsafe in ("true", False, 0, None):
            historical["fields"][-1]["value"] = unsafe
            with self.subTest(unsafe=unsafe), self.assertRaisesRegex(ValueError, "conflicting historical"):
                extract_reference_result(historical)
        historical["fields"][-1]["value"] = "false"
        historical["fields"].extend([
            {"key": "result_values_json", "value": '[{"n":4}]'},
            {"key": "result_payload_complete", "value": "true"},
            {"key": "output_row_count", "value": "1"},
            *schema_payload([{"n": 4}]),
        ])
        with self.assertRaisesRegex(ValueError, "duplicate report field"):
            extract_reference_result(historical)

    def test_live_result_rejects_duplicate_keys_and_false_scalar_admission(self):
        for payload in ('[{"n":1,"n":2}]', '[{"n":NaN}]', '{"n":1}', '[1]', '[]'):
            result = envelope("no payload here")
            result["fields"].extend([
                {"key": "result_values_json", "value": payload},
                {"key": "result_payload_complete", "value": "true"},
                {"key": "output_row_count", "value": "1"},
                *schema_payload([{"n": 1}]),
            ])
            with self.subTest(payload=payload), self.assertRaises(ValueError):
                extract_result(result)
        for known, count in ((1, "2"), ("false", "2"), ("true", True), ("true", "-1")):
            result = envelope("value summary: 2")
            result["fields"].extend([
                {"key": "result_known", "value": known}, {"key": "count", "value": count},
            ])
            with self.subTest(known=known, count=count), self.assertRaises(ValueError):
                extract_result(result)
        result = envelope("no payload here")
        result["fields"].append(result["fields"][0])
        with self.assertRaisesRegex(ValueError, "duplicate report field"):
            extract_result(result)

    def test_public_request_parameters_preserve_the_workload_declaration(self):
        statement = "SELECT 'the; complete declaration' AS label FROM 'memory://rows'"
        bindings = {"memory://rows": {"input_format": "memory", "memory_input": {
            "kind": "rows", "schema": [["id", "int64"]], "rows": [["1"]],
        }}}
        for surface in ("sql", "python", "dataframe", "cli"):
            args = public_workflow_command("/native engine", statement, surface=surface,
                                           source_bindings=bindings, memory_gb=2, max_parallelism=3)
            self.assertEqual(args[:3], ["/native engine", "run", surface])
            self.assertEqual(args[args.index("--sql") + 1], statement)
            self.assertEqual(json.loads(args[args.index("--source-bindings") + 1]), bindings)
            self.assertNotIn("--input", args)
            self.assertFalse(any(arg.startswith("--vortex-") for arg in args))
        for format in ("vortex", "json", "jsonl", "csv", "parquet", "arrow_ipc", "avro", "orc"):
            args = public_workflow_command("native", "SELECT * FROM data", input_path="input.data",
                                           input_format="csv", requested_output=f"write_{format}",
                                           output=f"result.{format}", allow_overwrite=True)
            self.assertEqual(args[args.index("--request") + 1], f"write_{format}")
            self.assertEqual(args[args.index("--input-format") + 1], "csv")
            self.assertIn("--allow-overwrite", args)
        for kwargs in ({"memory_gb": 0}, {"memory_gb": True}, {"max_parallelism": 0},
                       {"input_path": "input"}, {"input_format": "csv"},
                       {"requested_output": "write_jsonl"}, {"output": "unexpected"},
                       {"allow_overwrite": True}, {"surface": "benchmark-only"}):
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                public_workflow_command("native", "SELECT 1", **kwargs)

    def test_descriptors_truncated_values_and_missing_evidence_fail_closed(self):
        for summary in ('result summary: projected_columns=id rows=4',
                        'result summary: aggregate values={"rows":4,"values":[{"x":1}]}'):
            with self.assertRaises(ValueError):
                extract_result(envelope(summary))
        for change in ({"fields": []}, {"status": "failed"}):
            with self.assertRaises(ValueError):
                extract_result(envelope("value summary: 4") | change)
        unsafe = envelope("value summary: 4")
        unsafe["fields"].append({"key": "kernel_external_engine_invoked", "value": "true"})
        with self.assertRaises(ValueError):
            extract_result(unsafe)

    def test_complete_jsonl_payload_is_independent_of_diagnostic_summary(self):
        for rows in ([], [{"identifier": (1 << 63) + 7, "label": "東京\nλ", "missing": None}],
                     [{"n": 2}, {"n": 3}]):
            with self.subTest(rows=rows):
                result = envelope('result summary: aggregate values={"rows":99,"values":null}')
                result["fields"].extend([
                    {"key": "result_jsonl", "value": "".join(json.dumps(row) + "\n" for row in rows)},
                    {"key": "result_payload_complete", "value": "true"},
                    {"key": "output_row_count", "value": str(len(rows))},
                ] + schema_payload(rows))
                self.assertEqual(extract_result(result), rows)

    def test_jsonl_rejects_missing_ambiguous_truncated_or_invalid_payloads(self):
        valid = [
            {"key": "result_jsonl", "value": '{"n":2}\n'},
            {"key": "result_payload_complete", "value": "true"},
            {"key": "output_row_count", "value": "1"},
        ] + schema_payload([{"n": 2}], [{"Primitive": ["i64", False]}])
        invalid = [valid[:index] + valid[index + 1:] for index in range(len(valid))]
        invalid.extend(valid + [item] for item in valid)
        for key, value in (
            ("result_payload_complete", "false"), ("result_payload_complete", 1),
            ("output_row_count", "2"), ("output_row_count", True),
            ("output_row_count", "-1"), ("output_row_count", "unknown"),
            ("result_jsonl", None), ("result_jsonl", ""),
            ("result_jsonl", "[2]\n"), ("result_jsonl", '{"n":NaN}\n'),
            ("result_jsonl", '{"n":1e999}\n'), ("result_jsonl", '{"n":2}\n\n'),
        ):
            invalid.append([dict(item, value=value) if item["key"] == key else item for item in valid])
        invalid.extend([
            [item for item in valid if item["key"] != "result_schema_json"],
            [item for item in valid if item["key"] != "result_schema_format"],
            [dict(item, value='{"Primitive":["i64",false]}') if item["key"] == "result_schema_json" else item
             for item in valid],
            [dict(item, value="unknown") if item["key"] == "result_schema_format" else item
             for item in valid],
            [dict(item, value=json.dumps({"Struct": [{"names": ["other"], "dtypes": [{"Primitive": ["i64", False]}]}, False]}))
             if item["key"] == "result_schema_json" else item for item in valid],
        ])
        for fields in invalid:
            with self.subTest(fields=fields):
                result = envelope('result summary: aggregate values={"rows":1,"values":[{"n":2}]}')
                result["fields"].extend(fields)
                with self.assertRaises(ValueError):
                    extract_result(result)

    def test_live_rows_are_checked_against_schema_before_returning_original_values(self):
        good = envelope("ignored")
        good["fields"].extend([
            {"key": "result_values_json", "value": json.dumps([{"n": 3, "flag": True}])},
            {"key": "result_payload_complete", "value": True},
            {"key": "output_row_count", "value": "1"},
            *schema_payload([{"n": 3, "flag": True}],
                            [{"Primitive": ["i64", False]}, {"Bool": False}]),
        ])
        self.assertEqual(extract_result(good), [{"n": 3, "flag": True}])

        invalid_rows = [
            ([{"n": None}], [{"Primitive": ["i64", False]}]),
            ([{"other": 3}], [{"Primitive": ["i64", False]}], ["n"]),
            ([{"n": 1 << 64}], [{"Primitive": ["u64", False]}]),
            ([{"n": 1}], [{"Bool": False}]),
        ]
        for case in invalid_rows:
            rows, dtypes, *declared_names = case
            result = envelope("ignored")
            result["fields"].extend([
                {"key": "result_values_json", "value": json.dumps(rows)},
                {"key": "result_payload_complete", "value": True},
                {"key": "output_row_count", "value": str(len(rows))},
                *schema_payload(rows, dtypes, declared_names[0] if declared_names else None),
            ])
            with self.subTest(rows=rows, dtypes=dtypes), self.assertRaises(ValueError):
                extract_result(result)

    def test_result_comparison_handles_order_nulls_and_float_tolerance(self):
        self.assertFalse(equivalent([1, 2], [2, 1]))
        self.assertFalse(equivalent(435090932899640449, float(435090932899640449)))
        self.assertFalse(equivalent(None, "null"))
        self.assertTrue(equivalent(1.0, 1.0 + 1e-14))
        self.assertFalse(equivalent(1.0, 1.01))
        for invalid in ('[NaN]', '[Infinity]', '[1e999]'):
            with self.assertRaises(ValueError):
                strict_json(invalid)

    def test_full_suite_default_executes_each_query_once_and_has_no_hot_score(self):
        with tempfile.TemporaryDirectory() as directory:
            result, summary = self._run_mocked_single(Path(directory), query_ids=None)
            with Path(summary["timing_csv"]).open(newline="") as stream:
                timing_rows = list(csv.DictReader(stream))
        self.assertEqual(result, 0)
        self.assertTrue(summary["complete"])
        self.assertTrue(summary["full_result_validation"])
        self.assertEqual(summary["repeats"], 1)
        self.assertEqual(summary["runs_completed"], 43)
        self.assertEqual(summary["runs_passed"], 43)
        self.assertEqual([(row["query"], row["run"]) for row in summary["records"]],
                         [(query, 1) for query in range(1, 44)])
        self.assertIsNone(summary["hot_total_seconds"])
        self.assertIsNone(summary["hot_geomean_seconds"])
        self.assertEqual(summary["query_total_seconds"], summary["all_raw_run_seconds"])
        self.assertEqual([int(row["query"]) for row in timing_rows], list(range(1, 44)))
        self.assertEqual(summary["timing_totals"]["calls_with_engine_timing"], 43)
        self.assertAlmostEqual(summary["timing_totals"]["totals"]["engine_execution_seconds"], 43 * 0.06)
        self.assertEqual(summary["schema_version"], "shardloom.clickbench.public_result_regression.v2")
        self.assertEqual(summary["timing_metric"], "engine_execution_seconds")
        self.assertAlmostEqual(summary["query_total_seconds"], 43 * 0.06)
        self.assertAlmostEqual(summary["secondary_process_wall"]["query_total_seconds"], 43 * 0.1)

    def test_repeats_are_explicit_and_persisted_by_both_runners(self):
        for paired in (False, True):
            for repeats in (None, 3):
                with self.subTest(paired=paired, repeats=repeats), tempfile.TemporaryDirectory() as directory:
                    result, summary = self._run_mocked_single(
                        Path(directory), repeats=repeats, paired=paired)
                count = repeats or 1
                self.assertEqual(result, 0)
                self.assertEqual(summary["repeats"], count)
                self.assertEqual(len(summary["records"]), count * (2 if paired else 1))
                self.assertEqual({row["run"] for row in summary["records"]}, set(range(1, count + 1)))
                scores = summary["scores"]["roles"].values() if paired else [summary]
                for result_score in scores:
                    self.assertTrue(result_score["complete"])
                    self.assertTrue(result_score["timing_available"])
                    self.assertEqual(result_score["timing_metric"], "engine_execution_seconds")
                    self.assertEqual(result_score["query_total_seconds"], 0.06)
                    self.assertEqual(result_score["hot_total_seconds"] is None, count == 1)
                if paired:
                    self.assertEqual(summary["schema_version"], "shardloom.clickbench.counterbalanced_pairs.v2")
                    self.assertEqual(summary["scores"]["secondary_process_wall"]["roles"]["control"]["query_total_seconds"], 0.1)
                    self.assertTrue(summary["complete_result_validation"])
                    self.assertTrue(summary["completed_identity_check"])
                    self.assertEqual({row["role"] for row in summary["records"]}, {"control", "candidate"})

    def test_historical_runners_validate_values_without_engine_scores(self):
        for paired in (False, True):
            with self.subTest(paired=paired), tempfile.TemporaryDirectory() as directory:
                result, summary = self._run_mocked_single(
                    Path(directory), paired=paired, engine_timing=False,
                    log_options=("--allow-missing-engine-timing",))
            self.assertEqual(result, 0)
            scores = summary["scores"] if paired else summary
            self.assertTrue(scores["complete"])
            self.assertFalse(scores["timing_available"])
            self.assertTrue(scores["secondary_process_wall"]["timing_available"])
            role_scores = scores["roles"].values() if paired else [scores]
            for result_score in role_scores:
                self.assertIsNone(result_score["query_total_seconds"])
            self.assertTrue(summary["completed_identity_check"])
            if paired:
                self.assertTrue(summary["complete_result_validation"])
                self.assertIsNone(scores["per_query"])

    def test_invalid_repeats_fail_before_either_runner_creates_logs(self):
        for paired in (False, True):
            for repeats in (0, -1):
                with self.subTest(paired=paired, repeats=repeats), tempfile.TemporaryDirectory() as directory:
                    with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as error:
                        self._run_mocked_single(Path(directory), repeats=repeats, paired=paired)
                    self.assertEqual(error.exception.code, 2)
                    self.assertFalse((Path(directory) / "uat/logs").exists())
                    self.assertFalse((Path(directory) / "uat/.ingest-uat.lock").exists())

    def test_hot_score_uses_only_second_and_third_runs_when_explicit(self):
        records = [{"query": 1, "run": i + 1, "seconds": seconds, "passed": True} for i, seconds in enumerate([1.0, 5.0, 3.0])]
        result = score(records, 1, repeats=3)
        self.assertEqual(result["query_total_seconds"], 1.0)
        self.assertEqual(result["hot_total_seconds"], 3.0)
        self.assertEqual(result["runs_completed"], 3)
        self.assertFalse(score(records[:2], 1, repeats=3)["complete"])
        self.assertFalse(score([records[0]] * 3, 1, repeats=3)["complete"])
        self.assertFalse(score(records, 1)["complete"])
        self.assertTrue(score(records[:1], 1)["complete"])
        for repeats in (0, -1, 1.5, True):
            with self.subTest(repeats=repeats), self.assertRaisesRegex(ValueError, "positive integer"):
                score(records, 1, repeats=repeats)

    def test_process_clock_excludes_watchdog_wait_and_timeout_stops_child(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            def guard():
                pass
            result = run_command([sys.executable, "-c", "print('{}')"], root / "out", root / "err", 10, guard)
            self.assertEqual(result["returncode"], 0)
            self.assertFalse(result["guard_failures"])
            started = time.monotonic()
            result = run_command([sys.executable, "-c", "import time; time.sleep(30)"], root / "slow", root / "slowerr", 0.1, guard)
            self.assertNotEqual(result["returncode"], 0)
            self.assertIn("timeout", result["guard_failures"][0])
            self.assertLess(time.monotonic() - started, 5)

    def test_targeted_results_cannot_be_scored_as_complete_full_suite(self):
        records = [{"query": q, "run": r, "seconds": float(r), "passed": True}
                   for q in [34, 35] for r in [3, 1, 2]]
        self.assertFalse(score(records, 43)["complete"])
        result = score(records, 43, [34, 35], repeats=3)
        self.assertEqual(result["query_ids"], [34, 35])
        self.assertEqual(result["hot_total_seconds"], 4.0)
        for selected in ([], [0], [44], [34, 34]):
            with self.assertRaises(ValueError):
                score(records, 43, selected)

    def test_native_profile_covers_one_child_and_complete_output(self):
        with tempfile.TemporaryDirectory() as directory:
            prefix = Path(directory) / "profile"
            result = run_profiled_command([sys.executable, "-c", "print('complete output')"],
                                          prefix, 10, lambda: None)
            self.assertEqual(result["returncode"], 0)
            self.assertFalse(result["guard_failures"])
            self.assertGreater(result["native_peak_rss_bytes"], 0)
            self.assertGreaterEqual(result["user_cpu_seconds"], 0)
            self.assertGreaterEqual(result["system_cpu_seconds"], 0)
            for key in ("minor_page_faults", "major_page_faults", "input_block_operations", "output_block_operations"):
                self.assertGreaterEqual(result[key], 0)
            self.assertLess(result["seconds"], result["supervised_wall_seconds"])
            self.assertEqual(prefix.with_suffix(".stdout.json").read_text(), "complete output\n")

    def test_active_log_flood_stops_query_child_below_retained_limit(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            logs = root / "logs/active"
            logs.mkdir(parents=True)
            prefix = logs / "flood"
            def guard():
                return check_budgets(root, prefix, logs, min_free_bytes=0, reserve_bytes=0,
                                     max_workspace_bytes=1 << 30, max_log_bytes=8 << 20,
                                     max_run_log_bytes=1 << 19)
            # The final storage check also fails after the watchdog has stopped
            # the child; the original oversized evidence remains inspectable.
            with self.assertRaisesRegex(StorageGuardError, "active run log budget"):
                run_profiled_command([
                    sys.executable, "-c",
                    "import time; print('x' * (2 * 1024 * 1024), flush=True); time.sleep(10)",
                ], prefix, 5, guard)
            timing = json.loads(prefix.with_suffix(".timing.json").read_text())
            self.assertNotEqual(timing["returncode"], 0)
            self.assertGreater(prefix.with_suffix(".stdout.json").stat().st_size, 1 << 19)
            with self.assertRaises(ProcessLookupError):
                os.kill(int(prefix.with_suffix(".pid").read_text()), 0)

    @unittest.skipUnless(os.name == "posix", "process-group ownership requires POSIX")
    def test_profile_timeout_kills_native_child_that_ignores_termination(self):
        with tempfile.TemporaryDirectory() as directory:
            prefix = Path(directory) / "ignores-term"
            result = run_profiled_command([
                sys.executable, "-c",
                "import signal,time; signal.signal(signal.SIGTERM, signal.SIG_IGN); "
                "print('native-ready', flush=True); time.sleep(30)",
            ], prefix, 1, lambda: None)
            self.assertIn("native command timeout", result["guard_failures"])
            self.assertEqual(prefix.with_suffix(".stdout.json").read_text(), "native-ready\n")
            # The supervisor must finish its own cleanup instead of dying in
            # the outer SIGKILL. Timing is published only after the child wait.
            self.assertEqual(result["returncode"], 130)
            timing = json.loads(prefix.with_suffix(".timing.json").read_text())
            self.assertEqual(timing["returncode"], 130)
            self.assertNotIn("native timing evidence is missing", result["guard_failures"])
            pid = int(prefix.with_suffix(".pid").read_text())
            # Signal zero sees zombies too. Require immediate disappearance on
            # supervisor return, not eventual reaping by a different parent.
            with self.assertRaises(ProcessLookupError):
                os.kill(pid, 0)


if __name__ == "__main__":
    unittest.main()
