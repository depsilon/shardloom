# SPDX-License-Identifier: Apache-2.0
import json
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
from run_clickbench_query_uat import compress_completed_log, correctness_boundary, equivalent, extract_reference_result, extract_result, read_json_log, run_command, run_profiled_command, score, strict_json
from native_workflow_protocol import public_workflow_command
from clickbench_reference_packet import canonical_values_sha256


def envelope(summary):
    return {"status": "success", "human_text": summary, "fields": [
        {"key": "public_workflow_fallback_attempted", "value": "false"},
        {"key": "public_workflow_external_engine_invoked", "value": "false"},
    ]}


def schema_payload(rows, dtypes=None, names=None):
    names = list(rows[0]) if names is None and rows else (names or [])
    if dtypes is None:
        dtypes = []
        for name in names:
            values = [row[name] for row in rows]
            present = next((value for value in values if value is not None), None)
            nullable = any(value is None for value in values)
            if present is None:
                dtype = "Null"
            elif type(present) is bool:
                dtype = {"Bool": nullable}
            elif type(present) is int:
                kind = "u64" if present > (1 << 63) - 1 else "i64"
                dtype = {"Primitive": [kind, nullable]}
            elif isinstance(present, str):
                dtype = {"Utf8": nullable}
            else:
                raise AssertionError(f"test schema needs an explicit dtype for {name}")
            dtypes.append(dtype)
    schema = {"Struct": [{"names": names, "dtypes": dtypes}, False]}
    return [
        {"key": "result_schema_json", "value": json.dumps(schema)},
        {"key": "result_schema_format", "value": "vortex.dtype.serde.v1"},
    ]


class ClickBenchUatTests(unittest.TestCase):
    def _runner_fixture(self, root, reference_kind="independent_reference"):
        query_path = root / "queries.sql"
        statements = [f"SELECT {query_id} AS value" for query_id in range(1, 44)]
        query_bytes = ("\n".join(f"{statement};" for statement in statements) + "\n").encode()
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

    def _run_mocked_single(self, root, *, identity_mutation=None):
        query_path, packet_path, binary, source, uat_root = self._runner_fixture(root)
        result_rows = [{"value": 1}]
        schema = {"Struct": [{"names": ["value"],
                             "dtypes": [{"Primitive": ["i64", False]}]}, False]}

        def fake_profiled_command(command, prefix, timeout, guard):
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
                ],
            }
            prefix.with_suffix(".stdout.json").write_text(json.dumps(envelope))
            prefix.with_suffix(".stderr.txt").write_text("")
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

        argv = [
            "run_clickbench_query_uat.py",
            "--binary", str(binary),
            "--input", str(source),
            "--uat-root", str(uat_root),
            "--queries", str(query_path),
            "--reference-packet", str(packet_path),
            "--build-commit", "test-commit",
            "--query-ids", "1",
        ]
        with (
            patch.object(sys, "argv", argv),
            patch.object(query_uat, "check_budgets", return_value={}),
            patch.object(query_uat, "run_profiled_command", side_effect=fake_profiled_command),
            patch.object(query_uat, "file_sha256", side_effect=maybe_changed_hash),
            contextlib.redirect_stdout(io.StringIO()),
        ):
            result = query_uat.main()
        summaries = list((uat_root / "logs").glob("full43_*/summary.json"))
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
        self.assertEqual(len(summary["records"]), 3)
        hashed_names = {Path(path).name for path in summary["harness_sha256"]}
        self.assertTrue({
            "run_clickbench_query_uat.py", "clickbench_reference_packet.py",
            "native_workflow_protocol.py", "timed_native_command.py", "local_uat_storage.py",
            "_result_schema.py",
        } <= hashed_names)
        package_sources = (Path(__file__).resolve().parents[1] / "python" / "src" / "shardloom").rglob("*.py")
        self.assertTrue({str(path.resolve()) for path in package_sources} <= set(summary["harness_sha256"]))

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
                self.assertEqual(len(summary["records"]), 3)
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

    def test_hot_score_uses_only_second_and_third_runs(self):
        records = [{"query": 1, "run": i + 1, "seconds": seconds, "passed": True} for i, seconds in enumerate([1.0, 5.0, 3.0])]
        result = score(records, 1)
        self.assertEqual(result["query_total_seconds"], 1.0)
        self.assertEqual(result["hot_total_seconds"], 3.0)
        self.assertEqual(result["runs_completed"], 3)
        self.assertFalse(score(records[:2], 1)["complete"])
        self.assertFalse(score([records[0]] * 3, 1)["complete"])

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
        result = score(records, 43, [34, 35])
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
