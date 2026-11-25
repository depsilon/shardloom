# SPDX-License-Identifier: Apache-2.0
"""Offline unit tests for multi-table TPC-H and SSB workloads, oracles, and runner."""
from __future__ import annotations

import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

SCRIPTS_DIR = Path(__file__).resolve().parent
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

import run_tpch_ssb_suite as runner
import tpch_ssb_workloads as workloads


class TpchSsbSuiteTests(unittest.TestCase):
    def test_all_14_queries_match_dual_offline_oracles(self) -> None:
        tpch_tables = workloads.generate_tpch_tables(scale_rows=240, seed=20261124)
        ssb_tables = workloads.generate_ssb_tables(scale_rows=240, seed=20261124)
        self.assertEqual(set(tpch_tables), set(workloads.TPCH_SCHEMAS))
        self.assertEqual(set(ssb_tables), set(workloads.SSB_SCHEMAS))

        tpch_verified = workloads.verify_dual_oracles("tpch", list(workloads.TPCH_QUERIES), tpch_tables)
        ssb_verified = workloads.verify_dual_oracles("ssb", list(workloads.SSB_QUERIES), ssb_tables)
        self.assertEqual(len(tpch_verified), 8)
        self.assertEqual(len(ssb_verified), 6)
        for code, payload in {**tpch_verified, **ssb_verified}.items():
            self.assertEqual(payload["oracles"], ["python_reference", "sqlite3_sql"], code)
            self.assertGreater(len(payload["expected"]), 0, code)

    def test_bindings_only_include_referenced_tables_per_query(self) -> None:
        paths = {name: Path(f"/local/{name}.vortex") for name in workloads.TPCH_SCHEMAS}
        q5 = next(q for q in workloads.TPCH_QUERIES if q.code == "TPCH-Q5")
        q6 = next(q for q in workloads.TPCH_QUERIES if q.code == "TPCH-Q6")
        self.assertEqual(
            set(workloads.build_source_bindings(q5, paths, "vortex").keys()),
            {str(paths[t]) for t in ("customer", "orders", "lineitem", "supplier", "nation", "region")},
        )
        self.assertEqual(
            set(workloads.build_source_bindings(q6, paths, "vortex").keys()),
            {str(paths["lineitem"])},
        )

    def test_execute_suite_with_mocked_cli_produces_paired_non_claim_summaries(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            fake_bin = root / "shardloom"
            fake_bin.write_bytes(b"fake-cli")
            fake_bin.chmod(0o755)
            uat_root = root / "uat"
            fixtures_dir = root / "fixtures"
            # Pre-seed dummy fixture files so no PyArrow or real binary is needed in unit test.
            for suite_name, schemas in (("tpch", workloads.TPCH_SCHEMAS), ("ssb", workloads.SSB_SCHEMAS)):
                vx_dir = fixtures_dir / suite_name / "vortex"
                pq_dir = fixtures_dir / suite_name / "parquet"
                vx_dir.mkdir(parents=True)
                pq_dir.mkdir(parents=True)
                for tname in schemas:
                    (vx_dir / f"{tname}.vortex").write_bytes(b"VX")
                    (pq_dir / f"{tname}.parquet").write_bytes(b"PAR1")

            tpch_rows = workloads.generate_tpch_tables(scale_rows=120, seed=7)
            ssb_rows = workloads.generate_ssb_tables(scale_rows=120, seed=7)
            expected_by_sql = {
                spec.sql: workloads.python_oracle_query(spec, tpch_rows if spec.suite == "tpch" else ssb_rows)
                for spec in workloads.ALL_QUERIES
            }

            class FakeWorker:
                start_seconds = 0.01

                def __init__(self, binary: Path, stderr_path: Path):
                    stderr_path.write_text("")

                def run_query(self, command: list[str], stdout_path: Path, timeout: float, guard):
                    assert not stdout_path.exists(), f"duplicate stdout path {stdout_path}"
                    return _fake_run(command, Path(str(stdout_path).removesuffix(".stdout.json")), warm=True)

                def close(self) -> None:
                    pass

            def _fake_schema(rows: list[dict]) -> str:
                sample = rows[0] if rows else {}
                names = list(sample.keys())
                dtypes = []
                for name in names:
                    val = sample[name]
                    if isinstance(val, int) and not isinstance(val, bool):
                        dtypes.append({"Primitive": ["i64", False]})
                    elif isinstance(val, float):
                        dtypes.append({"Primitive": ["f64", False]})
                    else:
                        dtypes.append({"Utf8": False})
                return json.dumps({"Struct": [{"names": names, "dtypes": dtypes}, False]})

            def _fake_run(command: list[str], prefix: Path, *args, warm: bool = False, **kwargs):
                sql = command[command.index("--sql") + 1]
                rows = expected_by_sql[sql]
                envelope = {
                    "status": "success",
                    "fallback": {"attempted": False},
                    "fields": [
                        {"key": "public_workflow_fallback_attempted", "value": "false"},
                        {"key": "public_workflow_external_engine_invoked", "value": "false"},
                        {"key": "query_answer_cache_hit", "value": "false"},
                        {"key": "native_query_timing_reported", "value": "true"},
                        {"key": "native_query_timing_clock", "value": "monotonic_wall_not_cpu"},
                        {
                            "key": "native_query_timing_boundary",
                            "value": "prepared_native_execution_including_io_generation_validation_and_certification_excluding_result_serialization",
                        },
                        {"key": "native_query_execution_nanos", "value": "12000000"},
                        {"key": "native_result_serialization_nanos", "value": "1000000"},
                        {"key": "result_payload_complete", "value": "true"},
                        {"key": "output_row_count", "value": str(len(rows))},
                        {"key": "result_values_json", "value": json.dumps(rows)},
                        {"key": "result_schema_format", "value": "vortex.dtype.serde.v1"},
                        {"key": "result_schema_json", "value": _fake_schema(rows)},
                    ],
                }
                with prefix.with_suffix(".stdout.json").open("xb") as fh:
                    fh.write(json.dumps(envelope).encode("utf-8"))
                if not warm:
                    with prefix.with_suffix(".stderr.txt").open("xb") as fh:
                        fh.write(b"")
                return {
                    "returncode": 0,
                    "seconds": 0.05,
                    "guard_failures": [],
                    "native_peak_rss_bytes": 1024,
                    "system_cpu_seconds": 0.01,
                    "user_cpu_seconds": 0.02,
                    "minor_page_faults": 10,
                    "major_page_faults": 0,
                }

            args = runner.parse_args(
                [
                    "--binary",
                    str(fake_bin),
                    "--uat-root",
                    str(uat_root),
                    "--fixtures-dir",
                    str(fixtures_dir),
                    "--suite",
                    "all",
                    "--input-format",
                    "vortex",
                    "--lane",
                    "paired",
                    "--scale-rows",
                    "120",
                    "--seed",
                    "7",
                ]
            )
            with (
                patch.object(runner, "require_local_path", side_effect=lambda p, *a, **k: Path(p).resolve()),
                patch.object(runner, "check_budgets", return_value={"ok": True, "free_bytes": 50 * runner.GIB}),
                patch.object(runner, "run_profiled_command", side_effect=_fake_run),
                patch.object(runner, "PersistentResidentWorker", FakeWorker),
            ):
                summary, summary_path = runner.execute_suite(args)

            self.assertTrue(summary_path.is_file())
            self.assertTrue(summary["complete"])
            self.assertTrue(summary["full_result_validation"])
            self.assertEqual(summary["claim_gate_status"], "not_claim_grade")
            self.assertFalse(summary["performance_claim_allowed"])
            self.assertFalse(summary["fallback_attempted"])
            self.assertEqual(summary["selected_query_ids"], list(range(1, 15)))
            for lane_name in ("cold_process_cli", "warm_resident_session"):
                lane = summary["lanes"][lane_name]
                self.assertEqual(lane["timing_metric"], "engine_execution_seconds")
                self.assertEqual(lane["runs_passed"], 14)
                self.assertAlmostEqual(lane["total_seconds"], 14 * 0.012, places=6)

    def test_prepare_fixtures_without_pyarrow_namespaces_and_cleans_jsonl_across_suites(self) -> None:
        import types

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            fake_bin = root / "shardloom"
            fake_bin.write_bytes(b"fake-cli")
            fake_bin.chmod(0o755)
            fixtures_dir = root / "fixtures"
            seen_jsonl_paths: list[Path] = []

            def _fake_subproc_run(cmd, *args, **kwargs):
                bindings = json.loads(cmd[cmd.index("--source-bindings") + 1])
                jsonl_path = Path(next(iter(bindings.keys())))
                self.assertTrue(jsonl_path.is_file())
                seen_jsonl_paths.append(jsonl_path)
                dest = Path(cmd[cmd.index("--output") + 1])
                dest.parent.mkdir(parents=True, exist_ok=True)
                dest.write_bytes(b"PAR1")
                return types.SimpleNamespace(returncode=0, stdout="", stderr="")

            with (
                patch.object(runner.importlib.util, "find_spec", return_value=None),
                patch.object(runner.subprocess, "run", side_effect=_fake_subproc_run),
            ):
                table_paths, _, _ = runner.prepare_fixtures(
                    fixtures_dir,
                    ["tpch", "ssb"],
                    scale_rows=60,
                    seed=7,
                    need_vortex=False,
                    prepare_binary=fake_bin,
                    timeout=5.0,
                )

            for suite_name, schemas in (("tpch", workloads.TPCH_SCHEMAS), ("ssb", workloads.SSB_SCHEMAS)):
                for tname in schemas:
                    self.assertTrue(table_paths["parquet"][suite_name][tname].is_file())
            self.assertEqual(len(seen_jsonl_paths), len(workloads.TPCH_SCHEMAS) + len(workloads.SSB_SCHEMAS))
            self.assertIn(fixtures_dir / "tpch/.jsonl-tmp/part.jsonl", seen_jsonl_paths)
            self.assertIn(fixtures_dir / "ssb/.jsonl-tmp/part.jsonl", seen_jsonl_paths)
            self.assertEqual(list(fixtures_dir.rglob("*.jsonl")), [])

    def test_ssb_dotted_query_codes_preserve_distinct_repeat_log_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            fake_bin = root / "shardloom"
            fake_bin.write_bytes(b"fake-cli")
            fake_bin.chmod(0o755)
            uat_root = root / "uat"
            fixtures_dir = root / "fixtures"
            pq_dir = fixtures_dir / "ssb" / "parquet"
            vx_dir = fixtures_dir / "ssb" / "vortex"
            pq_dir.mkdir(parents=True)
            vx_dir.mkdir(parents=True)
            for tname in workloads.SSB_SCHEMAS:
                (pq_dir / f"{tname}.parquet").write_bytes(b"PAR1")
                (vx_dir / f"{tname}.vortex").write_bytes(b"VX")
            ssb_rows = workloads.generate_ssb_tables(scale_rows=120, seed=7)
            expected_by_sql = {
                spec.sql: workloads.python_oracle_query(spec, ssb_rows)
                for spec in workloads.SSB_QUERIES
            }

            class FakeWorker:
                start_seconds = 0.01

                def __init__(self, binary: Path, stderr_path: Path):
                    stderr_path.write_text("")

                def run_query(self, command: list[str], stdout_path: Path, timeout: float, guard):
                    assert not stdout_path.exists(), f"duplicate stdout path {stdout_path}"
                    return _fake_run(command, Path(str(stdout_path).removesuffix(".stdout.json")), warm=True)

                def close(self) -> None:
                    pass

            def _fake_schema(rows: list[dict]) -> str:
                sample = rows[0] if rows else {}
                names = list(sample.keys())
                dtypes = [
                    {"Primitive": ["i64", False]}
                    if isinstance(sample[name], int) and not isinstance(sample[name], bool)
                    else ({"Primitive": ["f64", False]} if isinstance(sample[name], float) else {"Utf8": False})
                    for name in names
                ]
                return json.dumps({"Struct": [{"names": names, "dtypes": dtypes}, False]})

            def _fake_run(command: list[str], prefix: Path, *args, warm: bool = False, **kwargs):
                sql = command[command.index("--sql") + 1]
                rows = expected_by_sql[sql]
                envelope = {
                    "status": "success",
                    "fallback": {"attempted": False},
                    "fields": [
                        {"key": "public_workflow_fallback_attempted", "value": "false"},
                        {"key": "public_workflow_external_engine_invoked", "value": "false"},
                        {"key": "query_answer_cache_hit", "value": "false"},
                        {"key": "native_query_timing_reported", "value": "true"},
                        {"key": "native_query_timing_clock", "value": "monotonic_wall_not_cpu"},
                        {"key": "native_query_timing_boundary", "value": "prepared_native_execution_including_io_generation_validation_and_certification_excluding_result_serialization"},
                        {"key": "native_query_execution_nanos", "value": "12000000"},
                        {"key": "native_result_serialization_nanos", "value": "1000000"},
                        {"key": "result_payload_complete", "value": "true"},
                        {"key": "output_row_count", "value": str(len(rows))},
                        {"key": "result_values_json", "value": json.dumps(rows)},
                        {"key": "result_schema_format", "value": "vortex.dtype.serde.v1"},
                        {"key": "result_schema_json", "value": _fake_schema(rows)},
                    ],
                }
                with prefix.with_suffix(".stdout.json").open("xb") as fh:
                    fh.write(json.dumps(envelope).encode("utf-8"))
                if not warm:
                    with prefix.with_suffix(".stderr.txt").open("xb") as fh:
                        fh.write(b"")
                return {
                    "returncode": 0,
                    "seconds": 0.05,
                    "guard_failures": [],
                    "native_peak_rss_bytes": 1024,
                    "system_cpu_seconds": 0.01,
                    "user_cpu_seconds": 0.02,
                    "minor_page_faults": 10,
                    "major_page_faults": 0,
                }

            args = runner.parse_args(
                [
                    "--binary",
                    str(fake_bin),
                    "--uat-root",
                    str(uat_root),
                    "--fixtures-dir",
                    str(fixtures_dir),
                    "--suite",
                    "ssb",
                    "--input-format",
                    "vortex",
                    "--lane",
                    "paired",
                    "--repeats",
                    "2",
                    "--scale-rows",
                    "120",
                    "--seed",
                    "7",
                ]
            )
            with (
                patch.object(runner, "require_local_path", side_effect=lambda p, *a, **k: Path(p).resolve()),
                patch.object(runner, "check_budgets", return_value={"ok": True, "free_bytes": 50 * runner.GIB}),
                patch.object(runner, "run_profiled_command", side_effect=_fake_run),
                patch.object(runner, "PersistentResidentWorker", FakeWorker),
            ):
                summary, summary_path = runner.execute_suite(args)

            self.assertTrue(summary["complete"])
            run_logs_root = summary_path.parent
            for lane_name in ("cold_process_cli", "warm_resident_session"):
                self.assertEqual(summary["lanes"][lane_name]["runs_passed"], 12)
                self.assertTrue((run_logs_root / lane_name / "q09_SSB-Q1_1_run1.stdout.json").is_file())
                self.assertTrue((run_logs_root / lane_name / "q09_SSB-Q1_1_run2.stdout.json").is_file())


if __name__ == "__main__":
    unittest.main()
