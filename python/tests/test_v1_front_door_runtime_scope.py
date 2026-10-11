from __future__ import annotations

import importlib.util
import json
import sys
import tempfile
import textwrap
import unittest
from pathlib import Path
from types import SimpleNamespace


REPO_ROOT = Path(__file__).resolve().parents[2]


def load_scope_module():
    module_path = REPO_ROOT / "scripts" / "check_v1_front_door_runtime_scope.py"
    spec = importlib.util.spec_from_file_location(
        "check_v1_front_door_runtime_scope_for_test",
        module_path,
    )
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    try:
        spec.loader.exec_module(module)
    finally:
        sys.modules.pop(spec.name, None)
    return module


def load_scenario_support_module():
    module_path = REPO_ROOT / "examples" / "local-python-benchmark-scenarios" / "scenario_support.py"
    spec = importlib.util.spec_from_file_location(
        "local_python_benchmark_scenario_support_for_test",
        module_path,
    )
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    try:
        spec.loader.exec_module(module)
    finally:
        sys.modules.pop(spec.name, None)
    return module


class V1FrontDoorRuntimeScopeTests(unittest.TestCase):
    def fake_cli(
        self,
        calls_path: Path | None = None,
        *,
        fail_write: bool = False,
    ) -> list[str]:
        tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(tempdir.cleanup)
        if calls_path is None:
            calls_path = Path(tempdir.name) / "commands.jsonl"
        path = Path(tempdir.name) / "fake_shardloom.py"
        path.write_text(
            textwrap.dedent(
                """
                import json
                import sys


                def envelope(command, status, diagnostics=None):
                    diagnostics = diagnostics or []
                    result_schema = json.dumps({"Struct": [{"names": [], "dtypes": []}, False]})
                    result_fields = [
                        {"key": "result_jsonl", "value": ""},
                        {"key": "result_schema_json", "value": result_schema},
                        {"key": "result_schema_format", "value": "vortex.dtype.serde.v1"},
                        {"key": "output_row_count", "value": "0"},
                        {"key": "result_payload_complete", "value": "true"},
                        {"key": "resident_relational_handle_retained", "value": "true"},
                        {"key": "public_workflow_route_id", "value": "native_vortex_query"},
                        {"key": "public_workflow_execution_mode", "value": "native_vortex"},
                        {"key": "fallback_attempted", "value": "false"},
                        {"key": "external_engine_invoked", "value": "false"},
                    ]
                    if status != "success":
                        result_fields.extend([
                            {"key": "blocker_id", "value": "cg21.workflow.v1_front_door_scope"},
                            {"key": "required_evidence", "value": "v1_front_door_runtime_scope"},
                        ])
                    print(json.dumps({
                        "schema_version": "shardloom.output.v2",
                        "command": command,
                        "status": status,
                        "summary": status,
                        "human_text": status,
                        "fallback": {
                            "attempted": False,
                            "allowed": False,
                            "engine": None,
                            "reason": "disabled",
                        },
                        "diagnostics": diagnostics,
                        "result": {"fields": result_fields},
                        "result_refs": [],
                        "artifacts": [],
                        "artifact_refs": [],
                        "certificates": [],
                        "policy": {"fields": []},
                        "lifecycle": {"fields": []},
                        "capability_snapshot": {"fields": []},
                        "fields": result_fields,
                    }))


                from pathlib import Path
                args = sys.argv[1:]
                calls_path = Path(args[1])
                fail_write = "--simulate-write-failure" in args
                args = [arg for arg in args[2:] if arg != "--simulate-write-failure"]
                with calls_path.open("a", encoding="utf-8") as handle:
                    handle.write(json.dumps(args) + "\\n")
                command = args[0] if args else "missing"
                text = " ".join(args).lower()
                if command == "workflow-unsupported-plan":
                    envelope(
                        command,
                        "unsupported",
                        diagnostics=[{
                            "code": "SL_UNSUPPORTED_SQL",
                            "severity": "error",
                            "category": "unsupported_feature",
                            "message": "unsupported workflow operation",
                            "feature": "workflow_unsupported_plan",
                            "reason": "not in v1 scope",
                            "suggested_next_step": "inspect v1 front-door runtime scope",
                            "fallback": {
                                "attempted": False,
                                "allowed": False,
                                "engine": None,
                                "reason": "disabled",
                            },
                        }],
                    )
                    sys.exit(1)
                if "raw_event_time" in text and "date32" in text:
                    envelope(
                        command,
                        "error",
                        diagnostics=[{
                            "code": "SL_UNSUPPORTED_CAST",
                            "severity": "error",
                            "category": "unsupported_feature",
                            "message": "date32 cast rejected current data",
                            "feature": "cast",
                            "reason": "malformed timestamp",
                            "suggested_next_step": "clean source field before date32 cast",
                            "fallback": {
                                "attempted": False,
                                "allowed": False,
                                "engine": None,
                                "reason": "disabled",
                            },
                        }],
                    )
                    sys.exit(1)
                if fail_write and "--request" in args and args[args.index("--request") + 1] == "write_csv":
                    envelope(
                        command,
                        "unsupported",
                        diagnostics=[{
                            "code": "SL_UNSUPPORTED_SINK",
                            "severity": "error",
                            "category": "unsupported_feature",
                            "message": "CSV sink rejected by transport fixture",
                            "feature": "csv_sink",
                            "reason": "simulated transport failure",
                            "suggested_next_step": "inspect sink admission",
                            "fallback": {
                                "attempted": False,
                                "allowed": False,
                                "engine": None,
                                "reason": "disabled",
                            },
                        }],
                    )
                    sys.exit(1)
                envelope(command, "success")
                """
            ),
            encoding="utf-8",
        )
        command = [sys.executable, str(path), "--calls-path", str(calls_path)]
        if fail_write:
            command.append("--simulate-write-failure")
        return command

    def test_scope_validator_passes_current_repo_contract(self) -> None:
        module = load_scope_module()

        report = module.build_report(REPO_ROOT)

        self.assertEqual(report["status"], "passed", report["blockers"])
        self.assertEqual(
            report["schema_version"],
            "shardloom.v1_front_door_runtime_scope_report.v1",
        )
        self.assertTrue(report["scoped_local_front_door_parity_supported"])
        self.assertTrue(report["all_no_fallback_no_external_engine"])
        self.assertFalse(report["performance_equivalence_claim_allowed"])
        self.assertEqual(
            {row["front_door_id"] for row in report["public_front_door_rows"]},
            module.EXPECTED_PUBLIC_FRONT_DOOR_IDS,
        )
        self.assertTrue(
            all(
                row["owning_route_id"] == "native_vortex_query"
                and row["execution_mode"] == "native_vortex"
                for row in report["public_front_door_rows"]
            )
        )
        self.assertTrue(
            all("route_runtime_status" not in row for row in report["public_front_door_rows"])
        )
        self.assertIn("selective_filter", report["example_scenario_ids"])
        self.assertEqual(report["expected_error_scenario_ids"], [])

    def test_route_validator_checks_shared_native_owner_and_exact_front_doors(self) -> None:
        module = load_scope_module()

        def route_row(front_door_id: str, **overrides: object) -> SimpleNamespace:
            values: dict[str, object] = {
                "front_door_id": front_door_id,
                "owning_route_id": "native_vortex_query",
                "input_family": "declared_input",
                "public_user_surface": "public entrypoint",
                "vortex_normalization_point": "native Vortex admission",
                "execution_mode": "native_vortex",
                "output_route": "typed result or declared sink",
                "required_evidence": (),
                "fallback_attempted": False,
                "external_engine_invoked": False,
                "claim_boundary": "scoped workflow",
            }
            values.update(overrides)
            return SimpleNamespace(**values)

        ids = sorted(module.EXPECTED_PUBLIC_FRONT_DOOR_IDS)
        report = SimpleNamespace(
            public_front_door_route_rows=tuple(route_row(item) for item in ids),
            all_no_fallback_no_external_engine=True,
        )
        rows, blockers = module.validate_route_report(report)
        self.assertEqual(blockers, [])
        self.assertEqual({row["front_door_id"] for row in rows}, module.EXPECTED_PUBLIC_FRONT_DOOR_IDS)

        invalid = route_row(
            ids[0],
            owning_route_id="local_source_runtime",
            execution_mode="compatibility_fallback",
            fallback_attempted=True,
        )
        report = SimpleNamespace(
            public_front_door_route_rows=(invalid, *(route_row(item) for item in ids[1:])),
            all_no_fallback_no_external_engine=True,
        )
        _, blockers = module.validate_route_report(report)
        self.assertTrue(any("shared native route" in blocker for blocker in blockers), blockers)
        self.assertTrue(any("execution_mode must be native_vortex" in blocker for blocker in blockers), blockers)
        self.assertTrue(any("fallback_attempted must be false" in blocker for blocker in blockers), blockers)

    def test_scenarios_send_sequential_public_sql_requests(self) -> None:
        # This fake-CLI test checks request construction only, not SQL semantics.
        scenario_support = load_scenario_support_module()
        with tempfile.TemporaryDirectory() as tempdir:
            run_dir = Path(tempdir) / "run"
            calls_path = Path(tempdir) / "commands.jsonl"
            original_cwd = Path.cwd()

            payload = scenario_support.run_scenarios(
                memory_gb=4, max_parallelism=2,
                repo_root=REPO_ROOT,
                run_dir=run_dir,
                binary=self.fake_cli(calls_path),
                profile_order=("release", "debug"),
            )
            command_lines = calls_path.read_text(encoding="utf-8").splitlines()
            self.assertEqual(Path.cwd(), original_cwd)

        self.assertTrue(payload["passed"], json.dumps(payload, indent=2, sort_keys=True))
        self.assertEqual(payload["scenario_count"], 9)
        by_name = {row["name"]: row for row in payload["results"]}
        self.assertEqual(
            set(by_name),
            {
                "selective_filter",
                "filter_projection_limit",
                "group_by_aggregation",
                "hash_join",
                "global_top_n",
                "clean_cast_filter_write",
                "malformed_timestamp_cast",
                "null_heavy_aggregate",
                "nested_json_field_scan",
            },
        )
        self.assertFalse(by_name["malformed_timestamp_cast"]["expected_error"])
        for result in payload["results"]:
            self.assertFalse(result["fallback_attempted"], result)
            self.assertFalse(result["external_engine_invoked"], result)
            self.assertIn("python_wall_millis", result["timing_components"])

        commands = [json.loads(line) for line in command_lines]
        run_commands = [args for args in commands if args and args[0] == "run"]
        self.assertEqual(len(run_commands), 10, commands)
        self.assertEqual([args[:2] for args in run_commands], [["run", "sql"]] * 10)
        self.assertEqual({args[0] for args in commands}, {"run", "python-worker"})
        requests = [args[args.index("--request") + 1] for args in run_commands]
        self.assertEqual(
            requests,
            ["collect"] * 5 + ["write_csv", "collect"] + ["collect"] * 3,
        )
        fact_sql = scenario_support.quote_sql_path(run_dir / "data" / "fact.csv")
        dim_sql = scenario_support.quote_sql_path(run_dir / "data" / "dim.csv")
        workloads = scenario_support.load_workload_declarations(REPO_ROOT)
        expected_requests = []
        for _, workload_name, _ in scenario_support.SCENARIO_ROUTES:
            statements, write_statement = workloads[workload_name].bind(
                {"fact": fact_sql, "dim": dim_sql}
            )
            if write_statement is not None:
                expected_requests.append(("write_csv", write_statement))
            expected_requests.extend(("collect", statement) for statement in statements)
        actual_requests = [
            (args[args.index("--request") + 1], args[args.index("--sql") + 1])
            for args in run_commands
        ]
        self.assertEqual(actual_requests, expected_requests)
        clean_write = by_name["clean_cast_filter_write"]["write_result"]
        self.assertEqual(clean_write["status"], "success")
        self.assertTrue(clean_write["ok"])

        with tempfile.TemporaryDirectory() as tempdir:
            failed_run_dir = Path(tempdir) / "failed-run"
            failed_calls_path = Path(tempdir) / "commands.jsonl"
            failed = scenario_support.run_scenarios(
                memory_gb=4, max_parallelism=2,
                repo_root=REPO_ROOT,
                run_dir=failed_run_dir,
                binary=self.fake_cli(failed_calls_path, fail_write=True),
                profile_order=("release", "debug"),
            )
            failed_commands = [
                json.loads(line)
                for line in failed_calls_path.read_text(encoding="utf-8").splitlines()
            ]
        self.assertFalse(failed["passed"])
        failed_clean = next(
            row for row in failed["results"] if row["name"] == "clean_cast_filter_write"
        )
        self.assertFalse(failed_clean["ok"])
        self.assertEqual(failed_clean["write_result"]["status"], "unsupported")
        failed_collect_sql = [
            args[args.index("--sql") + 1]
            for args in failed_commands
            if "--sql" in args and args[args.index("--request") + 1] == "collect"
        ]
        self.assertFalse(
            any("SUM(clean_numeric)" in statement for statement in failed_collect_sql),
            failed_collect_sql,
        )

        with tempfile.TemporaryDirectory() as tempdir:
            existing_run_dir = Path(tempdir) / "existing-run"
            existing_run_dir.mkdir()
            marker = existing_run_dir / "keep.txt"
            marker.write_text("preserve", encoding="utf-8")
            with self.assertRaises(FileExistsError):
                scenario_support.run_scenarios(
                    memory_gb=4, max_parallelism=2,
                    repo_root=REPO_ROOT,
                    run_dir=existing_run_dir,
                    binary=self.fake_cli(),
                )
            self.assertEqual(marker.read_text(encoding="utf-8"), "preserve")

    def test_unsupported_front_door_shapes_return_no_fallback_reports(self) -> None:
        source_path = str(REPO_ROOT / "python" / "src")
        if source_path not in sys.path:
            sys.path.insert(0, source_path)
        from shardloom import context
        from shardloom import UnsupportedWorkflowOperationReport

        ctx = context(repo_root=REPO_ROOT, binary=self.fake_cli())
        frame = ctx.read_csv("events.csv", schema={"id": "int64"})
        unsupported_reports = [
            frame.sql("SELECT * FROM remote_table"),
            frame.query("id > @threshold", threshold=10),
            frame.merge("remote.parquet", on="id", how="outer", indicator=True),
        ]

        for report in unsupported_reports:
            self.assertIsInstance(report, UnsupportedWorkflowOperationReport)
            self.assertFalse(report.runtime_execution)
            self.assertFalse(report.data_read)
            self.assertFalse(report.write_io)
            self.assertFalse(report.fallback_attempted)
            self.assertFalse(report.external_engine_invoked)
            self.assertIsNotNone(report.blocker_id)


if __name__ == "__main__":
    unittest.main()
