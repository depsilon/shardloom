# SPDX-License-Identifier: Apache-2.0
"""Contract tests for the public declarations used by local golden workflows."""

from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))

import check_golden_workflows as golden  # noqa: E402


class NativeGoldenWorkflowDeclarationTests(unittest.TestCase):
    def test_csv_fanout_uses_public_sql_and_checks_complete_files(self):
        expected_jsonl = [
            {"id": 2, "label": "beta", "amount": 15},
            {"id": 3, "label": "gamma", "amount": 21},
        ]

        for bad_format in (None, "jsonl", "csv"):
            with self.subTest(bad_format=bad_format), tempfile.TemporaryDirectory() as raw:
                root = Path(raw)
                run_dir, stage_dir = root / "run", root / "stages"
                run_dir.mkdir()
                stage_dir.mkdir()
                calls = []

                def fake_stage(**kwargs):
                    calls.append(kwargs)
                    stage_id = kwargs["stage_id"]
                    for artifact in kwargs.get("artifact_paths", ()):
                        if artifact.suffix == ".jsonl":
                            rows = expected_jsonl
                            if bad_format == "jsonl":
                                rows = expected_jsonl[:1]
                            artifact.write_text(
                                "".join(json.dumps(row) + "\n" for row in rows),
                                encoding="utf-8",
                            )
                        elif artifact.suffix == ".csv":
                            content = "id,label,amount\n2,beta,15\n3,gamma,21\n"
                            if bad_format == "csv":
                                content = "id,label,amount\n2,beta,15\n"
                            artifact.write_text(content, encoding="utf-8")
                        else:
                            artifact.write_bytes(b"fixture")
                    return {"stage_id": stage_id, "blockers": []}

                with (
                    patch.object(golden, "run_cli_stage", side_effect=fake_stage),
                    patch.object(
                        golden,
                        "run_python_wrapper_stage",
                        return_value={"stage_id": "wrapper", "blockers": []},
                    ),
                ):
                    report = golden.workflow_local_csv_to_prepared_and_fanout(
                        repo_root=root,
                        binary=root / "shardloom",
                        run_dir=run_dir,
                        stage_dir=stage_dir,
                        local_source=run_dir / "orders.csv",
                        wrapper_source=run_dir / "wrapper.csv",
                    )

                fanout = next(
                    call for call in calls
                    if call["stage_id"] == "local_prepared_vortex_sql_jsonl_csv_fanout"
                )
                args = fanout["args"]
                self.assertEqual(args[:2], ["run", "dataframe"])
                self.assertIn("--input-format", args)
                self.assertIn("vortex", args)
                self.assertEqual(args[args.index("--materialization-policy") + 1], "bounded")
                self.assertEqual(args[args.index("--memory-gb") + 1], "1")
                self.assertEqual(args[args.index("--max-parallelism") + 1], "1")
                for retired in (
                    "--plan", "--vortex-primitive", "--native-vortex-operation-family",
                    "--evidence-level", "zero_decode",
                ):
                    self.assertNotIn(retired, args)
                self.assertEqual(report["status"], "failed" if bad_format else "passed")
                if bad_format:
                    self.assertTrue(any("rows differ" in item for item in report["blockers"]))
                else:
                    fields = fanout["expected_fields"]
                    self.assertEqual(fields["native_vortex_result_export_all_targets_committed"], "true")
                    self.assertEqual(fields["native_vortex_result_export_target_count"], "2")
                    self.assertEqual(fields["native_vortex_result_export_target_formats"], "jsonl,csv")

    def test_source_free_values_and_replay_are_public_sql(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            run_dir, stage_dir = root / "run", root / "stages"
            run_dir.mkdir()
            stage_dir.mkdir()
            calls = []

            def fake_stage(**kwargs):
                calls.append(kwargs)
                for artifact in kwargs.get("artifact_paths", ()):
                    artifact.write_bytes(b"vortex fixture")
                return {"stage_id": kwargs["stage_id"], "blockers": []}

            with patch.object(golden, "run_cli_stage", side_effect=fake_stage):
                report = golden.workflow_source_free_sql_to_vortex(
                    repo_root=root,
                    binary=root / "shardloom",
                    run_dir=run_dir,
                    stage_dir=stage_dir,
                )

            self.assertEqual(report["status"], "passed")
            write_args, replay_args = (call["args"] for call in calls)
            self.assertEqual(write_args[:2], ["run", "sql"])
            self.assertIn("--request", write_args)
            self.assertEqual(write_args[write_args.index("--request") + 1], "write_vortex")
            values_sql = write_args[write_args.index("--sql") + 1]
            self.assertIn("FROM (VALUES (1,'alpha',1.5),(2,'beta',2.25),(3,'gamma',4.5))", values_sql)
            self.assertEqual(
                calls[0]["expected_fields"]["native_vortex_result_export_target_commit_statuses"],
                "primary:vortex:committed",
            )
            self.assertEqual(
                calls[0]["expected_fields"]["public_workflow_output_ref"],
                str(run_dir / "source-free-sql-output.vortex"),
            )
            self.assertEqual(replay_args[:2], ["run", "sql"])
            self.assertIn("SELECT id,label,score", replay_args[replay_args.index("--sql") + 1])
            self.assertEqual(calls[1]["expected_rows"], [
                {"id": 1, "label": "alpha", "score": 1.5},
                {"id": 2, "label": "beta", "score": 2.25},
                {"id": 3, "label": "gamma", "score": 4.5},
            ])
            self.assertNotIn("generated-source-user-rows", " ".join(write_args))
            self.assertNotIn("--execute-local-primitive", " ".join(replay_args))

    def test_run_cli_stage_rejects_truncated_and_altered_native_results(self):
        result_schema = {
            "Struct": [{
                "names": ["label"],
                "dtypes": [{"Utf8": False}],
            }, False],
        }
        expected = [{"label": "beta"}, {"label": "gamma"}]

        def envelope(rows):
            fields = [
                {"key": "result_schema_json", "value": json.dumps(result_schema)},
                {"key": "result_schema_format", "value": "vortex.dtype.serde.v1"},
                {"key": "result_values_json", "value": json.dumps(rows)},
                {"key": "result_payload_complete", "value": "true"},
                {"key": "output_row_count", "value": "2"},
                {"key": "public_workflow_requested_output", "value": "collect"},
                {"key": "public_workflow_fallback_attempted", "value": "false"},
                {"key": "public_workflow_external_engine_invoked", "value": "false"},
            ]
            return {
                "status": "success",
                "fallback": {"attempted": False, "allowed": False},
                "fields": fields,
            }

        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            stage_dir = root / "stages"
            stage_dir.mkdir()
            for rows, expected_status in (
                (expected, "passed"),
                ([{"label": "beta"}], "failed"),
                ([{"label": "beta"}, {"label": "delta"}], "failed"),
            ):
                with self.subTest(rows=rows), patch.object(
                    golden,
                    "run_subprocess",
                    return_value=subprocess.CompletedProcess(
                        args=["shardloom"],
                        returncode=0,
                        stdout=json.dumps(envelope(rows)),
                        stderr="",
                    ),
                ):
                    report = golden.run_cli_stage(
                        repo_root=root,
                        binary=root / "shardloom",
                        stage_dir=stage_dir,
                        stage_id="replay",
                        args=["run", "sql"],
                        expected_fields={"public_workflow_requested_output": "collect"},
                        expected_rows=expected,
                    )
                    self.assertEqual(report["status"], expected_status)


if __name__ == "__main__":
    unittest.main()
