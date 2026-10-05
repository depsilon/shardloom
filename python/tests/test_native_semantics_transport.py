from __future__ import annotations

import importlib.util
import copy
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest import mock


REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPTS = REPO_ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))


def load_checker():
    path = SCRIPTS / "check_admitted_semantics_matrix.py"
    spec = importlib.util.spec_from_file_location("admitted_semantics_transport_test", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    try:
        spec.loader.exec_module(module)
    finally:
        sys.modules.pop(spec.name, None)
    return module


def schema(names: list[str], dtypes: list[dict]) -> str:
    return json.dumps({"Struct": [{"names": names, "dtypes": dtypes}, False]})


def workflow_envelope(fields: list[dict], *, status: str = "success", diagnostics=()):
    return {
        "schema_version": "shardloom.output.v2",
        "command": "run",
        "status": status,
        "summary": "native workflow response",
        "human_text": "native workflow response",
        "fallback": {"attempted": False, "allowed": False, "engine": None},
        "diagnostics": list(diagnostics),
        "fields": [
            {
                "key": "public_workflow_fallback_attempted",
                "value": False,
            },
            {
                "key": "public_workflow_external_engine_invoked",
                "value": False,
            },
            *copy.deepcopy(fields),
        ],
    }


class NativeSemanticsTransportTests(unittest.TestCase):
    def setUp(self) -> None:
        self.checker = load_checker()
        self.binary = Path("/fixture/bin/shardloom")

    def typed_result_fields(self, rows: list[dict], *, row_count: int | None = None) -> list[dict]:
        return [
            {"key": "result_jsonl", "value": "".join(json.dumps(row) + "\n" for row in rows)},
            {"key": "result_payload_complete", "value": True},
            {"key": "output_row_count", "value": str(len(rows) if row_count is None else row_count)},
            {
                "key": "result_schema_json",
                "value": schema(
                    ["id", "score", "label"],
                    [
                        {"Primitive": ["i64", False]},
                        {"Primitive": ["f64", True]},
                        {"Utf8": True},
                    ],
                ),
            },
            {"key": "result_schema_format", "value": "vortex.dtype.serde.v1"},
            {"key": "fallback_attempted", "value": "false"},
            {"key": "external_engine_invoked", "value": "false"},
        ]

    def completed(self, command: list[str], payload: dict, returncode: int = 0):
        return subprocess.CompletedProcess(
            command,
            returncode,
            stdout=json.dumps(payload),
            stderr="",
        )

    def test_collect_uses_complete_typed_result_and_binds_auxiliary_csv(self) -> None:
        expected = [{"id": 1, "score": 2.5, "label": None}]
        case = self.checker.SqlFixtureCase(
            case_id="transport_auxiliary_collect",
            source_name="fact.csv",
            source_text="id,score,label\n1,2.5,\n",
            statement_template=(
                "SELECT f.id,f.score,d.label FROM '{source}' AS f "
                "JOIN '{dim}' AS d ON f.id = d.id"
            ),
            expected_jsonl='{"id":1,"score":2.5,"label":null}\n',
            auxiliary_sources=(("dim", "dim.csv", "id,label\n1,\n"),),
            property_seed=20260619,
        )
        payload = workflow_envelope(self.typed_result_fields(expected))

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            work_dir = root / "work"
            with mock.patch.object(
                self.checker,
                "run_subprocess",
                side_effect=lambda **kwargs: self.completed(kwargs["command"], payload),
            ) as run:
                stage = self.checker.run_executable_case(
                    repo_root=root,
                    binary=self.binary,
                    work_dir=work_dir,
                    case=case,
                    matrix_row={
                        "support_state": "property_executed",
                        "property_seed": 20260619,
                        "decoded_reference_kind": "jsonl_inline_reference",
                    },
                )

        command = run.call_args.kwargs["command"]
        self.assertEqual(command[:3], [str(self.binary), "run", "sql"])
        self.assertEqual(command.count("--format"), 1)
        self.assertEqual(command[command.index("--request") + 1], "collect")
        self.assertEqual(command[command.index("--bounded") + 1], "true")
        bindings = json.loads(command[command.index("--source-bindings") + 1])
        self.assertEqual(len(bindings), 2)
        self.assertTrue(all(value == {"input_format": "csv"} for value in bindings.values()))
        statement = command[command.index("--sql") + 1]
        self.assertIn(next(path for path in bindings if "fact.csv" in path), statement)
        self.assertIn(next(path for path in bindings if "dim.csv" in path), statement)
        self.assertEqual(stage["kind"], "sql_native_decoded_reference")
        self.assertEqual(stage["status"], "passed", stage["blockers"])
        self.assertEqual(stage["expected_output_digest_source"], "canonical_decoded_reference_rows")
        self.assertEqual(stage["observed_output_digest_source"], "complete_native_result_rows")
        self.assertNotIn("correctness_digest", stage)
        self.assertNotIn("result_digest", stage)
        self.assertNotIn("sql_statement_kind", stage["selected_fields"])

    def test_collect_rejects_truncated_corrupt_and_unsafe_results(self) -> None:
        expected = [{"id": 1, "score": 2.5, "label": None}]
        case = self.checker.SqlFixtureCase(
            case_id="transport_invalid_result",
            source_name="fact.csv",
            source_text="id,score,label\n1,2.5,\n",
            statement_template="SELECT * FROM '{source}'",
            expected_jsonl='{"id":1,"score":2.5,"label":null}\n',
        )
        base_fields = self.typed_result_fields(expected)
        truncated = workflow_envelope(
            [{**base_fields[0]}, *base_fields[1:]],
        )
        truncated["fields"][4]["value"] = "2"
        corrupt = workflow_envelope(base_fields)
        next(row for row in corrupt["fields"] if row["key"] == "result_schema_json")["value"] = "not json"
        unsafe = workflow_envelope(base_fields)
        next(row for row in unsafe["fields"] if row["key"] == "public_workflow_external_engine_invoked")["value"] = True

        for payload, failure in ((truncated, "truncated"), (corrupt, "complete native result is invalid"),
                                 (unsafe, "invalid public workflow evidence")):
            with self.subTest(payload=payload):
                with tempfile.TemporaryDirectory() as directory:
                    root = Path(directory).resolve()
                    with mock.patch.object(
                        self.checker,
                        "run_subprocess",
                        side_effect=lambda **kwargs: self.completed(kwargs["command"], payload),
                    ):
                        stage = self.checker.run_executable_case(
                            repo_root=root,
                            binary=self.binary,
                            work_dir=root / "work",
                            case=case,
                            matrix_row={
                                "support_state": "executable",
                                "decoded_reference_kind": "jsonl_inline_reference",
                            },
                        )
                self.assertEqual(stage["status"], "failed")
                self.assertTrue(any(failure in blocker for blocker in stage["blockers"]), stage["blockers"])

    def test_writer_requires_commit_evidence_and_exact_complete_output_bytes(self) -> None:
        case = self.checker.SqlFixtureCase(
            case_id="transport_csv_writer",
            source_name="fact.csv",
            source_text="id,label\n1,alpha\n",
            statement_template="SELECT id,label FROM '{source}'",
            expected_jsonl='{"id":1,"label":"alpha"}\n',
            output_format="csv",
            output_name="result.csv",
            expected_output_text="id,label\n1,alpha\n",
        )
        fields = [
            {"key": "native_vortex_result_export_path", "value": ""},
            {"key": "native_vortex_result_export_format", "value": "csv"},
            {"key": "public_workflow_requested_output", "value": "write_csv"},
            {"key": "native_vortex_result_export_rows_written", "value": "1"},
            {"key": "native_vortex_result_export_all_targets_committed", "value": "true"},
            {"key": "fallback_attempted", "value": "false"},
            {"key": "external_engine_invoked", "value": "false"},
        ]
        payload = workflow_envelope(fields)

        def run_writer(**kwargs):
            command = kwargs["command"]
            output = Path(command[command.index("--output") + 1])
            output.write_bytes(case.expected_output_text.encode("utf-8"))
            next(field for field in payload["fields"] if field["key"] == "native_vortex_result_export_path")["value"] = str(output)
            return self.completed(command, payload)

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            with mock.patch.object(self.checker, "run_subprocess", side_effect=run_writer) as run:
                stage = self.checker.run_executable_case(
                    repo_root=root,
                    binary=self.binary,
                    work_dir=root / "work",
                    case=case,
                    matrix_row={
                        "support_state": "fuzz_executed",
                        "fuzz_seed": 20260617,
                        "fuzz_surface": "output_writer_policy",
                        "decoded_reference_kind": "jsonl_inline_reference",
                    },
                )

        command = run.call_args.kwargs["command"]
        self.assertEqual(command[command.index("--request") + 1], "write_csv")
        output = Path(command[command.index("--output") + 1])
        self.assertEqual(output.relative_to(root).as_posix(), stage["output_ref"])
        self.assertEqual(stage["status"], "passed", stage["blockers"])
        self.assertEqual(stage["expected_output_digest_source"], "decoded_reference_output_artifact")
        self.assertEqual(stage["observed_output_digest_source"], "sink_output_artifact")
        self.assertNotIn("result_payload_complete", stage["selected_fields"])

    def test_writer_rejects_missing_commit_evidence_and_changed_bytes(self) -> None:
        case = self.checker.SqlFixtureCase(
            case_id="transport_writer_rejection",
            source_name="fact.csv",
            source_text="id,label\n1,alpha\n",
            statement_template="SELECT id,label FROM '{source}'",
            expected_jsonl='{"id":1,"label":"alpha"}\n',
            output_format="csv",
            expected_output_text="id,label\n1,alpha\n",
        )
        cases = (
            ("missing_commit", "id,label\n1,alpha\n", False),
            ("changed_bytes", "id,label\n1,beta\n", True),
            ("invalid_boolean", "id,label\n1,alpha\n", 1),
            ("wrong_path", "id,label\n1,alpha\n", True),
            ("wrong_format", "id,label\n1,alpha\n", True),
            ("wrong_count", "id,label\n1,alpha\n", True),
        )
        for case_id, output_text, committed in cases:
            with self.subTest(case_id=case_id), tempfile.TemporaryDirectory() as directory:
                root = Path(directory).resolve()
                fields = [
                    {
                        "key": "native_vortex_result_export_all_targets_committed",
                        "value": committed,
                    },
                    {"key": "public_workflow_requested_output", "value": "write_csv"},
                    {"key": "native_vortex_result_export_format", "value": "csv"},
                    {"key": "native_vortex_result_export_rows_written", "value": "1"},
                    {"key": "fallback_attempted", "value": "false"},
                    {"key": "external_engine_invoked", "value": "false"},
                ]
                if not committed:
                    fields = [row for row in fields if row["key"] != "native_vortex_result_export_all_targets_committed"]
                payload = workflow_envelope(fields)

                def run_writer(**kwargs):
                    command = kwargs["command"]
                    Path(command[command.index("--output") + 1]).write_bytes(
                        output_text.encode("utf-8")
                    )
                    payload["fields"].append({
                        "key": "native_vortex_result_export_path",
                        "value": command[command.index("--output") + 1],
                    })
                    mutation = {
                        "wrong_path": ("native_vortex_result_export_path", "different.csv"),
                        "wrong_format": ("native_vortex_result_export_format", "vortex"),
                        "wrong_count": ("native_vortex_result_export_rows_written", "2"),
                    }.get(case_id)
                    if mutation:
                        next(field for field in payload["fields"] if field["key"] == mutation[0])["value"] = mutation[1]
                    return self.completed(command, payload)

                with mock.patch.object(
                    self.checker,
                    "run_subprocess",
                    side_effect=run_writer,
                ):
                    stage = self.checker.run_executable_case(
                        repo_root=root,
                        binary=self.binary,
                        work_dir=root / "work",
                        case=case,
                        matrix_row={
                            "support_state": "executable",
                            "decoded_reference_kind": "jsonl_inline_reference",
                        },
                    )
                self.assertEqual(stage["status"], "failed")
                if case_id in ("missing_commit", "invalid_boolean"):
                    self.assertTrue(any("all targets committed" in item for item in stage["blockers"]))
                elif case_id == "changed_bytes":
                    self.assertTrue(any("does not match decoded reference" in item for item in stage["blockers"]))
                else:
                    key = {"wrong_path": "path", "wrong_format": "format", "wrong_count": "rows_written"}[case_id]
                    self.assertTrue(any(f"native_vortex_result_export_{key}=" in item for item in stage["blockers"]))

    def test_diagnostic_writer_preserves_existing_bytes_without_overwrite(self) -> None:
        case = self.checker.UnsupportedCase(
            case_id="transport_no_overwrite",
            source_name="fact.csv",
            source_text="id,label\n1,alpha\n",
            statement_template="SELECT * FROM '{source}'",
            diagnostic_code="SL_INVALID_INPUT",
            diagnostic_fragment="output target already exists and overwrite is disabled",
            output_format="csv",
            output_name="existing.csv",
            allow_overwrite=False,
            preexisting_output_text="old,bytes\n",
        )
        message = case.diagnostic_fragment
        for result_status in ("error", "unsupported"):
            with self.subTest(result_status=result_status), tempfile.TemporaryDirectory() as directory:
                root = Path(directory).resolve()
                payload = workflow_envelope(
                    [
                        {"key": "fallback_attempted", "value": False},
                        {"key": "external_engine_invoked", "value": False},
                    ],
                    status=result_status,
                    diagnostics=[{"code": case.diagnostic_code, "message": message}],
                )
                payload["human_text"] = message + " external_engine_invoked=false"
                with mock.patch.object(
                    self.checker,
                    "run_subprocess",
                    side_effect=lambda **kwargs: self.completed(kwargs["command"], payload, 2),
                ) as run:
                    stage = self.checker.run_unsupported_case(
                        repo_root=root,
                        binary=self.binary,
                        work_dir=root / "work",
                        case=case,
                        matrix_row={
                            "support_state": case.support_state,
                            "oracle_boundary": case.oracle_boundary,
                            "unsupported_diagnostic_code": case.diagnostic_code,
                            "unsupported_diagnostic_message": message,
                        },
                    )
                    output = root / "work" / case.case_id / case.output_name
                    self.assertEqual(output.read_bytes(), case.preexisting_output_text.encode("utf-8"))

                command = run.call_args.kwargs["command"]
                self.assertEqual(command[:3], [str(self.binary), "run", "sql"])
                self.assertEqual(command[command.index("--request") + 1], "write_csv")
                self.assertNotIn("--allow-overwrite", command)
                self.assertEqual(command.count("--format"), 1)
                self.assertEqual(stage["status"], "passed", stage["blockers"])
                self.assertEqual(stage["diagnostic_code"], case.diagnostic_code)
                self.assertEqual(stage["diagnostic_fragment"], case.diagnostic_fragment)


if __name__ == "__main__":
    unittest.main()
