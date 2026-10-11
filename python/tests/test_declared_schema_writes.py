from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import shardloom as sl
from shardloom.client import PublicWorkflowExecution, ShardLoomClient, VortexIngestSmokeReport
from shardloom.models import OutputEnvelope
from shardloom.query import VortexWorkflowExecutionReport, UnsupportedWorkflowOperationReport


class _CapturingClient:
    def __init__(
        self,
        *,
        prepare_status: str = "success",
        run_status: str = "success",
    ) -> None:
        self.prepare_status = prepare_status
        self.run_status = run_status
        self.prepare_calls: list[dict[str, Any]] = []
        self.run_calls: list[dict[str, Any]] = []

    def vortex_prepare(self, *args: object, **kwargs: object) -> VortexIngestSmokeReport:
        self.prepare_calls.append({"args": args, "kwargs": kwargs})
        envelope = OutputEnvelope.from_field_mapping(
            {},
            command="vortex-prepare",
            status=self.prepare_status,
            summary="mocked preparation",
        )
        return VortexIngestSmokeReport(envelope)

    def public_workflow_run(
        self,
        surface: str,
        **kwargs: object,
    ) -> PublicWorkflowExecution:
        self.run_calls.append({"surface": surface, **kwargs})
        envelope = OutputEnvelope.from_field_mapping(
            {}, command="run", status=self.run_status, summary="mocked public workflow run"
        )
        return PublicWorkflowExecution(envelope)


class DeclaredSchemaWriteTests(unittest.TestCase):
    output_formats = (
        "vortex",
        "parquet",
        "arrow-ipc",
        "avro",
        "orc",
        "json",
        "jsonl",
        "csv",
    )

    def _source(self, directory: str, client: _CapturingClient) -> tuple[Path, Any]:
        source = Path(directory) / "labels.csv"
        source.write_text("label\nalpha\nbeta\n", encoding="utf-8")
        return source, sl.read_csv(source, schema={"label": "utf8"}, client=client)

    def _assert_write_forwards_declared_schema(
        self,
        frame: Any,
        source: Path,
        client: _CapturingClient,
        output_format: str,
        output_path: Path,
    ) -> None:
        report = frame.write(output_path, output_format=output_format, check=False, memory_gb=4, max_parallelism=2)

        self.assertIsInstance(report, VortexWorkflowExecutionReport)
        self.assertNotIsInstance(report, UnsupportedWorkflowOperationReport)
        self.assertEqual(client.prepare_calls, [])
        self.assertEqual(len(client.run_calls), 1)
        run = client.run_calls[0]
        self.assertEqual(run["input_uri"], str(source))
        self.assertEqual(run["input_format"], "csv")
        self.assertEqual(run["source_schema"], (("label", "utf8"),))
        self.assertIn(str(source), run["sql_statement"])

    def test_projection_write_forwards_declared_csv_schema_for_all_sinks(self) -> None:
        for output_format in self.output_formats:
            with self.subTest(output_format=output_format), tempfile.TemporaryDirectory() as directory:
                client = _CapturingClient()
                source, frame = self._source(directory, client)
                self._assert_write_forwards_declared_schema(
                    frame.select("label").limit(2),
                    source,
                    client,
                    output_format,
                    Path(directory) / f"projection.{output_format}",
                )

    def test_scalar_min_write_forwards_declared_csv_schema_for_all_sinks(self) -> None:
        for output_format in self.output_formats:
            with self.subTest(output_format=output_format), tempfile.TemporaryDirectory() as directory:
                client = _CapturingClient()
                source, frame = self._source(directory, client)
                self._assert_write_forwards_declared_schema(
                    frame.aggregate("MIN(label)").limit(1),
                    source,
                    client,
                    output_format,
                    Path(directory) / f"minimum.{output_format}",
                )

    def test_fanout_forwards_secondary_sinks_with_declared_schema(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            client = _CapturingClient()
            source, frame = self._source(directory, client)
            primary = Path(directory) / "primary.parquet"
            secondary = Path(directory) / "secondary.csv"

            report = frame.select("label").limit(2).fanout(
                {"parquet": primary, "csv": secondary}, check=False, memory_gb=4, max_parallelism=2
            )

            self.assertIsInstance(report, VortexWorkflowExecutionReport)
            self.assertNotIsInstance(report, UnsupportedWorkflowOperationReport)
            self.assertEqual(client.prepare_calls, [])
            self.assertEqual(len(client.run_calls), 1)
            self.assertEqual(client.run_calls[0]["fanout_outputs"], (("csv", str(secondary)),))
            self.assertEqual(client.run_calls[0]["input_format"], "csv")
            self.assertEqual(client.run_calls[0]["input_uri"], str(source))
            self.assertEqual(client.run_calls[0]["source_schema"], (("label", "utf8"),))
            self.assertIn(str(source), client.run_calls[0]["sql_statement"])

    def test_failed_single_public_workflow_run_propagates_with_check_false(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            client = _CapturingClient(run_status="error")
            _, frame = self._source(directory, client)

            report = frame.select("label").limit(2).write(
                Path(directory) / "output.parquet",
                output_format="parquet",
                check=False,
                memory_gb=4,
                max_parallelism=2,
            )

            self.assertIsInstance(report, VortexWorkflowExecutionReport)
            self.assertNotIsInstance(report, UnsupportedWorkflowOperationReport)
            self.assertEqual(report.status, "error")
            self.assertEqual(client.prepare_calls, [])
            self.assertEqual(len(client.run_calls), 1)

    def test_public_workflow_run_serializes_source_schema(self) -> None:
        client = ShardLoomClient(binary="unused", memory_gb=4, max_parallelism=2)
        captured: dict[str, Any] = {}

        def capture_run(args: list[str], *, check: bool = True) -> OutputEnvelope:
            captured["args"] = args
            captured["check"] = check
            return OutputEnvelope.from_field_mapping({}, command="run")

        client.run = capture_run  # type: ignore[method-assign]
        client.public_workflow_run(
            "dataframe",
            input_uri="source.csv",
            input_format="csv",
            source_schema={"label": "utf8"},
            requested_output="write_parquet",
            output_ref="out.parquet",
        )
        args = captured["args"]
        self.assertIn("--source-schema", args)
        self.assertEqual(args[args.index("--source-schema") + 1], "label:utf8")

    def test_public_workflow_run_omits_absent_source_schema(self) -> None:
        client = ShardLoomClient(binary="unused", memory_gb=4, max_parallelism=2)
        captured: dict[str, Any] = {}

        def capture_run(args: list[str], *, check: bool = True) -> OutputEnvelope:
            captured["args"] = args
            return OutputEnvelope.from_field_mapping({}, command="run")

        client.run = capture_run  # type: ignore[method-assign]
        client.public_workflow_run("dataframe", input_uri="source.vortex", input_format="vortex")
        self.assertNotIn("--source-schema", captured["args"])


if __name__ == "__main__":
    unittest.main()
