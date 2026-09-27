from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import shardloom as sl
from shardloom.client import PublicWorkflowExecution, VortexIngestSmokeReport
from shardloom.models import OutputEnvelope
from shardloom.query import SqlLocalSourceSmokeReport, UnsupportedWorkflowOperationReport


class _CapturingClient:
    def __init__(self, *, prepare_status: str = "success") -> None:
        self.prepare_status = prepare_status
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
            {}, command="run", summary="mocked public workflow run"
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

    def _assert_write_uses_prepared_vortex(
        self,
        frame: Any,
        source: Path,
        client: _CapturingClient,
        output_format: str,
        output_path: Path,
    ) -> None:
        report = frame.write(output_path, output_format=output_format, check=False)

        self.assertIsInstance(report, SqlLocalSourceSmokeReport)
        self.assertNotIsInstance(report, UnsupportedWorkflowOperationReport)
        self.assertEqual(len(client.prepare_calls), 1)
        self.assertEqual(len(client.run_calls), 1)
        prepare = client.prepare_calls[0]
        run = client.run_calls[0]
        self.assertEqual(prepare["args"][0], str(source))
        self.assertEqual(prepare["kwargs"]["input_format"], "csv")
        self.assertEqual(prepare["kwargs"]["schema"], (("label", "utf8"),))
        self.assertEqual(run["input_format"], "vortex")
        self.assertTrue(str(run["input_uri"]).endswith(".vortex"))
        self.assertNotEqual(run["input_uri"], str(source))
        self.assertNotIn(str(source), run["sql_statement"])
        self.assertIn(str(run["input_uri"]), run["sql_statement"])

    def test_projection_write_prepares_declared_csv_schema_for_all_sinks(self) -> None:
        for output_format in self.output_formats:
            with self.subTest(output_format=output_format), tempfile.TemporaryDirectory() as directory:
                client = _CapturingClient()
                source, frame = self._source(directory, client)
                self._assert_write_uses_prepared_vortex(
                    frame.select("label").limit(2),
                    source,
                    client,
                    output_format,
                    Path(directory) / f"projection.{output_format}",
                )

    def test_scalar_min_write_prepares_declared_csv_schema_for_all_sinks(self) -> None:
        for output_format in self.output_formats:
            with self.subTest(output_format=output_format), tempfile.TemporaryDirectory() as directory:
                client = _CapturingClient()
                source, frame = self._source(directory, client)
                self._assert_write_uses_prepared_vortex(
                    frame.aggregate("MIN(label)").limit(1),
                    source,
                    client,
                    output_format,
                    Path(directory) / f"minimum.{output_format}",
                )

    def test_fanout_forwards_secondary_sinks_after_declared_schema_preparation(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            client = _CapturingClient()
            source, frame = self._source(directory, client)
            primary = Path(directory) / "primary.parquet"
            secondary = Path(directory) / "secondary.csv"

            report = frame.select("label").limit(2).fanout(
                {"parquet": primary, "csv": secondary}, check=False
            )

            self.assertIsInstance(report, SqlLocalSourceSmokeReport)
            self.assertNotIsInstance(report, UnsupportedWorkflowOperationReport)
            self.assertEqual(len(client.prepare_calls), 1)
            self.assertEqual(len(client.run_calls), 1)
            self.assertEqual(client.prepare_calls[0]["kwargs"]["schema"], (("label", "utf8"),))
            self.assertEqual(client.run_calls[0]["fanout_outputs"], (("csv", str(secondary)),))
            self.assertEqual(client.run_calls[0]["input_format"], "vortex")
            self.assertNotIn(str(source), client.run_calls[0]["sql_statement"])

    def test_failed_preparation_with_check_false_skips_public_workflow_run(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            client = _CapturingClient(prepare_status="error")
            _, frame = self._source(directory, client)

            report = frame.select("label").limit(2).write(
                Path(directory) / "output.parquet",
                output_format="parquet",
                check=False,
            )

            self.assertIsInstance(report, SqlLocalSourceSmokeReport)
            self.assertNotIsInstance(report, UnsupportedWorkflowOperationReport)
            self.assertEqual(report.status, "error")
            self.assertEqual(len(client.prepare_calls), 1)
            self.assertEqual(client.prepare_calls[0]["kwargs"]["check"], False)
            self.assertEqual(client.run_calls, [])


if __name__ == "__main__":
    unittest.main()
