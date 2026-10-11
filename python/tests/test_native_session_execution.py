"""Sessions forward every execution and expose only native preparation reuse."""
from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from shardloom import ShardLoomClient, ShardLoomContext, SessionSqlResult, VortexWorkflowExecutionReport
from shardloom.models import OutputEnvelope


def response(value, *, reused=False):
    envelope = OutputEnvelope.from_field_mapping({
        "result_jsonl": json.dumps({"id": value}) + "\n",
        "resident_relational_declaration_reused": str(reused).lower(),
        "result_schema_format": "vortex.dtype.serde.v1",
        "result_schema_json": json.dumps({"Struct": [{"names": ["id"],
            "dtypes": [{"Primitive": ["i64", False]}]}, False]}),
        "fallback_attempted": "false", "external_engine_invoked": "false",
    }, command="run")
    return SimpleNamespace(envelope=envelope)


class NativeSessionExecutionTests(unittest.TestCase):
    def setUp(self):
        self.client = ShardLoomClient(binary="unused", memory_gb=4, max_parallelism=2)
        self.session = ShardLoomContext(self.client).session()
        self.addCleanup(self.session.close)

    def test_unchanged_file_still_executes_and_uses_each_native_response(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "source.csv"
            source.write_text("id\n1\n")
            frame = self.session.read_csv(source).select("id").limit(1)
            with mock.patch.object(self.client, "public_workflow_run", side_effect=[
                response(1), response(2, reused=True), response(3),
            ]) as execute:
                results = [frame.collect() for _ in range(3)]
            self.assertEqual(execute.call_count, 3)
            self.assertEqual([r.report.result_rows[0]["id"] for r in results], [1, 2, 3])
            self.assertEqual([r.reuse_hit for r in results], [False, True, False])
            self.assertFalse(results[1].evidence()["query_answer_cached"])
            self.assertEqual(self.session.cache_hit_count, 1)
            self.assertEqual(self.session.cache_miss_count, 2)

    def test_source_free_sql_reset_and_close_share_the_native_owner(self):
        workflow = self.session.sql("SELECT 1 AS id")
        with mock.patch.object(self.client, "close") as close:
            with mock.patch.object(self.client, "public_workflow_run", return_value=response(1)) as execute:
                result = workflow.collect(reuse=False)
            self.assertIsInstance(result, SessionSqlResult)
            self.assertEqual(execute.call_count, 1)
            self.assertEqual(close.call_count, 1)
            self.session.close()
            self.assertEqual(close.call_count, 2)
            with self.assertRaisesRegex(RuntimeError, "closed"):
                workflow.collect()

    def test_input_preparation_evidence_distinguishes_creation_from_reuse(self):
        frame = ShardLoomContext(self.client).read_csv("declared.csv")
        for performed in (True, False):
            with self.subTest(performed=performed):
                envelope = OutputEnvelope.from_field_mapping({
                    "public_workflow_preparation_vortex_ingest_performed": str(performed).lower(),
                    "public_workflow_preparation_prepared_state_reused": str(not performed).lower(),
                    "public_workflow_preparation_source_state_id": "source-state-1",
                    "public_workflow_preparation_source_state_digest": "source-digest-1",
                    "public_workflow_local_source_prepared_vortex_path": "/native/input.vortex",
                }, command="run")
                report = VortexWorkflowExecutionReport(frame, "collect", envelope)
                self.assertEqual(report.vortex_ingest_performed, performed)
                self.assertEqual(report.prepared_vortex_path, "/native/input.vortex")
                self.assertEqual(report.source_state_id, "source-state-1")
                self.assertEqual(report.source_state_digest, "source-digest-1")
        report = VortexWorkflowExecutionReport(frame, "collect", response(1).envelope)
        self.assertFalse(report.vortex_ingest_performed)
        self.assertIsNone(report.prepared_vortex_path)
        self.assertIsNone(report.source_state_id)

    def test_existing_output_does_not_bypass_the_native_write_error(self):
        with tempfile.TemporaryDirectory() as directory:
            source, target = Path(directory) / "source.csv", Path(directory) / "result.jsonl"
            source.write_text("id\n1\n")
            target.write_text('{"id":1}\n')
            frame = self.session.read_csv(source).select("id").limit(1)
            with mock.patch.object(self.client, "public_workflow_run", side_effect=[
                response(1), RuntimeError("native output already exists"),
            ]) as execute:
                frame.write_jsonl(target)
                with self.assertRaisesRegex(RuntimeError, "native output already exists"):
                    frame.write_jsonl(target)
            self.assertEqual(execute.call_count, 2)
            self.assertEqual(target.read_text(), '{"id":1}\n')
