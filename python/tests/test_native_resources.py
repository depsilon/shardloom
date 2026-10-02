from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import shardloom as sl
from shardloom.models import OutputEnvelope


class NativeResourceTransportTests(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.client = sl.ShardLoomClient(binary="unused")
        self.context = sl.ShardLoomContext(self.client)
        self.commands: list[list[str]] = []
        self.spill = {
            "workspace": str(self.root / "absent-spill"),
            "quota_bytes": 64 << 20,
            "buffer_bytes": 2 << 20,
        }
        self.resources = {"memory_gb": 3, "max_parallelism": 7, "spill": self.spill}

        def run(args: list[str], *, check: bool = True) -> OutputEnvelope:
            self.commands.append(args)
            return OutputEnvelope.from_field_mapping({
                "result_jsonl": '{"key":1}\n', "output_row_count": "1",
                "result_payload_complete": "true", "fallback_attempted": "false",
                "external_engine_invoked": "false",
            }, command=args[0])

        self.client.run = run  # type: ignore[method-assign]

    def assert_resources(self, command: list[str], *, execution: bool = True) -> None:
        self.assertEqual(command[command.index("--memory-gb") + 1], "3", command)
        self.assertEqual(command[command.index("--max-parallelism") + 1], "7")
        if execution:
            self.assertEqual(command.count("--spill"), 1)
            self.assertEqual(json.loads(command[command.index("--spill") + 1]), self.spill)
        else:
            self.assertNotIn("--spill", command)

    def test_client_run_and_route_transport_policy_without_interpreting_it(self) -> None:
        for method in (self.client.public_workflow_run, self.client.public_workflow_route):
            method("sql", sql_statement="SELECT 1", **self.resources)
            self.assert_resources(self.commands[-1])
            method("sql", spill=None)
            self.assertNotIn("--spill", self.commands[-1])
            # The shared backend owns validation, including unsupported fields.
            method("sql", spill='{"unsupported":true}')
            self.assertEqual(self.commands[-1][self.commands[-1].index("--spill") + 1], '{"unsupported":true}')
        self.assertFalse((self.root / "absent-spill").exists())

    def test_sql_and_dataframe_collect_and_all_writers_keep_one_resource_request(self) -> None:
        native = self.context.read_vortex(str(self.root / "source.vortex"), schema={"key": "int64"})
        compat = self.context.read_csv(str(self.root / "source.csv"), schema={"key": "int64"})
        statement = f"SELECT key FROM (SELECT key FROM '{self.root / 'source.vortex'}' LIMIT 9) AS derived WHERE key > 1"
        workflows = [
            native.select("key").limit(9),
            native.select("key").limit(9).filter("key > 1"),
            compat.select("key").limit(9),
            native.select("key").union_all(compat.select("key")).sort("key"),
            self.context.sql(statement),
            self.context.sql(f"SELECT key FROM '{self.root / 'source.csv'}' ORDER BY key LIMIT 9"),
        ]
        for index, workflow in enumerate(workflows):
            with self.subTest(workflow=index):
                self.commands.clear()
                workflow.collect(check=True, **self.resources)
                self.assertTrue(self.commands)
                for command in self.commands:
                    self.assert_resources(command, execution=command[0] != "vortex-prepare")
                for output in ["vortex", "json", "jsonl", "csv", "parquet", "arrow_ipc", "avro", "orc"]:
                    self.commands.clear()
                    getattr(workflow, f"write_{output}")(str(self.root / f"result.{output}"), **self.resources)
                    self.assertEqual(len([call for call in self.commands if call[0] == "run"]), 1)
                    for command in self.commands:
                        self.assert_resources(command, execution=command[0] != "vortex-prepare")
        self.assertFalse((self.root / "absent-spill").exists())

    def test_sql_route_run_and_fanout_and_dataframe_count_preserve_resources(self) -> None:
        statement = f"SELECT key FROM '{self.root / 'input.vortex'}' ORDER BY key LIMIT 9"
        sql = self.context.sql(statement)
        sql.route(**self.resources)
        self.assert_resources(self.commands[-1])
        sql.run(**self.resources)
        self.assert_resources(self.commands[-1])
        for workflow in [sql, self.context.read_vortex(str(self.root / "input.vortex")).select("key").limit(9)]:
            workflow.fanout({"jsonl": self.root / "one.jsonl", "csv": self.root / "two.csv"}, **self.resources)
            self.assert_resources(self.commands[-1])
        frame = self.context.read_vortex(str(self.root / "input.vortex"), schema={"key": "int64"})
        frame.limit(9).route(**self.resources)
        self.assert_resources(self.commands[-1])
        frame.limit(9).run(**self.resources)
        self.assert_resources(self.commands[-1])
        self.commands.clear()
        frame.select("key").limit(9).filter("key > 1").count(**self.resources)
        self.assertEqual(len(self.commands), 1)
        self.assert_resources(self.commands[-1])


if __name__ == "__main__":
    unittest.main()
