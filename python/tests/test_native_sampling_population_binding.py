# SPDX-License-Identifier: Apache-2.0
"""Sampling population spill through the installed in-process Python binding."""

from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

if __package__:
    from .test_native_binding import _load_native
    from .test_native_unary_binding import SCHEMA, arrow_schema
else:
    from test_native_binding import _load_native
    from test_native_unary_binding import SCHEMA, arrow_schema


class NativeSamplingPopulationBindingTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.native = _load_native()

    def client(self, memory_bytes=32 << 20):
        import shardloom as sl

        client = sl.ShardLoomClient(transport="native", memory_bytes=memory_bytes,
                                    max_parallelism=1)
        self.addCleanup(client.close)
        return client

    def completed(self, report, events, *, input_batches, input_rows, repeated=False):
        self.assertEqual(events, ["opened", "ended", "closed"])
        envelope = report.envelope
        self.assertEqual(envelope.field_int("native_input_batch_sources"), 1)
        self.assertEqual(envelope.field_int("native_input_batches"), input_batches)
        self.assertEqual(envelope.field_int("native_input_batch_rows"), input_rows)
        self.assertTrue(envelope.field_bool("native_input_end_observed"))
        self.assertTrue(envelope.field_bool("native_input_output_ownership_detached"))
        self.assertEqual(envelope.field_int("native_input_replay_sources"), int(repeated))
        self.assertEqual(envelope.field_int("native_input_replay_passes"), int(repeated))
        self.assertFalse(envelope.fallback.attempted)
        self.assertFalse(envelope.fallback.allowed)
        self.assertEqual(envelope.field("external_engine_invoked"), "false")

    def test_sampling_population_spill_matches_fixture_oracle_across_terminals(self):
        import pyarrow as pa
        import shardloom as sl

        fixture_path = (Path(__file__).resolve().parents[2] /
                        "shardloom-vortex/tests/fixtures/native_sampling_population_reference.json")
        cases = json.loads(fixture_path.read_text())["failure_cases"]
        self.assertEqual(len(cases), 6)
        self.assertEqual({case["name"] for case in cases}, {
            "fraction-unweighted", "replacement-unweighted",
            "fraction-replacement-unweighted", "fraction-weighted",
            "replacement-weighted", "fraction-replacement-weighted",
        })

        input_rows = [
            {"n": ordinal,
             "s": f"{ordinal:08d}" + "u" * (32768 - 8),
             "items": [ordinal, None]}
            for ordinal in range(65)
        ]
        input_batches = (len(input_rows) + 12) // 13

        for case in cases:
            self.assertEqual(case["rows"], len(input_rows))
            self.assertEqual(case["width"], 32768)
            self.assertEqual(case["grant_bytes"], 32 << 20)
            wanted_rows = [input_rows[index] for index in case["expected"]]
            wanted = pa.Table.from_pylist(wanted_rows, schema=arrow_schema())
            for columnar in (False, True):
                for terminal in ("collect", "batches", "vortex", "fanout"):
                    with self.subTest(case=case["name"], columnar=columnar, terminal=terminal), \
                         tempfile.TemporaryDirectory(
                             prefix="shardloom-native-sampling-population-") as directory, \
                         patch("subprocess.Popen",
                               side_effect=AssertionError("child process")), \
                         patch("shardloom._batches.BatchInput.encode",
                               side_effect=AssertionError("JSON input")):
                        client, retained = self.client(), None
                        verification_client = None
                        root = Path(directory)
                        workspace = root / "spill"
                        workspace.mkdir()
                        foreign = workspace / "foreign"
                        foreign.write_bytes(b"preserve")
                        spill = {"workspace": str(workspace), "quota_bytes": 128 << 20,
                                 "buffer_bytes": 1 << 20}
                        events = []

                        def produce():
                            events.append("opened")
                            try:
                                for start in range(0, len(input_rows), 13):
                                    batch = input_rows[start:start + 13]
                                    yield (pa.Table.from_pylist(batch, schema=arrow_schema())
                                           if columnar else batch)
                                events.append("ended")
                            finally:
                                events.append("closed")

                        source = sl.from_batches(produce, schema=SCHEMA, streaming=True,
                                                 client=client)
                        if case["weighted"]:
                            source = source.filter("n > 0")
                        sample_args = {
                            "random_state": case["seed"],
                            "replace": case["replace"],
                            "weights": "n" if case["weighted"] else None,
                        }
                        if case["count"] is not None:
                            sample_args["n"] = case["count"]
                        else:
                            sample_args["frac"] = case["fraction"]
                        query = source.sample(**sample_args)

                        if terminal == "collect":
                            result = query.collect(check=True, spill=spill)
                            report = result
                            retained = result.to_arrow()
                        elif terminal == "batches":
                            tables = []
                            with query.iter_batches(batch_rows=17, spill=spill) as stream:
                                for batch in stream:
                                    self.assertIsNone(stream.report)
                                    table = batch.to_arrow()
                                    self.assertTrue(table.schema.equals(
                                        wanted.schema, check_metadata=True))
                                    tables.append(table)
                                report = stream.report
                            retained = pa.concat_tables(tables)
                        else:
                            extensions = (("vortex",) if terminal == "vortex" else
                                          ("vortex", "parquet", "jsonl"))
                            targets = {extension: root / ("result." + extension)
                                       for extension in extensions}
                            report = (query.fanout(targets, spill=spill)
                                      if terminal == "fanout" else
                                      query.write_vortex(targets["vortex"], spill=spill))
                            if terminal == "fanout":
                                self.assertEqual(report.envelope.field_int(
                                    "native_vortex_result_export_execution_count"), 1)
                                self.assertEqual(report.envelope.field_int(
                                    "native_vortex_result_export_target_count"), 3)
                                self.assertEqual(report.envelope.field_int(
                                    "native_vortex_result_export_replay_count"), 3)
                            verification_client = self.client(128 << 20)
                            for extension, path in targets.items():
                                if extension == "jsonl":
                                    self.assertEqual(
                                        [json.loads(line) for line in path.read_text().splitlines()],
                                        wanted_rows,
                                    )
                                else:
                                    reopened = getattr(sl, "read_" + extension)(
                                        path, client=verification_client)
                                    table = reopened.collect(check=True).to_arrow()
                                    self.assertTrue(table.schema.equals(
                                        wanted.schema, check_metadata=True))
                                    self.assertTrue(table.equals(wanted, check_metadata=True))
                                    if extension == "vortex":
                                        retained = table

                        self.assertIsNotNone(retained)
                        self.assertTrue(retained.schema.equals(wanted.schema,
                                                                check_metadata=True))
                        self.assertTrue(retained.equals(wanted, check_metadata=True),
                                        (case["name"], terminal))
                        self.completed(report, events, input_batches=input_batches,
                                       input_rows=len(input_rows))
                        envelope = report.envelope
                        self.assertEqual(envelope.field_int(
                            "relational_spillable_sampling_stages"), 1)
                        self.assertEqual(envelope.field_int(
                            "relational_spillable_sampling_input_rows"), case["population"])
                        self.assertEqual(envelope.field_int(
                            "relational_spillable_sampling_population_rows"),
                            case["population"])
                        self.assertEqual(envelope.field_int(
                            "relational_spillable_sampling_output_rows"), len(wanted_rows))
                        self.assertEqual(envelope.field_int(
                            "relational_spillable_sampling_replacement_draws"),
                            len(wanted_rows) if case["replace"] else 0)
                        self.assertEqual(envelope.field_int(
                            "relational_spillable_sampling_lookup_blocks") > 0,
                            case["replace"])
                        self.assertGreater(envelope.field_int("relational_spill_runs_written"), 0)
                        self.assertTrue(envelope.field_bool(
                            "relational_spill_owned_cleanup_completed"))
                        self.assertLessEqual(envelope.field_int(
                            "relational_spill_peak_disk_bytes"), spill["quota_bytes"])
                        self.assertLessEqual(envelope.field_int(
                            "resident_peak_reserved_buffer_bytes"), 32 << 20)
                        self.assertEqual(envelope.field_int("native_input_replay_sources"), 0)
                        self.assertEqual(envelope.field(
                            "local_primitive_native_io_certificate_status"), "certified")
                        self.assertEqual(list(workspace.iterdir()), [foreign])
                        self.assertEqual(foreign.read_bytes(), b"preserve")
                        client.close()
                        if verification_client is not None:
                            verification_client.close()
                        self.assertTrue(retained.equals(wanted, check_metadata=True))
