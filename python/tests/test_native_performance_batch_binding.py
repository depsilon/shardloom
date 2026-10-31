# SPDX-License-Identifier: Apache-2.0
"""Complete public controls separate from the batch's acceptance inputs."""

import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(ROOT / "benchmarks/traditional_analytics"), str(ROOT / "scripts")]
if __package__:
    from . import native_performance_batch_cases as fixture
    from .test_native_binding import _load_native
else:
    import native_performance_batch_cases as fixture
    from test_native_binding import _load_native

from query_surface.evidence import no_fallback
from query_surface.semantics import ResultContract, TypedResult, compare
from shardloom._result_schema import schema_fields


def public_cases():
    cases = []
    for family, _, shapes in fixture.GRID:
        if family == "B3":
            # Native tests persist and inspect these four physical Vortex shapes.
            continue
        for shape in shapes:
            rows = 4099 if family in ("B7", "B8") else 2051 if family == "B9" else 33
            if family == "B9" and shape == "pressured":
                rows = 16385
            cases.append(dict(family=family, shape=shape, rows=rows,
                              case_id=f"control_{family}_{shape}_{rows}"))
    assert len(cases) == 36
    assert not {case["case_id"] for case in cases} & {case["case_id"] for case in fixture.cases()}
    return cases


def producer(rows, width, trace):
    def read():
        trace["opened"] += 1
        try:
            for start in range(0, len(rows), width):
                batch = rows[start:start + width]
                trace["rows"] += len(batch)
                trace["batches"] += 1
                yield batch
            trace["ended"] = True
        finally:
            trace["closed"] = True
    return read


class NativePerformanceBatchTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.native = _load_native()

    def checked(self, case, expected, actual, report):
        compare(expected, actual, ResultContract(expected.schema, ordered=True))
        no_fallback(report["fields"])
        self.assertTrue(report["source_session_released_before_export"])
        self.assertTrue(report["complete_consumption"])

    def execute(self, case, root):
        native = self.native
        records = fixture.records(case)
        expected = fixture.reference(case, records)
        policy = fixture.resources(case)
        session = native.Session(native.ExecutionResources(policy["memory_bytes"], policy["parallelism"]))
        files, sources, batches, traces, references = [], [], [], {}, {}
        workspace = root / "spill"
        try:
            kind = fixture.source_kind(case)
            if kind == "parquet":
                paths = fixture.write_parquet(case, root, records)
                files = [(str(path), "parquet", None) for path in paths.values()]
                references = {name: str(path) for name, path in paths.items()}
            else:
                self.assertIn(kind, ("memory", "batches"))
                for name, rows in records.items():
                    reference = f"memory://batch-control/{name}"
                    references[name] = reference
                    fields = [(field.name, json.dumps({"Primitive": ["i64", field.nullable]}))
                              for field in fixture.source_schema(case, name)]
                    if kind == "memory":
                        sources.append((reference, session.from_rows(rows, fields=fields)))
                    else:
                        trace = dict(opened=0, rows=0, batches=0, ended=False, closed=False)
                        traces[name] = trace
                        width = 1 if case["shape"] == "pressured" else int(case["shape"].split("_")[1])
                        batches.append((reference, producer(rows, width, trace), fields, True))
            spill = None
            if policy["spill"]:
                workspace.mkdir()
                spill = json.dumps(dict(workspace=str(workspace), quota_bytes=policy["spill_quota_bytes"],
                                        buffer_bytes=policy["spill_buffer_bytes"]))
            plan = session.sql(fixture.statement(case, references), files=files,
                               sources=sources, batches=batches, spill=spill)
            self.assertTrue(all(trace["opened"] == 0 for trace in traces.values()))
            result = plan.collect(timeout=policy["timeout_seconds"])
            report = dict(fields=dict(result.report), producer_traces=traces,
                          source_session_released_before_export=True, complete_consumption=True)
            del plan
            sources.clear()
            batches.clear()
            session.close()
            rows, _ = result.row_export(temporal_objects=False)
            actual = TypedResult(schema_fields(result.schema_json, "vortex.dtype.serde.v1"), rows)
            self.checked(case, expected, actual, report)
            if case["family"] in ("B7", "B8") and policy["spill"]:
                self.assertGreater(int(report["fields"]["relational_spill_runs_written"]), 0)
                self.assertEqual(report["fields"]["relational_spill_owned_cleanup_completed"], "true")
            for name, trace in traces.items():
                self.assertEqual(trace["opened"], 1)
                self.assertEqual(trace["rows"], len(records[name]))
                self.assertTrue(trace["ended"] and trace["closed"])
            if case["family"] == "B9":
                fields = report["fields"]
                self.assertEqual(int(fields["native_input_replay_sources"]), 1)
                self.assertEqual(int(fields["native_input_replay_rows_detached"]), case["rows"])
                self.assertGreaterEqual(int(fields["native_input_replay_rows"]), case["rows"])
                if case["shape"] == "pressured":
                    self.assertEqual(int(fields["native_input_replay_spilled_sources"]), 1)
            if policy["spill"]:
                self.assertEqual(list(workspace.iterdir()), [])
            del result, actual, rows
            self.assertEqual(session.snapshot()["reserved_bytes"], 0)
        finally:
            sources.clear()
            batches.clear()
            session.close()

    def test_complete_workflows_and_source_lifetimes(self):
        for case in public_cases():
            with self.subTest(case=case["case_id"]), \
                 tempfile.TemporaryDirectory(prefix="shardloom-performance-batch-") as directory, \
                 patch("subprocess.Popen", side_effect=AssertionError("external execution")):
                self.execute(case, Path(directory))


if __name__ == "__main__":
    unittest.main()
