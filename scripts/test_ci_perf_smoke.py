# SPDX-License-Identifier: Apache-2.0
import json
from pathlib import Path
import tempfile
import unittest

from run_ci_perf_smoke import build_smoke_rows, run_smoke_suite, smoke_queries


class CiPerfSmokeTests(unittest.TestCase):
    def test_wide_integer_cancellation_oracle_preserves_exact_total_over_2_53(self):
        rows = build_smoke_rows(16)
        cases = {c["name"]: c for c in smoke_queries(rows)}
        wide = cases["wide_i64_cancellation_alpha"]["expected"][0]
        self.assertGreater(abs(wide["sum_wide"] - 7.0), -1.0)
        beta = cases["filtered_beta_wide_count"]["expected"][0]
        self.assertEqual(beta, {"sum_beta": 5.0, "cnt_beta": 2})

    def test_smoke_suite_validates_mocked_native_outputs_and_budgets(self):
        with tempfile.TemporaryDirectory(prefix="shardloom-ci-smoke-test-") as tmp:
            work_dir = Path(tmp)
            expected = {idx + 1: c for idx, c in enumerate(smoke_queries(build_smoke_rows(16)))}
            idx_holder = {"n": 0}

            def schema_for(rows):
                names = list(rows[0].keys())
                dtypes = []
                for name in names:
                    val = rows[0][name]
                    if isinstance(val, bool):
                        dtypes.append({"Bool": False})
                    elif isinstance(val, int):
                        dtypes.append({"Primitive": ["i64", False]})
                    elif isinstance(val, float):
                        dtypes.append({"Primitive": ["f64", False]})
                    else:
                        dtypes.append({"Utf8": False})
                return {"Struct": [{"names": names, "dtypes": dtypes}, False]}

            def ok_exec(_cmd, prefix: Path):
                idx_holder["n"] += 1
                rows = expected[idx_holder["n"]]["expected"]
                envelope = {
                    "status": "success",
                    "fallback": {"attempted": False, "allowed": False},
                    "fields": [
                        {"key": "public_workflow_fallback_attempted", "value": "false"},
                        {"key": "public_workflow_external_engine_invoked", "value": "false"},
                        {"key": "result_values_json", "value": json.dumps(rows)},
                        {"key": "result_payload_complete", "value": "true"},
                        {"key": "output_row_count", "value": str(len(rows))},
                        {"key": "result_schema_json", "value": json.dumps(schema_for(rows))},
                        {"key": "result_schema_format", "value": "vortex.dtype.serde.v1"},
                    ],
                }
                prefix.with_suffix(".stdout.json").write_text(json.dumps(envelope), encoding="utf-8")
                return {"returncode": 0, "guard_failures": [], "seconds": 0.05, "native_peak_rss_bytes": 16_000_000}

            report = run_smoke_suite(
                binary=Path("/tmp/fake-shardloom"),
                work_dir=work_dir,
                rows=16,
                max_wall_seconds=1.0,
                max_peak_rss_bytes=64_000_000,
                executor=ok_exec,
            )
            self.assertEqual(report["status"], "passed")
            self.assertFalse(report["fallback_attempted"])

            # Over-budget RSS must fail cleanly.
            idx_holder["n"] = 0
            bad_report = run_smoke_suite(
                binary=Path("/tmp/fake-shardloom"),
                work_dir=work_dir,
                rows=16,
                max_wall_seconds=1.0,
                max_peak_rss_bytes=1_000_000,
                executor=ok_exec,
            )
            self.assertEqual(bad_report["status"], "failed")


if __name__ == "__main__":
    unittest.main()
