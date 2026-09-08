"""Complete-value and ownership checks for an explicitly built local extension.

The extension path is supplied by SHARDLOOM_NATIVE_EXPERIMENT; no auto-build or
subprocess fallback is permitted. The five-row oracle is checked into Rust source.
"""

import gc
import json
import os
from pathlib import Path
import shutil
import tempfile
import unittest

from load_native import load


class NativeBinding(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.native = load(Path(os.environ["SHARDLOOM_NATIVE_EXPERIMENT"]))
        cls.original = Path(__file__).resolve().parents[2] / "shardloom-vortex/tests/fixtures/local_primitive_struct_five.vortex"
        cls.expected = [{"value": i, "metric": i * 10} for i in range(1, 6)]

    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix="shardloom-native-binding-")
        self.source = Path(self.directory.name) / "source.vortex"
        shutil.copyfile(self.original, self.source)
        self.session = self.native.Session(memory_gb=1, max_parallelism=2)

    def tearDown(self):
        self.session.close()
        gc.collect()
        self.directory.cleanup()

    def test_metadata_and_real_filtered_counts_reexecute_all_comparisons(self):
        count = self.session.prepare_count(str(self.source))
        for _ in range(3):
            self.assertEqual(count.execute_count(), 5)
        self.assertEqual(self.session.snapshot()["completed_executions"], 3)
        self.assertEqual(self.session.snapshot()["prepared_source_opens"], 1)
        comparisons = {"eq": lambda a: a == 30, "ne": lambda a: a != 30,
                       "lt": lambda a: a < 30, "le": lambda a: a <= 30,
                       "gt": lambda a: a > 30, "ge": lambda a: a >= 30}
        for name, predicate in comparisons.items():
            prepared = self.session.prepare_count_where_i64(str(self.source), "metric", name, 30)
            expected = sum(predicate(row["metric"]) for row in self.expected)
            for _ in range(3):
                self.assertEqual(prepared.execute_count(), expected)
            prepared.close()

    def test_projection_complete_values_and_explicit_retained_json_after_close(self):
        plan = self.session.prepare_projection(str(self.source), ["metric", "value"], 5)
        batch = plan.execute_arrays()
        self.assertEqual(batch.info()["rows"], 5)
        self.assertEqual(json.loads(plan.execute_json()), self.expected)
        at_close = self.session.close()
        self.assertEqual(at_close["prepared_source_opens"], 1)
        self.assertEqual(at_close["completed_executions"], 2)
        with self.assertRaisesRegex(RuntimeError, "SL_NATIVE_CLOSED"):
            plan.execute_arrays()
        self.assertEqual(json.loads(batch.to_json()), self.expected)
        batch.close()
        with self.assertRaisesRegex(RuntimeError, "SL_NATIVE_CLOSED"):
            batch.to_json()

    def test_empty_filter_replacement_and_unlink_invalidate_without_old_answer(self):
        for mutation in ("replace", "unlink", "append"):
            with self.subTest(mutation=mutation):
                shutil.copyfile(self.original, self.source)
                plan = self.session.prepare_count_where_i64(str(self.source), "metric", "ge", 999)
                self.assertEqual(plan.execute_count(), 0)
                completed = self.session.snapshot()["completed_executions"]
                if mutation == "replace":
                    replacement = self.source.with_name("replacement.vortex")
                    shutil.copyfile(self.original, replacement)
                    os.replace(replacement, self.source)
                elif mutation == "unlink":
                    self.source.unlink()
                else:
                    with self.source.open("ab") as stream:
                        stream.write(b"mutation")
                with self.assertRaises(RuntimeError):
                    plan.execute_count()
                self.assertEqual(self.session.snapshot()["completed_executions"], completed)
                plan.close()

    def test_bounds_types_closed_prepared_and_failed_sink_leave_usable_owner(self):
        for columns, limit in (([], 5), (["value", "value"], 5), (["value"], 0), (["value"], 65537)):
            with self.assertRaises(ValueError):
                self.session.prepare_projection(str(self.source), columns, limit)
        with self.assertRaises(ValueError):
            self.session.prepare_count("relative.vortex")
        plan = self.session.prepare_projection(str(self.source), ["value"], 2)
        batch = plan.execute_arrays()
        for limit in (0, 1, 8 * 1024 * 1024 + 1):
            with self.assertRaises(RuntimeError):
                batch.to_json(limit)
        self.assertEqual(json.loads(batch.to_json()), [{"value": 1}, {"value": 2}])
        with self.assertRaises(ValueError):
            plan.execute_count()
        batch.close()
        plan.close()
        with self.assertRaisesRegex(RuntimeError, "SL_NATIVE_CLOSED"):
            plan.execute_arrays()
        self.assertEqual(self.session.snapshot()["native_owned_bytes"], 0)

    def test_plan_cap_refunds_slots_and_drop_keeps_prepared_owner_valid(self):
        plans = [self.session.prepare_count(str(self.source)) for _ in range(64)]
        with self.assertRaisesRegex(RuntimeError, "SL_NATIVE_PLAN_LIMIT"):
            self.session.prepare_count(str(self.source))
        del plans[-1]
        gc.collect()
        replacement = self.session.prepare_count(str(self.source))
        self.assertEqual(replacement.execute_count(), 5)
        del plans
        replacement.close()
        self.assertEqual(self.session.close()["native_owned_bytes"], 0)
        temporary = self.native.Session(memory_gb=1, max_parallelism=1)
        retained = temporary.prepare_count(str(self.source))
        del temporary
        gc.collect()
        self.assertEqual(retained.execute_count(), 5)
        retained.close()


if __name__ == "__main__":
    unittest.main()
