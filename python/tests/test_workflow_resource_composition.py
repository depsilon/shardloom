from __future__ import annotations

import unittest

import shardloom as sl
from shardloom.models import OutputEnvelope


class WorkflowResourceCompositionTests(unittest.TestCase):
    def setUp(self):
        self.client = sl.ShardLoomClient(binary="unresolved")
        self.addCleanup(self.client.close)
        self.calls = []

        def run(args, *, check=True):
            self.calls.append(list(args))
            return OutputEnvelope.from_field_mapping({
                "result_jsonl": '{"id":1}\n', "output_row_count": "1",
                "result_payload_complete": "true", "fallback_attempted": "false",
                "external_engine_invoked": "false",
            }, command=args[0])

        self.client.run = run
        self.left_limits = sl.ExecutionResourceLimits(96 << 20, 2)
        self.right_limits = sl.ExecutionResourceLimits(64 << 20, 4)
        self.combined_limits = sl.ExecutionResourceLimits(64 << 20, 2)

    def workflow(self, kind, side, *, configured=True, source_free=False, memory_bytes=32 << 20):
        options = {"client": self.client,
                   "resource_limits": self.left_limits if side == "left" else self.right_limits}
        if configured:
            options.update(memory_bytes=memory_bytes, max_parallelism=1)
        path = f"missing-{side}.csv"
        if kind == "sql":
            if source_free:
                return sl.sql("SELECT 1 AS id", **options)
            return sl.sql(f"SELECT id FROM '{path}'", input=path, input_format="csv", **options)
        return sl.read_csv(path, schema={"id": "int64"}, **options).select("id")

    def assert_policy(self, workflow, *, configured=True):
        self.assertIsInstance(workflow, (sl.LazyFrame, sl.SqlWorkflow))
        self.assertEqual(workflow.resource_limits, self.combined_limits)
        self.assertEqual(self.calls, [])
        for request in ({"memory_bytes": (64 << 20) + 1, "max_parallelism": 1},
                        {"memory_bytes": 32 << 20, "max_parallelism": 3}):
            with self.subTest(request=request), self.assertRaises(sl.ShardLoomResourceConfigurationError):
                workflow.collect(**request)
            self.assertEqual(self.calls, [])
        if configured:
            workflow.collect()
        else:
            self.assertIsNone(workflow.resources)
            workflow.collect(memory_bytes=32 << 20, max_parallelism=1)
        self.assertTrue(self.calls)
        for command in self.calls:
            self.assertEqual(command[command.index("--memory-limit-bytes") + 1], str(64 << 20))
            self.assertEqual(command[command.index("--parallelism-limit") + 1], "2")
        self.calls.clear()

    def test_all_set_compositions_intersect_both_ceilings(self):
        for left_kind in ("frame", "sql"):
            for right_kind in ("frame", "sql"):
                for configured in (False, True):
                    for method in ("union", "union_all", "intersect", "except_"):
                        with self.subTest(left=left_kind, right=right_kind, configured=configured, method=method):
                            left = self.workflow(left_kind, "left", configured=configured)
                            right = self.workflow(right_kind, "right", configured=configured)
                            self.assert_policy(getattr(left, method)(right).filter("id > 0"),
                                               configured=configured)

    def test_join_merge_and_concat_keep_the_right_context_ceiling(self):
        for left_kind in ("frame", "sql"):
            for right_kind in ("frame", "sql"):
                with self.subTest(left=left_kind, right=right_kind):
                    left = self.workflow(left_kind, "left")
                    right = self.workflow(right_kind, "right")
                    self.assert_policy(left.join(right, on="id").select("f.id AS id"))
        left = self.workflow("frame", "left")
        right = self.workflow("frame", "right")
        self.assert_policy(left.merge(right, on="id"))
        self.assert_policy(left.concat(right))

    def test_incompatible_inherited_grant_is_rejected_without_reducing_it(self):
        left = self.workflow("sql", "left", memory_bytes=80 << 20)
        right = self.workflow("sql", "right")
        for method in ("union", "union_all", "intersect", "except_"):
            with self.subTest(method=method), self.assertRaises(sl.ShardLoomResourceConfigurationError):
                getattr(left, method)(right)
        with self.assertRaises(sl.ShardLoomResourceConfigurationError):
            left.join(right, on="id")
        self.assertEqual(self.calls, [])
        self.assertEqual(left.resources.memory_bytes, 80 << 20)

    def test_subquery_expressions_keep_policy_through_composition_and_projection(self):
        for left_kind in ("frame", "sql"):
            for right_kind in ("frame", "sql"):
                with self.subTest(left=left_kind, right=right_kind):
                    left = self.workflow(left_kind, "left")
                    right = self.workflow(right_kind, "right")
                    scalar = sl.scalar_subquery(right.limit(1) if right_kind == "frame" else right)
                    computed = (-scalar).abs().cast("int64").fill_null(0) + 1
                    self.assert_policy(left.with_column("other", computed))
                    predicate = (sl.col("id") >= computed) & ~sl.col("id").isin_source(right, "id")
                    self.assert_policy(left.filter(predicate))
                    self.assert_policy(left.filter(sl.exists_source(right, where=sl.col("id") > 0)))

    def test_source_free_subquery_still_carries_its_execution_ceiling(self):
        left = self.workflow("sql", "left", source_free=True)
        right = self.workflow("sql", "right", source_free=True)
        self.assert_policy(left.union_all(right))
        self.assert_policy(left.with_column("nested", -sl.scalar_subquery(right)))
        self.assert_policy(left.filter(sl.col("id") == sl.scalar_subquery(right)))

    def test_aggregates_windows_and_nested_branches_preserve_expression_limits(self):
        right = self.workflow("sql", "right", source_free=True)
        scalar = sl.scalar_subquery(right)
        distinct = sl.count_distinct(scalar)
        for kind in ("frame", "sql"):
            with self.subTest(kind=kind):
                left = self.workflow(kind, "left")
                self.assert_policy(left.agg(unique_ids=distinct))
                self.assert_policy(left.group_by("id").agg(unique_ids=distinct))
                self.assert_policy(left.window(sl.row_number(order_by=scalar)))
                self.assert_policy(left.sort(scalar))
                self.assert_policy(left.group_by(scalar).agg(rows="count(*)"))
                branch = sl.case_when(sl.col("id") > 0, scalar, -scalar)
                self.assert_policy(left.with_columns({"other": branch}))
                filtered = left.filter(sl.col("id") == branch)
                self.assert_policy(filtered.dropna(subset="id") if kind == "frame" else filtered)
        grouped = self.workflow("sql", "left").group_by("id").agg(rows="count(*)")
        self.assert_policy(grouped.having(sl.col("rows") >= scalar))

    def test_ceiling_intersection_is_order_independent_and_transitive(self):
        left = self.workflow("frame", "left")
        right = self.workflow("sql", "right")
        self.assert_policy(right.union_all(left))
        third = sl.sql("SELECT 1 AS id", client=self.client,
                       memory_bytes=32 << 20, max_parallelism=1,
                       resource_limits=sl.ExecutionResourceLimits(48 << 20, 1))
        combined = left.union_all(right).intersect(third)
        self.assertEqual(combined.resource_limits, sl.ExecutionResourceLimits(48 << 20, 1))
        self.assertEqual(combined.resources.memory_bytes, 32 << 20)
        for request in ({"memory_bytes": (48 << 20) + 1}, {"max_parallelism": 2}):
            with self.subTest(request=request), self.assertRaises(sl.ShardLoomResourceConfigurationError):
                combined.collect(**request)
        self.assertEqual(self.calls, [])


if __name__ == "__main__":
    unittest.main()
