# SPDX-License-Identifier: Apache-2.0
"""Public Python reachability tests for Track A/B direct Parquet aggregation and early join filters."""

from __future__ import annotations

import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

if __package__:
    from .test_native_expression_binding import _load_native
else:
    from test_native_expression_binding import _load_native

_REQUIRE_NATIVE = os.environ.get("SHARDLOOM_REQUIRE_NATIVE_PYTHON") == "1"


def _sorted_rows(rows: list[dict[str, object]], key: str) -> list[dict[str, object]]:
    return sorted(rows, key=lambda row: row[key])


class NativeParquetAggAndJoinFilterTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.native = _load_native()
        try:
            import pyarrow as pa
            import pyarrow.compute as pc
            import pyarrow.ipc as ipc
            import pyarrow.parquet as pq
        except ImportError as error:
            if _REQUIRE_NATIVE:
                raise AssertionError("required PyArrow fixture dependency is absent") from error
            raise unittest.SkipTest("optional PyArrow dependency is absent") from error
        cls.pa = pa
        cls.pc = pc
        cls.ipc = ipc
        cls.pq = pq

    def setUp(self) -> None:
        self._no_proc_run = patch(
            "shardloom.client.subprocess.run", side_effect=AssertionError("unexpected CLI subprocess run")
        )
        self._no_proc_popen = patch(
            "shardloom.client.subprocess.Popen", side_effect=AssertionError("unexpected CLI subprocess popen")
        )
        self._no_proc_run.start()
        self._no_proc_popen.start()
        self.addCleanup(self._no_proc_popen.stop)
        self.addCleanup(self._no_proc_run.stop)

    def _build_fixtures(self, directory: Path) -> dict[str, object]:
        pa, pq, pc, ipc = self.pa, self.pq, self.pc, self.ipc
        fact_rows = 16_384  # 4 row groups x 4,096 rows -> meets Track A1 threshold
        fact_tbl = pa.table({
            "k": pa.array([i % 512 for i in range(fact_rows)], type=pa.int64()),
            "grp": pa.array([i % 16 for i in range(fact_rows)], type=pa.int64()),
            "val_i64": pa.array([((i * 7) % 100) - 20 for i in range(fact_rows)], type=pa.int64()),
            "val_f64": pa.array([float((i * 3) % 50) for i in range(fact_rows)], type=pa.float64()),
            "payload": pa.array([f"p-{i:05}-" + ("x" * 24) for i in range(fact_rows)], type=pa.string()),
        })
        fact_pq = directory / "fact.parquet"
        pq.write_table(fact_tbl, fact_pq, row_group_size=4096, use_dictionary=False)

        # 256-row dimension; 70 spaced keys have tier == 1 (k = i * 7 mod 512 -> >64 unique keys)
        dim_keys = [i * 7 if i < 70 else 1000 + i for i in range(256)]
        dim_tiers = [1 if i < 70 else 0 for i in range(256)]
        dim_regions = [i % 4 for i in range(256)]
        dim_tbl = pa.table({
            "k": pa.array(dim_keys, type=pa.int64()),
            "tier": pa.array(dim_tiers, type=pa.int64()),
            "region": pa.array(dim_regions, type=pa.int64()),
        })
        dim_pq = directory / "dim.parquet"
        pq.write_table(dim_tbl, dim_pq, row_group_size=256)

        # Prepared .vortex fact control written cleanly through native Session.
        vortex_fact = directory / "fact.vortex"
        sess = self.native.Session(self.native.ExecutionResources(128 << 20, 4))
        try:
            src = sess.from_arrow(fact_tbl)
            sess.plan(self.native.Relation.scan("memory://fact"), sources=[("memory://fact", src)]).write(
                vortex_fact, "write_vortex"
            )
        finally:
            sess.close()

        # Arrow IPC control table.
        ipc_path = directory / "control.arrow"
        with ipc.new_file(ipc_path, fact_tbl.schema) as writer:
            writer.write_table(fact_tbl)

        # Independent PyArrow reference answers.
        ref_agg = [
            {
                "grp": r["grp"],
                "cnt": r["val_i64_count"],
                "sum_v": r["val_i64_sum"],
                "min_v": r["val_i64_min"],
                "max_v": r["val_i64_max"],
            }
            for r in fact_tbl.group_by("grp")
            .aggregate([
                ("val_i64", "count"),
                ("val_i64", "sum"),
                ("val_i64", "min"),
                ("val_i64", "max"),
            ])
            .sort_by([("grp", "ascending")])
            .to_pylist()
        ]
        ref_float = [
            {"grp": r["grp"], "sum_f": r["val_f64_sum"]}
            for r in fact_tbl.group_by("grp")
            .aggregate([("val_f64", "sum")])
            .sort_by([("grp", "ascending")])
            .to_pylist()
        ]
        dim_sel = dim_tbl.filter(pc.equal(dim_tbl["tier"], 1))
        joined = fact_tbl.join(dim_sel, keys="k", join_type="inner")
        ref_join_agg = [
            {"region": r["region"], "sum_v": r["val_i64_sum"]}
            for r in joined.group_by("region")
            .aggregate([("val_i64", "sum")])
            .sort_by([("region", "ascending")])
            .to_pylist()
        ]
        return {
            "fact_pq": fact_pq,
            "dim_pq": dim_pq,
            "vortex_fact": vortex_fact,
            "ipc_path": ipc_path,
            "ref_agg": ref_agg,
            "ref_float": ref_float,
            "ref_join_agg": ref_join_agg,
        }

    def test_public_sql_lazyframe_dataframe_session_and_typed_relation_reach_track_a_and_b(self) -> None:
        import shardloom as sl

        with tempfile.TemporaryDirectory(prefix="shardloom-py-track-ab-") as tmpdir:
            fx = self._build_fixtures(Path(tmpdir))
            fact_pq = fx["fact_pq"]
            dim_pq = fx["dim_pq"]
            vortex_fact = fx["vortex_fact"]
            ipc_path = fx["ipc_path"]

            with sl.context(transport="native", memory_bytes=256 << 20, max_parallelism=4) as ctx:
                # 1a. Context SQL: Track A parallel agg + persistent state merge.
                sql_agg = (
                    f"SELECT grp, COUNT(*) AS cnt, SUM(val_i64) AS sum_v, "
                    f"MIN(val_i64) AS min_v, MAX(val_i64) AS max_v "
                    f"FROM '{fact_pq}' GROUP BY grp ORDER BY grp ASC"
                )
                r_agg = ctx.sql(sql_agg).collect(check=True)
                self.assertEqual(r_agg.to_arrow().to_pylist(), fx["ref_agg"])
                self.assertFalse(r_agg.fallback_attempted)
                self.assertFalse(r_agg.external_engine_invoked)
                self.assertIsNotNone(r_agg.parquet_parallel_workers)
                self.assertGreaterEqual(r_agg.parquet_parallel_workers, 2)
                self.assertGreaterEqual(r_agg.parquet_parallel_range_tasks, 2)
                self.assertGreaterEqual(r_agg.parquet_persistent_aggregate_states, 2)
                self.assertGreaterEqual(r_agg.parquet_aggregate_partial_groups_emitted, 2)
                self.assertGreaterEqual(r_agg.parquet_aggregate_partial_groups_merged, 1)
                self.assertEqual(r_agg.parquet_aggregate_final_groups, 16)

                # 1b. Context SQL: Float SUM declines Track A2 persistent state merge while keeping A1 workers.
                sql_float = f"SELECT grp, SUM(val_f64) AS sum_f FROM '{fact_pq}' GROUP BY grp ORDER BY grp ASC"
                r_flt = ctx.sql(sql_float).collect(check=True)
                self.assertEqual(r_flt.to_arrow().to_pylist(), fx["ref_float"])
                self.assertFalse(r_flt.fallback_attempted)
                self.assertGreaterEqual(r_flt.parquet_parallel_workers, 2)
                self.assertEqual(r_flt.parquet_persistent_aggregate_states, 0)

                # 1c. Context SQL: Track B1 + B2 early key filter + join_aggregate pushdown.
                sql_join_agg = (
                    f"SELECT d.region AS region, SUM(f.val_i64) AS sum_v "
                    f"FROM '{fact_pq}' AS f INNER JOIN '{dim_pq}' AS d ON f.k = d.k "
                    f"WHERE d.tier = 1 GROUP BY d.region ORDER BY d.region ASC"
                )
                r_ja = ctx.sql(sql_join_agg).collect(check=True)
                self.assertEqual(r_ja.to_arrow().to_pylist(), fx["ref_join_agg"])
                self.assertFalse(r_ja.fallback_attempted)
                self.assertEqual(r_ja.parquet_key_filter_rows_decoded, 16_384)
                self.assertIsNotNone(r_ja.parquet_payload_rows_decoded)
                self.assertLess(r_ja.parquet_payload_rows_decoded, 16_384)
                join_work = json.loads(r_ja.envelope.field("local_primitive_native_io_join_work_json"))
                self.assertGreaterEqual(join_work["direct_aggregate"]["probe_filters_pushed"], 1)

                # 1d. Prepared .vortex fact control through SQL join_aggregate.
                sql_vortex_ja = (
                    f"SELECT d.region AS region, SUM(f.val_i64) AS sum_v "
                    f"FROM '{vortex_fact}' AS f INNER JOIN '{dim_pq}' AS d ON f.k = d.k "
                    f"WHERE d.tier = 1 GROUP BY d.region ORDER BY d.region ASC"
                )
                r_vtx = ctx.sql(sql_vortex_ja).collect(check=True)
                self.assertEqual(r_vtx.to_arrow().to_pylist(), fx["ref_join_agg"])
                join_work_vtx = json.loads(r_vtx.envelope.field("local_primitive_native_io_join_work_json"))
                self.assertGreaterEqual(join_work_vtx["direct_aggregate"]["probe_filters_pushed"], 1)

                # 1e. Arrow IPC non-Parquet control: omits native_input_parquet_* metrics cleanly.
                sql_ipc = f"SELECT grp, COUNT(*) AS cnt FROM '{ipc_path}' GROUP BY grp ORDER BY grp ASC"
                r_ipc = ctx.sql(sql_ipc).collect(check=True)
                self.assertEqual(
                    r_ipc.to_arrow().to_pylist(),
                    [{"grp": row["grp"], "cnt": row["cnt"]} for row in fx["ref_agg"]],
                )
                self.assertIsNone(r_ipc.parquet_parallel_workers)
                self.assertIsNone(r_ipc.parquet_key_filter_rows_decoded)
                self.assertEqual(r_ipc.envelope.field("native_source_demand_formats"), "arrow-ipc")

                # 2. LazyFrame API over Parquet + Session API over Parquet.
                lf_agg = (
                    ctx.read_parquet(fact_pq)
                    .group_by("grp")
                    .agg(
                        cnt="count(*)",
                        sum_v="sum(val_i64)",
                        min_v="min(val_i64)",
                        max_v="max(val_i64)",
                    )
                    .sort("grp")
                    .collect(check=True)
                )
                self.assertEqual(lf_agg.to_arrow().to_pylist(), fx["ref_agg"])
                self.assertGreaterEqual(lf_agg.parquet_persistent_aggregate_states, 2)

                lf_join = (
                    ctx.read_parquet(fact_pq)
                    .join(ctx.read_parquet(dim_pq).filter("tier = 1"), condition="f.k = d.k", how="inner")
                    .select("d.region AS region", "f.val_i64 AS val_i64")
                    .group_by("region")
                    .agg(sum_v="sum(val_i64)")
                    .sort("region")
                    .collect(check=True)
                )
                self.assertEqual(lf_join.to_arrow().to_pylist(), fx["ref_join_agg"])
                self.assertEqual(lf_join.parquet_key_filter_rows_decoded, 16_384)
                self.assertLess(lf_join.parquet_payload_rows_decoded, 16_384)

            # 3. Bound Python Session API (session.read_parquet -> DataFrame/LazyFrame).
            with sl.session(transport="native", memory_bytes=256 << 20, max_parallelism=4) as py_sess:
                r_sess = (
                    py_sess.read_parquet(fact_pq)
                    .group_by("grp")
                    .agg(
                        cnt="count(*)",
                        sum_v="sum(val_i64)",
                        min_v="min(val_i64)",
                        max_v="max(val_i64)",
                    )
                    .sort("grp")
                    .collect(check=True)
                )
                self.assertEqual(r_sess.to_arrow().to_pylist(), fx["ref_agg"])
                self.assertGreaterEqual(r_sess.parquet_parallel_workers, 2)
                self.assertGreaterEqual(r_sess.parquet_persistent_aggregate_states, 2)

            # 4. Typed native.Relation + embedded native.Session directly.
            session = self.native.Session(self.native.ExecutionResources(256 << 20, 4))
            try:
                fact_rel = self.native.Relation.scan(str(fact_pq))
                agg_rel = fact_rel.aggregate(
                    ["grp"],
                    [
                        ("count", None, "cnt"),
                        ("sum", "val_i64", "sum_v"),
                        ("min", "val_i64", "min_v"),
                        ("max", "val_i64", "max_v"),
                    ],
                )
                nat_agg = session.plan(agg_rel, files=[(str(fact_pq), "parquet", None)]).collect()
                nat_agg_rows = _sorted_rows(
                    self.pa.RecordBatchReader.from_stream(nat_agg).read_all().to_pylist(), "grp"
                )
                self.assertEqual(nat_agg_rows, fx["ref_agg"])
                self.assertGreaterEqual(int(nat_agg.report["native_input_parquet_parallel_workers"]), 2)
                self.assertGreaterEqual(
                    int(nat_agg.report["native_input_parquet_persistent_aggregate_states"]), 2
                )

                dim_rel = self.native.Relation.scan(str(dim_pq)).filter(
                    self.native.Expression.column("tier") == self.native.Expression.literal(1)
                )
                ja_rel = fact_rel.join(
                    dim_rel,
                    [("k", "k")],
                    [("right", "region", "region"), ("left", "val_i64", "val_i64")],
                    kind="inner",
                ).aggregate(["region"], [("sum", "val_i64", "sum_v")])
                nat_ja = session.plan(
                    ja_rel,
                    files=[(str(fact_pq), "parquet", None), (str(dim_pq), "parquet", None)],
                ).collect()
                nat_ja_rows = _sorted_rows(
                    self.pa.RecordBatchReader.from_stream(nat_ja).read_all().to_pylist(), "region"
                )
                self.assertEqual(nat_ja_rows, fx["ref_join_agg"])
                self.assertEqual(nat_ja.report["native_input_parquet_key_filter_rows_decoded"], "16384")
                self.assertLess(int(nat_ja.report["native_input_parquet_payload_rows_decoded"]), 16_384)
                nat_jw = json.loads(nat_ja.report["local_primitive_native_io_join_work_json"])
                self.assertGreaterEqual(nat_jw["direct_aggregate"]["probe_filters_pushed"], 1)
            finally:
                session.close()


if __name__ == "__main__":
    unittest.main()
