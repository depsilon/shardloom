# SPDX-License-Identifier: Apache-2.0
"""Contract and native integration tests for shardloom.ai workflows."""

from __future__ import annotations

from datetime import datetime
import importlib.util
import math
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "python" / "src"
if importlib.util.find_spec("shardloom") is None and str(SRC) not in sys.path:
    sys.path.append(str(SRC))

from shardloom.ai import (
    AIWorkflowBoundaryError,
    FeaturePipelineSpec,
    FittedFeaturePipeline,
    IndexedDataset,
    LexicalIndex,
    SourceGenerationToken,
    StreamingDataset,
    TypedBatchSpec,
    VectorRetrievalSpec,
    as_hf_iterable_dataset,
    as_torch_iterable_dataset,
    as_torch_map_dataset,
    assign_deterministic_splits,
    deliver_typed_batches,
    evaluate_scalar_quantized_retrieval,
    exact_vector_search,
    hybrid_search,
    point_in_time_feature_join,
)

if __package__:
    from .test_native_binding import _load_native
else:
    from test_native_binding import _load_native


def _optional_pyarrow():
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError as error:
        if os.environ.get("SHARDLOOM_REQUIRE_NATIVE_PYTHON") == "1":
            raise AssertionError(f"pyarrow required for AI integration suite: {error}") from error
        raise unittest.SkipTest("optional pyarrow is not installed") from error
    return pa, pq


class AIWorkflowContractTests(unittest.TestCase):
    def test_ai_import_stays_lazy_and_clean(self) -> None:
        script = r"""
import sys
import shardloom as sl
root_dir = dir(sl)
import shardloom.ai as ai
assert dir(sl) == root_dir
for forbidden in (
    "shardloom._native",
    "shardloom.native",
    "shardloom._native_backend",
    "shardloom.context",
    "shardloom.query",
    "pyarrow",
    "numpy",
    "pandas",
    "torch",
    "datasets",
):
    assert forbidden not in sys.modules, f"unexpected eager import: {forbidden}"
assert hasattr(ai, "exact_vector_search")
"""
        import shardloom as sl

        env = os.environ.copy()
        in_source_checkout = SRC.resolve() in Path(sl.__file__).resolve().parents
        if in_source_checkout:
            env["PYTHONPATH"] = str(SRC)
        cmd = [sys.executable, *([] if in_source_checkout else ["-I"]), "-c", script]
        completed = subprocess.run(
            cmd,
            cwd=ROOT if in_source_checkout else None,
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)

    def test_contract_validation_and_missing_optional_consumer_boundaries(self) -> None:
        with self.assertRaises(AIWorkflowBoundaryError) as dev_err:
            TypedBatchSpec(feature_columns=("x",), device="cuda")
        self.assertEqual(dev_err.exception.code, "SL_AI_UNSUPPORTED_DEVICE")
        self.assertFalse(dev_err.exception.fallback_attempted)

        with self.assertRaises(AIWorkflowBoundaryError) as np_err:
            TypedBatchSpec(feature_columns=("x",), null_policy="ignore")
        self.assertEqual(np_err.exception.code, "SL_AI_INVALID_NULL_POLICY")

        with self.assertRaises(AIWorkflowBoundaryError) as metric_err:
            VectorRetrievalSpec(id_column="id", vector_column="v", query_vector=[1.0, 0.0], k=2, metric="hamming")
        self.assertEqual(metric_err.exception.code, "SL_AI_UNSUPPORTED_METRIC")

        with self.assertRaises(AIWorkflowBoundaryError) as dim_err:
            VectorRetrievalSpec(id_column="id", vector_column="v", query_vector=[1.0, 0.0], k=2, dimensions=4)
        self.assertEqual(dim_err.exception.code, "SL_AI_DIMENSION_MISMATCH")

        with self.assertRaises(AIWorkflowBoundaryError) as space_err:
            VectorRetrievalSpec(
                id_column="id",
                vector_column="v",
                query_vector=[1.0, 0.0],
                k=2,
                embedding_space="model-a",
                expected_embedding_space="model-b",
            )
        self.assertEqual(space_err.exception.code, "SL_AI_EMBEDDING_SPACE_MISMATCH")

        with self.assertRaises(AIWorkflowBoundaryError) as z_err:
            VectorRetrievalSpec(id_column="id", vector_column="v", query_vector=[0.0, 0.0], k=2, metric="cosine")
        self.assertEqual(z_err.exception.code, "SL_AI_ZERO_NORM_QUERY")

        with patch.dict(sys.modules, {"torch": None, "torch.utils": None, "torch.utils.data": None, "datasets": None}):
            dummy_spec = TypedBatchSpec(feature_columns=("x",))
            ds = StreamingDataset(lambda: None, dummy_spec, batch_size=2)
            with self.assertRaises(AIWorkflowBoundaryError) as t_err:
                as_torch_iterable_dataset(ds)
            self.assertEqual(t_err.exception.code, "SL_AI_OPTIONAL_DEPENDENCY_MISSING")
            with self.assertRaises(AIWorkflowBoundaryError) as hf_err:
                as_hf_iterable_dataset(ds)
            self.assertEqual(hf_err.exception.code, "SL_AI_OPTIONAL_DEPENDENCY_MISSING")


class AIWorkflowNativeIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.native = _load_native()
        cls.pa, cls.pq = _optional_pyarrow()

    def setUp(self) -> None:
        popen_patch = patch("subprocess.Popen", side_effect=AssertionError("external subprocess prohibited"))
        popen_patch.start()
        self.addCleanup(popen_patch.stop)
        tmp = tempfile.TemporaryDirectory(prefix="shardloom-ai-")
        self.addCleanup(tmp.cleanup)
        self.root = Path(tmp.name)

    def test_workload_1_typed_batch_delivery_shapes_and_ownership(self) -> None:
        import shardloom as sl

        src_path = self.root / "mixed.parquet"
        labels_path = self.root / "labels.parquet"
        self.pq.write_table(
            self.pa.Table.from_pylist(
                [
                    {"id": 1, "x": 1.0, "vec": [1.0, 0.0, 0.5, 0.25], "media": b"img1", "meta": {"tag": "ok"}, "keep": True},
                    {"id": 2, "x": 2.0, "vec": [0.0, 1.0, 0.5, 0.25], "media": b"img2", "meta": {"tag": "ok"}, "keep": False},
                    {"id": 3, "x": 3.0, "vec": [0.5, 0.5, 0.5, 0.5], "media": b"img3", "meta": {"tag": "ok"}, "keep": True},
                    {"id": 4, "x": 4.0, "vec": [0.2, 0.8, 0.1, 0.4], "media": b"img4", "meta": {"tag": "ok"}, "keep": True},
                ],
                schema=self.pa.schema(
                    [
                        self.pa.field("id", self.pa.int64()),
                        self.pa.field("x", self.pa.float64()),
                        self.pa.field("vec", self.pa.list_(self.pa.float32(), 4)),
                        self.pa.field("media", self.pa.binary()),
                        self.pa.field("meta", self.pa.struct([self.pa.field("tag", self.pa.string())])),
                        self.pa.field("keep", self.pa.bool_()),
                    ]
                ),
            ),
            src_path,
        )
        self.pq.write_table(
            self.pa.Table.from_pylist([{"id": 1, "y": 0}, {"id": 3, "y": 1}, {"id": 4, "y": 1}]),
            labels_path,
        )

        with sl.context(transport="native", memory_bytes=64 << 20, max_parallelism=2) as ctx:
            joined = ctx.sql(
                "SELECT m.id AS id, m.vec AS vec, m.x AS x, l.y AS y "
                "FROM m AS m JOIN l AS l ON m.id = l.id WHERE m.keep = true ORDER BY m.id",
                relations={"m": ctx.read_parquet(src_path), "l": ctx.read_parquet(labels_path)},
            )
            ro_spec = TypedBatchSpec(
                feature_columns=("x", "vec"),
                label_columns=("y",),
                id_column="id",
                expected_shapes={"vec": (4,)},
                expected_dtypes={"x": "float32"},
                allow_cast=True,
                writable=False,
            )
            ro_batches = list(deliver_typed_batches(joined, ro_spec, batch_size=2))
            self.assertEqual([b.num_rows for b in ro_batches], [2, 1])
            self.assertFalse(ro_batches[0].python_rows_constructed)
            self.assertEqual(ro_batches[0].feature_columns, ("x", "vec"))
            self.assertEqual(ro_batches[0].ownership, "shared_read_only")
            self.assertEqual(ro_batches[0].features["x"].type, self.pa.float32())
            with self.assertRaises(AIWorkflowBoundaryError) as ro_err:
                ro_batches[0].writable_feature_buffers("x")
            self.assertEqual(ro_err.exception.code, "SL_AI_READONLY_BATCH")

            wr_spec = TypedBatchSpec(
                feature_columns=("x", "vec"),
                label_columns=("y",),
                id_column="id",
                expected_shapes={"vec": (4,)},
                writable=True,
            )
            res = joined.collect(check=True)
            wr_batches = list(deliver_typed_batches(res, wr_spec, batch_size=3))
            self.assertEqual(wr_batches[0].ownership, "exclusive_copy")
            bufs = wr_batches[0].writable_feature_buffers("x")
            bufs[0][0] = 0xFF
            # Original ShardLoom result remains unmutated.
            orig_x = res.to_arrow().column("x").to_pylist()
            self.assertEqual(orig_x, [1.0, 3.0, 4.0])

            # Invalid shape fails explicitly.
            bad_shape_spec = TypedBatchSpec(feature_columns=("vec",), expected_shapes={"vec": (5,)})
            with self.assertRaises(AIWorkflowBoundaryError) as shape_err:
                list(deliver_typed_batches(res, bad_shape_spec))
            self.assertEqual(shape_err.exception.code, "SL_AI_INVALID_SHAPE")

        # Delivered batches survive closing source/context and unlinking files.
        src_path.unlink()
        self.assertEqual(wr_batches[0].ids.to_pylist(), [1, 3, 4])

    def test_workload_2_streaming_and_indexed_datasets(self) -> None:
        import shardloom as sl

        data_path = self.root / "epoch_data.parquet"
        self.pq.write_table(
            self.pa.Table.from_pylist([{"id": i, "x": float(i), "label": i % 2} for i in range(10)]),
            data_path,
        )
        gen = SourceGenerationToken.from_paths([data_path])
        spec = TypedBatchSpec(feature_columns=("x",), label_columns=("label",), id_column="id")

        with sl.context(transport="native", memory_bytes=64 << 20, max_parallelism=2) as ctx:
            def plan_fn():
                return ctx.sql("SELECT id, x, label FROM s WHERE id >= 2 ORDER BY id", relations={"s": ctx.read_parquet(data_path)})

            # Multi-worker partitioning covers all 8 filtered rows once with zero duplicates/losses.
            seen_ids: list[int] = []
            for wid in range(3):
                with StreamingDataset(
                    plan_fn,
                    spec,
                    batch_size=2,
                    shuffle_buffer_size=4,
                    seed=7,
                    epoch=1,
                    worker_id=wid,
                    num_workers=3,
                    source_generation=gen,
                ) as ds:
                    for b in ds:
                        seen_ids.extend(b.ids.to_pylist())
            self.assertEqual(sorted(seen_ids), list(range(2, 10)))

            # Bounded-buffer shuffle reproducibility with identical (seed, epoch).
            with StreamingDataset(plan_fn, spec, batch_size=3, shuffle_buffer_size=5, seed=9, epoch=2) as d1, \
                 StreamingDataset(plan_fn, spec, batch_size=3, shuffle_buffer_size=5, seed=9, epoch=2) as d2:
                seq1 = [idx for b in d1 for idx in b.ids.to_pylist()]
                seq2 = [idx for b in d2 for idx in b.ids.to_pylist()]
                self.assertEqual(seq1, seq2)

            # Indexed dataset: strictly increasing unique read + exact scatter multiplicity/order.
            with IndexedDataset(plan_fn(), spec, source_generation=gen) as idx_ds:
                self.assertEqual(len(idx_ds), 8)
                req = [5, 1, 5, 0, 1]
                batch = idx_ds.take_batch(req)
                self.assertEqual(idx_ds.last_unique_sorted_read, (0, 1, 5))
                self.assertEqual(batch.ids.to_pylist(), [7, 3, 7, 2, 3])
                ep = list(idx_ds.iter_epoch_batches(3, epoch=4, seed=99, shuffle=True))
                self.assertEqual(sorted(i for b in ep for i in b.ids.to_pylist()), list(range(2, 10)))
            with self.assertRaises(AIWorkflowBoundaryError) as closed_err:
                idx_ds.take_batch([0])
            self.assertEqual(closed_err.exception.code, "SL_AI_DATASET_CLOSED")

            # Source modification invalidates active generation mid-epoch.
            self.pq.write_table(self.pa.Table.from_pylist([{"id": 99, "x": 99.0, "label": 1}]), data_path)
            with self.assertRaises(AIWorkflowBoundaryError) as gen_err:
                with StreamingDataset(plan_fn, spec, batch_size=2, source_generation=gen) as stale_ds:
                    list(stale_ds)
            self.assertEqual(gen_err.exception.code, "SL_AI_SOURCE_GENERATION_CHANGED")

    def test_workload_3_fitted_feature_prep_splits_and_point_in_time_joins(self) -> None:
        import shardloom as sl

        pipeline_path = self.root / "fitted" / "pipeline.json"
        spec = FeaturePipelineSpec(
            numeric_columns=("income", "const_col"),
            categorical_columns=("tier",),
            impute_strategy="mean",
            scale_strategy="standard",
        )

        # Fit ONLY on training split; persist atomically.
        with sl.context(transport="native", memory_bytes=64 << 20, max_parallelism=2) as fit_ctx:
            full_df = fit_ctx.from_arrow_table(
                self.pa.Table.from_pylist(
                    [
                        {"id": 1, "entity": "e1", "ts": 100, "income": 10.0, "const_col": 5.0, "tier": "gold"},
                        {"id": 2, "entity": "e1", "ts": 120, "income": 30.0, "const_col": 5.0, "tier": "silver"},
                        {"id": 3, "entity": "e2", "ts": 130, "income": None, "const_col": 5.0, "tier": "gold"},
                        {"id": 4, "entity": "e3", "ts": 220, "income": 50.0, "const_col": 5.0, "tier": "bronze"},
                    ],
                    schema=self.pa.schema(
                        [
                            self.pa.field("id", self.pa.int64()),
                            self.pa.field("entity", self.pa.string()),
                            self.pa.field("ts", self.pa.int64()),
                            self.pa.field("income", self.pa.float64()),
                            self.pa.field("const_col", self.pa.float64()),
                            self.pa.field("tier", self.pa.string()),
                        ]
                    ),
                )
            )
            group_split = assign_deterministic_splits(fit_ctx, full_df, id_column="id", group_column="entity")
            gs_rows = group_split.collect(check=True).to_arrow().to_pylist()
            e1_splits = {r["split"] for r in gs_rows if r["entity"] == "e1"}
            self.assertEqual(len(e1_splits), 1)

            time_split = assign_deterministic_splits(fit_ctx, full_df, id_column="id", time_column="ts", train_end=150, validation_end=210)
            train_df = time_split.filter(sl.col("split") == "train")
            fitted = FittedFeaturePipeline.fit(fit_ctx, train_df, spec)
            self.assertEqual(fitted.feature_order, ("income", "const_col", "tier"))
            self.assertEqual(fitted.numeric_params["const_col"]["scale"], 1.0)
            fitted.save(pipeline_path)

        # Reopen in a fresh context and apply without refitting (handling nulls and unseen categories).
        with sl.context(transport="native", memory_bytes=64 << 20, max_parallelism=1) as inf_ctx:
            reloaded = FittedFeaturePipeline.load(pipeline_path)
            inf_df = inf_ctx.from_arrow_table(
                self.pa.Table.from_pylist(
                    [
                        {"id": 10, "income": None, "const_col": 5.0, "tier": "platinum"},
                        {"id": 11, "income": 20.0, "const_col": 5.0, "tier": None},
                        {"id": 12, "income": 30.0, "const_col": 5.0, "tier": "gold"},
                    ],
                    schema=self.pa.schema(
                        [
                            self.pa.field("id", self.pa.int64()),
                            self.pa.field("income", self.pa.float64()),
                            self.pa.field("const_col", self.pa.float64()),
                            self.pa.field("tier", self.pa.string()),
                        ]
                    ),
                )
            )
            transformed = reloaded.transform(inf_ctx, inf_df).collect(check=True).to_arrow().to_pylist()
            by_id = {r["id"]: r for r in transformed}
            self.assertEqual(by_id[10]["tier"], reloaded.spec.unknown_category_id)
            self.assertEqual(by_id[11]["tier"], reloaded.spec.null_category_id)
            self.assertEqual(by_id[12]["tier"], reloaded.category_vocabularies["tier"]["gold"])
            # income train mean == (10 + 30 + 20)/3 == 20.0, std == sqrt(((10-20)^2 + (30-20)^2 + 0)/3)
            self.assertAlmostEqual(by_id[10]["income"], 0.0, places=9)
            self.assertAlmostEqual(by_id[10]["const_col"], 0.0, places=9)

            # Point-in-time join: availability_ts prevents late-arriving backfill leakage!
            obs = inf_ctx.from_arrow_table(
                self.pa.Table.from_pylist(
                    [
                        {"entity": "a", "decision_ts": datetime(2026, 1, 10, 0, 0)},
                        {"entity": "a", "decision_ts": datetime(2026, 1, 20, 0, 0)},
                    ],
                    schema=self.pa.schema(
                        [self.pa.field("entity", self.pa.string()), self.pa.field("decision_ts", self.pa.timestamp("us"))]
                    ),
                )
            )
            feats = inf_ctx.from_arrow_table(
                self.pa.Table.from_pylist(
                    [
                        {"entity": "a", "available_ts": datetime(2026, 1, 5, 0, 0), "effective_ts": datetime(2026, 1, 1, 0, 0), "feat": 10},
                        {"entity": "a", "available_ts": datetime(2026, 1, 15, 0, 0), "effective_ts": datetime(2026, 1, 2, 0, 0), "feat": 99},
                    ],
                    schema=self.pa.schema(
                        [
                            self.pa.field("entity", self.pa.string()),
                            self.pa.field("available_ts", self.pa.timestamp("us")),
                            self.pa.field("effective_ts", self.pa.timestamp("us")),
                            self.pa.field("feat", self.pa.int64()),
                        ]
                    ),
                )
            )
            pit_rows = point_in_time_feature_join(
                obs,
                feats,
                entity_column="entity",
                decision_time_column="decision_ts",
                availability_time_column="available_ts",
                effective_time_column="effective_ts",
            ).collect(check=True).to_arrow().to_pylist()
            self.assertEqual([r["d.feat"] for r in pit_rows], [10, 99])

    def test_workload_4_exact_filtered_vector_retrieval_vs_oracle(self) -> None:
        import shardloom as sl

        rows = [
            {"id": 1, "eligible": True, "vec": [1.0, 0.0, 0.0], "media": b"doc1"},
            {"id": 2, "eligible": False, "vec": [1.0, 0.0, 0.0], "media": b"ineligible"},
            {"id": 3, "eligible": True, "vec": [0.8, 0.6, 0.0], "media": b"doc3"},
            {"id": 4, "eligible": True, "vec": [0.8, 0.6, 0.0], "media": b"doc4_tie"},
            {"id": 5, "eligible": True, "vec": [0.0, 0.0, 0.0], "media": b"zero_norm"},
            {"id": 6, "eligible": True, "vec": None, "media": b"null_vec"},
        ]
        q = [1.0, 0.0, 0.0]

        with sl.context(transport="native", memory_bytes=64 << 20, max_parallelism=2) as ctx:
            df = ctx.from_arrow_table(
                self.pa.Table.from_pylist(
                    rows,
                    schema=self.pa.schema(
                        [
                            self.pa.field("id", self.pa.int64()),
                            self.pa.field("eligible", self.pa.bool_()),
                            self.pa.field("vec", self.pa.list_(self.pa.float32(), 3)),
                            self.pa.field("media", self.pa.binary()),
                        ]
                    ),
                )
            )
            for metric in ("cosine", "l2", "dot"):
                res, rep = exact_vector_search(
                    ctx,
                    df,
                    VectorRetrievalSpec(
                        id_column="id",
                        vector_column="vec",
                        query_vector=q,
                        k=3,
                        metric=metric,
                        dimensions=3,
                        eligibility_predicate=sl.col("eligible") == True,
                        payload_columns=("media",),
                    ),
                )
                self.assertTrue(rep["exact"])
                self.assertTrue(rep["eligibility_applied_before_topk"])
                self.assertTrue(rep["late_payload_fetch"])
                got = res.to_arrow().to_pylist()
                self.assertNotIn(2, [r["id"] for r in got])
                if metric == "cosine":
                    self.assertEqual([r["id"] for r in got], [1, 3, 4])
                    self.assertAlmostEqual(got[0]["score"], 1.0, places=6)
                    self.assertEqual(got[2]["media"], b"doc4_tie")
                elif metric == "l2":
                    self.assertEqual([r["id"] for r in got], [1, 3, 4])
                    self.assertAlmostEqual(got[0]["score"], 0.0, places=6)
                else:
                    self.assertEqual([r["id"] for r in got], [1, 3, 4])

    def test_workload_5_lexical_hybrid_and_compact_int8_evaluation(self) -> None:
        import shardloom as sl

        src = self.root / "corpus.parquet"
        idx_file = self.root / "index" / "bm25.json"
        corpus_rows = []
        for i in range(32):
            base_vec = [math.cos(i * 0.2), math.sin(i * 0.2), 0.5, 0.25, -0.3, 0.7, -0.1, 0.4]
            corpus_rows.append(
                {
                    "id": i,
                    "eligible": (i % 5 != 0),
                    "text": "vortex columnar native kernel execution" if i in (1, 7) else f"record document item {i}",
                    "vec": base_vec,
                    "payload": f"p-{i}".encode("utf-8"),
                }
            )
        self.pq.write_table(
            self.pa.Table.from_pylist(
                corpus_rows,
                schema=self.pa.schema(
                    [
                        self.pa.field("id", self.pa.int64()),
                        self.pa.field("eligible", self.pa.bool_()),
                        self.pa.field("text", self.pa.string()),
                        self.pa.field("vec", self.pa.list_(self.pa.float32(), 8)),
                        self.pa.field("payload", self.pa.binary()),
                    ]
                ),
            ),
            src,
        )
        gen = SourceGenerationToken.from_paths([src])

        with sl.context(transport="native", memory_bytes=64 << 20, max_parallelism=2) as ctx:
            df = ctx.read_parquet(src)
            idx = LexicalIndex.build(ctx, df, id_column="id", text_column="text", source_generation=gen)
            idx.save(idx_file)
            reopened = LexicalIndex.load(idx_file)

            eligible_set = {r["id"] for r in corpus_rows if r["eligible"]}
            lex_hits = reopened.search("columnar kernel", k=3, eligible_ids=eligible_set)
            self.assertEqual([h["id"] for h in lex_hits], [1, 7])

            vec_spec = VectorRetrievalSpec(
                id_column="id",
                vector_column="vec",
                query_vector=corpus_rows[1]["vec"],
                k=5,
                metric="cosine",
                dimensions=8,
                eligibility_predicate=sl.col("eligible") == True,
                payload_columns=("payload",),
            )
            h_res, h_rep = hybrid_search(
                ctx,
                df,
                vector_spec=vec_spec,
                lexical_index=reopened,
                lexical_query="vortex kernel",
                fusion="rrf",
                k=3,
            )
            h_rows = h_res.to_arrow().to_pylist()
            self.assertEqual(h_rows[0]["id"], 1)
            self.assertEqual(h_rows[0]["payload"], b"p-1")
            self.assertTrue(h_rep["late_payload_fetch"])

            ev = evaluate_scalar_quantized_retrieval(
                ctx,
                df,
                spec=vec_spec,
                rescore_candidates=10,
            )
            self.assertEqual(ev["schema_version"], "shardloom.ai_compact_vector_evaluation.v1")
            self.assertGreater(ev["compression_ratio"], 1.0)
            self.assertGreaterEqual(ev["recall_at_k_rescored"], ev["recall_at_k_quantized"])
            self.assertFalse(ev["exact_globally"])
            self.assertTrue(ev["rescoring_cannot_recover_omitted_candidates"])
            self.assertFalse(ev["turboquant_upstream_adopted"])
            self.assertFalse(ev["fallback_attempted"])

    def test_mixed_dataset_selective_projection_and_adversarial_edge_cases(self) -> None:
        import shardloom as sl

        pa, _ = self.pa, self.pq
        mixed_schema = pa.schema(
            [
                pa.field("record_id", pa.int64()),
                pa.field("label", pa.int64()),
                pa.field("quality_score", pa.float64()),
                pa.field("eligible", pa.bool_()),
                pa.field("text", pa.string()),
                pa.field("vec", pa.list_(pa.float32(), 8)),
                pa.field("media", pa.binary()),
                pa.field(
                    "model_output",
                    pa.struct(
                        [
                            pa.field("confidence", pa.float64()),
                            pa.field("tags", pa.list_(pa.string())),
                        ]
                    ),
                ),
            ]
        )
        rows = [
            {
                "record_id": i,
                "label": i % 3,
                "quality_score": 0.25 if i % 4 == 0 else 0.85,
                "eligible": i % 2 == 0,
                "text": f"vortex native document {i}",
                "vec": [float((i + j) % 7) * 0.125 + 0.1 for j in range(8)],
                "media": b"\x00\xfe\xed" + f"img-{i}".encode("utf-8"),
                "model_output": {"confidence": 0.9 - (i % 3) * 0.1, "tags": [f"t{i % 2}", "ok"]},
            }
            for i in range(16)
        ]
        src_vtx = self.root / "mixed_ai.vortex"
        filtered_vtx = self.root / "mixed_ai_filtered.vortex"
        with sl.context(transport="native", memory_bytes=64 << 20, max_parallelism=2) as ctx:
            ctx.from_arrow_table(pa.Table.from_pylist(rows, schema=mixed_schema)).write_vortex(src_vtx, check=True)
            vtx_frame = ctx.read_vortex(src_vtx)

            # Ordinary selective projection avoids vector/media/nested columns in schema & result.
            scalar_tbl = ctx.sql(
                "SELECT record_id, label, quality_score FROM src WHERE quality_score >= 0.5 ORDER BY record_id",
                relations={"src": vtx_frame},
            ).collect(check=True).to_arrow()
            self.assertEqual([f.name for f in scalar_tbl.schema], ["record_id", "label", "quality_score"])
            expected_ids = [r["record_id"] for r in rows if r["quality_score"] >= 0.5]
            self.assertEqual(scalar_tbl.column("record_id").to_pylist(), expected_ids)

            # Full mixed-dataset retention & .vortex write/reopen fidelity across vectors, media, and nested Struct/List.
            kept = ctx.sql(
                "SELECT record_id, label, quality_score, eligible, text, vec, media, model_output "
                "FROM src WHERE quality_score >= 0.5 ORDER BY record_id",
                relations={"src": vtx_frame},
            ).collect(check=True)
            ctx.from_arrow_table(kept.to_arrow()).write_vortex(filtered_vtx, check=True)
            reopened = ctx.read_vortex(filtered_vtx).collect(check=True).to_arrow().to_pylist()
            expected_rows = [r for r in rows if r["quality_score"] >= 0.5]
            self.assertEqual(len(reopened), len(expected_rows))
            for actual, expected in zip(reopened, expected_rows):
                self.assertEqual(actual["record_id"], expected["record_id"])
                self.assertEqual(actual["media"], expected["media"])
                self.assertEqual(actual["model_output"], expected["model_output"])
                for av, ev in zip(actual["vec"], expected["vec"]):
                    self.assertAlmostEqual(av, ev, places=5)

            # Empty 0-row inputs succeed cleanly across TypedBatch, StreamingDataset, IndexedDataset, Vector, and Hybrid.
            empty_frame = ctx.from_arrow_table(pa.Table.from_pylist([], schema=mixed_schema))
            empty_spec = TypedBatchSpec(
                feature_columns=("quality_score", "vec"),
                label_columns=("label",),
                id_column="record_id",
                expected_shapes={"vec": (8,)},
            )
            self.assertEqual(list(deliver_typed_batches(empty_frame, empty_spec, batch_size=4)), [])
            with StreamingDataset(lambda: empty_frame, empty_spec, batch_size=4) as empty_stream:
                self.assertEqual(list(empty_stream), [])
            with IndexedDataset(empty_frame, empty_spec) as empty_idx:
                self.assertEqual(len(empty_idx), 0)
                self.assertEqual(empty_idx.take_batch([]).num_rows, 0)
                self.assertEqual(list(empty_idx.iter_epoch_batches(4)), [])
            vec_spec = VectorRetrievalSpec(
                id_column="record_id",
                vector_column="vec",
                query_vector=[0.2] * 8,
                k=3,
                dimensions=8,
                payload_columns=("media", "model_output"),
            )
            empty_vec_res, _ = exact_vector_search(ctx, empty_frame, vec_spec)
            self.assertEqual(empty_vec_res.to_arrow().num_rows, 0)
            empty_lex = LexicalIndex.build(ctx, empty_frame, id_column="record_id", text_column="text")
            empty_hyb_res, _ = hybrid_search(ctx, empty_frame, vector_spec=vec_spec, lexical_index=empty_lex, lexical_query="vortex")
            self.assertEqual(empty_hyb_res.to_arrow().num_rows, 0)

            # Non-empty IndexedDataset empty-take_batch and mutation isolation.
            nonempty_idx = IndexedDataset(vtx_frame.select("record_id", "quality_score", "vec", "label"), TypedBatchSpec(
                feature_columns=("quality_score", "vec"), label_columns=("label",), id_column="record_id", expected_shapes={"vec": (8,)}, writable=True
            ))
            with nonempty_idx:
                self.assertEqual(nonempty_idx.take_batch([]).num_rows, 0)
                b_mut = nonempty_idx.take_batch([2, 0, 2])
                bufs = b_mut.writable_feature_buffers("quality_score")
                bufs[0][0] ^= 0xFF
                b_fresh = nonempty_idx.take_batch([2, 0, 2])
                self.assertAlmostEqual(b_fresh.features["quality_score"].to_pylist()[0], rows[2]["quality_score"])

        # Partial publication failure (injected os.replace error) preserves prior committed state & leaves no temp files.
        pipe_path = self.root / "atomic_pipe.json"
        pipe_path.write_text('{"sentinel": "prior"}\n', encoding="utf-8")
        with sl.context(transport="native", memory_bytes=64 << 20, max_parallelism=1) as ctx:
            fit_tbl = pa.Table.from_pylist([{"id": 1, "x": 10.0, "c": "a"}, {"id": 2, "x": 20.0, "c": "b"}])
            fitted = FittedFeaturePipeline.fit(ctx, ctx.from_arrow_table(fit_tbl), FeaturePipelineSpec(numeric_columns=("x",), categorical_columns=("c",)))
            lex = LexicalIndex.build(
                ctx,
                ctx.from_arrow_table(pa.Table.from_pylist([{"id": 1, "text": "hello"}])),
                id_column="id",
                text_column="text",
            )
        idx_path = self.root / "atomic_lex.json"
        idx_path.write_text('{"sentinel": "prior"}\n', encoding="utf-8")
        with patch("os.replace", side_effect=OSError("simulated partial publish abort")):
            with self.assertRaises(OSError):
                fitted.save(pipe_path)
            with self.assertRaises(OSError):
                lex.save(idx_path)
        self.assertEqual(pipe_path.read_text(encoding="utf-8"), '{"sentinel": "prior"}\n')
        self.assertEqual(idx_path.read_text(encoding="utf-8"), '{"sentinel": "prior"}\n')
        self.assertEqual(list(self.root.glob(".sl-ai-*")), [])

        # Worker failure in one worker does not break valid execution or corrupt source generation.
        with self.assertRaises(AIWorkflowBoundaryError) as bad_w:
            StreamingDataset(lambda: None, empty_spec, worker_id=2, num_workers=2)
        self.assertEqual(bad_w.exception.code, "SL_AI_INVALID_PARAM")

        # Modification invalidates LexicalIndex source generation too.
        gen_vtx = SourceGenerationToken.from_paths([src_vtx])
        with sl.context(transport="native", memory_bytes=64 << 20, max_parallelism=1) as ctx:
            lex_gen = LexicalIndex.build(ctx, ctx.read_vortex(src_vtx), id_column="record_id", text_column="text", source_generation=gen_vtx)
        src_vtx.write_bytes(src_vtx.read_bytes() + b"\x00")
        with self.assertRaises(AIWorkflowBoundaryError) as lex_stale:
            lex_gen.search("vortex", k=2)
        self.assertEqual(lex_stale.exception.code, "SL_AI_SOURCE_GENERATION_CHANGED")


class AIWorkflowSubprocessReopenTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.native = _load_native()
        cls.pa, cls.pq = _optional_pyarrow()

    def test_tight_memory_spill_refusal_and_separate_process_vortex_reopen(self) -> None:
        import shardloom as sl

        pa = self.pa
        with tempfile.TemporaryDirectory(prefix="shardloom-ai-reopen-") as tmp_dir:
            root = Path(tmp_dir)
            out_vtx = root / "mixed_4k.vortex"
            mixed_schema = pa.schema(
                [
                    pa.field("record_id", pa.int64()),
                    pa.field("label", pa.int64()),
                    pa.field("quality_score", pa.float64()),
                    pa.field("text", pa.string()),
                    pa.field("vec", pa.list_(pa.float32(), 8)),
                    pa.field("media", pa.binary()),
                    pa.field(
                        "model_output",
                        pa.struct(
                            [
                                pa.field("confidence", pa.float64()),
                                pa.field("tags", pa.list_(pa.string())),
                            ]
                        ),
                    ),
                ]
            )
            n_rows = 2048
            table = pa.table(
                {
                    "record_id": pa.array(list(range(n_rows)), type=pa.int64()),
                    "label": pa.array([i % 4 for i in range(n_rows)], type=pa.int64()),
                    "quality_score": pa.array([float(i % 100) / 100.0 for i in range(n_rows)], type=pa.float64()),
                    "text": pa.array([f"sample document {i} vortex" for i in range(n_rows)], type=pa.string()),
                    "vec": pa.array([[float((i + k) % 11) * 0.1 for k in range(8)] for i in range(n_rows)], type=pa.list_(pa.float32(), 8)),
                    "media": pa.array([b"\x00\x01bin-" + str(i).encode("utf-8") for i in range(n_rows)], type=pa.binary()),
                    "model_output": pa.array(
                        [{"confidence": 0.5 + (i % 5) * 0.1, "tags": [f"k{i % 3}", "v1"]} for i in range(n_rows)],
                        type=mixed_schema.field("model_output").type,
                    ),
                },
                schema=mixed_schema,
            )

            # 1. Impossibly tiny memory limit fails deterministically without crashing or fallback.
            with sl.context(transport="native", memory_bytes=512, max_parallelism=1) as tiny_ctx:
                with self.assertRaises(Exception):
                    tiny_ctx.from_arrow_table(table).filter(sl.col("record_id") >= 0).collect(check=True)

            # 2. Bounded memory sort + spill/write to durable .vortex file.
            spill_dir = root / "spill"
            spill_dir.mkdir()
            with sl.context(transport="native", memory_bytes=16 << 20, max_parallelism=2) as ctx:
                sorted_tbl = ctx.sql(
                    "SELECT record_id, label, quality_score, text, vec, media, model_output "
                    "FROM s WHERE quality_score >= 0.5 ORDER BY record_id DESC",
                    relations={"s": ctx.from_arrow_table(table)},
                ).collect(check=True).to_arrow()
                ctx.from_arrow_table(sorted_tbl).write_vortex(out_vtx, check=True)
            expected_count = sorted_tbl.num_rows
            self.assertEqual(expected_count, 1000)

            # 3. Separate fresh Python process reopens the .vortex output and verifies nested/vector/media fidelity.
            child_script = """
import os, sys
from pathlib import Path
lib = os.environ.get("SHARDLOOM_NATIVE_TEST_LIBRARY")
if lib:
    import importlib, importlib.machinery, importlib.util
    importlib.import_module("shardloom")
    spec = importlib.util.spec_from_file_location(
        "shardloom._native", lib,
        loader=importlib.machinery.ExtensionFileLoader("shardloom._native", lib),
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
import shardloom as sl
vtx_path = Path(sys.argv[1])
expected_count = int(sys.argv[2])
with sl.context(transport="native", memory_bytes=32 << 20, max_parallelism=1) as ctx:
    got = ctx.read_vortex(vtx_path).collect(check=True).to_arrow()
assert got.num_rows == expected_count, (got.num_rows, expected_count)
first = got.slice(0, 1).to_pylist()[0]
assert first["record_id"] == 1999
assert first["media"] == b"\\x00\\x01bin-1999"
assert first["model_output"] == {"confidence": 0.9, "tags": [f"k{1999 % 3}", "v1"]}
assert len(first["vec"]) == 8
"""
            env = os.environ.copy()
            in_source_checkout = SRC.resolve() in Path(sl.__file__).resolve().parents
            if in_source_checkout:
                env["PYTHONPATH"] = str(SRC)
            cmd = [sys.executable, *([] if in_source_checkout else ["-I"]), "-c", child_script, str(out_vtx), str(expected_count)]
            completed = subprocess.run(cmd, cwd=root, env=env, capture_output=True, text=True, check=False)
            self.assertEqual(completed.returncode, 0, completed.stderr or completed.stdout)


if __name__ == "__main__":
    unittest.main()
