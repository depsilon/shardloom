//! Track A unit and integration tests for bounded multi-row-group Parquet
//! concurrency (A1) and contiguous-range persistent Aggregate merge (A2).

use super::*;
use crate::query_primitive::VortexSimpleAggregateMeasure;
use crate::relational_query::{
    VortexRelationalAggregate, VortexRelationalFilter, VortexRelationalJoin,
    VortexRelationalJoinColumn, VortexRelationalJoinKey, VortexRelationalJoinKind,
    VortexRelationalLimit, VortexRelationalPlan, VortexRelationalSide,
};
use arrow_array::{Float64Array, Int64Array, StringArray};
use arrow_schema::{DataType, Field, Schema};
use serde_json::Value;
use shardloom_core::native_io_decisions::NativeIoCapability;
use shardloom_core::{
    ComparisonOp, ExecutionResourceOrigin, ExecutionResources, ExprId, Expression, ExpressionKind,
    ScalarValue,
};

fn with_parallelism(
    max_parallelism: usize,
    memory_bytes: u64,
) -> VortexLocalPrimitiveExecutionPolicy {
    VortexLocalPrimitiveExecutionPolicy::from_resources(
        ExecutionResources::from_bytes(
            memory_bytes,
            max_parallelism,
            ExecutionResourceOrigin::ExecutionCall,
        )
        .unwrap(),
    )
    .unwrap()
}

fn prepare_with_policy(
    fixture: &ParquetFixture,
    execution_policy: VortexLocalPrimitiveExecutionPolicy,
    plan: &VortexRelationalPlan,
) -> PreparedVortexRelational {
    prepare_relational_with_schema(execution_policy, |preparation| {
        let source = LocalColumnarSource::admit(
            &fixture.path(),
            "parquet",
            &preparation.shared_memory_pool(),
            &preparation.cancellation_token(),
        )?;
        preparation.register_local_source(
            DatasetUri::new(fixture.path().display().to_string())?,
            source,
        )?;
        Ok(plan.clone())
    })
    .unwrap()
}

fn measure(function: &str, column: Option<&str>, alias: &str) -> VortexSimpleAggregateMeasure {
    VortexSimpleAggregateMeasure::new(
        function,
        column.map(|name| ColumnRef::new(name).unwrap()),
        alias.into(),
    )
}

fn aggregate(
    input: VortexRelationalPlan,
    group_by: &[&str],
    measures: Vec<VortexSimpleAggregateMeasure>,
) -> VortexRelationalPlan {
    VortexRelationalPlan::Aggregate(Box::new(VortexRelationalAggregate {
        input,
        group_by: group_by
            .iter()
            .map(|name| ColumnRef::new(*name).unwrap())
            .collect(),
        grouping_sets: None,
        measures,
    }))
}

#[allow(clippy::cast_possible_wrap, clippy::cast_precision_loss)]
fn rich_batch(rows: usize) -> RecordBatch {
    let g_int = Int64Array::from_iter_values(
        (0..rows).map(|i| (((i * 7) + (i / 8_192) * 3) % 17) as i64 - 4),
    );
    let g_str = StringArray::from_iter_values(
        (0..rows).map(|i| format!("grp-{:02}", ((i * 5) + (i / 8_192)) % 19)),
    );
    let n =
        Int64Array::from_iter((0..rows).map(|i| (i % 11 != 0).then_some(((i as i64) % 97) - 31)));
    let label = StringArray::from_iter(
        (0..rows).map(|i| (i % 7 != 0).then(|| format!("val-{:03}", (i * 3) % 101))),
    );
    let f = Float64Array::from_iter_values((0..rows).map(|i| {
        if i == 0 || i == 100 {
            -0.0_f64
        } else if i == 9_000 || i == 17_000 {
            0.0_f64
        } else {
            ((i % 251) as f64) * 0.25 - 12.5
        }
    }));
    let flag = Int64Array::from_iter_values((0..rows).map(|i| i64::from(i % 2 == 0)));
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("g_int", DataType::Int64, false),
            Field::new("g_str", DataType::Utf8, false),
            Field::new("n", DataType::Int64, true),
            Field::new("label", DataType::Utf8, true),
            Field::new("f", DataType::Float64, false),
            Field::new("flag", DataType::Int64, false),
        ])),
        vec![
            Arc::new(g_int),
            Arc::new(g_str),
            Arc::new(n),
            Arc::new(label),
            Arc::new(f),
            Arc::new(flag),
        ],
    )
    .unwrap()
}

#[allow(clippy::cast_possible_wrap)]
fn wide_payload_batch(rows: usize) -> RecordBatch {
    let payload_unit = "x".repeat(512);
    let k = Int64Array::from_iter_values((0..rows).map(|i| i as i64));
    let wide = StringArray::from_iter_values((0..rows).map(|_| payload_unit.as_str()));
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("wide", DataType::Utf8, false),
        ])),
        vec![Arc::new(k), Arc::new(wide)],
    )
    .unwrap()
}

fn sort_keyed_rows(rows: &mut [Value], keys: &[&str]) {
    rows.sort_by(|left, right| {
        for key in keys {
            let ord = left[*key].to_string().cmp(&right[*key].to_string());
            if ord != std::cmp::Ordering::Equal {
                return ord;
            }
        }
        std::cmp::Ordering::Equal
    });
}

#[test]
fn a1_parquet_parallel_range_delivery_preserves_order_and_single_worker_progress() {
    let fixture =
        ParquetFixture::with_batch_rows(rich_batch(32_768), EnabledStatistics::Chunk, 8_192);
    let plan = project(
        fixture.scan(&["g_int", "n", "label"], None),
        &[("g_int", "g_int"), ("n", "n"), ("label", "label")],
    );

    let serial = prepare_with_policy(&fixture, with_parallelism(1, 64 << 20), &plan)
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    let parallel_prepared = prepare_with_policy(&fixture, with_parallelism(4, 64 << 20), &plan);
    let parallel = parallel_prepared
        .collect_jsonl(&CancellationToken::default())
        .unwrap();

    assert_eq!(json_rows(&parallel), json_rows(&serial));
    let ser_input = serial.execution.input.as_ref().unwrap();
    let par_input = parallel.execution.input.as_ref().unwrap();
    assert_eq!(ser_input.parallel_range_tasks, 0);
    assert_eq!(par_input.parallel_range_tasks, 4);
    assert_eq!(par_input.persistent_aggregate_states, 0);
    assert_eq!(par_input.columnar_rows_decoded, 32_768);
    assert!(par_input.columnar_selected_chunk_bytes_read > 0);

    // Must still make full progress without deadlock when active worker limit is throttled to 1.
    let throttled_prepared = prepare_with_policy(&fixture, with_parallelism(4, 64 << 20), &plan);
    let throttled = {
        let pool = throttled_prepared
            .session
            .shared_compute_pool(4)
            .unwrap()
            .unwrap();
        let _one_worker = pool.scope_active_worker_limit(1);
        throttled_prepared
            .collect_jsonl(&CancellationToken::default())
            .unwrap()
    };
    assert_eq!(json_rows(&throttled), json_rows(&serial));
    assert_eq!(
        throttled
            .execution
            .input
            .as_ref()
            .unwrap()
            .parallel_range_tasks,
        4
    );
}

#[test]
fn a1_parquet_cancellation_consumer_error_and_tight_headroom_behave_cleanly() {
    let fixture =
        ParquetFixture::with_batch_rows(rich_batch(32_768), EnabledStatistics::Chunk, 8_192);
    let plan = fixture.scan(&["g_int", "n"], None);

    // Consumer error on first batch cleanly aborts bounded sync_channel(2) and drains workers.
    let err_prepared = prepare_with_policy(&fixture, with_parallelism(4, 64 << 20), &plan);
    let err_memory = err_prepared.session.shared_memory_pool();
    let err = err_prepared
        .for_each_batch(&CancellationToken::default(), |_batch, _ctx| {
            Err(shardloom_core::ShardLoomError::InvalidOperation(
                "stop after first batch".into(),
            ))
        })
        .map(|_| ())
        .unwrap_err();
    assert!(err.to_string().contains("stop after first batch"), "{err}");
    drop(err_prepared);
    err_memory.reclaim_optional_memory();
    assert_eq!(err_memory.snapshot().reserved_bytes, 0);

    // Mid-stream cancellation cleanly cancels workers and frees every reservation.
    let cancel_prepared = prepare_with_policy(&fixture, with_parallelism(4, 64 << 20), &plan);
    let cancel_memory = cancel_prepared.session.shared_memory_pool();
    let token = CancellationToken::default();
    let mut batches = 0usize;
    let cancel_err = cancel_prepared
        .for_each_batch(&token, |_batch, _ctx| {
            batches += 1;
            if batches == 1 {
                token.cancel();
            }
            Ok(())
        })
        .map(|_| ())
        .unwrap_err();
    assert!(
        cancel_err.to_string().contains("cancel"),
        "expected cancellation error, got {cancel_err}"
    );
    drop(cancel_prepared);
    cancel_memory.reclaim_optional_memory();
    assert_eq!(cancel_memory.snapshot().reserved_bytes, 0);

    // Tight headroom fallback: 4 RGs of ~4.1 MiB non-dictionary PLAIN payloads under a 24 MiB
    // budget fits serial streaming decode cleanly while available / (4 * rg_estimate) == 1 < 2
    // declines A1 (`for_each_batch` avoids buffering all 16 MiB of strings as JSON).
    let wide_fixture = ParquetFixture::with_properties(
        wide_payload_batch(32_768),
        WriterProperties::builder()
            .set_max_row_group_row_count(Some(8_192))
            .set_dictionary_enabled(false)
            .set_statistics_enabled(EnabledStatistics::Chunk)
            .build(),
    );
    let tight_plan = wide_fixture.scan(&["k", "wide"], None);
    let mut tight_rows = 0usize;
    let tight = prepare_with_policy(&wide_fixture, with_parallelism(4, 24 << 20), &tight_plan)
        .for_each_batch(&CancellationToken::default(), |batch, _ctx| {
            tight_rows += batch.len();
            Ok(())
        })
        .unwrap();
    let tight_input = tight.input.as_ref().unwrap();
    assert_eq!(tight_rows, 32_768);
    assert_eq!(tight_input.parallel_range_tasks, 0);
    assert_eq!(tight_input.parallel_range_declined_sources, 1);
    assert_eq!(tight_input.columnar_rows_decoded, 32_768);
}

#[test]
fn a1_parquet_declines_single_row_group_small_input_limit_and_shared_self_join() {
    // 1. Single row group (32,768 rows in 1 RG) -> declines A1.
    let single_rg =
        ParquetFixture::with_batch_rows(rich_batch(32_768), EnabledStatistics::Chunk, 32_768);
    let single_res = prepare_with_policy(
        &single_rg,
        with_parallelism(4, 64 << 20),
        &single_rg.scan(&["g_int", "n"], None),
    )
    .collect_jsonl(&CancellationToken::default())
    .unwrap();
    assert_eq!(
        single_res
            .execution
            .input
            .as_ref()
            .unwrap()
            .parallel_range_tasks,
        0
    );

    // 2. Small input (< 16,384 rows across 4 row groups) -> declines A1.
    let small_multi =
        ParquetFixture::with_batch_rows(rich_batch(8_192), EnabledStatistics::Chunk, 2_048);
    let small_res = prepare_with_policy(
        &small_multi,
        with_parallelism(4, 64 << 20),
        &small_multi.scan(&["g_int", "n"], None),
    )
    .collect_jsonl(&CancellationToken::default())
    .unwrap();
    assert_eq!(
        small_res
            .execution
            .input
            .as_ref()
            .unwrap()
            .parallel_range_tasks,
        0
    );

    // 3. Multi-row-group >= 16,384 rows with LIMIT (max_rows().is_some()) -> declines A1.
    let multi =
        ParquetFixture::with_batch_rows(rich_batch(32_768), EnabledStatistics::Chunk, 8_192);
    let limited = VortexRelationalPlan::Limit(Box::new(VortexRelationalLimit {
        input: multi.scan(&["g_int", "n"], None),
        offset: 0,
        count: 100,
    }));
    let limited_res = prepare_with_policy(&multi, with_parallelism(4, 64 << 20), &limited)
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(
        limited_res
            .execution
            .input
            .as_ref()
            .unwrap()
            .parallel_range_tasks,
        0
    );

    // 4. Shared self-join (requires replay) -> declines A1.
    let self_join = VortexRelationalPlan::Join(Box::new(VortexRelationalJoin {
        left: multi.scan(&["g_int", "n"], None),
        right: multi.scan(&["g_int", "n"], None),
        kind: VortexRelationalJoinKind::Inner,
        keys: vec![VortexRelationalJoinKey {
            left: ColumnRef::new("g_int").unwrap(),
            right: ColumnRef::new("g_int").unwrap(),
        }],
        condition: None,
        columns: vec![
            VortexRelationalJoinColumn {
                side: VortexRelationalSide::Left,
                column: ColumnRef::new("g_int").unwrap(),
                output_column: "left_g".into(),
            },
            VortexRelationalJoinColumn {
                side: VortexRelationalSide::Right,
                column: ColumnRef::new("n").unwrap(),
                output_column: "right_n".into(),
            },
        ],
    }));
    let self_join_limited = VortexRelationalPlan::Limit(Box::new(VortexRelationalLimit {
        input: self_join,
        offset: 0,
        count: 32,
    }));
    let self_join_res =
        prepare_with_policy(&multi, with_parallelism(4, 128 << 20), &self_join_limited)
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
    assert_eq!(
        self_join_res
            .execution
            .input
            .as_ref()
            .unwrap()
            .parallel_range_tasks,
        0
    );
}

#[test]
#[allow(clippy::too_many_lines)]
fn a2_parquet_grouped_scalar_pipeline_and_distinct_match_serial_and_populate_counters() {
    let fixture =
        ParquetFixture::with_batch_rows(rich_batch(32_768), EnabledStatistics::Chunk, 8_192);

    // 1. Grouped aggregate with int + utf8 keys and Count/Sum/Avg/Min/Max across int, utf8, float.
    let grouped = aggregate(
        fixture.scan(&[], None),
        &["g_int", "g_str"],
        vec![
            measure("count", None, "cnt"),
            measure("count", Some("n"), "cnt_n"),
            measure("sum", Some("n"), "sum_n"),
            measure("avg", Some("n"), "avg_n"),
            measure("min", Some("n"), "min_n"),
            measure("max", Some("n"), "max_n"),
            measure("min", Some("label"), "min_l"),
            measure("max", Some("label"), "max_l"),
            measure("min", Some("f"), "min_f"),
            measure("max", Some("f"), "max_f"),
        ],
    );
    let ser_grouped = prepare_with_policy(&fixture, with_parallelism(1, 64 << 20), &grouped)
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    let par_grouped = prepare_with_policy(&fixture, with_parallelism(4, 64 << 20), &grouped)
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(json_rows(&par_grouped), json_rows(&ser_grouped));
    let par_in = par_grouped.execution.input.as_ref().unwrap();
    assert_eq!(par_in.parallel_range_tasks, 4);
    assert_eq!(par_in.persistent_aggregate_states, 4);
    assert!(par_in.columnar_selected_chunk_bytes_read > 0);
    assert!(
        par_grouped
            .execution
            .native_io_certificate
            .capability_decisions
            .get(NativeIoCapability::RepresentationRetention)
            .is_some()
    );

    // 2. Filter -> Project -> Aggregate pipeline.
    let filtered = VortexRelationalPlan::Filter(Box::new(VortexRelationalFilter {
        input: fixture.scan(&[], None),
        predicate: Expression::new(
            ExprId::new("flag_eq").unwrap(),
            ExpressionKind::Compare {
                left: Box::new(column("flag")),
                op: ComparisonOp::Eq,
                right: Box::new(Expression::literal(
                    ExprId::new("one").unwrap(),
                    ScalarValue::Int64(1),
                )),
            },
        ),
    }));
    let projected = project(filtered, &[("g_int", "k"), ("n", "v")]);
    let pipe_agg = aggregate(
        projected,
        &["k"],
        vec![
            measure("count", None, "cnt"),
            measure("sum", Some("v"), "sum_v"),
            measure("min", Some("v"), "min_v"),
            measure("max", Some("v"), "max_v"),
        ],
    );
    let ser_pipe = prepare_with_policy(&fixture, with_parallelism(1, 64 << 20), &pipe_agg)
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    let par_pipe = prepare_with_policy(&fixture, with_parallelism(4, 64 << 20), &pipe_agg)
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(json_rows(&par_pipe), json_rows(&ser_pipe));
    assert_eq!(
        par_pipe
            .execution
            .input
            .as_ref()
            .unwrap()
            .persistent_aggregate_states,
        4
    );

    // 3. Ungrouped scalar over filter (metadata-count declines filtered scans, so A2 runs).
    let scalar_filtered = VortexRelationalPlan::Filter(Box::new(VortexRelationalFilter {
        input: fixture.scan(&[], None),
        predicate: Expression::new(
            ExprId::new("flag_eq_scalar").unwrap(),
            ExpressionKind::Compare {
                left: Box::new(column("flag")),
                op: ComparisonOp::Eq,
                right: Box::new(Expression::literal(
                    ExprId::new("one_scalar").unwrap(),
                    ScalarValue::Int64(1),
                )),
            },
        ),
    }));
    let scalar_agg = aggregate(
        scalar_filtered,
        &[],
        vec![
            measure("count", None, "cnt"),
            measure("sum", Some("n"), "sum_n"),
            measure("avg", Some("n"), "avg_n"),
            measure("min", Some("n"), "min_n"),
            measure("max", Some("n"), "max_n"),
        ],
    );
    let ser_scalar = prepare_with_policy(&fixture, with_parallelism(1, 64 << 20), &scalar_agg)
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    let par_scalar = prepare_with_policy(&fixture, with_parallelism(4, 64 << 20), &scalar_agg)
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(json_rows(&par_scalar), json_rows(&ser_scalar));
    assert_eq!(
        par_scalar
            .execution
            .input
            .as_ref()
            .unwrap()
            .persistent_aggregate_states,
        4
    );

    // 4. 0-measure GROUP BY DISTINCT preserves exact first-seen group order as serial.
    let distinct_only = aggregate(fixture.scan(&[], None), &["g_str"], vec![]);
    let ser_dist = prepare_with_policy(&fixture, with_parallelism(1, 64 << 20), &distinct_only)
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    let par_dist = prepare_with_policy(&fixture, with_parallelism(4, 64 << 20), &distinct_only)
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(json_rows(&par_dist), json_rows(&ser_dist));
    assert_eq!(
        par_dist
            .execution
            .input
            .as_ref()
            .unwrap()
            .persistent_aggregate_states,
        4
    );
    let mut sorted_ser = json_rows(&ser_grouped);
    let mut sorted_par = json_rows(&par_grouped);
    sort_keyed_rows(&mut sorted_ser, &["g_int", "g_str"]);
    sort_keyed_rows(&mut sorted_par, &["g_int", "g_str"]);
    assert_eq!(sorted_par, sorted_ser);
}

#[test]
fn a2_declines_float_sum_and_avg_while_retaining_a1_parallel_decode() {
    let fixture =
        ParquetFixture::with_batch_rows(rich_batch(32_768), EnabledStatistics::Chunk, 8_192);
    let float_agg = aggregate(
        fixture.scan(&[], None),
        &["g_int"],
        vec![
            measure("count", None, "cnt"),
            measure("sum", Some("f"), "sum_f"),
            measure("avg", Some("f"), "avg_f"),
        ],
    );
    let serial = prepare_with_policy(&fixture, with_parallelism(1, 64 << 20), &float_agg)
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    let parallel = prepare_with_policy(&fixture, with_parallelism(4, 64 << 20), &float_agg)
        .collect_jsonl(&CancellationToken::default())
        .unwrap();

    assert_eq!(json_rows(&parallel), json_rows(&serial));
    let input = parallel.execution.input.as_ref().unwrap();
    assert_eq!(input.persistent_aggregate_states, 0);
    assert_eq!(input.parallel_range_tasks, 4);
}
