use super::super::predicate_to_vortex_expr;
use super::*;
use crate::{
    VortexLocalPrimitiveExecutionPolicy, VortexQueryPrimitiveKind, VortexQueryPrimitiveRequest,
    VortexSimpleAggregateMeasure, VortexSimpleAggregateRequest,
    execute_vortex_local_partitioned_primitive_with_policy,
};
use shardloom_core::{ColumnRef, DatasetUri};
use std::sync::Arc;
use vortex::array::{
    dtype::{FieldNames, Nullability, StructFields},
    expr::stats::Precision,
    scalar::ScalarValue,
    stats::StatsSet,
};

fn build_stats_set(
    field_dtypes: Vec<DType>,
    field_stats: Vec<Vec<(Stat, Precision<ScalarValue>)>>,
) -> FileStatistics {
    let sets = field_stats
        .into_iter()
        .map(|entries| {
            let mut set = StatsSet::default();
            for (stat, value) in entries {
                set.set(stat, value);
            }
            set
        })
        .collect::<Vec<_>>();
    FileStatistics::from_stats(Arc::from(sets), Arc::from(field_dtypes))
}

fn sample_pruning_fixture() -> (StructFields, FileStatistics) {
    let fields = StructFields::new(
        FieldNames::from(["f32_col", "f64_col", "flag", "tag", "num", "nullable_num"]),
        vec![
            DType::Primitive(PType::F32, Nullability::NonNullable),
            DType::Primitive(PType::F64, Nullability::Nullable),
            DType::Bool(Nullability::NonNullable),
            DType::Utf8(Nullability::Nullable),
            DType::Primitive(PType::I64, Nullability::NonNullable),
            DType::Primitive(PType::I64, Nullability::Nullable),
        ],
    );
    let dtypes = vec![
        DType::Primitive(PType::F32, Nullability::NonNullable),
        DType::Primitive(PType::F64, Nullability::Nullable),
        DType::Bool(Nullability::NonNullable),
        DType::Utf8(Nullability::Nullable),
        DType::Primitive(PType::I64, Nullability::NonNullable),
        DType::Primitive(PType::I64, Nullability::Nullable),
    ];
    let stats = build_stats_set(
        dtypes,
        vec![
            vec![
                (Stat::Min, Precision::Exact(ScalarValue::from(1.5_f32))),
                (Stat::Max, Precision::Exact(ScalarValue::from(10.5_f32))),
                (Stat::NullCount, Precision::Exact(0_u64.into())),
                (Stat::NaNCount, Precision::Exact(0_u64.into())),
            ],
            vec![
                (Stat::Min, Precision::Exact(ScalarValue::from(-5.0_f64))),
                (Stat::Max, Precision::Exact(ScalarValue::from(25.0_f64))),
                (Stat::NullCount, Precision::Exact(1_u64.into())),
                (Stat::NaNCount, Precision::Exact(0_u64.into())),
            ],
            vec![
                (Stat::Min, Precision::Exact(ScalarValue::from(true))),
                (Stat::Max, Precision::Exact(ScalarValue::from(true))),
                (Stat::NullCount, Precision::Exact(0_u64.into())),
            ],
            vec![
                (Stat::Min, Precision::Exact(ScalarValue::from("alpha-only"))),
                (Stat::Max, Precision::Exact(ScalarValue::from("alpha-only"))),
                (Stat::NullCount, Precision::Exact(0_u64.into())),
            ],
            vec![
                (Stat::Min, Precision::Exact(ScalarValue::from(42_i64))),
                (Stat::Max, Precision::Exact(ScalarValue::from(42_i64))),
                (Stat::NullCount, Precision::Exact(0_u64.into())),
            ],
            vec![
                (Stat::Min, Precision::Exact(ScalarValue::from(42_i64))),
                (Stat::Max, Precision::Exact(ScalarValue::from(42_i64))),
                (Stat::NullCount, Precision::Exact(2_u64.into())),
            ],
        ],
    );
    (fields, stats)
}

#[test]
fn file_pruning_float_and_bool_soundness() {
    let (fields, stats) = sample_pruning_fixture();

    // Finite F32 / F64 pruning when NaNCount == 0.
    assert!(predicate_falsified_by_file_stats(
        &PredicateExpr::Compare {
            column: ColumnRef::new("f32_col").unwrap(),
            op: ComparisonOp::Gt,
            value: StatValue::Float64(10.5),
        },
        &fields,
        10,
        &stats,
    ));
    assert!(predicate_falsified_by_file_stats(
        &PredicateExpr::Compare {
            column: ColumnRef::new("f64_col").unwrap(),
            op: ComparisonOp::Lt,
            value: StatValue::Float64(-5.0),
        },
        &fields,
        10,
        &stats,
    ));
    assert!(!predicate_falsified_by_file_stats(
        &PredicateExpr::Compare {
            column: ColumnRef::new("f64_col").unwrap(),
            op: ComparisonOp::Eq,
            value: StatValue::Float64(f64::NAN),
        },
        &fields,
        10,
        &stats,
    ));

    // Constant Bool pruning + Nullability checks.
    assert!(predicate_falsified_by_file_stats(
        &PredicateExpr::Compare {
            column: ColumnRef::new("flag").unwrap(),
            op: ComparisonOp::NotEq,
            value: StatValue::Boolean(true),
        },
        &fields,
        10,
        &stats,
    ));
    assert!(predicate_falsified_by_file_stats(
        &PredicateExpr::IsNull {
            column: ColumnRef::new("flag").unwrap(),
        },
        &fields,
        10,
        &stats,
    ));
    assert!(!predicate_falsified_by_file_stats(
        &PredicateExpr::IsNull {
            column: ColumnRef::new("f64_col").unwrap(),
        },
        &fields,
        10,
        &stats,
    ));
}

#[test]
fn file_pruning_utf8_inlist_noteq_and_conjunction_soundness() {
    let (fields, stats) = sample_pruning_fixture();

    // Constant Utf8 StringContains positive and negated.
    assert!(predicate_falsified_by_file_stats(
        &PredicateExpr::StringContains {
            column: ColumnRef::new("tag").unwrap(),
            needle: "beta".to_string(),
            negated: false,
        },
        &fields,
        10,
        &stats,
    ));
    assert!(predicate_falsified_by_file_stats(
        &PredicateExpr::StringContains {
            column: ColumnRef::new("tag").unwrap(),
            needle: "alpha".to_string(),
            negated: true,
        },
        &fields,
        10,
        &stats,
    ));

    // InList positive and negated, including NULL soundness on nullable columns.
    assert!(predicate_falsified_by_file_stats(
        &PredicateExpr::InList {
            column: ColumnRef::new("num").unwrap(),
            values: vec![StatValue::Int64(1), StatValue::Int64(99)],
            negated: false,
        },
        &fields,
        10,
        &stats,
    ));
    assert!(predicate_falsified_by_file_stats(
        &PredicateExpr::InList {
            column: ColumnRef::new("num").unwrap(),
            values: vec![StatValue::Int64(42), StatValue::Int64(99)],
            negated: true,
        },
        &fields,
        10,
        &stats,
    ));
    // Nullable column with nulls must NOT prune constant NotEq or negated InList without null exclusion.
    assert!(!predicate_falsified_by_file_stats(
        &PredicateExpr::Compare {
            column: ColumnRef::new("nullable_num").unwrap(),
            op: ComparisonOp::NotEq,
            value: StatValue::Int64(42),
        },
        &fields,
        10,
        &stats,
    ));
    assert!(!predicate_falsified_by_file_stats(
        &PredicateExpr::InList {
            column: ColumnRef::new("nullable_num").unwrap(),
            values: vec![StatValue::Int64(42)],
            negated: true,
        },
        &fields,
        10,
        &stats,
    ));
    assert!(!predicate_falsified_by_file_stats(
        &PredicateExpr::InList {
            column: ColumnRef::new("f64_col").unwrap(),
            values: vec![StatValue::Float64(1000.0), StatValue::Null],
            negated: false,
        },
        &fields,
        10,
        &stats,
    ));

    // Conjunction prunes if ANY conjunct prunes.
    assert!(predicate_falsified_by_file_stats(
        &PredicateExpr::And(vec![
            PredicateExpr::IsNotNull {
                column: ColumnRef::new("f64_col").unwrap(),
            },
            PredicateExpr::Compare {
                column: ColumnRef::new("num").unwrap(),
                op: ComparisonOp::Eq,
                value: StatValue::Int64(7),
            },
        ]),
        &fields,
        10,
        &stats,
    ));
}

#[test]
fn file_pruning_declines_float_bounds_when_nan_count_is_nonzero_or_missing() {
    let fields = StructFields::new(
        FieldNames::from(["with_nan", "no_nan_stat"]),
        vec![
            DType::Primitive(PType::F64, Nullability::NonNullable),
            DType::Primitive(PType::F64, Nullability::NonNullable),
        ],
    );
    let stats = build_stats_set(
        vec![
            DType::Primitive(PType::F64, Nullability::NonNullable),
            DType::Primitive(PType::F64, Nullability::NonNullable),
        ],
        vec![
            vec![
                (Stat::Min, Precision::Exact(ScalarValue::from(1.0_f64))),
                (Stat::Max, Precision::Exact(ScalarValue::from(5.0_f64))),
                (Stat::NullCount, Precision::Exact(0_u64.into())),
                (Stat::NaNCount, Precision::Exact(1_u64.into())),
            ],
            vec![
                (Stat::Min, Precision::Exact(ScalarValue::from(1.0_f64))),
                (Stat::Max, Precision::Exact(ScalarValue::from(5.0_f64))),
                (Stat::NullCount, Precision::Exact(0_u64.into())),
            ],
        ],
    );

    for column in ["with_nan", "no_nan_stat"] {
        assert!(!predicate_falsified_by_file_stats(
            &PredicateExpr::Compare {
                column: ColumnRef::new(column).unwrap(),
                op: ComparisonOp::Gt,
                value: StatValue::Float64(100.0),
            },
            &fields,
            4,
            &stats,
        ));
    }
}

#[cfg(all(feature = "vortex-write", unix))]
fn write_pruning_partition(
    dir: &std::path::Path,
    prefix: &str,
    idx: usize,
    scores: Vec<f64>,
    tags: Vec<i64>,
    with_stats: bool,
) -> std::path::PathBuf {
    use vortex::VortexSessionDefault as _;
    use vortex::array::{
        IntoArray as _,
        arrays::{PrimitiveArray, StructArray},
        validity::Validity,
    };
    use vortex::file::WriteOptionsSessionExt as _;
    use vortex::io::runtime::{BlockingRuntime as _, single::SingleThreadRuntime};
    use vortex::io::session::RuntimeSessionExt as _;
    use vortex::layout::layouts::flat::writer::FlatLayoutStrategy;
    use vortex::session::VortexSession;

    let runtime = SingleThreadRuntime::default();
    let session = VortexSession::default().with_handle(runtime.handle());
    let stats_list = [Stat::Min, Stat::Max, Stat::NullCount, Stat::NaNCount]
        .iter()
        .filter_map(Stat::aggregate_fn)
        .collect::<Vec<_>>();
    let path = dir.join(format!("{prefix}-{idx}.vortex"));
    let rows = scores.len();
    let array = StructArray::try_new(
        ["score", "bucket"].into(),
        vec![
            PrimitiveArray::from_iter(scores).into_array(),
            PrimitiveArray::from_iter(tags).into_array(),
        ],
        rows,
        Validity::NonNullable,
    )
    .unwrap()
    .into_array();
    let mut options = session
        .write_options()
        .with_strategy(Arc::new(FlatLayoutStrategy::default()));
    options = if with_stats {
        options.with_file_statistics(stats_list)
    } else {
        options.with_file_statistics(Vec::new())
    };
    let mut f = std::fs::File::create(&path).unwrap();
    options
        .blocking(&runtime)
        .write(&mut f, array.to_array_iterator())
        .unwrap();
    path
}

#[cfg(all(feature = "vortex-write", unix))]
fn write_pruning_fixture_suite(
    dir: &std::path::Path,
) -> (Vec<std::path::PathBuf>, Vec<std::path::PathBuf>) {
    let stats_paths = vec![
        write_pruning_partition(dir, "stats", 0, vec![1.0, 2.0, 3.0], vec![10, 10, 10], true),
        write_pruning_partition(
            dir,
            "stats",
            1,
            vec![10.0, 11.0, 12.0],
            vec![20, 20, 20],
            true,
        ),
        write_pruning_partition(dir, "stats", 2, vec![50.0, 60.0], vec![30, 30], true),
    ];
    let nostats_paths = vec![
        write_pruning_partition(
            dir,
            "nostats",
            0,
            vec![1.0, 2.0, 3.0],
            vec![10, 10, 10],
            false,
        ),
        write_pruning_partition(
            dir,
            "nostats",
            1,
            vec![10.0, 11.0, 12.0],
            vec![20, 20, 20],
            false,
        ),
        write_pruning_partition(dir, "nostats", 2, vec![50.0, 60.0], vec![30, 30], false),
    ];
    (stats_paths, nostats_paths)
}

#[cfg(all(feature = "vortex-write", unix))]
#[test]
fn partitioned_scan_prunes_f64_inlist_and_constant_noteq_while_preserving_exact_results() {
    use vortex::VortexSessionDefault as _;
    use vortex::file::OpenOptionsSessionExt as _;
    use vortex::io::runtime::{BlockingRuntime as _, single::SingleThreadRuntime};
    use vortex::io::session::RuntimeSessionExt as _;
    use vortex::session::VortexSession;

    struct DirGuard(std::path::PathBuf);
    impl Drop for DirGuard {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    let dir = std::env::temp_dir().join(format!(
        "shardloom-file-pruning-part-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let _guard = DirGuard(dir.clone());

    let runtime = SingleThreadRuntime::default();
    let session = VortexSession::default().with_handle(runtime.handle());
    let (stats_paths, nostats_paths) = write_pruning_fixture_suite(&dir);

    // Confirm single-file pruning helper sees flat no-stats vs stats difference directly.
    let f0_stats = runtime
        .block_on(session.open_options().open_path(&stats_paths[0]))
        .unwrap();
    let f0_nostats = runtime
        .block_on(session.open_options().open_path(&nostats_paths[0]))
        .unwrap();
    let pred = PredicateExpr::And(vec![
        PredicateExpr::InList {
            column: ColumnRef::new("bucket").unwrap(),
            values: vec![StatValue::Int64(20), StatValue::Int64(99)],
            negated: false,
        },
        PredicateExpr::Compare {
            column: ColumnRef::new("score").unwrap(),
            op: ComparisonOp::GtEq,
            value: StatValue::Float64(10.5),
        },
    ]);
    let expr = predicate_to_vortex_expr(
        &pred,
        f0_stats.dtype(),
        VortexQueryPrimitiveKind::CountWhere,
    )
    .unwrap();
    assert!(can_prune_file_with_predicate(&f0_stats, &expr, Some(&pred)).unwrap());
    assert!(!can_prune_file_with_predicate(&f0_nostats, &expr, Some(&pred)).unwrap());

    let policy = VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 2).unwrap();
    let uris_for = |paths: &[std::path::PathBuf]| {
        paths
            .iter()
            .map(|p| DatasetUri::new(p.display().to_string()).unwrap())
            .collect::<Vec<_>>()
    };
    let stats_uris = uris_for(&stats_paths);
    let nostats_uris = uris_for(&nostats_paths);

    let mut stats_req = VortexQueryPrimitiveRequest::simple_aggregate(
        stats_uris[0].clone(),
        VortexSimpleAggregateRequest::new(vec![VortexSimpleAggregateMeasure::new(
            "count",
            None,
            "cnt".into(),
        )]),
    );
    stats_req.predicate = Some(pred);
    let mut nostats_req = stats_req.clone();
    nostats_req.source_uri = Some(nostats_uris[0].clone());

    let rep_stats =
        execute_vortex_local_partitioned_primitive_with_policy(&stats_req, &stats_uris, policy)
            .unwrap();
    let rep_nostats =
        execute_vortex_local_partitioned_primitive_with_policy(&nostats_req, &nostats_uris, policy)
            .unwrap();

    assert!(rep_stats.embedded_layout.skipped_segment_count > 0);
    assert_eq!(rep_nostats.embedded_layout.skipped_segment_count, 0);
    assert!(
        rep_stats.embedded_layout.selected_segment_count
            < rep_nostats.embedded_layout.selected_segment_count
    );
    assert_eq!(rep_stats.arrays_read_count, 1);
    assert_eq!(rep_nostats.arrays_read_count, 1);
    let extract_cnt = |r: &crate::VortexLocalPrimitiveExecutionReport| {
        let s = r.result_summary.as_deref().unwrap();
        let (_, json_str) = s.split_once(" values=").unwrap();
        let v: serde_json::Value = serde_json::from_str(json_str).unwrap();
        v["values"]["cnt"].clone()
    };
    assert_eq!(extract_cnt(&rep_stats), serde_json::json!(2));
    assert_eq!(extract_cnt(&rep_nostats), serde_json::json!(2));
}

#[cfg(all(feature = "vortex-write", unix))]
#[test]
fn runtime_predicate_selectivity_guard_rejects_unselective_integer_range_hints() {
    use shardloom_core::ComparisonOp;
    use vortex::VortexSessionDefault as _;
    use vortex::file::OpenOptionsSessionExt as _;
    use vortex::io::runtime::{BlockingRuntime as _, single::SingleThreadRuntime};
    use vortex::io::session::RuntimeSessionExt as _;
    use vortex::session::VortexSession;

    struct DirGuard(std::path::PathBuf);
    impl Drop for DirGuard {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    let dir = std::env::temp_dir().join(format!(
        "shardloom-runtime-selectivity-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let _guard = DirGuard(dir.clone());

    let tags: Vec<i64> = (0..=100).collect();
    let scores: Vec<f64> = (0..=100_u32).map(f64::from).collect();
    let stats_path = write_pruning_partition(&dir, "range", 0, scores, tags, true);

    let runtime = SingleThreadRuntime::default();
    let session = VortexSession::default().with_handle(runtime.handle());
    let file = runtime
        .block_on(session.open_options().open_path(&stats_path))
        .unwrap();

    let broad_range = PredicateExpr::And(vec![
        PredicateExpr::Compare {
            column: ColumnRef::new("bucket").unwrap(),
            op: ComparisonOp::GtEq,
            value: StatValue::Int64(0),
        },
        PredicateExpr::Compare {
            column: ColumnRef::new("bucket").unwrap(),
            op: ComparisonOp::LtEq,
            value: StatValue::Int64(99),
        },
    ]);
    assert!(
        !runtime_predicate_is_selective_on_file(&file, &broad_range),
        "range covering >= 50% of [0, 100] should decline optional runtime scan filter"
    );

    let narrow_range = PredicateExpr::And(vec![
        PredicateExpr::Compare {
            column: ColumnRef::new("bucket").unwrap(),
            op: ComparisonOp::GtEq,
            value: StatValue::Int64(0),
        },
        PredicateExpr::Compare {
            column: ColumnRef::new("bucket").unwrap(),
            op: ComparisonOp::LtEq,
            value: StatValue::Int64(10),
        },
    ]);
    assert!(
        runtime_predicate_is_selective_on_file(&file, &narrow_range),
        "range covering < 50% of [0, 100] should stay enabled"
    );

    let disjoint_range = PredicateExpr::And(vec![
        PredicateExpr::Compare {
            column: ColumnRef::new("bucket").unwrap(),
            op: ComparisonOp::GtEq,
            value: StatValue::Int64(500),
        },
        PredicateExpr::Compare {
            column: ColumnRef::new("bucket").unwrap(),
            op: ComparisonOp::LtEq,
            value: StatValue::Int64(600),
        },
    ]);
    assert!(
        runtime_predicate_is_selective_on_file(&file, &disjoint_range),
        "disjoint range should remain enabled so file pruning fires"
    );
}
