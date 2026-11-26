//! Source-aware engine foundation integration tests for Orders 1-5:
//! wrapped Parquet Stage-1 selection + filter-masked Bloom, shared parallel
//! aggregate controller (`.vortex` File + `OwnedArraySource` + float serial
//! operator determinism), parallel `SealedJoin`, and multi-measure `join_aggregate`.

#![allow(
    clippy::cast_possible_wrap,
    clippy::cast_precision_loss,
    clippy::too_many_lines
)]

use super::*;
use crate::owned_array_source::OwnedArraySourceBounds;
use crate::query_primitive::VortexSimpleAggregateMeasure;
use crate::relational_query::{
    VortexRelationalAggregate, VortexRelationalFilter, VortexRelationalJoin,
    VortexRelationalJoinColumn, VortexRelationalJoinKey, VortexRelationalJoinKind as JoinKind,
    VortexRelationalPlan, VortexRelationalSide as Side,
};
use arrow_array::Float64Array;
use serde_json::{Value, json};
use shardloom_core::{
    ComparisonOp, ExecutionResourceOrigin, ExecutionResources, ExprId, Expression, ExpressionKind,
};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use vortex::array::{
    arrays::{StructArray, VarBinViewArray},
    dtype::FieldNames,
    expr::stats::Stat,
    validity::Validity,
};

fn parallel_policy(
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

fn measure(function: &str, col: Option<&str>, alias: &str) -> VortexSimpleAggregateMeasure {
    VortexSimpleAggregateMeasure::new(
        function,
        col.map(|name| ColumnRef::new(name).unwrap()),
        alias.into(),
    )
}

fn sorted_rows(mut rows: Vec<Value>) -> Vec<Value> {
    rows.sort_by_key(Value::to_string);
    rows
}

fn write_parquet_batch(path: &Path, batch: &RecordBatch, rg_rows: usize) {
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    let mut writer = ArrowWriter::try_new(
        fs::File::create(path).unwrap(),
        batch.schema(),
        Some(
            WriterProperties::builder()
                .set_max_row_group_row_count(Some(rg_rows))
                .set_statistics_enabled(EnabledStatistics::Chunk)
                .build(),
        ),
    )
    .unwrap();
    writer.write(batch).unwrap();
    writer.close().unwrap();
}

#[test]
fn wrapped_parquet_collections_preserve_stage1_row_selection_and_filter_masked_bloom() {
    let base = ParquetFixture::new();
    let per_part = 8_192_usize;
    let total_rows = per_part * 2;
    for (part_idx, part_name) in ["a", "b"].into_iter().enumerate() {
        let start = (part_idx * per_part) as i64;
        let end = start + per_part as i64;
        let batch = if part_idx % 2 == 0 {
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("k", DataType::Int64, false),
                    Field::new("sel", DataType::Int64, false),
                    Field::new("val", DataType::Int64, false),
                    Field::new("payload", DataType::Utf8, false),
                ])),
                vec![
                    Arc::new(Int64Array::from_iter_values((start..end).map(|r| r % 512))),
                    Arc::new(Int64Array::from_iter_values((start..end).map(|r| r % 20))),
                    Arc::new(Int64Array::from_iter_values((start..end).map(|r| r % 17))),
                    Arc::new(StringArray::from_iter_values(
                        (start..end).map(|r| format!("wide-payload-{r:05}")),
                    )),
                ],
            )
            .unwrap()
        } else {
            // Reordered schema + extra nullable column to test union_by_name alignment.
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("payload", DataType::Utf8, false),
                    Field::new("val", DataType::Int64, false),
                    Field::new("sel", DataType::Int64, false),
                    Field::new("k", DataType::Int64, false),
                    Field::new("extra", DataType::Int64, true),
                ])),
                vec![
                    Arc::new(StringArray::from_iter_values(
                        (start..end).map(|r| format!("wide-payload-{r:05}")),
                    )),
                    Arc::new(Int64Array::from_iter_values((start..end).map(|r| r % 17))),
                    Arc::new(Int64Array::from_iter_values((start..end).map(|r| r % 20))),
                    Arc::new(Int64Array::from_iter_values((start..end).map(|r| r % 512))),
                    Arc::new(Int64Array::from_iter(
                        (start..end).map(|r| (r % 3 != 0).then_some(r % 10)),
                    )),
                ],
            )
            .unwrap()
        };
        write_parquet_batch(
            &base.0.join(format!("part={part_name}/part.parquet")),
            &batch,
            4_096,
        );
    }

    let manifest_path = base.0.join("wrapped.manifest");
    fs::write(
        &manifest_path,
        json!({
            "version": 1,
            "format": "parquet",
            "schema": "union_by_name",
            "partitioning": "hive",
            "files": ["part=a/part.parquet", "part=b/part.parquet"]
        })
        .to_string(),
    )
    .unwrap();

    // Small build table selecting k in {0, 41} -> exercises non-empty Stage-1 survival across
    // both Hive partitions (`part=a` and `part=b`) including PartitionValue + MissingNull slots.
    let dim_batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("tag", DataType::Int64, false),
        ])),
        vec![
            Arc::new(Int64Array::from_iter_values([0_i64, 41_i64])),
            Arc::new(Int64Array::from_iter_values([10_i64, 20_i64])),
        ],
    )
    .unwrap();
    let dim = ParquetFixture::with_batch_rows(dim_batch, EnabledStatistics::Chunk, 2);

    let fact_scan = VortexRelationalPlan::Scan(VortexRelationalScan {
        source_uri: DatasetUri::new(manifest_path.display().to_string()).unwrap(),
        projection: ProjectionRequest::Columns(vec![
            ColumnRef::new("k").unwrap(),
            ColumnRef::new("sel").unwrap(),
            ColumnRef::new("val").unwrap(),
            ColumnRef::new("payload").unwrap(),
            ColumnRef::new("extra").unwrap(),
            ColumnRef::new("part").unwrap(),
        ]),
        predicate: Some(PredicateExpr::Compare {
            column: ColumnRef::new("sel").unwrap(),
            op: ComparisonOp::Lt,
            value: StatValue::Int64(2),
        }),
    });
    let plan = VortexRelationalPlan::Aggregate(Box::new(VortexRelationalAggregate {
        input: VortexRelationalPlan::Join(Box::new(VortexRelationalJoin {
            left: fact_scan,
            right: dim.scan(&["k", "tag"], None),
            kind: JoinKind::Inner,
            keys: vec![VortexRelationalJoinKey {
                left: ColumnRef::new("k").unwrap(),
                right: ColumnRef::new("k").unwrap(),
            }],
            condition: None,
            columns: vec![
                VortexRelationalJoinColumn {
                    side: Side::Left,
                    column: ColumnRef::new("part").unwrap(),
                    output_column: "part".into(),
                },
                VortexRelationalJoinColumn {
                    side: Side::Right,
                    column: ColumnRef::new("tag").unwrap(),
                    output_column: "tag".into(),
                },
                VortexRelationalJoinColumn {
                    side: Side::Left,
                    column: ColumnRef::new("val").unwrap(),
                    output_column: "val".into(),
                },
                VortexRelationalJoinColumn {
                    side: Side::Left,
                    column: ColumnRef::new("payload").unwrap(),
                    output_column: "payload".into(),
                },
                VortexRelationalJoinColumn {
                    side: Side::Left,
                    column: ColumnRef::new("extra").unwrap(),
                    output_column: "extra".into(),
                },
            ],
        })),
        group_by: vec![
            ColumnRef::new("part").unwrap(),
            ColumnRef::new("tag").unwrap(),
        ],
        grouping_sets: None,
        measures: vec![
            measure("count", None, "cnt"),
            measure("sum", Some("val"), "sum_v"),
            measure("min", Some("payload"), "min_p"),
            measure("count", Some("extra"), "non_null_extra"),
        ],
    }));

    let prepared = prepare_relational_with_schema(parallel_policy(4, 256 << 20), |prep| {
        let wrapped = LocalColumnarSource::admit(
            &manifest_path,
            "parquet",
            &prep.shared_memory_pool(),
            &prep.cancellation_token(),
        )?;
        prep.register_local_source(
            DatasetUri::new(manifest_path.display().to_string())?,
            wrapped,
        )?;
        let build = LocalColumnarSource::admit(
            &dim.path(),
            "parquet",
            &prep.shared_memory_pool(),
            &prep.cancellation_token(),
        )?;
        prep.register_local_source(DatasetUri::new(dim.path().display().to_string())?, build)?;
        Ok(plan.clone())
    })
    .unwrap();

    let collected = prepared
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    let input = collected.execution.input.as_ref().unwrap();
    assert!(
        input.columnar_payload_rows_decoded > 0
            && input.columnar_payload_rows_decoded < total_rows as u64,
        "expected non-zero wrapped Parquet Stage-1 selection to skip payload decode rows; payload={}",
        input.columnar_payload_rows_decoded
    );
    assert!(
        !collected
            .execution
            .native_io_certificate
            .source_capability_report
            .fallback_attempted
    );

    let mut expected: BTreeMap<(String, i64), (i64, i64, String, i64)> = BTreeMap::new();
    for r in 0..total_rows {
        let ri = r as i64;
        let k = ri % 512;
        let sel = ri % 20;
        let tag = match k {
            0 => Some(10_i64),
            41 => Some(20_i64),
            _ => None,
        };
        if sel < 2
            && let Some(tag) = tag
        {
            let part = if r < per_part { "a" } else { "b" }.to_string();
            let payload = format!("wide-payload-{r:05}");
            let entry = expected
                .entry((part, tag))
                .or_insert_with(|| (0, 0, payload.clone(), 0));
            entry.0 += 1;
            entry.1 += ri % 17;
            if payload < entry.2 {
                entry.2 = payload;
            }
            if r >= per_part && (ri % 3 != 0) {
                entry.3 += 1;
            }
        }
    }
    assert_eq!(expected.len(), 4);
    let actual_rows = json_rows(&collected);
    assert_eq!(actual_rows.len(), expected.len());
    for row in actual_rows {
        let part = row["part"].as_str().unwrap().to_string();
        let tag = row["tag"].as_i64().unwrap();
        let (exp_cnt, exp_sum, ref exp_min_p, exp_non_null_extra) = expected[&(part.clone(), tag)];
        assert_eq!(row["cnt"].as_i64(), Some(exp_cnt));
        assert_eq!(row["min_p"].as_str(), Some(exp_min_p.as_str()));
        assert_eq!(row["non_null_extra"].as_i64(), Some(exp_non_null_extra));
        if part == "a" {
            assert_eq!(exp_non_null_extra, 0);
        } else {
            assert!(exp_non_null_extra > 0);
        }
        let got_sum = row["sum_v"].as_f64().unwrap();
        assert!((got_sum - exp_sum as f64).abs() <= 1e-9);
    }
}

#[test]
fn shared_parallel_controller_covers_vortex_file_owned_array_canonical_and_float_policy() {
    let total_rows = 20_480_usize;
    let grp: Vec<i64> = (0..total_rows).map(|i| (i % 13) as i64).collect();
    let val_i64: Vec<i64> = (0..total_rows)
        .map(|i| ((i * 7) % 101) as i64 - 25)
        .collect();
    let val_f64: Vec<f64> = (0..total_rows)
        .map(|i| {
            if i == 0 || i == 9_000 {
                -0.0_f64
            } else {
                ((i % 37) as f64) * 0.125 - 2.0
            }
        })
        .collect();
    let grp_tag: Vec<String> = (0..total_rows)
        .map(|i| format!("tag-{:02}", (i * 3) % 23))
        .collect();

    let vtx_array = StructArray::try_new(
        FieldNames::from(["grp", "val_i64", "val_f64", "grp_tag"]),
        vec![
            vortex::array::arrays::PrimitiveArray::from_iter(grp.clone()).into_array(),
            vortex::array::arrays::PrimitiveArray::from_iter(val_i64.clone()).into_array(),
            vortex::array::arrays::PrimitiveArray::from_iter(val_f64.clone()).into_array(),
            VarBinViewArray::from_iter_str(grp_tag.iter().map(String::as_str)).into_array(),
        ],
        total_rows,
        Validity::NonNullable,
    )
    .unwrap()
    .into_array();
    let vtx_fixture = super::super::Fixture::with_strategy_statistics(
        vtx_array,
        4_096,
        vec![],
        crate::local_primitives::native_flat_layout::SequentialNativeFlatLayout::strategy(5),
        vec![Stat::Min, Stat::Max, Stat::NullCount],
    );

    let session = ResidentVortexSession::new(256 << 20, 4).unwrap();
    let arrow_batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("grp", DataType::Int64, false),
            Field::new("val_i64", DataType::Int64, false),
            Field::new("val_f64", DataType::Float64, false),
            Field::new("grp_tag", DataType::Utf8, false),
        ])),
        vec![
            Arc::new(Int64Array::from_iter_values(grp.clone())),
            Arc::new(Int64Array::from_iter_values(val_i64.clone())),
            Arc::new(Float64Array::from_iter_values(val_f64.clone())),
            Arc::new(StringArray::from_iter_values(
                grp_tag.iter().map(String::as_str),
            )),
        ],
    )
    .unwrap();
    let mem = session
        .with_native_execution_context(&CancellationToken::default(), |ctx| {
            ResidentMemorySource::from_local_arrow_with_ownership(
                &session,
                arrow_batch,
                ctx,
                crate::vortex_ingest::arrow_ownership::ArrowOwnershipPolicy::ValidatedCopy,
            )
        })
        .unwrap();
    let owned = mem
        .prepare_projection(&["grp", "val_i64", "val_f64", "grp_tag"], None, None)
        .unwrap()
        .execute_arrays()
        .unwrap()
        .into_owned_array_source(
            OwnedArraySourceBounds::default(),
            &CancellationToken::default(),
        )
        .unwrap();
    let owned_uri = DatasetUri::new("memory://parallel-owned-ctrl").unwrap();

    let mut expected_int: BTreeMap<i64, (i64, i64, BTreeSet<String>)> = BTreeMap::new();
    let mut expected_flt: BTreeMap<i64, f64> = BTreeMap::new();
    for r in 0..total_rows {
        let g = grp[r];
        let entry = expected_int.entry(g).or_default();
        entry.0 += 1;
        entry.1 += val_i64[r];
        entry.2.insert(grp_tag[r].clone());
        let acc = expected_flt.entry(g).or_insert(0.0);
        *acc += val_f64[r];
    }
    let expected_int: BTreeMap<i64, (i64, f64, f64, usize)> = expected_int
        .into_iter()
        .map(|(g, (cnt, sum_v, uniq))| {
            let sum_f = sum_v as f64;
            (g, (cnt, sum_f, sum_f / (cnt as f64), uniq.len()))
        })
        .collect();

    for is_owned in [false, true] {
        let scan = if is_owned {
            VortexRelationalPlan::Scan(VortexRelationalScan {
                source_uri: owned_uri.clone(),
                projection: ProjectionRequest::All,
                predicate: None,
            })
        } else {
            vtx_fixture.scan()
        };

        // Non-distinct canonical SUM/AVG + COUNT: both source & operator workers >= 2.
        let int_plan = VortexRelationalPlan::Aggregate(Box::new(VortexRelationalAggregate {
            input: scan.clone(),
            group_by: vec![ColumnRef::new("grp").unwrap()],
            grouping_sets: None,
            measures: vec![
                measure("count", None, "cnt"),
                measure("sum", Some("val_i64"), "sum_v"),
                measure("avg", Some("val_i64"), "avg_v"),
            ],
        }));
        let prepared_int = prepare_relational_with_schema_in_session(
            parallel_policy(4, 256 << 20),
            &session,
            |schemas| {
                if is_owned {
                    schemas.register_owned_array_source(owned_uri.clone(), owned.clone())?;
                }
                Ok(int_plan.clone())
            },
        )
        .unwrap();
        let res_int = prepared_int
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        let int_rows = json_rows(&res_int);
        assert_eq!(int_rows.len(), expected_int.len(), "is_owned={is_owned}");
        for row in int_rows {
            let g = row["grp"].as_i64().unwrap();
            let (exp_cnt, exp_sum, exp_avg, _) = expected_int[&g];
            assert_eq!(row["cnt"].as_i64(), Some(exp_cnt), "grp={g}");
            let got_sum = row["sum_v"].as_f64().unwrap();
            let got_avg = row["avg_v"].as_f64().unwrap();
            assert!(
                (got_sum - exp_sum).abs() <= 1e-9,
                "grp={g}: {got_sum} vs {exp_sum}"
            );
            assert!(
                (got_avg - exp_avg).abs() <= 1e-12,
                "grp={g}: {got_avg} vs {exp_avg}"
            );
        }
        assert!(res_int.execution.source_workers >= 2, "is_owned={is_owned}");
        if is_owned {
            assert!(
                res_int.execution.operator_workers >= 2,
                "is_owned={is_owned}"
            );
        } else {
            // Unfiltered .vortex direct column-ref aggregates execute via Strategy::Aggregate FileKernel.
            assert_eq!(res_int.execution.operator_workers, 1, "is_owned={is_owned}");
            let filtered_plan =
                VortexRelationalPlan::Aggregate(Box::new(VortexRelationalAggregate {
                    input: VortexRelationalPlan::Filter(Box::new(VortexRelationalFilter {
                        input: scan.clone(),
                        predicate: Expression::new(
                            ExprId::new("grp_eq_grp").unwrap(),
                            ExpressionKind::Compare {
                                left: Box::new(Expression::column(
                                    ExprId::new("grp").unwrap(),
                                    ColumnRef::new("grp").unwrap(),
                                )),
                                op: ComparisonOp::Eq,
                                right: Box::new(Expression::column(
                                    ExprId::new("grp").unwrap(),
                                    ColumnRef::new("grp").unwrap(),
                                )),
                            },
                        ),
                    })),
                    group_by: vec![ColumnRef::new("grp").unwrap()],
                    grouping_sets: None,
                    measures: vec![
                        measure("count", None, "cnt"),
                        measure("sum", Some("val_i64"), "sum_v"),
                        measure("avg", Some("val_i64"), "avg_v"),
                    ],
                }));
            let prepared_filtered = prepare_relational_with_schema_in_session(
                parallel_policy(4, 256 << 20),
                &session,
                |_| Ok(filtered_plan.clone()),
            )
            .unwrap();
            let res_filtered = prepared_filtered
                .collect_jsonl(&CancellationToken::default())
                .unwrap();
            let filtered_rows = json_rows(&res_filtered);
            assert_eq!(filtered_rows.len(), expected_int.len());
            for row in filtered_rows {
                let g = row["grp"].as_i64().unwrap();
                let (exp_cnt, exp_sum, exp_avg, _) = expected_int[&g];
                assert_eq!(row["cnt"].as_i64(), Some(exp_cnt), "filtered grp={g}");
                let got_sum = row["sum_v"].as_f64().unwrap();
                let got_avg = row["avg_v"].as_f64().unwrap();
                assert!(
                    (got_sum - exp_sum).abs() <= 1e-9,
                    "filtered grp={g}: {got_sum} vs {exp_sum}"
                );
                assert!(
                    (got_avg - exp_avg).abs() <= 1e-12,
                    "filtered grp={g}: {got_avg} vs {exp_avg}"
                );
            }
            assert!(
                res_filtered.execution.source_workers >= 2,
                "filtered .vortex source_workers"
            );
            assert!(
                res_filtered.execution.operator_workers >= 2,
                "filtered .vortex operator_workers"
            );
        }

        // Canonical SUM/AVG + borrowed COUNT(DISTINCT): parallel source decode + serial operator.
        let distinct_plan = VortexRelationalPlan::Aggregate(Box::new(VortexRelationalAggregate {
            input: scan.clone(),
            group_by: vec![ColumnRef::new("grp").unwrap()],
            grouping_sets: None,
            measures: vec![
                measure("count", None, "cnt"),
                measure("sum", Some("val_i64"), "sum_v"),
                measure("avg", Some("val_i64"), "avg_v"),
                measure("count_distinct", Some("grp_tag"), "uniq_t"),
            ],
        }));
        let prepared_distinct = prepare_relational_with_schema_in_session(
            parallel_policy(4, 256 << 20),
            &session,
            |schemas| {
                if is_owned {
                    schemas.register_owned_array_source(owned_uri.clone(), owned.clone())?;
                }
                Ok(distinct_plan.clone())
            },
        )
        .unwrap();
        let res_distinct = prepared_distinct
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        for row in json_rows(&res_distinct) {
            let g = row["grp"].as_i64().unwrap();
            let (exp_cnt, exp_sum, exp_avg, exp_uniq) = expected_int[&g];
            assert_eq!(row["cnt"].as_i64(), Some(exp_cnt), "grp={g}");
            assert_eq!(row["uniq_t"].as_u64(), Some(exp_uniq as u64), "grp={g}");
            assert!((row["sum_v"].as_f64().unwrap() - exp_sum).abs() <= 1e-9);
            assert!((row["avg_v"].as_f64().unwrap() - exp_avg).abs() <= 1e-12);
        }
        assert!(
            res_distinct.execution.source_workers >= 2,
            "is_owned={is_owned}"
        );
        assert_eq!(
            res_distinct.execution.operator_workers, 1,
            "is_owned={is_owned}"
        );

        // Pure float SUM (may route via Strategy::Aggregate on .vortex) + combined float SUM +
        // COUNT(DISTINCT) (routes directly through scan_parallel `shared_spec == None` float gate):
        // both keep operator_workers == 1 while source_workers >= 2.
        let flt_plan = VortexRelationalPlan::Aggregate(Box::new(VortexRelationalAggregate {
            input: scan.clone(),
            group_by: vec![ColumnRef::new("grp").unwrap()],
            grouping_sets: None,
            measures: vec![measure("sum", Some("val_f64"), "sum_f")],
        }));
        let prepared_flt = prepare_relational_with_schema_in_session(
            parallel_policy(4, 256 << 20),
            &session,
            |schemas| {
                if is_owned {
                    schemas.register_owned_array_source(owned_uri.clone(), owned.clone())?;
                }
                Ok(flt_plan.clone())
            },
        )
        .unwrap();
        let res_flt = prepared_flt
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        let flt_rows = json_rows(&res_flt);
        assert_eq!(flt_rows.len(), expected_flt.len(), "is_owned={is_owned}");
        for row in flt_rows {
            let g = row["grp"].as_i64().unwrap();
            let got_sum = row["sum_f"].as_f64().unwrap();
            let exp_sum = expected_flt[&g];
            assert!(
                (got_sum - exp_sum).abs() <= 1e-12,
                "grp={g}: {got_sum} vs {exp_sum}"
            );
        }
        assert!(res_flt.execution.source_workers >= 2, "is_owned={is_owned}");
        assert_eq!(res_flt.execution.operator_workers, 1, "is_owned={is_owned}");

        let flt_mixed_plan = VortexRelationalPlan::Aggregate(Box::new(VortexRelationalAggregate {
            input: scan,
            group_by: vec![ColumnRef::new("grp").unwrap()],
            grouping_sets: None,
            measures: vec![
                measure("sum", Some("val_f64"), "sum_f"),
                measure("count_distinct", Some("grp_tag"), "uniq_t"),
            ],
        }));
        let prepared_flt_mixed = prepare_relational_with_schema_in_session(
            parallel_policy(4, 256 << 20),
            &session,
            |schemas| {
                if is_owned {
                    schemas.register_owned_array_source(owned_uri.clone(), owned.clone())?;
                }
                Ok(flt_mixed_plan.clone())
            },
        )
        .unwrap();
        let res_flt_mixed = prepared_flt_mixed
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        for row in json_rows(&res_flt_mixed) {
            let g = row["grp"].as_i64().unwrap();
            assert!((row["sum_f"].as_f64().unwrap() - expected_flt[&g]).abs() <= 1e-12);
            assert_eq!(row["uniq_t"].as_u64(), Some(expected_int[&g].3 as u64));
        }
        assert!(
            res_flt_mixed.execution.source_workers >= 2,
            "is_owned={is_owned}"
        );
        assert_eq!(
            res_flt_mixed.execution.operator_workers, 1,
            "is_owned={is_owned}"
        );
    }
}

#[test]
#[allow(clippy::too_many_lines)]
fn parallel_sealed_join_all_keyed_kinds_and_multi_measure_join_aggregate() {
    let total_rows = 16_384_usize;
    let fact_batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, true),
            Field::new("val", DataType::Int64, false),
        ])),
        vec![
            Arc::new(Int64Array::from_iter(
                (0..total_rows).map(|r| (r % 31 != 0).then_some((r % 64) as i64)),
            )),
            Arc::new(Int64Array::from_iter_values(
                (0..total_rows).map(|r| (r % 19) as i64),
            )),
        ],
    )
    .unwrap();
    let fact_pq =
        ParquetFixture::with_batch_rows(fact_batch.clone(), EnabledStatistics::Chunk, 4_096);

    let vtx_fact_array = StructArray::try_new(
        FieldNames::from(["k", "val"]),
        vec![
            vortex::array::arrays::PrimitiveArray::from_option_iter(
                (0..total_rows).map(|r| (r % 31 != 0).then_some((r % 64) as i64)),
            )
            .into_array(),
            vortex::array::arrays::PrimitiveArray::from_iter(
                (0..total_rows).map(|r| (r % 19) as i64),
            )
            .into_array(),
        ],
        total_rows,
        Validity::NonNullable,
    )
    .unwrap()
    .into_array();
    let fact_vtx = super::super::Fixture::with_strategy_statistics(
        vtx_fact_array,
        4_096,
        vec![],
        crate::local_primitives::native_flat_layout::SequentialNativeFlatLayout::strategy(4),
        vec![Stat::Min, Stat::Max, Stat::NullCount],
    );

    // Dimension has keys 0..31 (matching half of left) plus unmatched right keys 90, 91 and NULL.
    let dim_keys: Vec<Option<i64>> = (0..32_i64)
        .map(Some)
        .chain([Some(90), Some(91), None])
        .collect();
    let dim_vals: Vec<i64> = (0..dim_keys.len()).map(|i| (i as i64) * 3 + 10).collect();
    let dim_batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, true),
            Field::new("dval", DataType::Int64, false),
            Field::new("region", DataType::Int64, false),
        ])),
        vec![
            Arc::new(Int64Array::from_iter(dim_keys)),
            Arc::new(Int64Array::from_iter_values(dim_vals)),
            Arc::new(Int64Array::from_iter_values((0..35_i64).map(|i| i % 4))),
        ],
    )
    .unwrap();
    let dim_pq = ParquetFixture::with_batch_rows(dim_batch, EnabledStatistics::Chunk, 64);

    // 1. Parallel SealedJoin parity vs single-worker oracle across all 6 keyed join kinds on Parquet & .vortex.
    for kind in [
        JoinKind::Inner,
        JoinKind::Left,
        JoinKind::Right,
        JoinKind::Full,
        JoinKind::LeftSemi,
        JoinKind::LeftAnti,
    ] {
        let mut cols = vec![
            VortexRelationalJoinColumn {
                side: Side::Left,
                column: ColumnRef::new("k").unwrap(),
                output_column: "lk".into(),
            },
            VortexRelationalJoinColumn {
                side: Side::Left,
                column: ColumnRef::new("val").unwrap(),
                output_column: "val".into(),
            },
        ];
        if !matches!(kind, JoinKind::LeftSemi | JoinKind::LeftAnti) {
            cols.push(VortexRelationalJoinColumn {
                side: Side::Right,
                column: ColumnRef::new("k").unwrap(),
                output_column: "rk".into(),
            });
            cols.push(VortexRelationalJoinColumn {
                side: Side::Right,
                column: ColumnRef::new("dval").unwrap(),
                output_column: "dval".into(),
            });
        }
        for is_vortex in [false, true] {
            let plan = VortexRelationalPlan::Join(Box::new(VortexRelationalJoin {
                left: if is_vortex {
                    fact_vtx.scan()
                } else {
                    fact_pq.scan(&["k", "val"], None)
                },
                right: dim_pq.scan(&["k", "dval"], None),
                kind,
                keys: vec![VortexRelationalJoinKey {
                    left: ColumnRef::new("k").unwrap(),
                    right: ColumnRef::new("k").unwrap(),
                }],
                condition: None,
                columns: cols.clone(),
            }));
            let serial = prepare_relational_with_schema(parallel_policy(1, 256 << 20), |prep| {
                if !is_vortex {
                    let left = LocalColumnarSource::admit(
                        &fact_pq.path(),
                        "parquet",
                        &prep.shared_memory_pool(),
                        &prep.cancellation_token(),
                    )?;
                    prep.register_local_source(
                        DatasetUri::new(fact_pq.path().display().to_string())?,
                        left,
                    )?;
                }
                let right = LocalColumnarSource::admit(
                    &dim_pq.path(),
                    "parquet",
                    &prep.shared_memory_pool(),
                    &prep.cancellation_token(),
                )?;
                prep.register_local_source(
                    DatasetUri::new(dim_pq.path().display().to_string())?,
                    right,
                )?;
                Ok(plan.clone())
            })
            .unwrap()
            .collect_jsonl(&CancellationToken::default())
            .unwrap();

            let parallel = prepare_relational_with_schema(parallel_policy(4, 256 << 20), |prep| {
                if !is_vortex {
                    let left = LocalColumnarSource::admit(
                        &fact_pq.path(),
                        "parquet",
                        &prep.shared_memory_pool(),
                        &prep.cancellation_token(),
                    )?;
                    prep.register_local_source(
                        DatasetUri::new(fact_pq.path().display().to_string())?,
                        left,
                    )?;
                }
                let right = LocalColumnarSource::admit(
                    &dim_pq.path(),
                    "parquet",
                    &prep.shared_memory_pool(),
                    &prep.cancellation_token(),
                )?;
                prep.register_local_source(
                    DatasetUri::new(dim_pq.path().display().to_string())?,
                    right,
                )?;
                Ok(plan.clone())
            })
            .unwrap()
            .collect_jsonl(&CancellationToken::default())
            .unwrap();

            assert_eq!(
                sorted_rows(json_rows(&parallel)),
                sorted_rows(json_rows(&serial)),
                "kind={kind:?}, is_vortex={is_vortex}"
            );
            assert!(
                parallel.execution.source_workers >= 2,
                "kind={kind:?}, is_vortex={is_vortex}"
            );
            assert!(
                parallel.execution.operator_workers >= 2,
                "kind={kind:?}, is_vortex={is_vortex}"
            );
        }
    }

    // 2. Multi-measure join_aggregate (COUNT(*) + fact SUM/AVG/MIN/MAX + dimension SUM/MAX) across Parquet & .vortex.
    for is_vortex in [false, true] {
        let ja_plan = VortexRelationalPlan::Aggregate(Box::new(VortexRelationalAggregate {
            input: VortexRelationalPlan::Join(Box::new(VortexRelationalJoin {
                left: if is_vortex {
                    fact_vtx.scan()
                } else {
                    fact_pq.scan(&["k", "val"], None)
                },
                right: dim_pq.scan(&["k", "dval", "region"], None),
                kind: JoinKind::Inner,
                keys: vec![VortexRelationalJoinKey {
                    left: ColumnRef::new("k").unwrap(),
                    right: ColumnRef::new("k").unwrap(),
                }],
                condition: None,
                columns: vec![
                    VortexRelationalJoinColumn {
                        side: Side::Right,
                        column: ColumnRef::new("region").unwrap(),
                        output_column: "region".into(),
                    },
                    VortexRelationalJoinColumn {
                        side: Side::Left,
                        column: ColumnRef::new("val").unwrap(),
                        output_column: "val".into(),
                    },
                    VortexRelationalJoinColumn {
                        side: Side::Right,
                        column: ColumnRef::new("dval").unwrap(),
                        output_column: "dval".into(),
                    },
                ],
            })),
            group_by: vec![ColumnRef::new("region").unwrap()],
            grouping_sets: None,
            measures: vec![
                measure("count", None, "cnt"),
                measure("sum", Some("val"), "sum_v"),
                measure("avg", Some("val"), "avg_v"),
                measure("min", Some("val"), "min_v"),
                measure("max", Some("val"), "max_v"),
                measure("sum", Some("dval"), "sum_d"),
                measure("max", Some("dval"), "max_d"),
            ],
        }));
        let res = prepare_relational_with_schema(parallel_policy(4, 256 << 20), |prep| {
            if !is_vortex {
                let left = LocalColumnarSource::admit(
                    &fact_pq.path(),
                    "parquet",
                    &prep.shared_memory_pool(),
                    &prep.cancellation_token(),
                )?;
                prep.register_local_source(
                    DatasetUri::new(fact_pq.path().display().to_string())?,
                    left,
                )?;
            }
            let right = LocalColumnarSource::admit(
                &dim_pq.path(),
                "parquet",
                &prep.shared_memory_pool(),
                &prep.cancellation_token(),
            )?;
            prep.register_local_source(
                DatasetUri::new(dim_pq.path().display().to_string())?,
                right,
            )?;
            Ok(ja_plan.clone())
        })
        .unwrap()
        .collect_jsonl(&CancellationToken::default())
        .unwrap();

        let ja_work = res
            .execution
            .native_io_certificate
            .join_work
            .direct_aggregate;
        assert_eq!(ja_work.chains_considered, 1, "is_vortex={is_vortex}");
        assert!(ja_work.probe_filters_pushed >= 1, "is_vortex={is_vortex}");
        if is_vortex {
            assert!(
                ja_work.source_batches_declined > 0,
                "compressed .vortex join_aggregate declines direct key borrowing cleanly"
            );
        } else {
            assert!(
                ja_work.aggregate_rows > 0 && ja_work.source_batches_direct > 0,
                "multi-measure Parquet join_aggregate must execute directly"
            );
        }
        assert!(
            !res.execution
                .native_io_certificate
                .source_capability_report
                .fallback_attempted
        );

        let mut expected: BTreeMap<i64, (i64, i64, i64, i64, i64, i64)> = BTreeMap::new();
        for r in 0..total_rows {
            if r % 31 != 0 && (r % 64) < 32 {
                let k = (r % 64) as i64;
                let region = k % 4;
                let val = (r % 19) as i64;
                let dval = k * 3 + 10;
                let entry = expected.entry(region).or_insert((0, 0, val, val, 0, dval));
                entry.0 += 1;
                entry.1 += val;
                entry.2 = entry.2.min(val);
                entry.3 = entry.3.max(val);
                entry.4 += dval;
                entry.5 = entry.5.max(dval);
            }
        }
        let actual_rows = json_rows(&res);
        assert_eq!(actual_rows.len(), expected.len(), "is_vortex={is_vortex}");
        for row in actual_rows {
            let region = row["region"].as_i64().unwrap();
            let (cnt, sum_v, min_v, max_v, sum_d, max_d) = expected[&region];
            assert_eq!(row["cnt"].as_i64(), Some(cnt), "region={region}");
            assert_eq!(row["min_v"].as_i64(), Some(min_v), "region={region}");
            assert_eq!(row["max_v"].as_i64(), Some(max_v), "region={region}");
            assert_eq!(row["max_d"].as_i64(), Some(max_d), "region={region}");
            assert!(
                (row["sum_v"].as_f64().unwrap() - (sum_v as f64)).abs() <= 1e-9,
                "region={region}"
            );
            assert!(
                (row["avg_v"].as_f64().unwrap() - ((sum_v as f64) / (cnt as f64))).abs() <= 1e-12,
                "region={region}"
            );
            assert!(
                (row["sum_d"].as_f64().unwrap() - (sum_d as f64)).abs() <= 1e-9,
                "region={region}"
            );
        }
    }
}
