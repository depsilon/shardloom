//! Exact matches must preserve ordered aggregate updates and skip join payloads.

use super::aggregate_tests::{aggregate, measure};
use super::*;
use crate::{
    relational_query::{VortexRelationalPlan, VortexRelationalProject},
    resident_memory_source::{MemoryColumn, MemoryColumnValues, MemorySourceBounds},
};
use serde_json::json;
use shardloom_core::{ExprId, Expression, ExpressionKind};

fn uri(name: &str) -> DatasetUri {
    DatasetUri::new(format!("memory://join-aggregate/{name}")).unwrap()
}

fn scan(name: &str) -> VortexRelationalPlan {
    VortexRelationalPlan::Scan(VortexRelationalScan {
        source_uri: uri(name),
        projection: shardloom_plan::ProjectionRequest::All,
        predicate: None,
    })
}

fn column(side: Side, name: &str, alias: &str) -> VortexRelationalJoinColumn {
    VortexRelationalJoinColumn {
        side,
        column: ColumnRef::new(name).unwrap(),
        output_column: alias.into(),
    }
}

fn star(reverse: bool, alias: bool, function: &str) -> VortexRelationalPlan {
    star_sources(reverse, alias, function, &scan)
}

fn star_sources(
    reverse: bool,
    alias: bool,
    function: &str,
    source: &dyn Fn(&str) -> VortexRelationalPlan,
) -> VortexRelationalPlan {
    let order = if reverse { ["b", "a"] } else { ["a", "b"] };
    let mut input = source("fact");
    let mut retained = vec!["a", "b", "value"];
    for (index, dimension) in order.into_iter().enumerate() {
        let label = if dimension == "a" { "ga" } else { "gb" };
        input = VortexRelationalPlan::Join(Box::new(VortexRelationalJoin {
            left: input,
            right: source(dimension),
            kind: JoinKind::Inner,
            condition: None,
            keys: vec![VortexRelationalJoinKey {
                left: ColumnRef::new(if alias && index == 1 {
                    "probe_key"
                } else {
                    dimension
                })
                .unwrap(),
                right: ColumnRef::new("id").unwrap(),
            }],
            columns: retained
                .iter()
                .map(|name| column(Side::Left, name, name))
                .chain([column(Side::Right, "label", label)])
                .collect(),
        }));
        retained.push(label);
        if alias && index == 0 {
            input = VortexRelationalPlan::Project(Box::new(VortexRelationalProject {
                input,
                expressions: retained
                    .iter()
                    .map(|name| {
                        let alias = if *name == order[1] { "probe_key" } else { name };
                        (
                            alias.into(),
                            Expression::new(
                                ExprId::new(alias).unwrap(),
                                ExpressionKind::Column(ColumnRef::new(*name).unwrap()),
                            ),
                        )
                    })
                    .collect(),
            }));
            for name in &mut retained {
                if *name == order[1] {
                    *name = "probe_key";
                }
            }
        }
    }
    if alias {
        input = VortexRelationalPlan::Project(Box::new(VortexRelationalProject {
            input,
            expressions: [
                ("left_group", "ga"),
                ("right_group", "gb"),
                ("amount", "value"),
            ]
            .into_iter()
            .map(|(alias, name)| {
                (
                    alias.into(),
                    Expression::new(
                        ExprId::new(alias).unwrap(),
                        ExpressionKind::Column(ColumnRef::new(name).unwrap()),
                    ),
                )
            })
            .collect(),
        }));
    }
    aggregate(
        input,
        if alias {
            &["left_group", "right_group"]
        } else {
            &["ga", "gb"]
        },
        vec![measure(
            function,
            (function != "count").then_some(if alias { "amount" } else { "value" }),
            "total",
        )],
    )
}

fn prepare(plan: VortexRelationalPlan, duplicate: bool) -> PreparedVortexRelational {
    prepare_labels(plan, duplicate, false)
}

fn prepare_labels(
    plan: VortexRelationalPlan,
    duplicate: bool,
    text: bool,
) -> PreparedVortexRelational {
    prepare_values(
        plan,
        duplicate,
        text,
        MemoryColumnValues::Int64NonNullable(&[2, 3, 4, 6, 7, 8, 9]),
    )
}

fn prepare_values(
    plan: VortexRelationalPlan,
    duplicate: bool,
    text: bool,
    values: MemoryColumnValues<'_>,
) -> PreparedVortexRelational {
    prepare_relational_with_schema(policy(), |schemas| {
        schemas.register_memory_source(uri("fact"), |session| {
            ResidentMemorySource::from_columns(
                session,
                &[
                    MemoryColumn {
                        name: "a",
                        values: MemoryColumnValues::Int64(&[
                            Some(1),
                            Some(2),
                            Some(3),
                            Some(1),
                            None,
                            Some(8),
                            Some(2),
                        ]),
                    },
                    MemoryColumn {
                        name: "b",
                        values: MemoryColumnValues::Int64NonNullable(&[1, 1, 2, 8, 1, 1, 2]),
                    },
                    MemoryColumn {
                        name: "value",
                        values,
                    },
                ],
                MemorySourceBounds::default(),
            )
        })?;
        schemas.register_memory_source(uri("a"), |session| {
            ResidentMemorySource::from_columns(
                session,
                &[
                    MemoryColumn {
                        name: "id",
                        values: MemoryColumnValues::Int64NonNullable(if duplicate {
                            &[1, 2, 3, 2]
                        } else {
                            &[1, 2, 3]
                        }),
                    },
                    MemoryColumn {
                        name: "label",
                        values: if text {
                            MemoryColumnValues::Utf8(&[
                                Some("shared dimension label"),
                                Some("shared dimension label"),
                                None,
                            ])
                        } else {
                            MemoryColumnValues::Int64NonNullable(if duplicate {
                                &[10, 10, 20, 10]
                            } else {
                                &[10, 10, 20]
                            })
                        },
                    },
                ],
                MemorySourceBounds::default(),
            )
        })?;
        schemas.register_memory_source(uri("b"), |session| {
            ResidentMemorySource::from_columns(
                session,
                &[
                    MemoryColumn {
                        name: "id",
                        values: MemoryColumnValues::Int64NonNullable(&[1, 2]),
                    },
                    MemoryColumn {
                        name: "label",
                        values: if text {
                            MemoryColumnValues::Utf8(&[Some(""), Some("é\0")])
                        } else {
                            MemoryColumnValues::Int64NonNullable(&[30, 40])
                        },
                    },
                ],
                MemorySourceBounds::default(),
            )
        })?;
        Ok(plan)
    })
    .unwrap()
}

#[test]
fn direct_join_aggregate_borrows_offset_text_and_keeps_null_and_byte_identity() {
    for reverse in [false, true] {
        for alias in [false, true] {
            for batch_rows in [1, 3, 7] {
                let prepared = prepare_labels(star(reverse, alias, "sum"), false, true);
                let initial = prepared.snapshot().reserved_without_source_segment_cache();
                let mut rows = Vec::<serde_json::Value>::new();
                let result = prepared
                    .for_each_json_batch(
                        &CancellationToken::default(),
                        batch_rows,
                        1 << 20,
                        |batch| {
                            rows.extend(
                                serde_json::from_str::<Vec<serde_json::Value>>(
                                    batch.values_json.value(),
                                )
                                .unwrap(),
                            );
                            Ok(())
                        },
                    )
                    .unwrap();
                let names = if alias {
                    ["left_group", "right_group"]
                } else {
                    ["ga", "gb"]
                };
                assert_eq!(
                    rows,
                    vec![
                        json!({names[0]:"shared dimension label", names[1]:"", "total":5.0}),
                        json!({names[0]:null, names[1]:"é\0", "total":4.0}),
                        json!({names[0]:"shared dimension label", names[1]:"é\0", "total":9.0}),
                    ]
                );
                let work = result.native_io_certificate.join_work.direct_aggregate;
                assert_eq!(work.aggregate_rows, 4, "{work:?}");
                assert_eq!(work.source_batches_declined, 0, "{work:?}");
                drop(result);
                assert_eq!(
                    prepared.snapshot().reserved_without_source_segment_cache(),
                    initial
                );
            }
        }
    }
}

#[test]
fn direct_join_aggregate_both_orders_aliases_and_batches_keep_exact_groups() {
    for reverse in [false, true] {
        for alias in [false, true] {
            for batch_rows in [1, 2, 7] {
                for (function, totals) in [
                    ("sum", [json!(5.0), json!(4.0), json!(9.0)]),
                    ("avg", [json!(2.5), json!(4.0), json!(9.0)]),
                    ("min", [json!(2), json!(4), json!(9)]),
                    ("max", [json!(3), json!(4), json!(9)]),
                    ("count", [json!(2), json!(1), json!(1)]),
                ] {
                    let prepared = prepare(star(reverse, alias, function), false);
                    let initial = prepared.snapshot().reserved_without_source_segment_cache();
                    let mut rows = Vec::<serde_json::Value>::new();
                    #[cfg(debug_assertions)]
                    let observation =
                        crate::local_primitives::native_relational_observation::Session::start();
                    let result = prepared
                        .for_each_json_batch(
                            &CancellationToken::default(),
                            batch_rows,
                            1 << 20,
                            |batch| {
                                rows.extend(
                                    serde_json::from_str::<Vec<serde_json::Value>>(
                                        batch.values_json.value(),
                                    )
                                    .unwrap(),
                                );
                                Ok(())
                            },
                        )
                        .unwrap();
                    let names = if alias {
                        ["left_group", "right_group"]
                    } else {
                        ["ga", "gb"]
                    };
                    let expected: Vec<_> = [(10, 30), (20, 40), (10, 40)]
                        .into_iter()
                        .zip(totals)
                        .map(|((a, b), total)| json!({names[0]:a, names[1]:b, "total":total}))
                        .collect();
                    assert_eq!(rows, expected, "{reverse}/{alias}/{batch_rows}/{function}");
                    let work = result.native_io_certificate.join_work.direct_aggregate;
                    assert_eq!(work.chains_considered, 1);
                    assert!(work.source_batches_direct > 0, "{work:?}");
                    assert_eq!(work.source_batches_declined, 0, "{work:?}");
                    assert_eq!(work.aggregate_rows, 4);
                    assert!(work.match_rows > work.aggregate_rows);
                    assert!(work.validated_cells >= work.match_rows);
                    #[cfg(debug_assertions)]
                    {
                        let counts = observation.finish();
                        assert_eq!(counts.join_output_rows, 0);
                        assert_eq!(counts.join_output_cells, 0);
                    }
                    assert!(result.native_io_certificate.is_certified());
                    drop(result);
                    assert_eq!(
                        prepared.snapshot().reserved_without_source_segment_cache(),
                        initial
                    );
                }
            }
        }
    }
}

#[test]
fn direct_join_aggregate_duplicate_dimension_reuses_ordinary_multiplicity() {
    for reverse in [false, true] {
        let prepared = prepare(star(reverse, false, "sum"), true);
        let result = prepared
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        assert_eq!(
            json_rows(&result),
            vec![
                json!({"ga":10,"gb":30,"total":8.0}),
                json!({"ga":20,"gb":40,"total":4.0}),
                json!({"ga":10,"gb":40,"total":18.0}),
            ]
        );
        let work = result
            .execution
            .native_io_certificate
            .join_work
            .direct_aggregate;
        assert_eq!(work.chains_declined_duplicates, 1);
        assert_eq!(work.source_batches_direct, 0);
        assert_eq!(work.match_rows, 0);
        assert!(work.source_batches_declined > 0);
        assert_eq!(result.execution.prepared_sources, 3);
    }
}

fn file_star(
    a: &[i64],
    b: &[i64],
    values: &[f64],
    duplicate: bool,
) -> (PreparedVortexRelational, [Fixture; 3]) {
    file_star_aggregate(a, b, values, duplicate, "sum", false)
}

fn file_star_aggregate(
    a: &[i64],
    b: &[i64],
    values: &[f64],
    duplicate: bool,
    function: &str,
    scalar: bool,
) -> (PreparedVortexRelational, [Fixture; 3]) {
    let fact = Fixture::new(
        StructArray::new(
            FieldNames::from(["a", "b", "value"]),
            vec![
                PrimitiveArray::from_iter(a.iter().copied()).into_array(),
                PrimitiveArray::from_iter(b.iter().copied()).into_array(),
                PrimitiveArray::from_iter(values.iter().copied()).into_array(),
            ],
            values.len(),
            Validity::NonNullable,
        )
        .into_array(),
        values.len().max(1),
    );
    let dimension = |label: i64, count: usize| {
        Fixture::new(
            StructArray::new(
                FieldNames::from(["id", "label"]),
                vec![
                    PrimitiveArray::from_iter(std::iter::repeat_n(1_i64, count)).into_array(),
                    PrimitiveArray::from_iter(std::iter::repeat_n(label, count)).into_array(),
                ],
                count,
                Validity::NonNullable,
            )
            .into_array(),
            1,
        )
    };
    let a = dimension(10, if duplicate { 2 } else { 1 });
    let b = dimension(20, 1);
    let mut plan = star_sources(false, false, function, &|name| match name {
        "fact" => fact.scan(),
        "a" => a.scan(),
        "b" => b.scan(),
        _ => unreachable!(),
    });
    if scalar {
        let VortexRelationalPlan::Aggregate(aggregate) = &mut plan else {
            unreachable!();
        };
        aggregate.group_by.clear();
    }
    (prepare_relational(&plan, policy()).unwrap(), [fact, a, b])
}

#[test]
fn direct_join_aggregate_scalar_nullable_measure_keeps_count_and_empty_state() {
    for reverse in [false, true] {
        for all_null in [false, true] {
            for (function, expected) in [
                ("count", json!(2)),
                ("sum", json!(12.0)),
                ("avg", json!(6.0)),
                ("min", json!(3)),
                ("max", json!(9)),
            ] {
                let mut plan = star(reverse, true, function);
                let VortexRelationalPlan::Aggregate(aggregate) = &mut plan else {
                    unreachable!();
                };
                aggregate.group_by.clear();
                aggregate.measures = vec![measure(function, Some("amount"), "total")];
                let values = if all_null {
                    [None; 7]
                } else {
                    [None, Some(3), None, Some(6), Some(7), Some(8), Some(9)]
                };
                let prepared =
                    prepare_values(plan, false, false, MemoryColumnValues::Int64(&values));
                let result = prepared
                    .collect_jsonl(&CancellationToken::default())
                    .unwrap();
                let total = if all_null {
                    if function == "count" {
                        json!(0)
                    } else {
                        json!(null)
                    }
                } else {
                    expected
                };
                assert_eq!(json_rows(&result), vec![json!({"total": total})]);
                assert_eq!(
                    result
                        .execution
                        .native_io_certificate
                        .join_work
                        .direct_aggregate
                        .aggregate_rows,
                    4
                );
            }
        }
    }
}

#[test]
fn direct_join_aggregate_empty_and_no_match_inputs_keep_scalar_and_grouped_schema() {
    for no_match in [false, true] {
        for scalar in [false, true] {
            for function in ["count", "sum", "avg", "min", "max"] {
                let (prepared, _fixtures) = file_star_aggregate(
                    if no_match { &[8] } else { &[] },
                    if no_match { &[1] } else { &[] },
                    if no_match { &[2.0] } else { &[] },
                    false,
                    function,
                    scalar,
                );
                let result = prepared
                    .collect_jsonl(&CancellationToken::default())
                    .unwrap();
                let expected = if scalar {
                    vec![json!({"total": if function == "count" { json!(0) } else { json!(null) }})]
                } else {
                    Vec::new()
                };
                assert_eq!(json_rows(&result), expected);
                let work = result
                    .execution
                    .native_io_certificate
                    .join_work
                    .direct_aggregate;
                assert_eq!(work.chains_considered, 1);
                assert_eq!(work.aggregate_rows, 0);
                assert_eq!(work.source_batches_declined, 0);
            }
        }
    }
}

#[test]
fn direct_join_aggregate_pressure_and_cancel_refund_then_allow_fresh_execution() {
    let prepared = prepare_labels(star(false, false, "sum"), false, true);
    let memory = prepared.session.memory();
    let initial = memory.snapshot().reserved_bytes;
    let pressure = memory
        .reserve(memory.snapshot().limit_bytes - initial - 1)
        .unwrap();
    assert!(
        prepared
            .collect_jsonl(&CancellationToken::default())
            .is_err()
    );
    assert_eq!(prepared.snapshot().completed_executions, 0);
    assert_eq!(memory.snapshot().reserved_bytes, initial + pressure.bytes());
    drop(pressure);
    assert_eq!(memory.snapshot().reserved_bytes, initial);
    let token = CancellationToken::default();
    let mut delivered = 0;
    let error = prepared
        .for_each_batch(&token, |_, _| {
            delivered += 1;
            token.cancel();
            Ok(())
        })
        .err()
        .unwrap();
    assert!(error.to_string().contains("execution cancelled"), "{error}");
    assert_eq!(delivered, 1);
    assert_eq!(prepared.snapshot().completed_executions, 0);
    assert_eq!(memory.snapshot().reserved_bytes, initial);
    let result = prepared
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(
        result
            .execution
            .native_io_certificate
            .join_work
            .direct_aggregate
            .aggregate_rows,
        4
    );
    drop(result);
    assert_eq!(memory.snapshot().reserved_bytes, initial);
}

#[test]
fn direct_join_aggregate_keeps_earlier_payload_errors_before_later_rejection() {
    for duplicate in [false, true] {
        for rejected_earlier in [false, true] {
            let (prepared, _fixtures) = file_star(
                &[if rejected_earlier { 8 } else { 1 }, 1],
                &[8, 1],
                &[f64::NAN, 2.0],
                duplicate,
            );
            let baseline = prepared.snapshot().reserved_without_source_segment_cache();
            let result = prepared.collect_jsonl(&CancellationToken::default());
            if rejected_earlier {
                let result = result.unwrap();
                assert_eq!(
                    json_rows(&result),
                    vec![json!({"ga":10,"gb":20,"total":if duplicate {4.0} else {2.0}})]
                );
                if !duplicate {
                    assert_eq!(
                        result
                            .execution
                            .native_io_certificate
                            .join_work
                            .direct_aggregate
                            .aggregate_rows,
                        1
                    );
                }
            } else {
                assert_eq!(
                    result.err().unwrap().to_string(),
                    "native result batch: numeric value differs from its finite declared dtype; no fallback execution was attempted"
                );
            }
            assert_eq!(
                prepared.snapshot().reserved_without_source_segment_cache(),
                baseline
            );
        }
    }
}

#[test]
fn direct_join_aggregate_preserves_floating_update_order_and_overflow() {
    for batch_rows in [1, 2, 3] {
        let (prepared, _fixtures) = file_star(&[1, 1, 1], &[1, 1, 1], &[1e16, 1.0, -1e16], false);
        let mut rows = Vec::<serde_json::Value>::new();
        let result = prepared
            .for_each_json_batch(
                &CancellationToken::default(),
                batch_rows,
                1 << 20,
                |batch| {
                    rows.extend(
                        serde_json::from_str::<Vec<serde_json::Value>>(batch.values_json.value())
                            .unwrap(),
                    );
                    Ok(())
                },
            )
            .unwrap();
        assert_eq!(rows, vec![json!({"ga":10,"gb":20,"total":0.0})]);
        assert_eq!(
            result
                .native_io_certificate
                .join_work
                .direct_aggregate
                .aggregate_rows,
            3
        );
        let (prepared, _fixtures) = file_star(
            &[1, 1, 1],
            &[1, 1, 1],
            &[f64::MAX, f64::MAX, -f64::MAX],
            false,
        );
        let error = prepared
            .for_each_json_batch(&CancellationToken::default(), batch_rows, 1 << 20, |_| {
                Ok(())
            })
            .err()
            .unwrap();
        assert!(error.to_string().contains("nonfinite"), "{error}");
    }
}

#[test]
fn direct_join_aggregate_completed_output_owns_copied_group_payloads() {
    use vortex::array::arrays::{VarBinViewArray, varbinview::VarBinViewArrayExt as _};
    let prepared = prepare_labels(star(false, false, "sum"), false, true);
    let memory = prepared.session.memory().clone();
    let completed = prepared
        .execute_native(&CancellationToken::default())
        .unwrap();
    assert_eq!(
        completed
            .execution
            .native_io_certificate
            .join_work
            .direct_aggregate
            .aggregate_rows,
        4
    );
    let mut execution = completed.result.create_execution_ctx();
    let arrays = completed.result.arrays().to_vec();
    drop((completed, prepared));
    assert!(memory.snapshot().reserved_bytes > 0);
    let groups = crate::local_primitives::logical_field_from_native_array(&arrays[0], "ga")
        .unwrap()
        .execute::<VarBinViewArray>(&mut execution)
        .unwrap();
    assert_eq!(groups.utf8_slice_at(0).unwrap(), "shared dimension label");
    drop((groups, arrays, execution));
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn direct_join_aggregate_groups_across_dictionary_codebooks_and_build_chunks() {
    use vortex::array::arrays::{DictArray, VarBinViewArray};
    let dictionary = |labels: [&str; 2]| {
        DictArray::try_new(
            PrimitiveArray::from_iter([0_u8, 1]).into_array(),
            VarBinViewArray::from_iter_str(labels).into_array(),
        )
        .unwrap()
        .into_array()
    };
    let first = dictionary(["shared long label", "other long label"]);
    let second = dictionary(["other long label", "shared long label"]);
    let chunks = [([1_i64, 2], first), ([3_i64, 4], second)].map(|(keys, labels)| {
        StructArray::from_fields(&[
            ("id", PrimitiveArray::from_iter(keys).into_array()),
            ("label", labels),
        ])
        .unwrap()
        .into_array()
    });
    let a = dictionary_chunks(chunks);
    let b = Fixture::new(
        StructArray::from_fields(&[
            ("id", PrimitiveArray::from_iter([1_i64]).into_array()),
            ("label", PrimitiveArray::from_iter([20_i64]).into_array()),
        ])
        .unwrap()
        .into_array(),
        1,
    );
    let fact = Fixture::new(
        StructArray::from_fields(&[
            (
                "a",
                PrimitiveArray::from_iter([1_i64, 2, 3, 4]).into_array(),
            ),
            ("b", PrimitiveArray::from_iter([1_i64; 4]).into_array()),
            (
                "value",
                PrimitiveArray::from_iter([1_i64, 2, 3, 4]).into_array(),
            ),
        ])
        .unwrap()
        .into_array(),
        1,
    );
    for reverse in [false, true] {
        let plan = star_sources(reverse, true, "sum", &|name| match name {
            "fact" => fact.scan(),
            "a" => a.scan(),
            "b" => b.scan(),
            _ => unreachable!(),
        });
        let prepared = prepare_relational(&plan, policy()).unwrap();
        let result = prepared
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        assert_eq!(
            json_rows(&result),
            vec![
                json!({"left_group":"shared long label", "right_group":20, "total":5.0}),
                json!({"left_group":"other long label", "right_group":20, "total":5.0}),
            ]
        );
        let work = result
            .execution
            .native_io_certificate
            .join_work
            .direct_aggregate;
        assert_eq!(work.aggregate_rows, 4, "{work:?}");
        assert_eq!(work.source_batches_declined, 0, "{work:?}");
    }
}

fn dictionary_chunks(chunks: [ArrayRef; 2]) -> Fixture {
    let directory = std::env::temp_dir().join(format!(
        "shardloom-join-aggregate-dictionaries-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed),
    ));
    fs::create_dir(&directory).unwrap();
    let fixture = Fixture(directory);
    let runtime = CurrentThreadRuntime::new();
    let session = VortexSession::default().with_handle(runtime.handle());
    let mut file = fs::File::create(fixture.path()).unwrap();
    let mut writer = session
        .write_options()
        .with_strategy(
            crate::local_primitives::native_flat_layout::SequentialNativeFlatLayout::strategy(2),
        )
        .blocking(&runtime)
        .writer(&mut file, chunks[0].dtype().clone());
    for chunk in chunks {
        writer.push(chunk).unwrap();
    }
    writer.finish().unwrap();
    fixture
}

#[test]
fn direct_join_aggregate_retains_only_observed_high_cardinality_groups() {
    let rows = 2049_u32;
    let fact = Fixture::new(
        StructArray::from_fields(&[
            ("a", PrimitiveArray::from_iter(0..rows).into_array()),
            (
                "b",
                PrimitiveArray::from_iter(std::iter::repeat_n(
                    1_i64,
                    usize::try_from(rows).unwrap(),
                ))
                .into_array(),
            ),
            ("value", PrimitiveArray::from_iter(0..rows).into_array()),
        ])
        .unwrap()
        .into_array(),
        257,
    );
    let a = Fixture::new(
        StructArray::from_fields(&[
            ("id", PrimitiveArray::from_iter(0..rows).into_array()),
            ("label", PrimitiveArray::from_iter(0..rows).into_array()),
        ])
        .unwrap()
        .into_array(),
        257,
    );
    let b = Fixture::new(
        StructArray::from_fields(&[
            ("id", PrimitiveArray::from_iter([1_i64]).into_array()),
            ("label", PrimitiveArray::from_iter([9_i64]).into_array()),
        ])
        .unwrap()
        .into_array(),
        1,
    );
    let plan = star_sources(false, false, "sum", &|name| match name {
        "fact" => fact.scan(),
        "a" => a.scan(),
        "b" => b.scan(),
        _ => unreachable!(),
    });
    let prepared = prepare_relational(&plan, policy()).unwrap();
    let result = prepared
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(
        json_rows(&result),
        (0..rows)
            .map(|row| json!({"ga":row,"gb":9,"total":f64::from(row)}))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        result
            .execution
            .native_io_certificate
            .join_work
            .direct_aggregate
            .aggregate_rows,
        u64::from(rows)
    );
}
