//! Actual candidate work, finite admission, complete values and owner cleanup.

use super::*;
use crate::local_primitives::prepared_relational::cyclic_exists::observation;

fn prepare(values: &[Relation; 3], plan: VortexRelationalPlan) -> PreparedVortexRelational {
    prepare_relational_with_schema(policy(), |schemas| {
        for (name, value) in ["r", "s", "t"].into_iter().zip(values) {
            schemas.register_memory_source(uri(name), |session| value.source(session))?;
        }
        Ok(plan)
    })
    .unwrap()
}

fn complete(
    prepared: &PreparedVortexRelational,
    expected: &[Value],
    batch_rows: usize,
    wide: bool,
) -> observation::Counts {
    let memory = prepared.session.memory().clone();
    let baseline = memory.snapshot().reserved_bytes;
    let mut fields = vec![
        (
            "outer_id".to_owned(),
            DType::Primitive(PType::I64, Nullability::NonNullable),
        ),
        (
            "b".to_owned(),
            DType::Primitive(PType::I64, Nullability::Nullable),
        ),
        (
            "a".to_owned(),
            DType::Primitive(PType::I64, Nullability::Nullable),
        ),
    ];
    if wide {
        fields.extend((0..8).map(|index| {
            (
                format!("payload_{index}"),
                DType::Primitive(PType::I64, Nullability::NonNullable),
            )
        }));
    }
    let schema = serde_json::to_value(DType::struct_(fields, Nullability::NonNullable)).unwrap();
    let mut rows = Vec::new();
    observation::reset();
    let result = prepared
        .for_each_json_batch(
            &CancellationToken::default(),
            batch_rows,
            1 << 20,
            |batch| {
                assert!(batch.rows <= batch_rows);
                assert_eq!(
                    serde_json::from_str::<Value>(batch.result_schema_json.value()).unwrap(),
                    schema
                );
                rows.extend(serde_json::from_str::<Vec<Value>>(batch.values_json.value()).unwrap());
                Ok(())
            },
        )
        .unwrap();
    assert_eq!(rows, expected);
    assert_eq!(result.output_rows, expected.len() as u64);
    assert!(result.native_io_certificate.is_certified());
    assert!(!result.native_io_certificate.side_effects.fallback_attempted);
    let counts = observation::snapshot();
    drop(result);
    assert_eq!(memory.snapshot().reserved_bytes, baseline);
    counts
}

fn grid(rows: i64, side: i64) -> Relation {
    Relation::new(
        &(0..rows)
            .map(|row| (Some(row % side), Some(row / side)))
            .collect::<Vec<_>>(),
    )
}

fn prepare_wide(values: &[Relation; 3]) -> PreparedVortexRelational {
    let names = (0..8)
        .map(|index| format!("payload_{index}"))
        .collect::<Vec<_>>();
    let payloads = (0..8)
        .map(|index| {
            values[0]
                .id
                .iter()
                .map(|id| id * 17 - 3 * index)
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    prepare_relational_with_schema(policy(), |schemas| {
        for (name, value) in ["r", "s", "t"].into_iter().zip(values) {
            schemas.register_memory_source(uri(name), |session| {
                if name != "r" {
                    return value.source(session);
                }
                let mut columns = vec![
                    MemoryColumn {
                        name: "x",
                        values: MemoryColumnValues::Int64(&value.x),
                    },
                    MemoryColumn {
                        name: "y",
                        values: MemoryColumnValues::Int64(&value.y),
                    },
                    MemoryColumn {
                        name: "id",
                        values: MemoryColumnValues::Int64NonNullable(&value.id),
                    },
                ];
                columns.extend(
                    names
                        .iter()
                        .zip(&payloads)
                        .map(|(name, values)| MemoryColumn {
                            name,
                            values: MemoryColumnValues::Int64NonNullable(values),
                        }),
                );
                ResidentMemorySource::from_columns(session, &columns, MemorySourceBounds::default())
            })?;
        }
        let VortexRelationalPlan::Join(mut plan) = existence_plan(JoinKind::LeftSemi, false) else {
            unreachable!()
        };
        plan.columns
            .extend(names.iter().map(|name| column(Side::Left, name, name)));
        Ok(VortexRelationalPlan::Join(plan))
    })
    .unwrap()
}

#[test]
fn cyclic_candidate_counts_actual_work_for_all_four_frozen_targets() {
    for case in ["abundant", "rare", "absent", "skew_wide"] {
        let mut values = [grid(4096, 64), grid(4096, 64), grid(4096, 64)];
        for row in 0..4096 {
            if case == "absent" || (case == "rare" && values[2].y[row] != Some(63)) {
                values[2].x[row] = values[2].x[row].map(|value| value + 1024);
            }
        }
        let wide = case == "skew_wide";
        if wide {
            values[0].y.fill(Some(7));
            values[1] = Relation::new(
                &(0..8192)
                    .map(|row| (Some(7), Some(row % 64)))
                    .collect::<Vec<_>>(),
            );
        }
        let expected = if case == "absent" {
            Vec::new()
        } else {
            values[0]
                .id
                .iter()
                .enumerate()
                .map(|(row, id)| {
                    let mut value =
                        json!({"outer_id":id,"b":values[0].y[row],"a":values[0].x[row]});
                    if wide {
                        for index in 0..8 {
                            value[format!("payload_{index}")] = json!(id * 17 - 3 * index);
                        }
                    }
                    value
                })
                .collect()
        };
        let prepared = if wide {
            prepare_wide(&values)
        } else {
            prepare_existence(&values, JoinKind::LeftSemi, false)
        };
        let memory = prepared.session.memory().clone();
        for call in 0..2 {
            let counts = complete(&prepared, &expected, 2048, wide);
            let visits = if matches!(case, "rare" | "absent") {
                262_144
            } else {
                4096
            };
            assert_eq!(
                counts,
                observation::Counts {
                    executions: 1,
                    last_rows: 4096,
                    middle_rows: if wide { 8192 } else { 4096 },
                    outer_rows: 4096,
                    middle_visits: visits,
                    last_probes: visits,
                    witness_exits: expected.len(),
                    gathered_rows: expected.len(),
                },
                "{case}, call {call}"
            );
            eprintln!(
                "CYCLIC_EXISTS_WORK {}",
                json!({
                    "case":case,"call":call,"executions":counts.executions,
                    "t_build_rows":counts.last_rows,"s_build_rows":counts.middle_rows,"r_rows":counts.outer_rows,
                    "s_visits":counts.middle_visits,"t_probes":counts.last_probes,
                    "witness_exits":counts.witness_exits,"r_gathered_rows":counts.gathered_rows,
                })
            );
        }
        drop(prepared);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

fn nullable_duplicates() -> [Relation; 3] {
    let patterns = [
        [
            (Some(2), Some(7)),
            (Some(1), Some(7)),
            (None, Some(7)),
            (Some(2), None),
            (Some(2), Some(7)),
            (Some(-1), Some(9)),
        ],
        [
            (Some(7), Some(3)),
            (Some(7), Some(4)),
            (Some(7), Some(3)),
            (None, Some(3)),
            (Some(9), None),
            (Some(9), Some(5)),
        ],
        [
            (Some(2), Some(3)),
            (Some(1), Some(3)),
            (Some(2), Some(3)),
            (Some(2), None),
            (None, Some(3)),
            (Some(-1), Some(4)),
        ],
    ];
    patterns.map(|rows| Relation::new(&rows.into_iter().cycle().take(132).collect::<Vec<_>>()))
}

#[test]
fn cyclic_candidate_preserves_admitted_nulls_duplicates_aliases_and_batch_boundaries() {
    let values = nullable_duplicates();
    let expected = existence_reference(&values, JoinKind::LeftSemi);
    for reverse_keys in [false, true] {
        for reverse_projection in [false, true] {
            let VortexRelationalPlan::Join(mut plan) =
                existence_plan(JoinKind::LeftSemi, reverse_keys)
            else {
                unreachable!()
            };
            if reverse_projection && let VortexRelationalPlan::Join(inner) = &mut plan.right {
                inner.columns.reverse();
            }
            let prepared = prepare(&values, VortexRelationalPlan::Join(plan));
            let memory = prepared.session.memory().clone();
            for batch_rows in [1, 2, 2048] {
                let counts = complete(&prepared, &expected, batch_rows, false);
                assert_eq!(counts.executions, 1);
                assert_eq!(counts.outer_rows, 132);
                assert_eq!(counts.witness_exits, 66);
                assert_eq!(counts.gathered_rows, 66);
                assert_eq!(counts.middle_visits, 66 + 22 * 44);
                assert_eq!(counts.last_probes, 66 + 22 * 22);
            }
            drop(prepared);
            assert_eq!(memory.snapshot().reserved_bytes, 0);
        }
    }
    let values = [
        Relation::new(&vec![(Some(2), Some(7)); 2051]),
        Relation::new(&[(Some(7), Some(3)); 128]),
        Relation::new(&[(Some(2), Some(3)); 128]),
    ];
    let prepared = prepare_existence(&values, JoinKind::LeftSemi, false);
    let expected = existence_reference(&values, JoinKind::LeftSemi);
    for batch_rows in [1, 2, 2048] {
        let counts = complete(&prepared, &expected, batch_rows, false);
        assert_eq!(counts.executions, 1);
        assert_eq!(counts.gathered_rows, 2051);
        assert_eq!(counts.middle_visits, 2051);
    }
    let memory = prepared.session.memory().clone();
    drop(prepared);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn cyclic_candidate_drains_consumer_failure_probe_cancellation_and_partial_allocation_denial() {
    let values = [
        Relation::new(&[(Some(2), Some(7)); 128]),
        Relation::new(&[(Some(7), Some(3)); 128]),
        Relation::new(&[(Some(2), Some(3)); 128]),
    ];
    let prepared = prepare_existence(&values, JoinKind::LeftSemi, false);
    let memory = prepared.session.memory().clone();
    let baseline = memory.snapshot().reserved_bytes;
    for cancel in [false, true] {
        observation::reset();
        let token = CancellationToken::default();
        let failure = prepared.for_each_json_batch(&token, 2, 4096, |_| {
            if cancel {
                token.cancel();
                Ok(())
            } else {
                Err(ShardLoomError::InvalidOperation(
                    "cyclic candidate consumer failed".into(),
                ))
            }
        });
        assert!(failure.is_err());
        let counts = observation::snapshot();
        assert_eq!(counts.executions, 1);
        assert_eq!(counts.gathered_rows, 2);
        assert_eq!(memory.snapshot().reserved_bytes, baseline);
    }
    // Include the existing report metadata admission, then observe failures
    // after candidate entry and real input delivery instead of guessing which
    // owner a small residual grant reaches.
    let mut partial_denials = 0;
    let mut successes = 0;
    for remaining in [
        0, 1024, 32_768, 65_536, 69_632, 73_728, 81_920, 98_304, 131_072, 262_144,
    ] {
        observation::reset();
        let occupied = memory
            .reserve(memory.snapshot().limit_bytes - baseline - remaining)
            .unwrap();
        let mut rows = 0;
        let result = prepared.for_each_batch(&CancellationToken::default(), |array, _| {
            rows += array.len();
            Ok(())
        });
        let counts = observation::snapshot();
        match result {
            Ok(report) => {
                assert_eq!(rows, 128);
                assert_eq!(report.output_rows, 128);
                assert_eq!(counts.executions, 1);
                successes += 1;
            }
            Err(error) => {
                assert!(error.to_string().contains("memory reservation denied"));
                if counts.executions == 1 && counts.last_rows > 0 {
                    partial_denials += 1;
                }
            }
        }
        drop(occupied);
        assert_eq!(memory.snapshot().reserved_bytes, baseline);
    }
    assert!(partial_denials > 0);
    assert!(successes > 0);
    assert_eq!(
        complete(
            &prepared,
            &existence_reference(&values, JoinKind::LeftSemi),
            2048,
            false
        )
        .executions,
        1
    );
    drop(prepared);
    assert_eq!(memory.snapshot().reserved_bytes, 0);

    // A long all-NULL c band never probes T. Cancellation must still stop its walk.
    let values = [
        Relation::new(&[(Some(2), Some(7)); 128]),
        Relation::new(&vec![(Some(7), None); 4096]),
        Relation::new(&[(Some(2), Some(3)); 128]),
    ];
    let prepared = prepare_existence(&values, JoinKind::LeftSemi, false);
    let memory = prepared.session.memory().clone();
    let baseline = memory.snapshot().reserved_bytes;
    observation::reset();
    let token = CancellationToken::default();
    observation::cancel_at_visit(7, token.clone());
    assert!(prepared.collect_jsonl(&token).is_err());
    let counts = observation::snapshot();
    assert_eq!(counts.executions, 1);
    assert!((7..=1030).contains(&counts.middle_visits));
    assert_eq!(counts.last_probes, 0);
    assert_eq!(counts.gathered_rows, 0);
    assert_eq!(memory.snapshot().reserved_bytes, baseline);
    assert_eq!(complete(&prepared, &[], 2048, false).executions, 1);
    drop(prepared);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn cyclic_candidate_admits_identity_but_keeps_real_transformations_ordinary() {
    let values = [grid(128, 16), grid(128, 16), grid(128, 16)];
    for source in 0..3 {
        for rows in [0, 127] {
            let mut small = values.clone();
            small[source] = grid(rows, 16);
            let prepared = prepare_existence(&small, JoinKind::LeftSemi, false);
            assert_eq!(
                complete(
                    &prepared,
                    &existence_reference(&small, JoinKind::LeftSemi),
                    2048,
                    false
                )
                .executions,
                0
            );
        }
    }
    let anti = prepare_existence(&values, JoinKind::LeftAnti, false);
    assert_eq!(
        complete(
            &anti,
            &existence_reference(&values, JoinKind::LeftAnti),
            2048,
            false
        )
        .executions,
        0
    );

    for shape in ["filtered", "identity", "computed", "reordered"] {
        let VortexRelationalPlan::Join(mut plan) = existence_plan(JoinKind::LeftSemi, false) else {
            unreachable!()
        };
        if shape != "filtered" {
            let names = if shape == "reordered" {
                ["id", "x", "y"]
            } else {
                ["x", "y", "id"]
            };
            plan.left = VortexRelationalPlan::Project(Box::new(
                crate::relational_query::VortexRelationalProject {
                    input: scan("r"),
                    expressions: names
                        .into_iter()
                        .map(|name| {
                            use shardloom_core::{
                                BinaryOp, ExprId, Expression, ExpressionKind, ScalarValue,
                            };
                            let column = Expression::column(
                                ExprId::new(name).unwrap(),
                                ColumnRef::new(name).unwrap(),
                            );
                            let expression = if shape == "computed" && name == "id" {
                                Expression::new(
                                    ExprId::new("computed_id").unwrap(),
                                    ExpressionKind::Binary {
                                        left: Box::new(column),
                                        op: BinaryOp::Add,
                                        right: Box::new(Expression::literal(
                                            ExprId::new("zero").unwrap(),
                                            ScalarValue::Int64(0),
                                        )),
                                    },
                                )
                            } else {
                                column
                            };
                            (name.to_owned(), expression)
                        })
                        .collect(),
                },
            ));
        } else if let VortexRelationalPlan::Scan(scan) = &mut plan.left {
            scan.predicate = Some(shardloom_core::PredicateExpr::Compare {
                column: ColumnRef::new("id").unwrap(),
                op: shardloom_core::ComparisonOp::Gt,
                value: shardloom_core::StatValue::Int64(-1),
            });
        }
        let prepared = prepare(&values, VortexRelationalPlan::Join(plan));
        let memory = prepared.session.memory().clone();
        assert_eq!(
            complete(
                &prepared,
                &existence_reference(&values, JoinKind::LeftSemi),
                2048,
                false
            )
            .executions,
            usize::from(shape == "identity"),
            "{shape}"
        );
        drop(prepared);
        assert_eq!(memory.snapshot().reserved_bytes, 0, "{shape}");
    }
}

#[test]
fn cyclic_candidate_handles_admitted_all_null_sources_and_full_integer_width() {
    let values = [grid(128, 16), grid(128, 16), grid(128, 16)];
    for source in 0..3 {
        let mut nulls = values.clone();
        nulls[source] = Relation::new(&[(None, None); 128]);
        let prepared = prepare_existence(&nulls, JoinKind::LeftSemi, false);
        let counts = complete(&prepared, &[], 2048, false);
        assert_eq!(counts.executions, 1);
        assert_eq!(counts.witness_exits, 0);
        assert_eq!(counts.gathered_rows, 0);
        let memory = prepared.session.memory().clone();
        drop(prepared);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
    let values = [
        Relation::new(
            &[
                (Some(i64::MAX), Some(i64::MIN)),
                (Some(i64::MAX - 1), Some(i64::MIN)),
                (Some(9_007_199_254_740_993), Some(7)),
                (Some(9_007_199_254_740_992), Some(7)),
            ]
            .repeat(32),
        ),
        Relation::new(&[(Some(i64::MIN), Some(-1)), (Some(7), Some(i64::MAX))].repeat(64)),
        Relation::new(
            &[
                (Some(i64::MAX), Some(-1)),
                (Some(9_007_199_254_740_993), Some(i64::MAX)),
            ]
            .repeat(64),
        ),
    ];
    let expected = existence_reference(&values, JoinKind::LeftSemi);
    assert_eq!(expected.len(), 64);
    let prepared = prepare_existence(&values, JoinKind::LeftSemi, true);
    assert_eq!(complete(&prepared, &expected, 7, false).executions, 1);
    let memory = prepared.session.memory().clone();
    drop(prepared);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn cyclic_candidate_admits_nonnullable_keys_and_preserves_other_left_payload_types() {
    let integers = (0..128).collect::<Vec<i64>>();
    let strings = (0..128)
        .map(|row| {
            if row % 5 == 0 {
                None
            } else {
                Some("native payload")
            }
        })
        .collect::<Vec<_>>();
    let bools = (0..128)
        .map(|row| {
            if row % 7 == 0 {
                None
            } else {
                Some(row % 2 == 0)
            }
        })
        .collect::<Vec<_>>();
    let prepared = prepare_relational_with_schema(policy(), |schemas| {
        for name in ["r", "s", "t"] {
            schemas.register_memory_source(uri(name), |session| {
                ResidentMemorySource::from_columns(
                    session,
                    &[
                        MemoryColumn {
                            name: "x",
                            values: MemoryColumnValues::Int64NonNullable(&integers),
                        },
                        MemoryColumn {
                            name: "y",
                            values: MemoryColumnValues::Int64NonNullable(&integers),
                        },
                        MemoryColumn {
                            name: "id",
                            values: MemoryColumnValues::Int64NonNullable(&integers),
                        },
                        MemoryColumn {
                            name: "text",
                            values: MemoryColumnValues::Utf8(&strings),
                        },
                        MemoryColumn {
                            name: "flag",
                            values: MemoryColumnValues::Bool(&bools),
                        },
                    ],
                    MemorySourceBounds::default(),
                )
            })?;
        }
        let VortexRelationalPlan::Join(mut plan) = existence_plan(JoinKind::LeftSemi, false) else {
            unreachable!()
        };
        plan.columns.extend([
            column(Side::Left, "text", "text"),
            column(Side::Left, "flag", "flag"),
        ]);
        Ok(VortexRelationalPlan::Join(plan))
    })
    .unwrap();
    let memory = prepared.session.memory().clone();
    let baseline = memory.snapshot().reserved_bytes;
    observation::reset();
    let result = prepared
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(observation::snapshot().executions, 1);
    let expected = (0..128).map(|row| json!({"outer_id":integers[row],"b":integers[row],"a":integers[row],"text":strings[row],"flag":bools[row]})).collect::<Vec<_>>();
    assert_eq!(json_rows(&result), expected);
    drop(result);
    assert_eq!(memory.snapshot().reserved_bytes, baseline);
    drop(prepared);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn cyclic_candidate_native_payload_outlives_preparation_and_final_owner_drains() {
    let values = [grid(2051, 64), grid(128, 64), grid(128, 64)];
    let prepared = prepare_existence(&values, JoinKind::LeftSemi, false);
    let memory = prepared.session.memory().clone();
    observation::reset();
    let completed = prepared
        .execute_native(&CancellationToken::default())
        .unwrap();
    assert_eq!(observation::snapshot().executions, 1);
    assert_eq!(completed.result.row_count(), 2051);
    let mut context = completed.result.create_execution_ctx();
    let retained = completed.result.arrays().to_vec();
    drop((completed, prepared));
    assert!(memory.snapshot().reserved_bytes > 0);
    let mut start = 0;
    for batch in &retained {
        for (name, input) in [
            ("outer_id", &values[0].id),
            ("b", &values[0].id),
            ("a", &values[0].id),
        ] {
            let column = crate::local_primitives::logical_field_from_native_array(batch, name)
                .unwrap()
                .execute::<PrimitiveArray>(&mut context)
                .unwrap();
            for (offset, value) in column.as_slice::<i64>().iter().enumerate() {
                let ordinal = input[start + offset];
                let expected = match name {
                    "b" => ordinal / 64,
                    "a" => ordinal % 64,
                    _ => ordinal,
                };
                assert_eq!(*value, expected);
            }
        }
        start += batch.len();
    }
    assert_eq!(start, 2051);
    drop((retained, context));
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn cyclic_candidate_keeps_noninteger_keys_explicit_conditions_and_spill_ordinary() {
    let floats = (0..128).map(|row| Some(f64::from(row))).collect::<Vec<_>>();
    let ids = (0..128).collect::<Vec<i64>>();
    let prepared = prepare_relational_with_schema(policy(), |schemas| {
        for name in ["r", "s", "t"] {
            schemas.register_memory_source(uri(name), |session| {
                ResidentMemorySource::from_columns(
                    session,
                    &[
                        MemoryColumn {
                            name: "x",
                            values: MemoryColumnValues::Float64(&floats),
                        },
                        MemoryColumn {
                            name: "y",
                            values: MemoryColumnValues::Float64(&floats),
                        },
                        MemoryColumn {
                            name: "id",
                            values: MemoryColumnValues::Int64NonNullable(&ids),
                        },
                    ],
                    MemorySourceBounds::default(),
                )
            })?;
        }
        let VortexRelationalPlan::Join(mut plan) = existence_plan(JoinKind::LeftSemi, false) else {
            unreachable!()
        };
        plan.columns.truncate(1);
        Ok(VortexRelationalPlan::Join(plan))
    })
    .unwrap();
    let memory = prepared.session.memory().clone();
    observation::reset();
    let result = prepared
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(observation::snapshot().executions, 0);
    assert_eq!(
        json_rows(&result),
        ids.iter()
            .map(|id| json!({"outer_id":id}))
            .collect::<Vec<_>>()
    );
    drop((result, prepared));
    assert_eq!(memory.snapshot().reserved_bytes, 0);

    let values = [grid(128, 16), grid(128, 16), grid(128, 16)];
    let expected = existence_reference(&values, JoinKind::LeftSemi);
    for inner_condition in [false, true] {
        let VortexRelationalPlan::Join(mut plan) = existence_plan(JoinKind::LeftSemi, false) else {
            unreachable!()
        };
        let condition = Some(shardloom_core::Expression::literal(
            shardloom_core::ExprId::new("true_condition").unwrap(),
            shardloom_core::ScalarValue::Boolean(true),
        ));
        if inner_condition {
            let VortexRelationalPlan::Join(inner) = &mut plan.right else {
                unreachable!()
            };
            inner.condition = condition;
        } else {
            plan.condition = condition;
        }
        let prepared = prepare(&values, VortexRelationalPlan::Join(plan));
        assert_eq!(complete(&prepared, &expected, 2048, false).executions, 0);
    }

    let directory = std::env::temp_dir().join(format!(
        "shardloom-cyclic-spill-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    fs::create_dir(&directory).unwrap();
    let cleanup = Fixture(directory.clone());
    let prepared = prepare_existence(&values, JoinKind::LeftSemi, false)
        .with_spill(
            crate::relational_query::VortexRelationalSpillPolicy::new(
                directory.clone(),
                128 << 20,
                1 << 20,
            )
            .unwrap(),
        )
        .unwrap();
    assert_eq!(complete(&prepared, &expected, 2048, false).executions, 0);
    assert_eq!(fs::read_dir(directory).unwrap().count(), 0);
    let memory = prepared.session.memory().clone();
    drop((prepared, cleanup));
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}
