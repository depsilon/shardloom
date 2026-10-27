//! Selector observation and ownership checks use complete native execution.

use super::*;
use shardloom_core::ShardLoomError;

fn full_frame() -> Frame {
    Frame {
        unit: Unit::Rows,
        start: Bound::UnboundedPreceding,
        end: Bound::UnboundedFollowing,
        exclusion: Exclusion::NoOthers,
    }
}

#[test]
fn ordered_selector_replaced_payloads_remain_unobserved_across_input_batches() {
    for invalid in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        for function in ["first", "last", "arg_min", "arg_max"] {
            let fixture = Fixture::new(
                StructArray::new(
                    FieldNames::from(["id", "window_key", "value", "key"]),
                    vec![
                        PrimitiveArray::from_iter([0u64, 1]).into_array(),
                        PrimitiveArray::from_iter([0u64, 1]).into_array(),
                        PrimitiveArray::from_iter([invalid, 7.0]).into_array(),
                        PrimitiveArray::from_iter(if function == "arg_max" {
                            [0i32, 1]
                        } else {
                            [1i32, 0]
                        })
                        .into_array(),
                    ],
                    2,
                    Validity::NonNullable,
                )
                .into_array(),
                1,
            );
            let mut selected = measure(function, Some("value"), "selected");
            if function.starts_with("arg_") {
                selected.secondary_column = Some(ColumnRef::new("key").unwrap());
            } else if function == "first" {
                selected.order_by = order("key", false);
            }
            check(
                &fixture,
                &aggregate(fixture.scan(), &[], vec![selected.clone()]),
                &[json!({"selected":7.0})],
            );
            check(
                &fixture,
                &window_plan(&fixture, &full_frame(), vec![selected]),
                &[
                    json!({"id":0,"selected":7.0}),
                    json!({"id":1,"selected":7.0}),
                ],
            );
        }
    }
}

#[test]
fn ordered_selector_null_pairs_skip_comparison_values_but_selected_payloads_stay_strict() {
    let fixture = Fixture::new(
        StructArray::new(
            FieldNames::from(["id", "window_key", "value", "key", "local_order"]),
            vec![
                PrimitiveArray::from_iter([0u64, 1, 2]).into_array(),
                PrimitiveArray::from_iter([0u64, 1, 2]).into_array(),
                PrimitiveArray::from_option_iter([None, Some(7i32), None]).into_array(),
                PrimitiveArray::from_iter([f64::NAN, 2.0, f64::INFINITY]).into_array(),
                PrimitiveArray::from_iter([f64::NEG_INFINITY, 1.0, f64::NAN]).into_array(),
            ],
            3,
            Validity::NonNullable,
        )
        .into_array(),
        1,
    );
    let measures = ["arg_min", "arg_max"]
        .map(|function| {
            measure(function, Some("value"), function)
                .with_secondary_column(ColumnRef::new("key").unwrap())
                .with_order_by(order("local_order", false))
        })
        .to_vec();
    check(
        &fixture,
        &aggregate(fixture.scan(), &[], measures.clone()),
        &[json!({"arg_min":7,"arg_max":7})],
    );
    check(
        &fixture,
        &window_plan(&fixture, &full_frame(), measures),
        &(0..3)
            .map(|id| json!({"id":id,"arg_min":7,"arg_max":7}))
            .collect::<Vec<_>>(),
    );
    for column in ["key", "local_order"] {
        let selected = measure("first", Some(column), "observed");
        let plans = [
            aggregate(fixture.scan(), &[], vec![selected.clone()]),
            window_plan(&fixture, &full_frame(), vec![selected]),
        ];
        for plan in &plans {
            let prepared = prepare_relational(plan, policy()).unwrap();
            let baseline = prepared.snapshot().memory.reserved_bytes;
            let error = prepared
                .collect_jsonl(&CancellationToken::default())
                .err()
                .unwrap();
            assert!(error.to_string().contains("finite"), "{error}");
            assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
        }
    }
}

#[test]
fn ordered_selector_shared_spill_transport_preserves_count_and_null_pair_domains() {
    let fixture = Fixture::new(
        StructArray::new(
            FieldNames::from(["id", "window_key", "group", "value", "key"]),
            vec![
                PrimitiveArray::from_iter(0u64..5).into_array(),
                PrimitiveArray::from_iter(0u64..5).into_array(),
                PrimitiveArray::from_iter([0u64; 5]).into_array(),
                PrimitiveArray::from_option_iter([
                    Some(f64::INFINITY),
                    None,
                    Some(1.0),
                    Some(2.0),
                    Some(3.0),
                ])
                .into_array(),
                PrimitiveArray::from_option_iter([
                    None,
                    Some(f64::NEG_INFINITY),
                    Some(2.0),
                    Some(4.0),
                    Some(6.0),
                ])
                .into_array(),
            ],
            5,
            Validity::NonNullable,
        )
        .into_array(),
        1,
    );
    let mut measures = ["covar_pop", "covar_samp", "corr"]
        .map(|function| {
            measure(function, Some("value"), function)
                .with_secondary_column(ColumnRef::new("key").unwrap())
        })
        .to_vec();
    measures.push(measure("count", Some("value"), "values"));
    measures.push(measure("count", Some("key"), "keys"));
    check(
        &fixture,
        &aggregate(fixture.scan(), &[], measures.clone()),
        &[json!({"covar_pop":4.0/3.0,"covar_samp":2.0,"corr":1.0,"values":4,"keys":4})],
    );
    check(
        &fixture,
        &aggregate(fixture.scan(), &["group"], measures.clone()),
        &[json!({"group":0,"covar_pop":4.0/3.0,"covar_samp":2.0,"corr":1.0,"values":4,"keys":4})],
    );
    check(
        &fixture,
        &window_plan(&fixture, &full_frame(), measures),
        &(0..5)
            .map(|id| json!({"id":id,"covar_pop":4.0/3.0,"covar_samp":2.0,"corr":1.0,"values":4,"keys":4}))
            .collect::<Vec<_>>(),
    );
}

#[test]
fn ordered_selector_window_pressure_cancel_output_failure_and_actual_spill_release_owners() {
    let fixture = Fixture::new(
        StructArray::new(
            FieldNames::from(["id", "window_key", "value", "key"]),
            vec![
                PrimitiveArray::from_iter(0u64..8192).into_array(),
                PrimitiveArray::from_iter(0u64..8192).into_array(),
                PrimitiveArray::from_iter((0u64..8192).map(|row| (1u64 << 60) + row)).into_array(),
                PrimitiveArray::from_iter((0u64..8192).map(|row| row % 17)).into_array(),
            ],
            8192,
            Validity::NonNullable,
        )
        .into_array(),
        257,
    );
    let frame = Frame {
        start: Bound::Preceding(Offset::Number(ScalarValue::UInt64(31))),
        end: Bound::CurrentRow,
        ..full_frame()
    };
    let measures = selectors("value", "key")
        .into_iter()
        .map(|measure| measure.with_order_by(order("value", true)))
        .collect();
    let plan = window_plan(&fixture, &frame, measures);
    let workspace = fixture.0.join("selector-recovery");
    fs::create_dir(&workspace).unwrap();
    for spill in [false, true] {
        let prepared = prepare_relational(&plan, policy()).unwrap();
        let prepared = if spill {
            prepared
                .with_spill(
                    VortexRelationalSpillPolicy::new(&workspace, 64 << 20, 1 << 20).unwrap(),
                )
                .unwrap()
        } else {
            prepared
        };
        let baseline = prepared.snapshot().memory.reserved_bytes;
        let pressure = prepared
            .session
            .memory()
            .reserve(prepared.snapshot().memory.limit_bytes - baseline - 1024)
            .unwrap();
        assert!(
            prepared
                .collect_jsonl(&CancellationToken::default())
                .is_err()
        );
        drop(pressure);
        for cancel in [false, true] {
            let token = CancellationToken::default();
            let mut delivered = 0;
            let result = prepared.for_each_batch(&token, |_, _| {
                delivered += 1;
                if cancel {
                    token.cancel();
                    Ok(())
                } else {
                    Err(ShardLoomError::InvalidOperation(
                        "selector output failure".into(),
                    ))
                }
            });
            assert!(result.is_err());
            assert_eq!(delivered, 1);
            assert_eq!(prepared.snapshot().completed_executions, 0);
            assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
            assert_eq!(fs::read_dir(&workspace).unwrap().count(), 0);
        }
        let result = prepared
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        let expected = (0u64..8192)
            .map(|row| {
                let start = row.saturating_sub(31);
                let minimum = (start..=row).map(|value| value % 17).min().unwrap();
                let maximum = (start..=row).map(|value| value % 17).max().unwrap();
                let minimum_row = (start..=row)
                    .rev()
                    .find(|value| value % 17 == minimum)
                    .unwrap();
                let maximum_row = (start..=row)
                    .rev()
                    .find(|value| value % 17 == maximum)
                    .unwrap();
                json!({"id":row,"first":(1u64<<60)+row,"last":(1u64<<60)+start,
                "arg_min":(1u64<<60)+minimum_row,"arg_max":(1u64<<60)+maximum_row})
            })
            .collect::<Vec<_>>();
        assert_eq!(json_rows(&result), expected);
        if spill {
            let evidence = result.execution.spill.as_ref().unwrap();
            assert!(evidence.runs_written > 0);
            assert!(evidence.owned_cleanup_completed);
        }
        drop(result);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
        assert_eq!(fs::read_dir(&workspace).unwrap().count(), 0);
    }
}

#[test]
fn ordered_selector_parent_validity_does_not_prepare_million_hidden_list_children() {
    use crate::local_primitives::{native_relational_aggregate, native_relational_window};
    use vortex::array::{
        VortexSessionExecute as _,
        arrays::{ConstantArray, ListViewArray},
    };
    let values = ListViewArray::try_new(
        ConstantArray::new("unobserved", 1_000_000).into_array(),
        PrimitiveArray::from_iter([0u64, 0]).into_array(),
        PrimitiveArray::from_iter([1_000_000u64, 0]).into_array(),
        Validity::from_iter([false, true]),
        &mut vortex::array::array_session().create_execution_ctx(),
    )
    .unwrap()
    .into_array();
    let input = StructArray::new(
        FieldNames::from(["id", "window_key", "value", "key"]),
        vec![
            PrimitiveArray::from_iter([0u64, 1]).into_array(),
            PrimitiveArray::from_iter([0u64, 1]).into_array(),
            values,
            PrimitiveArray::from_iter([0u64, 1]).into_array(),
        ],
        2,
        Validity::NonNullable,
    )
    .into_array();
    // Bind only an empty schema. The native kernels receive the original
    // encoded array directly, before a writer can mask hidden list children.
    let fixture = Fixture::new(input.slice(0..0).unwrap(), 1);
    let measures = selectors("value", "key");
    for plan in [
        aggregate(fixture.scan(), &[], measures.clone()),
        window_plan(&fixture, &full_frame(), measures),
    ] {
        let prepared = prepare_relational(&plan, policy()).unwrap();
        let PreparedRoot::Bound(root) = &prepared.root else {
            panic!("static plan")
        };
        let session = ResidentVortexSession::new(1 << 20, 1).unwrap();
        let memory = session.memory().clone();
        session
            .with_native_execution_context(&CancellationToken::default(), |context| {
                let mut output = Vec::new();
                let mut consume = |array| {
                    output.push(array);
                    Ok(())
                };
                match &root.kind {
                    NodeKind::Aggregate { spec, .. } => {
                        let mut state =
                            native_relational_aggregate::Aggregate::new(spec, context.memory())?;
                        state.consume(&input.slice(0..1).unwrap(), context, 1)?;
                        state.consume(&input.slice(1..2).unwrap(), context, 1)?;
                        state.finish(context, 2, &mut consume)?;
                    }
                    NodeKind::Window { spec, .. } => {
                        let mut state =
                            native_relational_window::Window::new(spec, context.memory())?;
                        state.build(input.clone(), context)?;
                        state.finish(context, 2, &mut consume)?;
                    }
                    _ => panic!("selector owner"),
                }
                let mut execution = context.native_session().create_execution_ctx();
                for batch in &output {
                    for row in 0..batch.len() {
                        let first = crate::local_primitives::logical_field_from_native_array(
                            batch, "first",
                        )?;
                        assert!(first.execute_scalar(row, &mut execution).unwrap().is_null());
                        for name in ["last", "arg_min", "arg_max"] {
                            let value = crate::local_primitives::logical_field_from_native_array(
                                batch, name,
                            )?;
                            assert_eq!(
                                value
                                    .execute_scalar(row, &mut execution)
                                    .unwrap()
                                    .as_list()
                                    .elements()
                                    .unwrap(),
                                [] as [vortex::array::scalar::Scalar; 0]
                            );
                        }
                    }
                }
                assert!(memory.snapshot().reserved_bytes > 0);
                drop(output);
                Ok(())
            })
            .unwrap();
        drop(session);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}
