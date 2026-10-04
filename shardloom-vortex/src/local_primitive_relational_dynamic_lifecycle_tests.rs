use super::*;
use crate::relational_query::{VortexRelationalSubquery, VortexRelationalSubqueryKind};
use std::cell::Cell;

fn exists(input: VortexRelationalPlan, relation: VortexRelationalPlan) -> VortexRelationalPlan {
    VortexRelationalPlan::CorrelatedSubquery(Box::new(VortexRelationalSubquery {
        input,
        relation,
        kind: VortexRelationalSubqueryKind::Exists,
        correlation: vec![],
        output_column: "present".into(),
        negated: false,
    }))
}

#[test]
fn native_dynamic_pivot_rejects_completed_state_in_a_parameterized_relation() {
    let fixture = fixture();
    let source = fixture.scan();
    let prepared = prepare_relational_with_dynamic_schema(
        &[uri(&fixture)],
        policy(),
        65_536,
        move |schemas| {
            let (result, _) = schemas.resolve_output(&pivot(source.clone()))?;
            Ok(exists(source.clone(), result))
        },
    )
    .unwrap();
    let baseline = prepared.snapshot().memory.reserved_bytes;
    let error = prepared.execute_owned().err().unwrap();
    assert!(
        error
            .to_string()
            .contains("cannot be reused across correlated parameters"),
        "{error}"
    );
    assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
    assert_eq!(prepared.snapshot().completed_executions, 0);
}

#[test]
fn native_dynamic_pivot_rejects_unconsumed_misplaced_and_repeated_declarations() {
    let fixture = fixture();
    for (mode, reason) in [
        (0, "unconsumed native relation"),
        (1, "require a correlated subquery relation"),
        (2, "consumed twice"),
    ] {
        let source = fixture.scan();
        let prepared = prepare_relational_with_dynamic_schema(
            &[uri(&fixture)],
            policy(),
            65_536,
            move |schemas| {
                let inner = pivot(source.clone());
                let relation = schemas.defer_subquery(65_536, move |schemas| {
                    schemas.resolve_output(&inner).map(|(plan, _)| plan)
                })?;
                Ok(match mode {
                    0 => source.clone(),
                    1 => relation,
                    _ => VortexRelationalPlan::Set(Box::new(VortexRelationalSet {
                        left: exists(source.clone(), relation.clone()),
                        right: exists(source.clone(), relation),
                        kind: SetKind::UnionAll,
                    })),
                })
            },
        )
        .unwrap();
        let baseline = prepared.snapshot().memory.reserved_bytes;
        let error = prepared.execute_owned().err().unwrap();
        assert!(error.to_string().contains(reason), "mode {mode}: {error}");
        assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
        assert_eq!(prepared.snapshot().completed_executions, 0);
    }
}

#[test]
fn native_dynamic_pivot_rejects_declarations_from_a_previous_execution() {
    let fixture = fixture();
    let source = fixture.scan();
    let previous = RefCell::new(None);
    let prepared = prepare_relational_with_dynamic_schema(
        &[uri(&fixture)],
        policy(),
        65_536,
        move |schemas| {
            let saved = previous.borrow_mut().take();
            let relation = if let Some(saved) = saved {
                saved
            } else {
                let inner = pivot(source.clone());
                let declaration = schemas.defer_subquery(65_536, move |schemas| {
                    schemas.resolve_output(&inner).map(|(plan, _)| plan)
                })?;
                *previous.borrow_mut() = Some(declaration.clone());
                declaration
            };
            Ok(exists(source.clone(), relation))
        },
    )
    .unwrap();
    let baseline = prepared.snapshot().memory.reserved_bytes;
    drop(prepared.execute_owned().unwrap());
    assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
    let error = prepared.execute_owned().err().unwrap();
    assert!(error.to_string().contains("different execution"), "{error}");
    assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
    assert_eq!(prepared.snapshot().completed_executions, 1);
}

#[test]
fn native_dynamic_pivot_repeated_calls_do_not_retain_a_previous_empty_or_nonempty_schema() {
    let fixture = fixture();
    let source = fixture.scan();
    let calls = Cell::new(0);
    let observed = Rc::new(RefCell::new(Vec::new()));
    let columns = observed.clone();
    let prepared = prepare_relational_with_dynamic_schema(
        &[uri(&fixture)],
        policy(),
        65_536,
        move |schemas| {
            let count = if calls.get() % 2 == 0 { 0 } else { 4 };
            calls.set(calls.get() + 1);
            let input = VortexRelationalPlan::Limit(Box::new(VortexRelationalLimit {
                input: source.clone(),
                offset: 0,
                count,
            }));
            let (result, fields) = schemas.resolve_output(&pivot(input))?;
            columns.borrow_mut().push(fields.clone());
            Ok(project(result, &fields))
        },
    )
    .unwrap();
    let baseline = prepared.snapshot().memory.reserved_bytes;
    for call in 0..3 {
        let result = prepared
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        assert_eq!(
            json_rows(&result),
            if call == 1 { expected() } else { vec![] }
        );
        drop(result);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
    }
    assert_eq!(
        *observed.borrow(),
        vec![
            vec!["entity"],
            vec!["entity", "pivot_a", "pivot_b"],
            vec!["entity"],
        ]
    );
    assert_eq!(prepared.snapshot().prepared_source_opens, 1);
}

#[test]
fn native_dynamic_pivot_consumer_failure_cancellation_and_source_changes_release_state() {
    let fixture = fixture();
    let prepared = prepare(&fixture);
    let baseline = prepared.snapshot().memory.reserved_bytes;
    let cancelled = CancellationToken::default();
    cancelled.cancel();
    let target = fixture.0.join("cancelled.vortex");
    assert!(
        prepared
            .write_controlled(
                &target,
                crate::local_primitives::VortexLocalPrimitiveRowExportFormat::Vortex,
                false,
                &cancelled
            )
            .is_err()
    );
    assert!(!target.exists());
    let mut delivered = 0;
    assert!(
        prepared
            .for_each_batch(&CancellationToken::default(), |_, _| {
                delivered += 1;
                Err(failed("deliberate consumer failure"))
            })
            .is_err()
    );
    assert_eq!(delivered, 1);
    assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
    let cancelled = CancellationToken::default();
    assert!(
        prepared
            .for_each_batch(&cancelled, |_, _| {
                cancelled.cancel();
                Ok(())
            })
            .is_err()
    );
    assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
    assert!(
        prepared
            .for_each_batch(&CancellationToken::default(), |_, _| {
                fixture.replace();
                Ok(())
            })
            .is_err()
    );
    assert!(prepared.execute_owned().is_err());
    assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
    assert_eq!(prepared.snapshot().prepared_source_opens, 1);
    assert_eq!(prepared.snapshot().completed_executions, 0);
}

#[test]
fn native_dynamic_pivot_denies_sparse_state_pressure_before_publication() {
    let rows = 500;
    let fixture = Fixture::new(
        StructArray::new(
            FieldNames::from(["entity", "category", "amount"]),
            vec![
                PrimitiveArray::from_iter(0..rows as u64).into_array(),
                VarBinArray::from(vec!["a"; rows]).into_array(),
                PrimitiveArray::from_iter(vec![1_u64; rows]).into_array(),
            ],
            rows,
            Validity::NonNullable,
        )
        .into_array(),
        31,
    );
    let plan = pivot(fixture.scan());
    let mut narrow = policy();
    narrow.resource_envelope.memory_budget_bytes = 1 << 20;
    let prepared =
        prepare_relational_with_dynamic_schema(&[uri(&fixture)], narrow, 65_536, move |schemas| {
            schemas.resolve_output(&plan).map(|(plan, _)| plan)
        })
        .unwrap();
    let baseline = prepared.snapshot().memory.reserved_bytes;
    let path = fixture.0.join("pressure.vortex");
    let error = prepared
        .write(
            &path,
            crate::local_primitives::VortexLocalPrimitiveRowExportFormat::Vortex,
            false,
        )
        .err()
        .unwrap();
    assert!(error.to_string().contains("reservation"), "{error}");
    assert!(!path.exists());
    assert_eq!(fs::read_dir(&fixture.0).unwrap().count(), 1);
    assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
    assert_eq!(prepared.snapshot().completed_executions, 0);
}

#[test]
fn native_dynamic_pivot_shared_completion_drops_partial_state_when_discovery_is_cancelled() {
    let fixture = fixture();
    let prepared = prepare(&fixture);
    let VortexRelationalPlan::Unary(unary) = pivot(fixture.scan()) else {
        unreachable!()
    };
    let bound = BoundUnary::for_relation(
        &unary.request,
        prepared.sources[0].dtype(),
        prepared.session.memory(),
    )
    .unwrap();
    let baseline = prepared.snapshot().memory.reserved_bytes;
    let cancellation = CancellationToken::default();
    let mut consumed = false;
    let result =
        prepared
            .session
            .with_sources_execution(&prepared.sources, &cancellation, |context| {
                bound.complete_relation_pivot(context, |accept| {
                    let batch = StructArray::new(
                        FieldNames::from(["entity", "category", "amount"]),
                        vec![
                            PrimitiveArray::from_iter([1_u64]).into_array(),
                            VarBinArray::from(vec!["a"]).into_array(),
                            PrimitiveArray::from_iter([3_u64]).into_array(),
                        ],
                        1,
                        Validity::NonNullable,
                    )
                    .into_array();
                    accept(batch)?;
                    consumed = true;
                    cancellation.cancel();
                    Ok(())
                })
            });
    assert!(consumed);
    assert!(result.is_err());
    assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
    assert_eq!(prepared.snapshot().completed_executions, 0);
}

#[test]
fn native_dynamic_pivot_correlated_state_uses_each_parameter_once_and_releases_its_credits() {
    let fixture = fixture();
    let source = fixture.scan();
    let inner = pivot(VortexRelationalPlan::Join(Box::new(VortexRelationalJoin {
        left: source.clone(),
        right: VortexRelationalPlan::Outer,
        kind: JoinKind::Inner,
        keys: vec![VortexRelationalJoinKey {
            left: ColumnRef::new("entity").unwrap(),
            right: ColumnRef::new("entity").unwrap(),
        }],
        condition: None,
        columns: ["entity", "category", "amount"]
            .into_iter()
            .map(|name| VortexRelationalJoinColumn {
                side: Side::Left,
                column: ColumnRef::new(name).unwrap(),
                output_column: name.into(),
            })
            .collect(),
    })));
    let observed = Rc::new(RefCell::new(Vec::new()));
    let domains = observed.clone();
    let prepared = prepare_relational_with_dynamic_schema(
        &[uri(&fixture)],
        policy(),
        65_536,
        move |schemas| {
            let inner = inner.clone();
            let domains = domains.clone();
            let relation = schemas.defer_subquery(65_536, move |schemas| {
                let (result, columns) = schemas.resolve_output(&inner)?;
                domains.borrow_mut().push(columns);
                Ok(project(result, &["entity".into()]))
            })?;
            Ok(VortexRelationalPlan::CorrelatedSubquery(Box::new(
                VortexRelationalSubquery {
                    input: source.clone(),
                    relation,
                    kind: VortexRelationalSubqueryKind::In {
                        columns: vec![VortexRelationalJoinKey {
                            left: ColumnRef::new("entity").unwrap(),
                            right: ColumnRef::new("entity").unwrap(),
                        }],
                    },
                    correlation: vec![],
                    output_column: "present".into(),
                    negated: false,
                },
            )))
        },
    )
    .unwrap();
    let baseline = prepared.snapshot().memory.reserved_bytes;
    for call in 1..=2 {
        let result = prepared
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        assert_eq!(
            serde_json::json!(json_rows(&result)),
            serde_json::json!([
                {"entity":1,"category":"a","amount":3,"present":true},
                {"entity":1,"category":"b","amount":5,"present":true},
                {"entity":2,"category":"a","amount":7,"present":true},
                {"entity":2,"category":"a","amount":11,"present":true},
            ])
        );
        assert_eq!(result.execution.scan_rows_delivered, 20);
        assert_eq!(result.execution.runtime.completed_executions, call);
        assert!(
            result
                .execution
                .native_io_certificate
                .source_pushdown_report
                .proof_basis
                .contains("dynamic_schema_stages=4")
        );
        drop(result);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
    }
    assert_eq!(
        *observed.borrow(),
        [
            vec!["entity", "pivot_a", "pivot_b"],
            vec!["entity", "pivot_a", "pivot_b"],
            vec!["entity", "pivot_a"],
            vec!["entity", "pivot_a"],
        ]
        .into_iter()
        .cycle()
        .take(8)
        .collect::<Vec<_>>()
    );
    assert_eq!(prepared.snapshot().prepared_source_opens, 1);
}
