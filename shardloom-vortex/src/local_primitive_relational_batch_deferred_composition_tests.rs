//! Nested declarations and repeated input use keep the original replay owner.

use super::*;
use crate::relational_query::{VortexRelationalSet, VortexRelationalSetKind};

#[test]
fn streaming_dynamic_subqueries_compose_nested_declarations_repeated_sources_and_dynamic_outer() {
    for shared in [false, true] {
        for mode in ["nested", "set", "join", "dynamic-outer"] {
            let prepared = prepare_streams(if shared { 1 } else { 2 }, move |schemas| {
                let index = usize::from(!shared);
                let outer = if mode == "dynamic-outer" {
                    if shared {
                        schemas.declare_source_reuse(&DatasetUri::new(URIS[0])?)?;
                    }
                    schemas.resolve_output(&pivot(scan(0)))?.0
                } else {
                    scan(0)
                };
                let relation = schemas.defer_subquery_with_sources(
                    &[DatasetUri::new(URIS[index])?],
                    65_536,
                    move |schemas| {
                        if mode == "nested" {
                            let relation = schemas.defer_subquery_with_sources(
                                &[DatasetUri::new(URIS[index])?],
                                65_536,
                                move |schemas| schemas.resolve_output(&inner(index)).map(|r| r.0),
                            )?;
                            let mut nested = query(relation, Kind::Exists);
                            nested.input = VortexRelationalPlan::Outer;
                            return Ok(VortexRelationalPlan::CorrelatedSubquery(Box::new(nested)));
                        }
                        let relation = match mode {
                            "set" => VortexRelationalPlan::Set(Box::new(VortexRelationalSet {
                                left: inner(index),
                                right: inner(index),
                                kind: VortexRelationalSetKind::UnionAll,
                            })),
                            "join" => VortexRelationalPlan::Join(Box::new(VortexRelationalJoin {
                                left: inner(index),
                                right: inner(index),
                                kind: JoinKind::Inner,
                                keys: vec![key()],
                                condition: None,
                                columns: vec![VortexRelationalJoinColumn {
                                    side: Side::Left,
                                    column: ColumnRef::new("entity")?,
                                    output_column: "entity".into(),
                                }],
                            })),
                            _ => inner(index),
                        };
                        schemas.resolve_output(&relation).map(|r| r.0)
                    },
                )?;
                let mut query = query(relation, Kind::Exists);
                query.input = outer;
                Ok(VortexRelationalPlan::CorrelatedSubquery(Box::new(query)))
            });
            let baseline = prepared.snapshot().memory.reserved_bytes;
            for execution in 1..=2 {
                let (result, calls) = collect(&prepared);
                assert_eq!(
                    json_rows(&result),
                    if mode == "dynamic-outer" {
                        vec![
                            json!({"entity":1,"pivot_a":3.0,"pivot_b":5.0,"answer":true}),
                            json!({"entity":2,"pivot_a":18.0,"pivot_b":null,"answer":true}),
                        ]
                    } else {
                        expected(true)
                    },
                    "{shared}: {mode}"
                );
                assert_eq!(calls, [4, if shared { 0 } else { 4 }]);
                let input = result.execution.input.as_ref().unwrap();
                assert_eq!(input.replay_sources, 1);
                assert!(input.end_of_input_observed);
                assert_eq!(result.execution.runtime.completed_executions, execution);
                drop(result);
                assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
            }
        }
    }
}

#[test]
fn streaming_dynamic_subqueries_preserve_mixed_source_indices_and_equivalent_file_identity() {
    let fixture = fixture();
    let file = uri(&fixture);
    let file_alias = DatasetUri::new(format!("file://{}", fixture.path().display())).unwrap();
    let memory = DatasetUri::new(URIS[1]).unwrap();
    let sources = [
        DatasetUri::new(URIS[0]).unwrap(),
        memory.clone(),
        file.clone(),
    ];
    let prepared = prepare_relational_with_dynamic_inputs(
        &sources,
        policy(),
        65_536,
        |schemas| {
            schemas.register_batch_source(sources[0].clone(), |session| {
                source(session, &[], &[], &[])
            })?;
            schemas.register_memory_source(sources[1].clone(), |session| {
                ResidentMemorySource::from_columns(
                    session,
                    &[
                        MemoryColumn {
                            name: "entity",
                            values: MemoryColumnValues::Int64(&[Some(9)]),
                        },
                        MemoryColumn {
                            name: "category",
                            values: MemoryColumnValues::Utf8(&[Some("c")]),
                        },
                        MemoryColumn {
                            name: "amount",
                            values: MemoryColumnValues::Int64(&[Some(42)]),
                        },
                    ],
                    crate::resident_memory_source::MemorySourceBounds::default(),
                )
            })?;
            Ok(())
        },
        move |schemas| {
            let calls = AtomicUsize::new(0);
            let file = file.clone();
            let memory = memory.clone();
            let relation = schemas.defer_subquery_with_sources(
                &[file_alias.clone(), file.clone(), memory.clone()],
                65_536,
                move |schemas| {
                    let source_uri = if calls.fetch_add(1, Ordering::Relaxed).is_multiple_of(2) {
                        file.clone()
                    } else {
                        memory.clone()
                    };
                    let input = VortexRelationalPlan::Scan(VortexRelationalScan {
                        source_uri,
                        projection: shardloom_plan::ProjectionRequest::All,
                        predicate: None,
                    });
                    schemas.resolve_output(&pivot(input)).map(|(plan, _)| plan)
                },
            )?;
            Ok(VortexRelationalPlan::CorrelatedSubquery(Box::new(query(
                relation,
                Kind::Exists,
            ))))
        },
    )
    .unwrap();
    let baseline = prepared.snapshot().memory.reserved_bytes;
    for execution in 1..=2 {
        let (result, calls) = collect(&prepared);
        assert_eq!(json_rows(&result), expected(true));
        assert_eq!(calls, [4, 0]);
        assert_eq!(result.execution.dynamic_schema_stages, 4);
        assert_eq!(result.execution.input.as_ref().unwrap().sources, 1);
        assert_eq!(result.execution.input.as_ref().unwrap().replay_sources, 0);
        assert_eq!(result.execution.runtime.completed_executions, execution);
        drop(result);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
    }
}
