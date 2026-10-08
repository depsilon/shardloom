//! Complete streaming aggregation owns retained state without pinning producers.

use super::*;
use crate::{
    query_primitive::VortexSimpleAggregateMeasure,
    relational_query::{
        VortexRelationalAggregate, VortexRelationalLimit, VortexRelationalSpillPolicy,
    },
};
use serde_json::json;
use std::cell::Cell;

#[path = "local_primitive_relational_batch_aggregate_pressure_tests.rs"]
mod pressure_tests;

#[path = "local_primitive_relational_batch_aggregate_fault_tests.rs"]
mod fault_tests;

fn measure(function: &str, column: Option<&str>, alias: &str) -> VortexSimpleAggregateMeasure {
    VortexSimpleAggregateMeasure::new(
        function,
        column.map(|column| ColumnRef::new(column).unwrap()),
        alias.into(),
    )
}

fn aggregate(
    input: VortexRelationalPlan,
    groups: &[&str],
    measures: Vec<VortexSimpleAggregateMeasure>,
) -> VortexRelationalPlan {
    VortexRelationalPlan::Aggregate(Box::new(VortexRelationalAggregate {
        input,
        group_by: groups
            .iter()
            .map(|column| ColumnRef::new(*column).unwrap())
            .collect(),
        measures,
    }))
}

fn workspace() -> Fixture {
    Fixture::new(keyed(&[], &[]), 1)
}

fn prepared(
    plan: &VortexRelationalPlan,
    fixture: &Fixture,
    spill: bool,
    bytes: u64,
) -> PreparedVortexRelational {
    let prepared = prepare(plan, bytes).unwrap();
    if spill {
        let path = fixture.0.join("runs");
        fs::create_dir_all(&path).unwrap();
        prepared
            .with_spill(VortexRelationalSpillPolicy::new(path, 512 << 20, 1 << 20).unwrap())
            .unwrap()
    } else {
        prepared
    }
}

#[test]
fn ordered_aggregate_streaming_releases_input_and_retained_results_keep_credits() {
    for spill in [false, true] {
        let fixture = workspace();
        let plan = aggregate(
            scan(),
            &["s"],
            vec![
                measure("count", None, "rows"),
                measure("count_distinct", Some("n"), "distinct"),
                measure("sum", Some("n"), "sum"),
                measure("avg", Some("n"), "avg"),
                measure("min", Some("n"), "min"),
                measure("max", Some("n"), "max"),
            ],
        );
        let prepared = prepared(&plan, &fixture, spill, 8 << 20);
        let ended = Cell::new(false);
        let mut calls = 0;
        let mut prior: Option<Weak<MemoryLease>> = None;
        let mut input = |session: &ResidentVortexSession| {
            assert!(
                prior
                    .as_ref()
                    .is_none_or(|witness| witness.strong_count() == 0)
            );
            let batch = match calls {
                0 | 2 => source(session, &[], &[])?,
                1 => source(
                    session,
                    &[Some(-2), Some(5), None],
                    &[Some("B"), Some("A"), Some("B")],
                )?,
                3 => source(
                    session,
                    &[Some(9), Some(5), Some(4), None],
                    &[None, Some("A"), Some("B"), None],
                )?,
                4 => {
                    ended.set(true);
                    return Ok(None);
                }
                _ => unreachable!(),
            };
            calls += 1;
            prior = Some(batch.batch_release_witness()?);
            Ok(Some(batch))
        };
        let mut actual = Vec::new();
        let mut retained = Vec::new();
        let execution = prepared
            .with_batch_input(&mut input)
            .unwrap()
            .for_each_batch(&CancellationToken::default(), |array, context| {
                assert!(ended.get(), "aggregate output preceded complete input");
                actual.extend(values(&array, context)?);
                retained.push(array);
                Ok(())
            })
            .unwrap();
        assert_eq!(
            actual,
            vec![
                json!({"s":"B","rows":3,"distinct":2,"sum":2.0,"avg":1.0,"min":-2,"max":4}),
                json!({"s":"A","rows":2,"distinct":1,"sum":10.0,"avg":5.0,"min":5,"max":5}),
                json!({"s":null,"rows":2,"distinct":1,"sum":9.0,"avg":9.0,"min":9,"max":9})
            ]
        );
        let input_report = execution.input.as_ref().unwrap();
        assert!(input_report.end_of_input_observed && input_report.output_ownership_detached);
        assert_eq!(input_report.payload_batches, 4);
        assert_eq!(input_report.rows, 7);
        assert_eq!(execution.ordered_aggregate_stages, u64::from(spill));
        let memory = prepared.session.memory().clone();
        drop(execution);
        drop(prepared);
        assert!(memory.snapshot().reserved_bytes > 0);
        let slice = retained[0].slice(0..1).unwrap();
        let clone = slice.clone();
        drop((retained, slice));
        assert!(memory.snapshot().reserved_bytes > 0);
        drop(clone);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
        if spill {
            assert_eq!(fs::read_dir(fixture.0.join("runs")).unwrap().count(), 0);
        }
    }
}

#[test]
fn ordered_aggregate_streaming_late_failures_and_zero_limit_never_complete() {
    for spill in [false, true] {
        for limit in [false, true] {
            for failure in ["producer", "schema", "cancel"] {
                let fixture = workspace();
                let plan = aggregate(
                    scan(),
                    &["s"],
                    vec![measure("count_distinct", Some("n"), "distinct")],
                );
                let plan = if limit {
                    VortexRelationalPlan::Limit(Box::new(VortexRelationalLimit {
                        input: plan,
                        offset: 0,
                        count: 0,
                    }))
                } else {
                    plan
                };
                let prepared = prepared(&plan, &fixture, spill, 8 << 20);
                let baseline = prepared.snapshot().memory.reserved_bytes;
                let token = CancellationToken::default();
                let mut calls = 0;
                let mut input = |session: &ResidentVortexSession| {
                    calls += 1;
                    if calls == 1 {
                        return Ok(Some(source(session, &[Some(1)], &[Some("first")])?));
                    }
                    match failure {
                        "producer" => Err(batch_input::failed("aggregate producer sentinel")),
                        "schema" => Ok(Some(ResidentMemorySource::from_batch_columns(
                            session,
                            &[MemoryColumn {
                                name: "different",
                                values: MemoryColumnValues::Int64(&[Some(2)]),
                            }],
                        )?)),
                        "cancel" => {
                            token.cancel();
                            Ok(None)
                        }
                        _ => unreachable!(),
                    }
                };
                let result = prepared
                    .with_batch_input(&mut input)
                    .unwrap()
                    .for_each_batch(&token, |_, _| panic!("incomplete aggregate was delivered"));
                assert!(result.is_err(), "{spill} {limit} {failure}");
                assert_eq!(calls, 2);
                assert_eq!(prepared.snapshot().completed_executions, 0);
                assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
                if spill {
                    assert_eq!(fs::read_dir(fixture.0.join("runs")).unwrap().count(), 0);
                }
            }
        }
    }
}

#[test]
fn ordered_aggregate_streaming_nested_composition_consumes_source_once() {
    for spill in [false, true] {
        let fixture = workspace();
        let first = aggregate(scan(), &["s"], vec![measure("count", None, "rows")]);
        let plan = aggregate(
            first,
            &[],
            vec![
                measure("sum", Some("rows"), "total"),
                measure("count_distinct", Some("rows"), "distinct"),
            ],
        );
        let prepared = prepared(&plan, &fixture, spill, 8 << 20);
        let mut calls = 0;
        let mut input = |session: &ResidentVortexSession| {
            calls += 1;
            match calls {
                1 => Ok(Some(source(
                    session,
                    &[Some(1), Some(2), Some(3)],
                    &[Some("B"), Some("A"), Some("B")],
                )?)),
                2 => Ok(Some(source(
                    session,
                    &[Some(4), Some(5)],
                    &[Some("B"), Some("A")],
                )?)),
                3 => Ok(None),
                _ => panic!("input was replayed"),
            }
        };
        let result = prepared
            .with_batch_input(&mut input)
            .unwrap()
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        assert_eq!(json_rows(&result), vec![json!({"total":5.0,"distinct":2})]);
        assert_eq!(calls, 3);
        assert_eq!(
            result.execution.ordered_aggregate_stages,
            if spill { 2 } else { 0 }
        );
        if spill {
            assert_eq!(result.execution.ordered_aggregate_input_rows, 7);
            assert_eq!(result.execution.ordered_aggregate_distinct_rows, 2);
        }
    }
}
