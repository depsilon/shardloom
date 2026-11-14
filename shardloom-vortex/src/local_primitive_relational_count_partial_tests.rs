//! Admission, complete producer consumption and public prepared-result ownership.

use super::*;
use crate::{
    relational_query::VortexRelationalSpillPolicy,
    resident_memory_source::{MemoryColumn, MemoryColumnValues},
};
use serde_json::json;

fn source(
    session: &ResidentVortexSession,
    rows: usize,
    nullable: bool,
) -> Result<ResidentMemorySource> {
    let groups: Vec<_> = (0..rows).map(|row| [3_i64, 1, -1][row % 3]).collect();
    let values: Vec<_> = (0..rows)
        .map(|row| i64::try_from(row % 17).unwrap())
        .collect();
    let optional: Vec<_> = values
        .iter()
        .enumerate()
        .map(|(row, &value)| (!row.is_multiple_of(7)).then_some(value))
        .collect();
    ResidentMemorySource::from_batch_columns(
        session,
        &[
            MemoryColumn {
                name: "g",
                values: MemoryColumnValues::Int64NonNullable(&groups),
            },
            MemoryColumn {
                name: "v",
                values: if nullable {
                    MemoryColumnValues::Int64(&optional)
                } else {
                    MemoryColumnValues::Int64NonNullable(&values)
                },
            },
        ],
    )
}

fn prepared(order: &str, nullable: bool, all: bool, mixed: bool) -> PreparedVortexRelational {
    let uri = DatasetUri::new("memory://count-partial").unwrap();
    let mut measures = vec![measure("count", (!all).then_some("v"), "n")];
    if mixed {
        measures.push(measure("sum", Some("v"), "total"));
    }
    let mut plan = aggregate(
        VortexRelationalPlan::Scan(VortexRelationalScan {
            source_uri: uri.clone(),
            projection: shardloom_plan::ProjectionRequest::All,
            predicate: None,
        }),
        &["g"],
        measures,
    );
    if order != "source" {
        plan = VortexRelationalPlan::Sort(Box::new(VortexRelationalSort {
            input: plan,
            keys: vec![VortexRelationalOrderKey {
                column: ColumnRef::new(if order == "count-only" { "n" } else { "g" }).unwrap(),
                descending: order != "ascending",
                nulls: Some(VortexRelationalNullOrder::Last),
            }],
        }));
    }
    prepare_relational_with_schema(policy(), |preparation| {
        preparation.register_batch_source(uri, |session| source(session, 0, nullable))?;
        Ok(plan)
    })
    .unwrap()
}

#[test]
#[allow(clippy::too_many_lines)] // Compare complete values, admission and retained ownership in one matrix.
fn count_partial_prepared_admission_preserves_total_sort_and_source_ties() {
    for (order, nullable, all, mixed) in [
        ("ascending", false, false, false),
        ("descending", false, false, false),
        ("ascending", false, true, false),
        ("count-only", false, false, false),
        ("source", false, false, false),
        ("ascending", true, false, false),
        ("ascending", false, false, true),
    ] {
        let prepared = prepared(order, nullable, all, mixed);
        assert!(!prepared.root_has_file_kernel());
        let initial = prepared.snapshot().memory.reserved_bytes;
        let memory = prepared.session.memory().clone();
        let mut calls = 0;
        let mut producer = |session: &ResidentVortexSession| {
            calls += 1;
            match calls {
                1 | 2 => source(session, 2048, nullable).map(Some),
                3 => Ok(None),
                _ => panic!("COUNT producer replay"),
            }
        };
        #[cfg(debug_assertions)]
        let observation = crate::local_primitives::native_relational_observation::Session::start();
        let result = prepared
            .with_batch_input(&mut producer)
            .unwrap()
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        assert_eq!(calls, 3);
        let expected_groups = if order == "ascending" {
            [-1_i64, 1, 3]
        } else {
            [3_i64, 1, -1]
        };
        let expected: Vec<_> = expected_groups
            .into_iter()
            .map(|g| {
                let selected: Vec<_> = (0..2048)
                    .filter(|row| {
                        [3_i64, 1, -1][row % 3] == g && (!nullable || all || !row.is_multiple_of(7))
                    })
                    .collect();
                let mut row = json!({"g": g, "n": selected.len() * 2});
                if mixed {
                    row["total"] = json!(f64::from(
                        u32::try_from(selected.iter().map(|row| row % 17).sum::<usize>() * 2)
                            .unwrap()
                    ));
                }
                row
            })
            .collect();
        assert_eq!(
            json_rows(&result),
            expected,
            "{order}/{nullable}/{all}/{mixed}"
        );
        assert!(result.execution.native_io_certificate.is_certified());
        assert!(
            !result
                .execution
                .native_io_certificate
                .side_effects
                .fallback_attempted
        );
        assert!(
            !result
                .execution
                .native_io_certificate
                .side_effects
                .fallback_execution_allowed
        );
        let report = result.execution.input.as_ref().unwrap();
        assert_eq!(report.rows, 4096);
        assert!(report.end_of_input_observed);
        let admitted = matches!(order, "ascending" | "descending") && !nullable && !mixed;
        let work = result.execution.native_io_certificate.count_partial_work;
        assert_eq!(work.admitted_batches, if admitted { 2 } else { 0 });
        assert_eq!(work.input_rows, if admitted { 4096 } else { 0 });
        assert_eq!(work.reduced_groups, if admitted { 6 } else { 0 });
        assert_eq!(work.considered_batches, work.admitted_batches);
        assert_eq!(work.declined_capacity_batches, 0);
        assert_eq!(work.declined_unique_batches, 0);
        #[cfg(debug_assertions)]
        {
            let counts = observation.finish();
            let admitted = matches!(order, "ascending" | "descending") && !nullable && !mixed;
            assert_eq!(
                counts.aggregate_count_partial_batches,
                if admitted { 2 } else { 0 }
            );
            if admitted {
                assert_eq!(counts.aggregate_count_partial_input_rows, 4096);
                assert_eq!(counts.aggregate_count_updates, 6);
                assert_eq!(counts.aggregate_count_contributions, 4096);
            }
        }
        drop(result);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, initial);
        drop(prepared);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn count_partial_keeps_late_producer_failure_and_does_not_replay() {
    let prepared = prepared("ascending", false, false, false);
    let initial = prepared.snapshot().memory.reserved_bytes;
    let memory = prepared.session.memory().clone();
    let mut calls = 0;
    let mut producer = |session: &ResidentVortexSession| {
        calls += 1;
        match calls {
            1 => source(session, 2048, false).map(Some),
            2 => Err(shardloom_core::ShardLoomError::InvalidOperation(
                "late COUNT producer failure".into(),
            )),
            _ => panic!("COUNT producer replay after failure"),
        }
    };
    #[cfg(debug_assertions)]
    let observation = crate::local_primitives::native_relational_observation::Session::start();
    let error = prepared
        .with_batch_input(&mut producer)
        .unwrap()
        .collect_jsonl(&CancellationToken::default())
        .err()
        .unwrap()
        .to_string();
    assert!(error.contains("late COUNT producer failure"), "{error}");
    assert_eq!(calls, 2);
    #[cfg(debug_assertions)]
    assert_eq!(observation.finish().aggregate_count_partial_batches, 1);
    assert_eq!(prepared.snapshot().memory.reserved_bytes, initial);
    drop(prepared);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn count_partial_keeps_ordered_spill_on_its_existing_owner() {
    let fixture = Fixture::new(keyed(&[], &[]), 1);
    let workspace = fixture.0.join("count-partial-spill");
    fs::create_dir(&workspace).unwrap();
    let prepared = prepared("ascending", false, false, false)
        .with_spill(VortexRelationalSpillPolicy::new(workspace.clone(), 64 << 20, 1 << 20).unwrap())
        .unwrap();
    let initial = prepared.snapshot().memory.reserved_bytes;
    let memory = prepared.session.memory().clone();
    let mut calls = 0;
    let mut producer = |session: &ResidentVortexSession| {
        calls += 1;
        match calls {
            1 | 2 => source(session, 2048, false).map(Some),
            3 => Ok(None),
            _ => panic!("COUNT spill replay"),
        }
    };
    #[cfg(debug_assertions)]
    let observation = crate::local_primitives::native_relational_observation::Session::start();
    let result = prepared
        .with_batch_input(&mut producer)
        .unwrap()
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(calls, 3);
    assert_eq!(
        json_rows(&result),
        [
            json!({"g": -1, "n": 1364}),
            json!({"g": 1, "n": 1366}),
            json!({"g": 3, "n": 1366})
        ]
    );
    assert_eq!(result.execution.ordered_aggregate_stages, 1);
    assert_eq!(
        result.execution.native_io_certificate.count_partial_work,
        shardloom_core::native_count_partial_work::NativeCountPartialWork::default()
    );
    assert_eq!(result.execution.ordered_aggregate_input_rows, 4096);
    assert!(
        result
            .execution
            .spill
            .as_ref()
            .unwrap()
            .owned_cleanup_completed
    );
    assert!(result.execution.native_io_certificate.is_certified());
    assert!(
        !result
            .execution
            .native_io_certificate
            .side_effects
            .fallback_attempted
    );
    #[cfg(debug_assertions)]
    assert_eq!(observation.finish().aggregate_count_partial_batches, 0);
    drop(result);
    assert_eq!(fs::read_dir(workspace).unwrap().count(), 0);
    assert_eq!(prepared.snapshot().memory.reserved_bytes, initial);
    drop(prepared);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}
