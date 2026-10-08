//! Complete analytic values and ownership through the explicit native strategy.

use super::*;
use crate::relational_query::{
    VortexRelationalFrameExclusion as Exclusion, VortexRelationalFrameFunction as Function,
    VortexRelationalFrameUnit as Unit, VortexRelationalNullOrder as NullOrder,
    VortexRelationalSpillPolicy, VortexRelationalWindowFrame as Frame,
    VortexRelationalWindowFunction,
};
use serde_json::{Value, json};

#[path = "local_primitive_relational_window_pressure_tests.rs"]
mod pressure_tests;

#[path = "local_primitive_relational_window_spill_semantics_tests.rs"]
mod semantics_tests;

#[cfg(feature = "universal-format-io")]
#[path = "local_primitive_relational_window_spill_writer_tests.rs"]
mod writer_cases;

fn ordered(fixture: &Fixture, plan: &VortexRelationalPlan) -> PreparedVortexRelational {
    let workspace = fixture.0.join("window-runs");
    fs::create_dir_all(&workspace).unwrap();
    prepare_relational(plan, policy())
        .unwrap()
        .with_spill(VortexRelationalSpillPolicy::new(workspace, 128 << 20, 1 << 20).unwrap())
        .unwrap()
}

fn complete(
    prepared: &PreparedVortexRelational,
    batch_rows: usize,
) -> (Vec<Value>, ExecutedVortexRelational) {
    let mut rows = Vec::new();
    let report = prepared
        .for_each_json_batch(
            &CancellationToken::default(),
            batch_rows,
            1 << 20,
            |batch| {
                let values = serde_json::from_str::<Vec<Value>>(batch.values_json.value()).unwrap();
                assert!(values.len() <= batch_rows);
                rows.extend(values);
                Ok(())
            },
        )
        .unwrap();
    assert!(report.native_io_certificate.is_certified());
    assert!(!report.native_io_certificate.side_effects.fallback_attempted);
    assert_eq!(report.ordered_window_stages, 1);
    assert_eq!(report.ordered_window_input_rows, rows.len() as u64);
    assert!(
        report
            .native_io_certificate
            .source_pushdown_report
            .proof_basis
            .contains("ordered_window_stages=1")
    );
    let spill = report.spill.as_ref().unwrap();
    assert!(spill.owned_cleanup_completed);
    assert_eq!(fs::read_dir(&spill.workspace).unwrap().count(), 0);
    (rows, report)
}

#[test]
fn ordered_window_ranking_navigation_groups_and_original_order_match_complete_reference() {
    let fixture = window_tests::fixture();
    let prepared = ordered(&fixture, &window_tests::plan(fixture.scan()));
    let baseline = prepared.snapshot().memory.reserved_bytes;
    for batch_rows in [1, 3, 2048] {
        let (rows, report) = complete(&prepared, batch_rows);
        assert_eq!(rows, window_tests::expected(), "batch_rows={batch_rows}");
        assert_eq!(report.ordered_window_groups, 2);
        assert_eq!(report.ordered_window_partitions, 6);
        assert_eq!(report.ordered_window_bounds_rows, 0);
        assert!(report.ordered_window_peer_records > 0);
        assert!(report.ordered_window_lookup_blocks > 0);
        drop(report);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
    }
}

#[test]
fn ordered_window_all_frame_units_exclusions_directions_and_nulls_match_membership_oracle() {
    for chunk in [1, 3, 64] {
        let fixture = window_frame_tests::fixture(chunk);
        for unit in [Unit::Rows, Unit::Groups, Unit::Range] {
            for exclusion in [
                Exclusion::NoOthers,
                Exclusion::CurrentRow,
                Exclusion::Group,
                Exclusion::Ties,
            ] {
                for descending in [false, true] {
                    for nulls in [NullOrder::First, NullOrder::Last] {
                        let plan = window_frame_tests::plan(
                            fixture.scan(),
                            unit,
                            exclusion,
                            descending,
                            nulls,
                        );
                        let prepared = ordered(&fixture, &plan);
                        let expected =
                            window_frame_tests::reference(unit, exclusion, descending, nulls);
                        let baseline = prepared.snapshot().memory.reserved_bytes;
                        for batch_rows in [1, 7] {
                            let (rows, report) = complete(&prepared, batch_rows);
                            assert_eq!(
                                rows, expected,
                                "{chunk} {unit:?} {exclusion:?} {descending} {nulls:?} batch={batch_rows}"
                            );
                            assert_eq!(report.ordered_window_groups, 1);
                            assert_eq!(report.ordered_window_partitions, 2);
                            assert_eq!(report.ordered_window_bounds_rows, 30);
                            drop(report);
                            assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
                        }
                    }
                }
            }
        }
    }
}

#[test]
fn ordered_window_count_and_exclusions_preserve_unobserved_nonfinite_values() {
    let fixture = Fixture::new(
        single(
            "value",
            PrimitiveArray::from_option_iter([Some(f64::NAN), Some(f64::INFINITY), None])
                .into_array(),
        ),
        1,
    );
    let column = ColumnRef::new("value").unwrap();
    let plan = window_frame_tests::one(
        &fixture,
        VortexRelationalWindowFunction::Framed(Function::Count(column.clone())),
        None,
        None,
    );
    let prepared = ordered(&fixture, &plan);
    assert_eq!(complete(&prepared, 1).0, vec![json!({"out": 2}); 3]);
    for function in [
        Function::Sum(column.clone()),
        Function::Avg(column.clone()),
        Function::Min(column.clone()),
        Function::Max(column.clone()),
        Function::CountDistinct(column.clone()),
    ] {
        let expected = if matches!(function, Function::CountDistinct(_)) {
            json!(0)
        } else {
            Value::Null
        };
        let excluded = Frame {
            exclusion: Exclusion::Group,
            ..Frame::default()
        };
        let plan = window_frame_tests::one(
            &fixture,
            VortexRelationalWindowFunction::Framed(function.clone()),
            Some(excluded),
            None,
        );
        let prepared = ordered(&fixture, &plan);
        let baseline = prepared.snapshot().memory.reserved_bytes;
        assert_eq!(complete(&prepared, 2).0, vec![json!({"out": expected}); 3]);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
        let plan = window_frame_tests::one(
            &fixture,
            VortexRelationalWindowFunction::Framed(function),
            None,
            None,
        );
        let resident = prepare_relational(&plan, policy()).unwrap();
        let prepared = ordered(&fixture, &plan);
        let baseline = prepared.snapshot().memory.reserved_bytes;
        assert_eq!(
            prepared.execute_owned().err().unwrap().to_string(),
            resident.execute_owned().err().unwrap().to_string()
        );
        assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
        assert_eq!(prepared.snapshot().completed_executions, 0);
        assert_eq!(
            fs::read_dir(fixture.0.join("window-runs")).unwrap().count(),
            0
        );
    }
}
