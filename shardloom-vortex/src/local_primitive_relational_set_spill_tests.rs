//! Exact full-row domains and first-occurrence payloads through native set runs.

use super::*;
use crate::relational_query::VortexRelationalSpillPolicy;
use serde_json::{Value, json};

fn ordered(fixture: &Fixture, plan: &VortexRelationalPlan) -> PreparedVortexRelational {
    let directory = fixture.0.join("set-runs");
    fs::create_dir_all(&directory).unwrap();
    prepare_relational(plan, policy())
        .unwrap()
        .with_spill(VortexRelationalSpillPolicy::new(directory, 128 << 20, 1 << 20).unwrap())
        .unwrap()
}

pub(super) fn assert_ordered(fixture: &Fixture, plan: &VortexRelationalPlan, expected: &[Value]) {
    let resident = prepare_relational(plan, policy()).unwrap();
    let prepared = ordered(fixture, plan);
    assert_eq!(prepared.output_dtype(), resident.output_dtype());
    let baseline = prepared.snapshot().memory.reserved_bytes;
    for batch_rows in [1, 3, 2048] {
        let mut rows = Vec::new();
        let report = prepared
            .for_each_json_batch(
                &CancellationToken::default(),
                batch_rows,
                1 << 20,
                |batch| {
                    let values: Vec<Value> =
                        serde_json::from_str(batch.values_json.value()).unwrap();
                    assert!(values.len() <= batch_rows);
                    rows.extend(values);
                    Ok(())
                },
            )
            .unwrap();
        assert_eq!(rows, expected, "batch_rows={batch_rows}");
        assert_eq!(report.ordered_set_stages, 1);
        assert_eq!(report.ordered_set_output_rows, expected.len() as u64);
        assert!(report.native_io_certificate.is_certified());
        assert!(!report.native_io_certificate.side_effects.fallback_attempted);
        let spill = report.spill.as_ref().unwrap();
        assert!(spill.owned_cleanup_completed);
        assert_eq!(fs::read_dir(&spill.workspace).unwrap().count(), 0);
        drop(report);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
    }
}

#[test]
fn ordered_sets_keep_first_signed_zero_payload_and_private_name_collisions() {
    let array = |numbers: &[Option<f64>]| {
        StructArray::new(
            FieldNames::from(["key_0", "side", "ordinal"]),
            vec![
                PrimitiveArray::from_option_iter(numbers.iter().copied()).into_array(),
                PrimitiveArray::from_iter(vec![u64::MAX; numbers.len()]).into_array(),
                PrimitiveArray::from_iter(vec![i64::MIN; numbers.len()]).into_array(),
            ],
            numbers.len(),
            Validity::NonNullable,
        )
        .into_array()
    };
    let left = Fixture::new(
        array(&[Some(-0.0), Some(1.0), Some(0.0), Some(-2.0), None]),
        2,
    );
    let right = Fixture::new(array(&[Some(0.0), Some(2.0), None]), 1);
    for (kind, values) in [
        (
            SetKind::UnionDistinct,
            vec![Some(-0.0), Some(1.0), Some(-2.0), None, Some(2.0)],
        ),
        (SetKind::Intersect, vec![Some(-0.0), None]),
        (SetKind::Except, vec![Some(1.0), Some(-2.0)]),
    ] {
        let plan = VortexRelationalPlan::Set(Box::new(VortexRelationalSet {
            left: left.scan(),
            right: right.scan(),
            kind,
        }));
        let expected = values
            .into_iter()
            .map(|value| json!({"key_0":value,"side":u64::MAX,"ordinal":i64::MIN}))
            .collect::<Vec<_>>();
        assert_ordered(&left, &plan, &expected);
        let prepared = ordered(&left, &plan);
        let mut zeros = 0;
        prepared
            .for_each_batch(&CancellationToken::default(), |array, context| {
                let column =
                    crate::local_primitives::logical_field_from_native_array(&array, "key_0")?;
                for row in 0..array.len() {
                    let scalar = crate::local_primitives::result_batch::scalar_value(
                        &column,
                        row,
                        &mut context.native_session().create_execution_ctx(),
                    )?;
                    if let crate::local_primitives::result_batch::Value::Float(value) = scalar
                        && value == 0.0
                    {
                        assert_eq!(value.to_bits(), (-0.0_f64).to_bits());
                        zeros += 1;
                    }
                }
                Ok(())
            })
            .unwrap();
        assert_eq!(zeros, usize::from(kind != SetKind::Except));
    }
}

#[test]
fn ordered_sets_changed_file_sources_after_delivery_prevent_completion() {
    const ROWS: u32 = 65_537;
    for kind in [SetKind::UnionDistinct, SetKind::Intersect, SetKind::Except] {
        for changed_left in [false, true] {
            let input = |offset| {
                Fixture::new(
                    keyed(
                        &(offset..offset + ROWS)
                            .map(|row| Some(u64::from(row)))
                            .collect::<Vec<_>>(),
                        &(offset..offset + ROWS).collect::<Vec<_>>(),
                    ),
                    509,
                )
            };
            let left = input(0);
            let right = input(ROWS / 2);
            let plan = VortexRelationalPlan::Set(Box::new(VortexRelationalSet {
                left: left.scan(),
                right: right.scan(),
                kind,
            }));
            let prepared = ordered(&left, &plan);
            let baseline = prepared.snapshot().memory.reserved_bytes;
            let mut delivered = 0;
            let error = prepared
                .for_each_batch(&CancellationToken::default(), |array, _| {
                    assert!(!array.is_empty());
                    if delivered == 0 {
                        let directory = left.0.join("set-runs");
                        let actual_runs = fs::read_dir(&directory)
                            .unwrap()
                            .flat_map(|entry| {
                                fs::read_dir(entry.unwrap().path())
                                    .unwrap()
                                    .map(|entry| entry.unwrap().path())
                                    .collect::<Vec<_>>()
                            })
                            .filter(|path| {
                                path.extension()
                                    .is_some_and(|extension| extension == "vortex")
                            })
                            .count();
                        assert!(actual_runs > 0);
                        if changed_left {
                            left.replace();
                        } else {
                            right.replace();
                        }
                    }
                    delivered += array.len();
                    Ok(())
                })
                .err()
                .expect("a replaced source must invalidate completion")
                .to_string();
            assert!(
                error.contains("changed"),
                "{kind:?}/{changed_left}: {error}"
            );
            assert!(delivered > 0);
            assert_eq!(prepared.snapshot().completed_executions, 0);
            assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
            assert_eq!(fs::read_dir(left.0.join("set-runs")).unwrap().count(), 0);
        }
    }
}
