//! Complete native statistical results, independent values and owner cleanup.

use super::*;
use crate::relational_query::VortexRelationalSpillPolicy;
use serde_json::{Value, json};

#[path = "local_primitive_relational_statistical_window_tests.rs"]
mod window_tests;

fn moments() -> Vec<Measure> {
    ["var_pop", "var_samp", "stddev_pop", "stddev_samp"]
        .map(|name| measure(name, Some("number"), name))
        .to_vec()
}

fn assert_close(actual: &[Value], expected: &[Value]) {
    assert_eq!(actual.len(), expected.len());
    for (actual, expected) in actual.iter().zip(expected) {
        let actual = actual.as_object().unwrap();
        let expected = expected.as_object().unwrap();
        assert_eq!(
            actual.keys().collect::<Vec<_>>(),
            expected.keys().collect::<Vec<_>>()
        );
        for (name, expected) in expected {
            if let Some(expected) = expected.as_f64() {
                let actual = actual[name].as_f64().unwrap();
                let error = if expected == 0.0 {
                    actual.abs()
                } else {
                    (actual / expected - 1.0).abs()
                };
                assert!(error <= 1e-12, "{name}: {actual} != {expected}");
            } else {
                assert_eq!(&actual[name], expected, "{name}");
            }
        }
    }
}

fn verify(fixture: &Fixture, groups: &[&str], measures: Vec<Measure>, expected: &[Value]) {
    let plan = aggregate(fixture.scan(), groups, measures);
    for spill in [false, true] {
        let prepared = prepare_relational(&plan, policy()).unwrap();
        let workspace = fixture.0.join(if groups.is_empty() {
            "scalar"
        } else {
            "grouped"
        });
        let prepared = if spill {
            fs::create_dir(&workspace).unwrap();
            prepared
                .with_spill(
                    VortexRelationalSpillPolicy::new(&workspace, 64 << 20, 1 << 20).unwrap(),
                )
                .unwrap()
        } else {
            prepared
        };
        for dtype in prepared
            .output_dtype()
            .unwrap()
            .as_struct_fields_opt()
            .unwrap()
            .fields()
            .skip(groups.len())
        {
            assert_eq!(dtype, DType::Primitive(PType::F64, Nullability::Nullable));
        }
        let baseline = prepared.snapshot().memory.reserved_bytes;
        for batch_rows in [1, 3] {
            let mut rows = Vec::new();
            let report = prepared
                .for_each_json_batch(
                    &CancellationToken::default(),
                    batch_rows,
                    1 << 20,
                    |batch| {
                        rows.extend(
                            serde_json::from_str::<Vec<Value>>(batch.values_json.value()).unwrap(),
                        );
                        Ok(())
                    },
                )
                .unwrap();
            assert_close(&rows, expected);
            assert!(report.native_io_certificate.is_certified());
            assert!(!report.native_io_certificate.side_effects.fallback_attempted);
            if spill {
                assert_eq!(report.ordered_aggregate_stages, 1);
                assert!(report.spill.as_ref().unwrap().owned_cleanup_completed);
                assert_eq!(fs::read_dir(&workspace).unwrap().count(), 0);
            }
            drop(report);
            assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
        }
    }
}

#[test]
fn native_statistical_aggregate_grouped_scalar_and_spill_match_independent_values() {
    let fixture = fixture();
    verify(
        &fixture,
        &["group"],
        moments(),
        &[
            json!({"group":"B", "var_pop":9.0, "var_samp":18.0, "stddev_pop":3.0, "stddev_samp":18.0f64.sqrt()}),
            json!({"group":"A", "var_pop":0.0, "var_samp":0.0, "stddev_pop":0.0, "stddev_samp":0.0}),
            json!({"group":null, "var_pop":0.0, "var_samp":null, "stddev_pop":0.0, "stddev_samp":null}),
        ],
    );
    verify(
        &fixture,
        &[],
        moments(),
        &[
            json!({"var_pop":12.56, "var_samp":15.7, "stddev_pop":12.56f64.sqrt(), "stddev_samp":15.7f64.sqrt()}),
        ],
    );
}

#[test]
fn native_statistical_scaled_results_survive_grouping_batch_merges_and_spill() {
    for (x, y, covariance) in [
        (1e-200, -1e-200, -0.0),
        (2.0f64.powi(1000), 2.0f64.powi(-1000), 0.25),
        (2.0f64.powi(-500), 2.0f64.powi(-572), f64::from_bits(1)),
    ] {
        let fixture = Fixture::new(
            StructArray::new(
                FieldNames::from(["group", "number", "paired"]),
                vec![
                    PrimitiveArray::from_iter([0u64, 0]).into_array(),
                    PrimitiveArray::from_iter([0.0, x]).into_array(),
                    PrimitiveArray::from_iter([0.0, y]).into_array(),
                ],
                2,
                Validity::NonNullable,
            )
            .into_array(),
            1,
        );
        for groups in [&[][..], &["group"][..]] {
            let mut expected =
                json!({"stddev":x.abs()*0.5, "covariance":covariance, "correlation":y.signum()});
            if !groups.is_empty() {
                expected["group"] = json!(0);
            }
            verify(
                &fixture,
                groups,
                vec![
                    measure("stddev_pop", Some("number"), "stddev"),
                    measure("covar_pop", Some("number"), "covariance")
                        .with_secondary_column(ColumnRef::new("paired").unwrap()),
                    measure("corr", Some("number"), "correlation")
                        .with_secondary_column(ColumnRef::new("paired").unwrap()),
                ],
                &[expected],
            );
        }
    }
}

#[test]
fn native_statistical_aggregate_empty_null_and_wide_integer_inputs_preserve_semantics() {
    for values in [
        vec![],
        vec![None, None],
        vec![Some(u64::MAX), None],
        vec![
            Some(u64::MAX - 3),
            Some(u64::MAX - 2),
            Some(u64::MAX - 1),
            Some(u64::MAX),
        ],
    ] {
        let count = values.iter().flatten().count();
        let fixture = Fixture::new(
            single(
                "number",
                PrimitiveArray::from_option_iter(values).into_array(),
            ),
            1,
        );
        let (population, sample) = match count {
            0 => (None, None),
            1 => (Some(0.0f64), None),
            4 => (Some(1.25), Some(5.0 / 3.0)),
            _ => unreachable!(),
        };
        verify(
            &fixture,
            &[],
            moments(),
            &[json!({
                "var_pop":population, "var_samp":sample,
                "stddev_pop":population.map(f64::sqrt), "stddev_samp":sample.map(f64::sqrt),
            })],
        );
    }
}

#[test]
fn native_statistical_aggregate_failure_releases_charged_state_and_spill_files() {
    for values in [[0.0, f64::NAN], [0.0, f64::INFINITY], [-f64::MAX, f64::MAX]] {
        let fixture = Fixture::new(
            single("number", PrimitiveArray::from_iter(values).into_array()),
            1,
        );
        let plan = aggregate(fixture.scan(), &[], moments());
        for spill in [false, true] {
            let prepared = prepare_relational(&plan, policy()).unwrap();
            let workspace = fixture.0.join("failed");
            let prepared = if spill {
                fs::create_dir(&workspace).unwrap();
                prepared
                    .with_spill(
                        VortexRelationalSpillPolicy::new(&workspace, 64 << 20, 1 << 20).unwrap(),
                    )
                    .unwrap()
            } else {
                prepared
            };
            let baseline = prepared.snapshot().memory.reserved_bytes;
            assert!(
                prepared
                    .collect_jsonl(&CancellationToken::default())
                    .is_err()
            );
            assert_eq!(prepared.snapshot().completed_executions, 0);
            assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
            if spill {
                assert_eq!(fs::read_dir(&workspace).unwrap().count(), 0);
            }
        }
    }
}

#[test]
fn native_statistical_paired_aggregate_retains_both_inputs_and_uses_complete_pairs() {
    let fixture = Fixture::new(
        StructArray::new(
            FieldNames::from(["group", "number", "paired"]),
            vec![
                PrimitiveArray::from_iter([0u8, 0, 1, 1, 2, 2]).into_array(),
                PrimitiveArray::from_option_iter([
                    Some(1i64),
                    Some(2),
                    Some(3),
                    Some(4),
                    None,
                    Some(9),
                ])
                .into_array(),
                PrimitiveArray::from_option_iter([
                    Some(2i64),
                    Some(1),
                    Some(4),
                    Some(3),
                    Some(1),
                    None,
                ])
                .into_array(),
            ],
            6,
            Validity::NonNullable,
        )
        .into_array(),
        1,
    );
    let measures = ["covar_pop", "covar_samp", "corr"]
        .map(|name| {
            measure(name, Some("number"), name)
                .with_secondary_column(ColumnRef::new("paired").unwrap())
        })
        .to_vec();
    verify(
        &fixture,
        &["group"],
        measures.clone(),
        &[
            json!({"group":0,"covar_pop":-0.25,"covar_samp":-0.5,"corr":-1.0}),
            json!({"group":1,"covar_pop":-0.25,"covar_samp":-0.5,"corr":-1.0}),
            json!({"group":2,"covar_pop":null,"covar_samp":null,"corr":null}),
        ],
    );
    verify(
        &fixture,
        &[],
        measures,
        &[json!({"covar_pop":0.75,"covar_samp":1.0,"corr":0.6})],
    );
    for invalid in [
        measure("corr", Some("number"), "bad"),
        measure("sum", Some("number"), "bad")
            .with_secondary_column(ColumnRef::new("paired").unwrap()),
        measure("corr", Some("number"), "bad")
            .with_secondary_column(ColumnRef::new("missing").unwrap()),
    ] {
        assert!(
            prepare_relational(&aggregate(fixture.scan(), &[], vec![invalid]), policy()).is_err()
        );
    }
}
