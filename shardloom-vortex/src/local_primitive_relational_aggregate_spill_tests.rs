//! General ordered aggregation matches complete independent values and schemas.

use super::*;
use crate::relational_query::VortexRelationalSpillPolicy;
use serde_json::{Value, json};
use vortex::array::{
    arrays::{DecimalArray, ExtensionArray, FixedSizeListArray, ListViewArray, VarBinArray},
    dtype::DecimalDType,
    extension::datetime::{Date, TimeUnit, Timestamp},
};

#[cfg(feature = "universal-format-io")]
#[path = "local_primitive_relational_aggregate_spill_writer_tests.rs"]
mod writer_tests;

fn ordered(fixture: &Fixture, plan: &VortexRelationalPlan, name: &str) -> PreparedVortexRelational {
    let workspace = fixture.0.join(name);
    fs::create_dir(&workspace).unwrap();
    prepare_relational(plan, policy())
        .unwrap()
        .with_spill(VortexRelationalSpillPolicy::new(workspace, 256 << 20, 1 << 20).unwrap())
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
                rows.extend(serde_json::from_str::<Vec<Value>>(batch.values_json.value()).unwrap());
                Ok(())
            },
        )
        .unwrap();
    assert!(report.native_io_certificate.is_certified());
    assert!(!report.native_io_certificate.side_effects.fallback_attempted);
    let spill = report.spill.as_ref().unwrap();
    assert!(spill.owned_cleanup_completed);
    assert_eq!(fs::read_dir(&spill.workspace).unwrap().count(), 0);
    (rows, report)
}

#[test]
fn ordered_aggregate_all_measures_compound_groups_aliases_and_scalar_match_independent_values() {
    let fixture = fixture();
    let expected_grouped = vec![
        json!({"group":"B","rows":3,"present":2,"distinct":2,"sum":2.0,"avg":1.0,"min":-2,"max":4,"words":2,"first":"Z","last":"ß"}),
        json!({"group":"A","rows":2,"present":2,"distinct":1,"sum":10.0,"avg":5.0,"min":5,"max":5,"words":1,"first":"é","last":"é"}),
        json!({"group":null,"rows":2,"present":1,"distinct":1,"sum":9.0,"avg":9.0,"min":9,"max":9,"words":1,"first":"","last":""}),
    ];
    let expected_scalar = vec![
        json!({"rows":7,"present":5,"distinct":4,"sum":21.0,"avg":4.2,"min":-2,"max":9,"words":4,"first":"","last":"é"}),
    ];
    for (name, groups, expected) in [
        ("grouped", vec!["group"], expected_grouped),
        ("scalar", vec![], expected_scalar),
    ] {
        let mut measures = all_measures();
        for alias in ["v0", "kind", "ordinal", "d0", "o0"] {
            measures.push(measure("count_distinct", Some("number"), alias));
        }
        let expected = expected
            .into_iter()
            .map(|mut row| {
                for alias in ["v0", "kind", "ordinal", "d0", "o0"] {
                    row[alias] = row["distinct"].clone();
                }
                row
            })
            .collect::<Vec<_>>();
        let plan = aggregate(fixture.scan(), &groups, measures);
        let resident = prepare_relational(&plan, policy()).unwrap();
        assert_eq!(
            json_rows(
                &resident
                    .collect_jsonl(&CancellationToken::default())
                    .unwrap()
            ),
            expected
        );
        let prepared = ordered(&fixture, &plan, name);
        assert_eq!(
            prepared.output_dtype().unwrap(),
            resident.output_dtype().unwrap()
        );
        let baseline = prepared.snapshot().memory.reserved_bytes;
        for batch_rows in [1, 2, 3, 2048] {
            let (rows, report) = complete(&prepared, batch_rows);
            assert_eq!(rows, expected);
            assert_eq!(report.ordered_aggregate_stages, 1);
            assert_eq!(report.ordered_aggregate_input_rows, 7);
            assert_eq!(
                report.ordered_aggregate_distinct_rows, 11,
                "repeated aliases share distinct records"
            );
            drop(report);
            assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
        }
    }
    let expected = vec![
        json!({"word":"Z","number":-2}),
        json!({"word":"é","number":5}),
        json!({"word":"Z","number":null}),
        json!({"word":null,"number":9}),
        json!({"word":"ß","number":4}),
        json!({"word":"","number":null}),
    ];
    for with_count in [false, true] {
        let measures = if with_count {
            vec![measure("count", None, "rows")]
        } else {
            vec![]
        };
        let plan = aggregate(fixture.scan(), &["word", "number"], measures);
        let prepared = ordered(
            &fixture,
            &plan,
            if with_count { "compound" } else { "distinct" },
        );
        let mut expected = expected.clone();
        if with_count {
            for (index, row) in expected.iter_mut().enumerate() {
                row["rows"] = json!(if index == 1 { 2 } else { 1 });
            }
        }
        assert_eq!(complete(&prepared, 2).0, expected);
    }
}

#[test]
fn ordered_aggregate_empty_all_null_and_nested_aggregates_keep_complete_schema_and_order() {
    for values in [vec![], vec![None::<i64>, None, None]] {
        let fixture = Fixture::new(
            single(
                "number",
                PrimitiveArray::from_option_iter(values.clone()).into_array(),
            ),
            1,
        );
        let measures = ["count", "count_distinct", "sum", "avg", "min", "max"]
            .map(|function| measure(function, Some("number"), function))
            .to_vec();
        for grouped in [false, true] {
            let plan = aggregate(
                fixture.scan(),
                if grouped { &["number"] } else { &[] },
                measures.clone(),
            );
            let prepared = ordered(&fixture, &plan, if grouped { "grouped" } else { "scalar" });
            let mut row =
                json!({"count":0,"count_distinct":0,"sum":null,"avg":null,"min":null,"max":null});
            if grouped {
                row["number"] = Value::Null;
            }
            let expected = if grouped && values.is_empty() {
                vec![]
            } else {
                vec![row]
            };
            let (actual, report) = complete(&prepared, 1);
            assert_eq!(actual, expected);
            assert_eq!(report.ordered_aggregate_input_rows, values.len() as u64);
            assert_eq!(report.ordered_aggregate_distinct_rows, 0);
            let resident = prepare_relational(&plan, policy()).unwrap();
            assert_eq!(
                prepared.output_dtype().unwrap(),
                resident.output_dtype().unwrap()
            );
        }
    }
    let fixture = fixture();
    let grouped = aggregate(fixture.scan(), &["group"], all_measures());
    let plan = aggregate(
        grouped,
        &[],
        vec![
            measure("sum", Some("rows"), "total"),
            measure("count_distinct", Some("rows"), "distinct"),
        ],
    );
    let prepared = ordered(&fixture, &plan, "nested");
    let (rows, report) = complete(&prepared, 2);
    assert_eq!(rows, vec![json!({"total":7.0,"distinct":2})]);
    assert_eq!(report.ordered_aggregate_stages, 2);
    assert_eq!(report.ordered_aggregate_input_rows, 10);
    assert_eq!(report.ordered_aggregate_distinct_rows, 14);
}

#[test]
fn ordered_aggregate_floating_permutations_signed_zero_and_unsigned_extrema_are_exact() {
    for (index, (values, sum)) in [
        ([1e16_f64, -1e16, 1.0], 1.0),
        ([1e16, 1.0, -1e16], 0.0),
        ([1.0, 1e16, -1e16], 0.0),
    ]
    .into_iter()
    .enumerate()
    {
        let fixture = Fixture::new(
            StructArray::new(
                FieldNames::from(["value", "zero", "wide"]),
                vec![
                    PrimitiveArray::from_iter(values).into_array(),
                    PrimitiveArray::from_iter([-0.0_f64, 0.0, -0.0]).into_array(),
                    PrimitiveArray::from_iter([u64::MAX, 0, 9_007_199_254_740_993]).into_array(),
                ],
                3,
                Validity::NonNullable,
            )
            .into_array(),
            1,
        );
        let plan = aggregate(
            fixture.scan(),
            &["zero"],
            vec![
                measure("sum", Some("value"), "sum"),
                measure("avg", Some("value"), "avg"),
                measure("count_distinct", Some("value"), "distinct"),
                measure("min", Some("zero"), "first"),
                measure("count_distinct", Some("zero"), "zeros"),
                measure("min", Some("wide"), "minimum"),
                measure("max", Some("wide"), "maximum"),
            ],
        );
        let prepared = ordered(&fixture, &plan, &format!("permutation-{index}"));
        assert_eq!(
            complete(&prepared, 1).0,
            vec![
                json!({"zero":-0.0,"sum":sum,"avg":sum/3.0,"distinct":3,"first":-0.0,"zeros":1,"minimum":0,"maximum":u64::MAX})
            ]
        );
        prepared
            .for_each_batch(&CancellationToken::default(), |array, context| {
                for name in ["zero", "first"] {
                    let value =
                        crate::local_primitives::logical_field_from_native_array(&array, name)?;
                    let scalar = crate::local_primitives::result_batch::scalar_value(
                        &value,
                        0,
                        &mut context.native_session().create_execution_ctx(),
                    )?;
                    let crate::local_primitives::result_batch::Value::Float(value) = scalar else {
                        panic!("float dtype lost")
                    };
                    assert_eq!(value.to_bits(), (-0.0_f64).to_bits());
                }
                Ok(())
            })
            .unwrap();
    }
}

// This is a declarative typed fixture matrix.
#[allow(clippy::too_many_lines)]
fn typed_values() -> Vec<(&'static str, ArrayRef, Vec<Value>, Value, Value)> {
    let lists = ListViewArray::try_new(
        PrimitiveArray::from_option_iter([Some(9i64), None, Some(-4)]).into_array(),
        PrimitiveArray::from_iter([0u64, 2, 0, 0, 0, 2]).into_array(),
        PrimitiveArray::from_iter([2u64, 1, 0, 2, 0, 1]).into_array(),
        Validity::from_iter([true, true, false, true, true, true]),
    )
    .unwrap()
    .into_array();
    let fixed = FixedSizeListArray::new(
        PrimitiveArray::from_option_iter([
            Some(9i64),
            None,
            Some(-4),
            Some(1),
            Some(99),
            Some(99),
            Some(9),
            None,
            Some(0),
            Some(0),
            Some(-4),
            Some(1),
        ])
        .into_array(),
        2,
        Validity::from_iter([true, true, false, true, true, true]),
        6,
    )
    .into_array();
    let structs = StructArray::new(
        FieldNames::from(["x", "s"]),
        vec![
            PrimitiveArray::from_iter([9i64, -4, 99, 9, 0, -4]).into_array(),
            VarBinViewArray::from_iter_nullable_str([
                None,
                Some("λ"),
                Some("hidden"),
                None,
                Some(""),
                Some("λ"),
            ])
            .into_array(),
        ],
        6,
        Validity::from_iter([true, true, false, true, true, true]),
    )
    .into_array();
    vec![
        (
            "binary",
            VarBinArray::from(vec![
                Some(&b"\xff\0"[..]),
                Some(&b"\x01"[..]),
                None,
                Some(&b"\xff\0"[..]),
                Some(&b""[..]),
                Some(&b"\x01"[..]),
            ])
            .into_array(),
            vec![json!("ff00"), json!("01"), Value::Null, json!("")],
            json!(""),
            json!("ff00"),
        ),
        (
            "decimal",
            DecimalArray::from_option_iter(
                [
                    Some(201i128),
                    Some(-100),
                    None,
                    Some(201),
                    Some(0),
                    Some(-100),
                ],
                DecimalDType::new(12, 2),
            )
            .into_array(),
            vec![
                json!("decimal128(12,2):201"),
                json!("decimal128(12,2):-100"),
                Value::Null,
                json!("decimal128(12,2):0"),
            ],
            json!("decimal128(12,2):-100"),
            json!("decimal128(12,2):201"),
        ),
        (
            "date",
            ExtensionArray::new(
                Date::new(TimeUnit::Days, Nullability::Nullable).erased(),
                PrimitiveArray::from_option_iter([
                    Some(42i32),
                    Some(-9),
                    None,
                    Some(42),
                    Some(0),
                    Some(-9),
                ])
                .into_array(),
            )
            .into_array(),
            vec![json!(42), json!(-9), Value::Null, json!(0)],
            json!(-9),
            json!(42),
        ),
        (
            "timestamp",
            ExtensionArray::new(
                Timestamp::new(TimeUnit::Microseconds, Nullability::Nullable).erased(),
                PrimitiveArray::from_option_iter([
                    Some(42i64),
                    Some(-9),
                    None,
                    Some(42),
                    Some(0),
                    Some(-9),
                ])
                .into_array(),
            )
            .into_array(),
            vec![json!(42), json!(-9), Value::Null, json!(0)],
            json!(-9),
            json!(42),
        ),
        (
            "list",
            lists,
            vec![json!([9, null]), json!([-4]), Value::Null, json!([])],
            json!([]),
            json!([9, null]),
        ),
        (
            "fixed",
            fixed,
            vec![json!([9, null]), json!([-4, 1]), Value::Null, json!([0, 0])],
            json!([-4, 1]),
            json!([9, null]),
        ),
        (
            "struct",
            structs,
            vec![
                json!({"x":9,"s":null}),
                json!({"x":-4,"s":"λ"}),
                Value::Null,
                json!({"x":0,"s":""}),
            ],
            json!({"x":-4,"s":"λ"}),
            json!({"x":9,"s":null}),
        ),
    ]
}

#[test]
fn ordered_aggregate_typed_nested_grouping_distinct_and_extrema_preserve_metadata() {
    for (name, array, group_values, minimum, maximum) in typed_values() {
        let fixture = Fixture::new(single("kind", array), 2);
        let mut measures = vec![
            measure("count", Some("kind"), "count"),
            measure("count_distinct", Some("kind"), "distinct"),
            measure("min", Some("kind"), "min"),
            measure("max", Some("kind"), "max"),
        ];
        let mut expected = json!({"count":5,"distinct":3,"min":minimum,"max":maximum});
        if name == "decimal" {
            measures.extend([
                measure("sum", Some("kind"), "sum"),
                measure("avg", Some("kind"), "avg"),
            ]);
            expected["sum"] = json!("decimal128(38,2):202");
            expected["avg"] = json!("decimal128(38,6):404000");
        }
        let plan = aggregate(fixture.scan(), &[], measures);
        let prepared = ordered(&fixture, &plan, "scalar");
        let resident = prepare_relational(&plan, policy()).unwrap();
        assert_eq!(
            prepared.output_dtype().unwrap(),
            resident.output_dtype().unwrap(),
            "{name}"
        );
        for batch in [1, 2, 5] {
            assert_eq!(
                complete(&prepared, batch).0,
                vec![expected.clone()],
                "{name}"
            );
        }
        assert_eq!(
            json_rows(
                &resident
                    .collect_jsonl(&CancellationToken::default())
                    .unwrap()
            ),
            vec![expected],
            "{name}"
        );
        let plan = aggregate(
            fixture.scan(),
            &["kind"],
            vec![
                measure("count", None, "ordinal"),
                measure("count", Some("kind"), "v0"),
                measure("count_distinct", Some("kind"), "d0"),
            ],
        );
        let prepared = ordered(&fixture, &plan, "grouped");
        let expected = group_values.into_iter().enumerate().map(|(index, value)| {
            let rows = if index < 2 { 2 } else { 1 };
            json!({"kind":value,"ordinal":rows,"v0":if index == 2 {0} else {rows},"d0":u8::from(index != 2)})
        }).collect::<Vec<_>>();
        assert_eq!(complete(&prepared, 2).0, expected, "{name}");
    }
}

#[test]
fn ordered_aggregate_errors_consumer_cancel_and_source_mutation_release_state() {
    for values in [[f64::MAX, f64::MAX], [1.0, f64::NAN], [1.0, f64::INFINITY]] {
        let fixture = Fixture::new(
            single("value", PrimitiveArray::from_iter(values).into_array()),
            1,
        );
        let plan = aggregate(
            fixture.scan(),
            &[],
            vec![measure("sum", Some("value"), "sum")],
        );
        let prepared = ordered(&fixture, &plan, "invalid");
        let baseline = prepared.snapshot().memory.reserved_bytes;
        assert!(
            prepared
                .collect_jsonl(&CancellationToken::default())
                .is_err()
        );
        assert_eq!(prepared.snapshot().completed_executions, 0);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
        assert_eq!(fs::read_dir(fixture.0.join("invalid")).unwrap().count(), 0);
    }
    for failure in ["consumer", "cancel", "source"] {
        let fixture = fixture();
        let prepared = ordered(
            &fixture,
            &aggregate(fixture.scan(), &["group"], all_measures()),
            "failed",
        );
        let baseline = prepared.snapshot().memory.reserved_bytes;
        let token = CancellationToken::default();
        let mut calls = 0;
        let result = prepared.for_each_batch(&token, |_, _| {
            calls += 1;
            match failure {
                "consumer" => return Err(failed("ordered aggregate consumer sentinel")),
                "cancel" => token.cancel(),
                "source" => fixture.replace(),
                _ => unreachable!(),
            }
            Ok(())
        });
        assert!(result.is_err(), "{failure}");
        assert_eq!(calls, 1);
        assert_eq!(prepared.snapshot().completed_executions, 0);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
        assert_eq!(fs::read_dir(fixture.0.join("failed")).unwrap().count(), 0);
    }
}

#[test]
fn ordered_aggregate_nested_count_does_not_observe_child_payload_values() {
    let items = ListViewArray::try_new(
        PrimitiveArray::from_iter([f64::NAN, f64::INFINITY]).into_array(),
        PrimitiveArray::from_iter([0u64, 0, 1]).into_array(),
        PrimitiveArray::from_iter([1u64, 0, 1]).into_array(),
        Validity::from_iter([true, false, true]),
    )
    .unwrap()
    .into_array();
    let fixture = Fixture::new(single("items", items), 1);
    let plan = aggregate(
        fixture.scan(),
        &[],
        vec![
            measure("count", Some("items"), "present"),
            measure("count", None, "rows"),
        ],
    );
    let resident = prepare_relational(&plan, policy()).unwrap();
    let expected = vec![json!({"present":2,"rows":3})];
    assert_eq!(
        json_rows(
            &resident
                .collect_jsonl(&CancellationToken::default())
                .unwrap()
        ),
        expected
    );
    let prepared = ordered(&fixture, &plan, "count-only");
    assert_eq!(complete(&prepared, 1).0, expected);
}
