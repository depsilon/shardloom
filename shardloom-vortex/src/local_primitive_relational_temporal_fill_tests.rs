//! Full-result fill/interpolation semantics over the existing window owners.

use super::*;
use crate::relational_query::{
    VortexRelationalFillMethod as Method, VortexRelationalNullOrder, VortexRelationalOrderKey,
    VortexRelationalWindow, VortexRelationalWindowExpression,
    VortexRelationalWindowFunction as Function,
};

fn plan(
    input: VortexRelationalPlan,
    descending: bool,
    grouped: bool,
    linear: bool,
) -> VortexRelationalPlan {
    let mut methods = vec![("forward", Method::Forward), ("backward", Method::Backward)];
    if linear {
        methods.push(("linear", Method::Linear));
    }
    VortexRelationalPlan::Window(Box::new(VortexRelationalWindow {
        input,
        columns: vec![ColumnRef::new("id").unwrap()],
        expressions: methods
            .into_iter()
            .map(|(name, method)| VortexRelationalWindowExpression {
                output_column: name.into(),
                function: Function::Fill {
                    column: ColumnRef::new("value").unwrap(),
                    method,
                },
                partition_by: if grouped {
                    vec![ColumnRef::new("group").unwrap()]
                } else {
                    vec![]
                },
                order_by: vec![VortexRelationalOrderKey {
                    column: ColumnRef::new("instant").unwrap(),
                    descending,
                    nulls: Some(VortexRelationalNullOrder::Last),
                }],
                frame: None,
            })
            .collect(),
    }))
}

fn simple(times: &[Option<i64>], values: ArrayRef) -> Fixture {
    Fixture::new(
        StructArray::new(
            FieldNames::from(["id", "instant", "value"]),
            vec![
                PrimitiveArray::from_iter(0..times.len() as u64).into_array(),
                temporal_array("timestamp(ns)", times),
                values,
            ],
            times.len(),
            Validity::NonNullable,
        )
        .into_array(),
        2,
    )
}

#[test]
#[allow(clippy::too_many_lines)] // Compare complete resident/spill results and retained ownership on the same fixture.
fn native_temporal_fill_uses_exact_elapsed_ticks_partitions_and_original_order() {
    let epoch = 9_000_000_000_000_000_000;
    let times = [3, 0, 0, 5, 7, 1, 2, 6].map(|time| Some(epoch + time));
    let fixture = Fixture::new(
        StructArray::new(
            FieldNames::from(["id", "group", "instant", "value"]),
            vec![
                PrimitiveArray::from_iter(0u64..8).into_array(),
                VarBinArray::from(vec![
                    Some("a"),
                    None,
                    Some("a"),
                    Some("a"),
                    None,
                    Some("a"),
                    None,
                    Some("a"),
                ])
                .into_array(),
                temporal_array("timestamp(ns)", &times),
                PrimitiveArray::from_option_iter([
                    None,
                    Some(5i64),
                    None,
                    Some(30),
                    Some(19),
                    Some(10),
                    None,
                    None,
                ])
                .into_array(),
            ],
            8,
            Validity::NonNullable,
        )
        .into_array(),
        2,
    );
    let expected = [
        json!({"id":0,"forward":10,"backward":30,"linear":20.0}),
        json!({"id":1,"forward":5,"backward":5,"linear":5.0}),
        json!({"id":2,"forward":null,"backward":10,"linear":null}),
        json!({"id":3,"forward":30,"backward":30,"linear":30.0}),
        json!({"id":4,"forward":19,"backward":19,"linear":19.0}),
        json!({"id":5,"forward":10,"backward":10,"linear":10.0}),
        json!({"id":6,"forward":5,"backward":19,"linear":9.0}),
        json!({"id":7,"forward":30,"backward":null,"linear":null}),
    ];
    for descending in [false, true] {
        let plan = plan(fixture.scan(), descending, true, true);
        let expected = expected
            .iter()
            .cloned()
            .map(|mut row| {
                if descending {
                    let forward = row["forward"].clone();
                    row["forward"] = row["backward"].clone();
                    row["backward"] = forward;
                }
                row
            })
            .collect::<Vec<_>>();
        assert_eq!(collect(&plan), expected);
        let workspace = fixture.0.join(if descending {
            "reverse-fill"
        } else {
            "forward-fill"
        });
        fs::create_dir_all(&workspace).unwrap();
        let prepared = prepare_relational(&plan, policy())
            .unwrap()
            .with_spill(
                crate::relational_query::VortexRelationalSpillPolicy::new(
                    workspace.clone(),
                    128 << 20,
                    1 << 20,
                )
                .unwrap(),
            )
            .unwrap();
        let baseline = prepared.snapshot().memory.reserved_bytes;
        let output = prepared
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        let rows = output
            .result_jsonl
            .value()
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(rows, expected);
        assert_eq!(output.execution.ordered_window_stages, 1);
        assert!(
            output
                .execution
                .spill
                .as_ref()
                .unwrap()
                .owned_cleanup_completed
        );
        assert_eq!(fs::read_dir(&workspace).unwrap().count(), 0);
        drop(output);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
    }
    let prepared = prepare_relational(&plan(fixture.scan(), false, true, true), policy()).unwrap();
    let dtype = prepared.output_dtype().unwrap();
    let memory = prepared.session.memory().clone();
    let output = prepared
        .execute_native(&CancellationToken::default())
        .unwrap();
    drop((prepared, fixture));
    assert_eq!(output.result.row_count(), 8);
    assert_eq!(output.result.dtype(), &dtype);
    drop(output);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn native_temporal_fill_preserves_nested_values_and_all_null_edges() {
    let values = StructArray::new(
        FieldNames::from(["text"]),
        vec![
            VarBinArray::from(vec!["unobserved", "a", "unobserved", "b", "unobserved"])
                .into_array(),
        ],
        5,
        Validity::from_iter([false, true, false, true, false]),
    )
    .into_array();
    let fixture = simple(&[Some(0), Some(1), Some(2), Some(3), Some(4)], values);
    assert_eq!(
        collect(&plan(fixture.scan(), false, false, false)),
        vec![
            json!({"id":0,"forward":null,"backward":{"text":"a"}}),
            json!({"id":1,"forward":{"text":"a"},"backward":{"text":"a"}}),
            json!({"id":2,"forward":{"text":"a"},"backward":{"text":"b"}}),
            json!({"id":3,"forward":{"text":"b"},"backward":{"text":"b"}}),
            json!({"id":4,"forward":{"text":"b"},"backward":null}),
        ]
    );
    assert_eq!(
        collect(&plan(empty(fixture.scan()), false, false, false)),
        [] as [serde_json::Value; 0]
    );
}

#[test]
fn native_temporal_fill_rejects_ambiguous_times_and_lossy_numeric_inputs() {
    for times in [[Some(0), Some(0)], [Some(0), None]] {
        let fixture = simple(
            &times,
            PrimitiveArray::from_option_iter([Some(1i64), None]).into_array(),
        );
        let prepared =
            prepare_relational(&plan(fixture.scan(), false, false, true), policy()).unwrap();
        let baseline = prepared.snapshot().memory.reserved_bytes;
        assert!(
            prepared
                .execute_native(&CancellationToken::default())
                .is_err()
        );
        assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
    }
    for values in [
        PrimitiveArray::from_option_iter([Some(9_007_199_254_740_993u64), None]).into_array(),
        PrimitiveArray::from_option_iter([Some(f64::INFINITY), None]).into_array(),
    ] {
        let fixture = simple(&[Some(0), Some(1)], values);
        let prepared =
            prepare_relational(&plan(fixture.scan(), false, false, true), policy()).unwrap();
        let baseline = prepared.snapshot().memory.reserved_bytes;
        assert!(
            prepared
                .execute_native(&CancellationToken::default())
                .is_err()
        );
        assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
    }
    let fixture = simple(
        &[Some(0), Some(1)],
        VarBinArray::from(vec![Some("x"), None]).into_array(),
    );
    assert!(
        prepare_relational(&plan(empty(fixture.scan()), false, false, true), policy()).is_err()
    );
    let mut declaration = plan(empty(fixture.scan()), false, false, false);
    let VortexRelationalPlan::Window(window) = &mut declaration else {
        panic!()
    };
    window.expressions[0].frame =
        Some(crate::relational_query::VortexRelationalWindowFrame::default());
    assert!(prepare_relational(&declaration, policy()).is_err());
}

#[test]
fn native_temporal_fill_extreme_floats_and_tick_spans_do_not_overflow() {
    let fixture = simple(
        &[Some(i64::MIN), Some(0), Some(i64::MAX)],
        PrimitiveArray::from_option_iter([Some(-f64::MAX), None, Some(f64::MAX)]).into_array(),
    );
    let rows = collect(&plan(fixture.scan(), false, false, true));
    assert_eq!(rows[1]["linear"], json!(0.0));
    assert_eq!(rows[0]["linear"], json!(-f64::MAX));
    assert_eq!(rows[2]["linear"], json!(f64::MAX));
    let fixture = simple(
        &[Some(0), Some(1), Some(3)],
        PrimitiveArray::from_option_iter([None::<f64>, None, None]).into_array(),
    );
    assert_eq!(
        collect(&plan(fixture.scan(), false, false, true)),
        vec![
            json!({"id":0,"forward":null,"backward":null,"linear":null}),
            json!({"id":1,"forward":null,"backward":null,"linear":null}),
            json!({"id":2,"forward":null,"backward":null,"linear":null}),
        ]
    );
}

#[test]
fn native_temporal_fill_decimal_conversion_has_an_explicit_precision_boundary() {
    use vortex::array::{arrays::DecimalArray, dtype::DecimalDType};
    let fixture = simple(
        &[Some(0), Some(1), Some(4)],
        DecimalArray::from_option_iter([Some(10i128), None, Some(50)], DecimalDType::new(4, 2))
            .into_array(),
    );
    let rows = collect(&plan(fixture.scan(), false, false, true));
    assert_eq!(rows[0]["linear"], json!(0.1));
    assert_eq!(rows[1]["linear"], json!(0.2));
    assert_eq!(rows[2]["linear"], json!(0.5));
    let fixture = simple(
        &[Some(0)],
        DecimalArray::from_option_iter([Some(9_007_199_254_740_993i128)], DecimalDType::new(18, 2))
            .into_array(),
    );
    let prepared = prepare_relational(&plan(fixture.scan(), false, false, true), policy()).unwrap();
    let error = prepared
        .execute_native(&CancellationToken::default())
        .err()
        .unwrap();
    assert!(
        error.to_string().contains("coefficients within +/-2^53"),
        "{error}"
    );
}
