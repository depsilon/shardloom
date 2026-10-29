use super::*;

fn declared(name: &str, mut args: Vec<Expression>, p: i64, s: i64, mode: &str) -> Expression {
    args.extend([
        literal(ScalarValue::Int64(p)),
        literal(ScalarValue::Int64(s)),
        literal(ScalarValue::Utf8(mode.into())),
    ]);
    function(name, args)
}

#[test]
fn native_decimal_output_rounds_once_and_keeps_modes_distinct() {
    let fixture = Fixture::new(
        StructArray::new(
            FieldNames::from(["amount"]),
            vec![
                DecimalArray::from_option_iter(
                    [Some(125_i128), Some(-125), Some(135), Some(-135), None],
                    DecimalDType::new(5, 2),
                )
                .into_array(),
            ],
            5,
            Validity::NonNullable,
        )
        .into_array(),
        2,
    );
    let expressions = [
        ("even", "decimal_rescale", "half_even", None, 1),
        ("away", "decimal_rescale", "half_away", None, 1),
        ("floor", "decimal_rescale", "floor", None, 1),
        ("ceil", "decimal_rescale", "ceil", None, 1),
        ("trunc", "decimal_rescale", "toward_zero", None, 1),
        ("add", "decimal_add", "half_even", Some(2), 1),
        ("sub", "decimal_subtract", "half_even", Some(2), 1),
        ("mul", "decimal_multiply", "exact", Some(2), 2),
        ("div", "decimal_divide", "half_even", Some(2), 2),
    ]
    .into_iter()
    .map(|(alias, name, mode, right, scale)| {
        let mut args = vec![col("amount")];
        if let Some(right) = right {
            args.push(literal(ScalarValue::Int64(right)));
        }
        (alias, declared(name, args, 5, scale, mode))
    })
    .collect::<Vec<_>>();
    let plan = project(fixture.scan(), expressions.clone());
    let expected = [
        [12, 13, 12, 13, 12, 32, -8, 250, 62],
        [-12, -13, -13, -12, -12, 8, -32, -250, -62],
        [14, 14, 13, 14, 13, 34, -6, 270, 68],
        [-14, -14, -14, -13, -13, 6, -34, -270, -68],
    ]
    .into_iter()
    .map(|values| {
        expressions
            .iter()
            .zip(values)
            .map(|((name, _), value)| {
                let scale = if matches!(*name, "mul" | "div") { 2 } else { 1 };
                (
                    (*name).to_owned(),
                    json!(format!("decimal128(5,{scale}):{value}")),
                )
            })
            .collect::<serde_json::Map<_, _>>()
    })
    .map(serde_json::Value::Object)
    .chain(std::iter::once(json!({
        "even":null,"away":null,"floor":null,"ceil":null,"trunc":null,
        "add":null,"sub":null,"mul":null,"div":null,
    })))
    .collect::<Vec<_>>();
    assert_eq!(collect(&plan), expected);
    let empty_plan = project(empty(fixture.scan()), expressions);
    assert_eq!(
        prepare_relational(&plan, policy()).unwrap().output_dtype(),
        prepare_relational(&empty_plan, policy())
            .unwrap()
            .output_dtype(),
    );
    assert_eq!(collect(&empty_plan), [] as [serde_json::Value; 0]);
}

#[test]
fn native_decimal_output_validates_declarations_before_empty_execution() {
    let fixture = decimals();
    let mut dynamic = vec![
        col("amount"),
        col("n"),
        literal(ScalarValue::Int64(1)),
        literal(ScalarValue::Utf8("exact".into())),
    ];
    let invalid = [
        declared("decimal_rescale", vec![col("amount")], 0, 0, "exact"),
        declared("decimal_rescale", vec![col("amount")], 39, 0, "exact"),
        declared("decimal_rescale", vec![col("amount")], 2, 3, "exact"),
        declared("decimal_rescale", vec![col("amount")], 5, 1, "half_up"),
        declared("decimal_rescale", vec![col("text")], 5, 1, "exact"),
        declared(
            "decimal_rescale",
            vec![literal(ScalarValue::Float64(1.0))],
            5,
            1,
            "exact",
        ),
        function("decimal_rescale", std::mem::take(&mut dynamic)),
    ];
    for expression in invalid {
        assert!(
            prepare_relational(
                &project(empty(fixture.scan()), vec![("value", expression)]),
                policy()
            )
            .is_err()
        );
    }
    let null = project(
        fixture.scan(),
        vec![(
            "value",
            declared(
                "decimal_divide",
                vec![literal(ScalarValue::Null), literal(ScalarValue::Int64(0))],
                5,
                2,
                "exact",
            ),
        )],
    );
    assert_eq!(collect(&null), vec![json!({"value":null}); 4]);
}

#[test]
fn native_decimal_output_rejects_selected_inexact_overflow_and_zero_division() {
    let fixture = decimals();
    for (expression, message) in [
        (
            declared("decimal_rescale", vec![col("amount")], 5, 1, "exact"),
            "nonzero fractional digits",
        ),
        (
            declared(
                "decimal_rescale",
                vec![literal(ScalarValue::Decimal128 {
                    value: 99995,
                    precision: 5,
                    scale: 3,
                })],
                4,
                2,
                "half_even",
            ),
            "precision overflow",
        ),
        (
            declared(
                "decimal_divide",
                vec![col("amount"), literal(ScalarValue::Int64(0))],
                5,
                2,
                "half_even",
            ),
            "division by zero",
        ),
    ] {
        let error = prepare_relational(
            &project(fixture.scan(), vec![("value", expression)]),
            policy(),
        )
        .unwrap()
        .collect_jsonl(&CancellationToken::default())
        .err()
        .unwrap();
        assert!(error.to_string().contains(message), "{error}");
    }
}
