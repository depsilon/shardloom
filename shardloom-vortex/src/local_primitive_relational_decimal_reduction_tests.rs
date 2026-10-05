//! Complete native reduction values, declared metadata, and failed-state release.

use super::*;
use vortex::array::{arrays::DecimalArray, dtype::DecimalDType};

fn decimal_fixture(values: &[Option<i128>], precision: u8, scale: i8) -> Fixture {
    Fixture::new(
        single(
            "amount",
            DecimalArray::from_option_iter(
                values.iter().copied(),
                DecimalDType::new(precision, scale),
            )
            .into_array(),
        ),
        1,
    )
}

fn decimal_measures() -> Vec<Measure> {
    ["sum", "avg", "min", "max", "count", "count_distinct"]
        .into_iter()
        .map(|function| measure(function, Some("amount"), function))
        .collect()
}

#[test]
fn native_decimal_reduction_grouped_values_nulls_types_and_primitive_order() {
    let fixture = Fixture::new(
        StructArray::new(
            FieldNames::from(["group", "amount", "primitive"]),
            vec![
                VarBinViewArray::from_iter_str(["A", "A", "A", "B", "N"]).into_array(),
                DecimalArray::from_option_iter(
                    [Some(100i128), Some(201), None, Some(-100), None],
                    DecimalDType::new(12, 2),
                )
                .into_array(),
                PrimitiveArray::from_iter([1e16f64, -1e16, 1.0, 5.0, 6.0]).into_array(),
            ],
            5,
            Validity::NonNullable,
        )
        .into_array(),
        1,
    );
    let mut measures = decimal_measures();
    measures.insert(2, measure("sum", Some("primitive"), "ordered_sum"));
    let prepared =
        prepare_relational(&aggregate(fixture.scan(), &["group"], measures), policy()).unwrap();
    let baseline = prepared.snapshot().memory.reserved_bytes;
    let fields = prepared.output_dtype().unwrap();
    let fields = fields.as_struct_fields_opt().unwrap();
    for (name, precision, scale) in [
        ("sum", 38, 2),
        ("avg", 38, 6),
        ("min", 12, 2),
        ("max", 12, 2),
    ] {
        assert_eq!(
            fields.field(name).unwrap(),
            DType::Decimal(DecimalDType::new(precision, scale), Nullability::Nullable)
        );
    }
    let expected = vec![
        serde_json::json!({"group":"A", "sum":"decimal128(38,2):301", "avg":"decimal128(38,6):1505000", "min":"decimal128(12,2):100", "max":"decimal128(12,2):201", "count":2, "count_distinct":2, "ordered_sum":1.0}),
        serde_json::json!({"group":"B", "sum":"decimal128(38,2):-100", "avg":"decimal128(38,6):-1000000", "min":"decimal128(12,2):-100", "max":"decimal128(12,2):-100", "count":1, "count_distinct":1, "ordered_sum":5.0}),
        serde_json::json!({"group":"N", "sum":null, "avg":null, "min":null, "max":null, "count":0, "count_distinct":0, "ordered_sum":6.0}),
    ];
    for _ in 0..2 {
        let result = prepared
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        assert_eq!(json_rows(&result), expected);
        assert_eq!(result.execution.runtime.prepared_source_opens, 1);
        drop(result);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
    }
}

#[test]
fn native_decimal_reduction_empty_groups_and_all_null_scalar_keep_schema() {
    for values in [vec![], vec![None, None]] {
        let fixture = decimal_fixture(&values, 7, 3);
        let plan = aggregate(fixture.scan(), &[], decimal_measures());
        let scalar = prepare_relational(&plan, policy()).unwrap();
        assert_eq!(
            json_rows(&scalar.collect_jsonl(&CancellationToken::default()).unwrap()),
            vec![
                serde_json::json!({"sum":null,"avg":null,"min":null,"max":null,"count":0,"count_distinct":0})
            ]
        );
        let dtype = scalar.output_dtype().unwrap();
        assert_eq!(
            dtype.as_struct_fields_opt().unwrap().field("avg").unwrap(),
            DType::Decimal(DecimalDType::new(38, 6), Nullability::Nullable)
        );
        let grouped = prepare_relational(
            &aggregate(
                fixture.scan(),
                &["amount"],
                vec![measure("sum", Some("amount"), "sum")],
            ),
            policy(),
        )
        .unwrap();
        let result = grouped
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        assert_eq!(
            json_rows(&result),
            if values.is_empty() {
                vec![]
            } else {
                vec![serde_json::json!({"amount":null,"sum":null})]
            }
        );
    }
}

#[test]
fn native_decimal_reduction_wide_intermediate_and_scale_boundaries() {
    let maximum = 10i128.pow(38) - 1;
    for (values, scale, function, expected) in [
        (
            vec![Some(maximum), Some(maximum), Some(-maximum)],
            6,
            "sum",
            maximum,
        ),
        (vec![Some(maximum), Some(maximum)], 6, "avg", maximum),
        (vec![Some(1), Some(2)], 0, "avg", 1_500_000),
        (vec![Some(10), Some(20)], 38, "avg", 15),
    ] {
        let fixture = decimal_fixture(&values, 38, scale);
        let prepared = prepare_relational(
            &aggregate(
                fixture.scan(),
                &[],
                vec![measure(function, Some("amount"), "value")],
            ),
            policy(),
        )
        .unwrap();
        let output_scale = if function == "avg" {
            scale.max(6)
        } else {
            scale
        };
        assert_eq!(
            json_rows(
                &prepared
                    .collect_jsonl(&CancellationToken::default())
                    .unwrap()
            ),
            vec![serde_json::json!({"value":format!("decimal128(38,{output_scale}):{expected}")})]
        );
    }
}

#[test]
fn native_decimal_reduction_overflow_inexact_average_and_cancel_release_state() {
    let maximum = 10i128.pow(38) - 1;
    for (values, function, reason) in [
        (
            vec![Some(maximum), Some(maximum)],
            "sum",
            "precision overflow",
        ),
        (
            vec![Some(1), Some(0), Some(0)],
            "avg",
            "nonzero fractional digits",
        ),
    ] {
        let fixture = decimal_fixture(&values, 38, 6);
        let prepared = prepare_relational(
            &aggregate(
                fixture.scan(),
                &[],
                vec![measure(function, Some("amount"), "value")],
            ),
            policy(),
        )
        .unwrap();
        let baseline = prepared.snapshot().memory.reserved_bytes;
        for _ in 0..2 {
            let error = prepared
                .collect_jsonl(&CancellationToken::default())
                .err()
                .unwrap();
            assert!(error.to_string().contains(reason), "{error}");
            assert_eq!(prepared.snapshot().completed_executions, 0);
            assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
        }
    }
    let fixture = decimal_fixture(&[Some(1), None, Some(2)], 3, 0);
    let prepared = prepare_relational(
        &aggregate(fixture.scan(), &[], decimal_measures()),
        policy(),
    )
    .unwrap();
    let baseline = prepared.snapshot().memory.reserved_bytes;
    let token = CancellationToken::default();
    assert!(
        prepared
            .for_each_batch(&token, |_, _| {
                token.cancel();
                Ok(())
            })
            .is_err()
    );
    assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
    let block = prepared
        .session
        .memory()
        .reserve((32 << 20) - baseline - 1024)
        .unwrap();
    assert!(
        prepared
            .collect_jsonl(&CancellationToken::default())
            .is_err()
    );
    drop(block);
    assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
    assert!(
        prepared
            .collect_jsonl(&CancellationToken::default())
            .is_ok()
    );
}

#[test]
fn native_decimal_reduction_count_of_untyped_null_is_not_count_star() {
    use crate::relational_query::VortexRelationalProject;
    use shardloom_core::{ExprId, Expression, ScalarValue};
    let fixture = decimal_fixture(&[Some(1), None, Some(2)], 3, 0);
    let projected = VortexRelationalPlan::Project(Box::new(VortexRelationalProject {
        input: fixture.scan(),
        expressions: vec![(
            "missing".into(),
            Expression::literal(ExprId::new("missing").unwrap(), ScalarValue::Null),
        )],
    }));
    let prepared = prepare_relational(
        &aggregate(
            projected,
            &[],
            vec![
                measure("count", Some("missing"), "present"),
                measure("count_distinct", Some("missing"), "distinct"),
                measure("count", None, "rows"),
            ],
        ),
        policy(),
    )
    .unwrap();
    assert_eq!(
        json_rows(
            &prepared
                .collect_jsonl(&CancellationToken::default())
                .unwrap()
        ),
        vec![serde_json::json!({"present":0,"distinct":0,"rows":3})]
    );
}
