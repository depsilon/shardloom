use super::*;
use crate::local_primitives::prepared_unary::prepare_unary;
use crate::query_primitive::{
    VortexDuplicateKeepPolicy as Keep, VortexExpressionProjectionRequest,
    VortexExpressionRewrite as Rewrite, VortexMeltProjectionRequest, VortexPivotProjectionRequest,
    VortexQueryPrimitiveKind as Kind, VortexQueryPrimitiveRequest, VortexRollingWindowRequest,
};
use crate::relational_query::VortexRelationalUnary;
use shardloom_core::{ComparisonOp, PredicateExpr, ScalarValue, StatValue};
use shardloom_plan::ProjectionRequest;

#[path = "local_primitive_relational_typed_unary_ownership_tests.rs"]
mod ownership_tests;

#[path = "local_primitive_relational_typed_unary_boundary_tests.rs"]
mod boundary_tests;
#[path = "local_primitive_relational_decimal_pivot_tests.rs"]
mod decimal_pivot_tests;
#[path = "local_primitive_relational_decimal_rolling_tests.rs"]
mod decimal_rolling_tests;
#[path = "local_primitive_relational_decimal_unary_ownership_tests.rs"]
mod decimal_unary_ownership_tests;

#[cfg(feature = "universal-format-io")]
#[path = "local_primitive_relational_typed_unary_io_tests.rs"]
mod io_tests;

#[cfg(feature = "universal-format-io")]
#[path = "local_primitive_relational_decimal_unary_io_tests.rs"]
mod decimal_unary_io_tests;

const TYPED: [&str; 4] = ["bytes", "decimal", "day", "instant"];
const ALL: [&str; 5] = ["id", "bytes", "decimal", "day", "instant"];

fn source() -> Fixture {
    Fixture::new(source_array(), 3)
}

fn source_array() -> ArrayRef {
    StructArray::new(
        FieldNames::from(ALL),
        vec![
            PrimitiveArray::from_iter(0u64..6).into_array(),
            VarBinArray::from(vec![
                Some(&b"\0\xff"[..]),
                Some(&b""[..]),
                Some(&b"\0\xff"[..]),
                None,
                Some(&b""[..]),
                Some(&b"\x0a\0"[..]),
            ])
            .into_array(),
            DecimalArray::from_option_iter(
                [
                    Some(123i128),
                    Some(-456),
                    Some(123),
                    None,
                    Some(-456),
                    Some(0),
                ],
                DecimalDType::new(20, 2),
            )
            .into_array(),
            ExtensionArray::new(
                Date::new(TimeUnit::Days, Nullability::Nullable).erased(),
                PrimitiveArray::from_option_iter([
                    Some(i32::MIN),
                    Some(i32::MAX),
                    Some(i32::MIN),
                    None,
                    Some(i32::MAX),
                    Some(0),
                ])
                .into_array(),
            )
            .into_array(),
            ExtensionArray::new(
                Timestamp::new(TimeUnit::Microseconds, Nullability::Nullable).erased(),
                PrimitiveArray::from_option_iter([
                    Some(i64::MIN),
                    Some(i64::MAX),
                    Some(i64::MIN),
                    None,
                    Some(i64::MAX),
                    Some(0),
                ])
                .into_array(),
            )
            .into_array(),
        ],
        6,
        Validity::NonNullable,
    )
    .into_array()
}

fn oracle() -> Vec<Value> {
    vec![
        json!({"id":0,"bytes":"00ff","decimal":"decimal128(20,2):123","day":i32::MIN,"instant":i64::MIN}),
        json!({"id":1,"bytes":"","decimal":"decimal128(20,2):-456","day":i32::MAX,"instant":i64::MAX}),
        json!({"id":2,"bytes":"00ff","decimal":"decimal128(20,2):123","day":i32::MIN,"instant":i64::MIN}),
        json!({"id":3,"bytes":null,"decimal":null,"day":null,"instant":null}),
        json!({"id":4,"bytes":"","decimal":"decimal128(20,2):-456","day":i32::MAX,"instant":i64::MAX}),
        json!({"id":5,"bytes":"0a00","decimal":"decimal128(20,2):0","day":0,"instant":0}),
    ]
}

fn columns(names: &[&str]) -> ProjectionRequest {
    ProjectionRequest::columns(
        names
            .iter()
            .map(|name| ColumnRef::new(*name).unwrap())
            .collect(),
    )
}

fn request(kind: Kind, names: &[&str]) -> VortexQueryPrimitiveRequest {
    VortexQueryPrimitiveRequest::for_relational_input(kind, columns(names))
}

fn unary(
    input: VortexRelationalPlan,
    request: VortexQueryPrimitiveRequest,
) -> VortexRelationalPlan {
    VortexRelationalPlan::Unary(Box::new(VortexRelationalUnary { input, request }))
}

fn prepared_composed(
    fixture: &Fixture,
    request: &VortexQueryPrimitiveRequest,
) -> Result<PreparedVortexRelational> {
    let plan = unary(fixture.scan(), request.clone());
    if request.kind == Kind::PivotRows {
        let source = DatasetUri::new(fixture.path().display().to_string()).unwrap();
        prepare_relational_with_dynamic_schema(&[source], policy(), 65_536, move |schemas| {
            schemas.resolve_output(&plan).map(|(resolved, _)| resolved)
        })
    } else {
        prepare_relational(&plan, policy())
    }
}

fn rows(fixture: &Fixture, request: &VortexQueryPrimitiveRequest, direct: bool) -> Vec<Value> {
    if direct {
        let mut request = request.clone();
        request.source_uri = Some(DatasetUri::new(fixture.path().display().to_string()).unwrap());
        let prepared = prepare_unary(&request, policy()).unwrap();
        let before = prepared.snapshot().memory.reserved_bytes;
        let mut expected = None;
        for _ in 0..2 {
            let output = prepared
                .collect_jsonl(&CancellationToken::default())
                .unwrap();
            assert!(output.execution.native_io_certificate.is_certified());
            assert!(
                !output
                    .execution
                    .native_io_certificate
                    .side_effects
                    .fallback_attempted
            );
            let values = output
                .result_jsonl
                .value()
                .lines()
                .map(|line| serde_json::from_str(line).unwrap())
                .collect::<Vec<Value>>();
            if let Some(expected) = &expected {
                assert_eq!(&values, expected);
            }
            expected = Some(values);
            drop(output);
            assert_eq!(prepared.snapshot().memory.reserved_bytes, before);
        }
        return expected.unwrap();
    }
    let prepared = prepared_composed(fixture, request).unwrap();
    let before = prepared.snapshot().memory.reserved_bytes;
    let mut expected = None;
    for _ in 0..2 {
        let output = prepared
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        assert!(output.execution.native_io_certificate.is_certified());
        assert!(
            !output
                .execution
                .native_io_certificate
                .side_effects
                .fallback_attempted
        );
        let values = json_rows(&output);
        if let Some(expected) = &expected {
            assert_eq!(&values, expected);
        }
        expected = Some(values);
        drop(output);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, before);
    }
    expected.unwrap()
}

#[test]
fn typed_unary_selector_keys_preserve_full_values_and_all_keep_policies() {
    let fixture = source();
    let expected = oracle();
    for direct in [true, false] {
        for key in TYPED {
            for (keep, indices, mask) in [
                (
                    Keep::First,
                    vec![0, 1, 3, 5],
                    vec![false, false, true, false, true, false],
                ),
                (
                    Keep::Last,
                    vec![2, 3, 4, 5],
                    vec![true, true, false, false, false, false],
                ),
                (
                    Keep::AllDuplicates,
                    vec![3, 5],
                    vec![true, true, true, false, true, false],
                ),
            ] {
                let mut req = request(Kind::DropDuplicateRows, &ALL);
                req.deduplicate_key_projection = Some(columns(&[key]));
                req.duplicate_keep = keep;
                assert_eq!(
                    rows(&fixture, &req, direct),
                    indices
                        .iter()
                        .map(|i| expected[*i].clone())
                        .collect::<Vec<_>>()
                );
                let mut req = request(Kind::DuplicateMaskRows, &[key]);
                req.duplicate_keep = keep;
                assert_eq!(
                    rows(&fixture, &req, direct),
                    mask.iter()
                        .map(|v| json!({"duplicated":v}))
                        .collect::<Vec<_>>()
                );
            }
        }
        let req = request(Kind::DistinctRows, &TYPED);
        let wanted = [0, 1, 3, 5].map(|index| {
            let mut value = expected[index].clone();
            value.as_object_mut().unwrap().remove("id");
            value
        });
        assert_eq!(rows(&fixture, &req, direct), wanted);
    }
}

#[test]
fn typed_unary_tail_and_sampling_retain_typed_selected_payloads() {
    let fixture = source();
    for direct in [true, false] {
        let mut tail = request(Kind::TailRows, &ALL);
        tail.source_order_limit = Some(3);
        assert_eq!(rows(&fixture, &tail, direct), oracle()[3..]);
        let mut sample = request(Kind::SampleRows, &ALL);
        sample.source_order_limit = Some(6);
        sample.sample_seed = Some(7);
        assert_eq!(rows(&fixture, &sample, direct), oracle());
    }
}

#[test]
fn typed_unary_forward_fill_crosses_batches_and_preserves_logical_domains() {
    let fixture = source();
    let mut expected = oracle();
    for name in TYPED {
        expected[3][name] = expected[2][name].clone();
    }
    let mut req = request(Kind::ExpressionProjectRows, &ALL);
    req.expression_projection = Some(VortexExpressionProjectionRequest::new(
        TYPED
            .iter()
            .map(|name| Rewrite::ForwardFillNull {
                target_column: ColumnRef::new(*name).unwrap(),
                limit: Some(1),
            })
            .collect(),
    ));
    for direct in [true, false] {
        assert_eq!(rows(&fixture, &req, direct), expected);
    }
}

#[test]
fn typed_unary_melt_and_rolling_count_use_the_declared_scalar_domain() {
    let fixture = source();
    for key in TYPED {
        let mut melt = request(Kind::MeltRows, &["id", key]);
        melt.melt_projection = Some(VortexMeltProjectionRequest::new(
            vec![ColumnRef::new("id").unwrap()],
            vec![ColumnRef::new(key).unwrap()],
            "variable".into(),
            "value".into(),
        ));
        let expected = oracle()
            .iter()
            .map(|row| json!({"id":row["id"],"variable":key,"value":row[key]}))
            .collect::<Vec<_>>();
        for direct in [true, false] {
            assert_eq!(rows(&fixture, &melt, direct), expected);
            let mut count = request(Kind::RollingWindowRows, &[key]);
            count.rolling_window = Some(VortexRollingWindowRequest::new(
                ColumnRef::new(key).unwrap(),
                "valid".into(),
                3,
                1,
                "count".into(),
            ));
            assert_eq!(
                rows(&fixture, &count, direct),
                [1, 2, 3, 2, 2, 2].map(|n| json!({"valid":n}))
            );
        }
    }
}

fn decimal(value: i128, precision: u8, scale: u8) -> ScalarValue {
    ScalarValue::Decimal128 {
        value,
        precision,
        scale,
    }
}

fn rewritten(rewrites: Vec<Rewrite>) -> VortexQueryPrimitiveRequest {
    let mut req = request(Kind::ExpressionProjectRows, &ALL);
    req.expression_projection = Some(VortexExpressionProjectionRequest::new(rewrites));
    req
}

#[test]
fn typed_unary_replacement_and_mask_bind_exact_literals_and_preserve_other_columns() {
    let fixture = source();
    let cases = [
        (
            "bytes",
            ScalarValue::Binary(vec![0, 255]),
            ScalarValue::Binary(vec![10, 254]),
            json!("0afe"),
        ),
        (
            "decimal",
            decimal(1230, 10, 3),
            decimal(9870, 10, 3),
            json!("decimal128(20,2):987"),
        ),
        (
            "day",
            ScalarValue::Date32(i32::MIN),
            ScalarValue::Date32(7),
            json!(7),
        ),
        (
            "instant",
            ScalarValue::TimestampMicros(i64::MIN),
            ScalarValue::TimestampMicros(17),
            json!(17),
        ),
    ];
    let mut rewrites = Vec::new();
    let mut expected = oracle();
    for (name, original, replacement, result) in cases {
        rewrites.push(Rewrite::ReplaceScalar {
            target_column: ColumnRef::new(name).unwrap(),
            to_replace: original,
            replacement: replacement.clone(),
        });
        rewrites.push(Rewrite::MaskScalar {
            target_column: ColumnRef::new(name).unwrap(),
            predicate: PredicateExpr::Compare {
                column: ColumnRef::new("id").unwrap(),
                op: ComparisonOp::Eq,
                value: StatValue::UInt64(3),
            },
            replacement,
        });
        for row in [0, 2, 3] {
            expected[row][name] = result.clone();
        }
    }
    let req = rewritten(rewrites);
    for direct in [true, false] {
        assert_eq!(rows(&fixture, &req, direct), expected);
    }
}

#[test]
fn typed_unary_decimal_arithmetic_uses_checked_native_precision_and_null_rules() {
    let fixture = source();
    for (operator, precision, scale, values) in [
        (
            "+",
            21,
            2,
            [
                Some(243),
                Some(-336),
                Some(243),
                None,
                Some(-336),
                Some(120),
            ],
        ),
        (
            "-",
            21,
            2,
            [Some(3), Some(-576), Some(3), None, Some(-576), Some(-120)],
        ),
        (
            "*",
            23,
            4,
            [
                Some(14760),
                Some(-54720),
                Some(14760),
                None,
                Some(-54720),
                Some(0),
            ],
        ),
        (
            "/",
            38,
            6,
            [
                Some(1_025_000),
                Some(-3_800_000),
                Some(1_025_000),
                None,
                Some(-3_800_000),
                Some(0),
            ],
        ),
    ] {
        let req = rewritten(vec![Rewrite::NumericScalarArithmetic {
            target_column: ColumnRef::new("decimal").unwrap(),
            operator: operator.into(),
            operand: decimal(120, 3, 2),
        }]);
        let mut expected = oracle();
        for (row, value) in expected.iter_mut().zip(values) {
            row["decimal"] = value.map_or(Value::Null, |value| {
                json!(format!("decimal128({precision},{scale}):{value}"))
            });
        }
        for direct in [true, false] {
            assert_eq!(rows(&fixture, &req, direct), expected, "{operator}");
        }
    }
}

#[test]
fn typed_unary_melt_rescales_decimal_and_integer_values_without_losing_typed_ids() {
    let fixture = source();
    let mut req = request(Kind::MeltRows, &["bytes", "decimal", "id"]);
    req.melt_projection = Some(VortexMeltProjectionRequest::new(
        vec![ColumnRef::new("bytes").unwrap()],
        vec![
            ColumnRef::new("decimal").unwrap(),
            ColumnRef::new("id").unwrap(),
        ],
        "variable".into(),
        "value".into(),
    ));
    let amounts = [Some(123), Some(-456), Some(123), None, Some(-456), Some(0)];
    let mut expected = Vec::new();
    for (index, row) in oracle().iter().enumerate() {
        expected.push(json!({"bytes":row["bytes"],"variable":"decimal","value":
            amounts[index].map(|value| format!("decimal128(22,2):{value}"))}));
        expected.push(json!({"bytes":row["bytes"],"variable":"id","value":
            format!("decimal128(22,2):{}", index * 100)}));
    }
    for direct in [true, false] {
        assert_eq!(rows(&fixture, &req, direct), expected);
    }
}

fn pivot(index: &str, domain: &str, value: &str, aggregate: &str) -> VortexQueryPrimitiveRequest {
    let mut req = request(Kind::PivotRows, &[index, domain, value]);
    req.pivot_projection = Some(
        VortexPivotProjectionRequest::new(
            ColumnRef::new(index).unwrap(),
            ColumnRef::new(domain).unwrap(),
            ColumnRef::new(value).unwrap(),
            aggregate,
        )
        .with_output_policy(None, false, false, "All"),
    );
    req
}

#[test]
fn typed_unary_pivot_preserves_typed_indices_cells_and_null_domains() {
    let fixture = source();
    let original = oracle();
    for (key, order) in [
        ("bytes", [1, 0, 5, 3]),
        ("decimal", [1, 5, 0, 3]),
        ("day", [0, 5, 1, 3]),
        ("instant", [3, 0, 5, 1]),
    ] {
        for aggregate in ["first", "first_unique", "count"] {
            let req = pivot(key, "id", key, aggregate);
            let expected = order.map(|index| {
                let mut row = serde_json::Map::new();
                row.insert(key.into(), original[index][key].clone());
                for (source, value) in original.iter().enumerate() {
                    row.insert(
                        format!("pivot_{source}"),
                        if value[key] == original[index][key] {
                            if aggregate == "count" {
                                json!(1)
                            } else {
                                value[key].clone()
                            }
                        } else {
                            Value::Null
                        },
                    );
                }
                Value::Object(row)
            });
            for direct in [true, false] {
                assert_eq!(rows(&fixture, &req, direct), expected, "{key}/{aggregate}");
            }
        }
    }
    let req = pivot("id", "bytes", "decimal", "first_unique");
    let expected = original
        .iter()
        .enumerate()
        .map(|(index, row)| {
            let mut value = json!({"id":index,"pivot_binary":null,"pivot_binary_00ff":null,
                "pivot_binary_0a00":null,"pivot_value":null});
            let field = match index {
                0 | 2 => "pivot_binary_00ff",
                1 | 4 => "pivot_binary",
                3 => "pivot_value",
                _ => "pivot_binary_0a00",
            };
            value[field] = row["decimal"].clone();
            value
        })
        .collect::<Vec<_>>();
    for direct in [true, false] {
        assert_eq!(rows(&fixture, &req, direct), expected);
    }
}

fn denied(fixture: &Fixture, request: &VortexQueryPrimitiveRequest, direct: bool) -> String {
    let error = if direct {
        let mut request = request.clone();
        request.source_uri = Some(DatasetUri::new(fixture.path().display().to_string()).unwrap());
        match prepare_unary(&request, policy()) {
            Err(error) => error,
            Ok(prepared) => prepared
                .collect_jsonl(&CancellationToken::default())
                .err()
                .expect("unsupported unary must fail"),
        }
    } else {
        match prepared_composed(fixture, request) {
            Err(error) => error,
            Ok(prepared) => prepared
                .collect_jsonl(&CancellationToken::default())
                .err()
                .expect("unsupported composed unary must fail"),
        }
    };
    error.to_string()
}

#[test]
fn typed_unary_empty_and_populated_plans_reject_incompatible_domains() {
    let full = source();
    let empty = Fixture::new(source_array().slice(0..0).unwrap(), 1);
    let mut requests = vec![
        rewritten(vec![Rewrite::ReplaceScalar {
            target_column: ColumnRef::new("day").unwrap(),
            to_replace: ScalarValue::Int64(0),
            replacement: ScalarValue::Int64(1),
        }]),
        rewritten(vec![Rewrite::ReplaceScalar {
            target_column: ColumnRef::new("decimal").unwrap(),
            to_replace: decimal(123, 20, 2),
            replacement: decimal(1231, 10, 3),
        }]),
        rewritten(vec![Rewrite::NumericScalarArithmetic {
            target_column: ColumnRef::new("decimal").unwrap(),
            operator: "*".into(),
            operand: decimal(1, 38, 0),
        }]),
        pivot("bytes", "id", "day", "sum"),
    ];
    let mut rolling = request(Kind::RollingWindowRows, &["day"]);
    rolling.rolling_window = Some(VortexRollingWindowRequest::new(
        ColumnRef::new("day").unwrap(),
        "total".into(),
        2,
        1,
        "sum".into(),
    ));
    requests.push(rolling);
    let mut melt = request(Kind::MeltRows, &["id", "day"]);
    melt.melt_projection = Some(VortexMeltProjectionRequest::new(
        vec![],
        vec![
            ColumnRef::new("id").unwrap(),
            ColumnRef::new("day").unwrap(),
        ],
        "variable".into(),
        "value".into(),
    ));
    requests.push(melt);
    let mut sample = request(Kind::SampleRows, &ALL);
    sample.source_order_limit = Some(1);
    sample.sample_weight_column = Some(ColumnRef::new("decimal").unwrap());
    requests.push(sample);
    let mut predicate = request(Kind::TailRows, &ALL);
    predicate.source_order_limit = Some(1);
    predicate.predicate = Some(PredicateExpr::IsNull {
        column: ColumnRef::new("day").unwrap(),
    });
    requests.push(predicate);
    for (index, req) in requests.iter().enumerate() {
        for direct in [true, false] {
            let full_error = denied(&full, req, direct);
            let empty_error = denied(&empty, req, direct);
            assert_eq!(full_error, empty_error, "request {index}/{direct}");
            assert!(empty_error.contains("no fallback"), "{empty_error}");
        }
    }
}
