use super::*;

#[test]
fn typed_decimal_branch_domains_have_fixed_lossless_common_types() {
    for (left, right, expected) in [
        ((10, 2), (3, 2), (10, 2)),
        ((10, 2), (3, 3), (11, 3)),
        ((10, 2), (9, 0), (11, 2)),
        ((38, 38), (1, 1), (38, 38)),
        ((38, 0), (1, 0), (38, 0)),
    ] {
        let left = Decimal128Operand::decimal(0, left.0, left.1).unwrap();
        let right = Decimal128Operand::decimal(0, right.0, right.1).unwrap();
        assert_eq!(left.common_type(right).unwrap(), expected);
        assert_eq!(right.common_type(left).unwrap(), expected);
    }
    let integer_domain = Decimal128Operand::decimal(0, 38, 0).unwrap();
    let fractional_domain = Decimal128Operand::decimal(0, 38, 38).unwrap();
    assert!(integer_domain.common_type(fractional_domain).is_err());
    assert!(fractional_domain.common_type(integer_domain).is_err());
}

#[test]
fn typed_casts_check_unsigned_and_signed_numeric_domains() {
    use LogicalDType::{Float64 as F64, Int64 as I64, UInt64 as U64};
    use ScalarValue::{Float64 as F, Int64 as I, UInt64 as U, Utf8 as S};
    for (source, target, expected) in [
        (S("5".into()), U64, Some(U(5))),
        (S(u64::MAX.to_string()), U64, Some(U(u64::MAX))),
        (S("18446744073709551616".into()), U64, None),
        (S("-1".into()), U64, None),
        (S(" 5".into()), U64, None),
        (S("1.5".into()), U64, None),
        (S("bad".into()), U64, None),
        (I(0), U64, Some(U(0))),
        (I(i64::MAX), U64, Some(U(9_223_372_036_854_775_807))),
        (I(-1), U64, None),
        (I(i64::MIN), U64, None),
        (U(u64::MAX), U64, Some(U(u64::MAX))),
        (U(5), I64, Some(I(5))),
        (U(9_223_372_036_854_775_807), I64, Some(I(i64::MAX))),
        (U(9_223_372_036_854_775_808), I64, None),
        (U(u64::MAX), I64, None),
        (U(5), F64, Some(F(5.0))),
        (U(u64::MAX), F64, Some(F(18_446_744_073_709_551_616.0))),
        (F(-0.0), U64, Some(U(0))),
        (F(5.0), U64, Some(U(5))),
        (
            F(18_446_744_073_709_549_568.0),
            U64,
            Some(U(18_446_744_073_709_549_568)),
        ),
        (F(18_446_744_073_709_551_616.0), U64, None),
        (F(-1.0), U64, None),
        (F(1.5), U64, None),
        (F(-9_223_372_036_854_775_808.0), I64, Some(I(i64::MIN))),
        (
            F(9_223_372_036_854_774_784.0),
            I64,
            Some(I(9_223_372_036_854_774_784)),
        ),
        (F(9_223_372_036_854_775_808.0), I64, None),
        (F(-9_223_372_036_854_777_856.0), I64, None),
        (F(1.5), I64, None),
        (ScalarValue::Null, U64, Some(ScalarValue::Null)),
    ] {
        for tolerant in [false, true] {
            let input = Expression::literal(ExprId::new("source").unwrap(), source.clone());
            let id = ExprId::new("numeric-cast").unwrap();
            let expression = if tolerant {
                Expression::try_cast(id, input, target.clone())
            } else {
                Expression::cast(id, input, target.clone())
            };
            let report = evaluate_expression(&expression, &ExpressionInputRow::new());
            if let Some(expected) = &expected {
                assert_eq!(
                    report.status,
                    ExpressionEvaluationStatus::Evaluated,
                    "{expression:?}"
                );
                assert_eq!(report.value.as_ref(), Some(expected), "{expression:?}");
                assert_eq!(report.output_dtype.as_ref(), Some(&target));
            } else if tolerant {
                assert_eq!(
                    report.status,
                    ExpressionEvaluationStatus::Evaluated,
                    "{expression:?}"
                );
                assert_eq!(report.value, Some(ScalarValue::Null));
                assert_eq!(report.output_dtype.as_ref(), Some(&target));
            } else {
                assert_eq!(
                    report.status,
                    ExpressionEvaluationStatus::InvalidInput,
                    "{expression:?}"
                );
                assert!(report.has_errors());
            }
            assert!(!report.fallback_attempted);
            assert!(!report.external_engine_invoked);
        }
    }
    for value in [
        ScalarValue::Boolean(true),
        ScalarValue::Binary(vec![5]),
        ScalarValue::Date32(5),
        F(f64::NAN),
        F(f64::INFINITY),
        F(f64::NEG_INFINITY),
    ] {
        let expression = Expression::try_cast(
            ExprId::new("unsupported-cast").unwrap(),
            Expression::literal(ExprId::new("value").unwrap(), value),
            U64,
        );
        assert_eq!(
            evaluate_expression(&expression, &ExpressionInputRow::new()).status,
            ExpressionEvaluationStatus::Unsupported
        );
    }
}

#[test]
fn typed_decimal_arithmetic_binds_before_values_and_checks_exact_results() {
    let dec = |value| Decimal128Operand::decimal(value, 10, 2).unwrap();
    let integer = |value| Decimal128Operand::integer(value, 19).unwrap();
    for (op, expected_type, value) in [
        (BinaryOp::Add, (22, 2), 1434),
        (BinaryOp::Subtract, (22, 2), 1034),
        (BinaryOp::Multiply, (29, 2), 2468),
        (BinaryOp::Divide, (38, 6), 6_170_000),
    ] {
        assert_eq!(
            dec(0).arithmetic_type(op, integer(0)).unwrap(),
            expected_type
        );
        let result = dec(1234).checked_binary(op, integer(2)).unwrap();
        assert_eq!(result.precision_scale(), expected_type);
        assert_eq!(result.value(), value);
    }
    assert!(
        dec(100)
            .checked_binary(BinaryOp::Divide, integer(3))
            .is_err()
    );
    assert!(
        dec(100)
            .checked_binary(BinaryOp::Divide, integer(0))
            .is_err()
    );
    assert!(
        dec(0)
            .arithmetic_type(BinaryOp::Add, Decimal128Operand::decimal(0, 10, 3).unwrap())
            .is_err()
    );
    assert!(
        Decimal128Operand::decimal(0, 38, 2)
            .unwrap()
            .arithmetic_type(BinaryOp::Add, dec(0))
            .is_err()
    );
    for (value, precision, scale) in [
        (i128::MIN, 38, 0),
        (100, 2, 0),
        (0, 0, 0),
        (0, 39, 0),
        (0, 2, 3),
    ] {
        assert!(Decimal128Operand::decimal(value, precision, scale).is_err());
    }
}

#[test]
fn typed_decimal_rounding_and_rescaling_are_exact() {
    for (value, floor, ceil, rounded) in [
        (-199, -2, -1, -2),
        (-150, -2, -1, -2),
        (-149, -2, -1, -1),
        (-1, -1, 0, 0),
        (0, 0, 0, 0),
        (1, 0, 1, 0),
        (149, 1, 2, 1),
        (150, 1, 2, 2),
        (199, 1, 2, 2),
        (9999, 99, 100, 100),
    ] {
        let operand = Decimal128Operand::decimal(value, 4, 2).unwrap();
        assert_eq!(operand.floor().value(), floor);
        assert_eq!(operand.ceil().value(), ceil);
        assert_eq!(operand.round().value(), rounded);
        assert_eq!(operand.round().precision_scale(), (3, 0));
        assert_eq!(operand.abs().value(), value.abs());
        assert_eq!(operand.negate().value(), -value);
    }
    let exact = Decimal128Operand::decimal(-12300, 5, 3)
        .unwrap()
        .rescale(4, 2)
        .unwrap();
    assert_eq!((exact.value(), exact.precision_scale()), (-1230, (4, 2)));
    assert!(
        Decimal128Operand::decimal(12301, 5, 3)
            .unwrap()
            .rescale(4, 2)
            .is_err()
    );
    assert!(
        Decimal128Operand::decimal(12300, 5, 3)
            .unwrap()
            .rescale(3, 2)
            .is_err()
    );
    assert_eq!(
        Decimal128Operand::decimal(9, 1, 0).unwrap().round().value(),
        9
    );
}

#[test]
fn typed_calendar_checks_final_range_after_wider_intermediates() {
    assert_eq!(date32_add_days(i32::MIN, 4_294_967_295).unwrap(), i32::MAX);
    assert_eq!(date32_add_days(i32::MAX, -4_294_967_295).unwrap(), i32::MIN);
    assert!(date32_add_days(i32::MAX, 1).is_err());
    assert!(date32_add_days(1, i128::MAX).is_err());
    assert_eq!(
        timestamp_micros_add_seconds(i64::MIN, 18_446_744_073_709).unwrap(),
        9_223_372_036_854_224_192
    );
    assert!(timestamp_micros_add_seconds(i64::MAX, 1).is_err());
    assert!(timestamp_micros_add_seconds(0, i128::MAX).is_err());
    assert_eq!(
        timestamp_micros_diff_seconds(i64::MAX, i64::MIN),
        18_446_744_073_709
    );
    assert_eq!(
        timestamp_micros_diff_seconds(i64::MIN, i64::MAX),
        -18_446_744_073_709
    );
    assert_eq!(timestamp_micros_diff_seconds(-1, 0), 0);
    assert_eq!(timestamp_micros_diff_seconds(-1_000_001, 0), -1);
}

#[test]
fn typed_calendar_reference_and_native_helpers_admit_unsigned_offsets() {
    for (name, value, offset, expected) in [
        (
            "date_add_days",
            ScalarValue::Date32(i32::MIN),
            4_294_967_295,
            ScalarValue::Date32(i32::MAX),
        ),
        (
            "date_sub_days",
            ScalarValue::Date32(i32::MAX),
            4_294_967_295,
            ScalarValue::Date32(i32::MIN),
        ),
        (
            "timestamp_add_seconds",
            ScalarValue::TimestampMicros(i64::MIN),
            18_446_744_073_709,
            ScalarValue::TimestampMicros(9_223_372_036_854_224_192),
        ),
    ] {
        for offset in [offset, u64::MAX] {
            let expression = Expression::new(
                ExprId::new("calendar").unwrap(),
                ExpressionKind::FunctionCall {
                    name: name.into(),
                    args: vec![
                        Expression::literal(ExprId::new("value").unwrap(), value.clone()),
                        Expression::literal(
                            ExprId::new("offset").unwrap(),
                            ScalarValue::UInt64(offset),
                        ),
                    ],
                },
            );
            let report = evaluate_expression(&expression, &ExpressionInputRow::new());
            if offset == u64::MAX {
                assert_eq!(report.status, ExpressionEvaluationStatus::InvalidInput);
            } else {
                assert_eq!(report.status, ExpressionEvaluationStatus::Evaluated);
                assert_eq!(report.value, Some(expected.clone()));
            }
            assert!(!report.fallback_attempted);
            assert!(!report.external_engine_invoked);
        }
    }
}

#[test]
fn typed_calendar_formats_full_date_storage() {
    // Expected boundary dates independently use 400-year Gregorian cycles
    // around the standard 1970 epoch, not this module's conversion algorithm.
    for (days, expected, year, month, day) in [
        (i32::MIN, "-5877641-06-23", -5_877_641, 6, 23),
        (i32::MIN + 1, "-5877641-06-24", -5_877_641, 6, 24),
        (-1, "1969-12-31", 1969, 12, 31),
        (0, "1970-01-01", 1970, 1, 1),
        (i32::MAX - 1, "5881580-07-10", 5_881_580, 7, 10),
        (i32::MAX, "5881580-07-11", 5_881_580, 7, 11),
    ] {
        assert_eq!(format_iso_date32(days), expected);
        assert_eq!(date32_year(days), year);
        assert_eq!(date32_month(days), month);
        assert_eq!(date32_day(days), day);
    }
}

#[test]
fn typed_date_to_timestamp_range_is_checked() {
    for days in [
        i32::MIN,
        -106_751_992,
        -106_751_991,
        0,
        106_751_991,
        106_751_992,
        i32::MAX,
    ] {
        let value = Expression::literal(
            ExprId::new("date-value").unwrap(),
            ScalarValue::Date32(days),
        );
        let expected = i64::from(days).checked_mul(86_400_000_000);
        for tolerant in [false, true] {
            let id = ExprId::new("checked-date-cast").unwrap();
            let cast = if tolerant {
                Expression::try_cast(id, value.clone(), LogicalDType::TimestampMicros)
            } else {
                Expression::cast(id, value.clone(), LogicalDType::TimestampMicros)
            };
            let report = evaluate_expression(&cast, &ExpressionInputRow::new());
            match expected {
                Some(micros) => {
                    assert_eq!(report.status, ExpressionEvaluationStatus::Evaluated);
                    assert_eq!(report.value, Some(ScalarValue::TimestampMicros(micros)));
                }
                None if tolerant => {
                    assert_eq!(report.status, ExpressionEvaluationStatus::Evaluated);
                    assert_eq!(report.value, Some(ScalarValue::Null));
                    assert_eq!(report.output_dtype, Some(LogicalDType::TimestampMicros));
                }
                None => assert_eq!(report.status, ExpressionEvaluationStatus::InvalidInput),
            }
            assert!(!report.fallback_attempted);
            assert!(!report.external_engine_invoked);
        }
    }
}
