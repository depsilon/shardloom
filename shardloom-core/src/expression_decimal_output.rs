//! Explicit decimal declarations and reference evaluation, using the shared
//! integer-ratio semantic owner rather than the existing default arithmetic.

use super::{
    BinaryOp, Decimal128Operand, EvalFailure, EvalResult, EvalValue, Expression,
    ExpressionInputRow, ExpressionKind, LogicalDType, NullBehavior, ScalarValue,
    decimal_eval_value, decimal128_dtype, decimal128_dtype_parts, decimal128_operand_from_scalar,
    eval_function_args,
};
use crate::decimal_output::{DecimalOutput, DecimalRounding};

pub(super) fn declaration(
    name: &str,
    args: &[Expression],
) -> EvalResult<(Option<BinaryOp>, DecimalOutput, usize)> {
    let operation = match name {
        "decimal_rescale" => None,
        "decimal_add" => Some(BinaryOp::Add),
        "decimal_subtract" => Some(BinaryOp::Subtract),
        "decimal_multiply" => Some(BinaryOp::Multiply),
        "decimal_divide" => Some(BinaryOp::Divide),
        _ => {
            return Err(EvalFailure::invalid(
                "decimal_output",
                "unknown decimal operation",
            ));
        }
    };
    let count = if operation.is_some() { 2 } else { 1 };
    if args.len() != count + 3 {
        return Err(EvalFailure::invalid(
            "decimal_output",
            "decimal operation requires value operands and literal precision, scale and rounding",
        ));
    }
    let control = |index: usize| match &args[index].kind {
        ExpressionKind::Literal(value) => Ok(value),
        _ => Err(EvalFailure::invalid(
            "decimal_output",
            "decimal output controls must be literals",
        )),
    };
    let integer = |value: &ScalarValue| {
        match value {
            ScalarValue::Int64(value) => u8::try_from(*value).ok(),
            ScalarValue::UInt64(value) => u8::try_from(*value).ok(),
            _ => None,
        }
        .ok_or_else(|| {
            EvalFailure::invalid(
                "decimal_output",
                "decimal precision and scale require integer literals in 0..38",
            )
        })
    };
    let ScalarValue::Utf8(rounding) = control(count + 2)? else {
        return Err(EvalFailure::invalid(
            "decimal_output",
            "decimal rounding requires a nonnull UTF8 literal",
        ));
    };
    let output = DecimalOutput::new(
        integer(control(count)?)?,
        integer(control(count + 1)?)?,
        DecimalRounding::parse(rounding)
            .map_err(|error| EvalFailure::invalid("decimal_output", error.to_string()))?,
    )
    .map_err(|error| EvalFailure::invalid("decimal_output", error.to_string()))?;
    Ok((operation, output, count))
}

pub(super) fn validate_dtype(dtype: &LogicalDType) -> EvalResult<()> {
    if let Some((precision, scale)) = decimal128_dtype_parts(dtype) {
        Decimal128Operand::decimal(0, precision, scale)
            .map(|_| ())
            .map_err(|error| EvalFailure::invalid("decimal_output", error.to_string()))
    } else if matches!(
        dtype,
        LogicalDType::Unknown | LogicalDType::Int64 | LogicalDType::UInt64
    ) {
        Ok(())
    } else {
        Err(EvalFailure::unsupported(
            "decimal_output",
            "explicit decimal operations require decimal or integer operands",
        ))
    }
}

pub(super) fn evaluate(
    name: &str,
    args: &[Expression],
    row: &ExpressionInputRow,
) -> EvalResult<EvalValue> {
    let (operation, output, count) = declaration(name, args)?;
    let (values, materialized) = eval_function_args(&args[..count], row)?;
    for value in &values {
        validate_dtype(&value.dtype())?;
    }
    if values.iter().any(ScalarValue::is_null) {
        return Ok(EvalValue::null(
            decimal128_dtype(output.precision(), output.scale()),
            NullBehavior::NullPropagating,
        )
        .carry_materialization(materialized));
    }
    let operand = |value: &ScalarValue| {
        decimal128_operand_from_scalar(value)?.ok_or_else(|| {
            EvalFailure::unsupported(
                "decimal_output",
                "explicit decimal operations require decimal or integer operands",
            )
        })
    };
    let left = operand(&values[0])?;
    let result = if let Some(operation) = operation {
        output.binary(left, operation, operand(&values[1])?)
    } else {
        output.rescale(left)
    };
    result
        .map(|value| decimal_eval_value(value).carry_materialization(materialized))
        .map_err(|error| EvalFailure::invalid("decimal_output", error.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ExprId, ExpressionEvaluationStatus, evaluate_expression};

    fn literal(value: ScalarValue) -> Expression {
        Expression::literal(ExprId::new("decimal-value").unwrap(), value)
    }

    #[test]
    fn decimal_output_reference_binding_preserves_domain_nulls_and_denials() {
        let input = ScalarValue::Decimal128 {
            value: -125,
            precision: 3,
            scale: 2,
        };
        for (name, right, mode, expected) in [
            ("decimal_rescale", None, "half_even", -12),
            ("decimal_rescale", None, "half_away", -13),
            ("decimal_add", Some(2), "half_even", 8),
            ("decimal_subtract", Some(2), "half_even", -32),
            ("decimal_multiply", Some(2), "exact", -25),
            ("decimal_divide", Some(2), "floor", -7),
        ] {
            for value in [input.clone(), ScalarValue::Null] {
                let is_null = value.is_null();
                let mut args = vec![literal(value)];
                if let Some(right) = right {
                    args.push(literal(ScalarValue::Int64(right)));
                }
                args.extend([
                    literal(ScalarValue::Int64(4)),
                    literal(ScalarValue::Int64(1)),
                    literal(ScalarValue::Utf8(mode.into())),
                ]);
                let expression = Expression::new(
                    ExprId::new("decimal-call").unwrap(),
                    ExpressionKind::FunctionCall {
                        name: name.into(),
                        args: args.clone(),
                    },
                );
                let report = evaluate_expression(&expression, &ExpressionInputRow::new());
                assert_eq!(
                    report.status,
                    ExpressionEvaluationStatus::Evaluated,
                    "{report:?}"
                );
                assert_eq!(report.output_dtype, Some(decimal128_dtype(4, 1)));
                assert_eq!(
                    report.value,
                    Some(if is_null {
                        ScalarValue::Null
                    } else {
                        ScalarValue::Decimal128 {
                            value: expected,
                            precision: 4,
                            scale: 1,
                        }
                    })
                );
                assert!(!report.fallback_attempted && !report.external_engine_invoked);
                // Invalid controls are not erased by a NULL operand.
                let last = args.len() - 1;
                args[last] = literal(ScalarValue::Utf8("unknown".into()));
                assert!(evaluate(name, &args, &ExpressionInputRow::new()).is_err());
            }
        }
    }
}
