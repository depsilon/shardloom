//! Result-type resolution for lazy decoded-reference branches. This never
//! evaluates a value, cast, function or unused branch and is not a native kernel.

use super::{
    BinaryOp, Decimal128Operand, EvalFailure, EvalResult, EvalValue, Expression,
    ExpressionInputRow, ExpressionKind, LogicalDType, NullBehavior, ScalarValue, UnaryOp,
    decimal128_checked_operand, decimal128_dtype, decimal128_dtype_parts, numeric_output_dtype,
};

pub(super) fn conditional(
    name: &str,
    expressions: &[Expression],
    row: &ExpressionInputRow,
) -> EvalResult<LogicalDType> {
    let mut resolver = Resolver {
        row,
        remaining: 4095,
    };
    resolver.function(name, expressions, 0)
}

fn common_dtype(left: &LogicalDType, right: &LogicalDType) -> EvalResult<LogicalDType> {
    if left == &LogicalDType::Unknown {
        return Ok(right.clone());
    }
    if right == &LogicalDType::Unknown || left == right {
        return Ok(left.clone());
    }
    if let (Some((lp, ls)), Some((rp, rs))) =
        (decimal128_dtype_parts(left), decimal128_dtype_parts(right))
    {
        let (precision, scale) = Decimal128Operand::decimal(0, lp, ls)
            .and_then(|left| left.common_type(Decimal128Operand::decimal(0, rp, rs)?))
            .map_err(|error| EvalFailure::unsupported("conditional_type", error.to_string()))?;
        return Ok(decimal128_dtype(precision, scale));
    }
    Err(EvalFailure::unsupported(
        "conditional_type",
        format!(
            "branches require a lossless common scalar type, got {} and {}",
            left.as_str(),
            right.as_str()
        ),
    ))
}

pub(super) fn cast_type(source: &LogicalDType, target: &LogicalDType) -> EvalResult<()> {
    use LogicalDType::{
        Binary, Boolean, Date32, Float64, Int64, TimestampMicros, UInt64, Unknown, Utf8,
    };
    let decimal = |dtype: &LogicalDType| decimal128_dtype_parts(dtype).is_some();
    let numeric =
        |dtype: &LogicalDType| matches!(dtype, Int64 | UInt64 | Float64) || decimal(dtype);
    let target_admitted = matches!(
        target,
        Boolean | Int64 | UInt64 | Float64 | Utf8 | Binary | Date32 | TimestampMicros
    ) || decimal(target);
    let admitted = target_admitted
        && match source {
            Unknown | Utf8 => true,
            Boolean => matches!(target, Boolean | Utf8 | Binary),
            Binary => matches!(target, Utf8 | Binary),
            Date32 | TimestampMicros => matches!(target, Date32 | TimestampMicros | Utf8 | Binary),
            source if numeric(source) => numeric(target) || matches!(target, Utf8 | Binary),
            _ => false,
        };
    if admitted {
        Ok(())
    } else {
        Err(EvalFailure::unsupported(
            "cast",
            format!(
                "cast from {} to {} is not admitted",
                source.as_str(),
                target.as_str()
            ),
        ))
    }
}

struct Resolver<'a> {
    row: &'a ExpressionInputRow,
    remaining: usize,
}

impl Resolver<'_> {
    fn dtype(&mut self, expression: &Expression, depth: usize) -> EvalResult<LogicalDType> {
        if depth > 24 || self.remaining == 0 {
            return Err(EvalFailure::unsupported(
                "conditional_type",
                "scalar expressions exceed 24 levels or 4096 nodes",
            ));
        }
        self.remaining -= 1;
        match &expression.kind {
            ExpressionKind::Literal(value) => {
                match value {
                    ScalarValue::Decimal128 {
                        value,
                        precision,
                        scale,
                    } => {
                        decimal128_checked_operand(*value, *precision, *scale)?;
                    }
                    ScalarValue::Float64(value) if !value.is_finite() => {
                        return Err(EvalFailure::unsupported(
                            "literal",
                            "nonfinite scalar values are not admitted",
                        ));
                    }
                    _ => {}
                }
                Ok(value.dtype())
            }
            ExpressionKind::Column(column) => {
                if let Some(dtype) = &expression.dtype {
                    return Ok(dtype.clone());
                }
                self.row
                    .get(column.as_str())
                    .map(ScalarValue::dtype)
                    .ok_or_else(|| {
                        EvalFailure::invalid(
                            "column_reference",
                            format!(
                                "column {:?} is absent and has no declared dtype",
                                column.as_str()
                            ),
                        )
                    })
            }
            ExpressionKind::Alias { expr, .. } => self.dtype(expr, depth + 1),
            ExpressionKind::Cast { expr, target_dtype }
            | ExpressionKind::TryCast { expr, target_dtype } => {
                cast_type(&self.dtype(expr, depth + 1)?, target_dtype)?;
                Ok(target_dtype.clone())
            }
            ExpressionKind::Unary { op, expr } => {
                let dtype = self.dtype(expr, depth + 1)?;
                match op {
                    UnaryOp::IsNull | UnaryOp::IsNotNull => Ok(LogicalDType::Boolean),
                    UnaryOp::Not => {
                        accepts(&dtype, &[LogicalDType::Boolean])?;
                        Ok(LogicalDType::Boolean)
                    }
                    UnaryOp::Negate => {
                        let dtype = numeric_unary_type(&dtype, false)?;
                        Ok(if dtype == LogicalDType::UInt64 {
                            LogicalDType::Int64
                        } else {
                            dtype
                        })
                    }
                }
            }
            ExpressionKind::Binary { left, op, right } => {
                let left = self.dtype(left, depth + 1)?;
                let right = self.dtype(right, depth + 1)?;
                if matches!(op, BinaryOp::And | BinaryOp::Or) {
                    accepts(&left, &[LogicalDType::Boolean])?;
                    accepts(&right, &[LogicalDType::Boolean])?;
                    Ok(LogicalDType::Boolean)
                } else {
                    numeric_type(&left, *op, &right)
                }
            }
            ExpressionKind::Compare { left, right, .. } => {
                comparable(
                    &self.dtype(left, depth + 1)?,
                    &self.dtype(right, depth + 1)?,
                )?;
                Ok(LogicalDType::Boolean)
            }
            ExpressionKind::FunctionCall { name, args } => self.function(name, args, depth),
            ExpressionKind::List { .. } => Ok(LogicalDType::List),
            ExpressionKind::Struct { .. } => Ok(LogicalDType::Struct),
            ExpressionKind::Unsupported { feature, reason } => {
                Err(EvalFailure::unsupported(feature.clone(), reason.clone()))
            }
        }
    }

    fn function(
        &mut self,
        name: &str,
        args: &[Expression],
        depth: usize,
    ) -> EvalResult<LogicalDType> {
        let normalized = name.trim().to_ascii_lowercase();
        let arity = function_arity(&normalized)?;
        if !arity.contains(&args.len()) {
            return Err(EvalFailure::invalid(
                "function_arity",
                format!(
                    "function {name:?} requires {} through {} arguments",
                    arity.start(),
                    arity.end()
                ),
            ));
        }
        let types = args
            .iter()
            .map(|arg| self.dtype(arg, depth + 1))
            .collect::<EvalResult<Vec<_>>>()?;
        for dtype in &types {
            scalar(dtype)?;
        }
        function_result_dtype(&normalized, &types)
    }
}

fn function_result_dtype(name: &str, types: &[LogicalDType]) -> EvalResult<LogicalDType> {
    use LogicalDType::{Binary, Boolean, Date32, Int64, TimestampMicros, UInt64, Unknown, Utf8};
    match name {
        "coalesce" => types
            .iter()
            .try_fold(Unknown, |dtype, next| common_dtype(&dtype, next)),
        "case_when" => {
            accepts(&types[0], &[Boolean])?;
            common_dtype(&types[1], &types[2])
        }
        "nullif" => {
            comparable(&types[0], &types[1])?;
            Ok(types[0].clone())
        }
        "abs" | "numeric_abs" => numeric_unary_type(&types[0], false),
        "floor" | "numeric_floor" | "ceil" | "ceiling" | "numeric_ceil" | "round"
        | "numeric_round" => numeric_unary_type(&types[0], true),
        "binary_byte_length" | "byte_length" | "octet_length" => {
            accepts(&types[0], &[Utf8, Binary])?;
            Ok(Int64)
        }
        "binary_unhex" | "unhex" | "binary_from_base64" | "from_base64" => {
            accepts(&types[0], &[Utf8])?;
            Ok(Binary)
        }
        "date_add_days" | "date_sub_days" => {
            accepts(&types[0], &[Date32])?;
            accepts(&types[1], &[Int64, UInt64])?;
            Ok(Date32)
        }
        "timestamp_add_seconds" | "timestamp_sub_seconds" => {
            accepts(&types[0], &[TimestampMicros])?;
            accepts(&types[1], &[Int64, UInt64])?;
            Ok(TimestampMicros)
        }
        "date_year" | "year" | "date_month" | "month" | "date_day" | "day" | "date_diff_days" => {
            for dtype in types {
                accepts(dtype, &[Date32])?;
            }
            Ok(Int64)
        }
        "timestamp_year"
        | "timestamp_month"
        | "timestamp_day"
        | "timestamp_hour"
        | "timestamp_minute"
        | "timestamp_second"
        | "timestamp_diff_seconds" => {
            for dtype in types {
                accepts(dtype, &[TimestampMicros])?;
            }
            Ok(Int64)
        }
        _ => {
            let offsets = matches!(
                name,
                "substr"
                    | "substring"
                    | "utf8_substr"
                    | "utf8_substring"
                    | "left"
                    | "right"
                    | "utf8_left"
                    | "utf8_right"
            );
            for (index, dtype) in types.iter().enumerate() {
                if offsets && index > 0 {
                    accepts(dtype, &[Int64, UInt64])?;
                } else {
                    accepts(dtype, &[Utf8])?;
                }
            }
            Ok(match name {
                "length" | "utf8_length" => Int64,
                "starts_with" | "utf8_starts_with" | "ends_with" | "utf8_ends_with"
                | "contains" | "utf8_contains" | "regex_match" | "utf8_regex_match" | "rlike"
                | "regexp" | "regexp_like" => Boolean,
                _ => Utf8,
            })
        }
    }
}

fn function_arity(name: &str) -> EvalResult<std::ops::RangeInclusive<usize>> {
    Ok(match name {
        "coalesce" => 1..=128,
        "concat" | "utf8_concat" => 2..=128,
        "case_when" | "substr" | "substring" | "utf8_substr" | "utf8_substring" | "replace"
        | "utf8_replace" => 3..=3,
        "nullif"
        | "left"
        | "right"
        | "utf8_left"
        | "utf8_right"
        | "starts_with"
        | "utf8_starts_with"
        | "ends_with"
        | "utf8_ends_with"
        | "contains"
        | "utf8_contains"
        | "regex_match"
        | "utf8_regex_match"
        | "rlike"
        | "regexp"
        | "regexp_like"
        | "date_add_days"
        | "date_sub_days"
        | "timestamp_add_seconds"
        | "timestamp_sub_seconds"
        | "date_diff_days"
        | "timestamp_diff_seconds" => 2..=2,
        "abs" | "numeric_abs" | "floor" | "numeric_floor" | "ceil" | "ceiling" | "numeric_ceil"
        | "round" | "numeric_round" | "lower" | "utf8_lower" | "upper" | "utf8_upper" | "trim"
        | "utf8_trim" | "length" | "utf8_length" | "binary_byte_length" | "byte_length"
        | "octet_length" | "binary_unhex" | "unhex" | "binary_from_base64" | "from_base64"
        | "date_year" | "year" | "date_month" | "month" | "date_day" | "day" | "timestamp_year"
        | "timestamp_month" | "timestamp_day" | "timestamp_hour" | "timestamp_minute"
        | "timestamp_second" => 1..=1,
        _ => {
            return Err(EvalFailure::unsupported(
                "function_call",
                format!("function {name:?} has no admitted reference result type"),
            ));
        }
    })
}

fn accepts(dtype: &LogicalDType, allowed: &[LogicalDType]) -> EvalResult<()> {
    if dtype == &LogicalDType::Unknown || allowed.contains(dtype) {
        Ok(())
    } else {
        Err(EvalFailure::unsupported(
            "function_type",
            format!("function does not admit {}", dtype.as_str()),
        ))
    }
}

fn scalar(dtype: &LogicalDType) -> EvalResult<()> {
    if dtype == &LogicalDType::Unknown {
        Ok(())
    } else {
        cast_type(dtype, dtype)
    }
}

fn comparable(left: &LogicalDType, right: &LogicalDType) -> EvalResult<()> {
    use LogicalDType::{Float64, Int64, UInt64, Unknown};
    scalar(left)?;
    scalar(right)?;
    // Preserve the decoded baseline's exact mixed comparison contract. Native
    // key admission remains independent and can require an explicit cast.
    let decimal_peer = |dtype: &LogicalDType| {
        matches!(dtype, Int64 | UInt64) || decimal128_dtype_parts(dtype).is_some()
    };
    if left == right
        || left == &Unknown
        || right == &Unknown
        || matches!((left, right), (Int64 | UInt64, Int64 | UInt64))
        || matches!((left, right), (Float64, Int64) | (Int64, Float64))
        || (decimal128_dtype_parts(left).is_some() && decimal_peer(right))
        || (decimal128_dtype_parts(right).is_some() && decimal_peer(left))
    {
        Ok(())
    } else {
        Err(EvalFailure::unsupported(
            "comparison",
            "incompatible scalar types require an explicit lossless cast",
        ))
    }
}

fn numeric_type(
    left: &LogicalDType,
    op: BinaryOp,
    right: &LogicalDType,
) -> EvalResult<LogicalDType> {
    numeric_output_dtype(
        &EvalValue::null(left.clone(), NullBehavior::NullPropagating),
        op,
        &EvalValue::null(right.clone(), NullBehavior::NullPropagating),
    )
}

pub(super) fn numeric_unary_type(dtype: &LogicalDType, rounding: bool) -> EvalResult<LogicalDType> {
    if let Some((precision, scale)) = decimal128_dtype_parts(dtype) {
        let operand = decimal128_checked_operand(0, precision, scale)?;
        let (precision, scale) = if rounding {
            operand.round().precision_scale()
        } else {
            operand.precision_scale()
        };
        Ok(decimal128_dtype(precision, scale))
    } else {
        numeric_type(dtype, BinaryOp::Add, dtype)
    }
}
