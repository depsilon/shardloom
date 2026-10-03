//! Bind the shared expression IR to explicit native scalar kernels.

use super::{
    Binder, DType, Nullability, PType, Result, common_dtype, failed, field, integer, validate_key,
    validate_key_pair, validate_name,
};
use crate::local_primitives::native_relational_expression::scalar::{Function, decimal_operand};
use crate::local_primitives::native_relational_expression::{Expression, Kind};
use shardloom_core::{
    BinaryOp, Expression as Input, ExpressionKind, LogicalDType, ScalarValue, UnaryOp,
};
use vortex::array::{
    dtype::DecimalDType,
    extension::datetime::{Date, TimeUnit, Timestamp},
    scalar::{DecimalValue, Scalar},
};

impl Binder<'_> {
    pub(super) fn join_condition(
        &mut self,
        condition: Option<&Input>,
        left: &[(String, DType)],
        right: &[(String, DType)],
    ) -> Result<Option<crate::local_primitives::native_relational_join::Condition>> {
        use crate::relational_query::VortexRelationalSide as Side;
        let Some(condition) = condition else {
            return Ok(None);
        };
        self.charge((left.len() + right.len()) * 8192)?;
        let mut fields = Vec::new();
        for (prefix, input) in [("left", left), ("right", right)] {
            for (name, dtype) in input {
                fields.push((format!("{prefix}.{name}"), dtype.clone()));
            }
        }
        let expression = self.expression(condition, &fields, 0)?;
        boolean(&expression.dtype)?;
        let mut used = Vec::new();
        expression.visit_columns(&mut |name| {
            if !used.iter().any(|prior: &String| prior == name) {
                used.push(name.to_owned());
            }
            Ok(())
        })?;
        fields.retain(|(name, _)| used.contains(name));
        let columns = fields
            .iter()
            .map(|(name, _)| {
                if let Some(name) = name.strip_prefix("left.") {
                    Ok((Side::Left, name.to_owned()))
                } else if let Some(name) = name.strip_prefix("right.") {
                    Ok((Side::Right, name.to_owned()))
                } else {
                    Err(failed(
                        "ON predicate requires explicit left/right column scope",
                    ))
                }
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Some(
            crate::local_primitives::native_relational_join::Condition {
                expression,
                fields,
                columns,
            },
        ))
    }

    pub(super) fn expression(
        &mut self,
        input: &Input,
        fields: &[(String, DType)],
        depth: usize,
    ) -> Result<Expression> {
        self.expression_nodes += 1;
        if depth > 24 || self.expression_nodes > 4096 {
            return Err(failed("scalar expressions exceed 24 levels or 4096 nodes"));
        }
        self.charge(4096)?;
        let (dtype, kind) = match &input.kind {
            ExpressionKind::Column(column) => {
                validate_name(column.as_str())?;
                (
                    field(fields, column.as_str())?.clone(),
                    Kind::Column(column.as_str().to_owned()),
                )
            }
            ExpressionKind::Alias { expr, .. } => return self.expression(expr, fields, depth + 1),
            ExpressionKind::Literal(value) => return self.literal_expression(value),
            ExpressionKind::Unary { op, expr } => {
                let child = Box::new(self.expression(expr, fields, depth + 1)?);
                scalar_operand(&child.dtype)?;
                let dtype = match op {
                    UnaryOp::IsNull | UnaryOp::IsNotNull => DType::Bool(Nullability::NonNullable),
                    UnaryOp::Not => {
                        boolean(&child.dtype)?;
                        DType::Bool(child.dtype.nullability())
                    }
                    UnaryOp::Negate => {
                        numeric(&child.dtype)?;
                        if matches!(child.dtype, DType::Decimal(..)) {
                            child.dtype.clone()
                        } else {
                            DType::Primitive(
                                if floating(&child.dtype) {
                                    PType::F64
                                } else {
                                    PType::I64
                                },
                                child.dtype.nullability(),
                            )
                        }
                    }
                };
                (dtype, Kind::Unary(*op, child))
            }
            ExpressionKind::Binary { left, op, right } => {
                let left = Box::new(self.expression(left, fields, depth + 1)?);
                let right = Box::new(self.expression(right, fields, depth + 1)?);
                let dtype = if matches!(op, BinaryOp::And | BinaryOp::Or) {
                    boolean(&left.dtype)?;
                    boolean(&right.dtype)?;
                    DType::Bool(nullable(&left.dtype, &right.dtype))
                } else {
                    arithmetic_dtype(&left.dtype, *op, &right.dtype)?
                };
                (dtype, Kind::Binary(left, *op, right))
            }
            ExpressionKind::Compare { left, op, right } => {
                let left = Box::new(self.expression(left, fields, depth + 1)?);
                let right = Box::new(self.expression(right, fields, depth + 1)?);
                compatible(&left.dtype, &right.dtype)?;
                (
                    DType::Bool(nullable(&left.dtype, &right.dtype)),
                    Kind::Compare(left, *op, right),
                )
            }
            ExpressionKind::FunctionCall { name, args } => {
                return self.function(name, args, fields, depth);
            }
            ExpressionKind::Cast { expr, target_dtype }
            | ExpressionKind::TryCast { expr, target_dtype } => {
                let child = Box::new(self.expression(expr, fields, depth + 1)?);
                let tolerant = matches!(input.kind, ExpressionKind::TryCast { .. });
                if child.dtype != DType::Null {
                    validate_key(&child.dtype)?;
                }
                let dtype = cast_dtype(&child.dtype, target_dtype, tolerant)?;
                (
                    dtype,
                    Kind::Cast {
                        input: child,
                        tolerant,
                    },
                )
            }
            _ => {
                return Err(failed(
                    "expression kind has no admitted native scalar kernel",
                ));
            }
        };
        Ok(Expression { dtype, kind })
    }

    fn function(
        &mut self,
        name: &str,
        args: &[Input],
        fields: &[(String, DType)],
        depth: usize,
    ) -> Result<Expression> {
        if args.len() > 128 {
            return Err(failed("scalar function exceeds 128 arguments"));
        }
        self.charge(args.len() * 4096)?;
        let mut args = args
            .iter()
            .map(|arg| self.expression(arg, fields, depth + 1))
            .collect::<Result<Vec<_>>>()?;
        for argument in &args {
            scalar_operand(&argument.dtype)?;
        }
        let (dtype, kind) = match (name.to_ascii_lowercase().as_str(), args.len()) {
            ("case_when", 3) => {
                let no = args.pop().expect("length bound");
                let yes = args.pop().expect("length bound");
                let condition = args.pop().expect("length bound");
                boolean(&condition.dtype)?;
                (
                    common(&yes.dtype, &no.dtype)?,
                    Kind::Conditional(Box::new(condition), Box::new(yes), Box::new(no)),
                )
            }
            ("nullif", 2) => {
                let right = args.pop().expect("length bound");
                let left = args.pop().expect("length bound");
                compatible(&left.dtype, &right.dtype)?;
                (
                    left.dtype.as_nullable(),
                    Kind::NullIf(Box::new(left), Box::new(right)),
                )
            }
            ("coalesce", 1..) => {
                let mut dtype = DType::Null;
                for arg in &args {
                    dtype = common(&dtype, &arg.dtype)?;
                }
                if args.iter().any(|arg| !arg.dtype.is_nullable()) {
                    dtype = dtype.as_nonnullable();
                }
                (dtype, Kind::Coalesce(args))
            }
            _ => return self.scalar_function(name, args),
        };
        Ok(Expression { dtype, kind })
    }

    fn scalar_function(&mut self, name: &str, args: Vec<Expression>) -> Result<Expression> {
        use crate::local_primitives::native_relational_expression::scalar::Function as F;
        let normalized = name.to_ascii_lowercase();
        let function = match (normalized.as_str(), args.len()) {
            ("abs" | "numeric_abs", 1) => F::Abs,
            ("floor" | "numeric_floor", 1) => F::Floor,
            ("ceil" | "ceiling" | "numeric_ceil", 1) => F::Ceil,
            ("round" | "numeric_round", 1) => F::Round,
            ("lower" | "utf8_lower", 1) => F::Lower,
            ("upper" | "utf8_upper", 1) => F::Upper,
            ("trim" | "utf8_trim", 1) => F::Trim,
            ("length" | "utf8_length", 1) => F::Length,
            ("starts_with" | "utf8_starts_with", 2) => F::StartsWith,
            ("ends_with" | "utf8_ends_with", 2) => F::EndsWith,
            ("contains" | "utf8_contains", 2) => F::Contains,
            ("regex_match" | "utf8_regex_match" | "rlike" | "regexp" | "regexp_like", 2) => {
                self.charge(1024 * 1024)?;
                let Kind::Literal(pattern) = &args[1].kind else {
                    return Err(failed("regular expressions require a literal pattern"));
                };
                let pattern = match pattern.value() {
                    Some(vortex::array::scalar::ScalarValue::Utf8(value)) => value.as_str(),
                    None => "",
                    _ => return Err(failed("regular expression pattern must be UTF8")),
                };
                F::Regex(
                    regex::RegexBuilder::new(pattern)
                        .size_limit(256 * 1024)
                        .dfa_size_limit(256 * 1024)
                        .build()
                        .map_err(super::vortex_error)?,
                )
            }
            ("concat" | "utf8_concat", 2..) => F::Concat,
            ("substr" | "substring" | "utf8_substr" | "utf8_substring", 3) => F::Substr,
            ("left" | "utf8_left", 2) => F::Left,
            ("right" | "utf8_right", 2) => F::Right,
            ("replace" | "utf8_replace", 3) => F::Replace,
            ("binary_byte_length" | "byte_length" | "octet_length", 1) => F::ByteLength,
            ("binary_unhex" | "unhex", 1) => F::Unhex,
            ("binary_from_base64" | "from_base64", 1) => F::FromBase64,
            _ => calendar_function(&normalized, args.len()).ok_or_else(|| {
                failed(&format!(
                    "function '{name}' has no admitted native kernel for this arity"
                ))
            })?,
        };
        let nullable = if args.iter().any(|arg| arg.dtype.is_nullable()) {
            Nullability::Nullable
        } else {
            Nullability::NonNullable
        };
        let dtype = if let Some(dtype) = typed_function_dtype(&function, &args, nullable)? {
            dtype
        } else if matches!(function, F::Abs | F::Floor | F::Ceil | F::Round) {
            numeric(&args[0].dtype)?;
            arithmetic_dtype(&args[0].dtype, BinaryOp::Add, &args[0].dtype)?
        } else {
            for (index, arg) in args.iter().enumerate() {
                if index > 0 && matches!(function, F::Substr | F::Left | F::Right) {
                    if !matches!(arg.dtype, DType::Null)
                        && !matches!(arg.dtype, DType::Primitive(ptype, _) if integer(ptype).is_some())
                    {
                        return Err(failed("substring offsets and lengths require integers"));
                    }
                } else if !matches!(arg.dtype, DType::Utf8(_) | DType::Null) {
                    return Err(failed("string function requires UTF8 arguments"));
                }
            }
            match function {
                F::Length => DType::Primitive(PType::I64, nullable),
                F::StartsWith | F::EndsWith | F::Contains | F::Regex(_) => DType::Bool(nullable),
                _ => DType::Utf8(nullable),
            }
        };
        Ok(Expression {
            dtype,
            kind: Kind::Function { function, args },
        })
    }

    fn literal_expression(&mut self, value: &ScalarValue) -> Result<Expression> {
        let bytes = match value {
            ScalarValue::Utf8(text) => text.len(),
            ScalarValue::Binary(bytes) => bytes.len(),
            _ => 0,
        };
        if bytes > 0 {
            self.charge(
                bytes
                    .checked_mul(8)
                    .ok_or_else(|| failed("literal metadata overflow"))?,
            )?;
        }
        let scalar = literal(value)?;
        let dtype = match value {
            ScalarValue::Date32(_) => {
                DType::Extension(Date::new(TimeUnit::Days, Nullability::NonNullable).erased())
            }
            ScalarValue::TimestampMicros(_) => DType::Extension(
                Timestamp::new(TimeUnit::Microseconds, Nullability::NonNullable).erased(),
            ),
            _ => scalar.dtype().clone(),
        };
        Ok(Expression {
            dtype,
            kind: Kind::Literal(scalar),
        })
    }
}

fn calendar_function(name: &str, arity: usize) -> Option<Function> {
    use shardloom_core::expression as calendar;
    Some(match (name, arity) {
        ("date_year" | "year", 1) => {
            Function::DateExtract(|value| i64::from(calendar::date32_year(value)))
        }
        ("date_month" | "month", 1) => {
            Function::DateExtract(|value| i64::from(calendar::date32_month(value)))
        }
        ("date_day" | "day", 1) => {
            Function::DateExtract(|value| i64::from(calendar::date32_day(value)))
        }
        ("timestamp_year", 1) => {
            Function::TimestampExtract(|value| i64::from(calendar::timestamp_micros_year(value)))
        }
        ("timestamp_month", 1) => {
            Function::TimestampExtract(|value| i64::from(calendar::timestamp_micros_month(value)))
        }
        ("timestamp_day", 1) => {
            Function::TimestampExtract(|value| i64::from(calendar::timestamp_micros_day(value)))
        }
        ("timestamp_hour", 1) => {
            Function::TimestampExtract(|value| i64::from(calendar::timestamp_micros_hour(value)))
        }
        ("timestamp_minute", 1) => {
            Function::TimestampExtract(|value| i64::from(calendar::timestamp_micros_minute(value)))
        }
        ("timestamp_second", 1) => {
            Function::TimestampExtract(|value| i64::from(calendar::timestamp_micros_second(value)))
        }
        ("date_add_days", 2) => Function::DateOffset { subtract: false },
        ("date_sub_days", 2) => Function::DateOffset { subtract: true },
        ("timestamp_add_seconds", 2) => Function::TimestampOffset { subtract: false },
        ("timestamp_sub_seconds", 2) => Function::TimestampOffset { subtract: true },
        ("date_diff_days", 2) => Function::DateDifference,
        ("timestamp_diff_seconds", 2) => Function::TimestampDifference,
        _ => return None,
    })
}

fn cast_dtype(source: &DType, target: &LogicalDType, tolerant: bool) -> Result<DType> {
    let nullable = if tolerant || source.is_nullable() {
        Nullability::Nullable
    } else {
        Nullability::NonNullable
    };
    let target = match target {
        LogicalDType::Boolean => DType::Bool(nullable),
        LogicalDType::Int64 => DType::Primitive(PType::I64, nullable),
        LogicalDType::UInt64 => DType::Primitive(PType::U64, nullable),
        LogicalDType::Float64 => DType::Primitive(PType::F64, nullable),
        LogicalDType::Utf8 => DType::Utf8(nullable),
        LogicalDType::Binary => DType::Binary(nullable),
        LogicalDType::Date32 => DType::Extension(Date::new(TimeUnit::Days, nullable).erased()),
        LogicalDType::TimestampMicros => {
            DType::Extension(Timestamp::new(TimeUnit::Microseconds, nullable).erased())
        }
        LogicalDType::Extension(_) => {
            let (precision, scale) = shardloom_core::expression::decimal128_dtype_parts(target)
                .ok_or_else(|| failed("cast target requires valid decimal128 precision/scale"))?;
            DType::Decimal(
                DecimalDType::new(precision, i8::try_from(scale).expect("validated scale")),
                nullable,
            )
        }
        _ => return Err(failed("cast target has no admitted native scalar kernel")),
    };
    let admitted = match source {
        DType::Null | DType::Utf8(_) => true,
        DType::Bool(_) => matches!(target, DType::Bool(_) | DType::Utf8(_) | DType::Binary(_)),
        DType::Primitive(..) | DType::Decimal(..) => matches!(
            target,
            DType::Primitive(..) | DType::Decimal(..) | DType::Utf8(_) | DType::Binary(_)
        ),
        DType::Binary(_) => matches!(target, DType::Utf8(_) | DType::Binary(_)),
        DType::Extension(_) => matches!(
            target,
            DType::Extension(_) | DType::Utf8(_) | DType::Binary(_)
        ),
        _ => false,
    };
    if !admitted {
        return Err(failed(
            "source and target have no admitted explicit scalar conversion",
        ));
    }
    Ok(target)
}

fn literal(value: &ScalarValue) -> Result<Scalar> {
    Ok(match value {
        ScalarValue::Null => Scalar::null(DType::Null),
        ScalarValue::Boolean(value) => Scalar::from(*value),
        ScalarValue::Int64(value) | ScalarValue::TimestampMicros(value) => Scalar::from(*value),
        ScalarValue::UInt64(value) => Scalar::from(*value),
        ScalarValue::Float64(value) if value.is_finite() => Scalar::from(*value),
        ScalarValue::Utf8(value) => Scalar::from(value.as_str()),
        ScalarValue::Binary(value) => Scalar::binary(
            vortex::buffer::ByteBuffer::copy_from(value.as_slice()),
            Nullability::NonNullable,
        ),
        ScalarValue::Decimal128 {
            value,
            precision,
            scale,
        } => {
            shardloom_core::expression::Decimal128Operand::decimal(*value, *precision, *scale)?;
            Scalar::decimal(
                DecimalValue::I128(*value),
                DecimalDType::new(*precision, i8::try_from(*scale).expect("validated scale")),
                Nullability::NonNullable,
            )
        }
        // Keep constants in their primitive storage domain. Upstream temporal
        // scalar validation uses a narrower calendar and can panic for valid i64
        // microseconds. Evaluation wraps the constant with its bound extension.
        ScalarValue::Date32(value) => Scalar::from(*value),
        _ => {
            return Err(failed(
                "scalar literal requires an admitted flat type and finite numeric value",
            ));
        }
    })
}

fn nullable(left: &DType, right: &DType) -> Nullability {
    if left.is_nullable() || right.is_nullable() {
        Nullability::Nullable
    } else {
        Nullability::NonNullable
    }
}

fn boolean(dtype: &DType) -> Result<()> {
    if matches!(dtype, DType::Bool(_) | DType::Null) {
        Ok(())
    } else {
        Err(failed("boolean expression requires boolean operands"))
    }
}

fn numeric(dtype: &DType) -> Result<()> {
    if matches!(
        dtype,
        DType::Primitive(_, _) | DType::Decimal(..) | DType::Null
    ) && !matches!(dtype, DType::Primitive(PType::F16, _))
    {
        Ok(())
    } else {
        Err(failed(
            "arithmetic requires integer, F32/F64 or admitted decimal operands",
        ))
    }
}

fn floating(dtype: &DType) -> bool {
    matches!(dtype, DType::Primitive(PType::F32 | PType::F64, _))
}

fn arithmetic_dtype(left: &DType, op: BinaryOp, right: &DType) -> Result<DType> {
    numeric(left)?;
    numeric(right)?;
    if matches!(left, DType::Decimal(..)) || matches!(right, DType::Decimal(..)) {
        let (precision, scale) =
            decimal_operand(0, left)?.arithmetic_type(op, decimal_operand(0, right)?)?;
        return Ok(DType::Decimal(
            DecimalDType::new(precision, i8::try_from(scale).expect("validated scale")),
            nullable(left, right),
        ));
    }
    let dtype = if floating(left) || floating(right) {
        PType::F64
    } else {
        let common = common(left, right)?;
        match common {
            DType::Primitive(ptype, _) if integer(ptype).is_some_and(|(signed, _)| !signed) => {
                PType::U64
            }
            _ => PType::I64,
        }
    };
    Ok(DType::Primitive(dtype, nullable(left, right)))
}

fn typed_function_dtype(
    function: &Function,
    args: &[Expression],
    nullable: Nullability,
) -> Result<Option<DType>> {
    use Function as F;
    let first = &args[0].dtype;
    let temporal = |dtype: &DType, ptype| {
        if dtype == &DType::Null
            || crate::native_payload_schema::temporal_storage(dtype) == Some(ptype)
        {
            Ok(())
        } else {
            Err(failed(
                "calendar function requires its declared date/timestamp unit",
            ))
        }
    };
    let dtype = match function {
        F::Abs | F::Floor | F::Ceil | F::Round if matches!(first, DType::Decimal(..)) => {
            let operand = decimal_operand(0, first)?;
            let (precision, scale) = if matches!(function, F::Abs) {
                operand.precision_scale()
            } else {
                operand.round().precision_scale()
            };
            DType::Decimal(
                DecimalDType::new(precision, i8::try_from(scale).expect("validated scale")),
                nullable,
            )
        }
        F::ByteLength => {
            if !matches!(first, DType::Null | DType::Utf8(_) | DType::Binary(_)) {
                return Err(failed("byte length requires UTF8 or binary"));
            }
            DType::Primitive(PType::I64, nullable)
        }
        F::Unhex | F::FromBase64 => {
            if !matches!(first, DType::Null | DType::Utf8(_)) {
                return Err(failed("binary decoding requires UTF8"));
            }
            DType::Binary(nullable)
        }
        F::DateExtract(_) | F::TimestampExtract(_) | F::DateDifference | F::TimestampDifference => {
            let ptype = if matches!(function, F::DateExtract(_) | F::DateDifference) {
                PType::I32
            } else {
                PType::I64
            };
            for arg in args {
                temporal(&arg.dtype, ptype)?;
            }
            DType::Primitive(PType::I64, nullable)
        }
        F::DateOffset { .. } | F::TimestampOffset { .. } => {
            let date = matches!(function, F::DateOffset { .. });
            temporal(first, if date { PType::I32 } else { PType::I64 })?;
            if args[1].dtype != DType::Null
                && !matches!(args[1].dtype, DType::Primitive(ptype, _) if integer(ptype).is_some())
            {
                return Err(failed("calendar offset requires an integer"));
            }
            DType::Extension(if date {
                Date::new(TimeUnit::Days, nullable).erased()
            } else {
                Timestamp::new(TimeUnit::Microseconds, nullable).erased()
            })
        }
        _ => return Ok(None),
    };
    Ok(Some(dtype))
}

fn compatible(left: &DType, right: &DType) -> Result<()> {
    scalar_operand(left)?;
    scalar_operand(right)?;
    if left == &DType::Null || right == &DType::Null {
        Ok(())
    } else {
        validate_key_pair(left, right)
    }
}

fn scalar_operand(dtype: &DType) -> Result<()> {
    if dtype == &DType::Null {
        Ok(())
    } else {
        validate_key(dtype)
    }
}

fn common(left: &DType, right: &DType) -> Result<DType> {
    if left == &DType::Null {
        Ok(right.as_nullable())
    } else if right == &DType::Null {
        Ok(left.as_nullable())
    } else if matches!((left, right), (DType::Decimal(..), DType::Decimal(..))) {
        let (precision, scale) =
            decimal_operand(0, left)?.common_type(decimal_operand(0, right)?)?;
        Ok(DType::Decimal(
            DecimalDType::new(precision, i8::try_from(scale).expect("validated scale")),
            nullable(left, right),
        ))
    } else {
        common_dtype(left, right)
    }
}
