//! Bind the shared expression IR to explicit native scalar kernels.

use super::{
    Binder, DType, Nullability, PType, Result, common_dtype, failed, field, integer,
    validate_flat_operand, validate_key, validate_key_pair, validate_name,
};
use crate::local_primitives::native_relational_expression::scalar::{Function, decimal_operand};
use crate::local_primitives::native_relational_expression::{Expression, Kind, PREPARATION_BYTES};
use shardloom_core::expression::builtins::{
    self, ScalarDefinition, ScalarNullRule, ScalarTypeRule,
};
use shardloom_core::{
    BinaryOp, Expression as Input, ExpressionKind, LogicalDType, ScalarValue, UnaryOp,
};
use vortex::array::{
    dtype::{DecimalDType, StructFields},
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
        if depth > crate::relational_query::NATIVE_RECURSION_LIMIT {
            return Err(failed("recursive scalar expression exceeds 24 levels"));
        }
        // Each expression is admitted before allocation, including independent
        // projected fields in a wide schema. Do not cap their cumulative count.
        self.charge(PREPARATION_BYTES)?;
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
            ExpressionKind::List { values } => {
                return self.list_expression(values, fields, depth);
            }
            ExpressionKind::Struct { fields: children } => {
                return self.struct_expression(children, fields, depth);
            }
            ExpressionKind::Unary { op, expr } => {
                return self.unary_expression(*op, expr, fields, depth);
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
                let (left, right) = comparison_operands(
                    self.expression(left, fields, depth + 1)?,
                    self.expression(right, fields, depth + 1)?,
                )?;
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
                    validate_flat_operand(&child.dtype)?;
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
            ExpressionKind::RelationalValue { .. } => {
                return Err(failed(
                    "relational value has no resolved native query binding",
                ));
            }
            ExpressionKind::Unsupported { .. } => {
                return Err(failed(
                    "expression kind has no admitted native scalar kernel",
                ));
            }
        };
        Ok(Expression::new(dtype, kind))
    }

    fn unary_expression(
        &mut self,
        op: UnaryOp,
        input: &Input,
        fields: &[(String, DType)],
        depth: usize,
    ) -> Result<Expression> {
        let child = Box::new(self.expression(input, fields, depth + 1)?);
        if matches!(op, UnaryOp::IsNull | UnaryOp::IsNotNull) {
            selected_operand(&child.dtype)?;
        } else {
            scalar_operand(&child.dtype)?;
        }
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
        Ok(Expression::new(dtype, Kind::Unary(op, child)))
    }

    fn list_expression(
        &mut self,
        values: &[Input],
        fields: &[(String, DType)],
        depth: usize,
    ) -> Result<Expression> {
        if values.len() > 128 {
            return Err(failed("list constructor exceeds 128 elements"));
        }
        self.charge_fields(values.len())?;
        let values = values
            .iter()
            .map(|value| self.expression(value, fields, depth + 1))
            .collect::<Result<Vec<_>>>()?;
        let mut element = DType::Null;
        for value in &values {
            selected_operand(&value.dtype)?;
            element = common(&element, &value.dtype)?;
        }
        if element == DType::Null {
            element = DType::Bool(Nullability::Nullable);
        }
        Ok(Expression::new(
            DType::List(element.into(), Nullability::NonNullable),
            Kind::List(values),
        ))
    }

    fn struct_expression(
        &mut self,
        children: &[(String, Input)],
        fields: &[(String, DType)],
        depth: usize,
    ) -> Result<Expression> {
        if children.is_empty() || children.len() > 128 {
            return Err(failed("struct constructor requires 1..=128 fields"));
        }
        self.charge_fields(children.len())?;
        let mut values = Vec::new();
        let mut output_fields = Vec::new();
        for (name, value) in children {
            validate_name(name)?;
            if output_fields.iter().any(|(prior, _)| prior == name) {
                return Err(failed("struct constructor field names must be distinct"));
            }
            let mut value = self.expression(value, fields, depth + 1)?;
            selected_operand(&value.dtype)?;
            if value.dtype == DType::Null {
                value = bound_cast(value, DType::Bool(Nullability::Nullable));
            }
            output_fields.push((name.clone(), value.dtype.clone()));
            values.push(value);
        }
        Ok(Expression::new(
            DType::Struct(
                StructFields::from_iter(output_fields),
                Nullability::NonNullable,
            ),
            Kind::Struct(values),
        ))
    }

    fn function(
        &mut self,
        name: &str,
        args: &[Input],
        fields: &[(String, DType)],
        depth: usize,
    ) -> Result<Expression> {
        let definition = builtins::scalar(name)
            .ok_or_else(|| failed(&format!("function '{name}' has no admitted native kernel")))?;
        if !definition.accepts_arity(args.len()) {
            return Err(failed(&format!(
                "function '{}' requires {} through {} arguments",
                definition.name, definition.min_args, definition.max_args
            )));
        }
        self.charge_fields(args.len())?;
        let mut args = args
            .iter()
            .map(|arg| self.expression(arg, fields, depth + 1))
            .collect::<Result<Vec<_>>>()?;
        for argument in &args {
            selected_operand(&argument.dtype)?;
        }
        let nullability = function_nullability(definition.null_rule, &args);
        let (dtype, kind) = match definition.type_rule {
            ScalarTypeRule::Conditional => {
                let no = args.pop().expect("length bound");
                let yes = args.pop().expect("length bound");
                let condition = args.pop().expect("length bound");
                boolean(&condition.dtype)?;
                (
                    common(&yes.dtype, &no.dtype)?,
                    Kind::Conditional(Box::new(condition), Box::new(yes), Box::new(no)),
                )
            }
            ScalarTypeRule::NullIf => {
                let right = args.pop().expect("length bound");
                let left = args.pop().expect("length bound");
                // NULLIF retains its first operand's type and values.
                compatible(&left.dtype, &right.dtype)?;
                (
                    left.dtype.clone(),
                    Kind::NullIf(Box::new(left), Box::new(right)),
                )
            }
            ScalarTypeRule::Coalesce => {
                let mut dtype = DType::Null;
                for arg in &args {
                    dtype = common(&dtype, &arg.dtype)?;
                }
                (dtype, Kind::Coalesce(args))
            }
            _ => return self.scalar_function(definition, args),
        };
        Ok(Expression::new(dtype.with_nullability(nullability), kind))
    }

    fn scalar_function(
        &mut self,
        definition: &ScalarDefinition,
        args: Vec<Expression>,
    ) -> Result<Expression> {
        use crate::local_primitives::native_relational_expression::scalar::Function as F;
        for argument in &args {
            scalar_operand(&argument.dtype)?;
        }
        let name = definition.name;
        let function = match name {
            "abs" => F::Abs,
            "floor" => F::Floor,
            "ceil" => F::Ceil,
            "round" => F::Round,
            "lower" => F::Lower,
            "upper" => F::Upper,
            "trim" => F::Trim,
            "length" => F::Length,
            "url_domain" => F::Domain { regex: false },
            "regex_domain" => F::Domain { regex: true },
            "regexp_replace" => fixed_domain_replacement(&args)?,
            "starts_with" => F::StartsWith,
            "ends_with" => F::EndsWith,
            "contains" => F::Contains,
            "regex_match" => {
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
            "concat" => F::Concat,
            "substr" => F::Substr,
            "left" => F::Left,
            "right" => F::Right,
            "replace" => F::Replace,
            "byte_length" => F::ByteLength,
            "unhex" => F::Unhex,
            "from_base64" => F::FromBase64,
            "json_extract" => {
                use crate::local_primitives::native_relational_expression::scalar::text_kernels::JsonPath;
                F::JsonExtract(JsonPath::parse(
                    literal_text(&args[1], "JSON path")?.unwrap_or("$"),
                )?)
            }
            "strptime" | "try_strptime" => {
                use crate::local_primitives::native_relational_expression::scalar::text_kernels::TimestampFormat;
                F::Strptime {
                    format: TimestampFormat::parse(
                        literal_text(&args[1], "timestamp format")?.unwrap_or("%Y-%m-%d"),
                    )?,
                    tolerant: name == "try_strptime",
                }
            }
            _ => calendar_function(name).ok_or_else(|| {
                failed(&format!(
                    "function '{name}' has no admitted native kernel for this arity"
                ))
            })?,
        };
        let args = calendar_arguments(&function, args);
        let nullable = function_nullability(definition.null_rule, &args);
        let dtype = typed_function_dtype(definition.type_rule, &args, nullable)?;
        Ok(Expression::new(dtype, Kind::Function { function, args }))
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
        let dtype = literal_dtype(value)?;
        let scalar = literal(value)?;
        Ok(Expression::new(dtype, Kind::Literal(scalar)))
    }
}

fn fixed_domain_replacement(args: &[Expression]) -> Result<Function> {
    use crate::local_primitives::native_relational_expression::scalar::Function as F;

    if literal_text(&args[1], "domain replacement pattern")?
        != Some(r"^https?://(?:www\.)?([^/]+)/.*$")
        || literal_text(&args[2], "domain replacement capture")? != Some(r"\1")
    {
        return Err(failed(
            "REGEXP_REPLACE admits only the fixed HTTP(S) domain capture; arbitrary regex replacement is unsupported",
        ));
    }
    Ok(F::Domain { regex: true })
}

fn calendar_function(name: &str) -> Option<Function> {
    use shardloom_core::expression as calendar;
    Some(match name {
        "extract_minute" => Function::EpochMinute { truncate: false },
        "date_trunc_minute" => Function::EpochMinute { truncate: true },
        "date_year" => Function::DateExtract(|value| i64::from(calendar::date32_year(value))),
        "date_month" => Function::DateExtract(|value| i64::from(calendar::date32_month(value))),
        "date_day" => Function::DateExtract(|value| i64::from(calendar::date32_day(value))),
        "timestamp_year" => {
            Function::TimestampExtract(|value| i64::from(calendar::timestamp_micros_year(value)))
        }
        "timestamp_month" => {
            Function::TimestampExtract(|value| i64::from(calendar::timestamp_micros_month(value)))
        }
        "timestamp_day" => {
            Function::TimestampExtract(|value| i64::from(calendar::timestamp_micros_day(value)))
        }
        "timestamp_hour" => {
            Function::TimestampExtract(|value| i64::from(calendar::timestamp_micros_hour(value)))
        }
        "timestamp_minute" => {
            Function::TimestampExtract(|value| i64::from(calendar::timestamp_micros_minute(value)))
        }
        "timestamp_second" => {
            Function::TimestampExtract(|value| i64::from(calendar::timestamp_micros_second(value)))
        }
        "date_add_days" => Function::DateOffset { subtract: false },
        "date_sub_days" => Function::DateOffset { subtract: true },
        "timestamp_add_seconds" => Function::TimestampOffset { subtract: false },
        "timestamp_sub_seconds" => Function::TimestampOffset { subtract: true },
        "date_diff_days" => Function::DateDifference,
        "timestamp_diff_seconds" => Function::TimestampDifference,
        _ => return None,
    })
}

fn literal_text<'a>(expression: &'a Expression, label: &str) -> Result<Option<&'a str>> {
    let Kind::Literal(value) = &expression.kind else {
        return Err(failed(&format!("{label} requires a literal UTF8 argument")));
    };
    match value.value() {
        Some(vortex::array::scalar::ScalarValue::Utf8(value)) => Ok(Some(value.as_str())),
        None => Ok(None),
        _ => Err(failed(&format!("{label} requires a literal UTF8 argument"))),
    }
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

/// Shared literal admission for native expression and retained unary binding.
/// This inspects metadata without allocating payload or calendar scalars.
pub(in crate::local_primitives) fn literal_dtype(value: &ScalarValue) -> Result<DType> {
    let nullability = Nullability::NonNullable;
    Ok(match value {
        ScalarValue::Null => DType::Null,
        ScalarValue::Boolean(_) => DType::Bool(nullability),
        ScalarValue::Int64(_) => DType::Primitive(PType::I64, nullability),
        ScalarValue::UInt64(_) => DType::Primitive(PType::U64, nullability),
        ScalarValue::Float64(value) if value.is_finite() => {
            DType::Primitive(PType::F64, nullability)
        }
        ScalarValue::Utf8(_) => DType::Utf8(nullability),
        ScalarValue::Binary(_) => DType::Binary(nullability),
        ScalarValue::Decimal128 {
            value,
            precision,
            scale,
        } => {
            shardloom_core::expression::Decimal128Operand::decimal(*value, *precision, *scale)?;
            DType::Decimal(
                DecimalDType::new(*precision, i8::try_from(*scale).expect("validated scale")),
                nullability,
            )
        }
        ScalarValue::Date32(_) => DType::Extension(Date::new(TimeUnit::Days, nullability).erased()),
        ScalarValue::TimestampMicros(_) => {
            DType::Extension(Timestamp::new(TimeUnit::Microseconds, nullability).erased())
        }
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

pub(in crate::local_primitives) fn arithmetic_dtype(
    left: &DType,
    op: BinaryOp,
    right: &DType,
) -> Result<DType> {
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

fn function_nullability(rule: ScalarNullRule, args: &[Expression]) -> Nullability {
    let nullable = match rule {
        ScalarNullRule::Propagate => args.iter().any(|arg| arg.dtype.is_nullable()),
        ScalarNullRule::AlwaysNullable => true,
        ScalarNullRule::AllNullable => args.iter().all(|arg| arg.dtype.is_nullable()),
        ScalarNullRule::Branches => args[1..].iter().any(|arg| arg.dtype.is_nullable()),
    };
    if nullable {
        Nullability::Nullable
    } else {
        Nullability::NonNullable
    }
}

fn typed_function_dtype(
    rule: ScalarTypeRule,
    args: &[Expression],
    nullable: Nullability,
) -> Result<DType> {
    use ScalarTypeRule as T;
    let first = &args[0].dtype;
    let dtype = match rule {
        T::EpochMinute | T::EpochTruncate => match first {
            DType::Primitive(ptype, _) if ptype.is_signed_int() => {
                DType::Primitive(PType::I64, nullable)
            }
            DType::Primitive(ptype, _) if ptype.is_unsigned_int() => {
                DType::Primitive(PType::U64, nullable)
            }
            DType::Utf8(_) if rule == T::EpochTruncate => DType::Utf8(nullable),
            DType::Utf8(_) => DType::Primitive(PType::U64, nullable),
            DType::Null => DType::Primitive(PType::I64, nullable),
            _ => {
                return Err(failed(
                    "minute functions require integer epoch seconds or timestamp text; use timestamp_minute for TimestampMicros",
                ));
            }
        },
        T::JsonText | T::TimestampParse => {
            if args
                .iter()
                .any(|arg| !matches!(arg.dtype, DType::Null | DType::Utf8(_)))
            {
                return Err(failed("text parsing requires UTF8 arguments"));
            }
            if rule == T::JsonText {
                DType::Utf8(nullable)
            } else {
                DType::Extension(Timestamp::new(TimeUnit::Microseconds, nullable).erased())
            }
        }
        T::NumericAbs | T::NumericRound if matches!(first, DType::Decimal(..)) => {
            let operand = decimal_operand(0, first)?;
            let (precision, scale) = if rule == T::NumericAbs {
                operand.precision_scale()
            } else {
                operand.round().precision_scale()
            };
            DType::Decimal(
                DecimalDType::new(precision, i8::try_from(scale).expect("validated scale")),
                nullable,
            )
        }
        T::NumericAbs | T::NumericRound => {
            numeric(first)?;
            arithmetic_dtype(first, BinaryOp::Add, first)?
        }
        T::ByteLength => {
            if !matches!(first, DType::Null | DType::Utf8(_) | DType::Binary(_)) {
                return Err(failed("byte length requires UTF8 or binary"));
            }
            DType::Primitive(PType::I64, nullable)
        }
        T::BinaryDecode => {
            if !matches!(first, DType::Null | DType::Utf8(_)) {
                return Err(failed("binary decoding requires UTF8"));
            }
            DType::Binary(nullable)
        }
        T::DateField | T::TimestampField | T::DateDifference | T::TimestampDifference => {
            let ptype = if matches!(rule, T::DateField | T::DateDifference) {
                PType::I32
            } else {
                PType::I64
            };
            for arg in args {
                temporal_function_argument(&arg.dtype, ptype)?;
            }
            DType::Primitive(PType::I64, nullable)
        }
        T::DateOffset | T::TimestampOffset => {
            let date = rule == T::DateOffset;
            temporal_function_argument(first, if date { PType::I32 } else { PType::I64 })?;
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
        T::Utf8 | T::Utf8Offsets | T::Utf8Length | T::Utf8Predicate => {
            return text_function_dtype(rule, args, nullable);
        }
        T::Coalesce | T::NullIf | T::Conditional => {
            return Err(failed(
                "selected-operand function requires its dedicated binding",
            ));
        }
    };
    Ok(dtype)
}

fn temporal_function_argument(dtype: &DType, ptype: PType) -> Result<()> {
    if dtype == &DType::Null || crate::native_payload_schema::temporal_storage(dtype) == Some(ptype)
    {
        Ok(())
    } else {
        Err(failed(
            "calendar function requires its declared date/timestamp unit",
        ))
    }
}

fn text_function_dtype(
    rule: ScalarTypeRule,
    args: &[Expression],
    nullable: Nullability,
) -> Result<DType> {
    for (index, arg) in args.iter().enumerate() {
        if index > 0 && rule == ScalarTypeRule::Utf8Offsets {
            if !matches!(arg.dtype, DType::Null)
                && !matches!(arg.dtype, DType::Primitive(ptype, _) if integer(ptype).is_some())
            {
                return Err(failed("substring offsets and lengths require integers"));
            }
        } else if !matches!(arg.dtype, DType::Utf8(_) | DType::Null) {
            return Err(failed("string function requires UTF8 arguments"));
        }
    }
    Ok(match rule {
        ScalarTypeRule::Utf8Length => DType::Primitive(PType::I64, nullable),
        ScalarTypeRule::Utf8Predicate => DType::Bool(nullable),
        _ => DType::Utf8(nullable),
    })
}

fn comparison_operands(
    mut left: Expression,
    mut right: Expression,
) -> Result<(Box<Expression>, Box<Expression>)> {
    if matches!(left.dtype, DType::Utf8(_))
        && crate::native_payload_schema::temporal_storage(&right.dtype).is_some()
    {
        let dtype = right.dtype.with_nullability(left.dtype.nullability());
        left = bound_cast(left, dtype);
    }
    if matches!(right.dtype, DType::Utf8(_))
        && crate::native_payload_schema::temporal_storage(&left.dtype).is_some()
    {
        let dtype = left.dtype.with_nullability(right.dtype.nullability());
        right = bound_cast(right, dtype);
    }
    if matches!(
        (&left.dtype, &right.dtype),
        (DType::Decimal(..), DType::Decimal(..))
    ) {
        let dtype = common(&left.dtype, &right.dtype)?;
        let left_dtype = dtype.with_nullability(left.dtype.nullability());
        let right_dtype = dtype.with_nullability(right.dtype.nullability());
        left = bound_cast(left, left_dtype);
        right = bound_cast(right, right_dtype);
    }
    compatible(&left.dtype, &right.dtype)?;
    Ok((Box::new(left), Box::new(right)))
}

fn bound_cast(expression: Expression, dtype: DType) -> Expression {
    if expression.dtype == dtype {
        return expression;
    }
    Expression::new(
        dtype,
        Kind::Cast {
            input: Box::new(expression),
            tolerant: false,
        },
    )
}

fn calendar_arguments(function: &Function, args: Vec<Expression>) -> Vec<Expression> {
    use Function as F;
    args.into_iter()
        .enumerate()
        .map(|(index, arg)| {
            let target = match function {
                F::DateExtract(_) | F::DateDifference => Some(true),
                F::TimestampExtract(_) | F::TimestampDifference => Some(false),
                F::DateOffset { .. } if index == 0 => Some(true),
                F::TimestampOffset { .. } if index == 0 => Some(false),
                _ => None,
            };
            if let Some(date) = target
                && matches!(arg.dtype, DType::Utf8(_))
            {
                let nullable = arg.dtype.nullability();
                let dtype = DType::Extension(if date {
                    Date::new(TimeUnit::Days, nullable).erased()
                } else {
                    Timestamp::new(TimeUnit::Microseconds, nullable).erased()
                });
                bound_cast(arg, dtype)
            } else {
                arg
            }
        })
        .collect()
}

fn compatible(left: &DType, right: &DType) -> Result<()> {
    selected_operand(left)?;
    selected_operand(right)?;
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
        validate_flat_operand(dtype)
    }
}

fn selected_operand(dtype: &DType) -> Result<()> {
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
