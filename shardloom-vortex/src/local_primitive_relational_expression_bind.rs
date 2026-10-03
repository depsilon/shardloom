//! Bind the shared expression IR to explicit native scalar kernels.

use super::{
    Binder, DType, Nullability, PType, Result, common_dtype, failed, field, integer, validate_key,
    validate_key_pair, validate_name, validate_scalar,
};
use crate::local_primitives::native_relational_expression::{Expression, Kind};
use shardloom_core::{
    BinaryOp, Expression as Input, ExpressionKind, LogicalDType, ScalarValue, UnaryOp,
};
use vortex::array::scalar::Scalar;

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
            ExpressionKind::Literal(value) => {
                if let ScalarValue::Utf8(text) = value {
                    self.charge(
                        text.len()
                            .checked_mul(8)
                            .ok_or_else(|| failed("literal metadata overflow"))?,
                    )?;
                }
                let scalar = literal(value)?;
                (scalar.dtype().clone(), Kind::Literal(scalar))
            }
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
                        DType::Primitive(
                            if floating(&child.dtype) {
                                PType::F64
                            } else {
                                PType::I64
                            },
                            child.dtype.nullability(),
                        )
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
                    arithmetic_dtype(&left.dtype, &right.dtype)?
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
                    validate_scalar(&child.dtype)?;
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
        let function = match (name.to_ascii_lowercase().as_str(), args.len()) {
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
            _ => {
                return Err(failed(&format!(
                    "function '{name}' has no admitted native kernel for this arity"
                )));
            }
        };
        let nullable = if args.iter().any(|arg| arg.dtype.is_nullable()) {
            Nullability::Nullable
        } else {
            Nullability::NonNullable
        };
        let dtype = if matches!(function, F::Abs | F::Floor | F::Ceil | F::Round) {
            numeric(&args[0].dtype)?;
            arithmetic_dtype(&args[0].dtype, &args[0].dtype)?
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
        _ => return Err(failed("cast target has no admitted native scalar kernel")),
    };
    if matches!(source, DType::Bool(_)) && !matches!(target, DType::Bool(_) | DType::Utf8(_))
        || matches!(source, DType::Primitive(_, _)) && matches!(target, DType::Bool(_))
    {
        return Err(failed(
            "numeric and boolean casts require a supported explicit conversion",
        ));
    }
    Ok(target)
}

fn literal(value: &ScalarValue) -> Result<Scalar> {
    Ok(match value {
        ScalarValue::Null => Scalar::null(DType::Null),
        ScalarValue::Boolean(value) => Scalar::from(*value),
        ScalarValue::Int64(value) => Scalar::from(*value),
        ScalarValue::UInt64(value) => Scalar::from(*value),
        ScalarValue::Float64(value) if value.is_finite() => Scalar::from(*value),
        ScalarValue::Utf8(value) => Scalar::from(value.as_str()),
        _ => {
            return Err(failed(
                "scalar literal requires boolean, integer, finite float or UTF8",
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
    if matches!(dtype, DType::Primitive(_, _) | DType::Null)
        && !matches!(dtype, DType::Primitive(PType::F16, _))
    {
        Ok(())
    } else {
        Err(failed("arithmetic requires integer or F32/F64 operands"))
    }
}

fn floating(dtype: &DType) -> bool {
    matches!(dtype, DType::Primitive(PType::F32 | PType::F64, _))
}

fn arithmetic_dtype(left: &DType, right: &DType) -> Result<DType> {
    numeric(left)?;
    numeric(right)?;
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
    } else {
        common_dtype(left, right)
    }
}
