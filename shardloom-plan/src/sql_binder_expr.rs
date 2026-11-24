// SPDX-License-Identifier: Apache-2.0

//! Expression lowering from `sqlparser::ast::Expr` into canonical `ShardLoom`
//! logical and core expression nodes without fallback execution.

use shardloom_core::{
    BinaryOp, ColumnRef, ComparisonOp, Diagnostic, DiagnosticCode, ExprId, Expression,
    ExpressionKind, LogicalDType, Result, ScalarValue, ShardLoomError, UnaryOp, parse_iso_date32,
    parse_iso_timestamp_micros,
};
use sqlparser::ast::{
    self, BinaryOperator as SqlBinaryOp, CastKind, DataType as SqlDataType, Expr as SqlExpr,
    FunctionArg, FunctionArgExpr, FunctionArgumentClause, FunctionArguments, InTarget,
    TrimWhereField, UnaryOperator as SqlUnaryOp, Value as SqlValue, WindowFrameUnits, WindowType,
};

use crate::sql_binder::{
    LogicalRelation, LogicalSortKey, LogicalWindowFrame, LogicalWindowSpec, LogicalWindowUnits,
    bind_ast_query, unsupported_sql_error,
};

/// Canonical AST-bound expression node independent of external engines.
#[derive(Debug, Clone, PartialEq)]
pub enum LogicalExpr {
    Literal(ScalarValue),
    Column {
        qualifier: Option<String>,
        name: String,
    },
    Parameter(String),
    Unary {
        op: UnaryOp,
        expr: Box<LogicalExpr>,
    },
    Binary {
        left: Box<LogicalExpr>,
        op: BinaryOp,
        right: Box<LogicalExpr>,
    },
    Compare {
        left: Box<LogicalExpr>,
        op: ComparisonOp,
        right: Box<LogicalExpr>,
    },
    IsDistinctFrom {
        left: Box<LogicalExpr>,
        right: Box<LogicalExpr>,
        negated: bool,
    },
    Between {
        expr: Box<LogicalExpr>,
        low: Box<LogicalExpr>,
        high: Box<LogicalExpr>,
        negated: bool,
    },
    InList {
        expr: Box<LogicalExpr>,
        list: Vec<LogicalExpr>,
        negated: bool,
    },
    InSubquery {
        expr: Box<LogicalExpr>,
        subquery: Box<LogicalRelation>,
        negated: bool,
    },
    Exists {
        subquery: Box<LogicalRelation>,
        negated: bool,
    },
    ScalarSubquery(Box<LogicalRelation>),
    Like {
        expr: Box<LogicalExpr>,
        pattern: Box<LogicalExpr>,
        escape: Option<String>,
        negated: bool,
        case_insensitive: bool,
    },
    Cast {
        expr: Box<LogicalExpr>,
        target_dtype: String,
        try_cast: bool,
    },
    Case {
        operand: Option<Box<LogicalExpr>>,
        when_then: Vec<(LogicalExpr, LogicalExpr)>,
        else_expr: Option<Box<LogicalExpr>>,
    },
    Function {
        name: String,
        args: Vec<LogicalExpr>,
        distinct: bool,
        filter: Option<Box<LogicalExpr>>,
        order_by: Vec<LogicalSortKey>,
        over: Option<LogicalWindowSpec>,
    },
    Interval {
        value: String,
        unit: Option<String>,
    },
    Array(Vec<LogicalExpr>),
    Tuple(Vec<LogicalExpr>),
    Wildcard {
        qualifier: Option<String>,
    },
}

impl LogicalExpr {
    /// Converts pure scalar expressions into `shardloom_core::Expression`.
    ///
    /// # Errors
    /// Returns [`ShardLoomError::InvalidOperation`] or
    /// [`ShardLoomError::NotImplemented`] with explicit no-fallback
    /// diagnostics when an expression contains relational/window/parameter
    /// nodes that require full relational lowering.
    pub fn try_into_core_expression(&self, id_prefix: &str) -> Result<Expression> {
        let id = ExprId::new(id_prefix)?;
        match self {
            Self::Literal(value) => Ok(Expression::literal(id, value.clone())),
            Self::Column { qualifier, name } => {
                let raw = match qualifier {
                    Some(q) => format!("{q}.{name}"),
                    None => name.clone(),
                };
                Ok(Expression::column(id, ColumnRef::new(raw)?))
            }
            Self::Parameter(name) => Err(ShardLoomError::InvalidOperation(format!(
                "unresolved SQL parameter :{name} cannot be converted into a literal core expression; bind parameters first; fallback_attempted=false external_engine_invoked=false"
            ))),
            Self::Unary { op, expr } => Ok(Expression::new(
                id,
                ExpressionKind::Unary {
                    op: *op,
                    expr: Box::new(expr.try_into_core_expression(&format!("{id_prefix}.u"))?),
                },
            )),
            Self::Binary { left, op, right } => Ok(Expression::new(
                id,
                ExpressionKind::Binary {
                    left: Box::new(left.try_into_core_expression(&format!("{id_prefix}.l"))?),
                    op: *op,
                    right: Box::new(right.try_into_core_expression(&format!("{id_prefix}.r"))?),
                },
            )),
            Self::Compare { left, op, right } => Ok(Expression::new(
                id,
                ExpressionKind::Compare {
                    left: Box::new(left.try_into_core_expression(&format!("{id_prefix}.l"))?),
                    op: *op,
                    right: Box::new(right.try_into_core_expression(&format!("{id_prefix}.r"))?),
                },
            )),
            Self::Cast {
                expr,
                target_dtype,
                try_cast,
            } => {
                let inner = expr.try_into_core_expression(&format!("{id_prefix}.c"))?;
                let dtype = parse_core_dtype_label(target_dtype)?;
                let kind = if *try_cast {
                    ExpressionKind::TryCast {
                        expr: Box::new(inner),
                        target_dtype: dtype,
                    }
                } else {
                    ExpressionKind::Cast {
                        expr: Box::new(inner),
                        target_dtype: dtype,
                    }
                };
                Ok(Expression::new(id, kind))
            }
            Self::Array(items) => {
                let values = items
                    .iter()
                    .enumerate()
                    .map(|(i, item)| item.try_into_core_expression(&format!("{id_prefix}.{i}")))
                    .collect::<Result<Vec<_>>>()?;
                Ok(Expression::new(id, ExpressionKind::List { values }))
            }
            Self::Function {
                name,
                args,
                distinct: false,
                filter: None,
                order_by,
                over: None,
            } if order_by.is_empty() => {
                let lowered = args
                    .iter()
                    .enumerate()
                    .map(|(i, arg)| arg.try_into_core_expression(&format!("{id_prefix}.{i}")))
                    .collect::<Result<Vec<_>>>()?;
                Ok(Expression::new(
                    id,
                    ExpressionKind::FunctionCall {
                        name: name.to_ascii_lowercase(),
                        args: lowered,
                    },
                ))
            }
            other => Err(ShardLoomError::NotImplemented(format!(
                "expression variant `{other:?}` requires relational lowering before core Expression evaluation; fallback_attempted=false external_engine_invoked=false"
            ))),
        }
    }
}

pub(crate) fn bind_ast_expr(
    expr: &SqlExpr,
    diagnostics: &mut Vec<Diagnostic>,
) -> Result<LogicalExpr> {
    match expr {
        SqlExpr::Identifier(ident) => Ok(LogicalExpr::Column {
            qualifier: None,
            name: ident.value.clone(),
        }),
        SqlExpr::CompoundIdentifier(parts) => bind_compound_identifier(parts),
        SqlExpr::Nested(inner) => bind_ast_expr(inner, diagnostics),
        SqlExpr::Value(v) => bind_value(&v.value, diagnostics),
        SqlExpr::TypedString(ts) => bind_typed_string(ts, diagnostics),
        SqlExpr::UnaryOp { op, expr } => bind_unary(*op, expr, diagnostics),
        SqlExpr::BinaryOp { left, op, right } => bind_binary(left, op, right, diagnostics),
        SqlExpr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => bind_case(
            operand.as_deref(),
            conditions,
            else_result.as_deref(),
            diagnostics,
        ),
        SqlExpr::Function(func) => bind_function(func, diagnostics),
        SqlExpr::IsNull(_)
        | SqlExpr::IsNotNull(_)
        | SqlExpr::IsTrue(_)
        | SqlExpr::IsNotTrue(_)
        | SqlExpr::IsFalse(_)
        | SqlExpr::IsNotFalse(_)
        | SqlExpr::IsDistinctFrom(_, _)
        | SqlExpr::IsNotDistinctFrom(_, _)
        | SqlExpr::Between { .. }
        | SqlExpr::In { .. }
        | SqlExpr::Like { .. }
        | SqlExpr::ILike { .. } => bind_predicate_expr(expr, diagnostics),
        _ => bind_extended_expr(expr, diagnostics),
    }
}

fn bind_predicate_expr(expr: &SqlExpr, diagnostics: &mut Vec<Diagnostic>) -> Result<LogicalExpr> {
    match expr {
        SqlExpr::IsNull(inner) => Ok(LogicalExpr::Unary {
            op: UnaryOp::IsNull,
            expr: Box::new(bind_ast_expr(inner, diagnostics)?),
        }),
        SqlExpr::IsNotNull(inner) => Ok(LogicalExpr::Unary {
            op: UnaryOp::IsNotNull,
            expr: Box::new(bind_ast_expr(inner, diagnostics)?),
        }),
        SqlExpr::IsTrue(inner) => Ok(LogicalExpr::IsDistinctFrom {
            left: Box::new(bind_ast_expr(inner, diagnostics)?),
            right: Box::new(LogicalExpr::Literal(ScalarValue::Boolean(true))),
            negated: true,
        }),
        SqlExpr::IsNotTrue(inner) => Ok(LogicalExpr::IsDistinctFrom {
            left: Box::new(bind_ast_expr(inner, diagnostics)?),
            right: Box::new(LogicalExpr::Literal(ScalarValue::Boolean(true))),
            negated: false,
        }),
        SqlExpr::IsFalse(inner) => Ok(LogicalExpr::IsDistinctFrom {
            left: Box::new(bind_ast_expr(inner, diagnostics)?),
            right: Box::new(LogicalExpr::Literal(ScalarValue::Boolean(false))),
            negated: true,
        }),
        SqlExpr::IsNotFalse(inner) => Ok(LogicalExpr::IsDistinctFrom {
            left: Box::new(bind_ast_expr(inner, diagnostics)?),
            right: Box::new(LogicalExpr::Literal(ScalarValue::Boolean(false))),
            negated: false,
        }),
        SqlExpr::IsDistinctFrom(left, right) => Ok(LogicalExpr::IsDistinctFrom {
            left: Box::new(bind_ast_expr(left, diagnostics)?),
            right: Box::new(bind_ast_expr(right, diagnostics)?),
            negated: false,
        }),
        SqlExpr::IsNotDistinctFrom(left, right) => Ok(LogicalExpr::IsDistinctFrom {
            left: Box::new(bind_ast_expr(left, diagnostics)?),
            right: Box::new(bind_ast_expr(right, diagnostics)?),
            negated: true,
        }),
        SqlExpr::Between {
            expr,
            negated,
            low,
            high,
        } => Ok(LogicalExpr::Between {
            expr: Box::new(bind_ast_expr(expr, diagnostics)?),
            low: Box::new(bind_ast_expr(low, diagnostics)?),
            high: Box::new(bind_ast_expr(high, diagnostics)?),
            negated: *negated,
        }),
        SqlExpr::In {
            expr,
            target,
            negated,
            global,
        } => bind_in_expr(expr, target, *negated, *global, diagnostics),
        SqlExpr::Like {
            negated,
            any: false,
            expr,
            pattern,
            escape_char,
        } => bind_like_expr(
            expr,
            pattern,
            escape_char.as_deref(),
            *negated,
            false,
            diagnostics,
        ),
        SqlExpr::ILike {
            negated,
            any: false,
            expr,
            pattern,
            escape_char,
        } => bind_like_expr(
            expr,
            pattern,
            escape_char.as_deref(),
            *negated,
            true,
            diagnostics,
        ),
        other => {
            let msg = format!("unsupported SQL predicate expression syntax `{other}`");
            diagnostics.push(unsupported_diag("sql.expression", &msg));
            Err(unsupported_sql_error(&msg))
        }
    }
}

fn bind_extended_expr(expr: &SqlExpr, diagnostics: &mut Vec<Diagnostic>) -> Result<LogicalExpr> {
    match expr {
        SqlExpr::Cast {
            kind,
            expr,
            data_type,
            format,
        } => {
            if format.is_some() {
                return Err(unsupported_sql_error(
                    "CAST(... FORMAT ...) is not admitted",
                ));
            }
            let try_cast = matches!(kind, CastKind::TryCast | CastKind::SafeCast);
            Ok(LogicalExpr::Cast {
                expr: Box::new(bind_ast_expr(expr, diagnostics)?),
                target_dtype: normalize_sql_dtype(data_type, diagnostics)?,
                try_cast,
            })
        }
        SqlExpr::Extract { .. }
        | SqlExpr::Trim { .. }
        | SqlExpr::Substring { .. }
        | SqlExpr::Position { .. } => bind_text_special_expr(expr, diagnostics),
        SqlExpr::Exists { subquery, negated } => Ok(LogicalExpr::Exists {
            subquery: Box::new(bind_ast_query(subquery, diagnostics)?),
            negated: *negated,
        }),
        SqlExpr::Subquery(query) => Ok(LogicalExpr::ScalarSubquery(Box::new(bind_ast_query(
            query,
            diagnostics,
        )?))),
        SqlExpr::Interval(interval) => {
            let value = match interval.value.as_ref() {
                SqlExpr::Value(v) => match &v.value {
                    SqlValue::SingleQuotedString(s) | SqlValue::Number(s, _) => s.clone(),
                    other => other.to_string(),
                },
                other => other.to_string(),
            };
            Ok(LogicalExpr::Interval {
                value,
                unit: interval.leading_field.as_ref().map(ToString::to_string),
            })
        }
        SqlExpr::Array(arr) => Ok(LogicalExpr::Array(
            arr.elem
                .iter()
                .map(|e| bind_ast_expr(e, diagnostics))
                .collect::<Result<Vec<_>>>()?,
        )),
        SqlExpr::Tuple(items) => Ok(LogicalExpr::Tuple(
            items
                .iter()
                .map(|e| bind_ast_expr(e, diagnostics))
                .collect::<Result<Vec<_>>>()?,
        )),
        SqlExpr::Wildcard(_) => Ok(LogicalExpr::Wildcard { qualifier: None }),
        SqlExpr::QualifiedWildcard(name, _) => Ok(LogicalExpr::Wildcard {
            qualifier: Some(name.to_string()),
        }),
        SqlExpr::Collate { collation, .. } => {
            let msg = format!(
                "SQL COLLATE ({collation}) is not admitted; UTF-8 comparisons remain codepoint comparisons"
            );
            diagnostics.push(unsupported_diag("sql.collate", &msg));
            Err(unsupported_sql_error(&msg))
        }
        other => {
            let msg = format!("unsupported SQL expression syntax `{other}`");
            diagnostics.push(unsupported_diag("sql.expression", &msg));
            Err(unsupported_sql_error(&msg))
        }
    }
}

fn bind_text_special_expr(
    expr: &SqlExpr,
    diagnostics: &mut Vec<Diagnostic>,
) -> Result<LogicalExpr> {
    match expr {
        SqlExpr::Extract { field, expr, .. } => Ok(LogicalExpr::Function {
            name: "extract".to_string(),
            args: vec![
                LogicalExpr::Literal(ScalarValue::Utf8(field.to_string().to_ascii_lowercase())),
                bind_ast_expr(expr, diagnostics)?,
            ],
            distinct: false,
            filter: None,
            order_by: Vec::new(),
            over: None,
        }),
        SqlExpr::Trim {
            expr,
            trim_where,
            trim_what,
            trim_characters,
        } => bind_trim(
            expr,
            *trim_where,
            trim_what.as_deref(),
            trim_characters.as_deref(),
            diagnostics,
        ),
        SqlExpr::Substring {
            expr,
            substring_from,
            substring_for,
            ..
        } => bind_substring(
            expr,
            substring_from.as_deref(),
            substring_for.as_deref(),
            diagnostics,
        ),
        SqlExpr::Position { expr, r#in } => Ok(LogicalExpr::Function {
            name: "position".to_string(),
            args: vec![
                bind_ast_expr(expr, diagnostics)?,
                bind_ast_expr(r#in, diagnostics)?,
            ],
            distinct: false,
            filter: None,
            order_by: Vec::new(),
            over: None,
        }),
        _ => unreachable!("non-text-special expr dispatched to bind_text_special_expr"),
    }
}

fn bind_compound_identifier(parts: &[ast::Ident]) -> Result<LogicalExpr> {
    match parts {
        [] => Err(unsupported_sql_error("empty identifier reference")),
        [only] => Ok(LogicalExpr::Column {
            qualifier: None,
            name: only.value.clone(),
        }),
        [qualifier, name] => Ok(LogicalExpr::Column {
            qualifier: Some(qualifier.value.clone()),
            name: name.value.clone(),
        }),
        _ => Err(unsupported_sql_error(
            "qualified columns admit at most <alias>.<column> syntax",
        )),
    }
}

fn bind_value(value: &SqlValue, diagnostics: &mut Vec<Diagnostic>) -> Result<LogicalExpr> {
    match value {
        SqlValue::Null => Ok(LogicalExpr::Literal(ScalarValue::Null)),
        SqlValue::Boolean(b) => Ok(LogicalExpr::Literal(ScalarValue::Boolean(*b))),
        SqlValue::Number(raw, _) => {
            if let Ok(i) = raw.parse::<i64>() {
                Ok(LogicalExpr::Literal(ScalarValue::Int64(i)))
            } else if let Ok(u) = raw.parse::<u64>() {
                Ok(LogicalExpr::Literal(ScalarValue::UInt64(u)))
            } else if let Ok(f) = raw.parse::<f64>()
                && f.is_finite()
            {
                Ok(LogicalExpr::Literal(ScalarValue::Float64(f)))
            } else {
                Err(unsupported_sql_error(&format!(
                    "numeric literal `{raw}` exceeds admitted int64/uint64/finite float64 range"
                )))
            }
        }
        SqlValue::SingleQuotedString(s) => Ok(LogicalExpr::Literal(ScalarValue::Utf8(s.clone()))),
        SqlValue::HexStringLiteral(hex) => {
            let bytes = decode_hex_bytes(hex)?;
            Ok(LogicalExpr::Literal(ScalarValue::Binary(bytes)))
        }
        SqlValue::Placeholder(raw) => {
            if let Some(name) = raw.strip_prefix(':') {
                validate_named_param(name)?;
                Ok(LogicalExpr::Parameter(name.to_string()))
            } else {
                let msg =
                    "positional parameters are not admitted; use named scalar parameters :name";
                diagnostics.push(unsupported_diag("sql.positional_parameter", msg));
                Err(unsupported_sql_error(msg))
            }
        }
        other => {
            let msg = format!("unsupported SQL literal `{other}`");
            diagnostics.push(unsupported_diag("sql.literal", &msg));
            Err(unsupported_sql_error(&msg))
        }
    }
}

fn bind_typed_string(
    ts: &ast::TypedString,
    diagnostics: &mut Vec<Diagnostic>,
) -> Result<LogicalExpr> {
    let raw_str = match &ts.value.value {
        SqlValue::SingleQuotedString(s) => s.as_str(),
        _ => {
            return Err(unsupported_sql_error(
                "typed literal requires a single-quoted string",
            ));
        }
    };
    let dtype_label = normalize_sql_dtype(&ts.data_type, diagnostics)?;
    match dtype_label.as_str() {
        "date32" => {
            let days = parse_iso_date32(raw_str)?;
            Ok(LogicalExpr::Literal(ScalarValue::Date32(days)))
        }
        "timestamp_micros" => {
            let micros = parse_iso_timestamp_micros(raw_str)?;
            Ok(LogicalExpr::Literal(ScalarValue::TimestampMicros(micros)))
        }
        "binary" => Ok(LogicalExpr::Literal(ScalarValue::Binary(
            raw_str.as_bytes().to_vec(),
        ))),
        _ => Ok(LogicalExpr::Cast {
            expr: Box::new(LogicalExpr::Literal(ScalarValue::Utf8(raw_str.to_string()))),
            target_dtype: dtype_label,
            try_cast: false,
        }),
    }
}

fn bind_unary(
    op: SqlUnaryOp,
    expr: &SqlExpr,
    diagnostics: &mut Vec<Diagnostic>,
) -> Result<LogicalExpr> {
    if matches!(op, SqlUnaryOp::Minus)
        && let SqlExpr::Value(v) = expr
        && let SqlValue::Number(raw, _) = &v.value
    {
        let signed = format!("-{raw}");
        if let Ok(i) = signed.parse::<i64>() {
            return Ok(LogicalExpr::Literal(ScalarValue::Int64(i)));
        }
        if let Ok(f) = signed.parse::<f64>()
            && f.is_finite()
        {
            return Ok(LogicalExpr::Literal(ScalarValue::Float64(f)));
        }
    }
    let inner = bind_ast_expr(expr, diagnostics)?;
    match op {
        SqlUnaryOp::Plus => Ok(inner),
        SqlUnaryOp::Minus => Ok(LogicalExpr::Unary {
            op: UnaryOp::Negate,
            expr: Box::new(inner),
        }),
        SqlUnaryOp::Not => Ok(LogicalExpr::Unary {
            op: UnaryOp::Not,
            expr: Box::new(inner),
        }),
        other => {
            let msg = format!("unsupported SQL unary operator `{other}`");
            diagnostics.push(unsupported_diag("sql.unary_operator", &msg));
            Err(unsupported_sql_error(&msg))
        }
    }
}

fn bind_binary(
    left: &SqlExpr,
    op: &SqlBinaryOp,
    right: &SqlExpr,
    diagnostics: &mut Vec<Diagnostic>,
) -> Result<LogicalExpr> {
    let lhs = Box::new(bind_ast_expr(left, diagnostics)?);
    let rhs = Box::new(bind_ast_expr(right, diagnostics)?);
    match op {
        SqlBinaryOp::Plus => Ok(LogicalExpr::Binary {
            left: lhs,
            op: BinaryOp::Add,
            right: rhs,
        }),
        SqlBinaryOp::Minus => Ok(LogicalExpr::Binary {
            left: lhs,
            op: BinaryOp::Subtract,
            right: rhs,
        }),
        SqlBinaryOp::Multiply => Ok(LogicalExpr::Binary {
            left: lhs,
            op: BinaryOp::Multiply,
            right: rhs,
        }),
        SqlBinaryOp::Divide => Ok(LogicalExpr::Binary {
            left: lhs,
            op: BinaryOp::Divide,
            right: rhs,
        }),
        SqlBinaryOp::Modulo => Ok(LogicalExpr::Binary {
            left: lhs,
            op: BinaryOp::Remainder,
            right: rhs,
        }),
        SqlBinaryOp::And => Ok(LogicalExpr::Binary {
            left: lhs,
            op: BinaryOp::And,
            right: rhs,
        }),
        SqlBinaryOp::Or => Ok(LogicalExpr::Binary {
            left: lhs,
            op: BinaryOp::Or,
            right: rhs,
        }),
        SqlBinaryOp::Eq => Ok(LogicalExpr::Compare {
            left: lhs,
            op: ComparisonOp::Eq,
            right: rhs,
        }),
        SqlBinaryOp::NotEq => Ok(LogicalExpr::Compare {
            left: lhs,
            op: ComparisonOp::NotEq,
            right: rhs,
        }),
        SqlBinaryOp::Lt => Ok(LogicalExpr::Compare {
            left: lhs,
            op: ComparisonOp::Lt,
            right: rhs,
        }),
        SqlBinaryOp::LtEq => Ok(LogicalExpr::Compare {
            left: lhs,
            op: ComparisonOp::LtEq,
            right: rhs,
        }),
        SqlBinaryOp::Gt => Ok(LogicalExpr::Compare {
            left: lhs,
            op: ComparisonOp::Gt,
            right: rhs,
        }),
        SqlBinaryOp::GtEq => Ok(LogicalExpr::Compare {
            left: lhs,
            op: ComparisonOp::GtEq,
            right: rhs,
        }),
        SqlBinaryOp::StringConcat => Ok(LogicalExpr::Function {
            name: "concat".to_string(),
            args: vec![*lhs, *rhs],
            distinct: false,
            filter: None,
            order_by: Vec::new(),
            over: None,
        }),
        other => {
            let msg = format!("unsupported SQL binary operator `{other}`");
            diagnostics.push(unsupported_diag("sql.binary_operator", &msg));
            Err(unsupported_sql_error(&msg))
        }
    }
}

fn bind_in_expr(
    expr: &SqlExpr,
    target: &InTarget,
    negated: bool,
    global: bool,
    diagnostics: &mut Vec<Diagnostic>,
) -> Result<LogicalExpr> {
    if global {
        return Err(unsupported_sql_error(
            "GLOBAL IN modifier is not admitted; use ordinary IN",
        ));
    }
    let lhs = Box::new(bind_ast_expr(expr, diagnostics)?);
    match target {
        InTarget::List(items) => Ok(LogicalExpr::InList {
            expr: lhs,
            list: items
                .iter()
                .map(|item| bind_ast_expr(item, diagnostics))
                .collect::<Result<Vec<_>>>()?,
            negated,
        }),
        InTarget::Subquery(query) => Ok(LogicalExpr::InSubquery {
            expr: lhs,
            subquery: Box::new(bind_ast_query(query, diagnostics)?),
            negated,
        }),
        other => Err(unsupported_sql_error(&format!(
            "unsupported IN target `{other}`"
        ))),
    }
}

fn bind_like_expr(
    expr: &SqlExpr,
    pattern: &SqlExpr,
    escape_char: Option<&SqlExpr>,
    negated: bool,
    case_insensitive: bool,
    diagnostics: &mut Vec<Diagnostic>,
) -> Result<LogicalExpr> {
    let escape = match escape_char {
        Some(SqlExpr::Value(v)) => match &v.value {
            SqlValue::SingleQuotedString(s) => Some(s.clone()),
            _ => return Err(unsupported_sql_error("LIKE ESCAPE must be a string")),
        },
        Some(_) => return Err(unsupported_sql_error("LIKE ESCAPE must be a string")),
        None => None,
    };
    Ok(LogicalExpr::Like {
        expr: Box::new(bind_ast_expr(expr, diagnostics)?),
        pattern: Box::new(bind_ast_expr(pattern, diagnostics)?),
        escape,
        negated,
        case_insensitive,
    })
}

fn bind_case(
    operand: Option<&SqlExpr>,
    conditions: &[ast::CaseWhen],
    else_result: Option<&SqlExpr>,
    diagnostics: &mut Vec<Diagnostic>,
) -> Result<LogicalExpr> {
    let operand = operand
        .map(|expr| bind_ast_expr(expr, diagnostics).map(Box::new))
        .transpose()?;
    let when_then = conditions
        .iter()
        .map(|branch| {
            Ok((
                bind_ast_expr(&branch.condition, diagnostics)?,
                bind_ast_expr(&branch.result, diagnostics)?,
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    let else_expr = else_result
        .map(|expr| bind_ast_expr(expr, diagnostics).map(Box::new))
        .transpose()?;
    Ok(LogicalExpr::Case {
        operand,
        when_then,
        else_expr,
    })
}

fn validate_null_treatment(
    nt: ast::NullTreatment,
    fn_lower: &str,
    has_over: bool,
    diagnostics: &mut Vec<Diagnostic>,
) -> Result<()> {
    match nt {
        ast::NullTreatment::IgnoreNulls => {
            let msg = "window and aggregate IGNORE NULLS modifiers are not admitted";
            diagnostics.push(unsupported_diag("sql.function", msg));
            Err(unsupported_sql_error(msg))
        }
        ast::NullTreatment::RespectNulls => {
            if has_over
                && matches!(
                    fn_lower,
                    "first_value" | "last_value" | "nth_value" | "lag" | "lead"
                )
            {
                Ok(())
            } else {
                let msg = "RESPECT NULLS is only admitted on window navigation functions";
                diagnostics.push(unsupported_diag("sql.function", msg));
                Err(unsupported_sql_error(msg))
            }
        }
    }
}

fn bind_function_arg_list(
    list: &ast::FunctionArgumentList,
    fn_lower: &str,
    has_over: bool,
    diagnostics: &mut Vec<Diagnostic>,
) -> Result<(Vec<LogicalExpr>, bool, Vec<LogicalSortKey>)> {
    let distinct = matches!(
        list.duplicate_treatment,
        Some(ast::DuplicateTreatment::Distinct)
    );
    let mut args = Vec::with_capacity(list.args.len());
    for arg in &list.args {
        args.push(bind_function_arg(arg, diagnostics)?);
    }
    let mut order_by = Vec::new();
    for clause in &list.clauses {
        match clause {
            FunctionArgumentClause::OrderBy(keys) => {
                for key in keys {
                    order_by.push(bind_sort_key(key, diagnostics)?);
                }
            }
            FunctionArgumentClause::Separator(sep) => {
                let SqlValue::SingleQuotedString(text) = &sep.value else {
                    return Err(unsupported_sql_error(
                        "SEPARATOR clause requires a single-quoted string literal",
                    ));
                };
                args.push(LogicalExpr::Literal(ScalarValue::Utf8(text.clone())));
            }
            FunctionArgumentClause::IgnoreOrRespectNulls(nt) => {
                validate_null_treatment(*nt, fn_lower, has_over, diagnostics)?;
            }
            other => {
                return Err(unsupported_sql_error(&format!(
                    "unsupported function argument clause `{other}`"
                )));
            }
        }
    }
    Ok((args, distinct, order_by))
}

fn bind_function(func: &ast::Function, diagnostics: &mut Vec<Diagnostic>) -> Result<LogicalExpr> {
    if func.uses_odbc_syntax {
        return Err(unsupported_sql_error(
            "ODBC `{fn ...}` function syntax is not admitted",
        ));
    }
    if !matches!(func.parameters, FunctionArguments::None) {
        return Err(unsupported_sql_error(
            "parametric SQL function syntax is not admitted",
        ));
    }
    let fn_name = func.name.to_string();
    let fn_lower = fn_name.to_ascii_lowercase();
    let has_over = func.over.is_some();
    if let Some(nt) = func.null_treatment {
        validate_null_treatment(nt, &fn_lower, has_over, diagnostics)?;
    }

    let mut args = Vec::new();
    let mut distinct = false;
    let mut order_by = Vec::new();
    match &func.args {
        FunctionArguments::None => {}
        FunctionArguments::Subquery(query) => {
            args.push(LogicalExpr::ScalarSubquery(Box::new(bind_ast_query(
                query,
                diagnostics,
            )?)));
        }
        FunctionArguments::List(list) => {
            (args, distinct, order_by) =
                bind_function_arg_list(list, &fn_lower, has_over, diagnostics)?;
        }
    }
    let filter = func
        .filter
        .as_deref()
        .map(|expr| bind_ast_expr(expr, diagnostics).map(Box::new))
        .transpose()?;
    for key in &func.within_group {
        order_by.push(bind_sort_key(key, diagnostics)?);
    }
    let over = func
        .over
        .as_ref()
        .map(|window| bind_window_type(window, diagnostics))
        .transpose()?;
    Ok(LogicalExpr::Function {
        name: fn_name,
        args,
        distinct,
        filter,
        order_by,
        over,
    })
}

pub(crate) fn bind_function_arg(
    arg: &FunctionArg,
    diagnostics: &mut Vec<Diagnostic>,
) -> Result<LogicalExpr> {
    match arg {
        FunctionArg::Unnamed(expr) => match expr {
            FunctionArgExpr::Expr(e) => bind_ast_expr(e, diagnostics),
            FunctionArgExpr::Wildcard => Ok(LogicalExpr::Wildcard { qualifier: None }),
            FunctionArgExpr::WildcardWithOptions(opts) => {
                crate::sql_binder::validate_wildcard_options(opts, diagnostics)?;
                Ok(LogicalExpr::Wildcard { qualifier: None })
            }
            FunctionArgExpr::QualifiedWildcard(name) => Ok(LogicalExpr::Wildcard {
                qualifier: Some(name.to_string()),
            }),
        },
        FunctionArg::Named { .. } | FunctionArg::ExprNamed { .. } => Err(unsupported_sql_error(
            "named SQL function arguments are not admitted; pass positional arguments",
        )),
    }
}

fn bind_trim(
    expr: &SqlExpr,
    trim_where: Option<TrimWhereField>,
    trim_what: Option<&SqlExpr>,
    trim_characters: Option<&[SqlExpr]>,
    diagnostics: &mut Vec<Diagnostic>,
) -> Result<LogicalExpr> {
    let fn_name = match trim_where {
        Some(TrimWhereField::Leading) => "ltrim",
        Some(TrimWhereField::Trailing) => "rtrim",
        Some(TrimWhereField::Both) | None => "trim",
    };
    let mut args = vec![bind_ast_expr(expr, diagnostics)?];
    if let Some(what) = trim_what {
        args.push(bind_ast_expr(what, diagnostics)?);
    } else if let Some(chars) = trim_characters {
        for ch in chars {
            args.push(bind_ast_expr(ch, diagnostics)?);
        }
    }
    Ok(LogicalExpr::Function {
        name: fn_name.to_string(),
        args,
        distinct: false,
        filter: None,
        order_by: Vec::new(),
        over: None,
    })
}

fn bind_substring(
    expr: &SqlExpr,
    from: Option<&SqlExpr>,
    len: Option<&SqlExpr>,
    diagnostics: &mut Vec<Diagnostic>,
) -> Result<LogicalExpr> {
    let mut args = vec![bind_ast_expr(expr, diagnostics)?];
    match (from, len) {
        (Some(from), Some(len)) => {
            args.push(bind_ast_expr(from, diagnostics)?);
            args.push(bind_ast_expr(len, diagnostics)?);
        }
        (Some(from), None) => {
            args.push(bind_ast_expr(from, diagnostics)?);
        }
        (None, Some(len)) => {
            // SQL `SUBSTRING(str FOR len)` omits `FROM`, so default start is 1-based offset 1.
            args.push(LogicalExpr::Literal(ScalarValue::Int64(1)));
            args.push(bind_ast_expr(len, diagnostics)?);
        }
        (None, None) => {
            return Err(unsupported_sql_error(
                "SUBSTRING requires at least a FROM or FOR argument",
            ));
        }
    }
    Ok(LogicalExpr::Function {
        name: "substring".to_string(),
        args,
        distinct: false,
        filter: None,
        order_by: Vec::new(),
        over: None,
    })
}

pub(crate) fn bind_sort_key(
    key: &ast::OrderByExpr,
    diagnostics: &mut Vec<Diagnostic>,
) -> Result<LogicalSortKey> {
    if key.with_fill.is_some() {
        return Err(unsupported_sql_error("ORDER BY WITH FILL is not admitted"));
    }
    let descending = match &key.options.sort {
        None | Some(ast::OrderBySort::Asc) => false,
        Some(ast::OrderBySort::Desc) => true,
        Some(ast::OrderBySort::Using(op)) => {
            return Err(unsupported_sql_error(&format!(
                "ORDER BY USING `{op}` is not admitted"
            )));
        }
    };
    Ok(LogicalSortKey {
        expr: bind_ast_expr(&key.expr, diagnostics)?,
        descending,
        nulls_first: key.options.nulls_first,
    })
}

pub(crate) fn bind_window_spec_ast(
    spec: &ast::WindowSpec,
    diagnostics: &mut Vec<Diagnostic>,
) -> Result<LogicalWindowSpec> {
    let partition_by = spec
        .partition_by
        .iter()
        .map(|e| bind_ast_expr(e, diagnostics))
        .collect::<Result<Vec<_>>>()?;
    let order_by = spec
        .order_by
        .iter()
        .map(|k| bind_sort_key(k, diagnostics))
        .collect::<Result<Vec<_>>>()?;
    let frame = spec
        .window_frame
        .as_ref()
        .map(|f| -> Result<LogicalWindowFrame> {
            let units = match f.units {
                WindowFrameUnits::Rows => LogicalWindowUnits::Rows,
                WindowFrameUnits::Range => LogicalWindowUnits::Range,
                WindowFrameUnits::Groups => LogicalWindowUnits::Groups,
            };
            Ok(LogicalWindowFrame {
                units,
                start: f.start_bound.to_string(),
                end: f.end_bound.as_ref().map(ToString::to_string),
                exclusion: None,
            })
        })
        .transpose()?;
    Ok(LogicalWindowSpec {
        name: spec.window_name.as_ref().map(|n| n.value.clone()),
        partition_by,
        order_by,
        frame,
    })
}

fn bind_window_type(
    window: &WindowType,
    diagnostics: &mut Vec<Diagnostic>,
) -> Result<LogicalWindowSpec> {
    match window {
        WindowType::WindowSpec(spec) => bind_window_spec_ast(spec, diagnostics),
        WindowType::NamedWindow(ident) => Ok(LogicalWindowSpec {
            name: Some(ident.value.clone()),
            partition_by: Vec::new(),
            order_by: Vec::new(),
            frame: None,
        }),
    }
}

fn normalize_sql_dtype(dtype: &SqlDataType, diagnostics: &mut Vec<Diagnostic>) -> Result<String> {
    match dtype {
        SqlDataType::Boolean | SqlDataType::Bool => Ok("boolean".to_string()),
        SqlDataType::BigInt(_)
        | SqlDataType::Int64
        | SqlDataType::Int(_)
        | SqlDataType::Integer(_)
        | SqlDataType::SmallInt(_)
        | SqlDataType::TinyInt(_) => Ok("int64".to_string()),
        SqlDataType::BigIntUnsigned(_) | SqlDataType::UInt64 => Ok("uint64".to_string()),
        SqlDataType::Double(_)
        | SqlDataType::DoublePrecision
        | SqlDataType::Float(_)
        | SqlDataType::Float64
        | SqlDataType::Real => Ok("float64".to_string()),
        SqlDataType::Varchar(_)
        | SqlDataType::Char(_)
        | SqlDataType::Character(_)
        | SqlDataType::Text
        | SqlDataType::String(_) => Ok("utf8".to_string()),
        SqlDataType::Binary(_)
        | SqlDataType::Varbinary(_)
        | SqlDataType::Blob(_)
        | SqlDataType::Bytea => Ok("binary".to_string()),
        SqlDataType::Date => Ok("date32".to_string()),
        SqlDataType::Timestamp(_, tz) => match tz {
            ast::TimezoneInfo::None | ast::TimezoneInfo::WithoutTimeZone => {
                Ok("timestamp_micros".to_string())
            }
            _ => {
                let msg = "timezone database semantics are not admitted; use UTC or naive timestamp_micros";
                diagnostics.push(unsupported_diag("sql.timezone_dtype", msg));
                Err(unsupported_sql_error(msg))
            }
        },
        SqlDataType::Decimal(info) | SqlDataType::Numeric(info) => match info {
            ast::ExactNumberInfo::PrecisionAndScale(p, s) => Ok(format!("decimal128({p},{s})")),
            ast::ExactNumberInfo::Precision(p) => Ok(format!("decimal128({p},0)")),
            ast::ExactNumberInfo::None => Ok("decimal128(38,10)".to_string()),
        },
        other => Ok(other.to_string().to_ascii_lowercase()),
    }
}

/// Parses a normalized core `DType` label into a [`LogicalDType`].
///
/// # Errors
/// Returns [`ShardLoomError::NotImplemented`] when `label` is not an admitted
/// core dtype string.
pub fn parse_core_dtype_label(label: &str) -> Result<LogicalDType> {
    match label {
        "boolean" => Ok(LogicalDType::Boolean),
        "int64" => Ok(LogicalDType::Int64),
        "uint64" => Ok(LogicalDType::UInt64),
        "float64" => Ok(LogicalDType::Float64),
        "utf8" => Ok(LogicalDType::Utf8),
        "binary" => Ok(LogicalDType::Binary),
        "date32" => Ok(LogicalDType::Date32),
        "timestamp_micros" => Ok(LogicalDType::TimestampMicros),
        other if other.starts_with("decimal128(") => Ok(LogicalDType::Extension(other.to_string())),
        other => Err(unsupported_sql_error(&format!(
            "unsupported cast target type `{other}`"
        ))),
    }
}

fn decode_hex_bytes(hex: &str) -> Result<Vec<u8>> {
    if !hex.len().is_multiple_of(2) {
        return Err(unsupported_sql_error(
            "binary hex literals require an even number of hexadecimal digits",
        ));
    }
    hex.as_bytes()
        .chunks(2)
        .map(|pair| {
            let text = std::str::from_utf8(pair).map_err(|_| {
                unsupported_sql_error("binary hex literals admit hexadecimal digits only")
            })?;
            u8::from_str_radix(text, 16).map_err(|_| {
                unsupported_sql_error("binary hex literals admit hexadecimal digits only")
            })
        })
        .collect()
}

fn validate_named_param(name: &str) -> Result<()> {
    let valid = !name.is_empty()
        && name
            .chars()
            .next()
            .is_some_and(|ch| ch == '_' || ch.is_ascii_alphabetic())
        && name
            .chars()
            .all(|ch| ch == '_' || ch.is_ascii_alphanumeric());
    if !valid {
        return Err(unsupported_sql_error(&format!(
            "invalid SQL named parameter `:{name}`"
        )));
    }
    Ok(())
}

pub(crate) fn unsupported_diag(feature: &str, message: &str) -> Diagnostic {
    Diagnostic::unsupported(
        DiagnosticCode::NotImplemented,
        feature,
        message,
        Some(
            "Rewrite query using supported ShardLoom SQL syntax without external fallback.".into(),
        ),
    )
}
