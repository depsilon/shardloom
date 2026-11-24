// SPDX-License-Identifier: Apache-2.0

//! Compiled contiguous columnar batch evaluator for `shardloom-core` expressions.
//! Falls back per-expression to row-wise `eval_expression` only for complex or
//! non-primitive function trees, preserving exact NULL and float policies.

use super::{
    EvalFailure, Expression, ExpressionEvaluationStatus, ExpressionInputRow, ExpressionKind,
    ScalarValue, cast_eval_value, eval_expression, eval_numeric_binary, i64_to_exact_f64,
    try_cast_eval_value, u64_to_exact_f64,
};
use crate::{BinaryOp, ComparisonOp, FloatPolicy, LogicalDType, Result, ShardLoomError, UnaryOp};
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, PartialEq)]
pub enum ColumnarColumn {
    Bool {
        values: Vec<bool>,
        valid: Vec<bool>,
    },
    Int64 {
        values: Vec<i64>,
        valid: Vec<bool>,
    },
    UInt64 {
        values: Vec<u64>,
        valid: Vec<bool>,
    },
    Float64 {
        values: Vec<f64>,
        valid: Vec<bool>,
    },
    Utf8 {
        values: Vec<String>,
        valid: Vec<bool>,
    },
    Date32 {
        values: Vec<i32>,
        valid: Vec<bool>,
    },
    TimestampMicros {
        values: Vec<i64>,
        valid: Vec<bool>,
    },
    ScalarFallback {
        values: Vec<ScalarValue>,
        dtype: LogicalDType,
    },
}

impl ColumnarColumn {
    #[must_use]
    pub fn len(&self) -> usize {
        match self {
            Self::Bool { values, .. } => values.len(),
            Self::Int64 { values, .. } | Self::TimestampMicros { values, .. } => values.len(),
            Self::UInt64 { values, .. } => values.len(),
            Self::Float64 { values, .. } => values.len(),
            Self::Utf8 { values, .. } => values.len(),
            Self::Date32 { values, .. } => values.len(),
            Self::ScalarFallback { values, .. } => values.len(),
        }
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[must_use]
    pub fn dtype(&self) -> LogicalDType {
        match self {
            Self::Bool { .. } => LogicalDType::Boolean,
            Self::Int64 { .. } => LogicalDType::Int64,
            Self::UInt64 { .. } => LogicalDType::UInt64,
            Self::Float64 { .. } => LogicalDType::Float64,
            Self::Utf8 { .. } => LogicalDType::Utf8,
            Self::Date32 { .. } => LogicalDType::Date32,
            Self::TimestampMicros { .. } => LogicalDType::TimestampMicros,
            Self::ScalarFallback { dtype, .. } => dtype.clone(),
        }
    }

    #[must_use]
    pub fn is_null_at(&self, row: usize) -> bool {
        match self {
            Self::Bool { valid, .. }
            | Self::Int64 { valid, .. }
            | Self::UInt64 { valid, .. }
            | Self::Float64 { valid, .. }
            | Self::Utf8 { valid, .. }
            | Self::Date32 { valid, .. }
            | Self::TimestampMicros { valid, .. } => !valid[row],
            Self::ScalarFallback { values, .. } => values[row].is_null(),
        }
    }

    #[must_use]
    pub fn get(&self, row: usize) -> ScalarValue {
        match self {
            Self::Bool { values, valid } => valid[row]
                .then_some(values[row])
                .map_or(ScalarValue::Null, ScalarValue::Boolean),
            Self::Int64 { values, valid } => valid[row]
                .then_some(values[row])
                .map_or(ScalarValue::Null, ScalarValue::Int64),
            Self::UInt64 { values, valid } => valid[row]
                .then_some(values[row])
                .map_or(ScalarValue::Null, ScalarValue::UInt64),
            Self::Float64 { values, valid } => valid[row]
                .then_some(values[row])
                .map_or(ScalarValue::Null, ScalarValue::Float64),
            Self::Utf8 { values, valid } => valid[row]
                .then(|| values[row].clone())
                .map_or(ScalarValue::Null, ScalarValue::Utf8),
            Self::Date32 { values, valid } => valid[row]
                .then_some(values[row])
                .map_or(ScalarValue::Null, ScalarValue::Date32),
            Self::TimestampMicros { values, valid } => valid[row]
                .then_some(values[row])
                .map_or(ScalarValue::Null, ScalarValue::TimestampMicros),
            Self::ScalarFallback { values, .. } => values[row].clone(),
        }
    }

    /// Converts homogeneous/nullable scalars into a compact typed `ColumnarColumn`.
    ///
    /// # Errors
    /// Returns [`ShardLoomError::InvalidOperation`] if scalar values mix incompatible types.
    pub fn from_scalars(values: Vec<ScalarValue>, dtype: LogicalDType) -> Result<Self> {
        let inferred = if dtype == LogicalDType::Unknown {
            values
                .iter()
                .find(|v| !v.is_null())
                .map_or(LogicalDType::Unknown, ScalarValue::dtype)
        } else {
            dtype
        };
        let len = values.len();
        macro_rules! build_typed {
            ($col:ident, $scalar:ident, $default:expr, $label:expr) => {{
                let mut out = Vec::with_capacity(len);
                let mut valid = Vec::with_capacity(len);
                for value in values {
                    match value {
                        ScalarValue::Null => {
                            out.push($default);
                            valid.push(false);
                        }
                        ScalarValue::$scalar(v) => {
                            out.push(v);
                            valid.push(true);
                        }
                        other => {
                            return Err(ShardLoomError::InvalidOperation(format!(
                                "columnar {} column cannot hold {}",
                                $label,
                                other.dtype().as_str()
                            )));
                        }
                    }
                }
                Ok(Self::$col { values: out, valid })
            }};
        }
        match inferred {
            LogicalDType::Boolean => build_typed!(Bool, Boolean, false, "boolean"),
            LogicalDType::Int64 => build_typed!(Int64, Int64, 0, "int64"),
            LogicalDType::UInt64 => build_typed!(UInt64, UInt64, 0, "uint64"),
            LogicalDType::Float64 => build_typed!(Float64, Float64, 0.0, "float64"),
            LogicalDType::Utf8 => build_typed!(Utf8, Utf8, String::new(), "utf8"),
            LogicalDType::Date32 => build_typed!(Date32, Date32, 0, "date32"),
            LogicalDType::TimestampMicros => {
                build_typed!(TimestampMicros, TimestampMicros, 0, "timestamp_micros")
            }
            dtype => Ok(Self::ScalarFallback { values, dtype }),
        }
    }

    #[must_use]
    pub fn to_scalars(&self) -> Vec<ScalarValue> {
        (0..self.len()).map(|row| self.get(row)).collect()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ColumnarBatch {
    rows: usize,
    columns: BTreeMap<String, ColumnarColumn>,
}

impl ColumnarBatch {
    /// Builds a validated `ColumnarBatch` from named columnar vectors.
    ///
    /// # Errors
    /// Returns [`ShardLoomError::InvalidOperation`] if any column's row count mismatches `rows`.
    pub fn new(rows: usize, columns: BTreeMap<String, ColumnarColumn>) -> Result<Self> {
        for (name, column) in &columns {
            if column.len() != rows {
                return Err(ShardLoomError::InvalidOperation(format!(
                    "column {name:?} has {} rows, expected {rows}",
                    column.len()
                )));
            }
        }
        Ok(Self { rows, columns })
    }

    /// Transposes row maps into typed contiguous `ColumnarColumn`s.
    ///
    /// # Errors
    /// Returns [`ShardLoomError::InvalidOperation`] if column schemas are inconsistent across rows.
    pub fn from_rows(rows: &[ExpressionInputRow]) -> Result<Self> {
        let count = rows.len();
        if count == 0 {
            return Ok(Self {
                rows: 0,
                columns: BTreeMap::new(),
            });
        }
        let mut names = BTreeSet::<String>::new();
        for row in rows {
            for name in row.keys() {
                names.insert(name.clone());
            }
        }
        let mut columns = BTreeMap::new();
        for name in &names {
            let mut scalars = Vec::with_capacity(count);
            for row in rows {
                scalars.push(row.get(name).cloned().unwrap_or(ScalarValue::Null));
            }
            columns.insert(
                name.clone(),
                ColumnarColumn::from_scalars(scalars, LogicalDType::Unknown)?,
            );
        }
        Self::new(count, columns)
    }

    #[must_use]
    pub fn to_rows(&self) -> Vec<ExpressionInputRow> {
        let mut rows = Vec::with_capacity(self.rows);
        for row_idx in 0..self.rows {
            let mut row = ExpressionInputRow::new();
            for (name, col) in &self.columns {
                row.insert(name.clone(), col.get(row_idx));
            }
            rows.push(row);
        }
        rows
    }

    #[must_use]
    pub const fn len(&self) -> usize {
        self.rows
    }

    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.rows == 0
    }

    #[must_use]
    pub fn column(&self, name: &str) -> Option<&ColumnarColumn> {
        self.columns.get(name)
    }
}

/// Evaluates an expression directly against a `ColumnarBatch` under strict float policy.
///
/// # Errors
/// Returns [`ShardLoomError`] with diagnostic details if evaluation fails or is unsupported.
pub fn evaluate_expression_batch(
    expression: &Expression,
    batch: &ColumnarBatch,
) -> Result<ColumnarColumn> {
    evaluate_expression_batch_with_policy(expression, batch, FloatPolicy::Strict)
}

/// Evaluates an expression directly against a `ColumnarBatch` under an explicit float policy.
///
/// # Errors
/// Returns [`ShardLoomError`] with diagnostic details if evaluation fails or is unsupported.
pub fn evaluate_expression_batch_with_policy(
    expression: &Expression,
    batch: &ColumnarBatch,
    float_policy: FloatPolicy,
) -> Result<ColumnarColumn> {
    eval_batch_inner(expression, batch, float_policy).map_err(|failure| failure_to_error(&failure))
}

fn failure_to_error(failure: &EvalFailure) -> ShardLoomError {
    match failure.status {
        ExpressionEvaluationStatus::Unsupported => {
            ShardLoomError::NotImplemented(failure.diagnostic.message.clone())
        }
        _ => ShardLoomError::InvalidOperation(failure.diagnostic.message.clone()),
    }
}

fn eval_batch_inner(
    expression: &Expression,
    batch: &ColumnarBatch,
    float_policy: FloatPolicy,
) -> super::EvalResult<ColumnarColumn> {
    let rows = batch.len();
    match &expression.kind {
        ExpressionKind::Literal(value) => Ok(eval_literal_batch(value, rows)),
        ExpressionKind::Column(column) => batch.column(column.as_str()).cloned().ok_or_else(|| {
            EvalFailure::invalid(
                "column_reference",
                format!(
                    "column {:?} is not present in the expression input row",
                    column.as_str()
                ),
            )
        }),
        ExpressionKind::Alias { expr, .. } => eval_batch_inner(expr, batch, float_policy),
        ExpressionKind::Unary { op, expr } => {
            let child = eval_batch_inner(expr, batch, float_policy)?;
            eval_unary_batch(*op, &child, float_policy)
        }
        ExpressionKind::Binary { left, op, right } => {
            let left_col = eval_batch_inner(left, batch, float_policy)?;
            let right_col = eval_batch_inner(right, batch, float_policy)?;
            eval_binary_batch(&left_col, *op, &right_col, float_policy)
        }
        ExpressionKind::Compare { left, op, right } => {
            let left_col = eval_batch_inner(left, batch, float_policy)?;
            let right_col = eval_batch_inner(right, batch, float_policy)?;
            eval_compare_batch(&left_col, *op, &right_col)
        }
        ExpressionKind::Cast { expr, target_dtype } => {
            let child = eval_batch_inner(expr, batch, float_policy)?;
            eval_cast_batch(&child, target_dtype, false, float_policy)
        }
        ExpressionKind::TryCast { expr, target_dtype } => {
            let child = eval_batch_inner(expr, batch, float_policy)?;
            eval_cast_batch(&child, target_dtype, true, float_policy)
        }
        _ => eval_scalar_fallback(expression, batch),
    }
}

fn eval_scalar_fallback(
    expression: &Expression,
    batch: &ColumnarBatch,
) -> super::EvalResult<ColumnarColumn> {
    let input_rows = batch.to_rows();
    let mut out = Vec::with_capacity(input_rows.len());
    let mut out_dtype = expression.dtype.clone().unwrap_or(LogicalDType::Unknown);
    for row in &input_rows {
        let evaluated = eval_expression(expression, row)?;
        if out_dtype == LogicalDType::Unknown && evaluated.dtype != LogicalDType::Unknown {
            out_dtype = evaluated.dtype;
        }
        out.push(evaluated.value);
    }
    ColumnarColumn::from_scalars(out, out_dtype)
        .map_err(|err| EvalFailure::invalid("columnar_eval", err.to_string()))
}

fn eval_literal_batch(value: &ScalarValue, rows: usize) -> ColumnarColumn {
    match value {
        ScalarValue::Boolean(v) => ColumnarColumn::Bool {
            values: vec![*v; rows],
            valid: vec![true; rows],
        },
        ScalarValue::Int64(v) => ColumnarColumn::Int64 {
            values: vec![*v; rows],
            valid: vec![true; rows],
        },
        ScalarValue::UInt64(v) => ColumnarColumn::UInt64 {
            values: vec![*v; rows],
            valid: vec![true; rows],
        },
        ScalarValue::Float64(v) => ColumnarColumn::Float64 {
            values: vec![*v; rows],
            valid: vec![true; rows],
        },
        ScalarValue::Utf8(v) => ColumnarColumn::Utf8 {
            values: vec![v.clone(); rows],
            valid: vec![true; rows],
        },
        ScalarValue::Date32(v) => ColumnarColumn::Date32 {
            values: vec![*v; rows],
            valid: vec![true; rows],
        },
        ScalarValue::TimestampMicros(v) => ColumnarColumn::TimestampMicros {
            values: vec![*v; rows],
            valid: vec![true; rows],
        },
        other => ColumnarColumn::ScalarFallback {
            values: vec![other.clone(); rows],
            dtype: other.dtype(),
        },
    }
}

fn eval_unary_batch(
    op: UnaryOp,
    child: &ColumnarColumn,
    float_policy: FloatPolicy,
) -> super::EvalResult<ColumnarColumn> {
    let rows = child.len();
    match op {
        UnaryOp::IsNull | UnaryOp::IsNotNull => {
            let want_null = op == UnaryOp::IsNull;
            let values = (0..rows)
                .map(|row| child.is_null_at(row) == want_null)
                .collect();
            Ok(ColumnarColumn::Bool {
                values,
                valid: vec![true; rows],
            })
        }
        UnaryOp::Not => match child {
            ColumnarColumn::Bool { values, valid } => Ok(ColumnarColumn::Bool {
                values: values.iter().map(|v| !v).collect(),
                valid: valid.clone(),
            }),
            ColumnarColumn::ScalarFallback { values, .. }
                if values.iter().all(ScalarValue::is_null) =>
            {
                Ok(ColumnarColumn::Bool {
                    values: vec![false; rows],
                    valid: vec![false; rows],
                })
            }
            other => Err(EvalFailure::unsupported(
                "unary_not",
                format!(
                    "NOT supports boolean or null, got {}",
                    other.dtype().as_str()
                ),
            )),
        },
        UnaryOp::Negate => eval_negate_batch(child, float_policy),
    }
}

fn eval_negate_batch(
    child: &ColumnarColumn,
    float_policy: FloatPolicy,
) -> super::EvalResult<ColumnarColumn> {
    let rows = child.len();
    match child {
        ColumnarColumn::Int64 { values, valid } => {
            let mut out = Vec::with_capacity(rows);
            for (&v, &ok) in values.iter().zip(valid) {
                if ok {
                    out.push(v.checked_neg().ok_or_else(|| {
                        EvalFailure::invalid("negate", "int64 negation overflow")
                    })?);
                } else {
                    out.push(0);
                }
            }
            Ok(ColumnarColumn::Int64 {
                values: out,
                valid: valid.clone(),
            })
        }
        ColumnarColumn::UInt64 { values, valid } => {
            let mut out = Vec::with_capacity(rows);
            for (&v, &ok) in values.iter().zip(valid) {
                if ok {
                    out.push(i64::try_from(-i128::from(v)).map_err(|_| {
                        EvalFailure::invalid("negate", "uint64 negation exceeds int64 range")
                    })?);
                } else {
                    out.push(0);
                }
            }
            Ok(ColumnarColumn::Int64 {
                values: out,
                valid: valid.clone(),
            })
        }
        ColumnarColumn::Float64 { values, valid } => {
            let mut out = Vec::with_capacity(rows);
            for (&v, &ok) in values.iter().zip(valid) {
                if ok {
                    if !float_policy.admits(v) {
                        return Err(EvalFailure::unsupported(
                            "negate",
                            format!(
                                "negate requires int64, uint64, decimal128, finite float64, or null, got {}",
                                LogicalDType::Float64.as_str()
                            ),
                        ));
                    }
                    out.push(-v);
                } else {
                    out.push(0.0);
                }
            }
            Ok(ColumnarColumn::Float64 {
                values: out,
                valid: valid.clone(),
            })
        }
        ColumnarColumn::ScalarFallback { values, dtype }
            if values.iter().all(ScalarValue::is_null) =>
        {
            let out_dtype = super::reference_types::numeric_unary_type(dtype, false)?;
            let out_dtype = if out_dtype == LogicalDType::UInt64 {
                LogicalDType::Int64
            } else {
                out_dtype
            };
            ColumnarColumn::from_scalars(vec![ScalarValue::Null; rows], out_dtype)
                .map_err(|err| EvalFailure::invalid("negate", err.to_string()))
        }
        other => {
            let mut out = Vec::with_capacity(rows);
            let mut out_dtype = LogicalDType::Unknown;
            for row in 0..rows {
                let evaluated = super::eval_unary(
                    UnaryOp::Negate,
                    super::EvalValue::new(
                        other.get(row),
                        other.dtype(),
                        super::NullBehavior::NullAware,
                    ),
                )?;
                out_dtype = evaluated.dtype;
                out.push(evaluated.value);
            }
            ColumnarColumn::from_scalars(out, out_dtype)
                .map_err(|err| EvalFailure::invalid("negate", err.to_string()))
        }
    }
}

fn eval_bool_binary_batch(
    left: &ColumnarColumn,
    op: BinaryOp,
    right: &ColumnarColumn,
) -> Option<ColumnarColumn> {
    let (
        ColumnarColumn::Bool {
            values: lv,
            valid: lvalid,
        },
        ColumnarColumn::Bool {
            values: rv,
            valid: rvalid,
        },
    ) = (left, right)
    else {
        return None;
    };
    let rows = lv.len();
    let mut values = Vec::with_capacity(rows);
    let mut valid = Vec::with_capacity(rows);
    for i in 0..rows {
        let short = match op {
            BinaryOp::And => ((lvalid[i] && !lv[i]) || (rvalid[i] && !rv[i])).then_some(false),
            BinaryOp::Or => ((lvalid[i] && lv[i]) || (rvalid[i] && rv[i])).then_some(true),
            _ => return None,
        };
        if let Some(bit) = short {
            values.push(bit);
            valid.push(true);
        } else if lvalid[i] && rvalid[i] {
            values.push(lv[i]);
            valid.push(true);
        } else {
            values.push(false);
            valid.push(false);
        }
    }
    Some(ColumnarColumn::Bool { values, valid })
}

fn eval_binary_pairs<L: Copy, R: Copy, O: Default>(
    lv: &[L],
    lvalid: &[bool],
    rv: &[R],
    rvalid: &[bool],
    mut calc: impl FnMut(L, R) -> super::EvalResult<O>,
) -> super::EvalResult<(Vec<O>, Vec<bool>)> {
    let rows = lv.len();
    let mut values = Vec::with_capacity(rows);
    let mut valid = Vec::with_capacity(rows);
    for i in 0..rows {
        if lvalid[i] && rvalid[i] {
            values.push(calc(lv[i], rv[i])?);
            valid.push(true);
        } else {
            values.push(O::default());
            valid.push(false);
        }
    }
    Ok((values, valid))
}

fn eval_binary_batch(
    left: &ColumnarColumn,
    op: BinaryOp,
    right: &ColumnarColumn,
    float_policy: FloatPolicy,
) -> super::EvalResult<ColumnarColumn> {
    if matches!(op, BinaryOp::And | BinaryOp::Or)
        && let Some(out) = eval_bool_binary_batch(left, op, right)
    {
        return Ok(out);
    }
    match (left, right) {
        (
            ColumnarColumn::Int64 {
                values: lv,
                valid: lvalid,
            },
            ColumnarColumn::Int64 {
                values: rv,
                valid: rvalid,
            },
        ) => {
            let (values, valid) =
                eval_binary_pairs(lv, lvalid, rv, rvalid, |l, r| eval_i64_scalar(l, op, r))?;
            Ok(ColumnarColumn::Int64 { values, valid })
        }
        (
            ColumnarColumn::UInt64 {
                values: lv,
                valid: lvalid,
            },
            ColumnarColumn::UInt64 {
                values: rv,
                valid: rvalid,
            },
        ) => {
            let (values, valid) =
                eval_binary_pairs(lv, lvalid, rv, rvalid, |l, r| eval_u64_scalar(l, op, r))?;
            Ok(ColumnarColumn::UInt64 { values, valid })
        }
        (
            ColumnarColumn::Float64 {
                values: lv,
                valid: lvalid,
            },
            ColumnarColumn::Float64 {
                values: rv,
                valid: rvalid,
            },
        ) => {
            let (values, valid) = eval_binary_pairs(lv, lvalid, rv, rvalid, |l, r| {
                eval_f64_scalar(l, op, r, float_policy)
            })?;
            Ok(ColumnarColumn::Float64 { values, valid })
        }
        _ => eval_mixed_or_fallback_binary_batch(left, op, right, float_policy),
    }
}

fn eval_mixed_or_fallback_binary_batch(
    left: &ColumnarColumn,
    op: BinaryOp,
    right: &ColumnarColumn,
    float_policy: FloatPolicy,
) -> super::EvalResult<ColumnarColumn> {
    match (left, right) {
        (
            ColumnarColumn::Int64 {
                values: lv,
                valid: lvalid,
            },
            ColumnarColumn::Float64 {
                values: rv,
                valid: rvalid,
            },
        ) => {
            let (values, valid) = eval_binary_pairs(lv, lvalid, rv, rvalid, |l, r| {
                eval_f64_scalar(i64_to_exact_f64(l)?, op, r, float_policy)
            })?;
            Ok(ColumnarColumn::Float64 { values, valid })
        }
        (
            ColumnarColumn::Float64 {
                values: lv,
                valid: lvalid,
            },
            ColumnarColumn::Int64 {
                values: rv,
                valid: rvalid,
            },
        ) => {
            let (values, valid) = eval_binary_pairs(lv, lvalid, rv, rvalid, |l, r| {
                eval_f64_scalar(l, op, i64_to_exact_f64(r)?, float_policy)
            })?;
            Ok(ColumnarColumn::Float64 { values, valid })
        }
        (
            ColumnarColumn::UInt64 {
                values: lv,
                valid: lvalid,
            },
            ColumnarColumn::Float64 {
                values: rv,
                valid: rvalid,
            },
        ) => {
            let (values, valid) = eval_binary_pairs(lv, lvalid, rv, rvalid, |l, r| {
                eval_f64_scalar(u64_to_exact_f64(l)?, op, r, float_policy)
            })?;
            Ok(ColumnarColumn::Float64 { values, valid })
        }
        (
            ColumnarColumn::Float64 {
                values: lv,
                valid: lvalid,
            },
            ColumnarColumn::UInt64 {
                values: rv,
                valid: rvalid,
            },
        ) => {
            let (values, valid) = eval_binary_pairs(lv, lvalid, rv, rvalid, |l, r| {
                eval_f64_scalar(l, op, u64_to_exact_f64(r)?, float_policy)
            })?;
            Ok(ColumnarColumn::Float64 { values, valid })
        }
        _ => eval_binary_fallback(left, op, right),
    }
}

fn eval_binary_fallback(
    left: &ColumnarColumn,
    op: BinaryOp,
    right: &ColumnarColumn,
) -> super::EvalResult<ColumnarColumn> {
    let rows = left.len();
    let ldtype = left.dtype();
    let rdtype = right.dtype();
    let mut out = Vec::with_capacity(rows);
    let mut out_dtype = LogicalDType::Unknown;
    for i in 0..rows {
        let lv = super::EvalValue::new(left.get(i), ldtype.clone(), super::NullBehavior::NullAware);
        let rv =
            super::EvalValue::new(right.get(i), rdtype.clone(), super::NullBehavior::NullAware);
        let res = match op {
            BinaryOp::And => super::eval_boolean_and(lv.value, rv.value)?,
            BinaryOp::Or => super::eval_boolean_or(lv.value, rv.value)?,
            _ => eval_numeric_binary(lv, op, rv)?,
        };
        if out_dtype == LogicalDType::Unknown {
            out_dtype = res.dtype;
        }
        out.push(res.value);
    }
    ColumnarColumn::from_scalars(out, out_dtype)
        .map_err(|err| EvalFailure::invalid("binary", err.to_string()))
}

fn compare_slice_pair<T: Ord>(
    lv: &[T],
    lvalid: &[bool],
    rv: &[T],
    rvalid: &[bool],
    op: ComparisonOp,
) -> ColumnarColumn {
    let rows = lv.len();
    let mut values = Vec::with_capacity(rows);
    let mut valid = Vec::with_capacity(rows);
    for i in 0..rows {
        if lvalid[i] && rvalid[i] {
            values.push(compare_order_matches(lv[i].cmp(&rv[i]), op));
            valid.push(true);
        } else {
            values.push(false);
            valid.push(false);
        }
    }
    ColumnarColumn::Bool { values, valid }
}

fn eval_compare_batch(
    left: &ColumnarColumn,
    op: ComparisonOp,
    right: &ColumnarColumn,
) -> super::EvalResult<ColumnarColumn> {
    let rows = left.len();
    match (left, right) {
        (
            ColumnarColumn::Int64 {
                values: lv,
                valid: lvalid,
            },
            ColumnarColumn::Int64 {
                values: rv,
                valid: rvalid,
            },
        )
        | (
            ColumnarColumn::TimestampMicros {
                values: lv,
                valid: lvalid,
            },
            ColumnarColumn::TimestampMicros {
                values: rv,
                valid: rvalid,
            },
        ) => Ok(compare_slice_pair(lv, lvalid, rv, rvalid, op)),
        (
            ColumnarColumn::UInt64 {
                values: lv,
                valid: lvalid,
            },
            ColumnarColumn::UInt64 {
                values: rv,
                valid: rvalid,
            },
        ) => Ok(compare_slice_pair(lv, lvalid, rv, rvalid, op)),
        (
            ColumnarColumn::Utf8 {
                values: lv,
                valid: lvalid,
            },
            ColumnarColumn::Utf8 {
                values: rv,
                valid: rvalid,
            },
        ) => Ok(compare_slice_pair(lv, lvalid, rv, rvalid, op)),
        (
            ColumnarColumn::Date32 {
                values: lv,
                valid: lvalid,
            },
            ColumnarColumn::Date32 {
                values: rv,
                valid: rvalid,
            },
        ) => Ok(compare_slice_pair(lv, lvalid, rv, rvalid, op)),
        (
            ColumnarColumn::Float64 {
                values: lv,
                valid: lvalid,
            },
            ColumnarColumn::Float64 {
                values: rv,
                valid: rvalid,
            },
        ) => {
            let mut values = Vec::with_capacity(rows);
            let mut valid = Vec::with_capacity(rows);
            for i in 0..rows {
                if lvalid[i] && rvalid[i] {
                    values.push(super::compare_f64(lv[i], rv[i], op)?);
                    valid.push(true);
                } else {
                    values.push(false);
                    valid.push(false);
                }
            }
            Ok(ColumnarColumn::Bool { values, valid })
        }
        _ => eval_compare_fallback(left, op, right),
    }
}

fn eval_compare_fallback(
    left: &ColumnarColumn,
    op: ComparisonOp,
    right: &ColumnarColumn,
) -> super::EvalResult<ColumnarColumn> {
    let rows = left.len();
    let ldtype = left.dtype();
    let rdtype = right.dtype();
    let mut values = Vec::with_capacity(rows);
    let mut valid = Vec::with_capacity(rows);
    for i in 0..rows {
        let lv = super::EvalValue::new(left.get(i), ldtype.clone(), super::NullBehavior::NullAware);
        let rv =
            super::EvalValue::new(right.get(i), rdtype.clone(), super::NullBehavior::NullAware);
        let res = super::eval_compare(&lv, op, &rv)?;
        match res.value {
            ScalarValue::Boolean(b) => {
                values.push(b);
                valid.push(true);
            }
            ScalarValue::Null => {
                values.push(false);
                valid.push(false);
            }
            _ => unreachable!("eval_compare returns bool or null"),
        }
    }
    Ok(ColumnarColumn::Bool { values, valid })
}

fn map_checked_slice<I: Copy, O: Default>(
    values: &[I],
    valid: &[bool],
    tolerant: bool,
    mut convert: impl FnMut(I) -> super::EvalResult<O>,
) -> super::EvalResult<(Vec<O>, Vec<bool>)> {
    let rows = values.len();
    let mut out = Vec::with_capacity(rows);
    let mut out_valid = Vec::with_capacity(rows);
    for (&v, &ok) in values.iter().zip(valid) {
        if !ok {
            out.push(O::default());
            out_valid.push(false);
            continue;
        }
        match convert(v) {
            Ok(casted) => {
                out.push(casted);
                out_valid.push(true);
            }
            Err(failure)
                if tolerant && failure.status == ExpressionEvaluationStatus::InvalidInput =>
            {
                out.push(O::default());
                out_valid.push(false);
            }
            Err(failure) => return Err(failure),
        }
    }
    Ok((out, out_valid))
}

#[allow(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]
fn eval_cast_batch(
    child: &ColumnarColumn,
    target_dtype: &LogicalDType,
    tolerant: bool,
    float_policy: FloatPolicy,
) -> super::EvalResult<ColumnarColumn> {
    super::reference_types::cast_type(&child.dtype(), target_dtype)?;
    match (child, target_dtype) {
        (ColumnarColumn::Int64 { values, valid }, LogicalDType::Float64) => {
            Ok(ColumnarColumn::Float64 {
                values: values.iter().map(|&v| v as f64).collect(),
                valid: valid.clone(),
            })
        }
        (ColumnarColumn::UInt64 { values, valid }, LogicalDType::Float64) => {
            Ok(ColumnarColumn::Float64 {
                values: values.iter().map(|&v| v as f64).collect(),
                valid: valid.clone(),
            })
        }
        (ColumnarColumn::Float64 { values, valid }, LogicalDType::Int64) => {
            let (out, out_valid) = map_checked_slice(values, valid, tolerant, |v| {
                if !v.is_finite() {
                    return Err(EvalFailure::unsupported(
                        "cast",
                        "nonfinite scalar values are not admitted",
                    ));
                }
                if v.fract() != 0.0
                    || !(-9_223_372_036_854_775_808.0..9_223_372_036_854_775_808.0).contains(&v)
                {
                    return Err(EvalFailure::invalid(
                        "cast",
                        "float64 value is not an integral int64 in range",
                    ));
                }
                Ok(v as i64)
            })?;
            Ok(ColumnarColumn::Int64 {
                values: out,
                valid: out_valid,
            })
        }
        (ColumnarColumn::Float64 { values, valid }, LogicalDType::UInt64) => {
            let (out, out_valid) = map_checked_slice(values, valid, tolerant, |v| {
                if !v.is_finite() {
                    return Err(EvalFailure::unsupported(
                        "cast",
                        "nonfinite scalar values are not admitted",
                    ));
                }
                if v.fract() != 0.0 || !(0.0..18_446_744_073_709_551_616.0).contains(&v) {
                    return Err(EvalFailure::invalid(
                        "cast",
                        "float64 value is not an integral uint64 in range",
                    ));
                }
                Ok(v as u64)
            })?;
            Ok(ColumnarColumn::UInt64 {
                values: out,
                valid: out_valid,
            })
        }
        (ColumnarColumn::Int64 { values, valid }, LogicalDType::UInt64) => {
            let (out, out_valid) = map_checked_slice(values, valid, tolerant, |v| {
                u64::try_from(v)
                    .map_err(|_| EvalFailure::invalid("cast", "int64 value exceeds uint64 range"))
            })?;
            Ok(ColumnarColumn::UInt64 {
                values: out,
                valid: out_valid,
            })
        }
        (ColumnarColumn::UInt64 { values, valid }, LogicalDType::Int64) => {
            let (out, out_valid) = map_checked_slice(values, valid, tolerant, |v| {
                i64::try_from(v)
                    .map_err(|_| EvalFailure::invalid("cast", "uint64 value exceeds int64 range"))
            })?;
            Ok(ColumnarColumn::Int64 {
                values: out,
                valid: out_valid,
            })
        }
        _ => eval_cast_fallback(child, target_dtype, tolerant, float_policy),
    }
}

fn eval_cast_fallback(
    child: &ColumnarColumn,
    target_dtype: &LogicalDType,
    tolerant: bool,
    float_policy: FloatPolicy,
) -> super::EvalResult<ColumnarColumn> {
    let rows = child.len();
    let dtype = child.dtype();
    let mut out = Vec::with_capacity(rows);
    for row in 0..rows {
        let eval_val = super::EvalValue::new(
            child.get(row),
            dtype.clone(),
            super::NullBehavior::NullAware,
        );
        let converted = if tolerant {
            try_cast_eval_value(&eval_val, target_dtype)?
        } else {
            cast_eval_value(&eval_val, target_dtype)?
        };
        if float_policy == FloatPolicy::Strict
            && let ScalarValue::Float64(f) = converted.value
            && !f.is_finite()
        {
            return Err(EvalFailure::unsupported(
                "cast",
                "nonfinite scalar values are not admitted",
            ));
        }
        out.push(converted.value);
    }
    ColumnarColumn::from_scalars(out, target_dtype.clone())
        .map_err(|err| EvalFailure::invalid("cast", err.to_string()))
}

fn eval_i64_scalar(left: i64, op: BinaryOp, right: i64) -> super::EvalResult<i64> {
    match op {
        BinaryOp::Add => left.checked_add(right),
        BinaryOp::Subtract => left.checked_sub(right),
        BinaryOp::Multiply => left.checked_mul(right),
        BinaryOp::Divide if right == 0 => {
            return Err(EvalFailure::invalid("divide", "division by zero"));
        }
        BinaryOp::Divide => left.checked_div(right),
        BinaryOp::Remainder if right == 0 => {
            return Err(EvalFailure::invalid("remainder", "remainder by zero"));
        }
        BinaryOp::Remainder => i64::try_from(i128::from(left) % i128::from(right)).ok(),
        BinaryOp::And | BinaryOp::Or => None,
    }
    .ok_or_else(|| EvalFailure::invalid("numeric_binary", "int64 arithmetic overflow"))
}

fn eval_u64_scalar(left: u64, op: BinaryOp, right: u64) -> super::EvalResult<u64> {
    match op {
        BinaryOp::Add => left.checked_add(right),
        BinaryOp::Subtract => left.checked_sub(right),
        BinaryOp::Multiply => left.checked_mul(right),
        BinaryOp::Divide if right == 0 => {
            return Err(EvalFailure::invalid("divide", "division by zero"));
        }
        BinaryOp::Divide => left.checked_div(right),
        BinaryOp::Remainder if right == 0 => {
            return Err(EvalFailure::invalid("remainder", "remainder by zero"));
        }
        BinaryOp::Remainder => left.checked_rem(right),
        BinaryOp::And | BinaryOp::Or => None,
    }
    .ok_or_else(|| EvalFailure::invalid("numeric_binary", "uint64 arithmetic overflow"))
}

fn eval_f64_scalar(
    left: f64,
    op: BinaryOp,
    right: f64,
    float_policy: FloatPolicy,
) -> super::EvalResult<f64> {
    if !left.is_finite() || !right.is_finite() {
        return Err(EvalFailure::unsupported(
            "numeric_binary",
            "non-finite float semantics are not admitted by this slice",
        ));
    }
    if matches!(op, BinaryOp::Divide | BinaryOp::Remainder) && right == 0.0 {
        return Err(EvalFailure::invalid("divide", "division by zero"));
    }
    let output = match op {
        BinaryOp::Add => left + right,
        BinaryOp::Subtract => left - right,
        BinaryOp::Multiply => left * right,
        BinaryOp::Divide => left / right,
        BinaryOp::Remainder => left % right,
        BinaryOp::And | BinaryOp::Or => unreachable!("boolean ops handled above"),
    };
    if !float_policy.admits(output) {
        return Err(EvalFailure::invalid(
            "numeric_binary",
            "float arithmetic produced a non-finite value",
        ));
    }
    Ok(output)
}

const fn compare_order_matches(ordering: Ordering, op: ComparisonOp) -> bool {
    match op {
        ComparisonOp::Eq => matches!(ordering, Ordering::Equal),
        ComparisonOp::NotEq => !matches!(ordering, Ordering::Equal),
        ComparisonOp::Lt => matches!(ordering, Ordering::Less),
        ComparisonOp::LtEq => matches!(ordering, Ordering::Less | Ordering::Equal),
        ComparisonOp::Gt => matches!(ordering, Ordering::Greater),
        ComparisonOp::GtEq => matches!(ordering, Ordering::Greater | Ordering::Equal),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ColumnRef, ExprId, evaluate_expression};

    fn id(name: &str) -> ExprId {
        ExprId::new(name).unwrap()
    }

    fn col(name: &str) -> Expression {
        Expression::column(id(name), ColumnRef::new(name).unwrap())
    }

    fn lit(v: ScalarValue) -> Expression {
        Expression::literal(id("lit"), v)
    }

    fn assert_parity(expr: &Expression, rows: &[ExpressionInputRow]) {
        let batch = ColumnarBatch::from_rows(rows).unwrap();
        assert_eq!(batch.to_rows(), rows);
        let batch_res = evaluate_expression_batch(expr, &batch);
        let mut expected = Vec::with_capacity(rows.len());
        let mut first_err: Option<ShardLoomError> = None;
        for row in rows {
            let rep = evaluate_expression(expr, row);
            if let Some(v) = rep.value {
                expected.push(v);
            } else {
                first_err = Some(ShardLoomError::InvalidOperation(
                    rep.diagnostics[0].message.clone(),
                ));
                break;
            }
        }
        if first_err.is_some() {
            assert!(batch_res.is_err(), "expected error parity for {expr:?}");
        } else {
            let actual = batch_res.unwrap().to_scalars();
            assert_eq!(actual, expected, "mismatch for {expr:?}");
        }
    }

    #[test]
    fn columnar_batch_matches_row_wise_expression_evaluation_across_types_nulls_and_casts() {
        let mut r0 = ExpressionInputRow::new();
        r0.insert("i".into(), ScalarValue::Int64(10));
        r0.insert("f".into(), ScalarValue::Float64(0.25));
        r0.insert("b".into(), ScalarValue::Boolean(true));
        r0.insert("s".into(), ScalarValue::Utf8("42".into()));

        let mut r1 = ExpressionInputRow::new();
        r1.insert("i".into(), ScalarValue::Null);
        r1.insert("f".into(), ScalarValue::Float64(2.5));
        r1.insert("b".into(), ScalarValue::Null);
        r1.insert("s".into(), ScalarValue::Utf8("bad".into()));

        let mut r2 = ExpressionInputRow::new();
        r2.insert("i".into(), ScalarValue::Int64(-5));
        r2.insert("f".into(), ScalarValue::Null);
        r2.insert("b".into(), ScalarValue::Boolean(false));
        r2.insert("s".into(), ScalarValue::Null);

        let rows = vec![r0, r1, r2];

        let arith = Expression::new(
            id("sub"),
            ExpressionKind::Binary {
                left: Box::new(Expression::new(
                    id("mul"),
                    ExpressionKind::Binary {
                        left: Box::new(col("i")),
                        op: BinaryOp::Multiply,
                        right: Box::new(lit(ScalarValue::Int64(2))),
                    },
                )),
                op: BinaryOp::Subtract,
                right: Box::new(lit(ScalarValue::Int64(3))),
            },
        );
        assert_parity(&arith, &rows);

        let mixed = Expression::new(
            id("mix"),
            ExpressionKind::Binary {
                left: Box::new(col("i")),
                op: BinaryOp::Multiply,
                right: Box::new(col("f")),
            },
        );
        assert_parity(&mixed, &rows);

        let pred = Expression::new(
            id("or"),
            ExpressionKind::Binary {
                left: Box::new(Expression::new(
                    id("cmp"),
                    ExpressionKind::Compare {
                        left: Box::new(col("i")),
                        op: ComparisonOp::Gt,
                        right: Box::new(lit(ScalarValue::Int64(0))),
                    },
                )),
                op: BinaryOp::Or,
                right: Box::new(Expression::new(
                    id("notb"),
                    ExpressionKind::Unary {
                        op: UnaryOp::Not,
                        expr: Box::new(col("b")),
                    },
                )),
            },
        );
        assert_parity(&pred, &rows);
        assert_parity(
            &Expression::new(
                id("isnull"),
                ExpressionKind::Unary {
                    op: UnaryOp::IsNull,
                    expr: Box::new(col("i")),
                },
            ),
            &rows,
        );

        let try_c = Expression::try_cast(id("tc"), col("s"), LogicalDType::Int64);
        assert_parity(&try_c, &rows);
        let strict_c = Expression::cast(id("sc"), col("s"), LogicalDType::Int64);
        assert_parity(&strict_c, &rows);

        let mut r_div0 = ExpressionInputRow::new();
        r_div0.insert("i".into(), ScalarValue::Int64(0));
        let div0 = Expression::new(
            id("d0"),
            ExpressionKind::Binary {
                left: Box::new(lit(ScalarValue::Int64(1))),
                op: BinaryOp::Divide,
                right: Box::new(col("i")),
            },
        );
        assert_parity(&div0, &[r_div0]);
    }
}
