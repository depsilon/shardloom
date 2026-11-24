// SPDX-License-Identifier: Apache-2.0

//! Compiled contiguous columnar batch evaluator for `shardloom-core` expressions.
//! Falls back per-expression to row-wise `eval_expression` only for complex or
//! non-primitive function trees, preserving exact NULL and float policies.

use super::{
    EvalFailure, Expression, ExpressionEvaluationStatus, ExpressionInputRow, ExpressionKind,
    ScalarValue, cast_eval_value_with_policy, compare_f64_with_policy, eval_compare_with_policy,
    eval_expression_with_policy, eval_numeric_binary_with_policy, eval_unary_with_policy,
    i64_to_exact_f64, try_cast_eval_value_with_policy, u64_to_exact_f64,
};
use crate::float_policy::canonicalize_f64_result;
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

    /// Validates that internal validity buffers match value vector lengths.
    ///
    /// # Errors
    /// Returns [`ShardLoomError::InvalidOperation`] if `valid.len() != values.len()`.
    pub fn validate(&self) -> Result<()> {
        let valid_len = match self {
            Self::Bool { valid, .. }
            | Self::Int64 { valid, .. }
            | Self::UInt64 { valid, .. }
            | Self::Float64 { valid, .. }
            | Self::Utf8 { valid, .. }
            | Self::Date32 { valid, .. }
            | Self::TimestampMicros { valid, .. } => valid.len(),
            Self::ScalarFallback { .. } => return Ok(()),
        };
        if valid_len != self.len() {
            return Err(ShardLoomError::InvalidOperation(format!(
                "columnar {} column has {} values but {} validity flags",
                self.dtype().as_str(),
                self.len(),
                valid_len
            )));
        }
        Ok(())
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
            | Self::TimestampMicros { valid, .. } => !valid.get(row).copied().unwrap_or(false),
            Self::ScalarFallback { values, .. } => values.get(row).is_none_or(ScalarValue::is_null),
        }
    }

    #[must_use]
    pub fn get(&self, row: usize) -> ScalarValue {
        match self {
            Self::Bool { values, valid } => valid
                .get(row)
                .copied()
                .filter(|v| *v)
                .and_then(|_| values.get(row).copied())
                .map_or(ScalarValue::Null, ScalarValue::Boolean),
            Self::Int64 { values, valid } => valid
                .get(row)
                .copied()
                .filter(|v| *v)
                .and_then(|_| values.get(row).copied())
                .map_or(ScalarValue::Null, ScalarValue::Int64),
            Self::UInt64 { values, valid } => valid
                .get(row)
                .copied()
                .filter(|v| *v)
                .and_then(|_| values.get(row).copied())
                .map_or(ScalarValue::Null, ScalarValue::UInt64),
            Self::Float64 { values, valid } => valid
                .get(row)
                .copied()
                .filter(|v| *v)
                .and_then(|_| values.get(row).copied())
                .map_or(ScalarValue::Null, ScalarValue::Float64),
            Self::Utf8 { values, valid } => valid
                .get(row)
                .copied()
                .filter(|v| *v)
                .and_then(|_| values.get(row).cloned())
                .map_or(ScalarValue::Null, ScalarValue::Utf8),
            Self::Date32 { values, valid } => valid
                .get(row)
                .copied()
                .filter(|v| *v)
                .and_then(|_| values.get(row).copied())
                .map_or(ScalarValue::Null, ScalarValue::Date32),
            Self::TimestampMicros { values, valid } => valid
                .get(row)
                .copied()
                .filter(|v| *v)
                .and_then(|_| values.get(row).copied())
                .map_or(ScalarValue::Null, ScalarValue::TimestampMicros),
            Self::ScalarFallback { values, .. } => {
                values.get(row).cloned().unwrap_or(ScalarValue::Null)
            }
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
    /// Returns [`ShardLoomError::InvalidOperation`] if any column's row count mismatches `rows`
    /// or its internal validity buffer length mismatches its value vector length.
    pub fn new(rows: usize, columns: BTreeMap<String, ColumnarColumn>) -> Result<Self> {
        for (name, column) in &columns {
            column.validate().map_err(|err| {
                ShardLoomError::InvalidOperation(format!("column {name:?}: {err}"))
            })?;
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
    for (name, column) in &batch.columns {
        column
            .validate()
            .map_err(|err| ShardLoomError::InvalidOperation(format!("column {name:?}: {err}")))?;
        if column.len() != batch.rows {
            return Err(ShardLoomError::InvalidOperation(format!(
                "column {name:?} has {} rows, expected {}",
                column.len(),
                batch.rows
            )));
        }
    }
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
            eval_compare_batch(&left_col, *op, &right_col, float_policy)
        }
        ExpressionKind::Cast { expr, target_dtype } => {
            let child = eval_batch_inner(expr, batch, float_policy)?;
            eval_cast_batch(&child, target_dtype, false, float_policy)
        }
        ExpressionKind::TryCast { expr, target_dtype } => {
            let child = eval_batch_inner(expr, batch, float_policy)?;
            eval_cast_batch(&child, target_dtype, true, float_policy)
        }
        _ => eval_scalar_fallback(expression, batch, float_policy),
    }
}

fn eval_scalar_fallback(
    expression: &Expression,
    batch: &ColumnarBatch,
    float_policy: FloatPolicy,
) -> super::EvalResult<ColumnarColumn> {
    let input_rows = batch.to_rows();
    let mut out = Vec::with_capacity(input_rows.len());
    let mut out_dtype = expression.dtype.clone().unwrap_or(LogicalDType::Unknown);
    for row in &input_rows {
        let evaluated = eval_expression_with_policy(expression, row, float_policy)?;
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
            other => {
                let mut values = Vec::with_capacity(rows);
                let mut valid = Vec::with_capacity(rows);
                for row in 0..rows {
                    match other.get(row) {
                        ScalarValue::Null => {
                            values.push(false);
                            valid.push(false);
                        }
                        ScalarValue::Boolean(v) => {
                            values.push(!v);
                            valid.push(true);
                        }
                        val => {
                            return Err(EvalFailure::unsupported(
                                "unary_not",
                                format!(
                                    "NOT supports boolean or null, got {}",
                                    val.dtype().as_str()
                                ),
                            ));
                        }
                    }
                }
                Ok(ColumnarColumn::Bool { values, valid })
            }
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
                    out.push(canonicalize_f64_result(-v));
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
                let evaluated = eval_unary_with_policy(
                    UnaryOp::Negate,
                    super::EvalValue::new(
                        other.get(row),
                        other.dtype(),
                        super::NullBehavior::NullAware,
                    ),
                    float_policy,
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
    if matches!(op, BinaryOp::And | BinaryOp::Or) {
        if let Some(out) = eval_bool_binary_batch(left, op, right) {
            return Ok(out);
        }
        return eval_binary_fallback(left, op, right, float_policy);
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
        _ => eval_binary_fallback(left, op, right, float_policy),
    }
}

fn eval_binary_fallback(
    left: &ColumnarColumn,
    op: BinaryOp,
    right: &ColumnarColumn,
    float_policy: FloatPolicy,
) -> super::EvalResult<ColumnarColumn> {
    let rows = left.len();
    let ldtype = left.dtype();
    let rdtype = right.dtype();
    let mut out = Vec::with_capacity(rows);
    let mut out_dtype = if matches!(op, BinaryOp::And | BinaryOp::Or) {
        LogicalDType::Boolean
    } else {
        LogicalDType::Unknown
    };
    for i in 0..rows {
        let lv = super::EvalValue::new(left.get(i), ldtype.clone(), super::NullBehavior::NullAware);
        let rv =
            super::EvalValue::new(right.get(i), rdtype.clone(), super::NullBehavior::NullAware);
        let res = match op {
            BinaryOp::And => super::eval_boolean_and(lv.value, rv.value)?,
            BinaryOp::Or => super::eval_boolean_or(lv.value, rv.value)?,
            _ => eval_numeric_binary_with_policy(lv, op, rv, float_policy)?,
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
    float_policy: FloatPolicy,
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
                    values.push(compare_f64_with_policy(lv[i], rv[i], op, float_policy)?);
                    valid.push(true);
                } else {
                    values.push(false);
                    valid.push(false);
                }
            }
            Ok(ColumnarColumn::Bool { values, valid })
        }
        _ => eval_compare_fallback(left, op, right, float_policy),
    }
}

fn eval_compare_fallback(
    left: &ColumnarColumn,
    op: ComparisonOp,
    right: &ColumnarColumn,
    float_policy: FloatPolicy,
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
        let res = eval_compare_with_policy(&lv, op, &rv, float_policy)?;
        match res.value {
            ScalarValue::Boolean(b) => {
                values.push(b);
                valid.push(true);
            }
            ScalarValue::Null => {
                values.push(false);
                valid.push(false);
            }
            other => {
                return Err(EvalFailure::invalid(
                    "comparison",
                    format!("unexpected comparison output {}", other.dtype().as_str()),
                ));
            }
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

fn check_float64_cast_operand(value: f64, float_policy: FloatPolicy) -> super::EvalResult<()> {
    if !float_policy.admits(value) {
        return Err(EvalFailure::unsupported(
            "cast",
            "nonfinite scalar values are not admitted",
        ));
    }
    Ok(())
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
        (ColumnarColumn::Float64 { values, valid }, LogicalDType::Float64) => {
            for (&v, &ok) in values.iter().zip(valid) {
                if ok {
                    check_float64_cast_operand(v, float_policy)?;
                }
            }
            Ok(child.clone())
        }
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
                check_float64_cast_operand(v, float_policy)?;
                if !v.is_finite()
                    || v.fract() != 0.0
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
                check_float64_cast_operand(v, float_policy)?;
                if !v.is_finite()
                    || v.fract() != 0.0
                    || !(0.0..18_446_744_073_709_551_616.0).contains(&v)
                {
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
            try_cast_eval_value_with_policy(&eval_val, target_dtype, float_policy)?
        } else {
            cast_eval_value_with_policy(&eval_val, target_dtype, float_policy)?
        };
        if let ScalarValue::Float64(f) = converted.value {
            check_float64_cast_operand(f, float_policy)?;
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
        BinaryOp::And | BinaryOp::Or => {
            return Err(EvalFailure::unsupported(
                op.as_str(),
                "boolean operator routed to numeric scalar kernel",
            ));
        }
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
        BinaryOp::And | BinaryOp::Or => {
            return Err(EvalFailure::unsupported(
                op.as_str(),
                "boolean operator routed to numeric scalar kernel",
            ));
        }
    }
    .ok_or_else(|| EvalFailure::invalid("numeric_binary", "uint64 arithmetic overflow"))
}

fn eval_f64_scalar(
    left: f64,
    op: BinaryOp,
    right: f64,
    float_policy: FloatPolicy,
) -> super::EvalResult<f64> {
    if !float_policy.admits(left) || !float_policy.admits(right) {
        return Err(EvalFailure::unsupported(
            "numeric_binary",
            "non-finite float semantics are not admitted by this slice",
        ));
    }
    if float_policy == FloatPolicy::Strict
        && matches!(op, BinaryOp::Divide | BinaryOp::Remainder)
        && right == 0.0
    {
        return Err(EvalFailure::invalid("divide", "division by zero"));
    }
    let output = match op {
        BinaryOp::Add => left + right,
        BinaryOp::Subtract => left - right,
        BinaryOp::Multiply => left * right,
        BinaryOp::Divide => left / right,
        BinaryOp::Remainder => left % right,
        BinaryOp::And | BinaryOp::Or => {
            return Err(EvalFailure::unsupported(
                op.as_str(),
                "boolean operator routed to numeric scalar kernel",
            ));
        }
    };
    if !float_policy.admits(output) {
        return Err(EvalFailure::invalid(
            "numeric_binary",
            "float arithmetic produced a non-finite value",
        ));
    }
    Ok(canonicalize_f64_result(output))
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

    fn bin(left: Expression, op: BinaryOp, right: Expression) -> Expression {
        Expression::new(
            id("bin"),
            ExpressionKind::Binary {
                left: Box::new(left),
                op,
                right: Box::new(right),
            },
        )
    }

    fn float_bits(column: &ColumnarColumn) -> Vec<Option<u64>> {
        column
            .to_scalars()
            .into_iter()
            .map(|value| match value {
                ScalarValue::Null => None,
                ScalarValue::Float64(v) => Some(v.to_bits()),
                other => panic!("expected Float64 or Null, got {other:?}"),
            })
            .collect()
    }

    fn extended_rows() -> Vec<ExpressionInputRow> {
        let payload_nan = f64::from_bits(0xfff8_0000_0000_1234);
        let samples = [
            (Some(f64::INFINITY), Some(2_i64), Some(3_u64), 0.0),
            (Some(f64::NEG_INFINITY), Some(0), Some(1), -0.0),
            (Some(payload_nan), Some(4), Some(2), -0.0),
            (Some(-0.0), Some(5), Some(4), 0.0),
            (None, Some(1), None, 0.0),
        ];
        samples
            .into_iter()
            .map(|(f, i, u, zero)| {
                let mut row = ExpressionInputRow::new();
                row.insert(
                    "f".into(),
                    f.map_or(ScalarValue::Null, ScalarValue::Float64),
                );
                row.insert("i".into(), i.map_or(ScalarValue::Null, ScalarValue::Int64));
                row.insert("u".into(), u.map_or(ScalarValue::Null, ScalarValue::UInt64));
                row.insert("zero".into(), ScalarValue::Float64(zero));
                row
            })
            .collect()
    }

    #[test]
    fn extended_policy_admits_nonfinite_arithmetic_and_canonicalizes_nan() {
        let canon_nan = crate::float_policy::CANONICAL_F64_NAN_BITS;
        let batch = ColumnarBatch::from_rows(&extended_rows()).unwrap();

        let add_one = bin(col("f"), BinaryOp::Add, lit(ScalarValue::Float64(1.0)));
        assert!(matches!(
            evaluate_expression_batch_with_policy(&add_one, &batch, FloatPolicy::Strict),
            Err(ShardLoomError::NotImplemented(_))
        ));
        let out =
            evaluate_expression_batch_with_policy(&add_one, &batch, FloatPolicy::Extended).unwrap();
        assert_eq!(
            float_bits(&out),
            vec![
                Some(f64::INFINITY.to_bits()),
                Some(f64::NEG_INFINITY.to_bits()),
                Some(canon_nan),
                Some(1.0_f64.to_bits()),
                None,
            ]
        );

        let sub_self = bin(col("f"), BinaryOp::Subtract, col("f"));
        let sub_out =
            evaluate_expression_batch_with_policy(&sub_self, &batch, FloatPolicy::Extended)
                .unwrap();
        assert_eq!(
            float_bits(&sub_out),
            vec![
                Some(canon_nan),
                Some(canon_nan),
                Some(canon_nan),
                Some(0.0_f64.to_bits()),
                None,
            ]
        );

        let div_zero = bin(col("i"), BinaryOp::Divide, col("zero"));
        assert!(
            evaluate_expression_batch_with_policy(&div_zero, &batch, FloatPolicy::Strict).is_err()
        );
        let div_out =
            evaluate_expression_batch_with_policy(&div_zero, &batch, FloatPolicy::Extended)
                .unwrap();
        assert_eq!(
            float_bits(&div_out),
            vec![
                Some(f64::INFINITY.to_bits()),
                Some(canon_nan),
                Some(f64::NEG_INFINITY.to_bits()),
                Some(f64::INFINITY.to_bits()),
                Some(f64::INFINITY.to_bits()),
            ]
        );

        let mixed_mul = bin(col("u"), BinaryOp::Multiply, col("f"));
        let mixed_out =
            evaluate_expression_batch_with_policy(&mixed_mul, &batch, FloatPolicy::Extended)
                .unwrap();
        assert_eq!(
            float_bits(&mixed_out),
            vec![
                Some(f64::INFINITY.to_bits()),
                Some(f64::NEG_INFINITY.to_bits()),
                Some(canon_nan),
                Some((-0.0_f64).to_bits()),
                None,
            ]
        );
    }

    #[test]
    fn extended_policy_handles_negate_and_comparisons() {
        let payload_nan_bits = 0xfff8_0000_0000_1234_u64;
        let canon_nan = crate::float_policy::CANONICAL_F64_NAN_BITS;
        let batch = ColumnarBatch::from_rows(&extended_rows()).unwrap();

        let neg = Expression::new(
            id("neg"),
            ExpressionKind::Unary {
                op: UnaryOp::Negate,
                expr: Box::new(col("f")),
            },
        );
        assert!(matches!(
            evaluate_expression_batch_with_policy(&neg, &batch, FloatPolicy::Strict),
            Err(ShardLoomError::NotImplemented(_))
        ));
        let neg_out =
            evaluate_expression_batch_with_policy(&neg, &batch, FloatPolicy::Extended).unwrap();
        assert_eq!(
            float_bits(&neg_out),
            vec![
                Some(f64::NEG_INFINITY.to_bits()),
                Some(f64::INFINITY.to_bits()),
                Some(canon_nan),
                Some((0.0_f64).to_bits()),
                None,
            ]
        );

        let id_cast = Expression::cast(id("id_cast"), col("f"), LogicalDType::Float64);
        assert!(matches!(
            evaluate_expression_batch_with_policy(&id_cast, &batch, FloatPolicy::Strict),
            Err(ShardLoomError::NotImplemented(_))
        ));
        let id_out =
            evaluate_expression_batch_with_policy(&id_cast, &batch, FloatPolicy::Extended).unwrap();
        assert_eq!(
            float_bits(&id_out),
            vec![
                Some(f64::INFINITY.to_bits()),
                Some(f64::NEG_INFINITY.to_bits()),
                Some(payload_nan_bits),
                Some((-0.0_f64).to_bits()),
                None,
            ]
        );

        let eq_self = Expression::new(
            id("eq"),
            ExpressionKind::Compare {
                left: Box::new(col("f")),
                op: ComparisonOp::Eq,
                right: Box::new(col("f")),
            },
        );
        assert!(matches!(
            evaluate_expression_batch_with_policy(&eq_self, &batch, FloatPolicy::Strict),
            Err(ShardLoomError::NotImplemented(_))
        ));
        assert_eq!(
            evaluate_expression_batch_with_policy(&eq_self, &batch, FloatPolicy::Extended)
                .unwrap()
                .to_scalars(),
            vec![
                ScalarValue::Boolean(true),
                ScalarValue::Boolean(true),
                ScalarValue::Boolean(true),
                ScalarValue::Boolean(true),
                ScalarValue::Null,
            ]
        );

        let mixed_lt = Expression::new(
            id("lt"),
            ExpressionKind::Compare {
                left: Box::new(col("i")),
                op: ComparisonOp::Lt,
                right: Box::new(col("f")),
            },
        );
        assert_eq!(
            evaluate_expression_batch_with_policy(&mixed_lt, &batch, FloatPolicy::Extended)
                .unwrap()
                .to_scalars(),
            vec![
                ScalarValue::Boolean(true),
                ScalarValue::Boolean(false),
                ScalarValue::Boolean(true),
                ScalarValue::Boolean(false),
                ScalarValue::Null,
            ]
        );
    }

    #[test]
    fn extended_policy_handles_casts() {
        let batch = ColumnarBatch::from_rows(&extended_rows()).unwrap();

        let try_int = Expression::try_cast(id("ti"), col("f"), LogicalDType::Int64);
        assert!(matches!(
            evaluate_expression_batch_with_policy(&try_int, &batch, FloatPolicy::Strict),
            Err(ShardLoomError::NotImplemented(_))
        ));
        assert_eq!(
            evaluate_expression_batch_with_policy(&try_int, &batch, FloatPolicy::Extended)
                .unwrap()
                .to_scalars(),
            vec![
                ScalarValue::Null,
                ScalarValue::Null,
                ScalarValue::Null,
                ScalarValue::Int64(0),
                ScalarValue::Null,
            ]
        );
        let strict_int = Expression::cast(id("si"), col("f"), LogicalDType::Int64);
        assert!(matches!(
            evaluate_expression_batch_with_policy(&strict_int, &batch, FloatPolicy::Extended),
            Err(ShardLoomError::InvalidOperation(_))
        ));

        let to_utf8 = Expression::cast(id("u8"), col("f"), LogicalDType::Utf8);
        assert_eq!(
            evaluate_expression_batch_with_policy(&to_utf8, &batch, FloatPolicy::Extended)
                .unwrap()
                .to_scalars(),
            vec![
                ScalarValue::Utf8("inf".to_string()),
                ScalarValue::Utf8("-inf".to_string()),
                ScalarValue::Utf8("NaN".to_string()),
                ScalarValue::Utf8("-0".to_string()),
                ScalarValue::Null,
            ]
        );

        let parse_inf = Expression::cast(
            id("pinf"),
            lit(ScalarValue::Utf8("inf".into())),
            LogicalDType::Float64,
        );
        assert!(
            evaluate_expression_batch_with_policy(&parse_inf, &batch, FloatPolicy::Strict).is_err()
        );
        assert_eq!(
            float_bits(
                &evaluate_expression_batch_with_policy(&parse_inf, &batch, FloatPolicy::Extended)
                    .unwrap()
            ),
            vec![Some(f64::INFINITY.to_bits()); 5]
        );
    }

    #[test]
    fn extended_policy_handles_scalar_fallbacks() {
        let payload_nan_bits = 0xfff8_0000_0000_1234_u64;
        let canon_nan = crate::float_policy::CANONICAL_F64_NAN_BITS;
        let batch = ColumnarBatch::from_rows(&extended_rows()).unwrap();
        let fn_call = |name: &str, args: Vec<Expression>| {
            Expression::new(
                id(name),
                ExpressionKind::FunctionCall {
                    name: name.to_string(),
                    args,
                },
            )
        };
        assert_eq!(
            float_bits(
                &evaluate_expression_batch_with_policy(
                    &fn_call("abs", vec![col("f")]),
                    &batch,
                    FloatPolicy::Extended
                )
                .unwrap()
            ),
            vec![
                Some(f64::INFINITY.to_bits()),
                Some(f64::INFINITY.to_bits()),
                Some(canon_nan),
                Some(0.0_f64.to_bits()),
                None,
            ]
        );
        assert_eq!(
            evaluate_expression_batch_with_policy(
                &fn_call("signbit", vec![col("f")]),
                &batch,
                FloatPolicy::Extended
            )
            .unwrap()
            .to_scalars(),
            vec![
                ScalarValue::Boolean(false),
                ScalarValue::Boolean(true),
                ScalarValue::Boolean(true),
                ScalarValue::Boolean(true),
                ScalarValue::Null,
            ]
        );
        assert_eq!(
            float_bits(
                &evaluate_expression_batch_with_policy(
                    &fn_call(
                        "nullif",
                        vec![col("f"), lit(ScalarValue::Float64(f64::NAN))]
                    ),
                    &batch,
                    FloatPolicy::Extended,
                )
                .unwrap()
            ),
            vec![
                Some(f64::INFINITY.to_bits()),
                Some(f64::NEG_INFINITY.to_bits()),
                None,
                Some((-0.0_f64).to_bits()),
                None,
            ]
        );
        let coal = fn_call(
            "coalesce",
            vec![col("f"), lit(ScalarValue::Float64(f64::INFINITY))],
        );
        assert_eq!(
            float_bits(
                &evaluate_expression_batch_with_policy(&coal, &batch, FloatPolicy::Extended)
                    .unwrap()
            ),
            vec![
                Some(f64::INFINITY.to_bits()),
                Some(f64::NEG_INFINITY.to_bits()),
                Some(payload_nan_bits),
                Some((-0.0_f64).to_bits()),
                Some(f64::INFINITY.to_bits()),
            ]
        );
    }

    #[test]
    fn logical_operators_reject_nonboolean_operands_without_panicking() {
        let mut r0 = ExpressionInputRow::new();
        r0.insert("f".into(), ScalarValue::Float64(1.5));
        r0.insert("i".into(), ScalarValue::Int64(2));
        r0.insert("u".into(), ScalarValue::UInt64(3));
        r0.insert("bf".into(), ScalarValue::Boolean(false));
        r0.insert("bt".into(), ScalarValue::Boolean(true));
        let rows = [r0];
        let batch = ColumnarBatch::from_rows(&rows).unwrap();

        for op in [BinaryOp::And, BinaryOp::Or] {
            for (left, right) in [
                (col("f"), col("f")),
                (col("i"), col("i")),
                (col("u"), col("u")),
                (col("i"), col("f")),
            ] {
                let expr = bin(left, op, right);
                for policy in [FloatPolicy::Strict, FloatPolicy::Extended] {
                    assert!(matches!(
                        evaluate_expression_batch_with_policy(&expr, &batch, policy),
                        Err(ShardLoomError::NotImplemented(_))
                    ));
                }
                assert_parity(&expr, &rows);
            }
        }

        // 3VL short-circuit and NULL parity across mixed/typed columns.
        assert_parity(&bin(col("bf"), BinaryOp::And, col("f")), &rows);
        assert_parity(&bin(col("bt"), BinaryOp::Or, col("i")), &rows);
        assert_parity(
            &bin(col("bt"), BinaryOp::And, lit(ScalarValue::Null)),
            &rows,
        );
        assert_parity(&bin(col("bf"), BinaryOp::Or, lit(ScalarValue::Null)), &rows);

        let not_float = Expression::new(
            id("not_f"),
            ExpressionKind::Unary {
                op: UnaryOp::Not,
                expr: Box::new(col("f")),
            },
        );
        assert!(matches!(
            evaluate_expression_batch(&not_float, &batch),
            Err(ShardLoomError::NotImplemented(_))
        ));
        assert_parity(&not_float, &rows);

        let mut null_i_row = ExpressionInputRow::new();
        null_i_row.insert("ni".into(), ScalarValue::Null);
        let null_typed_batch = ColumnarBatch::new(
            1,
            BTreeMap::from([(
                "ni".to_string(),
                ColumnarColumn::Int64 {
                    values: vec![0],
                    valid: vec![false],
                },
            )]),
        )
        .unwrap();
        let not_null_typed = Expression::new(
            id("not_ni"),
            ExpressionKind::Unary {
                op: UnaryOp::Not,
                expr: Box::new(col("ni")),
            },
        );
        assert_eq!(
            evaluate_expression_batch(&not_null_typed, &null_typed_batch)
                .unwrap()
                .to_scalars(),
            vec![ScalarValue::Null]
        );
        assert_parity(&not_null_typed, &[null_i_row]);
    }

    #[test]
    fn columnar_column_validity_mismatch_is_rejected_without_panicking() {
        let malformed = vec![
            ColumnarColumn::Bool {
                values: vec![true, false],
                valid: vec![true],
            },
            ColumnarColumn::Int64 {
                values: vec![1, 2],
                valid: vec![true],
            },
            ColumnarColumn::UInt64 {
                values: vec![1, 2],
                valid: vec![true, false, true],
            },
            ColumnarColumn::Float64 {
                values: vec![1.0, 2.0],
                valid: Vec::new(),
            },
            ColumnarColumn::Utf8 {
                values: vec!["a".into(), "b".into()],
                valid: vec![true],
            },
            ColumnarColumn::Date32 {
                values: vec![1, 2],
                valid: vec![true],
            },
            ColumnarColumn::TimestampMicros {
                values: vec![1, 2],
                valid: vec![true],
            },
        ];
        for column in malformed {
            assert!(column.validate().is_err());
            assert!(column.is_null_at(1));
            assert_eq!(column.get(1), ScalarValue::Null);
            let err = ColumnarBatch::new(2, BTreeMap::from([("c".to_string(), column)]));
            assert!(err.is_err());
        }
    }
}
