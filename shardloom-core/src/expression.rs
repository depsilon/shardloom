//! Expression and kernel registry domain skeleton.
//!
//! This module defines native `ShardLoom` domain types for expression modeling,
//! kernel capability metadata, deterministic no-fallback selection results, and
//! a small shared semantics baseline for local fixture/runtime promotion work.

use std::collections::BTreeMap;

use regex::Regex;

use crate::{
    ColumnRef, ComparisonOp, Diagnostic, DiagnosticCode, DiagnosticSeverity, EncodingKind,
    ExecutionState, LogicalDType, MaterializationRequirement, Result, ShardLoomError,
};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ExprId(String);
impl ExprId {
    /// Creates a validated expression identifier.
    ///
    /// # Errors
    /// Returns [`ShardLoomError::InvalidOperation`] when the identifier is empty or whitespace only.
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        if value.trim().is_empty() {
            return Err(ShardLoomError::InvalidOperation(
                "expression id must not be empty".to_string(),
            ));
        }
        Ok(Self(value))
    }
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ScalarValue {
    Null,
    Boolean(bool),
    Int64(i64),
    UInt64(u64),
    Float64(f64),
    Utf8(String),
    Binary(Vec<u8>),
    Date32(i32),
    TimestampMicros(i64),
}
impl ScalarValue {
    #[must_use]
    pub fn dtype(&self) -> LogicalDType {
        match self {
            Self::Null => LogicalDType::Unknown,
            Self::Boolean(_) => LogicalDType::Boolean,
            Self::Int64(_) => LogicalDType::Int64,
            Self::UInt64(_) => LogicalDType::UInt64,
            Self::Float64(_) => LogicalDType::Float64,
            Self::Utf8(_) => LogicalDType::Utf8,
            Self::Binary(_) => LogicalDType::Binary,
            Self::Date32(_) => LogicalDType::Date32,
            Self::TimestampMicros(_) => LogicalDType::TimestampMicros,
        }
    }
    #[must_use]
    pub fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }
    #[must_use]
    pub fn summary(&self) -> String {
        match self {
            Self::Null => "null".to_string(),
            Self::Boolean(v) => format!("bool:{v}"),
            Self::Int64(v) => format!("i64:{v}"),
            Self::UInt64(v) => format!("u64:{v}"),
            Self::Float64(v) => format!("f64:{v}"),
            Self::Utf8(v) => format!("utf8:{v}"),
            Self::Binary(v) => format!("binary[len={}]", v.len()),
            Self::Date32(v) => format!("date32:{v}"),
            Self::TimestampMicros(v) => format!("ts_micros:{v}"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnaryOp {
    Not,
    IsNull,
    IsNotNull,
    Negate,
}
impl UnaryOp {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Not => "not",
            Self::IsNull => "is_null",
            Self::IsNotNull => "is_not_null",
            Self::Negate => "negate",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinaryOp {
    Add,
    Subtract,
    Multiply,
    Divide,
    And,
    Or,
}
impl BinaryOp {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Add => "add",
            Self::Subtract => "subtract",
            Self::Multiply => "multiply",
            Self::Divide => "divide",
            Self::And => "and",
            Self::Or => "or",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ExpressionKind {
    Literal(ScalarValue),
    Column(ColumnRef),
    Alias {
        expr: Box<Expression>,
        alias: String,
    },
    Cast {
        expr: Box<Expression>,
        target_dtype: LogicalDType,
    },
    TryCast {
        expr: Box<Expression>,
        target_dtype: LogicalDType,
    },
    Unary {
        op: UnaryOp,
        expr: Box<Expression>,
    },
    Binary {
        left: Box<Expression>,
        op: BinaryOp,
        right: Box<Expression>,
    },
    Compare {
        left: Box<Expression>,
        op: ComparisonOp,
        right: Box<Expression>,
    },
    FunctionCall {
        name: String,
        args: Vec<Expression>,
    },
    Unsupported {
        feature: String,
        reason: String,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct Expression {
    pub id: ExprId,
    pub kind: ExpressionKind,
    pub dtype: Option<LogicalDType>,
    pub diagnostics: Vec<Diagnostic>,
}
impl Expression {
    #[must_use]
    pub fn new(id: ExprId, kind: ExpressionKind) -> Self {
        Self {
            id,
            kind,
            dtype: None,
            diagnostics: Vec::new(),
        }
    }
    #[must_use]
    pub fn literal(id: ExprId, value: ScalarValue) -> Self {
        Self {
            id,
            dtype: Some(value.dtype()),
            kind: ExpressionKind::Literal(value),
            diagnostics: Vec::new(),
        }
    }
    #[must_use]
    pub fn column(id: ExprId, column: ColumnRef) -> Self {
        Self::new(id, ExpressionKind::Column(column))
    }
    #[must_use]
    pub fn cast(id: ExprId, expr: Expression, target_dtype: LogicalDType) -> Self {
        Self {
            id,
            dtype: Some(target_dtype.clone()),
            kind: ExpressionKind::Cast {
                expr: Box::new(expr),
                target_dtype,
            },
            diagnostics: Vec::new(),
        }
    }
    #[must_use]
    pub fn try_cast(id: ExprId, expr: Expression, target_dtype: LogicalDType) -> Self {
        Self {
            id,
            dtype: Some(target_dtype.clone()),
            kind: ExpressionKind::TryCast {
                expr: Box::new(expr),
                target_dtype,
            },
            diagnostics: Vec::new(),
        }
    }
    #[must_use]
    pub fn unsupported(id: ExprId, feature: impl Into<String>, reason: impl Into<String>) -> Self {
        let feature = feature.into();
        let reason = reason.into();
        let mut expr = Self::new(
            id,
            ExpressionKind::Unsupported {
                feature: feature.clone(),
                reason: reason.clone(),
            },
        );
        expr.add_diagnostic(Diagnostic::unsupported(
            DiagnosticCode::NotImplemented,
            feature,
            format!("Unsupported expression: {reason}"),
            Some("Use a supported expression kind.".to_string()),
        ));
        expr
    }
    #[must_use]
    pub fn with_dtype(mut self, dtype: LogicalDType) -> Self {
        self.dtype = Some(dtype);
        self
    }
    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
    }
    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.diagnostics.iter().any(|d| {
            matches!(
                d.severity,
                DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
            )
        })
    }
    #[must_use]
    pub fn summary(&self) -> String {
        format!(
            "expr[id={}, kind={}]",
            self.id.as_str(),
            match &self.kind {
                ExpressionKind::Literal(v) => format!("literal({})", v.summary()),
                ExpressionKind::Column(c) => format!("column({})", c.as_str()),
                ExpressionKind::Alias { alias, .. } => format!("alias({alias})"),
                ExpressionKind::Cast { target_dtype, .. } => {
                    format!("cast({})", target_dtype.as_str())
                }
                ExpressionKind::TryCast { target_dtype, .. } => {
                    format!("try_cast({})", target_dtype.as_str())
                }
                ExpressionKind::Unary { op, .. } => format!("unary({})", op.as_str()),
                ExpressionKind::Binary { op, .. } => format!("binary({})", op.as_str()),
                ExpressionKind::Compare { op, .. } => format!("compare({})", op.as_str()),
                ExpressionKind::FunctionCall { name, args } => format!("fn({name}/{})", args.len()),
                ExpressionKind::Unsupported { feature, .. } => format!("unsupported({feature})"),
            }
        )
    }
}

/// Materialized scalar row used by the scoped native semantics baseline.
///
/// This is intentionally an in-memory row contract for local fixture and first
/// runtime promotion work. It does not read datasets, invoke external engines,
/// or imply that broad SQL/DataFrame execution is supported.
pub type ExpressionInputRow = BTreeMap<String, ScalarValue>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExpressionEvaluationStatus {
    Evaluated,
    InvalidInput,
    Unsupported,
}

impl ExpressionEvaluationStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Evaluated => "evaluated",
            Self::InvalidInput => "invalid_input",
            Self::Unsupported => "unsupported",
        }
    }

    #[must_use]
    pub const fn is_success(&self) -> bool {
        matches!(self, Self::Evaluated)
    }
}

#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Clone, PartialEq)]
pub struct ExpressionEvaluationReport {
    pub schema_version: &'static str,
    pub expression_id: String,
    pub operator_family: String,
    pub status: ExpressionEvaluationStatus,
    pub value: Option<ScalarValue>,
    pub output_dtype: Option<LogicalDType>,
    pub null_behavior: NullBehavior,
    pub materialization_requirement: MaterializationRequirement,
    pub data_decoded: bool,
    pub data_materialized: bool,
    pub fallback_attempted: bool,
    pub external_engine_invoked: bool,
    pub claim_gate_status: &'static str,
    pub diagnostics: Vec<Diagnostic>,
}

impl ExpressionEvaluationReport {
    fn evaluated(expression: &Expression, value: EvalValue) -> Self {
        Self {
            schema_version: "shardloom.expression_semantics.v1",
            expression_id: expression.id.as_str().to_string(),
            operator_family: expression_operator_family(expression).to_string(),
            status: ExpressionEvaluationStatus::Evaluated,
            value: Some(value.value),
            output_dtype: Some(value.dtype),
            null_behavior: value.null_behavior,
            materialization_requirement: value.materialization_requirement,
            data_decoded: false,
            data_materialized: value.data_materialized,
            fallback_attempted: false,
            external_engine_invoked: false,
            claim_gate_status: "not_claim_grade",
            diagnostics: Vec::new(),
        }
    }

    fn blocked(expression: &Expression, failure: EvalFailure) -> Self {
        Self {
            schema_version: "shardloom.expression_semantics.v1",
            expression_id: expression.id.as_str().to_string(),
            operator_family: expression_operator_family(expression).to_string(),
            status: failure.status,
            value: None,
            output_dtype: expression.dtype.clone(),
            null_behavior: NullBehavior::Unsupported,
            materialization_requirement: MaterializationRequirement::Unknown {
                reason: "expression semantics blocked".to_string(),
            },
            data_decoded: false,
            data_materialized: false,
            fallback_attempted: false,
            external_engine_invoked: false,
            claim_gate_status: "not_claim_grade",
            diagnostics: vec![failure.diagnostic],
        }
    }

    #[must_use]
    pub fn has_errors(&self) -> bool {
        !self.status.is_success()
            || self.diagnostics.iter().any(|d| {
                matches!(
                    d.severity,
                    DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
                )
            })
    }

    #[must_use]
    pub const fn fallback_execution_allowed(&self) -> bool {
        false
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ProjectedExpressionValue {
    pub name: String,
    pub value: ScalarValue,
    pub dtype: LogicalDType,
}

#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Clone, PartialEq)]
pub struct ProjectionEvaluationReport {
    pub schema_version: &'static str,
    pub status: ExpressionEvaluationStatus,
    pub projected_columns: Vec<ProjectedExpressionValue>,
    pub data_decoded: bool,
    pub data_materialized: bool,
    pub fallback_attempted: bool,
    pub external_engine_invoked: bool,
    pub claim_gate_status: &'static str,
    pub diagnostics: Vec<Diagnostic>,
}

impl ProjectionEvaluationReport {
    #[must_use]
    pub fn has_errors(&self) -> bool {
        !self.status.is_success()
            || self.diagnostics.iter().any(|d| {
                matches!(
                    d.severity,
                    DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
                )
            })
    }

    #[must_use]
    pub const fn fallback_execution_allowed(&self) -> bool {
        false
    }
}

#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FilterEvaluationReport {
    pub schema_version: &'static str,
    pub status: ExpressionEvaluationStatus,
    pub input_row_count: usize,
    pub selected_row_indexes: Vec<usize>,
    pub null_predicate_row_count: usize,
    pub data_decoded: bool,
    pub data_materialized: bool,
    pub fallback_attempted: bool,
    pub external_engine_invoked: bool,
    pub claim_gate_status: &'static str,
    pub diagnostics: Vec<Diagnostic>,
}

impl FilterEvaluationReport {
    #[must_use]
    pub fn selected_row_count(&self) -> usize {
        self.selected_row_indexes.len()
    }

    #[must_use]
    pub fn has_errors(&self) -> bool {
        !self.status.is_success()
            || self.diagnostics.iter().any(|d| {
                matches!(
                    d.severity,
                    DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
                )
            })
    }

    #[must_use]
    pub const fn fallback_execution_allowed(&self) -> bool {
        false
    }
}

#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LimitEvaluationReport {
    pub schema_version: &'static str,
    pub status: ExpressionEvaluationStatus,
    pub input_row_count: usize,
    pub limit: usize,
    pub output_row_count: usize,
    pub data_decoded: bool,
    pub data_materialized: bool,
    pub fallback_attempted: bool,
    pub external_engine_invoked: bool,
    pub claim_gate_status: &'static str,
    pub diagnostics: Vec<Diagnostic>,
}

impl LimitEvaluationReport {
    #[must_use]
    pub fn has_errors(&self) -> bool {
        !self.status.is_success()
            || self.diagnostics.iter().any(|d| {
                matches!(
                    d.severity,
                    DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
                )
            })
    }

    #[must_use]
    pub const fn fallback_execution_allowed(&self) -> bool {
        false
    }
}

#[derive(Debug, Clone, PartialEq)]
struct EvalValue {
    value: ScalarValue,
    dtype: LogicalDType,
    null_behavior: NullBehavior,
    materialization_requirement: MaterializationRequirement,
    data_materialized: bool,
}

impl EvalValue {
    fn new(value: ScalarValue, dtype: LogicalDType, null_behavior: NullBehavior) -> Self {
        let dtype = if matches!(value, ScalarValue::Null) {
            dtype
        } else {
            value.dtype()
        };
        Self {
            value,
            dtype,
            null_behavior,
            materialization_requirement: MaterializationRequirement::None,
            data_materialized: false,
        }
    }

    fn materialized(mut self) -> Self {
        self.materialization_requirement = MaterializationRequirement::Full {
            reason: "in-memory expression row semantics baseline".to_string(),
        };
        self.data_materialized = true;
        self
    }

    fn carry_materialization(mut self, data_materialized: bool) -> Self {
        if data_materialized && !self.data_materialized {
            self = self.materialized();
        }
        self
    }

    fn null(dtype: LogicalDType, null_behavior: NullBehavior) -> Self {
        Self::new(ScalarValue::Null, dtype, null_behavior)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct EvalFailure {
    status: ExpressionEvaluationStatus,
    diagnostic: Diagnostic,
}

impl EvalFailure {
    fn invalid(feature: impl Into<String>, reason: impl Into<String>) -> Box<Self> {
        Box::new(Self {
            status: ExpressionEvaluationStatus::InvalidInput,
            diagnostic: Diagnostic::invalid_input(
                feature,
                reason,
                "Use admitted expression semantics for the current runtime slice.",
            ),
        })
    }

    fn unsupported(feature: impl Into<String>, reason: impl Into<String>) -> Box<Self> {
        Box::new(Self {
            status: ExpressionEvaluationStatus::Unsupported,
            diagnostic: Diagnostic::not_implemented(
                feature,
                reason,
                "Use a supported expression or wait for a later native semantics slice.",
            ),
        })
    }
}

type EvalResult<T> = std::result::Result<T, Box<EvalFailure>>;

#[must_use]
pub fn evaluate_expression(
    expression: &Expression,
    row: &ExpressionInputRow,
) -> ExpressionEvaluationReport {
    match eval_expression(expression, row) {
        Ok(value) => ExpressionEvaluationReport::evaluated(expression, value),
        Err(failure) => ExpressionEvaluationReport::blocked(expression, *failure),
    }
}

#[must_use]
pub fn evaluate_projection(
    expressions: &[Expression],
    row: &ExpressionInputRow,
) -> ProjectionEvaluationReport {
    let mut projected_columns = Vec::with_capacity(expressions.len());
    let mut diagnostics = Vec::new();
    let mut data_materialized = false;
    for expression in expressions {
        match eval_expression(expression, row) {
            Ok(value) => {
                data_materialized |= value.data_materialized;
                projected_columns.push(ProjectedExpressionValue {
                    name: projection_name(expression),
                    value: value.value,
                    dtype: value.dtype,
                });
            }
            Err(failure) => diagnostics.push(failure.diagnostic),
        }
    }
    let status = if diagnostics.is_empty() {
        ExpressionEvaluationStatus::Evaluated
    } else {
        ExpressionEvaluationStatus::Unsupported
    };
    ProjectionEvaluationReport {
        schema_version: "shardloom.projection_semantics.v1",
        status,
        projected_columns,
        data_decoded: false,
        data_materialized,
        fallback_attempted: false,
        external_engine_invoked: false,
        claim_gate_status: "not_claim_grade",
        diagnostics,
    }
}

#[must_use]
pub fn evaluate_filter(
    predicate: &Expression,
    rows: &[ExpressionInputRow],
) -> FilterEvaluationReport {
    let mut selected_row_indexes = Vec::new();
    let mut null_predicate_row_count = 0;
    let mut diagnostics = Vec::new();
    let mut data_materialized = false;

    for (row_index, row) in rows.iter().enumerate() {
        match eval_expression(predicate, row) {
            Ok(value) => {
                data_materialized |= value.data_materialized;
                match value.value {
                    ScalarValue::Boolean(true) => selected_row_indexes.push(row_index),
                    ScalarValue::Boolean(false) => {}
                    ScalarValue::Null => null_predicate_row_count += 1,
                    other => diagnostics.push(
                        EvalFailure::invalid(
                            "filter_predicate",
                            format!(
                                "filter predicate must evaluate to boolean or null, got {}",
                                other.dtype().as_str()
                            ),
                        )
                        .diagnostic,
                    ),
                }
            }
            Err(failure) => diagnostics.push(failure.diagnostic),
        }
    }

    let status = if diagnostics.is_empty() {
        ExpressionEvaluationStatus::Evaluated
    } else {
        ExpressionEvaluationStatus::Unsupported
    };
    FilterEvaluationReport {
        schema_version: "shardloom.filter_semantics.v1",
        status,
        input_row_count: rows.len(),
        selected_row_indexes,
        null_predicate_row_count,
        data_decoded: false,
        data_materialized,
        fallback_attempted: false,
        external_engine_invoked: false,
        claim_gate_status: "not_claim_grade",
        diagnostics,
    }
}

#[must_use]
pub fn evaluate_limit(input_row_count: usize, limit: usize) -> LimitEvaluationReport {
    LimitEvaluationReport {
        schema_version: "shardloom.limit_semantics.v1",
        status: ExpressionEvaluationStatus::Evaluated,
        input_row_count,
        limit,
        output_row_count: input_row_count.min(limit),
        data_decoded: false,
        data_materialized: false,
        fallback_attempted: false,
        external_engine_invoked: false,
        claim_gate_status: "not_claim_grade",
        diagnostics: Vec::new(),
    }
}

fn eval_expression(expression: &Expression, row: &ExpressionInputRow) -> EvalResult<EvalValue> {
    match &expression.kind {
        ExpressionKind::Literal(value) => Ok(EvalValue::new(
            value.clone(),
            value.dtype(),
            if value.is_null() {
                NullBehavior::NullAware
            } else {
                NullBehavior::NullPropagating
            },
        )),
        ExpressionKind::Column(column) => row
            .get(column.as_str())
            .cloned()
            .map(|value| EvalValue::new(value.clone(), value.dtype(), NullBehavior::NullAware))
            .map(EvalValue::materialized)
            .ok_or_else(|| {
                EvalFailure::invalid(
                    "column_reference",
                    format!(
                        "column {:?} is not present in the expression input row",
                        column.as_str()
                    ),
                )
            }),
        ExpressionKind::Alias { expr, .. } => eval_expression(expr, row),
        ExpressionKind::Cast { expr, target_dtype } => {
            let value = eval_expression(expr, row)?;
            cast_eval_value(&value, target_dtype)
        }
        ExpressionKind::TryCast { expr, target_dtype } => {
            let value = eval_expression(expr, row)?;
            try_cast_eval_value(&value, target_dtype)
        }
        ExpressionKind::Unary { op, expr } => {
            let value = eval_expression(expr, row)?;
            eval_unary(*op, value)
        }
        ExpressionKind::Binary { left, op, right } => {
            let left = eval_expression(left, row)?;
            let right = eval_expression(right, row)?;
            eval_binary(left, *op, right)
        }
        ExpressionKind::Compare { left, op, right } => {
            let left = eval_expression(left, row)?;
            let right = eval_expression(right, row)?;
            eval_compare(&left, *op, &right)
        }
        ExpressionKind::FunctionCall { name, args } => eval_function_call(name, args, row),
        ExpressionKind::Unsupported { feature, reason } => {
            Err(EvalFailure::unsupported(feature.clone(), reason.clone()))
        }
    }
}

fn eval_unary(op: UnaryOp, value: EvalValue) -> EvalResult<EvalValue> {
    let data_materialized = value.data_materialized;
    let result = match op {
        UnaryOp::IsNull => Ok(EvalValue::new(
            ScalarValue::Boolean(value.value.is_null()),
            LogicalDType::Boolean,
            NullBehavior::NullAware,
        )),
        UnaryOp::IsNotNull => Ok(EvalValue::new(
            ScalarValue::Boolean(!value.value.is_null()),
            LogicalDType::Boolean,
            NullBehavior::NullAware,
        )),
        UnaryOp::Not => match value.value {
            ScalarValue::Null => Ok(EvalValue::null(
                LogicalDType::Boolean,
                NullBehavior::NullPropagating,
            )),
            ScalarValue::Boolean(v) => Ok(EvalValue::new(
                ScalarValue::Boolean(!v),
                LogicalDType::Boolean,
                NullBehavior::NullAware,
            )),
            other => Err(EvalFailure::unsupported(
                "unary_not",
                format!(
                    "NOT supports boolean or null, got {}",
                    other.dtype().as_str()
                ),
            )),
        },
        UnaryOp::Negate => match value.value {
            ScalarValue::Null => Ok(EvalValue::null(value.dtype, NullBehavior::NullPropagating)),
            ScalarValue::Int64(v) => v
                .checked_neg()
                .map(|out| {
                    EvalValue::new(
                        ScalarValue::Int64(out),
                        LogicalDType::Int64,
                        NullBehavior::NullPropagating,
                    )
                })
                .ok_or_else(|| EvalFailure::invalid("negate", "int64 negation overflow")),
            ScalarValue::Float64(v) if v.is_finite() => Ok(EvalValue::new(
                ScalarValue::Float64(-v),
                LogicalDType::Float64,
                NullBehavior::NullPropagating,
            )),
            other => Err(EvalFailure::unsupported(
                "negate",
                format!(
                    "negate supports finite int64/float64 values, got {}",
                    other.dtype().as_str()
                ),
            )),
        },
    }?;
    Ok(result.carry_materialization(data_materialized))
}

fn eval_binary(left: EvalValue, op: BinaryOp, right: EvalValue) -> EvalResult<EvalValue> {
    let data_materialized = left.data_materialized || right.data_materialized;
    match op {
        BinaryOp::And => eval_boolean_and(left.value, right.value),
        BinaryOp::Or => eval_boolean_or(left.value, right.value),
        BinaryOp::Add | BinaryOp::Subtract | BinaryOp::Multiply | BinaryOp::Divide => {
            eval_numeric_binary(left.value, op, right.value)
        }
    }
    .map(|value| value.carry_materialization(data_materialized))
}

fn eval_function_call(
    name: &str,
    args: &[Expression],
    row: &ExpressionInputRow,
) -> EvalResult<EvalValue> {
    let normalized = name.trim().to_ascii_lowercase();
    match normalized.as_str() {
        "utf8_starts_with" | "starts_with" => {
            eval_string_predicate(name, args, row, |value, needle| value.starts_with(needle))
        }
        "utf8_contains" | "contains" => {
            eval_string_predicate(name, args, row, |value, needle| value.contains(needle))
        }
        "utf8_ends_with" | "ends_with" => {
            eval_string_predicate(name, args, row, |value, needle| value.ends_with(needle))
        }
        "utf8_regex_match" | "regex_match" | "rlike" | "regexp" | "regexp_like" => {
            eval_string_regex_predicate(name, args, row)
        }
        "utf8_lower" | "lower" => eval_string_transform(name, args, row, str::to_lowercase),
        "utf8_upper" | "upper" => eval_string_transform(name, args, row, str::to_uppercase),
        "utf8_trim" | "trim" => {
            eval_string_transform(name, args, row, |value| value.trim().to_string())
        }
        "utf8_length" | "length" => eval_string_length(name, args, row),
        "utf8_concat" | "concat" => eval_string_concat(name, args, row),
        "utf8_substr" | "utf8_substring" | "substr" | "substring" => {
            eval_string_substr(name, args, row)
        }
        "utf8_left" | "left" => eval_string_left_right(name, args, row, false),
        "utf8_right" | "right" => eval_string_left_right(name, args, row, true),
        "utf8_replace" | "replace" => eval_string_replace(name, args, row),
        "binary_unhex" | "unhex" => eval_binary_unhex(name, args, row),
        "binary_from_base64" | "from_base64" => eval_binary_from_base64(name, args, row),
        "numeric_abs" | "abs" => eval_numeric_abs(name, args, row),
        "numeric_floor" | "floor" => eval_numeric_rounding(name, args, row, f64::floor),
        "numeric_ceil" | "ceil" | "ceiling" => eval_numeric_rounding(name, args, row, f64::ceil),
        "numeric_round" | "round" => eval_numeric_rounding(name, args, row, f64::round),
        "date_year" | "year" => eval_date_extract(name, args, row, date32_year),
        "date_month" | "month" => eval_date_extract(name, args, row, |days| {
            i32::try_from(date32_month(days)).expect("month fits i32")
        }),
        "date_day" | "day" => eval_date_extract(name, args, row, |days| {
            i32::try_from(date32_day(days)).expect("day fits i32")
        }),
        "timestamp_year" => eval_timestamp_extract(name, args, row, |micros| {
            i64::from(timestamp_micros_year(micros))
        }),
        "timestamp_month" => eval_timestamp_extract(name, args, row, |micros| {
            timestamp_micros_month(micros).into()
        }),
        "timestamp_day" => eval_timestamp_extract(name, args, row, |micros| {
            timestamp_micros_day(micros).into()
        }),
        "timestamp_hour" => eval_timestamp_extract(name, args, row, |micros| {
            timestamp_micros_hour(micros).into()
        }),
        "timestamp_minute" => eval_timestamp_extract(name, args, row, |micros| {
            timestamp_micros_minute(micros).into()
        }),
        "timestamp_second" => eval_timestamp_extract(name, args, row, |micros| {
            timestamp_micros_second(micros).into()
        }),
        "date_add_days" => eval_date_add_days(name, args, row, 1),
        "date_sub_days" => eval_date_add_days(name, args, row, -1),
        "timestamp_add_seconds" => eval_timestamp_add_seconds(name, args, row, 1),
        "timestamp_sub_seconds" => eval_timestamp_add_seconds(name, args, row, -1),
        "date_diff_days" => eval_date_diff_days(name, args, row),
        "timestamp_diff_seconds" => eval_timestamp_diff_seconds(name, args, row),
        "coalesce" => eval_coalesce(name, args, row),
        "nullif" => eval_nullif(name, args, row),
        "case_when" => eval_case_when(name, args, row),
        _ => Err(EvalFailure::unsupported(
            "function_call",
            format!("function {name:?} is not admitted by the current native semantics baseline"),
        )),
    }
}

fn eval_coalesce(
    name: &str,
    args: &[Expression],
    row: &ExpressionInputRow,
) -> EvalResult<EvalValue> {
    if args.len() != 2 {
        return Err(EvalFailure::invalid(
            "null_coalesce",
            format!("function {name:?} requires exactly two arguments"),
        ));
    }
    let value = eval_expression(&args[0], row)?;
    let fallback = eval_expression(&args[1], row)?;
    let data_materialized = value.data_materialized || fallback.data_materialized;
    if !value.value.is_null() {
        return Ok(value.carry_materialization(data_materialized));
    }
    Ok(fallback.carry_materialization(data_materialized))
}

fn eval_nullif(name: &str, args: &[Expression], row: &ExpressionInputRow) -> EvalResult<EvalValue> {
    if args.len() != 2 {
        return Err(EvalFailure::invalid(
            "nullif_projection",
            format!("function {name:?} requires exactly two arguments"),
        ));
    }
    let value = eval_expression(&args[0], row)?;
    let sentinel = eval_expression(&args[1], row)?;
    let data_materialized = value.data_materialized || sentinel.data_materialized;
    if value.value.is_null() || sentinel.value.is_null() {
        return Ok(value.carry_materialization(data_materialized));
    }
    let equals = match eval_compare(&value, ComparisonOp::Eq, &sentinel)?.value {
        ScalarValue::Boolean(result) => result,
        ScalarValue::Null => false,
        other => {
            return Err(EvalFailure::unsupported(
                "nullif_projection",
                format!("function {name:?} equality comparison returned {other:?}"),
            ));
        }
    };
    if equals {
        Ok(EvalValue::null(value.dtype, NullBehavior::NullAware)
            .carry_materialization(data_materialized))
    } else {
        Ok(value.carry_materialization(data_materialized))
    }
}

fn eval_case_when(
    name: &str,
    args: &[Expression],
    row: &ExpressionInputRow,
) -> EvalResult<EvalValue> {
    if args.len() != 3 {
        return Err(EvalFailure::invalid(
            "conditional_projection",
            format!("function {name:?} requires exactly three arguments"),
        ));
    }
    let predicate = eval_expression(&args[0], row)?;
    let predicate_materialized = predicate.data_materialized;
    let selected = match predicate.value {
        ScalarValue::Boolean(true) => eval_expression(&args[1], row)?,
        ScalarValue::Boolean(false) | ScalarValue::Null => eval_expression(&args[2], row)?,
        other => {
            return Err(EvalFailure::unsupported(
                "conditional_projection",
                format!(
                    "function {name:?} requires a boolean/null predicate, got {}",
                    other.dtype().as_str()
                ),
            ));
        }
    };
    let data_materialized = predicate_materialized || selected.data_materialized;
    Ok(selected.carry_materialization(data_materialized))
}

fn eval_timestamp_extract(
    name: &str,
    args: &[Expression],
    row: &ExpressionInputRow,
    extract: impl FnOnce(i64) -> i64,
) -> EvalResult<EvalValue> {
    if args.len() != 1 {
        return Err(EvalFailure::invalid(
            "timestamp_extract",
            format!("function {name:?} requires exactly one TimestampMicros argument"),
        ));
    }
    let value = eval_expression(&args[0], row)?;
    let data_materialized = value.data_materialized;
    match value.value {
        ScalarValue::Null => Ok(EvalValue::null(
            LogicalDType::Int64,
            NullBehavior::NullPropagating,
        )
        .carry_materialization(data_materialized)),
        ScalarValue::TimestampMicros(micros) => Ok(EvalValue::new(
            ScalarValue::Int64(extract(micros)),
            LogicalDType::Int64,
            NullBehavior::NullPropagating,
        )
        .carry_materialization(data_materialized)),
        other => Err(EvalFailure::unsupported(
            "timestamp_extract",
            format!(
                "function {name:?} supports TimestampMicros/null operands only, got {}",
                other.dtype().as_str()
            ),
        )),
    }
}

fn eval_date_add_days(
    name: &str,
    args: &[Expression],
    row: &ExpressionInputRow,
    multiplier: i32,
) -> EvalResult<EvalValue> {
    if args.len() != 2 {
        return Err(EvalFailure::invalid(
            "date_arithmetic",
            format!("function {name:?} requires Date32 and int64 day-count arguments"),
        ));
    }
    let value = eval_expression(&args[0], row)?;
    let days = eval_expression(&args[1], row)?;
    let data_materialized = value.data_materialized || days.data_materialized;
    if value.value.is_null() || days.value.is_null() {
        return Ok(
            EvalValue::null(LogicalDType::Date32, NullBehavior::NullPropagating)
                .carry_materialization(data_materialized),
        );
    }
    match (value.value, days.value) {
        (ScalarValue::Date32(date_days), ScalarValue::Int64(day_count)) => {
            let signed_day_count = i32::try_from(day_count).map_err(|_| {
                EvalFailure::invalid("date_arithmetic", "day-count argument exceeds i32 range")
            })?;
            let offset = signed_day_count.checked_mul(multiplier).ok_or_else(|| {
                EvalFailure::invalid("date_arithmetic", "day-count multiplication overflow")
            })?;
            let result = date_days.checked_add(offset).ok_or_else(|| {
                EvalFailure::invalid("date_arithmetic", "date32 day arithmetic overflow")
            })?;
            Ok(EvalValue::new(
                ScalarValue::Date32(result),
                LogicalDType::Date32,
                NullBehavior::NullPropagating,
            )
            .carry_materialization(data_materialized))
        }
        (value, days) => Err(EvalFailure::unsupported(
            "date_arithmetic",
            format!(
                "function {name:?} supports Date32/null and int64/null operands only, got {} and {}",
                value.dtype().as_str(),
                days.dtype().as_str()
            ),
        )),
    }
}

fn eval_timestamp_add_seconds(
    name: &str,
    args: &[Expression],
    row: &ExpressionInputRow,
    multiplier: i64,
) -> EvalResult<EvalValue> {
    if args.len() != 2 {
        return Err(EvalFailure::invalid(
            "timestamp_arithmetic",
            format!("function {name:?} requires TimestampMicros and int64 second-count arguments"),
        ));
    }
    let value = eval_expression(&args[0], row)?;
    let seconds = eval_expression(&args[1], row)?;
    let data_materialized = value.data_materialized || seconds.data_materialized;
    if value.value.is_null() || seconds.value.is_null() {
        return Ok(
            EvalValue::null(LogicalDType::TimestampMicros, NullBehavior::NullPropagating)
                .carry_materialization(data_materialized),
        );
    }
    match (value.value, seconds.value) {
        (ScalarValue::TimestampMicros(micros), ScalarValue::Int64(second_count)) => {
            let signed_second_count = second_count.checked_mul(multiplier).ok_or_else(|| {
                EvalFailure::invalid(
                    "timestamp_arithmetic",
                    "second-count multiplication overflow",
                )
            })?;
            let offset_micros = signed_second_count
                .checked_mul(MICROS_PER_SECOND)
                .ok_or_else(|| {
                    EvalFailure::invalid(
                        "timestamp_arithmetic",
                        "second-count microsecond conversion overflow",
                    )
                })?;
            let result = micros.checked_add(offset_micros).ok_or_else(|| {
                EvalFailure::invalid("timestamp_arithmetic", "timestamp arithmetic overflow")
            })?;
            Ok(EvalValue::new(
                ScalarValue::TimestampMicros(result),
                LogicalDType::TimestampMicros,
                NullBehavior::NullPropagating,
            )
            .carry_materialization(data_materialized))
        }
        (value, seconds) => Err(EvalFailure::unsupported(
            "timestamp_arithmetic",
            format!(
                "function {name:?} supports TimestampMicros/null and int64/null operands only, got {} and {}",
                value.dtype().as_str(),
                seconds.dtype().as_str()
            ),
        )),
    }
}

fn eval_date_diff_days(
    name: &str,
    args: &[Expression],
    row: &ExpressionInputRow,
) -> EvalResult<EvalValue> {
    if args.len() != 2 {
        return Err(EvalFailure::invalid(
            "temporal_difference",
            format!("function {name:?} requires exactly two Date32 arguments"),
        ));
    }
    let left = eval_expression(&args[0], row)?;
    let right = eval_expression(&args[1], row)?;
    let data_materialized = left.data_materialized || right.data_materialized;
    if left.value.is_null() || right.value.is_null() {
        return Ok(
            EvalValue::null(LogicalDType::Int64, NullBehavior::NullPropagating)
                .carry_materialization(data_materialized),
        );
    }
    match (left.value, right.value) {
        (ScalarValue::Date32(left_days), ScalarValue::Date32(right_days)) => {
            let result = i64::from(left_days) - i64::from(right_days);
            Ok(EvalValue::new(
                ScalarValue::Int64(result),
                LogicalDType::Int64,
                NullBehavior::NullPropagating,
            )
            .carry_materialization(data_materialized))
        }
        (left, right) => Err(EvalFailure::unsupported(
            "temporal_difference",
            format!(
                "function {name:?} supports Date32/null operands only, got {} and {}",
                left.dtype().as_str(),
                right.dtype().as_str()
            ),
        )),
    }
}

fn eval_timestamp_diff_seconds(
    name: &str,
    args: &[Expression],
    row: &ExpressionInputRow,
) -> EvalResult<EvalValue> {
    if args.len() != 2 {
        return Err(EvalFailure::invalid(
            "temporal_difference",
            format!("function {name:?} requires exactly two TimestampMicros arguments"),
        ));
    }
    let left = eval_expression(&args[0], row)?;
    let right = eval_expression(&args[1], row)?;
    let data_materialized = left.data_materialized || right.data_materialized;
    if left.value.is_null() || right.value.is_null() {
        return Ok(
            EvalValue::null(LogicalDType::Int64, NullBehavior::NullPropagating)
                .carry_materialization(data_materialized),
        );
    }
    match (left.value, right.value) {
        (ScalarValue::TimestampMicros(left_micros), ScalarValue::TimestampMicros(right_micros)) => {
            let delta_micros = left_micros.checked_sub(right_micros).ok_or_else(|| {
                EvalFailure::invalid("temporal_difference", "timestamp difference overflow")
            })?;
            Ok(EvalValue::new(
                ScalarValue::Int64(delta_micros / MICROS_PER_SECOND),
                LogicalDType::Int64,
                NullBehavior::NullPropagating,
            )
            .carry_materialization(data_materialized))
        }
        (left, right) => Err(EvalFailure::unsupported(
            "temporal_difference",
            format!(
                "function {name:?} supports TimestampMicros/null operands only, got {} and {}",
                left.dtype().as_str(),
                right.dtype().as_str()
            ),
        )),
    }
}

fn eval_date_extract(
    name: &str,
    args: &[Expression],
    row: &ExpressionInputRow,
    extract: impl FnOnce(i32) -> i32,
) -> EvalResult<EvalValue> {
    if args.len() != 1 {
        return Err(EvalFailure::invalid(
            "date_extract",
            format!("function {name:?} requires exactly one Date32 argument"),
        ));
    }
    let value = eval_expression(&args[0], row)?;
    let data_materialized = value.data_materialized;
    match value.value {
        ScalarValue::Null => Ok(EvalValue::null(
            LogicalDType::Int64,
            NullBehavior::NullPropagating,
        )
        .carry_materialization(data_materialized)),
        ScalarValue::Date32(days) => Ok(EvalValue::new(
            ScalarValue::Int64(i64::from(extract(days))),
            LogicalDType::Int64,
            NullBehavior::NullPropagating,
        )
        .carry_materialization(data_materialized)),
        other => Err(EvalFailure::unsupported(
            "date_extract",
            format!(
                "function {name:?} supports Date32/null operands only, got {}",
                other.dtype().as_str()
            ),
        )),
    }
}

fn eval_string_transform(
    name: &str,
    args: &[Expression],
    row: &ExpressionInputRow,
    transform: impl FnOnce(&str) -> String,
) -> EvalResult<EvalValue> {
    if args.len() != 1 {
        return Err(EvalFailure::invalid(
            "string_transform",
            format!("function {name:?} requires exactly one UTF-8 argument"),
        ));
    }
    let value = eval_expression(&args[0], row)?;
    let data_materialized = value.data_materialized;
    match value.value {
        ScalarValue::Null => Ok(
            EvalValue::null(LogicalDType::Utf8, NullBehavior::NullPropagating)
                .carry_materialization(data_materialized),
        ),
        ScalarValue::Utf8(value) => Ok(EvalValue::new(
            ScalarValue::Utf8(transform(&value)),
            LogicalDType::Utf8,
            NullBehavior::NullPropagating,
        )
        .carry_materialization(data_materialized)),
        other => Err(EvalFailure::unsupported(
            "string_transform",
            format!(
                "function {name:?} supports UTF-8/null operands only, got {}",
                other.dtype().as_str()
            ),
        )),
    }
}

fn eval_string_length(
    name: &str,
    args: &[Expression],
    row: &ExpressionInputRow,
) -> EvalResult<EvalValue> {
    if args.len() != 1 {
        return Err(EvalFailure::invalid(
            "string_length",
            format!("function {name:?} requires exactly one UTF-8 argument"),
        ));
    }
    let value = eval_expression(&args[0], row)?;
    let data_materialized = value.data_materialized;
    match value.value {
        ScalarValue::Null => Ok(EvalValue::null(
            LogicalDType::Int64,
            NullBehavior::NullPropagating,
        )
        .carry_materialization(data_materialized)),
        ScalarValue::Utf8(value) => {
            let length = i64::try_from(value.chars().count()).map_err(|_| {
                EvalFailure::unsupported(
                    "string_length",
                    format!("function {name:?} input length exceeds int64 range"),
                )
            })?;
            Ok(EvalValue::new(
                ScalarValue::Int64(length),
                LogicalDType::Int64,
                NullBehavior::NullPropagating,
            )
            .carry_materialization(data_materialized))
        }
        other => Err(EvalFailure::unsupported(
            "string_length",
            format!(
                "function {name:?} supports UTF-8/null operands only, got {}",
                other.dtype().as_str()
            ),
        )),
    }
}

fn eval_string_concat(
    name: &str,
    args: &[Expression],
    row: &ExpressionInputRow,
) -> EvalResult<EvalValue> {
    if args.len() < 2 {
        return Err(EvalFailure::invalid(
            "string_function",
            format!("function {name:?} requires at least two UTF-8 arguments"),
        ));
    }
    let (values, data_materialized) = eval_function_args(args, row)?;
    if let Some(other) = values
        .iter()
        .find(|value| !matches!(value, ScalarValue::Utf8(_) | ScalarValue::Null))
    {
        return Err(EvalFailure::unsupported(
            "string_function",
            format!(
                "function {name:?} supports UTF-8/null operands only, got {}",
                other.dtype().as_str()
            ),
        ));
    }
    if values.iter().any(ScalarValue::is_null) {
        return Ok(
            EvalValue::null(LogicalDType::Utf8, NullBehavior::NullPropagating)
                .carry_materialization(data_materialized),
        );
    }
    let mut output = String::new();
    for value in values {
        match value {
            ScalarValue::Utf8(value) => output.push_str(&value),
            other => {
                return Err(EvalFailure::unsupported(
                    "string_function",
                    format!(
                        "function {name:?} supports UTF-8/null operands only, got {}",
                        other.dtype().as_str()
                    ),
                ));
            }
        }
    }
    Ok(EvalValue::new(
        ScalarValue::Utf8(output),
        LogicalDType::Utf8,
        NullBehavior::NullPropagating,
    )
    .carry_materialization(data_materialized))
}

fn eval_string_substr(
    name: &str,
    args: &[Expression],
    row: &ExpressionInputRow,
) -> EvalResult<EvalValue> {
    if args.len() != 3 {
        return Err(EvalFailure::invalid(
            "string_function",
            format!("function {name:?} requires UTF-8, int64 start, and int64 length arguments"),
        ));
    }
    let (values, data_materialized) = eval_function_args(args, row)?;
    if !matches!(values[0], ScalarValue::Utf8(_) | ScalarValue::Null)
        || !matches!(values[1], ScalarValue::Int64(_) | ScalarValue::Null)
        || !matches!(values[2], ScalarValue::Int64(_) | ScalarValue::Null)
    {
        return Err(EvalFailure::unsupported(
            "string_function",
            format!(
                "function {name:?} supports UTF-8/null, int64/null, int64/null operands only, got {}, {}, and {}",
                values[0].dtype().as_str(),
                values[1].dtype().as_str(),
                values[2].dtype().as_str()
            ),
        ));
    }
    if values.iter().any(ScalarValue::is_null) {
        return Ok(
            EvalValue::null(LogicalDType::Utf8, NullBehavior::NullPropagating)
                .carry_materialization(data_materialized),
        );
    }
    let [value, start, length]: [ScalarValue; 3] =
        values.try_into().expect("validated substring arity");
    match (value, start, length) {
        (ScalarValue::Utf8(value), ScalarValue::Int64(start), ScalarValue::Int64(length)) => {
            if start < 1 {
                return Err(EvalFailure::invalid(
                    "string_function",
                    format!("function {name:?} requires a 1-based start index >= 1"),
                ));
            }
            if length < 0 {
                return Err(EvalFailure::invalid(
                    "string_function",
                    format!("function {name:?} requires a non-negative length"),
                ));
            }
            let skip = usize::try_from(start - 1).map_err(|_| {
                EvalFailure::invalid(
                    "string_function",
                    "substring start index exceeds usize range",
                )
            })?;
            let take = usize::try_from(length).map_err(|_| {
                EvalFailure::invalid("string_function", "substring length exceeds usize range")
            })?;
            Ok(EvalValue::new(
                ScalarValue::Utf8(value.chars().skip(skip).take(take).collect()),
                LogicalDType::Utf8,
                NullBehavior::NullPropagating,
            )
            .carry_materialization(data_materialized))
        }
        (value, start, length) => Err(EvalFailure::unsupported(
            "string_function",
            format!(
                "function {name:?} supports UTF-8/null, int64/null, int64/null operands only, got {}, {}, and {}",
                value.dtype().as_str(),
                start.dtype().as_str(),
                length.dtype().as_str()
            ),
        )),
    }
}

fn eval_string_left_right(
    name: &str,
    args: &[Expression],
    row: &ExpressionInputRow,
    from_right: bool,
) -> EvalResult<EvalValue> {
    if args.len() != 2 {
        return Err(EvalFailure::invalid(
            "string_function",
            format!("function {name:?} requires UTF-8 and int64 count arguments"),
        ));
    }
    let (values, data_materialized) = eval_function_args(args, row)?;
    if !matches!(values[0], ScalarValue::Utf8(_) | ScalarValue::Null)
        || !matches!(values[1], ScalarValue::Int64(_) | ScalarValue::Null)
    {
        return Err(EvalFailure::unsupported(
            "string_function",
            format!(
                "function {name:?} supports UTF-8/null and int64/null operands only, got {} and {}",
                values[0].dtype().as_str(),
                values[1].dtype().as_str()
            ),
        ));
    }
    if values.iter().any(ScalarValue::is_null) {
        return Ok(
            EvalValue::null(LogicalDType::Utf8, NullBehavior::NullPropagating)
                .carry_materialization(data_materialized),
        );
    }
    let [value, count]: [ScalarValue; 2] = values.try_into().expect("validated left/right arity");
    match (value, count) {
        (ScalarValue::Utf8(value), ScalarValue::Int64(count)) => {
            if count < 0 {
                return Err(EvalFailure::invalid(
                    "string_function",
                    format!("function {name:?} requires a non-negative count"),
                ));
            }
            let count = usize::try_from(count).map_err(|_| {
                EvalFailure::invalid("string_function", "string count exceeds usize range")
            })?;
            let output = if from_right {
                let chars = value.chars().collect::<Vec<_>>();
                let start = chars.len().saturating_sub(count);
                chars[start..].iter().copied().collect()
            } else {
                value.chars().take(count).collect()
            };
            Ok(EvalValue::new(
                ScalarValue::Utf8(output),
                LogicalDType::Utf8,
                NullBehavior::NullPropagating,
            )
            .carry_materialization(data_materialized))
        }
        (value, count) => Err(EvalFailure::unsupported(
            "string_function",
            format!(
                "function {name:?} supports UTF-8/null and int64/null operands only, got {} and {}",
                value.dtype().as_str(),
                count.dtype().as_str()
            ),
        )),
    }
}

fn eval_string_replace(
    name: &str,
    args: &[Expression],
    row: &ExpressionInputRow,
) -> EvalResult<EvalValue> {
    if args.len() != 3 {
        return Err(EvalFailure::invalid(
            "string_function",
            format!("function {name:?} requires exactly three UTF-8 arguments"),
        ));
    }
    let (values, data_materialized) = eval_function_args(args, row)?;
    if !values
        .iter()
        .all(|value| matches!(value, ScalarValue::Utf8(_) | ScalarValue::Null))
    {
        return Err(EvalFailure::unsupported(
            "string_function",
            format!(
                "function {name:?} supports UTF-8/null operands only, got {}, {}, and {}",
                values[0].dtype().as_str(),
                values[1].dtype().as_str(),
                values[2].dtype().as_str()
            ),
        ));
    }
    if values.iter().any(ScalarValue::is_null) {
        return Ok(
            EvalValue::null(LogicalDType::Utf8, NullBehavior::NullPropagating)
                .carry_materialization(data_materialized),
        );
    }
    let [value, needle, replacement]: [ScalarValue; 3] =
        values.try_into().expect("validated replace arity");
    match (value, needle, replacement) {
        (ScalarValue::Utf8(value), ScalarValue::Utf8(needle), ScalarValue::Utf8(replacement)) => {
            if needle.is_empty() {
                return Err(EvalFailure::invalid(
                    "string_function",
                    format!("function {name:?} requires a non-empty search pattern"),
                ));
            }
            Ok(EvalValue::new(
                ScalarValue::Utf8(value.replace(&needle, &replacement)),
                LogicalDType::Utf8,
                NullBehavior::NullPropagating,
            )
            .carry_materialization(data_materialized))
        }
        (value, needle, replacement) => Err(EvalFailure::unsupported(
            "string_function",
            format!(
                "function {name:?} supports UTF-8/null operands only, got {}, {}, and {}",
                value.dtype().as_str(),
                needle.dtype().as_str(),
                replacement.dtype().as_str()
            ),
        )),
    }
}

fn eval_binary_unhex(
    name: &str,
    args: &[Expression],
    row: &ExpressionInputRow,
) -> EvalResult<EvalValue> {
    eval_binary_utf8_decode(name, args, row, "binary_unhex", decode_hex_bytes)
}

fn eval_binary_from_base64(
    name: &str,
    args: &[Expression],
    row: &ExpressionInputRow,
) -> EvalResult<EvalValue> {
    eval_binary_utf8_decode(
        name,
        args,
        row,
        "binary_from_base64",
        decode_standard_base64,
    )
}

fn eval_binary_utf8_decode(
    name: &str,
    args: &[Expression],
    row: &ExpressionInputRow,
    feature: &'static str,
    decode: fn(&str) -> std::result::Result<Vec<u8>, &'static str>,
) -> EvalResult<EvalValue> {
    if args.len() != 1 {
        return Err(EvalFailure::invalid(
            feature,
            format!("function {name:?} requires exactly one UTF-8 argument"),
        ));
    }
    let value = eval_expression(&args[0], row)?;
    let data_materialized = value.data_materialized;
    match value.value {
        ScalarValue::Null => Ok(EvalValue::null(
            LogicalDType::Binary,
            NullBehavior::NullPropagating,
        )
        .carry_materialization(data_materialized)),
        ScalarValue::Utf8(value) => {
            let decoded = decode(&value)
                .map_err(|reason| EvalFailure::invalid(feature, format!("{name}: {reason}")))?;
            Ok(EvalValue::new(
                ScalarValue::Binary(decoded),
                LogicalDType::Binary,
                NullBehavior::NullPropagating,
            )
            .carry_materialization(data_materialized))
        }
        other => Err(EvalFailure::unsupported(
            feature,
            format!(
                "function {name:?} supports UTF-8/null operands only, got {}",
                other.dtype().as_str()
            ),
        )),
    }
}

fn decode_hex_bytes(value: &str) -> std::result::Result<Vec<u8>, &'static str> {
    if value.len() % 2 != 0 {
        return Err("UNHEX requires an even number of hexadecimal digits");
    }
    value
        .as_bytes()
        .chunks(2)
        .map(|pair| {
            let high = hex_nibble(pair[0]).ok_or("UNHEX admits hexadecimal digits only")?;
            let low = hex_nibble(pair[1]).ok_or("UNHEX admits hexadecimal digits only")?;
            Ok((high << 4) | low)
        })
        .collect()
}

fn hex_nibble(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        b'A'..=b'F' => Some(value - b'A' + 10),
        _ => None,
    }
}

fn decode_standard_base64(value: &str) -> std::result::Result<Vec<u8>, &'static str> {
    let bytes = value.as_bytes();
    if bytes.len() % 4 != 0 {
        return Err("FROM_BASE64 requires standard padded base64 with length multiple of 4");
    }
    let mut decoded = Vec::with_capacity(bytes.len() / 4 * 3);
    for (chunk_index, chunk) in bytes.chunks(4).enumerate() {
        let last_chunk = chunk_index + 1 == bytes.len() / 4;
        let mut sextets = [0_u8; 4];
        let mut padding = 0_usize;
        for (index, byte) in chunk.iter().copied().enumerate() {
            if byte == b'=' {
                if !last_chunk || index < 2 {
                    return Err("FROM_BASE64 padding may appear only in the final two positions");
                }
                padding += 1;
                sextets[index] = 0;
                continue;
            }
            if padding > 0 {
                return Err("FROM_BASE64 padding must be trailing");
            }
            sextets[index] = base64_sextet(byte)
                .ok_or("FROM_BASE64 admits the standard base64 alphabet only")?;
        }
        if padding > 2 {
            return Err("FROM_BASE64 admits at most two trailing padding characters");
        }
        if padding == 1 && sextets[2] & 0b0000_0011 != 0 {
            return Err("FROM_BASE64 has non-zero trailing bits before padding");
        }
        if padding == 2 && sextets[1] & 0b0000_1111 != 0 {
            return Err("FROM_BASE64 has non-zero trailing bits before padding");
        }
        decoded.push((sextets[0] << 2) | (sextets[1] >> 4));
        if padding < 2 {
            decoded.push((sextets[1] << 4) | (sextets[2] >> 2));
        }
        if padding == 0 {
            decoded.push((sextets[2] << 6) | sextets[3]);
        }
    }
    Ok(decoded)
}

fn base64_sextet(value: u8) -> Option<u8> {
    match value {
        b'A'..=b'Z' => Some(value - b'A'),
        b'a'..=b'z' => Some(value - b'a' + 26),
        b'0'..=b'9' => Some(value - b'0' + 52),
        b'+' => Some(62),
        b'/' => Some(63),
        _ => None,
    }
}

fn eval_function_args(
    args: &[Expression],
    row: &ExpressionInputRow,
) -> EvalResult<(Vec<ScalarValue>, bool)> {
    let mut data_materialized = false;
    let mut values = Vec::with_capacity(args.len());
    for arg in args {
        let value = eval_expression(arg, row)?;
        data_materialized |= value.data_materialized;
        values.push(value.value);
    }
    Ok((values, data_materialized))
}

fn eval_numeric_abs(
    name: &str,
    args: &[Expression],
    row: &ExpressionInputRow,
) -> EvalResult<EvalValue> {
    if args.len() != 1 {
        return Err(EvalFailure::invalid(
            "numeric_abs",
            format!("function {name:?} requires exactly one numeric argument"),
        ));
    }
    let value = eval_expression(&args[0], row)?;
    let data_materialized = value.data_materialized;
    match value.value {
        ScalarValue::Null => Ok(EvalValue::null(value.dtype, NullBehavior::NullPropagating)
            .carry_materialization(data_materialized)),
        ScalarValue::Int64(value) => {
            let output = value.checked_abs().ok_or_else(|| {
                EvalFailure::invalid("numeric_abs", "int64 absolute value overflow")
            })?;
            Ok(EvalValue::new(
                ScalarValue::Int64(output),
                LogicalDType::Int64,
                NullBehavior::NullPropagating,
            )
            .carry_materialization(data_materialized))
        }
        ScalarValue::Float64(value) if value.is_finite() => Ok(EvalValue::new(
            ScalarValue::Float64(value.abs()),
            LogicalDType::Float64,
            NullBehavior::NullPropagating,
        )
        .carry_materialization(data_materialized)),
        other => Err(EvalFailure::unsupported(
            "numeric_abs",
            format!(
                "function {name:?} supports finite int64/float64/null operands only, got {}",
                other.dtype().as_str()
            ),
        )),
    }
}

fn eval_numeric_rounding(
    name: &str,
    args: &[Expression],
    row: &ExpressionInputRow,
    op: impl FnOnce(f64) -> f64,
) -> EvalResult<EvalValue> {
    if args.len() != 1 {
        return Err(EvalFailure::invalid(
            "numeric_rounding",
            format!("function {name:?} requires exactly one numeric argument"),
        ));
    }
    let value = eval_expression(&args[0], row)?;
    let data_materialized = value.data_materialized;
    match value.value {
        ScalarValue::Null => Ok(EvalValue::null(value.dtype, NullBehavior::NullPropagating)
            .carry_materialization(data_materialized)),
        ScalarValue::Int64(value) => Ok(EvalValue::new(
            ScalarValue::Int64(value),
            LogicalDType::Int64,
            NullBehavior::NullPropagating,
        )
        .carry_materialization(data_materialized)),
        ScalarValue::Float64(value) if value.is_finite() => Ok(EvalValue::new(
            ScalarValue::Float64(op(value)),
            LogicalDType::Float64,
            NullBehavior::NullPropagating,
        )
        .carry_materialization(data_materialized)),
        other => Err(EvalFailure::unsupported(
            "numeric_rounding",
            format!(
                "function {name:?} supports finite int64/float64/null operands only, got {}",
                other.dtype().as_str()
            ),
        )),
    }
}

fn eval_string_predicate(
    name: &str,
    args: &[Expression],
    row: &ExpressionInputRow,
    predicate: impl FnOnce(&str, &str) -> bool,
) -> EvalResult<EvalValue> {
    if args.len() != 2 {
        return Err(EvalFailure::invalid(
            "string_predicate",
            format!("function {name:?} requires exactly two UTF-8 arguments"),
        ));
    }
    let value = eval_expression(&args[0], row)?;
    let needle = eval_expression(&args[1], row)?;
    let data_materialized = value.data_materialized || needle.data_materialized;
    if !matches!(value.value, ScalarValue::Utf8(_) | ScalarValue::Null)
        || !matches!(needle.value, ScalarValue::Utf8(_) | ScalarValue::Null)
    {
        return Err(EvalFailure::unsupported(
            "string_predicate",
            format!(
                "function {name:?} supports UTF-8/null operands only, got {} and {}",
                value.value.dtype().as_str(),
                needle.value.dtype().as_str()
            ),
        ));
    }
    if value.value.is_null() || needle.value.is_null() {
        return Ok(
            EvalValue::null(LogicalDType::Boolean, NullBehavior::NullPropagating)
                .carry_materialization(data_materialized),
        );
    }
    match (value.value, needle.value) {
        (ScalarValue::Utf8(value), ScalarValue::Utf8(needle)) => Ok(EvalValue::new(
            ScalarValue::Boolean(predicate(&value, &needle)),
            LogicalDType::Boolean,
            NullBehavior::NullPropagating,
        )
        .carry_materialization(data_materialized)),
        (value, needle) => Err(EvalFailure::unsupported(
            "string_predicate",
            format!(
                "function {name:?} supports UTF-8/null operands only, got {} and {}",
                value.dtype().as_str(),
                needle.dtype().as_str()
            ),
        )),
    }
}

fn eval_string_regex_predicate(
    name: &str,
    args: &[Expression],
    row: &ExpressionInputRow,
) -> EvalResult<EvalValue> {
    if args.len() != 2 {
        return Err(EvalFailure::invalid(
            "string_predicate",
            format!("function {name:?} requires exactly two UTF-8 arguments"),
        ));
    }
    let value = eval_expression(&args[0], row)?;
    let pattern = eval_expression(&args[1], row)?;
    let data_materialized = value.data_materialized || pattern.data_materialized;
    if !matches!(value.value, ScalarValue::Utf8(_) | ScalarValue::Null)
        || !matches!(pattern.value, ScalarValue::Utf8(_) | ScalarValue::Null)
    {
        return Err(EvalFailure::unsupported(
            "string_predicate",
            format!(
                "function {name:?} supports UTF-8/null operands only, got {} and {}",
                value.value.dtype().as_str(),
                pattern.value.dtype().as_str()
            ),
        ));
    }
    if value.value.is_null() || pattern.value.is_null() {
        return Ok(
            EvalValue::null(LogicalDType::Boolean, NullBehavior::NullPropagating)
                .carry_materialization(data_materialized),
        );
    }
    match (value.value, pattern.value) {
        (ScalarValue::Utf8(value), ScalarValue::Utf8(pattern)) => {
            let regex = Regex::new(&pattern).map_err(|error| {
                EvalFailure::invalid(
                    "string_predicate",
                    format!("function {name:?} received invalid regex pattern: {error}"),
                )
            })?;
            Ok(EvalValue::new(
                ScalarValue::Boolean(regex.is_match(&value)),
                LogicalDType::Boolean,
                NullBehavior::NullPropagating,
            )
            .carry_materialization(data_materialized))
        }
        (value, pattern) => Err(EvalFailure::unsupported(
            "string_predicate",
            format!(
                "function {name:?} supports UTF-8/null operands only, got {} and {}",
                value.dtype().as_str(),
                pattern.dtype().as_str()
            ),
        )),
    }
}

fn eval_boolean_and(left: ScalarValue, right: ScalarValue) -> EvalResult<EvalValue> {
    let value = match (left, right) {
        (ScalarValue::Boolean(false), _) | (_, ScalarValue::Boolean(false)) => {
            ScalarValue::Boolean(false)
        }
        (ScalarValue::Boolean(true), ScalarValue::Boolean(true)) => ScalarValue::Boolean(true),
        (ScalarValue::Boolean(true) | ScalarValue::Null, ScalarValue::Null)
        | (ScalarValue::Null, ScalarValue::Boolean(true)) => ScalarValue::Null,
        (left, right) => {
            return Err(EvalFailure::unsupported(
                "boolean_and",
                format!(
                    "AND supports boolean/null operands, got {} and {}",
                    left.dtype().as_str(),
                    right.dtype().as_str()
                ),
            ));
        }
    };
    Ok(EvalValue::new(
        value,
        LogicalDType::Boolean,
        NullBehavior::NullAware,
    ))
}

fn eval_boolean_or(left: ScalarValue, right: ScalarValue) -> EvalResult<EvalValue> {
    let value = match (left, right) {
        (ScalarValue::Boolean(true), _) | (_, ScalarValue::Boolean(true)) => {
            ScalarValue::Boolean(true)
        }
        (ScalarValue::Boolean(false), ScalarValue::Boolean(false)) => ScalarValue::Boolean(false),
        (ScalarValue::Boolean(false) | ScalarValue::Null, ScalarValue::Null)
        | (ScalarValue::Null, ScalarValue::Boolean(false)) => ScalarValue::Null,
        (left, right) => {
            return Err(EvalFailure::unsupported(
                "boolean_or",
                format!(
                    "OR supports boolean/null operands, got {} and {}",
                    left.dtype().as_str(),
                    right.dtype().as_str()
                ),
            ));
        }
    };
    Ok(EvalValue::new(
        value,
        LogicalDType::Boolean,
        NullBehavior::NullAware,
    ))
}

fn eval_numeric_binary(
    left: ScalarValue,
    op: BinaryOp,
    right: ScalarValue,
) -> EvalResult<EvalValue> {
    if left.is_null() || right.is_null() {
        return Ok(EvalValue::null(
            numeric_output_dtype(&left, &right)?,
            NullBehavior::NullPropagating,
        ));
    }
    match (left, right) {
        (ScalarValue::Int64(left), ScalarValue::Int64(right)) => eval_i64_binary(left, op, right),
        (ScalarValue::Float64(left), ScalarValue::Float64(right)) => {
            eval_f64_binary(left, op, right)
        }
        (ScalarValue::Int64(left), ScalarValue::Float64(right)) => {
            eval_f64_binary(i64_to_exact_f64(left)?, op, right)
        }
        (ScalarValue::Float64(left), ScalarValue::Int64(right)) => {
            eval_f64_binary(left, op, i64_to_exact_f64(right)?)
        }
        (left, right) => Err(EvalFailure::unsupported(
            "numeric_binary",
            format!(
                "{} supports int64, float64, or exact int64/float64 mixed operands for this slice, got {} and {}",
                op.as_str(),
                left.dtype().as_str(),
                right.dtype().as_str()
            ),
        )),
    }
}

fn i64_to_exact_f64(value: i64) -> EvalResult<f64> {
    const F64_MAX_EXACT_INTEGER: i64 = 9_007_199_254_740_992;
    if (-F64_MAX_EXACT_INTEGER..=F64_MAX_EXACT_INTEGER).contains(&value) {
        #[allow(clippy::cast_precision_loss)]
        let output = value as f64;
        Ok(output)
    } else {
        Err(EvalFailure::unsupported(
            "numeric_coercion",
            "mixed int64/float64 numeric coercion requires the int64 operand to be exactly representable as float64",
        ))
    }
}

fn eval_i64_binary(left: i64, op: BinaryOp, right: i64) -> EvalResult<EvalValue> {
    let output = match op {
        BinaryOp::Add => left.checked_add(right),
        BinaryOp::Subtract => left.checked_sub(right),
        BinaryOp::Multiply => left.checked_mul(right),
        BinaryOp::Divide if right == 0 => {
            return Err(EvalFailure::invalid("divide", "division by zero"));
        }
        BinaryOp::Divide => left.checked_div(right),
        BinaryOp::And | BinaryOp::Or => None,
    }
    .ok_or_else(|| EvalFailure::invalid("numeric_binary", "int64 arithmetic overflow"))?;
    Ok(EvalValue::new(
        ScalarValue::Int64(output),
        LogicalDType::Int64,
        NullBehavior::NullPropagating,
    ))
}

fn eval_f64_binary(left: f64, op: BinaryOp, right: f64) -> EvalResult<EvalValue> {
    if !left.is_finite() || !right.is_finite() {
        return Err(EvalFailure::unsupported(
            "numeric_binary",
            "non-finite float semantics are not admitted by this slice",
        ));
    }
    if matches!(op, BinaryOp::Divide) && right == 0.0 {
        return Err(EvalFailure::invalid("divide", "division by zero"));
    }
    let output = match op {
        BinaryOp::Add => left + right,
        BinaryOp::Subtract => left - right,
        BinaryOp::Multiply => left * right,
        BinaryOp::Divide => left / right,
        BinaryOp::And | BinaryOp::Or => unreachable!("boolean ops handled before numeric binary"),
    };
    if !output.is_finite() {
        return Err(EvalFailure::invalid(
            "numeric_binary",
            "float arithmetic produced a non-finite value",
        ));
    }
    Ok(EvalValue::new(
        ScalarValue::Float64(output),
        LogicalDType::Float64,
        NullBehavior::NullPropagating,
    ))
}

fn numeric_output_dtype(left: &ScalarValue, right: &ScalarValue) -> EvalResult<LogicalDType> {
    match (left, right) {
        (ScalarValue::Float64(_), _) | (_, ScalarValue::Float64(_)) => Ok(LogicalDType::Float64),
        (ScalarValue::Int64(_), _)
        | (_, ScalarValue::Int64(_))
        | (ScalarValue::Null, ScalarValue::Null) => Ok(LogicalDType::Int64),
        (left, right) => Err(EvalFailure::unsupported(
            "numeric_binary",
            format!(
                "null numeric operations require admitted numeric peers, got {} and {}",
                left.dtype().as_str(),
                right.dtype().as_str()
            ),
        )),
    }
}

#[allow(clippy::match_same_arms)]
fn eval_compare(left: &EvalValue, op: ComparisonOp, right: &EvalValue) -> EvalResult<EvalValue> {
    let data_materialized = left.data_materialized || right.data_materialized;
    if left.value.is_null() || right.value.is_null() {
        return Ok(
            EvalValue::null(LogicalDType::Boolean, NullBehavior::NullPropagating)
                .carry_materialization(data_materialized),
        );
    }
    let result = match (&left.value, &right.value) {
        (ScalarValue::Boolean(left), ScalarValue::Boolean(right)) => compare_ordering(
            bool_ordering(*left, *right),
            op,
            "boolean comparison admits equality and inequality only",
        )?,
        (ScalarValue::Int64(left), ScalarValue::Int64(right)) => {
            compare_ordering(left.cmp(right), op, "")?
        }
        (ScalarValue::UInt64(left), ScalarValue::UInt64(right)) => {
            compare_ordering(left.cmp(right), op, "")?
        }
        (ScalarValue::Float64(left), ScalarValue::Float64(right)) => {
            compare_f64(*left, *right, op)?
        }
        (ScalarValue::Int64(left), ScalarValue::Float64(right)) => {
            compare_f64(i64_to_exact_f64(*left)?, *right, op)?
        }
        (ScalarValue::Float64(left), ScalarValue::Int64(right)) => {
            compare_f64(*left, i64_to_exact_f64(*right)?, op)?
        }
        (ScalarValue::Utf8(left), ScalarValue::Utf8(right)) => {
            compare_ordering(left.cmp(right), op, "")?
        }
        (ScalarValue::Binary(left), ScalarValue::Binary(right)) => compare_ordering(
            left.cmp(right),
            op,
            "binary comparison admits equality and inequality only",
        )?,
        (ScalarValue::Date32(left), ScalarValue::Date32(right)) => {
            compare_ordering(left.cmp(right), op, "")?
        }
        (ScalarValue::TimestampMicros(left), ScalarValue::TimestampMicros(right)) => {
            compare_ordering(left.cmp(right), op, "")?
        }
        (left, right) => Err(EvalFailure::unsupported(
            "comparison",
            format!(
                "comparison operands are not admitted together: {} and {}",
                left.dtype().as_str(),
                right.dtype().as_str()
            ),
        ))?,
    };
    Ok(EvalValue::new(
        ScalarValue::Boolean(result),
        LogicalDType::Boolean,
        NullBehavior::NullPropagating,
    )
    .carry_materialization(data_materialized))
}

fn compare_f64(left: f64, right: f64, op: ComparisonOp) -> EvalResult<bool> {
    if !left.is_finite() || !right.is_finite() {
        return Err(EvalFailure::unsupported(
            "float_comparison",
            "non-finite float comparison semantics are not admitted by this slice",
        ));
    }
    let Some(ordering) = left.partial_cmp(&right) else {
        return Err(EvalFailure::unsupported(
            "float_comparison",
            "unordered float comparison is not admitted by this slice",
        ));
    };
    compare_ordering(ordering, op, "")
}

fn compare_ordering(
    ordering: std::cmp::Ordering,
    op: ComparisonOp,
    unsupported_order_reason: &str,
) -> EvalResult<bool> {
    use std::cmp::Ordering::{Equal, Greater, Less};
    match op {
        ComparisonOp::Eq => Ok(ordering == Equal),
        ComparisonOp::NotEq => Ok(ordering != Equal),
        ComparisonOp::Lt | ComparisonOp::LtEq | ComparisonOp::Gt | ComparisonOp::GtEq
            if !unsupported_order_reason.is_empty() =>
        {
            Err(EvalFailure::unsupported(
                "comparison",
                unsupported_order_reason,
            ))
        }
        ComparisonOp::Lt => Ok(ordering == Less),
        ComparisonOp::LtEq => Ok(matches!(ordering, Less | Equal)),
        ComparisonOp::Gt => Ok(ordering == Greater),
        ComparisonOp::GtEq => Ok(matches!(ordering, Greater | Equal)),
    }
}

fn bool_ordering(left: bool, right: bool) -> std::cmp::Ordering {
    left.cmp(&right)
}

#[allow(clippy::cast_possible_truncation, clippy::cast_precision_loss)]
fn cast_eval_value(value: &EvalValue, target_dtype: &LogicalDType) -> EvalResult<EvalValue> {
    let data_materialized = value.data_materialized;
    if value.value.is_null() {
        return Ok(
            EvalValue::null(target_dtype.clone(), NullBehavior::NullPropagating)
                .carry_materialization(data_materialized),
        );
    }
    let casted = match (&value.value, target_dtype) {
        (value, dtype) if value.dtype() == *dtype => value.clone(),
        (ScalarValue::Int64(value), LogicalDType::Float64) => ScalarValue::Float64(*value as f64),
        (ScalarValue::Float64(value), LogicalDType::Int64)
            if value.is_finite()
                && value.fract() == 0.0
                && *value >= i64::MIN as f64
                && *value <= i64::MAX as f64 =>
        {
            ScalarValue::Int64(*value as i64)
        }
        (ScalarValue::Int64(value), LogicalDType::Utf8) => ScalarValue::Utf8(value.to_string()),
        (ScalarValue::Float64(value), LogicalDType::Utf8) if value.is_finite() => {
            ScalarValue::Utf8(value.to_string())
        }
        (ScalarValue::Boolean(value), LogicalDType::Utf8) => ScalarValue::Utf8(value.to_string()),
        (ScalarValue::Date32(value), LogicalDType::Utf8) => {
            ScalarValue::Utf8(format_iso_date32(*value))
        }
        (ScalarValue::TimestampMicros(value), LogicalDType::Utf8) => {
            ScalarValue::Utf8(format_iso_timestamp_micros(*value))
        }
        (ScalarValue::Date32(value), LogicalDType::TimestampMicros) => {
            ScalarValue::TimestampMicros(i64::from(*value) * MICROS_PER_DAY)
        }
        (ScalarValue::TimestampMicros(value), LogicalDType::Date32) => {
            ScalarValue::Date32(timestamp_micros_date32(*value))
        }
        (ScalarValue::Utf8(value), LogicalDType::Date32) => {
            ScalarValue::Date32(parse_iso_date32(value).map_err(|_| {
                EvalFailure::invalid("cast", "utf8 value cannot be parsed as ISO date32")
            })?)
        }
        (ScalarValue::Utf8(value), LogicalDType::TimestampMicros) => {
            ScalarValue::TimestampMicros(parse_iso_timestamp_micros(value).map_err(|_| {
                EvalFailure::invalid(
                    "cast",
                    "utf8 value cannot be parsed as UTC ISO timestamp_micros",
                )
            })?)
        }
        (ScalarValue::Utf8(value), LogicalDType::Int64) => {
            ScalarValue::Int64(value.parse::<i64>().map_err(|_| {
                EvalFailure::invalid("cast", "utf8 value cannot be parsed as int64")
            })?)
        }
        (ScalarValue::Utf8(value), LogicalDType::Float64) => {
            let parsed = value.parse::<f64>().map_err(|_| {
                EvalFailure::invalid("cast", "utf8 value cannot be parsed as float64")
            })?;
            if !parsed.is_finite() {
                return Err(EvalFailure::invalid(
                    "cast",
                    "utf8 value parsed as non-finite float64",
                ));
            }
            ScalarValue::Float64(parsed)
        }
        (ScalarValue::Utf8(value), LogicalDType::Boolean) if value == "true" => {
            ScalarValue::Boolean(true)
        }
        (ScalarValue::Utf8(value), LogicalDType::Boolean) if value == "false" => {
            ScalarValue::Boolean(false)
        }
        (source, target) => {
            return Err(EvalFailure::unsupported(
                "cast",
                format!(
                    "cast from {} to {} is not admitted by this slice",
                    source.dtype().as_str(),
                    target.as_str()
                ),
            ));
        }
    };
    Ok(
        EvalValue::new(casted, target_dtype.clone(), NullBehavior::NullPropagating)
            .carry_materialization(data_materialized),
    )
}

fn try_cast_eval_value(value: &EvalValue, target_dtype: &LogicalDType) -> EvalResult<EvalValue> {
    let data_materialized = value.data_materialized;
    match cast_eval_value(value, target_dtype) {
        Ok(mut casted) => {
            casted.null_behavior = NullBehavior::NullAware;
            Ok(casted)
        }
        Err(failure) if failure.status == ExpressionEvaluationStatus::InvalidInput => Ok(
            EvalValue::null(target_dtype.clone(), NullBehavior::NullAware)
                .carry_materialization(data_materialized),
        ),
        Err(failure) => Err(failure),
    }
}

fn projection_name(expression: &Expression) -> String {
    match &expression.kind {
        ExpressionKind::Alias { alias, .. } => alias.clone(),
        ExpressionKind::Column(column) => column.as_str().to_string(),
        _ => expression.id.as_str().to_string(),
    }
}

fn expression_operator_family(expression: &Expression) -> &'static str {
    match &expression.kind {
        ExpressionKind::Literal(_) => "literal",
        ExpressionKind::Column(_) => "column",
        ExpressionKind::Alias { .. } => "alias",
        ExpressionKind::Cast { .. } => "cast",
        ExpressionKind::TryCast { .. } => "try_cast",
        ExpressionKind::Unary { op, .. } => match op {
            UnaryOp::Not => "boolean",
            UnaryOp::IsNull | UnaryOp::IsNotNull => "null_predicate",
            UnaryOp::Negate => "numeric",
        },
        ExpressionKind::Binary { op, .. } => match op {
            BinaryOp::And | BinaryOp::Or => "boolean",
            BinaryOp::Add | BinaryOp::Subtract | BinaryOp::Multiply | BinaryOp::Divide => "numeric",
        },
        ExpressionKind::Compare { .. } => "comparison",
        ExpressionKind::FunctionCall { name, .. } => function_operator_family(name),
        ExpressionKind::Unsupported { .. } => "unsupported",
    }
}

fn function_operator_family(name: &str) -> &'static str {
    match name.trim().to_ascii_lowercase().as_str() {
        "utf8_starts_with" | "starts_with" | "utf8_contains" | "contains" | "utf8_ends_with"
        | "ends_with" | "utf8_regex_match" | "regex_match" | "rlike" | "regexp" | "regexp_like" => {
            "string_predicate"
        }
        "utf8_lower" | "lower" | "utf8_upper" | "upper" | "utf8_trim" | "trim" => {
            "string_transform"
        }
        "utf8_length" | "length" => "string_length",
        "utf8_concat" | "concat" | "utf8_substr" | "utf8_substring" | "substr" | "substring"
        | "utf8_left" | "left" | "utf8_right" | "right" | "utf8_replace" | "replace" => {
            "string_function"
        }
        "binary_unhex" | "unhex" | "binary_from_base64" | "from_base64" => "binary_function",
        "numeric_abs" | "abs" => "numeric_abs",
        "numeric_floor" | "floor" | "numeric_ceil" | "ceil" | "ceiling" | "numeric_round"
        | "round" => "numeric_rounding",
        "date_year" | "year" | "date_month" | "month" | "date_day" | "day" => "date_extract",
        "timestamp_year" | "timestamp_month" | "timestamp_day" | "timestamp_hour"
        | "timestamp_minute" | "timestamp_second" => "timestamp_extract",
        "date_add_days" | "date_sub_days" => "date_arithmetic",
        "timestamp_add_seconds" | "timestamp_sub_seconds" => "timestamp_arithmetic",
        "date_diff_days" | "timestamp_diff_seconds" => "temporal_difference",
        "coalesce" => "null_coalesce",
        "nullif" => "nullif_projection",
        "case_when" => "conditional_projection",
        _ => "function",
    }
}

/// Parses an ISO `YYYY-MM-DD` date into Arrow-compatible Date32 days since 1970-01-01.
///
/// # Errors
/// Returns [`ShardLoomError::InvalidOperation`] when the input is not an admitted ISO date.
pub fn parse_iso_date32(value: &str) -> Result<i32> {
    let (year, month, day) = parse_iso_ymd(value)?;
    Ok(days_from_civil(year, month, day))
}

/// Formats Arrow-compatible Date32 days since 1970-01-01 as ISO `YYYY-MM-DD`.
#[must_use]
pub fn format_iso_date32(days: i32) -> String {
    let (year, month, day) = civil_from_days(days);
    format!("{year:04}-{month:02}-{day:02}")
}

/// Returns the UTC-calendar year component of an admitted Date32 value.
#[must_use]
pub fn date32_year(days: i32) -> i32 {
    civil_from_days(days).0
}

/// Returns the UTC-calendar month component of an admitted Date32 value.
#[must_use]
pub fn date32_month(days: i32) -> u32 {
    civil_from_days(days).1
}

/// Returns the UTC-calendar day-of-month component of an admitted Date32 value.
#[must_use]
pub fn date32_day(days: i32) -> u32 {
    civil_from_days(days).2
}

/// Parses a scoped UTC ISO timestamp into microseconds since the Unix epoch.
///
/// This runtime slice admits only `YYYY-MM-DDTHH:MM:SSZ` plus optional fractional seconds
/// up to six digits. Offset time zones and named time zones remain deterministic blockers.
///
/// # Errors
/// Returns [`ShardLoomError::InvalidOperation`] when the input is not an admitted UTC timestamp.
pub fn parse_iso_timestamp_micros(value: &str) -> Result<i64> {
    let text = value.trim();
    let Some(timestamp) = text.strip_suffix('Z') else {
        return Err(ShardLoomError::InvalidOperation(
            "UTC timestamp_micros literals must end with Z".to_string(),
        ));
    };
    let Some((date, time)) = timestamp.split_once('T') else {
        return Err(ShardLoomError::InvalidOperation(
            "UTC timestamp_micros literals must use YYYY-MM-DDTHH:MM:SSZ".to_string(),
        ));
    };
    let (year, month, day) = parse_iso_ymd(date)?;
    let (hour, minute, second, micros) = parse_iso_time_micros(time)?;
    let days = i64::from(days_from_civil(year, month, day));
    let seconds_of_day = i64::from(hour) * 3_600 + i64::from(minute) * 60 + i64::from(second);
    Ok(days * MICROS_PER_DAY + seconds_of_day * MICROS_PER_SECOND + i64::from(micros))
}

/// Formats a timestamp-micros value as a scoped UTC ISO timestamp.
#[must_use]
pub fn format_iso_timestamp_micros(value: i64) -> String {
    let micros_of_day = value.rem_euclid(MICROS_PER_DAY);
    let (year, month, day) = civil_from_days(timestamp_micros_date32(value));
    let hour = micros_of_day / MICROS_PER_HOUR;
    let minute = (micros_of_day % MICROS_PER_HOUR) / MICROS_PER_MINUTE;
    let second = (micros_of_day % MICROS_PER_MINUTE) / MICROS_PER_SECOND;
    let micros = micros_of_day % MICROS_PER_SECOND;
    if micros == 0 {
        format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}Z")
    } else {
        format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}.{micros:06}Z")
    }
}

/// Returns the UTC Date32 day for a timestamp-micros value.
#[must_use]
pub fn timestamp_micros_date32(value: i64) -> i32 {
    timestamp_micros_day_index(value)
}

/// Returns the UTC-calendar year component of a timestamp-micros value.
#[must_use]
pub fn timestamp_micros_year(value: i64) -> i32 {
    date32_year(timestamp_micros_date32(value))
}

/// Returns the UTC-calendar month component of a timestamp-micros value.
#[must_use]
pub fn timestamp_micros_month(value: i64) -> u32 {
    date32_month(timestamp_micros_date32(value))
}

/// Returns the UTC-calendar day-of-month component of a timestamp-micros value.
#[must_use]
pub fn timestamp_micros_day(value: i64) -> u32 {
    date32_day(timestamp_micros_date32(value))
}

/// Returns the UTC hour component of a timestamp-micros value.
#[must_use]
pub fn timestamp_micros_hour(value: i64) -> u32 {
    timestamp_time_component(value, MICROS_PER_DAY, MICROS_PER_HOUR)
}

/// Returns the UTC minute component of a timestamp-micros value.
#[must_use]
pub fn timestamp_micros_minute(value: i64) -> u32 {
    timestamp_time_component(value, MICROS_PER_HOUR, MICROS_PER_MINUTE)
}

/// Returns the UTC second component of a timestamp-micros value.
#[must_use]
pub fn timestamp_micros_second(value: i64) -> u32 {
    timestamp_time_component(value, MICROS_PER_MINUTE, MICROS_PER_SECOND)
}

fn timestamp_micros_day_index(value: i64) -> i32 {
    i32::try_from(value.div_euclid(MICROS_PER_DAY))
        .expect("i64 timestamp_micros day range fits Date32")
}

fn timestamp_time_component(value: i64, period_micros: i64, unit_micros: i64) -> u32 {
    let component = value.rem_euclid(MICROS_PER_DAY) % period_micros / unit_micros;
    u32::try_from(component).expect("timestamp component fits u32")
}

fn parse_iso_ymd(value: &str) -> Result<(i32, u32, u32)> {
    let text = value.trim();
    let mut parts = text.split('-');
    let (Some(year), Some(month), Some(day), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return Err(ShardLoomError::InvalidOperation(
            "ISO date32 literals must use YYYY-MM-DD".to_string(),
        ));
    };
    if year.len() != 4 || month.len() != 2 || day.len() != 2 {
        return Err(ShardLoomError::InvalidOperation(
            "ISO date32 literals must use zero-padded YYYY-MM-DD".to_string(),
        ));
    }
    let year = year.parse::<i32>().map_err(|_| {
        ShardLoomError::InvalidOperation("ISO date32 year must be numeric".to_string())
    })?;
    let month = month.parse::<u32>().map_err(|_| {
        ShardLoomError::InvalidOperation("ISO date32 month must be numeric".to_string())
    })?;
    let day = day.parse::<u32>().map_err(|_| {
        ShardLoomError::InvalidOperation("ISO date32 day must be numeric".to_string())
    })?;
    if !(1..=9999).contains(&year) {
        return Err(ShardLoomError::InvalidOperation(
            "ISO date32 year must be in 0001..=9999".to_string(),
        ));
    }
    if !(1..=12).contains(&month) {
        return Err(ShardLoomError::InvalidOperation(
            "ISO date32 month must be in 01..=12".to_string(),
        ));
    }
    let max_day = days_in_month(year, month);
    if day == 0 || day > max_day {
        return Err(ShardLoomError::InvalidOperation(format!(
            "ISO date32 day must be in 01..={max_day:02} for the given month"
        )));
    }
    Ok((year, month, day))
}

fn parse_iso_time_micros(value: &str) -> Result<(u32, u32, u32, u32)> {
    let (hms, micros) = match value.split_once('.') {
        Some((hms, fraction)) => {
            if fraction.is_empty()
                || fraction.len() > 6
                || !fraction.bytes().all(|b| b.is_ascii_digit())
            {
                return Err(ShardLoomError::InvalidOperation(
                    "UTC timestamp fractional seconds must use 1..=6 digits".to_string(),
                ));
            }
            let mut padded = fraction.to_string();
            while padded.len() < 6 {
                padded.push('0');
            }
            let micros = padded.parse::<u32>().map_err(|_| {
                ShardLoomError::InvalidOperation(
                    "UTC timestamp fractional seconds must be numeric".to_string(),
                )
            })?;
            (hms, micros)
        }
        None => (value, 0),
    };
    let mut parts = hms.split(':');
    let (Some(hour), Some(minute), Some(second), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return Err(ShardLoomError::InvalidOperation(
            "UTC timestamp time must use HH:MM:SS".to_string(),
        ));
    };
    if hour.len() != 2 || minute.len() != 2 || second.len() != 2 {
        return Err(ShardLoomError::InvalidOperation(
            "UTC timestamp time must use zero-padded HH:MM:SS".to_string(),
        ));
    }
    let hour = parse_two_digit_time_component(hour, "hour")?;
    let minute = parse_two_digit_time_component(minute, "minute")?;
    let second = parse_two_digit_time_component(second, "second")?;
    if hour > 23 {
        return Err(ShardLoomError::InvalidOperation(
            "UTC timestamp hour must be in 00..=23".to_string(),
        ));
    }
    if minute > 59 {
        return Err(ShardLoomError::InvalidOperation(
            "UTC timestamp minute must be in 00..=59".to_string(),
        ));
    }
    if second > 59 {
        return Err(ShardLoomError::InvalidOperation(
            "UTC timestamp second must be in 00..=59".to_string(),
        ));
    }
    Ok((hour, minute, second, micros))
}

fn parse_two_digit_time_component(value: &str, name: &str) -> Result<u32> {
    if !value.bytes().all(|b| b.is_ascii_digit()) {
        return Err(ShardLoomError::InvalidOperation(format!(
            "UTC timestamp {name} must be numeric"
        )));
    }
    value.parse::<u32>().map_err(|_| {
        ShardLoomError::InvalidOperation(format!("UTC timestamp {name} must be numeric"))
    })
}

const fn days_in_month(year: i32, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap_year(year) => 29,
        2 => 28,
        _ => 0,
    }
}

const fn is_leap_year(year: i32) -> bool {
    (year % 4 == 0 && year % 100 != 0) || year % 400 == 0
}

fn days_from_civil(year: i32, month: u32, day: u32) -> i32 {
    let month = i32::try_from(month).expect("month fits i32");
    let day = i32::try_from(day).expect("day fits i32");
    let year = year - i32::from(month <= 2);
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let year_of_era = year - era * 400;
    let day_of_year = (153 * (month + if month > 2 { -3 } else { 9 }) + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

fn civil_from_days(days: i32) -> (i32, u32, u32) {
    let days = days + 719_468;
    let era = if days >= 0 { days } else { days - 146_096 } / 146_097;
    let day_of_era = days - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let mut year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_prime = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_prime + 2) / 5 + 1;
    let month = month_prime + if month_prime < 10 { 3 } else { -9 };
    year += i32::from(month <= 2);
    (
        year,
        u32::try_from(month).expect("month is positive"),
        u32::try_from(day).expect("day is positive"),
    )
}

const MICROS_PER_SECOND: i64 = 1_000_000;
const MICROS_PER_MINUTE: i64 = 60 * MICROS_PER_SECOND;
const MICROS_PER_HOUR: i64 = 60 * MICROS_PER_MINUTE;
const MICROS_PER_DAY: i64 = 24 * MICROS_PER_HOUR;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NullBehavior {
    NullPropagating,
    NullIgnoring,
    NullAware,
    NullRejecting,
    Custom,
    Unsupported,
}
impl NullBehavior {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::NullPropagating => "null_propagating",
            Self::NullIgnoring => "null_ignoring",
            Self::NullAware => "null_aware",
            Self::NullRejecting => "null_rejecting",
            Self::Custom => "custom",
            Self::Unsupported => "unsupported",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Determinism {
    Deterministic,
    Nondeterministic,
    Unknown,
}
impl Determinism {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Deterministic => "deterministic",
            Self::Nondeterministic => "nondeterministic",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EffectLevel {
    PureDeterministic,
    PureNondeterministic,
    ExternalRead,
    ExternalWrite,
    ModelCall,
    EmbeddingCall,
    VectorSearch,
    Unknown,
}
impl EffectLevel {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::PureDeterministic => "pure_deterministic",
            Self::PureNondeterministic => "pure_nondeterministic",
            Self::ExternalRead => "external_read",
            Self::ExternalWrite => "external_write",
            Self::ModelCall => "model_call",
            Self::EmbeddingCall => "embedding_call",
            Self::VectorSearch => "vector_search",
            Self::Unknown => "unknown",
        }
    }
    #[must_use]
    pub const fn is_effectful(&self) -> bool {
        !matches!(self, Self::PureDeterministic | Self::PureNondeterministic)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FunctionCategory {
    Scalar,
    Predicate,
    Aggregate,
    Window,
    Table,
    Udf,
    Translation,
    ExternalRead,
    ExternalWrite,
    ModelCall,
    EmbeddingGeneration,
    VectorSearch,
}
impl FunctionCategory {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Scalar => "scalar",
            Self::Predicate => "predicate",
            Self::Aggregate => "aggregate",
            Self::Window => "window",
            Self::Table => "table",
            Self::Udf => "udf",
            Self::Translation => "translation",
            Self::ExternalRead => "external_read",
            Self::ExternalWrite => "external_write",
            Self::ModelCall => "model_call",
            Self::EmbeddingGeneration => "embedding_generation",
            Self::VectorSearch => "vector_search",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FunctionSignature {
    pub name: String,
    pub category: FunctionCategory,
    pub input_types: Vec<LogicalDType>,
    pub output_type: LogicalDType,
    pub null_behavior: NullBehavior,
    pub determinism: Determinism,
    pub effect_level: EffectLevel,
    pub variadic: bool,
}
impl FunctionSignature {
    /// Creates a function signature skeleton with deterministic no-fallback metadata defaults.
    ///
    /// # Errors
    /// Returns [`ShardLoomError::InvalidOperation`] when the function name is empty or whitespace only.
    pub fn new(
        name: impl Into<String>,
        category: FunctionCategory,
        output_type: LogicalDType,
    ) -> Result<Self> {
        let name = name.into();
        if name.trim().is_empty() {
            return Err(ShardLoomError::InvalidOperation(
                "function name must not be empty".to_string(),
            ));
        }
        Ok(Self {
            name,
            category,
            input_types: Vec::new(),
            output_type,
            null_behavior: NullBehavior::Unsupported,
            determinism: Determinism::Unknown,
            effect_level: EffectLevel::Unknown,
            variadic: false,
        })
    }
    #[must_use]
    pub fn with_input_types(mut self, input_types: Vec<LogicalDType>) -> Self {
        self.input_types = input_types;
        self
    }
    #[must_use]
    pub fn with_null_behavior(mut self, null_behavior: NullBehavior) -> Self {
        self.null_behavior = null_behavior;
        self
    }
    #[must_use]
    pub fn with_determinism(mut self, determinism: Determinism) -> Self {
        self.determinism = determinism;
        self
    }
    #[must_use]
    pub fn with_effect_level(mut self, effect_level: EffectLevel) -> Self {
        self.effect_level = effect_level;
        self
    }
    #[must_use]
    pub fn variadic(mut self, variadic: bool) -> Self {
        self.variadic = variadic;
        self
    }
    #[must_use]
    pub const fn is_effectful(&self) -> bool {
        self.effect_level.is_effectful()
    }
    #[must_use]
    pub fn summary(&self) -> String {
        format!(
            "fn[name={}, category={}, inputs={}, output={}, effect={}]",
            self.name,
            self.category.as_str(),
            self.input_types.len(),
            self.output_type.as_str(),
            self.effect_level.as_str()
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KernelKind {
    Metadata,
    Encoded,
    PartialDecode,
    DecodedReference,
    Compatibility,
    Effect,
    Unsupported,
}
impl KernelKind {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Metadata => "metadata",
            Self::Encoded => "encoded",
            Self::PartialDecode => "partial_decode",
            Self::DecodedReference => "decoded_reference",
            Self::Compatibility => "compatibility",
            Self::Effect => "effect",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn is_reference_only(&self) -> bool {
        matches!(self, Self::DecodedReference)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KernelEvalMode {
    MetadataOnly,
    Encoded,
    PartialDecode,
    LateMaterialized,
    FullMaterialized,
    Effectful,
    Unsupported,
}
impl KernelEvalMode {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::MetadataOnly => "metadata_only",
            Self::Encoded => "encoded",
            Self::PartialDecode => "partial_decode",
            Self::LateMaterialized => "late_materialized",
            Self::FullMaterialized => "full_materialized",
            Self::Effectful => "effectful",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn to_execution_state(&self) -> ExecutionState {
        match self {
            Self::MetadataOnly => ExecutionState::MetadataOnly,
            Self::Encoded => ExecutionState::EncodedEvaluation,
            Self::PartialDecode | Self::LateMaterialized => ExecutionState::PartialDecode,
            Self::FullMaterialized => ExecutionState::FullMaterialization,
            Self::Effectful => ExecutionState::ExternalRead,
            Self::Unsupported => ExecutionState::Unsupported,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KernelCapability {
    pub kind: KernelKind,
    pub eval_mode: KernelEvalMode,
    pub supported_dtypes: Vec<LogicalDType>,
    pub supported_encodings: Vec<EncodingKind>,
    pub supports_selection_vectors: bool,
    pub supports_streaming: bool,
    pub materialization: MaterializationRequirement,
    pub effect_level: EffectLevel,
}
impl KernelCapability {
    #[must_use]
    pub fn metadata() -> Self {
        Self {
            kind: KernelKind::Metadata,
            eval_mode: KernelEvalMode::MetadataOnly,
            supported_dtypes: Vec::new(),
            supported_encodings: Vec::new(),
            supports_selection_vectors: true,
            supports_streaming: true,
            materialization: MaterializationRequirement::None,
            effect_level: EffectLevel::PureDeterministic,
        }
    }
    #[must_use]
    pub fn encoded() -> Self {
        Self {
            kind: KernelKind::Encoded,
            eval_mode: KernelEvalMode::Encoded,
            supported_dtypes: Vec::new(),
            supported_encodings: Vec::new(),
            supports_selection_vectors: true,
            supports_streaming: true,
            materialization: MaterializationRequirement::None,
            effect_level: EffectLevel::PureDeterministic,
        }
    }
    #[must_use]
    pub fn decoded_reference() -> Self {
        Self {
            kind: KernelKind::DecodedReference,
            eval_mode: KernelEvalMode::FullMaterialized,
            supported_dtypes: Vec::new(),
            supported_encodings: Vec::new(),
            supports_selection_vectors: true,
            supports_streaming: false,
            materialization: MaterializationRequirement::Full {
                reason: "decoded reference kernel".to_string(),
            },
            effect_level: EffectLevel::PureDeterministic,
        }
    }
    #[must_use]
    pub fn unsupported() -> Self {
        Self {
            kind: KernelKind::Unsupported,
            eval_mode: KernelEvalMode::Unsupported,
            supported_dtypes: Vec::new(),
            supported_encodings: Vec::new(),
            supports_selection_vectors: false,
            supports_streaming: false,
            materialization: MaterializationRequirement::Unknown {
                reason: "kernel unsupported".to_string(),
            },
            effect_level: EffectLevel::Unknown,
        }
    }
    #[must_use]
    pub fn supports_dtype(&self, dtype: &LogicalDType) -> bool {
        !matches!(self.kind, KernelKind::Unsupported)
            && (self.supported_dtypes.is_empty() || self.supported_dtypes.contains(dtype))
    }
    #[must_use]
    pub fn supports_encoding(&self, encoding: &EncodingKind) -> bool {
        !matches!(self.kind, KernelKind::Unsupported)
            && (self.supported_encodings.is_empty() || self.supported_encodings.contains(encoding))
    }
    #[must_use]
    pub const fn is_effectful(&self) -> bool {
        self.effect_level.is_effectful()
    }
    #[must_use]
    pub fn summary(&self) -> String {
        format!(
            "kernel_capability[kind={}, eval_mode={}, dtypes={}, encodings={}]",
            self.kind.as_str(),
            self.eval_mode.as_str(),
            self.supported_dtypes.len(),
            self.supported_encodings.len()
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct KernelId(String);
impl KernelId {
    /// Creates a validated kernel identifier.
    ///
    /// # Errors
    /// Returns [`ShardLoomError::InvalidOperation`] when the identifier is empty or whitespace only.
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        if value.trim().is_empty() {
            return Err(ShardLoomError::InvalidOperation(
                "kernel id must not be empty".to_string(),
            ));
        }
        Ok(Self(value))
    }
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct KernelDescriptor {
    pub id: KernelId,
    pub function: FunctionSignature,
    pub capability: KernelCapability,
    pub diagnostics: Vec<Diagnostic>,
}
impl KernelDescriptor {
    #[must_use]
    pub fn new(id: KernelId, function: FunctionSignature, capability: KernelCapability) -> Self {
        Self {
            id,
            function,
            capability,
            diagnostics: Vec::new(),
        }
    }
    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
    }
    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.diagnostics.iter().any(|d| {
            matches!(
                d.severity,
                DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
            )
        })
    }
    #[must_use]
    pub const fn is_reference_only(&self) -> bool {
        self.capability.kind.is_reference_only()
    }
    #[must_use]
    pub fn summary(&self) -> String {
        format!(
            "kernel[id={}, {}, {}]",
            self.id.as_str(),
            self.function.summary(),
            self.capability.summary()
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct KernelRegistrySnapshot {
    pub kernels: Vec<KernelDescriptor>,
    pub diagnostics: Vec<Diagnostic>,
}
impl KernelRegistrySnapshot {
    #[must_use]
    pub fn empty() -> Self {
        Self {
            kernels: Vec::new(),
            diagnostics: Vec::new(),
        }
    }
    pub fn add_kernel(&mut self, kernel: KernelDescriptor) {
        self.kernels.push(kernel);
    }
    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
    }
    #[must_use]
    pub fn kernel_count(&self) -> usize {
        self.kernels.len()
    }
    #[must_use]
    pub fn find_by_function_name(&self, name: &str) -> Vec<&KernelDescriptor> {
        self.kernels
            .iter()
            .filter(|k| k.function.name == name)
            .collect()
    }
    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.diagnostics.iter().any(|d| {
            matches!(
                d.severity,
                DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
            )
        }) || self.kernels.iter().any(KernelDescriptor::has_errors)
    }
    #[must_use]
    pub fn summary(&self) -> String {
        format!(
            "kernel_registry_snapshot[kernels={}, diagnostics={}]",
            self.kernels.len(),
            self.diagnostics.len()
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct KernelSelectionRequest {
    pub expression: Expression,
    pub input_dtype: Option<LogicalDType>,
    pub encoding: Option<EncodingKind>,
    pub prefer_zero_decode: bool,
    pub allow_partial_decode: bool,
    pub allow_full_materialization: bool,
}
impl KernelSelectionRequest {
    #[must_use]
    pub fn new(expression: Expression) -> Self {
        Self {
            expression,
            input_dtype: None,
            encoding: None,
            prefer_zero_decode: true,
            allow_partial_decode: true,
            allow_full_materialization: false,
        }
    }
    #[must_use]
    pub fn with_input_dtype(mut self, dtype: LogicalDType) -> Self {
        self.input_dtype = Some(dtype);
        self
    }
    #[must_use]
    pub fn with_encoding(mut self, encoding: EncodingKind) -> Self {
        self.encoding = Some(encoding);
        self
    }
    #[must_use]
    pub fn prefer_zero_decode(mut self, value: bool) -> Self {
        self.prefer_zero_decode = value;
        self
    }
    #[must_use]
    pub fn allow_partial_decode(mut self, value: bool) -> Self {
        self.allow_partial_decode = value;
        self
    }
    #[must_use]
    pub fn allow_full_materialization(mut self, value: bool) -> Self {
        self.allow_full_materialization = value;
        self
    }
    #[must_use]
    pub fn summary(&self) -> String {
        format!(
            "kernel_selection_request[expr={}, prefer_zero_decode={}, allow_partial_decode={}, allow_full_materialization={}]",
            self.expression.id.as_str(),
            self.prefer_zero_decode,
            self.allow_partial_decode,
            self.allow_full_materialization
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KernelSelectionStatus {
    Selected,
    NoMatchingKernel,
    MaterializationRequiredButDisabled,
    EffectDisabled,
    Unsupported,
}
impl KernelSelectionStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Selected => "selected",
            Self::NoMatchingKernel => "no_matching_kernel",
            Self::MaterializationRequiredButDisabled => "materialization_required_but_disabled",
            Self::EffectDisabled => "effect_disabled",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn is_error(&self) -> bool {
        !matches!(self, Self::Selected)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct KernelSelectionResult {
    pub status: KernelSelectionStatus,
    pub selected_kernel: Option<KernelDescriptor>,
    pub diagnostics: Vec<Diagnostic>,
}
impl KernelSelectionResult {
    #[must_use]
    pub fn selected(kernel: KernelDescriptor) -> Self {
        Self {
            status: KernelSelectionStatus::Selected,
            selected_kernel: Some(kernel),
            diagnostics: Vec::new(),
        }
    }
    #[must_use]
    pub fn no_matching_kernel(feature: impl Into<String>, reason: impl Into<String>) -> Self {
        let feature = feature.into();
        let reason = reason.into();
        Self {
            status: KernelSelectionStatus::NoMatchingKernel,
            selected_kernel: None,
            diagnostics: vec![Diagnostic::unsupported(
                DiagnosticCode::NotImplemented,
                feature,
                format!("No matching kernel: {reason}"),
                Some("Review kernel capabilities and expression requirements.".to_string()),
            )],
        }
    }
    #[must_use]
    pub fn unsupported(feature: impl Into<String>, reason: impl Into<String>) -> Self {
        let feature = feature.into();
        let reason = reason.into();
        Self {
            status: KernelSelectionStatus::Unsupported,
            selected_kernel: None,
            diagnostics: vec![Diagnostic::unsupported(
                DiagnosticCode::NotImplemented,
                feature,
                reason,
                Some("Use supported native kernel paths.".to_string()),
            )],
        }
    }
    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
    }
    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.status.is_error()
            || self.diagnostics.iter().any(|d| {
                matches!(
                    d.severity,
                    DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
                )
            })
    }
    #[must_use]
    pub fn summary(&self) -> String {
        format!(
            "kernel_selection_result[status={}, selected={}]",
            self.status.as_str(),
            self.selected_kernel.is_some()
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn expr_id(value: &str) -> ExprId {
        ExprId::new(value).expect("expression id")
    }

    fn col(value: &str) -> ColumnRef {
        ColumnRef::new(value).expect("column")
    }

    fn row(values: &[(&str, ScalarValue)]) -> ExpressionInputRow {
        values
            .iter()
            .map(|(name, value)| ((*name).to_string(), value.clone()))
            .collect()
    }

    #[test]
    fn expr_id_rejects_empty_ids() {
        assert!(ExprId::new(" ").is_err());
    }
    #[test]
    fn scalar_null_is_null() {
        assert!(ScalarValue::Null.is_null());
    }
    #[test]
    fn scalar_utf8_dtype() {
        assert_eq!(ScalarValue::Utf8("x".into()).dtype(), LogicalDType::Utf8);
    }
    #[test]
    fn scalar_binary_summary_has_length_only() {
        let s = ScalarValue::Binary(vec![1, 2, 3]).summary();
        assert!(s.contains("len=3"));
        assert!(!s.contains("1,2,3"));
    }
    #[test]
    fn unary_not_as_str() {
        assert_eq!(UnaryOp::Not.as_str(), "not");
    }
    #[test]
    fn binary_add_as_str() {
        assert_eq!(BinaryOp::Add.as_str(), "add");
    }
    #[test]
    fn expression_literal_sets_dtype() {
        let e = Expression::literal(ExprId::new("e1").expect("ok"), ScalarValue::Int64(1));
        assert_eq!(e.dtype, Some(LogicalDType::Int64));
    }
    #[test]
    fn expression_cast_sets_dtype() {
        let input = Expression::literal(expr_id("e1"), ScalarValue::Int64(1));
        let e = Expression::cast(expr_id("cast"), input, LogicalDType::Float64);
        assert_eq!(e.dtype, Some(LogicalDType::Float64));
        assert!(e.summary().contains("cast(float64)"));
    }
    #[test]
    fn expression_try_cast_sets_dtype() {
        let input = Expression::literal(expr_id("e1"), ScalarValue::Utf8("1".to_string()));
        let e = Expression::try_cast(expr_id("try-cast"), input, LogicalDType::Int64);
        assert_eq!(e.dtype, Some(LogicalDType::Int64));
        assert!(e.summary().contains("try_cast(int64)"));
    }
    #[test]
    fn expression_unsupported_has_errors() {
        let e = Expression::unsupported(ExprId::new("e1").expect("ok"), "feature", "reason");
        assert!(e.has_errors());
    }
    #[test]
    fn null_behavior_as_str() {
        assert_eq!(NullBehavior::NullAware.as_str(), "null_aware");
    }
    #[test]
    fn effect_level_pure_not_effectful() {
        assert!(!EffectLevel::PureDeterministic.is_effectful());
    }
    #[test]
    fn effect_level_external_read_effectful() {
        assert!(EffectLevel::ExternalRead.is_effectful());
    }
    #[test]
    fn function_signature_rejects_empty_name() {
        assert!(
            FunctionSignature::new(" ", FunctionCategory::Scalar, LogicalDType::Int64).is_err()
        );
    }
    #[test]
    fn function_signature_effectful() {
        let s = FunctionSignature::new("f", FunctionCategory::Scalar, LogicalDType::Int64)
            .expect("ok")
            .with_effect_level(EffectLevel::ExternalWrite);
        assert!(s.is_effectful());
    }
    #[test]
    fn kernel_kind_decoded_reference_only() {
        assert!(KernelKind::DecodedReference.is_reference_only());
    }
    #[test]
    fn kernel_eval_metadata_maps() {
        assert_eq!(
            KernelEvalMode::MetadataOnly.to_execution_state(),
            ExecutionState::MetadataOnly
        );
    }
    #[test]
    fn kernel_eval_encoded_maps() {
        assert_eq!(
            KernelEvalMode::Encoded.to_execution_state(),
            ExecutionState::EncodedEvaluation
        );
    }
    #[test]
    fn kernel_capability_metadata_kind() {
        assert_eq!(KernelCapability::metadata().kind, KernelKind::Metadata);
    }
    #[test]
    fn kernel_capability_decoded_reference_is_reference_only() {
        let c = KernelCapability::decoded_reference();
        assert!(c.kind.is_reference_only());
    }
    #[test]
    fn kernel_capability_supports_dtype_when_empty() {
        assert!(KernelCapability::metadata().supports_dtype(&LogicalDType::Int64));
    }
    #[test]
    fn kernel_capability_unsupported_never_supports_dtype_or_encoding() {
        let unsupported = KernelCapability::unsupported();
        assert!(!unsupported.supports_dtype(&LogicalDType::Int64));
        assert!(!unsupported.supports_encoding(&EncodingKind::Dictionary));
    }
    #[test]
    fn kernel_id_rejects_empty_ids() {
        assert!(KernelId::new("").is_err());
    }
    #[test]
    fn kernel_descriptor_is_reference_only() {
        let kd = KernelDescriptor::new(
            KernelId::new("k").expect("ok"),
            FunctionSignature::new("f", FunctionCategory::Scalar, LogicalDType::Int64).expect("ok"),
            KernelCapability::decoded_reference(),
        );
        assert!(kd.is_reference_only());
    }
    #[test]
    fn registry_empty_zero_kernels() {
        assert_eq!(KernelRegistrySnapshot::empty().kernel_count(), 0);
    }
    #[test]
    fn registry_find_by_function_name() {
        let mut reg = KernelRegistrySnapshot::empty();
        reg.add_kernel(KernelDescriptor::new(
            KernelId::new("k").expect("ok"),
            FunctionSignature::new("fn1", FunctionCategory::Scalar, LogicalDType::Int64)
                .expect("ok"),
            KernelCapability::metadata(),
        ));
        assert_eq!(reg.find_by_function_name("fn1").len(), 1);
    }
    #[test]
    fn selection_request_default_prefer_zero_decode_true() {
        let req = KernelSelectionRequest::new(Expression::literal(
            ExprId::new("e").expect("ok"),
            ScalarValue::Boolean(true),
        ));
        assert!(req.prefer_zero_decode);
    }
    #[test]
    fn selection_result_selected_has_no_errors() {
        let kernel = KernelDescriptor::new(
            KernelId::new("k").expect("ok"),
            FunctionSignature::new("f", FunctionCategory::Scalar, LogicalDType::Int64).expect("ok"),
            KernelCapability::metadata(),
        );
        assert!(!KernelSelectionResult::selected(kernel).has_errors());
    }
    #[test]
    fn selection_result_no_matching_has_errors_and_fallback_disabled() {
        let result = KernelSelectionResult::no_matching_kernel("x", "y");
        assert!(result.has_errors());
        assert!(!result.diagnostics[0].fallback.attempted);
    }

    #[test]
    fn expression_semantics_evaluates_comparison_without_fallback() {
        let expression = Expression::new(
            expr_id("pred"),
            ExpressionKind::Compare {
                left: Box::new(Expression::column(expr_id("value"), col("value"))),
                op: ComparisonOp::GtEq,
                right: Box::new(Expression::literal(expr_id("lit"), ScalarValue::Int64(3))),
            },
        );
        let report = evaluate_expression(&expression, &row(&[("value", ScalarValue::Int64(5))]));

        assert_eq!(report.schema_version, "shardloom.expression_semantics.v1");
        assert_eq!(report.status, ExpressionEvaluationStatus::Evaluated);
        assert_eq!(report.operator_family, "comparison");
        assert_eq!(report.value, Some(ScalarValue::Boolean(true)));
        assert_eq!(report.output_dtype, Some(LogicalDType::Boolean));
        assert_eq!(report.null_behavior, NullBehavior::NullPropagating);
        assert!(report.data_materialized);
        assert!(!report.data_decoded);
        assert!(!report.fallback_attempted);
        assert!(!report.external_engine_invoked);
        assert_eq!(report.claim_gate_status, "not_claim_grade");
        assert!(!report.fallback_execution_allowed());
    }

    #[test]
    fn expression_semantics_evaluates_mixed_numeric_binary_without_fallback() {
        let expression = Expression::new(
            expr_id("mixed-add"),
            ExpressionKind::Binary {
                left: Box::new(Expression::literal(expr_id("int"), ScalarValue::Int64(5))),
                op: BinaryOp::Add,
                right: Box::new(Expression::literal(
                    expr_id("float"),
                    ScalarValue::Float64(2.5),
                )),
            },
        );
        let report = evaluate_expression(&expression, &ExpressionInputRow::new());

        assert_eq!(report.status, ExpressionEvaluationStatus::Evaluated);
        assert_eq!(report.value, Some(ScalarValue::Float64(7.5)));
        assert_eq!(report.output_dtype, Some(LogicalDType::Float64));
        assert!(!report.fallback_attempted);
        assert!(!report.external_engine_invoked);
    }

    #[test]
    fn expression_semantics_evaluates_mixed_numeric_compare_without_fallback() {
        let expression = Expression::new(
            expr_id("mixed-compare"),
            ExpressionKind::Compare {
                left: Box::new(Expression::literal(expr_id("int"), ScalarValue::Int64(5))),
                op: ComparisonOp::Lt,
                right: Box::new(Expression::literal(
                    expr_id("float"),
                    ScalarValue::Float64(5.5),
                )),
            },
        );
        let report = evaluate_expression(&expression, &ExpressionInputRow::new());

        assert_eq!(report.status, ExpressionEvaluationStatus::Evaluated);
        assert_eq!(report.value, Some(ScalarValue::Boolean(true)));
        assert_eq!(report.output_dtype, Some(LogicalDType::Boolean));
        assert!(!report.fallback_attempted);
        assert!(!report.external_engine_invoked);
    }

    #[test]
    fn expression_semantics_blocks_lossy_mixed_numeric_coercion_without_fallback() {
        let expression = Expression::new(
            expr_id("lossy-mixed-add"),
            ExpressionKind::Binary {
                left: Box::new(Expression::literal(
                    expr_id("large-int"),
                    ScalarValue::Int64(9_007_199_254_740_993),
                )),
                op: BinaryOp::Add,
                right: Box::new(Expression::literal(
                    expr_id("float"),
                    ScalarValue::Float64(1.0),
                )),
            },
        );
        let report = evaluate_expression(&expression, &ExpressionInputRow::new());

        assert_eq!(report.status, ExpressionEvaluationStatus::Unsupported);
        assert!(report.has_errors());
        let diagnostics = format!("{:?}", report.diagnostics);
        assert!(
            diagnostics.contains("exactly representable as float64"),
            "{diagnostics}"
        );
        assert!(!report.fallback_attempted);
        assert!(!report.external_engine_invoked);
    }

    #[test]
    fn expression_semantics_uses_three_valued_boolean_logic() {
        let expression = Expression::new(
            expr_id("and"),
            ExpressionKind::Binary {
                left: Box::new(Expression::literal(expr_id("null"), ScalarValue::Null)),
                op: BinaryOp::And,
                right: Box::new(Expression::literal(
                    expr_id("false"),
                    ScalarValue::Boolean(false),
                )),
            },
        );
        let report = evaluate_expression(&expression, &ExpressionInputRow::new());

        assert_eq!(report.status, ExpressionEvaluationStatus::Evaluated);
        assert_eq!(report.value, Some(ScalarValue::Boolean(false)));
        assert_eq!(report.output_dtype, Some(LogicalDType::Boolean));
        assert!(!report.has_errors());
    }

    #[test]
    fn expression_semantics_null_comparison_returns_boolean_null() {
        let expression = Expression::new(
            expr_id("cmp-null"),
            ExpressionKind::Compare {
                left: Box::new(Expression::literal(expr_id("null"), ScalarValue::Null)),
                op: ComparisonOp::Eq,
                right: Box::new(Expression::literal(expr_id("one"), ScalarValue::Int64(1))),
            },
        );
        let report = evaluate_expression(&expression, &ExpressionInputRow::new());

        assert_eq!(report.status, ExpressionEvaluationStatus::Evaluated);
        assert_eq!(report.value, Some(ScalarValue::Null));
        assert_eq!(report.output_dtype, Some(LogicalDType::Boolean));
    }

    #[test]
    fn expression_semantics_evaluates_binary_equality_without_fallback() {
        for (op, right, expected) in [
            (ComparisonOp::Eq, vec![1, 2, 3], true),
            (ComparisonOp::Eq, vec![1, 2, 4], false),
            (ComparisonOp::NotEq, vec![1, 2, 3], false),
            (ComparisonOp::NotEq, vec![1, 2, 4], true),
        ] {
            let expression = Expression::new(
                expr_id("binary-cmp"),
                ExpressionKind::Compare {
                    left: Box::new(Expression::literal(
                        expr_id("left"),
                        ScalarValue::Binary(vec![1, 2, 3]),
                    )),
                    op,
                    right: Box::new(Expression::literal(
                        expr_id("right"),
                        ScalarValue::Binary(right),
                    )),
                },
            );
            let report = evaluate_expression(&expression, &ExpressionInputRow::new());

            assert_eq!(report.status, ExpressionEvaluationStatus::Evaluated);
            assert_eq!(report.operator_family, "comparison");
            assert_eq!(report.value, Some(ScalarValue::Boolean(expected)));
            assert_eq!(report.output_dtype, Some(LogicalDType::Boolean));
            assert!(!report.fallback_attempted);
            assert!(!report.external_engine_invoked);
        }
    }

    #[test]
    fn expression_semantics_blocks_binary_ordering_without_fallback() {
        let expression = Expression::new(
            expr_id("binary-lt"),
            ExpressionKind::Compare {
                left: Box::new(Expression::literal(
                    expr_id("left"),
                    ScalarValue::Binary(vec![1, 2, 3]),
                )),
                op: ComparisonOp::Lt,
                right: Box::new(Expression::literal(
                    expr_id("right"),
                    ScalarValue::Binary(vec![1, 2, 4]),
                )),
            },
        );
        let report = evaluate_expression(&expression, &ExpressionInputRow::new());

        assert_eq!(report.status, ExpressionEvaluationStatus::Unsupported);
        assert_eq!(report.operator_family, "comparison");
        assert!(report.has_errors());
        assert!(
            report
                .diagnostics
                .iter()
                .any(|diagnostic| diagnostic.reason.as_deref()
                    == Some("binary comparison admits equality and inequality only"))
        );
        assert!(!report.fallback_attempted);
        assert!(!report.external_engine_invoked);
    }

    #[test]
    fn expression_semantics_casts_utf8_to_int64() {
        let expression = Expression::cast(
            expr_id("cast"),
            Expression::literal(expr_id("text"), ScalarValue::Utf8("42".to_string())),
            LogicalDType::Int64,
        );
        let report = evaluate_expression(&expression, &ExpressionInputRow::new());

        assert_eq!(report.status, ExpressionEvaluationStatus::Evaluated);
        assert_eq!(report.value, Some(ScalarValue::Int64(42)));
        assert_eq!(report.output_dtype, Some(LogicalDType::Int64));
    }

    #[test]
    fn expression_semantics_try_casts_invalid_utf8_to_null_without_fallback() {
        let expression = Expression::try_cast(
            expr_id("try-cast"),
            Expression::column(expr_id("amount"), col("amount")),
            LogicalDType::Int64,
        );
        let valid = evaluate_expression(
            &expression,
            &row(&[("amount", ScalarValue::Utf8("42".to_string()))]),
        );
        assert_eq!(valid.status, ExpressionEvaluationStatus::Evaluated);
        assert_eq!(valid.operator_family, "try_cast");
        assert_eq!(valid.value, Some(ScalarValue::Int64(42)));
        assert_eq!(valid.output_dtype, Some(LogicalDType::Int64));
        assert_eq!(valid.null_behavior, NullBehavior::NullAware);
        assert!(!valid.fallback_attempted);
        assert!(!valid.external_engine_invoked);

        let invalid = evaluate_expression(
            &expression,
            &row(&[("amount", ScalarValue::Utf8("not-an-int".to_string()))]),
        );
        assert_eq!(invalid.status, ExpressionEvaluationStatus::Evaluated);
        assert_eq!(invalid.operator_family, "try_cast");
        assert_eq!(invalid.value, Some(ScalarValue::Null));
        assert_eq!(invalid.output_dtype, Some(LogicalDType::Int64));
        assert_eq!(invalid.null_behavior, NullBehavior::NullAware);
        assert!(!invalid.has_errors());
        assert!(!invalid.fallback_attempted);
        assert!(!invalid.external_engine_invoked);
    }

    #[test]
    fn expression_semantics_try_cast_blocks_unadmitted_pairs_without_fallback() {
        let expression = Expression::try_cast(
            expr_id("try-cast-binary"),
            Expression::literal(expr_id("binary"), ScalarValue::Binary(vec![1, 2, 3])),
            LogicalDType::Int64,
        );
        let report = evaluate_expression(&expression, &ExpressionInputRow::new());

        assert_eq!(report.status, ExpressionEvaluationStatus::Unsupported);
        assert_eq!(report.operator_family, "try_cast");
        assert!(report.has_errors());
        assert!(!report.fallback_attempted);
        assert!(!report.external_engine_invoked);
    }

    #[test]
    fn expression_semantics_evaluates_utf8_ends_with_without_fallback() {
        let expression = Expression::new(
            expr_id("ends-with"),
            ExpressionKind::FunctionCall {
                name: "utf8_ends_with".to_string(),
                args: vec![
                    Expression::column(expr_id("label"), col("label")),
                    Expression::literal(expr_id("suffix"), ScalarValue::Utf8("ta".to_string())),
                ],
            },
        );
        let report = evaluate_expression(
            &expression,
            &row(&[("label", ScalarValue::Utf8("beta".to_string()))]),
        );

        assert_eq!(report.status, ExpressionEvaluationStatus::Evaluated);
        assert_eq!(report.operator_family, "string_predicate");
        assert_eq!(report.value, Some(ScalarValue::Boolean(true)));
        assert_eq!(report.output_dtype, Some(LogicalDType::Boolean));
        assert!(!report.fallback_attempted);
        assert!(!report.external_engine_invoked);
    }

    #[test]
    fn expression_semantics_evaluates_utf8_regex_match_without_fallback() {
        let expression = Expression::new(
            expr_id("regex-match"),
            ExpressionKind::FunctionCall {
                name: "utf8_regex_match".to_string(),
                args: vec![
                    Expression::column(expr_id("label"), col("label")),
                    Expression::literal(
                        expr_id("pattern"),
                        ScalarValue::Utf8("^(alpha|gamma)$".to_string()),
                    ),
                ],
            },
        );
        let report = evaluate_expression(
            &expression,
            &row(&[("label", ScalarValue::Utf8("gamma".to_string()))]),
        );

        assert_eq!(report.status, ExpressionEvaluationStatus::Evaluated);
        assert_eq!(report.operator_family, "string_predicate");
        assert_eq!(report.value, Some(ScalarValue::Boolean(true)));
        assert_eq!(report.output_dtype, Some(LogicalDType::Boolean));
        assert_eq!(report.null_behavior, NullBehavior::NullPropagating);
        assert!(!report.fallback_attempted);
        assert!(!report.external_engine_invoked);

        let null_report = evaluate_expression(&expression, &row(&[("label", ScalarValue::Null)]));
        assert_eq!(null_report.status, ExpressionEvaluationStatus::Evaluated);
        assert_eq!(null_report.value, Some(ScalarValue::Null));
        assert_eq!(null_report.output_dtype, Some(LogicalDType::Boolean));
        assert!(!null_report.fallback_attempted);
        assert!(!null_report.external_engine_invoked);
    }

    #[test]
    fn expression_semantics_blocks_invalid_regex_pattern_without_fallback() {
        let expression = Expression::new(
            expr_id("invalid-regex"),
            ExpressionKind::FunctionCall {
                name: "regexp".to_string(),
                args: vec![
                    Expression::column(expr_id("label"), col("label")),
                    Expression::literal(expr_id("pattern"), ScalarValue::Utf8("[".to_string())),
                ],
            },
        );
        let report = evaluate_expression(
            &expression,
            &row(&[("label", ScalarValue::Utf8("alpha".to_string()))]),
        );

        assert_eq!(report.status, ExpressionEvaluationStatus::InvalidInput);
        assert_eq!(report.operator_family, "string_predicate");
        assert!(report.has_errors());
        assert!(!report.fallback_attempted);
        assert!(!report.external_engine_invoked);
        assert!(report.diagnostics.iter().all(|d| !d.fallback.attempted));
    }

    #[test]
    fn expression_semantics_rejects_string_predicate_type_errors_before_nulls() {
        let expression = Expression::new(
            expr_id("contains-null-wrong-type"),
            ExpressionKind::FunctionCall {
                name: "contains".to_string(),
                args: vec![
                    Expression::literal(expr_id("null-text"), ScalarValue::Null),
                    Expression::literal(expr_id("bad-needle"), ScalarValue::Int64(1)),
                ],
            },
        );
        let report = evaluate_expression(&expression, &row(&[]));

        assert_eq!(report.status, ExpressionEvaluationStatus::Unsupported);
        assert_eq!(report.operator_family, "string_predicate");
        assert!(report.has_errors());
        assert!(!report.fallback_attempted);
        assert!(!report.external_engine_invoked);
    }

    #[test]
    fn expression_semantics_evaluates_utf8_length_without_fallback() {
        let expression = Expression::new(
            expr_id("length"),
            ExpressionKind::FunctionCall {
                name: "length".to_string(),
                args: vec![Expression::column(expr_id("label"), col("label"))],
            },
        );
        let report = evaluate_expression(
            &expression,
            &row(&[("label", ScalarValue::Utf8("beta".to_string()))]),
        );

        assert_eq!(report.status, ExpressionEvaluationStatus::Evaluated);
        assert_eq!(report.operator_family, "string_length");
        assert_eq!(report.value, Some(ScalarValue::Int64(4)));
        assert_eq!(report.output_dtype, Some(LogicalDType::Int64));
        assert_eq!(report.null_behavior, NullBehavior::NullPropagating);
        assert!(!report.fallback_attempted);
        assert!(!report.external_engine_invoked);
    }

    #[test]
    fn expression_semantics_evaluates_string_functions_without_fallback() {
        let concat = Expression::new(
            expr_id("concat"),
            ExpressionKind::FunctionCall {
                name: "concat".to_string(),
                args: vec![
                    Expression::column(expr_id("label"), col("label")),
                    Expression::literal(expr_id("separator"), ScalarValue::Utf8("-".to_string())),
                    Expression::column(expr_id("segment"), col("segment")),
                ],
            },
        );
        let concat_report = evaluate_expression(
            &concat,
            &row(&[
                ("label", ScalarValue::Utf8("alpha".to_string())),
                ("segment", ScalarValue::Utf8("north".to_string())),
            ]),
        );
        assert_eq!(concat_report.status, ExpressionEvaluationStatus::Evaluated);
        assert_eq!(concat_report.operator_family, "string_function");
        assert_eq!(
            concat_report.value,
            Some(ScalarValue::Utf8("alpha-north".to_string()))
        );
        assert_eq!(concat_report.output_dtype, Some(LogicalDType::Utf8));
        assert_eq!(concat_report.null_behavior, NullBehavior::NullPropagating);
        assert!(!concat_report.fallback_attempted);
        assert!(!concat_report.external_engine_invoked);

        let substr = Expression::new(
            expr_id("substr"),
            ExpressionKind::FunctionCall {
                name: "substring".to_string(),
                args: vec![
                    Expression::column(expr_id("label"), col("label")),
                    Expression::literal(expr_id("start"), ScalarValue::Int64(2)),
                    Expression::literal(expr_id("length"), ScalarValue::Int64(3)),
                ],
            },
        );
        let substr_report = evaluate_expression(
            &substr,
            &row(&[("label", ScalarValue::Utf8("crane".to_string()))]),
        );
        assert_eq!(substr_report.status, ExpressionEvaluationStatus::Evaluated);
        assert_eq!(substr_report.operator_family, "string_function");
        assert_eq!(
            substr_report.value,
            Some(ScalarValue::Utf8("ran".to_string()))
        );
        assert!(!substr_report.fallback_attempted);
        assert!(!substr_report.external_engine_invoked);

        let replace = Expression::new(
            expr_id("replace"),
            ExpressionKind::FunctionCall {
                name: "replace".to_string(),
                args: vec![
                    Expression::column(expr_id("label"), col("label")),
                    Expression::literal(expr_id("needle"), ScalarValue::Utf8(" ".to_string())),
                    Expression::literal(expr_id("replacement"), ScalarValue::Utf8("_".to_string())),
                ],
            },
        );
        let replace_report = evaluate_expression(
            &replace,
            &row(&[("label", ScalarValue::Utf8("alpha beta".to_string()))]),
        );
        assert_eq!(replace_report.status, ExpressionEvaluationStatus::Evaluated);
        assert_eq!(replace_report.operator_family, "string_function");
        assert_eq!(
            replace_report.value,
            Some(ScalarValue::Utf8("alpha_beta".to_string()))
        );
        assert!(!replace_report.fallback_attempted);
        assert!(!replace_report.external_engine_invoked);
    }

    #[test]
    fn expression_semantics_evaluates_string_left_right_without_fallback() {
        let input = row(&[("label", ScalarValue::Utf8("crane".to_string()))]);

        for (function, expected) in [("left", "cra"), ("right", "ane")] {
            let expression = Expression::new(
                expr_id(function),
                ExpressionKind::FunctionCall {
                    name: function.to_string(),
                    args: vec![
                        Expression::column(expr_id("label"), col("label")),
                        Expression::literal(expr_id("count"), ScalarValue::Int64(3)),
                    ],
                },
            );
            let report = evaluate_expression(&expression, &input);

            assert_eq!(report.status, ExpressionEvaluationStatus::Evaluated);
            assert_eq!(report.operator_family, "string_function");
            assert_eq!(report.value, Some(ScalarValue::Utf8(expected.to_string())));
            assert_eq!(report.output_dtype, Some(LogicalDType::Utf8));
            assert_eq!(report.null_behavior, NullBehavior::NullPropagating);
            assert!(!report.fallback_attempted);
            assert!(!report.external_engine_invoked);
        }
    }

    #[test]
    fn expression_semantics_evaluates_binary_helpers_without_fallback() {
        for (function, input, expected) in [
            ("unhex", "00ff10", vec![0x00, 0xff, 0x10]),
            ("from_base64", "AP8Q", vec![0x00, 0xff, 0x10]),
            ("from_base64", "YQ==", vec![b'a']),
        ] {
            let expression = Expression::new(
                expr_id(function),
                ExpressionKind::FunctionCall {
                    name: function.to_string(),
                    args: vec![Expression::column(expr_id("payload"), col("payload"))],
                },
            );
            let report = evaluate_expression(
                &expression,
                &row(&[("payload", ScalarValue::Utf8(input.to_string()))]),
            );

            assert_eq!(report.status, ExpressionEvaluationStatus::Evaluated);
            assert_eq!(report.operator_family, "binary_function");
            assert_eq!(report.value, Some(ScalarValue::Binary(expected)));
            assert_eq!(report.output_dtype, Some(LogicalDType::Binary));
            assert_eq!(report.null_behavior, NullBehavior::NullPropagating);
            assert!(!report.fallback_attempted);
            assert!(!report.external_engine_invoked);
        }
    }

    #[test]
    fn expression_semantics_blocks_invalid_binary_helpers_without_fallback() {
        for (function, input, expected_status) in [
            (
                "unhex",
                ScalarValue::Utf8("0".to_string()),
                ExpressionEvaluationStatus::InvalidInput,
            ),
            (
                "unhex",
                ScalarValue::Utf8("00xz".to_string()),
                ExpressionEvaluationStatus::InvalidInput,
            ),
            (
                "from_base64",
                ScalarValue::Utf8("AP8".to_string()),
                ExpressionEvaluationStatus::InvalidInput,
            ),
            (
                "from_base64",
                ScalarValue::Utf8("AP9=".to_string()),
                ExpressionEvaluationStatus::InvalidInput,
            ),
            (
                "unhex",
                ScalarValue::Int64(1),
                ExpressionEvaluationStatus::Unsupported,
            ),
        ] {
            let expression = Expression::new(
                expr_id(function),
                ExpressionKind::FunctionCall {
                    name: function.to_string(),
                    args: vec![Expression::column(expr_id("payload"), col("payload"))],
                },
            );
            let report = evaluate_expression(&expression, &row(&[("payload", input)]));

            assert_eq!(report.status, expected_status);
            assert_eq!(report.operator_family, "binary_function");
            assert!(report.has_errors());
            assert!(!report.fallback_attempted);
            assert!(!report.external_engine_invoked);
        }
    }

    #[test]
    fn expression_semantics_null_propagates_binary_helpers_without_fallback() {
        let expression = Expression::new(
            expr_id("unhex-null"),
            ExpressionKind::FunctionCall {
                name: "unhex".to_string(),
                args: vec![Expression::column(expr_id("payload"), col("payload"))],
            },
        );
        let report = evaluate_expression(&expression, &row(&[("payload", ScalarValue::Null)]));

        assert_eq!(report.status, ExpressionEvaluationStatus::Evaluated);
        assert_eq!(report.operator_family, "binary_function");
        assert_eq!(report.value, Some(ScalarValue::Null));
        assert_eq!(report.output_dtype, Some(LogicalDType::Binary));
        assert_eq!(report.null_behavior, NullBehavior::NullPropagating);
        assert!(!report.fallback_attempted);
        assert!(!report.external_engine_invoked);
    }

    fn assert_string_function_error(
        expression: &Expression,
        input: &ExpressionInputRow,
        expected_status: ExpressionEvaluationStatus,
    ) {
        let report = evaluate_expression(expression, input);
        assert_eq!(report.status, expected_status);
        assert_eq!(report.operator_family, "string_function");
        assert!(report.has_errors());
        assert!(!report.fallback_attempted);
        assert!(!report.external_engine_invoked);
    }

    #[test]
    fn expression_semantics_blocks_invalid_string_functions_without_fallback() {
        let invalid_start = Expression::new(
            expr_id("substr-invalid"),
            ExpressionKind::FunctionCall {
                name: "substr".to_string(),
                args: vec![
                    Expression::column(expr_id("label"), col("label")),
                    Expression::literal(expr_id("start"), ScalarValue::Int64(0)),
                    Expression::literal(expr_id("length"), ScalarValue::Int64(3)),
                ],
            },
        );
        assert_string_function_error(
            &invalid_start,
            &row(&[("label", ScalarValue::Utf8("alpha".to_string()))]),
            ExpressionEvaluationStatus::InvalidInput,
        );

        let empty_replace = Expression::new(
            expr_id("replace-empty"),
            ExpressionKind::FunctionCall {
                name: "replace".to_string(),
                args: vec![
                    Expression::column(expr_id("label"), col("label")),
                    Expression::literal(expr_id("needle"), ScalarValue::Utf8(String::new())),
                    Expression::literal(expr_id("replacement"), ScalarValue::Utf8("_".to_string())),
                ],
            },
        );
        assert_string_function_error(
            &empty_replace,
            &row(&[("label", ScalarValue::Utf8("alpha".to_string()))]),
            ExpressionEvaluationStatus::InvalidInput,
        );

        let wrong_type = Expression::new(
            expr_id("concat-wrong-type"),
            ExpressionKind::FunctionCall {
                name: "concat".to_string(),
                args: vec![
                    Expression::column(expr_id("label"), col("label")),
                    Expression::literal(expr_id("bad"), ScalarValue::Int64(1)),
                ],
            },
        );
        assert_string_function_error(
            &wrong_type,
            &row(&[("label", ScalarValue::Utf8("alpha".to_string()))]),
            ExpressionEvaluationStatus::Unsupported,
        );

        let null_and_wrong_type = Expression::new(
            expr_id("concat-null-wrong-type"),
            ExpressionKind::FunctionCall {
                name: "concat".to_string(),
                args: vec![
                    Expression::literal(expr_id("null"), ScalarValue::Null),
                    Expression::literal(expr_id("bad"), ScalarValue::Int64(1)),
                ],
            },
        );
        assert_string_function_error(
            &null_and_wrong_type,
            &row(&[]),
            ExpressionEvaluationStatus::Unsupported,
        );

        let substr_null_and_wrong_type = Expression::new(
            expr_id("substr-null-wrong-type"),
            ExpressionKind::FunctionCall {
                name: "substr".to_string(),
                args: vec![
                    Expression::literal(expr_id("null-text"), ScalarValue::Null),
                    Expression::literal(expr_id("bad-start"), ScalarValue::Utf8("1".to_string())),
                    Expression::literal(expr_id("length"), ScalarValue::Int64(3)),
                ],
            },
        );
        assert_string_function_error(
            &substr_null_and_wrong_type,
            &row(&[]),
            ExpressionEvaluationStatus::Unsupported,
        );
    }

    #[test]
    fn expression_semantics_evaluates_numeric_abs_without_fallback() {
        let expression = Expression::new(
            expr_id("abs"),
            ExpressionKind::FunctionCall {
                name: "abs".to_string(),
                args: vec![Expression::column(expr_id("amount"), col("amount"))],
            },
        );
        let report = evaluate_expression(&expression, &row(&[("amount", ScalarValue::Int64(-42))]));

        assert_eq!(report.status, ExpressionEvaluationStatus::Evaluated);
        assert_eq!(report.operator_family, "numeric_abs");
        assert_eq!(report.value, Some(ScalarValue::Int64(42)));
        assert_eq!(report.output_dtype, Some(LogicalDType::Int64));
        assert_eq!(report.null_behavior, NullBehavior::NullPropagating);
        assert!(!report.fallback_attempted);
        assert!(!report.external_engine_invoked);
    }

    #[test]
    fn expression_semantics_blocks_numeric_abs_overflow_without_fallback() {
        let expression = Expression::new(
            expr_id("abs-overflow"),
            ExpressionKind::FunctionCall {
                name: "abs".to_string(),
                args: vec![Expression::column(expr_id("amount"), col("amount"))],
            },
        );
        let report = evaluate_expression(
            &expression,
            &row(&[("amount", ScalarValue::Int64(i64::MIN))]),
        );

        assert_eq!(report.status, ExpressionEvaluationStatus::InvalidInput);
        assert_eq!(report.operator_family, "numeric_abs");
        assert!(report.has_errors());
        assert!(!report.fallback_attempted);
        assert!(!report.external_engine_invoked);
    }

    #[test]
    fn expression_semantics_evaluates_numeric_rounding_without_fallback() {
        let cases = [
            (
                "floor",
                ScalarValue::Float64(-2.75),
                ScalarValue::Float64(-3.0),
            ),
            (
                "ceil",
                ScalarValue::Float64(-2.75),
                ScalarValue::Float64(-2.0),
            ),
            (
                "round",
                ScalarValue::Float64(2.5),
                ScalarValue::Float64(3.0),
            ),
            ("floor", ScalarValue::Int64(7), ScalarValue::Int64(7)),
        ];

        for (name, input, expected) in cases {
            let expression = Expression::new(
                expr_id(name),
                ExpressionKind::FunctionCall {
                    name: name.to_string(),
                    args: vec![Expression::column(expr_id("amount"), col("amount"))],
                },
            );
            let report = evaluate_expression(&expression, &row(&[("amount", input)]));

            assert_eq!(report.status, ExpressionEvaluationStatus::Evaluated);
            assert_eq!(report.operator_family, "numeric_rounding");
            assert_eq!(report.value, Some(expected));
            assert_eq!(report.null_behavior, NullBehavior::NullPropagating);
            assert!(!report.fallback_attempted);
            assert!(!report.external_engine_invoked);
        }
    }

    #[test]
    fn expression_semantics_blocks_numeric_rounding_non_finite_without_fallback() {
        let expression = Expression::new(
            expr_id("round-nan"),
            ExpressionKind::FunctionCall {
                name: "round".to_string(),
                args: vec![Expression::column(expr_id("amount"), col("amount"))],
            },
        );
        let report = evaluate_expression(
            &expression,
            &row(&[("amount", ScalarValue::Float64(f64::NAN))]),
        );

        assert_eq!(report.status, ExpressionEvaluationStatus::Unsupported);
        assert_eq!(report.operator_family, "numeric_rounding");
        assert!(report.has_errors());
        assert!(!report.fallback_attempted);
        assert!(!report.external_engine_invoked);
    }

    #[test]
    fn expression_semantics_evaluates_date_add_days_without_fallback() {
        let expression = Expression::new(
            expr_id("date-add"),
            ExpressionKind::FunctionCall {
                name: "date_add_days".to_string(),
                args: vec![
                    Expression::column(expr_id("event_date"), col("event_date")),
                    Expression::literal(expr_id("days"), ScalarValue::Int64(3)),
                ],
            },
        );
        let base_date = parse_iso_date32("2026-05-19").expect("date parses");
        let expected = parse_iso_date32("2026-05-22").expect("date parses");
        let report = evaluate_expression(
            &expression,
            &row(&[("event_date", ScalarValue::Date32(base_date))]),
        );

        assert_eq!(report.status, ExpressionEvaluationStatus::Evaluated);
        assert_eq!(report.operator_family, "date_arithmetic");
        assert_eq!(report.value, Some(ScalarValue::Date32(expected)));
        assert_eq!(report.output_dtype, Some(LogicalDType::Date32));
        assert_eq!(report.null_behavior, NullBehavior::NullPropagating);
        assert!(!report.fallback_attempted);
        assert!(!report.external_engine_invoked);
    }

    #[test]
    fn expression_semantics_evaluates_timestamp_add_seconds_without_fallback() {
        let expression = Expression::new(
            expr_id("timestamp-add"),
            ExpressionKind::FunctionCall {
                name: "timestamp_add_seconds".to_string(),
                args: vec![
                    Expression::column(expr_id("event_ts"), col("event_ts")),
                    Expression::literal(expr_id("seconds"), ScalarValue::Int64(90)),
                ],
            },
        );
        let base_timestamp =
            parse_iso_timestamp_micros("2026-05-19T12:34:45Z").expect("timestamp parses");
        let expected =
            parse_iso_timestamp_micros("2026-05-19T12:36:15Z").expect("timestamp parses");
        let report = evaluate_expression(
            &expression,
            &row(&[("event_ts", ScalarValue::TimestampMicros(base_timestamp))]),
        );

        assert_eq!(report.status, ExpressionEvaluationStatus::Evaluated);
        assert_eq!(report.operator_family, "timestamp_arithmetic");
        assert_eq!(report.value, Some(ScalarValue::TimestampMicros(expected)));
        assert_eq!(report.output_dtype, Some(LogicalDType::TimestampMicros));
        assert_eq!(report.null_behavior, NullBehavior::NullPropagating);
        assert!(!report.fallback_attempted);
        assert!(!report.external_engine_invoked);
    }

    #[test]
    fn expression_semantics_evaluates_temporal_difference_without_fallback() {
        let date_expression = Expression::new(
            expr_id("date-diff"),
            ExpressionKind::FunctionCall {
                name: "date_diff_days".to_string(),
                args: vec![
                    Expression::column(expr_id("end_date"), col("end_date")),
                    Expression::column(expr_id("start_date"), col("start_date")),
                ],
            },
        );
        let start_date = parse_iso_date32("2026-05-19").expect("date parses");
        let end_date = parse_iso_date32("2026-05-23").expect("date parses");
        let date_report = evaluate_expression(
            &date_expression,
            &row(&[
                ("start_date", ScalarValue::Date32(start_date)),
                ("end_date", ScalarValue::Date32(end_date)),
            ]),
        );

        assert_eq!(date_report.status, ExpressionEvaluationStatus::Evaluated);
        assert_eq!(date_report.operator_family, "temporal_difference");
        assert_eq!(date_report.value, Some(ScalarValue::Int64(4)));
        assert_eq!(date_report.output_dtype, Some(LogicalDType::Int64));
        assert_eq!(date_report.null_behavior, NullBehavior::NullPropagating);
        assert!(!date_report.fallback_attempted);
        assert!(!date_report.external_engine_invoked);

        let timestamp_expression = Expression::new(
            expr_id("timestamp-diff"),
            ExpressionKind::FunctionCall {
                name: "timestamp_diff_seconds".to_string(),
                args: vec![
                    Expression::column(expr_id("end_ts"), col("end_ts")),
                    Expression::column(expr_id("start_ts"), col("start_ts")),
                ],
            },
        );
        let start_ts =
            parse_iso_timestamp_micros("2026-05-19T12:34:45Z").expect("timestamp parses");
        let end_ts = parse_iso_timestamp_micros("2026-05-19T12:37:50Z").expect("timestamp parses");
        let timestamp_report = evaluate_expression(
            &timestamp_expression,
            &row(&[
                ("start_ts", ScalarValue::TimestampMicros(start_ts)),
                ("end_ts", ScalarValue::TimestampMicros(end_ts)),
            ]),
        );

        assert_eq!(
            timestamp_report.status,
            ExpressionEvaluationStatus::Evaluated
        );
        assert_eq!(timestamp_report.operator_family, "temporal_difference");
        assert_eq!(timestamp_report.value, Some(ScalarValue::Int64(185)));
        assert_eq!(timestamp_report.output_dtype, Some(LogicalDType::Int64));
        assert_eq!(
            timestamp_report.null_behavior,
            NullBehavior::NullPropagating
        );
        assert!(!timestamp_report.fallback_attempted);
        assert!(!timestamp_report.external_engine_invoked);

        let null_report = evaluate_expression(
            &timestamp_expression,
            &row(&[
                ("start_ts", ScalarValue::Null),
                ("end_ts", ScalarValue::TimestampMicros(end_ts)),
            ]),
        );
        assert_eq!(null_report.status, ExpressionEvaluationStatus::Evaluated);
        assert_eq!(null_report.value, Some(ScalarValue::Null));
        assert_eq!(null_report.output_dtype, Some(LogicalDType::Int64));
        assert_eq!(null_report.null_behavior, NullBehavior::NullPropagating);
        assert!(!null_report.fallback_attempted);
        assert!(!null_report.external_engine_invoked);
    }

    #[test]
    fn expression_semantics_evaluates_date_extract_without_fallback() {
        let expression = Expression::new(
            expr_id("date-month"),
            ExpressionKind::FunctionCall {
                name: "date_month".to_string(),
                args: vec![Expression::column(expr_id("event_date"), col("event_date"))],
            },
        );
        let base_date = parse_iso_date32("2026-05-19").expect("date parses");
        let report = evaluate_expression(
            &expression,
            &row(&[("event_date", ScalarValue::Date32(base_date))]),
        );

        assert_eq!(report.status, ExpressionEvaluationStatus::Evaluated);
        assert_eq!(report.operator_family, "date_extract");
        assert_eq!(report.value, Some(ScalarValue::Int64(5)));
        assert_eq!(report.output_dtype, Some(LogicalDType::Int64));
        assert_eq!(report.null_behavior, NullBehavior::NullPropagating);
        assert!(!report.fallback_attempted);
        assert!(!report.external_engine_invoked);
    }

    #[test]
    fn expression_semantics_evaluates_date_sub_days_and_nulls_without_fallback() {
        let expression = Expression::new(
            expr_id("date-sub"),
            ExpressionKind::FunctionCall {
                name: "date_sub_days".to_string(),
                args: vec![
                    Expression::column(expr_id("event_date"), col("event_date")),
                    Expression::literal(expr_id("days"), ScalarValue::Int64(2)),
                ],
            },
        );
        let base_date = parse_iso_date32("2026-05-19").expect("date parses");
        let expected = parse_iso_date32("2026-05-17").expect("date parses");
        let report = evaluate_expression(
            &expression,
            &row(&[("event_date", ScalarValue::Date32(base_date))]),
        );

        assert_eq!(report.status, ExpressionEvaluationStatus::Evaluated);
        assert_eq!(report.operator_family, "date_arithmetic");
        assert_eq!(report.value, Some(ScalarValue::Date32(expected)));
        assert!(!report.fallback_attempted);
        assert!(!report.external_engine_invoked);

        let null_report =
            evaluate_expression(&expression, &row(&[("event_date", ScalarValue::Null)]));
        assert_eq!(null_report.status, ExpressionEvaluationStatus::Evaluated);
        assert_eq!(null_report.value, Some(ScalarValue::Null));
        assert_eq!(null_report.output_dtype, Some(LogicalDType::Date32));
        assert_eq!(null_report.null_behavior, NullBehavior::NullPropagating);
        assert!(!null_report.fallback_attempted);
        assert!(!null_report.external_engine_invoked);
    }

    #[test]
    fn expression_semantics_evaluates_coalesce_without_fallback() {
        let expression = Expression::new(
            expr_id("coalesce"),
            ExpressionKind::FunctionCall {
                name: "coalesce".to_string(),
                args: vec![
                    Expression::column(expr_id("label"), col("label")),
                    Expression::literal(
                        expr_id("fallback"),
                        ScalarValue::Utf8("unknown".to_string()),
                    ),
                ],
            },
        );
        let present = evaluate_expression(
            &expression,
            &row(&[("label", ScalarValue::Utf8("alpha".to_string()))]),
        );
        assert_eq!(present.status, ExpressionEvaluationStatus::Evaluated);
        assert_eq!(present.operator_family, "null_coalesce");
        assert_eq!(present.value, Some(ScalarValue::Utf8("alpha".to_string())));
        assert_eq!(present.output_dtype, Some(LogicalDType::Utf8));
        assert!(!present.fallback_attempted);
        assert!(!present.external_engine_invoked);

        let missing = evaluate_expression(&expression, &row(&[("label", ScalarValue::Null)]));
        assert_eq!(missing.status, ExpressionEvaluationStatus::Evaluated);
        assert_eq!(
            missing.value,
            Some(ScalarValue::Utf8("unknown".to_string()))
        );
        assert_eq!(missing.output_dtype, Some(LogicalDType::Utf8));
        assert!(!missing.fallback_attempted);
        assert!(!missing.external_engine_invoked);
    }

    #[test]
    fn expression_semantics_evaluates_nullif_without_fallback() {
        let expression = Expression::new(
            expr_id("nullif"),
            ExpressionKind::FunctionCall {
                name: "nullif".to_string(),
                args: vec![
                    Expression::column(expr_id("label"), col("label")),
                    Expression::literal(
                        expr_id("sentinel"),
                        ScalarValue::Utf8("missing".to_string()),
                    ),
                ],
            },
        );
        let retained = evaluate_expression(
            &expression,
            &row(&[("label", ScalarValue::Utf8("alpha".to_string()))]),
        );
        assert_eq!(retained.status, ExpressionEvaluationStatus::Evaluated);
        assert_eq!(retained.operator_family, "nullif_projection");
        assert_eq!(retained.value, Some(ScalarValue::Utf8("alpha".to_string())));
        assert_eq!(retained.output_dtype, Some(LogicalDType::Utf8));
        assert!(!retained.fallback_attempted);
        assert!(!retained.external_engine_invoked);

        let nulled = evaluate_expression(
            &expression,
            &row(&[("label", ScalarValue::Utf8("missing".to_string()))]),
        );
        assert_eq!(nulled.status, ExpressionEvaluationStatus::Evaluated);
        assert_eq!(nulled.operator_family, "nullif_projection");
        assert_eq!(nulled.value, Some(ScalarValue::Null));
        assert_eq!(nulled.output_dtype, Some(LogicalDType::Utf8));
        assert!(!nulled.fallback_attempted);
        assert!(!nulled.external_engine_invoked);
    }

    #[test]
    fn expression_semantics_evaluates_case_when_without_fallback() {
        let expression = Expression::new(
            expr_id("case_when"),
            ExpressionKind::FunctionCall {
                name: "case_when".to_string(),
                args: vec![
                    Expression::new(
                        expr_id("predicate"),
                        ExpressionKind::Compare {
                            left: Box::new(Expression::column(expr_id("amount"), col("amount"))),
                            op: ComparisonOp::GtEq,
                            right: Box::new(Expression::literal(
                                expr_id("threshold"),
                                ScalarValue::Int64(10),
                            )),
                        },
                    ),
                    Expression::literal(expr_id("then"), ScalarValue::Utf8("large".to_string())),
                    Expression::literal(expr_id("else"), ScalarValue::Utf8("small".to_string())),
                ],
            },
        );
        let true_report =
            evaluate_expression(&expression, &row(&[("amount", ScalarValue::Int64(12))]));
        assert_eq!(true_report.status, ExpressionEvaluationStatus::Evaluated);
        assert_eq!(true_report.operator_family, "conditional_projection");
        assert_eq!(
            true_report.value,
            Some(ScalarValue::Utf8("large".to_string()))
        );
        assert_eq!(true_report.output_dtype, Some(LogicalDType::Utf8));
        assert!(!true_report.fallback_attempted);
        assert!(!true_report.external_engine_invoked);

        let false_report =
            evaluate_expression(&expression, &row(&[("amount", ScalarValue::Int64(4))]));
        assert_eq!(false_report.status, ExpressionEvaluationStatus::Evaluated);
        assert_eq!(
            false_report.value,
            Some(ScalarValue::Utf8("small".to_string()))
        );
        assert_eq!(false_report.output_dtype, Some(LogicalDType::Utf8));
        assert!(!false_report.fallback_attempted);
        assert!(!false_report.external_engine_invoked);

        let null_report = evaluate_expression(&expression, &row(&[("amount", ScalarValue::Null)]));
        assert_eq!(null_report.status, ExpressionEvaluationStatus::Evaluated);
        assert_eq!(
            null_report.value,
            Some(ScalarValue::Utf8("small".to_string()))
        );
        assert_eq!(null_report.output_dtype, Some(LogicalDType::Utf8));
        assert!(!null_report.fallback_attempted);
        assert!(!null_report.external_engine_invoked);
    }

    #[test]
    fn expression_semantics_blocks_unsupported_function_without_fallback() {
        let expression = Expression::new(
            expr_id("fn"),
            ExpressionKind::FunctionCall {
                name: "regexp_extract".to_string(),
                args: Vec::new(),
            },
        );
        let report = evaluate_expression(&expression, &ExpressionInputRow::new());

        assert_eq!(report.status, ExpressionEvaluationStatus::Unsupported);
        assert!(report.has_errors());
        assert!(!report.fallback_attempted);
        assert!(!report.external_engine_invoked);
        assert!(report.diagnostics.iter().all(|d| !d.fallback.attempted));
    }

    #[test]
    fn projection_semantics_preserves_aliases_and_values() {
        let projection = vec![
            Expression::new(
                expr_id("alias"),
                ExpressionKind::Alias {
                    expr: Box::new(Expression::column(expr_id("value"), col("value"))),
                    alias: "amount".to_string(),
                },
            ),
            Expression::literal(expr_id("flag"), ScalarValue::Boolean(true)),
        ];
        let report = evaluate_projection(&projection, &row(&[("value", ScalarValue::Int64(7))]));

        assert_eq!(report.schema_version, "shardloom.projection_semantics.v1");
        assert_eq!(report.status, ExpressionEvaluationStatus::Evaluated);
        assert_eq!(report.projected_columns.len(), 2);
        assert_eq!(report.projected_columns[0].name, "amount");
        assert_eq!(report.projected_columns[0].value, ScalarValue::Int64(7));
        assert_eq!(report.projected_columns[1].name, "flag");
        assert!(!report.data_decoded);
        assert!(report.data_materialized);
        assert!(!report.fallback_attempted);
        assert!(!report.external_engine_invoked);
    }

    #[test]
    fn filter_semantics_selects_true_rows_and_drops_null_predicates() {
        let predicate = Expression::new(
            expr_id("pred"),
            ExpressionKind::Compare {
                left: Box::new(Expression::column(expr_id("value"), col("value"))),
                op: ComparisonOp::Gt,
                right: Box::new(Expression::literal(expr_id("two"), ScalarValue::Int64(2))),
            },
        );
        let rows = vec![
            row(&[("value", ScalarValue::Int64(1))]),
            row(&[("value", ScalarValue::Int64(3))]),
            row(&[("value", ScalarValue::Null)]),
        ];
        let report = evaluate_filter(&predicate, &rows);

        assert_eq!(report.schema_version, "shardloom.filter_semantics.v1");
        assert_eq!(report.status, ExpressionEvaluationStatus::Evaluated);
        assert_eq!(report.input_row_count, 3);
        assert_eq!(report.selected_row_indexes, vec![1]);
        assert_eq!(report.selected_row_count(), 1);
        assert_eq!(report.null_predicate_row_count, 1);
        assert!(!report.data_decoded);
        assert!(report.data_materialized);
        assert!(!report.fallback_attempted);
        assert!(!report.external_engine_invoked);
    }

    #[test]
    fn filter_semantics_blocks_non_boolean_predicates() {
        let predicate = Expression::column(expr_id("value"), col("value"));
        let report = evaluate_filter(&predicate, &[row(&[("value", ScalarValue::Int64(1))])]);

        assert_eq!(report.status, ExpressionEvaluationStatus::Unsupported);
        assert!(report.has_errors());
        assert!(report.diagnostics.iter().all(|d| !d.fallback.attempted));
    }

    #[test]
    fn limit_semantics_caps_output_count_without_fallback() {
        let report = evaluate_limit(10, 3);

        assert_eq!(report.schema_version, "shardloom.limit_semantics.v1");
        assert_eq!(report.status, ExpressionEvaluationStatus::Evaluated);
        assert_eq!(report.output_row_count, 3);
        assert!(!report.data_decoded);
        assert!(!report.data_materialized);
        assert!(!report.fallback_attempted);
        assert!(!report.external_engine_invoked);
        assert!(!report.fallback_execution_allowed());
    }
}
