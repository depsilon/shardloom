//! Expression and kernel registry domain skeleton.
//!
//! This module defines native `ShardLoom` domain types for expression modeling,
//! kernel capability metadata, and deterministic no-fallback selection results.
//! It intentionally does not perform expression evaluation or kernel execution.

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
                ExpressionKind::Unary { op, .. } => format!("unary({})", op.as_str()),
                ExpressionKind::Binary { op, .. } => format!("binary({})", op.as_str()),
                ExpressionKind::Compare { op, .. } => format!("compare({})", op.as_str()),
                ExpressionKind::FunctionCall { name, args } => format!("fn({name}/{})", args.len()),
                ExpressionKind::Unsupported { feature, .. } => format!("unsupported({feature})"),
            }
        )
    }
}

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
        self.supported_dtypes.is_empty() || self.supported_dtypes.contains(dtype)
    }
    #[must_use]
    pub fn supports_encoding(&self, encoding: &EncodingKind) -> bool {
        self.supported_encodings.is_empty() || self.supported_encodings.contains(encoding)
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
}
