/// Severity level for structured diagnostics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiagnosticSeverity {
    Info,
    Warning,
    Error,
    Fatal,
}

impl DiagnosticSeverity {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Info => "info",
            Self::Warning => "warning",
            Self::Error => "error",
            Self::Fatal => "fatal",
        }
    }
}

/// Domain category for diagnostics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiagnosticCategory {
    UnsupportedFeature,
    InvalidInput,
    Configuration,
    Planning,
    Execution,
    VortexIo,
    Statistics,
    Pruning,
    Materialization,
    Translation,
    MetadataLoss,
    ObjectStore,
    ResourceBudget,
    ExternalEffect,
    ModelCall,
    ApiCall,
    Embedding,
    VectorSearch,
    NoFallbackPolicy,
}

impl DiagnosticCategory {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::UnsupportedFeature => "unsupported_feature",
            Self::InvalidInput => "invalid_input",
            Self::Configuration => "configuration",
            Self::Planning => "planning",
            Self::Execution => "execution",
            Self::VortexIo => "vortex_io",
            Self::Statistics => "statistics",
            Self::Pruning => "pruning",
            Self::Materialization => "materialization",
            Self::Translation => "translation",
            Self::MetadataLoss => "metadata_loss",
            Self::ObjectStore => "object_store",
            Self::ResourceBudget => "resource_budget",
            Self::ExternalEffect => "external_effect",
            Self::ModelCall => "model_call",
            Self::ApiCall => "api_call",
            Self::Embedding => "embedding",
            Self::VectorSearch => "vector_search",
            Self::NoFallbackPolicy => "no_fallback_policy",
        }
    }
}

/// Stable diagnostic code for machine-readable behavior.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiagnosticCode {
    UnsupportedEncoding,
    UnsupportedDType,
    UnsupportedSql,
    UnsupportedUdf,
    UnsupportedEffect,
    UnsupportedOutputFormat,
    MissingStatistics,
    PruningInconclusive,
    MetadataLoss,
    MaterializationRequired,
    ExternalEffectDisabled,
    LlmCallDisabled,
    ApiCallDisabled,
    EmbeddingModelUnconfigured,
    VectorIndexUnavailable,
    ObjectStoreUnsupported,
    CommitNotAtomic,
    ResourceBudgetExceeded,
    NoFallbackExecution,
}

impl DiagnosticCode {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::UnsupportedEncoding => "SL_UNSUPPORTED_ENCODING",
            Self::UnsupportedDType => "SL_UNSUPPORTED_DTYPE",
            Self::UnsupportedSql => "SL_UNSUPPORTED_SQL",
            Self::UnsupportedUdf => "SL_UNSUPPORTED_UDF",
            Self::UnsupportedEffect => "SL_UNSUPPORTED_EFFECT",
            Self::UnsupportedOutputFormat => "SL_UNSUPPORTED_OUTPUT_FORMAT",
            Self::MissingStatistics => "SL_MISSING_STATISTICS",
            Self::PruningInconclusive => "SL_PRUNING_INCONCLUSIVE",
            Self::MetadataLoss => "SL_METADATA_LOSS",
            Self::MaterializationRequired => "SL_MATERIALIZATION_REQUIRED",
            Self::ExternalEffectDisabled => "SL_EXTERNAL_EFFECT_DISABLED",
            Self::LlmCallDisabled => "SL_LLM_CALL_DISABLED",
            Self::ApiCallDisabled => "SL_API_CALL_DISABLED",
            Self::EmbeddingModelUnconfigured => "SL_EMBEDDING_MODEL_UNCONFIGURED",
            Self::VectorIndexUnavailable => "SL_VECTOR_INDEX_UNAVAILABLE",
            Self::ObjectStoreUnsupported => "SL_OBJECT_STORE_UNSUPPORTED",
            Self::CommitNotAtomic => "SL_COMMIT_NOT_ATOMIC",
            Self::ResourceBudgetExceeded => "SL_RESOURCE_BUDGET_EXCEEDED",
            Self::NoFallbackExecution => "SL_NO_FALLBACK_EXECUTION",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FallbackStatus {
    pub attempted: bool,
    pub allowed: bool,
    pub engine: Option<String>,
    pub reason: String,
}

impl FallbackStatus {
    #[must_use]
    pub fn disabled_by_policy() -> Self {
        Self {
            attempted: false,
            allowed: false,
            engine: None,
            reason: "ShardLoom prohibits Spark, DataFusion, DuckDB, Polars, Velox, and other fallback execution engines.".to_string(),
        }
    }

    #[must_use]
    pub fn to_json(&self) -> String {
        format!(
            "{{\"attempted\":{},\"allowed\":{},\"engine\":{},\"reason\":{}}}",
            crate::output::json_bool(self.attempted),
            crate::output::json_bool(self.allowed),
            self.engine
                .as_ref()
                .map_or_else(|| "null".to_string(), |e| crate::output::json_string(e)),
            crate::output::json_string(&self.reason)
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Diagnostic {
    pub code: DiagnosticCode,
    pub severity: DiagnosticSeverity,
    pub category: DiagnosticCategory,
    pub message: String,
    pub feature: Option<String>,
    pub reason: Option<String>,
    pub suggested_next_step: Option<String>,
    pub fallback: FallbackStatus,
}

impl Diagnostic {
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        code: DiagnosticCode,
        severity: DiagnosticSeverity,
        category: DiagnosticCategory,
        message: impl Into<String>,
        feature: Option<String>,
        reason: Option<String>,
        suggested_next_step: Option<String>,
        fallback: FallbackStatus,
    ) -> Self {
        Self {
            code,
            severity,
            category,
            message: message.into(),
            feature,
            reason,
            suggested_next_step,
            fallback,
        }
    }

    #[must_use]
    pub fn unsupported(
        code: DiagnosticCode,
        feature: impl Into<String>,
        message: impl Into<String>,
        suggested_next_step: Option<String>,
    ) -> Self {
        Self::new(
            code,
            DiagnosticSeverity::Error,
            DiagnosticCategory::UnsupportedFeature,
            message,
            Some(feature.into()),
            Some("Feature is not yet implemented for native ShardLoom execution.".to_string()),
            suggested_next_step,
            FallbackStatus::disabled_by_policy(),
        )
    }

    #[must_use]
    pub fn no_fallback_execution(message: impl Into<String>) -> Self {
        Self::new(
            DiagnosticCode::NoFallbackExecution,
            DiagnosticSeverity::Error,
            DiagnosticCategory::NoFallbackPolicy,
            message,
            None,
            Some("Fallback execution is disabled by project policy.".to_string()),
            Some("Adjust the query or wait for native support in ShardLoom.".to_string()),
            FallbackStatus::disabled_by_policy(),
        )
    }

    #[must_use]
    pub fn to_human_text(&self) -> String {
        format!(
            "[{severity}] {code} ({category}): {message}",
            severity = self.severity.as_str(),
            code = self.code.as_str(),
            category = self.category.as_str(),
            message = self.message
        )
    }

    #[must_use]
    pub fn to_json(&self) -> String {
        format!(
            "{{\"code\":{},\"severity\":{},\"category\":{},\"message\":{},\"feature\":{},\"reason\":{},\"suggested_next_step\":{},\"fallback\":{}}}",
            crate::output::json_string(self.code.as_str()),
            crate::output::json_string(self.severity.as_str()),
            crate::output::json_string(self.category.as_str()),
            crate::output::json_string(&self.message),
            self.feature
                .as_ref()
                .map_or_else(|| "null".to_string(), |v| crate::output::json_string(v)),
            self.reason
                .as_ref()
                .map_or_else(|| "null".to_string(), |v| crate::output::json_string(v)),
            self.suggested_next_step
                .as_ref()
                .map_or_else(|| "null".to_string(), |v| crate::output::json_string(v)),
            self.fallback.to_json(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fallback_disabled_by_policy() {
        let fallback = FallbackStatus::disabled_by_policy();
        assert!(!fallback.attempted);
        assert!(!fallback.allowed);
        assert!(fallback.engine.is_none());
    }

    #[test]
    fn diagnostic_code_string_stability() {
        assert_eq!(
            DiagnosticCode::NoFallbackExecution.as_str(),
            "SL_NO_FALLBACK_EXECUTION"
        );
    }

    #[test]
    fn unsupported_has_fallback_attempted_false() {
        let diagnostic = Diagnostic::unsupported(
            DiagnosticCode::UnsupportedSql,
            "window_function",
            "Window functions are not supported yet.",
            None,
        );
        assert!(!diagnostic.fallback.attempted);
    }

    #[test]
    fn no_fallback_execution_sets_expected_category_and_code() {
        let diagnostic = Diagnostic::no_fallback_execution("No fallback will be attempted.");
        assert_eq!(diagnostic.category, DiagnosticCategory::NoFallbackPolicy);
        assert_eq!(diagnostic.code, DiagnosticCode::NoFallbackExecution);
    }

    #[test]
    fn human_text_contains_code_and_message() {
        let diagnostic = Diagnostic::no_fallback_execution("No fallback will be attempted.");
        let text = diagnostic.to_human_text();
        assert!(text.contains("SL_NO_FALLBACK_EXECUTION"));
        assert!(text.contains("No fallback will be attempted."));
    }

    #[test]
    fn diagnostic_to_json_includes_code() {
        let diagnostic = Diagnostic::no_fallback_execution("No fallback will be attempted.");
        let json = diagnostic.to_json();
        assert!(json.contains("SL_NO_FALLBACK_EXECUTION"));
    }

    #[test]
    fn diagnostic_to_json_unsupported_has_fallback_attempted_false() {
        let diagnostic = Diagnostic::unsupported(
            DiagnosticCode::UnsupportedSql,
            "window_function",
            "Window functions are not supported yet.",
            None,
        );
        let json = diagnostic.to_json();
        assert!(json.contains("\"attempted\":false"));
    }
}
