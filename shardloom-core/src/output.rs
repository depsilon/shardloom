//! Machine-readable CLI output envelope.
//!
//! Text rendering is for humans, JSON rendering is for agents/automation.
//! This module only renders output metadata and diagnostics; it does not execute work.

use crate::{Diagnostic, DiagnosticSeverity, FallbackStatus, Result, ShardLoomError};
use std::fmt::Write as _;

/// Output rendering format.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputFormat {
    Text,
    Json,
}

impl OutputFormat {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Text => "text",
            Self::Json => "json",
        }
    }

    /// Parses CLI output format.
    ///
    /// # Errors
    ///
    /// Returns [`ShardLoomError::InvalidOperation`] for unsupported values.
    pub fn parse(value: &str) -> Result<Self> {
        match value.to_ascii_lowercase().as_str() {
            "text" => Ok(Self::Text),
            "json" => Ok(Self::Json),
            _ => Err(ShardLoomError::InvalidOperation(format!(
                "unsupported output format: {value}"
            ))),
        }
    }
}

/// Top-level command status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommandStatus {
    Success,
    Warning,
    Error,
    Unsupported,
}

impl CommandStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Warning => "warning",
            Self::Error => "error",
            Self::Unsupported => "unsupported",
        }
    }

    #[must_use]
    pub const fn is_error(&self) -> bool {
        matches!(self, Self::Error | Self::Unsupported)
    }
}

/// Stable command envelope for machine-readable CLI output.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutputEnvelope {
    pub schema_version: &'static str,
    pub command: String,
    pub status: CommandStatus,
    pub summary: String,
    pub human_text: String,
    pub fallback: FallbackStatus,
    pub diagnostics: Vec<Diagnostic>,
    pub fields: Vec<(String, String)>,
}

impl OutputEnvelope {
    #[must_use]
    pub fn new(
        command: impl Into<String>,
        status: CommandStatus,
        summary: impl Into<String>,
        human_text: impl Into<String>,
    ) -> Self {
        Self {
            schema_version: "shardloom.output.v1",
            command: command.into(),
            status,
            summary: summary.into(),
            human_text: human_text.into(),
            fallback: FallbackStatus::disabled_by_policy(),
            diagnostics: Vec::new(),
            fields: Vec::new(),
        }
    }

    #[must_use]
    pub fn success(
        command: impl Into<String>,
        summary: impl Into<String>,
        human_text: impl Into<String>,
    ) -> Self {
        Self::new(command, CommandStatus::Success, summary, human_text)
    }

    #[must_use]
    pub fn unsupported(
        command: impl Into<String>,
        summary: impl Into<String>,
        human_text: impl Into<String>,
    ) -> Self {
        Self::new(command, CommandStatus::Unsupported, summary, human_text)
    }

    #[must_use]
    pub fn with_field(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.fields.push((key.into(), value.into()));
        self
    }

    #[must_use]
    pub fn with_diagnostic(mut self, diagnostic: Diagnostic) -> Self {
        self.diagnostics.push(diagnostic);
        self
    }

    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
    }

    /// Builds an envelope from a diagnostic while preserving structured fallback status.
    ///
    /// This is intended for deterministic agent-facing JSON and human-facing text output
    /// without adding serialization dependencies.
    #[must_use]
    pub fn from_diagnostic(
        command: impl Into<String>,
        summary: impl Into<String>,
        human_text: impl Into<String>,
        diagnostic: Diagnostic,
    ) -> Self {
        let status = if matches!(
            diagnostic.category,
            crate::DiagnosticCategory::UnsupportedFeature
                | crate::DiagnosticCategory::NoFallbackPolicy
        ) {
            CommandStatus::Unsupported
        } else {
            match diagnostic.severity {
                DiagnosticSeverity::Fatal | DiagnosticSeverity::Error => CommandStatus::Error,
                DiagnosticSeverity::Warning => CommandStatus::Warning,
                DiagnosticSeverity::Info => CommandStatus::Success,
            }
        };
        let fallback = diagnostic.fallback.clone();
        let mut envelope =
            Self::new(command, status, summary, human_text).with_diagnostic(diagnostic);
        envelope.fallback = fallback;
        envelope
    }

    /// Builds an envelope from a plain error by converting it to a structured diagnostic.
    ///
    /// Plain errors should be normalized before user-facing rendering so text/json output
    /// remains stable for both humans and agents.
    #[must_use]
    pub fn from_error(
        command: impl Into<String>,
        summary: impl Into<String>,
        error: &ShardLoomError,
    ) -> Self {
        let diagnostic = error.to_diagnostic();
        Self::from_diagnostic(command, summary, error.to_string(), diagnostic)
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
    pub fn to_text(&self) -> String {
        self.human_text.clone()
    }

    #[must_use]
    pub fn to_json(&self) -> String {
        let diagnostics = self
            .diagnostics
            .iter()
            .map(Diagnostic::to_json)
            .collect::<Vec<_>>()
            .join(",");
        let fields = self
            .fields
            .iter()
            .map(|(k, v)| {
                format!(
                    "{{\"key\":{},\"value\":{}}}",
                    json_string(k),
                    json_string(v)
                )
            })
            .collect::<Vec<_>>()
            .join(",");
        format!(
            "{{\"schema_version\":{},\"command\":{},\"status\":{},\"summary\":{},\"human_text\":{},\"fallback\":{},\"diagnostics\":[{}],\"fields\":[{}]}}",
            json_string(self.schema_version),
            json_string(&self.command),
            json_string(self.status.as_str()),
            json_string(&self.summary),
            json_string(&self.human_text),
            self.fallback.to_json(),
            diagnostics,
            fields,
        )
    }

    #[must_use]
    pub fn render(&self, format: OutputFormat) -> String {
        match format {
            OutputFormat::Text => self.to_text(),
            OutputFormat::Json => self.to_json(),
        }
    }
}

/// Report-only CG-11 contract for the stable CLI/API JSON protocol foundation.
///
/// This is the protocol surface that a future thin Python wrapper or other
/// client can consume before native bindings exist. It describes the existing
/// [`OutputEnvelope`] JSON shape without executing commands, probing local
/// state, publishing packages, or enabling fallback execution.
#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CliApiJsonProtocolReport {
    pub schema_version: &'static str,
    pub protocol_id: &'static str,
    pub protocol_stability: &'static str,
    pub output_envelope_schema_version: &'static str,
    pub required_envelope_fields: Vec<&'static str>,
    pub required_fallback_fields: Vec<&'static str>,
    pub required_diagnostic_fields: Vec<&'static str>,
    pub required_field_entry_fields: Vec<&'static str>,
    pub command_status_values: Vec<&'static str>,
    pub output_formats: Vec<&'static str>,
    pub thin_python_wrapper_boundary: &'static str,
    pub pyo3_maturin_allowed: bool,
    pub foundry_required: bool,
    pub dataframe_api_implemented: bool,
    pub side_effect_free: bool,
    pub filesystem_probe: bool,
    pub network_probe: bool,
    pub catalog_probe: bool,
    pub adapter_probe: bool,
    pub parser_executed: bool,
    pub runtime_execution: bool,
    pub write_io: bool,
    pub external_publish: bool,
    pub fallback_execution_allowed: bool,
    pub fallback_attempted: bool,
    pub diagnostics: Vec<Diagnostic>,
}

/// Report-only CG-11 contract for the first thin Python wrapper boundary.
///
/// The foundation wrapper is a future client over the CLI JSON protocol, not a
/// native Python binding, package publication, `DataFrame` implementation, UDF
/// runtime, or hidden fallback execution path.
#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PythonWrapperFoundationReport {
    pub schema_version: &'static str,
    pub wrapper_id: &'static str,
    pub wrapper_status: &'static str,
    pub transport_protocol_id: &'static str,
    pub output_envelope_schema_version: &'static str,
    pub invocation_model: &'static str,
    pub initial_command_scope: Vec<&'static str>,
    pub required_client_behaviors: Vec<&'static str>,
    pub package_status: &'static str,
    pub native_binding_status: &'static str,
    pub pyo3_maturin_allowed: bool,
    pub python_package_created: bool,
    pub native_extension_required: bool,
    pub dataframe_api_implemented: bool,
    pub notebook_api_implemented: bool,
    pub python_udf_runtime_implemented: bool,
    pub materialization_boundary_reporting_required: bool,
    pub diagnostics_passthrough_required: bool,
    pub side_effect_free: bool,
    pub filesystem_probe: bool,
    pub network_probe: bool,
    pub catalog_probe: bool,
    pub adapter_probe: bool,
    pub parser_executed: bool,
    pub runtime_execution: bool,
    pub write_io: bool,
    pub external_publish: bool,
    pub fallback_execution_allowed: bool,
    pub fallback_attempted: bool,
    pub diagnostics: Vec<Diagnostic>,
}

impl PythonWrapperFoundationReport {
    /// Builds the static Python wrapper foundation report without creating
    /// Python files, invoking Python, probing the host, or executing runtime work.
    #[must_use]
    pub fn contract_only() -> Self {
        Self {
            schema_version: "shardloom.python_wrapper_foundation.v1",
            wrapper_id: "shardloom_python_cli_json_client",
            wrapper_status: "planned_foundation",
            transport_protocol_id: "shardloom.cli_json.v1",
            output_envelope_schema_version: "shardloom.output.v1",
            invocation_model: "subprocess_cli_json",
            initial_command_scope: vec![
                "status",
                "capabilities",
                "api-compat-plan",
                "python-wrapper-plan",
            ],
            required_client_behaviors: vec![
                "invoke_shardloom_with_format_json",
                "parse_output_envelope",
                "preserve_diagnostics",
                "preserve_fallback_status",
                "surface_materialization_boundaries",
                "do_not_retry_as_fallback_engine",
            ],
            package_status: "not_created",
            native_binding_status: "not_created",
            pyo3_maturin_allowed: false,
            python_package_created: false,
            native_extension_required: false,
            dataframe_api_implemented: false,
            notebook_api_implemented: false,
            python_udf_runtime_implemented: false,
            materialization_boundary_reporting_required: true,
            diagnostics_passthrough_required: true,
            side_effect_free: true,
            filesystem_probe: false,
            network_probe: false,
            catalog_probe: false,
            adapter_probe: false,
            parser_executed: false,
            runtime_execution: false,
            write_io: false,
            external_publish: false,
            fallback_execution_allowed: false,
            fallback_attempted: false,
            diagnostics: Vec::new(),
        }
    }

    #[must_use]
    pub fn status(&self) -> CommandStatus {
        if self.has_errors() {
            CommandStatus::Error
        } else {
            CommandStatus::Success
        }
    }

    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.python_package_created
            || self.pyo3_maturin_allowed
            || self.native_extension_required
            || self.dataframe_api_implemented
            || self.notebook_api_implemented
            || self.python_udf_runtime_implemented
            || self.runtime_execution
            || self.write_io
            || self.external_publish
            || self.fallback_execution_allowed
            || self.fallback_attempted
            || self.diagnostics.iter().any(|diagnostic| {
                matches!(
                    diagnostic.severity,
                    DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
                )
            })
    }

    #[must_use]
    pub fn to_human_text(&self) -> String {
        format!(
            "python wrapper foundation\nschema_version: {}\nwrapper_id: {}\nwrapper_status: {}\ntransport_protocol_id: {}\noutput_envelope_schema_version: {}\ninvocation_model: {}\ninitial command scope: {}\nrequired client behaviors: {}\npackage_status: {}\nnative_binding_status: {}\npyo3/maturin allowed: {}\npython package created: {}\nruntime execution: disabled\nwrite io: disabled\nfallback execution: disabled",
            self.schema_version,
            self.wrapper_id,
            self.wrapper_status,
            self.transport_protocol_id,
            self.output_envelope_schema_version,
            self.invocation_model,
            self.initial_command_scope.join(", "),
            self.required_client_behaviors.join(", "),
            self.package_status,
            self.native_binding_status,
            self.pyo3_maturin_allowed,
            self.python_package_created,
        )
    }
}

impl CliApiJsonProtocolReport {
    /// Builds the static protocol-foundation report without any probing.
    #[must_use]
    pub fn contract_only() -> Self {
        Self {
            schema_version: "shardloom.cli_api_json_protocol.v1",
            protocol_id: "shardloom.cli_json.v1",
            protocol_stability: "experimental",
            output_envelope_schema_version: "shardloom.output.v1",
            required_envelope_fields: vec![
                "schema_version",
                "command",
                "status",
                "summary",
                "human_text",
                "fallback",
                "diagnostics",
                "fields",
            ],
            required_fallback_fields: vec!["attempted", "allowed", "engine", "reason"],
            required_diagnostic_fields: vec![
                "code",
                "severity",
                "category",
                "message",
                "feature",
                "reason",
                "suggested_next_step",
                "fallback",
            ],
            required_field_entry_fields: vec!["key", "value"],
            command_status_values: vec!["success", "warning", "error", "unsupported"],
            output_formats: vec!["text", "json"],
            thin_python_wrapper_boundary: "cli_json_subprocess_first",
            pyo3_maturin_allowed: false,
            foundry_required: false,
            dataframe_api_implemented: false,
            side_effect_free: true,
            filesystem_probe: false,
            network_probe: false,
            catalog_probe: false,
            adapter_probe: false,
            parser_executed: false,
            runtime_execution: false,
            write_io: false,
            external_publish: false,
            fallback_execution_allowed: false,
            fallback_attempted: false,
            diagnostics: Vec::new(),
        }
    }

    #[must_use]
    pub fn status(&self) -> CommandStatus {
        if self.has_errors() {
            CommandStatus::Error
        } else {
            CommandStatus::Success
        }
    }

    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.fallback_execution_allowed
            || self.fallback_attempted
            || self.runtime_execution
            || self.write_io
            || self.external_publish
            || self.diagnostics.iter().any(|diagnostic| {
                matches!(
                    diagnostic.severity,
                    DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
                )
            })
    }

    #[must_use]
    pub fn to_human_text(&self) -> String {
        format!(
            "cli/api json protocol\nschema_version: {}\nprotocol_id: {}\nprotocol_stability: {}\noutput_envelope_schema_version: {}\nrequired envelope fields: {}\ncommand statuses: {}\noutput formats: {}\npython wrapper boundary: {}\npyo3/maturin allowed: {}\nfoundry required: {}\nruntime execution: disabled\nwrite io: disabled\nfallback execution: disabled",
            self.schema_version,
            self.protocol_id,
            self.protocol_stability,
            self.output_envelope_schema_version,
            self.required_envelope_fields.join(", "),
            self.command_status_values.join(", "),
            self.output_formats.join(", "),
            self.thin_python_wrapper_boundary,
            self.pyo3_maturin_allowed,
            self.foundry_required,
        )
    }
}

pub(crate) fn json_escape(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '\\' => escaped.push_str("\\\\"),
            '"' => escaped.push_str("\\\""),
            '\n' => escaped.push_str("\\n"),
            '\r' => escaped.push_str("\\r"),
            '\t' => escaped.push_str("\\t"),
            '\u{08}' => escaped.push_str("\\b"),
            '\u{0C}' => escaped.push_str("\\f"),
            c if c.is_control() => {
                let _ = write!(&mut escaped, "\\u{:04X}", c as u32);
            }
            c => escaped.push(c),
        }
    }
    escaped
}

pub(crate) fn json_string(value: &str) -> String {
    format!("\"{}\"", json_escape(value))
}

#[must_use]
pub(crate) const fn json_bool(value: bool) -> &'static str {
    if value { "true" } else { "false" }
}

#[cfg(test)]
mod tests {
    use crate::{Diagnostic, DiagnosticCode};

    use super::*;

    #[test]
    fn format_parses_text() {
        assert_eq!(OutputFormat::parse("text").expect("ok"), OutputFormat::Text);
    }
    #[test]
    fn format_parses_json() {
        assert_eq!(OutputFormat::parse("json").expect("ok"), OutputFormat::Json);
    }
    #[test]
    fn format_parsing_case_insensitive() {
        assert_eq!(OutputFormat::parse("JsOn").expect("ok"), OutputFormat::Json);
    }
    #[test]
    fn format_rejects_unknown() {
        assert!(OutputFormat::parse("yaml").is_err());
    }
    #[test]
    fn command_status_error_is_error() {
        assert!(CommandStatus::Error.is_error());
    }
    #[test]
    fn command_status_unsupported_is_error() {
        assert!(CommandStatus::Unsupported.is_error());
    }
    #[test]
    fn json_escape_escapes_quotes() {
        assert_eq!(json_escape("\""), "\\\"");
    }
    #[test]
    fn json_escape_escapes_backslashes() {
        assert_eq!(json_escape("\\"), "\\\\");
    }
    #[test]
    fn json_escape_escapes_newline() {
        assert_eq!(json_escape("\n"), "\\n");
    }
    #[test]
    fn json_escape_escapes_other_control_chars() {
        assert_eq!(json_escape("\u{0000}"), "\\u0000");
        assert_eq!(json_escape("\u{0008}"), "\\b");
        assert_eq!(json_escape("\u{000C}"), "\\f");
    }
    #[test]
    fn success_defaults_fallback_false() {
        assert!(
            !OutputEnvelope::success("status", "ok", "ok")
                .fallback
                .allowed
        );
    }
    #[test]
    fn has_errors_false_for_success_without_diagnostics() {
        assert!(!OutputEnvelope::success("status", "ok", "ok").has_errors());
    }
    #[test]
    fn unsupported_has_errors() {
        assert!(OutputEnvelope::unsupported("explain", "unsupported", "unsupported").has_errors());
    }
    #[test]
    fn to_json_includes_schema_version() {
        assert!(
            OutputEnvelope::success("status", "ok", "ok")
                .to_json()
                .contains("\"schema_version\"")
        );
    }
    #[test]
    fn to_json_includes_fallback() {
        assert!(
            OutputEnvelope::success("status", "ok", "ok")
                .to_json()
                .contains("\"fallback\"")
        );
    }
    #[test]
    fn to_json_includes_diagnostics_array() {
        assert!(
            OutputEnvelope::success("status", "ok", "ok")
                .to_json()
                .contains("\"diagnostics\":[")
        );
    }
    #[test]
    fn render_text_returns_text() {
        assert_eq!(
            OutputEnvelope::success("status", "ok", "hello").render(OutputFormat::Text),
            "hello"
        );
    }
    #[test]
    fn render_json_looks_like_json() {
        assert!(
            OutputEnvelope::success("status", "ok", "hello")
                .render(OutputFormat::Json)
                .starts_with('{')
        );
    }
    #[test]
    fn has_errors_true_for_error_diagnostic() {
        let envelope = OutputEnvelope::success("status", "ok", "ok").with_diagnostic(
            Diagnostic::unsupported(DiagnosticCode::UnsupportedSql, "sql", "unsupported", None),
        );
        assert!(envelope.has_errors());
    }

    #[test]
    fn from_diagnostic_unsupported_feature_sets_unsupported_status() {
        let diagnostic =
            Diagnostic::unsupported(DiagnosticCode::UnsupportedSql, "sql", "unsupported", None);
        let envelope =
            OutputEnvelope::from_diagnostic("scan-plan", "unsupported", "unsupported", diagnostic);
        assert_eq!(envelope.status, CommandStatus::Unsupported);
    }

    #[test]
    fn from_diagnostic_no_fallback_policy_sets_unsupported_status() {
        let diagnostic = Diagnostic::new(
            DiagnosticCode::NoFallbackExecution,
            DiagnosticSeverity::Error,
            crate::DiagnosticCategory::NoFallbackPolicy,
            "fallback disabled",
            None,
            None,
            None,
            FallbackStatus::disabled_by_policy(),
        );
        let envelope = OutputEnvelope::from_diagnostic(
            "scan-plan",
            "fallback disabled",
            "fallback disabled",
            diagnostic,
        );
        assert_eq!(envelope.status, CommandStatus::Unsupported);
    }

    #[test]
    fn from_diagnostic_error_severity_sets_error_status_for_supported_category() {
        let diagnostic = Diagnostic::new(
            DiagnosticCode::ConfigurationError,
            DiagnosticSeverity::Error,
            crate::DiagnosticCategory::Configuration,
            "config error",
            None,
            None,
            None,
            FallbackStatus::disabled_by_policy(),
        );
        let envelope = OutputEnvelope::from_diagnostic("scan-plan", "config", "config", diagnostic);
        assert_eq!(envelope.status, CommandStatus::Error);
    }

    #[test]
    fn from_diagnostic_fatal_severity_sets_error_status_for_supported_category() {
        let diagnostic = Diagnostic::new(
            DiagnosticCode::NotImplemented,
            DiagnosticSeverity::Fatal,
            crate::DiagnosticCategory::Execution,
            "execution failed",
            None,
            None,
            None,
            FallbackStatus::disabled_by_policy(),
        );
        let envelope = OutputEnvelope::from_diagnostic("scan-plan", "failed", "failed", diagnostic);
        assert_eq!(envelope.status, CommandStatus::Error);
    }

    #[test]
    fn from_diagnostic_warning_severity_sets_warning_status() {
        let diagnostic = Diagnostic::new(
            DiagnosticCode::MetadataLoss,
            DiagnosticSeverity::Warning,
            crate::DiagnosticCategory::MetadataLoss,
            "metadata loss",
            None,
            None,
            None,
            FallbackStatus::disabled_by_policy(),
        );
        let envelope =
            OutputEnvelope::from_diagnostic("scan-plan", "warning", "warning", diagnostic);
        assert_eq!(envelope.status, CommandStatus::Warning);
    }

    #[test]
    fn from_diagnostic_info_severity_sets_success_status() {
        let diagnostic = Diagnostic::new(
            DiagnosticCode::MissingStatistics,
            DiagnosticSeverity::Info,
            crate::DiagnosticCategory::Planning,
            "planning",
            None,
            None,
            None,
            FallbackStatus::disabled_by_policy(),
        );
        let envelope = OutputEnvelope::from_diagnostic("scan-plan", "ok", "ok", diagnostic);
        assert_eq!(envelope.status, CommandStatus::Success);
    }

    #[test]
    fn from_error_invalid_operation_normalizes_invalid_input_and_fallback_disabled() {
        let envelope = OutputEnvelope::from_error(
            "scan-plan",
            "bad input",
            &ShardLoomError::InvalidOperation("bad".to_string()),
        );
        assert_eq!(envelope.status, CommandStatus::Error);
        let diagnostic = &envelope.diagnostics[0];
        assert_eq!(diagnostic.code, DiagnosticCode::InvalidInput);
        assert_eq!(diagnostic.category, crate::DiagnosticCategory::InvalidInput);
        assert!(!envelope.fallback.attempted);
        assert!(!envelope.fallback.allowed);
    }

    #[test]
    fn has_errors_true_for_unsupported_status_even_with_info_diagnostic() {
        let envelope = OutputEnvelope::new(
            "status",
            CommandStatus::Unsupported,
            "unsupported",
            "unsupported",
        )
        .with_diagnostic(Diagnostic::new(
            DiagnosticCode::MissingStatistics,
            DiagnosticSeverity::Info,
            crate::DiagnosticCategory::Planning,
            "info",
            None,
            None,
            None,
            FallbackStatus::disabled_by_policy(),
        ));
        assert!(envelope.has_errors());
    }

    #[test]
    fn to_json_includes_status_field() {
        assert!(
            OutputEnvelope::success("status", "ok", "ok")
                .to_json()
                .contains("\"status\"")
        );
    }

    #[test]
    fn from_diagnostic_includes_diagnostic() {
        let diagnostic = Diagnostic::invalid_input("dataset_uri", "invalid", "fix");
        let envelope =
            OutputEnvelope::from_diagnostic("scan-plan", "bad input", "bad input", diagnostic);
        assert_eq!(envelope.diagnostics.len(), 1);
    }
    #[test]
    fn from_diagnostic_copies_fallback_status() {
        let diagnostic = Diagnostic::new(
            DiagnosticCode::UnsupportedSql,
            DiagnosticSeverity::Error,
            crate::DiagnosticCategory::UnsupportedFeature,
            "unsupported",
            None,
            None,
            None,
            FallbackStatus {
                attempted: true,
                allowed: true,
                engine: Some("legacy".to_string()),
                reason: "test".to_string(),
            },
        );
        let envelope =
            OutputEnvelope::from_diagnostic("scan-plan", "bad input", "bad input", diagnostic);
        assert!(envelope.fallback.allowed);
        assert!(envelope.fallback.attempted);
    }

    #[test]
    fn from_error_includes_diagnostic() {
        let envelope = OutputEnvelope::from_error(
            "scan-plan",
            "bad input",
            &ShardLoomError::InvalidOperation("bad".to_string()),
        );
        assert_eq!(envelope.diagnostics.len(), 1);
    }

    #[test]
    fn from_error_json_has_fallback_attempted_false() {
        let envelope = OutputEnvelope::from_error(
            "scan-plan",
            "bad input",
            &ShardLoomError::InvalidOperation("bad".to_string()),
        );
        assert!(envelope.to_json().contains("\"attempted\":false"));
    }

    #[test]
    fn cli_api_json_protocol_is_report_only() {
        let report = CliApiJsonProtocolReport::contract_only();
        assert_eq!(report.output_envelope_schema_version, "shardloom.output.v1");
        assert!(report.required_envelope_fields.contains(&"fallback"));
        assert!(report.required_diagnostic_fields.contains(&"code"));
        assert!(!report.fallback_execution_allowed);
        assert!(!report.fallback_attempted);
        assert!(!report.runtime_execution);
        assert!(!report.write_io);
        assert!(!report.external_publish);
        assert!(!report.has_errors());
    }

    #[test]
    fn cli_api_json_protocol_disallows_native_python_binding_claim() {
        let report = CliApiJsonProtocolReport::contract_only();
        assert_eq!(
            report.thin_python_wrapper_boundary,
            "cli_json_subprocess_first"
        );
        assert!(!report.pyo3_maturin_allowed);
        assert!(!report.dataframe_api_implemented);
    }

    #[test]
    fn python_wrapper_foundation_is_cli_json_only() {
        let report = PythonWrapperFoundationReport::contract_only();
        assert_eq!(report.transport_protocol_id, "shardloom.cli_json.v1");
        assert_eq!(report.invocation_model, "subprocess_cli_json");
        assert!(report.initial_command_scope.contains(&"api-compat-plan"));
        assert!(
            report
                .required_client_behaviors
                .contains(&"parse_output_envelope")
        );
        assert!(!report.python_package_created);
        assert!(!report.pyo3_maturin_allowed);
        assert!(!report.native_extension_required);
        assert!(!report.has_errors());
    }

    #[test]
    fn python_wrapper_foundation_defers_mature_python_surfaces() {
        let report = PythonWrapperFoundationReport::contract_only();
        assert!(!report.dataframe_api_implemented);
        assert!(!report.notebook_api_implemented);
        assert!(!report.python_udf_runtime_implemented);
        assert!(!report.runtime_execution);
        assert!(!report.write_io);
        assert!(!report.fallback_execution_allowed);
    }
}
