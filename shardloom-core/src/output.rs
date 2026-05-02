//! Machine-readable CLI output envelope.
//!
//! Text rendering is for humans, JSON rendering is for agents/automation.
//! This module only renders output metadata and diagnostics; it does not execute work.

use crate::{Diagnostic, DiagnosticSeverity, FallbackStatus, Result, ShardLoomError};

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

pub(crate) fn json_escape(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
        .replace('\t', "\\t")
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
}
