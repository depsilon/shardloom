use shardloom_core::{Diagnostic, DiagnosticCode};

/// Estimate value where unknown is represented explicitly.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EstimateValue {
    Known(u64),
    Unknown,
}

impl EstimateValue {
    #[must_use]
    pub const fn known(value: u64) -> Self {
        Self::Known(value)
    }
    #[must_use]
    pub const fn unknown() -> Self {
        Self::Unknown
    }
    #[must_use]
    pub const fn is_known(&self) -> bool {
        matches!(self, Self::Known(_))
    }
    #[must_use]
    pub const fn unwrap_or(&self, fallback: u64) -> u64 {
        match self {
            Self::Known(v) => *v,
            Self::Unknown => fallback,
        }
    }
    #[must_use]
    pub fn to_human_text(&self) -> String {
        match self {
            Self::Known(v) => v.to_string(),
            Self::Unknown => "unknown".to_string(),
        }
    }
}

/// Confidence level for estimate uncertainty communication.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EstimateConfidence {
    High,
    Medium,
    Low,
    Unknown,
}

impl EstimateConfidence {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::High => "high",
            Self::Medium => "medium",
            Self::Low => "low",
            Self::Unknown => "unknown",
        }
    }
}

/// Structured estimate report for planning inspectability and uncertainty.
#[derive(Debug, Clone, PartialEq)]
pub struct EstimateReport {
    pub operation_summary: String,
    pub confidence: EstimateConfidence,
    pub estimated_bytes_read: EstimateValue,
    pub estimated_bytes_decoded: EstimateValue,
    pub estimated_rows_scanned: EstimateValue,
    pub estimated_rows_materialized: EstimateValue,
    pub estimated_segments_considered: EstimateValue,
    pub estimated_segments_pruned: EstimateValue,
    pub estimated_object_store_requests: EstimateValue,
    pub estimated_output_bytes: EstimateValue,
    pub uncertainty_notes: Vec<String>,
    pub diagnostics: Vec<Diagnostic>,
}

impl EstimateReport {
    /// Creates an all-unknown estimate report.
    #[must_use]
    pub fn unknown(operation_summary: impl Into<String>) -> Self {
        Self {
            operation_summary: operation_summary.into(),
            confidence: EstimateConfidence::Unknown,
            estimated_bytes_read: EstimateValue::Unknown,
            estimated_bytes_decoded: EstimateValue::Unknown,
            estimated_rows_scanned: EstimateValue::Unknown,
            estimated_rows_materialized: EstimateValue::Unknown,
            estimated_segments_considered: EstimateValue::Unknown,
            estimated_segments_pruned: EstimateValue::Unknown,
            estimated_object_store_requests: EstimateValue::Unknown,
            estimated_output_bytes: EstimateValue::Unknown,
            uncertainty_notes: Vec::new(),
            diagnostics: Vec::new(),
        }
    }

    pub fn add_uncertainty(&mut self, note: impl Into<String>) {
        self.uncertainty_notes.push(note.into());
    }
    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
    }

    #[must_use]
    pub fn has_known_work(&self) -> bool {
        [
            self.estimated_bytes_read,
            self.estimated_bytes_decoded,
            self.estimated_rows_scanned,
            self.estimated_rows_materialized,
            self.estimated_segments_considered,
            self.estimated_segments_pruned,
            self.estimated_object_store_requests,
            self.estimated_output_bytes,
        ]
        .into_iter()
        .any(|v| v.is_known())
    }

    #[must_use]
    pub fn to_human_text(&self) -> String {
        let mut lines = vec![
            format!("operation: {}", self.operation_summary),
            format!("confidence: {}", self.confidence.as_str()),
            format!("bytes read: {}", self.estimated_bytes_read.to_human_text()),
            format!(
                "bytes decoded: {}",
                self.estimated_bytes_decoded.to_human_text()
            ),
            format!(
                "rows scanned: {}",
                self.estimated_rows_scanned.to_human_text()
            ),
            format!(
                "rows materialized: {}",
                self.estimated_rows_materialized.to_human_text()
            ),
            format!(
                "segments considered: {}",
                self.estimated_segments_considered.to_human_text()
            ),
            format!(
                "segments pruned: {}",
                self.estimated_segments_pruned.to_human_text()
            ),
            format!(
                "object-store requests: {}",
                self.estimated_object_store_requests.to_human_text()
            ),
            format!(
                "output bytes: {}",
                self.estimated_output_bytes.to_human_text()
            ),
        ];
        if !self.uncertainty_notes.is_empty() {
            lines.push("uncertainty notes:".to_string());
            for note in &self.uncertainty_notes {
                lines.push(format!("- {note}"));
            }
        }
        if !self.diagnostics.is_empty() {
            lines.push("diagnostics:".to_string());
            for d in &self.diagnostics {
                lines.push(format!("- {}", d.to_human_text()));
            }
        }
        lines.join("\n")
    }

    /// Creates an unsupported estimate report with explicit no-fallback diagnostics.
    #[must_use]
    pub fn unsupported(
        operation_summary: impl Into<String>,
        feature: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        let mut report = Self::unknown(operation_summary);
        report.add_uncertainty("cost model is not implemented yet");
        report.add_diagnostic(Diagnostic::unsupported(
            DiagnosticCode::UnsupportedSql,
            feature,
            reason,
            Some("Native estimate planning is not implemented yet.".to_string()),
        ));
        report
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn estimate_value_known_unknown_behavior() {
        let known = EstimateValue::known(42);
        let unknown = EstimateValue::unknown();
        assert!(known.is_known());
        assert!(!unknown.is_known());
        assert_eq!(known.unwrap_or(7), 42);
        assert_eq!(unknown.unwrap_or(7), 7);
    }

    #[test]
    fn estimate_report_unknown_has_no_known_work() {
        let report = EstimateReport::unknown("op");
        assert!(!report.has_known_work());
    }

    #[test]
    fn estimate_report_unsupported_has_diagnostics() {
        let report = EstimateReport::unsupported("op", "estimation", "not implemented");
        assert!(!report.diagnostics.is_empty());
    }

    #[test]
    fn estimate_report_human_text_includes_confidence_and_uncertainty() {
        let mut report = EstimateReport::unknown("op");
        report.add_uncertainty("missing stats");
        let text = report.to_human_text();
        assert!(text.contains("confidence: unknown"));
        assert!(text.contains("uncertainty notes"));
    }
}
