use shardloom_core::{
    Diagnostic, DiagnosticCode, DiagnosticSeverity, ExecutionState, Result, ShardLoomError,
};

/// Stable identifier for a node in a logical or physical explain plan.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PlanNodeId(String);

impl PlanNodeId {
    /// Creates a new plan node identifier.
    ///
    /// # Errors
    /// Returns an error when the id is empty or whitespace-only.
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        if value.trim().is_empty() {
            return Err(ShardLoomError::InvalidOperation(
                "plan node id must not be empty".to_string(),
            ));
        }

        Ok(Self(value))
    }

    /// Returns the string form of this identifier.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Stable plan node kinds for explain-domain inspectability.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlanNodeKind {
    Scan,
    Filter,
    Projection,
    Aggregate,
    Join,
    Sort,
    Limit,
    Write,
    Translation,
    ExternalRead,
    ExternalWrite,
    ModelCall,
    Unsupported,
}

impl PlanNodeKind {
    /// Returns a stable string representation for deterministic output.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Scan => "scan",
            Self::Filter => "filter",
            Self::Projection => "projection",
            Self::Aggregate => "aggregate",
            Self::Join => "join",
            Self::Sort => "sort",
            Self::Limit => "limit",
            Self::Write => "write",
            Self::Translation => "translation",
            Self::ExternalRead => "external_read",
            Self::ExternalWrite => "external_write",
            Self::ModelCall => "model_call",
            Self::Unsupported => "unsupported",
        }
    }
}

/// Execution boundary markers shown in explain reports.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionBoundary {
    NativeVortexInput,
    MetadataOnly,
    SegmentPruning,
    EncodedEvaluation,
    PartialDecode,
    FullMaterialization,
    Translation,
    NativeVortexOutput,
    CompatibilityOutput,
    ExternalEffect,
    Unsupported,
}

impl ExecutionBoundary {
    /// Returns a stable string representation for deterministic output.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::NativeVortexInput => "native_vortex_input",
            Self::MetadataOnly => "metadata_only",
            Self::SegmentPruning => "segment_pruning",
            Self::EncodedEvaluation => "encoded_evaluation",
            Self::PartialDecode => "partial_decode",
            Self::FullMaterialization => "full_materialization",
            Self::Translation => "translation",
            Self::NativeVortexOutput => "native_vortex_output",
            Self::CompatibilityOutput => "compatibility_output",
            Self::ExternalEffect => "external_effect",
            Self::Unsupported => "unsupported",
        }
    }
}

/// A single explain plan node with explicit boundaries and diagnostics.
#[derive(Debug, Clone, PartialEq)]
pub struct ExplainPlanNode {
    pub id: PlanNodeId,
    pub kind: PlanNodeKind,
    pub label: String,
    pub execution_state: ExecutionState,
    pub boundaries: Vec<ExecutionBoundary>,
    pub diagnostics: Vec<Diagnostic>,
}

impl ExplainPlanNode {
    /// Creates an explain node used for inspectability; it does not execute work.
    #[must_use]
    pub fn new(
        id: PlanNodeId,
        kind: PlanNodeKind,
        label: impl Into<String>,
        execution_state: ExecutionState,
    ) -> Self {
        Self {
            id,
            kind,
            label: label.into(),
            execution_state,
            boundaries: Vec::new(),
            diagnostics: Vec::new(),
        }
    }

    /// Adds an execution boundary to this node.
    #[must_use]
    pub fn with_boundary(mut self, boundary: ExecutionBoundary) -> Self {
        self.boundaries.push(boundary);
        self
    }

    /// Adds a diagnostic to this node.
    #[must_use]
    pub fn with_diagnostic(mut self, diagnostic: Diagnostic) -> Self {
        self.diagnostics.push(diagnostic);
        self
    }

    /// Renders this node as concise human-readable text.
    #[must_use]
    pub fn to_human_text(&self) -> String {
        format!(
            "- node {} [{}]: {} ({})",
            self.id.as_str(),
            self.kind.as_str(),
            self.label,
            self.execution_state.as_str()
        )
    }
}

/// Structured explain report for inspectability without fallback execution.
#[derive(Debug, Clone, PartialEq)]
pub struct ExplainReport {
    pub operation_summary: String,
    pub input_datasets: Vec<String>,
    pub output_target: Option<String>,
    pub native_vortex_input: bool,
    pub native_vortex_output: bool,
    pub fallback_execution_allowed: bool,
    pub nodes: Vec<ExplainPlanNode>,
    pub diagnostics: Vec<Diagnostic>,
}

impl ExplainReport {
    /// Creates a new explain report with Vortex-native defaults and no fallback.
    #[must_use]
    pub fn new(operation_summary: impl Into<String>) -> Self {
        Self {
            operation_summary: operation_summary.into(),
            input_datasets: Vec::new(),
            output_target: None,
            native_vortex_input: true,
            native_vortex_output: true,
            fallback_execution_allowed: false,
            nodes: Vec::new(),
            diagnostics: Vec::new(),
        }
    }

    pub fn add_input_dataset(&mut self, dataset: impl Into<String>) {
        self.input_datasets.push(dataset.into());
    }
    pub fn set_output_target(&mut self, target: impl Into<String>) {
        self.output_target = Some(target.into());
    }
    pub fn add_node(&mut self, node: ExplainPlanNode) {
        self.nodes.push(node);
    }
    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
    }

    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.diagnostics
            .iter()
            .chain(self.nodes.iter().flat_map(|n| n.diagnostics.iter()))
            .any(|d| {
                matches!(
                    d.severity,
                    DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
                )
            })
    }

    /// Renders a deterministic explain summary.
    #[must_use]
    pub fn to_human_text(&self) -> String {
        let mut lines = vec![
            format!("operation: {}", self.operation_summary),
            format!(
                "native vortex input: {}",
                if self.native_vortex_input {
                    "enabled"
                } else {
                    "disabled"
                }
            ),
            format!(
                "native vortex output: {}",
                if self.native_vortex_output {
                    "enabled"
                } else {
                    "disabled"
                }
            ),
            format!(
                "fallback execution: {}",
                if self.fallback_execution_allowed {
                    "enabled"
                } else {
                    "disabled"
                }
            ),
            format!("node count: {}", self.nodes.len()),
        ];
        for node in &self.nodes {
            lines.push(node.to_human_text());
            if !node.diagnostics.is_empty() {
                lines.push(format!("  node diagnostics [{}]:", node.id.as_str()));
                for diagnostic in &node.diagnostics {
                    lines.push(format!("  - {}", diagnostic.to_human_text()));
                }
            }
        }
        if !self.diagnostics.is_empty() {
            lines.push("diagnostics:".to_string());
            for diagnostic in &self.diagnostics {
                lines.push(format!("- {}", diagnostic.to_human_text()));
            }
        }
        lines.join("\n")
    }

    /// Creates an explicit unsupported explain report with no fallback allowed.
    #[must_use]
    pub fn unsupported(
        operation_summary: impl Into<String>,
        feature: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        let reason = reason.into();
        let mut report = Self::new(operation_summary);
        report.add_diagnostic(Diagnostic::unsupported(
            DiagnosticCode::UnsupportedSql,
            feature,
            reason,
            Some("Planning and execution are not implemented yet; use status/capabilities/doctor for current support.".to_string()),
        ));
        report
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plan_node_id_rejects_empty_ids() {
        assert!(PlanNodeId::new("").is_err());
        assert!(PlanNodeId::new("   ").is_err());
    }

    #[test]
    fn plan_node_kind_as_str_is_stable() {
        assert_eq!(PlanNodeKind::Scan.as_str(), "scan");
        assert_eq!(PlanNodeKind::ExternalWrite.as_str(), "external_write");
    }

    #[test]
    fn execution_boundary_as_str_is_stable() {
        assert_eq!(ExecutionBoundary::MetadataOnly.as_str(), "metadata_only");
        assert_eq!(
            ExecutionBoundary::NativeVortexOutput.as_str(),
            "native_vortex_output"
        );
    }

    #[test]
    fn explain_plan_node_human_text_contains_id_kind_and_label() {
        let node = ExplainPlanNode::new(
            PlanNodeId::new("n1").expect("valid id"),
            PlanNodeKind::Filter,
            "filter events",
            ExecutionState::MetadataOnly,
        );
        let text = node.to_human_text();
        assert!(text.contains("n1"));
        assert!(text.contains("filter"));
        assert!(text.contains("filter events"));
    }

    #[test]
    fn explain_report_defaults_fallback_disabled() {
        let report = ExplainReport::new("op");
        assert!(!report.fallback_execution_allowed);
    }

    #[test]
    fn explain_report_unsupported_has_errors() {
        let report = ExplainReport::unsupported("op", "planning", "not implemented");
        assert!(report.has_errors());
    }

    #[test]
    fn explain_report_human_text_surfaces_node_diagnostics() {
        let node = ExplainPlanNode::new(
            PlanNodeId::new("n1").expect("valid id"),
            PlanNodeKind::Unsupported,
            "unsupported node",
            ExecutionState::Unsupported,
        )
        .with_diagnostic(Diagnostic::unsupported(
            DiagnosticCode::UnsupportedSql,
            "feature",
            "node-level unsupported",
            None,
        ));

        let mut report = ExplainReport::new("op");
        report.add_node(node);
        let text = report.to_human_text();

        assert!(text.contains("node diagnostics [n1]:"));
        assert!(text.contains("node-level unsupported"));
    }

    #[test]
    fn explain_report_human_text_includes_fallback_disabled() {
        let report = ExplainReport::new("op");
        assert!(
            report
                .to_human_text()
                .contains("fallback execution: disabled")
        );
    }
}
