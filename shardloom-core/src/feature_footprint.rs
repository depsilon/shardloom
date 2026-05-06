use crate::{Diagnostic, DiagnosticSeverity, Result, ShardLoomError};
use std::fmt::Write as _;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GateBool(pub bool);

impl GateBool {
    #[must_use]
    pub const fn get(self) -> bool {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FeatureFootprintGateStatus {
    Enabled,
    Disabled,
    Planned,
    RequiresToolchain,
    Unsupported,
}

impl FeatureFootprintGateStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Enabled => "enabled",
            Self::Disabled => "disabled",
            Self::Planned => "planned",
            Self::RequiresToolchain => "requires_toolchain",
            Self::Unsupported => "unsupported",
        }
    }

    #[must_use]
    pub const fn is_enabled(&self) -> bool {
        matches!(self, Self::Enabled)
    }

    #[must_use]
    pub const fn is_blocking(&self) -> bool {
        matches!(self, Self::RequiresToolchain | Self::Unsupported)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeatureFootprintGate {
    pub name: String,
    pub status: FeatureFootprintGateStatus,
    pub compiled: GateBool,
    pub enabled: GateBool,
    pub default_enabled: GateBool,
    pub requires_toolchain: GateBool,
    pub allows_io: GateBool,
    pub allows_scan: GateBool,
    pub allows_write: GateBool,
    pub allows_object_store: GateBool,
    pub diagnostics: Vec<Diagnostic>,
}

impl FeatureFootprintGate {
    /// Creates a new [`FeatureFootprintGate`] without any probing.
    ///
    /// # Errors
    ///
    /// Returns [`ShardLoomError::InvalidOperation`] when `name` is empty or whitespace-only.
    pub fn new(name: impl Into<String>, status: FeatureFootprintGateStatus) -> Result<Self> {
        let name = name.into();
        if name.trim().is_empty() {
            return Err(ShardLoomError::InvalidOperation(
                "feature footprint gate name must not be empty".to_string(),
            ));
        }

        Ok(Self {
            name,
            status,
            compiled: GateBool(false),
            enabled: GateBool(status.is_enabled()),
            default_enabled: GateBool(false),
            requires_toolchain: GateBool(matches!(
                status,
                FeatureFootprintGateStatus::RequiresToolchain
            )),
            allows_io: GateBool(false),
            allows_scan: GateBool(false),
            allows_write: GateBool(false),
            allows_object_store: GateBool(false),
            diagnostics: Vec::new(),
        })
    }

    /// Creates a disabled [`FeatureFootprintGate`] without any probing.
    ///
    /// # Errors
    ///
    /// Returns [`ShardLoomError::InvalidOperation`] when `name` is empty or whitespace-only.
    pub fn disabled(name: impl Into<String>) -> Result<Self> {
        Self::new(name, FeatureFootprintGateStatus::Disabled)
    }

    /// Creates a planned [`FeatureFootprintGate`] without any probing.
    ///
    /// # Errors
    ///
    /// Returns [`ShardLoomError::InvalidOperation`] when `name` is empty or whitespace-only.
    pub fn planned(name: impl Into<String>) -> Result<Self> {
        Self::new(name, FeatureFootprintGateStatus::Planned)
    }

    #[must_use]
    pub fn compiled(mut self, value: bool) -> Self {
        self.compiled = GateBool(value);
        self
    }
    #[must_use]
    pub fn enabled(mut self, value: bool) -> Self {
        self.enabled = GateBool(value);
        self
    }
    #[must_use]
    pub fn default_enabled(mut self, value: bool) -> Self {
        self.default_enabled = GateBool(value);
        self
    }
    #[must_use]
    pub fn requires_toolchain(mut self, value: bool) -> Self {
        self.requires_toolchain = GateBool(value);
        self
    }
    #[must_use]
    pub fn allows_io(mut self, value: bool) -> Self {
        self.allows_io = GateBool(value);
        self
    }
    #[must_use]
    pub fn allows_scan(mut self, value: bool) -> Self {
        self.allows_scan = GateBool(value);
        self
    }
    #[must_use]
    pub fn allows_write(mut self, value: bool) -> Self {
        self.allows_write = GateBool(value);
        self
    }
    #[must_use]
    pub fn allows_object_store(mut self, value: bool) -> Self {
        self.allows_object_store = GateBool(value);
        self
    }
    #[must_use]
    pub fn with_diagnostic(mut self, diagnostic: Diagnostic) -> Self {
        self.diagnostics.push(diagnostic);
        self
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
        format!("{}: {}", self.name, self.status.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExternalBaselineAvailability {
    pub baseline_engine: String,
    pub available: bool,
    pub runtime_fallback_allowed: bool,
    pub notes: Option<String>,
}

impl ExternalBaselineAvailability {
    /// Creates an unavailable external baseline record.
    ///
    /// # Errors
    ///
    /// Returns [`ShardLoomError::InvalidOperation`] when `baseline_engine` is empty.
    pub fn unavailable(baseline_engine: impl Into<String>) -> Result<Self> {
        let baseline_engine = baseline_engine.into();
        if baseline_engine.trim().is_empty() {
            return Err(ShardLoomError::InvalidOperation(
                "external baseline engine must not be empty".to_string(),
            ));
        }
        Ok(Self {
            baseline_engine,
            available: false,
            runtime_fallback_allowed: false,
            notes: Some("comparison-only external baseline; not runtime fallback".to_string()),
        })
    }

    /// Creates an available comparison-only baseline record.
    ///
    /// # Errors
    ///
    /// Returns [`ShardLoomError::InvalidOperation`] when `baseline_engine` is empty.
    pub fn available_external_only(baseline_engine: impl Into<String>) -> Result<Self> {
        let baseline_engine = baseline_engine.into();
        if baseline_engine.trim().is_empty() {
            return Err(ShardLoomError::InvalidOperation(
                "external baseline engine must not be empty".to_string(),
            ));
        }
        Ok(Self {
            baseline_engine,
            available: true,
            runtime_fallback_allowed: false,
            notes: Some("available for comparison only; not runtime fallback".to_string()),
        })
    }

    /// Creates a no-probe baseline record with unknown availability.
    ///
    /// # Errors
    ///
    /// Returns [`ShardLoomError::InvalidOperation`] when `baseline_engine` is empty.
    pub fn unknown_external_only(baseline_engine: impl Into<String>) -> Result<Self> {
        let baseline_engine = baseline_engine.into();
        if baseline_engine.trim().is_empty() {
            return Err(ShardLoomError::InvalidOperation(
                "external baseline engine must not be empty".to_string(),
            ));
        }
        Ok(Self {
            baseline_engine,
            available: false,
            runtime_fallback_allowed: false,
            notes: Some("availability unknown in no-probe contract; comparison only".to_string()),
        })
    }

    #[must_use]
    pub const fn is_runtime_fallback(&self) -> bool {
        self.runtime_fallback_allowed
    }

    #[must_use]
    pub fn summary(&self) -> String {
        let availability = if self.available {
            "available"
        } else {
            "unavailable"
        };
        format!(
            "{}: {} (runtime fallback: {})",
            self.baseline_engine, availability, self.runtime_fallback_allowed
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeatureFootprintReport {
    pub schema_version: &'static str,
    pub engine_version: String,
    pub crate_versions: Vec<(String, String)>,
    pub compiled_features: Vec<String>,
    pub enabled_features: Vec<String>,
    pub disabled_features: Vec<String>,
    pub upstream_vortex_dependency_status: String,
    pub upstream_vortex_version: Option<String>,
    pub vortex_gates: Vec<FeatureFootprintGate>,
    pub encoded_read_gates: Vec<FeatureFootprintGate>,
    pub metadata_io_gates: Vec<FeatureFootprintGate>,
    pub write_gates: Vec<FeatureFootprintGate>,
    pub spill_gates: Vec<FeatureFootprintGate>,
    pub cleanup_gates: Vec<FeatureFootprintGate>,
    pub object_store_gates: Vec<FeatureFootprintGate>,
    pub distributed_execution_gates: Vec<FeatureFootprintGate>,
    pub external_baseline_availability: Vec<ExternalBaselineAvailability>,
    pub fallback_engines_absent: bool,
    pub fallback_execution_allowed: bool,
    pub diagnostics: Vec<Diagnostic>,
}

impl FeatureFootprintReport {
    /// Builds a deterministic `FeatureFootprintReport` contract with no probing.
    #[must_use]
    pub fn contract_only() -> Self {
        let named_gate = |name| {
            FeatureFootprintGate::disabled(name)
                .unwrap_or_else(|_| unreachable!("deterministic gate names are valid"))
        };
        Self {
            schema_version: "shardloom.feature_footprint.v1",
            engine_version: env!("CARGO_PKG_VERSION").to_string(),
            crate_versions: vec![(
                "shardloom-core".to_string(),
                env!("CARGO_PKG_VERSION").to_string(),
            )],
            compiled_features: Vec::new(),
            enabled_features: Vec::new(),
            disabled_features: Vec::new(),
            upstream_vortex_dependency_status: "deferred".to_string(),
            upstream_vortex_version: None,
            vortex_gates: vec![
                named_gate("upstream_vortex"),
                named_gate("vortex_file_io"),
                named_gate("vortex_metadata_executor"),
                named_gate("vortex_encoded_read_executor"),
                named_gate("vortex_staged_output_fs"),
                named_gate("vortex_write"),
                named_gate("vortex_object_store"),
                named_gate("vortex_output_payload"),
                named_gate("vortex_commit_execution"),
            ],
            encoded_read_gates: vec![named_gate("vortex_encoded_read_executor")],
            metadata_io_gates: vec![
                named_gate("vortex_file_io"),
                named_gate("vortex_metadata_executor"),
            ],
            write_gates: vec![
                named_gate("vortex_write"),
                named_gate("vortex_output_payload"),
            ],
            spill_gates: vec![
                FeatureFootprintGate::planned("spill_payload_fs")
                    .unwrap_or_else(|_| unreachable!("deterministic static baseline name")),
            ],
            cleanup_gates: vec![
                FeatureFootprintGate::planned("cleanup_execution")
                    .unwrap_or_else(|_| unreachable!("deterministic static baseline name")),
            ],
            object_store_gates: vec![named_gate("vortex_object_store")],
            distributed_execution_gates: vec![
                FeatureFootprintGate::planned("distributed_execution")
                    .unwrap_or_else(|_| unreachable!("deterministic static baseline name")),
            ],
            external_baseline_availability: vec![
                ExternalBaselineAvailability::unavailable("spark")
                    .unwrap_or_else(|_| unreachable!("deterministic static baseline name")),
                ExternalBaselineAvailability::unknown_external_only("datafusion")
                    .unwrap_or_else(|_| unreachable!("deterministic static baseline name")),
            ],
            fallback_engines_absent: false,
            fallback_execution_allowed: false,
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
        }) || self
            .all_gates()
            .into_iter()
            .any(FeatureFootprintGate::has_errors)
    }

    #[must_use]
    pub fn all_gates(&self) -> Vec<&FeatureFootprintGate> {
        let mut gates = Vec::new();
        gates.extend(self.vortex_gates.iter());
        gates.extend(self.encoded_read_gates.iter());
        gates.extend(self.metadata_io_gates.iter());
        gates.extend(self.write_gates.iter());
        gates.extend(self.spill_gates.iter());
        gates.extend(self.cleanup_gates.iter());
        gates.extend(self.object_store_gates.iter());
        gates.extend(self.distributed_execution_gates.iter());
        gates
    }

    #[must_use]
    pub fn gate_by_name(&self, name: &str) -> Option<&FeatureFootprintGate> {
        self.all_gates().into_iter().find(|gate| gate.name == name)
    }

    #[must_use]
    pub const fn fallback_execution_allowed(&self) -> bool {
        self.fallback_execution_allowed
    }

    #[must_use]
    pub fn to_human_text(&self) -> String {
        let mut out = String::new();
        let _ = writeln!(out, "schema_version: {}", self.schema_version);
        let _ = writeln!(out, "engine_version: {}", self.engine_version);
        let _ = writeln!(out, "fallback execution: disabled");
        let _ = writeln!(
            out,
            "fallback_engines_absent: {}",
            self.fallback_engines_absent
        );
        let _ = writeln!(
            out,
            "upstream_vortex_dependency_status: {}",
            self.upstream_vortex_dependency_status
        );
        let _ = writeln!(out, "vortex gates:");
        for gate in &self.vortex_gates {
            let _ = writeln!(out, "  - {} [{}]", gate.name, gate.status.as_str());
        }
        let _ = writeln!(
            out,
            "external baselines: comparison-only, not runtime fallback"
        );
        for baseline in &self.external_baseline_availability {
            let _ = writeln!(out, "  - {}", baseline.summary());
        }
        if !self.diagnostics.is_empty() {
            let _ = writeln!(out, "diagnostics:");
            for diag in &self.diagnostics {
                let _ = writeln!(out, "  - {}: {}", diag.code.as_str(), diag.message);
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DiagnosticCategory, DiagnosticCode, FallbackStatus};

    #[test]
    fn gate_rejects_empty_name() {
        assert!(FeatureFootprintGate::new("   ", FeatureFootprintGateStatus::Disabled).is_err());
    }
    #[test]
    fn disabled_gate_defaults_disallow_effects() {
        let gate = FeatureFootprintGate::disabled("x").expect("valid gate");
        assert!(
            !gate.allows_io.get()
                && !gate.allows_scan.get()
                && !gate.allows_write.get()
                && !gate.allows_object_store.get()
        );
    }
    #[test]
    fn gate_status_enabled_is_enabled() {
        assert!(FeatureFootprintGateStatus::Enabled.is_enabled());
    }
    #[test]
    fn gate_status_requires_toolchain_is_blocking() {
        assert!(FeatureFootprintGateStatus::RequiresToolchain.is_blocking());
    }
    #[test]
    fn external_baseline_unavailable_rejects_empty_engine() {
        assert!(ExternalBaselineAvailability::unavailable(" ").is_err());
    }
    #[test]
    fn external_baseline_external_only_disallows_runtime_fallback() {
        let baseline =
            ExternalBaselineAvailability::available_external_only("spark").expect("valid baseline");
        assert!(!baseline.runtime_fallback_allowed);
    }
    #[test]
    fn contract_only_fallback_execution_is_false() {
        assert!(!FeatureFootprintReport::contract_only().fallback_execution_allowed());
    }
    #[test]
    fn contract_only_fallback_engines_absent_false_without_probe() {
        assert!(!FeatureFootprintReport::contract_only().fallback_engines_absent);
    }
    #[test]
    fn contract_only_schema_version() {
        assert_eq!(
            FeatureFootprintReport::contract_only().schema_version,
            "shardloom.feature_footprint.v1"
        );
    }
    #[test]
    fn contract_only_includes_upstream_vortex_gate() {
        assert!(
            FeatureFootprintReport::contract_only()
                .gate_by_name("upstream_vortex")
                .is_some()
        );
    }
    #[test]
    fn contract_only_includes_encoded_read_gate() {
        assert!(
            FeatureFootprintReport::contract_only()
                .gate_by_name("vortex_encoded_read_executor")
                .is_some()
        );
    }
    #[test]
    fn contract_only_includes_file_io_gate() {
        assert!(
            FeatureFootprintReport::contract_only()
                .gate_by_name("vortex_file_io")
                .is_some()
        );
    }
    #[test]
    fn no_generated_at_field_exists() {
        let report = FeatureFootprintReport::contract_only();
        let text = report.to_human_text();
        assert!(!text.contains("generated_at"));
    }
    #[test]
    fn report_has_errors_is_severity_based() {
        let mut report = FeatureFootprintReport::contract_only();
        report.add_diagnostic(Diagnostic::new(
            DiagnosticCode::InvalidInput,
            DiagnosticSeverity::Warning,
            DiagnosticCategory::InvalidInput,
            "warn",
            None,
            None,
            None,
            FallbackStatus::disabled_by_policy(),
        ));
        assert!(!report.has_errors());
        report.add_diagnostic(Diagnostic::new(
            DiagnosticCode::InvalidInput,
            DiagnosticSeverity::Error,
            DiagnosticCategory::InvalidInput,
            "err",
            None,
            None,
            None,
            FallbackStatus::disabled_by_policy(),
        ));
        assert!(report.has_errors());
    }
    #[test]
    fn to_human_text_mentions_fallback_disabled() {
        assert!(
            FeatureFootprintReport::contract_only()
                .to_human_text()
                .contains("fallback execution: disabled")
        );
    }
    #[test]
    fn to_human_text_mentions_external_baselines_not_fallback() {
        assert!(
            FeatureFootprintReport::contract_only()
                .to_human_text()
                .contains("comparison-only, not runtime fallback")
        );
    }
}
