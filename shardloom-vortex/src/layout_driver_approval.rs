#![allow(clippy::must_use_candidate)]

use std::fmt::Write as _;

use shardloom_core::{Diagnostic, DiagnosticCode, DiagnosticSeverity, Result};

use crate::{
    VortexEncodedReadApiBoundaryReport, VortexEncodedReadApiItem, VortexEncodedReadApiRisk,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexLayoutReaderDriverApprovalStatus {
    Planned,
    ApprovedForLayoutRowCountOnly,
    BlockedByMissingLayoutReader,
    BlockedByMissingLayoutRowCount,
    BlockedByRuntimeDriverPolicy,
    BlockedByNonLocalScope,
    BlockedByExecutionRequest,
    Unsupported,
}
impl VortexLayoutReaderDriverApprovalStatus {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Planned => "planned",
            Self::ApprovedForLayoutRowCountOnly => "approved_for_layout_row_count_only",
            Self::BlockedByMissingLayoutReader => "blocked_by_missing_layout_reader",
            Self::BlockedByMissingLayoutRowCount => "blocked_by_missing_layout_row_count",
            Self::BlockedByRuntimeDriverPolicy => "blocked_by_runtime_driver_policy",
            Self::BlockedByNonLocalScope => "blocked_by_non_local_scope",
            Self::BlockedByExecutionRequest => "blocked_by_execution_request",
            Self::Unsupported => "unsupported",
        }
    }

    pub const fn is_error(&self) -> bool {
        !matches!(self, Self::Planned | Self::ApprovedForLayoutRowCountOnly)
    }

    pub const fn approved(&self) -> bool {
        matches!(self, Self::ApprovedForLayoutRowCountOnly)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexLayoutReaderDriverApprovalMode {
    ReportOnly,
    LayoutRowCountOnlyApproval,
    Unsupported,
}
impl VortexLayoutReaderDriverApprovalMode {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::ReportOnly => "report_only",
            Self::LayoutRowCountOnlyApproval => "layout_row_count_only_approval",
            Self::Unsupported => "unsupported",
        }
    }

    pub const fn constructs_layout_reader(&self) -> bool {
        false
    }

    pub const fn starts_runtime_driver(&self) -> bool {
        false
    }

    pub const fn reads_data(&self) -> bool {
        false
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexLayoutReaderDriverApprovalSignal {
    LocalFixtureOnly,
    CallerSessionAllowed,
    RuntimeDriverStartAllowed,
    LayoutRowCountOnlyIntent,
    ScanForbidden,
    EvaluationForbidden,
    DataReadForbidden,
    DecodeForbidden,
    MaterializationForbidden,
    ArrowForbidden,
    ObjectStoreForbidden,
    WriteForbidden,
    FallbackForbidden,
}
impl VortexLayoutReaderDriverApprovalSignal {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::LocalFixtureOnly => "local_fixture_only",
            Self::CallerSessionAllowed => "caller_session_allowed",
            Self::RuntimeDriverStartAllowed => "runtime_driver_start_allowed",
            Self::LayoutRowCountOnlyIntent => "layout_row_count_only_intent",
            Self::ScanForbidden => "scan_forbidden",
            Self::EvaluationForbidden => "evaluation_forbidden",
            Self::DataReadForbidden => "data_read_forbidden",
            Self::DecodeForbidden => "decode_forbidden",
            Self::MaterializationForbidden => "materialization_forbidden",
            Self::ArrowForbidden => "arrow_forbidden",
            Self::ObjectStoreForbidden => "object_store_forbidden",
            Self::WriteForbidden => "write_forbidden",
            Self::FallbackForbidden => "fallback_forbidden",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VortexLayoutReaderDriverApprovalInput {
    pub api_boundary_report: VortexEncodedReadApiBoundaryReport,
    pub signals: Vec<VortexLayoutReaderDriverApprovalSignal>,
}
impl VortexLayoutReaderDriverApprovalInput {
    pub fn new(api_boundary_report: VortexEncodedReadApiBoundaryReport) -> Self {
        Self {
            api_boundary_report,
            signals: vec![],
        }
    }

    pub fn add_signal(&mut self, signal: VortexLayoutReaderDriverApprovalSignal) {
        if !self.signals.contains(&signal) {
            self.signals.push(signal);
        }
    }

    fn set_signal(mut self, signal: VortexLayoutReaderDriverApprovalSignal, value: bool) -> Self {
        if value {
            self.add_signal(signal);
        } else {
            self.signals.retain(|existing| *existing != signal);
        }
        self
    }

    pub fn has_signal(&self, signal: VortexLayoutReaderDriverApprovalSignal) -> bool {
        self.signals.contains(&signal)
    }
}

macro_rules! signal_builder {
    ($name:ident, $signal:ident) => {
        #[must_use]
        pub fn $name(self, value: bool) -> Self {
            self.set_signal(VortexLayoutReaderDriverApprovalSignal::$signal, value)
        }
    };
}
impl VortexLayoutReaderDriverApprovalInput {
    signal_builder!(local_fixture_only, LocalFixtureOnly);
    signal_builder!(caller_session_allowed, CallerSessionAllowed);
    signal_builder!(runtime_driver_start_allowed, RuntimeDriverStartAllowed);
    signal_builder!(layout_row_count_only_intent, LayoutRowCountOnlyIntent);
    signal_builder!(scan_forbidden, ScanForbidden);
    signal_builder!(evaluation_forbidden, EvaluationForbidden);
    signal_builder!(data_read_forbidden, DataReadForbidden);
    signal_builder!(decode_forbidden, DecodeForbidden);
    signal_builder!(materialization_forbidden, MaterializationForbidden);
    signal_builder!(arrow_forbidden, ArrowForbidden);
    signal_builder!(object_store_forbidden, ObjectStoreForbidden);
    signal_builder!(write_forbidden, WriteForbidden);
    signal_builder!(fallback_forbidden, FallbackForbidden);
}

#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VortexLayoutReaderDriverApprovalReport {
    pub status: VortexLayoutReaderDriverApprovalStatus,
    pub mode: VortexLayoutReaderDriverApprovalMode,
    pub input: VortexLayoutReaderDriverApprovalInput,
    pub layout_reader_surface_present: bool,
    pub layout_row_count_surface_present: bool,
    pub runtime_driver_risk_present: bool,
    pub layout_reader_blocker_summary: Option<String>,
    pub layout_reader_constructed: bool,
    pub runtime_driver_started: bool,
    pub scan_called: bool,
    pub evaluation_called: bool,
    pub data_read: bool,
    pub row_read: bool,
    pub data_decoded: bool,
    pub data_materialized: bool,
    pub arrow_converted: bool,
    pub object_store_io: bool,
    pub write_io: bool,
    pub fallback_execution_allowed: bool,
    pub diagnostics: Vec<Diagnostic>,
}
impl VortexLayoutReaderDriverApprovalReport {
    /// # Errors
    /// Returns an error when deterministic report construction fails.
    pub fn from_input(input: VortexLayoutReaderDriverApprovalInput) -> Result<Self> {
        let layout_reader = item_named(&input.api_boundary_report, "VortexFile::layout_reader");
        let layout_row_count = item_named(&input.api_boundary_report, "LayoutReader::row_count");
        let layout_reader_surface_present = layout_reader.is_some();
        let layout_row_count_surface_present = layout_row_count.is_some();
        let runtime_driver_risk_present =
            layout_reader.is_some_and(|item| item.risk == VortexEncodedReadApiRisk::RuntimeDriver);
        let layout_reader_blocker_summary = layout_reader
            .filter(|item| item.is_blocked())
            .map(VortexEncodedReadApiItem::summary);

        let status = derive_status(
            &input,
            layout_reader_surface_present,
            layout_row_count_surface_present,
            runtime_driver_risk_present,
        );
        let mode = if status.approved() {
            VortexLayoutReaderDriverApprovalMode::LayoutRowCountOnlyApproval
        } else if status.is_error() {
            VortexLayoutReaderDriverApprovalMode::Unsupported
        } else {
            VortexLayoutReaderDriverApprovalMode::ReportOnly
        };

        let mut report = Self {
            status,
            mode,
            input,
            layout_reader_surface_present,
            layout_row_count_surface_present,
            runtime_driver_risk_present,
            layout_reader_blocker_summary,
            layout_reader_constructed: false,
            runtime_driver_started: false,
            scan_called: false,
            evaluation_called: false,
            data_read: false,
            row_read: false,
            data_decoded: false,
            data_materialized: false,
            arrow_converted: false,
            object_store_io: false,
            write_io: false,
            fallback_execution_allowed: false,
            diagnostics: vec![],
        };
        if report.status.is_error() {
            report.diagnostics.push(Diagnostic::unsupported(
                DiagnosticCode::NotImplemented,
                "vortex_layout_reader_driver_approval",
                "Layout-reader row-count construction is not approved for execution.",
                Some("Fallback attempted: false".to_string()),
            ));
        }
        Ok(report)
    }

    pub const fn approved(&self) -> bool {
        self.status.approved()
    }

    pub fn has_errors(&self) -> bool {
        self.status.is_error()
            || self.diagnostics.iter().any(|diagnostic| {
                matches!(
                    diagnostic.severity,
                    DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
                )
            })
    }

    pub const fn is_side_effect_free(&self) -> bool {
        !self.layout_reader_constructed
            && !self.runtime_driver_started
            && !self.scan_called
            && !self.evaluation_called
            && !self.data_read
            && !self.row_read
            && !self.data_decoded
            && !self.data_materialized
            && !self.arrow_converted
            && !self.object_store_io
            && !self.write_io
            && !self.fallback_execution_allowed
    }

    pub fn to_human_text(&self) -> String {
        let mut out = String::new();
        let _ = writeln!(out, "status={}", self.status.as_str());
        let _ = writeln!(out, "mode={}", self.mode.as_str());
        let _ = writeln!(
            out,
            "layout_reader_surface_present={}",
            self.layout_reader_surface_present
        );
        let _ = writeln!(
            out,
            "layout_row_count_surface_present={}",
            self.layout_row_count_surface_present
        );
        let _ = writeln!(
            out,
            "runtime_driver_risk_present={}",
            self.runtime_driver_risk_present
        );
        let _ = writeln!(out, "layout_reader_constructed=false");
        let _ = writeln!(out, "runtime_driver_started=false");
        let _ = writeln!(out, "scan_called=false");
        let _ = writeln!(out, "data_read=false");
        let _ = writeln!(out, "fallback_execution_allowed=false");
        if let Some(summary) = &self.layout_reader_blocker_summary {
            let _ = writeln!(out, "layout_reader_blocker={summary}");
        }
        out
    }
}

fn item_named<'a>(
    report: &'a VortexEncodedReadApiBoundaryReport,
    name: &str,
) -> Option<&'a VortexEncodedReadApiItem> {
    report.items.iter().find(|item| item.name == name)
}

fn has_all_safe_intent_signals(input: &VortexLayoutReaderDriverApprovalInput) -> bool {
    use VortexLayoutReaderDriverApprovalSignal as S;
    [
        S::CallerSessionAllowed,
        S::LayoutRowCountOnlyIntent,
        S::ScanForbidden,
        S::EvaluationForbidden,
        S::DataReadForbidden,
        S::DecodeForbidden,
        S::MaterializationForbidden,
        S::ArrowForbidden,
        S::ObjectStoreForbidden,
        S::WriteForbidden,
        S::FallbackForbidden,
    ]
    .into_iter()
    .all(|signal| input.has_signal(signal))
}

fn derive_status(
    input: &VortexLayoutReaderDriverApprovalInput,
    layout_reader_surface_present: bool,
    layout_row_count_surface_present: bool,
    runtime_driver_risk_present: bool,
) -> VortexLayoutReaderDriverApprovalStatus {
    use VortexLayoutReaderDriverApprovalSignal as S;
    if !layout_reader_surface_present {
        return VortexLayoutReaderDriverApprovalStatus::BlockedByMissingLayoutReader;
    }
    if !layout_row_count_surface_present {
        return VortexLayoutReaderDriverApprovalStatus::BlockedByMissingLayoutRowCount;
    }
    if !input.has_signal(S::LocalFixtureOnly) {
        return VortexLayoutReaderDriverApprovalStatus::BlockedByNonLocalScope;
    }
    if !has_all_safe_intent_signals(input) {
        return VortexLayoutReaderDriverApprovalStatus::BlockedByExecutionRequest;
    }
    if runtime_driver_risk_present && !input.has_signal(S::RuntimeDriverStartAllowed) {
        return VortexLayoutReaderDriverApprovalStatus::BlockedByRuntimeDriverPolicy;
    }
    VortexLayoutReaderDriverApprovalStatus::ApprovedForLayoutRowCountOnly
}

/// # Errors
/// Returns any deterministic report construction failure.
pub fn plan_vortex_layout_reader_driver_approval(
    input: VortexLayoutReaderDriverApprovalInput,
) -> Result<VortexLayoutReaderDriverApprovalReport> {
    VortexLayoutReaderDriverApprovalReport::from_input(input)
}

pub const fn vortex_layout_reader_driver_approval_is_side_effect_free(
    report: &VortexLayoutReaderDriverApprovalReport,
) -> bool {
    report.is_side_effect_free()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        VortexEncodedReadApiArea, VortexEncodedReadApiBoundaryReport, VortexEncodedReadApiItem,
        VortexEncodedReadApiStatus, vortex_encoded_read_public_api_boundary,
    };

    fn safe_intent_input(
        api: VortexEncodedReadApiBoundaryReport,
    ) -> VortexLayoutReaderDriverApprovalInput {
        VortexLayoutReaderDriverApprovalInput::new(api)
            .local_fixture_only(true)
            .caller_session_allowed(true)
            .layout_row_count_only_intent(true)
            .scan_forbidden(true)
            .evaluation_forbidden(true)
            .data_read_forbidden(true)
            .decode_forbidden(true)
            .materialization_forbidden(true)
            .arrow_forbidden(true)
            .object_store_forbidden(true)
            .write_forbidden(true)
            .fallback_forbidden(true)
    }

    #[test]
    fn current_public_api_blocks_without_runtime_driver_approval() {
        let report = plan_vortex_layout_reader_driver_approval(safe_intent_input(
            vortex_encoded_read_public_api_boundary(),
        ))
        .expect("report");

        assert_eq!(
            report.status,
            VortexLayoutReaderDriverApprovalStatus::BlockedByRuntimeDriverPolicy
        );
        assert!(report.runtime_driver_risk_present);
        assert!(report.layout_reader_surface_present);
        assert!(report.layout_row_count_surface_present);
        assert!(report.has_errors());
        assert!(report.is_side_effect_free());
        assert!(!report.layout_reader_constructed);
        assert!(!report.runtime_driver_started);
        assert!(!report.scan_called);
        assert!(!report.data_read);
        assert!(!report.fallback_execution_allowed);
        assert!(
            report
                .layout_reader_blocker_summary
                .as_deref()
                .is_some_and(|summary| summary.contains("runtime_driver"))
        );
    }

    #[test]
    fn missing_layout_row_count_surface_blocks() {
        let api = VortexEncodedReadApiBoundaryReport::from_items(vec![
            VortexEncodedReadApiItem::new(
                VortexEncodedReadApiArea::Layout,
                "VortexFile::layout_reader",
                VortexEncodedReadApiStatus::ConfirmedPublicButDeferred,
            )
            .expect("item")
            .with_risk(VortexEncodedReadApiRisk::RuntimeDriver),
        ]);
        let report =
            plan_vortex_layout_reader_driver_approval(safe_intent_input(api)).expect("report");

        assert_eq!(
            report.status,
            VortexLayoutReaderDriverApprovalStatus::BlockedByMissingLayoutRowCount
        );
        assert!(report.has_errors());
        assert!(report.is_side_effect_free());
    }

    #[test]
    fn approved_report_still_constructs_nothing() {
        let input = safe_intent_input(vortex_encoded_read_public_api_boundary())
            .runtime_driver_start_allowed(true);

        let report = plan_vortex_layout_reader_driver_approval(input).expect("report");

        assert_eq!(
            report.status,
            VortexLayoutReaderDriverApprovalStatus::ApprovedForLayoutRowCountOnly
        );
        assert!(report.approved());
        assert!(!report.has_errors());
        assert!(vortex_layout_reader_driver_approval_is_side_effect_free(
            &report
        ));
        assert!(!report.layout_reader_constructed);
        assert!(!report.runtime_driver_started);
        assert!(!report.data_read);
        assert!(!report.fallback_execution_allowed);
        assert!(
            report
                .to_human_text()
                .contains("layout_reader_constructed=false")
        );
    }

    #[test]
    fn missing_no_fallback_intent_blocks_approval() {
        let input =
            VortexLayoutReaderDriverApprovalInput::new(vortex_encoded_read_public_api_boundary())
                .local_fixture_only(true)
                .caller_session_allowed(true)
                .runtime_driver_start_allowed(true)
                .layout_row_count_only_intent(true)
                .scan_forbidden(true)
                .evaluation_forbidden(true)
                .data_read_forbidden(true)
                .decode_forbidden(true)
                .materialization_forbidden(true)
                .arrow_forbidden(true)
                .object_store_forbidden(true)
                .write_forbidden(true);

        let report = plan_vortex_layout_reader_driver_approval(input).expect("report");

        assert_eq!(
            report.status,
            VortexLayoutReaderDriverApprovalStatus::BlockedByExecutionRequest
        );
        assert!(report.has_errors());
        assert!(report.is_side_effect_free());
    }
}
