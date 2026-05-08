use std::fmt::Write as _;

use shardloom_core::{Diagnostic, DiagnosticCode, DiagnosticSeverity, Result, ShardLoomError};

use crate::{
    VortexCountCandidateSource, VortexCountReadinessReport, VortexCountReadinessStatus,
    VortexEncodedReadApiArea, VortexEncodedReadApiBoundaryReport,
    VortexEncodedReadApiBoundaryStatus, VortexEncodedReadApiItem, VortexEncodedReadApiRisk,
    VortexLayoutReaderDriverApprovalReport,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexEncodedCountDataPathApprovalStatus {
    ApprovedForDeferredCount,
    BlockedByCountReadiness,
    BlockedByApiBoundary,
    BlockedByMissingExecutionUsableDataPath,
    BlockedByDecodeRisk,
    BlockedByMaterializationRisk,
    BlockedByArrowDefaultRisk,
    BlockedByObjectStoreIo,
    BlockedByWriteIo,
    BlockedByFallbackPolicy,
    BlockedByLayoutDriverApproval,
    Unsupported,
}
impl VortexEncodedCountDataPathApprovalStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ApprovedForDeferredCount => "approved_for_deferred_count",
            Self::BlockedByCountReadiness => "blocked_by_count_readiness",
            Self::BlockedByApiBoundary => "blocked_by_api_boundary",
            Self::BlockedByMissingExecutionUsableDataPath => {
                "blocked_by_missing_execution_usable_data_path"
            }
            Self::BlockedByDecodeRisk => "blocked_by_decode_risk",
            Self::BlockedByMaterializationRisk => "blocked_by_materialization_risk",
            Self::BlockedByArrowDefaultRisk => "blocked_by_arrow_default_risk",
            Self::BlockedByObjectStoreIo => "blocked_by_object_store_io",
            Self::BlockedByWriteIo => "blocked_by_write_io",
            Self::BlockedByFallbackPolicy => "blocked_by_fallback_policy",
            Self::BlockedByLayoutDriverApproval => "blocked_by_layout_driver_approval",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn is_error(self) -> bool {
        !matches!(self, Self::ApprovedForDeferredCount)
    }
    #[must_use]
    pub const fn approved(self) -> bool {
        matches!(self, Self::ApprovedForDeferredCount)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexEncodedCountDataPathApprovalMode {
    ApprovalOnly,
    Blocked,
    Unsupported,
}
impl VortexEncodedCountDataPathApprovalMode {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ApprovalOnly => "approval_only",
            Self::Blocked => "blocked",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn executes_count(self) -> bool {
        false
    }
    #[must_use]
    pub const fn reads_encoded_data(self) -> bool {
        false
    }
    #[must_use]
    pub const fn reads_rows(self) -> bool {
        false
    }
    #[must_use]
    pub const fn decodes_data(self) -> bool {
        false
    }
    #[must_use]
    pub const fn materializes_data(self) -> bool {
        false
    }
    #[must_use]
    pub const fn converts_to_arrow(self) -> bool {
        false
    }
    #[must_use]
    pub const fn performs_object_store_io(self) -> bool {
        false
    }
    #[must_use]
    pub const fn writes_data(self) -> bool {
        false
    }
    #[must_use]
    pub const fn calls_upstream_scan(self) -> bool {
        false
    }
    #[must_use]
    pub const fn fallback_execution_allowed(self) -> bool {
        false
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VortexEncodedCountDataPathApprovalInput {
    pub count_readiness_report: VortexCountReadinessReport,
    pub api_boundary_report: VortexEncodedReadApiBoundaryReport,
    pub layout_driver_approval_report: Option<VortexLayoutReaderDriverApprovalReport>,
    pub require_execution_usable_data_path: bool,
    pub diagnostics: Vec<Diagnostic>,
}
impl VortexEncodedCountDataPathApprovalInput {
    #[must_use]
    pub const fn new(
        count_readiness_report: VortexCountReadinessReport,
        api_boundary_report: VortexEncodedReadApiBoundaryReport,
    ) -> Self {
        Self {
            count_readiness_report,
            api_boundary_report,
            layout_driver_approval_report: None,
            require_execution_usable_data_path: true,
            diagnostics: vec![],
        }
    }
    #[must_use]
    pub fn with_layout_driver_approval(
        mut self,
        report: VortexLayoutReaderDriverApprovalReport,
    ) -> Self {
        self.layout_driver_approval_report = Some(report);
        self
    }

    #[must_use]
    pub fn require_execution_usable_data_path(mut self, value: bool) -> Self {
        self.require_execution_usable_data_path = value;
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
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct VortexEncodedCountDataPathApprovalReport {
    pub status: VortexEncodedCountDataPathApprovalStatus,
    pub mode: VortexEncodedCountDataPathApprovalMode,
    pub input: VortexEncodedCountDataPathApprovalInput,
    pub metadata_count_surface_ready: bool,
    pub execution_usable_data_path_count: usize,
    pub api_boundary_blockers: Vec<String>,
    pub layout_driver_approval_status: Option<String>,
    pub layout_row_count_path_approved: bool,
    pub count_executed: bool,
    pub encoded_data_read: bool,
    pub row_read: bool,
    pub array_decoded: bool,
    pub values_materialized: bool,
    pub arrow_converted: bool,
    pub object_store_io: bool,
    pub data_written: bool,
    pub upstream_scan_called: bool,
    pub fallback_execution_allowed: bool,
    pub diagnostics: Vec<Diagnostic>,
}
impl VortexEncodedCountDataPathApprovalReport {
    /// # Errors
    /// Returns an error if approval report construction fails.
    pub fn from_input(input: VortexEncodedCountDataPathApprovalInput) -> Result<Self> {
        let metadata_count_surface_ready =
            has_metadata_count_surface(&input.api_boundary_report.items);
        let layout_row_count_path_approved = layout_driver_row_count_path_approved(&input);
        let execution_usable_data_path_count = input.api_boundary_report.execution_usable_count
            + usize::from(layout_row_count_path_approved);
        let api_boundary_blockers = input
            .api_boundary_report
            .items
            .iter()
            .filter(|item| item.is_blocked())
            .map(VortexEncodedReadApiItem::summary)
            .collect::<Vec<_>>();
        let status = derive_status(
            &input,
            execution_usable_data_path_count,
            &api_boundary_blockers,
        );
        let layout_driver_approval_status = input
            .layout_driver_approval_report
            .as_ref()
            .map(|report| report.status.as_str().to_string());
        let mode = derive_mode(status);
        let mut diagnostics = input.diagnostics.clone();
        diagnostics.extend(input.count_readiness_report.diagnostics.clone());
        diagnostics.extend(input.api_boundary_report.diagnostics.clone());
        if status.is_error() {
            diagnostics.push(Diagnostic::unsupported(
                DiagnosticCode::NotImplemented,
                "vortex_encoded_count_data_path_approval",
                format!(
                    "Encoded-data count approval is not available: {}",
                    status.as_str()
                ),
                Some("No count execution, encoded data read, scan, decode, materialization, Arrow conversion, object-store IO, write IO, or fallback execution was attempted.".to_string()),
            ));
        }
        Ok(Self {
            status,
            mode,
            input,
            metadata_count_surface_ready,
            execution_usable_data_path_count,
            api_boundary_blockers,
            layout_driver_approval_status,
            layout_row_count_path_approved,
            count_executed: false,
            encoded_data_read: false,
            row_read: false,
            array_decoded: false,
            values_materialized: false,
            arrow_converted: false,
            object_store_io: false,
            data_written: false,
            upstream_scan_called: false,
            fallback_execution_allowed: false,
            diagnostics,
        })
    }
    #[must_use]
    pub fn approved(&self) -> bool {
        self.status.approved()
    }
    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.status.is_error()
            || self.input.has_errors()
            || self.input.count_readiness_report.request.has_errors()
            || self.diagnostics.iter().any(|d| {
                matches!(
                    d.severity,
                    DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
                )
            })
    }
    #[must_use]
    pub const fn is_side_effect_free(&self) -> bool {
        !self.count_executed
            && !self.encoded_data_read
            && !self.row_read
            && !self.array_decoded
            && !self.values_materialized
            && !self.arrow_converted
            && !self.object_store_io
            && !self.data_written
            && !self.upstream_scan_called
            && !self.fallback_execution_allowed
    }
    #[must_use]
    pub fn to_human_text(&self) -> String {
        let mut out = String::new();
        let _ = writeln!(
            &mut out,
            "encoded count data-path approval status={}",
            self.status.as_str()
        );
        let _ = writeln!(&mut out, "mode={}", self.mode.as_str());
        let _ = writeln!(
            &mut out,
            "metadata_count_surface_ready={}",
            self.metadata_count_surface_ready
        );
        let _ = writeln!(
            &mut out,
            "execution_usable_data_path_count={}",
            self.execution_usable_data_path_count
        );
        let _ = writeln!(
            &mut out,
            "api_boundary_blocker_count={}",
            self.api_boundary_blockers.len()
        );
        let _ = writeln!(
            &mut out,
            "layout_driver_approval_status={}",
            self.layout_driver_approval_status
                .as_deref()
                .unwrap_or("absent")
        );
        let _ = writeln!(
            &mut out,
            "layout_row_count_path_approved={}",
            self.layout_row_count_path_approved
        );
        let _ = writeln!(&mut out, "count_executed={}", self.count_executed);
        let _ = writeln!(&mut out, "encoded_data_read={}", self.encoded_data_read);
        let _ = writeln!(&mut out, "row_read={}", self.row_read);
        let _ = writeln!(&mut out, "array_decoded={}", self.array_decoded);
        let _ = writeln!(&mut out, "values_materialized={}", self.values_materialized);
        let _ = writeln!(&mut out, "arrow_converted={}", self.arrow_converted);
        let _ = writeln!(&mut out, "object_store_io={}", self.object_store_io);
        let _ = writeln!(&mut out, "data_written={}", self.data_written);
        let _ = writeln!(
            &mut out,
            "upstream_scan_called={}",
            self.upstream_scan_called
        );
        let _ = writeln!(
            &mut out,
            "fallback_execution_allowed={}",
            self.fallback_execution_allowed
        );
        out
    }
}

fn has_metadata_count_surface(items: &[VortexEncodedReadApiItem]) -> bool {
    items.iter().any(|item| {
        item.name == "VortexFile::row_count"
            && item.area == VortexEncodedReadApiArea::FileMetadata
            && item.is_contract_usable()
            && item.risk == VortexEncodedReadApiRisk::None
    })
}

fn derive_status(
    input: &VortexEncodedCountDataPathApprovalInput,
    execution_usable_data_path_count: usize,
    api_boundary_blockers: &[String],
) -> VortexEncodedCountDataPathApprovalStatus {
    let readiness = &input.count_readiness_report;
    if readiness
        .request
        .has_signal(crate::VortexCountReadinessSignal::FallbackPolicyBlocked)
    {
        return VortexEncodedCountDataPathApprovalStatus::BlockedByFallbackPolicy;
    }
    if readiness.status != VortexCountReadinessStatus::CountReady
        || readiness.request.candidate_source != VortexCountCandidateSource::EncodedDataPath
        || !readiness.encoded_data_path_ready()
        || readiness.has_errors()
    {
        return VortexEncodedCountDataPathApprovalStatus::BlockedByCountReadiness;
    }
    let api = &input.api_boundary_report;
    if api.fallback_execution_allowed {
        return VortexEncodedCountDataPathApprovalStatus::BlockedByFallbackPolicy;
    }
    if input.layout_driver_approval_report.is_some() {
        if !layout_driver_row_count_path_approved(input) {
            return VortexEncodedCountDataPathApprovalStatus::BlockedByLayoutDriverApproval;
        }
        return VortexEncodedCountDataPathApprovalStatus::ApprovedForDeferredCount;
    }
    if api.has_errors()
        || matches!(
            api.status,
            VortexEncodedReadApiBoundaryStatus::BlockedByRisk
                | VortexEncodedReadApiBoundaryStatus::Unsupported
        )
        || !api_boundary_blockers.is_empty()
    {
        return VortexEncodedCountDataPathApprovalStatus::BlockedByApiBoundary;
    }
    if api.decode_api_count > 0 {
        return VortexEncodedCountDataPathApprovalStatus::BlockedByDecodeRisk;
    }
    if api.materialization_api_count > 0 {
        return VortexEncodedCountDataPathApprovalStatus::BlockedByMaterializationRisk;
    }
    if api.arrow_default_risk_count > 0 {
        return VortexEncodedCountDataPathApprovalStatus::BlockedByArrowDefaultRisk;
    }
    if api.object_store_api_count > 0 {
        return VortexEncodedCountDataPathApprovalStatus::BlockedByObjectStoreIo;
    }
    if api.write_api_count > 0 {
        return VortexEncodedCountDataPathApprovalStatus::BlockedByWriteIo;
    }
    if input.require_execution_usable_data_path && execution_usable_data_path_count == 0 {
        return VortexEncodedCountDataPathApprovalStatus::BlockedByMissingExecutionUsableDataPath;
    }
    VortexEncodedCountDataPathApprovalStatus::ApprovedForDeferredCount
}

fn layout_driver_row_count_path_approved(input: &VortexEncodedCountDataPathApprovalInput) -> bool {
    input
        .layout_driver_approval_report
        .as_ref()
        .is_some_and(|report| {
            report.approved()
                && report.is_side_effect_free()
                && !report.has_errors()
                && !report.fallback_execution_allowed
                && report.input.api_boundary_report == input.api_boundary_report
        })
}

const fn derive_mode(
    status: VortexEncodedCountDataPathApprovalStatus,
) -> VortexEncodedCountDataPathApprovalMode {
    match status {
        VortexEncodedCountDataPathApprovalStatus::ApprovedForDeferredCount => {
            VortexEncodedCountDataPathApprovalMode::ApprovalOnly
        }
        VortexEncodedCountDataPathApprovalStatus::Unsupported => {
            VortexEncodedCountDataPathApprovalMode::Unsupported
        }
        _ => VortexEncodedCountDataPathApprovalMode::Blocked,
    }
}

/// # Errors
/// Returns an error if approval report construction fails.
pub fn plan_vortex_encoded_count_data_path_approval(
    count_readiness_report: VortexCountReadinessReport,
    api_boundary_report: VortexEncodedReadApiBoundaryReport,
) -> Result<VortexEncodedCountDataPathApprovalReport> {
    VortexEncodedCountDataPathApprovalReport::from_input(
        VortexEncodedCountDataPathApprovalInput::new(count_readiness_report, api_boundary_report),
    )
    .map_err(|e| {
        ShardLoomError::NotImplemented(format!(
            "encoded-count data-path approval planning failed: {e}"
        ))
    })
}

/// # Errors
/// Returns an error if encoded-count approval report construction fails.
pub fn plan_vortex_encoded_count_data_path_approval_with_layout_driver(
    count_readiness_report: VortexCountReadinessReport,
    api_boundary_report: VortexEncodedReadApiBoundaryReport,
    layout_driver_approval_report: VortexLayoutReaderDriverApprovalReport,
) -> Result<VortexEncodedCountDataPathApprovalReport> {
    VortexEncodedCountDataPathApprovalReport::from_input(
        VortexEncodedCountDataPathApprovalInput::new(count_readiness_report, api_boundary_report)
            .with_layout_driver_approval(layout_driver_approval_report),
    )
    .map_err(|e| {
        ShardLoomError::NotImplemented(format!(
            "encoded-count layout-driver approval planning failed: {e}"
        ))
    })
}

#[must_use]
pub fn vortex_encoded_count_data_path_approval_is_side_effect_free(
    report: &VortexEncodedCountDataPathApprovalReport,
) -> bool {
    report.is_side_effect_free()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        VortexCountReadinessRequest, VortexEncodedReadApiStatus,
        VortexLayoutReaderDriverApprovalInput, plan_vortex_count_readiness,
        plan_vortex_layout_reader_driver_approval, vortex_encoded_read_public_api_boundary,
    };
    use shardloom_core::DatasetUri;

    fn uri() -> DatasetUri {
        DatasetUri::new("file://tmp/count.vortex").expect("uri")
    }

    fn encoded_count_ready_report() -> VortexCountReadinessReport {
        plan_vortex_count_readiness(
            VortexCountReadinessRequest::new(uri(), VortexCountCandidateSource::EncodedDataPath)
                .feature_gate_enabled(true)
                .query_primitive_ready(true)
                .count_primitive(true)
                .encoded_data_path_ready(true),
        )
        .expect("count readiness")
    }

    fn approved_layout_driver_report(
        api: VortexEncodedReadApiBoundaryReport,
    ) -> VortexLayoutReaderDriverApprovalReport {
        plan_vortex_layout_reader_driver_approval(
            VortexLayoutReaderDriverApprovalInput::new(api)
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
                .write_forbidden(true)
                .fallback_forbidden(true),
        )
        .expect("layout driver approval")
    }

    #[test]
    fn current_public_api_blocks_encoded_count_despite_metadata_row_count_surface() {
        let report = plan_vortex_encoded_count_data_path_approval(
            encoded_count_ready_report(),
            vortex_encoded_read_public_api_boundary(),
        )
        .expect("approval");

        assert_eq!(
            report.status,
            VortexEncodedCountDataPathApprovalStatus::BlockedByApiBoundary
        );
        assert!(report.metadata_count_surface_ready);
        assert_eq!(report.execution_usable_data_path_count, 0);
        assert!(!report.approved());
        assert!(report.has_errors());
        assert!(report.is_side_effect_free());
        assert!(!report.encoded_data_read);
        assert!(!report.upstream_scan_called);
        assert!(!report.fallback_execution_allowed);
        assert!(
            report
                .api_boundary_blockers
                .iter()
                .any(|b| b.contains("VortexFile::scan"))
        );
        assert!(
            report
                .api_boundary_blockers
                .iter()
                .any(|b| b.contains("VortexFile::layout_reader"))
        );
        assert!(
            report
                .api_boundary_blockers
                .iter()
                .any(|b| b.contains("ScanBuilder::into_array_stream"))
        );
        assert!(
            report
                .api_boundary_blockers
                .iter()
                .all(|b| !b.contains("VortexFile::row_count"))
        );
    }

    #[test]
    fn metadata_row_count_item_alone_is_not_encoded_data_path_approval() {
        let api = VortexEncodedReadApiBoundaryReport::from_items(vec![
            VortexEncodedReadApiItem::new(
                VortexEncodedReadApiArea::FileMetadata,
                "VortexFile::row_count",
                VortexEncodedReadApiStatus::ConfirmedPublic,
            )
            .expect("item"),
        ]);

        let report =
            plan_vortex_encoded_count_data_path_approval(encoded_count_ready_report(), api)
                .expect("approval");

        assert_eq!(
            report.status,
            VortexEncodedCountDataPathApprovalStatus::BlockedByMissingExecutionUsableDataPath
        );
        assert!(report.metadata_count_surface_ready);
        assert_eq!(report.execution_usable_data_path_count, 0);
        assert!(!report.approved());
        assert!(report.is_side_effect_free());
    }

    #[test]
    fn approved_layout_driver_row_count_path_allows_deferred_count_approval() {
        let api = vortex_encoded_read_public_api_boundary();
        let layout = approved_layout_driver_report(api.clone());

        let report = plan_vortex_encoded_count_data_path_approval_with_layout_driver(
            encoded_count_ready_report(),
            api,
            layout,
        )
        .expect("approval");

        assert_eq!(
            report.status,
            VortexEncodedCountDataPathApprovalStatus::ApprovedForDeferredCount
        );
        assert!(report.approved());
        assert!(report.layout_row_count_path_approved);
        assert_eq!(
            report.layout_driver_approval_status.as_deref(),
            Some("approved_for_layout_row_count_only")
        );
        assert_eq!(report.execution_usable_data_path_count, 1);
        assert!(!report.api_boundary_blockers.is_empty());
        assert!(!report.count_executed);
        assert!(!report.encoded_data_read);
        assert!(!report.row_read);
        assert!(!report.array_decoded);
        assert!(!report.values_materialized);
        assert!(!report.upstream_scan_called);
        assert!(!report.fallback_execution_allowed);
        assert!(report.is_side_effect_free());
        assert!(
            report
                .to_human_text()
                .contains("layout_driver_approval_status=approved_for_layout_row_count_only")
        );
    }

    #[test]
    fn mismatched_layout_driver_approval_blocks_deferred_count_approval() {
        let api = vortex_encoded_read_public_api_boundary();
        let layout = approved_layout_driver_report(api);

        let report = plan_vortex_encoded_count_data_path_approval_with_layout_driver(
            encoded_count_ready_report(),
            VortexEncodedReadApiBoundaryReport::default_deferred(),
            layout,
        )
        .expect("approval");

        assert_eq!(
            report.status,
            VortexEncodedCountDataPathApprovalStatus::BlockedByLayoutDriverApproval
        );
        assert!(!report.approved());
        assert!(!report.layout_row_count_path_approved);
        assert_eq!(report.execution_usable_data_path_count, 0);
        assert!(report.has_errors());
        assert!(report.is_side_effect_free());
    }

    #[test]
    fn non_encoded_count_readiness_blocks_approval() {
        let readiness = plan_vortex_count_readiness(
            VortexCountReadinessRequest::new(uri(), VortexCountCandidateSource::MetadataFooter)
                .feature_gate_enabled(true)
                .query_primitive_ready(true)
                .count_primitive(true)
                .metadata_footer_ready(true),
        )
        .expect("readiness");

        let report = plan_vortex_encoded_count_data_path_approval(
            readiness,
            vortex_encoded_read_public_api_boundary(),
        )
        .expect("approval");

        assert_eq!(
            report.status,
            VortexEncodedCountDataPathApprovalStatus::BlockedByCountReadiness
        );
        assert!(report.has_errors());
        assert!(report.is_side_effect_free());
    }

    #[test]
    fn text_and_mode_flags_remain_report_only() {
        let report = plan_vortex_encoded_count_data_path_approval(
            encoded_count_ready_report(),
            vortex_encoded_read_public_api_boundary(),
        )
        .expect("approval");
        let text = report.to_human_text();

        assert!(text.contains("fallback_execution_allowed=false"));
        assert!(text.contains("encoded_data_read=false"));
        assert!(!report.mode.executes_count());
        assert!(!report.mode.reads_encoded_data());
        assert!(!report.mode.reads_rows());
        assert!(!report.mode.decodes_data());
        assert!(!report.mode.materializes_data());
        assert!(!report.mode.converts_to_arrow());
        assert!(!report.mode.performs_object_store_io());
        assert!(!report.mode.writes_data());
        assert!(!report.mode.calls_upstream_scan());
        assert!(!report.mode.fallback_execution_allowed());
    }
}
