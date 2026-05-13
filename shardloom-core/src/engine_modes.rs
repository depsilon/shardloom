//! CG-22 engine-mode contracts and report-only selection surfaces.
//!
//! This module is vocabulary and planning evidence only. It does not read data,
//! run streaming loops, write checkpoints, flush segments, invoke brokers, call
//! object stores, or delegate to external engines.

use crate::{Diagnostic, DiagnosticCode, FallbackStatus, Result, ShardLoomError};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EngineMode {
    Batch,
    Live,
    Hybrid,
    Auto,
}

impl EngineMode {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Batch => "batch",
            Self::Live => "live",
            Self::Hybrid => "hybrid",
            Self::Auto => "auto",
        }
    }

    #[must_use]
    pub const fn all() -> [Self; 4] {
        [Self::Batch, Self::Live, Self::Hybrid, Self::Auto]
    }

    /// Parses user/API engine-mode vocabulary.
    ///
    /// # Errors
    /// Returns `ShardLoomError::InvalidOperation` for unknown modes.
    pub fn parse(value: &str) -> Result<Self> {
        match normalized_token(value).as_str() {
            "batch" => Ok(Self::Batch),
            "live" => Ok(Self::Live),
            "hybrid" => Ok(Self::Hybrid),
            "auto" => Ok(Self::Auto),
            other => Err(ShardLoomError::InvalidOperation(format!(
                "unknown engine mode: {other}"
            ))),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Boundedness {
    Bounded,
    Unbounded,
    Snapshot,
    Unknown,
}

impl Boundedness {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Bounded => "bounded",
            Self::Unbounded => "unbounded",
            Self::Snapshot => "snapshot",
            Self::Unknown => "unknown",
        }
    }

    #[must_use]
    pub const fn all() -> [Self; 4] {
        [
            Self::Bounded,
            Self::Unbounded,
            Self::Snapshot,
            Self::Unknown,
        ]
    }

    /// Parses input boundedness vocabulary.
    ///
    /// # Errors
    /// Returns `ShardLoomError::InvalidOperation` for unknown values.
    pub fn parse(value: &str) -> Result<Self> {
        match normalized_token(value).as_str() {
            "bounded" => Ok(Self::Bounded),
            "unbounded" => Ok(Self::Unbounded),
            "snapshot" | "bounded-snapshot" => Ok(Self::Snapshot),
            "unknown" => Ok(Self::Unknown),
            other => Err(ShardLoomError::InvalidOperation(format!(
                "unknown boundedness: {other}"
            ))),
        }
    }

    #[must_use]
    pub const fn is_batch_compatible(self) -> bool {
        matches!(self, Self::Bounded | Self::Snapshot)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpdateMode {
    Snapshot,
    AppendOnly,
    Upsert,
    Delete,
    Retract,
    Tombstone,
    Changelog,
}

impl UpdateMode {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Snapshot => "snapshot",
            Self::AppendOnly => "append_only",
            Self::Upsert => "upsert",
            Self::Delete => "delete",
            Self::Retract => "retract",
            Self::Tombstone => "tombstone",
            Self::Changelog => "changelog",
        }
    }

    #[must_use]
    pub const fn all() -> [Self; 7] {
        [
            Self::Snapshot,
            Self::AppendOnly,
            Self::Upsert,
            Self::Delete,
            Self::Retract,
            Self::Tombstone,
            Self::Changelog,
        ]
    }

    /// Parses update/change semantics vocabulary.
    ///
    /// # Errors
    /// Returns `ShardLoomError::InvalidOperation` for unknown values.
    pub fn parse(value: &str) -> Result<Self> {
        match normalized_token(value).as_str() {
            "snapshot" => Ok(Self::Snapshot),
            "append-only" | "append" => Ok(Self::AppendOnly),
            "upsert" => Ok(Self::Upsert),
            "delete" => Ok(Self::Delete),
            "retract" => Ok(Self::Retract),
            "tombstone" => Ok(Self::Tombstone),
            "changelog" => Ok(Self::Changelog),
            other => Err(ShardLoomError::InvalidOperation(format!(
                "unknown update mode: {other}"
            ))),
        }
    }

    #[must_use]
    pub const fn is_batch_compatible(self) -> bool {
        matches!(self, Self::Snapshot)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputMode {
    Snapshot,
    Append,
    Update,
    Complete,
    Changelog,
    ContinuousView,
}

impl OutputMode {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Snapshot => "snapshot",
            Self::Append => "append",
            Self::Update => "update",
            Self::Complete => "complete",
            Self::Changelog => "changelog",
            Self::ContinuousView => "continuous_view",
        }
    }

    #[must_use]
    pub const fn all() -> [Self; 6] {
        [
            Self::Snapshot,
            Self::Append,
            Self::Update,
            Self::Complete,
            Self::Changelog,
            Self::ContinuousView,
        ]
    }

    /// Parses output mode vocabulary.
    ///
    /// # Errors
    /// Returns `ShardLoomError::InvalidOperation` for unknown values.
    pub fn parse(value: &str) -> Result<Self> {
        match normalized_token(value).as_str() {
            "snapshot" => Ok(Self::Snapshot),
            "append" => Ok(Self::Append),
            "update" => Ok(Self::Update),
            "complete" => Ok(Self::Complete),
            "changelog" => Ok(Self::Changelog),
            "continuous-view" | "continuous" => Ok(Self::ContinuousView),
            other => Err(ShardLoomError::InvalidOperation(format!(
                "unknown output mode: {other}"
            ))),
        }
    }

    #[must_use]
    pub const fn is_batch_compatible(self) -> bool {
        matches!(self, Self::Snapshot)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EngineSelectionStatus {
    Selected,
    Rejected,
}

impl EngineSelectionStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Selected => "selected",
            Self::Rejected => "rejected",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EngineSelectionRequest {
    pub requested: EngineMode,
    pub boundedness: Boundedness,
    pub update_mode: UpdateMode,
    pub output_mode: OutputMode,
}

impl EngineSelectionRequest {
    #[must_use]
    pub const fn new(
        requested: EngineMode,
        boundedness: Boundedness,
        update_mode: UpdateMode,
        output_mode: OutputMode,
    ) -> Self {
        Self {
            requested,
            boundedness,
            update_mode,
            output_mode,
        }
    }

    #[must_use]
    pub const fn default_auto_snapshot() -> Self {
        Self::new(
            EngineMode::Auto,
            Boundedness::Snapshot,
            UpdateMode::Snapshot,
            OutputMode::Snapshot,
        )
    }

    #[must_use]
    pub const fn batch_compatible(&self) -> bool {
        self.boundedness.is_batch_compatible()
            && self.update_mode.is_batch_compatible()
            && self.output_mode.is_batch_compatible()
    }

    #[must_use]
    pub const fn live_fixture_compatible(&self) -> bool {
        matches!(
            self.boundedness,
            Boundedness::Bounded | Boundedness::Unbounded
        ) && matches!(
            self.update_mode,
            UpdateMode::AppendOnly
                | UpdateMode::Upsert
                | UpdateMode::Delete
                | UpdateMode::Retract
                | UpdateMode::Tombstone
                | UpdateMode::Changelog
        ) && matches!(
            self.output_mode,
            OutputMode::Append
                | OutputMode::Update
                | OutputMode::Changelog
                | OutputMode::ContinuousView
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct EngineSelectionReport {
    pub schema_version: &'static str,
    pub report_id: &'static str,
    pub request: EngineSelectionRequest,
    pub status: EngineSelectionStatus,
    pub selected: Option<EngineMode>,
    pub allowed_modes: Vec<EngineMode>,
    pub rejected_modes: Vec<EngineMode>,
    pub rejection_reasons: Vec<String>,
    pub fallback: FallbackStatus,
    pub external_engine_invoked: bool,
    pub runtime_execution: bool,
    pub data_read: bool,
    pub write_io: bool,
}

impl EngineSelectionReport {
    #[must_use]
    pub fn evaluate(request: EngineSelectionRequest) -> Self {
        let batch_allowed = request.batch_compatible();
        let live_allowed = request.live_fixture_compatible();
        let mut allowed_modes = Vec::new();
        let mut rejection_reasons = Vec::new();

        if batch_allowed {
            allowed_modes.push(EngineMode::Batch);
        }
        if live_allowed {
            allowed_modes.push(EngineMode::Live);
        }

        append_batch_compatibility_reasons(&request, &mut rejection_reasons);
        append_live_fixture_compatibility_reasons(&request, &mut rejection_reasons);

        let selected = match request.requested {
            EngineMode::Batch | EngineMode::Auto if batch_allowed => Some(EngineMode::Batch),
            EngineMode::Live | EngineMode::Auto if live_allowed => Some(EngineMode::Live),
            _ => None,
        };

        if selected.is_none() {
            append_requested_engine_rejection(&request, &mut rejection_reasons);
        }

        let rejected_modes = EngineMode::all()
            .into_iter()
            .filter(|mode| *mode != EngineMode::Auto && !allowed_modes.contains(mode))
            .collect::<Vec<_>>();

        Self {
            schema_version: "shardloom.engine_selection.v1",
            report_id: "cg22.engine_selection",
            request,
            status: if selected.is_some() {
                EngineSelectionStatus::Selected
            } else {
                EngineSelectionStatus::Rejected
            },
            selected,
            allowed_modes,
            rejected_modes,
            rejection_reasons: dedupe_strings(rejection_reasons),
            fallback: FallbackStatus::disabled_by_policy(),
            external_engine_invoked: false,
            runtime_execution: false,
            data_read: false,
            write_io: false,
        }
    }

    #[must_use]
    pub const fn fallback_attempted(&self) -> bool {
        self.fallback.attempted
    }

    #[must_use]
    pub const fn has_errors(&self) -> bool {
        matches!(self.status, EngineSelectionStatus::Rejected)
    }

    #[must_use]
    pub fn diagnostics(&self) -> Vec<Diagnostic> {
        if !self.has_errors() {
            return vec![];
        }
        vec![Diagnostic::unsupported(
            DiagnosticCode::NotImplemented,
            "engine-selection-plan",
            format!(
                "requested engine mode {} is rejected: {}",
                self.request.requested.as_str(),
                self.rejection_reason_text()
            ),
            Some("Use engine=batch for bounded snapshot workloads, or wait for CG-22 live/hybrid state, checkpoint, delta-overlay, and freshness evidence.".to_string()),
        )]
    }

    #[must_use]
    pub fn selected_text(&self) -> &'static str {
        self.selected.map_or("none", EngineMode::as_str)
    }

    #[must_use]
    pub fn allowed_modes_text(&self) -> String {
        join_modes(&self.allowed_modes)
    }

    #[must_use]
    pub fn rejected_modes_text(&self) -> String {
        join_modes(&self.rejected_modes)
    }

    #[must_use]
    pub fn rejection_reason_text(&self) -> String {
        if self.rejection_reasons.is_empty() {
            "none".to_string()
        } else {
            self.rejection_reasons.join(";")
        }
    }

    #[must_use]
    pub fn to_human_text(&self) -> String {
        format!(
            "requested engine: {}\nselected engine: {}\nselection status: {}\nallowed engines: {}\nrejection reasons: {}\nfallback execution: disabled\nexternal engine invoked: false",
            self.request.requested.as_str(),
            self.selected_text(),
            self.status.as_str(),
            self.allowed_modes_text(),
            self.rejection_reason_text(),
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EngineSupportStatus {
    PartiallySupported,
    Planned,
    Blocked,
}

impl EngineSupportStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PartiallySupported => "partially_supported",
            Self::Planned => "planned",
            Self::Blocked => "blocked",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct EngineCapabilityRow {
    pub engine_mode: EngineMode,
    pub support_status: EngineSupportStatus,
    pub operator_support: Vec<&'static str>,
    pub function_support: Vec<&'static str>,
    pub source_support: Vec<&'static str>,
    pub sink_support: Vec<&'static str>,
    pub bounded_snapshot_support: bool,
    pub append_only_stream_support: bool,
    pub upsert_delete_tombstone_support: bool,
    pub changelog_support: bool,
    pub continuous_materialized_view_support: bool,
    pub global_sort_supported: bool,
    pub unbounded_join_supported: bool,
    pub state_required: bool,
    pub checkpoint_required: bool,
    pub output_modes: Vec<OutputMode>,
    pub production_claim_allowed: bool,
    pub blockers: Vec<&'static str>,
}

impl EngineCapabilityRow {
    #[must_use]
    pub fn output_modes_text(&self) -> String {
        self.output_modes
            .iter()
            .map(|mode| mode.as_str())
            .collect::<Vec<_>>()
            .join(",")
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct EngineCapabilityMatrixReport {
    pub schema_version: &'static str,
    pub report_id: &'static str,
    pub rows: Vec<EngineCapabilityRow>,
    pub fallback: FallbackStatus,
    pub external_engine_invoked: bool,
    pub runtime_execution: bool,
    pub data_read: bool,
    pub write_io: bool,
}

impl EngineCapabilityMatrixReport {
    #[must_use]
    pub fn cg22_contract() -> Self {
        Self {
            schema_version: "shardloom.engine_capability_matrix.v1",
            report_id: "cg22.engine_capability_matrix",
            rows: vec![batch_row(), live_row(), hybrid_row()],
            fallback: FallbackStatus::disabled_by_policy(),
            external_engine_invoked: false,
            runtime_execution: false,
            data_read: false,
            write_io: false,
        }
    }

    #[must_use]
    pub fn row(&self, mode: EngineMode) -> Option<&EngineCapabilityRow> {
        self.rows.iter().find(|row| row.engine_mode == mode)
    }

    #[must_use]
    pub fn partially_supported_count(&self) -> usize {
        self.rows
            .iter()
            .filter(|row| row.support_status == EngineSupportStatus::PartiallySupported)
            .count()
    }

    #[must_use]
    pub fn planned_count(&self) -> usize {
        self.rows
            .iter()
            .filter(|row| row.support_status == EngineSupportStatus::Planned)
            .count()
    }

    #[must_use]
    pub fn blocked_count(&self) -> usize {
        self.rows
            .iter()
            .filter(|row| row.support_status == EngineSupportStatus::Blocked)
            .count()
    }

    #[must_use]
    pub fn live_hybrid_claim_blocked_count(&self) -> usize {
        self.rows
            .iter()
            .filter(|row| {
                matches!(row.engine_mode, EngineMode::Live | EngineMode::Hybrid)
                    && !row.production_claim_allowed
            })
            .count()
    }

    #[must_use]
    pub const fn fallback_attempted(&self) -> bool {
        self.fallback.attempted
    }

    #[must_use]
    pub fn to_human_text(&self) -> String {
        let rows = self
            .rows
            .iter()
            .map(|row| {
                format!(
                    "{}: {} operators={} sources={} sinks={} output_modes={} claim_allowed={}",
                    row.engine_mode.as_str(),
                    row.support_status.as_str(),
                    row.operator_support.join(","),
                    row.source_support.join(","),
                    row.sink_support.join(","),
                    row.output_modes_text(),
                    row.production_claim_allowed,
                )
            })
            .collect::<Vec<_>>()
            .join("\n");
        format!(
            "engine capability matrix\n{rows}\nfallback execution: disabled\nexternal engine invoked: false"
        )
    }
}

#[must_use]
pub fn engine_mode_vocabulary() -> String {
    EngineMode::all()
        .into_iter()
        .map(EngineMode::as_str)
        .collect::<Vec<_>>()
        .join(",")
}

#[must_use]
pub fn boundedness_vocabulary() -> String {
    Boundedness::all()
        .into_iter()
        .map(Boundedness::as_str)
        .collect::<Vec<_>>()
        .join(",")
}

#[must_use]
pub fn update_mode_vocabulary() -> String {
    UpdateMode::all()
        .into_iter()
        .map(UpdateMode::as_str)
        .collect::<Vec<_>>()
        .join(",")
}

#[must_use]
pub fn output_mode_vocabulary() -> String {
    OutputMode::all()
        .into_iter()
        .map(OutputMode::as_str)
        .collect::<Vec<_>>()
        .join(",")
}

fn batch_row() -> EngineCapabilityRow {
    EngineCapabilityRow {
        engine_mode: EngineMode::Batch,
        support_status: EngineSupportStatus::PartiallySupported,
        operator_support: vec![
            "filter",
            "project",
            "count",
            "count_where",
            "filter_project",
        ],
        function_support: vec!["count"],
        source_support: vec!["local_vortex_fixture", "compatibility_source_planning"],
        sink_support: vec!["vortex_output_planning"],
        bounded_snapshot_support: true,
        append_only_stream_support: false,
        upsert_delete_tombstone_support: false,
        changelog_support: false,
        continuous_materialized_view_support: false,
        global_sort_supported: false,
        unbounded_join_supported: false,
        state_required: false,
        checkpoint_required: false,
        output_modes: vec![OutputMode::Snapshot],
        production_claim_allowed: false,
        blockers: vec![
            "workload_correctness_evidence",
            "benchmark_evidence",
            "broad_source_sink_certification",
        ],
    }
}

fn live_row() -> EngineCapabilityRow {
    EngineCapabilityRow {
        engine_mode: EngineMode::Live,
        support_status: EngineSupportStatus::PartiallySupported,
        operator_support: vec![
            "fixture_filter",
            "fixture_project",
            "fixture_count",
            "fixture_count_where",
            "fixture_group_count",
        ],
        function_support: vec!["count", "group_count"],
        source_support: vec!["in_memory_change_fixture"],
        sink_support: vec!["in_memory_output_changelog", "in_memory_continuous_view"],
        bounded_snapshot_support: false,
        append_only_stream_support: true,
        upsert_delete_tombstone_support: true,
        changelog_support: true,
        continuous_materialized_view_support: true,
        global_sort_supported: false,
        unbounded_join_supported: false,
        state_required: true,
        checkpoint_required: true,
        output_modes: vec![
            OutputMode::Append,
            OutputMode::Update,
            OutputMode::Changelog,
            OutputMode::ContinuousView,
        ],
        production_claim_allowed: false,
        blockers: vec![
            "external_broker_adapters",
            "durable_checkpoint_store",
            "unbounded_runtime_scheduler",
            "workload_correctness_evidence",
            "benchmark_evidence",
        ],
    }
}

fn hybrid_row() -> EngineCapabilityRow {
    EngineCapabilityRow {
        engine_mode: EngineMode::Hybrid,
        support_status: EngineSupportStatus::Planned,
        operator_support: vec![
            "base_plus_delta_filter_planned",
            "base_plus_delta_project_planned",
            "base_plus_delta_count_planned",
            "base_plus_delta_group_count_planned",
        ],
        function_support: vec!["count_planned", "group_count_planned"],
        source_support: vec!["local_vortex_base_planned", "fixture_hot_delta_planned"],
        sink_support: vec!["vortex_micro_segment_planned", "delta_overlay_planned"],
        bounded_snapshot_support: false,
        append_only_stream_support: false,
        upsert_delete_tombstone_support: false,
        changelog_support: false,
        continuous_materialized_view_support: false,
        global_sort_supported: false,
        unbounded_join_supported: false,
        state_required: true,
        checkpoint_required: true,
        output_modes: vec![
            OutputMode::Snapshot,
            OutputMode::Update,
            OutputMode::Changelog,
            OutputMode::ContinuousView,
        ],
        production_claim_allowed: false,
        blockers: vec![
            "delta_overlay_certificate",
            "hot_cold_contribution_report",
            "micro_segment_flush_evidence",
            "freshness_certificate",
            "state_checkpoint_certificate",
        ],
    }
}

fn append_batch_compatibility_reasons(
    request: &EngineSelectionRequest,
    rejection_reasons: &mut Vec<String>,
) {
    if !request.boundedness.is_batch_compatible() {
        rejection_reasons.push(
            "batch requires bounded or snapshot input; unbounded/unknown inputs need live or hybrid evidence"
                .to_string(),
        );
    }
    if !request.update_mode.is_batch_compatible() {
        rejection_reasons.push(
            "batch currently supports snapshot update mode only; append/upsert/delete/retract/tombstone/changelog need CG-22 change contracts"
                .to_string(),
        );
    }
    if !request.output_mode.is_batch_compatible() {
        rejection_reasons.push(
            "batch currently supports snapshot output mode only; changelog and continuous view outputs need CG-22 state and freshness certificates"
                .to_string(),
        );
    }
}

fn append_live_fixture_compatibility_reasons(
    request: &EngineSelectionRequest,
    rejection_reasons: &mut Vec<String>,
) {
    if !matches!(
        request.boundedness,
        Boundedness::Bounded | Boundedness::Unbounded
    ) {
        rejection_reasons.push(
            "live fixture requires bounded or unbounded change streams; snapshot/unknown inputs remain batch or unsupported"
                .to_string(),
        );
    }
    if !matches!(
        request.update_mode,
        UpdateMode::AppendOnly
            | UpdateMode::Upsert
            | UpdateMode::Delete
            | UpdateMode::Retract
            | UpdateMode::Tombstone
            | UpdateMode::Changelog
    ) {
        rejection_reasons.push(
            "live fixture requires append/upsert/delete/retract/tombstone/changelog update modes"
                .to_string(),
        );
    }
    if !matches!(
        request.output_mode,
        OutputMode::Append
            | OutputMode::Update
            | OutputMode::Changelog
            | OutputMode::ContinuousView
    ) {
        rejection_reasons.push(
            "live fixture requires append/update/changelog/continuous-view output modes"
                .to_string(),
        );
    }
}

fn append_requested_engine_rejection(
    request: &EngineSelectionRequest,
    rejection_reasons: &mut Vec<String>,
) {
    match request.requested {
        EngineMode::Batch | EngineMode::Auto => {}
        EngineMode::Live => rejection_reasons.push(
            "live engine is only partially supported for CG-22 in-memory fixture change streams; the requested workload contract is outside that support"
                .to_string(),
        ),
        EngineMode::Hybrid => rejection_reasons.push(
            "hybrid engine is planned but blocked until delta-overlay, hot/cold contribution, micro-segment flush, state, checkpoint, and freshness evidence exists"
                .to_string(),
        ),
    }
    if request.requested == EngineMode::Auto && rejection_reasons.is_empty() {
        rejection_reasons.push(
            "auto could not select a native engine for the requested workload contract".to_string(),
        );
    }
}

fn join_modes(modes: &[EngineMode]) -> String {
    if modes.is_empty() {
        "none".to_string()
    } else {
        modes
            .iter()
            .map(|mode| mode.as_str())
            .collect::<Vec<_>>()
            .join(",")
    }
}

fn dedupe_strings(values: Vec<String>) -> Vec<String> {
    let mut deduped = Vec::new();
    for value in values {
        if !deduped.contains(&value) {
            deduped.push(value);
        }
    }
    deduped
}

fn normalized_token(value: &str) -> String {
    value.trim().to_ascii_lowercase().replace('_', "-")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn vocabularies_include_cg22_modes() {
        assert_eq!(engine_mode_vocabulary(), "batch,live,hybrid,auto");
        assert_eq!(
            Boundedness::parse("bounded_snapshot").unwrap(),
            Boundedness::Snapshot
        );
        assert_eq!(
            UpdateMode::parse("append-only").unwrap(),
            UpdateMode::AppendOnly
        );
        assert_eq!(
            OutputMode::parse("continuous-view").unwrap(),
            OutputMode::ContinuousView
        );
    }

    #[test]
    fn auto_selects_batch_for_bounded_snapshot_workloads_without_fallback() {
        let report =
            EngineSelectionReport::evaluate(EngineSelectionRequest::default_auto_snapshot());
        assert_eq!(report.status, EngineSelectionStatus::Selected);
        assert_eq!(report.selected, Some(EngineMode::Batch));
        assert!(!report.fallback_attempted());
        assert!(!report.external_engine_invoked);
        assert!(!report.runtime_execution);
        assert!(report.diagnostics().is_empty());
    }

    #[test]
    fn live_fixture_workloads_select_live_without_fallback() {
        let live = EngineSelectionReport::evaluate(EngineSelectionRequest::new(
            EngineMode::Live,
            Boundedness::Unbounded,
            UpdateMode::AppendOnly,
            OutputMode::Changelog,
        ));
        assert_eq!(live.status, EngineSelectionStatus::Selected);
        assert_eq!(live.selected, Some(EngineMode::Live));
        assert!(live.rejected_modes.contains(&EngineMode::Hybrid));
        assert!(!live.fallback_attempted());
        assert!(live.diagnostics().is_empty());
    }

    #[test]
    fn hybrid_is_rejected_until_overlay_flush_and_freshness_evidence_exists() {
        let hybrid = EngineSelectionReport::evaluate(EngineSelectionRequest::new(
            EngineMode::Hybrid,
            Boundedness::Snapshot,
            UpdateMode::Upsert,
            OutputMode::ContinuousView,
        ));
        assert_eq!(hybrid.status, EngineSelectionStatus::Rejected);
        assert!(
            hybrid
                .rejection_reason_text()
                .contains("hybrid engine is planned")
        );
        assert!(!hybrid.external_engine_invoked);
    }

    #[test]
    fn capability_matrix_separates_batch_live_and_hybrid_support() {
        let matrix = EngineCapabilityMatrixReport::cg22_contract();
        assert_eq!(matrix.rows.len(), 3);
        assert_eq!(matrix.partially_supported_count(), 2);
        assert_eq!(matrix.planned_count(), 1);
        assert_eq!(matrix.live_hybrid_claim_blocked_count(), 2);
        assert!(
            matrix
                .row(EngineMode::Batch)
                .unwrap()
                .bounded_snapshot_support
        );
        assert!(matrix.row(EngineMode::Live).unwrap().state_required);
        assert!(matrix.row(EngineMode::Live).unwrap().changelog_support);
        assert!(matrix.row(EngineMode::Hybrid).unwrap().checkpoint_required);
        assert!(!matrix.fallback_attempted());
        assert!(!matrix.external_engine_invoked);
    }
}
