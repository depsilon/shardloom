use std::fmt::Write as _;

use shardloom_core::{
    ByteRange, ColumnRef, DatasetUri, Diagnostic, DiagnosticCode, MaterializationPolicy, Result,
    SegmentId, ShardLoomError,
};
use shardloom_plan::ProjectionRequest;

/// Planning-only status for a `Vortex` read intent in `ShardLoom`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexReadIntentStatus {
    Planned,
    MetadataOnly,
    Pruned,
    NeedsEncodedRead,
    NeedsPartialDecode,
    BlockedByMissingMetadata,
    Unsupported,
}
impl VortexReadIntentStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Planned => "planned",
            Self::MetadataOnly => "metadata_only",
            Self::Pruned => "pruned",
            Self::NeedsEncodedRead => "needs_encoded_read",
            Self::NeedsPartialDecode => "needs_partial_decode",
            Self::BlockedByMissingMetadata => "blocked_by_missing_metadata",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn is_error(&self) -> bool {
        matches!(self, Self::Unsupported)
    }
    #[must_use]
    pub const fn requires_data_read(&self) -> bool {
        matches!(self, Self::NeedsEncodedRead | Self::NeedsPartialDecode)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexReadPlanningMode {
    MetadataOnly,
    EncodedReadPlan,
    PartialDecodePlan,
    MixedPlan,
    Unsupported,
}
impl VortexReadPlanningMode {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::MetadataOnly => "metadata_only",
            Self::EncodedReadPlan => "encoded_read_plan",
            Self::PartialDecodePlan => "partial_decode_plan",
            Self::MixedPlan => "mixed_plan",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn executes_data(&self) -> bool {
        false
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VortexByteRangeIntent {
    pub uri: Option<DatasetUri>,
    pub range: ByteRange,
    pub reason: String,
}
impl VortexByteRangeIntent {
    #[must_use]
    pub fn new(range: ByteRange, reason: impl Into<String>) -> Self {
        Self {
            uri: None,
            range,
            reason: reason.into(),
        }
    }
    #[must_use]
    pub fn with_uri(mut self, uri: DatasetUri) -> Self {
        self.uri = Some(uri);
        self
    }
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.range.is_empty()
    }
    #[must_use]
    pub fn summary(&self) -> String {
        format!(
            "planned byte-range intent only: [{}..{}) reason={}",
            self.range.start,
            self.range.end_exclusive(),
            self.reason
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct VortexReadSplitDescriptor {
    pub split_id: String,
    pub segment_id: Option<SegmentId>,
    pub required_columns: Vec<ColumnRef>,
    pub byte_ranges: Vec<VortexByteRangeIntent>,
    pub status: VortexReadIntentStatus,
    pub diagnostics: Vec<Diagnostic>,
}
impl VortexReadSplitDescriptor {
    /// # Errors
    /// Returns `ShardLoomError::InvalidOperation` for empty split ids.
    pub fn new(split_id: impl Into<String>) -> Result<Self> {
        let split_id = split_id.into();
        if split_id.trim().is_empty() {
            return Err(ShardLoomError::InvalidOperation(
                "split_id must not be empty".to_string(),
            ));
        }
        Ok(Self {
            split_id,
            segment_id: None,
            required_columns: vec![],
            byte_ranges: vec![],
            status: VortexReadIntentStatus::Planned,
            diagnostics: vec![],
        })
    }
    #[must_use]
    pub fn with_segment_id(mut self, segment_id: SegmentId) -> Self {
        self.segment_id = Some(segment_id);
        self
    }
    pub fn add_required_column(&mut self, column: ColumnRef) {
        self.required_columns.push(column);
    }
    pub fn add_byte_range(&mut self, byte_range: VortexByteRangeIntent) {
        self.byte_ranges.push(byte_range);
    }
    #[must_use]
    pub fn with_status(mut self, status: VortexReadIntentStatus) -> Self {
        self.status = status;
        self
    }
    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
    }
    #[must_use]
    pub fn requires_data_read(&self) -> bool {
        self.status.requires_data_read()
    }
    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.status.is_error()
            || self
                .diagnostics
                .iter()
                .any(|d| matches!(d.severity.as_str(), "error" | "fatal"))
    }
    #[must_use]
    pub fn summary(&self) -> String {
        format!(
            "planned split only id={} status={}",
            self.split_id,
            self.status.as_str()
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct VortexSegmentReadIntent {
    pub segment_id: Option<SegmentId>,
    pub pruning_result: Option<crate::VortexSegmentPruningResult>,
    pub split: Option<VortexReadSplitDescriptor>,
    pub status: VortexReadIntentStatus,
    pub materialization: MaterializationPolicy,
    pub diagnostics: Vec<Diagnostic>,
}
impl VortexSegmentReadIntent {
    fn new_status(
        segment_id: Option<SegmentId>,
        status: VortexReadIntentStatus,
        reason: impl Into<String>,
        materialization: MaterializationPolicy,
    ) -> Self {
        let mut out = Self {
            segment_id,
            pruning_result: None,
            split: None,
            status,
            materialization,
            diagnostics: vec![],
        };
        out.add_diagnostic(Diagnostic::new(
            DiagnosticCode::NotImplemented,
            shardloom_core::DiagnosticSeverity::Info,
            shardloom_core::DiagnosticCategory::Planning,
            "Vortex read intent planned only",
            Some("vortex-read-plan".to_string()),
            Some(reason.into()),
            Some("No data read is executed in this skeleton".to_string()),
            shardloom_core::FallbackStatus::disabled_by_policy(),
        ));
        out
    }
    #[must_use]
    pub fn metadata_only(segment_id: Option<SegmentId>, reason: impl Into<String>) -> Self {
        Self::new_status(
            segment_id,
            VortexReadIntentStatus::MetadataOnly,
            reason,
            MaterializationPolicy::Late,
        )
    }
    #[must_use]
    pub fn pruned(segment_id: Option<SegmentId>, reason: impl Into<String>) -> Self {
        Self::new_status(
            segment_id,
            VortexReadIntentStatus::Pruned,
            reason,
            MaterializationPolicy::Never,
        )
    }
    #[must_use]
    pub fn encoded_read(segment_id: Option<SegmentId>, reason: impl Into<String>) -> Self {
        Self::new_status(
            segment_id,
            VortexReadIntentStatus::NeedsEncodedRead,
            reason,
            MaterializationPolicy::Late,
        )
    }
    #[must_use]
    pub fn partial_decode(segment_id: Option<SegmentId>, reason: impl Into<String>) -> Self {
        Self::new_status(
            segment_id,
            VortexReadIntentStatus::NeedsPartialDecode,
            reason,
            MaterializationPolicy::Partial {
                reason: "partial decode may be required".to_string(),
            },
        )
    }
    #[must_use]
    pub fn blocked_by_missing_metadata(
        segment_id: Option<SegmentId>,
        reason: impl Into<String>,
    ) -> Self {
        Self::new_status(
            segment_id,
            VortexReadIntentStatus::BlockedByMissingMetadata,
            reason,
            MaterializationPolicy::Late,
        )
    }
    #[must_use]
    pub fn unsupported(
        segment_id: Option<SegmentId>,
        feature: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        let mut out = Self::new_status(
            segment_id,
            VortexReadIntentStatus::Unsupported,
            reason.into(),
            MaterializationPolicy::Never,
        );
        out.add_diagnostic(Diagnostic::unsupported(
            DiagnosticCode::NotImplemented,
            feature,
            "Unsupported Vortex read planning path",
            Some("Fallback attempted: false".to_string()),
        ));
        out
    }
    #[must_use]
    pub fn with_pruning_result(mut self, result: crate::VortexSegmentPruningResult) -> Self {
        self.pruning_result = Some(result);
        self
    }
    #[must_use]
    pub fn with_split(mut self, split: VortexReadSplitDescriptor) -> Self {
        self.split = Some(split);
        self
    }
    #[must_use]
    pub fn with_materialization(mut self, materialization: MaterializationPolicy) -> Self {
        self.materialization = materialization;
        self
    }
    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
    }
    #[must_use]
    pub fn requires_data_read(&self) -> bool {
        self.status.requires_data_read()
    }
    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.status.is_error()
            || self
                .diagnostics
                .iter()
                .any(|d| matches!(d.severity.as_str(), "error" | "fatal"))
            || self
                .pruning_result
                .as_ref()
                .is_some_and(crate::VortexSegmentPruningResult::has_errors)
            || self
                .split
                .as_ref()
                .is_some_and(VortexReadSplitDescriptor::has_errors)
    }
    #[must_use]
    pub fn summary(&self) -> String {
        format!(
            "segment={} status={} planned only",
            self.segment_id
                .as_ref()
                .map_or("<unknown>", SegmentId::as_str),
            self.status.as_str()
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct VortexReadPlanningInput {
    pub universal_input_plan: Option<crate::VortexUniversalInputPlan>,
    pub pruning_report: Option<crate::VortexMetadataPruningReport>,
    pub projection: ProjectionRequest,
    pub materialization_policy: MaterializationPolicy,
}
impl VortexReadPlanningInput {
    #[must_use]
    pub fn new() -> Self {
        Self {
            universal_input_plan: None,
            pruning_report: None,
            projection: ProjectionRequest::All,
            materialization_policy: MaterializationPolicy::Late,
        }
    }
    #[must_use]
    pub fn from_universal_input_plan(plan: crate::VortexUniversalInputPlan) -> Self {
        Self {
            pruning_report: plan.metadata_pruning_report.clone(),
            universal_input_plan: Some(plan),
            ..Self::new()
        }
    }
    #[must_use]
    pub fn with_pruning_report(mut self, report: crate::VortexMetadataPruningReport) -> Self {
        self.pruning_report = Some(report);
        self
    }
    #[must_use]
    pub fn with_projection(mut self, projection: ProjectionRequest) -> Self {
        self.projection = projection;
        self
    }
    #[must_use]
    pub fn with_materialization_policy(mut self, policy: MaterializationPolicy) -> Self {
        self.materialization_policy = policy;
        self
    }
    #[must_use]
    pub fn summary(&self) -> String {
        format!(
            "projection={} materialization={} pruning_report={}",
            self.projection.summary(),
            self.materialization_policy.summary(),
            self.pruning_report.is_some()
        )
    }
}
impl Default for VortexReadPlanningInput {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::struct_excessive_bools)]
pub struct VortexReadPlanningReport {
    pub status: VortexReadIntentStatus,
    pub mode: VortexReadPlanningMode,
    pub input: VortexReadPlanningInput,
    pub segment_intents: Vec<VortexSegmentReadIntent>,
    pub split_descriptors: Vec<VortexReadSplitDescriptor>,
    pub segments_considered: usize,
    pub segments_pruned: usize,
    pub segments_metadata_only: usize,
    pub segments_planned_for_encoded_read: usize,
    pub segments_planned_for_partial_decode: usize,
    pub byte_range_intents: usize,
    pub data_executed: bool,
    pub data_read: bool,
    pub data_materialized: bool,
    pub object_store_io: bool,
    pub write_io: bool,
    pub external_effects_executed: bool,
    pub fallback_execution_allowed: bool,
    pub diagnostics: Vec<Diagnostic>,
}
impl VortexReadPlanningReport {
    /// # Errors
    /// Returns errors from split-id validation when synthesizing split descriptors.
    #[allow(clippy::too_many_lines)]
    pub fn from_input(input: VortexReadPlanningInput) -> Result<Self> {
        let mut out = Self {
            status: VortexReadIntentStatus::Planned,
            mode: VortexReadPlanningMode::MetadataOnly,
            input,
            segment_intents: vec![],
            split_descriptors: vec![],
            segments_considered: 0,
            segments_pruned: 0,
            segments_metadata_only: 0,
            segments_planned_for_encoded_read: 0,
            segments_planned_for_partial_decode: 0,
            byte_range_intents: 0,
            data_executed: false,
            data_read: false,
            data_materialized: false,
            object_store_io: false,
            write_io: false,
            external_effects_executed: false,
            fallback_execution_allowed: false,
            diagnostics: vec![],
        };
        if let Some(pruning) = out.input.pruning_report.clone() {
            if matches!(
                pruning.status,
                crate::VortexMetadataPruningStatus::Unsupported
            ) {
                out.add_diagnostic(Diagnostic::unsupported(
                    DiagnosticCode::NotImplemented,
                    "vortex-read-plan",
                    "Upstream metadata pruning reported unsupported planning state",
                    Some("Fallback attempted: false".to_string()),
                ));
            }
            for diagnostic in pruning.diagnostics {
                out.add_diagnostic(diagnostic);
            }
            for result in pruning.results {
                let mut intent = match result.status {
                    crate::VortexMetadataPruningStatus::Pruned => VortexSegmentReadIntent::pruned(
                        result.segment_id.clone(),
                        result.decision.reason(),
                    ),
                    crate::VortexMetadataPruningStatus::MetadataOnly => {
                        VortexSegmentReadIntent::metadata_only(
                            result.segment_id.clone(),
                            result.decision.reason(),
                        )
                    }
                    crate::VortexMetadataPruningStatus::NeedsEncodedRead => {
                        VortexSegmentReadIntent::encoded_read(
                            result.segment_id.clone(),
                            result.decision.reason(),
                        )
                    }
                    crate::VortexMetadataPruningStatus::NeedsPartialDecode => {
                        VortexSegmentReadIntent::partial_decode(
                            result.segment_id.clone(),
                            result.decision.reason(),
                        )
                    }
                    crate::VortexMetadataPruningStatus::StatisticsUnavailable
                    | crate::VortexMetadataPruningStatus::Planned => {
                        VortexSegmentReadIntent::blocked_by_missing_metadata(
                            result.segment_id.clone(),
                            result.decision.reason(),
                        )
                    }
                    crate::VortexMetadataPruningStatus::Unsupported => {
                        VortexSegmentReadIntent::unsupported(
                            result.segment_id.clone(),
                            "vortex-read-plan",
                            result.decision.reason(),
                        )
                    }
                };
                intent = intent.with_pruning_result(result);
                if intent.requires_data_read() {
                    let id = format!("split-{}", out.segment_intents.len());
                    let mut split = VortexReadSplitDescriptor::new(id)?.with_status(intent.status);
                    if let Some(seg) = intent.segment_id.clone() {
                        split = split.with_segment_id(seg);
                    }
                    if let ProjectionRequest::Columns(cols) = &out.input.projection {
                        for c in cols {
                            split.add_required_column(c.clone());
                        }
                    }
                    intent = intent.with_split(split.clone());
                    out.add_split_descriptor(split);
                }
                out.add_segment_intent(intent);
            }
        }
        out.recompute_counts();
        let has_partial = out.segments_planned_for_partial_decode > 0;
        let has_encoded = out.segments_planned_for_encoded_read > 0;
        let has_meta = out.segments_metadata_only + out.segments_pruned > 0;
        out.mode = if out.segments_considered == 0 || (has_meta && !has_partial && !has_encoded) {
            VortexReadPlanningMode::MetadataOnly
        } else if has_partial && (has_encoded || has_meta) {
            VortexReadPlanningMode::MixedPlan
        } else if has_partial {
            VortexReadPlanningMode::PartialDecodePlan
        } else if has_encoded {
            VortexReadPlanningMode::EncodedReadPlan
        } else {
            VortexReadPlanningMode::MixedPlan
        };
        if out.has_errors() {
            out.status = VortexReadIntentStatus::Unsupported;
        } else if has_partial {
            out.status = VortexReadIntentStatus::NeedsPartialDecode;
        } else if has_encoded {
            out.status = VortexReadIntentStatus::NeedsEncodedRead;
        } else if out.segments_metadata_only > 0 {
            out.status = VortexReadIntentStatus::MetadataOnly;
        } else if out.segments_pruned > 0 {
            out.status = VortexReadIntentStatus::Pruned;
        } else if out.segment_intents.iter().any(|intent| {
            matches!(
                intent.status,
                VortexReadIntentStatus::BlockedByMissingMetadata
            )
        }) {
            out.status = VortexReadIntentStatus::BlockedByMissingMetadata;
        }
        Ok(out)
    }
    /// # Errors
    /// Returns errors from nested `VortexReadPlanningReport::from_input` construction.
    pub fn from_universal_input_plan(plan: crate::VortexUniversalInputPlan) -> Result<Self> {
        Self::from_input(VortexReadPlanningInput::from_universal_input_plan(plan))
    }
    pub fn unsupported(feature: impl Into<String>, reason: impl Into<String>) -> Self {
        let mut out = Self {
            status: VortexReadIntentStatus::Unsupported,
            mode: VortexReadPlanningMode::Unsupported,
            input: VortexReadPlanningInput::new(),
            segment_intents: vec![],
            split_descriptors: vec![],
            segments_considered: 0,
            segments_pruned: 0,
            segments_metadata_only: 0,
            segments_planned_for_encoded_read: 0,
            segments_planned_for_partial_decode: 0,
            byte_range_intents: 0,
            data_executed: false,
            data_read: false,
            data_materialized: false,
            object_store_io: false,
            write_io: false,
            external_effects_executed: false,
            fallback_execution_allowed: false,
            diagnostics: vec![],
        };
        out.add_diagnostic(Diagnostic::unsupported(
            DiagnosticCode::NotImplemented,
            feature,
            reason,
            Some("Fallback attempted: false".to_string()),
        ));
        out
    }
    pub fn add_segment_intent(&mut self, intent: VortexSegmentReadIntent) {
        self.segment_intents.push(intent);
    }
    pub fn add_split_descriptor(&mut self, split: VortexReadSplitDescriptor) {
        self.split_descriptors.push(split);
    }
    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
    }
    pub fn recompute_counts(&mut self) {
        self.segments_considered = self.segment_intents.len();
        self.segments_pruned = self
            .segment_intents
            .iter()
            .filter(|s| matches!(s.status, VortexReadIntentStatus::Pruned))
            .count();
        self.segments_metadata_only = self
            .segment_intents
            .iter()
            .filter(|s| matches!(s.status, VortexReadIntentStatus::MetadataOnly))
            .count();
        self.segments_planned_for_encoded_read = self
            .segment_intents
            .iter()
            .filter(|s| matches!(s.status, VortexReadIntentStatus::NeedsEncodedRead))
            .count();
        self.segments_planned_for_partial_decode = self
            .segment_intents
            .iter()
            .filter(|s| matches!(s.status, VortexReadIntentStatus::NeedsPartialDecode))
            .count();
        self.byte_range_intents = self
            .split_descriptors
            .iter()
            .map(|s| s.byte_ranges.len())
            .sum();
    }
    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.status.is_error()
            || self
                .diagnostics
                .iter()
                .any(|d| matches!(d.severity.as_str(), "error" | "fatal"))
            || self
                .segment_intents
                .iter()
                .any(VortexSegmentReadIntent::has_errors)
            || self
                .split_descriptors
                .iter()
                .any(VortexReadSplitDescriptor::has_errors)
    }
    #[must_use]
    pub const fn is_side_effect_free(&self) -> bool {
        !self.data_executed
            && !self.data_read
            && !self.data_materialized
            && !self.object_store_io
            && !self.write_io
            && !self.external_effects_executed
            && !self.fallback_execution_allowed
    }
    #[must_use]
    pub fn to_human_text(&self) -> String {
        let mut out = String::new();
        let _ = write!(
            out,
            "vortex read planning status: {}\nmode: {}\nsegments considered: {}\nsegments pruned: {}\nsegments metadata only: {}\nsegments planned for encoded read: {}\nsegments planned for partial decode: {}\nbyte range intents: {}\ndata executed: false\ndata read: false\ndata materialized: false\nobject-store IO: false\nwrite IO: false\nexternal effects executed: false\nfallback execution disabled",
            self.status.as_str(),
            self.mode.as_str(),
            self.segments_considered,
            self.segments_pruned,
            self.segments_metadata_only,
            self.segments_planned_for_encoded_read,
            self.segments_planned_for_partial_decode,
            self.byte_range_intents
        );
        if !self.diagnostics.is_empty() {
            out.push_str("\ndiagnostics:");
            for d in &self.diagnostics {
                let _ = write!(out, "\n- {}", d.to_human_text());
            }
        }
        out
    }
}

/// # Errors
/// Returns errors from `VortexReadPlanningReport` construction.
pub fn plan_vortex_read_from_universal_input(
    plan: crate::VortexUniversalInputPlan,
) -> Result<VortexReadPlanningReport> {
    VortexReadPlanningReport::from_universal_input_plan(plan)
}
#[must_use]
pub fn vortex_read_planning_is_side_effect_free(report: &VortexReadPlanningReport) -> bool {
    report.is_side_effect_free()
}

#[cfg(test)]
mod tests {
    use super::*;
    use shardloom_core::{DiagnosticCode, FallbackStatus, PredicateProof, PruningDecision};

    #[test]
    fn status_checks() {
        assert!(VortexReadIntentStatus::Unsupported.is_error());
        assert!(VortexReadIntentStatus::NeedsEncodedRead.requires_data_read());
        assert!(VortexReadIntentStatus::NeedsPartialDecode.requires_data_read());
        assert!(!VortexReadIntentStatus::Pruned.requires_data_read());
    }
    #[test]
    fn mode_plan_only() {
        assert!(!VortexReadPlanningMode::EncodedReadPlan.executes_data());
    }
    #[test]
    fn byte_range_empty() {
        assert!(VortexByteRangeIntent::new(ByteRange::new(1, 0), "r").is_empty());
    }
    #[test]
    fn split_rejects_empty() {
        assert!(VortexReadSplitDescriptor::new("  ").is_err());
    }
    #[test]
    fn split_requires_read() {
        let s = VortexReadSplitDescriptor::new("x")
            .unwrap()
            .with_status(VortexReadIntentStatus::NeedsEncodedRead);
        assert!(s.requires_data_read());
    }
    #[test]
    fn intent_reads() {
        assert!(!VortexSegmentReadIntent::pruned(None, "r").requires_data_read());
        assert!(VortexSegmentReadIntent::encoded_read(None, "r").requires_data_read());
        assert!(VortexSegmentReadIntent::partial_decode(None, "r").requires_data_read());
    }
    #[test]
    fn intent_unsupported_error() {
        let i = VortexSegmentReadIntent::unsupported(None, "f", "r");
        assert!(i.has_errors());
        assert!(
            i.diagnostics
                .iter()
                .any(|d| d.fallback == FallbackStatus::disabled_by_policy())
        );
    }
    #[test]
    fn input_defaults() {
        let i = VortexReadPlanningInput::new();
        assert!(matches!(i.projection, ProjectionRequest::All));
        assert_eq!(i.materialization_policy, MaterializationPolicy::Late);
    }
    #[test]
    fn report_unsupported_error() {
        let r = VortexReadPlanningReport::unsupported("f", "r");
        assert!(r.has_errors());
        assert!(!r.fallback_execution_allowed);
    }
    #[test]
    fn report_empty_side_effect_free() {
        let r = VortexReadPlanningReport::from_input(VortexReadPlanningInput::new()).unwrap();
        assert!(r.is_side_effect_free());
    }
    #[test]
    fn report_preserves_pruning_unsupported_without_results() {
        let pruning =
            crate::VortexMetadataPruningReport::unsupported("vortex-prune", "unsupported");
        let report = VortexReadPlanningReport::from_input(
            VortexReadPlanningInput::new().with_pruning_report(pruning),
        )
        .unwrap();
        assert!(report.has_errors());
        assert!(matches!(report.status, VortexReadIntentStatus::Unsupported));
    }
    #[test]
    fn report_surfaces_blocked_by_missing_metadata_status() {
        let pruning = mk_pruning(crate::VortexMetadataPruningStatus::StatisticsUnavailable);
        let report = VortexReadPlanningReport::from_input(
            VortexReadPlanningInput::new().with_pruning_report(pruning),
        )
        .unwrap();
        assert!(matches!(
            report.status,
            VortexReadIntentStatus::BlockedByMissingMetadata
        ));
    }

    fn mk_pruning(
        status: crate::VortexMetadataPruningStatus,
    ) -> crate::VortexMetadataPruningReport {
        let decision = match status {
            crate::VortexMetadataPruningStatus::Pruned => {
                PruningDecision::PruneSegment { reason: "r".into() }
            }
            crate::VortexMetadataPruningStatus::NeedsEncodedRead => {
                PruningDecision::ReadEncoded { reason: "r".into() }
            }
            crate::VortexMetadataPruningStatus::NeedsPartialDecode => {
                PruningDecision::NeedPartialDecode { reason: "r".into() }
            }
            _ => PruningDecision::MetadataOnlyAnswer { reason: "r".into() },
        };
        let mut seg = crate::VortexSegmentPruningResult::new(
            None,
            PredicateProof::MayMatch { reason: "r".into() },
            decision,
        );
        seg.status = status;
        crate::VortexMetadataPruningReport {
            status,
            mode: crate::VortexMetadataPruningMode::Conservative,
            metadata_planning: None,
            results: vec![seg],
            segments_considered: 1,
            segments_pruned: 0,
            segments_metadata_answered: 0,
            segments_requiring_read: 0,
            data_executed: false,
            data_materialized: false,
            object_store_io: false,
            write_io: false,
            fallback_execution_allowed: false,
            diagnostics: vec![],
        }
    }
    #[test]
    fn report_counts() {
        let r = VortexReadPlanningReport::from_input(
            VortexReadPlanningInput::new()
                .with_pruning_report(mk_pruning(crate::VortexMetadataPruningStatus::Pruned)),
        )
        .unwrap();
        assert_eq!(r.segments_pruned, 1);
        let r = VortexReadPlanningReport::from_input(
            VortexReadPlanningInput::new().with_pruning_report(mk_pruning(
                crate::VortexMetadataPruningStatus::NeedsEncodedRead,
            )),
        )
        .unwrap();
        assert_eq!(r.segments_planned_for_encoded_read, 1);
        let r = VortexReadPlanningReport::from_input(
            VortexReadPlanningInput::new().with_pruning_report(mk_pruning(
                crate::VortexMetadataPruningStatus::NeedsPartialDecode,
            )),
        )
        .unwrap();
        assert_eq!(r.segments_planned_for_partial_decode, 1);
    }
    #[test]
    fn report_human_text() {
        let mut r = VortexReadPlanningReport::from_input(VortexReadPlanningInput::new()).unwrap();
        r.add_diagnostic(Diagnostic::unsupported(
            DiagnosticCode::NotImplemented,
            "f",
            "m",
            None,
        ));
        let t = r.to_human_text();
        assert!(t.contains("fallback execution disabled"));
        assert!(t.contains("data read: false"));
        assert!(t.contains("data materialized: false"));
        assert!(t.contains("diagnostics:"));
    }
}
