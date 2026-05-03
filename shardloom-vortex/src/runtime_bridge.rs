use std::fmt::Write as _;

use shardloom_core::{
    Diagnostic, DiagnosticCode, DiagnosticSeverity, FallbackStatus, Result, SegmentId,
};
use shardloom_exec::{
    ByteRangeRequest, ReadPolicy, ResourceBudget, RetryPolicy, RuntimePlanSkeleton, SegmentTask,
    TaskGraph, TaskId, TaskKind,
};

/// Planning-only bridge status from `Vortex` read intent into `ShardLoom` runtime task skeletons.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexRuntimeBridgeStatus {
    Planned,
    MetadataOnlyTasks,
    EncodedReadTasksPlanned,
    PartialDecodeTasksPlanned,
    MixedTasksPlanned,
    NoTasksRequired,
    BlockedByMissingMetadata,
    Unsupported,
}
impl VortexRuntimeBridgeStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Planned => "planned",
            Self::MetadataOnlyTasks => "metadata_only_tasks",
            Self::EncodedReadTasksPlanned => "encoded_read_tasks_planned",
            Self::PartialDecodeTasksPlanned => "partial_decode_tasks_planned",
            Self::MixedTasksPlanned => "mixed_tasks_planned",
            Self::NoTasksRequired => "no_tasks_required",
            Self::BlockedByMissingMetadata => "blocked_by_missing_metadata",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn is_error(&self) -> bool {
        matches!(self, Self::Unsupported)
    }
    #[must_use]
    pub const fn requires_future_execution(&self) -> bool {
        matches!(
            self,
            Self::EncodedReadTasksPlanned
                | Self::PartialDecodeTasksPlanned
                | Self::MixedTasksPlanned
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexRuntimeBridgeMode {
    TaskGraphPlanOnly,
    MetadataOnly,
    ReadTaskPlanning,
    Unsupported,
}
impl VortexRuntimeBridgeMode {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::TaskGraphPlanOnly => "task_graph_plan_only",
            Self::MetadataOnly => "metadata_only",
            Self::ReadTaskPlanning => "read_task_planning",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn executes_tasks(&self) -> bool {
        false
    }
}

/// Planning-only mapping kind from `Vortex` read intent to `ShardLoom` `SegmentTask`.
///
/// This mapping records future scheduling intent and never executes tasks, reads,
/// materialization, object-store IO, writes, or external effects.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexTaskMappingKind {
    NoTaskNeeded,
    MetadataTask,
    SegmentScanTask,
    EncodedEvaluateTask,
    PartialDecodeTask,
    Unsupported,
}
impl VortexTaskMappingKind {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::NoTaskNeeded => "no_task_needed",
            Self::MetadataTask => "metadata_task",
            Self::SegmentScanTask => "segment_scan_task",
            Self::EncodedEvaluateTask => "encoded_evaluate_task",
            Self::PartialDecodeTask => "partial_decode_task",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn requires_future_execution(&self) -> bool {
        matches!(
            self,
            Self::SegmentScanTask | Self::EncodedEvaluateTask | Self::PartialDecodeTask
        )
    }
}

/// Planning-only mapping from a `Vortex` read intent to runtime `TaskGraph` entries.
///
/// The mapping is metadata only and does not execute the `SegmentTask`.
#[derive(Debug, Clone, PartialEq)]
pub struct VortexTaskMapping {
    pub kind: VortexTaskMappingKind,
    pub segment_id: Option<SegmentId>,
    pub split_id: Option<String>,
    pub task_id: Option<TaskId>,
    pub task: Option<SegmentTask>,
    pub diagnostics: Vec<Diagnostic>,
}
impl VortexTaskMapping {
    pub fn no_task_needed(segment_id: Option<SegmentId>, reason: impl Into<String>) -> Self {
        let mut out = Self {
            kind: VortexTaskMappingKind::NoTaskNeeded,
            segment_id,
            split_id: None,
            task_id: None,
            task: None,
            diagnostics: vec![],
        };
        out.add_diagnostic(Diagnostic::new(
            DiagnosticCode::NotImplemented,
            DiagnosticSeverity::Info,
            shardloom_core::DiagnosticCategory::Planning,
            "No runtime task required",
            Some("vortex-runtime-bridge".to_string()),
            Some(reason.into()),
            Some("Mapping only; no task execution performed".to_string()),
            FallbackStatus::disabled_by_policy(),
        ));
        out
    }
    #[must_use]
    pub fn metadata_task(segment_id: Option<SegmentId>, task_id: TaskId) -> Self {
        Self {
            kind: VortexTaskMappingKind::MetadataTask,
            segment_id,
            split_id: None,
            task_id: Some(task_id),
            task: None,
            diagnostics: vec![],
        }
    }
    #[must_use]
    pub fn segment_scan_task(segment_id: Option<SegmentId>, task_id: TaskId) -> Self {
        Self {
            kind: VortexTaskMappingKind::SegmentScanTask,
            segment_id,
            split_id: None,
            task_id: Some(task_id),
            task: None,
            diagnostics: vec![],
        }
    }
    #[must_use]
    pub fn encoded_evaluate_task(segment_id: Option<SegmentId>, task_id: TaskId) -> Self {
        Self {
            kind: VortexTaskMappingKind::EncodedEvaluateTask,
            segment_id,
            split_id: None,
            task_id: Some(task_id),
            task: None,
            diagnostics: vec![],
        }
    }
    #[must_use]
    pub fn partial_decode_task(segment_id: Option<SegmentId>, task_id: TaskId) -> Self {
        Self {
            kind: VortexTaskMappingKind::PartialDecodeTask,
            segment_id,
            split_id: None,
            task_id: Some(task_id),
            task: None,
            diagnostics: vec![],
        }
    }
    pub fn unsupported(
        segment_id: Option<SegmentId>,
        feature: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        let mut out = Self {
            kind: VortexTaskMappingKind::Unsupported,
            segment_id,
            split_id: None,
            task_id: None,
            task: None,
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
    #[must_use]
    pub fn with_split_id(mut self, split_id: impl Into<String>) -> Self {
        self.split_id = Some(split_id.into());
        self
    }
    #[must_use]
    pub fn with_task(mut self, task: SegmentTask) -> Self {
        self.task = Some(task);
        self
    }
    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
    }
    #[must_use]
    pub fn requires_future_execution(&self) -> bool {
        self.kind.requires_future_execution()
    }
    pub fn has_errors(&self) -> bool {
        self.kind == VortexTaskMappingKind::Unsupported
            || self.diagnostics.iter().any(|d| {
                matches!(
                    d.severity,
                    DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
                )
            })
            || self.task.as_ref().is_some_and(SegmentTask::has_errors)
    }
    pub fn summary(&self) -> String {
        format!(
            "mapping-only kind={} segment={} execution=not_performed",
            self.kind.as_str(),
            self.segment_id
                .as_ref()
                .map_or("<unknown>", SegmentId::as_str)
        )
    }
}

/// Planning-only bridge input for generating `VortexRuntimeBridgeReport`.
#[derive(Debug, Clone, PartialEq)]
pub struct VortexRuntimeBridgeInput {
    pub read_planning_report: crate::VortexReadPlanningReport,
    pub resource_budget: ResourceBudget,
    pub retry_policy: RetryPolicy,
}
impl VortexRuntimeBridgeInput {
    #[must_use]
    pub fn new(read_planning_report: crate::VortexReadPlanningReport) -> Self {
        Self {
            read_planning_report,
            resource_budget: ResourceBudget::unbounded(),
            retry_policy: RetryPolicy::none(),
        }
    }
    #[must_use]
    pub fn with_resource_budget(mut self, budget: ResourceBudget) -> Self {
        self.resource_budget = budget;
        self
    }
    #[must_use]
    pub fn with_retry_policy(mut self, policy: RetryPolicy) -> Self {
        self.retry_policy = policy;
        self
    }
    #[must_use]
    pub fn summary(&self) -> String {
        format!(
            "runtime bridge input: intents={} budget={} retry={}",
            self.read_planning_report.segment_intents.len(),
            self.resource_budget.summary(),
            self.retry_policy.summary()
        )
    }
}

/// Planning-only bridge report from `Vortex` read planning into a runtime `TaskGraph`.
///
/// This report contains task skeletons for future execution scheduling and never
/// performs data reads, decode, materialization, writes, object-store IO, or
/// fallback execution.
#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::struct_excessive_bools)]
pub struct VortexRuntimeBridgeReport {
    pub status: VortexRuntimeBridgeStatus,
    pub mode: VortexRuntimeBridgeMode,
    pub input: VortexRuntimeBridgeInput,
    pub task_graph: TaskGraph,
    pub runtime_plan: RuntimePlanSkeleton,
    pub mappings: Vec<VortexTaskMapping>,
    pub task_count: usize,
    pub metadata_task_count: usize,
    pub encoded_task_count: usize,
    pub partial_decode_task_count: usize,
    pub no_task_count: usize,
    pub data_executed: bool,
    pub data_read: bool,
    pub data_materialized: bool,
    pub object_store_io: bool,
    pub write_io: bool,
    pub external_effects_executed: bool,
    pub fallback_execution_allowed: bool,
    pub diagnostics: Vec<Diagnostic>,
}
impl VortexRuntimeBridgeReport {
    /// Builds a planning-only `VortexRuntimeBridgeReport` from a `VortexReadPlanningReport`.
    ///
    /// # Errors
    /// Returns identifier-construction errors while building deterministic `TaskId` values.
    pub fn from_input(input: VortexRuntimeBridgeInput) -> Result<Self> {
        let mut out = Self {
            status: VortexRuntimeBridgeStatus::Planned,
            mode: VortexRuntimeBridgeMode::TaskGraphPlanOnly,
            task_graph: TaskGraph::new(),
            runtime_plan: RuntimePlanSkeleton::planned(TaskGraph::new()),
            mappings: vec![],
            task_count: 0,
            metadata_task_count: 0,
            encoded_task_count: 0,
            partial_decode_task_count: 0,
            no_task_count: 0,
            data_executed: false,
            data_read: false,
            data_materialized: false,
            object_store_io: false,
            write_io: false,
            external_effects_executed: false,
            fallback_execution_allowed: false,
            diagnostics: vec![],
            input,
        };
        for diagnostic in out.input.read_planning_report.diagnostics.clone() {
            out.add_diagnostic(diagnostic);
        }
        if out.input.read_planning_report.has_errors() {
            out.status = VortexRuntimeBridgeStatus::Unsupported;
            out.mode = VortexRuntimeBridgeMode::Unsupported;
            out.runtime_plan = RuntimePlanSkeleton::unsupported(
                out.task_graph.clone(),
                "vortex-runtime-bridge",
                "read planning report contains errors",
            );
            return Ok(out);
        }
        let intents = out.input.read_planning_report.segment_intents.clone();
        for (i, intent) in intents.iter().enumerate() {
            let id = TaskId::new(format!("vortex-task-{i}"))?;
            let mapping = Self::mapping_for_intent(intent, id.clone());
            let mut mapping = if let Some(split) = intent.split.as_ref() {
                mapping.with_split_id(split.split_id.clone())
            } else {
                mapping
            };
            Self::attach_task_if_needed(&out.input, intent, &mut out.task_graph, &mut mapping, &id);
            out.add_mapping(mapping);
        }
        out.runtime_plan = RuntimePlanSkeleton::planned(out.task_graph.clone());
        out.recompute_counts();
        let has_unsupported = out.has_errors();
        let has_encoded = out.encoded_task_count > 0;
        let has_partial = out.partial_decode_task_count > 0;
        let has_meta = out.metadata_task_count > 0;
        out.status = if matches!(
            out.input.read_planning_report.status,
            crate::VortexReadIntentStatus::BlockedByMissingMetadata
        ) && out.task_count == 0
        {
            VortexRuntimeBridgeStatus::BlockedByMissingMetadata
        } else if has_unsupported {
            VortexRuntimeBridgeStatus::Unsupported
        } else if !out.mappings.is_empty() && out.no_task_count == out.mappings.len() {
            if out.mappings.iter().any(|m| {
                m.diagnostics
                    .iter()
                    .any(|d| d.code == DiagnosticCode::MissingStatistics)
            }) {
                VortexRuntimeBridgeStatus::BlockedByMissingMetadata
            } else {
                VortexRuntimeBridgeStatus::NoTasksRequired
            }
        } else if has_meta && !has_encoded && !has_partial {
            VortexRuntimeBridgeStatus::MetadataOnlyTasks
        } else if has_encoded && !has_partial {
            VortexRuntimeBridgeStatus::EncodedReadTasksPlanned
        } else if has_partial && !has_encoded && !has_meta {
            VortexRuntimeBridgeStatus::PartialDecodeTasksPlanned
        } else if has_partial || has_encoded {
            VortexRuntimeBridgeStatus::MixedTasksPlanned
        } else {
            VortexRuntimeBridgeStatus::Planned
        };
        out.mode = match out.status {
            VortexRuntimeBridgeStatus::NoTasksRequired
            | VortexRuntimeBridgeStatus::MetadataOnlyTasks
            | VortexRuntimeBridgeStatus::BlockedByMissingMetadata => {
                VortexRuntimeBridgeMode::MetadataOnly
            }
            VortexRuntimeBridgeStatus::EncodedReadTasksPlanned
            | VortexRuntimeBridgeStatus::PartialDecodeTasksPlanned
            | VortexRuntimeBridgeStatus::MixedTasksPlanned => {
                VortexRuntimeBridgeMode::ReadTaskPlanning
            }
            VortexRuntimeBridgeStatus::Unsupported => VortexRuntimeBridgeMode::Unsupported,
            VortexRuntimeBridgeStatus::Planned => VortexRuntimeBridgeMode::TaskGraphPlanOnly,
        };
        Ok(out)
    }
    fn mapping_for_intent(
        intent: &crate::VortexSegmentReadIntent,
        id: TaskId,
    ) -> VortexTaskMapping {
        match intent.status {
            crate::VortexReadIntentStatus::Planned
            | crate::VortexReadIntentStatus::MetadataOnly => {
                VortexTaskMapping::metadata_task(intent.segment_id.clone(), id)
            }
            crate::VortexReadIntentStatus::Pruned => {
                VortexTaskMapping::no_task_needed(intent.segment_id.clone(), "pruned by metadata")
            }
            crate::VortexReadIntentStatus::NeedsEncodedRead => {
                VortexTaskMapping::encoded_evaluate_task(intent.segment_id.clone(), id)
            }
            crate::VortexReadIntentStatus::NeedsPartialDecode => {
                VortexTaskMapping::partial_decode_task(intent.segment_id.clone(), id)
            }
            crate::VortexReadIntentStatus::BlockedByMissingMetadata => {
                let mut m = VortexTaskMapping::no_task_needed(
                    intent.segment_id.clone(),
                    "blocked by missing metadata",
                );
                m.add_diagnostic(Diagnostic::new(
                    DiagnosticCode::MissingStatistics,
                    DiagnosticSeverity::Warning,
                    shardloom_core::DiagnosticCategory::Statistics,
                    "Blocked by missing metadata",
                    Some("vortex-runtime-bridge".to_string()),
                    Some("No runtime task created".to_string()),
                    None,
                    FallbackStatus::disabled_by_policy(),
                ));
                m
            }
            crate::VortexReadIntentStatus::Unsupported => VortexTaskMapping::unsupported(
                intent.segment_id.clone(),
                "vortex-runtime-bridge",
                "unsupported read intent",
            ),
        }
    }
    fn attach_task_if_needed(
        input: &VortexRuntimeBridgeInput,
        intent: &crate::VortexSegmentReadIntent,
        task_graph: &mut TaskGraph,
        mapping: &mut VortexTaskMapping,
        id: &TaskId,
    ) {
        if !mapping.requires_future_execution()
            && mapping.kind != VortexTaskMappingKind::MetadataTask
        {
            return;
        }
        let kind = match mapping.kind {
            VortexTaskMappingKind::MetadataTask => TaskKind::MetadataRead,
            VortexTaskMappingKind::EncodedEvaluateTask => TaskKind::EncodedEvaluate,
            VortexTaskMappingKind::PartialDecodeTask => TaskKind::PartialDecode,
            _ => TaskKind::SegmentScan,
        };
        let mut task =
            SegmentTask::new(mapping.task_id.clone().unwrap_or_else(|| id.clone()), kind)
                .with_materialization(intent.materialization.clone())
                .with_resource_budget(input.resource_budget.clone())
                .with_retry_policy(input.retry_policy.clone());
        if let Some(seg) = intent.segment_id.clone() {
            task.add_segment(seg);
        }
        if let Some(split) = intent.split.as_ref() {
            for col in &split.required_columns {
                task.add_required_column(col.clone());
            }
            for br in &split.byte_ranges {
                if let Some(uri) = br.uri.clone() {
                    task.add_byte_range(
                        ByteRangeRequest::new(uri, br.range)
                            .with_policy(ReadPolicy::ByteRangePreferred),
                    );
                }
            }
        }
        task_graph.add_task(task.clone());
        *mapping = mapping.clone().with_task(task);
    }
    /// # Errors
    /// Propagates errors from `VortexRuntimeBridgeReport::from_input`.
    pub fn from_read_planning_report(report: crate::VortexReadPlanningReport) -> Result<Self> {
        Self::from_input(VortexRuntimeBridgeInput::new(report))
    }
    pub fn unsupported(
        input: VortexRuntimeBridgeInput,
        feature: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        let mut out = Self {
            status: VortexRuntimeBridgeStatus::Unsupported,
            mode: VortexRuntimeBridgeMode::Unsupported,
            task_graph: TaskGraph::new(),
            runtime_plan: RuntimePlanSkeleton::planned(TaskGraph::new()),
            mappings: vec![],
            task_count: 0,
            metadata_task_count: 0,
            encoded_task_count: 0,
            partial_decode_task_count: 0,
            no_task_count: 0,
            data_executed: false,
            data_read: false,
            data_materialized: false,
            object_store_io: false,
            write_io: false,
            external_effects_executed: false,
            fallback_execution_allowed: false,
            diagnostics: vec![],
            input,
        };
        out.add_diagnostic(Diagnostic::unsupported(
            DiagnosticCode::NotImplemented,
            feature,
            reason,
            Some("Fallback attempted: false".to_string()),
        ));
        out
    }
    pub fn add_mapping(&mut self, mapping: VortexTaskMapping) {
        self.mappings.push(mapping);
    }
    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
    }
    pub fn recompute_counts(&mut self) {
        self.task_count = self.task_graph.task_count();
        self.metadata_task_count = self
            .mappings
            .iter()
            .filter(|m| m.kind == VortexTaskMappingKind::MetadataTask)
            .count();
        self.encoded_task_count = self
            .mappings
            .iter()
            .filter(|m| m.kind == VortexTaskMappingKind::EncodedEvaluateTask)
            .count();
        self.partial_decode_task_count = self
            .mappings
            .iter()
            .filter(|m| m.kind == VortexTaskMappingKind::PartialDecodeTask)
            .count();
        self.no_task_count = self
            .mappings
            .iter()
            .filter(|m| m.kind == VortexTaskMappingKind::NoTaskNeeded)
            .count();
    }
    pub fn has_errors(&self) -> bool {
        self.status.is_error()
            || self.diagnostics.iter().any(|d| {
                matches!(
                    d.severity,
                    DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
                )
            })
            || self.mappings.iter().any(VortexTaskMapping::has_errors)
            || self.task_graph.has_errors()
            || self.runtime_plan.has_errors()
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
        let _ = writeln!(&mut out, "bridge status: {}", self.status.as_str());
        let _ = writeln!(&mut out, "mode: {}", self.mode.as_str());
        let _ = writeln!(&mut out, "task count: {}", self.task_count);
        let _ = writeln!(
            &mut out,
            "metadata task count: {}",
            self.metadata_task_count
        );
        let _ = writeln!(&mut out, "encoded task count: {}", self.encoded_task_count);
        let _ = writeln!(
            &mut out,
            "partial decode task count: {}",
            self.partial_decode_task_count
        );
        let _ = writeln!(&mut out, "no-task count: {}", self.no_task_count);
        let _ = writeln!(&mut out, "tasks executed: false");
        let _ = writeln!(&mut out, "data executed: false");
        let _ = writeln!(&mut out, "data read: false");
        let _ = writeln!(&mut out, "data materialized: false");
        let _ = writeln!(&mut out, "object-store IO: false");
        let _ = writeln!(&mut out, "write IO: false");
        let _ = writeln!(&mut out, "external effects executed: false");
        let _ = writeln!(&mut out, "fallback execution disabled: true");
        if !self.diagnostics.is_empty() {
            let _ = writeln!(&mut out, "diagnostics:");
            for d in &self.diagnostics {
                let _ = writeln!(&mut out, "- {} [{}]", d.message, d.code.as_str());
            }
        }
        out
    }
}

/// # Errors
/// Propagates errors from `VortexRuntimeBridgeReport::from_read_planning_report`.
pub fn build_vortex_runtime_task_graph(
    read_planning_report: crate::VortexReadPlanningReport,
) -> Result<VortexRuntimeBridgeReport> {
    VortexRuntimeBridgeReport::from_read_planning_report(read_planning_report)
}
#[must_use]
pub fn vortex_runtime_bridge_is_side_effect_free(report: &VortexRuntimeBridgeReport) -> bool {
    report.is_side_effect_free()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bridge_status_flags() {
        assert!(VortexRuntimeBridgeStatus::Unsupported.is_error());
        assert!(VortexRuntimeBridgeStatus::EncodedReadTasksPlanned.requires_future_execution());
        assert!(!VortexRuntimeBridgeStatus::NoTasksRequired.requires_future_execution());
        assert!(!VortexRuntimeBridgeMode::TaskGraphPlanOnly.executes_tasks());
        assert!(VortexTaskMappingKind::EncodedEvaluateTask.requires_future_execution());
        assert!(!VortexTaskMappingKind::NoTaskNeeded.requires_future_execution());
    }

    #[test]
    fn unsupported_mapping_has_error_and_no_fallback() {
        let m = VortexTaskMapping::unsupported(None, "feat", "reason");
        assert!(m.has_errors());
        assert!(m.diagnostics.iter().any(|d| !d.fallback.attempted));
    }

    #[test]
    fn empty_report_side_effect_free() {
        let read =
            crate::VortexReadPlanningReport::from_input(crate::VortexReadPlanningInput::new())
                .expect("read report");
        let out =
            VortexRuntimeBridgeReport::from_read_planning_report(read).expect("bridge report");
        assert!(out.is_side_effect_free());
        assert!(out.to_human_text().contains("fallback execution disabled"));
        assert!(out.to_human_text().contains("data read: false"));
    }

    #[test]
    fn unsupported_read_plan_propagates_to_bridge() {
        let read = crate::VortexReadPlanningReport::unsupported("vortex-read-plan", "unsupported");
        let out =
            VortexRuntimeBridgeReport::from_read_planning_report(read).expect("bridge report");
        assert!(matches!(out.status, VortexRuntimeBridgeStatus::Unsupported));
        assert!(matches!(
            out.runtime_plan.status,
            shardloom_exec::RuntimePlanningStatus::Unsupported
        ));
        assert!(out.has_errors());
        assert_eq!(out.task_count, 0);
        assert!(!out.data_read);
        assert!(!out.data_materialized);
        assert!(!out.write_io);
        assert!(!out.object_store_io);
        assert!(!out.fallback_execution_allowed);
        let text = out.to_human_text();
        assert!(text.contains("fallback execution disabled"));
        assert!(text.contains("unsupported"));
    }

    #[test]
    fn blocked_read_plan_stays_blocked_without_tasks() {
        let mut read =
            crate::VortexReadPlanningReport::from_input(crate::VortexReadPlanningInput::new())
                .expect("read report");
        read.status = crate::VortexReadIntentStatus::BlockedByMissingMetadata;
        read.add_diagnostic(Diagnostic::new(
            DiagnosticCode::MissingStatistics,
            DiagnosticSeverity::Warning,
            shardloom_core::DiagnosticCategory::Statistics,
            "missing stats",
            Some("vortex-read-plan".to_string()),
            Some("blocked".to_string()),
            None,
            FallbackStatus::disabled_by_policy(),
        ));
        let out =
            VortexRuntimeBridgeReport::from_read_planning_report(read).expect("bridge report");
        assert!(matches!(
            out.status,
            VortexRuntimeBridgeStatus::BlockedByMissingMetadata
        ));
        assert_eq!(out.task_count, 0);
        assert!(!out.data_read);
        assert!(!out.data_materialized);
        assert!(!out.fallback_execution_allowed);
    }

    #[test]
    fn top_level_read_plan_diagnostic_is_preserved() {
        let mut read =
            crate::VortexReadPlanningReport::from_input(crate::VortexReadPlanningInput::new())
                .expect("read report");
        read.add_diagnostic(Diagnostic::new(
            DiagnosticCode::NotImplemented,
            DiagnosticSeverity::Warning,
            shardloom_core::DiagnosticCategory::Planning,
            "top-level bridge note",
            Some("vortex-read-plan".to_string()),
            Some("detail".to_string()),
            None,
            FallbackStatus::disabled_by_policy(),
        ));
        let out =
            VortexRuntimeBridgeReport::from_read_planning_report(read).expect("bridge report");
        assert!(out.to_human_text().contains("top-level bridge note"));
    }

    #[test]
    fn input_new_defaults_unbounded_budget_and_no_retry() {
        let read =
            crate::VortexReadPlanningReport::from_input(crate::VortexReadPlanningInput::new())
                .expect("read report");
        let input = VortexRuntimeBridgeInput::new(read);
        assert_eq!(input.resource_budget, ResourceBudget::unbounded());
        assert_eq!(input.retry_policy, RetryPolicy::none());
    }

    #[test]
    fn unsupported_report_has_errors_and_fallback_false() {
        let read =
            crate::VortexReadPlanningReport::from_input(crate::VortexReadPlanningInput::new())
                .expect("read report");
        let report = VortexRuntimeBridgeReport::unsupported(
            VortexRuntimeBridgeInput::new(read),
            "feature",
            "reason",
        );
        assert!(report.has_errors());
        assert!(report.diagnostics.iter().any(|d| !d.fallback.attempted));
    }

    #[test]
    fn helper_functions_delegate_without_io() {
        let read =
            crate::VortexReadPlanningReport::from_input(crate::VortexReadPlanningInput::new())
                .expect("read report");
        let report = build_vortex_runtime_task_graph(read).expect("bridge");
        assert!(vortex_runtime_bridge_is_side_effect_free(&report));
    }
}
