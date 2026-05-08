#![allow(
    clippy::missing_panics_doc,
    clippy::must_use_candidate,
    clippy::struct_excessive_bools
)]

use std::fmt::Write as _;

use shardloom_core::{DatasetUri, Diagnostic, DiagnosticCode, DiagnosticSeverity};
use shardloom_exec::{
    EncodedBatchRepresentation, EncodedStreamingBatchPlanReport, EncodedStreamingBatchPlanStatus,
    StreamingSourceKind, ZeroDecodeStatus,
};

use crate::{
    VortexEncodedReadExecutionMode, VortexEncodedReadExecutionReport,
    VortexEncodedReadExecutionStatus, VortexEncodedReadExecutorFeatureStatus,
};

const STREAMING_BATCH_RUNTIME_SCHEMA_VERSION: &str = "shardloom.vortex.streaming-batch-runtime.v1";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexStreamingBatchRuntimeStatus {
    LocalEncodedCountBatchesExecuted,
    BlockedByPlan,
    BlockedBySourceMismatch,
    BlockedByLocalScan,
    BlockedByUnsafeEffect,
    Unsupported,
}
impl VortexStreamingBatchRuntimeStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::LocalEncodedCountBatchesExecuted => "local_encoded_count_batches_executed",
            Self::BlockedByPlan => "blocked_by_plan",
            Self::BlockedBySourceMismatch => "blocked_by_source_mismatch",
            Self::BlockedByLocalScan => "blocked_by_local_scan",
            Self::BlockedByUnsafeEffect => "blocked_by_unsafe_effect",
            Self::Unsupported => "unsupported",
        }
    }

    pub const fn is_error(self) -> bool {
        !matches!(self, Self::LocalEncodedCountBatchesExecuted)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexStreamingBatchRuntimeMode {
    LocalEncodedCountBatches,
    ReportOnly,
    Unsupported,
}
impl VortexStreamingBatchRuntimeMode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::LocalEncodedCountBatches => "local_encoded_count_batches",
            Self::ReportOnly => "report_only",
            Self::Unsupported => "unsupported",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::struct_excessive_bools)]
pub struct VortexStreamingBatchRuntimeReport {
    pub schema_version: &'static str,
    pub status: VortexStreamingBatchRuntimeStatus,
    pub mode: VortexStreamingBatchRuntimeMode,
    pub plan: EncodedStreamingBatchPlanReport,
    pub encoded_read: VortexEncodedReadExecutionReport,
    pub representation: EncodedBatchRepresentation,
    pub zero_decode: ZeroDecodeStatus,
    pub encoded_representation_preserved: bool,
    pub selection_vector_preserved: bool,
    pub bounded_parallelism: bool,
    pub bounded_memory: bool,
    pub backpressure_bounded: bool,
    pub source_uri: Option<DatasetUri>,
    pub local_scan_target_uri: Option<DatasetUri>,
    pub source_uri_matches_local_scan: bool,
    pub batches_executed: usize,
    pub rows_processed: u64,
    pub count_result: Option<u64>,
    pub streams_executed: bool,
    pub tasks_executed: bool,
    pub data_read: bool,
    pub data_decoded: bool,
    pub data_materialized: bool,
    pub row_read: bool,
    pub arrow_converted: bool,
    pub object_store_io: bool,
    pub write_io: bool,
    pub spill_io_performed: bool,
    pub external_effects_executed: bool,
    pub fallback_execution_allowed: bool,
    pub diagnostics: Vec<Diagnostic>,
}
impl VortexStreamingBatchRuntimeReport {
    fn report_only(
        plan: EncodedStreamingBatchPlanReport,
        encoded_read: VortexEncodedReadExecutionReport,
    ) -> Self {
        let source_uri = source_uri(&plan).cloned();
        let local_scan_target_uri = encoded_read.local_scan_target_uri.clone();
        let source_uri_matches_local_scan =
            source_uri.as_ref() == local_scan_target_uri.as_ref() && source_uri.is_some();
        let mut diagnostics = plan.diagnostics.clone();
        diagnostics.extend(encoded_read.diagnostics.clone());
        Self {
            schema_version: STREAMING_BATCH_RUNTIME_SCHEMA_VERSION,
            status: VortexStreamingBatchRuntimeStatus::Unsupported,
            mode: VortexStreamingBatchRuntimeMode::ReportOnly,
            representation: plan.representation,
            zero_decode: plan.zero_decode,
            encoded_representation_preserved: plan.encoded_representation_preserved,
            selection_vector_preserved: plan.selection_vector_preserved,
            bounded_parallelism: plan.bounded_parallelism,
            bounded_memory: plan.bounded_memory,
            backpressure_bounded: plan.backpressure_bounded,
            source_uri,
            local_scan_target_uri,
            source_uri_matches_local_scan,
            batches_executed: 0,
            rows_processed: 0,
            count_result: None,
            streams_executed: false,
            tasks_executed: false,
            data_read: false,
            data_decoded: false,
            data_materialized: false,
            row_read: false,
            arrow_converted: false,
            object_store_io: false,
            write_io: false,
            spill_io_performed: false,
            external_effects_executed: false,
            fallback_execution_allowed: false,
            diagnostics,
            plan,
            encoded_read,
        }
    }

    fn block(
        &mut self,
        status: VortexStreamingBatchRuntimeStatus,
        feature: &'static str,
        reason: impl Into<String>,
    ) {
        self.status = status;
        self.mode = VortexStreamingBatchRuntimeMode::Unsupported;
        self.diagnostics.push(Diagnostic::unsupported(
            DiagnosticCode::NotImplemented,
            feature,
            reason,
            Some("Fallback attempted: false".to_string()),
        ));
    }

    fn execute_from_local_scan(&mut self) {
        self.status = VortexStreamingBatchRuntimeStatus::LocalEncodedCountBatchesExecuted;
        self.mode = VortexStreamingBatchRuntimeMode::LocalEncodedCountBatches;
        self.batches_executed = self.encoded_read.arrays_read_count;
        self.rows_processed = self.encoded_read.rows_counted;
        self.count_result = self.encoded_read.count_result;
        self.streams_executed = true;
        self.tasks_executed = true;
        self.data_read = self.encoded_read.data_read;
        self.data_decoded = self.encoded_read.data_decoded;
        self.data_materialized = self.encoded_read.data_materialized;
        self.row_read = self.encoded_read.row_read;
        self.arrow_converted = self.encoded_read.arrow_converted;
        self.object_store_io = self.encoded_read.object_store_io;
        self.write_io = self.encoded_read.write_io;
        self.spill_io_performed = self.encoded_read.spill_io_performed;
        self.external_effects_executed = self.encoded_read.external_effects_executed;
        self.fallback_execution_allowed = self.encoded_read.fallback_execution_allowed;
    }

    pub fn has_errors(&self) -> bool {
        self.status.is_error()
            || self.plan.has_errors()
            || self.encoded_read.has_errors()
            || self.diagnostics.iter().any(|diagnostic| {
                matches!(
                    diagnostic.severity,
                    DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
                )
            })
    }

    pub const fn is_side_effect_free(&self) -> bool {
        !self.streams_executed
            && !self.tasks_executed
            && !self.data_read
            && !self.data_decoded
            && !self.data_materialized
            && !self.row_read
            && !self.arrow_converted
            && !self.object_store_io
            && !self.write_io
            && !self.spill_io_performed
            && !self.external_effects_executed
            && !self.fallback_execution_allowed
    }

    pub fn to_human_text(&self) -> String {
        let mut out = String::new();
        let _ = writeln!(out, "vortex streaming-batch runtime report");
        let _ = writeln!(out, "schema version: {}", self.schema_version);
        let _ = writeln!(out, "status: {}", self.status.as_str());
        let _ = writeln!(out, "mode: {}", self.mode.as_str());
        let _ = writeln!(out, "representation: {}", self.representation.as_str());
        let _ = writeln!(out, "zero decode: {}", self.zero_decode.as_str());
        let _ = writeln!(
            out,
            "encoded representation preserved: {}",
            self.encoded_representation_preserved
        );
        let _ = writeln!(
            out,
            "selection vector preserved: {}",
            self.selection_vector_preserved
        );
        let _ = writeln!(out, "bounded parallelism: {}", self.bounded_parallelism);
        let _ = writeln!(out, "bounded memory: {}", self.bounded_memory);
        let _ = writeln!(out, "backpressure bounded: {}", self.backpressure_bounded);
        let _ = writeln!(
            out,
            "source URI: {}",
            self.source_uri
                .as_ref()
                .map_or("<none>", DatasetUri::as_str)
        );
        let _ = writeln!(
            out,
            "local scan target URI: {}",
            self.local_scan_target_uri
                .as_ref()
                .map_or("<none>", DatasetUri::as_str)
        );
        let _ = writeln!(
            out,
            "source URI matches local scan: {}",
            self.source_uri_matches_local_scan
        );
        let _ = writeln!(out, "batches executed: {}", self.batches_executed);
        let _ = writeln!(out, "rows processed: {}", self.rows_processed);
        let _ = writeln!(
            out,
            "count result: {}",
            self.count_result
                .map_or_else(|| "none".to_string(), |count| count.to_string())
        );
        let _ = writeln!(out, "streams executed: {}", self.streams_executed);
        let _ = writeln!(out, "tasks executed: {}", self.tasks_executed);
        let _ = writeln!(out, "data read: {}", self.data_read);
        let _ = writeln!(out, "data decoded: {}", self.data_decoded);
        let _ = writeln!(out, "data materialized: {}", self.data_materialized);
        let _ = writeln!(out, "row read: {}", self.row_read);
        let _ = writeln!(out, "Arrow converted: {}", self.arrow_converted);
        let _ = writeln!(out, "object-store IO: {}", self.object_store_io);
        let _ = writeln!(out, "write IO: {}", self.write_io);
        let _ = writeln!(out, "spill IO performed: {}", self.spill_io_performed);
        let _ = writeln!(
            out,
            "external effects executed: {}",
            self.external_effects_executed
        );
        let _ = writeln!(out, "fallback execution disabled");
        if !self.diagnostics.is_empty() {
            let _ = writeln!(out, "diagnostics:");
            for diagnostic in &self.diagnostics {
                let _ = writeln!(
                    out,
                    "- [{}] {}",
                    diagnostic.code.as_str(),
                    diagnostic.message
                );
            }
        }
        out
    }
}

pub fn execute_vortex_streaming_batches_from_local_encoded_count(
    plan: EncodedStreamingBatchPlanReport,
    encoded_read: VortexEncodedReadExecutionReport,
) -> VortexStreamingBatchRuntimeReport {
    let mut report = VortexStreamingBatchRuntimeReport::report_only(plan, encoded_read);

    if report.plan.status != EncodedStreamingBatchPlanStatus::Planned
        || report.plan.has_errors()
        || report.plan.materialization_boundary.required
        || report.plan.input.sink.requirement.requires_materialization
        || !report.plan.encoded_representation_preserved
        || !matches!(
            report.plan.representation,
            EncodedBatchRepresentation::VortexEncoded
                | EncodedBatchRepresentation::SelectionVectorEncoded
        )
        || report.plan.data_decoded
        || report.plan.data_materialized
        || report.plan.row_read
        || report.plan.arrow_converted
        || report.plan.object_store_io
        || report.plan.write_io
        || report.plan.spill_io_performed
        || report.plan.fallback_execution_allowed
    {
        report.block(
            VortexStreamingBatchRuntimeStatus::BlockedByPlan,
            "vortex_streaming_batch_runtime",
            "streaming-batch runtime requires a planned zero-decode Vortex source/sink path with no materialization, object-store IO, writes, spill, or fallback",
        );
        return report;
    }

    if report.plan.input.source.kind != StreamingSourceKind::VortexSegment {
        report.block(
            VortexStreamingBatchRuntimeStatus::BlockedByPlan,
            "vortex_streaming_batch_runtime",
            "streaming-batch runtime is currently limited to local Vortex segment sources",
        );
        return report;
    }

    if !report.source_uri_matches_local_scan {
        report.block(
            VortexStreamingBatchRuntimeStatus::BlockedBySourceMismatch,
            "vortex_streaming_batch_runtime",
            "streaming-batch plan source URI must match the executed local scan target URI",
        );
        return report;
    }

    if report.encoded_read.feature_status != VortexEncodedReadExecutorFeatureStatus::Enabled
        || report.encoded_read.status
            != VortexEncodedReadExecutionStatus::LocalScanEncodedCountExecuted
        || report.encoded_read.mode
            != VortexEncodedReadExecutionMode::LocalScanEncodedArrayLengthCount
        || report.encoded_read.has_errors()
        || !report.encoded_read.upstream_scan_called
        || !report.encoded_read.data_read
        || report.encoded_read.count_result.is_none()
    {
        report.block(
            VortexStreamingBatchRuntimeStatus::BlockedByLocalScan,
            "vortex_streaming_batch_runtime",
            "streaming-batch runtime requires a successful approved local scan encoded-count execution report",
        );
        return report;
    }

    if report.encoded_read.data_decoded
        || report.encoded_read.data_materialized
        || report.encoded_read.row_read
        || report.encoded_read.arrow_converted
        || report.encoded_read.object_store_io
        || report.encoded_read.write_io
        || report.encoded_read.spill_io_performed
        || report.encoded_read.external_effects_executed
        || report.encoded_read.fallback_execution_allowed
    {
        report.block(
            VortexStreamingBatchRuntimeStatus::BlockedByUnsafeEffect,
            "vortex_streaming_batch_runtime",
            "streaming-batch runtime rejects local scan reports with decode, materialization, row reads, Arrow conversion, object-store IO, writes, spill, external effects, or fallback",
        );
        return report;
    }

    report.execute_from_local_scan();
    report
}

pub const fn vortex_streaming_batch_runtime_schema_version() -> &'static str {
    STREAMING_BATCH_RUNTIME_SCHEMA_VERSION
}

pub fn vortex_streaming_batch_runtime_is_side_effect_free(
    report: &VortexStreamingBatchRuntimeReport,
) -> bool {
    report.is_side_effect_free()
}

fn source_uri(plan: &EncodedStreamingBatchPlanReport) -> Option<&DatasetUri> {
    plan.input
        .source
        .dataset
        .as_ref()
        .map(|dataset| &dataset.uri)
}

#[cfg(test)]
mod tests {
    use super::*;
    use shardloom_core::{DatasetRef, DatasetUri};
    use shardloom_exec::{
        BoundedMemoryPolicy, ByteSize, EncodedStreamingBatchPlanInput, StreamingSink,
        StreamingSource, plan_encoded_streaming_batches,
    };

    use crate::{
        VortexEncodedReadExecutionInput, VortexSchedulerBridgeInput, VortexTaskSchedulingDecision,
        plan_native_vortex_universal_input,
    };

    fn uri() -> DatasetUri {
        DatasetUri::new("file://tmp/runtime.vortex").expect("uri")
    }

    fn streaming_plan(target_uri: DatasetUri) -> EncodedStreamingBatchPlanReport {
        let source =
            StreamingSource::vortex_dataset(DatasetRef::from_uri(target_uri).expect("dataset ref"));
        let input = EncodedStreamingBatchPlanInput::new(
            source,
            StreamingSink::null_benchmark(),
            BoundedMemoryPolicy::required(ByteSize::from_mib(64)),
            2,
        )
        .expect("input");
        plan_encoded_streaming_batches(input).expect("plan")
    }

    fn readiness_for_uri(target_uri: DatasetUri) -> crate::VortexEncodedReadReadinessReport {
        let source =
            shardloom_core::UniversalInputSource::from_dataset_uri(target_uri).expect("source");
        let input_plan = plan_native_vortex_universal_input(source).expect("input plan");
        let read_report =
            crate::plan_vortex_read_from_universal_input(input_plan).expect("read plan");
        let runtime_report =
            crate::build_vortex_runtime_task_graph(read_report).expect("runtime bridge");
        let sizing_report = crate::size_vortex_runtime_task_graph(
            runtime_report,
            shardloom_exec::AdaptiveSizingPolicy::memory_limited(ByteSize::from_gib(1)),
        )
        .expect("sizing");
        let memory = crate::plan_vortex_memory_safety(
            sizing_report,
            shardloom_exec::MemoryBudget::from_gib(1).expect("memory budget"),
        )
        .expect("memory bridge");
        let mut scheduler =
            crate::VortexSchedulerBridgeReport::from_input(VortexSchedulerBridgeInput::new(memory))
                .expect("scheduler bridge");
        scheduler.decisions.clear();
        scheduler
            .decisions
            .push(VortexTaskSchedulingDecision::schedule_now(
                None,
                "local streaming-batch count scan",
            ));
        scheduler.recompute_counts();
        crate::VortexEncodedReadReadinessReport::from_scheduler_report(scheduler)
            .expect("readiness")
    }

    fn local_scan_report(target_uri: DatasetUri, count: u64) -> VortexEncodedReadExecutionReport {
        let readiness = readiness_for_uri(target_uri.clone());
        let mut report = VortexEncodedReadExecutionReport::feature_disabled(
            VortexEncodedReadExecutionInput::new(readiness).allow_encoded_read_execution(true),
        );
        report.feature_status = VortexEncodedReadExecutorFeatureStatus::Enabled;
        report.status = VortexEncodedReadExecutionStatus::LocalScanEncodedCountExecuted;
        report.mode = VortexEncodedReadExecutionMode::LocalScanEncodedArrayLengthCount;
        report.data_read = true;
        report.upstream_scan_called = true;
        report.arrays_read_count = 3;
        report.rows_counted = count;
        report.count_result = Some(count);
        report.local_scan_target_uri = Some(target_uri.clone());
        report.local_scan_readiness_source_uri = Some(target_uri);
        report.local_scan_source_uri_matches_target = true;
        report
    }

    #[test]
    fn local_encoded_count_scan_becomes_streaming_batch_runtime_evidence() {
        let target_uri = uri();
        let plan = streaming_plan(target_uri.clone());
        let encoded_read = local_scan_report(target_uri, 42);

        let report = execute_vortex_streaming_batches_from_local_encoded_count(plan, encoded_read);

        assert_eq!(
            report.status,
            VortexStreamingBatchRuntimeStatus::LocalEncodedCountBatchesExecuted
        );
        assert_eq!(
            report.mode,
            VortexStreamingBatchRuntimeMode::LocalEncodedCountBatches
        );
        assert_eq!(report.batches_executed, 3);
        assert_eq!(report.rows_processed, 42);
        assert_eq!(report.count_result, Some(42));
        assert!(report.streams_executed);
        assert!(report.tasks_executed);
        assert!(report.data_read);
        assert!(!report.data_decoded);
        assert!(!report.data_materialized);
        assert!(!report.row_read);
        assert!(!report.arrow_converted);
        assert!(!report.object_store_io);
        assert!(!report.write_io);
        assert!(!report.spill_io_performed);
        assert!(!report.external_effects_executed);
        assert!(!report.fallback_execution_allowed);
        assert!(!report.is_side_effect_free());
        assert!(!report.has_errors());
    }

    #[test]
    fn source_mismatch_blocks_streaming_batch_runtime() {
        let plan = streaming_plan(uri());
        let encoded_read =
            local_scan_report(DatasetUri::new("file://tmp/other.vortex").expect("uri"), 42);

        let report = execute_vortex_streaming_batches_from_local_encoded_count(plan, encoded_read);

        assert_eq!(
            report.status,
            VortexStreamingBatchRuntimeStatus::BlockedBySourceMismatch
        );
        assert!(report.has_errors());
        assert!(report.is_side_effect_free());
        assert!(!report.data_read);
        assert!(!report.fallback_execution_allowed);
    }

    #[test]
    fn decode_or_row_read_blocks_streaming_batch_runtime() {
        let target_uri = uri();
        let plan = streaming_plan(target_uri.clone());
        let mut encoded_read = local_scan_report(target_uri, 42);
        encoded_read.row_read = true;

        let report = execute_vortex_streaming_batches_from_local_encoded_count(plan, encoded_read);

        assert_eq!(
            report.status,
            VortexStreamingBatchRuntimeStatus::BlockedByUnsafeEffect
        );
        assert!(report.has_errors());
        assert!(report.is_side_effect_free());
        assert!(!report.data_read);
        assert!(!report.fallback_execution_allowed);
    }
}
