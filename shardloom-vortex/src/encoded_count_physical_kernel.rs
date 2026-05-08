use std::fmt::Write as _;

use shardloom_core::{
    BenchmarkEvidenceState, BenchmarkFallbackState, Diagnostic, DiagnosticCode,
    ExecutionCertificate, ExpectedOutcome, KernelKind, OperatorMemoryCertification,
    PhysicalKernelAdmissionReport, PhysicalKernelAdmissionStatus, PhysicalKernelRequirement,
    PhysicalKernelSlot, PhysicalOperatorContract, PhysicalOperatorExecutionLevel,
    PhysicalOperatorKind,
};

use crate::{
    VortexEncodedReadExecutionMode, VortexEncodedReadExecutionReport,
    VortexEncodedReadExecutionStatus, VortexLocalExecutionMode, VortexLocalExecutionReport,
    VortexLocalExecutionStatus, VortexLocalExecutionValue, VortexQueryPrimitiveKind,
    VortexQueryPrimitiveValue,
};

const SCHEMA_VERSION: &str = "shardloom.vortex_encoded_count_physical_kernel.v1";
const ADMISSION_SCHEMA_VERSION: &str = "shardloom.vortex_encoded_count_kernel_admission.v1";
const EXECUTION_KIND: &str = "vortex.local_encoded_count";
const KERNEL_REPORT_ID: &str = "vortex.query-primitive.count_all.encoded-count-physical-kernel";
const COUNT_OPERATOR_ID: &str = "vortex.query_primitive.count_all.count_aggregate";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexEncodedCountPhysicalKernelStatus {
    EvaluatedEncodedNative,
    BlockedByCertificate,
    BlockedByExecutionReport,
    BlockedByValue,
    BlockedByUnsafeEffect,
    Unsupported,
}

impl VortexEncodedCountPhysicalKernelStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::EvaluatedEncodedNative => "evaluated_encoded_native",
            Self::BlockedByCertificate => "blocked_by_certificate",
            Self::BlockedByExecutionReport => "blocked_by_execution_report",
            Self::BlockedByValue => "blocked_by_value",
            Self::BlockedByUnsafeEffect => "blocked_by_unsafe_effect",
            Self::Unsupported => "unsupported",
        }
    }

    #[must_use]
    pub const fn is_error(&self) -> bool {
        !matches!(self, Self::EvaluatedEncodedNative)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct VortexEncodedCountPhysicalKernelDiscoveryReport {
    pub schema_version: &'static str,
    pub kernel_report_id: &'static str,
    pub supported_primitive: VortexQueryPrimitiveKind,
    pub operator_kind: PhysicalOperatorKind,
    pub kernel_kind: KernelKind,
    pub execution_level: PhysicalOperatorExecutionLevel,
    pub contextual_only: bool,
    pub requires_execution_certificate: bool,
    pub requires_correctness_evidence: bool,
    pub requires_memory_safety_evidence: bool,
    pub requires_benchmark_for_production: bool,
    pub discovery_reads_data: bool,
    pub evaluated_path_reads_data: bool,
    pub runtime_execution_allowed_by_discovery: bool,
    pub fallback_execution_allowed: bool,
}

impl VortexEncodedCountPhysicalKernelDiscoveryReport {
    #[must_use]
    pub const fn report_only() -> Self {
        Self {
            schema_version: SCHEMA_VERSION,
            kernel_report_id: KERNEL_REPORT_ID,
            supported_primitive: VortexQueryPrimitiveKind::CountAll,
            operator_kind: PhysicalOperatorKind::CountAggregate,
            kernel_kind: KernelKind::Encoded,
            execution_level: PhysicalOperatorExecutionLevel::EncodedNative,
            contextual_only: true,
            requires_execution_certificate: true,
            requires_correctness_evidence: true,
            requires_memory_safety_evidence: true,
            requires_benchmark_for_production: true,
            discovery_reads_data: false,
            evaluated_path_reads_data: true,
            runtime_execution_allowed_by_discovery: false,
            fallback_execution_allowed: false,
        }
    }

    #[must_use]
    pub const fn is_side_effect_free(&self) -> bool {
        !self.discovery_reads_data
            && !self.runtime_execution_allowed_by_discovery
            && !self.fallback_execution_allowed
    }

    #[must_use]
    pub fn to_human_text(&self) -> String {
        format!(
            "Vortex encoded count physical kernel discovery\nschema_version: {}\nkernel: {}\nprimitive: {}\noperator: {}\nkernel kind: {}\nexecution level: {}\ncontextual only: {}\ndiscovery reads data: {}\nruntime execution: disabled\nfallback execution: disabled",
            self.schema_version,
            self.kernel_report_id,
            self.supported_primitive.as_str(),
            self.operator_kind.as_str(),
            self.kernel_kind.as_str(),
            self.execution_level.as_str(),
            self.contextual_only,
            self.discovery_reads_data,
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::struct_excessive_bools)]
pub struct VortexEncodedCountPhysicalKernelReport {
    pub schema_version: &'static str,
    pub kernel_report_id: String,
    pub primitive_kind: VortexQueryPrimitiveKind,
    pub operator_kind: PhysicalOperatorKind,
    pub kernel_kind: KernelKind,
    pub execution_level: PhysicalOperatorExecutionLevel,
    pub execution_certificate_id: Option<String>,
    pub status: VortexEncodedCountPhysicalKernelStatus,
    pub count_result: Option<u64>,
    pub arrays_read_count: usize,
    pub rows_counted: u64,
    pub data_read: bool,
    pub upstream_scan_called: bool,
    pub data_decoded: bool,
    pub data_materialized: bool,
    pub row_read: bool,
    pub arrow_converted: bool,
    pub object_store_io: bool,
    pub write_io: bool,
    pub spill_io_performed: bool,
    pub external_effects_executed: bool,
    pub fallback_attempted: bool,
    pub fallback_execution_allowed: bool,
    pub production_claim_allowed: bool,
    pub diagnostics: Vec<Diagnostic>,
}

impl VortexEncodedCountPhysicalKernelReport {
    #[must_use]
    pub fn evaluated(
        encoded_read: &VortexEncodedReadExecutionReport,
        local_execution: &VortexLocalExecutionReport,
        certificate: &ExecutionCertificate,
        count_result: u64,
    ) -> Self {
        Self {
            schema_version: SCHEMA_VERSION,
            kernel_report_id: KERNEL_REPORT_ID.to_string(),
            primitive_kind: VortexQueryPrimitiveKind::CountAll,
            operator_kind: PhysicalOperatorKind::CountAggregate,
            kernel_kind: KernelKind::Encoded,
            execution_level: PhysicalOperatorExecutionLevel::EncodedNative,
            execution_certificate_id: Some(certificate.certificate_id.clone()),
            status: VortexEncodedCountPhysicalKernelStatus::EvaluatedEncodedNative,
            count_result: Some(count_result),
            arrays_read_count: encoded_read.arrays_read_count,
            rows_counted: encoded_read.rows_counted,
            data_read: encoded_read.data_read || local_execution.data_read || certificate.data_read,
            upstream_scan_called: encoded_read.upstream_scan_called,
            data_decoded: false,
            data_materialized: false,
            row_read: false,
            arrow_converted: false,
            object_store_io: false,
            write_io: false,
            spill_io_performed: false,
            external_effects_executed: false,
            fallback_attempted: false,
            fallback_execution_allowed: false,
            production_claim_allowed: false,
            diagnostics: Vec::new(),
        }
    }

    #[must_use]
    pub fn blocked(
        encoded_read: &VortexEncodedReadExecutionReport,
        local_execution: &VortexLocalExecutionReport,
        certificate: &ExecutionCertificate,
        status: VortexEncodedCountPhysicalKernelStatus,
        diagnostic: Diagnostic,
    ) -> Self {
        let mut diagnostics = Vec::new();
        diagnostics.extend(encoded_read.diagnostics.clone());
        diagnostics.extend(local_execution.diagnostics.clone());
        diagnostics.extend(certificate.diagnostics.clone());
        diagnostics.push(diagnostic);
        Self {
            schema_version: SCHEMA_VERSION,
            kernel_report_id: KERNEL_REPORT_ID.to_string(),
            primitive_kind: VortexQueryPrimitiveKind::CountAll,
            operator_kind: PhysicalOperatorKind::CountAggregate,
            kernel_kind: KernelKind::Encoded,
            execution_level: PhysicalOperatorExecutionLevel::EncodedNative,
            execution_certificate_id: Some(certificate.certificate_id.clone()),
            status,
            count_result: None,
            arrays_read_count: encoded_read.arrays_read_count,
            rows_counted: encoded_read.rows_counted,
            data_read: false,
            upstream_scan_called: false,
            data_decoded: false,
            data_materialized: false,
            row_read: false,
            arrow_converted: false,
            object_store_io: false,
            write_io: false,
            spill_io_performed: false,
            external_effects_executed: false,
            fallback_attempted: certificate.fallback_attempted,
            fallback_execution_allowed: false,
            production_claim_allowed: false,
            diagnostics,
        }
    }

    #[must_use]
    pub const fn is_safe_native_kernel_evidence(&self) -> bool {
        matches!(
            self.status,
            VortexEncodedCountPhysicalKernelStatus::EvaluatedEncodedNative
        ) && self.data_read
            && self.upstream_scan_called
            && !self.data_decoded
            && !self.data_materialized
            && !self.row_read
            && !self.arrow_converted
            && !self.object_store_io
            && !self.write_io
            && !self.spill_io_performed
            && !self.external_effects_executed
            && !self.fallback_attempted
            && !self.fallback_execution_allowed
    }

    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.status.is_error()
            || self.diagnostics.iter().any(|diagnostic| {
                matches!(
                    diagnostic.severity,
                    shardloom_core::DiagnosticSeverity::Error
                        | shardloom_core::DiagnosticSeverity::Fatal
                )
            })
    }

    #[must_use]
    pub fn to_human_text(&self) -> String {
        let mut text = String::new();
        let _ = writeln!(text, "schema_version: {}", self.schema_version);
        let _ = writeln!(text, "kernel report: {}", self.kernel_report_id);
        let _ = writeln!(text, "primitive: {}", self.primitive_kind.as_str());
        let _ = writeln!(text, "operator: {}", self.operator_kind.as_str());
        let _ = writeln!(text, "kernel kind: {}", self.kernel_kind.as_str());
        let _ = writeln!(text, "execution level: {}", self.execution_level.as_str());
        let _ = writeln!(text, "status: {}", self.status.as_str());
        let _ = writeln!(
            text,
            "count result: {}",
            self.count_result
                .map_or_else(|| "none".to_string(), |count| count.to_string())
        );
        let _ = writeln!(text, "data read: {}", self.data_read);
        let _ = writeln!(text, "data decoded: {}", self.data_decoded);
        let _ = writeln!(text, "data materialized: {}", self.data_materialized);
        let _ = writeln!(text, "row read: {}", self.row_read);
        let _ = writeln!(text, "Arrow converted: {}", self.arrow_converted);
        let _ = writeln!(text, "object-store IO: {}", self.object_store_io);
        let _ = writeln!(text, "write IO: {}", self.write_io);
        let _ = writeln!(text, "spill IO performed: {}", self.spill_io_performed);
        let _ = writeln!(text, "fallback attempted: {}", self.fallback_attempted);
        let _ = writeln!(text, "fallback execution: disabled");
        let _ = writeln!(
            text,
            "production claim allowed: {}",
            self.production_claim_allowed
        );
        text
    }
}

#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::struct_excessive_bools)]
pub struct VortexEncodedCountKernelAdmissionReport {
    pub schema_version: &'static str,
    pub admission_id: String,
    pub physical_kernel_report_id: String,
    pub slot_id: String,
    pub operator_kind: PhysicalOperatorKind,
    pub required_kernel_kind: KernelKind,
    pub candidate_kernel_kind: KernelKind,
    pub correctness_evidence: BenchmarkEvidenceState,
    pub benchmark_evidence: BenchmarkEvidenceState,
    pub memory: OperatorMemoryCertification,
    pub fallback: BenchmarkFallbackState,
    pub status: PhysicalKernelAdmissionStatus,
    pub slot_marked_present: bool,
    pub production_claim_allowed: bool,
    pub runtime_execution_allowed: bool,
    pub fallback_execution_allowed: bool,
    pub diagnostics: Vec<Diagnostic>,
}

impl VortexEncodedCountKernelAdmissionReport {
    #[must_use]
    pub fn from_admission(
        physical_kernel: &VortexEncodedCountPhysicalKernelReport,
        admission: PhysicalKernelAdmissionReport,
    ) -> Self {
        let mut diagnostics = physical_kernel.diagnostics.clone();
        diagnostics.extend(admission.diagnostics.clone());
        let slot_marked_present = admission.can_mark_kernel_present();
        let production_claim_allowed = admission.can_satisfy_production_claim();
        Self {
            schema_version: ADMISSION_SCHEMA_VERSION,
            admission_id: format!("{}.admission", physical_kernel.kernel_report_id),
            physical_kernel_report_id: physical_kernel.kernel_report_id.clone(),
            slot_id: admission.slot_id,
            operator_kind: admission.operator_kind,
            required_kernel_kind: admission.required_kernel_kind,
            candidate_kernel_kind: admission.candidate_kernel_kind,
            correctness_evidence: admission.correctness_evidence,
            benchmark_evidence: admission.benchmark_evidence,
            memory: admission.memory,
            fallback: admission.fallback,
            status: admission.status,
            slot_marked_present,
            production_claim_allowed,
            runtime_execution_allowed: false,
            fallback_execution_allowed: false,
            diagnostics,
        }
    }

    #[must_use]
    pub fn has_errors(&self) -> bool {
        !self.status.can_enter_registry()
            || self.diagnostics.iter().any(|diagnostic| {
                matches!(
                    diagnostic.severity,
                    shardloom_core::DiagnosticSeverity::Error
                        | shardloom_core::DiagnosticSeverity::Fatal
                )
            })
    }

    #[must_use]
    pub const fn is_side_effect_free(&self) -> bool {
        !self.runtime_execution_allowed && !self.fallback_execution_allowed
    }

    #[must_use]
    pub fn to_human_text(&self) -> String {
        format!(
            "encoded count kernel admission\nschema_version: {}\nadmission: {}\nslot: {}\noperator: {}\nrequired kernel: {}\ncandidate kernel: {}\nstatus: {}\nslot marked present: {}\nproduction claim allowed: {}\nruntime execution: disabled\nfallback execution: disabled",
            self.schema_version,
            self.admission_id,
            self.slot_id,
            self.operator_kind.as_str(),
            self.required_kernel_kind.as_str(),
            self.candidate_kernel_kind.as_str(),
            self.status.as_str(),
            self.slot_marked_present,
            self.production_claim_allowed
        )
    }
}

/// Evaluates the first contextual encoded-native count physical kernel from
/// already-approved local encoded count evidence.
///
/// This function does not open files, scan data, evaluate predicates, decode,
/// materialize, convert to Arrow, touch object stores, write, spill, invoke
/// baselines, or fallback. It only checks that the existing local encoded
/// `CountAll` execution report, local execution bridge, and CG-16 execution
/// certificate agree.
#[must_use]
pub fn evaluate_vortex_local_encoded_count_physical_kernel(
    encoded_read: &VortexEncodedReadExecutionReport,
    local_execution: &VortexLocalExecutionReport,
    certificate: &ExecutionCertificate,
) -> VortexEncodedCountPhysicalKernelReport {
    if !certificate.is_certified()
        || !certificate.fallback_free()
        || certificate.unsafe_effect_detected
        || certificate.execution_kind != EXECUTION_KIND
    {
        return VortexEncodedCountPhysicalKernelReport::blocked(
            encoded_read,
            local_execution,
            certificate,
            VortexEncodedCountPhysicalKernelStatus::BlockedByCertificate,
            Diagnostic::not_implemented(
                "vortex_encoded_count_physical_kernel",
                "encoded count physical kernel evaluation requires a certified local encoded count execution certificate",
                "Use a certified CG-16 local encoded CountAll execution certificate before evaluating the encoded count physical kernel.",
            ),
        );
    }
    if encoded_read.status != VortexEncodedReadExecutionStatus::LocalScanEncodedCountExecuted
        || encoded_read.mode != VortexEncodedReadExecutionMode::LocalScanEncodedArrayLengthCount
        || local_execution.status != VortexLocalExecutionStatus::LocalEncodedCountExecuted
        || local_execution.mode != VortexLocalExecutionMode::LocalEncodedCount
        || encoded_read.has_errors()
        || local_execution.has_errors()
    {
        return VortexEncodedCountPhysicalKernelReport::blocked(
            encoded_read,
            local_execution,
            certificate,
            VortexEncodedCountPhysicalKernelStatus::BlockedByExecutionReport,
            Diagnostic::not_implemented(
                "vortex_encoded_count_physical_kernel",
                "encoded count physical kernel evaluation requires successful local encoded count execution reports",
                "Provide the approved local scan count report and matching local execution bridge report.",
            ),
        );
    }
    if unsafe_effect_detected(encoded_read, local_execution, certificate) {
        return VortexEncodedCountPhysicalKernelReport::blocked(
            encoded_read,
            local_execution,
            certificate,
            VortexEncodedCountPhysicalKernelStatus::BlockedByUnsafeEffect,
            Diagnostic::unsupported(
                DiagnosticCode::NoFallbackExecution,
                "vortex_encoded_count_physical_kernel",
                "encoded count physical kernel evidence contained unsafe effects",
                Some("Fallback attempted: false".to_string()),
            ),
        );
    }
    let Some(count_result) = matching_count(encoded_read, local_execution, certificate) else {
        return VortexEncodedCountPhysicalKernelReport::blocked(
            encoded_read,
            local_execution,
            certificate,
            VortexEncodedCountPhysicalKernelStatus::BlockedByValue,
            Diagnostic::invalid_input(
                "vortex_encoded_count_physical_kernel",
                "encoded count physical kernel evidence has mismatched count values",
                "Use encoded-read, local-execution, and certificate evidence for the same CountAll result.",
            ),
        );
    };

    VortexEncodedCountPhysicalKernelReport::evaluated(
        encoded_read,
        local_execution,
        certificate,
        count_result,
    )
}

#[must_use]
pub const fn vortex_encoded_count_physical_kernel_discovery_report()
-> VortexEncodedCountPhysicalKernelDiscoveryReport {
    VortexEncodedCountPhysicalKernelDiscoveryReport::report_only()
}

/// Admits the contextual encoded `CountAll` kernel evidence into the CG-7
/// count-aggregate encoded kernel slot.
///
/// This is still an evidence bridge. It does not register a global runtime
/// kernel, execute data, claim production readiness, or close the broader
/// count/aggregate kernel checklist.
///
/// # Errors
/// Returns an error only if the static count-aggregate operator contract cannot
/// be built.
pub fn admit_vortex_encoded_count_kernel(
    physical_kernel: &VortexEncodedCountPhysicalKernelReport,
) -> shardloom_core::Result<VortexEncodedCountKernelAdmissionReport> {
    let slot = encoded_count_kernel_slot()?;
    let safe_evidence = physical_kernel.is_safe_native_kernel_evidence();
    let admission = PhysicalKernelAdmissionReport::evaluate(
        &slot,
        KernelKind::Encoded,
        if safe_evidence {
            BenchmarkEvidenceState::Present
        } else {
            BenchmarkEvidenceState::Missing
        },
        BenchmarkEvidenceState::Missing,
        if safe_evidence {
            safe_encoded_count_memory()
        } else {
            OperatorMemoryCertification::unsupported()
        },
        if physical_kernel.fallback_attempted {
            BenchmarkFallbackState::Attempted
        } else {
            BenchmarkFallbackState::NotAttempted
        },
    );
    Ok(VortexEncodedCountKernelAdmissionReport::from_admission(
        physical_kernel,
        admission,
    ))
}

fn encoded_count_kernel_slot() -> shardloom_core::Result<PhysicalKernelSlot> {
    let operator = PhysicalOperatorContract::new(
        COUNT_OPERATOR_ID,
        PhysicalOperatorKind::CountAggregate,
        PhysicalOperatorExecutionLevel::EncodedNative,
        vec![
            PhysicalKernelRequirement::missing(KernelKind::Metadata),
            PhysicalKernelRequirement::missing(KernelKind::Encoded),
        ],
    )?;
    Ok(PhysicalKernelSlot::from_requirement(
        &operator,
        PhysicalKernelRequirement::missing(KernelKind::Encoded),
    ))
}

const fn safe_encoded_count_memory() -> OperatorMemoryCertification {
    OperatorMemoryCertification {
        streaming: true,
        bounded_memory: true,
        spillable: false,
        requires_full_materialization: false,
        requires_shuffle: false,
        oom_safe: true,
    }
}

fn matching_count(
    encoded_read: &VortexEncodedReadExecutionReport,
    local_execution: &VortexLocalExecutionReport,
    certificate: &ExecutionCertificate,
) -> Option<u64> {
    let encoded_count = encoded_read.count_result?;
    if encoded_read.rows_counted != encoded_count {
        return None;
    }
    let VortexLocalExecutionValue::QueryPrimitive(VortexQueryPrimitiveValue::Count(local_count)) =
        local_execution.value
    else {
        return None;
    };
    let Some(ExpectedOutcome::EncodedCount {
        count: certificate_count,
    }) = certificate.actual_outcome
    else {
        return None;
    };
    let Some(ExpectedOutcome::EncodedCount {
        count: expected_count,
    }) = certificate.expected_outcome
    else {
        return None;
    };
    (encoded_count == local_count
        && encoded_count == certificate_count
        && encoded_count == expected_count)
        .then_some(encoded_count)
}

fn unsafe_effect_detected(
    encoded_read: &VortexEncodedReadExecutionReport,
    local_execution: &VortexLocalExecutionReport,
    certificate: &ExecutionCertificate,
) -> bool {
    !encoded_read.data_read
        || !encoded_read.upstream_scan_called
        || !local_execution.tasks_executed
        || !local_execution.data_read
        || encoded_read.data_decoded
        || encoded_read.data_materialized
        || encoded_read.row_read
        || encoded_read.arrow_converted
        || encoded_read.object_store_io
        || encoded_read.write_io
        || encoded_read.spill_io_performed
        || encoded_read.external_effects_executed
        || encoded_read.fallback_execution_allowed
        || local_execution.data_decoded
        || local_execution.data_materialized
        || local_execution.object_store_io
        || local_execution.write_io
        || local_execution.spill_io_performed
        || local_execution.external_effects_executed
        || local_execution.fallback_execution_allowed
        || certificate.data_decoded
        || certificate.data_materialized
        || certificate.row_read
        || certificate.arrow_converted
        || certificate.object_store_io
        || certificate.write_io
        || certificate.spill_io_performed
        || certificate.external_effects_executed
        || certificate.unsafe_effect_detected
        || certificate.fallback_attempted
        || certificate.fallback_execution_allowed
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        VortexCountCandidateSource, VortexCountReadinessRequest, VortexEncodedReadExecutionInput,
        VortexEncodedReadExecutorFeatureStatus, VortexLocalExecutionInput,
        VortexQueryPrimitiveRequest,
    };
    use shardloom_core::{
        DatasetUri, ExecutionCertificateInput, ExecutionCertificateStatus, UniversalInputSource,
    };
    use shardloom_exec::{AdaptiveSizingPolicy, ByteSize, MemoryBudget};

    fn uri() -> DatasetUri {
        DatasetUri::new("file://tmp/a.vortex").expect("uri")
    }

    fn readiness_for_uri(target_uri: DatasetUri) -> crate::VortexEncodedReadReadinessReport {
        let source = UniversalInputSource::from_dataset_uri(target_uri).expect("source");
        let input_plan = crate::plan_native_vortex_universal_input(source).expect("input plan");
        let read_report =
            crate::plan_vortex_read_from_universal_input(input_plan).expect("read plan");
        let runtime_report =
            crate::build_vortex_runtime_task_graph(read_report).expect("runtime bridge");
        let sizing_report = crate::size_vortex_runtime_task_graph(
            runtime_report,
            AdaptiveSizingPolicy::memory_limited(ByteSize::from_gib(1)),
        )
        .expect("sizing");
        let memory = crate::plan_vortex_memory_safety(
            sizing_report,
            MemoryBudget::from_gib(1).expect("memory budget"),
        )
        .expect("memory bridge");
        let mut scheduler = crate::VortexSchedulerBridgeReport::from_input(
            crate::VortexSchedulerBridgeInput::new(memory),
        )
        .expect("scheduler bridge");
        scheduler.decisions.clear();
        scheduler
            .decisions
            .push(crate::VortexTaskSchedulingDecision::schedule_now(
                None,
                "local encoded count physical kernel test",
            ));
        scheduler.recompute_counts();
        crate::VortexEncodedReadReadinessReport::from_scheduler_report(scheduler)
            .expect("readiness")
    }

    fn encoded_report(count: u64) -> VortexEncodedReadExecutionReport {
        let target_uri = uri();
        let readiness = readiness_for_uri(target_uri.clone());
        let mut report = VortexEncodedReadExecutionReport::feature_disabled(
            VortexEncodedReadExecutionInput::new(readiness).allow_encoded_read_execution(true),
        );
        report.feature_status = VortexEncodedReadExecutorFeatureStatus::Enabled;
        report.status = VortexEncodedReadExecutionStatus::LocalScanEncodedCountExecuted;
        report.mode = VortexEncodedReadExecutionMode::LocalScanEncodedArrayLengthCount;
        report.data_read = true;
        report.upstream_scan_called = true;
        report.arrays_read_count = 2;
        report.rows_counted = count;
        report.count_result = Some(count);
        report.local_scan_target_uri = Some(target_uri.clone());
        report.local_scan_readiness_source_uri = Some(target_uri);
        report.local_scan_source_uri_matches_target = true;
        report
    }

    fn local_report(count: u64) -> VortexLocalExecutionReport {
        VortexLocalExecutionReport::local_encoded_count_executed(
            VortexLocalExecutionInput::new(VortexQueryPrimitiveRequest::count_all(uri()))
                .allow_encoded_read(true),
            count,
        )
    }

    fn certificate(count: u64) -> ExecutionCertificate {
        let mut input =
            ExecutionCertificateInput::new("fixture.execution-certificate", EXECUTION_KIND)
                .expect("certificate input");
        input.plan_ref = Some("vortex-count:local-encoded-count".to_string());
        input.input_ref = Some(uri().as_str().to_string());
        input.output_ref = Some(format!("count_all_result={count}"));
        input.correctness_fixture_id = Some("fixture".to_string());
        input.expected_outcome = Some(ExpectedOutcome::EncodedCount { count });
        input.actual_outcome = Some(ExpectedOutcome::EncodedCount { count });
        input.side_effects_performed = vec![
            "local_vortex_scan".to_string(),
            "local_execution_task".to_string(),
        ];
        input.data_read = true;
        input.correctness_passed = true;
        ExecutionCertificate::evaluate(input)
    }

    #[test]
    fn discovery_report_is_report_only() {
        let report = vortex_encoded_count_physical_kernel_discovery_report();

        assert_eq!(report.schema_version, SCHEMA_VERSION);
        assert_eq!(
            report.supported_primitive,
            VortexQueryPrimitiveKind::CountAll
        );
        assert_eq!(report.operator_kind, PhysicalOperatorKind::CountAggregate);
        assert_eq!(report.kernel_kind, KernelKind::Encoded);
        assert_eq!(
            report.execution_level,
            PhysicalOperatorExecutionLevel::EncodedNative
        );
        assert!(report.contextual_only);
        assert!(report.requires_execution_certificate);
        assert!(report.requires_correctness_evidence);
        assert!(report.requires_memory_safety_evidence);
        assert!(report.requires_benchmark_for_production);
        assert!(!report.discovery_reads_data);
        assert!(report.evaluated_path_reads_data);
        assert!(report.is_side_effect_free());
        assert!(
            report
                .to_human_text()
                .contains("fallback execution: disabled")
        );
    }

    #[test]
    fn certified_local_encoded_count_evaluates_encoded_kernel() {
        let encoded = encoded_report(42);
        let local = local_report(42);
        let certificate = certificate(42);
        assert_eq!(certificate.status, ExecutionCertificateStatus::Certified);

        let report =
            evaluate_vortex_local_encoded_count_physical_kernel(&encoded, &local, &certificate);

        assert_eq!(
            report.status,
            VortexEncodedCountPhysicalKernelStatus::EvaluatedEncodedNative
        );
        assert_eq!(report.count_result, Some(42));
        assert_eq!(report.rows_counted, 42);
        assert_eq!(report.arrays_read_count, 2);
        assert_eq!(report.operator_kind, PhysicalOperatorKind::CountAggregate);
        assert_eq!(report.kernel_kind, KernelKind::Encoded);
        assert_eq!(
            report.execution_level,
            PhysicalOperatorExecutionLevel::EncodedNative
        );
        assert!(report.data_read);
        assert!(report.upstream_scan_called);
        assert!(!report.data_decoded);
        assert!(!report.data_materialized);
        assert!(!report.row_read);
        assert!(!report.arrow_converted);
        assert!(!report.object_store_io);
        assert!(!report.write_io);
        assert!(!report.spill_io_performed);
        assert!(!report.external_effects_executed);
        assert!(!report.fallback_attempted);
        assert!(!report.fallback_execution_allowed);
        assert!(!report.production_claim_allowed);
        assert!(report.is_safe_native_kernel_evidence());
        assert!(!report.has_errors());
    }

    #[test]
    fn safe_encoded_count_kernel_admits_encoded_slot_without_production_claim() {
        let encoded = encoded_report(42);
        let local = local_report(42);
        let certificate = certificate(42);
        let physical_kernel =
            evaluate_vortex_local_encoded_count_physical_kernel(&encoded, &local, &certificate);

        let admission =
            admit_vortex_encoded_count_kernel(&physical_kernel).expect("admission report");

        assert_eq!(admission.schema_version, ADMISSION_SCHEMA_VERSION);
        assert_eq!(
            admission.status,
            PhysicalKernelAdmissionStatus::RegistryReady
        );
        assert_eq!(
            admission.operator_kind,
            PhysicalOperatorKind::CountAggregate
        );
        assert_eq!(admission.required_kernel_kind, KernelKind::Encoded);
        assert_eq!(admission.candidate_kernel_kind, KernelKind::Encoded);
        assert_eq!(
            admission.correctness_evidence,
            BenchmarkEvidenceState::Present
        );
        assert_eq!(
            admission.benchmark_evidence,
            BenchmarkEvidenceState::Missing
        );
        assert!(admission.memory.streaming);
        assert!(admission.memory.bounded_memory);
        assert!(admission.memory.oom_safe);
        assert!(!admission.memory.requires_full_materialization);
        assert!(admission.slot_marked_present);
        assert!(!admission.production_claim_allowed);
        assert!(admission.is_side_effect_free());
        assert!(!admission.has_errors());
        assert!(!admission.fallback_execution_allowed);
        assert!(
            admission
                .to_human_text()
                .contains("fallback execution: disabled")
        );
    }

    #[test]
    fn blocked_physical_kernel_cannot_admit_encoded_slot() {
        let encoded = encoded_report(42);
        let local = local_report(42);
        let certificate = certificate(43);
        let physical_kernel =
            evaluate_vortex_local_encoded_count_physical_kernel(&encoded, &local, &certificate);

        let admission =
            admit_vortex_encoded_count_kernel(&physical_kernel).expect("admission report");

        assert_eq!(
            admission.status,
            PhysicalKernelAdmissionStatus::BlockedMissingCorrectness
        );
        assert_eq!(
            admission.correctness_evidence,
            BenchmarkEvidenceState::Missing
        );
        assert!(!admission.slot_marked_present);
        assert!(!admission.production_claim_allowed);
        assert!(admission.has_errors());
        assert!(!admission.fallback_execution_allowed);
    }

    #[test]
    fn mismatched_certificate_count_blocks_kernel_evaluation() {
        let encoded = encoded_report(42);
        let local = local_report(42);
        let certificate = certificate(43);

        let report =
            evaluate_vortex_local_encoded_count_physical_kernel(&encoded, &local, &certificate);

        assert_eq!(
            report.status,
            VortexEncodedCountPhysicalKernelStatus::BlockedByValue
        );
        assert!(report.has_errors());
        assert!(!report.data_read);
        assert!(!report.fallback_execution_allowed);
    }

    #[test]
    fn unsafe_effect_blocks_kernel_evaluation() {
        let mut encoded = encoded_report(42);
        encoded.row_read = true;
        let local = local_report(42);
        let certificate = certificate(42);

        let report =
            evaluate_vortex_local_encoded_count_physical_kernel(&encoded, &local, &certificate);

        assert_eq!(
            report.status,
            VortexEncodedCountPhysicalKernelStatus::BlockedByUnsafeEffect
        );
        assert!(report.has_errors());
        assert!(!report.data_read);
        assert!(!report.fallback_execution_allowed);
    }

    #[test]
    fn wrong_execution_status_blocks_kernel_evaluation() {
        let mut encoded = encoded_report(42);
        encoded.status = VortexEncodedReadExecutionStatus::WouldExecuteEncodedRead;
        let local = local_report(42);
        let certificate = certificate(42);

        let report =
            evaluate_vortex_local_encoded_count_physical_kernel(&encoded, &local, &certificate);

        assert_eq!(
            report.status,
            VortexEncodedCountPhysicalKernelStatus::BlockedByExecutionReport
        );
        assert!(report.has_errors());
        assert!(!report.data_read);
    }

    #[test]
    fn wrong_certificate_kind_blocks_kernel_evaluation() {
        let encoded = encoded_report(42);
        let local = local_report(42);
        let mut input =
            ExecutionCertificateInput::new("fixture.execution-certificate", "wrong.kind")
                .expect("certificate input");
        input.expected_outcome = Some(ExpectedOutcome::EncodedCount { count: 42 });
        input.actual_outcome = Some(ExpectedOutcome::EncodedCount { count: 42 });
        input.correctness_passed = true;
        let certificate = ExecutionCertificate::evaluate(input);

        let report =
            evaluate_vortex_local_encoded_count_physical_kernel(&encoded, &local, &certificate);

        assert_eq!(
            report.status,
            VortexEncodedCountPhysicalKernelStatus::BlockedByCertificate
        );
        assert!(report.has_errors());
    }

    #[test]
    fn count_readiness_import_stays_used_for_public_boundary_shape() {
        let readiness = crate::plan_vortex_count_readiness(
            VortexCountReadinessRequest::new(uri(), VortexCountCandidateSource::EncodedDataPath)
                .feature_gate_enabled(true)
                .query_primitive_ready(true)
                .count_primitive(true)
                .encoded_data_path_ready(true),
        )
        .expect("readiness");

        assert!(readiness.encoded_data_path_ready());
    }
}
