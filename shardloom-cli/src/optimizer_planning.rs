//! Optimizer, kernel registry, and CPU specialization CLI planning handlers.
//!
//! These handlers remain report-only planning surfaces. They do not execute
//! optimizer work, physical kernels, CPU-specialized kernels, external engines,
//! writes, materialization, or fallback execution.

use std::process::ExitCode;

use shardloom_core::{
    CommandStatus, CpuOperatorSpecializationReport, KernelRegistrySnapshot, OutputFormat,
    PhysicalKernelRegistryPlan, PhysicalOperatorExecutionLevel,
    PhysicalOperatorExecutionProfileMatrix, plan_cpu_operator_specialization,
};
use shardloom_plan::{
    AdaptiveOptimizerMemoryReport, OptimizerPhase, OptimizerPlanSkeleton,
    plan_adaptive_optimizer_memory,
};

use crate::cli_output::emit;

pub(crate) fn handle_kernel_registry(format: OutputFormat) -> ExitCode {
    let snapshot = KernelRegistrySnapshot::empty();
    let physical_plan = PhysicalKernelRegistryPlan::cg7_foundation();
    let execution_profiles = PhysicalOperatorExecutionProfileMatrix::cg7_foundation();
    let fields = kernel_registry_fields(&snapshot, &physical_plan, &execution_profiles);

    emit(
        "kernel-registry",
        format,
        CommandStatus::Success,
        "kernel registry snapshot".to_string(),
        format!("{}\n{}", snapshot.summary(), physical_plan.to_human_text()),
        physical_plan.diagnostics.clone(),
        fields,
    );
    ExitCode::SUCCESS
}

fn kernel_registry_fields(
    snapshot: &KernelRegistrySnapshot,
    physical_plan: &PhysicalKernelRegistryPlan,
    execution_profiles: &PhysicalOperatorExecutionProfileMatrix,
) -> Vec<(String, String)> {
    let mut fields = vec![
        field("fallback_execution_allowed", "false"),
        field("mode", "kernel_registry_snapshot"),
        field("status", "report_only_missing_required_kernels"),
        field("registered_kernel_count", &snapshot.kernel_count()),
    ];
    append_physical_kernel_registry_fields(&mut fields, physical_plan);
    append_physical_operator_execution_level_fields(&mut fields, execution_profiles);
    append_metadata_kernel_admission_fields(&mut fields);
    append_projection_kernel_admission_fields(&mut fields);
    append_encoded_count_kernel_fields(&mut fields);
    append_encoded_filter_kernel_fields(&mut fields);
    append_plan_only_fields(&mut fields);
    fields
}

fn field<T: std::fmt::Display + ?Sized>(key: &str, value: &T) -> (String, String) {
    (key.to_string(), value.to_string())
}

fn append_static_fields(fields: &mut Vec<(String, String)>, pairs: &[(&str, &str)]) {
    fields.extend(pairs.iter().map(|&(key, value)| field(key, value)));
}

fn append_physical_kernel_registry_fields(
    fields: &mut Vec<(String, String)>,
    physical_plan: &PhysicalKernelRegistryPlan,
) {
    fields.extend([
        field(
            "physical_kernel_schema_version",
            &physical_plan.schema_version,
        ),
        field("physical_kernel_registry_id", &physical_plan.registry_id),
        field(
            "physical_kernel_required_slot_count",
            &physical_plan.required_slot_count(),
        ),
        field(
            "physical_kernel_present_slot_count",
            &physical_plan.present_slot_count(),
        ),
        field(
            "physical_kernel_missing_slot_count",
            &physical_plan.missing_slot_count(),
        ),
        field(
            "physical_kernel_reference_only_rejected_count",
            &physical_plan.reference_only_rejected_count(),
        ),
        field(
            "physical_kernel_runtime_execution_allowed",
            &physical_plan.runtime_execution_allowed(),
        ),
        field(
            "physical_kernel_fallback_execution_allowed",
            &physical_plan.fallback_execution_allowed(),
        ),
    ]);
}

fn append_physical_operator_execution_level_fields(
    fields: &mut Vec<(String, String)>,
    execution_profiles: &PhysicalOperatorExecutionProfileMatrix,
) {
    fields.extend([
        field(
            "physical_operator_native_execution_level_count",
            &execution_profiles.native_execution_level_count(),
        ),
        field(
            "physical_operator_metadata_only_level_count",
            &execution_profiles.allowed_level_count(PhysicalOperatorExecutionLevel::MetadataOnly),
        ),
        field(
            "physical_operator_encoded_native_level_count",
            &execution_profiles.allowed_level_count(PhysicalOperatorExecutionLevel::EncodedNative),
        ),
        field(
            "physical_operator_hybrid_native_level_count",
            &execution_profiles.allowed_level_count(PhysicalOperatorExecutionLevel::HybridNative),
        ),
        field(
            "physical_operator_native_decoded_level_count",
            &execution_profiles.allowed_level_count(PhysicalOperatorExecutionLevel::NativeDecoded),
        ),
    ]);
}

fn append_metadata_kernel_admission_fields(fields: &mut Vec<(String, String)>) {
    append_static_fields(
        fields,
        &[
            (
                "metadata_physical_kernel_schema_version",
                "shardloom.vortex_metadata_physical_kernel.v1",
            ),
            (
                "metadata_physical_kernel_supported_primitives",
                "count_all,count_where,filter_predicate",
            ),
            ("metadata_physical_kernel_contextual_only", "true"),
            (
                "metadata_physical_kernel_requires_correctness_evidence",
                "true",
            ),
            (
                "metadata_physical_kernel_requires_memory_safety_evidence",
                "true",
            ),
            (
                "metadata_physical_kernel_requires_benchmark_for_production",
                "true",
            ),
            ("metadata_physical_kernel_runtime_execution", "false"),
            (
                "metadata_physical_kernel_fallback_execution_allowed",
                "false",
            ),
            (
                "metadata_count_kernel_admission_schema_version",
                "shardloom.vortex_metadata_count_kernel_admission.v1",
            ),
            ("metadata_count_kernel_admission_contextual_only", "true"),
            (
                "metadata_count_kernel_admission_operator_kind",
                "count_aggregate",
            ),
            (
                "metadata_count_kernel_admission_required_kernel_kind",
                "metadata",
            ),
            (
                "metadata_count_kernel_admission_requires_metadata_kernel_evidence",
                "true",
            ),
            (
                "metadata_count_kernel_admission_requires_correctness_evidence",
                "true",
            ),
            (
                "metadata_count_kernel_admission_requires_memory_safety_evidence",
                "true",
            ),
            (
                "metadata_count_kernel_admission_requires_benchmark_for_production",
                "true",
            ),
            ("metadata_count_kernel_admission_runtime_execution", "false"),
            (
                "metadata_count_kernel_admission_fallback_execution_allowed",
                "false",
            ),
            (
                "metadata_filter_kernel_admission_schema_version",
                "shardloom.vortex_metadata_filter_kernel_admission.v1",
            ),
            ("metadata_filter_kernel_admission_contextual_only", "true"),
            ("metadata_filter_kernel_admission_operator_kind", "filter"),
            (
                "metadata_filter_kernel_admission_required_kernel_kind",
                "metadata",
            ),
            (
                "metadata_filter_kernel_admission_requires_metadata_kernel_evidence",
                "true",
            ),
            (
                "metadata_filter_kernel_admission_requires_correctness_evidence",
                "true",
            ),
            (
                "metadata_filter_kernel_admission_requires_memory_safety_evidence",
                "true",
            ),
            (
                "metadata_filter_kernel_admission_requires_benchmark_for_production",
                "true",
            ),
            (
                "metadata_filter_kernel_admission_runtime_execution",
                "false",
            ),
            (
                "metadata_filter_kernel_admission_fallback_execution_allowed",
                "false",
            ),
        ],
    );
}

fn append_projection_kernel_admission_fields(fields: &mut Vec<(String, String)>) {
    append_static_fields(
        fields,
        &[
            (
                "metadata_projection_kernel_admission_schema_version",
                "shardloom.vortex_metadata_projection_kernel_admission.v1",
            ),
            (
                "metadata_projection_kernel_admission_contextual_only",
                "true",
            ),
            (
                "metadata_projection_kernel_admission_operator_kind",
                "project",
            ),
            (
                "metadata_projection_kernel_admission_required_kernel_kind",
                "metadata",
            ),
            (
                "metadata_projection_kernel_admission_requires_projection_readiness",
                "true",
            ),
            (
                "metadata_projection_kernel_admission_requires_correctness_evidence",
                "true",
            ),
            (
                "metadata_projection_kernel_admission_requires_memory_safety_evidence",
                "true",
            ),
            (
                "metadata_projection_kernel_admission_requires_benchmark_for_production",
                "true",
            ),
            (
                "metadata_projection_kernel_admission_runtime_execution",
                "false",
            ),
            (
                "metadata_projection_kernel_admission_fallback_execution_allowed",
                "false",
            ),
            (
                "encoded_projection_kernel_admission_schema_version",
                "shardloom.vortex_encoded_projection_kernel_admission.v1",
            ),
            (
                "encoded_projection_kernel_admission_contextual_only",
                "true",
            ),
            (
                "encoded_projection_kernel_admission_operator_kind",
                "project",
            ),
            (
                "encoded_projection_kernel_admission_required_kernel_kind",
                "encoded",
            ),
            (
                "encoded_projection_kernel_admission_requires_projection_readiness",
                "true",
            ),
            (
                "encoded_projection_kernel_admission_requires_encoded_column_path",
                "true",
            ),
            (
                "encoded_projection_kernel_admission_requires_correctness_evidence",
                "true",
            ),
            (
                "encoded_projection_kernel_admission_requires_memory_safety_evidence",
                "true",
            ),
            (
                "encoded_projection_kernel_admission_requires_benchmark_for_production",
                "true",
            ),
            (
                "encoded_projection_kernel_admission_runtime_execution",
                "false",
            ),
            (
                "encoded_projection_kernel_admission_fallback_execution_allowed",
                "false",
            ),
        ],
    );
}

fn append_encoded_count_kernel_fields(fields: &mut Vec<(String, String)>) {
    append_static_fields(
        fields,
        &[
            (
                "encoded_count_physical_kernel_schema_version",
                "shardloom.vortex_encoded_count_physical_kernel.v1",
            ),
            (
                "encoded_count_physical_kernel_supported_primitive",
                "count_all",
            ),
            (
                "encoded_count_physical_kernel_operator_kind",
                "count_aggregate",
            ),
            ("encoded_count_physical_kernel_kernel_kind", "encoded"),
            (
                "encoded_count_physical_kernel_execution_level",
                "encoded_native",
            ),
            ("encoded_count_physical_kernel_contextual_only", "true"),
            (
                "encoded_count_physical_kernel_requires_execution_certificate",
                "true",
            ),
            ("encoded_count_physical_kernel_runtime_execution", "false"),
            (
                "encoded_count_physical_kernel_fallback_execution_allowed",
                "false",
            ),
            (
                "encoded_count_kernel_admission_schema_version",
                "shardloom.vortex_encoded_count_kernel_admission.v1",
            ),
            ("encoded_count_kernel_admission_contextual_only", "true"),
            (
                "encoded_count_kernel_admission_operator_kind",
                "count_aggregate",
            ),
            (
                "encoded_count_kernel_admission_required_kernel_kind",
                "encoded",
            ),
            (
                "encoded_count_kernel_admission_requires_physical_kernel_evidence",
                "true",
            ),
            (
                "encoded_count_kernel_admission_requires_correctness_evidence",
                "true",
            ),
            (
                "encoded_count_kernel_admission_requires_memory_safety_evidence",
                "true",
            ),
            (
                "encoded_count_kernel_admission_requires_benchmark_for_production",
                "true",
            ),
            ("encoded_count_kernel_admission_runtime_execution", "false"),
            (
                "encoded_count_kernel_admission_fallback_execution_allowed",
                "false",
            ),
        ],
    );
}

fn append_encoded_filter_kernel_fields(fields: &mut Vec<(String, String)>) {
    append_encoded_predicate_evaluation_fields(fields);
    append_selection_vector_filter_kernel_fields(fields);
    append_selection_vector_filter_kernel_admission_fields(fields);
}

fn append_encoded_predicate_evaluation_fields(fields: &mut Vec<(String, String)>) {
    append_static_fields(
        fields,
        &[
            (
                "encoded_predicate_evaluation_schema_version",
                "shardloom.vortex_encoded_predicate_evaluation.v1",
            ),
            (
                "encoded_predicate_evaluation_id",
                "vortex.query-primitive.filter_predicate.encoded-predicate-evaluation",
            ),
            ("encoded_predicate_evaluation_operator_kind", "filter"),
            ("encoded_predicate_evaluation_kernel_kind", "encoded"),
            (
                "encoded_predicate_evaluation_execution_level",
                "encoded_native",
            ),
            ("encoded_predicate_evaluation_contextual_only", "true"),
            (
                "encoded_predicate_evaluation_emits_selection_vectors",
                "true",
            ),
            (
                "encoded_predicate_evaluation_supports_metadata_proven_all",
                "true",
            ),
            (
                "encoded_predicate_evaluation_supports_metadata_proven_none",
                "true",
            ),
            (
                "encoded_predicate_evaluation_defers_inconclusive_to_encoded_values",
                "true",
            ),
            ("encoded_predicate_evaluation_discovery_reads_data", "false"),
            ("encoded_predicate_evaluation_runtime_execution", "false"),
            (
                "encoded_predicate_evaluation_fallback_execution_allowed",
                "false",
            ),
        ],
    );
}

fn append_selection_vector_filter_kernel_fields(fields: &mut Vec<(String, String)>) {
    append_static_fields(
        fields,
        &[
            (
                "selection_vector_filter_kernel_schema_version",
                "shardloom.vortex_selection_vector_filter_kernel.v1",
            ),
            (
                "selection_vector_filter_kernel_id",
                "vortex.query-primitive.filter_predicate.selection-vector-filter-kernel",
            ),
            ("selection_vector_filter_kernel_operator_kind", "filter"),
            ("selection_vector_filter_kernel_kernel_kind", "encoded"),
            (
                "selection_vector_filter_kernel_execution_level",
                "encoded_native",
            ),
            ("selection_vector_filter_kernel_contextual_only", "true"),
            (
                "selection_vector_filter_kernel_requires_encoded_predicate_evaluation",
                "true",
            ),
            (
                "selection_vector_filter_kernel_requires_selection_vectors",
                "true",
            ),
            (
                "selection_vector_filter_kernel_requires_correctness_evidence",
                "true",
            ),
            (
                "selection_vector_filter_kernel_requires_memory_safety_evidence",
                "true",
            ),
            (
                "selection_vector_filter_kernel_requires_benchmark_for_production",
                "true",
            ),
            (
                "selection_vector_filter_kernel_discovery_reads_data",
                "false",
            ),
            ("selection_vector_filter_kernel_runtime_execution", "false"),
            (
                "selection_vector_filter_kernel_fallback_execution_allowed",
                "false",
            ),
        ],
    );
}

fn append_selection_vector_filter_kernel_admission_fields(fields: &mut Vec<(String, String)>) {
    append_static_fields(
        fields,
        &[
            (
                "selection_vector_filter_kernel_admission_schema_version",
                "shardloom.vortex_selection_vector_filter_kernel_admission.v1",
            ),
            (
                "selection_vector_filter_kernel_admission_contextual_only",
                "true",
            ),
            (
                "selection_vector_filter_kernel_admission_operator_kind",
                "filter",
            ),
            (
                "selection_vector_filter_kernel_admission_required_kernel_kind",
                "encoded",
            ),
            (
                "selection_vector_filter_kernel_admission_requires_filter_kernel_evidence",
                "true",
            ),
            (
                "selection_vector_filter_kernel_admission_requires_correctness_evidence",
                "true",
            ),
            (
                "selection_vector_filter_kernel_admission_requires_memory_safety_evidence",
                "true",
            ),
            (
                "selection_vector_filter_kernel_admission_requires_benchmark_for_production",
                "true",
            ),
            (
                "selection_vector_filter_kernel_admission_runtime_execution",
                "false",
            ),
            (
                "selection_vector_filter_kernel_admission_fallback_execution_allowed",
                "false",
            ),
        ],
    );
}

fn append_plan_only_fields(fields: &mut Vec<(String, String)>) {
    append_static_fields(
        fields,
        &[
            ("write_io", "false"),
            ("execution", "not_performed"),
            ("plan_only", "true"),
        ],
    );
}

pub(crate) fn handle_optimizer_plan(format: OutputFormat) -> ExitCode {
    let report = OptimizerPlanSkeleton::not_implemented(
        OptimizerPhase::VortexPhysical,
        "optimizer_execution",
        "ShardLoom optimizer planning skeleton exists, but real optimizer execution is not implemented yet.",
    );
    emit(
        "optimizer-plan",
        format,
        CommandStatus::Unsupported,
        "optimizer plan skeleton".to_string(),
        report.to_human_text(),
        report.diagnostics.clone(),
        vec![
            (
                "fallback_execution_allowed".to_string(),
                "false".to_string(),
            ),
            ("mode".to_string(), "optimizer_plan".to_string()),
            ("status".to_string(), "not_implemented".to_string()),
            ("write_io".to_string(), "false".to_string()),
            ("execution".to_string(), "not_performed".to_string()),
            ("plan_only".to_string(), "true".to_string()),
            ("optimizer_phase".to_string(), "vortex_physical".to_string()),
        ],
    );
    ExitCode::from(1)
}

pub(crate) fn handle_optimizer_adaptive_memory_plan(format: OutputFormat) -> ExitCode {
    let command = "optimizer-adaptive-memory-plan";
    let report = plan_adaptive_optimizer_memory();
    emit(
        command,
        format,
        if report.has_errors() {
            CommandStatus::Unsupported
        } else {
            CommandStatus::Success
        },
        "adaptive optimizer memory plan".to_string(),
        report.to_human_text(),
        report.diagnostics.clone(),
        adaptive_optimizer_memory_fields(&report),
    );
    if report.has_errors() {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

pub(crate) fn handle_cpu_specialization_plan(format: OutputFormat) -> ExitCode {
    let command = "cpu-specialization-plan";
    let report = plan_cpu_operator_specialization();
    emit(
        command,
        format,
        if report.has_errors() {
            CommandStatus::Unsupported
        } else {
            CommandStatus::Success
        },
        "cpu operator specialization plan".to_string(),
        report.to_human_text(),
        report.diagnostics.clone(),
        cpu_operator_specialization_fields(&report),
    );
    if report.has_errors() {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

fn adaptive_optimizer_memory_fields(
    report: &AdaptiveOptimizerMemoryReport,
) -> Vec<(String, String)> {
    let mut fields = vec![];
    append_adaptive_optimizer_memory_identity_fields(&mut fields, report);
    append_adaptive_optimizer_memory_gate_fields(&mut fields, report);
    append_adaptive_optimizer_memory_side_effect_fields(&mut fields, report);
    fields
}

fn append_adaptive_optimizer_memory_identity_fields(
    fields: &mut Vec<(String, String)>,
    report: &AdaptiveOptimizerMemoryReport,
) {
    push_field(fields, "mode", "optimizer_adaptive_memory_plan");
    push_field(fields, "execution", "not_performed");
    push_field(fields, "plan_only", "true");
    push_field(fields, "gar_id", "GAR-0016-A");
    push_field(fields, "support_status", report.support_status());
    push_field(fields, "claim_gate_status", report.claim_gate_status());
    push_field(fields, "schema_version", report.schema_version);
    push_field(fields, "report_id", &report.report_id);
    push_field(fields, "adaptive_optimizer_status", report.status.as_str());
    push_field(fields, "optimizer_phase", report.optimizer_phase.as_str());
    push_field(
        fields,
        "adaptive_runtime_gate_surface_order",
        report
            .adaptive_runtime_gate_surface_order()
            .join(",")
            .as_str(),
    );
    push_field(
        fields,
        "runtime_gate_prerequisite_order",
        report.runtime_gate_prerequisite_order().join(",").as_str(),
    );
    push_count_field(
        fields,
        "runtime_gate_prerequisite_count",
        report.runtime_gate_prerequisite_count(),
    );
    push_count_field(fields, "rule_decision_count", report.rule_decision_count());
    push_count_field(fields, "deferred_rule_count", report.deferred_rule_count());
    push_count_field(
        fields,
        "runtime_filter_count",
        report.runtime_filter_count(),
    );
    push_count_field(
        fields,
        "conservative_runtime_filter_count",
        report.conservative_runtime_filter_count(),
    );
    push_count_field(
        fields,
        "adaptive_decision_count",
        report.adaptive_decision_count(),
    );
    push_count_field(fields, "skew_signal_count", report.skew_signal_count());
    push_field(
        fields,
        "dynamic_pruning_decision",
        report.dynamic_pruning_decision.summary().as_str(),
    );
}

fn append_adaptive_optimizer_memory_gate_fields(
    fields: &mut Vec<(String, String)>,
    report: &AdaptiveOptimizerMemoryReport,
) {
    push_bool_field(
        fields,
        "conservative_runtime_filter_required",
        report.conservative_runtime_filter_required,
    );
    push_bool_field(
        fields,
        "dynamic_pruning_requires_proof",
        report.dynamic_pruning_requires_proof,
    );
    push_bool_field(
        fields,
        "memory_budget_required",
        report.memory_budget_required,
    );
    push_bool_field(
        fields,
        "bounded_memory_required",
        report.bounded_memory_required,
    );
    push_bool_field(
        fields,
        "spill_policy_required",
        report.spill_policy_required,
    );
    push_bool_field(
        fields,
        "deterministic_oom_boundary",
        report.deterministic_oom_boundary,
    );
    push_bool_field(
        fields,
        "sink_requirement_boundary_required",
        report.sink_requirement_boundary_required,
    );
    push_bool_field(
        fields,
        "runtime_fact_required_before_adaptation",
        report.runtime_fact_required_before_adaptation,
    );
    push_bool_field(
        fields,
        "adaptive_parallelism_required",
        report.adaptive_parallelism_required,
    );
    push_bool_field(
        fields,
        "compaction_write_boundary_required",
        report.compaction_write_boundary_required,
    );
    push_field(
        fields,
        "runtime_filter_execution_status",
        "report_only_blocked",
    );
    push_field(
        fields,
        "skew_handling_execution_status",
        "report_only_blocked",
    );
    push_field(
        fields,
        "adaptive_parallelism_execution_status",
        "report_only_blocked",
    );
    push_field(
        fields,
        "compaction_write_execution_status",
        "report_only_blocked",
    );
}

fn append_adaptive_optimizer_memory_side_effect_fields(
    fields: &mut Vec<(String, String)>,
    report: &AdaptiveOptimizerMemoryReport,
) {
    push_bool_field(fields, "optimizer_execution", report.optimizer_execution);
    push_bool_field(
        fields,
        "runtime_adaptation_applied",
        report.runtime_adaptation_applied,
    );
    push_bool_field(fields, "runtime_filter_built", report.runtime_filter_built);
    push_bool_field(
        fields,
        "runtime_filter_applied",
        report.runtime_filter_applied,
    );
    push_bool_field(
        fields,
        "adaptive_parallelism_applied",
        report.adaptive_parallelism_applied,
    );
    push_bool_field(
        fields,
        "compaction_write_allowed",
        report.compaction_write_allowed,
    );
    push_bool_field(
        fields,
        "compaction_execution_allowed",
        report.compaction_execution_allowed,
    );
    push_bool_field(fields, "plan_rewritten", report.plan_rewritten);
    push_bool_field(fields, "data_read", report.data_read);
    push_bool_field(fields, "data_decoded", report.data_decoded);
    push_bool_field(fields, "data_materialized", report.data_materialized);
    push_bool_field(fields, "row_read", report.row_read);
    push_bool_field(fields, "arrow_converted", report.arrow_converted);
    push_bool_field(fields, "object_store_io", report.object_store_io);
    push_bool_field(fields, "write_io", report.write_io);
    push_bool_field(fields, "spill_io_performed", report.spill_io_performed);
    push_bool_field(
        fields,
        "external_engine_execution",
        report.external_engine_execution,
    );
    push_bool_field(
        fields,
        "fallback_execution_allowed",
        report.fallback_execution_allowed,
    );
    push_bool_field(fields, "fallback_attempted", report.fallback_attempted);
    push_bool_field(
        fields,
        "production_claim_allowed",
        report.production_claim_allowed,
    );
    push_bool_field(fields, "side_effect_free", report.is_side_effect_free());
    push_count_field(fields, "diagnostic_count", report.diagnostics.len());
}

fn cpu_operator_specialization_fields(
    report: &CpuOperatorSpecializationReport,
) -> Vec<(String, String)> {
    let mut fields = vec![];
    append_cpu_specialization_identity_fields(&mut fields, report);
    append_cpu_specialization_evidence_fields(&mut fields, report);
    append_cpu_specialization_accelerator_fields(&mut fields, report);
    append_cpu_specialization_side_effect_fields(&mut fields, report);
    fields
}

fn append_cpu_specialization_identity_fields(
    fields: &mut Vec<(String, String)>,
    report: &CpuOperatorSpecializationReport,
) {
    push_field(fields, "mode", "cpu_operator_specialization_plan");
    push_field(fields, "execution", "not_performed");
    push_field(fields, "plan_only", "true");
    push_field(fields, "schema_version", report.schema_version);
    push_field(fields, "report_id", &report.report_id);
    push_field(fields, "cpu_specialization_status", report.status.as_str());
    push_count_field(fields, "entry_count", report.entry_count());
    push_count_field(
        fields,
        "specialization_candidate_count",
        report.specialization_candidate_count(),
    );
    push_count_field(
        fields,
        "simd_candidate_count",
        report.simd_candidate_count(),
    );
    push_count_field(
        fields,
        "cache_aware_candidate_count",
        report.cache_aware_candidate_count(),
    );
    push_count_field(
        fields,
        "encoded_layout_aware_candidate_count",
        report.encoded_layout_aware_candidate_count(),
    );
    push_field(fields, "operator_order", &report.operator_order());
    push_field(fields, "kernel_kind_order", &report.kernel_kind_order());
}

fn append_cpu_specialization_evidence_fields(
    fields: &mut Vec<(String, String)>,
    report: &CpuOperatorSpecializationReport,
) {
    push_bool_field(
        fields,
        "correctness_evidence_required",
        report.correctness_evidence_required,
    );
    push_bool_field(
        fields,
        "benchmark_evidence_required",
        report.benchmark_evidence_required,
    );
    push_bool_field(
        fields,
        "certified_primitive_kernel_required",
        report.certified_primitive_kernel_required,
    );
    push_bool_field(
        fields,
        "benchmark_workload_evidence_required",
        report.benchmark_workload_evidence_required,
    );
    push_bool_field(
        fields,
        "correctness_gate_open",
        report.correctness_gate_open,
    );
    push_bool_field(fields, "benchmark_gate_open", report.benchmark_gate_open);
    push_bool_field(
        fields,
        "specialization_admission_open",
        report.specialization_admission_open(),
    );
    push_bool_field(
        fields,
        "dispatch_classes_blocked",
        report.dispatch_classes_blocked(),
    );
    push_bool_field(
        fields,
        "cpu_feature_guard_required",
        report.cpu_feature_guard_required,
    );
    push_bool_field(
        fields,
        "portable_native_baseline_required",
        report.portable_native_baseline_required,
    );
    push_bool_field(
        fields,
        "deterministic_dispatch_required",
        report.deterministic_dispatch_required,
    );
    push_field(
        fields,
        "vectorized_kernel_admission_operator",
        report.vectorized_kernel_admission_operator.as_str(),
    );
    push_field(
        fields,
        "vectorized_kernel_admission_kernel",
        report.vectorized_kernel_admission_kernel.as_str(),
    );
    push_field(
        fields,
        "vectorized_kernel_admission_status",
        report.vectorized_kernel_admission_status.as_str(),
    );
    push_field(
        fields,
        "vectorized_kernel_admission_reason",
        &report.vectorized_kernel_admission_reason,
    );
    push_bool_field(
        fields,
        "vectorized_kernel_admission_allowed",
        report.vectorized_kernel_admission_allowed,
    );
}

fn append_cpu_specialization_accelerator_fields(
    fields: &mut Vec<(String, String)>,
    report: &CpuOperatorSpecializationReport,
) {
    push_bool_field(fields, "host_cpu_probe", report.host_cpu_probe);
    push_bool_field(
        fields,
        "host_cpu_probe_supported",
        report.host_cpu_feature_probe.probe_supported,
    );
    push_bool_field(
        fields,
        "host_cpu_probe_effect_free",
        report.host_cpu_feature_probe.probe_effect_free,
    );
    push_field(
        fields,
        "host_cpu_arch",
        &report.host_cpu_feature_probe.architecture,
    );
    push_field(
        fields,
        "host_cpu_detected_features",
        &report.host_cpu_feature_probe.detected_feature_labels(),
    );
    push_bool_field(
        fields,
        "host_cpu_simd_feature_detected",
        report.host_cpu_feature_probe.simd_feature_detected,
    );
    push_bool_field(
        fields,
        "runtime_dispatch_implemented",
        report.runtime_dispatch_implemented,
    );
    push_bool_field(
        fields,
        "simd_dispatch_allowed",
        report.simd_dispatch_allowed,
    );
    push_bool_field(
        fields,
        "cache_aware_dispatch_allowed",
        report.cache_aware_dispatch_allowed,
    );
    push_bool_field(
        fields,
        "encoded_layout_dispatch_allowed",
        report.encoded_layout_dispatch_allowed,
    );
    push_bool_field(
        fields,
        "specialization_runtime_allowed",
        report.specialization_runtime_allowed,
    );
    push_bool_field(fields, "unsafe_code_required", report.unsafe_code_required);
    push_bool_field(fields, "gpu_required", report.gpu_required);
    push_bool_field(fields, "fpga_required", report.fpga_required);
}

fn append_cpu_specialization_side_effect_fields(
    fields: &mut Vec<(String, String)>,
    report: &CpuOperatorSpecializationReport,
) {
    push_bool_field(fields, "runtime_execution", report.runtime_execution);
    push_bool_field(fields, "data_read", report.data_read);
    push_bool_field(fields, "data_decoded", report.data_decoded);
    push_bool_field(fields, "data_materialized", report.data_materialized);
    push_bool_field(fields, "row_read", report.row_read);
    push_bool_field(fields, "arrow_converted", report.arrow_converted);
    push_bool_field(fields, "object_store_io", report.object_store_io);
    push_bool_field(fields, "write_io", report.write_io);
    push_bool_field(fields, "spill_io_performed", report.spill_io_performed);
    push_bool_field(
        fields,
        "external_engine_execution",
        report.external_engine_execution,
    );
    push_bool_field(
        fields,
        "fallback_execution_allowed",
        report.fallback_execution_allowed,
    );
    push_bool_field(fields, "fallback_attempted", report.fallback_attempted);
    push_bool_field(
        fields,
        "production_claim_allowed",
        report.production_claim_allowed,
    );
    push_bool_field(fields, "side_effect_free", report.is_side_effect_free());
    push_count_field(fields, "diagnostic_count", report.diagnostics.len());
}

fn push_field(fields: &mut Vec<(String, String)>, key: &str, value: &str) {
    fields.push((key.to_string(), value.to_string()));
}

fn push_count_field(fields: &mut Vec<(String, String)>, key: &str, value: usize) {
    fields.push((key.to_string(), value.to_string()));
}

fn push_bool_field(fields: &mut Vec<(String, String)>, key: &str, value: bool) {
    fields.push((key.to_string(), value.to_string()));
}
