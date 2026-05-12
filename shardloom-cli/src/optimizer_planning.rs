//! Optimizer, kernel registry, and CPU specialization CLI planning handlers.
//!
//! These handlers remain report-only planning surfaces. They do not execute
//! optimizer work, physical kernels, CPU-specialized kernels, external engines,
//! writes, materialization, or fallback execution.

use std::process::ExitCode;

use shardloom_core::{
    CommandStatus, KernelRegistrySnapshot, OutputFormat, PhysicalKernelRegistryPlan,
    PhysicalOperatorExecutionLevel, PhysicalOperatorExecutionProfileMatrix,
    plan_cpu_operator_specialization,
};
use shardloom_plan::{OptimizerPhase, OptimizerPlanSkeleton, plan_adaptive_optimizer_memory};

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
        crate::adaptive_optimizer_memory_fields(&report),
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
        crate::cpu_operator_specialization_fields(&report),
    );
    if report.has_errors() {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}
