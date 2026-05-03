//! Command-line entry point for `ShardLoom`.
//!
//! The `CLI` remains intentionally small in setup phase and exposes basic
//! introspection commands for workspace bring-up.

use std::process::ExitCode;

use shardloom_core::{
    CatalogKind, CatalogRef, ChangeSet, CommandStatus, CorrectnessValidationPlan, DatasetManifest,
    DatasetRef, DatasetUri, ExtensionId, ExtensionInspectionReport, ExtensionLicenseKind,
    ExtensionManifest, ExtensionProvenance, ExtensionRegistrySnapshot, ExtensionVersion,
    IncrementalPlanSkeleton, InputAdapterRegistrySnapshot, KernelRegistrySnapshot, ManifestId,
    ObservabilityPlan, OutputEnvelope, OutputFormat, OutputTarget, RedactionPolicy, ReleasePlan,
    RuntimeObservabilityReport, SchemaDefinition, SchemaId, SchemaVersion, SecurityPlan,
    ShardLoomError, SnapshotId, SnapshotRef, TableCompatibilityPlan, TableFormatKind,
    TranslationPlan, UdfRuntimeKind, WriteIntent,
};
use shardloom_exec::{
    AdaptiveSizer, AdaptiveSizingPolicy, AttemptId, ByteSize, CancellationReason,
    CancellationRequest, CancellationScope, MemoryBudget, MemoryOwner, MemoryPoolPlan,
    OomSafetyPlan, OperatorMemoryClass, ParallelismLimit, ParallelismPlan, RecoveryPlan, RetryPlan,
    RuntimePlanSkeleton, SizeEstimate, SizingInput, SizingPlan, SpillPlan, SpillPolicy,
    StreamingPlanSkeleton, TaskAttemptRecord,
};
use shardloom_plan::{
    EstimateReport, ExplainReport, NativePlanDocument, OptimizerPhase, OptimizerPlanSkeleton,
    PlanExportRequest, PlanId, PlanImportRequest, PlanInteropFormat, ScanPlanSkeleton, ScanRequest,
    plan_universal_input_source,
};
use shardloom_vortex::{
    VortexAdapterCapabilityReport, VortexAdapterReadiness, VortexDTypeMappingReport,
    VortexEncodingLayoutMappingReport, VortexFileRef, VortexMetadataOpenRequest,
    VortexMetadataProbeReport, VortexReadPlan, VortexStatisticsMappingReport, VortexWriteOptions,
    VortexWritePlan, build_vortex_runtime_task_graph, metadata_planning_is_side_effect_free,
    metadata_pruning_is_side_effect_free, metadata_summary_is_plan_only, open_vortex_metadata_only,
    plan_from_vortex_metadata_summary, plan_native_vortex_universal_input,
    plan_vortex_memory_safety, plan_vortex_metadata_pruning, plan_vortex_read_from_universal_input,
    probe_vortex_metadata_only, size_vortex_runtime_task_graph, summarize_vortex_metadata_probe,
    vortex_file_io_feature_enabled,
};

fn main() -> ExitCode {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    run(args)
}

const CLI_COMMAND_NAME: &str = "shardloom";

fn cli_command_name() -> &'static str {
    CLI_COMMAND_NAME
}

fn cli_usage_line() -> String {
    format!(
        "usage: {} <status|release-plan|package-plan|api-compat-plan|capabilities|security-plan|agent-safety-plan|redaction-plan|kernel-registry|doctor|manifest-plan|incremental-plan|write-intent|scan-plan|runtime-plan|task-plan|sizing-plan|translation-plan|vortex-plan|vortex-output-plan|vortex-readiness|vortex-api-inventory|vortex-dtype-mapping|vortex-encoding-layout-mapping|vortex-statistics-mapping|vortex-metadata-probe|vortex-file-metadata-open|vortex-metadata-summary|vortex-metadata-plan|vortex-pruning-plan|optimizer-plan|explain|estimate|benchmark-plan|correctness-plan|recovery-plan|cancellation-plan|retry-plan|observability-plan|runtime-report|profile-plan|plan-ir|plan-import|plan-export|table-compat-plan|schema-plan|input-adapters|input-plan|vortex-input-plan|vortex-read-plan|vortex-task-graph|vortex-adaptive-sizing|vortex-memory-plan> [--format text|json]",
        cli_command_name()
    )
}

fn parse_output_format(args: Vec<String>) -> Result<(Vec<String>, OutputFormat), String> {
    let mut filtered = Vec::with_capacity(args.len());
    let mut format = OutputFormat::Text;
    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        if arg == "--format" {
            let Some(value) = iter.next() else {
                return Err("missing value for --format; expected text or json".to_string());
            };
            format = OutputFormat::parse(&value).map_err(|e| e.to_string())?;
        } else {
            filtered.push(arg);
        }
    }
    Ok((filtered, format))
}

fn detect_requested_output_format(args: &[String]) -> OutputFormat {
    let mut format = OutputFormat::Text;
    let mut iter = args.iter();
    while let Some(arg) = iter.next() {
        if arg == "--format" {
            if let Some(value) = iter.next() {
                if let Ok(parsed) = OutputFormat::parse(value) {
                    format = parsed;
                }
            } else {
                break;
            }
        }
    }
    format
}

fn emit(
    command: &str,
    format: OutputFormat,
    status: CommandStatus,
    summary: String,
    text: String,
    diagnostics: Vec<shardloom_core::Diagnostic>,
    fields: Vec<(String, String)>,
) {
    let mut envelope = OutputEnvelope::new(command, status, summary, text);
    for diagnostic in diagnostics {
        envelope.add_diagnostic(diagnostic);
    }
    for (key, value) in fields {
        envelope = envelope.with_field(key, value);
    }
    println!("{}", envelope.render(format));
}

fn emit_error(
    command: &str,
    format: OutputFormat,
    summary: &str,
    error: &ShardLoomError,
) -> ExitCode {
    let envelope = OutputEnvelope::from_error(command, summary, error);
    match format {
        OutputFormat::Text => eprintln!("{}", envelope.to_text()),
        OutputFormat::Json => println!("{}", envelope.to_json()),
    }
    ExitCode::from(2)
}

fn parse_plan_interop_format(value: &str) -> PlanInteropFormat {
    match value {
        "native" => PlanInteropFormat::ShardLoomNative,
        "agent" => PlanInteropFormat::AgentPlanSpec,
        "substrait-like" => PlanInteropFormat::SubstraitLike,
        "json-like" => PlanInteropFormat::JsonLike,
        _ => PlanInteropFormat::Unknown,
    }
}

#[allow(clippy::too_many_lines)]
fn run(args: Vec<String>) -> ExitCode {
    let requested_format = detect_requested_output_format(&args);
    let (args, format) = match parse_output_format(args) {
        Ok(parsed) => parsed,
        Err(message) => {
            return emit_error(
                "cli",
                requested_format,
                "cli argument parsing failed",
                &ShardLoomError::InvalidOperation(message),
            );
        }
    };
    let mut args = args.into_iter();

    match args.next().as_deref() {
        Some("status") => {
            let status = shardloom_exec::status();
            emit(
                "status",
                format,
                CommandStatus::Success,
                "engine status".to_string(),
                format!("{}\nfallback execution: disabled", status.summary),
                vec![],
                vec![(
                    "fallback_execution_allowed".to_string(),
                    "false".to_string(),
                )],
            );
            ExitCode::SUCCESS
        }
        Some("release-plan") => {
            let plan = ReleasePlan::default_foundation_plan();
            emit(
                "release-plan",
                format,
                CommandStatus::Success,
                "release plan skeleton".to_string(),
                plan.to_human_text(),
                plan.diagnostics.clone(),
                vec![
                    (
                        "fallback_execution_allowed".to_string(),
                        "false".to_string(),
                    ),
                    ("mode".to_string(), "release_plan".to_string()),
                    ("publish_allowed".to_string(), "false".to_string()),
                    ("published".to_string(), "false".to_string()),
                    ("write_io".to_string(), "false".to_string()),
                    ("execution".to_string(), "not_performed".to_string()),
                    ("plan_only".to_string(), "true".to_string()),
                    ("external_publish".to_string(), "not_performed".to_string()),
                ],
            );
            ExitCode::SUCCESS
        }
        Some("package-plan") => {
            let plan = ReleasePlan::default_foundation_plan();
            emit(
                "package-plan",
                format,
                CommandStatus::Success,
                "package plan skeleton".to_string(),
                plan.to_human_text(),
                plan.diagnostics.clone(),
                vec![
                    (
                        "fallback_execution_allowed".to_string(),
                        "false".to_string(),
                    ),
                    ("mode".to_string(), "package_plan".to_string()),
                    ("publish_allowed".to_string(), "false".to_string()),
                    ("published".to_string(), "false".to_string()),
                    ("write_io".to_string(), "false".to_string()),
                    ("execution".to_string(), "not_performed".to_string()),
                    ("plan_only".to_string(), "true".to_string()),
                    ("external_publish".to_string(), "not_performed".to_string()),
                ],
            );
            ExitCode::SUCCESS
        }
        Some("api-compat-plan") => {
            let plan = ReleasePlan::default_foundation_plan();
            emit(
                "api-compat-plan",
                format,
                CommandStatus::Success,
                "api compatibility plan skeleton".to_string(),
                plan.to_human_text(),
                plan.diagnostics.clone(),
                vec![
                    (
                        "fallback_execution_allowed".to_string(),
                        "false".to_string(),
                    ),
                    ("mode".to_string(), "api_compat_plan".to_string()),
                    ("publish_allowed".to_string(), "false".to_string()),
                    ("published".to_string(), "false".to_string()),
                    ("write_io".to_string(), "false".to_string()),
                    ("execution".to_string(), "not_performed".to_string()),
                    ("plan_only".to_string(), "true".to_string()),
                    ("external_publish".to_string(), "not_performed".to_string()),
                ],
            );
            ExitCode::SUCCESS
        }

        Some("input-adapters") => {
            let snapshot = InputAdapterRegistrySnapshot::foundation();
            emit(
                "input-adapters",
                format,
                CommandStatus::Success,
                "input adapters snapshot".to_string(),
                snapshot.to_human_text(),
                snapshot.diagnostics.clone(),
                vec![
                    (
                        "fallback_execution_allowed".to_string(),
                        "false".to_string(),
                    ),
                    ("mode".to_string(), "input_adapters".to_string()),
                    ("write_io".to_string(), "false".to_string()),
                    ("execution".to_string(), "not_performed".to_string()),
                    ("plan_only".to_string(), "true".to_string()),
                    ("external_effects_executed".to_string(), "false".to_string()),
                ],
            );
            ExitCode::SUCCESS
        }
        Some("input-plan") => {
            let Some(dataset_uri) = args.next() else {
                eprintln!("usage: shardloom input-plan <dataset_uri>");
                return ExitCode::from(2);
            };
            let uri = match DatasetUri::new(dataset_uri) {
                Ok(v) => v,
                Err(error) => return emit_error("input-plan", format, "input plan failed", &error),
            };
            let source = match shardloom_core::UniversalInputSource::from_dataset_uri(uri) {
                Ok(v) => v,
                Err(error) => return emit_error("input-plan", format, "input plan failed", &error),
            };
            let report = match plan_universal_input_source(source) {
                Ok(v) => v,
                Err(error) => return emit_error("input-plan", format, "input plan failed", &error),
            };
            let command_status = if report.has_errors() {
                CommandStatus::Unsupported
            } else {
                CommandStatus::Success
            };
            emit(
                "input-plan",
                format,
                command_status,
                "input plan report".to_string(),
                report.to_human_text(),
                report.diagnostics.clone(),
                vec![
                    (
                        "fallback_execution_allowed".to_string(),
                        "false".to_string(),
                    ),
                    ("mode".to_string(), "input_plan".to_string()),
                    ("data_read".to_string(), "false".to_string()),
                    ("data_materialized".to_string(), "false".to_string()),
                    ("object_store_io".to_string(), "false".to_string()),
                    ("external_effects_executed".to_string(), "false".to_string()),
                    ("write_io".to_string(), "false".to_string()),
                    ("execution".to_string(), "not_performed".to_string()),
                    ("plan_only".to_string(), "true".to_string()),
                ],
            );
            if report.has_errors() {
                ExitCode::from(1)
            } else {
                ExitCode::SUCCESS
            }
        }

        Some("vortex-input-plan") => {
            let Some(dataset_uri) = args.next() else {
                eprintln!("usage: shardloom vortex-input-plan <dataset_uri>");
                return ExitCode::from(2);
            };
            let uri = match DatasetUri::new(dataset_uri) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        "vortex-input-plan",
                        format,
                        "vortex input plan failed",
                        &error,
                    );
                }
            };
            let source = match shardloom_core::UniversalInputSource::from_dataset_uri(uri) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        "vortex-input-plan",
                        format,
                        "vortex input plan failed",
                        &error,
                    );
                }
            };
            let report = match plan_native_vortex_universal_input(source) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        "vortex-input-plan",
                        format,
                        "vortex input plan failed",
                        &error,
                    );
                }
            };
            let command_status = if report.has_errors() {
                CommandStatus::Unsupported
            } else {
                CommandStatus::Success
            };
            emit(
                "vortex-input-plan",
                format,
                command_status,
                "vortex universal input plan report".to_string(),
                report.to_human_text(),
                report.diagnostics.clone(),
                vec![
                    (
                        "fallback_execution_allowed".to_string(),
                        "false".to_string(),
                    ),
                    ("mode".to_string(), "vortex_input_plan".to_string()),
                    (
                        "native_vortex_input".to_string(),
                        report.source.is_native_vortex().to_string(),
                    ),
                    ("metadata_only".to_string(), "true".to_string()),
                    ("plan_only".to_string(), "true".to_string()),
                    ("data_read".to_string(), "false".to_string()),
                    ("data_materialized".to_string(), "false".to_string()),
                    ("object_store_io".to_string(), "false".to_string()),
                    ("write_io".to_string(), "false".to_string()),
                    ("external_effects_executed".to_string(), "false".to_string()),
                    ("execution".to_string(), "not_performed".to_string()),
                ],
            );
            if report.has_errors() {
                ExitCode::from(1)
            } else {
                ExitCode::SUCCESS
            }
        }

        Some("vortex-read-plan") => {
            let Some(dataset_uri) = args.next() else {
                eprintln!("usage: shardloom vortex-read-plan <dataset_uri>");
                return ExitCode::from(2);
            };
            let uri = match DatasetUri::new(dataset_uri) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        "vortex-read-plan",
                        format,
                        "vortex read plan failed",
                        &error,
                    );
                }
            };
            let source = match shardloom_core::UniversalInputSource::from_dataset_uri(uri) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        "vortex-read-plan",
                        format,
                        "vortex read plan failed",
                        &error,
                    );
                }
            };
            let input_plan = match plan_native_vortex_universal_input(source) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        "vortex-read-plan",
                        format,
                        "vortex read plan failed",
                        &error,
                    );
                }
            };
            let report = match plan_vortex_read_from_universal_input(input_plan.clone()) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        "vortex-read-plan",
                        format,
                        "vortex read plan failed",
                        &error,
                    );
                }
            };
            let command_status = if report.has_errors() {
                CommandStatus::Unsupported
            } else {
                CommandStatus::Success
            };
            emit(
                "vortex-read-plan",
                format,
                command_status,
                "vortex read planning report".to_string(),
                report.to_human_text(),
                report.diagnostics.clone(),
                vec![
                    (
                        "fallback_execution_allowed".to_string(),
                        "false".to_string(),
                    ),
                    ("mode".to_string(), "vortex_read_plan".to_string()),
                    (
                        "native_vortex_input".to_string(),
                        input_plan.source.is_native_vortex().to_string(),
                    ),
                    ("metadata_only".to_string(), "true".to_string()),
                    ("plan_only".to_string(), "true".to_string()),
                    ("data_executed".to_string(), "false".to_string()),
                    ("data_read".to_string(), "false".to_string()),
                    ("data_materialized".to_string(), "false".to_string()),
                    ("object_store_io".to_string(), "false".to_string()),
                    ("write_io".to_string(), "false".to_string()),
                    ("external_effects_executed".to_string(), "false".to_string()),
                    ("execution".to_string(), "not_performed".to_string()),
                ],
            );
            if report.has_errors() {
                ExitCode::from(1)
            } else {
                ExitCode::SUCCESS
            }
        }
        Some("vortex-task-graph") => {
            let Some(dataset_uri) = args.next() else {
                eprintln!("usage: shardloom vortex-task-graph <dataset_uri>");
                return ExitCode::from(2);
            };
            let uri = match DatasetUri::new(dataset_uri) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        "vortex-task-graph",
                        format,
                        "vortex task graph plan failed",
                        &error,
                    );
                }
            };
            let source = match shardloom_core::UniversalInputSource::from_dataset_uri(uri) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        "vortex-task-graph",
                        format,
                        "vortex task graph plan failed",
                        &error,
                    );
                }
            };
            let input_plan = match plan_native_vortex_universal_input(source) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        "vortex-task-graph",
                        format,
                        "vortex task graph plan failed",
                        &error,
                    );
                }
            };
            if input_plan.has_errors() {
                let command_status = CommandStatus::Unsupported;
                emit(
                    "vortex-task-graph",
                    format,
                    command_status,
                    "vortex task graph plan failed: unsupported input".to_string(),
                    input_plan.to_human_text(),
                    input_plan.diagnostics.clone(),
                    vec![
                        (
                            "fallback_execution_allowed".to_string(),
                            "false".to_string(),
                        ),
                        ("mode".to_string(), "vortex_task_graph".to_string()),
                        (
                            "native_vortex_input".to_string(),
                            input_plan.source.is_native_vortex().to_string(),
                        ),
                        ("plan_only".to_string(), "true".to_string()),
                        ("tasks_executed".to_string(), "false".to_string()),
                        ("data_executed".to_string(), "false".to_string()),
                        ("data_read".to_string(), "false".to_string()),
                        ("data_materialized".to_string(), "false".to_string()),
                        ("object_store_io".to_string(), "false".to_string()),
                        ("write_io".to_string(), "false".to_string()),
                        ("external_effects_executed".to_string(), "false".to_string()),
                        ("execution".to_string(), "not_performed".to_string()),
                    ],
                );
                return ExitCode::from(1);
            }
            let read_report = match plan_vortex_read_from_universal_input(input_plan.clone()) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        "vortex-task-graph",
                        format,
                        "vortex task graph plan failed",
                        &error,
                    );
                }
            };
            let report = match build_vortex_runtime_task_graph(read_report) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        "vortex-task-graph",
                        format,
                        "vortex task graph plan failed",
                        &error,
                    );
                }
            };
            let command_status = if report.has_errors() {
                CommandStatus::Unsupported
            } else {
                CommandStatus::Success
            };
            emit(
                "vortex-task-graph",
                format,
                command_status,
                "vortex runtime task graph planning report".to_string(),
                report.to_human_text(),
                report.diagnostics.clone(),
                vec![
                    (
                        "fallback_execution_allowed".to_string(),
                        "false".to_string(),
                    ),
                    ("mode".to_string(), "vortex_task_graph".to_string()),
                    (
                        "native_vortex_input".to_string(),
                        input_plan.source.is_native_vortex().to_string(),
                    ),
                    ("plan_only".to_string(), "true".to_string()),
                    ("tasks_executed".to_string(), "false".to_string()),
                    ("data_executed".to_string(), "false".to_string()),
                    ("data_read".to_string(), "false".to_string()),
                    ("data_materialized".to_string(), "false".to_string()),
                    ("object_store_io".to_string(), "false".to_string()),
                    ("write_io".to_string(), "false".to_string()),
                    ("external_effects_executed".to_string(), "false".to_string()),
                    ("execution".to_string(), "not_performed".to_string()),
                ],
            );
            if report.has_errors() {
                ExitCode::from(1)
            } else {
                ExitCode::SUCCESS
            }
        }

        Some("schema-plan") => {
            let schema = match (SchemaId::new("schema-placeholder"), SchemaVersion::new(1)) {
                (Ok(id), Ok(version)) => SchemaDefinition::new(id, version),
                (Err(error), _) | (_, Err(error)) => {
                    return emit_error("schema-plan", format, "schema plan failed", &error);
                }
            };
            let text = schema.summary();
            emit(
                "schema-plan",
                format,
                CommandStatus::Success,
                "schema plan skeleton".to_string(),
                text,
                vec![],
                vec![
                    (
                        "fallback_execution_allowed".to_string(),
                        "false".to_string(),
                    ),
                    ("mode".to_string(), "schema_plan".to_string()),
                    ("write_io".to_string(), "false".to_string()),
                    ("execution".to_string(), "not_performed".to_string()),
                    ("plan_only".to_string(), "true".to_string()),
                    (
                        "table_formats_are".to_string(),
                        "compatibility_targets_not_fallback_engines".to_string(),
                    ),
                ],
            );
            ExitCode::SUCCESS
        }
        Some("catalog-plan") => {
            let kind = match args.next().as_deref() {
                Some("local") => CatalogKind::LocalManifest,
                Some("object-store") => CatalogKind::ObjectStoreManifest,
                Some("iceberg") => CatalogKind::IcebergCompatible,
                Some("delta") => CatalogKind::DeltaCompatible,
                Some("hive") => CatalogKind::HiveStylePath,
                Some("foundry") => CatalogKind::FoundryCompatible,
                Some(_) | None => CatalogKind::Unknown,
            };
            let Some(name) = args.next() else {
                return emit_error(
                    "catalog-plan",
                    format,
                    "catalog plan failed",
                    &ShardLoomError::InvalidOperation("missing catalog name".to_string()),
                );
            };
            let catalog = match CatalogRef::new(kind, name) {
                Ok(c) => c,
                Err(error) => {
                    return emit_error("catalog-plan", format, "catalog plan failed", &error);
                }
            };
            emit(
                "catalog-plan",
                format,
                CommandStatus::Success,
                "catalog reference plan skeleton".to_string(),
                catalog.summary(),
                vec![],
                vec![
                    (
                        "fallback_execution_allowed".to_string(),
                        "false".to_string(),
                    ),
                    ("mode".to_string(), "catalog_plan".to_string()),
                    ("write_io".to_string(), "false".to_string()),
                    ("execution".to_string(), "not_performed".to_string()),
                    ("plan_only".to_string(), "true".to_string()),
                    (
                        "table_formats_are".to_string(),
                        "compatibility_targets_not_fallback_engines".to_string(),
                    ),
                ],
            );
            ExitCode::SUCCESS
        }
        Some("table-compat-plan") => {
            let format_kind = match args.next().as_deref() {
                Some("vortex") => TableFormatKind::NativeVortexManifest,
                Some("iceberg") => TableFormatKind::IcebergCompatible,
                Some("delta") => TableFormatKind::DeltaCompatible,
                Some("hive") => TableFormatKind::HiveStyle,
                Some("external") => TableFormatKind::ExternalCatalogOnly,
                Some(_) | None => TableFormatKind::Unknown,
            };
            let plan = if format_kind.is_native_vortex() {
                TableCompatibilityPlan::native_vortex()
            } else if format_kind.is_compatibility_target() {
                TableCompatibilityPlan::compatibility_target(format_kind)
            } else {
                TableCompatibilityPlan::unsupported(
                    format_kind,
                    "table_compat_plan",
                    "Unknown table format is unsupported for compatibility planning.",
                )
            };
            let status = if plan.has_errors() {
                CommandStatus::Unsupported
            } else {
                CommandStatus::Success
            };
            emit(
                "table-compat-plan",
                format,
                status,
                "table compatibility plan skeleton".to_string(),
                plan.to_human_text(),
                plan.diagnostics.clone(),
                vec![
                    (
                        "fallback_execution_allowed".to_string(),
                        "false".to_string(),
                    ),
                    ("mode".to_string(), "table_compat_plan".to_string()),
                    ("write_io".to_string(), "false".to_string()),
                    ("execution".to_string(), "not_performed".to_string()),
                    ("plan_only".to_string(), "true".to_string()),
                    (
                        "table_formats_are".to_string(),
                        "compatibility_targets_not_fallback_engines".to_string(),
                    ),
                ],
            );
            if plan.has_errors() {
                ExitCode::from(1)
            } else {
                ExitCode::SUCCESS
            }
        }
        Some("capabilities") => {
            let capabilities = shardloom_core::EngineCapabilities::current();
            emit(
                "capabilities",
                format,
                CommandStatus::Success,
                "engine capabilities".to_string(),
                capabilities.to_human_text(),
                vec![],
                vec![
                    (
                        "fallback_execution_allowed".to_string(),
                        "false".to_string(),
                    ),
                    ("native_input".to_string(), "vortex".to_string()),
                    ("native_output".to_string(), "vortex".to_string()),
                ],
            );
            ExitCode::SUCCESS
        }
        Some("extension-registry") => {
            let snapshot = ExtensionRegistrySnapshot::empty();
            emit(
                "extension-registry",
                format,
                CommandStatus::Success,
                "extension registry metadata-only snapshot".to_string(),
                snapshot.to_human_text(),
                snapshot.diagnostics.clone(),
                vec![
                    (
                        "fallback_execution_allowed".to_string(),
                        "false".to_string(),
                    ),
                    ("mode".to_string(), "extension_registry".to_string()),
                    ("write_io".to_string(), "false".to_string()),
                    ("execution".to_string(), "not_performed".to_string()),
                    ("plan_only".to_string(), "true".to_string()),
                    ("extension_code_executed".to_string(), "false".to_string()),
                    ("dynamic_loading".to_string(), "false".to_string()),
                ],
            );
            ExitCode::SUCCESS
        }
        Some("extension-inspect") => {
            let Some(extension_id) = args.next() else {
                return emit_error(
                    "extension-inspect",
                    format,
                    "extension inspect failed",
                    &ShardLoomError::InvalidOperation("missing extension_id".to_string()),
                );
            };
            let id = match ExtensionId::new(extension_id.clone()) {
                Ok(v) => v,
                Err(e) => {
                    return emit_error("extension-inspect", format, "extension inspect failed", &e);
                }
            };
            let manifest = match ExtensionManifest::new(
                id,
                extension_id,
                ExtensionVersion::new(0, 1, 0),
                shardloom_core::ExtensionCategory::Unknown,
                ExtensionProvenance::new(ExtensionLicenseKind::Unknown),
            ) {
                Ok(v) => v,
                Err(e) => {
                    return emit_error("extension-inspect", format, "extension inspect failed", &e);
                }
            };
            let report = ExtensionInspectionReport::requires_review(
                manifest,
                "Extension inspection is metadata-only and requires provenance review.",
            );
            let status = if report.has_errors() {
                CommandStatus::Warning
            } else {
                CommandStatus::Success
            };
            emit(
                "extension-inspect",
                format,
                status,
                "extension inspection metadata-only report".to_string(),
                report.to_human_text(),
                report.diagnostics.clone(),
                vec![
                    (
                        "fallback_execution_allowed".to_string(),
                        "false".to_string(),
                    ),
                    ("mode".to_string(), "extension_inspect".to_string()),
                    ("write_io".to_string(), "false".to_string()),
                    ("execution".to_string(), "not_performed".to_string()),
                    ("plan_only".to_string(), "true".to_string()),
                    ("extension_code_executed".to_string(), "false".to_string()),
                    ("dynamic_loading".to_string(), "false".to_string()),
                ],
            );
            ExitCode::SUCCESS
        }
        Some("udf-runtime-plan") => {
            let runtime = match args.next().as_deref() {
                Some("rust") => UdfRuntimeKind::RustNative,
                Some("wasm") => UdfRuntimeKind::Wasm,
                Some("python") => UdfRuntimeKind::Python,
                Some("sql") => UdfRuntimeKind::SqlDefined,
                Some("external") => UdfRuntimeKind::ExternalService,
                Some(_) | None => UdfRuntimeKind::Unknown,
            };
            let text = format!(
                "udf runtime={} available_initially={} sandboxing_required={} execution=not_performed fallback_execution=disabled",
                runtime.as_str(),
                runtime.is_available_initially(),
                runtime.requires_sandboxing()
            );
            emit(
                "udf-runtime-plan",
                format,
                CommandStatus::Success,
                "udf runtime availability skeleton".to_string(),
                text,
                vec![],
                vec![
                    (
                        "fallback_execution_allowed".to_string(),
                        "false".to_string(),
                    ),
                    ("mode".to_string(), "udf_runtime_plan".to_string()),
                    ("write_io".to_string(), "false".to_string()),
                    ("execution".to_string(), "not_performed".to_string()),
                    ("plan_only".to_string(), "true".to_string()),
                    ("extension_code_executed".to_string(), "false".to_string()),
                    ("dynamic_loading".to_string(), "false".to_string()),
                ],
            );
            ExitCode::SUCCESS
        }
        Some("security-plan") => {
            let plan = SecurityPlan::default_safe();
            let text = plan.to_human_text();
            emit(
                "security-plan",
                format,
                CommandStatus::Success,
                "security plan skeleton".to_string(),
                text,
                vec![],
                vec![
                    (
                        "fallback_execution_allowed".to_string(),
                        "false".to_string(),
                    ),
                    ("mode".to_string(), "security_plan".to_string()),
                    ("write_io".to_string(), "false".to_string()),
                    ("execution".to_string(), "not_performed".to_string()),
                    ("plan_only".to_string(), "true".to_string()),
                    ("external_effects".to_string(), "disabled".to_string()),
                    ("credentials_resolved".to_string(), "false".to_string()),
                    ("secrets_loaded".to_string(), "false".to_string()),
                ],
            );
            ExitCode::SUCCESS
        }
        Some("agent-safety-plan") => {
            let mut plan = SecurityPlan::default_safe();
            plan.agent_mode = shardloom_core::AgentSafetyMode::AgentDryRunOnly;
            let text = plan.to_human_text();
            emit(
                "agent-safety-plan",
                format,
                CommandStatus::Success,
                "agent safety plan skeleton".to_string(),
                text,
                vec![],
                vec![
                    (
                        "fallback_execution_allowed".to_string(),
                        "false".to_string(),
                    ),
                    ("mode".to_string(), "agent_safety_plan".to_string()),
                    ("write_io".to_string(), "false".to_string()),
                    ("execution".to_string(), "not_performed".to_string()),
                    ("plan_only".to_string(), "true".to_string()),
                    ("external_effects".to_string(), "disabled".to_string()),
                    ("credentials_resolved".to_string(), "false".to_string()),
                    ("secrets_loaded".to_string(), "false".to_string()),
                ],
            );
            ExitCode::SUCCESS
        }
        Some("redaction-plan") => {
            let redaction = RedactionPolicy::strict();
            let text = redaction.summary();
            emit(
                "redaction-plan",
                format,
                CommandStatus::Success,
                "redaction plan skeleton".to_string(),
                text,
                vec![],
                vec![
                    (
                        "fallback_execution_allowed".to_string(),
                        "false".to_string(),
                    ),
                    ("mode".to_string(), "redaction_plan".to_string()),
                    ("write_io".to_string(), "false".to_string()),
                    ("execution".to_string(), "not_performed".to_string()),
                    ("plan_only".to_string(), "true".to_string()),
                    ("external_effects".to_string(), "disabled".to_string()),
                    ("credentials_resolved".to_string(), "false".to_string()),
                    ("secrets_loaded".to_string(), "false".to_string()),
                ],
            );
            ExitCode::SUCCESS
        }
        Some("plan-ir") => {
            let plan_id = match PlanId::new("plan-placeholder") {
                Ok(v) => v,
                Err(error) => return emit_error("plan-ir", format, "invalid plan id", &error),
            };
            let mut document = NativePlanDocument::empty(plan_id);
            document.validate_skeleton();
            emit(
                "plan-ir",
                format,
                CommandStatus::Warning,
                "native plan ir skeleton".to_string(),
                document.to_human_text(),
                document.validation.diagnostics.clone(),
                vec![
                    (
                        "fallback_execution_allowed".to_string(),
                        "false".to_string(),
                    ),
                    ("mode".to_string(), "plan_ir".to_string()),
                    ("write_io".to_string(), "false".to_string()),
                    ("execution".to_string(), "not_performed".to_string()),
                    ("plan_only".to_string(), "true".to_string()),
                    ("interop_format".to_string(), "native".to_string()),
                    ("validation_required".to_string(), "true".to_string()),
                ],
            );
            ExitCode::SUCCESS
        }
        Some("plan-import") => {
            let Some(format_raw) = args.next() else {
                eprintln!("usage: shardloom plan-import <format> <source_label>");
                return ExitCode::from(2);
            };
            let Some(source_label) = args.next() else {
                eprintln!("usage: shardloom plan-import <format> <source_label>");
                return ExitCode::from(2);
            };
            let format_kind = parse_plan_interop_format(&format_raw);
            let request = match PlanImportRequest::not_implemented(format_kind, source_label) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error("plan-import", format, "invalid import request", &error);
                }
            };
            emit(
                "plan-import",
                format,
                CommandStatus::Unsupported,
                "plan import skeleton".to_string(),
                request.summary(),
                request.diagnostics.clone(),
                vec![
                    (
                        "fallback_execution_allowed".to_string(),
                        "false".to_string(),
                    ),
                    ("mode".to_string(), "plan_import".to_string()),
                    ("write_io".to_string(), "false".to_string()),
                    ("execution".to_string(), "not_performed".to_string()),
                    ("plan_only".to_string(), "true".to_string()),
                    (
                        "interop_format".to_string(),
                        format_kind.as_str().to_string(),
                    ),
                    ("validation_required".to_string(), "true".to_string()),
                ],
            );
            ExitCode::from(1)
        }
        Some("plan-export") => {
            let Some(format_raw) = args.next() else {
                eprintln!("usage: shardloom plan-export <format>");
                return ExitCode::from(2);
            };
            let format_kind = parse_plan_interop_format(&format_raw);
            let request = PlanExportRequest::not_implemented(format_kind);
            emit(
                "plan-export",
                format,
                CommandStatus::Unsupported,
                "plan export skeleton".to_string(),
                request.summary(),
                request.diagnostics.clone(),
                vec![
                    (
                        "fallback_execution_allowed".to_string(),
                        "false".to_string(),
                    ),
                    ("mode".to_string(), "plan_export".to_string()),
                    ("write_io".to_string(), "false".to_string()),
                    ("execution".to_string(), "not_performed".to_string()),
                    ("plan_only".to_string(), "true".to_string()),
                    (
                        "interop_format".to_string(),
                        format_kind.as_str().to_string(),
                    ),
                    ("validation_required".to_string(), "false".to_string()),
                ],
            );
            ExitCode::from(1)
        }
        Some("memory-plan") => {
            let Some(memory_gb) = args.next() else {
                eprintln!("usage: shardloom memory-plan <memory_gb>");
                return ExitCode::from(2);
            };
            let memory_gb = match memory_gb.parse::<u64>() {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        "memory-plan",
                        format,
                        "invalid memory_gb",
                        &ShardLoomError::InvalidOperation(format!("invalid memory_gb: {error}")),
                    );
                }
            };
            let budget = match MemoryBudget::from_gib(memory_gb) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error("memory-plan", format, "invalid memory budget", &error);
                }
            };
            let plan = OomSafetyPlan::new(MemoryPoolPlan::new(budget));
            emit(
                "memory-plan",
                format,
                CommandStatus::Success,
                "memory plan".to_string(),
                plan.to_human_text(),
                vec![],
                vec![("mode".to_string(), "plan_only".to_string())],
            );
            ExitCode::SUCCESS
        }
        Some("spill-plan") => {
            let Some(operator_label) = args.next() else {
                eprintln!("usage: shardloom spill-plan <operator_label> <memory_gb>");
                return ExitCode::from(2);
            };
            let Some(memory_gb) = args.next() else {
                eprintln!("usage: shardloom spill-plan <operator_label> <memory_gb>");
                return ExitCode::from(2);
            };
            let memory_gb = match memory_gb.parse::<u64>() {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        "spill-plan",
                        format,
                        "invalid memory_gb",
                        &ShardLoomError::InvalidOperation(format!("invalid memory_gb: {error}")),
                    );
                }
            };
            let budget = match MemoryBudget::from_gib(memory_gb) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error("spill-plan", format, "invalid memory budget", &error);
                }
            };
            let pool = MemoryPoolPlan::new(budget);
            let lower = operator_label.to_lowercase();
            let class = if lower.contains("sort") {
                OperatorMemoryClass::Sort
            } else if lower.contains("join") {
                OperatorMemoryClass::Join
            } else if lower.contains("agg") || lower.contains("aggregate") {
                OperatorMemoryClass::Aggregate
            } else {
                OperatorMemoryClass::Unknown
            };
            let owner = match MemoryOwner::new(class, operator_label) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error("spill-plan", format, "invalid operator label", &error);
                }
            };
            let spill_plan = SpillPlan::spill_not_implemented(owner, SpillPolicy::BestEffort);
            let mut plan = OomSafetyPlan::new(pool);
            plan.add_spill_plan(spill_plan);
            let status = if plan.has_errors() {
                CommandStatus::Unsupported
            } else {
                CommandStatus::Success
            };
            emit(
                "spill-plan",
                format,
                status,
                "spill plan".to_string(),
                plan.to_human_text(),
                vec![],
                vec![("mode".to_string(), "plan_only".to_string())],
            );
            if plan.has_errors() {
                ExitCode::from(1)
            } else {
                ExitCode::SUCCESS
            }
        }
        Some("correctness-plan") => {
            let plan = CorrectnessValidationPlan::default_foundation_plan();
            emit(
                "correctness-plan",
                format,
                CommandStatus::Success,
                "correctness validation foundation plan".to_string(),
                plan.to_human_text(),
                vec![],
                vec![
                    (
                        "fallback_execution_allowed".to_string(),
                        "false".to_string(),
                    ),
                    ("mode".to_string(), "correctness_plan".to_string()),
                    ("status".to_string(), "planned".to_string()),
                    (
                        "external_baselines".to_string(),
                        "test_oracles_only".to_string(),
                    ),
                ],
            );
            ExitCode::SUCCESS
        }
        Some("kernel-registry") => {
            let snapshot = KernelRegistrySnapshot::empty();
            emit(
                "kernel-registry",
                format,
                CommandStatus::Success,
                "kernel registry snapshot".to_string(),
                snapshot.summary(),
                vec![],
                vec![
                    (
                        "fallback_execution_allowed".to_string(),
                        "false".to_string(),
                    ),
                    ("mode".to_string(), "kernel_registry_snapshot".to_string()),
                    ("status".to_string(), "empty".to_string()),
                    ("write_io".to_string(), "false".to_string()),
                    ("execution".to_string(), "not_performed".to_string()),
                    ("plan_only".to_string(), "true".to_string()),
                ],
            );
            ExitCode::SUCCESS
        }
        Some("recovery-plan") => {
            let plan = RecoveryPlan::recovery_not_implemented(
                "recovery_execution",
                "Recovery planning skeleton exists, but actual recovery execution is not implemented yet.",
            );
            emit(
                "recovery-plan",
                format,
                CommandStatus::Unsupported,
                "recovery plan skeleton".to_string(),
                plan.to_human_text(),
                plan.diagnostics.clone(),
                vec![
                    (
                        "fallback_execution_allowed".to_string(),
                        "false".to_string(),
                    ),
                    ("mode".to_string(), "recovery_plan".to_string()),
                    ("write_io".to_string(), "false".to_string()),
                    ("execution".to_string(), "not_performed".to_string()),
                    ("plan_only".to_string(), "true".to_string()),
                ],
            );
            ExitCode::from(1)
        }
        Some("cancellation-plan") => {
            let scope = match args.next().as_deref() {
                Some("query") => CancellationScope::Query,
                Some("task") => CancellationScope::Task,
                Some("scan") => CancellationScope::Scan,
                Some("output-write") => CancellationScope::OutputWrite,
                Some("external-effect") => CancellationScope::ExternalEffect,
                Some("spill-cleanup") => CancellationScope::SpillCleanup,
                Some("runtime" | _) | None => CancellationScope::Runtime,
            };
            let request = CancellationRequest::new(scope, CancellationReason::UserRequested);
            emit(
                "cancellation-plan",
                format,
                CommandStatus::Success,
                "cancellation plan skeleton".to_string(),
                request.summary(),
                request.diagnostics.clone(),
                vec![
                    (
                        "fallback_execution_allowed".to_string(),
                        "false".to_string(),
                    ),
                    ("mode".to_string(), "cancellation_plan".to_string()),
                    ("write_io".to_string(), "false".to_string()),
                    ("execution".to_string(), "not_performed".to_string()),
                    ("plan_only".to_string(), "true".to_string()),
                ],
            );
            ExitCode::SUCCESS
        }
        Some("retry-plan") => {
            let Some(task_id) = args.next() else {
                eprintln!("usage: shardloom retry-plan <task_id> <attempt_id>");
                return ExitCode::from(2);
            };
            let Some(attempt_id) = args.next() else {
                eprintln!("usage: shardloom retry-plan <task_id> <attempt_id>");
                return ExitCode::from(2);
            };
            let task_id = match shardloom_exec::TaskId::new(task_id) {
                Ok(v) => v,
                Err(error) => return emit_error("retry-plan", format, "invalid task id", &error),
            };
            let attempt_id = match AttemptId::new(attempt_id) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error("retry-plan", format, "invalid attempt id", &error);
                }
            };
            let attempt = TaskAttemptRecord::new(task_id, attempt_id);
            let plan = RetryPlan::from_attempt(
                shardloom_exec::RetryPolicy::default_read_retries(),
                attempt,
            );
            emit(
                "retry-plan",
                format,
                CommandStatus::Success,
                "retry plan skeleton".to_string(),
                plan.summary(),
                plan.diagnostics.clone(),
                vec![
                    (
                        "fallback_execution_allowed".to_string(),
                        "false".to_string(),
                    ),
                    ("mode".to_string(), "retry_plan".to_string()),
                    ("write_io".to_string(), "false".to_string()),
                    ("execution".to_string(), "not_performed".to_string()),
                    ("plan_only".to_string(), "true".to_string()),
                ],
            );
            ExitCode::SUCCESS
        }
        Some("observability-plan") => {
            let plan = ObservabilityPlan::default_foundation_plan();
            emit(
                "observability-plan",
                format,
                CommandStatus::Success,
                "observability plan".to_string(),
                plan.to_human_text(),
                plan.diagnostics.clone(),
                vec![
                    (
                        "fallback_execution_allowed".to_string(),
                        "false".to_string(),
                    ),
                    ("mode".to_string(), "observability_plan".to_string()),
                    ("write_io".to_string(), "false".to_string()),
                    ("execution".to_string(), "not_performed".to_string()),
                    ("plan_only".to_string(), "true".to_string()),
                    (
                        "metrics_collection".to_string(),
                        "not_performed".to_string(),
                    ),
                ],
            );
            ExitCode::SUCCESS
        }
        Some("runtime-report") => {
            let report = RuntimeObservabilityReport::not_run();
            emit(
                "runtime-report",
                format,
                CommandStatus::Success,
                "runtime observability report".to_string(),
                report.to_human_text(),
                report.diagnostics.clone(),
                vec![
                    (
                        "fallback_execution_allowed".to_string(),
                        "false".to_string(),
                    ),
                    ("mode".to_string(), "runtime_report".to_string()),
                    ("write_io".to_string(), "false".to_string()),
                    ("execution".to_string(), "not_performed".to_string()),
                    ("plan_only".to_string(), "true".to_string()),
                    (
                        "metrics_collection".to_string(),
                        "not_performed".to_string(),
                    ),
                ],
            );
            ExitCode::SUCCESS
        }
        Some("profile-plan") => {
            let plan = ObservabilityPlan::collection_not_implemented(
                "profiling",
                "Profiling domain types exist, but runtime profiling collection is not implemented yet.",
            );
            emit(
                "profile-plan",
                format,
                CommandStatus::Unsupported,
                "profiling collection not implemented".to_string(),
                plan.to_human_text(),
                plan.diagnostics.clone(),
                vec![
                    (
                        "fallback_execution_allowed".to_string(),
                        "false".to_string(),
                    ),
                    ("mode".to_string(), "profile_plan".to_string()),
                    ("write_io".to_string(), "false".to_string()),
                    ("execution".to_string(), "not_performed".to_string()),
                    ("plan_only".to_string(), "true".to_string()),
                    (
                        "metrics_collection".to_string(),
                        "not_performed".to_string(),
                    ),
                ],
            );
            ExitCode::SUCCESS
        }
        Some("doctor") => {
            emit("doctor", format, CommandStatus::Success, "doctor checks".to_string(), "ShardLoom doctor\nfallback execution: disabled\nnative input target: vortex\nnative output target: vortex\nstatus: early implementation skeleton".to_string(), vec![], vec![("native_input".to_string(), "vortex".to_string()), ("native_output".to_string(), "vortex".to_string())]);
            ExitCode::SUCCESS
        }
        Some("explain") => {
            let operation = args
                .next()
                .unwrap_or_else(|| "<unspecified operation>".to_string());
            let report = ExplainReport::unsupported(
                operation,
                "planning",
                "Real planning is not implemented yet.",
            );
            emit(
                "explain",
                format,
                CommandStatus::Unsupported,
                "explain plan".to_string(),
                report.to_human_text(),
                report.diagnostics.clone(),
                vec![("mode".to_string(), "plan_only".to_string())],
            );
            if report.has_errors() {
                ExitCode::from(1)
            } else {
                ExitCode::SUCCESS
            }
        }
        Some("benchmark-plan") => {
            let plan = shardloom_core::BenchmarkPlan::default_foundation_plan();
            emit(
                "benchmark-plan",
                format,
                CommandStatus::Success,
                "benchmark plan".to_string(),
                plan.to_human_text(),
                vec![],
                vec![],
            );
            ExitCode::SUCCESS
        }
        Some("manifest-plan") => {
            let Some(dataset_uri) = args.next() else {
                eprintln!("usage: shardloom manifest-plan <dataset_uri>");
                return ExitCode::from(2);
            };
            let uri = match DatasetUri::new(dataset_uri) {
                Ok(uri) => uri,
                Err(error) => {
                    return emit_error(
                        "manifest-plan",
                        format,
                        "invalid dataset uri",
                        &ShardLoomError::InvalidOperation(format!("invalid dataset uri: {error}")),
                    );
                }
            };
            let dataset = match DatasetRef::from_uri(uri) {
                Ok(dataset) => dataset,
                Err(error) => {
                    eprintln!("failed to create dataset reference: {error}");
                    return ExitCode::from(2);
                }
            };
            let snapshot =
                SnapshotRef::new(SnapshotId::new("snapshot-placeholder").expect("valid"));
            let manifest = DatasetManifest::new(
                ManifestId::new("manifest-placeholder").expect("valid"),
                dataset,
                snapshot,
            );
            emit(
                "manifest-plan",
                format,
                CommandStatus::Success,
                "manifest plan".to_string(),
                manifest.summary(),
                vec![],
                vec![("mode".to_string(), "plan_only".to_string())],
            );
            ExitCode::SUCCESS
        }
        Some("incremental-plan") => {
            let Some(snapshot_id) = args.next() else {
                eprintln!("usage: shardloom incremental-plan <snapshot_id>");
                return ExitCode::from(2);
            };
            let snapshot_id = match SnapshotId::new(snapshot_id) {
                Ok(snapshot) => snapshot,
                Err(error) => {
                    eprintln!("invalid snapshot id: {error}");
                    return ExitCode::from(2);
                }
            };
            let change_set = ChangeSet::new(snapshot_id);
            let plan = IncrementalPlanSkeleton::from_change_set(change_set);
            emit(
                "incremental-plan",
                format,
                CommandStatus::Success,
                "incremental plan".to_string(),
                plan.to_human_text(),
                vec![],
                vec![],
            );
            ExitCode::SUCCESS
        }
        Some("write-intent") => {
            let Some(target_uri) = args.next() else {
                eprintln!("usage: shardloom write-intent <target_uri>");
                return ExitCode::from(2);
            };
            let uri = match DatasetUri::new(target_uri) {
                Ok(uri) => uri,
                Err(error) => {
                    eprintln!("invalid dataset uri: {error}");
                    return ExitCode::from(2);
                }
            };
            let intent = WriteIntent::write_not_implemented(OutputTarget::from_uri(uri));
            emit(
                "write-intent",
                format,
                CommandStatus::Unsupported,
                "write intent".to_string(),
                intent.summary(),
                intent.diagnostics.clone(),
                vec![],
            );
            if intent.has_errors() {
                ExitCode::from(1)
            } else {
                ExitCode::SUCCESS
            }
        }
        Some("scan-plan") => {
            let Some(dataset_uri) = args.next() else {
                eprintln!("usage: shardloom scan-plan <dataset_uri>");
                return ExitCode::from(2);
            };
            let uri = match DatasetUri::new(dataset_uri) {
                Ok(uri) => uri,
                Err(error) => {
                    eprintln!("invalid dataset uri: {error}");
                    return ExitCode::from(2);
                }
            };
            let dataset = match DatasetRef::from_uri(uri) {
                Ok(dataset) => dataset,
                Err(error) => {
                    eprintln!("failed to create dataset reference: {error}");
                    return ExitCode::from(2);
                }
            };
            let request = ScanRequest::new(dataset);
            let skeleton = ScanPlanSkeleton::plan_only(request);
            emit(
                "scan-plan",
                format,
                if skeleton.has_errors() {
                    CommandStatus::Unsupported
                } else {
                    CommandStatus::Success
                },
                "scan plan".to_string(),
                skeleton.to_human_text(),
                skeleton.diagnostics.clone(),
                vec![("mode".to_string(), "plan_only".to_string())],
            );
            ExitCode::SUCCESS
        }
        Some("streaming-plan") => {
            let Some(dataset_uri) = args.next() else {
                eprintln!("usage: shardloom streaming-plan <dataset_uri> <target_uri>");
                return ExitCode::from(2);
            };
            let Some(target_uri) = args.next() else {
                eprintln!("usage: shardloom streaming-plan <dataset_uri> <target_uri>");
                return ExitCode::from(2);
            };
            let dataset_uri = match DatasetUri::new(dataset_uri) {
                Ok(uri) => uri,
                Err(error) => {
                    eprintln!("invalid dataset uri: {error}");
                    return ExitCode::from(2);
                }
            };
            let dataset_ref = match DatasetRef::from_uri(dataset_uri) {
                Ok(dataset_ref) => dataset_ref,
                Err(error) => {
                    eprintln!("failed to create dataset reference: {error}");
                    return ExitCode::from(2);
                }
            };
            let target_uri = match DatasetUri::new(target_uri) {
                Ok(uri) => uri,
                Err(error) => {
                    eprintln!("invalid target uri: {error}");
                    return ExitCode::from(2);
                }
            };
            let output_target = OutputTarget::from_uri(target_uri);
            let plan = StreamingPlanSkeleton::for_vortex_to_target(dataset_ref, output_target);
            emit(
                "streaming-plan",
                format,
                if plan.has_errors() {
                    CommandStatus::Unsupported
                } else {
                    CommandStatus::Success
                },
                "streaming plan".to_string(),
                plan.to_human_text(),
                plan.diagnostics.clone(),
                vec![],
            );
            if plan.has_errors() {
                ExitCode::from(1)
            } else {
                ExitCode::SUCCESS
            }
        }
        Some("runtime-plan") => {
            let Some(dataset_uri) = args.next() else {
                eprintln!("usage: shardloom runtime-plan <dataset_uri>");
                return ExitCode::from(2);
            };
            let uri = match DatasetUri::new(dataset_uri) {
                Ok(uri) => uri,
                Err(error) => {
                    eprintln!("invalid dataset uri: {error}");
                    return ExitCode::from(2);
                }
            };
            let dataset = match DatasetRef::from_uri(uri) {
                Ok(dataset) => dataset,
                Err(error) => {
                    eprintln!("failed to create dataset reference: {error}");
                    return ExitCode::from(2);
                }
            };
            let plan = match RuntimePlanSkeleton::for_dataset(dataset) {
                Ok(plan) => plan,
                Err(error) => {
                    eprintln!("failed to build runtime plan: {error}");
                    return ExitCode::from(2);
                }
            };
            emit(
                "runtime-plan",
                format,
                CommandStatus::Success,
                "runtime plan".to_string(),
                plan.to_human_text(),
                plan.diagnostics.clone(),
                vec![],
            );
            ExitCode::SUCCESS
        }
        Some("sizing-plan") => {
            let Some(dataset_uri) = args.next() else {
                eprintln!("usage: shardloom sizing-plan <dataset_uri> --memory-gb <gb>");
                return ExitCode::from(2);
            };
            let Some(memory_flag) = args.next() else {
                eprintln!("usage: shardloom sizing-plan <dataset_uri> --memory-gb <gb>");
                return ExitCode::from(2);
            };
            if memory_flag != "--memory-gb" {
                eprintln!("usage: shardloom sizing-plan <dataset_uri> --memory-gb <gb>");
                return ExitCode::from(2);
            }
            let Some(memory_gb_raw) = args.next() else {
                eprintln!("usage: shardloom sizing-plan <dataset_uri> --memory-gb <gb>");
                return ExitCode::from(2);
            };
            let memory_gb = match memory_gb_raw.parse::<u64>() {
                Ok(value) if value > 0 => value,
                _ => {
                    return emit_error(
                        "sizing-plan",
                        format,
                        "invalid memory setting",
                        &ShardLoomError::InvalidOperation(
                            "memory-gb must be a positive integer".to_string(),
                        ),
                    );
                }
            };
            let uri = match DatasetUri::new(dataset_uri) {
                Ok(uri) => uri,
                Err(error) => {
                    return emit_error(
                        "sizing-plan",
                        format,
                        "invalid dataset uri",
                        &ShardLoomError::InvalidOperation(format!("invalid dataset uri: {error}")),
                    );
                }
            };
            let dataset = match DatasetRef::from_uri(uri) {
                Ok(dataset) => dataset,
                Err(error) => {
                    eprintln!("failed to create dataset reference: {error}");
                    return ExitCode::from(2);
                }
            };
            let policy = AdaptiveSizingPolicy::memory_limited(ByteSize::from_gib(memory_gb));
            let sizer = AdaptiveSizer::new(policy.clone());
            let input = SizingInput::new(
                shardloom_core::SegmentId::new("placeholder-segment").expect("valid segment id"),
                SizeEstimate::unknown(),
            );
            let decision = sizer.decide_for_segment(&input);
            let parallelism =
                ParallelismPlan::new(ParallelismLimit::auto(), 1, 1, "planning skeleton");
            let mut plan = SizingPlan::new(policy, parallelism);
            plan.add_decision(input.segment_id.clone(), decision);
            emit(
                "sizing-plan",
                format,
                CommandStatus::Success,
                "sizing plan".to_string(),
                format!("dataset: {}\n{}", dataset.summary(), plan.to_human_text()),
                vec![],
                vec![],
            );
            ExitCode::SUCCESS
        }
        Some("task-plan") => {
            let Some(dataset_uri) = args.next() else {
                eprintln!("usage: shardloom task-plan <dataset_uri>");
                return ExitCode::from(2);
            };
            let uri = match DatasetUri::new(dataset_uri) {
                Ok(uri) => uri,
                Err(error) => {
                    eprintln!("invalid dataset uri: {error}");
                    return ExitCode::from(2);
                }
            };
            let dataset = match DatasetRef::from_uri(uri) {
                Ok(dataset) => dataset,
                Err(error) => {
                    eprintln!("failed to create dataset reference: {error}");
                    return ExitCode::from(2);
                }
            };
            let plan = match RuntimePlanSkeleton::for_dataset(dataset) {
                Ok(plan) => plan,
                Err(error) => {
                    eprintln!("failed to build task plan: {error}");
                    return ExitCode::from(2);
                }
            };
            emit(
                "task-plan",
                format,
                CommandStatus::Success,
                "task plan".to_string(),
                plan.graph.summary(),
                vec![],
                vec![],
            );
            ExitCode::SUCCESS
        }

        Some("vortex-adaptive-sizing") => {
            let Some(dataset_uri) = args.next() else {
                eprintln!("usage: shardloom vortex-adaptive-sizing <dataset_uri> <memory_gb>");
                return ExitCode::from(2);
            };
            let Some(memory_gb_text) = args.next() else {
                eprintln!("usage: shardloom vortex-adaptive-sizing <dataset_uri> <memory_gb>");
                return ExitCode::from(2);
            };
            let uri = match DatasetUri::new(dataset_uri) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        "vortex-adaptive-sizing",
                        format,
                        "vortex adaptive sizing failed",
                        &error,
                    );
                }
            };
            let memory_gb: u64 = match memory_gb_text.parse() {
                Ok(v) => v,
                Err(_) => {
                    return emit_error(
                        "vortex-adaptive-sizing",
                        format,
                        "vortex adaptive sizing failed",
                        &ShardLoomError::InvalidOperation(
                            "memory_gb must be an unsigned integer".to_string(),
                        ),
                    );
                }
            };
            let source = match shardloom_core::UniversalInputSource::from_dataset_uri(uri.clone()) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        "vortex-adaptive-sizing",
                        format,
                        "vortex adaptive sizing failed",
                        &error,
                    );
                }
            };
            let input_plan = match plan_native_vortex_universal_input(source) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        "vortex-adaptive-sizing",
                        format,
                        "vortex adaptive sizing failed",
                        &error,
                    );
                }
            };
            if input_plan.has_errors() || !input_plan.source.is_native_vortex() {
                emit(
                    "vortex-memory-plan",
                    format,
                    CommandStatus::Unsupported,
                    "vortex memory planning report".to_string(),
                    input_plan.to_human_text(),
                    input_plan.diagnostics.clone(),
                    vec![
                        (
                            "fallback_execution_allowed".to_string(),
                            "false".to_string(),
                        ),
                        ("mode".to_string(), "vortex_memory_plan".to_string()),
                        ("execution".to_string(), "not_performed".to_string()),
                    ],
                );
                return ExitCode::from(1);
            }
            let read_report = match plan_vortex_read_from_universal_input(input_plan.clone()) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        "vortex-adaptive-sizing",
                        format,
                        "vortex adaptive sizing failed",
                        &error,
                    );
                }
            };
            if read_report.has_errors() {
                emit(
                    "vortex-memory-plan",
                    format,
                    CommandStatus::Unsupported,
                    "vortex memory planning report".to_string(),
                    read_report.to_human_text(),
                    read_report.diagnostics.clone(),
                    vec![
                        (
                            "fallback_execution_allowed".to_string(),
                            "false".to_string(),
                        ),
                        ("mode".to_string(), "vortex_memory_plan".to_string()),
                        ("execution".to_string(), "not_performed".to_string()),
                    ],
                );
                return ExitCode::from(1);
            }
            let runtime_report = match build_vortex_runtime_task_graph(read_report) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        "vortex-adaptive-sizing",
                        format,
                        "vortex adaptive sizing failed",
                        &error,
                    );
                }
            };
            if runtime_report.has_errors() {
                emit(
                    "vortex-memory-plan",
                    format,
                    CommandStatus::Unsupported,
                    "vortex memory planning report".to_string(),
                    runtime_report.to_human_text(),
                    runtime_report.diagnostics.clone(),
                    vec![
                        (
                            "fallback_execution_allowed".to_string(),
                            "false".to_string(),
                        ),
                        ("mode".to_string(), "vortex_memory_plan".to_string()),
                        ("execution".to_string(), "not_performed".to_string()),
                    ],
                );
                return ExitCode::from(1);
            }
            let policy = AdaptiveSizingPolicy::memory_limited(ByteSize::from_gib(memory_gb));
            let report = match size_vortex_runtime_task_graph(runtime_report, policy) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        "vortex-adaptive-sizing",
                        format,
                        "vortex adaptive sizing failed",
                        &error,
                    );
                }
            };
            emit(
                "vortex-adaptive-sizing",
                format,
                if report.has_errors() {
                    CommandStatus::Unsupported
                } else {
                    CommandStatus::Success
                },
                "vortex adaptive sizing report".to_string(),
                report.to_human_text(),
                report.diagnostics.clone(),
                vec![
                    (
                        "fallback_execution_allowed".to_string(),
                        "false".to_string(),
                    ),
                    ("mode".to_string(), "vortex_adaptive_sizing".to_string()),
                    (
                        "native_vortex_input".to_string(),
                        input_plan.source.is_native_vortex().to_string(),
                    ),
                    ("plan_only".to_string(), "true".to_string()),
                    ("tasks_executed".to_string(), "false".to_string()),
                    ("data_executed".to_string(), "false".to_string()),
                    ("data_read".to_string(), "false".to_string()),
                    ("data_materialized".to_string(), "false".to_string()),
                    ("object_store_io".to_string(), "false".to_string()),
                    ("write_io".to_string(), "false".to_string()),
                    ("external_effects_executed".to_string(), "false".to_string()),
                    ("execution".to_string(), "not_performed".to_string()),
                    ("memory_gb".to_string(), memory_gb.to_string()),
                ],
            );
            if report.has_errors() {
                ExitCode::from(1)
            } else {
                ExitCode::SUCCESS
            }
        }
        Some("vortex-memory-plan") => {
            let Some(dataset_uri) = args.next() else {
                eprintln!("usage: shardloom vortex-memory-plan <dataset_uri> <memory_gb>");
                return ExitCode::from(2);
            };
            let Some(memory_gb_text) = args.next() else {
                eprintln!("usage: shardloom vortex-memory-plan <dataset_uri> <memory_gb>");
                return ExitCode::from(2);
            };
            let uri = match DatasetUri::new(dataset_uri) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        "vortex-memory-plan",
                        format,
                        "vortex memory plan failed",
                        &error,
                    );
                }
            };
            let memory_gb: u64 = match memory_gb_text.parse() {
                Ok(v) => v,
                Err(_) => {
                    return emit_error(
                        "vortex-memory-plan",
                        format,
                        "vortex memory plan failed",
                        &ShardLoomError::InvalidOperation(
                            "memory_gb must be an unsigned integer".to_string(),
                        ),
                    );
                }
            };
            let source = match shardloom_core::UniversalInputSource::from_dataset_uri(uri.clone()) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        "vortex-memory-plan",
                        format,
                        "vortex memory plan failed",
                        &error,
                    );
                }
            };
            let input_plan = match plan_native_vortex_universal_input(source) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        "vortex-memory-plan",
                        format,
                        "vortex memory plan failed",
                        &error,
                    );
                }
            };
            let read_report = match plan_vortex_read_from_universal_input(input_plan.clone()) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        "vortex-memory-plan",
                        format,
                        "vortex memory plan failed",
                        &error,
                    );
                }
            };
            let runtime_report = match build_vortex_runtime_task_graph(read_report) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        "vortex-memory-plan",
                        format,
                        "vortex memory plan failed",
                        &error,
                    );
                }
            };
            let sizing_policy = AdaptiveSizingPolicy::memory_limited(ByteSize::from_gib(memory_gb));
            let sizing_report = match size_vortex_runtime_task_graph(runtime_report, sizing_policy)
            {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        "vortex-memory-plan",
                        format,
                        "vortex memory plan failed",
                        &error,
                    );
                }
            };
            if sizing_report.has_errors() {
                emit(
                    "vortex-memory-plan",
                    format,
                    CommandStatus::Unsupported,
                    "vortex memory planning report".to_string(),
                    sizing_report.to_human_text(),
                    sizing_report.diagnostics.clone(),
                    vec![
                        (
                            "fallback_execution_allowed".to_string(),
                            "false".to_string(),
                        ),
                        ("mode".to_string(), "vortex_memory_plan".to_string()),
                        ("execution".to_string(), "not_performed".to_string()),
                    ],
                );
                return ExitCode::from(1);
            }
            let budget = match MemoryBudget::from_gib(memory_gb) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        "vortex-memory-plan",
                        format,
                        "vortex memory plan failed",
                        &error,
                    );
                }
            };
            let report = match plan_vortex_memory_safety(sizing_report, budget) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        "vortex-memory-plan",
                        format,
                        "vortex memory plan failed",
                        &error,
                    );
                }
            };
            emit(
                "vortex-memory-plan",
                format,
                if report.has_errors() {
                    CommandStatus::Unsupported
                } else {
                    CommandStatus::Success
                },
                "vortex memory planning report".to_string(),
                report.to_human_text(),
                report.diagnostics.clone(),
                vec![
                    (
                        "fallback_execution_allowed".to_string(),
                        "false".to_string(),
                    ),
                    ("mode".to_string(), "vortex_memory_plan".to_string()),
                    (
                        "native_vortex_input".to_string(),
                        input_plan.source.is_native_vortex().to_string(),
                    ),
                    ("plan_only".to_string(), "true".to_string()),
                    ("tasks_executed".to_string(), "false".to_string()),
                    ("data_executed".to_string(), "false".to_string()),
                    ("data_read".to_string(), "false".to_string()),
                    ("data_materialized".to_string(), "false".to_string()),
                    ("object_store_io".to_string(), "false".to_string()),
                    ("write_io".to_string(), "false".to_string()),
                    ("spill_io_performed".to_string(), "false".to_string()),
                    ("external_effects_executed".to_string(), "false".to_string()),
                    ("execution".to_string(), "not_performed".to_string()),
                    ("memory_gb".to_string(), memory_gb.to_string()),
                ],
            );
            if report.has_errors() {
                ExitCode::from(1)
            } else {
                ExitCode::SUCCESS
            }
        }
        Some("vortex-plan") => {
            let Some(dataset_uri) = args.next() else {
                eprintln!("usage: shardloom vortex-plan <dataset_uri>");
                return ExitCode::from(2);
            };
            let uri = match DatasetUri::new(dataset_uri) {
                Ok(uri) => uri,
                Err(error) => {
                    eprintln!("invalid dataset uri: {error}");
                    return ExitCode::from(2);
                }
            };
            let file_ref = match VortexFileRef::from_uri(uri) {
                Ok(file_ref) => file_ref,
                Err(error) => {
                    eprintln!("{error}");
                    return ExitCode::from(2);
                }
            };
            emit(
                "vortex-plan",
                format,
                CommandStatus::Success,
                "vortex read plan".to_string(),
                VortexReadPlan::metadata_only(file_ref).to_human_text(),
                vec![],
                vec![("mode".to_string(), "metadata_only".to_string())],
            );
            ExitCode::SUCCESS
        }
        Some("translation-plan") => {
            let Some(target_uri) = args.next() else {
                eprintln!("usage: shardloom translation-plan <target_uri>");
                return ExitCode::from(2);
            };
            let uri = match DatasetUri::new(target_uri) {
                Ok(uri) => uri,
                Err(error) => {
                    eprintln!("invalid dataset uri: {error}");
                    return ExitCode::from(2);
                }
            };
            let target = OutputTarget::from_uri(uri);
            let plan = TranslationPlan::for_target(target);
            emit(
                "translation-plan",
                format,
                if plan.has_errors() {
                    CommandStatus::Unsupported
                } else {
                    CommandStatus::Success
                },
                "translation plan".to_string(),
                plan.to_human_text(),
                plan.diagnostics.clone(),
                vec![],
            );
            if plan.has_errors() {
                ExitCode::from(1)
            } else {
                ExitCode::SUCCESS
            }
        }
        Some("vortex-output-plan") => {
            let Some(target_uri) = args.next() else {
                eprintln!("usage: shardloom vortex-output-plan <target_uri>");
                return ExitCode::from(2);
            };
            let uri = match DatasetUri::new(target_uri) {
                Ok(uri) => uri,
                Err(error) => {
                    eprintln!("invalid dataset uri: {error}");
                    return ExitCode::from(2);
                }
            };
            let file_ref = match VortexFileRef::from_uri(uri) {
                Ok(file_ref) => file_ref,
                Err(error) => {
                    eprintln!("{error}");
                    return ExitCode::from(2);
                }
            };
            emit(
                "vortex-output-plan",
                format,
                CommandStatus::Success,
                "vortex output plan".to_string(),
                VortexWritePlan::planned(file_ref, VortexWriteOptions::native_defaults())
                    .to_human_text(),
                vec![],
                vec![("target_format".to_string(), "vortex".to_string())],
            );
            ExitCode::SUCCESS
        }
        Some("vortex-readiness") => {
            let readiness = VortexAdapterReadiness::dependency_added_compile_only();
            emit(
                "vortex-readiness",
                format,
                CommandStatus::Success,
                "vortex dependency readiness".to_string(),
                readiness.to_human_text(),
                readiness.diagnostics.clone(),
                vec![
                    (
                        "fallback_execution_allowed".to_string(),
                        "false".to_string(),
                    ),
                    ("mode".to_string(), "vortex_readiness".to_string()),
                    (
                        "upstream_vortex_dependency".to_string(),
                        readiness.dependency_status.as_str().to_string(),
                    ),
                    ("write_io".to_string(), "false".to_string()),
                    ("execution".to_string(), "not_performed".to_string()),
                    ("plan_only".to_string(), "true".to_string()),
                    ("io".to_string(), "not_performed".to_string()),
                ],
            );
            ExitCode::SUCCESS
        }
        Some("vortex-dtype-mapping") => {
            let report = if shardloom_vortex::typed_vortex_dtype_mapping_available() {
                VortexDTypeMappingReport::implemented("vortex::DType")
            } else {
                VortexDTypeMappingReport::deferred_api_unclear()
            };
            emit(
                "vortex-dtype-mapping",
                format,
                CommandStatus::Success,
                "vortex dtype mapping".to_string(),
                report.to_human_text(),
                report.diagnostics.clone(),
                vec![
                    (
                        "fallback_execution_allowed".to_string(),
                        "false".to_string(),
                    ),
                    ("mode".to_string(), "vortex_dtype_mapping".to_string()),
                    (
                        "upstream_vortex_dependency".to_string(),
                        "linked".to_string(),
                    ),
                    ("actual_io".to_string(), "not_implemented".to_string()),
                    ("write_io".to_string(), "false".to_string()),
                    ("execution".to_string(), "not_performed".to_string()),
                    ("plan_only".to_string(), "true".to_string()),
                    (
                        "name_based_mapping_available".to_string(),
                        "true".to_string(),
                    ),
                    (
                        "typed_mapping_status".to_string(),
                        report.status.as_str().to_string(),
                    ),
                ],
            );
            ExitCode::SUCCESS
        }
        Some("vortex-encoding-layout-mapping") => {
            let report = VortexEncodingLayoutMappingReport::deferred_api_unclear();
            emit(
                "vortex-encoding-layout-mapping",
                format,
                CommandStatus::Success,
                "vortex encoding/layout mapping".to_string(),
                report.to_human_text(),
                report.diagnostics.clone(),
                vec![
                    (
                        "fallback_execution_allowed".to_string(),
                        "false".to_string(),
                    ),
                    (
                        "mode".to_string(),
                        "vortex_encoding_layout_mapping".to_string(),
                    ),
                    (
                        "upstream_vortex_dependency".to_string(),
                        "linked".to_string(),
                    ),
                    ("actual_io".to_string(), "not_implemented".to_string()),
                    ("write_io".to_string(), "false".to_string()),
                    ("execution".to_string(), "not_performed".to_string()),
                    ("plan_only".to_string(), "true".to_string()),
                    (
                        "name_based_mapping_available".to_string(),
                        "true".to_string(),
                    ),
                    (
                        "encoding_mapping_status".to_string(),
                        report.encoding_status.as_str().to_string(),
                    ),
                    (
                        "layout_mapping_status".to_string(),
                        report.layout_status.as_str().to_string(),
                    ),
                ],
            );
            ExitCode::SUCCESS
        }

        Some("vortex-statistics-mapping") => {
            let report = if shardloom_vortex::typed_vortex_statistics_mapping_available() {
                VortexStatisticsMappingReport::implemented("vortex::statistics::<public_api>")
            } else {
                VortexStatisticsMappingReport::deferred_api_unclear()
            };
            emit(
                "vortex-statistics-mapping",
                format,
                CommandStatus::Success,
                "vortex statistics mapping".to_string(),
                report.to_human_text(),
                report.diagnostics.clone(),
                vec![
                    (
                        "fallback_execution_allowed".to_string(),
                        "false".to_string(),
                    ),
                    ("mode".to_string(), "vortex_statistics_mapping".to_string()),
                    (
                        "upstream_vortex_dependency".to_string(),
                        "linked".to_string(),
                    ),
                    ("actual_io".to_string(), "not_implemented".to_string()),
                    ("write_io".to_string(), "false".to_string()),
                    ("execution".to_string(), "not_performed".to_string()),
                    ("plan_only".to_string(), "true".to_string()),
                    ("segment_stats_available".to_string(), "true".to_string()),
                    (
                        "statistics_mapping_status".to_string(),
                        report.status.as_str().to_string(),
                    ),
                ],
            );
            ExitCode::SUCCESS
        }
        Some("vortex-file-metadata-open") => {
            let Some(uri_arg) = args.next() else {
                eprintln!("usage: shardloom vortex-file-metadata-open <dataset_uri>");
                return ExitCode::from(2);
            };
            let uri = match DatasetUri::new(uri_arg) {
                Ok(uri) => uri,
                Err(err) => {
                    return emit_error(
                        "vortex-file-metadata-open",
                        format,
                        "vortex file metadata open failed",
                        &err,
                    );
                }
            };
            let request = VortexMetadataOpenRequest::metadata_only(uri);
            let report = match open_vortex_metadata_only(request) {
                Ok(report) => report,
                Err(err) => {
                    return emit_error(
                        "vortex-file-metadata-open",
                        format,
                        "vortex file metadata open failed",
                        &err,
                    );
                }
            };
            let status = if report.has_errors() {
                CommandStatus::Error
            } else {
                CommandStatus::Success
            };
            emit(
                "vortex-file-metadata-open",
                format,
                status,
                "vortex file metadata-only open".to_string(),
                report.to_human_text(),
                report.diagnostics.clone(),
                vec![
                    ("mode".to_string(), "vortex_file_metadata_open".to_string()),
                    ("metadata_only".to_string(), "true".to_string()),
                    (
                        "file_io_feature_enabled".to_string(),
                        vortex_file_io_feature_enabled().to_string(),
                    ),
                    (
                        "fallback_execution_allowed".to_string(),
                        "false".to_string(),
                    ),
                    ("data_io_performed".to_string(), "false".to_string()),
                    ("object_store_io_performed".to_string(), "false".to_string()),
                    ("write_io_performed".to_string(), "false".to_string()),
                    ("write_io".to_string(), "false".to_string()),
                    ("execution".to_string(), "not_performed".to_string()),
                    ("plan_only".to_string(), "true".to_string()),
                ],
            );
            if matches!(status, CommandStatus::Error) {
                ExitCode::from(2)
            } else {
                ExitCode::SUCCESS
            }
        }
        Some("vortex-metadata-summary") => {
            let Some(uri_text) = args.next() else {
                return emit_error(
                    "vortex-metadata-summary",
                    format,
                    "missing dataset uri",
                    &ShardLoomError::InvalidOperation(
                        "missing required argument: <dataset_uri>".to_string(),
                    ),
                );
            };
            let uri = match DatasetUri::new(uri_text) {
                Ok(uri) => uri,
                Err(error) => {
                    return emit_error(
                        "vortex-metadata-summary",
                        format,
                        "invalid dataset uri",
                        &ShardLoomError::InvalidOperation(format!("invalid dataset uri: {error}")),
                    );
                }
            };
            let probe = probe_vortex_metadata_only(uri)
                .unwrap_or_else(|_| VortexMetadataProbeReport::deferred_api_unclear());
            let report = summarize_vortex_metadata_probe(&probe);
            emit(
                "vortex-metadata-summary",
                format,
                if report.has_errors() {
                    CommandStatus::Unsupported
                } else {
                    CommandStatus::Success
                },
                "vortex metadata summary".to_string(),
                report.to_human_text(),
                report.diagnostics.clone(),
                vec![
                    (
                        "fallback_execution_allowed".to_string(),
                        "false".to_string(),
                    ),
                    ("mode".to_string(), "vortex_metadata_summary".to_string()),
                    (
                        "metadata_summary_plan_only".to_string(),
                        metadata_summary_is_plan_only(&report).to_string(),
                    ),
                    ("data_materialized".to_string(), "false".to_string()),
                    ("object_store_io".to_string(), "false".to_string()),
                    ("write_io".to_string(), "false".to_string()),
                    ("write_io".to_string(), "false".to_string()),
                    ("execution".to_string(), "not_performed".to_string()),
                    ("plan_only".to_string(), "true".to_string()),
                ],
            );
            if report.has_errors() {
                ExitCode::from(1)
            } else {
                ExitCode::SUCCESS
            }
        }

        Some("vortex-metadata-plan") => {
            let Some(uri_text) = args.next() else {
                return emit_error(
                    "vortex-metadata-plan",
                    format,
                    "missing dataset uri",
                    &ShardLoomError::InvalidOperation(
                        "missing required argument: <dataset_uri>".to_string(),
                    ),
                );
            };
            let uri = match DatasetUri::new(uri_text) {
                Ok(uri) => uri,
                Err(error) => {
                    return emit_error(
                        "vortex-metadata-plan",
                        format,
                        "invalid dataset uri",
                        &ShardLoomError::InvalidOperation(format!("invalid dataset uri: {error}")),
                    );
                }
            };
            let probe = probe_vortex_metadata_only(uri)
                .unwrap_or_else(|_| VortexMetadataProbeReport::deferred_api_unclear());
            let summary = summarize_vortex_metadata_probe(&probe);
            let report = match plan_from_vortex_metadata_summary(summary) {
                Ok(report) => report,
                Err(error) => {
                    return emit_error(
                        "vortex-metadata-plan",
                        format,
                        "vortex metadata plan failed",
                        &error,
                    );
                }
            };
            emit(
                "vortex-metadata-plan",
                format,
                if report.has_errors() {
                    CommandStatus::Unsupported
                } else {
                    CommandStatus::Success
                },
                "vortex metadata planning".to_string(),
                report.to_human_text(),
                report.diagnostics.clone(),
                vec![
                    (
                        "fallback_execution_allowed".to_string(),
                        "false".to_string(),
                    ),
                    ("mode".to_string(), "vortex_metadata_plan".to_string()),
                    ("metadata_only".to_string(), "true".to_string()),
                    ("plan_only".to_string(), report.is_plan_only().to_string()),
                    ("data_executed".to_string(), "false".to_string()),
                    ("data_materialized".to_string(), "false".to_string()),
                    ("object_store_io".to_string(), "false".to_string()),
                    ("write_io".to_string(), "false".to_string()),
                    ("write_io".to_string(), "false".to_string()),
                    ("execution".to_string(), "not_performed".to_string()),
                    ("plan_only".to_string(), "true".to_string()),
                    (
                        "side_effect_free".to_string(),
                        metadata_planning_is_side_effect_free(&report).to_string(),
                    ),
                ],
            );
            if report.has_errors() {
                ExitCode::from(1)
            } else {
                ExitCode::SUCCESS
            }
        }

        Some("vortex-pruning-plan") => {
            let Some(uri_arg) = args.next() else {
                return emit_error(
                    "vortex-pruning-plan",
                    format,
                    "vortex pruning plan failed",
                    &ShardLoomError::InvalidOperation("missing <dataset_uri> argument".to_string()),
                );
            };
            let uri = match DatasetUri::new(uri_arg) {
                Ok(uri) => uri,
                Err(error) => {
                    return emit_error(
                        "vortex-pruning-plan",
                        format,
                        "vortex pruning plan failed",
                        &error,
                    );
                }
            };
            let probe = match probe_vortex_metadata_only(uri) {
                Ok(p) => p,
                Err(error) => {
                    return emit_error(
                        "vortex-pruning-plan",
                        format,
                        "vortex pruning plan failed",
                        &error,
                    );
                }
            };
            let summary = summarize_vortex_metadata_probe(&probe);
            let planning = match plan_from_vortex_metadata_summary(summary) {
                Ok(p) => p,
                Err(error) => {
                    return emit_error(
                        "vortex-pruning-plan",
                        format,
                        "vortex pruning plan failed",
                        &error,
                    );
                }
            };
            let report = match plan_vortex_metadata_pruning(planning, None) {
                Ok(r) => r,
                Err(error) => {
                    return emit_error(
                        "vortex-pruning-plan",
                        format,
                        "vortex pruning plan failed",
                        &error,
                    );
                }
            };
            let text = report.to_human_text();
            let status = if report.has_errors() {
                CommandStatus::Error
            } else {
                CommandStatus::Success
            };
            emit(
                "vortex-pruning-plan",
                format,
                status,
                "vortex metadata pruning plan".to_string(),
                text,
                report.diagnostics.clone(),
                vec![
                    (
                        "fallback_execution_allowed".to_string(),
                        "false".to_string(),
                    ),
                    ("mode".to_string(), "vortex_pruning_plan".to_string()),
                    ("metadata_only".to_string(), "true".to_string()),
                    ("plan_only".to_string(), report.is_plan_only().to_string()),
                    (
                        "data_executed".to_string(),
                        report.data_executed.to_string(),
                    ),
                    (
                        "data_materialized".to_string(),
                        report.data_materialized.to_string(),
                    ),
                    (
                        "object_store_io".to_string(),
                        report.object_store_io.to_string(),
                    ),
                    ("write_io".to_string(), report.write_io.to_string()),
                    ("write_io".to_string(), "false".to_string()),
                    ("execution".to_string(), "not_performed".to_string()),
                    ("plan_only".to_string(), "true".to_string()),
                    (
                        "side_effect_free".to_string(),
                        metadata_pruning_is_side_effect_free(&report).to_string(),
                    ),
                ],
            );
            if report.has_errors() {
                ExitCode::from(1)
            } else {
                ExitCode::SUCCESS
            }
        }

        Some("vortex-metadata-probe") => {
            let Some(uri_text) = args.next() else {
                return emit_error(
                    "vortex-metadata-probe",
                    format,
                    "missing dataset uri",
                    &ShardLoomError::InvalidOperation(
                        "missing required argument: <dataset_uri>".to_string(),
                    ),
                );
            };
            let uri = match DatasetUri::new(uri_text) {
                Ok(uri) => uri,
                Err(error) => {
                    return emit_error(
                        "vortex-metadata-probe",
                        format,
                        "invalid dataset uri",
                        &ShardLoomError::InvalidOperation(format!("invalid dataset uri: {error}")),
                    );
                }
            };
            let report = probe_vortex_metadata_only(uri)
                .unwrap_or_else(|_| VortexMetadataProbeReport::deferred_api_unclear());
            emit(
                "vortex-metadata-probe",
                format,
                if report.has_errors() {
                    CommandStatus::Unsupported
                } else {
                    CommandStatus::Success
                },
                "vortex metadata-only probe".to_string(),
                report.to_human_text(),
                report.diagnostics.clone(),
                vec![
                    (
                        "fallback_execution_allowed".to_string(),
                        "false".to_string(),
                    ),
                    ("mode".to_string(), "vortex_metadata_probe".to_string()),
                    ("metadata_only".to_string(), "true".to_string()),
                    ("data_materialized".to_string(), "false".to_string()),
                    ("object_store_io".to_string(), "false".to_string()),
                    ("write_io".to_string(), "false".to_string()),
                    ("write_io".to_string(), "false".to_string()),
                    ("execution".to_string(), "not_performed".to_string()),
                    ("plan_only".to_string(), "true".to_string()),
                    (
                        "metadata_io_status".to_string(),
                        report.status.as_str().to_string(),
                    ),
                ],
            );
            if report.has_errors() {
                ExitCode::from(1)
            } else {
                ExitCode::SUCCESS
            }
        }
        Some("vortex-api-inventory") => {
            let report = VortexAdapterCapabilityReport::foundation();
            emit(
                "vortex-api-inventory",
                format,
                CommandStatus::Success,
                "vortex API inventory".to_string(),
                report.to_human_text(),
                report.diagnostics.clone(),
                vec![
                    (
                        "fallback_execution_allowed".to_string(),
                        "false".to_string(),
                    ),
                    ("mode".to_string(), "vortex_api_inventory".to_string()),
                    (
                        "upstream_vortex_dependency".to_string(),
                        "linked".to_string(),
                    ),
                    ("actual_io".to_string(), "not_implemented".to_string()),
                    ("write_io".to_string(), "false".to_string()),
                    ("execution".to_string(), "not_performed".to_string()),
                    ("plan_only".to_string(), "true".to_string()),
                ],
            );
            ExitCode::SUCCESS
        }
        Some("optimizer-plan") => {
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
        Some("estimate") => {
            let operation = args
                .next()
                .unwrap_or_else(|| "<unspecified operation>".to_string());
            let report = EstimateReport::unsupported(
                operation,
                "estimation",
                "Real estimation is not implemented yet.",
            );
            emit(
                "estimate",
                format,
                CommandStatus::Unsupported,
                "estimate plan".to_string(),
                report.to_human_text(),
                report.diagnostics.clone(),
                vec![("mode".to_string(), "plan_only".to_string())],
            );
            if report.has_errors() {
                ExitCode::from(1)
            } else {
                ExitCode::SUCCESS
            }
        }
        _ => {
            eprintln!("{}", cli_usage_line());
            ExitCode::from(2)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn explain_unsupported_returns_non_zero() {
        let code = run(vec!["explain".to_string(), "demo-op".to_string()]);
        assert_ne!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn estimate_unsupported_returns_non_zero() {
        let code = run(vec!["estimate".to_string(), "demo-op".to_string()]);
        assert_ne!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn optimizer_plan_returns_non_zero() {
        let code = run(vec!["optimizer-plan".to_string()]);
        assert_ne!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn manifest_plan_with_dataset_uri_returns_success() {
        let code = run(vec![
            "manifest-plan".to_string(),
            "file://tmp/test.vortex".to_string(),
        ]);
        assert_eq!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn incremental_plan_with_snapshot_id_returns_success() {
        let code = run(vec!["incremental-plan".to_string(), "snap-1".to_string()]);
        assert_eq!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn write_intent_with_target_uri_returns_non_zero() {
        let code = run(vec![
            "write-intent".to_string(),
            "file://tmp/out.vortex".to_string(),
        ]);
        assert_ne!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn scan_plan_missing_dataset_uri_returns_non_zero() {
        let code = run(vec!["scan-plan".to_string()]);
        assert_ne!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn sizing_plan_with_dataset_uri_returns_success() {
        let code = run(vec![
            "sizing-plan".to_string(),
            "file://tmp/test.vortex".to_string(),
            "--memory-gb".to_string(),
            "8".to_string(),
        ]);
        assert_eq!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn scan_plan_with_dataset_uri_returns_success() {
        let code = run(vec![
            "scan-plan".to_string(),
            "file://tmp/test.vortex".to_string(),
        ]);
        assert_eq!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn release_plan_returns_success() {
        let code = run(vec!["release-plan".to_string()]);
        assert_eq!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn security_plan_returns_success() {
        let code = run(vec!["security-plan".to_string()]);
        assert_eq!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn observability_plan_returns_success() {
        let code = run(vec!["observability-plan".to_string()]);
        assert_eq!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn table_compat_plan_with_iceberg_returns_success() {
        let code = run(vec!["table-compat-plan".to_string(), "iceberg".to_string()]);
        assert_eq!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn table_compat_plan_with_unknown_returns_non_zero() {
        let code = run(vec!["table-compat-plan".to_string(), "unknown".to_string()]);
        assert_ne!(code, ExitCode::SUCCESS);
    }
    #[test]
    fn translation_plan_with_vortex_uri_returns_success() {
        let code = run(vec![
            "translation-plan".to_string(),
            "file://tmp/out.vortex".to_string(),
        ]);
        assert_eq!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn translation_plan_with_unknown_uri_returns_non_zero() {
        let code = run(vec![
            "translation-plan".to_string(),
            "file://tmp/out.unknown".to_string(),
        ]);
        assert_ne!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn vortex_plan_with_vortex_uri_returns_success() {
        let code = run(vec![
            "vortex-plan".to_string(),
            "file://tmp/test.vortex".to_string(),
        ]);
        assert_eq!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn vortex_output_plan_with_vortex_uri_returns_success() {
        let code = run(vec![
            "vortex-output-plan".to_string(),
            "file://tmp/test.vortex".to_string(),
        ]);
        assert_eq!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn vortex_plan_with_non_vortex_uri_returns_non_zero() {
        let code = run(vec![
            "vortex-plan".to_string(),
            "file://tmp/test.parquet".to_string(),
        ]);
        assert_ne!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn vortex_readiness_returns_success() {
        let code = run(vec!["vortex-readiness".to_string()]);
        assert_eq!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn vortex_api_inventory_returns_success() {
        let code = run(vec!["vortex-api-inventory".to_string()]);
        assert_eq!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn vortex_dtype_mapping_returns_success() {
        let code = run(vec!["vortex-dtype-mapping".to_string()]);
        assert_eq!(code, ExitCode::SUCCESS);
    }
    #[test]
    fn vortex_encoding_layout_mapping_returns_success() {
        let code = run(vec!["vortex-encoding-layout-mapping".to_string()]);
        assert_eq!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn vortex_statistics_mapping_command_returns_success() {
        let code = run(vec!["vortex-statistics-mapping".to_string()]);
        assert_eq!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn vortex_metadata_probe_missing_uri_returns_non_zero() {
        let code = run(vec!["vortex-metadata-probe".to_string()]);
        assert_ne!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn vortex_metadata_probe_invalid_uri_returns_non_zero() {
        let code = run(vec!["vortex-metadata-probe".to_string(), "   ".to_string()]);
        assert_ne!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn vortex_metadata_summary_with_non_vortex_uri_returns_non_zero() {
        let code = run(vec![
            "vortex-metadata-summary".to_string(),
            "file://tmp/data.parquet".to_string(),
        ]);
        assert_ne!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn vortex_metadata_probe_with_non_vortex_uri_returns_non_zero() {
        let code = run(vec![
            "vortex-metadata-probe".to_string(),
            "file://tmp/data.parquet".to_string(),
        ]);
        assert_ne!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn input_plan_with_vortex_uri_returns_success() {
        let code = run(vec![
            "input-plan".to_string(),
            "file://tmp/data.vortex".to_string(),
        ]);
        assert_eq!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn vortex_input_plan_with_vortex_uri_returns_success() {
        let code = run(vec![
            "vortex-input-plan".to_string(),
            "file://tmp/data.vortex".to_string(),
        ]);
        assert_eq!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn vortex_input_plan_with_parquet_uri_returns_non_zero() {
        let code = run(vec![
            "vortex-input-plan".to_string(),
            "file://tmp/data.parquet".to_string(),
        ]);
        assert_ne!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn vortex_task_graph_with_vortex_uri_returns_success() {
        let code = run(vec![
            "vortex-task-graph".to_string(),
            "file://tmp/data.vortex".to_string(),
        ]);
        assert_eq!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn vortex_task_graph_with_parquet_uri_returns_non_zero() {
        let code = run(vec![
            "vortex-task-graph".to_string(),
            "file://tmp/data.parquet".to_string(),
        ]);
        assert_ne!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn input_plan_with_unknown_uri_returns_non_zero() {
        let code = run(vec![
            "input-plan".to_string(),
            "file://tmp/data.unknown".to_string(),
        ]);
        assert_ne!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn correctness_plan_returns_success() {
        let code = run(vec!["correctness-plan".to_string()]);
        assert_eq!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn detect_requested_output_format_preserves_json_for_trailing_format_flag() {
        let args = vec![
            "status".to_string(),
            "--format".to_string(),
            "json".to_string(),
            "--format".to_string(),
        ];
        assert_eq!(detect_requested_output_format(&args), OutputFormat::Json);
    }

    #[test]
    fn plan_ir_returns_success() {
        let code = run(vec!["plan-ir".to_string()]);
        assert_eq!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn plan_import_returns_non_zero_for_not_implemented() {
        let code = run(vec![
            "plan-import".to_string(),
            "substrait-like".to_string(),
            "fixture".to_string(),
        ]);
        assert_ne!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn recovery_plan_returns_non_zero_for_not_implemented() {
        let code = run(vec!["recovery-plan".to_string()]);
        assert_ne!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn plan_export_returns_non_zero_for_not_implemented() {
        let code = run(vec!["plan-export".to_string(), "native".to_string()]);
        assert_ne!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn cli_contract_name_is_shardloom() {
        assert_eq!(cli_command_name(), "shardloom");
    }

    #[test]
    fn cli_contract_core_commands_dispatch_without_unknown_command_usage() {
        for command in [
            "status",
            "capabilities",
            "doctor",
            "release-plan",
            "optimizer-plan",
            "vortex-readiness",
        ] {
            let code = run(vec![command.to_string()]);
            assert_ne!(
                code,
                ExitCode::from(2),
                "command `{command}` should be recognized by dispatcher"
            );
        }
    }

    #[test]
    fn vortex_file_metadata_open_non_vortex_uri_returns_non_zero() {
        let code = run(vec![
            "vortex-file-metadata-open".to_string(),
            "file://tmp/not-vortex.parquet".to_string(),
        ]);
        assert_ne!(code, ExitCode::SUCCESS);
    }
}
