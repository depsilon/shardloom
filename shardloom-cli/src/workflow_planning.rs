//! Workflow, table, manifest, and stateful planning CLI handlers.
//!
//! These handlers emit report-only workflow planning surfaces. They do not read
//! datasets, probe catalogs, execute plans, write data, materialize outputs,
//! invoke external engines, or provide fallback execution.

use std::process::ExitCode;

use shardloom_core::{
    ByteRange, CapabilityCertificationReport, CatalogKind, CatalogMetadataIntegrationGateReport,
    CatalogRef, CdcEventKind, CdcEventSummary, CdcIncrementalPlanningReport, ChangeSet, ColumnRef,
    CommandStatus, CompactionPlanningPolicy, CompactionPlanningReport, DatasetFormat,
    DatasetManifest, DatasetRef, DatasetUri, DeleteModel, DeleteTombstoneCompatibilityReport,
    Diagnostic, DiagnosticCode, EncodedSegment, EncodingKind, FieldId, FieldName, FieldPath,
    FileDescriptor, FileRole, IncrementalPlanSkeleton, LayoutHealthPolicy, LayoutHealthReport,
    LayoutKind, LogicalDType, ManifestId, ManifestSegment, Nullability, OutputFormat, OutputTarget,
    PartitionEvolutionCompatibilityReport, PartitionField, PartitionSpec, PartitionTransform,
    SchemaDefinition, SchemaEvolutionCompatibilityReport, SchemaEvolutionPolicy, SchemaField,
    SchemaId, SchemaVersion, SegmentChange, SegmentChangeKind, SegmentId, SegmentLayout,
    SegmentStats, ShardLoomError, SnapshotId, SnapshotRef, StatefulReusePromotionGateReport,
    StatefulReuseReport, TableCompatibilityPlan, TableCompatibilityReport, TableFormatKind,
    TableIntelligenceReport, WriteIntent, evaluate_cdc_incremental_planning,
    evaluate_compaction_planning, evaluate_delete_tombstone_compatibility, evaluate_layout_health,
    evaluate_partition_evolution_compatibility, evaluate_schema_evolution_compatibility,
    plan_catalog_metadata_integration_gate, plan_stateful_reuse,
    plan_stateful_reuse_promotion_gate,
};
use shardloom_plan::{
    ImportedPlanCapabilityGateReport, NativePlanDocument, NativePlanNode, NativePlanNodeKind,
    PlanBoundaryKind, PlanCapabilityKind, PlanCapabilityRequirement, PlanExportRequest, PlanId,
    PlanImportRequest, PlanInteropFormat, PlanLayer, PlanNodeId, PlanPortabilityReport,
    ScanPlanSkeleton, ScanRequest,
};

use crate::{
    cli_output::{emit, emit_error},
    cli_unknown_arg_error,
};

pub(crate) fn handle_manifest_plan(
    mut args: impl Iterator<Item = String>,
    format: OutputFormat,
) -> ExitCode {
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
    let snapshot = SnapshotRef::new(SnapshotId::new("snapshot-placeholder").expect("valid"));
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

pub(crate) fn handle_layout_health_plan(
    mut args: impl Iterator<Item = String>,
    format: OutputFormat,
) -> ExitCode {
    let scenario = args.next().unwrap_or_else(|| "healthy".to_string());
    if let Some(extra) = args.next() {
        return emit_error(
            "layout-health-plan",
            format,
            "layout health planning failed",
            &cli_unknown_arg_error("layout-health-plan", &extra),
        );
    }
    emit_layout_health_plan(format, &scenario)
}

pub(crate) fn handle_compaction_plan(
    mut args: impl Iterator<Item = String>,
    format: OutputFormat,
) -> ExitCode {
    let scenario = args.next().unwrap_or_else(|| "healthy".to_string());
    if let Some(extra) = args.next() {
        return emit_error(
            "compaction-plan",
            format,
            "compaction planning failed",
            &cli_unknown_arg_error("compaction-plan", &extra),
        );
    }
    emit_compaction_plan(format, &scenario)
}

pub(crate) fn handle_table_intelligence_plan(
    mut args: impl Iterator<Item = String>,
    format: OutputFormat,
) -> ExitCode {
    if let Some(extra) = args.next() {
        return emit_error(
            "table-intelligence-plan",
            format,
            "table intelligence planning failed",
            &cli_unknown_arg_error("table-intelligence-plan", &extra),
        );
    }
    emit_table_intelligence_plan(format)
}

pub(crate) fn handle_schema_plan(
    mut args: std::vec::IntoIter<String>,
    format: OutputFormat,
) -> ExitCode {
    match args.next().as_deref() {
        None => emit_schema_plan_skeleton(format),
        Some("evolution") => {
            let scenario = args.next().unwrap_or_else(|| "add-nullable".to_string());
            if let Some(extra) = args.next() {
                return emit_error(
                    "schema-plan",
                    format,
                    "schema evolution plan failed",
                    &cli_unknown_arg_error("schema-plan evolution", &extra),
                );
            }
            emit_schema_evolution_plan(format, &scenario)
        }
        Some(value) => emit_error(
            "schema-plan",
            format,
            "schema plan failed",
            &cli_unknown_arg_error("schema-plan", value),
        ),
    }
}

pub(crate) fn handle_catalog_plan(
    mut args: impl Iterator<Item = String>,
    format: OutputFormat,
) -> ExitCode {
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
        Ok(catalog) => catalog,
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

pub(crate) fn handle_workflow_unsupported_plan(
    mut args: impl Iterator<Item = String>,
    format: OutputFormat,
) -> ExitCode {
    let Some(operation_token) = args.next() else {
        return emit_error(
            "workflow-unsupported-plan",
            format,
            "workflow unsupported plan failed",
            &ShardLoomError::InvalidOperation(
                "usage: shardloom workflow-unsupported-plan <operation> [workflow_summary] [target_ref]"
                    .to_string(),
            ),
        );
    };
    let Some(operation) = workflow_unsupported_operation(&operation_token) else {
        return emit_error(
            "workflow-unsupported-plan",
            format,
            "workflow unsupported plan failed",
            &cli_unknown_arg_error("workflow-unsupported-plan", &operation_token),
        );
    };
    let workflow_summary = args
        .next()
        .unwrap_or_else(|| "unspecified_workflow".to_string());
    let target_ref = args.next().unwrap_or_else(|| "none".to_string());
    if let Some(extra) = args.next() {
        return emit_error(
            "workflow-unsupported-plan",
            format,
            "workflow unsupported plan failed",
            &cli_unknown_arg_error("workflow-unsupported-plan", &extra),
        );
    }
    let diagnostic = Diagnostic::unsupported(
        operation.diagnostic_code,
        operation.feature,
        format!(
            "{} is not implemented for native ShardLoom workflow execution yet.",
            operation.label
        ),
        Some(operation.suggested_next_action.to_string()),
    );
    let human_text = format!(
        "workflow unsupported operation\noperation: {}\nblocker: {}\nrequired evidence: {}\nexecution: not_performed\nfallback: disabled",
        operation.operation, operation.blocker_id, operation.required_evidence
    );
    emit(
        "workflow-unsupported-plan",
        format,
        CommandStatus::Unsupported,
        "workflow operation unsupported".to_string(),
        human_text,
        vec![diagnostic],
        workflow_unsupported_fields(operation, &workflow_summary, &target_ref),
    );
    ExitCode::from(1)
}

pub(crate) fn handle_plan_ir(format: OutputFormat) -> ExitCode {
    let plan_id = match PlanId::new("plan-placeholder") {
        Ok(v) => v,
        Err(error) => return emit_error("plan-ir", format, "invalid plan id", &error),
    };
    let mut document = NativePlanDocument::empty(plan_id);
    document.validate_skeleton();
    let report = PlanPortabilityReport::native_skeleton(&document);
    emit(
        "plan-ir",
        format,
        CommandStatus::Warning,
        "native plan ir skeleton".to_string(),
        format!("{}\n\n{}", document.to_human_text(), report.to_human_text()),
        report.diagnostics.clone(),
        plan_portability_fields(&report, "plan_ir"),
    );
    ExitCode::SUCCESS
}

pub(crate) fn handle_plan_import(
    mut args: impl Iterator<Item = String>,
    format: OutputFormat,
) -> ExitCode {
    let Some(format_raw) = args.next() else {
        eprintln!("usage: shardloom plan-import <format> <source_label>");
        return ExitCode::from(2);
    };
    let Some(source_label) = args.next() else {
        eprintln!("usage: shardloom plan-import <format> <source_label>");
        return ExitCode::from(2);
    };
    let format_kind = parse_plan_interop_format(&format_raw);
    let request = if format_kind == PlanInteropFormat::ShardLoomNative {
        match PlanImportRequest::from_native_serialized(source_label) {
            Ok(v) => v,
            Err(error) => {
                return emit_error("plan-import", format, "invalid import request", &error);
            }
        }
    } else {
        match PlanImportRequest::not_implemented(format_kind, source_label) {
            Ok(v) => v,
            Err(error) => {
                return emit_error("plan-import", format, "invalid import request", &error);
            }
        }
    };
    let report = PlanPortabilityReport::for_import_request(&request);
    let mut fields = plan_portability_fields(&report, "plan_import");
    if let Some(document) = &request.imported_document {
        let certification = CapabilityCertificationReport::contract_only();
        let gate = ImportedPlanCapabilityGateReport::for_import_request(&request, &certification);
        push_field(&mut fields, "imported_plan_id", document.id.as_str());
        push_count_field(
            &mut fields,
            "imported_plan_node_count",
            document.node_count(),
        );
        fields.extend(imported_plan_capability_gate_fields(&gate));
    }
    emit_plan_portability_report(
        "plan-import",
        "plan import",
        format!("{}\n\n{}", request.summary(), report.to_human_text()),
        &report,
        fields,
        format,
    )
}

pub(crate) fn handle_plan_export(
    mut args: impl Iterator<Item = String>,
    format: OutputFormat,
) -> ExitCode {
    let Some(format_raw) = args.next() else {
        eprintln!("usage: shardloom plan-export <format>");
        return ExitCode::from(2);
    };
    let format_kind = parse_plan_interop_format(&format_raw);
    let mut serialized_plan = None;
    let mut serialized_plan_node_count = None;
    let request = if format_kind == PlanInteropFormat::ShardLoomNative {
        let document = match native_plan_export_document() {
            Ok(document) => document,
            Err(error) => {
                return emit_error("plan-export", format, "invalid native plan", &error);
            }
        };
        serialized_plan_node_count = Some(document.node_count());
        let request = PlanExportRequest::serialized_native(&document);
        serialized_plan.clone_from(&request.serialized_document);
        request
    } else {
        PlanExportRequest::not_implemented(format_kind)
    };
    let report = PlanPortabilityReport::for_export_request(&request);
    let mut fields = plan_portability_fields(&report, "plan_export");
    if let Some(serialized_plan) = &serialized_plan {
        push_field(&mut fields, "serialized_plan", serialized_plan);
    }
    if let Some(node_count) = serialized_plan_node_count {
        push_count_field(&mut fields, "serialized_plan_node_count", node_count);
    }
    emit_plan_portability_report(
        "plan-export",
        "plan export",
        format!("{}\n\n{}", request.summary(), report.to_human_text()),
        &report,
        fields,
        format,
    )
}

pub(crate) fn handle_table_compat_plan(
    mut args: std::vec::IntoIter<String>,
    format: OutputFormat,
) -> ExitCode {
    match args.next().as_deref() {
        Some("aggregate") => {
            let scenario = args.next().unwrap_or_else(|| "compatible".to_string());
            if let Some(extra) = args.next() {
                return emit_error(
                    "table-compat-plan",
                    format,
                    "table compatibility aggregation failed",
                    &cli_unknown_arg_error("table-compat-plan aggregate", &extra),
                );
            }
            emit_table_compatibility_aggregation(format, &scenario)
        }
        Some("partition-evolution") => {
            let scenario = args.next().unwrap_or_else(|| "add-field".to_string());
            if let Some(extra) = args.next() {
                return emit_error(
                    "table-compat-plan",
                    format,
                    "partition evolution plan failed",
                    &cli_unknown_arg_error("table-compat-plan partition-evolution", &extra),
                );
            }
            emit_partition_evolution_plan(format, &scenario)
        }
        Some("delete-semantics") => {
            let scenario = args.next().unwrap_or_else(|| "file-level".to_string());
            if let Some(extra) = args.next() {
                return emit_error(
                    "table-compat-plan",
                    format,
                    "delete/tombstone plan failed",
                    &cli_unknown_arg_error("table-compat-plan delete-semantics", &extra),
                );
            }
            emit_delete_tombstone_plan(format, &scenario)
        }
        maybe_format => emit_table_compat_plan(format, maybe_format),
    }
}

pub(crate) fn handle_catalog_metadata_gate(
    mut args: impl Iterator<Item = String>,
    format: OutputFormat,
) -> ExitCode {
    if let Some(extra) = args.next() {
        return emit_error(
            "cg9-catalog-metadata-gate",
            format,
            "CG-9 catalog metadata gate failed",
            &cli_unknown_arg_error("cg9-catalog-metadata-gate", &extra),
        );
    }
    let report = plan_catalog_metadata_integration_gate();
    emit(
        "cg9-catalog-metadata-gate",
        format,
        if report.has_errors() {
            CommandStatus::Unsupported
        } else {
            CommandStatus::Success
        },
        "CG-9 catalog metadata integration gate".to_string(),
        report.to_human_text(),
        report.diagnostics.clone(),
        catalog_metadata_integration_gate_fields(&report),
    );
    if report.has_errors() {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

pub(crate) fn handle_incremental_plan(
    mut args: impl Iterator<Item = String>,
    format: OutputFormat,
) -> ExitCode {
    let Some(snapshot_id) = args.next() else {
        eprintln!("usage: shardloom incremental-plan <snapshot_id>|cdc <scenario>");
        return ExitCode::from(2);
    };
    if snapshot_id == "cdc" {
        if let Some(scenario) = args.next() {
            if let Some(extra) = args.next() {
                return emit_error(
                    "incremental-plan",
                    format,
                    "CDC incremental plan failed",
                    &cli_unknown_arg_error("incremental-plan cdc", &extra),
                );
            }
            return emit_cdc_incremental_plan(format, &scenario);
        }
    } else if let Some(extra) = args.next() {
        return emit_error(
            "incremental-plan",
            format,
            "incremental plan failed",
            &cli_unknown_arg_error("incremental-plan", &extra),
        );
    }
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

pub(crate) fn handle_stateful_reuse_plan(format: OutputFormat) -> ExitCode {
    let report = plan_stateful_reuse();
    emit(
        "stateful-reuse-plan",
        format,
        if report.has_errors() {
            CommandStatus::Unsupported
        } else {
            CommandStatus::Success
        },
        "stateful reuse plan".to_string(),
        report.to_human_text(),
        report.diagnostics.clone(),
        stateful_reuse_fields(&report),
    );
    if report.has_errors() {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

pub(crate) fn handle_stateful_reuse_gate(format: OutputFormat) -> ExitCode {
    let report = plan_stateful_reuse_promotion_gate();
    emit(
        "cg17-stateful-reuse-gate",
        format,
        if report.has_errors() {
            CommandStatus::Unsupported
        } else {
            CommandStatus::Success
        },
        "CG-17 stateful reuse promotion gate".to_string(),
        report.to_human_text(),
        report.diagnostics.clone(),
        stateful_reuse_promotion_gate_fields(&report),
    );
    if report.has_errors() {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

pub(crate) fn handle_write_intent(
    mut args: impl Iterator<Item = String>,
    format: OutputFormat,
) -> ExitCode {
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

pub(crate) fn handle_scan_plan(
    mut args: impl Iterator<Item = String>,
    format: OutputFormat,
) -> ExitCode {
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

fn emit_plan_portability_report(
    command: &str,
    summary: &str,
    human_text: String,
    report: &PlanPortabilityReport,
    fields: Vec<(String, String)>,
    format: OutputFormat,
) -> ExitCode {
    emit(
        command,
        format,
        if report.has_errors() {
            CommandStatus::Unsupported
        } else {
            CommandStatus::Success
        },
        summary.to_string(),
        human_text,
        report.diagnostics.clone(),
        fields,
    );
    if report.has_errors() {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

fn push_field(fields: &mut Vec<(String, String)>, key: &str, value: &str) {
    fields.push((key.to_string(), value.to_string()));
}

fn push_count_field(fields: &mut Vec<(String, String)>, key: &str, value: usize) {
    push_field(fields, key, &value.to_string());
}

fn push_bool_field(fields: &mut Vec<(String, String)>, key: &str, value: bool) {
    push_field(fields, key, &value.to_string());
}

#[derive(Debug, Clone, Copy)]
struct WorkflowUnsupportedOperation {
    operation: &'static str,
    label: &'static str,
    surface: &'static str,
    feature: &'static str,
    blocker_id: &'static str,
    required_evidence: &'static str,
    suggested_next_action: &'static str,
    diagnostic_code: DiagnosticCode,
    materialization_required: bool,
    write_required: bool,
    runtime_required: bool,
}

fn workflow_unsupported_operation(token: &str) -> Option<WorkflowUnsupportedOperation> {
    let normalized = token.trim().to_ascii_lowercase().replace('_', "-");
    match normalized.as_str() {
        "profile" => Some(workflow_unsupported_profile()),
        "collect" => Some(workflow_unsupported_collect()),
        "to-pandas" => Some(workflow_unsupported_to_pandas()),
        "to-arrow" => Some(workflow_unsupported_to_arrow()),
        "write-vortex" => Some(workflow_unsupported_write_vortex()),
        "write-parquet" => Some(workflow_unsupported_write_parquet()),
        "sql" => Some(workflow_unsupported_sql()),
        "join" => Some(workflow_unsupported_join()),
        "aggregate" | "aggregation" | "group-by" | "groupby" => {
            Some(workflow_unsupported_aggregate())
        }
        "window" | "windows" => Some(workflow_unsupported_window()),
        "schema-contract" | "schema" => Some(workflow_unsupported_schema_contract()),
        "data-quality" | "data-quality-check" | "quality" => {
            Some(workflow_unsupported_data_quality())
        }
        _ => None,
    }
}

fn workflow_unsupported_profile() -> WorkflowUnsupportedOperation {
    WorkflowUnsupportedOperation {
        operation: "profile",
        label: "workflow profile collection",
        surface: "profile",
        feature: "cg21.workflow.profile",
        blocker_id: "cg21.workflow.profile.runtime_profile_unsupported",
        required_evidence: "runtime_profile_schema,observability_schema_coverage,workload_certificate",
        suggested_next_action: "Use profile-plan for report-only profiling posture until native workflow profile collection is certified.",
        diagnostic_code: DiagnosticCode::NotImplemented,
        materialization_required: false,
        write_required: false,
        runtime_required: true,
    }
}

fn workflow_unsupported_collect() -> WorkflowUnsupportedOperation {
    WorkflowUnsupportedOperation {
        operation: "collect",
        label: "workflow result collection",
        surface: "materialization",
        feature: "cg21.workflow.collect",
        blocker_id: "cg21.workflow.collect.materialization_unsupported",
        required_evidence: "execution_certificate,native_io_certificate,result_materialization_policy",
        suggested_next_action: "Use explain, estimate, certify, or an explicit certified local primitive path instead of collect.",
        diagnostic_code: DiagnosticCode::MaterializationRequired,
        materialization_required: true,
        write_required: false,
        runtime_required: true,
    }
}

fn workflow_unsupported_to_pandas() -> WorkflowUnsupportedOperation {
    WorkflowUnsupportedOperation {
        operation: "to_pandas",
        label: "pandas materialization",
        surface: "decoded_dataframe_materialization",
        feature: "cg21.workflow.to_pandas",
        blocker_id: "cg21.workflow.to_pandas.decoded_dataframe_unsupported",
        required_evidence: "decoded_columnar_boundary,native_io_certificate,materialization_policy",
        suggested_next_action: "Request a native result artifact or Arrow boundary report before converting to pandas.",
        diagnostic_code: DiagnosticCode::MaterializationRequired,
        materialization_required: true,
        write_required: false,
        runtime_required: true,
    }
}

fn workflow_unsupported_to_arrow() -> WorkflowUnsupportedOperation {
    WorkflowUnsupportedOperation {
        operation: "to_arrow",
        label: "Arrow materialization",
        surface: "decoded_columnar_materialization",
        feature: "cg21.workflow.to_arrow",
        blocker_id: "cg21.workflow.to_arrow.decoded_columnar_unsupported",
        required_evidence: "decoded_columnar_boundary,native_io_certificate,adapter_fidelity_report",
        suggested_next_action: "Use native Vortex artifact planning until Arrow IPC materialization is certified.",
        diagnostic_code: DiagnosticCode::MaterializationRequired,
        materialization_required: true,
        write_required: false,
        runtime_required: true,
    }
}

fn workflow_unsupported_write_vortex() -> WorkflowUnsupportedOperation {
    WorkflowUnsupportedOperation {
        operation: "write_vortex",
        label: "native Vortex workflow write",
        surface: "native_output_write",
        feature: "cg21.workflow.write_vortex",
        blocker_id: "cg21.workflow.write_vortex.write_policy_unsupported",
        required_evidence: "write_intent,staged_manifest,commit_protocol,recovery_certificate",
        suggested_next_action: "Use write-intent and staged-output readiness reports before enabling explicit Vortex writes.",
        diagnostic_code: DiagnosticCode::UnsupportedEffect,
        materialization_required: true,
        write_required: true,
        runtime_required: true,
    }
}

fn workflow_unsupported_write_parquet() -> WorkflowUnsupportedOperation {
    WorkflowUnsupportedOperation {
        operation: "write_parquet",
        label: "Parquet compatibility export",
        surface: "compatibility_export_write",
        feature: "cg21.workflow.write_parquet",
        blocker_id: "cg21.workflow.write_parquet.compatibility_export_unsupported",
        required_evidence: "translation_fidelity_report,decoded_columnar_boundary,write_intent",
        suggested_next_action: "Use plan-export for compatibility-export posture without writing an artifact.",
        diagnostic_code: DiagnosticCode::UnsupportedOutputFormat,
        materialization_required: true,
        write_required: true,
        runtime_required: true,
    }
}

fn workflow_unsupported_sql() -> WorkflowUnsupportedOperation {
    WorkflowUnsupportedOperation {
        operation: "sql",
        label: "SQL workflow execution",
        surface: "sql_frontend",
        feature: "cg21.workflow.sql",
        blocker_id: "cg21.workflow.sql.frontend_unsupported",
        required_evidence: "sql_parser,binder,semantic_profile,operator_capability_matrix",
        suggested_next_action: "Use capability discovery for SQL posture and keep SQL text in plan-only diagnostics.",
        diagnostic_code: DiagnosticCode::UnsupportedSql,
        materialization_required: false,
        write_required: false,
        runtime_required: true,
    }
}

fn workflow_unsupported_join() -> WorkflowUnsupportedOperation {
    WorkflowUnsupportedOperation {
        operation: "join",
        label: "DataFrame join workflow",
        surface: "dataframe_join",
        feature: "cg21.workflow.join",
        blocker_id: "cg21.workflow.join.operator_unsupported",
        required_evidence: "join_operator_capability,memory_spill_declaration,correctness_fixture",
        suggested_next_action: "Use capabilities operators and benchmark/correctness reports before relying on joins.",
        diagnostic_code: DiagnosticCode::UnsupportedSql,
        materialization_required: false,
        write_required: false,
        runtime_required: true,
    }
}

fn workflow_unsupported_aggregate() -> WorkflowUnsupportedOperation {
    WorkflowUnsupportedOperation {
        operation: "aggregate",
        label: "DataFrame aggregation workflow",
        surface: "dataframe_aggregation",
        feature: "cg21.workflow.aggregate",
        blocker_id: "cg21.workflow.aggregate.operator_unsupported",
        required_evidence: "aggregate_operator_capability,memory_spill_declaration,correctness_fixture",
        suggested_next_action: "Use capabilities operators and benchmark/correctness reports before relying on aggregations.",
        diagnostic_code: DiagnosticCode::UnsupportedSql,
        materialization_required: false,
        write_required: false,
        runtime_required: true,
    }
}

fn workflow_unsupported_window() -> WorkflowUnsupportedOperation {
    WorkflowUnsupportedOperation {
        operation: "window",
        label: "DataFrame window workflow",
        surface: "dataframe_window",
        feature: "cg21.workflow.window",
        blocker_id: "cg21.workflow.window.operator_unsupported",
        required_evidence: "window_operator_capability,sort_capability,correctness_fixture",
        suggested_next_action: "Use capabilities operators and correctness reports before relying on window functions.",
        diagnostic_code: DiagnosticCode::UnsupportedSql,
        materialization_required: false,
        write_required: false,
        runtime_required: true,
    }
}

fn workflow_unsupported_schema_contract() -> WorkflowUnsupportedOperation {
    WorkflowUnsupportedOperation {
        operation: "schema_contract",
        label: "workflow schema contract enforcement",
        surface: "schema_contract",
        feature: "cg21.workflow.schema_contract",
        blocker_id: "cg21.workflow.schema_contract.enforcement_unsupported",
        required_evidence: "schema_plan,table_compatibility_report,validation_certificate",
        suggested_next_action: "Use schema-plan for report-only schema posture before enforcing workflow schema contracts.",
        diagnostic_code: DiagnosticCode::NotImplemented,
        materialization_required: false,
        write_required: false,
        runtime_required: false,
    }
}

fn workflow_unsupported_data_quality() -> WorkflowUnsupportedOperation {
    WorkflowUnsupportedOperation {
        operation: "data_quality",
        label: "data-quality workflow checks",
        surface: "data_quality",
        feature: "cg21.workflow.data_quality",
        blocker_id: "cg21.workflow.data_quality.checks_unsupported",
        required_evidence: "quality_rule_contract,diagnostic_fixture,correctness_harness",
        suggested_next_action: "Use table-intelligence-plan and correctness-harness-plan until data-quality checks are certified.",
        diagnostic_code: DiagnosticCode::NotImplemented,
        materialization_required: false,
        write_required: false,
        runtime_required: false,
    }
}

fn workflow_unsupported_fields(
    operation: WorkflowUnsupportedOperation,
    workflow_summary: &str,
    target_ref: &str,
) -> Vec<(String, String)> {
    let mut fields = vec![];
    push_field(&mut fields, "mode", "workflow_unsupported_plan");
    push_field(
        &mut fields,
        "schema_version",
        "shardloom.workflow_unsupported.v1",
    );
    push_field(&mut fields, "report_id", "cg21.workflow.unsupported.parity");
    push_field(&mut fields, "workflow_operation", operation.operation);
    push_field(&mut fields, "workflow_surface", operation.surface);
    push_field(&mut fields, "workflow_summary", workflow_summary);
    push_field(&mut fields, "target_ref", target_ref);
    push_field(&mut fields, "blocker_id", operation.blocker_id);
    push_field(&mut fields, "severity", "error");
    push_field(&mut fields, "unsupported_status", "unsupported");
    push_field(
        &mut fields,
        "required_evidence",
        operation.required_evidence,
    );
    push_field(
        &mut fields,
        "suggested_next_action",
        operation.suggested_next_action,
    );
    push_bool_field(
        &mut fields,
        "materialization_required",
        operation.materialization_required,
    );
    push_bool_field(&mut fields, "write_required", operation.write_required);
    push_bool_field(&mut fields, "runtime_required", operation.runtime_required);
    push_bool_field(&mut fields, "plan_only", true);
    push_field(&mut fields, "execution", "not_performed");
    push_bool_field(&mut fields, "query_execution", false);
    push_bool_field(&mut fields, "runtime_execution", false);
    push_bool_field(&mut fields, "data_read", false);
    push_bool_field(&mut fields, "data_materialized", false);
    push_bool_field(&mut fields, "read_io", false);
    push_bool_field(&mut fields, "write_io", false);
    push_bool_field(&mut fields, "object_store_io", false);
    push_bool_field(&mut fields, "catalog_probe", false);
    push_bool_field(&mut fields, "network_probe", false);
    push_bool_field(&mut fields, "external_engine_invoked", false);
    push_bool_field(&mut fields, "external_effects_executed", false);
    push_bool_field(&mut fields, "fallback_execution_allowed", false);
    push_bool_field(&mut fields, "fallback_attempted", false);
    fields
}

pub(crate) fn emit_schema_plan_skeleton(format: OutputFormat) -> ExitCode {
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
            (
                "schema_evolution_report_emitted".to_string(),
                "false".to_string(),
            ),
            ("data_read".to_string(), "false".to_string()),
            ("write_io".to_string(), "false".to_string()),
            ("catalog_io".to_string(), "false".to_string()),
            ("object_store_io".to_string(), "false".to_string()),
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

pub(crate) fn emit_schema_evolution_plan(format: OutputFormat, scenario: &str) -> ExitCode {
    let (from, to, policy) = match schema_evolution_fixture(scenario) {
        Ok(parts) => parts,
        Err(error) => {
            return emit_error(
                "schema-plan",
                format,
                "schema evolution plan failed",
                &error,
            );
        }
    };
    let report = evaluate_schema_evolution_compatibility(&from, &to, &policy);
    let status = if report.has_errors() {
        CommandStatus::Unsupported
    } else {
        CommandStatus::Success
    };
    emit(
        "schema-plan",
        format,
        status,
        "schema evolution compatibility report".to_string(),
        report.to_human_text(),
        report.compatibility.diagnostics.clone(),
        schema_evolution_output_fields(&report, scenario),
    );
    if report.has_errors() {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

fn schema_evolution_output_fields(
    report: &SchemaEvolutionCompatibilityReport,
    scenario: &str,
) -> Vec<(String, String)> {
    vec![
        (
            "fallback_execution_allowed".to_string(),
            "false".to_string(),
        ),
        ("mode".to_string(), "schema_evolution_plan".to_string()),
        ("scenario".to_string(), scenario.to_string()),
        (
            "schema_evolution_report_emitted".to_string(),
            "true".to_string(),
        ),
        (
            "compatibility_level".to_string(),
            report.compatibility.level.as_str().to_string(),
        ),
        (
            "change_count".to_string(),
            report.compatibility.changes.len().to_string(),
        ),
        (
            "safe_change_count".to_string(),
            report.safe_change_count.to_string(),
        ),
        (
            "unsafe_change_count".to_string(),
            report.unsafe_change_count.to_string(),
        ),
        (
            "field_id_required_count".to_string(),
            report.field_id_required_count.to_string(),
        ),
        (
            "missing_field_id_count".to_string(),
            report.missing_field_id_count.to_string(),
        ),
        (
            "requires_projection".to_string(),
            report.requires_projection.to_string(),
        ),
        (
            "requires_cast".to_string(),
            report.requires_cast.to_string(),
        ),
        (
            "requires_default_values".to_string(),
            report.requires_default_values.to_string(),
        ),
        (
            "metadata_loss_reported".to_string(),
            report.metadata_loss_reported.to_string(),
        ),
        (
            "read_supported".to_string(),
            report.read_supported.to_string(),
        ),
        (
            "write_supported".to_string(),
            report.write_supported.to_string(),
        ),
        ("data_read".to_string(), report.data_read.to_string()),
        ("write_io".to_string(), report.write_io.to_string()),
        ("catalog_io".to_string(), report.catalog_io.to_string()),
        (
            "object_store_io".to_string(),
            report.object_store_io.to_string(),
        ),
        ("execution".to_string(), "not_performed".to_string()),
        ("plan_only".to_string(), "true".to_string()),
        (
            "table_formats_are".to_string(),
            "compatibility_targets_not_fallback_engines".to_string(),
        ),
    ]
}

fn schema_evolution_fixture(
    scenario: &str,
) -> Result<(SchemaDefinition, SchemaDefinition, SchemaEvolutionPolicy), ShardLoomError> {
    let policy = SchemaEvolutionPolicy::default_conservative();
    match scenario {
        "exact" => Ok((
            orders_schema_v1(true, LogicalDType::Int64)?,
            orders_schema_v1(true, LogicalDType::Int64)?,
            policy,
        )),
        "add-nullable" => Ok((
            orders_schema_v1(true, LogicalDType::Int64)?,
            orders_schema_with_extra_region()?,
            policy,
        )),
        "rename-with-id" => Ok((
            orders_schema_v1(true, LogicalDType::Int64)?,
            orders_schema_renamed_status(true)?,
            policy,
        )),
        "rename-without-id" => Ok((
            orders_schema_v1(false, LogicalDType::Int64)?,
            orders_schema_renamed_status(false)?,
            policy,
        )),
        "drop-field" => Ok((
            orders_schema_v1(true, LogicalDType::Int64)?,
            orders_schema_without_status()?,
            policy,
        )),
        "widen" => Ok((
            orders_schema_v1(true, LogicalDType::Int64)?,
            orders_schema_v1(true, LogicalDType::Float64)?,
            policy,
        )),
        "narrow" => Ok((
            orders_schema_v1(true, LogicalDType::Float64)?,
            orders_schema_v1(true, LogicalDType::Int64)?,
            policy,
        )),
        value => Err(cli_unknown_arg_error("schema-plan evolution", value)),
    }
}

fn table_compatibility_aggregation_fixture(
    scenario: &str,
) -> Result<(&'static str, &'static str, &'static str), ShardLoomError> {
    match scenario {
        "compatible" => Ok(("exact", "same", "none-to-file-level")),
        "schema-blocked" => Ok(("rename-without-id", "same", "none")),
        "partition-blocked" => Ok(("exact", "unknown-transform", "none")),
        "delete-blocked" => Ok(("exact", "same", "equality-delete")),
        value => Err(cli_unknown_arg_error("table-compat-plan aggregate", value)),
    }
}

fn orders_schema_v1(
    with_ids: bool,
    amount_dtype: LogicalDType,
) -> Result<SchemaDefinition, ShardLoomError> {
    let mut schema = SchemaDefinition::new(SchemaId::new("orders")?, SchemaVersion::new(1)?);
    schema.add_field(schema_fixture_field(
        with_ids,
        "f1",
        "order_id",
        LogicalDType::Int64,
        Nullability::NonNullable,
    )?);
    schema.add_field(schema_fixture_field(
        with_ids,
        "f2",
        "status",
        LogicalDType::Utf8,
        Nullability::Nullable,
    )?);
    schema.add_field(schema_fixture_field(
        with_ids,
        "f3",
        "amount",
        amount_dtype,
        Nullability::Nullable,
    )?);
    Ok(schema)
}

fn orders_schema_with_extra_region() -> Result<SchemaDefinition, ShardLoomError> {
    let mut schema = orders_schema_v1(true, LogicalDType::Int64)?;
    schema.version = SchemaVersion::new(2)?;
    schema.add_field(schema_fixture_field(
        true,
        "f4",
        "region",
        LogicalDType::Utf8,
        Nullability::Nullable,
    )?);
    Ok(schema)
}

fn orders_schema_renamed_status(with_ids: bool) -> Result<SchemaDefinition, ShardLoomError> {
    let mut schema = SchemaDefinition::new(SchemaId::new("orders")?, SchemaVersion::new(2)?);
    schema.add_field(schema_fixture_field(
        with_ids,
        "f1",
        "order_id",
        LogicalDType::Int64,
        Nullability::NonNullable,
    )?);
    schema.add_field(schema_fixture_field(
        with_ids,
        "f2",
        "order_status",
        LogicalDType::Utf8,
        Nullability::Nullable,
    )?);
    schema.add_field(schema_fixture_field(
        with_ids,
        "f3",
        "amount",
        LogicalDType::Int64,
        Nullability::Nullable,
    )?);
    Ok(schema)
}

fn orders_schema_without_status() -> Result<SchemaDefinition, ShardLoomError> {
    let mut schema = SchemaDefinition::new(SchemaId::new("orders")?, SchemaVersion::new(2)?);
    schema.add_field(schema_fixture_field(
        true,
        "f1",
        "order_id",
        LogicalDType::Int64,
        Nullability::NonNullable,
    )?);
    schema.add_field(schema_fixture_field(
        true,
        "f3",
        "amount",
        LogicalDType::Int64,
        Nullability::Nullable,
    )?);
    Ok(schema)
}

fn schema_fixture_field(
    with_id: bool,
    id: &str,
    name: &str,
    dtype: LogicalDType,
    nullability: Nullability,
) -> Result<SchemaField, ShardLoomError> {
    let field = SchemaField::new(FieldName::new(name)?, dtype, nullability);
    if with_id {
        Ok(field.with_id(FieldId::new(id)?))
    } else {
        Ok(field)
    }
}

pub(crate) fn emit_table_compat_plan(format: OutputFormat, format_token: Option<&str>) -> ExitCode {
    let format_kind = match format_token {
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

pub(crate) fn emit_table_compatibility_aggregation(
    format: OutputFormat,
    scenario: &str,
) -> ExitCode {
    let (schema_scenario, partition_scenario, delete_scenario) =
        match table_compatibility_aggregation_fixture(scenario) {
            Ok(parts) => parts,
            Err(error) => {
                return emit_error(
                    "table-compat-plan",
                    format,
                    "table compatibility aggregation failed",
                    &error,
                );
            }
        };
    let (from_schema, to_schema, policy) = match schema_evolution_fixture(schema_scenario) {
        Ok(parts) => parts,
        Err(error) => {
            return emit_error(
                "table-compat-plan",
                format,
                "table compatibility aggregation failed",
                &error,
            );
        }
    };
    let (from_spec, to_spec) = match partition_evolution_fixture(partition_scenario) {
        Ok(parts) => parts,
        Err(error) => {
            return emit_error(
                "table-compat-plan",
                format,
                "table compatibility aggregation failed",
                &error,
            );
        }
    };
    let (source_model, target_model) = match delete_tombstone_fixture(delete_scenario) {
        Ok(parts) => parts,
        Err(error) => {
            return emit_error(
                "table-compat-plan",
                format,
                "table compatibility aggregation failed",
                &error,
            );
        }
    };

    let schema_report = evaluate_schema_evolution_compatibility(&from_schema, &to_schema, &policy);
    let partition_report = evaluate_partition_evolution_compatibility(&from_spec, &to_spec);
    let delete_report = evaluate_delete_tombstone_compatibility(source_model, target_model);
    let plan = TableCompatibilityPlan::native_vortex().with_delete_model(target_model);
    let report = TableCompatibilityReport::from_plan(plan)
        .with_schema_evolution_report(schema_report)
        .with_partition_evolution_report(partition_report)
        .with_delete_tombstone_report(delete_report);
    let status = if report.has_errors() {
        CommandStatus::Unsupported
    } else {
        CommandStatus::Success
    };
    let diagnostics = table_compatibility_aggregation_diagnostics(&report);

    emit(
        "table-compat-plan",
        format,
        status,
        "table compatibility aggregation report".to_string(),
        report.to_human_text(),
        diagnostics,
        table_compatibility_aggregation_output_fields(
            &report,
            scenario,
            schema_scenario,
            partition_scenario,
            delete_scenario,
        ),
    );
    if report.has_errors() {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

fn table_compatibility_aggregation_output_fields(
    report: &TableCompatibilityReport,
    scenario: &str,
    schema_scenario: &str,
    partition_scenario: &str,
    delete_scenario: &str,
) -> Vec<(String, String)> {
    let mut fields = vec![
        (
            "fallback_execution_allowed".to_string(),
            "false".to_string(),
        ),
        (
            "mode".to_string(),
            "table_compatibility_aggregation".to_string(),
        ),
        ("scenario".to_string(), scenario.to_string()),
        ("schema_scenario".to_string(), schema_scenario.to_string()),
        (
            "partition_scenario".to_string(),
            partition_scenario.to_string(),
        ),
        ("delete_scenario".to_string(), delete_scenario.to_string()),
        (
            "table_compatibility_report_emitted".to_string(),
            "true".to_string(),
        ),
        (
            "evidence_report_count".to_string(),
            report.evidence_report_count().to_string(),
        ),
        (
            "read_supported".to_string(),
            report.read_supported().to_string(),
        ),
        (
            "write_supported".to_string(),
            report.write_supported().to_string(),
        ),
        (
            "side_effect_free".to_string(),
            report.side_effect_free().to_string(),
        ),
        ("data_read".to_string(), report.data_read.to_string()),
        ("write_io".to_string(), report.write_io.to_string()),
        ("catalog_io".to_string(), report.catalog_io.to_string()),
        (
            "object_store_io".to_string(),
            report.object_store_io.to_string(),
        ),
        ("execution".to_string(), "not_performed".to_string()),
        ("plan_only".to_string(), "true".to_string()),
        (
            "table_formats_are".to_string(),
            "compatibility_targets_not_fallback_engines".to_string(),
        ),
    ];
    if let Some(schema_report) = &report.schema_evolution_report {
        fields.push((
            "schema_evolution_report_emitted".to_string(),
            "true".to_string(),
        ));
        fields.push((
            "schema_compatibility_level".to_string(),
            schema_report.compatibility.level.as_str().to_string(),
        ));
        fields.push((
            "schema_unsafe_change_count".to_string(),
            schema_report.unsafe_change_count.to_string(),
        ));
    }
    if let Some(partition_report) = &report.partition_evolution_report {
        fields.push((
            "partition_evolution_report_emitted".to_string(),
            "true".to_string(),
        ));
        fields.push((
            "partition_compatibility_level".to_string(),
            partition_report.level.as_str().to_string(),
        ));
        fields.push((
            "partition_unsafe_change_count".to_string(),
            partition_report.unsafe_change_count.to_string(),
        ));
    }
    if let Some(delete_report) = &report.delete_tombstone_report {
        fields.push((
            "delete_tombstone_report_emitted".to_string(),
            "true".to_string(),
        ));
        fields.push((
            "delete_compatibility_level".to_string(),
            delete_report.level.as_str().to_string(),
        ));
        fields.push((
            "delete_unsafe_change_count".to_string(),
            delete_report.unsafe_change_count.to_string(),
        ));
    }
    fields
}

fn table_compatibility_aggregation_diagnostics(
    report: &TableCompatibilityReport,
) -> Vec<Diagnostic> {
    let mut diagnostics = report.plan.diagnostics.clone();
    if let Some(schema_report) = &report.schema_report {
        diagnostics.extend(schema_report.diagnostics.clone());
    }
    if let Some(schema_evolution_report) = &report.schema_evolution_report {
        diagnostics.extend(schema_evolution_report.compatibility.diagnostics.clone());
    }
    if let Some(partition_evolution_report) = &report.partition_evolution_report {
        diagnostics.extend(partition_evolution_report.diagnostics.clone());
    }
    if let Some(delete_tombstone_report) = &report.delete_tombstone_report {
        diagnostics.extend(delete_tombstone_report.diagnostics.clone());
    }
    diagnostics.extend(report.diagnostics.clone());
    diagnostics
}

pub(crate) fn emit_partition_evolution_plan(format: OutputFormat, scenario: &str) -> ExitCode {
    let (from_spec, to_spec) = match partition_evolution_fixture(scenario) {
        Ok(parts) => parts,
        Err(error) => {
            return emit_error(
                "table-compat-plan",
                format,
                "partition evolution plan failed",
                &error,
            );
        }
    };
    let report = evaluate_partition_evolution_compatibility(&from_spec, &to_spec);
    let status = if report.has_errors() {
        CommandStatus::Unsupported
    } else {
        CommandStatus::Success
    };
    emit(
        "table-compat-plan",
        format,
        status,
        "partition evolution compatibility report".to_string(),
        report.to_human_text(),
        report.diagnostics.clone(),
        partition_evolution_output_fields(&report, scenario),
    );
    if report.has_errors() {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

fn partition_evolution_output_fields(
    report: &PartitionEvolutionCompatibilityReport,
    scenario: &str,
) -> Vec<(String, String)> {
    vec![
        (
            "fallback_execution_allowed".to_string(),
            "false".to_string(),
        ),
        ("mode".to_string(), "partition_evolution_plan".to_string()),
        ("scenario".to_string(), scenario.to_string()),
        (
            "partition_evolution_report_emitted".to_string(),
            "true".to_string(),
        ),
        (
            "compatibility_level".to_string(),
            report.level.as_str().to_string(),
        ),
        ("change_count".to_string(), report.changes.len().to_string()),
        (
            "preserved_field_count".to_string(),
            report.preserved_field_count.to_string(),
        ),
        (
            "added_field_count".to_string(),
            report.added_field_count.to_string(),
        ),
        (
            "dropped_field_count".to_string(),
            report.dropped_field_count.to_string(),
        ),
        (
            "transform_change_count".to_string(),
            report.transform_change_count.to_string(),
        ),
        (
            "reorder_count".to_string(),
            report.reorder_count.to_string(),
        ),
        (
            "unsafe_change_count".to_string(),
            report.unsafe_change_count.to_string(),
        ),
        (
            "requires_partition_router".to_string(),
            report.requires_partition_router.to_string(),
        ),
        (
            "requires_metadata_rewrite".to_string(),
            report.requires_metadata_rewrite.to_string(),
        ),
        (
            "requires_repartition".to_string(),
            report.requires_repartition.to_string(),
        ),
        (
            "read_supported".to_string(),
            report.read_supported.to_string(),
        ),
        (
            "write_supported".to_string(),
            report.write_supported.to_string(),
        ),
        ("data_read".to_string(), report.data_read.to_string()),
        ("write_io".to_string(), report.write_io.to_string()),
        ("catalog_io".to_string(), report.catalog_io.to_string()),
        (
            "object_store_io".to_string(),
            report.object_store_io.to_string(),
        ),
        ("execution".to_string(), "not_performed".to_string()),
        ("plan_only".to_string(), "true".to_string()),
        (
            "table_formats_are".to_string(),
            "compatibility_targets_not_fallback_engines".to_string(),
        ),
    ]
}

pub(crate) fn emit_delete_tombstone_plan(format: OutputFormat, scenario: &str) -> ExitCode {
    let (source_model, target_model) = match delete_tombstone_fixture(scenario) {
        Ok(models) => models,
        Err(error) => {
            return emit_error(
                "table-compat-plan",
                format,
                "delete/tombstone plan failed",
                &error,
            );
        }
    };
    let report = evaluate_delete_tombstone_compatibility(source_model, target_model);
    let status = if report.has_errors() {
        CommandStatus::Unsupported
    } else {
        CommandStatus::Success
    };
    emit(
        "table-compat-plan",
        format,
        status,
        "delete/tombstone compatibility report".to_string(),
        report.to_human_text(),
        report.diagnostics.clone(),
        delete_tombstone_output_fields(&report, scenario),
    );
    if report.has_errors() {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

fn delete_tombstone_output_fields(
    report: &DeleteTombstoneCompatibilityReport,
    scenario: &str,
) -> Vec<(String, String)> {
    vec![
        (
            "fallback_execution_allowed".to_string(),
            "false".to_string(),
        ),
        ("mode".to_string(), "delete_tombstone_plan".to_string()),
        ("scenario".to_string(), scenario.to_string()),
        (
            "delete_tombstone_report_emitted".to_string(),
            "true".to_string(),
        ),
        (
            "compatibility_level".to_string(),
            report.level.as_str().to_string(),
        ),
        (
            "source_delete_model".to_string(),
            report.source_model.as_str().to_string(),
        ),
        (
            "target_delete_model".to_string(),
            report.target_model.as_str().to_string(),
        ),
        (
            "delete_semantics_preserved".to_string(),
            report.delete_semantics_preserved.to_string(),
        ),
        (
            "tombstone_semantics_preserved".to_string(),
            report.tombstone_semantics_preserved.to_string(),
        ),
        (
            "requires_explicit_delete_handling".to_string(),
            report.requires_explicit_delete_handling.to_string(),
        ),
        (
            "requires_file_delete_filter".to_string(),
            report.requires_file_delete_filter.to_string(),
        ),
        (
            "requires_tombstone_filter".to_string(),
            report.requires_tombstone_filter.to_string(),
        ),
        (
            "requires_row_identity".to_string(),
            report.requires_row_identity.to_string(),
        ),
        (
            "requires_position_identity".to_string(),
            report.requires_position_identity.to_string(),
        ),
        (
            "requires_equality_predicate".to_string(),
            report.requires_equality_predicate.to_string(),
        ),
        (
            "requires_external_table_metadata".to_string(),
            report.requires_external_table_metadata.to_string(),
        ),
        (
            "metadata_loss_reported".to_string(),
            report.metadata_loss_reported.to_string(),
        ),
        (
            "unsupported_model_count".to_string(),
            report.unsupported_model_count.to_string(),
        ),
        (
            "unsafe_change_count".to_string(),
            report.unsafe_change_count.to_string(),
        ),
        (
            "read_supported".to_string(),
            report.read_supported.to_string(),
        ),
        (
            "write_supported".to_string(),
            report.write_supported.to_string(),
        ),
        ("data_read".to_string(), report.data_read.to_string()),
        ("write_io".to_string(), report.write_io.to_string()),
        ("catalog_io".to_string(), report.catalog_io.to_string()),
        (
            "object_store_io".to_string(),
            report.object_store_io.to_string(),
        ),
        ("execution".to_string(), "not_performed".to_string()),
        ("plan_only".to_string(), "true".to_string()),
        (
            "table_formats_are".to_string(),
            "compatibility_targets_not_fallback_engines".to_string(),
        ),
    ]
}

fn delete_tombstone_fixture(scenario: &str) -> Result<(DeleteModel, DeleteModel), ShardLoomError> {
    match scenario {
        "none" => Ok((DeleteModel::None, DeleteModel::None)),
        "file-level" => Ok((DeleteModel::FileLevelDelete, DeleteModel::FileLevelDelete)),
        "none-to-file-level" => Ok((DeleteModel::None, DeleteModel::FileLevelDelete)),
        "file-to-none" => Ok((DeleteModel::FileLevelDelete, DeleteModel::None)),
        "segment-tombstone" => Ok((
            DeleteModel::SegmentLevelTombstone,
            DeleteModel::SegmentLevelTombstone,
        )),
        "row-level" => Ok((DeleteModel::RowLevelDelete, DeleteModel::RowLevelDelete)),
        "position-delete" => Ok((DeleteModel::PositionDelete, DeleteModel::PositionDelete)),
        "equality-delete" => Ok((DeleteModel::EqualityDelete, DeleteModel::EqualityDelete)),
        "external-table-metadata" => Ok((
            DeleteModel::ExternalTableMetadata,
            DeleteModel::ExternalTableMetadata,
        )),
        "unknown" => Ok((DeleteModel::Unknown, DeleteModel::Unknown)),
        value => Err(cli_unknown_arg_error(
            "table-compat-plan delete-semantics",
            value,
        )),
    }
}

fn partition_evolution_fixture(
    scenario: &str,
) -> Result<(PartitionSpec, PartitionSpec), ShardLoomError> {
    match scenario {
        "same" => {
            let spec = base_partition_spec()?;
            Ok((spec.clone(), spec))
        }
        "add-field" => Ok((base_partition_spec()?, added_partition_field_spec()?)),
        "change-transform" => Ok((base_partition_spec()?, changed_partition_transform_spec()?)),
        "drop-field" => Ok((added_partition_field_spec()?, base_partition_spec()?)),
        "reorder" => Ok((added_partition_field_spec()?, reordered_partition_spec()?)),
        "unknown-transform" => Ok((base_partition_spec()?, unknown_partition_transform_spec()?)),
        value => Err(cli_unknown_arg_error(
            "table-compat-plan partition-evolution",
            value,
        )),
    }
}

fn base_partition_spec() -> Result<PartitionSpec, ShardLoomError> {
    Ok(partition_spec_from_fields(vec![partition_fixture_field(
        "created_at",
        PartitionTransform::Day,
    )?]))
}

fn added_partition_field_spec() -> Result<PartitionSpec, ShardLoomError> {
    Ok(partition_spec_from_fields(vec![
        partition_fixture_field("created_at", PartitionTransform::Day)?,
        partition_fixture_field("customer_id", PartitionTransform::Bucket { buckets: 16 })?,
    ]))
}

fn changed_partition_transform_spec() -> Result<PartitionSpec, ShardLoomError> {
    Ok(partition_spec_from_fields(vec![partition_fixture_field(
        "created_at",
        PartitionTransform::Month,
    )?]))
}

fn reordered_partition_spec() -> Result<PartitionSpec, ShardLoomError> {
    Ok(partition_spec_from_fields(vec![
        partition_fixture_field("customer_id", PartitionTransform::Bucket { buckets: 16 })?,
        partition_fixture_field("created_at", PartitionTransform::Day)?,
    ]))
}

fn unknown_partition_transform_spec() -> Result<PartitionSpec, ShardLoomError> {
    Ok(partition_spec_from_fields(vec![partition_fixture_field(
        "created_at",
        PartitionTransform::Unknown("vendor_specific".to_string()),
    )?]))
}

fn partition_spec_from_fields(fields: Vec<PartitionField>) -> PartitionSpec {
    let mut spec = PartitionSpec::empty();
    for field in fields {
        spec.add_field(field);
    }
    spec
}

fn partition_fixture_field(
    source: &str,
    transform: PartitionTransform,
) -> Result<PartitionField, ShardLoomError> {
    Ok(PartitionField::new(
        FieldPath::from_dot_separated(source)?,
        transform,
    ))
}

#[must_use]
fn plan_portability_fields(report: &PlanPortabilityReport, mode: &str) -> Vec<(String, String)> {
    let mut fields = vec![];
    push_field(&mut fields, "mode", mode);
    push_field(&mut fields, "execution", "not_performed");
    push_field(&mut fields, "plan_only", "true");
    push_field(&mut fields, "schema_version", report.schema_version);
    push_field(&mut fields, "report_id", &report.report_id);
    push_field(&mut fields, "direction", report.direction.as_str());
    push_field(&mut fields, "portability_status", report.status.as_str());
    push_field(
        &mut fields,
        "interop_format",
        report.interop_format.as_str(),
    );
    push_field(
        &mut fields,
        "native_plan_schema_version",
        &report.native_plan_schema_version.summary(),
    );
    push_bool_field(&mut fields, "native_first", report.native_first);
    push_bool_field(&mut fields, "validation_only", report.validation_only);
    push_bool_field(
        &mut fields,
        "validation_required",
        report.validation_required,
    );
    push_bool_field(
        &mut fields,
        "capability_check_required",
        report.capability_check_required,
    );
    push_field(
        &mut fields,
        "supported_constructs",
        &report.supported_constructs.join(","),
    );
    push_field(
        &mut fields,
        "native_only_nodes",
        &report.native_only_nodes.join(","),
    );
    push_field(
        &mut fields,
        "substrait_like_representable_nodes",
        &report.substrait_like_representable_nodes.join(","),
    );
    push_field(&mut fields, "lossy_nodes", &report.lossy_nodes.join(","));
    push_field(
        &mut fields,
        "unsupported_nodes",
        &report.unsupported_nodes.join(","),
    );
    push_field(
        &mut fields,
        "residual_unsupported_constructs",
        &report.residual_unsupported_constructs.join(","),
    );
    push_field(
        &mut fields,
        "metadata_loss_boundaries",
        &report.metadata_loss_boundaries.join(","),
    );
    push_bool_field(
        &mut fields,
        "encoded_semantics_loss",
        report.encoded_semantics_loss,
    );
    push_bool_field(&mut fields, "redaction_required", report.redaction_required);
    push_bool_field(&mut fields, "parser_executed", report.parser_executed);
    push_bool_field(
        &mut fields,
        "import_export_serialization_performed",
        report.import_export_serialization_performed,
    );
    push_bool_field(&mut fields, "runtime_execution", report.runtime_execution);
    push_bool_field(
        &mut fields,
        "external_engine_execution",
        report.external_engine_execution,
    );
    push_bool_field(&mut fields, "filesystem_probe", report.filesystem_probe);
    push_bool_field(&mut fields, "network_probe", report.network_probe);
    push_bool_field(&mut fields, "catalog_probe", report.catalog_probe);
    push_bool_field(&mut fields, "adapter_probe", report.adapter_probe);
    push_bool_field(&mut fields, "read_io", report.read_io);
    push_bool_field(&mut fields, "write_io", report.write_io);
    push_bool_field(&mut fields, "side_effect_free", report.side_effect_free);
    push_bool_field(
        &mut fields,
        "fallback_execution_allowed",
        report.fallback_execution_allowed,
    );
    push_bool_field(&mut fields, "fallback_attempted", report.fallback_attempted);
    push_count_field(&mut fields, "diagnostic_count", report.diagnostics.len());
    fields
}

fn imported_plan_capability_gate_fields(
    report: &ImportedPlanCapabilityGateReport,
) -> Vec<(String, String)> {
    vec![
        (
            "imported_plan_capability_gate_schema_version".to_string(),
            report.schema_version.to_string(),
        ),
        (
            "imported_plan_capability_gate_status".to_string(),
            report.status.as_str().to_string(),
        ),
        (
            "imported_plan_capability_checked".to_string(),
            report.capability_checked.to_string(),
        ),
        (
            "imported_plan_execution_allowed".to_string(),
            report.execution_allowed.to_string(),
        ),
        (
            "imported_plan_required_capability_surfaces".to_string(),
            report.required_capability_surfaces.join(","),
        ),
        (
            "imported_plan_certified_capability_surfaces".to_string(),
            report.certified_capability_surfaces.join(","),
        ),
        (
            "imported_plan_missing_certification_surfaces".to_string(),
            report.missing_certification_surfaces.join(","),
        ),
        (
            "imported_plan_unsupported_node_count".to_string(),
            report.unsupported_node_count.to_string(),
        ),
        (
            "imported_plan_effect_boundary_count".to_string(),
            report.effect_boundary_count.to_string(),
        ),
        (
            "imported_plan_gate_runtime_execution".to_string(),
            report.runtime_execution.to_string(),
        ),
        (
            "imported_plan_gate_parser_executed".to_string(),
            report.parser_executed.to_string(),
        ),
        (
            "imported_plan_gate_filesystem_probe".to_string(),
            report.filesystem_probe.to_string(),
        ),
        (
            "imported_plan_gate_network_probe".to_string(),
            report.network_probe.to_string(),
        ),
        (
            "imported_plan_gate_catalog_probe".to_string(),
            report.catalog_probe.to_string(),
        ),
        (
            "imported_plan_gate_adapter_probe".to_string(),
            report.adapter_probe.to_string(),
        ),
        (
            "imported_plan_gate_external_engine_execution".to_string(),
            report.external_engine_execution.to_string(),
        ),
        (
            "imported_plan_gate_read_io".to_string(),
            report.read_io.to_string(),
        ),
        (
            "imported_plan_gate_write_io".to_string(),
            report.write_io.to_string(),
        ),
        (
            "imported_plan_gate_fallback_execution_allowed".to_string(),
            report.fallback_execution_allowed.to_string(),
        ),
        (
            "imported_plan_gate_fallback_attempted".to_string(),
            report.fallback_attempted.to_string(),
        ),
    ]
}

fn native_plan_export_document() -> Result<NativePlanDocument, ShardLoomError> {
    let mut document = NativePlanDocument::new(
        PlanId::new("plan-export-native-skeleton")?,
        PlanLayer::Logical,
    );
    let mut scan = NativePlanNode::new(
        PlanNodeId::new("scan_0")?,
        PlanLayer::Logical,
        NativePlanNodeKind::Scan,
        "native Vortex scan placeholder",
    );
    scan.add_capability(PlanCapabilityRequirement::required(
        PlanCapabilityKind::VortexNativeInput,
        "native serialization preserves ShardLoom plan capability requirements",
    ));
    scan.add_boundary(PlanBoundaryKind::NativeVortexInput);
    document.add_node(scan);
    document.validate_skeleton();
    Ok(document)
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
pub(crate) fn stateful_reuse_fields(report: &StatefulReuseReport) -> Vec<(String, String)> {
    let mut fields = vec![];
    append_stateful_reuse_identity_fields(&mut fields, report);
    append_stateful_reuse_requirement_fields(&mut fields, report);
    append_stateful_reuse_side_effect_fields(&mut fields, report);
    fields
}

fn append_stateful_reuse_identity_fields(
    fields: &mut Vec<(String, String)>,
    report: &StatefulReuseReport,
) {
    push_field(fields, "mode", "stateful_reuse_plan");
    push_field(fields, "execution", "not_performed");
    push_field(fields, "plan_only", "true");
    push_field(fields, "schema_version", report.schema_version);
    push_field(fields, "report_id", &report.report_id);
    push_field(fields, "stateful_reuse_status", report.status.as_str());
    push_count_field(fields, "boundary_count", report.boundary_count());
    push_count_field(
        fields,
        "invalidation_requirement_count",
        report.invalidation_requirement_count(),
    );
    push_count_field(
        fields,
        "correctness_proof_required_count",
        report.correctness_proof_required_count(),
    );
    push_count_field(
        fields,
        "invalidation_proof_required_count",
        report.invalidation_proof_required_count(),
    );
    push_count_field(
        fields,
        "execution_certificate_required_count",
        report.execution_certificate_required_count(),
    );
    push_field(fields, "cache_kind_order", &report.cache_kind_order());
    push_field(
        fields,
        "invalidation_signal_order",
        &report.invalidation_signal_order(),
    );
}

fn append_stateful_reuse_requirement_fields(
    fields: &mut Vec<(String, String)>,
    report: &StatefulReuseReport,
) {
    push_bool_field(
        fields,
        "typed_cache_boundaries_required",
        report.typed_cache_boundaries_required,
    );
    push_bool_field(
        fields,
        "deterministic_keys_required",
        report.deterministic_keys_required,
    );
    push_bool_field(
        fields,
        "invalidation_proofs_required",
        report.invalidation_proofs_required,
    );
    push_bool_field(
        fields,
        "correctness_proofs_required",
        report.correctness_proofs_required,
    );
    push_bool_field(
        fields,
        "execution_certificates_required",
        report.execution_certificates_required,
    );
    push_bool_field(
        fields,
        "manifest_diff_required",
        report.manifest_diff_required,
    );
}

fn append_stateful_reuse_side_effect_fields(
    fields: &mut Vec<(String, String)>,
    report: &StatefulReuseReport,
) {
    push_bool_field(fields, "cache_read", report.cache_read);
    push_bool_field(fields, "cache_write", report.cache_write);
    push_bool_field(fields, "cache_replay", report.cache_replay);
    push_bool_field(
        fields,
        "incremental_execution",
        report.incremental_execution,
    );
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

pub(crate) fn stateful_reuse_promotion_gate_fields(
    report: &StatefulReusePromotionGateReport,
) -> Vec<(String, String)> {
    let mut fields = vec![];
    append_stateful_reuse_promotion_gate_identity_fields(&mut fields, report);
    append_stateful_reuse_promotion_gate_requirement_fields(&mut fields, report);
    append_stateful_reuse_promotion_gate_side_effect_fields(&mut fields, report);
    fields
}

fn append_stateful_reuse_promotion_gate_identity_fields(
    fields: &mut Vec<(String, String)>,
    report: &StatefulReusePromotionGateReport,
) {
    push_field(fields, "mode", "cg17_stateful_reuse_promotion_gate");
    push_field(fields, "execution", "not_performed");
    push_field(fields, "plan_only", "true");
    push_field(fields, "schema_version", report.schema_version);
    push_field(fields, "report_id", report.report_id);
    push_count_field(fields, "surface_count", report.surface_count());
    push_count_field(
        fields,
        "existing_evidence_surface_count",
        report.existing_evidence_surface_count(),
    );
    push_count_field(
        fields,
        "blocked_surface_count",
        report.blocked_surface_count(),
    );
    push_field(fields, "surface_order", &report.surface_order().join(","));
    push_field(
        fields,
        "existing_report_refs",
        &report.existing_report_refs.join(","),
    );
    push_bool_field(
        fields,
        "existing_stateful_reuse_boundary_report_present",
        report.existing_stateful_reuse_boundary_report_present,
    );
    push_bool_field(
        fields,
        "existing_cdc_incremental_planning_report_present",
        report.existing_cdc_incremental_planning_report_present,
    );
}

fn append_stateful_reuse_promotion_gate_requirement_fields(
    fields: &mut Vec<(String, String)>,
    report: &StatefulReusePromotionGateReport,
) {
    push_bool_field(
        fields,
        "stable_reuse_keys_required",
        report.stable_reuse_keys_required,
    );
    push_bool_field(
        fields,
        "key_digest_and_scope_required",
        report.key_digest_and_scope_required,
    );
    push_bool_field(
        fields,
        "manifest_diff_inputs_required",
        report.manifest_diff_inputs_required,
    );
    push_bool_field(
        fields,
        "invalidation_evidence_required",
        report.invalidation_evidence_required,
    );
    push_bool_field(
        fields,
        "cache_safety_policy_required",
        report.cache_safety_policy_required,
    );
    push_bool_field(
        fields,
        "state_certificates_required",
        report.state_certificates_required,
    );
    push_bool_field(
        fields,
        "correctness_evidence_required",
        report.correctness_evidence_required,
    );
    push_bool_field(
        fields,
        "execution_certificate_required",
        report.execution_certificate_required,
    );
    push_bool_field(
        fields,
        "native_io_certificate_required",
        report.native_io_certificate_required,
    );
    push_bool_field(
        fields,
        "reuse_benchmark_required",
        report.reuse_benchmark_required,
    );
}

fn append_stateful_reuse_promotion_gate_side_effect_fields(
    fields: &mut Vec<(String, String)>,
    report: &StatefulReusePromotionGateReport,
) {
    push_bool_field(fields, "cache_read_allowed", report.cache_read_allowed);
    push_bool_field(fields, "cache_write_allowed", report.cache_write_allowed);
    push_bool_field(fields, "cache_replay_allowed", report.cache_replay_allowed);
    push_bool_field(
        fields,
        "incremental_execution_allowed",
        report.incremental_execution_allowed,
    );
    push_bool_field(
        fields,
        "runtime_execution_allowed",
        report.runtime_execution_allowed,
    );
    push_bool_field(
        fields,
        "manifest_diff_read_allowed",
        report.manifest_diff_read_allowed,
    );
    push_bool_field(
        fields,
        "state_certificate_claim_allowed",
        report.state_certificate_claim_allowed,
    );
    push_bool_field(
        fields,
        "reuse_performance_claim_allowed",
        report.reuse_performance_claim_allowed,
    );
    push_bool_field(
        fields,
        "incremental_performance_claim_allowed",
        report.incremental_performance_claim_allowed,
    );
    push_bool_field(
        fields,
        "production_claim_allowed",
        report.production_claim_allowed,
    );
    push_bool_field(
        fields,
        "external_engine_invoked",
        report.external_engine_invoked,
    );
    push_bool_field(
        fields,
        "fallback_execution_allowed",
        report.fallback_execution_allowed,
    );
    push_bool_field(fields, "fallback_attempted", report.fallback_attempted);
    push_bool_field(
        fields,
        "runtime_promotions_blocked",
        report.runtime_promotions_blocked(),
    );
    push_bool_field(fields, "claim_blocked", report.claim_blocked());
    push_bool_field(fields, "side_effect_free", report.side_effect_free());
    push_count_field(fields, "diagnostic_count", report.diagnostics.len());
}
pub(crate) fn emit_layout_health_plan(format: OutputFormat, scenario: &str) -> ExitCode {
    let manifest = match layout_health_fixture(scenario) {
        Ok(manifest) => manifest,
        Err(error) => {
            return emit_error(
                "layout-health-plan",
                format,
                "layout health planning failed",
                &error,
            );
        }
    };
    let report = evaluate_layout_health(manifest, LayoutHealthPolicy::default());
    let status = if report.has_errors() {
        CommandStatus::Unsupported
    } else {
        CommandStatus::Success
    };
    emit(
        "layout-health-plan",
        format,
        status,
        "layout health planning report".to_string(),
        report.to_human_text(),
        report.diagnostics.clone(),
        layout_health_output_fields(&report, scenario),
    );
    if report.has_errors() {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

fn layout_health_output_fields(
    report: &LayoutHealthReport,
    scenario: &str,
) -> Vec<(String, String)> {
    let mut fields = vec![];
    push_field(&mut fields, "fallback_execution_allowed", "false");
    push_field(&mut fields, "mode", "layout_health_plan");
    push_field(&mut fields, "scenario", scenario);
    push_field(&mut fields, "layout_health_status", report.status.as_str());
    append_layout_health_count_fields(&mut fields, report);
    append_layout_health_requirement_fields(&mut fields, report);
    append_layout_health_side_effect_fields(&mut fields, report);
    push_field(&mut fields, "execution", "not_performed");
    push_field(&mut fields, "plan_only", "true");
    fields
}

fn append_layout_health_count_fields(
    fields: &mut Vec<(String, String)>,
    report: &LayoutHealthReport,
) {
    push_count_field(fields, "file_count", report.file_count);
    push_count_field(fields, "segment_count", report.segment_count);
    push_count_field(
        fields,
        "native_vortex_file_count",
        report.native_vortex_file_count,
    );
    push_count_field(
        fields,
        "non_native_data_file_count",
        report.non_native_data_file_count,
    );
    push_count_field(fields, "small_file_count", report.small_file_count);
    push_count_field(fields, "small_segment_count", report.small_segment_count);
    push_count_field(
        fields,
        "missing_statistics_segment_count",
        report.missing_statistics_segment_count,
    );
    push_count_field(
        fields,
        "missing_byte_range_segment_count",
        report.missing_byte_range_segment_count,
    );
    push_count_field(fields, "unique_format_count", report.unique_format_count);
    push_count_field(
        fields,
        "unique_encoding_count",
        report.unique_encoding_count,
    );
    push_count_field(fields, "unique_layout_count", report.unique_layout_count);
    push_count_field(
        fields,
        "compaction_candidate_count",
        report.compaction_candidate_count,
    );
}

fn append_layout_health_requirement_fields(
    fields: &mut Vec<(String, String)>,
    report: &LayoutHealthReport,
) {
    push_bool_field(
        fields,
        "requires_statistics_refresh",
        report.requires_statistics_refresh,
    );
    push_bool_field(
        fields,
        "requires_byte_range_index",
        report.requires_byte_range_index,
    );
    push_bool_field(
        fields,
        "requires_layout_review",
        report.requires_layout_review,
    );
    push_bool_field(
        fields,
        "recommends_compaction",
        report.recommends_compaction,
    );
    push_bool_field(fields, "can_plan_without_io", report.can_plan_without_io);
}

fn append_layout_health_side_effect_fields(
    fields: &mut Vec<(String, String)>,
    report: &LayoutHealthReport,
) {
    push_bool_field(fields, "side_effect_free", report.side_effect_free());
    push_bool_field(fields, "data_read", report.data_read);
    push_bool_field(fields, "write_io", report.write_io);
    push_bool_field(fields, "catalog_io", report.catalog_io);
    push_bool_field(fields, "object_store_io", report.object_store_io);
    push_bool_field(
        fields,
        "compaction_execution_allowed",
        report.compaction_execution_allowed,
    );
}

pub(crate) fn emit_compaction_plan(format: OutputFormat, scenario: &str) -> ExitCode {
    let manifest = match layout_health_fixture(scenario) {
        Ok(manifest) => manifest,
        Err(error) => {
            return emit_error(
                "compaction-plan",
                format,
                "compaction planning failed",
                &error,
            );
        }
    };
    let report = evaluate_compaction_planning(
        manifest,
        LayoutHealthPolicy::default(),
        CompactionPlanningPolicy::default(),
    );
    let status = if report.has_errors() {
        CommandStatus::Unsupported
    } else {
        CommandStatus::Success
    };
    emit(
        "compaction-plan",
        format,
        status,
        "compaction planning report".to_string(),
        report.to_human_text(),
        report.diagnostics.clone(),
        compaction_plan_output_fields(&report, scenario),
    );
    if report.has_errors() {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

fn compaction_plan_output_fields(
    report: &CompactionPlanningReport,
    scenario: &str,
) -> Vec<(String, String)> {
    let mut fields = vec![];
    push_field(&mut fields, "fallback_execution_allowed", "false");
    push_field(&mut fields, "mode", "compaction_plan");
    push_field(&mut fields, "scenario", scenario);
    push_field(&mut fields, "compaction_status", report.status.as_str());
    append_compaction_count_fields(&mut fields, report);
    append_compaction_requirement_fields(&mut fields, report);
    append_compaction_side_effect_fields(&mut fields, report);
    push_field(&mut fields, "execution", "not_performed");
    push_field(&mut fields, "plan_only", "true");
    fields
}

fn append_compaction_count_fields(
    fields: &mut Vec<(String, String)>,
    report: &CompactionPlanningReport,
) {
    push_count_field(fields, "file_count", report.file_count);
    push_count_field(fields, "segment_count", report.segment_count);
    push_count_field(fields, "candidate_file_count", report.candidate_file_count);
    push_count_field(
        fields,
        "candidate_segment_count",
        report.candidate_segment_count,
    );
    push_count_field(fields, "candidate_count", report.candidate_count);
    push_count_field(
        fields,
        "blocked_candidate_count",
        report.blocked_candidate_count,
    );
    push_count_field(
        fields,
        "estimated_compaction_group_count",
        report.estimated_compaction_group_count,
    );
    push_count_field(
        fields,
        "missing_statistics_segment_count",
        report.missing_statistics_segment_count,
    );
    push_count_field(
        fields,
        "missing_byte_range_segment_count",
        report.missing_byte_range_segment_count,
    );
    push_count_field(
        fields,
        "non_native_data_file_count",
        report.non_native_data_file_count,
    );
    push_count_field(fields, "action_count", report.actions.len());
}

fn append_compaction_requirement_fields(
    fields: &mut Vec<(String, String)>,
    report: &CompactionPlanningReport,
) {
    push_bool_field(
        fields,
        "requires_statistics_refresh",
        report.requires_statistics_refresh,
    );
    push_bool_field(
        fields,
        "requires_byte_range_index",
        report.requires_byte_range_index,
    );
    push_bool_field(
        fields,
        "requires_layout_review",
        report.requires_layout_review,
    );
    push_bool_field(
        fields,
        "requires_native_input_review",
        report.requires_native_input_review,
    );
    push_bool_field(
        fields,
        "compaction_recommended",
        report.compaction_recommended,
    );
    push_bool_field(
        fields,
        "recommendation_emitted",
        report.recommendation_emitted,
    );
    push_bool_field(fields, "can_plan_without_io", report.can_plan_without_io);
}

fn append_compaction_side_effect_fields(
    fields: &mut Vec<(String, String)>,
    report: &CompactionPlanningReport,
) {
    push_bool_field(fields, "side_effect_free", report.side_effect_free());
    push_bool_field(fields, "data_read", report.data_read);
    push_bool_field(fields, "write_io", report.write_io);
    push_bool_field(fields, "catalog_io", report.catalog_io);
    push_bool_field(fields, "object_store_io", report.object_store_io);
    push_bool_field(
        fields,
        "compaction_execution_allowed",
        report.compaction_execution_allowed,
    );
}

pub(crate) fn emit_table_intelligence_plan(format: OutputFormat) -> ExitCode {
    let report = TableIntelligenceReport::report_only_foundation();
    let status = if report.has_errors() {
        CommandStatus::Unsupported
    } else {
        CommandStatus::Success
    };
    emit(
        "table-intelligence-plan",
        format,
        status,
        "table intelligence plan".to_string(),
        report.to_human_text(),
        report.diagnostics.clone(),
        table_intelligence_output_fields(&report),
    );
    if report.has_errors() {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

fn table_intelligence_output_fields(report: &TableIntelligenceReport) -> Vec<(String, String)> {
    let mut fields = vec![];
    push_field(&mut fields, "mode", "table_intelligence_plan");
    push_field(&mut fields, "schema_version", report.schema_version);
    push_field(&mut fields, "report_id", report.report_id);
    push_count_field(&mut fields, "surface_count", report.surfaces.len());
    push_count_field(
        &mut fields,
        "report_only_available_surface_count",
        report.report_only_available_surface_count(),
    );
    push_count_field(
        &mut fields,
        "required_cg9_surface_count",
        report.required_cg9_surface_count(),
    );
    push_count_field(
        &mut fields,
        "snapshot_boundary_surface_count",
        report.snapshot_boundary_surface_count(),
    );
    push_field(
        &mut fields,
        "compatibility_profiles",
        &report.compatibility_profiles.join(","),
    );
    push_field(
        &mut fields,
        "surface_order",
        &report.surface_order().join(","),
    );
    push_bool_field(
        &mut fields,
        "catalog_io_performed",
        report.catalog_io_performed,
    );
    push_bool_field(
        &mut fields,
        "table_metadata_io_performed",
        report.table_metadata_io_performed,
    );
    push_bool_field(&mut fields, "data_io_performed", report.data_io_performed);
    push_bool_field(&mut fields, "write_io_performed", report.write_io_performed);
    push_bool_field(
        &mut fields,
        "external_table_format_dependency_added",
        report.external_table_format_dependency_added,
    );
    push_bool_field(&mut fields, "fallback_attempted", report.fallback_attempted);
    push_bool_field(
        &mut fields,
        "fallback_execution_allowed",
        report.fallback_execution_allowed,
    );
    push_bool_field(&mut fields, "side_effect_free", report.side_effect_free());
    push_count_field(&mut fields, "diagnostic_count", report.diagnostics.len());
    push_field(&mut fields, "execution", "not_performed");
    push_field(&mut fields, "plan_only", "true");
    fields
}

pub(crate) fn catalog_metadata_integration_gate_fields(
    report: &CatalogMetadataIntegrationGateReport,
) -> Vec<(String, String)> {
    let mut fields = vec![];
    push_field(&mut fields, "mode", "catalog_metadata_integration_gate");
    push_field(&mut fields, "schema_version", report.schema_version);
    push_field(&mut fields, "report_id", report.report_id);
    push_count_field(&mut fields, "surface_count", report.surface_count());
    push_count_field(
        &mut fields,
        "existing_evidence_surface_count",
        report.existing_evidence_surface_count(),
    );
    push_count_field(
        &mut fields,
        "blocked_surface_count",
        report.blocked_surface_count(),
    );
    push_field(
        &mut fields,
        "surface_order",
        &report.surface_order().join(","),
    );
    push_field(
        &mut fields,
        "existing_report_refs",
        &report.existing_report_refs.join(","),
    );
    push_field(
        &mut fields,
        "compatibility_profiles",
        &report.compatibility_profiles.join(","),
    );
    append_catalog_metadata_existing_fields(&mut fields, report);
    append_catalog_metadata_allowed_fields(&mut fields, report);
    append_catalog_metadata_required_fields(&mut fields, report);
    append_catalog_metadata_status_fields(&mut fields, report);
    fields
}

fn append_catalog_metadata_existing_fields(
    fields: &mut Vec<(String, String)>,
    report: &CatalogMetadataIntegrationGateReport,
) {
    push_bool_field(
        fields,
        "existing_table_intelligence_foundation_present",
        report.existing_table_intelligence_foundation_present,
    );
    push_bool_field(
        fields,
        "existing_schema_partition_delete_compatibility_present",
        report.existing_schema_partition_delete_compatibility_present,
    );
    push_bool_field(
        fields,
        "existing_cdc_layout_compaction_planning_present",
        report.existing_cdc_layout_compaction_planning_present,
    );
    push_bool_field(
        fields,
        "existing_catalog_ref_skeleton_present",
        report.existing_catalog_ref_skeleton_present,
    );
}

fn append_catalog_metadata_allowed_fields(
    fields: &mut Vec<(String, String)>,
    report: &CatalogMetadataIntegrationGateReport,
) {
    push_bool_field(
        fields,
        "snapshot_manifest_metadata_read_allowed",
        report.snapshot_manifest_metadata_read_allowed,
    );
    push_bool_field(
        fields,
        "catalog_resolution_allowed",
        report.catalog_resolution_allowed,
    );
    push_bool_field(
        fields,
        "table_metadata_read_allowed",
        report.table_metadata_read_allowed,
    );
    push_bool_field(fields, "catalog_io_allowed", report.catalog_io_allowed);
    push_bool_field(
        fields,
        "object_store_io_allowed",
        report.object_store_io_allowed,
    );
    push_bool_field(fields, "data_io_allowed", report.data_io_allowed);
    push_bool_field(fields, "write_io_allowed", report.write_io_allowed);
    push_bool_field(
        fields,
        "external_table_format_dependency_allowed",
        report.external_table_format_dependency_allowed,
    );
    push_bool_field(
        fields,
        "credential_resolution_allowed",
        report.credential_resolution_allowed,
    );
    push_bool_field(
        fields,
        "metadata_cache_runtime_allowed",
        report.metadata_cache_runtime_allowed,
    );
    push_bool_field(
        fields,
        "metadata_integration_claim_allowed",
        report.metadata_integration_claim_allowed,
    );
}

fn append_catalog_metadata_required_fields(
    fields: &mut Vec<(String, String)>,
    report: &CatalogMetadataIntegrationGateReport,
) {
    push_bool_field(
        fields,
        "table_intelligence_report_required",
        report.table_intelligence_report_required,
    );
    push_bool_field(fields, "catalog_ref_required", report.catalog_ref_required);
    push_bool_field(
        fields,
        "snapshot_ref_required",
        report.snapshot_ref_required,
    );
    push_bool_field(
        fields,
        "schema_digest_required",
        report.schema_digest_required,
    );
    push_bool_field(
        fields,
        "partition_spec_required",
        report.partition_spec_required,
    );
    push_bool_field(
        fields,
        "delete_tombstone_policy_required",
        report.delete_tombstone_policy_required,
    );
    push_bool_field(
        fields,
        "dependency_license_approval_required",
        report.dependency_license_approval_required,
    );
    push_bool_field(
        fields,
        "credential_policy_required",
        report.credential_policy_required,
    );
    push_bool_field(
        fields,
        "effect_policy_required",
        report.effect_policy_required,
    );
    push_bool_field(
        fields,
        "materialization_boundary_required",
        report.materialization_boundary_required,
    );
    push_bool_field(
        fields,
        "execution_certificate_required",
        report.execution_certificate_required,
    );
    push_bool_field(
        fields,
        "native_io_certificate_required",
        report.native_io_certificate_required,
    );
    push_bool_field(
        fields,
        "benchmark_evidence_required",
        report.benchmark_evidence_required,
    );
}

fn append_catalog_metadata_status_fields(
    fields: &mut Vec<(String, String)>,
    report: &CatalogMetadataIntegrationGateReport,
) {
    push_bool_field(
        fields,
        "runtime_promotions_blocked",
        report.runtime_promotions_blocked(),
    );
    push_bool_field(fields, "claim_blocked", report.claim_blocked());
    push_bool_field(fields, "fallback_attempted", report.fallback_attempted);
    push_bool_field(
        fields,
        "fallback_execution_allowed",
        report.fallback_execution_allowed,
    );
    push_bool_field(fields, "side_effect_free", report.side_effect_free());
    push_count_field(fields, "diagnostic_count", report.diagnostics.len());
    push_field(fields, "execution", "not_performed");
    push_field(fields, "plan_only", "true");
}

fn layout_health_fixture(scenario: &str) -> Result<DatasetManifest, ShardLoomError> {
    let mut manifest = layout_health_base_manifest()?;
    match scenario {
        "healthy" => {
            layout_health_add_segment(
                &mut manifest,
                "healthy",
                64 * 1024 * 1024,
                Some(64_000),
                Some(8 * 1024 * 1024),
                true,
                DatasetFormat::Vortex,
            )?;
            Ok(manifest)
        }
        "small-files" => {
            layout_health_add_segment(
                &mut manifest,
                "small",
                1024,
                Some(10),
                Some(512),
                true,
                DatasetFormat::Vortex,
            )?;
            Ok(manifest)
        }
        "missing-stats" => {
            layout_health_add_segment(
                &mut manifest,
                "missing-stats",
                64 * 1024 * 1024,
                None,
                Some(8 * 1024 * 1024),
                false,
                DatasetFormat::Vortex,
            )?;
            Ok(manifest)
        }
        "mixed-layout" => {
            layout_health_add_segment(
                &mut manifest,
                "vortex",
                64 * 1024 * 1024,
                Some(64_000),
                Some(8 * 1024 * 1024),
                true,
                DatasetFormat::Vortex,
            )?;
            layout_health_add_segment(
                &mut manifest,
                "parquet",
                64 * 1024 * 1024,
                Some(64_000),
                Some(8 * 1024 * 1024),
                true,
                DatasetFormat::Parquet,
            )?;
            Ok(manifest)
        }
        "empty" => Ok(manifest),
        value => Err(cli_unknown_arg_error("layout-health-plan", value)),
    }
}

fn layout_health_base_manifest() -> Result<DatasetManifest, ShardLoomError> {
    Ok(DatasetManifest::new(
        ManifestId::new("layout-health-manifest")?,
        DatasetRef::from_uri(DatasetUri::new("file://layout-health/table.vortex")?)?,
        SnapshotRef::new(SnapshotId::new("layout-health-snapshot")?),
    ))
}

#[allow(clippy::too_many_arguments)]
fn layout_health_add_segment(
    manifest: &mut DatasetManifest,
    name: &str,
    file_size_bytes: u64,
    row_count: Option<u64>,
    physical_size_bytes: Option<u64>,
    has_byte_ranges: bool,
    format: DatasetFormat,
) -> Result<(), ShardLoomError> {
    let extension = if format.is_native_vortex() {
        "vortex"
    } else {
        "parquet"
    };
    let file = FileDescriptor::new(
        DatasetUri::new(format!("file://layout-health/{name}.{extension}"))?,
        format,
        FileRole::NativeVortexData,
    )
    .with_size_bytes(file_size_bytes);
    let segment = layout_health_segment(name, row_count, physical_size_bytes, has_byte_ranges)?;
    manifest.add_file(file.clone());
    manifest.add_segment(ManifestSegment::new(segment, file));
    Ok(())
}

fn layout_health_segment(
    name: &str,
    row_count: Option<u64>,
    physical_size_bytes: Option<u64>,
    has_byte_ranges: bool,
) -> Result<EncodedSegment, ShardLoomError> {
    let mut layout = SegmentLayout::new(EncodingKind::Plain, LayoutKind::Flat);
    layout.physical_size_bytes = physical_size_bytes;
    if has_byte_ranges {
        layout = layout.with_byte_ranges(vec![ByteRange::new(0, 1024)]);
    }
    let stats = row_count.map_or_else(SegmentStats::unknown, SegmentStats::with_row_count);
    Ok(EncodedSegment::new(
        SegmentId::new(format!("segment-{name}"))?,
        ColumnRef::new("value")?,
        LogicalDType::Int64,
        Nullability::Nullable,
        layout,
        stats,
    ))
}

pub(crate) fn emit_cdc_incremental_plan(format: OutputFormat, scenario: &str) -> ExitCode {
    let (change_set, cdc_events) = match cdc_incremental_fixture(scenario) {
        Ok(parts) => parts,
        Err(error) => {
            return emit_error(
                "incremental-plan",
                format,
                "CDC incremental plan failed",
                &error,
            );
        }
    };
    let report = evaluate_cdc_incremental_planning(change_set, cdc_events);
    let status = if report.has_errors() {
        CommandStatus::Unsupported
    } else {
        CommandStatus::Success
    };
    emit(
        "incremental-plan",
        format,
        status,
        "CDC incremental planning report".to_string(),
        report.to_human_text(),
        report.diagnostics.clone(),
        cdc_incremental_output_fields(&report, scenario),
    );
    if report.has_errors() {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

fn cdc_incremental_output_fields(
    report: &CdcIncrementalPlanningReport,
    scenario: &str,
) -> Vec<(String, String)> {
    let mut fields = vec![];
    push_field(&mut fields, "fallback_execution_allowed", "false");
    push_field(&mut fields, "mode", "cdc_incremental_plan");
    push_field(&mut fields, "scenario", scenario);
    push_field(&mut fields, "cdc_status", report.status.as_str());
    append_cdc_incremental_count_fields(&mut fields, report);
    append_cdc_incremental_requirement_fields(&mut fields, report);
    append_cdc_incremental_side_effect_fields(&mut fields, report);
    push_field(&mut fields, "execution", "not_performed");
    push_field(&mut fields, "plan_only", "true");
    fields
}

fn append_cdc_incremental_count_fields(
    fields: &mut Vec<(String, String)>,
    report: &CdcIncrementalPlanningReport,
) {
    push_count_field(
        fields,
        "changed_segment_count",
        report.changed_segment_count,
    );
    push_count_field(
        fields,
        "metadata_only_segment_count",
        report.metadata_only_segment_count,
    );
    push_count_field(
        fields,
        "unknown_segment_change_count",
        report.unknown_segment_change_count,
    );
    push_count_field(fields, "insert_count", report.insert_count);
    push_count_field(fields, "update_count", report.update_count);
    push_count_field(fields, "delete_count", report.delete_count);
    push_count_field(fields, "tombstone_count", report.tombstone_count);
    push_count_field(fields, "schema_change_count", report.schema_change_count);
    push_count_field(
        fields,
        "partition_change_count",
        report.partition_change_count,
    );
    push_count_field(fields, "metadata_only_count", report.metadata_only_count);
    push_count_field(fields, "unknown_event_count", report.unknown_event_count);
    push_count_field(
        fields,
        "unsupported_change_count",
        report.unsupported_change_count,
    );
}

fn append_cdc_incremental_requirement_fields(
    fields: &mut Vec<(String, String)>,
    report: &CdcIncrementalPlanningReport,
) {
    push_bool_field(
        fields,
        "requires_snapshot_pair",
        report.requires_snapshot_pair,
    );
    push_bool_field(
        fields,
        "requires_row_identity",
        report.requires_row_identity,
    );
    push_bool_field(
        fields,
        "requires_delete_handling",
        report.requires_delete_handling,
    );
    push_bool_field(
        fields,
        "requires_schema_compatibility",
        report.requires_schema_compatibility,
    );
    push_bool_field(
        fields,
        "requires_partition_compatibility",
        report.requires_partition_compatibility,
    );
    push_bool_field(
        fields,
        "can_reuse_unchanged_segments",
        report.can_reuse_unchanged_segments,
    );
    push_bool_field(
        fields,
        "can_execute_changed_segments_only",
        report.can_execute_changed_segments_only,
    );
    push_bool_field(
        fields,
        "requires_partial_recompute",
        report.requires_partial_recompute,
    );
    push_bool_field(
        fields,
        "requires_full_recompute",
        report.requires_full_recompute,
    );
}

fn append_cdc_incremental_side_effect_fields(
    fields: &mut Vec<(String, String)>,
    report: &CdcIncrementalPlanningReport,
) {
    push_bool_field(fields, "side_effect_free", report.side_effect_free());
    push_bool_field(fields, "data_read", report.data_read);
    push_bool_field(fields, "write_io", report.write_io);
    push_bool_field(fields, "catalog_io", report.catalog_io);
    push_bool_field(fields, "object_store_io", report.object_store_io);
}

fn cdc_incremental_fixture(
    scenario: &str,
) -> Result<(ChangeSet, Vec<CdcEventSummary>), ShardLoomError> {
    match scenario {
        "append-only" => {
            let mut change_set = cdc_change_set_between()?;
            change_set.add_change(SegmentChange::new(
                SegmentChangeKind::Added,
                SegmentId::new("segment-added")?,
            ));
            Ok((
                change_set,
                vec![CdcEventSummary::new(CdcEventKind::Insert, 10)],
            ))
        }
        "metadata-only" => {
            let mut change_set = cdc_change_set_between()?;
            change_set.add_change(SegmentChange::new(
                SegmentChangeKind::MetadataOnly,
                SegmentId::new("segment-metadata")?,
            ));
            Ok((
                change_set,
                vec![CdcEventSummary::new(CdcEventKind::MetadataOnly, 1)],
            ))
        }
        "delete" => {
            let mut change_set = cdc_change_set_between()?;
            change_set.add_change(SegmentChange::new(
                SegmentChangeKind::Removed,
                SegmentId::new("segment-removed")?,
            ));
            Ok((
                change_set,
                vec![CdcEventSummary::new(CdcEventKind::Delete, 1)],
            ))
        }
        "upsert" => {
            let mut change_set = cdc_change_set_between()?;
            change_set.add_change(SegmentChange::new(
                SegmentChangeKind::Replaced,
                SegmentId::new("segment-replaced")?,
            ));
            Ok((
                change_set,
                vec![CdcEventSummary::new(CdcEventKind::Update, 4)],
            ))
        }
        "schema-change" => {
            let mut change_set = cdc_change_set_between()?;
            change_set.add_change(SegmentChange::new(
                SegmentChangeKind::MetadataOnly,
                SegmentId::new("segment-schema")?,
            ));
            Ok((
                change_set,
                vec![CdcEventSummary::new(CdcEventKind::SchemaChange, 1)],
            ))
        }
        "partition-change" => {
            let mut change_set = cdc_change_set_between()?;
            change_set.add_change(SegmentChange::new(
                SegmentChangeKind::MetadataOnly,
                SegmentId::new("segment-partition")?,
            ));
            Ok((
                change_set,
                vec![CdcEventSummary::new(CdcEventKind::PartitionChange, 1)],
            ))
        }
        "missing-from-snapshot" => {
            let mut change_set = ChangeSet::new(SnapshotId::new("snapshot-current")?);
            change_set.add_change(SegmentChange::new(
                SegmentChangeKind::Added,
                SegmentId::new("segment-added")?,
            ));
            Ok((
                change_set,
                vec![CdcEventSummary::new(CdcEventKind::Insert, 1)],
            ))
        }
        "unknown" => {
            let mut change_set = cdc_change_set_between()?;
            change_set.add_change(SegmentChange::new(
                SegmentChangeKind::Unknown,
                SegmentId::new("segment-unknown")?,
            ));
            Ok((
                change_set,
                vec![CdcEventSummary::new(CdcEventKind::Unknown, 1)],
            ))
        }
        value => Err(cli_unknown_arg_error("incremental-plan cdc", value)),
    }
}

fn cdc_change_set_between() -> Result<ChangeSet, ShardLoomError> {
    Ok(ChangeSet::between(
        SnapshotId::new("snapshot-previous")?,
        SnapshotId::new("snapshot-current")?,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn table_intelligence_fields_include_report_only_no_io_no_fallback() {
        let report = TableIntelligenceReport::report_only_foundation();
        let fields = table_intelligence_output_fields(&report);

        assert_eq!(
            output_field(&fields, "schema_version"),
            "shardloom.table_intelligence.v1"
        );
        assert_eq!(
            output_field(&fields, "report_id"),
            "cg9.table_intelligence.foundation"
        );
        assert_eq!(output_field(&fields, "surface_count"), "10");
        assert_eq!(
            output_field(&fields, "report_only_available_surface_count"),
            "7"
        );
        assert_eq!(output_field(&fields, "required_cg9_surface_count"), "10");
        assert_eq!(output_field(&fields, "catalog_io_performed"), "false");
        assert_eq!(
            output_field(&fields, "table_metadata_io_performed"),
            "false"
        );
        assert_eq!(output_field(&fields, "data_io_performed"), "false");
        assert_eq!(output_field(&fields, "write_io_performed"), "false");
        assert_eq!(output_field(&fields, "fallback_execution_allowed"), "false");
        assert_eq!(output_field(&fields, "fallback_attempted"), "false");
        assert_eq!(output_field(&fields, "side_effect_free"), "true");
        assert!(output_field(&fields, "surface_order").contains("schema_evolution"));
        assert!(output_field(&fields, "surface_order").contains("commit_recovery"));
    }

    fn output_field<'a>(fields: &'a [(String, String)], key: &str) -> &'a str {
        fields
            .iter()
            .find(|(field_key, _)| field_key == key)
            .map_or("", |(_, value)| value.as_str())
    }
}
