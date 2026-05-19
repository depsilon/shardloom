//! Workflow, table, manifest, and stateful planning CLI handlers.
//!
//! These handlers emit report-only workflow planning surfaces. They do not read
//! datasets, probe catalogs, execute plans, write data, materialize outputs,
//! invoke external engines, or provide fallback execution.

use std::process::ExitCode;

use shardloom_core::{
    ByteRange, CapabilityCertificationReport, CatalogKind, CatalogMetadataIntegrationGateEntry,
    CatalogMetadataIntegrationGateReport, CatalogRef, CdcEventKind, CdcEventSummary,
    CdcIncrementalPlanningReport, CdcManifestTransactionGateEntry,
    CdcManifestTransactionGateReport, ChangeSet, ColumnRef, CommandStatus,
    CompactionPlanningPolicy, CompactionPlanningReport, DatasetFormat, DatasetManifest, DatasetRef,
    DatasetUri, DeleteModel, DeleteTombstoneCompatibilityReport, Diagnostic, DiagnosticCategory,
    DiagnosticCode, EncodedSegment, EncodingKind, FieldId, FieldName, FieldPath, FileDescriptor,
    FileRole, IncrementalPlanSkeleton, LayoutHealthPolicy, LayoutHealthReport, LayoutKind,
    LocalAppendOnlyCdcOverlaySmokeReport, LocalDeleteTombstoneReadSmokeReport,
    LocalTableMetadataReadSmokeReport, LogicalDType, ManifestId, ManifestSegment, Nullability,
    OutputFormat, OutputTarget, PartitionEvolutionCompatibilityReport, PartitionField,
    PartitionSpec, PartitionTransform, SchemaDefinition, SchemaEvolutionCompatibilityReport,
    SchemaEvolutionPolicy, SchemaField, SchemaId, SchemaVersion, SegmentChange, SegmentChangeKind,
    SegmentId, SegmentLayout, SegmentStats, ShardLoomError, SnapshotId, SnapshotRef,
    StatefulReusePromotionGateReport, StatefulReuseReport, TableCompatibilityPlan,
    TableCompatibilityReport, TableFormatKind, TableIntelligenceReport,
    TableMaintenanceExecutionMatrixReport, TableMaintenanceExecutionMatrixRow, WriteIntent,
    evaluate_cdc_incremental_planning, evaluate_compaction_planning,
    evaluate_delete_tombstone_compatibility, evaluate_layout_health,
    evaluate_partition_evolution_compatibility, evaluate_schema_evolution_compatibility,
    plan_catalog_metadata_integration_gate, plan_cdc_manifest_transaction_gate,
    plan_stateful_reuse, plan_stateful_reuse_promotion_gate,
    plan_table_maintenance_execution_matrix, run_local_append_only_cdc_overlay_smoke,
    run_local_delete_tombstone_read_smoke, run_local_table_metadata_read_smoke,
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
    let diagnostic = workflow_unsupported_diagnostic(operation);
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

pub(crate) fn handle_local_table_metadata_read_smoke(
    mut args: impl Iterator<Item = String>,
    format: OutputFormat,
) -> ExitCode {
    if let Some(extra) = args.next() {
        return emit_error(
            "local-table-metadata-read-smoke",
            format,
            "local table metadata read smoke failed",
            &cli_unknown_arg_error("local-table-metadata-read-smoke", &extra),
        );
    }
    let report = match run_local_table_metadata_read_smoke() {
        Ok(report) => report,
        Err(error) => {
            return emit_error(
                "local-table-metadata-read-smoke",
                format,
                "local table metadata read smoke failed",
                &error,
            );
        }
    };
    let has_errors = report.has_errors();
    emit(
        "local-table-metadata-read-smoke",
        format,
        if has_errors {
            CommandStatus::Unsupported
        } else {
            CommandStatus::Success
        },
        "local manifest-backed table metadata read smoke".to_string(),
        report.to_human_text(),
        report.diagnostics.clone(),
        local_table_metadata_read_smoke_fields(&report),
    );
    if has_errors {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

pub(crate) fn handle_local_delete_tombstone_read_smoke(
    mut args: impl Iterator<Item = String>,
    format: OutputFormat,
) -> ExitCode {
    if let Some(extra) = args.next() {
        return emit_error(
            "local-delete-tombstone-read-smoke",
            format,
            "local delete/tombstone read smoke failed",
            &cli_unknown_arg_error("local-delete-tombstone-read-smoke", &extra),
        );
    }
    let report = match run_local_delete_tombstone_read_smoke() {
        Ok(report) => report,
        Err(error) => {
            return emit_error(
                "local-delete-tombstone-read-smoke",
                format,
                "local delete/tombstone read smoke failed",
                &error,
            );
        }
    };
    let has_errors = report.has_errors();
    emit(
        "local-delete-tombstone-read-smoke",
        format,
        if has_errors {
            CommandStatus::Unsupported
        } else {
            CommandStatus::Success
        },
        "local manifest-backed delete/tombstone read smoke".to_string(),
        report.to_human_text(),
        report.diagnostics.clone(),
        local_delete_tombstone_read_smoke_fields(&report),
    );
    if has_errors {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

pub(crate) fn handle_local_append_only_cdc_overlay_smoke(
    mut args: impl Iterator<Item = String>,
    format: OutputFormat,
) -> ExitCode {
    if let Some(extra) = args.next() {
        return emit_error(
            "local-append-only-cdc-overlay-smoke",
            format,
            "local append-only CDC overlay smoke failed",
            &cli_unknown_arg_error("local-append-only-cdc-overlay-smoke", &extra),
        );
    }
    let report = match run_local_append_only_cdc_overlay_smoke() {
        Ok(report) => report,
        Err(error) => {
            return emit_error(
                "local-append-only-cdc-overlay-smoke",
                format,
                "local append-only CDC overlay smoke failed",
                &error,
            );
        }
    };
    let has_errors = report.has_errors();
    emit(
        "local-append-only-cdc-overlay-smoke",
        format,
        if has_errors {
            CommandStatus::Unsupported
        } else {
            CommandStatus::Success
        },
        "local append-only CDC overlay smoke".to_string(),
        report.to_human_text(),
        report.diagnostics.clone(),
        local_append_only_cdc_overlay_smoke_fields(&report),
    );
    if has_errors {
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
        "from-pandas" => Some(workflow_unsupported_from_pandas()),
        "from-arrow-table" => Some(workflow_unsupported_from_arrow_table()),
        "from-arrow-ipc" => Some(workflow_unsupported_from_arrow_ipc()),
        "to-pandas" => Some(workflow_unsupported_to_pandas()),
        "to-arrow" => Some(workflow_unsupported_to_arrow()),
        "to-arrow-table" => Some(workflow_unsupported_to_arrow_table()),
        "to-arrow-ipc" => Some(workflow_unsupported_to_arrow_ipc()),
        "to-numpy" => Some(workflow_unsupported_to_numpy()),
        "to-python-objects" | "to-py" | "to-pylist" => {
            Some(workflow_unsupported_to_python_objects())
        }
        "with-column" | "with_column" => Some(workflow_unsupported_with_column()),
        "group-by" | "group_by" | "groupby" => Some(workflow_unsupported_group_by()),
        "agg" => Some(workflow_unsupported_agg()),
        "sort" | "order-by" | "order_by" => Some(workflow_unsupported_sort()),
        "limit" => Some(workflow_unsupported_limit()),
        "write-vortex" => Some(workflow_unsupported_write_vortex()),
        "write-parquet" => Some(workflow_unsupported_write_parquet()),
        "sql" => Some(workflow_unsupported_sql()),
        "sql-parse" | "sql_parse" => Some(workflow_unsupported_sql_parse()),
        "sql-bind" | "sql_bind" => Some(workflow_unsupported_sql_bind()),
        "sql-plan" | "sql_plan" => Some(workflow_unsupported_sql_plan()),
        "sql-execute" | "sql_execute" => Some(workflow_unsupported_sql_execute()),
        "source-free-sequence"
        | "source_free_sequence"
        | "sequence"
        | "generate-series"
        | "generate_series" => Some(workflow_unsupported_source_free_sequence()),
        "sql-values" | "sql_values" | "values" => Some(workflow_unsupported_sql_values()),
        "sql-literal-select"
        | "sql_literal_select"
        | "literal-select"
        | "literal_select"
        | "sql-source-free-projection"
        | "sql_source_free_projection" => Some(workflow_unsupported_sql_literal_select()),
        "dataframe-source-free-projection"
        | "dataframe_source_free_projection"
        | "df-source-free-projection"
        | "df_source_free_projection" => {
            Some(workflow_unsupported_dataframe_source_free_projection())
        }
        "dataframe-generated-with-column"
        | "dataframe_generated_with_column"
        | "generated-with-column"
        | "generated_with_column" => Some(workflow_unsupported_dataframe_generated_with_column()),
        "object-store-generated-output"
        | "object_store_generated_output"
        | "generated-output-object-store"
        | "generated_output_object_store" => {
            Some(workflow_unsupported_object_store_generated_output())
        }
        "foundry-generated-output"
        | "foundry_generated_output"
        | "generated-output-foundry"
        | "generated_output_foundry" => Some(workflow_unsupported_foundry_generated_output()),
        "join" => Some(workflow_unsupported_join()),
        "aggregate" | "aggregation" => Some(workflow_unsupported_aggregate()),
        "window" | "windows" => Some(workflow_unsupported_window()),
        "schema-contract" => Some(workflow_unsupported_schema_contract()),
        "schema" | "schema-discovery" => Some(workflow_unsupported_schema_discovery()),
        "describe-schema" => Some(workflow_unsupported_describe_schema()),
        "validate-schema" => Some(workflow_unsupported_validate_schema()),
        "data-quality" | "data-quality-check" | "quality" => {
            Some(workflow_unsupported_data_quality())
        }
        "data-quality-summary" | "quality-summary" => {
            Some(workflow_unsupported_data_quality_summary())
        }
        "quarantine" => Some(workflow_unsupported_quarantine()),
        "preview" => Some(workflow_unsupported_preview()),
        "display" | "notebook-display" => Some(workflow_unsupported_display()),
        "object-store-read" | "object_store_read" | "remote-object-store" => {
            Some(workflow_unsupported_object_store_read())
        }
        "fallback-engine" | "spark-fallback" | "external-fallback" => {
            Some(workflow_unsupported_fallback_engine())
        }
        _ => None,
    }
}

fn workflow_unsupported_diagnostic(operation: WorkflowUnsupportedOperation) -> Diagnostic {
    let reason = if operation.diagnostic_code == DiagnosticCode::NoFallbackExecution {
        format!(
            "{} is prohibited by ShardLoom's no-fallback policy.",
            operation.label
        )
    } else {
        format!(
            "{} is not implemented for native ShardLoom workflow execution yet.",
            operation.label
        )
    };
    match operation.diagnostic_code {
        DiagnosticCode::MaterializationRequired => Diagnostic::materialization_required(
            operation.feature,
            reason,
            operation.suggested_next_action,
        ),
        DiagnosticCode::ObjectStoreUnsupported => Diagnostic::object_store_blocked(
            operation.feature,
            reason,
            operation.suggested_next_action,
        ),
        DiagnosticCode::NoFallbackExecution => Diagnostic::no_fallback_policy(
            operation.feature,
            reason,
            operation.suggested_next_action,
        ),
        DiagnosticCode::InvalidInput => {
            Diagnostic::invalid_input(operation.feature, reason, operation.suggested_next_action)
        }
        _ => Diagnostic::unsupported(
            operation.diagnostic_code,
            operation.feature,
            reason,
            Some(operation.suggested_next_action.to_string()),
        ),
    }
}

const fn workflow_unsupported_diagnostic_category(
    operation: WorkflowUnsupportedOperation,
) -> DiagnosticCategory {
    match operation.diagnostic_code {
        DiagnosticCode::MaterializationRequired => DiagnosticCategory::Materialization,
        DiagnosticCode::ObjectStoreUnsupported => DiagnosticCategory::ObjectStore,
        DiagnosticCode::NoFallbackExecution => DiagnosticCategory::NoFallbackPolicy,
        DiagnosticCode::InvalidInput => DiagnosticCategory::InvalidInput,
        _ => DiagnosticCategory::UnsupportedFeature,
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

fn workflow_unsupported_from_pandas() -> WorkflowUnsupportedOperation {
    WorkflowUnsupportedOperation {
        operation: "from_pandas",
        label: "pandas input conversion",
        surface: "materialized_python_input_boundary",
        feature: "cg21.workflow.from_pandas",
        blocker_id: "cg21.workflow.from_pandas.materialized_input_unsupported",
        required_evidence: "python_object_boundary,decoded_columnar_boundary,native_io_certificate",
        suggested_next_action: "Declare a file-backed source or wait for a certified pandas input-boundary report before importing in-memory data.",
        diagnostic_code: DiagnosticCode::MaterializationRequired,
        materialization_required: true,
        write_required: false,
        runtime_required: false,
    }
}

fn workflow_unsupported_from_arrow_table() -> WorkflowUnsupportedOperation {
    WorkflowUnsupportedOperation {
        operation: "from_arrow_table",
        label: "Arrow table input conversion",
        surface: "decoded_columnar_input_boundary",
        feature: "cg21.workflow.from_arrow_table",
        blocker_id: "cg21.workflow.from_arrow_table.decoded_columnar_input_unsupported",
        required_evidence: "arrow_table_boundary,adapter_fidelity_report,native_io_certificate",
        suggested_next_action: "Use a file-backed native Vortex or compatibility-source plan until Arrow table import is certified.",
        diagnostic_code: DiagnosticCode::MaterializationRequired,
        materialization_required: true,
        write_required: false,
        runtime_required: false,
    }
}

fn workflow_unsupported_from_arrow_ipc() -> WorkflowUnsupportedOperation {
    WorkflowUnsupportedOperation {
        operation: "from_arrow_ipc",
        label: "Arrow IPC input conversion",
        surface: "decoded_columnar_ipc_input_boundary",
        feature: "cg21.workflow.from_arrow_ipc",
        blocker_id: "cg21.workflow.from_arrow_ipc.decoded_ipc_input_unsupported",
        required_evidence: "arrow_ipc_boundary,adapter_fidelity_report,native_io_certificate",
        suggested_next_action: "Use input-plan arrow-ipc for report-only adapter posture until Arrow IPC import is certified.",
        diagnostic_code: DiagnosticCode::MaterializationRequired,
        materialization_required: true,
        write_required: false,
        runtime_required: false,
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

fn workflow_unsupported_to_arrow_table() -> WorkflowUnsupportedOperation {
    WorkflowUnsupportedOperation {
        operation: "to_arrow_table",
        label: "Arrow table materialization",
        surface: "decoded_columnar_table_materialization",
        feature: "cg21.workflow.to_arrow_table",
        blocker_id: "cg21.workflow.to_arrow_table.decoded_table_unsupported",
        required_evidence: "decoded_columnar_boundary,native_io_certificate,adapter_fidelity_report",
        suggested_next_action: "Use native Vortex artifact planning until Arrow table materialization is certified.",
        diagnostic_code: DiagnosticCode::MaterializationRequired,
        materialization_required: true,
        write_required: false,
        runtime_required: true,
    }
}

fn workflow_unsupported_to_arrow_ipc() -> WorkflowUnsupportedOperation {
    WorkflowUnsupportedOperation {
        operation: "to_arrow_ipc",
        label: "Arrow IPC materialization",
        surface: "decoded_columnar_ipc_materialization",
        feature: "cg21.workflow.to_arrow_ipc",
        blocker_id: "cg21.workflow.to_arrow_ipc.decoded_ipc_unsupported",
        required_evidence: "decoded_columnar_boundary,arrow_ipc_fidelity_report,native_io_certificate",
        suggested_next_action: "Use REST data-plane artifact-reference reports until Arrow IPC output is certified.",
        diagnostic_code: DiagnosticCode::MaterializationRequired,
        materialization_required: true,
        write_required: false,
        runtime_required: true,
    }
}

fn workflow_unsupported_to_numpy() -> WorkflowUnsupportedOperation {
    WorkflowUnsupportedOperation {
        operation: "to_numpy",
        label: "NumPy materialization",
        surface: "python_array_materialization",
        feature: "cg21.workflow.to_numpy",
        blocker_id: "cg21.workflow.to_numpy.python_array_unsupported",
        required_evidence: "python_object_boundary,decoded_columnar_boundary,materialization_policy",
        suggested_next_action: "Use native artifacts or Arrow boundary reports before requesting NumPy materialization.",
        diagnostic_code: DiagnosticCode::MaterializationRequired,
        materialization_required: true,
        write_required: false,
        runtime_required: true,
    }
}

fn workflow_unsupported_to_python_objects() -> WorkflowUnsupportedOperation {
    WorkflowUnsupportedOperation {
        operation: "to_python_objects",
        label: "Python object materialization",
        surface: "python_object_materialization",
        feature: "cg21.workflow.to_python_objects",
        blocker_id: "cg21.workflow.to_python_objects.object_materialization_unsupported",
        required_evidence: "python_object_boundary,materialization_policy,decoded_reference_check",
        suggested_next_action: "Keep results as certified native artifacts until Python-object materialization is certified.",
        diagnostic_code: DiagnosticCode::MaterializationRequired,
        materialization_required: true,
        write_required: false,
        runtime_required: true,
    }
}

fn workflow_unsupported_with_column() -> WorkflowUnsupportedOperation {
    WorkflowUnsupportedOperation {
        operation: "with_column",
        label: "DataFrame with_column expression",
        surface: "dataframe_expression_projection",
        feature: "cg21.workflow.with_column",
        blocker_id: "cg21.workflow.with_column.expression_unsupported",
        required_evidence: "expression_registry,semantic_conformance_suite,operator_capability_matrix,execution_certificate",
        suggested_next_action: "Use select/filter plan-only summaries until expression lowering and semantic fixtures are certified.",
        diagnostic_code: DiagnosticCode::UnsupportedSql,
        materialization_required: false,
        write_required: false,
        runtime_required: true,
    }
}

fn workflow_unsupported_group_by() -> WorkflowUnsupportedOperation {
    WorkflowUnsupportedOperation {
        operation: "group_by",
        label: "DataFrame group_by workflow",
        surface: "dataframe_group_by",
        feature: "cg21.workflow.group_by",
        blocker_id: "cg21.workflow.group_by.operator_unsupported",
        required_evidence: "grouped_aggregate_operator,semantic_conformance_suite,memory_spill_declaration,benchmark_row",
        suggested_next_action: "Use aggregate capability rows and semantic-conformance-suite output before relying on grouped aggregations.",
        diagnostic_code: DiagnosticCode::UnsupportedSql,
        materialization_required: false,
        write_required: false,
        runtime_required: true,
    }
}

fn workflow_unsupported_agg() -> WorkflowUnsupportedOperation {
    WorkflowUnsupportedOperation {
        operation: "agg",
        label: "DataFrame agg workflow",
        surface: "dataframe_agg",
        feature: "cg21.workflow.agg",
        blocker_id: "cg21.workflow.agg.operator_unsupported",
        required_evidence: "aggregate_operator_capability,grouped_aggregate_operator,semantic_conformance_suite,execution_certificate",
        suggested_next_action: "Use aggregate unsupported reports and compute-capability-matrix rows until agg execution is certified.",
        diagnostic_code: DiagnosticCode::UnsupportedSql,
        materialization_required: false,
        write_required: false,
        runtime_required: true,
    }
}

fn workflow_unsupported_sort() -> WorkflowUnsupportedOperation {
    WorkflowUnsupportedOperation {
        operation: "sort",
        label: "DataFrame sort workflow",
        surface: "dataframe_sort",
        feature: "cg21.workflow.sort",
        blocker_id: "cg21.workflow.sort.operator_unsupported",
        required_evidence: "sort_operator_capability,null_sort_ordering_semantics,memory_spill_declaration,benchmark_row",
        suggested_next_action: "Use semantic-conformance-suite for null ordering blockers before requesting sort execution.",
        diagnostic_code: DiagnosticCode::UnsupportedSql,
        materialization_required: false,
        write_required: false,
        runtime_required: true,
    }
}

fn workflow_unsupported_limit() -> WorkflowUnsupportedOperation {
    WorkflowUnsupportedOperation {
        operation: "limit",
        label: "DataFrame limit execution",
        surface: "dataframe_limit",
        feature: "cg21.workflow.limit",
        blocker_id: "cg21.workflow.limit.execution_uncertified",
        required_evidence: "limit_operator_capability,execution_certificate,benchmark_row,materialization_boundary",
        suggested_next_action: "Limit can appear in lazy plan summaries, but execution claims require certificate-backed runtime evidence.",
        diagnostic_code: DiagnosticCode::NotImplemented,
        materialization_required: false,
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
        required_evidence: "sql_parser,binder,semantic_profile,semantic_conformance_suite,operator_capability_matrix",
        suggested_next_action: "Use capability discovery for SQL posture and keep SQL text in plan-only diagnostics.",
        diagnostic_code: DiagnosticCode::UnsupportedSql,
        materialization_required: false,
        write_required: false,
        runtime_required: true,
    }
}

fn workflow_unsupported_sql_parse() -> WorkflowUnsupportedOperation {
    WorkflowUnsupportedOperation {
        operation: "sql_parse",
        label: "SQL parse",
        surface: "sql_parse",
        feature: "cg21.workflow.sql.parse",
        blocker_id: "cg21.workflow.sql.parse_unsupported",
        required_evidence: "sql_parser,sql_ast_contract,unsupported_diagnostic_snapshot",
        suggested_next_action: "Keep SQL text in unsupported diagnostics until parser coverage and AST contracts are certified.",
        diagnostic_code: DiagnosticCode::UnsupportedSql,
        materialization_required: false,
        write_required: false,
        runtime_required: false,
    }
}

fn workflow_unsupported_sql_bind() -> WorkflowUnsupportedOperation {
    WorkflowUnsupportedOperation {
        operation: "sql_bind",
        label: "SQL bind",
        surface: "sql_bind",
        feature: "cg21.workflow.sql.bind",
        blocker_id: "cg21.workflow.sql.bind_unsupported",
        required_evidence: "sql_binder,catalog_schema_contract,name_resolution_policy,semantic_conformance_suite",
        suggested_next_action: "Use schema and capability reports before binding SQL identifiers.",
        diagnostic_code: DiagnosticCode::UnsupportedSql,
        materialization_required: false,
        write_required: false,
        runtime_required: false,
    }
}

fn workflow_unsupported_sql_plan() -> WorkflowUnsupportedOperation {
    WorkflowUnsupportedOperation {
        operation: "sql_plan",
        label: "SQL plan",
        surface: "sql_plan",
        feature: "cg21.workflow.sql.plan",
        blocker_id: "cg21.workflow.sql.plan_unsupported",
        required_evidence: "sql_logical_plan_lowering,operator_capability_matrix,semantic_conformance_suite",
        suggested_next_action: "Use DataFrame/query-builder plan summaries until SQL lowering is certified.",
        diagnostic_code: DiagnosticCode::UnsupportedSql,
        materialization_required: false,
        write_required: false,
        runtime_required: false,
    }
}

fn workflow_unsupported_sql_execute() -> WorkflowUnsupportedOperation {
    WorkflowUnsupportedOperation {
        operation: "sql_execute",
        label: "SQL execute",
        surface: "sql_execute",
        feature: "cg21.workflow.sql.execute",
        blocker_id: "cg21.workflow.sql.execute_unsupported",
        required_evidence: "sql_parser,binder,planner,semantic_conformance_suite,execution_certificate,native_io_certificate",
        suggested_next_action: "Do not execute SQL through external engines; wait for ShardLoom-native SQL certification.",
        diagnostic_code: DiagnosticCode::UnsupportedSql,
        materialization_required: false,
        write_required: false,
        runtime_required: true,
    }
}

fn workflow_unsupported_source_free_sequence() -> WorkflowUnsupportedOperation {
    WorkflowUnsupportedOperation {
        operation: "source_free_sequence",
        label: "source-free sequence generator",
        surface: "source_free_generated_output",
        feature: "gar_gen_1.source_free_sequence",
        blocker_id: "gar-gen-1.sequence_runtime_not_implemented",
        required_evidence: "generator_node_contract,generated_source_certificate,output_native_io_certificate,execution_certificate,no_fallback_evidence",
        suggested_next_action: "Use ctx.range(...).write(...) for the scoped local range smoke; sequence/generate_series remains unsupported until a certified generator node contract exists.",
        diagnostic_code: DiagnosticCode::NotImplemented,
        materialization_required: false,
        write_required: false,
        runtime_required: true,
    }
}

fn workflow_unsupported_sql_values() -> WorkflowUnsupportedOperation {
    WorkflowUnsupportedOperation {
        operation: "sql_values",
        label: "SQL VALUES generated source",
        surface: "sql_generated_source",
        feature: "gar_gen_1.sql_values",
        blocker_id: "gar-gen-1.sql_values_runtime_not_implemented",
        required_evidence: "sql_parser,binder,planner,values_generator_contract,generated_source_certificate,output_native_io_certificate,no_fallback_evidence",
        suggested_next_action: "Use ctx.literal_table(...).write(...) for the scoped local smoke; SQL VALUES execution remains blocked until ShardLoom-native SQL parsing, binding, planning, and generated-source evidence are certified.",
        diagnostic_code: DiagnosticCode::UnsupportedSql,
        materialization_required: false,
        write_required: false,
        runtime_required: true,
    }
}

fn workflow_unsupported_sql_literal_select() -> WorkflowUnsupportedOperation {
    WorkflowUnsupportedOperation {
        operation: "sql_literal_select",
        label: "SQL source-free literal projection",
        surface: "sql_generated_source",
        feature: "gar_gen_1.sql_source_free_projection",
        blocker_id: "gar-gen-1.sql_source_free_projection_runtime_not_implemented",
        required_evidence: "sql_parser,binder,planner,source_free_projection_contract,generated_source_certificate,output_native_io_certificate,no_fallback_evidence",
        suggested_next_action: "Use generated-source Python smokes for local output; SQL source-free SELECT execution remains blocked until SQL frontend evidence is certified.",
        diagnostic_code: DiagnosticCode::UnsupportedSql,
        materialization_required: false,
        write_required: false,
        runtime_required: true,
    }
}

fn workflow_unsupported_dataframe_source_free_projection() -> WorkflowUnsupportedOperation {
    WorkflowUnsupportedOperation {
        operation: "dataframe_source_free_projection",
        label: "DataFrame source-free projection",
        surface: "dataframe_generated_source",
        feature: "gar_gen_1.dataframe_source_free_projection",
        blocker_id: "gar-gen-1.dataframe_source_free_projection_runtime_not_implemented",
        required_evidence: "dataframe_plan_contract,expression_registry,generated_source_certificate,output_native_io_certificate,execution_certificate,no_fallback_evidence",
        suggested_next_action: "Use ctx.from_rows(...).write(...), ctx.literal_table(...).write(...), ctx.range(...).write(...), or ctx.calendar(...).write(...) for scoped local generated-output smokes.",
        diagnostic_code: DiagnosticCode::NotImplemented,
        materialization_required: false,
        write_required: false,
        runtime_required: true,
    }
}

fn workflow_unsupported_dataframe_generated_with_column() -> WorkflowUnsupportedOperation {
    WorkflowUnsupportedOperation {
        operation: "dataframe_generated_with_column",
        label: "DataFrame generated with_column expression",
        surface: "dataframe_generated_source",
        feature: "gar_gen_1.dataframe_generated_with_column",
        blocker_id: "gar-gen-1.dataframe_generated_with_column_runtime_not_implemented",
        required_evidence: "dataframe_plan_contract,expression_registry,type_coercion_contract,generated_source_certificate,output_native_io_certificate,execution_certificate,no_fallback_evidence",
        suggested_next_action: "Use literal/user rows that already contain the desired values; generated expression columns remain blocked until expression lowering and evidence are certified.",
        diagnostic_code: DiagnosticCode::NotImplemented,
        materialization_required: false,
        write_required: false,
        runtime_required: true,
    }
}

fn workflow_unsupported_object_store_generated_output() -> WorkflowUnsupportedOperation {
    WorkflowUnsupportedOperation {
        operation: "object_store_generated_output",
        label: "object-store generated-output write",
        surface: "object_store_generated_output_sink",
        feature: "gar_gen_1.object_store_generated_output",
        blocker_id: "gar-gen-1.object_store_generated_output_blocked",
        required_evidence: "credential_policy,object_store_write_policy,output_commit_protocol,output_native_io_certificate,generated_source_certificate,no_fallback_evidence",
        suggested_next_action: "Write generated output to a local sink first; object-store generated-output writes require a separately admitted object-store runtime and commit protocol.",
        diagnostic_code: DiagnosticCode::ObjectStoreUnsupported,
        materialization_required: false,
        write_required: true,
        runtime_required: true,
    }
}

fn workflow_unsupported_foundry_generated_output() -> WorkflowUnsupportedOperation {
    WorkflowUnsupportedOperation {
        operation: "foundry_generated_output",
        label: "Foundry generated-output transform",
        surface: "foundry_generated_output_sink",
        feature: "gar_gen_1.foundry_generated_output",
        blocker_id: "gar-gen-1.foundry_generated_output_runtime_not_implemented",
        required_evidence: "foundry_transform_wrapper,foundry_output_dataset_evidence,generated_source_certificate,output_native_io_certificate,foundry_spark_invoked_false,no_fallback_evidence",
        suggested_next_action: "Keep Foundry generated-output work in local proof/report-only docs until a real Foundry transform writes output and evidence datasets without Spark fallback.",
        diagnostic_code: DiagnosticCode::NotImplemented,
        materialization_required: false,
        write_required: true,
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

fn workflow_unsupported_schema_discovery() -> WorkflowUnsupportedOperation {
    WorkflowUnsupportedOperation {
        operation: "schema",
        label: "workflow schema discovery",
        surface: "schema_discovery",
        feature: "cg21.workflow.schema",
        blocker_id: "cg21.workflow.schema.discovery_unsupported",
        required_evidence: "schema_metadata_report,input_adapter_certificate,native_io_certificate",
        suggested_next_action: "Use input-plan and schema-plan report surfaces until workflow schema discovery is certified.",
        diagnostic_code: DiagnosticCode::NotImplemented,
        materialization_required: false,
        write_required: false,
        runtime_required: false,
    }
}

fn workflow_unsupported_describe_schema() -> WorkflowUnsupportedOperation {
    WorkflowUnsupportedOperation {
        operation: "describe_schema",
        label: "workflow schema description",
        surface: "schema_description",
        feature: "cg21.workflow.describe_schema",
        blocker_id: "cg21.workflow.describe_schema.report_unsupported",
        required_evidence: "schema_metadata_report,schema_evolution_report,input_adapter_certificate",
        suggested_next_action: "Use schema-plan for report-only schema posture before requesting rich schema descriptions.",
        diagnostic_code: DiagnosticCode::NotImplemented,
        materialization_required: false,
        write_required: false,
        runtime_required: false,
    }
}

fn workflow_unsupported_validate_schema() -> WorkflowUnsupportedOperation {
    WorkflowUnsupportedOperation {
        operation: "validate_schema",
        label: "workflow schema validation",
        surface: "schema_validation",
        feature: "cg21.workflow.validate_schema",
        blocker_id: "cg21.workflow.validate_schema.validation_unsupported",
        required_evidence: "schema_contract,validation_certificate,table_compatibility_report",
        suggested_next_action: "Use schema-plan compatibility reports until workflow schema validation is certified.",
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

fn workflow_unsupported_data_quality_summary() -> WorkflowUnsupportedOperation {
    WorkflowUnsupportedOperation {
        operation: "data_quality_summary",
        label: "data-quality summary reporting",
        surface: "data_quality_summary",
        feature: "cg21.workflow.data_quality_summary",
        blocker_id: "cg21.workflow.data_quality_summary.report_unsupported",
        required_evidence: "quality_rule_contract,quality_summary_schema,correctness_harness",
        suggested_next_action: "Use data-quality unsupported reports and correctness harness plans before requesting quality summaries.",
        diagnostic_code: DiagnosticCode::NotImplemented,
        materialization_required: false,
        write_required: false,
        runtime_required: true,
    }
}

fn workflow_unsupported_quarantine() -> WorkflowUnsupportedOperation {
    WorkflowUnsupportedOperation {
        operation: "quarantine",
        label: "data-quality quarantine output",
        surface: "quarantine_output",
        feature: "cg21.workflow.quarantine",
        blocker_id: "cg21.workflow.quarantine.output_unsupported",
        required_evidence: "quality_rule_contract,quarantine_manifest,write_intent,recovery_certificate",
        suggested_next_action: "Use report-only quality diagnostics until quarantine output manifests and write policies are certified.",
        diagnostic_code: DiagnosticCode::UnsupportedEffect,
        materialization_required: true,
        write_required: true,
        runtime_required: true,
    }
}

fn workflow_unsupported_preview() -> WorkflowUnsupportedOperation {
    WorkflowUnsupportedOperation {
        operation: "preview",
        label: "notebook preview materialization",
        surface: "notebook_preview",
        feature: "cg21.workflow.preview",
        blocker_id: "cg21.workflow.preview.materialization_unsupported",
        required_evidence: "preview_materialization_policy,decoded_boundary_report,notebook_display_boundary",
        suggested_next_action: "Use explain and estimate reports until bounded preview materialization is certified.",
        diagnostic_code: DiagnosticCode::MaterializationRequired,
        materialization_required: true,
        write_required: false,
        runtime_required: true,
    }
}

fn workflow_unsupported_display() -> WorkflowUnsupportedOperation {
    WorkflowUnsupportedOperation {
        operation: "display",
        label: "notebook rich display",
        surface: "notebook_display",
        feature: "cg21.workflow.display",
        blocker_id: "cg21.workflow.display.rich_display_unsupported",
        required_evidence: "notebook_display_boundary,diagnostic_rendering_contract,materialization_policy",
        suggested_next_action: "Use explicit CLI/Python report objects; rich notebook display must not hide unsupported behavior.",
        diagnostic_code: DiagnosticCode::MaterializationRequired,
        materialization_required: true,
        write_required: false,
        runtime_required: true,
    }
}

fn workflow_unsupported_object_store_read() -> WorkflowUnsupportedOperation {
    WorkflowUnsupportedOperation {
        operation: "object_store_read",
        label: "remote object-store workflow read",
        surface: "object_store_source",
        feature: "cg21.workflow.object_store_read",
        blocker_id: "cg21.workflow.object_store_read.runtime_unsupported",
        required_evidence: "object_store_capability_policy,credential_policy,native_io_certificate,execution_certificate",
        suggested_next_action: "Use object-store request planning reports until remote object-store reads are certified.",
        diagnostic_code: DiagnosticCode::ObjectStoreUnsupported,
        materialization_required: false,
        write_required: false,
        runtime_required: true,
    }
}

fn workflow_unsupported_fallback_engine() -> WorkflowUnsupportedOperation {
    WorkflowUnsupportedOperation {
        operation: "fallback_engine",
        label: "external fallback engine workflow execution",
        surface: "fallback_policy",
        feature: "cg21.workflow.fallback_engine",
        blocker_id: "cg21.workflow.fallback_engine.no_fallback_policy",
        required_evidence: "no_fallback_policy,execution_certificate,native_operator_coverage",
        suggested_next_action: "Use ShardLoom-native capability and execution-certificate reports; fallback engines are not execution paths.",
        diagnostic_code: DiagnosticCode::NoFallbackExecution,
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
    push_field(&mut fields, "support_status", "unsupported");
    push_field(&mut fields, "unsupported_status", "unsupported");
    push_field(&mut fields, "claim_gate_status", "not_claim_grade");
    push_field(
        &mut fields,
        "required_evidence",
        operation.required_evidence,
    );
    push_field(
        &mut fields,
        "diagnostic_code",
        operation.diagnostic_code.as_str(),
    );
    push_field(
        &mut fields,
        "diagnostic_category",
        workflow_unsupported_diagnostic_category(operation).as_str(),
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
    push_bool_field(&mut fields, "side_effect_free", true);
    push_field(&mut fields, "execution", "not_performed");
    push_bool_field(&mut fields, "parser_executed", false);
    push_bool_field(&mut fields, "binder_executed", false);
    push_bool_field(&mut fields, "planner_executed", false);
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
    push_bool_field(&mut fields, "no_runtime", true);
    push_bool_field(&mut fields, "no_fallback", true);
    push_bool_field(&mut fields, "no_effects", true);
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
    let cdc_manifest_transaction_gate = plan_cdc_manifest_transaction_gate();
    let catalog_metadata_integration_gate = plan_catalog_metadata_integration_gate();
    let table_maintenance_execution_matrix = plan_table_maintenance_execution_matrix();
    let has_errors = report.has_errors()
        || cdc_manifest_transaction_gate.has_errors()
        || catalog_metadata_integration_gate.has_errors()
        || table_maintenance_execution_matrix.has_errors();
    let mut diagnostics = report.diagnostics.clone();
    diagnostics.extend(cdc_manifest_transaction_gate.diagnostics.clone());
    diagnostics.extend(catalog_metadata_integration_gate.diagnostics.clone());
    diagnostics.extend(table_maintenance_execution_matrix.diagnostics.clone());
    let status = if has_errors {
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
        diagnostics,
        table_intelligence_output_fields(
            &report,
            &cdc_manifest_transaction_gate,
            &catalog_metadata_integration_gate,
            &table_maintenance_execution_matrix,
        ),
    );
    if has_errors {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

fn table_intelligence_output_fields(
    report: &TableIntelligenceReport,
    cdc_manifest_transaction_gate: &CdcManifestTransactionGateReport,
    catalog_metadata_integration_gate: &CatalogMetadataIntegrationGateReport,
    table_maintenance_execution_matrix: &TableMaintenanceExecutionMatrixReport,
) -> Vec<(String, String)> {
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
    push_count_field(
        &mut fields,
        "diagnostic_count",
        report.diagnostics.len()
            + cdc_manifest_transaction_gate.diagnostics.len()
            + catalog_metadata_integration_gate.diagnostics.len()
            + table_maintenance_execution_matrix.diagnostics.len(),
    );
    append_cdc_manifest_transaction_gate_fields(
        &mut fields,
        "cdc_manifest_transaction_gate",
        cdc_manifest_transaction_gate,
    );
    append_catalog_metadata_integration_gate_prefixed_fields(
        &mut fields,
        "catalog_metadata_integration_gate",
        catalog_metadata_integration_gate,
    );
    append_table_maintenance_execution_matrix_fields(
        &mut fields,
        "table_maintenance_execution_matrix",
        table_maintenance_execution_matrix,
    );
    push_field(&mut fields, "execution", "not_performed");
    push_field(&mut fields, "plan_only", "true");
    fields
}

fn append_table_maintenance_execution_matrix_fields(
    fields: &mut Vec<(String, String)>,
    prefix: &str,
    report: &TableMaintenanceExecutionMatrixReport,
) {
    push_field(
        fields,
        &format!("{prefix}_schema_version"),
        report.schema_version,
    );
    push_field(fields, &format!("{prefix}_report_id"), report.report_id);
    push_field(fields, &format!("{prefix}_gar_id"), report.gar_id);
    push_field(
        fields,
        &format!("{prefix}_support_status"),
        report.support_status,
    );
    push_field(
        fields,
        &format!("{prefix}_claim_gate_status"),
        report.claim_gate_status,
    );
    push_field(
        fields,
        &format!("{prefix}_claim_boundary"),
        report.claim_boundary,
    );
    push_count_field(
        fields,
        &format!("{prefix}_operation_count"),
        report.operation_count(),
    );
    push_count_field(
        fields,
        &format!("{prefix}_report_only_operation_count"),
        report.report_only_operation_count(),
    );
    push_count_field(
        fields,
        &format!("{prefix}_unsupported_operation_count"),
        report.unsupported_operation_count(),
    );
    push_field(
        fields,
        &format!("{prefix}_operation_order"),
        &report.operation_order().join(","),
    );
    push_field(
        fields,
        &format!("{prefix}_family_order"),
        &report.family_order().join(","),
    );
    push_field(
        fields,
        &format!("{prefix}_existing_report_refs"),
        &report.existing_report_refs.join(","),
    );
    append_table_maintenance_execution_matrix_evidence_fields(fields, prefix, report);
    append_table_maintenance_execution_matrix_requirement_fields(fields, prefix, report);
    append_table_maintenance_execution_matrix_boundary_fields(fields, prefix, report);
    for row in &report.rows {
        append_table_maintenance_execution_matrix_row_fields(fields, prefix, row);
    }
}

fn append_table_maintenance_execution_matrix_evidence_fields(
    fields: &mut Vec<(String, String)>,
    prefix: &str,
    report: &TableMaintenanceExecutionMatrixReport,
) {
    push_bool_field(
        fields,
        &format!("{prefix}_delete_tombstone_compatibility_report_present"),
        report.delete_tombstone_compatibility_report_present,
    );
    push_bool_field(
        fields,
        &format!("{prefix}_cdc_incremental_planning_present"),
        report.cdc_incremental_planning_present,
    );
    push_bool_field(
        fields,
        &format!("{prefix}_layout_compaction_planning_present"),
        report.layout_compaction_planning_present,
    );
    push_bool_field(
        fields,
        &format!("{prefix}_local_metadata_smoke_present"),
        report.local_metadata_smoke_present,
    );
    push_bool_field(
        fields,
        &format!("{prefix}_local_delete_tombstone_smoke_present"),
        report.local_delete_tombstone_smoke_present,
    );
    push_bool_field(
        fields,
        &format!("{prefix}_local_append_only_cdc_overlay_smoke_present"),
        report.local_append_only_cdc_overlay_smoke_present,
    );
}

fn append_table_maintenance_execution_matrix_requirement_fields(
    fields: &mut Vec<(String, String)>,
    prefix: &str,
    report: &TableMaintenanceExecutionMatrixReport,
) {
    push_bool_field(
        fields,
        &format!("{prefix}_fixture_metadata_required"),
        report.fixture_metadata_required,
    );
    push_bool_field(
        fields,
        &format!("{prefix}_row_identity_required"),
        report.row_identity_required,
    );
    push_bool_field(
        fields,
        &format!("{prefix}_delete_tombstone_policy_required"),
        report.delete_tombstone_policy_required,
    );
    push_bool_field(
        fields,
        &format!("{prefix}_commit_semantics_required"),
        report.commit_semantics_required,
    );
    push_bool_field(
        fields,
        &format!("{prefix}_table_metadata_schema_required"),
        report.table_metadata_schema_required,
    );
    push_bool_field(
        fields,
        &format!("{prefix}_execution_certificate_required"),
        report.execution_certificate_required,
    );
    push_bool_field(
        fields,
        &format!("{prefix}_native_io_certificate_required"),
        report.native_io_certificate_required,
    );
    push_bool_field(
        fields,
        &format!("{prefix}_materialization_decode_evidence_required"),
        report.materialization_decode_evidence_required,
    );
    push_bool_field(
        fields,
        &format!("{prefix}_no_fallback_policy_required"),
        report.no_fallback_policy_required,
    );
}

fn append_table_maintenance_execution_matrix_boundary_fields(
    fields: &mut Vec<(String, String)>,
    prefix: &str,
    report: &TableMaintenanceExecutionMatrixReport,
) {
    append_table_maintenance_execution_matrix_allowed_fields(fields, prefix, report);
    append_table_maintenance_execution_matrix_status_fields(fields, prefix, report);
}

fn append_table_maintenance_execution_matrix_allowed_fields(
    fields: &mut Vec<(String, String)>,
    prefix: &str,
    report: &TableMaintenanceExecutionMatrixReport,
) {
    push_bool_field(
        fields,
        &format!("{prefix}_runtime_execution_allowed"),
        report.runtime_execution_allowed,
    );
    push_bool_field(
        fields,
        &format!("{prefix}_delete_tombstone_execution_allowed"),
        report.delete_tombstone_execution_allowed,
    );
    push_bool_field(
        fields,
        &format!("{prefix}_cdc_execution_allowed"),
        report.cdc_execution_allowed,
    );
    push_bool_field(
        fields,
        &format!("{prefix}_maintenance_write_allowed"),
        report.maintenance_write_allowed,
    );
    push_bool_field(
        fields,
        &format!("{prefix}_catalog_io_allowed"),
        report.catalog_io_allowed,
    );
    push_bool_field(
        fields,
        &format!("{prefix}_table_metadata_io_allowed"),
        report.table_metadata_io_allowed,
    );
    push_bool_field(
        fields,
        &format!("{prefix}_data_io_allowed"),
        report.data_io_allowed,
    );
    push_bool_field(
        fields,
        &format!("{prefix}_object_store_io_allowed"),
        report.object_store_io_allowed,
    );
    push_bool_field(
        fields,
        &format!("{prefix}_write_io_allowed"),
        report.write_io_allowed,
    );
    push_bool_field(
        fields,
        &format!("{prefix}_fallback_attempted"),
        report.fallback_attempted,
    );
    push_bool_field(
        fields,
        &format!("{prefix}_fallback_execution_allowed"),
        report.fallback_execution_allowed,
    );
    push_bool_field(
        fields,
        &format!("{prefix}_external_engine_invoked"),
        report.external_engine_invoked,
    );
    push_bool_field(
        fields,
        &format!("{prefix}_table_format_execution_claim_allowed"),
        report.table_format_execution_claim_allowed,
    );
}

fn append_table_maintenance_execution_matrix_status_fields(
    fields: &mut Vec<(String, String)>,
    prefix: &str,
    report: &TableMaintenanceExecutionMatrixReport,
) {
    push_bool_field(
        fields,
        &format!("{prefix}_runtime_promotions_blocked"),
        report.runtime_promotions_blocked(),
    );
    push_bool_field(
        fields,
        &format!("{prefix}_claim_blocked"),
        report.claim_blocked(),
    );
    push_bool_field(
        fields,
        &format!("{prefix}_deterministic_unsupported_diagnostics_ready"),
        report.deterministic_unsupported_diagnostics_ready(),
    );
    push_count_field(
        fields,
        &format!("{prefix}_unsupported_diagnostic_count"),
        report.unsupported_diagnostic_count(),
    );
    push_field(
        fields,
        &format!("{prefix}_unsupported_diagnostic_code_order"),
        &report.unsupported_diagnostic_code_order().join(","),
    );
    push_field(
        fields,
        &format!("{prefix}_unsupported_diagnostic_category_order"),
        &report.unsupported_diagnostic_category_order().join(","),
    );
    push_field(
        fields,
        &format!("{prefix}_unsupported_diagnostic_severity_order"),
        &report.unsupported_diagnostic_severity_order().join(","),
    );
    push_bool_field(
        fields,
        &format!("{prefix}_side_effect_free"),
        report.side_effect_free(),
    );
    push_count_field(
        fields,
        &format!("{prefix}_diagnostic_count"),
        report.diagnostics.len(),
    );
}

fn append_table_maintenance_execution_matrix_row_fields(
    fields: &mut Vec<(String, String)>,
    prefix: &str,
    row: &TableMaintenanceExecutionMatrixRow,
) {
    let row_prefix = format!("{prefix}_row_{}", row.operation.as_str());
    push_field(fields, &format!("{row_prefix}_family"), row.family.as_str());
    push_field(fields, &format!("{row_prefix}_status"), row.status.as_str());
    push_field(
        fields,
        &format!("{row_prefix}_existing_report_ref"),
        row.existing_report_ref.unwrap_or("none"),
    );
    push_field(
        fields,
        &format!("{row_prefix}_required_fixture"),
        row.required_fixture,
    );
    push_field(
        fields,
        &format!("{row_prefix}_required_commit_semantics"),
        row.required_commit_semantics,
    );
    push_field(
        fields,
        &format!("{row_prefix}_required_evidence"),
        row.required_evidence,
    );
    push_bool_field(
        fields,
        &format!("{row_prefix}_report_only_available"),
        row.report_only_available,
    );
    push_bool_field(
        fields,
        &format!("{row_prefix}_runtime_execution_allowed"),
        row.runtime_execution_allowed,
    );
    push_bool_field(
        fields,
        &format!("{row_prefix}_delete_tombstone_execution_allowed"),
        row.delete_tombstone_execution_allowed,
    );
    push_bool_field(
        fields,
        &format!("{row_prefix}_cdc_execution_allowed"),
        row.cdc_execution_allowed,
    );
    push_bool_field(
        fields,
        &format!("{row_prefix}_maintenance_write_allowed"),
        row.maintenance_write_allowed,
    );
    push_bool_field(fields, &format!("{row_prefix}_catalog_io"), row.catalog_io);
    push_bool_field(
        fields,
        &format!("{row_prefix}_table_metadata_io"),
        row.table_metadata_io,
    );
    push_bool_field(fields, &format!("{row_prefix}_data_io"), row.data_io);
    push_bool_field(
        fields,
        &format!("{row_prefix}_object_store_io"),
        row.object_store_io,
    );
    push_bool_field(fields, &format!("{row_prefix}_write_io"), row.write_io);
    push_bool_field(
        fields,
        &format!("{row_prefix}_fallback_attempted"),
        row.fallback_attempted,
    );
    push_bool_field(
        fields,
        &format!("{row_prefix}_fallback_execution_allowed"),
        row.fallback_execution_allowed,
    );
    push_bool_field(
        fields,
        &format!("{row_prefix}_external_engine_invoked"),
        row.external_engine_invoked,
    );
    push_bool_field(
        fields,
        &format!("{row_prefix}_side_effect_free"),
        row.side_effect_free(),
    );
    push_field(
        fields,
        &format!("{row_prefix}_support_status"),
        row.support_status,
    );
    push_field(
        fields,
        &format!("{row_prefix}_claim_gate_status"),
        row.claim_gate_status,
    );
}

fn append_cdc_manifest_transaction_gate_fields(
    fields: &mut Vec<(String, String)>,
    prefix: &str,
    report: &CdcManifestTransactionGateReport,
) {
    push_field(
        fields,
        &format!("{prefix}_schema_version"),
        report.schema_version,
    );
    push_field(fields, &format!("{prefix}_report_id"), report.report_id);
    push_count_field(
        fields,
        &format!("{prefix}_surface_count"),
        report.surface_count(),
    );
    push_count_field(
        fields,
        &format!("{prefix}_report_only_surface_count"),
        report.report_only_surface_count(),
    );
    push_count_field(
        fields,
        &format!("{prefix}_unsupported_surface_count"),
        report.unsupported_surface_count(),
    );
    push_field(
        fields,
        &format!("{prefix}_surface_order"),
        &report.surface_order().join(","),
    );
    push_field(
        fields,
        &format!("{prefix}_existing_report_refs"),
        &report.existing_report_refs.join(","),
    );
    append_cdc_manifest_transaction_gate_evidence_fields(fields, prefix, report);
    append_cdc_manifest_transaction_gate_requirement_fields(fields, prefix, report);
    append_cdc_manifest_transaction_gate_status_fields(fields, prefix, report);
    for entry in &report.entries {
        append_cdc_manifest_transaction_gate_entry_fields(fields, prefix, entry);
    }
}

fn append_cdc_manifest_transaction_gate_evidence_fields(
    fields: &mut Vec<(String, String)>,
    prefix: &str,
    report: &CdcManifestTransactionGateReport,
) {
    push_bool_field(
        fields,
        &format!("{prefix}_existing_cdc_planning_present"),
        report.existing_cdc_planning_present,
    );
    push_bool_field(
        fields,
        &format!("{prefix}_existing_manifest_contract_present"),
        report.existing_manifest_contract_present,
    );
    push_bool_field(
        fields,
        &format!("{prefix}_existing_object_store_commit_protocol_present"),
        report.existing_object_store_commit_protocol_present,
    );
    push_bool_field(
        fields,
        &format!("{prefix}_existing_local_staged_manifest_helpers_present"),
        report.existing_local_staged_manifest_helpers_present,
    );
}

fn append_cdc_manifest_transaction_gate_requirement_fields(
    fields: &mut Vec<(String, String)>,
    prefix: &str,
    report: &CdcManifestTransactionGateReport,
) {
    push_bool_field(
        fields,
        &format!("{prefix}_snapshot_pair_required"),
        report.snapshot_pair_required,
    );
    push_bool_field(
        fields,
        &format!("{prefix}_row_identity_required"),
        report.row_identity_required,
    );
    push_bool_field(
        fields,
        &format!("{prefix}_manifest_schema_required"),
        report.manifest_schema_required,
    );
    push_bool_field(
        fields,
        &format!("{prefix}_manifest_serialization_evidence_required"),
        report.manifest_serialization_evidence_required,
    );
    push_bool_field(
        fields,
        &format!("{prefix}_transaction_protocol_required"),
        report.transaction_protocol_required,
    );
    push_bool_field(
        fields,
        &format!("{prefix}_commit_protocol_required"),
        report.commit_protocol_required,
    );
    push_bool_field(
        fields,
        &format!("{prefix}_object_store_provider_evidence_required"),
        report.object_store_provider_evidence_required,
    );
    push_bool_field(
        fields,
        &format!("{prefix}_execution_certificate_required"),
        report.execution_certificate_required,
    );
    push_bool_field(
        fields,
        &format!("{prefix}_native_io_certificate_required"),
        report.native_io_certificate_required,
    );
    push_bool_field(
        fields,
        &format!("{prefix}_no_fallback_policy_required"),
        report.no_fallback_policy_required,
    );
}

fn append_cdc_manifest_transaction_gate_status_fields(
    fields: &mut Vec<(String, String)>,
    prefix: &str,
    report: &CdcManifestTransactionGateReport,
) {
    append_cdc_manifest_transaction_gate_runtime_status_fields(fields, prefix, report);
    append_cdc_manifest_transaction_gate_policy_status_fields(fields, prefix, report);
    append_cdc_manifest_transaction_gate_diagnostic_status_fields(fields, prefix, report);
}

fn append_cdc_manifest_transaction_gate_runtime_status_fields(
    fields: &mut Vec<(String, String)>,
    prefix: &str,
    report: &CdcManifestTransactionGateReport,
) {
    push_bool_field(
        fields,
        &format!("{prefix}_cdc_read_intent_report_only_available"),
        report.cdc_read_intent_report_only_available,
    );
    push_bool_field(
        fields,
        &format!("{prefix}_cdc_write_intent_allowed"),
        report.cdc_write_intent_allowed,
    );
    push_bool_field(
        fields,
        &format!("{prefix}_manifest_serialization_allowed"),
        report.manifest_serialization_allowed,
    );
    push_bool_field(
        fields,
        &format!("{prefix}_manifest_metadata_read_allowed"),
        report.manifest_metadata_read_allowed,
    );
    push_bool_field(
        fields,
        &format!("{prefix}_transaction_execution_allowed"),
        report.transaction_execution_allowed,
    );
    push_bool_field(
        fields,
        &format!("{prefix}_commit_execution_allowed"),
        report.commit_execution_allowed,
    );
    push_bool_field(
        fields,
        &format!("{prefix}_object_store_io_allowed"),
        report.object_store_io_allowed,
    );
    push_bool_field(
        fields,
        &format!("{prefix}_data_read_allowed"),
        report.data_read_allowed,
    );
    push_bool_field(
        fields,
        &format!("{prefix}_write_io_allowed"),
        report.write_io_allowed,
    );
}

fn append_cdc_manifest_transaction_gate_policy_status_fields(
    fields: &mut Vec<(String, String)>,
    prefix: &str,
    report: &CdcManifestTransactionGateReport,
) {
    push_bool_field(
        fields,
        &format!("{prefix}_runtime_promotions_blocked"),
        report.runtime_promotions_blocked(),
    );
    push_bool_field(
        fields,
        &format!("{prefix}_claim_blocked"),
        report.claim_blocked(),
    );
    push_bool_field(
        fields,
        &format!("{prefix}_side_effect_free"),
        report.side_effect_free(),
    );
    push_bool_field(
        fields,
        &format!("{prefix}_fallback_attempted"),
        report.fallback_attempted,
    );
    push_bool_field(
        fields,
        &format!("{prefix}_fallback_execution_allowed"),
        report.fallback_execution_allowed,
    );
    push_bool_field(
        fields,
        &format!("{prefix}_external_engine_invoked"),
        report.external_engine_invoked,
    );
    push_bool_field(
        fields,
        &format!("{prefix}_cdc_transaction_claim_allowed"),
        report.cdc_transaction_claim_allowed,
    );
    push_field(
        fields,
        &format!("{prefix}_claim_gate_status"),
        report.claim_gate_status,
    );
}

fn append_cdc_manifest_transaction_gate_diagnostic_status_fields(
    fields: &mut Vec<(String, String)>,
    prefix: &str,
    report: &CdcManifestTransactionGateReport,
) {
    push_bool_field(
        fields,
        &format!("{prefix}_deterministic_unsupported_diagnostics_ready"),
        report.deterministic_unsupported_diagnostics_ready(),
    );
    push_count_field(
        fields,
        &format!("{prefix}_unsupported_diagnostic_count"),
        report.unsupported_diagnostic_count(),
    );
    push_field(
        fields,
        &format!("{prefix}_unsupported_diagnostic_code_order"),
        &report.unsupported_diagnostic_code_order().join(","),
    );
    push_field(
        fields,
        &format!("{prefix}_unsupported_diagnostic_category_order"),
        &report.unsupported_diagnostic_category_order().join(","),
    );
    push_field(
        fields,
        &format!("{prefix}_unsupported_diagnostic_severity_order"),
        &report.unsupported_diagnostic_severity_order().join(","),
    );
    push_count_field(
        fields,
        &format!("{prefix}_diagnostic_count"),
        report.diagnostics.len(),
    );
}

fn append_cdc_manifest_transaction_gate_entry_fields(
    fields: &mut Vec<(String, String)>,
    prefix: &str,
    entry: &CdcManifestTransactionGateEntry,
) {
    let row_prefix = format!("{prefix}_row_{}", entry.surface.as_str());
    push_field(
        fields,
        &format!("{row_prefix}_status"),
        entry.status.as_str(),
    );
    push_field(
        fields,
        &format!("{row_prefix}_existing_report_ref"),
        entry.existing_report_ref.unwrap_or("none"),
    );
    push_field(
        fields,
        &format!("{row_prefix}_required_evidence"),
        entry.required_evidence,
    );
    push_bool_field(
        fields,
        &format!("{row_prefix}_report_only_available"),
        entry.report_only_available,
    );
    push_bool_field(
        fields,
        &format!("{row_prefix}_manifest_serialization_allowed"),
        entry.manifest_serialization_allowed,
    );
    push_bool_field(
        fields,
        &format!("{row_prefix}_manifest_metadata_read_allowed"),
        entry.manifest_metadata_read_allowed,
    );
    push_bool_field(
        fields,
        &format!("{row_prefix}_transaction_execution_allowed"),
        entry.transaction_execution_allowed,
    );
    push_bool_field(
        fields,
        &format!("{row_prefix}_commit_execution_allowed"),
        entry.commit_execution_allowed,
    );
    push_bool_field(fields, &format!("{row_prefix}_data_read"), entry.data_read);
    push_bool_field(
        fields,
        &format!("{row_prefix}_object_store_io"),
        entry.object_store_io,
    );
    push_bool_field(fields, &format!("{row_prefix}_write_io"), entry.write_io);
    push_bool_field(
        fields,
        &format!("{row_prefix}_fallback_attempted"),
        entry.fallback_attempted,
    );
    push_bool_field(
        fields,
        &format!("{row_prefix}_fallback_execution_allowed"),
        entry.fallback_execution_allowed,
    );
    push_bool_field(
        fields,
        &format!("{row_prefix}_external_engine_invoked"),
        entry.external_engine_invoked,
    );
    push_bool_field(
        fields,
        &format!("{row_prefix}_side_effect_free"),
        entry.side_effect_free(),
    );
    push_field(
        fields,
        &format!("{row_prefix}_claim_gate_status"),
        entry.claim_gate_status,
    );
}

pub(crate) fn catalog_metadata_integration_gate_fields(
    report: &CatalogMetadataIntegrationGateReport,
) -> Vec<(String, String)> {
    let mut fields = vec![];
    push_field(&mut fields, "mode", "catalog_metadata_integration_gate");
    push_field(&mut fields, "schema_version", report.schema_version);
    push_field(&mut fields, "report_id", report.report_id);
    push_field(&mut fields, "gar_id", report.gar_id);
    push_field(&mut fields, "gate_status", report.gate_status);
    push_field(&mut fields, "support_status", report.support_status);
    push_field(&mut fields, "claim_gate_status", report.claim_gate_status);
    push_field(&mut fields, "claim_boundary", report.claim_boundary);
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
    push_count_field(
        &mut fields,
        "unsupported_surface_count",
        report.unsupported_surface_count(),
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
    for entry in &report.entries {
        append_catalog_metadata_integration_gate_entry_fields(&mut fields, "", entry);
    }
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
    push_bool_field(
        fields,
        "local_manifest_table_metadata_smoke_supported",
        report.local_manifest_table_metadata_smoke_supported,
    );
    push_field(
        fields,
        "local_manifest_table_metadata_smoke_command",
        report.local_manifest_table_metadata_smoke_command,
    );
    push_field(
        fields,
        "local_manifest_table_metadata_smoke_report_ref",
        report.local_manifest_table_metadata_smoke_report_ref,
    );
    push_field(
        fields,
        "local_manifest_table_metadata_smoke_claim_gate_status",
        report.local_manifest_table_metadata_smoke_claim_gate_status,
    );
    push_field(
        fields,
        "local_manifest_table_metadata_smoke_claim_boundary",
        report.local_manifest_table_metadata_smoke_claim_boundary,
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
    push_bool_field(
        fields,
        "external_engine_invoked",
        report.external_engine_invoked,
    );
    push_field(fields, "support_status", report.support_status);
    push_field(fields, "claim_gate_status", report.claim_gate_status);
    push_field(fields, "claim_boundary", report.claim_boundary);
    push_bool_field(
        fields,
        "deterministic_unsupported_diagnostics_ready",
        report.deterministic_unsupported_diagnostics_ready(),
    );
    push_count_field(
        fields,
        "unsupported_diagnostic_count",
        report.unsupported_diagnostic_count(),
    );
    push_field(
        fields,
        "unsupported_diagnostic_code_order",
        &report.unsupported_diagnostic_code_order().join(","),
    );
    push_field(
        fields,
        "unsupported_diagnostic_category_order",
        &report.unsupported_diagnostic_category_order().join(","),
    );
    push_field(
        fields,
        "unsupported_diagnostic_severity_order",
        &report.unsupported_diagnostic_severity_order().join(","),
    );
    push_bool_field(fields, "side_effect_free", report.side_effect_free());
    push_count_field(fields, "diagnostic_count", report.diagnostics.len());
    push_field(fields, "execution", "not_performed");
    push_field(fields, "plan_only", "true");
}

fn append_catalog_metadata_integration_gate_prefixed_fields(
    fields: &mut Vec<(String, String)>,
    prefix: &str,
    report: &CatalogMetadataIntegrationGateReport,
) {
    push_field(
        fields,
        &format!("{prefix}_schema_version"),
        report.schema_version,
    );
    push_field(fields, &format!("{prefix}_report_id"), report.report_id);
    push_field(fields, &format!("{prefix}_gar_id"), report.gar_id);
    push_field(fields, &format!("{prefix}_gate_status"), report.gate_status);
    push_field(
        fields,
        &format!("{prefix}_support_status"),
        report.support_status,
    );
    push_field(
        fields,
        &format!("{prefix}_claim_gate_status"),
        report.claim_gate_status,
    );
    push_field(
        fields,
        &format!("{prefix}_claim_boundary"),
        report.claim_boundary,
    );
    push_count_field(
        fields,
        &format!("{prefix}_surface_count"),
        report.surface_count(),
    );
    push_count_field(
        fields,
        &format!("{prefix}_existing_evidence_surface_count"),
        report.existing_evidence_surface_count(),
    );
    push_count_field(
        fields,
        &format!("{prefix}_blocked_surface_count"),
        report.blocked_surface_count(),
    );
    push_count_field(
        fields,
        &format!("{prefix}_unsupported_surface_count"),
        report.unsupported_surface_count(),
    );
    push_field(
        fields,
        &format!("{prefix}_surface_order"),
        &report.surface_order().join(","),
    );
    push_field(
        fields,
        &format!("{prefix}_existing_report_refs"),
        &report.existing_report_refs.join(","),
    );
    push_field(
        fields,
        &format!("{prefix}_compatibility_profiles"),
        &report.compatibility_profiles.join(","),
    );
    append_catalog_metadata_integration_gate_prefixed_existing_fields(fields, prefix, report);
    append_catalog_metadata_integration_gate_prefixed_allowed_fields(fields, prefix, report);
    append_catalog_metadata_integration_gate_prefixed_required_fields(fields, prefix, report);
    append_catalog_metadata_integration_gate_prefixed_status_fields(fields, prefix, report);
    for entry in &report.entries {
        append_catalog_metadata_integration_gate_entry_fields(fields, prefix, entry);
    }
}

fn append_catalog_metadata_integration_gate_prefixed_existing_fields(
    fields: &mut Vec<(String, String)>,
    prefix: &str,
    report: &CatalogMetadataIntegrationGateReport,
) {
    push_bool_field(
        fields,
        &format!("{prefix}_existing_table_intelligence_foundation_present"),
        report.existing_table_intelligence_foundation_present,
    );
    push_bool_field(
        fields,
        &format!("{prefix}_existing_schema_partition_delete_compatibility_present"),
        report.existing_schema_partition_delete_compatibility_present,
    );
    push_bool_field(
        fields,
        &format!("{prefix}_existing_cdc_layout_compaction_planning_present"),
        report.existing_cdc_layout_compaction_planning_present,
    );
    push_bool_field(
        fields,
        &format!("{prefix}_existing_catalog_ref_skeleton_present"),
        report.existing_catalog_ref_skeleton_present,
    );
    push_bool_field(
        fields,
        &format!("{prefix}_local_manifest_table_metadata_smoke_supported"),
        report.local_manifest_table_metadata_smoke_supported,
    );
    push_field(
        fields,
        &format!("{prefix}_local_manifest_table_metadata_smoke_command"),
        report.local_manifest_table_metadata_smoke_command,
    );
    push_field(
        fields,
        &format!("{prefix}_local_manifest_table_metadata_smoke_report_ref"),
        report.local_manifest_table_metadata_smoke_report_ref,
    );
    push_field(
        fields,
        &format!("{prefix}_local_manifest_table_metadata_smoke_claim_gate_status"),
        report.local_manifest_table_metadata_smoke_claim_gate_status,
    );
    push_field(
        fields,
        &format!("{prefix}_local_manifest_table_metadata_smoke_claim_boundary"),
        report.local_manifest_table_metadata_smoke_claim_boundary,
    );
}

fn append_catalog_metadata_integration_gate_prefixed_allowed_fields(
    fields: &mut Vec<(String, String)>,
    prefix: &str,
    report: &CatalogMetadataIntegrationGateReport,
) {
    push_bool_field(
        fields,
        &format!("{prefix}_snapshot_manifest_metadata_read_allowed"),
        report.snapshot_manifest_metadata_read_allowed,
    );
    push_bool_field(
        fields,
        &format!("{prefix}_catalog_resolution_allowed"),
        report.catalog_resolution_allowed,
    );
    push_bool_field(
        fields,
        &format!("{prefix}_table_metadata_read_allowed"),
        report.table_metadata_read_allowed,
    );
    push_bool_field(
        fields,
        &format!("{prefix}_catalog_io_allowed"),
        report.catalog_io_allowed,
    );
    push_bool_field(
        fields,
        &format!("{prefix}_object_store_io_allowed"),
        report.object_store_io_allowed,
    );
    push_bool_field(
        fields,
        &format!("{prefix}_data_io_allowed"),
        report.data_io_allowed,
    );
    push_bool_field(
        fields,
        &format!("{prefix}_write_io_allowed"),
        report.write_io_allowed,
    );
    push_bool_field(
        fields,
        &format!("{prefix}_external_table_format_dependency_allowed"),
        report.external_table_format_dependency_allowed,
    );
    push_bool_field(
        fields,
        &format!("{prefix}_credential_resolution_allowed"),
        report.credential_resolution_allowed,
    );
    push_bool_field(
        fields,
        &format!("{prefix}_metadata_cache_runtime_allowed"),
        report.metadata_cache_runtime_allowed,
    );
    push_bool_field(
        fields,
        &format!("{prefix}_metadata_integration_claim_allowed"),
        report.metadata_integration_claim_allowed,
    );
}

fn append_catalog_metadata_integration_gate_prefixed_required_fields(
    fields: &mut Vec<(String, String)>,
    prefix: &str,
    report: &CatalogMetadataIntegrationGateReport,
) {
    push_bool_field(
        fields,
        &format!("{prefix}_table_intelligence_report_required"),
        report.table_intelligence_report_required,
    );
    push_bool_field(
        fields,
        &format!("{prefix}_catalog_ref_required"),
        report.catalog_ref_required,
    );
    push_bool_field(
        fields,
        &format!("{prefix}_snapshot_ref_required"),
        report.snapshot_ref_required,
    );
    push_bool_field(
        fields,
        &format!("{prefix}_schema_digest_required"),
        report.schema_digest_required,
    );
    push_bool_field(
        fields,
        &format!("{prefix}_partition_spec_required"),
        report.partition_spec_required,
    );
    push_bool_field(
        fields,
        &format!("{prefix}_delete_tombstone_policy_required"),
        report.delete_tombstone_policy_required,
    );
    push_bool_field(
        fields,
        &format!("{prefix}_dependency_license_approval_required"),
        report.dependency_license_approval_required,
    );
    push_bool_field(
        fields,
        &format!("{prefix}_credential_policy_required"),
        report.credential_policy_required,
    );
    push_bool_field(
        fields,
        &format!("{prefix}_effect_policy_required"),
        report.effect_policy_required,
    );
    push_bool_field(
        fields,
        &format!("{prefix}_materialization_boundary_required"),
        report.materialization_boundary_required,
    );
    push_bool_field(
        fields,
        &format!("{prefix}_execution_certificate_required"),
        report.execution_certificate_required,
    );
    push_bool_field(
        fields,
        &format!("{prefix}_native_io_certificate_required"),
        report.native_io_certificate_required,
    );
    push_bool_field(
        fields,
        &format!("{prefix}_benchmark_evidence_required"),
        report.benchmark_evidence_required,
    );
}

fn append_catalog_metadata_integration_gate_prefixed_status_fields(
    fields: &mut Vec<(String, String)>,
    prefix: &str,
    report: &CatalogMetadataIntegrationGateReport,
) {
    push_bool_field(
        fields,
        &format!("{prefix}_runtime_promotions_blocked"),
        report.runtime_promotions_blocked(),
    );
    push_bool_field(
        fields,
        &format!("{prefix}_claim_blocked"),
        report.claim_blocked(),
    );
    push_bool_field(
        fields,
        &format!("{prefix}_fallback_attempted"),
        report.fallback_attempted,
    );
    push_bool_field(
        fields,
        &format!("{prefix}_fallback_execution_allowed"),
        report.fallback_execution_allowed,
    );
    push_bool_field(
        fields,
        &format!("{prefix}_external_engine_invoked"),
        report.external_engine_invoked,
    );
    push_bool_field(
        fields,
        &format!("{prefix}_deterministic_unsupported_diagnostics_ready"),
        report.deterministic_unsupported_diagnostics_ready(),
    );
    push_count_field(
        fields,
        &format!("{prefix}_unsupported_diagnostic_count"),
        report.unsupported_diagnostic_count(),
    );
    push_field(
        fields,
        &format!("{prefix}_unsupported_diagnostic_code_order"),
        &report.unsupported_diagnostic_code_order().join(","),
    );
    push_field(
        fields,
        &format!("{prefix}_unsupported_diagnostic_category_order"),
        &report.unsupported_diagnostic_category_order().join(","),
    );
    push_field(
        fields,
        &format!("{prefix}_unsupported_diagnostic_severity_order"),
        &report.unsupported_diagnostic_severity_order().join(","),
    );
    push_bool_field(
        fields,
        &format!("{prefix}_side_effect_free"),
        report.side_effect_free(),
    );
    push_count_field(
        fields,
        &format!("{prefix}_diagnostic_count"),
        report.diagnostics.len(),
    );
}

fn append_catalog_metadata_integration_gate_entry_fields(
    fields: &mut Vec<(String, String)>,
    prefix: &str,
    entry: &CatalogMetadataIntegrationGateEntry,
) {
    let row_prefix = if prefix.is_empty() {
        format!("row_{}", entry.surface.as_str())
    } else {
        format!("{prefix}_row_{}", entry.surface.as_str())
    };
    push_field(
        fields,
        &format!("{row_prefix}_status"),
        entry.status.as_str(),
    );
    push_field(
        fields,
        &format!("{row_prefix}_existing_report_ref"),
        entry.existing_report_ref.unwrap_or("none"),
    );
    push_field(
        fields,
        &format!("{row_prefix}_required_evidence"),
        entry.required_evidence,
    );
    push_bool_field(
        fields,
        &format!("{row_prefix}_requires_catalog_ref"),
        entry.requires_catalog_ref,
    );
    push_bool_field(
        fields,
        &format!("{row_prefix}_requires_snapshot_ref"),
        entry.requires_snapshot_ref,
    );
    push_bool_field(
        fields,
        &format!("{row_prefix}_requires_table_metadata_io"),
        entry.requires_table_metadata_io,
    );
    push_bool_field(
        fields,
        &format!("{row_prefix}_requires_catalog_io"),
        entry.requires_catalog_io,
    );
    push_bool_field(
        fields,
        &format!("{row_prefix}_requires_object_store_io"),
        entry.requires_object_store_io,
    );
    push_bool_field(
        fields,
        &format!("{row_prefix}_requires_dependency_approval"),
        entry.requires_dependency_approval,
    );
    push_bool_field(
        fields,
        &format!("{row_prefix}_requires_credential_policy"),
        entry.requires_credential_policy,
    );
    push_bool_field(
        fields,
        &format!("{row_prefix}_requires_execution_certificate"),
        entry.requires_execution_certificate,
    );
    push_bool_field(
        fields,
        &format!("{row_prefix}_requires_native_io_certificate"),
        entry.requires_native_io_certificate,
    );
    push_bool_field(
        fields,
        &format!("{row_prefix}_runtime_allowed"),
        entry.runtime_allowed,
    );
    push_bool_field(
        fields,
        &format!("{row_prefix}_fallback_attempted"),
        entry.fallback_attempted,
    );
    push_bool_field(
        fields,
        &format!("{row_prefix}_fallback_execution_allowed"),
        entry.fallback_execution_allowed,
    );
    push_bool_field(
        fields,
        &format!("{row_prefix}_external_engine_invoked"),
        entry.external_engine_invoked,
    );
    push_bool_field(
        fields,
        &format!("{row_prefix}_side_effect_free"),
        entry.side_effect_free(),
    );
    push_field(
        fields,
        &format!("{row_prefix}_claim_gate_status"),
        entry.claim_gate_status,
    );
}

fn local_table_metadata_read_smoke_fields(
    report: &LocalTableMetadataReadSmokeReport,
) -> Vec<(String, String)> {
    let mut fields = vec![];
    append_local_table_metadata_identity_fields(&mut fields, report);
    append_local_table_metadata_summary_fields(&mut fields, report);
    append_local_table_metadata_evidence_fields(&mut fields, report);
    append_local_table_metadata_boundary_fields(&mut fields, report);
    append_local_table_metadata_diagnostic_fields(&mut fields, report);
    push_field(&mut fields, "execution", "performed");
    push_field(&mut fields, "plan_only", "false");
    fields
}

fn append_local_table_metadata_identity_fields(
    fields: &mut Vec<(String, String)>,
    report: &LocalTableMetadataReadSmokeReport,
) {
    push_field(fields, "mode", "local_table_metadata_read_smoke");
    push_field(fields, "schema_version", report.schema_version);
    push_field(fields, "report_id", report.report_id);
    push_field(fields, "gar_id", report.gar_id);
    push_field(fields, "support_status", report.support_status);
    push_field(fields, "claim_gate_status", report.claim_gate_status);
    push_field(fields, "claim_boundary", report.claim_boundary);
    push_field(fields, "catalog_kind", report.catalog_kind);
    push_field(fields, "catalog_ref_summary", &report.catalog_ref_summary);
    push_field(fields, "dataset_uri", &report.dataset_uri);
    push_field(fields, "dataset_format", &report.dataset_format);
    push_field(fields, "manifest_id", &report.manifest_id);
    push_field(fields, "manifest_version", &report.manifest_version);
    push_field(fields, "snapshot_id", &report.snapshot_id);
    push_field(fields, "schema_id", &report.schema_id);
    push_field(
        fields,
        "schema_version_number",
        &report.schema_version_number.to_string(),
    );
}

fn append_local_table_metadata_summary_fields(
    fields: &mut Vec<(String, String)>,
    report: &LocalTableMetadataReadSmokeReport,
) {
    push_count_field(fields, "schema_field_count", report.schema_field_count);
    push_bool_field(fields, "schema_has_field_ids", report.schema_has_field_ids);
    push_count_field(
        fields,
        "partition_field_count",
        report.partition_field_count,
    );
    push_bool_field(fields, "is_partitioned", report.is_partitioned);
    push_count_field(fields, "manifest_file_count", report.manifest_file_count);
    push_count_field(
        fields,
        "manifest_segment_count",
        report.manifest_segment_count,
    );
    push_count_field(
        fields,
        "native_vortex_file_count",
        report.native_vortex_file_count,
    );
    push_count_field(
        fields,
        "metadata_capable_segment_count",
        report.metadata_capable_segment_count,
    );
    push_field(
        fields,
        "declared_row_count",
        &report.declared_row_count.to_string(),
    );
    push_field(fields, "metadata_summary", &report.metadata_summary);
    push_field(
        fields,
        "metadata_summary_digest",
        &report.metadata_summary_digest,
    );
}

fn append_local_table_metadata_evidence_fields(
    fields: &mut Vec<(String, String)>,
    report: &LocalTableMetadataReadSmokeReport,
) {
    push_field(fields, "correctness_refs", report.correctness_refs);
    push_field(fields, "benchmark_refs", report.benchmark_refs);
    push_field(
        fields,
        "execution_certificate_refs",
        report.execution_certificate_refs,
    );
    push_field(
        fields,
        "native_io_certificate_refs",
        report.native_io_certificate_refs,
    );
    push_field(
        fields,
        "materialization_decode_refs",
        report.materialization_decode_refs,
    );
    push_field(fields, "policy_refs", report.policy_refs);
    push_field(
        fields,
        "dependency_boundary_refs",
        report.dependency_boundary_refs,
    );
}

fn append_local_table_metadata_boundary_fields(
    fields: &mut Vec<(String, String)>,
    report: &LocalTableMetadataReadSmokeReport,
) {
    push_bool_field(
        fields,
        "local_catalog_ref_resolved",
        report.local_catalog_ref_resolved,
    );
    push_bool_field(
        fields,
        "local_manifest_metadata_read_performed",
        report.local_manifest_metadata_read_performed,
    );
    push_bool_field(
        fields,
        "table_metadata_summary_emitted",
        report.table_metadata_summary_emitted,
    );
    push_bool_field(
        fields,
        "table_metadata_read_performed",
        report.table_metadata_read_performed,
    );
    push_bool_field(fields, "catalog_io_performed", report.catalog_io_performed);
    push_bool_field(
        fields,
        "table_metadata_file_io_performed",
        report.table_metadata_file_io_performed,
    );
    push_bool_field(
        fields,
        "object_store_io_performed",
        report.object_store_io_performed,
    );
    push_bool_field(
        fields,
        "data_file_read_performed",
        report.data_file_read_performed,
    );
    push_bool_field(fields, "write_io_performed", report.write_io_performed);
    push_bool_field(
        fields,
        "credential_resolution_performed",
        report.credential_resolution_performed,
    );
    push_bool_field(
        fields,
        "external_table_format_dependency_invoked",
        report.external_table_format_dependency_invoked,
    );
    push_bool_field(fields, "fallback_attempted", report.fallback_attempted);
    push_bool_field(
        fields,
        "fallback_execution_allowed",
        report.fallback_execution_allowed,
    );
    push_bool_field(
        fields,
        "external_engine_invoked",
        report.external_engine_invoked,
    );
    push_bool_field(
        fields,
        "performance_claim_allowed",
        report.performance_claim_allowed,
    );
    push_bool_field(
        fields,
        "production_table_catalog_claim_allowed",
        report.production_table_catalog_claim_allowed,
    );
    push_bool_field(
        fields,
        "lakehouse_claim_allowed",
        report.lakehouse_claim_allowed,
    );
}

fn append_local_table_metadata_diagnostic_fields(
    fields: &mut Vec<(String, String)>,
    report: &LocalTableMetadataReadSmokeReport,
) {
    push_bool_field(fields, "runtime_supported", report.runtime_supported());
    push_bool_field(fields, "claim_scoped", report.claim_scoped());
    push_bool_field(fields, "side_effect_free", report.side_effect_free());
    push_count_field(fields, "blocked_path_count", report.blocked_paths.len());
    push_field(
        fields,
        "blocked_path_order",
        &report.blocked_path_order().join(","),
    );
    push_count_field(
        fields,
        "unsupported_diagnostic_count",
        report.unsupported_diagnostic_count(),
    );
    push_bool_field(
        fields,
        "deterministic_unsupported_diagnostics_ready",
        report.deterministic_unsupported_diagnostics_ready(),
    );
    push_field(
        fields,
        "unsupported_diagnostic_code_order",
        &report.unsupported_diagnostic_code_order().join(","),
    );
    push_count_field(fields, "diagnostic_count", report.diagnostics.len());
}

fn local_delete_tombstone_read_smoke_fields(
    report: &LocalDeleteTombstoneReadSmokeReport,
) -> Vec<(String, String)> {
    let mut fields = vec![];
    append_local_delete_tombstone_identity_fields(&mut fields, report);
    append_local_delete_tombstone_summary_fields(&mut fields, report);
    append_local_delete_tombstone_evidence_fields(&mut fields, report);
    append_local_delete_tombstone_boundary_fields(&mut fields, report);
    append_local_delete_tombstone_diagnostic_fields(&mut fields, report);
    push_field(&mut fields, "execution", "performed");
    push_field(&mut fields, "plan_only", "false");
    fields
}

fn append_local_delete_tombstone_identity_fields(
    fields: &mut Vec<(String, String)>,
    report: &LocalDeleteTombstoneReadSmokeReport,
) {
    push_field(fields, "mode", "local_delete_tombstone_read_smoke");
    push_field(fields, "schema_version", report.schema_version);
    push_field(fields, "report_id", report.report_id);
    push_field(fields, "gar_id", report.gar_id);
    push_field(fields, "support_status", report.support_status);
    push_field(fields, "claim_gate_status", report.claim_gate_status);
    push_field(fields, "claim_boundary", report.claim_boundary);
    push_field(fields, "fixture_id", report.fixture_id);
    push_field(fields, "catalog_kind", report.catalog_kind);
    push_field(fields, "catalog_ref_summary", &report.catalog_ref_summary);
    push_field(fields, "dataset_uri", &report.dataset_uri);
    push_field(fields, "dataset_format", &report.dataset_format);
    push_field(fields, "manifest_id", &report.manifest_id);
    push_field(fields, "manifest_version", &report.manifest_version);
    push_field(fields, "snapshot_id", &report.snapshot_id);
    push_field(fields, "schema_id", &report.schema_id);
}

fn append_local_delete_tombstone_summary_fields(
    fields: &mut Vec<(String, String)>,
    report: &LocalDeleteTombstoneReadSmokeReport,
) {
    push_field(
        fields,
        "admitted_delete_model_order",
        &report.admitted_delete_model_order.join(","),
    );
    push_field(
        fields,
        "unsupported_delete_model_order",
        &report.unsupported_delete_model_order.join(","),
    );
    push_field(
        fields,
        "delete_tombstone_admission_rule",
        report.delete_tombstone_admission_rule,
    );
    push_field(fields, "row_identity_rule", report.row_identity_rule);
    push_count_field(fields, "base_row_count", report.base_row_count);
    push_count_field(
        fields,
        "file_deleted_row_count",
        report.file_deleted_row_count,
    );
    push_count_field(
        fields,
        "segment_tombstoned_row_count",
        report.segment_tombstoned_row_count,
    );
    push_count_field(fields, "effective_row_count", report.effective_row_count);
    push_count_field(fields, "manifest_file_count", report.manifest_file_count);
    push_count_field(
        fields,
        "manifest_segment_count",
        report.manifest_segment_count,
    );
    push_count_field(
        fields,
        "native_vortex_file_count",
        report.native_vortex_file_count,
    );
    push_count_field(
        fields,
        "admitted_file_delete_count",
        report.admitted_file_delete_count,
    );
    push_count_field(
        fields,
        "admitted_segment_tombstone_count",
        report.admitted_segment_tombstone_count,
    );
    push_field(
        fields,
        "effective_row_ids",
        &report
            .effective_row_ids
            .iter()
            .map(u64::to_string)
            .collect::<Vec<_>>()
            .join(","),
    );
    push_field(fields, "correctness_summary", &report.correctness_summary);
    push_field(fields, "correctness_digest", &report.correctness_digest);
}

fn append_local_delete_tombstone_evidence_fields(
    fields: &mut Vec<(String, String)>,
    report: &LocalDeleteTombstoneReadSmokeReport,
) {
    push_field(fields, "correctness_refs", report.correctness_refs);
    push_field(fields, "benchmark_refs", report.benchmark_refs);
    push_field(
        fields,
        "execution_certificate_refs",
        report.execution_certificate_refs,
    );
    push_field(
        fields,
        "native_io_certificate_refs",
        report.native_io_certificate_refs,
    );
    push_field(
        fields,
        "materialization_decode_refs",
        report.materialization_decode_refs,
    );
    push_field(fields, "policy_refs", report.policy_refs);
}

fn append_local_delete_tombstone_boundary_fields(
    fields: &mut Vec<(String, String)>,
    report: &LocalDeleteTombstoneReadSmokeReport,
) {
    push_bool_field(
        fields,
        "local_catalog_ref_resolved",
        report.local_catalog_ref_resolved,
    );
    push_bool_field(
        fields,
        "local_manifest_metadata_read_performed",
        report.local_manifest_metadata_read_performed,
    );
    push_bool_field(
        fields,
        "in_memory_fixture_rows_read",
        report.in_memory_fixture_rows_read,
    );
    push_bool_field(
        fields,
        "delete_tombstone_rule_applied",
        report.delete_tombstone_rule_applied,
    );
    push_bool_field(
        fields,
        "result_row_order_preserved",
        report.result_row_order_preserved,
    );
    push_bool_field(
        fields,
        "table_metadata_write_performed",
        report.table_metadata_write_performed,
    );
    push_bool_field(
        fields,
        "data_file_read_performed",
        report.data_file_read_performed,
    );
    push_bool_field(
        fields,
        "object_store_io_performed",
        report.object_store_io_performed,
    );
    push_bool_field(fields, "write_io_performed", report.write_io_performed);
    push_bool_field(
        fields,
        "credential_resolution_performed",
        report.credential_resolution_performed,
    );
    push_bool_field(
        fields,
        "external_table_format_dependency_invoked",
        report.external_table_format_dependency_invoked,
    );
    push_bool_field(fields, "fallback_attempted", report.fallback_attempted);
    push_bool_field(
        fields,
        "fallback_execution_allowed",
        report.fallback_execution_allowed,
    );
    push_bool_field(
        fields,
        "external_engine_invoked",
        report.external_engine_invoked,
    );
    push_bool_field(
        fields,
        "performance_claim_allowed",
        report.performance_claim_allowed,
    );
    push_bool_field(
        fields,
        "table_format_execution_claim_allowed",
        report.table_format_execution_claim_allowed,
    );
    push_bool_field(
        fields,
        "production_table_catalog_claim_allowed",
        report.production_table_catalog_claim_allowed,
    );
    push_bool_field(
        fields,
        "lakehouse_claim_allowed",
        report.lakehouse_claim_allowed,
    );
}

fn append_local_delete_tombstone_diagnostic_fields(
    fields: &mut Vec<(String, String)>,
    report: &LocalDeleteTombstoneReadSmokeReport,
) {
    push_bool_field(
        fields,
        "fixture_smoke_supported",
        report.fixture_smoke_supported(),
    );
    push_bool_field(fields, "claim_scoped", report.claim_scoped());
    push_bool_field(fields, "side_effect_free", report.side_effect_free());
    push_count_field(fields, "blocked_model_count", report.blocked_models.len());
    push_field(
        fields,
        "blocked_model_order",
        &report.blocked_model_order().join(","),
    );
    push_count_field(
        fields,
        "unsupported_diagnostic_count",
        report.unsupported_diagnostic_count(),
    );
    push_bool_field(
        fields,
        "deterministic_unsupported_diagnostics_ready",
        report.deterministic_unsupported_diagnostics_ready(),
    );
    push_field(
        fields,
        "unsupported_diagnostic_code_order",
        &report.unsupported_diagnostic_code_order().join(","),
    );
    push_count_field(fields, "diagnostic_count", report.diagnostics.len());
}

fn local_append_only_cdc_overlay_smoke_fields(
    report: &LocalAppendOnlyCdcOverlaySmokeReport,
) -> Vec<(String, String)> {
    let mut fields = vec![];
    append_local_append_only_cdc_identity_fields(&mut fields, report);
    append_local_append_only_cdc_summary_fields(&mut fields, report);
    append_local_append_only_cdc_evidence_fields(&mut fields, report);
    append_local_append_only_cdc_boundary_fields(&mut fields, report);
    append_local_append_only_cdc_diagnostic_fields(&mut fields, report);
    push_field(&mut fields, "execution", "performed");
    push_field(&mut fields, "plan_only", "false");
    fields
}

fn append_local_append_only_cdc_identity_fields(
    fields: &mut Vec<(String, String)>,
    report: &LocalAppendOnlyCdcOverlaySmokeReport,
) {
    push_field(fields, "mode", "local_append_only_cdc_overlay_smoke");
    push_field(fields, "schema_version", report.schema_version);
    push_field(fields, "report_id", report.report_id);
    push_field(fields, "gar_id", report.gar_id);
    push_field(fields, "support_status", report.support_status);
    push_field(fields, "claim_gate_status", report.claim_gate_status);
    push_field(fields, "claim_boundary", report.claim_boundary);
    push_field(fields, "fixture_id", report.fixture_id);
    push_field(fields, "catalog_kind", report.catalog_kind);
    push_field(fields, "catalog_ref_summary", &report.catalog_ref_summary);
    push_field(fields, "dataset_uri", &report.dataset_uri);
    push_field(fields, "dataset_format", &report.dataset_format);
    push_field(fields, "base_manifest_id", &report.base_manifest_id);
    push_field(fields, "delta_manifest_id", &report.delta_manifest_id);
    push_field(fields, "base_snapshot_id", &report.base_snapshot_id);
    push_field(fields, "delta_snapshot_id", &report.delta_snapshot_id);
    push_field(fields, "schema_id", &report.schema_id);
}

fn append_local_append_only_cdc_summary_fields(
    fields: &mut Vec<(String, String)>,
    report: &LocalAppendOnlyCdcOverlaySmokeReport,
) {
    push_field(
        fields,
        "incremental_plan_report_ref",
        report.incremental_plan_report_ref,
    );
    push_field(fields, "incremental_status", report.incremental_status);
    push_field(
        fields,
        "change_set_from_snapshot",
        &report.change_set_from_snapshot,
    );
    push_field(
        fields,
        "change_set_to_snapshot",
        &report.change_set_to_snapshot,
    );
    push_field(fields, "overlay_rule", report.overlay_rule);
    push_field(fields, "cdc_event_order", &report.cdc_event_order.join(","));
    push_field(
        fields,
        "blocked_path_order",
        &report.blocked_path_order().join(","),
    );
    push_count_field(fields, "base_row_count", report.base_row_count);
    push_count_field(fields, "append_row_count", report.append_row_count);
    push_count_field(fields, "effective_row_count", report.effective_row_count);
    push_count_field(
        fields,
        "base_manifest_file_count",
        report.base_manifest_file_count,
    );
    push_count_field(
        fields,
        "delta_manifest_file_count",
        report.delta_manifest_file_count,
    );
    push_count_field(
        fields,
        "base_manifest_segment_count",
        report.base_manifest_segment_count,
    );
    push_count_field(
        fields,
        "delta_manifest_segment_count",
        report.delta_manifest_segment_count,
    );
    push_count_field(
        fields,
        "changed_segment_count",
        report.changed_segment_count,
    );
    push_count_field(fields, "insert_count", report.insert_count);
    push_count_field(fields, "update_count", report.update_count);
    push_count_field(fields, "delete_count", report.delete_count);
    push_count_field(fields, "tombstone_count", report.tombstone_count);
    push_count_field(
        fields,
        "unsupported_change_count",
        report.unsupported_change_count,
    );
    push_field(fields, "base_row_ids", &join_u64s(&report.base_row_ids));
    push_field(
        fields,
        "appended_row_ids",
        &join_u64s(&report.appended_row_ids),
    );
    push_field(
        fields,
        "effective_row_ids",
        &join_u64s(&report.effective_row_ids),
    );
    push_field(fields, "correctness_summary", &report.correctness_summary);
    push_field(fields, "correctness_digest", &report.correctness_digest);
}

fn append_local_append_only_cdc_evidence_fields(
    fields: &mut Vec<(String, String)>,
    report: &LocalAppendOnlyCdcOverlaySmokeReport,
) {
    push_field(fields, "correctness_refs", report.correctness_refs);
    push_field(fields, "benchmark_refs", report.benchmark_refs);
    push_field(
        fields,
        "execution_certificate_refs",
        report.execution_certificate_refs,
    );
    push_field(
        fields,
        "native_io_certificate_refs",
        report.native_io_certificate_refs,
    );
    push_field(
        fields,
        "materialization_decode_refs",
        report.materialization_decode_refs,
    );
    push_field(fields, "policy_refs", report.policy_refs);
}

fn append_local_append_only_cdc_boundary_fields(
    fields: &mut Vec<(String, String)>,
    report: &LocalAppendOnlyCdcOverlaySmokeReport,
) {
    push_bool_field(
        fields,
        "local_catalog_ref_resolved",
        report.local_catalog_ref_resolved,
    );
    push_bool_field(
        fields,
        "local_base_snapshot_declared",
        report.local_base_snapshot_declared,
    );
    push_bool_field(
        fields,
        "local_append_delta_declared",
        report.local_append_delta_declared,
    );
    push_bool_field(
        fields,
        "cdc_incremental_plan_evaluated",
        report.cdc_incremental_plan_evaluated,
    );
    push_bool_field(
        fields,
        "append_overlay_rule_applied",
        report.append_overlay_rule_applied,
    );
    push_bool_field(
        fields,
        "result_row_order_preserved",
        report.result_row_order_preserved,
    );
    push_bool_field(
        fields,
        "table_metadata_write_performed",
        report.table_metadata_write_performed,
    );
    push_bool_field(
        fields,
        "manifest_write_performed",
        report.manifest_write_performed,
    );
    push_bool_field(
        fields,
        "transaction_execution_performed",
        report.transaction_execution_performed,
    );
    push_bool_field(
        fields,
        "commit_execution_performed",
        report.commit_execution_performed,
    );
    push_bool_field(
        fields,
        "data_file_read_performed",
        report.data_file_read_performed,
    );
    push_bool_field(
        fields,
        "object_store_io_performed",
        report.object_store_io_performed,
    );
    push_bool_field(fields, "write_io_performed", report.write_io_performed);
    push_bool_field(
        fields,
        "credential_resolution_performed",
        report.credential_resolution_performed,
    );
    push_bool_field(
        fields,
        "external_table_format_dependency_invoked",
        report.external_table_format_dependency_invoked,
    );
    push_bool_field(fields, "fallback_attempted", report.fallback_attempted);
    push_bool_field(
        fields,
        "fallback_execution_allowed",
        report.fallback_execution_allowed,
    );
    push_bool_field(
        fields,
        "external_engine_invoked",
        report.external_engine_invoked,
    );
    push_bool_field(
        fields,
        "performance_claim_allowed",
        report.performance_claim_allowed,
    );
    push_bool_field(
        fields,
        "production_incremental_claim_allowed",
        report.production_incremental_claim_allowed,
    );
    push_bool_field(
        fields,
        "lakehouse_claim_allowed",
        report.lakehouse_claim_allowed,
    );
}

fn append_local_append_only_cdc_diagnostic_fields(
    fields: &mut Vec<(String, String)>,
    report: &LocalAppendOnlyCdcOverlaySmokeReport,
) {
    push_bool_field(
        fields,
        "fixture_smoke_supported",
        report.fixture_smoke_supported(),
    );
    push_bool_field(fields, "claim_scoped", report.claim_scoped());
    push_bool_field(fields, "side_effect_free", report.side_effect_free());
    push_count_field(fields, "blocked_path_count", report.blocked_paths.len());
    push_count_field(
        fields,
        "unsupported_diagnostic_count",
        report.unsupported_diagnostic_count(),
    );
    push_bool_field(
        fields,
        "deterministic_unsupported_diagnostics_ready",
        report.deterministic_unsupported_diagnostics_ready(),
    );
    push_field(
        fields,
        "unsupported_diagnostic_code_order",
        &report.unsupported_diagnostic_code_order().join(","),
    );
    push_count_field(fields, "diagnostic_count", report.diagnostics.len());
}

fn join_u64s(values: &[u64]) -> String {
    values
        .iter()
        .map(u64::to_string)
        .collect::<Vec<_>>()
        .join(",")
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
    let cdc_manifest_transaction_gate = plan_cdc_manifest_transaction_gate();
    let has_errors = report.has_errors() || cdc_manifest_transaction_gate.has_errors();
    let mut diagnostics = report.diagnostics.clone();
    diagnostics.extend(cdc_manifest_transaction_gate.diagnostics.clone());
    let status = if has_errors {
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
        diagnostics,
        cdc_incremental_output_fields(&report, scenario, &cdc_manifest_transaction_gate),
    );
    if has_errors {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

fn cdc_incremental_output_fields(
    report: &CdcIncrementalPlanningReport,
    scenario: &str,
    cdc_manifest_transaction_gate: &CdcManifestTransactionGateReport,
) -> Vec<(String, String)> {
    let mut fields = vec![];
    push_field(&mut fields, "fallback_execution_allowed", "false");
    push_field(&mut fields, "mode", "cdc_incremental_plan");
    push_field(&mut fields, "scenario", scenario);
    push_field(&mut fields, "cdc_status", report.status.as_str());
    append_cdc_incremental_count_fields(&mut fields, report);
    append_cdc_incremental_requirement_fields(&mut fields, report);
    append_cdc_incremental_side_effect_fields(&mut fields, report);
    append_cdc_manifest_transaction_gate_fields(
        &mut fields,
        "cdc_manifest_transaction_gate",
        cdc_manifest_transaction_gate,
    );
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
        let cdc_manifest_transaction_gate = plan_cdc_manifest_transaction_gate();
        let catalog_metadata_integration_gate = plan_catalog_metadata_integration_gate();
        let table_maintenance_execution_matrix = plan_table_maintenance_execution_matrix();
        let fields = table_intelligence_output_fields(
            &report,
            &cdc_manifest_transaction_gate,
            &catalog_metadata_integration_gate,
            &table_maintenance_execution_matrix,
        );

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
        assert_eq!(
            output_field(&fields, "cdc_manifest_transaction_gate_report_id"),
            "gar0004a.cdc_manifest_transaction_gate"
        );
        assert_eq!(
            output_field(&fields, "cdc_manifest_transaction_gate_surface_count"),
            "8"
        );
        assert_eq!(
            output_field(
                &fields,
                "cdc_manifest_transaction_gate_runtime_promotions_blocked"
            ),
            "true"
        );
        assert_eq!(
            output_field(
                &fields,
                "cdc_manifest_transaction_gate_deterministic_unsupported_diagnostics_ready"
            ),
            "true"
        );
        assert_eq!(
            output_field(
                &fields,
                "cdc_manifest_transaction_gate_fallback_execution_allowed"
            ),
            "false"
        );
        assert_eq!(
            output_field(&fields, "catalog_metadata_integration_gate_gar_id"),
            "GAR-0020-A"
        );
        assert_eq!(
            output_field(
                &fields,
                "catalog_metadata_integration_gate_deterministic_unsupported_diagnostics_ready"
            ),
            "true"
        );
        assert_eq!(
            output_field(
                &fields,
                "catalog_metadata_integration_gate_table_metadata_read_allowed"
            ),
            "false"
        );
        assert_eq!(
            output_field(
                &fields,
                "catalog_metadata_integration_gate_external_engine_invoked"
            ),
            "false"
        );
    }

    #[test]
    fn table_intelligence_fields_include_table_maintenance_execution_matrix() {
        let report = TableIntelligenceReport::report_only_foundation();
        let cdc_manifest_transaction_gate = plan_cdc_manifest_transaction_gate();
        let catalog_metadata_integration_gate = plan_catalog_metadata_integration_gate();
        let table_maintenance_execution_matrix = plan_table_maintenance_execution_matrix();
        let fields = table_intelligence_output_fields(
            &report,
            &cdc_manifest_transaction_gate,
            &catalog_metadata_integration_gate,
            &table_maintenance_execution_matrix,
        );

        assert_eq!(
            output_field(&fields, "table_maintenance_execution_matrix_gar_id"),
            "GAR-0020-B"
        );
        assert_eq!(
            output_field(
                &fields,
                "table_maintenance_execution_matrix_runtime_promotions_blocked"
            ),
            "true"
        );
        assert_eq!(
            output_field(
                &fields,
                "table_maintenance_execution_matrix_local_delete_tombstone_smoke_present"
            ),
            "true"
        );
        assert_eq!(
            output_field(
                &fields,
                "table_maintenance_execution_matrix_local_append_only_cdc_overlay_smoke_present"
            ),
            "true"
        );
        assert_eq!(
            output_field(
                &fields,
                "table_maintenance_execution_matrix_row_table_metadata_write_status"
            ),
            "unsupported_until_certified"
        );
        assert_eq!(
            output_field(
                &fields,
                "table_maintenance_execution_matrix_row_table_metadata_write_external_engine_invoked"
            ),
            "false"
        );
    }

    #[test]
    fn cdc_incremental_fields_include_manifest_transaction_gate() {
        let (change_set, events) = cdc_incremental_fixture("append-only").expect("fixture");
        let report = evaluate_cdc_incremental_planning(change_set, events);
        let cdc_manifest_transaction_gate = plan_cdc_manifest_transaction_gate();
        let fields =
            cdc_incremental_output_fields(&report, "append-only", &cdc_manifest_transaction_gate);

        assert_eq!(output_field(&fields, "mode"), "cdc_incremental_plan");
        assert_eq!(
            output_field(&fields, "cdc_status"),
            "execute_changed_segments_only"
        );
        assert_eq!(
            output_field(
                &fields,
                "cdc_manifest_transaction_gate_cdc_read_intent_report_only_available"
            ),
            "true"
        );
        assert_eq!(
            output_field(
                &fields,
                "cdc_manifest_transaction_gate_commit_execution_allowed"
            ),
            "false"
        );
        assert_eq!(
            output_field(
                &fields,
                "cdc_manifest_transaction_gate_external_engine_invoked"
            ),
            "false"
        );
    }

    fn output_field<'a>(fields: &'a [(String, String)], key: &str) -> &'a str {
        fields
            .iter()
            .find(|(field_key, _)| field_key == key)
            .map_or("", |(_, value)| value.as_str())
    }
}
