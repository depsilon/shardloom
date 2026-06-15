//! Workflow, table, manifest, and stateful planning CLI handlers.
//!
//! These handlers emit report-only workflow planning surfaces. They do not read
//! datasets, probe catalogs, execute plans, write data, materialize outputs,
//! invoke external engines, or provide fallback execution.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::Path,
    process::ExitCode,
};

#[cfg(feature = "universal-format-io")]
use shardloom_core::ScalarValue;
use shardloom_core::{
    ByteRange, CapabilityCertificationReport, CatalogKind, CatalogMetadataIntegrationGateEntry,
    CatalogMetadataIntegrationGateReport, CatalogRef, CdcEventKind, CdcEventSummary,
    CdcIncrementalPlanningReport, CdcManifestTransactionGateEntry,
    CdcManifestTransactionGateReport, ChangeSet, ColumnRef, CommandStatus,
    CompactionPlanningPolicy, CompactionPlanningReport, DatasetFormat, DatasetManifest, DatasetRef,
    DatasetUri, DeleteModel, DeleteTombstoneCompatibilityReport, Diagnostic, DiagnosticCategory,
    DiagnosticCode, DiagnosticSeverity, EncodedSegment, EncodingKind, FallbackStatus, FieldId,
    FieldName, FieldPath, FileDescriptor, FileRole, IncrementalPlanSkeleton, LayoutHealthPolicy,
    LayoutHealthReport, LayoutKind, LocalAppendOnlyCdcOverlaySmokeReport,
    LocalDeleteTombstoneReadSmokeReport, LocalTableMetadataReadSmokeReport, LogicalDType,
    ManifestId, ManifestSegment, Nullability, OutputFormat, OutputTarget,
    PartitionEvolutionCompatibilityReport, PartitionField, PartitionSpec, PartitionTransform,
    SchemaDefinition, SchemaEvolutionCompatibilityReport, SchemaEvolutionPolicy, SchemaField,
    SchemaId, SchemaVersion, SegmentChange, SegmentChangeKind, SegmentId, SegmentLayout,
    SegmentStats, ShardLoomError, SnapshotId, SnapshotRef, StatefulReusePromotionGateReport,
    StatefulReuseReport, TableCompatibilityPlan, TableCompatibilityReport, TableFormatKind,
    TableIntelligenceReport, TableMaintenanceExecutionMatrixReport,
    TableMaintenanceExecutionMatrixRow, WriteIntent, evaluate_cdc_incremental_planning,
    evaluate_compaction_planning, evaluate_delete_tombstone_compatibility, evaluate_layout_health,
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
    if format_kind == PlanInteropFormat::SubstraitLike {
        append_substrait_report_contract_fields(&mut fields, "import");
    }
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
    if format_kind == PlanInteropFormat::SubstraitLike {
        append_substrait_report_contract_fields(&mut fields, "export");
    }
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

pub(crate) fn handle_iceberg_metadata_read_smoke(
    args: impl Iterator<Item = String>,
    format: OutputFormat,
) -> ExitCode {
    let request = match parse_iceberg_metadata_read_smoke_args(args) {
        Ok(request) => request,
        Err(error) => {
            return emit_error(
                "iceberg-metadata-read-smoke",
                format,
                "Iceberg metadata read smoke failed",
                &error,
            );
        }
    };
    let report = match run_iceberg_metadata_read_smoke(&request) {
        Ok(report) => report,
        Err(error) => {
            return emit_error(
                "iceberg-metadata-read-smoke",
                format,
                "Iceberg metadata read smoke failed",
                &error,
            );
        }
    };
    let has_errors = report.has_errors();
    emit(
        "iceberg-metadata-read-smoke",
        format,
        if has_errors {
            CommandStatus::Unsupported
        } else {
            CommandStatus::Success
        },
        "source-reviewed Iceberg metadata JSON read smoke".to_string(),
        report.to_human_text(),
        report.diagnostics.clone(),
        iceberg_metadata_read_smoke_fields(&report),
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
    if let Some(operation) = workflow_unsupported_dataframe_operation(&normalized) {
        return Some(operation);
    }
    match normalized.as_str() {
        "write-vortex" => Some(workflow_unsupported_write_vortex()),
        "write-parquet" => Some(workflow_unsupported_write_parquet()),
        "write-arrow-ipc" | "write-arrow" | "write-ipc" => {
            Some(workflow_unsupported_write_arrow_ipc())
        }
        "write-avro" => Some(workflow_unsupported_write_avro()),
        "write-orc" => Some(workflow_unsupported_write_orc()),
        "sql" => Some(workflow_unsupported_sql()),
        "sql-parse" | "sql_parse" => Some(workflow_unsupported_sql_parse()),
        "sql-bind" | "sql_bind" => Some(workflow_unsupported_sql_bind()),
        "sql-plan" | "sql_plan" => Some(workflow_unsupported_sql_plan()),
        "sql-execute" | "sql_execute" => Some(workflow_unsupported_sql_execute()),
        "sql-source-free-projection" | "sql_source_free_projection" => {
            Some(workflow_unsupported_sql_source_free_projection())
        }
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
        "object-store-write" | "object_store_write" | "remote-object-store-write" => {
            Some(workflow_unsupported_object_store_write())
        }
        "table-commit" | "table_commit" | "lakehouse-commit" | "lakehouse_commit" => {
            Some(workflow_unsupported_table_commit())
        }
        "catalog-integration" | "catalog_integration" | "catalog-runtime" => {
            Some(workflow_unsupported_catalog_integration())
        }
        "remote-result-delivery" | "remote_result_delivery" | "remote-result" | "remote_result" => {
            Some(workflow_unsupported_remote_result_delivery())
        }
        "fallback-engine" | "spark-fallback" | "external-fallback" => {
            Some(workflow_unsupported_fallback_engine())
        }
        _ => None,
    }
}

fn workflow_unsupported_dataframe_operation(token: &str) -> Option<WorkflowUnsupportedOperation> {
    match token {
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
        "rename" | "rename-columns" | "rename_columns" => Some(workflow_unsupported_rename()),
        "drop" | "drop-columns" | "drop_columns" => Some(workflow_unsupported_drop()),
        "sample" => Some(workflow_unsupported_sample()),
        "explode" => Some(workflow_unsupported_explode()),
        "merge" => Some(workflow_unsupported_merge()),
        "concat" => Some(workflow_unsupported_concat()),
        "pivot" => Some(workflow_unsupported_pivot()),
        "pivot-table" | "pivot_table" => Some(workflow_unsupported_pivot_table()),
        "melt" | "unpivot" => Some(workflow_unsupported_melt()),
        "rolling" => Some(workflow_unsupported_rolling()),
        "tail" => Some(workflow_unsupported_tail()),
        "describe" => Some(workflow_unsupported_describe()),
        "nunique" => Some(workflow_unsupported_nunique()),
        "value-counts" | "value_counts" => Some(workflow_unsupported_value_counts()),
        "fillna" | "fill-null" | "fill_null" => Some(workflow_unsupported_fillna()),
        "isna" | "isnull" | "is-null" | "is_null" => Some(workflow_unsupported_isna()),
        "notna" | "notnull" | "not-null" | "not_null" => Some(workflow_unsupported_notna()),
        "apply" => Some(workflow_unsupported_apply()),
        "pipe" => Some(workflow_unsupported_pipe()),
        "transform" => Some(workflow_unsupported_transform()),
        "applymap" | "map-elements" | "map_elements" => Some(workflow_unsupported_applymap()),
        "map" => Some(workflow_unsupported_map()),
        "map-rows" | "map_rows" => Some(workflow_unsupported_map_rows()),
        "eval" => Some(workflow_unsupported_eval()),
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

fn workflow_unsupported_rename() -> WorkflowUnsupportedOperation {
    WorkflowUnsupportedOperation {
        operation: "rename",
        label: "DataFrame column rename",
        surface: "dataframe_schema_transform",
        feature: "cg21.workflow.rename",
        blocker_id: "cg21.workflow.rename.schema_rewrite_unsupported",
        required_evidence: "schema_rewrite_semantics,projection_alias_contract,execution_certificate,native_io_certificate,no_fallback_evidence",
        suggested_next_action: "Use explicit select aliases where scoped projection evidence exists; broad DataFrame rename requires schema rewrite evidence before execution.",
        diagnostic_code: DiagnosticCode::NotImplemented,
        materialization_required: false,
        write_required: false,
        runtime_required: true,
    }
}

fn workflow_unsupported_drop() -> WorkflowUnsupportedOperation {
    WorkflowUnsupportedOperation {
        operation: "drop",
        label: "DataFrame column drop",
        surface: "dataframe_schema_transform",
        feature: "cg21.workflow.drop",
        blocker_id: "cg21.workflow.drop.schema_projection_unsupported",
        required_evidence: "schema_discovery,projection_rewrite_semantics,execution_certificate,native_io_certificate,no_fallback_evidence",
        suggested_next_action: "Use explicit select(...) projections for admitted columns; broad drop(...) requires schema-aware projection rewrite evidence before execution.",
        diagnostic_code: DiagnosticCode::NotImplemented,
        materialization_required: false,
        write_required: false,
        runtime_required: true,
    }
}

fn workflow_unsupported_sample() -> WorkflowUnsupportedOperation {
    WorkflowUnsupportedOperation {
        operation: "sample",
        label: "DataFrame sampling",
        surface: "dataframe_sampling",
        feature: "cg21.workflow.sample",
        blocker_id: "cg21.workflow.sample.sampling_semantics_unsupported",
        required_evidence: "sampling_semantics,deterministic_seed_policy,semantic_conformance_suite,execution_certificate,no_fallback_evidence",
        suggested_next_action: "Use deterministic limit/order evidence where applicable; random or fraction sampling needs native sampling semantics before execution.",
        diagnostic_code: DiagnosticCode::NotImplemented,
        materialization_required: false,
        write_required: false,
        runtime_required: true,
    }
}

fn workflow_unsupported_explode() -> WorkflowUnsupportedOperation {
    WorkflowUnsupportedOperation {
        operation: "explode",
        label: "DataFrame nested/list explode",
        surface: "dataframe_nested_transform",
        feature: "cg21.workflow.explode",
        blocker_id: "cg21.workflow.explode.nested_expansion_unsupported",
        required_evidence: "nested_type_semantics,list_expansion_operator,semantic_conformance_suite,execution_certificate,native_io_certificate,no_fallback_evidence",
        suggested_next_action: "Keep nested/list expansion as an explicit blocker until native nested semantics and conformance fixtures are certified.",
        diagnostic_code: DiagnosticCode::NotImplemented,
        materialization_required: false,
        write_required: false,
        runtime_required: true,
    }
}

fn workflow_unsupported_merge() -> WorkflowUnsupportedOperation {
    WorkflowUnsupportedOperation {
        operation: "merge",
        label: "DataFrame merge",
        surface: "dataframe_join_alias",
        feature: "cg21.workflow.merge",
        blocker_id: "cg21.workflow.merge.join_alias_unsupported",
        required_evidence: "join_alias_semantics,key_resolution_contract,join_operator_capability,execution_certificate,native_io_certificate,no_fallback_evidence",
        suggested_next_action: "Use scoped join(...) only where local runtime evidence exists; broad pandas-style merge requires key inference, suffix, alias, and no-fallback evidence before execution.",
        diagnostic_code: DiagnosticCode::UnsupportedSql,
        materialization_required: false,
        write_required: false,
        runtime_required: true,
    }
}

fn workflow_unsupported_concat() -> WorkflowUnsupportedOperation {
    WorkflowUnsupportedOperation {
        operation: "concat",
        label: "DataFrame concat",
        surface: "dataframe_union_concat",
        feature: "cg21.workflow.concat",
        blocker_id: "cg21.workflow.concat.union_alignment_unsupported",
        required_evidence: "schema_alignment_contract,set_operation_semantics,axis_semantics,execution_certificate,native_io_certificate,no_fallback_evidence",
        suggested_next_action: "Use scoped union/union_all only where both inputs lower to admitted local-source SQL; broad concat needs axis and schema-alignment evidence before execution.",
        diagnostic_code: DiagnosticCode::NotImplemented,
        materialization_required: false,
        write_required: false,
        runtime_required: true,
    }
}

fn workflow_unsupported_pivot() -> WorkflowUnsupportedOperation {
    WorkflowUnsupportedOperation {
        operation: "pivot",
        label: "DataFrame pivot",
        surface: "dataframe_reshape",
        feature: "cg21.workflow.pivot",
        blocker_id: "cg21.workflow.pivot.reshape_semantics_unsupported",
        required_evidence: "reshape_semantics,grouping_key_contract,materialization_boundary,execution_certificate,no_fallback_evidence",
        suggested_next_action: "Keep pivot as an explicit reshape blocker until key cardinality, output schema, materialization, and no-fallback evidence are certified.",
        diagnostic_code: DiagnosticCode::NotImplemented,
        materialization_required: false,
        write_required: false,
        runtime_required: true,
    }
}

fn workflow_unsupported_pivot_table() -> WorkflowUnsupportedOperation {
    WorkflowUnsupportedOperation {
        operation: "pivot_table",
        label: "DataFrame pivot table",
        surface: "dataframe_aggregate_reshape",
        feature: "cg21.workflow.pivot_table",
        blocker_id: "cg21.workflow.pivot_table.aggregate_reshape_unsupported",
        required_evidence: "aggregate_reshape_semantics,aggregate_operator_capability,grouping_key_contract,execution_certificate,no_fallback_evidence",
        suggested_next_action: "Use scoped aggregate/group_by only where evidence exists; aggregate reshape requires native pivot-table semantics and no-fallback evidence.",
        diagnostic_code: DiagnosticCode::UnsupportedSql,
        materialization_required: false,
        write_required: false,
        runtime_required: true,
    }
}

fn workflow_unsupported_melt() -> WorkflowUnsupportedOperation {
    WorkflowUnsupportedOperation {
        operation: "melt",
        label: "DataFrame melt",
        surface: "dataframe_reshape",
        feature: "cg21.workflow.melt",
        blocker_id: "cg21.workflow.melt.reshape_semantics_unsupported",
        required_evidence: "unpivot_semantics,schema_alignment_contract,materialization_boundary,execution_certificate,no_fallback_evidence",
        suggested_next_action: "Keep melt/unpivot as an explicit reshape blocker until native unpivot semantics and materialization evidence are certified.",
        diagnostic_code: DiagnosticCode::NotImplemented,
        materialization_required: false,
        write_required: false,
        runtime_required: true,
    }
}

fn workflow_unsupported_rolling() -> WorkflowUnsupportedOperation {
    WorkflowUnsupportedOperation {
        operation: "rolling",
        label: "DataFrame rolling window",
        surface: "dataframe_window",
        feature: "cg21.workflow.rolling",
        blocker_id: "cg21.workflow.rolling.window_semantics_unsupported",
        required_evidence: "window_frame_semantics,ordering_contract,window_operator_capability,execution_certificate,no_fallback_evidence",
        suggested_next_action: "Use scoped window(...) only for admitted SQL window projections; rolling DataFrame windows require native frame semantics and no-fallback evidence.",
        diagnostic_code: DiagnosticCode::UnsupportedSql,
        materialization_required: false,
        write_required: false,
        runtime_required: true,
    }
}

fn workflow_unsupported_tail() -> WorkflowUnsupportedOperation {
    WorkflowUnsupportedOperation {
        operation: "tail",
        label: "DataFrame tail",
        surface: "dataframe_row_inspection",
        feature: "cg21.workflow.tail",
        blocker_id: "cg21.workflow.tail.source_order_unsupported",
        required_evidence: "source_order_semantics,reverse_scan_or_stable_ordering,materialization_boundary,execution_certificate,no_fallback_evidence",
        suggested_next_action: "Use scoped head/preview for bounded source-order inspection; tail requires stable source ordering or reverse-scan evidence before execution.",
        diagnostic_code: DiagnosticCode::MaterializationRequired,
        materialization_required: true,
        write_required: false,
        runtime_required: true,
    }
}

fn workflow_unsupported_describe() -> WorkflowUnsupportedOperation {
    WorkflowUnsupportedOperation {
        operation: "describe",
        label: "DataFrame summary statistics",
        surface: "dataframe_summary_statistics",
        feature: "cg21.workflow.describe",
        blocker_id: "cg21.workflow.describe.summary_statistics_unsupported",
        required_evidence: "summary_statistics_semantics,numeric_dtype_policy,null_semantics,execution_certificate,no_fallback_evidence",
        suggested_next_action: "Use schema(), describe_schema(), data_quality_summary(), or profile() for scoped reports; pandas-style describe requires native summary-statistics semantics.",
        diagnostic_code: DiagnosticCode::NotImplemented,
        materialization_required: true,
        write_required: false,
        runtime_required: true,
    }
}

fn workflow_unsupported_nunique() -> WorkflowUnsupportedOperation {
    WorkflowUnsupportedOperation {
        operation: "nunique",
        label: "DataFrame distinct count",
        surface: "dataframe_summary_statistics",
        feature: "cg21.workflow.nunique",
        blocker_id: "cg21.workflow.nunique.distinct_count_semantics_unsupported",
        required_evidence: "distinct_count_semantics,dropna_policy,aggregate_operator_capability,execution_certificate,no_fallback_evidence",
        suggested_next_action: "Use explicit count_distinct(...) aggregates where scoped SQL evidence exists; pandas-style nunique requires axis/dropna/result-shape evidence.",
        diagnostic_code: DiagnosticCode::NotImplemented,
        materialization_required: true,
        write_required: false,
        runtime_required: true,
    }
}

fn workflow_unsupported_value_counts() -> WorkflowUnsupportedOperation {
    WorkflowUnsupportedOperation {
        operation: "value_counts",
        label: "DataFrame value counts",
        surface: "dataframe_summary_statistics",
        feature: "cg21.workflow.value_counts",
        blocker_id: "cg21.workflow.value_counts.grouped_count_semantics_unsupported",
        required_evidence: "grouped_count_semantics,dropna_policy,ordering_contract,execution_certificate,no_fallback_evidence",
        suggested_next_action: "Use explicit group_by(...).count() where scoped runtime evidence exists; pandas-style value_counts needs null, ordering, and result-shape evidence.",
        diagnostic_code: DiagnosticCode::NotImplemented,
        materialization_required: true,
        write_required: false,
        runtime_required: true,
    }
}

fn workflow_unsupported_fillna() -> WorkflowUnsupportedOperation {
    WorkflowUnsupportedOperation {
        operation: "fillna",
        label: "DataFrame fill nulls",
        surface: "dataframe_null_transform",
        feature: "cg21.workflow.fillna",
        blocker_id: "cg21.workflow.fillna.null_fill_semantics_unsupported",
        required_evidence: "null_fill_semantics,dtype_coercion_policy,projection_rewrite_semantics,execution_certificate,no_fallback_evidence",
        suggested_next_action: "Use explicit column expressions where scoped fill_null evidence exists; broad fillna requires dtype and schema rewrite evidence before execution.",
        diagnostic_code: DiagnosticCode::NotImplemented,
        materialization_required: false,
        write_required: false,
        runtime_required: true,
    }
}

fn workflow_unsupported_isna() -> WorkflowUnsupportedOperation {
    WorkflowUnsupportedOperation {
        operation: "isna",
        label: "DataFrame null mask",
        surface: "dataframe_null_mask",
        feature: "cg21.workflow.isna",
        blocker_id: "cg21.workflow.isna.null_mask_semantics_unsupported",
        required_evidence: "null_mask_semantics,three_valued_logic_policy,projection_result_shape,execution_certificate,no_fallback_evidence",
        suggested_next_action: "Use explicit column predicates where scoped null semantics exist; DataFrame-wide null masks require result-shape evidence before execution.",
        diagnostic_code: DiagnosticCode::NotImplemented,
        materialization_required: false,
        write_required: false,
        runtime_required: true,
    }
}

fn workflow_unsupported_notna() -> WorkflowUnsupportedOperation {
    WorkflowUnsupportedOperation {
        operation: "notna",
        label: "DataFrame non-null mask",
        surface: "dataframe_null_mask",
        feature: "cg21.workflow.notna",
        blocker_id: "cg21.workflow.notna.null_mask_semantics_unsupported",
        required_evidence: "not_null_mask_semantics,three_valued_logic_policy,projection_result_shape,execution_certificate,no_fallback_evidence",
        suggested_next_action: "Use explicit column predicates where scoped null semantics exist; DataFrame-wide non-null masks require result-shape evidence before execution.",
        diagnostic_code: DiagnosticCode::NotImplemented,
        materialization_required: false,
        write_required: false,
        runtime_required: true,
    }
}

fn workflow_unsupported_apply() -> WorkflowUnsupportedOperation {
    WorkflowUnsupportedOperation {
        operation: "apply",
        label: "DataFrame Python apply",
        surface: "dataframe_python_callable",
        feature: "cg21.workflow.apply",
        blocker_id: "cg21.workflow.apply.python_callable_unsupported",
        required_evidence: "python_callable_policy,udf_type_contract,sandbox_policy,execution_certificate,no_fallback_evidence",
        suggested_next_action: "Use typed ShardLoom expressions or registered UDF plans; Python apply cannot execute without explicit callable, sandbox, and effect evidence.",
        diagnostic_code: DiagnosticCode::UnsupportedEffect,
        materialization_required: false,
        write_required: false,
        runtime_required: true,
    }
}

fn workflow_unsupported_pipe() -> WorkflowUnsupportedOperation {
    WorkflowUnsupportedOperation {
        operation: "pipe",
        label: "DataFrame Python pipe",
        surface: "dataframe_python_callable",
        feature: "cg21.workflow.pipe",
        blocker_id: "cg21.workflow.pipe.python_callable_unsupported",
        required_evidence: "python_callable_policy,workflow_type_contract,sandbox_policy,execution_certificate,no_fallback_evidence",
        suggested_next_action: "Use typed ShardLoom workflow operators or registered extension plans; Python pipe cannot execute without explicit workflow typing, sandbox, and effect evidence.",
        diagnostic_code: DiagnosticCode::UnsupportedEffect,
        materialization_required: false,
        write_required: false,
        runtime_required: true,
    }
}

fn workflow_unsupported_transform() -> WorkflowUnsupportedOperation {
    WorkflowUnsupportedOperation {
        operation: "transform",
        label: "DataFrame Python transform",
        surface: "dataframe_python_callable",
        feature: "cg21.workflow.transform",
        blocker_id: "cg21.workflow.transform.python_callable_unsupported",
        required_evidence: "python_callable_policy,transform_result_shape_contract,sandbox_policy,execution_certificate,no_fallback_evidence",
        suggested_next_action: "Use typed ShardLoom projection expressions where scoped evidence exists; Python transform cannot execute without explicit result-shape, sandbox, and effect evidence.",
        diagnostic_code: DiagnosticCode::UnsupportedEffect,
        materialization_required: false,
        write_required: false,
        runtime_required: true,
    }
}

fn workflow_unsupported_applymap() -> WorkflowUnsupportedOperation {
    WorkflowUnsupportedOperation {
        operation: "applymap",
        label: "DataFrame Python applymap",
        surface: "dataframe_python_callable",
        feature: "cg21.workflow.applymap",
        blocker_id: "cg21.workflow.applymap.python_callable_unsupported",
        required_evidence: "python_callable_policy,elementwise_type_contract,sandbox_policy,execution_certificate,no_fallback_evidence",
        suggested_next_action: "Use typed ShardLoom scalar expressions where scoped evidence exists; Python applymap cannot execute without explicit element-wise typing, sandbox, and effect evidence.",
        diagnostic_code: DiagnosticCode::UnsupportedEffect,
        materialization_required: false,
        write_required: false,
        runtime_required: true,
    }
}

fn workflow_unsupported_map() -> WorkflowUnsupportedOperation {
    WorkflowUnsupportedOperation {
        operation: "map",
        label: "DataFrame Python map",
        surface: "dataframe_python_callable",
        feature: "cg21.workflow.map",
        blocker_id: "cg21.workflow.map.python_callable_unsupported",
        required_evidence: "python_callable_policy,elementwise_type_contract,sandbox_policy,execution_certificate,no_fallback_evidence",
        suggested_next_action: "Use typed ShardLoom expressions or registered UDF plans; Python map cannot execute without explicit callable, sandbox, and effect evidence.",
        diagnostic_code: DiagnosticCode::UnsupportedEffect,
        materialization_required: false,
        write_required: false,
        runtime_required: true,
    }
}

fn workflow_unsupported_map_rows() -> WorkflowUnsupportedOperation {
    WorkflowUnsupportedOperation {
        operation: "map_rows",
        label: "DataFrame row-wise Python map",
        surface: "dataframe_python_callable",
        feature: "cg21.workflow.map_rows",
        blocker_id: "cg21.workflow.map_rows.python_callable_unsupported",
        required_evidence: "python_callable_policy,row_udf_type_contract,sandbox_policy,execution_certificate,no_fallback_evidence",
        suggested_next_action: "Use typed ShardLoom expressions or registered UDF plans; row-wise Python maps require explicit schema, sandbox, and effect evidence.",
        diagnostic_code: DiagnosticCode::UnsupportedEffect,
        materialization_required: false,
        write_required: false,
        runtime_required: true,
    }
}

fn workflow_unsupported_eval() -> WorkflowUnsupportedOperation {
    WorkflowUnsupportedOperation {
        operation: "eval",
        label: "DataFrame expression eval",
        surface: "dataframe_expression",
        feature: "cg21.workflow.eval",
        blocker_id: "cg21.workflow.eval.expression_engine_unsupported",
        required_evidence: "expression_engine_policy,typed_expression_contract,semantic_conformance_suite,execution_certificate,no_fallback_evidence",
        suggested_next_action: "Use typed ShardLoom expression builders where scoped evidence exists; DataFrame eval cannot route to pandas, numexpr, Python eval, or another hidden expression engine.",
        diagnostic_code: DiagnosticCode::UnsupportedEffect,
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

fn workflow_unsupported_write_arrow_ipc() -> WorkflowUnsupportedOperation {
    WorkflowUnsupportedOperation {
        operation: "write_arrow_ipc",
        label: "Arrow IPC compatibility export",
        surface: "compatibility_export_write",
        feature: "cg21.workflow.write_arrow_ipc",
        blocker_id: "cg21.workflow.write_arrow_ipc.compatibility_export_unsupported",
        required_evidence: "translation_fidelity_report,decoded_columnar_boundary,write_intent",
        suggested_next_action: "Use scoped local-source output smokes or plan-export for compatibility-export posture before relying on broad Arrow IPC writes.",
        diagnostic_code: DiagnosticCode::UnsupportedOutputFormat,
        materialization_required: true,
        write_required: true,
        runtime_required: true,
    }
}

fn workflow_unsupported_write_avro() -> WorkflowUnsupportedOperation {
    WorkflowUnsupportedOperation {
        operation: "write_avro",
        label: "Avro compatibility export",
        surface: "compatibility_export_write",
        feature: "cg21.workflow.write_avro",
        blocker_id: "cg21.workflow.write_avro.compatibility_export_unsupported",
        required_evidence: "translation_fidelity_report,schema_evolution_policy,write_intent",
        suggested_next_action: "Use scoped local-source output smokes or plan-export for compatibility-export posture before relying on broad Avro writes.",
        diagnostic_code: DiagnosticCode::UnsupportedOutputFormat,
        materialization_required: true,
        write_required: true,
        runtime_required: true,
    }
}

fn workflow_unsupported_write_orc() -> WorkflowUnsupportedOperation {
    WorkflowUnsupportedOperation {
        operation: "write_orc",
        label: "ORC compatibility export",
        surface: "compatibility_export_write",
        feature: "cg21.workflow.write_orc",
        blocker_id: "cg21.workflow.write_orc.compatibility_export_unsupported",
        required_evidence: "translation_fidelity_report,stripe_statistics_policy,write_intent",
        suggested_next_action: "Use scoped local-source output smokes or plan-export for compatibility-export posture before relying on broad ORC writes.",
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

fn workflow_unsupported_sql_source_free_projection() -> WorkflowUnsupportedOperation {
    WorkflowUnsupportedOperation {
        operation: "sql_source_free_projection",
        label: "Broad SQL source-free projection",
        surface: "sql_generated_source",
        feature: "gar_gen_1.sql_source_free_projection",
        blocker_id: "gar-gen-1.sql_source_free_projection_broad_runtime_blocked",
        required_evidence: "sql_parser,binder,planner,source_free_projection_contract,generated_source_certificate,output_native_io_certificate,no_fallback_evidence",
        suggested_next_action: "Use ctx.sql_values(...).write(...), ctx.sql_literal_select(...).write(...), or ctx.sql(\"SELECT ... FROM range(...)\").write(...) for scoped local output smokes; arbitrary source-free SQL projection remains blocked until expression evidence is certified.",
        diagnostic_code: DiagnosticCode::UnsupportedSql,
        materialization_required: false,
        write_required: false,
        runtime_required: true,
    }
}

fn workflow_unsupported_dataframe_source_free_projection() -> WorkflowUnsupportedOperation {
    WorkflowUnsupportedOperation {
        operation: "dataframe_source_free_projection",
        label: "Broad DataFrame source-free projection",
        surface: "dataframe_generated_source",
        feature: "gar_gen_1.dataframe_source_free_projection",
        blocker_id: "gar-gen-1.dataframe_source_free_projection_broad_expression_blocked",
        required_evidence: "dataframe_plan_contract,expression_registry,broad_literal_and_expression_projection_contract,generated_source_certificate,output_native_io_certificate,execution_certificate,no_fallback_evidence",
        suggested_next_action: "Use ctx.dataframe_source_free_projection(\"lit(...).alias('name')\").write(...), ctx.from_rows(...).write(...), ctx.literal_table(...).write(...), ctx.range(...).write(...), or ctx.calendar(...).write(...) for scoped local generated-output smokes.",
        diagnostic_code: DiagnosticCode::NotImplemented,
        materialization_required: false,
        write_required: false,
        runtime_required: true,
    }
}

fn workflow_unsupported_dataframe_generated_with_column() -> WorkflowUnsupportedOperation {
    WorkflowUnsupportedOperation {
        operation: "dataframe_generated_with_column",
        label: "Broad generated DataFrame with_column expression",
        surface: "dataframe_generated_source",
        feature: "gar_gen_1.dataframe_generated_with_column",
        blocker_id: "gar-gen-1.dataframe_generated_with_column_broad_expression_runtime_blocked",
        required_evidence: "dataframe_plan_contract,expression_registry,type_coercion_contract,generated_source_certificate,output_native_io_certificate,execution_certificate,no_fallback_evidence",
        suggested_next_action: "Use ctx.from_rows(...).with_column(...) for scoped literal generated columns or ctx.range(...).with_column(...) for scoped int64 generated-range expressions; broad generated DataFrame expression columns remain blocked until expression lowering and evidence are certified.",
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

fn workflow_unsupported_object_store_write() -> WorkflowUnsupportedOperation {
    WorkflowUnsupportedOperation {
        operation: "object_store_write",
        label: "remote object-store workflow write",
        surface: "object_store_sink",
        feature: "cg21.workflow.object_store_write",
        blocker_id: "cg21.workflow.object_store_write.runtime_unsupported",
        required_evidence: "object_store_capability_policy,credential_policy,commit_protocol,retry_recovery_evidence,native_io_certificate,execution_certificate",
        suggested_next_action: "Use scoped local-emulator object-store write and recovery smokes until remote object-store writes have credential, commit, and replay evidence.",
        diagnostic_code: DiagnosticCode::ObjectStoreUnsupported,
        materialization_required: false,
        write_required: true,
        runtime_required: true,
    }
}

fn workflow_unsupported_table_commit() -> WorkflowUnsupportedOperation {
    WorkflowUnsupportedOperation {
        operation: "table_commit",
        label: "table/lakehouse workflow commit",
        surface: "table_lakehouse_commit",
        feature: "cg21.workflow.table_commit",
        blocker_id: "cg21.workflow.table_commit.runtime_unsupported",
        required_evidence: "table_catalog_contract,lakehouse_transaction_policy,commit_protocol,rollback_recovery_proof,native_io_certificate,execution_certificate",
        suggested_next_action: "Use local table append/recovery smokes for bounded manifest proof; production table commits require catalog transaction, rollback, and recovery evidence.",
        diagnostic_code: DiagnosticCode::ObjectStoreUnsupported,
        materialization_required: false,
        write_required: true,
        runtime_required: true,
    }
}

fn workflow_unsupported_catalog_integration() -> WorkflowUnsupportedOperation {
    WorkflowUnsupportedOperation {
        operation: "catalog_integration",
        label: "catalog integration workflow",
        surface: "catalog_integration",
        feature: "cg21.workflow.catalog_integration",
        blocker_id: "cg21.workflow.catalog_integration.runtime_unsupported",
        required_evidence: "catalog_schema_contract,catalog_transaction_policy,credential_policy,side_effect_policy,native_io_certificate,execution_certificate",
        suggested_next_action: "Use catalog metadata gate reports until external catalog resolution and transaction semantics are certified.",
        diagnostic_code: DiagnosticCode::ObjectStoreUnsupported,
        materialization_required: false,
        write_required: false,
        runtime_required: true,
    }
}

fn workflow_unsupported_remote_result_delivery() -> WorkflowUnsupportedOperation {
    WorkflowUnsupportedOperation {
        operation: "remote_result_delivery",
        label: "remote result delivery workflow",
        surface: "remote_result_delivery",
        feature: "cg21.workflow.remote_result_delivery",
        blocker_id: "cg21.workflow.remote_result_delivery.runtime_unsupported",
        required_evidence: "remote_result_contract,data_plane_policy,credential_policy,replay_fidelity_evidence,native_io_certificate,execution_certificate",
        suggested_next_action: "Use local result sinks and explicit output reports until remote result delivery has data-plane, replay, and credential evidence.",
        diagnostic_code: DiagnosticCode::ObjectStoreUnsupported,
        materialization_required: true,
        write_required: true,
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

fn append_substrait_report_contract_fields(fields: &mut Vec<(String, String)>, direction: &str) {
    push_field(
        fields,
        "substrait_report_contract_schema_version",
        "shardloom.substrait_report_only_contract.v1",
    );
    push_field(
        fields,
        "substrait_report_contract_report_id",
        "gar-0022-a.substrait_import_export_report_only_contract",
    );
    push_field(fields, "substrait_report_contract_direction", direction);
    push_field(
        fields,
        "substrait_report_contract_docs_ref",
        "docs/architecture/substrait-report-only-contract.md",
    );
    push_field(
        fields,
        "substrait_report_contract_support_status",
        "report_only",
    );
    push_field(fields, "substrait_import_parser_status", "not_implemented");
    push_field(
        fields,
        "substrait_export_serializer_status",
        "not_implemented",
    );
    push_field(
        fields,
        "substrait_imported_plan_execution_status",
        "blocked",
    );
    push_field(fields, "substrait_dependency_status", "not_added");
    push_bool_field(fields, "substrait_dependency_license_approved", false);
    push_bool_field(fields, "substrait_parser_executed", false);
    push_bool_field(fields, "substrait_payload_parsed", false);
    push_bool_field(fields, "substrait_export_serialization_performed", false);
    push_bool_field(fields, "substrait_imported_plan_execution_allowed", false);
    push_bool_field(fields, "substrait_runtime_execution", false);
    push_bool_field(fields, "substrait_external_engine_invoked", false);
    push_bool_field(fields, "substrait_fallback_attempted", false);
    push_field(fields, "substrait_claim_gate_status", "not_claim_grade");
    push_field(
        fields,
        "substrait_blocker_ids",
        "gar-0022-a.substrait_dependency_not_approved,gar-0022-a.substrait_parser_not_implemented,gar-0022-a.substrait_exporter_not_implemented,gar-0022-a.imported_plan_execution_blocked",
    );
    push_field(
        fields,
        "substrait_required_evidence",
        "dependency_license_approval,parser_schema_version,construct_coverage_matrix,roundtrip_fixtures,imported_plan_capability_gate,no_fallback_evidence",
    );
    push_field(
        fields,
        "substrait_claim_boundary",
        "report_only_no_substrait_import_export_execution",
    );
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
        "substrait" | "substrait-like" => PlanInteropFormat::SubstraitLike,
        "json-like" => PlanInteropFormat::JsonLike,
        _ => PlanInteropFormat::Unknown,
    }
}
pub(crate) fn stateful_reuse_fields(report: &StatefulReuseReport) -> Vec<(String, String)> {
    let mut fields = vec![];
    append_stateful_reuse_identity_fields(&mut fields, report);
    append_stateful_reuse_requirement_fields(&mut fields, report);
    append_stateful_reuse_side_effect_fields(&mut fields, report);
    fields.extend(crate::gar_0029_evidence::gar_0029_evidence_expansion_fields());
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
    push_bool_field(
        fields,
        &format!("{prefix}_local_table_append_commit_rehearsal_smoke_present"),
        report.local_table_append_commit_rehearsal_smoke_present,
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

#[derive(Debug, Clone, PartialEq, Eq)]
struct IcebergMetadataReadSmokeRequest {
    metadata_path: String,
    selection: IcebergSnapshotSelectionRequest,
    manifest_list_path: Option<String>,
    manifest_file_path: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum IcebergSnapshotSelectionRequest {
    Current,
    SnapshotId(String),
    AsOfTimestampMillis(i64),
}

impl IcebergSnapshotSelectionRequest {
    fn selector_kind(&self) -> &'static str {
        match self {
            Self::Current => "current_snapshot",
            Self::SnapshotId(_) => "snapshot_id",
            Self::AsOfTimestampMillis(_) => "as_of_timestamp_ms",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IcebergMetadataBlockedPath {
    ExternalCatalogResolution,
    RemoteObjectStoreMetadataRead,
    ManifestListRead,
    ManifestFileRead,
    DataFileScan,
    DeleteFileSemantics,
    TableWriteCommit,
    BroadIcebergRuntime,
    DeltaHudiRuntime,
    LakehouseProductionClaim,
}

impl IcebergMetadataBlockedPath {
    const fn as_str(self) -> &'static str {
        match self {
            Self::ExternalCatalogResolution => "external_catalog_resolution",
            Self::RemoteObjectStoreMetadataRead => "remote_object_store_metadata_read",
            Self::ManifestListRead => "manifest_list_read",
            Self::ManifestFileRead => "manifest_file_read",
            Self::DataFileScan => "data_file_scan",
            Self::DeleteFileSemantics => "delete_file_semantics",
            Self::TableWriteCommit => "table_write_commit",
            Self::BroadIcebergRuntime => "broad_iceberg_runtime",
            Self::DeltaHudiRuntime => "delta_hudi_runtime",
            Self::LakehouseProductionClaim => "lakehouse_production_claim",
        }
    }

    const fn diagnostic_code(self) -> DiagnosticCode {
        match self {
            Self::RemoteObjectStoreMetadataRead => DiagnosticCode::ObjectStoreUnsupported,
            Self::BroadIcebergRuntime | Self::DeltaHudiRuntime => {
                DiagnosticCode::ExternalEffectDisabled
            }
            Self::ExternalCatalogResolution
            | Self::ManifestListRead
            | Self::ManifestFileRead
            | Self::DataFileScan
            | Self::DeleteFileSemantics
            | Self::TableWriteCommit
            | Self::LakehouseProductionClaim => DiagnosticCode::NotImplemented,
        }
    }

    const fn diagnostic_category(self) -> DiagnosticCategory {
        match self {
            Self::RemoteObjectStoreMetadataRead => DiagnosticCategory::ObjectStore,
            Self::BroadIcebergRuntime | Self::DeltaHudiRuntime => {
                DiagnosticCategory::ExternalEffect
            }
            Self::DataFileScan | Self::DeleteFileSemantics | Self::TableWriteCommit => {
                DiagnosticCategory::Execution
            }
            Self::ExternalCatalogResolution
            | Self::ManifestListRead
            | Self::ManifestFileRead
            | Self::LakehouseProductionClaim => DiagnosticCategory::Planning,
        }
    }

    fn to_diagnostic(self) -> Diagnostic {
        Diagnostic::new(
            self.diagnostic_code(),
            DiagnosticSeverity::Info,
            self.diagnostic_category(),
            format!(
                "{} remains blocked outside the Iceberg metadata JSON smoke scope",
                self.as_str()
            ),
            Some(self.as_str().to_string()),
            Some(
                "The smoke only reads one local Iceberg table metadata JSON file and, when explicitly requested with the feature enabled, one local manifest-list Avro summary."
                    .to_string(),
            ),
            Some(
                "Keep catalog, manifest-file, data-file, delete, write, and production lakehouse paths blocked until dedicated runtime evidence lands."
                    .to_string(),
            ),
            FallbackStatus::disabled_by_policy(),
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct IcebergMetadataSnapshotSummary {
    snapshot_id: String,
    sequence_number: Option<i64>,
    timestamp_ms: Option<i64>,
    manifest_list: String,
    operation: String,
    delete_file_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct IcebergManifestListSummary {
    path: String,
    bytes_read: usize,
    schema_column_count: usize,
    projected_column_count: usize,
    entry_count: usize,
    partition_spec_id_order: Vec<String>,
    data_manifest_count: usize,
    delete_manifest_count: usize,
    unknown_content_manifest_count: usize,
    total_manifest_bytes: u64,
    added_data_file_count: u64,
    existing_data_file_count: u64,
    deleted_data_file_count: u64,
    added_delete_file_count: u64,
    existing_delete_file_count: u64,
    deleted_delete_file_count: u64,
    planned_manifest_split_count: usize,
    planned_data_file_count: u64,
    manifest_summary_pruning_rule: &'static str,
}

impl IcebergManifestListSummary {
    fn total_delete_file_count(&self) -> u64 {
        self.added_delete_file_count
            .saturating_add(self.existing_delete_file_count)
            .saturating_add(self.deleted_delete_file_count)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct IcebergManifestFileSummary {
    path: String,
    bytes_read: usize,
    schema_column_count: usize,
    projected_column_count: usize,
    entry_count: usize,
    added_data_file_count: u64,
    existing_data_file_count: u64,
    deleted_data_file_count: u64,
    delete_file_entry_count: u64,
    position_delete_file_entry_count: u64,
    equality_delete_file_entry_count: u64,
    deletion_vector_entry_count: u64,
    unknown_content_file_count: u64,
    unknown_status_entry_count: u64,
    total_record_count: u64,
    total_file_size_bytes: u64,
    planned_data_file_count: u64,
    planned_data_file_bytes: u64,
    split_planning_rule: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct IcebergSchemaEvolutionSummary {
    schema_id_order: Vec<String>,
    schema_evolution_present: bool,
    added_field_id_count: usize,
    dropped_field_id_count: usize,
    renamed_field_id_count: usize,
    type_changed_field_id_count: usize,
    requiredness_changed_field_id_count: usize,
    required_field_added_count: usize,
    missing_field_id_count: usize,
    duplicate_field_id_count: usize,
    complex_or_nested_evolution_field_count: usize,
    admission_status: &'static str,
    admission_rule: &'static str,
}

impl IcebergSchemaEvolutionSummary {
    fn blocks_runtime_projection(&self) -> bool {
        self.missing_field_id_count > 0
            || self.duplicate_field_id_count > 0
            || self.type_changed_field_id_count > 0
            || self.requiredness_changed_field_id_count > 0
            || self.required_field_added_count > 0
            || self.complex_or_nested_evolution_field_count > 0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct IcebergPartitionEvolutionSummary {
    partition_spec_id_order: Vec<String>,
    partition_evolution_present: bool,
    default_partition_spec_id: String,
    last_partition_id: String,
    added_partition_field_count: usize,
    removed_partition_field_count: usize,
    renamed_partition_field_count: usize,
    source_changed_partition_field_count: usize,
    transform_changed_partition_field_count: usize,
    missing_partition_field_id_count: usize,
    duplicate_partition_field_id_count: usize,
    field_id_reuse_mismatch_count: usize,
    unknown_transform_count: usize,
    manifest_partition_spec_id_order: Vec<String>,
    manifest_unknown_partition_spec_id_count: usize,
    admission_status: &'static str,
    admission_rule: &'static str,
}

impl IcebergPartitionEvolutionSummary {
    fn blocks_runtime_projection(&self) -> bool {
        self.missing_partition_field_id_count > 0
            || self.duplicate_partition_field_id_count > 0
            || self.field_id_reuse_mismatch_count > 0
            || self.source_changed_partition_field_count > 0
            || self.transform_changed_partition_field_count > 0
            || self.unknown_transform_count > 0
            || self.manifest_unknown_partition_spec_id_count > 0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct IcebergDeleteAdmissionSummary {
    selected_snapshot_delete_file_count: u64,
    manifest_list_delete_manifest_count: usize,
    manifest_list_delete_file_count: u64,
    manifest_file_deleted_data_file_count: u64,
    manifest_file_position_delete_file_count: u64,
    manifest_file_equality_delete_file_count: u64,
    manifest_file_deletion_vector_count: u64,
    manifest_file_unknown_delete_content_count: u64,
    admission_status: &'static str,
    admission_rule: &'static str,
}

impl IcebergDeleteAdmissionSummary {
    fn blocks_runtime_delete_execution(&self) -> bool {
        self.selected_snapshot_delete_file_count > 0
            || self.manifest_list_delete_manifest_count > 0
            || self.manifest_list_delete_file_count > 0
            || self.manifest_file_deleted_data_file_count > 0
            || self.manifest_file_position_delete_file_count > 0
            || self.manifest_file_equality_delete_file_count > 0
            || self.manifest_file_deletion_vector_count > 0
            || self.manifest_file_unknown_delete_content_count > 0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct IcebergReportScope {
    report_id: &'static str,
    claim_gate_status: &'static str,
    claim_boundary: &'static str,
    native_io_certificate_refs: &'static str,
    materialization_decode_refs: &'static str,
    dependency_boundary_refs: &'static str,
}

struct IcebergMetadataRootParts<'a> {
    format_version: u64,
    table_uuid: String,
    table_location: String,
    current_schema_id: String,
    current_snapshot_id: String,
    schemas: &'a [serde_json::Value],
    snapshots: &'a [serde_json::Value],
    current_schema: &'a serde_json::Map<String, serde_json::Value>,
    selected_snapshot: IcebergMetadataSnapshotSummary,
}

struct IcebergMetadataReportBuildContext<'a> {
    root_parts: IcebergMetadataRootParts<'a>,
    manifest_list_reader_feature_enabled: bool,
    manifest_list_summary: Option<IcebergManifestListSummary>,
    manifest_file_reader_feature_enabled: bool,
    manifest_file_summary: Option<IcebergManifestFileSummary>,
    schema_evolution_summary: IcebergSchemaEvolutionSummary,
    partition_evolution_summary: IcebergPartitionEvolutionSummary,
    delete_admission_summary: IcebergDeleteAdmissionSummary,
    unsupported_feature_order: Vec<&'static str>,
    metadata_summary: String,
    scope: IcebergReportScope,
    blocked_paths: Vec<IcebergMetadataBlockedPath>,
    diagnostics: Vec<Diagnostic>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
struct IcebergMetadataReadSmokeReport {
    schema_version: &'static str,
    report_id: &'static str,
    phase_id: &'static str,
    support_status: &'static str,
    claim_gate_status: &'static str,
    claim_boundary: &'static str,
    source_protocol: &'static str,
    source_review_ref: &'static str,
    metadata_path: String,
    metadata_bytes_read: usize,
    format_version: u64,
    table_uuid: String,
    table_location: String,
    current_schema_id: String,
    schema_count: usize,
    current_schema_field_count: usize,
    schema_field_ids_present: bool,
    complex_or_nested_schema_field_count: usize,
    schema_evolution_summary: IcebergSchemaEvolutionSummary,
    partition_spec_count: usize,
    default_partition_spec_id: String,
    partition_evolution_summary: IcebergPartitionEvolutionSummary,
    sort_order_count: usize,
    default_sort_order_id: String,
    snapshot_count: usize,
    current_snapshot_id: String,
    selected_snapshot: IcebergMetadataSnapshotSummary,
    snapshot_selector_kind: &'static str,
    manifest_list_ref_count: usize,
    manifest_list_path_requested: Option<String>,
    manifest_list_reader_feature_enabled: bool,
    manifest_list_summary: Option<IcebergManifestListSummary>,
    manifest_file_path_requested: Option<String>,
    manifest_file_reader_feature_enabled: bool,
    manifest_file_summary: Option<IcebergManifestFileSummary>,
    delete_admission_summary: IcebergDeleteAdmissionSummary,
    branch_or_tag_ref_count: usize,
    last_sequence_number: String,
    metadata_summary: String,
    metadata_summary_digest: String,
    correctness_refs: &'static str,
    execution_certificate_refs: &'static str,
    native_io_certificate_refs: &'static str,
    materialization_decode_refs: &'static str,
    dependency_boundary_refs: &'static str,
    local_metadata_json_read_performed: bool,
    table_metadata_read_performed: bool,
    snapshot_selection_performed: bool,
    time_travel_selection_performed: bool,
    catalog_io_performed: bool,
    object_store_io_performed: bool,
    manifest_list_read_performed: bool,
    manifest_file_read_performed: bool,
    data_file_read_performed: bool,
    write_io_performed: bool,
    credential_resolution_performed: bool,
    external_table_format_dependency_invoked: bool,
    fallback_attempted: bool,
    fallback_execution_allowed: bool,
    external_engine_invoked: bool,
    performance_claim_allowed: bool,
    production_table_catalog_claim_allowed: bool,
    lakehouse_claim_allowed: bool,
    unsupported_feature_order: Vec<&'static str>,
    blocked_paths: Vec<IcebergMetadataBlockedPath>,
    diagnostics: Vec<Diagnostic>,
}

impl IcebergMetadataReadSmokeReport {
    fn runtime_supported(&self) -> bool {
        self.support_status == "runtime_supported"
            && self.local_metadata_json_read_performed
            && self.table_metadata_read_performed
            && self.snapshot_selection_performed
            && self.unsupported_feature_order.is_empty()
    }

    fn claim_scoped(&self) -> bool {
        matches!(
            self.claim_gate_status,
            "scoped_iceberg_metadata_json_smoke_only"
                | "scoped_iceberg_metadata_manifest_list_summary_smoke"
                | "scoped_iceberg_manifest_file_split_plan_smoke"
        ) && !self.performance_claim_allowed
            && !self.production_table_catalog_claim_allowed
            && !self.lakehouse_claim_allowed
    }

    fn manifest_list_requested(&self) -> bool {
        self.manifest_list_path_requested.is_some()
    }

    fn manifest_file_requested(&self) -> bool {
        self.manifest_file_path_requested.is_some()
    }

    fn side_effect_free_except_local_metadata_read(&self) -> bool {
        self.side_effect_free_except_declared_local_table_reads()
            && !self.manifest_list_read_performed
            && self.manifest_list_summary.is_none()
            && !self.manifest_file_read_performed
            && self.manifest_file_summary.is_none()
    }

    fn side_effect_free_except_declared_local_table_reads(&self) -> bool {
        self.local_metadata_json_read_performed
            && self.table_metadata_read_performed
            && !self.catalog_io_performed
            && !self.object_store_io_performed
            && self.manifest_list_read_performed == self.manifest_list_summary.is_some()
            && self.manifest_file_read_performed == self.manifest_file_summary.is_some()
            && !self.data_file_read_performed
            && !self.write_io_performed
            && !self.credential_resolution_performed
            && !self.external_table_format_dependency_invoked
            && !self.fallback_attempted
            && !self.fallback_execution_allowed
            && !self.external_engine_invoked
    }

    fn unsupported_diagnostic_count(&self) -> usize {
        self.blocked_paths
            .iter()
            .filter(|path| {
                self.diagnostics.iter().any(|diagnostic| {
                    diagnostic.code == path.diagnostic_code()
                        && diagnostic.category == path.diagnostic_category()
                        && diagnostic.severity == DiagnosticSeverity::Info
                        && diagnostic.feature.as_deref() == Some(path.as_str())
                        && !diagnostic.fallback.attempted
                        && !diagnostic.fallback.allowed
                })
            })
            .count()
    }

    fn deterministic_unsupported_diagnostics_ready(&self) -> bool {
        !self.blocked_paths.is_empty()
            && self.unsupported_diagnostic_count() == self.blocked_paths.len()
    }

    fn blocked_path_order(&self) -> Vec<&'static str> {
        self.blocked_paths
            .iter()
            .map(|path| path.as_str())
            .collect()
    }

    fn unsupported_feature_order_text(&self) -> String {
        if self.unsupported_feature_order.is_empty() {
            "none".to_string()
        } else {
            self.unsupported_feature_order.join(",")
        }
    }

    fn has_errors(&self) -> bool {
        !self.runtime_supported()
            || !self.claim_scoped()
            || !self.side_effect_free_except_declared_local_table_reads()
            || !self.deterministic_unsupported_diagnostics_ready()
            || self.diagnostics.iter().any(|diagnostic| {
                matches!(
                    diagnostic.severity,
                    DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
                )
            })
    }

    fn to_human_text(&self) -> String {
        format!(
            "schema_version: {}\nreport_id: {}\nphase_id: {}\nsupport_status: {}\nclaim_gate_status: {}\nsource_protocol: {}\nmetadata_path: {}\nformat_version: {}\ntable_uuid: {}\ncurrent_snapshot_id: {}\nselected_snapshot_id: {}\nsnapshot_selector_kind: {}\nmanifest_list_refs: {}\nmanifest_list_requested: {}\nmanifest_list_read_performed: {}\nmanifest_file_requested: {}\nmanifest_file_read_performed: {}\nunsupported_features: {}\nfallback_attempted: {}\nexternal_engine_invoked: {}\n",
            self.schema_version,
            self.report_id,
            self.phase_id,
            self.support_status,
            self.claim_gate_status,
            self.source_protocol,
            self.metadata_path,
            self.format_version,
            self.table_uuid,
            self.current_snapshot_id,
            self.selected_snapshot.snapshot_id,
            self.snapshot_selector_kind,
            self.manifest_list_ref_count,
            self.manifest_list_requested(),
            self.manifest_list_read_performed,
            self.manifest_file_requested(),
            self.manifest_file_read_performed,
            self.unsupported_feature_order_text(),
            self.fallback_attempted,
            self.external_engine_invoked
        )
    }
}

fn iceberg_metadata_blocked_paths(
    manifest_list_read_performed: bool,
    manifest_file_read_performed: bool,
) -> Vec<IcebergMetadataBlockedPath> {
    let mut paths = vec![
        IcebergMetadataBlockedPath::ExternalCatalogResolution,
        IcebergMetadataBlockedPath::RemoteObjectStoreMetadataRead,
        IcebergMetadataBlockedPath::DataFileScan,
        IcebergMetadataBlockedPath::DeleteFileSemantics,
        IcebergMetadataBlockedPath::TableWriteCommit,
        IcebergMetadataBlockedPath::BroadIcebergRuntime,
        IcebergMetadataBlockedPath::DeltaHudiRuntime,
        IcebergMetadataBlockedPath::LakehouseProductionClaim,
    ];
    if !manifest_list_read_performed {
        paths.insert(2, IcebergMetadataBlockedPath::ManifestListRead);
    }
    if !manifest_file_read_performed {
        let insert_at = if manifest_list_read_performed { 2 } else { 3 };
        paths.insert(insert_at, IcebergMetadataBlockedPath::ManifestFileRead);
    }
    paths
}

fn iceberg_report_scope(
    manifest_list_read_performed: bool,
    manifest_file_read_performed: bool,
) -> IcebergReportScope {
    if manifest_file_read_performed {
        return IcebergReportScope {
            report_id: "prod-ready-1c.iceberg_manifest_file_split_plan_smoke",
            claim_gate_status: "scoped_iceberg_manifest_file_split_plan_smoke",
            claim_boundary: "one local Iceberg table metadata JSON read plus one explicit local Avro manifest-file read; data-file split planning only; no catalog service, object-store read, data-file scan, delete execution, write/commit, production lakehouse, or performance claim",
            native_io_certificate_refs: "local_metadata_json_and_local_manifest_file_avro_split_plan_read_no_object_store_native_io_certificate",
            materialization_decode_refs: "metadata_json_plus_manifest_file_metadata_decode_no_data_file_decode_no_row_materialization",
            dependency_boundary_refs: "serde_json_plus_arrow_avro_manifest_file_compat_adapter_no_iceberg_runtime_dependency_no_external_engine",
        };
    }
    if manifest_list_read_performed {
        return IcebergReportScope {
            report_id: "prod-ready-1c.iceberg_manifest_list_summary_smoke",
            claim_gate_status: "scoped_iceberg_metadata_manifest_list_summary_smoke",
            claim_boundary: "one local Iceberg table metadata JSON read plus one explicit local Avro manifest-list summary read; manifest-level summary pruning and split counting only; no catalog service, object-store read, manifest-file/data-file scan, delete execution, write/commit, production lakehouse, or performance claim",
            native_io_certificate_refs: "local_metadata_json_and_local_manifest_list_avro_summary_read_no_object_store_native_io_certificate",
            materialization_decode_refs: "metadata_json_plus_manifest_list_summary_decode_no_manifest_file_decode_no_data_file_decode_no_row_materialization",
            dependency_boundary_refs: "serde_json_plus_arrow_avro_manifest_list_compat_adapter_no_iceberg_runtime_dependency_no_external_engine",
        };
    }
    IcebergReportScope {
        report_id: "prod-ready-1c.iceberg_metadata_json_read_smoke",
        claim_gate_status: "scoped_iceberg_metadata_json_smoke_only",
        claim_boundary: "one local Iceberg table metadata JSON read and snapshot selection only; no catalog service, object-store read, manifest-list read, manifest/data-file scan, delete execution, write/commit, production lakehouse, or performance claim",
        native_io_certificate_refs: "local_metadata_json_read_only_no_object_store_native_io_certificate",
        materialization_decode_refs: "metadata_json_only_no_data_file_decode_no_row_materialization",
        dependency_boundary_refs: "serde_json_only_no_iceberg_runtime_dependency_no_external_engine",
    }
}

fn parse_iceberg_metadata_read_smoke_args(
    args: impl Iterator<Item = String>,
) -> Result<IcebergMetadataReadSmokeRequest, ShardLoomError> {
    let mut metadata_path = None;
    let mut snapshot_id = None;
    let mut as_of_timestamp_ms = None;
    let mut manifest_list_path = None;
    let mut manifest_file_path = None;
    let mut iter = args.peekable();
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--snapshot-id" => {
                let value = iter.next().ok_or_else(|| {
                    ShardLoomError::InvalidOperation(
                        "missing value after --snapshot-id".to_string(),
                    )
                })?;
                snapshot_id = Some(value);
            }
            "--as-of-timestamp-ms" => {
                let value = iter.next().ok_or_else(|| {
                    ShardLoomError::InvalidOperation(
                        "missing value after --as-of-timestamp-ms".to_string(),
                    )
                })?;
                let parsed = value.parse::<i64>().map_err(|_| {
                    ShardLoomError::InvalidOperation(
                        "--as-of-timestamp-ms must be an integer".to_string(),
                    )
                })?;
                as_of_timestamp_ms = Some(parsed);
            }
            "--manifest-list" => {
                let value = iter.next().ok_or_else(|| {
                    ShardLoomError::InvalidOperation(
                        "missing value after --manifest-list".to_string(),
                    )
                })?;
                manifest_list_path = Some(value);
            }
            "--manifest" => {
                let value = iter.next().ok_or_else(|| {
                    ShardLoomError::InvalidOperation("missing value after --manifest".to_string())
                })?;
                manifest_file_path = Some(value);
            }
            other if other.starts_with("--") => {
                return Err(cli_unknown_arg_error("iceberg-metadata-read-smoke", other));
            }
            _ if metadata_path.is_none() => metadata_path = Some(arg),
            _ => {
                return Err(ShardLoomError::InvalidOperation(
                    "usage: shardloom iceberg-metadata-read-smoke <metadata-json-path> [--snapshot-id id|--as-of-timestamp-ms ms] [--manifest-list local.avro] [--manifest local.avro]"
                        .to_string(),
                ));
            }
        }
    }
    if snapshot_id.is_some() && as_of_timestamp_ms.is_some() {
        return Err(ShardLoomError::InvalidOperation(
            "--snapshot-id and --as-of-timestamp-ms are mutually exclusive".to_string(),
        ));
    }
    let metadata_path = metadata_path.ok_or_else(|| {
        ShardLoomError::InvalidOperation(
            "usage: shardloom iceberg-metadata-read-smoke <metadata-json-path> [--snapshot-id id|--as-of-timestamp-ms ms] [--manifest-list local.avro] [--manifest local.avro]"
                .to_string(),
        )
    })?;
    let selection = match (snapshot_id, as_of_timestamp_ms) {
        (Some(id), None) => IcebergSnapshotSelectionRequest::SnapshotId(id),
        (None, Some(timestamp)) => IcebergSnapshotSelectionRequest::AsOfTimestampMillis(timestamp),
        (None, None) => IcebergSnapshotSelectionRequest::Current,
        (Some(_), Some(_)) => unreachable!("mutual exclusion checked above"),
    };
    Ok(IcebergMetadataReadSmokeRequest {
        metadata_path,
        selection,
        manifest_list_path,
        manifest_file_path,
    })
}

fn run_iceberg_metadata_read_smoke(
    request: &IcebergMetadataReadSmokeRequest,
) -> Result<IcebergMetadataReadSmokeReport, ShardLoomError> {
    reject_non_local_metadata_path(&request.metadata_path)?;
    let metadata = fs::read_to_string(Path::new(&request.metadata_path)).map_err(|error| {
        ShardLoomError::InvalidOperation(format!(
            "failed to read Iceberg metadata JSON {}: {error}",
            request.metadata_path
        ))
    })?;
    let root = parse_iceberg_metadata_json(&metadata)?;
    build_iceberg_metadata_report(request, &metadata, &root)
}

fn reject_non_local_metadata_path(path: &str) -> Result<(), ShardLoomError> {
    if path.contains("://") && !path.starts_with("file://") {
        return Err(ShardLoomError::InvalidOperation(
            "iceberg-metadata-read-smoke only accepts local metadata JSON paths".to_string(),
        ));
    }
    Ok(())
}

fn parse_iceberg_metadata_json(
    metadata: &str,
) -> Result<serde_json::Map<String, serde_json::Value>, ShardLoomError> {
    let value: serde_json::Value = serde_json::from_str(metadata).map_err(|error| {
        ShardLoomError::InvalidOperation(format!("invalid Iceberg metadata JSON: {error}"))
    })?;
    value.as_object().cloned().ok_or_else(|| {
        ShardLoomError::InvalidOperation("Iceberg metadata JSON must be an object".to_string())
    })
}

fn build_iceberg_metadata_report(
    request: &IcebergMetadataReadSmokeRequest,
    metadata: &str,
    root: &serde_json::Map<String, serde_json::Value>,
) -> Result<IcebergMetadataReadSmokeReport, ShardLoomError> {
    let context = prepare_iceberg_metadata_report_context(request, root)?;
    Ok(assemble_iceberg_metadata_report(
        request,
        metadata.len(),
        root,
        context,
    ))
}

fn prepare_iceberg_metadata_report_context<'a>(
    request: &IcebergMetadataReadSmokeRequest,
    root: &'a serde_json::Map<String, serde_json::Value>,
) -> Result<IcebergMetadataReportBuildContext<'a>, ShardLoomError> {
    let root_parts = parse_iceberg_metadata_root_parts(root, &request.selection)?;
    let manifest_list_reader_feature_enabled = iceberg_manifest_list_reader_feature_enabled();
    let manifest_list_summary =
        maybe_read_iceberg_manifest_list_summary(request.manifest_list_path.as_deref())?;
    let manifest_file_reader_feature_enabled = iceberg_manifest_file_reader_feature_enabled();
    let manifest_file_summary =
        maybe_read_iceberg_manifest_file_summary(request.manifest_file_path.as_deref())?;
    let schema_evolution_summary = iceberg_schema_evolution_summary(root_parts.schemas);
    let partition_evolution_summary =
        iceberg_partition_evolution_summary(root, manifest_list_summary.as_ref());
    let delete_admission_summary = iceberg_delete_admission_summary(
        &root_parts.selected_snapshot,
        manifest_list_summary.as_ref(),
        manifest_file_summary.as_ref(),
    );
    let unsupported_feature_order =
        iceberg_metadata_unsupported_feature_order(&IcebergMetadataUnsupportedFeatureContext {
            format_version: root_parts.format_version,
            selected_snapshot: &root_parts.selected_snapshot,
            schema_evolution_summary: &schema_evolution_summary,
            partition_evolution_summary: &partition_evolution_summary,
            delete_admission_summary: &delete_admission_summary,
            manifest_list_reader_feature_disabled: request.manifest_list_path.is_some()
                && !manifest_list_reader_feature_enabled,
            manifest_file_reader_feature_disabled: request.manifest_file_path.is_some()
                && !manifest_file_reader_feature_enabled,
            manifest_list_summary: manifest_list_summary.as_ref(),
            manifest_file_summary: manifest_file_summary.as_ref(),
        });
    let metadata_summary = iceberg_metadata_summary(&IcebergMetadataSummaryContext {
        table_uuid: &root_parts.table_uuid,
        current_snapshot_id: &root_parts.current_snapshot_id,
        selected_snapshot: &root_parts.selected_snapshot,
        manifest_list_summary: manifest_list_summary.as_ref(),
        manifest_file_summary: manifest_file_summary.as_ref(),
        schema_evolution_summary: &schema_evolution_summary,
        partition_evolution_summary: &partition_evolution_summary,
        delete_admission_summary: &delete_admission_summary,
        root,
        schemas: root_parts.schemas,
        snapshots: root_parts.snapshots,
    });
    let manifest_list_read_performed = manifest_list_summary.is_some();
    let manifest_file_read_performed = manifest_file_summary.is_some();
    let scope = iceberg_report_scope(manifest_list_read_performed, manifest_file_read_performed);
    let blocked_paths =
        iceberg_metadata_blocked_paths(manifest_list_read_performed, manifest_file_read_performed);
    let diagnostics = iceberg_metadata_diagnostics(&blocked_paths, &unsupported_feature_order);
    Ok(IcebergMetadataReportBuildContext {
        root_parts,
        manifest_list_reader_feature_enabled,
        manifest_list_summary,
        manifest_file_reader_feature_enabled,
        manifest_file_summary,
        schema_evolution_summary,
        partition_evolution_summary,
        delete_admission_summary,
        unsupported_feature_order,
        metadata_summary,
        scope,
        blocked_paths,
        diagnostics,
    })
}

fn assemble_iceberg_metadata_report(
    request: &IcebergMetadataReadSmokeRequest,
    metadata_bytes_read: usize,
    root: &serde_json::Map<String, serde_json::Value>,
    context: IcebergMetadataReportBuildContext<'_>,
) -> IcebergMetadataReadSmokeReport {
    let IcebergMetadataReportBuildContext {
        root_parts,
        manifest_list_reader_feature_enabled,
        manifest_list_summary,
        manifest_file_reader_feature_enabled,
        manifest_file_summary,
        schema_evolution_summary,
        partition_evolution_summary,
        delete_admission_summary,
        unsupported_feature_order,
        metadata_summary,
        scope,
        blocked_paths,
        diagnostics,
    } = context;
    let manifest_list_read_performed = manifest_list_summary.is_some();
    let manifest_file_read_performed = manifest_file_summary.is_some();
    IcebergMetadataReadSmokeReport {
        schema_version: "shardloom.iceberg_metadata_read_smoke.v1",
        report_id: scope.report_id,
        phase_id: "PROD-READY-1C",
        support_status: iceberg_report_support_status(&unsupported_feature_order),
        claim_gate_status: scope.claim_gate_status,
        claim_boundary: scope.claim_boundary,
        source_protocol: "apache_iceberg_table_metadata",
        source_review_ref: "docs/architecture/table-protocol-source-review.md",
        metadata_path: request.metadata_path.clone(),
        metadata_bytes_read,
        format_version: root_parts.format_version,
        table_uuid: root_parts.table_uuid,
        table_location: root_parts.table_location,
        current_schema_id: root_parts.current_schema_id,
        schema_count: root_parts.schemas.len(),
        current_schema_field_count: schema_field_count(root_parts.current_schema),
        schema_field_ids_present: schema_field_ids_present(root_parts.current_schema),
        complex_or_nested_schema_field_count: complex_or_nested_schema_field_count(
            root_parts.current_schema,
        ),
        schema_evolution_summary,
        partition_spec_count: optional_array_len(root, "partition-specs"),
        default_partition_spec_id: optional_i64_string(root, "default-spec-id"),
        partition_evolution_summary,
        sort_order_count: optional_array_len(root, "sort-orders"),
        default_sort_order_id: optional_i64_string(root, "default-sort-order-id"),
        snapshot_count: root_parts.snapshots.len(),
        current_snapshot_id: root_parts.current_snapshot_id,
        selected_snapshot: root_parts.selected_snapshot,
        snapshot_selector_kind: request.selection.selector_kind(),
        manifest_list_ref_count: manifest_list_ref_count(root_parts.snapshots),
        manifest_list_path_requested: request.manifest_list_path.clone(),
        manifest_list_reader_feature_enabled,
        manifest_list_summary,
        manifest_file_path_requested: request.manifest_file_path.clone(),
        manifest_file_reader_feature_enabled,
        manifest_file_summary,
        delete_admission_summary,
        branch_or_tag_ref_count: optional_object_len(root, "refs"),
        last_sequence_number: optional_i64_string(root, "last-sequence-number"),
        metadata_summary_digest: iceberg_metadata_digest(&metadata_summary),
        metadata_summary,
        correctness_refs: "shardloom-cli::workflow_planning::iceberg_metadata_read_smoke",
        execution_certificate_refs: "shardloom-cli/tests/iceberg_metadata_read_smoke.rs",
        native_io_certificate_refs: scope.native_io_certificate_refs,
        materialization_decode_refs: scope.materialization_decode_refs,
        dependency_boundary_refs: scope.dependency_boundary_refs,
        local_metadata_json_read_performed: true,
        table_metadata_read_performed: true,
        snapshot_selection_performed: true,
        time_travel_selection_performed: iceberg_time_travel_selection_performed(
            &request.selection,
        ),
        catalog_io_performed: false,
        object_store_io_performed: false,
        manifest_list_read_performed,
        manifest_file_read_performed,
        data_file_read_performed: false,
        write_io_performed: false,
        credential_resolution_performed: false,
        external_table_format_dependency_invoked: false,
        fallback_attempted: false,
        fallback_execution_allowed: false,
        external_engine_invoked: false,
        performance_claim_allowed: false,
        production_table_catalog_claim_allowed: false,
        lakehouse_claim_allowed: false,
        unsupported_feature_order,
        blocked_paths,
        diagnostics,
    }
}

fn iceberg_metadata_diagnostics(
    blocked_paths: &[IcebergMetadataBlockedPath],
    unsupported_feature_order: &[&'static str],
) -> Vec<Diagnostic> {
    let mut diagnostics: Vec<Diagnostic> = blocked_paths
        .iter()
        .map(|path| path.to_diagnostic())
        .collect();
    diagnostics.extend(iceberg_unsupported_feature_diagnostics(
        unsupported_feature_order,
    ));
    diagnostics
}

fn iceberg_report_support_status(unsupported_feature_order: &[&'static str]) -> &'static str {
    if unsupported_feature_order.is_empty() {
        "runtime_supported"
    } else {
        "unsupported_metadata_features"
    }
}

fn iceberg_time_travel_selection_performed(selection: &IcebergSnapshotSelectionRequest) -> bool {
    matches!(
        selection,
        IcebergSnapshotSelectionRequest::AsOfTimestampMillis(_)
    )
}

fn parse_iceberg_metadata_root_parts<'a>(
    root: &'a serde_json::Map<String, serde_json::Value>,
    selection: &IcebergSnapshotSelectionRequest,
) -> Result<IcebergMetadataRootParts<'a>, ShardLoomError> {
    let format_version = required_u64(root, "format-version")?;
    let table_uuid = required_string(root, "table-uuid")?;
    let table_location = required_string(root, "location")?;
    let current_schema_id = required_i64_string(root, "current-schema-id")?;
    let current_snapshot_id = required_i64_string(root, "current-snapshot-id")?;
    let schemas = required_array(root, "schemas")?;
    let snapshots = required_array(root, "snapshots")?;
    let current_schema = current_iceberg_schema(schemas, &current_schema_id)?;
    let selected_snapshot = select_iceberg_snapshot(snapshots, &current_snapshot_id, selection)?;
    Ok(IcebergMetadataRootParts {
        format_version,
        table_uuid,
        table_location,
        current_schema_id,
        current_snapshot_id,
        schemas,
        snapshots,
        current_schema,
        selected_snapshot,
    })
}

#[cfg(feature = "universal-format-io")]
const ICEBERG_MANIFEST_LIST_MAX_ROWS: usize = 16_384;
#[cfg(feature = "universal-format-io")]
const ICEBERG_MANIFEST_LIST_PROJECTION_COLUMNS: &[&str] = &[
    "manifest_path",
    "manifest_length",
    "partition_spec_id",
    "content",
    "sequence_number",
    "min_sequence_number",
    "added_snapshot_id",
    "added_data_files_count",
    "existing_data_files_count",
    "deleted_data_files_count",
    "added_delete_files_count",
    "existing_delete_files_count",
    "deleted_delete_files_count",
];

fn iceberg_manifest_list_reader_feature_enabled() -> bool {
    cfg!(feature = "universal-format-io")
}

fn maybe_read_iceberg_manifest_list_summary(
    manifest_list_path: Option<&str>,
) -> Result<Option<IcebergManifestListSummary>, ShardLoomError> {
    let Some(path) = manifest_list_path else {
        return Ok(None);
    };
    reject_non_local_metadata_path(path)?;
    if !iceberg_manifest_list_reader_feature_enabled() {
        return Ok(None);
    }
    read_iceberg_manifest_list_summary(path).map(Some)
}

#[cfg(feature = "universal-format-io")]
fn read_iceberg_manifest_list_summary(
    path: &str,
) -> Result<IcebergManifestListSummary, ShardLoomError> {
    let projection_columns: Vec<String> = ICEBERG_MANIFEST_LIST_PROJECTION_COLUMNS
        .iter()
        .map(|column| (*column).to_string())
        .collect();
    let manifest_list_path = Path::new(path);
    let bytes_read = usize::try_from(
        fs::metadata(manifest_list_path)
            .map_err(|error| {
                ShardLoomError::InvalidOperation(format!(
                    "failed to stat Iceberg manifest-list Avro '{}': {error}",
                    manifest_list_path.display()
                ))
            })?
            .len(),
    )
    .map_err(|_| {
        ShardLoomError::InvalidOperation(format!(
            "Iceberg manifest-list Avro '{}' is too large for this platform",
            manifest_list_path.display()
        ))
    })?;
    let table = shardloom_vortex::read_flat_avro_source_with_projection(
        manifest_list_path,
        ICEBERG_MANIFEST_LIST_MAX_ROWS,
        &projection_columns,
    )?;
    let mut summary = IcebergManifestListSummary {
        path: path.to_string(),
        bytes_read,
        schema_column_count: table.header.len(),
        projected_column_count: table.reader_projection_columns.len(),
        entry_count: table.rows.len(),
        partition_spec_id_order: Vec::new(),
        data_manifest_count: 0,
        delete_manifest_count: 0,
        unknown_content_manifest_count: 0,
        total_manifest_bytes: 0,
        added_data_file_count: 0,
        existing_data_file_count: 0,
        deleted_data_file_count: 0,
        added_delete_file_count: 0,
        existing_delete_file_count: 0,
        deleted_delete_file_count: 0,
        planned_manifest_split_count: 0,
        planned_data_file_count: 0,
        manifest_summary_pruning_rule: "data_manifests_only_delete_and_unknown_content_blocked",
    };
    for row in &table.rows {
        observe_iceberg_manifest_list_row(&mut summary, row, manifest_list_path)?;
    }
    summary.planned_manifest_split_count = summary.data_manifest_count;
    summary.planned_data_file_count = summary
        .added_data_file_count
        .saturating_add(summary.existing_data_file_count);
    Ok(summary)
}

#[cfg(not(feature = "universal-format-io"))]
fn read_iceberg_manifest_list_summary(
    _path: &str,
) -> Result<IcebergManifestListSummary, ShardLoomError> {
    Err(ShardLoomError::NotImplemented(
        "Iceberg manifest-list Avro summary reads require building shardloom-cli with --features universal-format-io"
            .to_string(),
    ))
}

#[cfg(feature = "universal-format-io")]
fn observe_iceberg_manifest_list_row(
    summary: &mut IcebergManifestListSummary,
    row: &BTreeMap<String, ScalarValue>,
    manifest_list_path: &Path,
) -> Result<(), ShardLoomError> {
    let manifest_path = iceberg_manifest_row_string(row, "manifest_path").ok_or_else(|| {
        ShardLoomError::InvalidOperation(format!(
            "Iceberg manifest-list Avro '{}' row is missing manifest_path",
            manifest_list_path.display()
        ))
    })?;
    if manifest_path.trim().is_empty() {
        return Err(ShardLoomError::InvalidOperation(format!(
            "Iceberg manifest-list Avro '{}' contains an empty manifest_path",
            manifest_list_path.display()
        )));
    }

    summary.total_manifest_bytes = summary
        .total_manifest_bytes
        .saturating_add(iceberg_manifest_row_u64(row, "manifest_length").unwrap_or(0));
    if let Some(partition_spec_id) = iceberg_manifest_row_i64(row, "partition_spec_id") {
        push_unique_string(
            &mut summary.partition_spec_id_order,
            &partition_spec_id.to_string(),
        );
    }
    summary.added_data_file_count = summary
        .added_data_file_count
        .saturating_add(iceberg_manifest_row_u64(row, "added_data_files_count").unwrap_or(0));
    summary.existing_data_file_count = summary
        .existing_data_file_count
        .saturating_add(iceberg_manifest_row_u64(row, "existing_data_files_count").unwrap_or(0));
    summary.deleted_data_file_count = summary
        .deleted_data_file_count
        .saturating_add(iceberg_manifest_row_u64(row, "deleted_data_files_count").unwrap_or(0));
    summary.added_delete_file_count = summary
        .added_delete_file_count
        .saturating_add(iceberg_manifest_row_u64(row, "added_delete_files_count").unwrap_or(0));
    summary.existing_delete_file_count = summary
        .existing_delete_file_count
        .saturating_add(iceberg_manifest_row_u64(row, "existing_delete_files_count").unwrap_or(0));
    summary.deleted_delete_file_count = summary
        .deleted_delete_file_count
        .saturating_add(iceberg_manifest_row_u64(row, "deleted_delete_files_count").unwrap_or(0));

    match iceberg_manifest_row_i64(row, "content") {
        Some(0) => summary.data_manifest_count += 1,
        Some(1) => summary.delete_manifest_count += 1,
        _ => summary.unknown_content_manifest_count += 1,
    }
    Ok(())
}

#[cfg(feature = "universal-format-io")]
fn iceberg_manifest_row_string<'a>(
    row: &'a BTreeMap<String, ScalarValue>,
    key: &str,
) -> Option<&'a str> {
    match row.get(key)? {
        ScalarValue::Utf8(value) => Some(value.as_str()),
        _ => None,
    }
}

#[cfg(feature = "universal-format-io")]
fn iceberg_manifest_row_u64(row: &BTreeMap<String, ScalarValue>, key: &str) -> Option<u64> {
    match row.get(key)? {
        ScalarValue::UInt64(value) => Some(*value),
        ScalarValue::Int64(value) => u64::try_from(*value).ok(),
        ScalarValue::Utf8(value) => value.parse::<u64>().ok(),
        _ => None,
    }
}

#[cfg(feature = "universal-format-io")]
fn iceberg_manifest_row_i64(row: &BTreeMap<String, ScalarValue>, key: &str) -> Option<i64> {
    match row.get(key)? {
        ScalarValue::Int64(value) => Some(*value),
        ScalarValue::UInt64(value) => i64::try_from(*value).ok(),
        ScalarValue::Utf8(value) => value.parse::<i64>().ok(),
        _ => None,
    }
}

#[cfg(feature = "universal-format-io")]
const ICEBERG_MANIFEST_FILE_MAX_ROWS: usize = 65_536;
#[cfg(feature = "universal-format-io")]
const ICEBERG_MANIFEST_FILE_PROJECTION_COLUMNS: &[&str] = &["status", "snapshot_id", "data_file"];

fn iceberg_manifest_file_reader_feature_enabled() -> bool {
    cfg!(feature = "universal-format-io")
}

fn maybe_read_iceberg_manifest_file_summary(
    manifest_file_path: Option<&str>,
) -> Result<Option<IcebergManifestFileSummary>, ShardLoomError> {
    let Some(path) = manifest_file_path else {
        return Ok(None);
    };
    reject_non_local_metadata_path(path)?;
    if !iceberg_manifest_file_reader_feature_enabled() {
        return Ok(None);
    }
    read_iceberg_manifest_file_summary(path).map(Some)
}

#[cfg(feature = "universal-format-io")]
fn read_iceberg_manifest_file_summary(
    path: &str,
) -> Result<IcebergManifestFileSummary, ShardLoomError> {
    let projection_columns: Vec<String> = ICEBERG_MANIFEST_FILE_PROJECTION_COLUMNS
        .iter()
        .map(|column| (*column).to_string())
        .collect();
    let manifest_file_path = Path::new(path);
    let bytes_read = usize::try_from(
        fs::metadata(manifest_file_path)
            .map_err(|error| {
                ShardLoomError::InvalidOperation(format!(
                    "failed to stat Iceberg manifest Avro '{}': {error}",
                    manifest_file_path.display()
                ))
            })?
            .len(),
    )
    .map_err(|_| {
        ShardLoomError::InvalidOperation(format!(
            "Iceberg manifest Avro '{}' is too large for this platform",
            manifest_file_path.display()
        ))
    })?;
    let table = shardloom_vortex::read_flat_avro_source_with_projection(
        manifest_file_path,
        ICEBERG_MANIFEST_FILE_MAX_ROWS,
        &projection_columns,
    )?;
    let mut summary = IcebergManifestFileSummary {
        path: path.to_string(),
        bytes_read,
        schema_column_count: table.header.len(),
        projected_column_count: table.reader_projection_columns.len(),
        entry_count: table.rows.len(),
        added_data_file_count: 0,
        existing_data_file_count: 0,
        deleted_data_file_count: 0,
        delete_file_entry_count: 0,
        position_delete_file_entry_count: 0,
        equality_delete_file_entry_count: 0,
        deletion_vector_entry_count: 0,
        unknown_content_file_count: 0,
        unknown_status_entry_count: 0,
        total_record_count: 0,
        total_file_size_bytes: 0,
        planned_data_file_count: 0,
        planned_data_file_bytes: 0,
        split_planning_rule: "added_and_existing_data_files_only_deleted_delete_and_unknown_entries_blocked",
    };
    for row in &table.rows {
        observe_iceberg_manifest_file_row(&mut summary, row, manifest_file_path)?;
    }
    Ok(summary)
}

#[cfg(not(feature = "universal-format-io"))]
fn read_iceberg_manifest_file_summary(
    _path: &str,
) -> Result<IcebergManifestFileSummary, ShardLoomError> {
    Err(ShardLoomError::NotImplemented(
        "Iceberg manifest Avro split planning requires building shardloom-cli with --features universal-format-io"
            .to_string(),
    ))
}

#[cfg(feature = "universal-format-io")]
fn observe_iceberg_manifest_file_row(
    summary: &mut IcebergManifestFileSummary,
    row: &BTreeMap<String, ScalarValue>,
    manifest_file_path: &Path,
) -> Result<(), ShardLoomError> {
    let status = iceberg_manifest_row_i64(row, "status");
    let data_file = iceberg_manifest_row_struct(row, "data_file").ok_or_else(|| {
        ShardLoomError::InvalidOperation(format!(
            "Iceberg manifest Avro '{}' row is missing data_file struct",
            manifest_file_path.display()
        ))
    })?;
    let content = iceberg_struct_i64(data_file, "content").unwrap_or(0);
    let file_path = iceberg_struct_string(data_file, "file_path").ok_or_else(|| {
        ShardLoomError::InvalidOperation(format!(
            "Iceberg manifest Avro '{}' data_file entry is missing file_path",
            manifest_file_path.display()
        ))
    })?;
    if file_path.trim().is_empty() {
        return Err(ShardLoomError::InvalidOperation(format!(
            "Iceberg manifest Avro '{}' contains an empty data_file.file_path",
            manifest_file_path.display()
        )));
    }

    let record_count = iceberg_struct_u64(data_file, "record_count").unwrap_or(0);
    let file_size = iceberg_struct_u64(data_file, "file_size_in_bytes").unwrap_or(0);
    summary.total_record_count = summary.total_record_count.saturating_add(record_count);
    summary.total_file_size_bytes = summary.total_file_size_bytes.saturating_add(file_size);

    match status {
        Some(0) if content == 0 => {
            summary.existing_data_file_count = summary.existing_data_file_count.saturating_add(1);
            summary.planned_data_file_count = summary.planned_data_file_count.saturating_add(1);
            summary.planned_data_file_bytes =
                summary.planned_data_file_bytes.saturating_add(file_size);
        }
        Some(1) if content == 0 => {
            summary.added_data_file_count = summary.added_data_file_count.saturating_add(1);
            summary.planned_data_file_count = summary.planned_data_file_count.saturating_add(1);
            summary.planned_data_file_bytes =
                summary.planned_data_file_bytes.saturating_add(file_size);
        }
        Some(2) if content == 0 => {
            summary.deleted_data_file_count = summary.deleted_data_file_count.saturating_add(1);
        }
        Some(0 | 1 | 2) => {}
        _ => {
            summary.unknown_status_entry_count =
                summary.unknown_status_entry_count.saturating_add(1);
        }
    }
    observe_iceberg_manifest_file_content(summary, content, data_file);
    Ok(())
}

#[cfg(feature = "universal-format-io")]
fn observe_iceberg_manifest_file_content(
    summary: &mut IcebergManifestFileSummary,
    content: i64,
    data_file: &[(String, ScalarValue)],
) {
    match content {
        0 => {}
        1 => {
            summary.delete_file_entry_count = summary.delete_file_entry_count.saturating_add(1);
            if iceberg_data_file_has_deletion_vector_metadata(data_file) {
                summary.deletion_vector_entry_count =
                    summary.deletion_vector_entry_count.saturating_add(1);
            } else {
                summary.position_delete_file_entry_count =
                    summary.position_delete_file_entry_count.saturating_add(1);
            }
        }
        2 => {
            summary.delete_file_entry_count = summary.delete_file_entry_count.saturating_add(1);
            summary.equality_delete_file_entry_count =
                summary.equality_delete_file_entry_count.saturating_add(1);
        }
        _ => {
            summary.unknown_content_file_count =
                summary.unknown_content_file_count.saturating_add(1);
        }
    }
}

#[cfg(feature = "universal-format-io")]
fn iceberg_data_file_has_deletion_vector_metadata(fields: &[(String, ScalarValue)]) -> bool {
    iceberg_struct_string(fields, "referenced_data_file").is_some_and(|value| !value.is_empty())
        && iceberg_struct_u64(fields, "content_offset").is_some()
        && iceberg_struct_u64(fields, "content_size_in_bytes").is_some()
}

#[cfg(feature = "universal-format-io")]
fn iceberg_manifest_row_struct<'a>(
    row: &'a BTreeMap<String, ScalarValue>,
    key: &str,
) -> Option<&'a [(String, ScalarValue)]> {
    match row.get(key)? {
        ScalarValue::Struct(fields) => Some(fields.as_slice()),
        _ => None,
    }
}

#[cfg(feature = "universal-format-io")]
fn iceberg_struct_string<'a>(fields: &'a [(String, ScalarValue)], key: &str) -> Option<&'a str> {
    fields.iter().find_map(|(field, value)| {
        (field == key).then(|| match value {
            ScalarValue::Utf8(value) => Some(value.as_str()),
            _ => None,
        })?
    })
}

#[cfg(feature = "universal-format-io")]
fn iceberg_struct_u64(fields: &[(String, ScalarValue)], key: &str) -> Option<u64> {
    fields
        .iter()
        .find_map(|(field, value)| (field == key).then_some(value))
        .and_then(|value| match value {
            ScalarValue::UInt64(value) => Some(*value),
            ScalarValue::Int64(value) => u64::try_from(*value).ok(),
            ScalarValue::Utf8(value) => value.parse::<u64>().ok(),
            _ => None,
        })
}

#[cfg(feature = "universal-format-io")]
fn iceberg_struct_i64(fields: &[(String, ScalarValue)], key: &str) -> Option<i64> {
    fields
        .iter()
        .find_map(|(field, value)| (field == key).then_some(value))
        .and_then(|value| match value {
            ScalarValue::Int64(value) => Some(*value),
            ScalarValue::UInt64(value) => i64::try_from(*value).ok(),
            ScalarValue::Utf8(value) => value.parse::<i64>().ok(),
            _ => None,
        })
}

fn required_array<'a>(
    root: &'a serde_json::Map<String, serde_json::Value>,
    key: &str,
) -> Result<&'a Vec<serde_json::Value>, ShardLoomError> {
    root.get(key)
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| {
            ShardLoomError::InvalidOperation(format!(
                "Iceberg metadata JSON missing array field {key}"
            ))
        })
}

fn required_string(
    root: &serde_json::Map<String, serde_json::Value>,
    key: &str,
) -> Result<String, ShardLoomError> {
    root.get(key)
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(ToOwned::to_owned)
        .ok_or_else(|| {
            ShardLoomError::InvalidOperation(format!(
                "Iceberg metadata JSON missing string field {key}"
            ))
        })
}

fn required_u64(
    root: &serde_json::Map<String, serde_json::Value>,
    key: &str,
) -> Result<u64, ShardLoomError> {
    root.get(key)
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| {
            ShardLoomError::InvalidOperation(format!(
                "Iceberg metadata JSON missing unsigned integer field {key}"
            ))
        })
}

fn required_i64_string(
    root: &serde_json::Map<String, serde_json::Value>,
    key: &str,
) -> Result<String, ShardLoomError> {
    root.get(key).and_then(json_i64_string).ok_or_else(|| {
        ShardLoomError::InvalidOperation(format!(
            "Iceberg metadata JSON missing integer field {key}"
        ))
    })
}

fn optional_i64_string(root: &serde_json::Map<String, serde_json::Value>, key: &str) -> String {
    root.get(key)
        .and_then(json_i64_string)
        .unwrap_or_else(|| "none".to_string())
}

fn json_i64_string(value: &serde_json::Value) -> Option<String> {
    value.as_i64().map(|number| number.to_string()).or_else(|| {
        value
            .as_u64()
            .and_then(|number| i64::try_from(number).ok())
            .map(|number| number.to_string())
    })
}

fn optional_array_len(root: &serde_json::Map<String, serde_json::Value>, key: &str) -> usize {
    root.get(key)
        .and_then(serde_json::Value::as_array)
        .map_or(0, Vec::len)
}

fn optional_object_len(root: &serde_json::Map<String, serde_json::Value>, key: &str) -> usize {
    root.get(key)
        .and_then(serde_json::Value::as_object)
        .map_or(0, serde_json::Map::len)
}

fn current_iceberg_schema<'a>(
    schemas: &'a [serde_json::Value],
    current_schema_id: &str,
) -> Result<&'a serde_json::Map<String, serde_json::Value>, ShardLoomError> {
    schemas
        .iter()
        .filter_map(serde_json::Value::as_object)
        .find(|schema| {
            schema.get("schema-id").and_then(json_i64_string).as_deref() == Some(current_schema_id)
        })
        .ok_or_else(|| {
            ShardLoomError::InvalidOperation(format!(
                "Iceberg current schema id {current_schema_id} was not found in schemas"
            ))
        })
}

fn select_iceberg_snapshot(
    snapshots: &[serde_json::Value],
    current_snapshot_id: &str,
    selection: &IcebergSnapshotSelectionRequest,
) -> Result<IcebergMetadataSnapshotSummary, ShardLoomError> {
    match selection {
        IcebergSnapshotSelectionRequest::Current => {
            snapshot_by_id(snapshots, current_snapshot_id, "current-snapshot-id")
        }
        IcebergSnapshotSelectionRequest::SnapshotId(snapshot_id) => {
            snapshot_by_id(snapshots, snapshot_id, "--snapshot-id")
        }
        IcebergSnapshotSelectionRequest::AsOfTimestampMillis(timestamp) => {
            snapshot_as_of_timestamp(snapshots, *timestamp)
        }
    }
}

fn snapshot_by_id(
    snapshots: &[serde_json::Value],
    snapshot_id: &str,
    selector_label: &str,
) -> Result<IcebergMetadataSnapshotSummary, ShardLoomError> {
    snapshots
        .iter()
        .filter_map(serde_json::Value::as_object)
        .find(|snapshot| snapshot_id_for(snapshot).as_deref() == Some(snapshot_id))
        .map(snapshot_summary)
        .transpose()?
        .ok_or_else(|| {
            ShardLoomError::InvalidOperation(format!(
                "Iceberg {selector_label} {snapshot_id} was not found in snapshots"
            ))
        })
}

fn snapshot_as_of_timestamp(
    snapshots: &[serde_json::Value],
    timestamp_ms: i64,
) -> Result<IcebergMetadataSnapshotSummary, ShardLoomError> {
    snapshots
        .iter()
        .filter_map(serde_json::Value::as_object)
        .filter_map(|snapshot| {
            let snapshot_ts = snapshot
                .get("timestamp-ms")
                .and_then(serde_json::Value::as_i64)?;
            (snapshot_ts <= timestamp_ms).then_some((snapshot_ts, snapshot))
        })
        .max_by_key(|(snapshot_ts, _)| *snapshot_ts)
        .map(|(_, snapshot)| snapshot_summary(snapshot))
        .transpose()?
        .ok_or_else(|| {
            ShardLoomError::InvalidOperation(format!(
                "no Iceberg snapshot exists at or before timestamp {timestamp_ms}"
            ))
        })
}

fn snapshot_summary(
    snapshot: &serde_json::Map<String, serde_json::Value>,
) -> Result<IcebergMetadataSnapshotSummary, ShardLoomError> {
    let snapshot_id = snapshot_id_for(snapshot).ok_or_else(|| {
        ShardLoomError::InvalidOperation("Iceberg snapshot missing snapshot-id".to_string())
    })?;
    Ok(IcebergMetadataSnapshotSummary {
        snapshot_id,
        sequence_number: snapshot
            .get("sequence-number")
            .and_then(serde_json::Value::as_i64),
        timestamp_ms: snapshot
            .get("timestamp-ms")
            .and_then(serde_json::Value::as_i64),
        manifest_list: snapshot
            .get("manifest-list")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("none")
            .to_string(),
        operation: snapshot
            .get("summary")
            .and_then(serde_json::Value::as_object)
            .and_then(|summary| summary.get("operation"))
            .and_then(serde_json::Value::as_str)
            .unwrap_or("unknown")
            .to_string(),
        delete_file_count: snapshot_delete_file_count(snapshot),
    })
}

fn snapshot_id_for(snapshot: &serde_json::Map<String, serde_json::Value>) -> Option<String> {
    snapshot.get("snapshot-id").and_then(json_i64_string)
}

fn snapshot_delete_file_count(snapshot: &serde_json::Map<String, serde_json::Value>) -> u64 {
    snapshot
        .get("summary")
        .and_then(serde_json::Value::as_object)
        .map_or(0, |summary| {
            [
                "total-delete-files",
                "added-delete-files",
                "removed-delete-files",
            ]
            .iter()
            .filter_map(|key| summary.get(*key))
            .filter_map(json_count_value)
            .sum()
        })
}

fn json_count_value(value: &serde_json::Value) -> Option<u64> {
    value
        .as_u64()
        .or_else(|| value.as_str().and_then(|raw| raw.parse::<u64>().ok()))
}

fn schema_fields(
    schema: &serde_json::Map<String, serde_json::Value>,
) -> Option<&Vec<serde_json::Value>> {
    schema.get("fields").and_then(serde_json::Value::as_array)
}

fn schema_field_count(schema: &serde_json::Map<String, serde_json::Value>) -> usize {
    schema_fields(schema).map_or(0, Vec::len)
}

fn schema_field_ids_present(schema: &serde_json::Map<String, serde_json::Value>) -> bool {
    schema_fields(schema).is_some_and(|fields| {
        !fields.is_empty()
            && fields
                .iter()
                .filter_map(serde_json::Value::as_object)
                .all(|field| field.get("id").and_then(json_i64_string).is_some())
    })
}

fn complex_or_nested_schema_field_count(
    schema: &serde_json::Map<String, serde_json::Value>,
) -> usize {
    schema_fields(schema).map_or(0, |fields| {
        fields
            .iter()
            .filter_map(serde_json::Value::as_object)
            .filter(|field| {
                field
                    .get("type")
                    .is_some_and(|field_type| !field_type.is_string())
            })
            .count()
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct IcebergSchemaFieldIdentity {
    name: String,
    required: bool,
    type_fingerprint: String,
    complex_or_nested: bool,
}

fn iceberg_schema_evolution_summary(
    schemas: &[serde_json::Value],
) -> IcebergSchemaEvolutionSummary {
    let schema_id_order = schemas
        .iter()
        .filter_map(serde_json::Value::as_object)
        .filter_map(|schema| schema.get("schema-id").and_then(json_i64_string))
        .collect::<Vec<_>>();
    let schema_maps = schemas
        .iter()
        .filter_map(serde_json::Value::as_object)
        .map(iceberg_schema_field_identity_map)
        .collect::<Vec<_>>();
    let missing_field_id_count = schema_maps
        .iter()
        .map(|schema| schema.missing_field_id_count)
        .sum();
    let duplicate_field_id_count = schema_maps
        .iter()
        .map(|schema| schema.duplicate_field_id_count)
        .sum();
    let mut summary = IcebergSchemaEvolutionSummary {
        schema_id_order,
        schema_evolution_present: schemas.len() > 1,
        added_field_id_count: 0,
        dropped_field_id_count: 0,
        renamed_field_id_count: 0,
        type_changed_field_id_count: 0,
        requiredness_changed_field_id_count: 0,
        required_field_added_count: 0,
        missing_field_id_count,
        duplicate_field_id_count,
        complex_or_nested_evolution_field_count: 0,
        admission_status: "single_schema_no_evolution",
        admission_rule: "field_ids_required_safe_add_drop_rename_reorder_metadata_only_no_data_projection",
    };
    for pair in schema_maps.windows(2) {
        observe_iceberg_schema_evolution_pair(&mut summary, &pair[0].fields, &pair[1].fields);
    }
    summary.admission_status = iceberg_schema_evolution_admission_status(&summary);
    summary
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct IcebergSchemaFieldIdentityMap {
    fields: BTreeMap<i64, IcebergSchemaFieldIdentity>,
    missing_field_id_count: usize,
    duplicate_field_id_count: usize,
}

fn iceberg_schema_field_identity_map(
    schema: &serde_json::Map<String, serde_json::Value>,
) -> IcebergSchemaFieldIdentityMap {
    let mut fields = BTreeMap::new();
    let mut seen = BTreeSet::new();
    let mut missing_field_id_count = 0;
    let mut duplicate_field_id_count = 0;
    for field in schema_fields(schema).into_iter().flatten() {
        let Some(field) = field.as_object() else {
            missing_field_id_count += 1;
            continue;
        };
        let Some(field_id) = field.get("id").and_then(json_i64) else {
            missing_field_id_count += 1;
            continue;
        };
        if !seen.insert(field_id) {
            duplicate_field_id_count += 1;
            continue;
        }
        fields.insert(field_id, iceberg_schema_field_identity(field));
    }
    IcebergSchemaFieldIdentityMap {
        fields,
        missing_field_id_count,
        duplicate_field_id_count,
    }
}

fn iceberg_schema_field_identity(
    field: &serde_json::Map<String, serde_json::Value>,
) -> IcebergSchemaFieldIdentity {
    let field_type = field
        .get("type")
        .cloned()
        .unwrap_or(serde_json::Value::String("unknown".to_string()));
    IcebergSchemaFieldIdentity {
        name: field
            .get("name")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("unknown")
            .to_string(),
        required: field
            .get("required")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false),
        type_fingerprint: iceberg_json_type_fingerprint(&field_type),
        complex_or_nested: !field_type.is_string(),
    }
}

fn observe_iceberg_schema_evolution_pair(
    summary: &mut IcebergSchemaEvolutionSummary,
    from: &BTreeMap<i64, IcebergSchemaFieldIdentity>,
    to: &BTreeMap<i64, IcebergSchemaFieldIdentity>,
) {
    for (field_id, to_field) in to {
        match from.get(field_id) {
            None => {
                summary.added_field_id_count += 1;
                if to_field.required {
                    summary.required_field_added_count += 1;
                }
                if to_field.complex_or_nested {
                    summary.complex_or_nested_evolution_field_count += 1;
                }
            }
            Some(from_field) => {
                observe_iceberg_schema_field_change(summary, from_field, to_field);
            }
        }
    }
    for (field_id, from_field) in from {
        if !to.contains_key(field_id) {
            summary.dropped_field_id_count += 1;
            if from_field.complex_or_nested {
                summary.complex_or_nested_evolution_field_count += 1;
            }
        }
    }
}

fn observe_iceberg_schema_field_change(
    summary: &mut IcebergSchemaEvolutionSummary,
    from_field: &IcebergSchemaFieldIdentity,
    to_field: &IcebergSchemaFieldIdentity,
) {
    if from_field.name != to_field.name {
        summary.renamed_field_id_count += 1;
    }
    if from_field.type_fingerprint != to_field.type_fingerprint {
        summary.type_changed_field_id_count += 1;
    }
    if from_field.required != to_field.required {
        summary.requiredness_changed_field_id_count += 1;
    }
    if (from_field.type_fingerprint != to_field.type_fingerprint
        || from_field.name != to_field.name
        || from_field.required != to_field.required)
        && (from_field.complex_or_nested || to_field.complex_or_nested)
    {
        summary.complex_or_nested_evolution_field_count += 1;
    }
}

fn iceberg_schema_evolution_admission_status(
    summary: &IcebergSchemaEvolutionSummary,
) -> &'static str {
    if summary.missing_field_id_count > 0 || summary.duplicate_field_id_count > 0 {
        "blocked_schema_field_id_integrity"
    } else if summary.blocks_runtime_projection() {
        "blocked_requires_schema_projection_semantics"
    } else if summary.schema_evolution_present {
        "metadata_only_id_based_schema_evolution_admitted_no_data_projection"
    } else {
        "single_schema_no_evolution"
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct IcebergPartitionFieldIdentity {
    source_id: String,
    name: String,
    transform: String,
}

impl IcebergPartitionFieldIdentity {
    fn signature(&self) -> String {
        format!("{}|{}|{}", self.source_id, self.transform, self.name)
    }
}

fn iceberg_partition_evolution_summary(
    root: &serde_json::Map<String, serde_json::Value>,
    manifest_list_summary: Option<&IcebergManifestListSummary>,
) -> IcebergPartitionEvolutionSummary {
    let partition_specs = root
        .get("partition-specs")
        .and_then(serde_json::Value::as_array)
        .map_or(&[][..], Vec::as_slice);
    let spec_maps = partition_specs
        .iter()
        .filter_map(serde_json::Value::as_object)
        .map(iceberg_partition_field_identity_map)
        .collect::<Vec<_>>();
    let partition_spec_id_order = partition_specs
        .iter()
        .filter_map(serde_json::Value::as_object)
        .filter_map(|spec| spec.get("spec-id").and_then(json_i64_string))
        .collect::<Vec<_>>();
    let mut summary = IcebergPartitionEvolutionSummary {
        partition_spec_id_order: partition_spec_id_order.clone(),
        partition_evolution_present: partition_specs.len() > 1,
        default_partition_spec_id: optional_i64_string(root, "default-spec-id"),
        last_partition_id: optional_i64_string(root, "last-partition-id"),
        added_partition_field_count: 0,
        removed_partition_field_count: 0,
        renamed_partition_field_count: 0,
        source_changed_partition_field_count: 0,
        transform_changed_partition_field_count: 0,
        missing_partition_field_id_count: spec_maps
            .iter()
            .map(|spec| spec.missing_field_id_count)
            .sum(),
        duplicate_partition_field_id_count: spec_maps
            .iter()
            .map(|spec| spec.duplicate_field_id_count)
            .sum(),
        field_id_reuse_mismatch_count: 0,
        unknown_transform_count: spec_maps
            .iter()
            .map(|spec| spec.unknown_transform_count)
            .sum(),
        manifest_partition_spec_id_order: manifest_list_summary
            .map_or_else(Vec::new, |summary| summary.partition_spec_id_order.clone()),
        manifest_unknown_partition_spec_id_count: 0,
        admission_status: "single_partition_spec_no_evolution",
        admission_rule: "partition_field_ids_and_manifest_spec_ids_required_metadata_only_no_filter_execution",
    };
    for pair in spec_maps.windows(2) {
        observe_iceberg_partition_evolution_pair(&mut summary, &pair[0].fields, &pair[1].fields);
    }
    summary.manifest_unknown_partition_spec_id_count =
        iceberg_unknown_manifest_partition_spec_count(&partition_spec_id_order, &summary);
    summary.admission_status = iceberg_partition_evolution_admission_status(&summary);
    summary
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct IcebergPartitionFieldIdentityMap {
    fields: BTreeMap<i64, IcebergPartitionFieldIdentity>,
    missing_field_id_count: usize,
    duplicate_field_id_count: usize,
    unknown_transform_count: usize,
}

fn iceberg_partition_field_identity_map(
    spec: &serde_json::Map<String, serde_json::Value>,
) -> IcebergPartitionFieldIdentityMap {
    let mut fields = BTreeMap::new();
    let mut seen = BTreeSet::new();
    let mut missing_field_id_count = 0;
    let mut duplicate_field_id_count = 0;
    let mut unknown_transform_count = 0;
    let spec_fields = spec
        .get("fields")
        .and_then(serde_json::Value::as_array)
        .map_or(&[][..], Vec::as_slice);
    for field in spec_fields {
        let Some(field) = field.as_object() else {
            missing_field_id_count += 1;
            continue;
        };
        let Some(field_id) = field.get("field-id").and_then(json_i64) else {
            missing_field_id_count += 1;
            continue;
        };
        if !seen.insert(field_id) {
            duplicate_field_id_count += 1;
            continue;
        }
        let identity = iceberg_partition_field_identity(field);
        if !iceberg_partition_transform_known(&identity.transform) {
            unknown_transform_count += 1;
        }
        fields.insert(field_id, identity);
    }
    IcebergPartitionFieldIdentityMap {
        fields,
        missing_field_id_count,
        duplicate_field_id_count,
        unknown_transform_count,
    }
}

fn iceberg_partition_field_identity(
    field: &serde_json::Map<String, serde_json::Value>,
) -> IcebergPartitionFieldIdentity {
    IcebergPartitionFieldIdentity {
        source_id: field
            .get("source-id")
            .and_then(json_i64_string)
            .unwrap_or_else(|| "none".to_string()),
        name: field
            .get("name")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("unknown")
            .to_string(),
        transform: field
            .get("transform")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("unknown")
            .to_string(),
    }
}

fn observe_iceberg_partition_evolution_pair(
    summary: &mut IcebergPartitionEvolutionSummary,
    from: &BTreeMap<i64, IcebergPartitionFieldIdentity>,
    to: &BTreeMap<i64, IcebergPartitionFieldIdentity>,
) {
    let from_signatures = from
        .values()
        .map(IcebergPartitionFieldIdentity::signature)
        .collect::<BTreeSet<_>>();
    let to_signatures = to
        .values()
        .map(IcebergPartitionFieldIdentity::signature)
        .collect::<BTreeSet<_>>();
    summary.added_partition_field_count += to_signatures.difference(&from_signatures).count();
    summary.removed_partition_field_count += from_signatures.difference(&to_signatures).count();
    for (field_id, to_field) in to {
        if let Some(from_field) = from.get(field_id) {
            if from_field.signature() != to_field.signature() {
                summary.field_id_reuse_mismatch_count += 1;
            }
            if from_field.name != to_field.name {
                summary.renamed_partition_field_count += 1;
            }
            if from_field.source_id != to_field.source_id {
                summary.source_changed_partition_field_count += 1;
            }
            if from_field.transform != to_field.transform {
                summary.transform_changed_partition_field_count += 1;
            }
        }
    }
}

fn iceberg_partition_evolution_admission_status(
    summary: &IcebergPartitionEvolutionSummary,
) -> &'static str {
    if summary.missing_partition_field_id_count > 0
        || summary.duplicate_partition_field_id_count > 0
        || summary.manifest_unknown_partition_spec_id_count > 0
    {
        "blocked_partition_field_or_manifest_spec_integrity"
    } else if summary.blocks_runtime_projection() {
        "blocked_requires_partition_projection_semantics"
    } else if summary.partition_evolution_present {
        "metadata_only_partition_evolution_admitted_no_filter_execution"
    } else {
        "single_partition_spec_no_evolution"
    }
}

fn iceberg_unknown_manifest_partition_spec_count(
    known_spec_ids: &[String],
    summary: &IcebergPartitionEvolutionSummary,
) -> usize {
    let known = known_spec_ids.iter().collect::<BTreeSet<_>>();
    summary
        .manifest_partition_spec_id_order
        .iter()
        .filter(|spec_id| !known.contains(spec_id))
        .count()
}

fn iceberg_partition_transform_known(transform: &str) -> bool {
    matches!(
        transform,
        "identity" | "year" | "month" | "day" | "hour" | "void"
    ) || transform.starts_with("bucket[")
        || transform.starts_with("truncate[")
}

fn iceberg_delete_admission_summary(
    selected_snapshot: &IcebergMetadataSnapshotSummary,
    manifest_list_summary: Option<&IcebergManifestListSummary>,
    manifest_file_summary: Option<&IcebergManifestFileSummary>,
) -> IcebergDeleteAdmissionSummary {
    let mut summary = IcebergDeleteAdmissionSummary {
        selected_snapshot_delete_file_count: selected_snapshot.delete_file_count,
        manifest_list_delete_manifest_count: manifest_list_summary
            .map_or(0, |summary| summary.delete_manifest_count),
        manifest_list_delete_file_count: manifest_list_summary
            .map_or(0, IcebergManifestListSummary::total_delete_file_count),
        manifest_file_deleted_data_file_count: manifest_file_summary
            .map_or(0, |summary| summary.deleted_data_file_count),
        manifest_file_position_delete_file_count: manifest_file_summary
            .map_or(0, |summary| summary.position_delete_file_entry_count),
        manifest_file_equality_delete_file_count: manifest_file_summary
            .map_or(0, |summary| summary.equality_delete_file_entry_count),
        manifest_file_deletion_vector_count: manifest_file_summary
            .map_or(0, |summary| summary.deletion_vector_entry_count),
        manifest_file_unknown_delete_content_count: manifest_file_summary
            .map_or(0, |summary| summary.unknown_content_file_count),
        admission_status: "delete_execution_blocked_no_delete_evidence_present",
        admission_rule: "metadata_detects_delete_manifests_position_deletes_equality_deletes_and_deletion_vectors_fail_closed",
    };
    summary.admission_status = iceberg_delete_admission_status(&summary);
    summary
}

fn iceberg_delete_admission_status(summary: &IcebergDeleteAdmissionSummary) -> &'static str {
    if !summary.blocks_runtime_delete_execution() {
        "delete_execution_blocked_no_delete_evidence_present"
    } else if summary.manifest_file_deletion_vector_count > 0 {
        "deletion_vectors_blocked_requires_puffin_vector_application"
    } else if summary.manifest_file_equality_delete_file_count > 0 {
        "equality_delete_files_blocked_requires_field_id_predicate_application"
    } else if summary.manifest_file_position_delete_file_count > 0 {
        "position_delete_files_blocked_requires_row_position_filtering"
    } else if summary.manifest_list_delete_manifest_count > 0
        || summary.manifest_list_delete_file_count > 0
        || summary.selected_snapshot_delete_file_count > 0
    {
        "delete_manifests_or_delete_files_blocked"
    } else if summary.manifest_file_deleted_data_file_count > 0 {
        "deleted_data_file_entries_blocked"
    } else if summary.manifest_file_unknown_delete_content_count > 0 {
        "unknown_delete_content_blocked"
    } else {
        "delete_execution_blocked_unclassified_delete_evidence_present"
    }
}

fn json_i64(value: &serde_json::Value) -> Option<i64> {
    value
        .as_i64()
        .or_else(|| value.as_u64().and_then(|number| i64::try_from(number).ok()))
}

fn iceberg_json_type_fingerprint(value: &serde_json::Value) -> String {
    value
        .as_str()
        .map_or_else(|| value.to_string(), ToOwned::to_owned)
}

#[cfg(feature = "universal-format-io")]
fn push_unique_string(values: &mut Vec<String>, value: &str) {
    if !values.iter().any(|existing| existing == value) {
        values.push(value.to_string());
    }
}

fn manifest_list_ref_count(snapshots: &[serde_json::Value]) -> usize {
    snapshots
        .iter()
        .filter_map(serde_json::Value::as_object)
        .filter(|snapshot| {
            snapshot
                .get("manifest-list")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|value| !value.trim().is_empty())
        })
        .count()
}

struct IcebergMetadataUnsupportedFeatureContext<'a> {
    format_version: u64,
    selected_snapshot: &'a IcebergMetadataSnapshotSummary,
    schema_evolution_summary: &'a IcebergSchemaEvolutionSummary,
    partition_evolution_summary: &'a IcebergPartitionEvolutionSummary,
    delete_admission_summary: &'a IcebergDeleteAdmissionSummary,
    manifest_list_reader_feature_disabled: bool,
    manifest_file_reader_feature_disabled: bool,
    manifest_list_summary: Option<&'a IcebergManifestListSummary>,
    manifest_file_summary: Option<&'a IcebergManifestFileSummary>,
}

fn iceberg_metadata_unsupported_feature_order(
    context: &IcebergMetadataUnsupportedFeatureContext<'_>,
) -> Vec<&'static str> {
    let mut features = Vec::new();
    if context.format_version > 2 {
        features.push("format_version_gt_2");
    }
    if context.selected_snapshot.delete_file_count > 0 {
        features.push("delete_files_present");
    }
    if context.schema_evolution_summary.missing_field_id_count > 0
        || context.schema_evolution_summary.duplicate_field_id_count > 0
    {
        features.push("schema_field_id_integrity_blocked");
    } else if context.schema_evolution_summary.blocks_runtime_projection() {
        features.push("schema_evolution_projection_required");
    }
    if context
        .partition_evolution_summary
        .missing_partition_field_id_count
        > 0
        || context
            .partition_evolution_summary
            .duplicate_partition_field_id_count
            > 0
        || context
            .partition_evolution_summary
            .manifest_unknown_partition_spec_id_count
            > 0
    {
        features.push("partition_field_or_manifest_spec_integrity_blocked");
    } else if context
        .partition_evolution_summary
        .blocks_runtime_projection()
    {
        features.push("partition_evolution_projection_required");
    }
    if context.manifest_list_reader_feature_disabled {
        features.push("manifest_list_reader_feature_disabled");
    }
    if context.manifest_file_reader_feature_disabled {
        features.push("manifest_file_reader_feature_disabled");
    }
    if let Some(summary) = context.manifest_list_summary {
        if summary.delete_manifest_count > 0 || summary.total_delete_file_count() > 0 {
            features.push("delete_manifests_present");
        }
        if summary.unknown_content_manifest_count > 0 {
            features.push("unknown_manifest_content_present");
        }
    }
    if let Some(summary) = context.manifest_file_summary {
        if context
            .delete_admission_summary
            .manifest_file_deletion_vector_count
            > 0
        {
            features.push("deletion_vector_entries_present");
        }
        if context
            .delete_admission_summary
            .manifest_file_position_delete_file_count
            > 0
        {
            features.push("position_delete_file_entries_present");
        }
        if context
            .delete_admission_summary
            .manifest_file_equality_delete_file_count
            > 0
        {
            features.push("equality_delete_file_entries_present");
        }
        if summary.delete_file_entry_count > 0
            && context
                .delete_admission_summary
                .manifest_file_position_delete_file_count
                == 0
            && context
                .delete_admission_summary
                .manifest_file_equality_delete_file_count
                == 0
            && context
                .delete_admission_summary
                .manifest_file_deletion_vector_count
                == 0
        {
            features.push("delete_file_entries_present");
        }
        if summary.deleted_data_file_count > 0 {
            features.push("deleted_data_file_entries_present");
        }
        if summary.unknown_content_file_count > 0 {
            features.push("unknown_data_file_content_present");
        }
        if summary.unknown_status_entry_count > 0 {
            features.push("unknown_manifest_entry_status_present");
        }
    }
    features
}

fn iceberg_unsupported_feature_diagnostics(features: &[&'static str]) -> Vec<Diagnostic> {
    features
        .iter()
        .map(|feature| {
            Diagnostic::new(
                DiagnosticCode::NotImplemented,
                DiagnosticSeverity::Error,
                DiagnosticCategory::UnsupportedFeature,
                format!("Iceberg metadata feature {feature} is not admitted for runtime support"),
                Some((*feature).to_string()),
                Some(
                    "The local metadata smoke parsed the table metadata but found an unadmitted Iceberg feature."
                        .to_string(),
                ),
                Some(
                    "Keep the request blocked until the phased table runtime item adds correctness and no-fallback evidence for this feature."
                        .to_string(),
                ),
                FallbackStatus::disabled_by_policy(),
            )
        })
        .collect()
}

struct IcebergMetadataSummaryContext<'a> {
    table_uuid: &'a str,
    current_snapshot_id: &'a str,
    selected_snapshot: &'a IcebergMetadataSnapshotSummary,
    manifest_list_summary: Option<&'a IcebergManifestListSummary>,
    manifest_file_summary: Option<&'a IcebergManifestFileSummary>,
    schema_evolution_summary: &'a IcebergSchemaEvolutionSummary,
    partition_evolution_summary: &'a IcebergPartitionEvolutionSummary,
    delete_admission_summary: &'a IcebergDeleteAdmissionSummary,
    root: &'a serde_json::Map<String, serde_json::Value>,
    schemas: &'a [serde_json::Value],
    snapshots: &'a [serde_json::Value],
}

fn iceberg_metadata_summary(context: &IcebergMetadataSummaryContext<'_>) -> String {
    let manifest_list_read = context.manifest_list_summary.is_some();
    let manifest_file_read = context.manifest_file_summary.is_some();
    let planned_manifest_splits = context
        .manifest_list_summary
        .map_or(0, |summary| summary.planned_manifest_split_count);
    let planned_data_files = context.manifest_file_summary.map_or_else(
        || {
            context
                .manifest_list_summary
                .map_or(0, |summary| summary.planned_data_file_count)
        },
        |summary| summary.planned_data_file_count,
    );
    format!(
        "protocol=iceberg table_uuid={} format_version={} current_schema_id={} current_snapshot_id={} selected_snapshot_id={} schemas={} schema_evolution_status={} partition_specs={} partition_evolution_status={} sort_orders={} snapshots={} manifest_list_refs={} manifest_list_read={} manifest_file_read={} planned_manifest_splits={} planned_data_files={} delete_files={} delete_admission_status={} catalog_io=false object_store_io=false data_file_read=false fallback_attempted=false external_engine_invoked=false",
        context.table_uuid,
        context
            .root
            .get("format-version")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0),
        context
            .root
            .get("current-schema-id")
            .and_then(json_i64_string)
            .unwrap_or_else(|| "none".to_string()),
        context.current_snapshot_id,
        context.selected_snapshot.snapshot_id,
        context.schemas.len(),
        context.schema_evolution_summary.admission_status,
        optional_array_len(context.root, "partition-specs"),
        context.partition_evolution_summary.admission_status,
        optional_array_len(context.root, "sort-orders"),
        context.snapshots.len(),
        manifest_list_ref_count(context.snapshots),
        manifest_list_read,
        manifest_file_read,
        planned_manifest_splits,
        planned_data_files,
        context.selected_snapshot.delete_file_count,
        context.delete_admission_summary.admission_status
    )
}

fn iceberg_metadata_digest(input: &str) -> String {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in input.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    format!("fnv1a64:{hash:016x}")
}

fn iceberg_metadata_read_smoke_fields(
    report: &IcebergMetadataReadSmokeReport,
) -> Vec<(String, String)> {
    let mut fields = vec![];
    append_iceberg_metadata_identity_fields(&mut fields, report);
    append_iceberg_metadata_summary_fields(&mut fields, report);
    append_iceberg_manifest_list_summary_fields(&mut fields, report);
    append_iceberg_manifest_file_summary_fields(&mut fields, report);
    append_iceberg_metadata_evidence_fields(&mut fields, report);
    append_iceberg_metadata_boundary_fields(&mut fields, report);
    append_iceberg_metadata_diagnostic_fields(&mut fields, report);
    push_field(&mut fields, "execution", "performed");
    push_bool_field(&mut fields, "plan_only", false);
    fields
}

fn append_iceberg_metadata_identity_fields(
    fields: &mut Vec<(String, String)>,
    report: &IcebergMetadataReadSmokeReport,
) {
    push_field(fields, "mode", "iceberg_metadata_read_smoke");
    push_field(fields, "schema_version", report.schema_version);
    push_field(fields, "report_id", report.report_id);
    push_field(fields, "phase_id", report.phase_id);
    push_field(fields, "support_status", report.support_status);
    push_field(fields, "claim_gate_status", report.claim_gate_status);
    push_field(fields, "claim_boundary", report.claim_boundary);
    push_field(fields, "source_protocol", report.source_protocol);
    push_field(fields, "source_review_ref", report.source_review_ref);
    push_field(fields, "metadata_path", &report.metadata_path);
}

fn append_iceberg_metadata_summary_fields(
    fields: &mut Vec<(String, String)>,
    report: &IcebergMetadataReadSmokeReport,
) {
    push_count_field(fields, "metadata_bytes_read", report.metadata_bytes_read);
    push_field(fields, "format_version", &report.format_version.to_string());
    push_field(fields, "table_uuid", &report.table_uuid);
    push_field(fields, "table_location", &report.table_location);
    push_field(fields, "current_schema_id", &report.current_schema_id);
    push_count_field(fields, "schema_count", report.schema_count);
    push_count_field(
        fields,
        "current_schema_field_count",
        report.current_schema_field_count,
    );
    push_bool_field(
        fields,
        "schema_field_ids_present",
        report.schema_field_ids_present,
    );
    push_count_field(
        fields,
        "complex_or_nested_schema_field_count",
        report.complex_or_nested_schema_field_count,
    );
    append_iceberg_schema_evolution_fields(fields, &report.schema_evolution_summary);
    push_count_field(fields, "partition_spec_count", report.partition_spec_count);
    push_field(
        fields,
        "default_partition_spec_id",
        &report.default_partition_spec_id,
    );
    append_iceberg_partition_evolution_fields(fields, &report.partition_evolution_summary);
    push_count_field(fields, "sort_order_count", report.sort_order_count);
    push_field(
        fields,
        "default_sort_order_id",
        &report.default_sort_order_id,
    );
    push_count_field(fields, "snapshot_count", report.snapshot_count);
    push_field(fields, "current_snapshot_id", &report.current_snapshot_id);
    push_field(
        fields,
        "selected_snapshot_id",
        &report.selected_snapshot.snapshot_id,
    );
    push_field(
        fields,
        "selected_snapshot_sequence_number",
        &optional_i64_text(report.selected_snapshot.sequence_number),
    );
    push_field(
        fields,
        "selected_snapshot_timestamp_ms",
        &optional_i64_text(report.selected_snapshot.timestamp_ms),
    );
    push_field(
        fields,
        "selected_snapshot_manifest_list",
        &report.selected_snapshot.manifest_list,
    );
    push_field(
        fields,
        "selected_snapshot_operation",
        &report.selected_snapshot.operation,
    );
    push_field(
        fields,
        "selected_snapshot_delete_file_count",
        &report.selected_snapshot.delete_file_count.to_string(),
    );
    push_field(
        fields,
        "snapshot_selector_kind",
        report.snapshot_selector_kind,
    );
    push_count_field(
        fields,
        "manifest_list_ref_count",
        report.manifest_list_ref_count,
    );
    push_count_field(
        fields,
        "branch_or_tag_ref_count",
        report.branch_or_tag_ref_count,
    );
    push_field(fields, "last_sequence_number", &report.last_sequence_number);
    push_field(fields, "metadata_summary", &report.metadata_summary);
    push_field(
        fields,
        "metadata_summary_digest",
        &report.metadata_summary_digest,
    );
}

fn append_iceberg_schema_evolution_fields(
    fields: &mut Vec<(String, String)>,
    summary: &IcebergSchemaEvolutionSummary,
) {
    push_bool_field(
        fields,
        "schema_evolution_present",
        summary.schema_evolution_present,
    );
    push_field(
        fields,
        "schema_id_order",
        &summary.schema_id_order.join(","),
    );
    push_count_field(
        fields,
        "schema_added_field_id_count",
        summary.added_field_id_count,
    );
    push_count_field(
        fields,
        "schema_dropped_field_id_count",
        summary.dropped_field_id_count,
    );
    push_count_field(
        fields,
        "schema_renamed_field_id_count",
        summary.renamed_field_id_count,
    );
    push_count_field(
        fields,
        "schema_type_changed_field_id_count",
        summary.type_changed_field_id_count,
    );
    push_count_field(
        fields,
        "schema_requiredness_changed_field_id_count",
        summary.requiredness_changed_field_id_count,
    );
    push_count_field(
        fields,
        "schema_required_field_added_count",
        summary.required_field_added_count,
    );
    push_count_field(
        fields,
        "schema_missing_field_id_count",
        summary.missing_field_id_count,
    );
    push_count_field(
        fields,
        "schema_duplicate_field_id_count",
        summary.duplicate_field_id_count,
    );
    push_count_field(
        fields,
        "schema_complex_or_nested_evolution_field_count",
        summary.complex_or_nested_evolution_field_count,
    );
    push_field(
        fields,
        "schema_evolution_admission_status",
        summary.admission_status,
    );
    push_field(
        fields,
        "schema_evolution_admission_rule",
        summary.admission_rule,
    );
}

fn append_iceberg_partition_evolution_fields(
    fields: &mut Vec<(String, String)>,
    summary: &IcebergPartitionEvolutionSummary,
) {
    push_bool_field(
        fields,
        "partition_evolution_present",
        summary.partition_evolution_present,
    );
    push_field(
        fields,
        "partition_spec_id_order",
        &summary.partition_spec_id_order.join(","),
    );
    push_field(
        fields,
        "partition_default_spec_id",
        &summary.default_partition_spec_id,
    );
    push_field(
        fields,
        "partition_last_partition_id",
        &summary.last_partition_id,
    );
    push_count_field(
        fields,
        "partition_added_field_count",
        summary.added_partition_field_count,
    );
    push_count_field(
        fields,
        "partition_removed_field_count",
        summary.removed_partition_field_count,
    );
    push_count_field(
        fields,
        "partition_renamed_field_count",
        summary.renamed_partition_field_count,
    );
    push_count_field(
        fields,
        "partition_source_changed_field_count",
        summary.source_changed_partition_field_count,
    );
    push_count_field(
        fields,
        "partition_transform_changed_field_count",
        summary.transform_changed_partition_field_count,
    );
    push_count_field(
        fields,
        "partition_missing_field_id_count",
        summary.missing_partition_field_id_count,
    );
    push_count_field(
        fields,
        "partition_duplicate_field_id_count",
        summary.duplicate_partition_field_id_count,
    );
    push_count_field(
        fields,
        "partition_field_id_reuse_mismatch_count",
        summary.field_id_reuse_mismatch_count,
    );
    push_count_field(
        fields,
        "partition_unknown_transform_count",
        summary.unknown_transform_count,
    );
    push_field(
        fields,
        "manifest_partition_spec_id_order",
        &summary.manifest_partition_spec_id_order.join(","),
    );
    push_count_field(
        fields,
        "manifest_unknown_partition_spec_id_count",
        summary.manifest_unknown_partition_spec_id_count,
    );
    push_field(
        fields,
        "partition_evolution_admission_status",
        summary.admission_status,
    );
    push_field(
        fields,
        "partition_evolution_admission_rule",
        summary.admission_rule,
    );
}

fn push_optional_count_field<T>(
    fields: &mut Vec<(String, String)>,
    key: &str,
    value: Option<&T>,
    extract: impl FnOnce(&T) -> usize,
) {
    push_count_field(fields, key, value.map_or(0, extract));
}

fn push_optional_u64_field<T>(
    fields: &mut Vec<(String, String)>,
    key: &str,
    value: Option<&T>,
    extract: impl FnOnce(&T) -> u64,
) {
    push_field(fields, key, &value.map_or(0, extract).to_string());
}

fn append_iceberg_manifest_list_summary_fields(
    fields: &mut Vec<(String, String)>,
    report: &IcebergMetadataReadSmokeReport,
) {
    let summary = report.manifest_list_summary.as_ref();
    append_iceberg_manifest_list_request_fields(fields, report, summary);
    append_iceberg_manifest_list_count_fields(fields, summary);
    append_iceberg_manifest_admission_fields(fields, report, summary);
}

fn append_iceberg_manifest_list_request_fields(
    fields: &mut Vec<(String, String)>,
    report: &IcebergMetadataReadSmokeReport,
    summary: Option<&IcebergManifestListSummary>,
) {
    push_bool_field(
        fields,
        "manifest_list_requested",
        report.manifest_list_requested(),
    );
    push_bool_field(
        fields,
        "manifest_list_reader_feature_enabled",
        report.manifest_list_reader_feature_enabled,
    );
    push_field(
        fields,
        "manifest_list_path",
        report
            .manifest_list_path_requested
            .as_deref()
            .unwrap_or("none"),
    );
    push_optional_count_field(fields, "manifest_list_bytes_read", summary, |summary| {
        summary.bytes_read
    });
    push_optional_count_field(
        fields,
        "manifest_list_schema_column_count",
        summary,
        |summary| summary.schema_column_count,
    );
    push_optional_count_field(
        fields,
        "manifest_list_projected_column_count",
        summary,
        |summary| summary.projected_column_count,
    );
    push_optional_count_field(fields, "manifest_list_entry_count", summary, |summary| {
        summary.entry_count
    });
    push_field(
        fields,
        "manifest_list_partition_spec_id_order",
        &summary.map_or_else(String::new, |summary| {
            summary.partition_spec_id_order.join(",")
        }),
    );
}

fn append_iceberg_manifest_list_count_fields(
    fields: &mut Vec<(String, String)>,
    summary: Option<&IcebergManifestListSummary>,
) {
    push_optional_count_field(
        fields,
        "manifest_list_data_manifest_count",
        summary,
        |summary| summary.data_manifest_count,
    );
    push_optional_count_field(
        fields,
        "manifest_list_delete_manifest_count",
        summary,
        |summary| summary.delete_manifest_count,
    );
    push_optional_count_field(
        fields,
        "manifest_list_unknown_content_manifest_count",
        summary,
        |summary| summary.unknown_content_manifest_count,
    );
    push_optional_u64_field(
        fields,
        "manifest_list_total_manifest_bytes",
        summary,
        |summary| summary.total_manifest_bytes,
    );
    push_optional_u64_field(
        fields,
        "manifest_list_added_data_file_count",
        summary,
        |summary| summary.added_data_file_count,
    );
    push_optional_u64_field(
        fields,
        "manifest_list_existing_data_file_count",
        summary,
        |summary| summary.existing_data_file_count,
    );
    push_optional_u64_field(
        fields,
        "manifest_list_deleted_data_file_count",
        summary,
        |summary| summary.deleted_data_file_count,
    );
    push_optional_u64_field(
        fields,
        "manifest_list_added_delete_file_count",
        summary,
        |summary| summary.added_delete_file_count,
    );
    push_optional_u64_field(
        fields,
        "manifest_list_existing_delete_file_count",
        summary,
        |summary| summary.existing_delete_file_count,
    );
    push_optional_u64_field(
        fields,
        "manifest_list_deleted_delete_file_count",
        summary,
        |summary| summary.deleted_delete_file_count,
    );
    push_bool_field(
        fields,
        "manifest_summary_pruning_performed",
        summary.is_some(),
    );
    push_field(
        fields,
        "manifest_summary_pruning_rule",
        summary.map_or("none", |summary| summary.manifest_summary_pruning_rule),
    );
    push_bool_field(
        fields,
        "manifest_split_planning_performed",
        summary.is_some(),
    );
    push_optional_count_field(fields, "planned_manifest_split_count", summary, |summary| {
        summary.planned_manifest_split_count
    });
    push_optional_u64_field(fields, "planned_data_file_count", summary, |summary| {
        summary.planned_data_file_count
    });
}

fn append_iceberg_manifest_admission_fields(
    fields: &mut Vec<(String, String)>,
    report: &IcebergMetadataReadSmokeReport,
    summary: Option<&IcebergManifestListSummary>,
) {
    push_field(
        fields,
        "schema_partition_evolution_admission_status",
        iceberg_schema_partition_combined_admission_status(report, summary),
    );
    push_field(
        fields,
        "delete_tombstone_deletion_vector_admission_status",
        report.delete_admission_summary.admission_status,
    );
    append_iceberg_delete_admission_fields(fields, &report.delete_admission_summary);
}

fn iceberg_schema_partition_combined_admission_status(
    report: &IcebergMetadataReadSmokeReport,
    summary: Option<&IcebergManifestListSummary>,
) -> &'static str {
    if report.schema_evolution_summary.blocks_runtime_projection()
        || report
            .partition_evolution_summary
            .blocks_runtime_projection()
    {
        "schema_or_partition_evolution_blocked_requires_projection_semantics"
    } else if report.manifest_file_summary.is_some() {
        "metadata_ids_manifest_partition_spec_ids_and_data_file_partition_struct_visible_no_evolution_execution"
    } else if summary.is_some() {
        "metadata_ids_and_manifest_partition_spec_ids_visible_no_evolution_execution"
    } else {
        "metadata_ids_visible_no_manifest_summary"
    }
}

fn append_iceberg_delete_admission_fields(
    fields: &mut Vec<(String, String)>,
    summary: &IcebergDeleteAdmissionSummary,
) {
    push_field(fields, "delete_admission_status", summary.admission_status);
    push_field(fields, "delete_admission_rule", summary.admission_rule);
    push_optional_u64_value_field(
        fields,
        "delete_selected_snapshot_delete_file_count",
        summary.selected_snapshot_delete_file_count,
    );
    push_count_field(
        fields,
        "delete_manifest_list_delete_manifest_count",
        summary.manifest_list_delete_manifest_count,
    );
    push_optional_u64_value_field(
        fields,
        "delete_manifest_list_delete_file_count",
        summary.manifest_list_delete_file_count,
    );
    push_optional_u64_value_field(
        fields,
        "delete_manifest_file_deleted_data_file_count",
        summary.manifest_file_deleted_data_file_count,
    );
    push_optional_u64_value_field(
        fields,
        "delete_manifest_file_position_delete_file_count",
        summary.manifest_file_position_delete_file_count,
    );
    push_optional_u64_value_field(
        fields,
        "delete_manifest_file_equality_delete_file_count",
        summary.manifest_file_equality_delete_file_count,
    );
    push_optional_u64_value_field(
        fields,
        "delete_manifest_file_deletion_vector_count",
        summary.manifest_file_deletion_vector_count,
    );
    push_optional_u64_value_field(
        fields,
        "delete_manifest_file_unknown_delete_content_count",
        summary.manifest_file_unknown_delete_content_count,
    );
}

fn push_optional_u64_value_field(fields: &mut Vec<(String, String)>, key: &str, value: u64) {
    push_field(fields, key, &value.to_string());
}

fn append_iceberg_manifest_file_summary_fields(
    fields: &mut Vec<(String, String)>,
    report: &IcebergMetadataReadSmokeReport,
) {
    let summary = report.manifest_file_summary.as_ref();
    append_iceberg_manifest_file_request_fields(fields, report, summary);
    append_iceberg_manifest_file_count_fields(fields, summary);
}

fn append_iceberg_manifest_file_request_fields(
    fields: &mut Vec<(String, String)>,
    report: &IcebergMetadataReadSmokeReport,
    summary: Option<&IcebergManifestFileSummary>,
) {
    push_bool_field(
        fields,
        "manifest_file_requested",
        report.manifest_file_requested(),
    );
    push_bool_field(
        fields,
        "manifest_file_reader_feature_enabled",
        report.manifest_file_reader_feature_enabled,
    );
    push_field(
        fields,
        "manifest_file_path",
        report
            .manifest_file_path_requested
            .as_deref()
            .unwrap_or("none"),
    );
    push_optional_count_field(fields, "manifest_file_bytes_read", summary, |summary| {
        summary.bytes_read
    });
    push_optional_count_field(
        fields,
        "manifest_file_schema_column_count",
        summary,
        |summary| summary.schema_column_count,
    );
    push_optional_count_field(
        fields,
        "manifest_file_projected_column_count",
        summary,
        |summary| summary.projected_column_count,
    );
    push_optional_count_field(fields, "manifest_file_entry_count", summary, |summary| {
        summary.entry_count
    });
}

fn append_iceberg_manifest_file_count_fields(
    fields: &mut Vec<(String, String)>,
    summary: Option<&IcebergManifestFileSummary>,
) {
    push_optional_u64_field(
        fields,
        "manifest_file_added_data_file_count",
        summary,
        |summary| summary.added_data_file_count,
    );
    push_optional_u64_field(
        fields,
        "manifest_file_existing_data_file_count",
        summary,
        |summary| summary.existing_data_file_count,
    );
    push_optional_u64_field(
        fields,
        "manifest_file_deleted_data_file_count",
        summary,
        |summary| summary.deleted_data_file_count,
    );
    push_optional_u64_field(
        fields,
        "manifest_file_delete_file_entry_count",
        summary,
        |summary| summary.delete_file_entry_count,
    );
    push_optional_u64_field(
        fields,
        "manifest_file_position_delete_file_entry_count",
        summary,
        |summary| summary.position_delete_file_entry_count,
    );
    push_optional_u64_field(
        fields,
        "manifest_file_equality_delete_file_entry_count",
        summary,
        |summary| summary.equality_delete_file_entry_count,
    );
    push_optional_u64_field(
        fields,
        "manifest_file_deletion_vector_entry_count",
        summary,
        |summary| summary.deletion_vector_entry_count,
    );
    push_optional_u64_field(
        fields,
        "manifest_file_unknown_content_file_count",
        summary,
        |summary| summary.unknown_content_file_count,
    );
    push_optional_u64_field(
        fields,
        "manifest_file_unknown_status_entry_count",
        summary,
        |summary| summary.unknown_status_entry_count,
    );
    push_optional_u64_field(
        fields,
        "manifest_file_total_record_count",
        summary,
        |summary| summary.total_record_count,
    );
    push_optional_u64_field(
        fields,
        "manifest_file_total_file_size_bytes",
        summary,
        |summary| summary.total_file_size_bytes,
    );
    push_bool_field(
        fields,
        "data_file_split_planning_performed",
        summary.is_some(),
    );
    push_field(
        fields,
        "data_file_split_planning_rule",
        summary.map_or("none", |summary| summary.split_planning_rule),
    );
    push_optional_u64_field(
        fields,
        "planned_data_file_split_count",
        summary,
        |summary| summary.planned_data_file_count,
    );
    push_optional_u64_field(
        fields,
        "planned_data_file_split_bytes",
        summary,
        |summary| summary.planned_data_file_bytes,
    );
}

fn append_iceberg_metadata_evidence_fields(
    fields: &mut Vec<(String, String)>,
    report: &IcebergMetadataReadSmokeReport,
) {
    push_field(fields, "correctness_refs", report.correctness_refs);
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
    push_field(
        fields,
        "dependency_boundary_refs",
        report.dependency_boundary_refs,
    );
}

fn append_iceberg_metadata_boundary_fields(
    fields: &mut Vec<(String, String)>,
    report: &IcebergMetadataReadSmokeReport,
) {
    push_bool_field(
        fields,
        "local_metadata_json_read_performed",
        report.local_metadata_json_read_performed,
    );
    push_bool_field(
        fields,
        "table_metadata_read_performed",
        report.table_metadata_read_performed,
    );
    push_bool_field(
        fields,
        "snapshot_selection_performed",
        report.snapshot_selection_performed,
    );
    push_bool_field(
        fields,
        "time_travel_selection_performed",
        report.time_travel_selection_performed,
    );
    push_bool_field(fields, "catalog_io_performed", report.catalog_io_performed);
    push_bool_field(
        fields,
        "object_store_io_performed",
        report.object_store_io_performed,
    );
    push_bool_field(
        fields,
        "manifest_list_read_performed",
        report.manifest_list_read_performed,
    );
    push_bool_field(
        fields,
        "manifest_file_read_performed",
        report.manifest_file_read_performed,
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

fn append_iceberg_metadata_diagnostic_fields(
    fields: &mut Vec<(String, String)>,
    report: &IcebergMetadataReadSmokeReport,
) {
    push_bool_field(fields, "runtime_supported", report.runtime_supported());
    push_bool_field(fields, "claim_scoped", report.claim_scoped());
    push_bool_field(
        fields,
        "side_effect_free_except_local_metadata_read",
        report.side_effect_free_except_local_metadata_read(),
    );
    push_bool_field(
        fields,
        "side_effect_free_except_declared_local_table_reads",
        report.side_effect_free_except_declared_local_table_reads(),
    );
    push_count_field(
        fields,
        "unsupported_feature_count",
        report.unsupported_feature_order.len(),
    );
    push_field(
        fields,
        "unsupported_feature_order",
        &report.unsupported_feature_order_text(),
    );
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
    push_count_field(fields, "diagnostic_count", report.diagnostics.len());
}

fn optional_i64_text(value: Option<i64>) -> String {
    value.map_or_else(|| "none".to_string(), |number| number.to_string())
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
                "table_maintenance_execution_matrix_local_table_append_commit_rehearsal_smoke_present"
            ),
            "true"
        );
        assert_eq!(
            output_field(
                &fields,
                "table_maintenance_execution_matrix_row_table_metadata_write_status"
            ),
            "report_only_available"
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
