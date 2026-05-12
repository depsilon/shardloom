//! Workflow, table, manifest, and stateful planning CLI handlers.
//!
//! These handlers emit report-only workflow planning surfaces. They do not read
//! datasets, probe catalogs, execute plans, write data, materialize outputs,
//! invoke external engines, or provide fallback execution.

use std::process::ExitCode;

use shardloom_core::{
    CapabilityCertificationReport, CatalogKind, CatalogRef, ChangeSet, CommandStatus,
    DatasetManifest, DatasetRef, DatasetUri, IncrementalPlanSkeleton, ManifestId, OutputFormat,
    OutputTarget, ShardLoomError, SnapshotId, SnapshotRef, WriteIntent,
    plan_catalog_metadata_integration_gate, plan_stateful_reuse,
    plan_stateful_reuse_promotion_gate,
};
use shardloom_plan::{
    ImportedPlanCapabilityGateReport, NativePlanDocument, PlanExportRequest, PlanId,
    PlanImportRequest, PlanInteropFormat, PlanPortabilityReport, ScanPlanSkeleton, ScanRequest,
};

use crate::{
    catalog_metadata_integration_gate_fields,
    cli_output::{emit, emit_error},
    cli_unknown_arg_error, emit_cdc_incremental_plan, emit_compaction_plan,
    emit_delete_tombstone_plan, emit_layout_health_plan, emit_partition_evolution_plan,
    emit_schema_evolution_plan, emit_schema_plan_skeleton, emit_table_compat_plan,
    emit_table_compatibility_aggregation, emit_table_intelligence_plan,
    imported_plan_capability_gate_fields, native_plan_export_document, parse_plan_interop_format,
    plan_portability_fields, push_count_field, push_field, stateful_reuse_fields,
    stateful_reuse_promotion_gate_fields,
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
