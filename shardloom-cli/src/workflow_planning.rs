//! Workflow, table, manifest, and stateful planning CLI handlers.
//!
//! These handlers emit report-only workflow planning surfaces. They do not read
//! datasets, probe catalogs, execute plans, write data, materialize outputs,
//! invoke external engines, or provide fallback execution.

use std::process::ExitCode;

use shardloom_core::{
    ChangeSet, CommandStatus, DatasetManifest, DatasetRef, DatasetUri, IncrementalPlanSkeleton,
    ManifestId, OutputFormat, ShardLoomError, SnapshotId, SnapshotRef,
    plan_catalog_metadata_integration_gate, plan_stateful_reuse,
    plan_stateful_reuse_promotion_gate,
};

use crate::{
    catalog_metadata_integration_gate_fields,
    cli_output::{emit, emit_error},
    cli_unknown_arg_error, emit_cdc_incremental_plan, emit_compaction_plan,
    emit_delete_tombstone_plan, emit_layout_health_plan, emit_partition_evolution_plan,
    emit_schema_evolution_plan, emit_schema_plan_skeleton, emit_table_compat_plan,
    emit_table_compatibility_aggregation, emit_table_intelligence_plan, stateful_reuse_fields,
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
