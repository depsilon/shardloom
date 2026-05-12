//! Vortex metadata and report-only planning CLI handlers.
//!
//! These handlers expose Vortex metadata, pruning, probe, and API-inventory
//! planning surfaces. They remain metadata/report-only and do not execute
//! tasks, read data beyond explicit metadata probe contracts, materialize
//! outputs, write data, invoke external engines, or allow fallback execution.

use std::process::ExitCode;

use shardloom_core::{CommandStatus, DatasetUri, OutputFormat, ShardLoomError};
use shardloom_vortex::{
    VortexAdapterCapabilityReport, VortexMetadataProbeReport,
    metadata_planning_is_side_effect_free, metadata_pruning_is_side_effect_free,
    plan_from_vortex_metadata_summary, plan_vortex_metadata_pruning, probe_vortex_metadata_only,
    summarize_vortex_metadata_probe,
};

use crate::cli_output::{emit, emit_error};

pub(crate) fn handle_vortex_metadata_plan(
    mut args: std::vec::IntoIter<String>,
    format: OutputFormat,
) -> ExitCode {
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

pub(crate) fn handle_vortex_pruning_plan(
    mut args: std::vec::IntoIter<String>,
    format: OutputFormat,
) -> ExitCode {
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
    emit(
        "vortex-pruning-plan",
        format,
        if report.has_errors() {
            CommandStatus::Error
        } else {
            CommandStatus::Success
        },
        "vortex metadata pruning plan".to_string(),
        report.to_human_text(),
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

pub(crate) fn handle_vortex_metadata_probe(
    mut args: std::vec::IntoIter<String>,
    format: OutputFormat,
) -> ExitCode {
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

pub(crate) fn handle_vortex_api_inventory(format: OutputFormat) -> ExitCode {
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
