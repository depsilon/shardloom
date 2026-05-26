//! Input adapter and Vortex input/read planning CLI handlers.
//!
//! These handlers expose metadata-only input planning surfaces. They do not read
//! datasets, probe object stores, execute tasks, materialize outputs, invoke
//! external engines, or provide fallback execution.

use std::process::ExitCode;

use shardloom_core::{
    CommandStatus, DatasetFormat, DatasetUri, InputAdapterRegistrySnapshot, OutputFormat,
    ShardLoomError, UniversalInputSource,
};
use shardloom_plan::{UniversalInputPlanningReport, plan_universal_input_source};
use shardloom_vortex::{
    build_vortex_runtime_task_graph, plan_native_vortex_universal_input,
    plan_vortex_read_from_universal_input,
};

use crate::{
    cli_output::{emit, emit_error},
    cli_unknown_arg_error,
};

pub(crate) fn handle_input_adapters(format: OutputFormat) -> ExitCode {
    let snapshot = InputAdapterRegistrySnapshot::foundation();
    emit(
        "input-adapters",
        format,
        CommandStatus::Success,
        "input adapters snapshot".to_string(),
        snapshot.to_human_text(),
        snapshot.diagnostics.clone(),
        input_adapter_registry_fields(&snapshot),
    );
    ExitCode::SUCCESS
}

pub(crate) fn handle_input_plan(
    mut args: std::vec::IntoIter<String>,
    format: OutputFormat,
) -> ExitCode {
    let Some(dataset_uri) = args.next() else {
        eprintln!("usage: shardloom input-plan <dataset_uri> [--source-format <format>]");
        return ExitCode::from(2);
    };
    let declared_format = match parse_declared_source_format("input-plan", &mut args, format) {
        Ok(value) => value,
        Err(code) => return code,
    };
    let source = match universal_input_source_from_dataset_uri(
        "input-plan",
        format,
        "input plan failed",
        dataset_uri,
        declared_format,
    ) {
        Ok(source) => source,
        Err(code) => return code,
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
        input_plan_fields(&report),
    );
    if report.has_errors() {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

fn input_plan_fields(report: &UniversalInputPlanningReport) -> Vec<(String, String)> {
    vec![
        (
            "fallback_execution_allowed".to_string(),
            "false".to_string(),
        ),
        ("mode".to_string(), "input_plan".to_string()),
        (
            "source_kind".to_string(),
            report.input_report.source.source_kind.as_str().to_string(),
        ),
        (
            "adapter_kind".to_string(),
            report.input_report.source.adapter_kind.as_str().to_string(),
        ),
        (
            "dataset_format".to_string(),
            report
                .input_report
                .source
                .dataset_format
                .as_str()
                .to_string(),
        ),
        (
            "uri_scheme".to_string(),
            report
                .input_report
                .source
                .uri
                .as_ref()
                .map_or("none", |uri| uri.scheme().as_str())
                .to_string(),
        ),
        (
            "capability_status".to_string(),
            report.input_report.capability_status.as_str().to_string(),
        ),
        (
            "metadata_availability".to_string(),
            report
                .input_report
                .metadata_availability
                .as_str()
                .to_string(),
        ),
        (
            "fidelity".to_string(),
            report.input_report.fidelity.as_str().to_string(),
        ),
        (
            "materialization_risk".to_string(),
            report
                .input_report
                .materialization_risk
                .as_str()
                .to_string(),
        ),
        (
            "effect_level".to_string(),
            report.input_report.effect_level.as_str().to_string(),
        ),
        (
            "native_vortex".to_string(),
            report.input_report.source.is_native_vortex().to_string(),
        ),
        (
            "compatibility_structured".to_string(),
            report
                .input_report
                .source
                .source_kind
                .is_compatibility_structured()
                .to_string(),
        ),
        (
            "requires_credentials".to_string(),
            report
                .input_report
                .source
                .requires_credentials()
                .to_string(),
        ),
        (
            "side_effect_free".to_string(),
            report.input_report.is_side_effect_free().to_string(),
        ),
        ("data_read".to_string(), "false".to_string()),
        ("data_materialized".to_string(), "false".to_string()),
        ("object_store_io".to_string(), "false".to_string()),
        ("external_effects_executed".to_string(), "false".to_string()),
        ("write_io".to_string(), "false".to_string()),
        ("execution".to_string(), "not_performed".to_string()),
        ("plan_only".to_string(), "true".to_string()),
    ]
}

pub(crate) fn handle_vortex_input_plan(
    mut args: std::vec::IntoIter<String>,
    format: OutputFormat,
) -> ExitCode {
    let Some(dataset_uri) = args.next() else {
        eprintln!("usage: shardloom vortex-input-plan <dataset_uri>");
        return ExitCode::from(2);
    };
    let source = match universal_input_source_from_dataset_uri(
        "vortex-input-plan",
        format,
        "vortex input plan failed",
        dataset_uri,
        None,
    ) {
        Ok(source) => source,
        Err(code) => return code,
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
        vortex_input_plan_fields(report.source.is_native_vortex(), "vortex_input_plan", false),
    );
    if report.has_errors() {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

pub(crate) fn handle_vortex_read_plan(
    mut args: std::vec::IntoIter<String>,
    format: OutputFormat,
) -> ExitCode {
    let Some(dataset_uri) = args.next() else {
        eprintln!("usage: shardloom vortex-read-plan <dataset_uri>");
        return ExitCode::from(2);
    };
    let source = match universal_input_source_from_dataset_uri(
        "vortex-read-plan",
        format,
        "vortex read plan failed",
        dataset_uri,
        None,
    ) {
        Ok(source) => source,
        Err(code) => return code,
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
    let fields = vortex_input_plan_fields(
        input_plan.source.is_native_vortex(),
        "vortex_read_plan",
        true,
    );
    emit(
        "vortex-read-plan",
        format,
        command_status,
        "vortex read planning report".to_string(),
        report.to_human_text(),
        report.diagnostics.clone(),
        fields,
    );
    if report.has_errors() {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

pub(crate) fn handle_vortex_task_graph(
    mut args: std::vec::IntoIter<String>,
    format: OutputFormat,
) -> ExitCode {
    let Some(dataset_uri) = args.next() else {
        eprintln!("usage: shardloom vortex-task-graph <dataset_uri>");
        return ExitCode::from(2);
    };
    let source = match universal_input_source_from_dataset_uri(
        "vortex-task-graph",
        format,
        "vortex task graph plan failed",
        dataset_uri,
        None,
    ) {
        Ok(source) => source,
        Err(code) => return code,
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
        emit(
            "vortex-task-graph",
            format,
            CommandStatus::Unsupported,
            "vortex task graph plan failed: unsupported input".to_string(),
            input_plan.to_human_text(),
            input_plan.diagnostics.clone(),
            vortex_task_graph_fields(input_plan.source.is_native_vortex()),
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
        vortex_task_graph_fields(input_plan.source.is_native_vortex()),
    );
    if report.has_errors() {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

fn universal_input_source_from_dataset_uri(
    command: &str,
    format: OutputFormat,
    failure_summary: &str,
    dataset_uri: String,
    declared_format: Option<DatasetFormat>,
) -> Result<UniversalInputSource, ExitCode> {
    let uri = DatasetUri::new(dataset_uri)
        .map_err(|error| emit_error(command, format, failure_summary, &error))?;
    if let Some(format) = declared_format {
        UniversalInputSource::from_dataset_uri_with_format(uri, format)
    } else {
        UniversalInputSource::from_dataset_uri(uri)
    }
    .map_err(|error| emit_error(command, format, failure_summary, &error))
}

fn parse_declared_source_format(
    command: &str,
    args: &mut std::vec::IntoIter<String>,
    output_format: OutputFormat,
) -> Result<Option<DatasetFormat>, ExitCode> {
    let mut declared_format = None;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--source-format" => {
                let Some(value) = args.next() else {
                    return Err(emit_error(
                        command,
                        output_format,
                        "input plan failed",
                        &ShardLoomError::InvalidOperation(
                            "--source-format requires a value".to_string(),
                        ),
                    ));
                };
                declared_format = Some(parse_source_format(command, output_format, &value)?);
            }
            other => {
                return Err(emit_error(
                    command,
                    output_format,
                    "input plan failed",
                    &cli_unknown_arg_error(command, other),
                ));
            }
        }
    }
    Ok(declared_format)
}

fn parse_source_format(
    command: &str,
    output_format: OutputFormat,
    value: &str,
) -> Result<DatasetFormat, ExitCode> {
    match value.trim().to_ascii_lowercase().replace('_', "-").as_str() {
        "vortex" => Ok(DatasetFormat::Vortex),
        "parquet" => Ok(DatasetFormat::Parquet),
        "arrow" | "arrow-ipc" | "ipc" => Ok(DatasetFormat::ArrowIpc),
        "avro" => Ok(DatasetFormat::Avro),
        "orc" => Ok(DatasetFormat::Orc),
        "csv" => Ok(DatasetFormat::Csv),
        "json" | "jsonl" | "json-lines" | "ndjson" => Ok(DatasetFormat::JsonLines),
        "iceberg" | "iceberg-compatible" => Ok(DatasetFormat::IcebergCompatible),
        "delta" | "delta-compatible" => Ok(DatasetFormat::DeltaCompatible),
        other => Err(emit_error(
            command,
            output_format,
            "input plan failed",
            &ShardLoomError::InvalidOperation(format!("unsupported --source-format {other:?}")),
        )),
    }
}

fn input_adapter_registry_fields(snapshot: &InputAdapterRegistrySnapshot) -> Vec<(String, String)> {
    let mut fields = vec![
        (
            "fallback_execution_allowed".to_string(),
            "false".to_string(),
        ),
        ("mode".to_string(), "input_adapters".to_string()),
        (
            "adapter_count".to_string(),
            snapshot.adapter_count().to_string(),
        ),
        ("adapter_order".to_string(), snapshot.adapter_order()),
        (
            "common_structured_adapter_order".to_string(),
            snapshot.common_structured_adapter_order(),
        ),
        (
            "critical_structured_adapter_order".to_string(),
            snapshot.critical_structured_adapter_order(),
        ),
        (
            "lakehouse_adapter_order".to_string(),
            snapshot.lakehouse_adapter_order(),
        ),
        (
            "object_store_adapter_order".to_string(),
            snapshot.object_store_adapter_order(),
        ),
        (
            "catalog_adapter_order".to_string(),
            snapshot.catalog_adapter_order(),
        ),
        (
            "database_adapter_order".to_string(),
            snapshot.database_adapter_order(),
        ),
        (
            "effectful_adapter_order".to_string(),
            snapshot.effectful_adapter_order(),
        ),
        (
            "unstructured_adapter_order".to_string(),
            snapshot.unstructured_adapter_order(),
        ),
        (
            "supported_adapter_count".to_string(),
            snapshot.supported_count().to_string(),
        ),
        (
            "planned_adapter_count".to_string(),
            snapshot.planned_count().to_string(),
        ),
        (
            "explicit_enablement_adapter_count".to_string(),
            snapshot.explicitly_enabled_count().to_string(),
        ),
    ];
    for adapter in [
        "native_vortex",
        "parquet",
        "arrow_ipc",
        "csv",
        "jsonl",
        "avro",
        "orc",
        "iceberg_compatible",
        "delta_compatible",
        "local_filesystem",
        "s3_compatible",
        "gcs",
        "azure_blob_adls",
        "http_range",
        "local_catalog",
        "hive_compatible_catalog",
        "sqlite",
        "postgres_mysql",
        "jdbc_odbc",
        "snowflake",
        "bigquery",
        "databricks_sql",
        "unstructured_text",
    ] {
        fields.push((
            format!("{adapter}_status"),
            snapshot
                .adapter_status(adapter)
                .unwrap_or("missing")
                .to_string(),
        ));
    }
    fields.extend([
        ("write_io".to_string(), "false".to_string()),
        ("execution".to_string(), "not_performed".to_string()),
        ("plan_only".to_string(), "true".to_string()),
        ("external_effects_executed".to_string(), "false".to_string()),
    ]);
    fields
}

fn vortex_input_plan_fields(
    native_vortex_input: bool,
    mode: &str,
    include_data_executed: bool,
) -> Vec<(String, String)> {
    let mut fields = vec![
        (
            "fallback_execution_allowed".to_string(),
            "false".to_string(),
        ),
        ("mode".to_string(), mode.to_string()),
        (
            "native_vortex_input".to_string(),
            native_vortex_input.to_string(),
        ),
        ("metadata_only".to_string(), "true".to_string()),
        ("plan_only".to_string(), "true".to_string()),
    ];
    if include_data_executed {
        fields.push(("data_executed".to_string(), "false".to_string()));
    }
    fields.extend([
        ("data_read".to_string(), "false".to_string()),
        ("data_materialized".to_string(), "false".to_string()),
        ("object_store_io".to_string(), "false".to_string()),
        ("write_io".to_string(), "false".to_string()),
        ("external_effects_executed".to_string(), "false".to_string()),
        ("execution".to_string(), "not_performed".to_string()),
    ]);
    fields
}

fn vortex_task_graph_fields(native_vortex_input: bool) -> Vec<(String, String)> {
    vec![
        (
            "fallback_execution_allowed".to_string(),
            "false".to_string(),
        ),
        ("mode".to_string(), "vortex_task_graph".to_string()),
        (
            "native_vortex_input".to_string(),
            native_vortex_input.to_string(),
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
    ]
}
