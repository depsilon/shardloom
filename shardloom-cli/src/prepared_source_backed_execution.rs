//! Prepared/source-backed encoded-read CLI handlers.
//!
//! These handlers route existing encoded-read probe/spike command behavior out
//! of `main.rs`. They preserve the current command contracts: probe-only paths
//! do not read, decode, materialize, write, spill, execute external effects, or
//! invoke fallback engines; spike paths keep the existing feature-gated local
//! encoded-read behavior and no-fallback evidence.

use std::process::ExitCode;

use shardloom_core::{
    CommandStatus, DatasetUri, OutputFormat, ShardLoomError, UniversalInputSource,
};
use shardloom_exec::{AdaptiveSizingPolicy, ByteSize, MemoryBudget};
use shardloom_vortex::{
    VortexEncodedReadBoundaryReport, VortexEncodedReadBoundaryRequest,
    VortexEncodedReadBoundarySignal, VortexEncodedReadFixtureRef,
    VortexEncodedReadMetadataProbeReport, VortexEncodedReadMetadataProbeRequest,
    VortexEncodedReadMetadataProbeSignal, VortexEncodedReadReadinessStatus,
    VortexLocalExecutionReport, VortexTaskSchedulingDecision, build_vortex_runtime_task_graph,
    evaluate_vortex_encoded_read_readiness, execute_vortex_encoded_read_contract,
    execute_vortex_encoded_read_spike, plan_native_vortex_universal_input,
    plan_vortex_encoded_read_boundary, plan_vortex_encoded_read_probe, plan_vortex_memory_safety,
    plan_vortex_read_from_universal_input, plan_vortex_scheduler_queue,
    probe_vortex_encoded_read_metadata, size_vortex_runtime_task_graph,
    vortex_encoded_read_executor_feature_enabled, vortex_encoded_read_public_api_boundary,
    vortex_encoded_read_spike_feature_enabled,
};

use crate::{
    cli_output::{emit, emit_error},
    cli_unknown_signal_error,
};

fn push_field(fields: &mut Vec<(String, String)>, key: &str, value: &str) {
    fields.push((key.to_string(), value.to_string()));
}

fn push_count_field(fields: &mut Vec<(String, String)>, key: &str, value: usize) {
    fields.push((key.to_string(), value.to_string()));
}

fn push_bool_field(fields: &mut Vec<(String, String)>, key: &str, value: bool) {
    fields.push((key.to_string(), value.to_string()));
}

pub(crate) fn parse_vortex_encoded_read_boundary_signals(
    signals_raw: &str,
) -> Result<Vec<VortexEncodedReadBoundarySignal>, ShardLoomError> {
    if signals_raw.trim().is_empty() {
        return Err(ShardLoomError::InvalidOperation(
            "encoded read boundary signals must not be empty".to_string(),
        ));
    }
    let mut signals = Vec::new();
    for token in signals_raw.split(',') {
        let token = token.trim();
        if token.is_empty() {
            return Err(ShardLoomError::InvalidOperation(
                "encoded read boundary signals must not contain empty tokens".to_string(),
            ));
        }
        let signal = match token {
            "upstream-open-options-available" => {
                VortexEncodedReadBoundarySignal::UpstreamOpenOptionsAvailable
            }
            "upstream-footer-available" => VortexEncodedReadBoundarySignal::UpstreamFooterAvailable,
            "upstream-metadata-surface-available" => {
                VortexEncodedReadBoundarySignal::UpstreamMetadataSurfaceAvailable
            }
            "upstream-scan-surface-deferred" => {
                VortexEncodedReadBoundarySignal::UpstreamScanSurfaceDeferred
            }
            "local-path-only" => VortexEncodedReadBoundarySignal::LocalPathOnly,
            "object-store-target" => VortexEncodedReadBoundarySignal::ObjectStoreTarget,
            "decode-risk" => VortexEncodedReadBoundarySignal::DecodeRisk,
            "materialization-risk" => VortexEncodedReadBoundarySignal::MaterializationRisk,
            "arrow-default-risk" => VortexEncodedReadBoundarySignal::ArrowDefaultRisk,
            "write-risk" => VortexEncodedReadBoundarySignal::WriteRisk,
            "feature-gate-enabled" => VortexEncodedReadBoundarySignal::FeatureGateEnabled,
            _ => {
                return Err(cli_unknown_signal_error(
                    "vortex-encoded-read-boundary",
                    "encoded-read-boundary",
                    token,
                ));
            }
        };
        if !signals.contains(&signal) {
            signals.push(signal);
        }
    }
    Ok(signals)
}

pub(crate) fn parse_vortex_encoded_read_metadata_probe_signals(
    signals_raw: &str,
) -> Result<Vec<VortexEncodedReadMetadataProbeSignal>, ShardLoomError> {
    if signals_raw.trim().is_empty() {
        return Err(ShardLoomError::InvalidOperation(
            "encoded read metadata probe signals must not be empty".to_string(),
        ));
    }
    let mut signals = Vec::new();
    for token in signals_raw.split(',') {
        let token = token.trim();
        if token.is_empty() {
            return Err(ShardLoomError::InvalidOperation(
                "encoded read metadata probe signals must not contain empty tokens".to_string(),
            ));
        }
        let signal = match token {
            "fixture-ready" => VortexEncodedReadMetadataProbeSignal::FixtureReady,
            "fixture-blocked" => VortexEncodedReadMetadataProbeSignal::FixtureBlocked,
            "fixture-ref-provided" => VortexEncodedReadMetadataProbeSignal::FixtureRefProvided,
            "local-path-only" => VortexEncodedReadMetadataProbeSignal::LocalPathOnly,
            "object-store-target" => VortexEncodedReadMetadataProbeSignal::ObjectStoreTarget,
            "scan-execution-risk" => VortexEncodedReadMetadataProbeSignal::ScanExecutionRisk,
            "decode-risk" => VortexEncodedReadMetadataProbeSignal::DecodeRisk,
            "materialization-risk" => VortexEncodedReadMetadataProbeSignal::MaterializationRisk,
            "arrow-default-risk" => VortexEncodedReadMetadataProbeSignal::ArrowDefaultRisk,
            "write-risk" => VortexEncodedReadMetadataProbeSignal::WriteRisk,
            "feature-gate-enabled" => VortexEncodedReadMetadataProbeSignal::FeatureGateEnabled,
            _ => {
                return Err(cli_unknown_signal_error(
                    "vortex-encoded-read-metadata-probe",
                    "encoded-read-metadata-probe",
                    token,
                ));
            }
        };
        if !signals.contains(&signal) {
            signals.push(signal);
        }
    }
    Ok(signals)
}

pub(crate) fn vortex_encoded_read_boundary_fields(
    report: &VortexEncodedReadBoundaryReport,
) -> Vec<(String, String)> {
    vec![
        (
            "fallback_execution_allowed".to_string(),
            "false".to_string(),
        ),
        (
            "mode".to_string(),
            "vortex_encoded_read_boundary".to_string(),
        ),
        (
            "upstream_open_options_available".to_string(),
            report.upstream_open_options_available().to_string(),
        ),
        (
            "upstream_footer_available".to_string(),
            report.upstream_footer_available().to_string(),
        ),
        (
            "upstream_metadata_surface_available".to_string(),
            report.upstream_metadata_surface_available().to_string(),
        ),
        (
            "upstream_scan_surface_deferred".to_string(),
            report.upstream_scan_surface_deferred().to_string(),
        ),
        (
            "local_path_only".to_string(),
            report.local_path_only().to_string(),
        ),
        (
            "object_store_target".to_string(),
            report.object_store_target().to_string(),
        ),
        ("decode_risk".to_string(), report.decode_risk().to_string()),
        (
            "materialization_risk".to_string(),
            report.materialization_risk().to_string(),
        ),
        (
            "arrow_default_risk".to_string(),
            report.arrow_default_risk().to_string(),
        ),
        ("write_risk".to_string(), report.write_risk().to_string()),
        ("data_read".to_string(), "false".to_string()),
        ("array_decoded".to_string(), "false".to_string()),
        ("values_materialized".to_string(), "false".to_string()),
        ("arrow_converted".to_string(), "false".to_string()),
        ("object_store_io".to_string(), "false".to_string()),
        ("data_written".to_string(), "false".to_string()),
        ("upstream_scan_called".to_string(), "false".to_string()),
        ("read_execution_allowed".to_string(), "false".to_string()),
        ("execution".to_string(), "not_performed".to_string()),
    ]
}

pub(crate) fn vortex_encoded_read_metadata_probe_fields(
    report: &VortexEncodedReadMetadataProbeReport,
) -> Vec<(String, String)> {
    vec![
        (
            "fallback_execution_allowed".to_string(),
            "false".to_string(),
        ),
        (
            "mode".to_string(),
            "vortex_encoded_read_metadata_probe".to_string(),
        ),
        (
            "fixture_ready".to_string(),
            report.fixture_ready().to_string(),
        ),
        (
            "fixture_ref_provided".to_string(),
            report.fixture_ref_provided().to_string(),
        ),
        (
            "local_path_only".to_string(),
            report.local_path_only().to_string(),
        ),
        (
            "object_store_target".to_string(),
            report.object_store_target().to_string(),
        ),
        (
            "scan_execution_risk".to_string(),
            report.scan_execution_risk().to_string(),
        ),
        ("decode_risk".to_string(), report.decode_risk().to_string()),
        (
            "materialization_risk".to_string(),
            report.materialization_risk().to_string(),
        ),
        (
            "arrow_default_risk".to_string(),
            report.arrow_default_risk().to_string(),
        ),
        ("write_risk".to_string(), report.write_risk().to_string()),
        (
            "metadata_opened".to_string(),
            report.metadata_opened().to_string(),
        ),
        (
            "footer_inspected".to_string(),
            report.footer_inspected().to_string(),
        ),
        (
            "encoded_data_read".to_string(),
            report.encoded_data_read().to_string(),
        ),
        ("row_read".to_string(), report.row_read().to_string()),
        (
            "array_decoded".to_string(),
            report.array_decoded().to_string(),
        ),
        (
            "values_materialized".to_string(),
            report.values_materialized().to_string(),
        ),
        (
            "arrow_converted".to_string(),
            report.arrow_converted().to_string(),
        ),
        (
            "object_store_io".to_string(),
            report.object_store_io().to_string(),
        ),
        (
            "data_written".to_string(),
            report.data_written().to_string(),
        ),
        (
            "upstream_scan_called".to_string(),
            report.upstream_scan_called().to_string(),
        ),
        (
            "metadata_probe_completed".to_string(),
            report.metadata_probe_completed().to_string(),
        ),
        ("execution".to_string(), "not_performed".to_string()),
    ]
}

pub(crate) fn vortex_encoded_read_spike_fields(
    memory_gb: u64,
    max_parallelism: usize,
    execute_local_count: bool,
    report: &shardloom_vortex::VortexEncodedReadExecutionReport,
    local_execution_report: Option<&VortexLocalExecutionReport>,
) -> Vec<(String, String)> {
    let mut fields = Vec::new();
    push_bool_field(
        &mut fields,
        "fallback_execution_allowed",
        report.fallback_execution_allowed,
    );
    push_field(&mut fields, "mode", "vortex_encoded_read_spike");
    push_bool_field(
        &mut fields,
        "feature_enabled",
        vortex_encoded_read_spike_feature_enabled(),
    );
    push_bool_field(
        &mut fields,
        "execute_local_count_requested",
        execute_local_count,
    );
    push_bool_field(
        &mut fields,
        "encoded_read_attempted",
        report.upstream_scan_called,
    );
    push_bool_field(&mut fields, "data_read", report.data_read);
    push_bool_field(&mut fields, "data_decoded", report.data_decoded);
    push_bool_field(&mut fields, "data_materialized", report.data_materialized);
    push_bool_field(&mut fields, "object_store_io", report.object_store_io);
    push_bool_field(&mut fields, "write_io", report.write_io);
    push_bool_field(&mut fields, "spill_io_performed", report.spill_io_performed);
    push_bool_field(
        &mut fields,
        "external_effects_executed",
        report.external_effects_executed,
    );
    push_field(&mut fields, "execution", report.status.as_str());
    fields.push(("memory_gb".to_string(), memory_gb.to_string()));
    push_count_field(&mut fields, "max_parallelism", max_parallelism);
    push_count_field(&mut fields, "arrays_read_count", report.arrays_read_count);
    fields.push(("rows_counted".to_string(), report.rows_counted.to_string()));
    fields.push((
        "count_result".to_string(),
        report
            .count_result
            .map_or_else(|| "unknown".to_string(), |count| count.to_string()),
    ));
    fields.push((
        "local_scan_target_uri".to_string(),
        report
            .local_scan_target_uri
            .as_ref()
            .map_or_else(|| "none".to_string(), |uri| uri.as_str().to_string()),
    ));
    fields.push((
        "local_scan_readiness_source_uri".to_string(),
        report
            .local_scan_readiness_source_uri
            .as_ref()
            .map_or_else(|| "none".to_string(), |uri| uri.as_str().to_string()),
    ));
    push_bool_field(
        &mut fields,
        "local_scan_source_uri_matches_target",
        report.local_scan_source_uri_matches_target,
    );
    if let Some(local) = local_execution_report {
        append_vortex_encoded_read_spike_local_execution_fields(&mut fields, local);
    }
    fields
}

pub(crate) fn append_vortex_encoded_read_spike_local_execution_fields(
    fields: &mut Vec<(String, String)>,
    local: &VortexLocalExecutionReport,
) {
    push_field(fields, "local_execution_status", local.status.as_str());
    push_field(fields, "local_execution_mode", local.mode.as_str());
    push_bool_field(
        fields,
        "local_execution_result_known",
        local.value.is_known(),
    );
    fields.push(("local_execution_value".to_string(), local.value.summary()));
    push_bool_field(
        fields,
        "local_execution_tasks_executed",
        local.tasks_executed,
    );
    push_bool_field(fields, "local_execution_data_read", local.data_read);
    push_bool_field(fields, "local_execution_data_decoded", local.data_decoded);
    push_bool_field(
        fields,
        "local_execution_data_materialized",
        local.data_materialized,
    );
    push_bool_field(
        fields,
        "local_execution_object_store_io",
        local.object_store_io,
    );
    push_bool_field(fields, "local_execution_write_io", local.write_io);
    push_bool_field(
        fields,
        "local_execution_spill_io_performed",
        local.spill_io_performed,
    );
    push_bool_field(
        fields,
        "local_execution_external_effects_executed",
        local.external_effects_executed,
    );
    push_bool_field(
        fields,
        "local_execution_fallback_execution_allowed",
        local.fallback_execution_allowed,
    );
}

pub(crate) fn parse_vortex_spike_args(
    command: &str,
    mut args: std::vec::IntoIter<String>,
) -> std::result::Result<(DatasetUri, u64, usize, bool), ExitCode> {
    let Some(dataset_uri) = args.next() else {
        eprintln!("usage: shardloom {command} <dataset_uri> <memory_gb> <max_parallelism>");
        return Err(ExitCode::from(2));
    };
    let Some(memory_gb_text) = args.next() else {
        eprintln!("usage: shardloom {command} <dataset_uri> <memory_gb> <max_parallelism>");
        return Err(ExitCode::from(2));
    };
    let Some(max_parallelism_text) = args.next() else {
        eprintln!("usage: shardloom {command} <dataset_uri> <memory_gb> <max_parallelism>");
        return Err(ExitCode::from(2));
    };
    let uri = DatasetUri::new(dataset_uri).map_err(|_| ExitCode::from(2))?;
    let memory_gb = memory_gb_text.parse().map_err(|_| ExitCode::from(2))?;
    let max_parallelism = max_parallelism_text
        .parse()
        .map_err(|_| ExitCode::from(2))?;
    let mut execute_local_count = false;
    for token in args {
        if token == "--execute-local-count" {
            execute_local_count = true;
        } else {
            eprintln!("unknown option for shardloom {command}: {token}");
            return Err(ExitCode::from(2));
        }
    }
    Ok((uri, memory_gb, max_parallelism, execute_local_count))
}

pub(crate) fn run_vortex_encoded_read_spike(
    uri: DatasetUri,
    memory_gb: u64,
    max_parallelism: usize,
    execute_local_count: bool,
) -> shardloom_core::Result<(
    u64,
    usize,
    bool,
    shardloom_vortex::VortexEncodedReadExecutionReport,
    Option<VortexLocalExecutionReport>,
)> {
    let source = shardloom_core::UniversalInputSource::from_dataset_uri(uri.clone())?;
    let input_plan = plan_native_vortex_universal_input(source)?;
    let read_report = plan_vortex_read_from_universal_input(input_plan)?;
    let runtime_report = build_vortex_runtime_task_graph(read_report)?;
    let sizing_report = size_vortex_runtime_task_graph(
        runtime_report,
        AdaptiveSizingPolicy::memory_limited(ByteSize::from_gib(memory_gb)),
    )?;
    let budget = MemoryBudget::from_gib(memory_gb)?;
    let memory_report = plan_vortex_memory_safety(sizing_report, budget)?;
    let mut scheduler_report = plan_vortex_scheduler_queue(memory_report, max_parallelism)?;
    if execute_local_count && scheduler_report.scheduled_task_count == 0 {
        scheduler_report
            .decisions
            .push(VortexTaskSchedulingDecision::schedule_now(
                None,
                "approved local encoded count execution",
            ));
        scheduler_report.recompute_counts();
    }
    let readiness_report = evaluate_vortex_encoded_read_readiness(scheduler_report)?;
    let (report, local_execution_report) = if execute_local_count {
        let (report, local_execution_report) =
            crate::vortex_primitive_execution::run_vortex_approved_local_encoded_count_from_readiness(uri, &readiness_report)?;
        (report, Some(local_execution_report))
    } else {
        let api = vortex_encoded_read_public_api_boundary();
        let probe = plan_vortex_encoded_read_probe(api.clone(), readiness_report.clone())?;
        (
            execute_vortex_encoded_read_spike(readiness_report, api, probe)?,
            None,
        )
    };
    Ok((
        memory_gb,
        max_parallelism,
        execute_local_count,
        report,
        local_execution_report,
    ))
}

pub(crate) fn handle_vortex_encoded_read_api(format: OutputFormat) -> ExitCode {
    let command = "vortex-encoded-read-api";
    let report = vortex_encoded_read_public_api_boundary();
    emit(
        command,
        format,
        if report.has_errors() {
            CommandStatus::Unsupported
        } else {
            CommandStatus::Success
        },
        "vortex encoded-read API boundary report".to_string(),
        report.to_human_text(),
        report.diagnostics.clone(),
        vec![
            (
                "fallback_execution_allowed".to_string(),
                "false".to_string(),
            ),
            ("mode".to_string(), "vortex_encoded_read_api".to_string()),
            ("contract_only".to_string(), "true".to_string()),
            ("execution_usable".to_string(), "false".to_string()),
            ("data_read".to_string(), "false".to_string()),
            ("data_decoded".to_string(), "false".to_string()),
            ("data_materialized".to_string(), "false".to_string()),
            ("object_store_io".to_string(), "false".to_string()),
            ("write_io".to_string(), "false".to_string()),
            ("spill_io_performed".to_string(), "false".to_string()),
            ("execution".to_string(), "not_performed".to_string()),
        ],
    );
    if report.has_errors() {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

pub(crate) fn handle_vortex_encoded_read_boundary(
    mut args: std::vec::IntoIter<String>,
    format: OutputFormat,
) -> ExitCode {
    let command = "vortex-encoded-read-boundary";
    let Some(target_uri) = args.next() else {
        eprintln!("usage: shardloom {command} <target_uri> <signals>");
        return ExitCode::from(2);
    };
    let Some(signals_raw) = args.next() else {
        eprintln!("usage: shardloom {command} <target_uri> <signals>");
        return ExitCode::from(2);
    };
    let target_uri = match DatasetUri::new(target_uri) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(
                command,
                format,
                "vortex encoded-read boundary failed",
                &error,
            );
        }
    };
    let signals = match parse_vortex_encoded_read_boundary_signals(&signals_raw) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(
                command,
                format,
                "vortex encoded-read boundary failed",
                &error,
            );
        }
    };
    let mut request = VortexEncodedReadBoundaryRequest::new(target_uri);
    for signal in signals {
        request.add_signal(signal);
    }
    let report = match plan_vortex_encoded_read_boundary(request) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(
                command,
                format,
                "vortex encoded-read boundary failed",
                &error,
            );
        }
    };
    emit(
        command,
        format,
        if report.has_errors() {
            CommandStatus::Unsupported
        } else {
            CommandStatus::Success
        },
        "vortex encoded-read boundary report".to_string(),
        report.to_human_text(),
        report.diagnostics.clone(),
        vortex_encoded_read_boundary_fields(&report),
    );
    if report.has_errors() {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

pub(crate) fn handle_vortex_encoded_read_metadata_probe(
    mut args: std::vec::IntoIter<String>,
    format: OutputFormat,
) -> ExitCode {
    let command = "vortex-encoded-read-metadata-probe";
    let Some(target_uri) = args.next() else {
        eprintln!("usage: shardloom {command} <target_uri> <fixture_ref> <signals>");
        return ExitCode::from(2);
    };
    let Some(fixture_ref_raw) = args.next() else {
        return emit_error(
            command,
            format,
            "vortex encoded read metadata probe failed",
            &crate::cli_missing_arg_error(command, "fixture_ref"),
        );
    };
    let Some(signals_raw) = args.next() else {
        eprintln!("usage: shardloom {command} <target_uri> <fixture_ref> <signals>");
        return ExitCode::from(2);
    };
    let target_uri = match DatasetUri::new(target_uri) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(
                command,
                format,
                "vortex encoded-read metadata probe failed",
                &error,
            );
        }
    };
    let fixture_ref = match VortexEncodedReadFixtureRef::new(fixture_ref_raw) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(
                command,
                format,
                "vortex encoded-read metadata probe failed",
                &error,
            );
        }
    };
    let signals = match parse_vortex_encoded_read_metadata_probe_signals(&signals_raw) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(
                command,
                format,
                "vortex encoded-read metadata probe failed",
                &error,
            );
        }
    };
    let mut request = VortexEncodedReadMetadataProbeRequest::new(target_uri, fixture_ref)
        .fixture_ref_provided(true);
    for signal in signals {
        request.add_signal(signal);
    }
    let report = match probe_vortex_encoded_read_metadata(request) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(
                command,
                format,
                "vortex encoded-read metadata probe failed",
                &error,
            );
        }
    };
    emit(
        command,
        format,
        if report.has_errors() {
            CommandStatus::Unsupported
        } else {
            CommandStatus::Success
        },
        "vortex encoded-read metadata probe report".to_string(),
        report.to_human_text(),
        report.diagnostics.clone(),
        vortex_encoded_read_metadata_probe_fields(&report),
    );
    if report.has_errors() {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

#[allow(clippy::too_many_lines)]
pub(crate) fn handle_vortex_encoded_read_readiness(
    mut args: std::vec::IntoIter<String>,
    format: OutputFormat,
) -> ExitCode {
    let command = "vortex-encoded-read-readiness";
    let Some(dataset_uri) = args.next() else {
        eprintln!("usage: shardloom {command} <dataset_uri> <memory_gb> <max_parallelism>");
        return ExitCode::from(2);
    };
    let Some(memory_gb_text) = args.next() else {
        eprintln!("usage: shardloom {command} <dataset_uri> <memory_gb> <max_parallelism>");
        return ExitCode::from(2);
    };
    let Some(max_parallelism_text) = args.next() else {
        eprintln!("usage: shardloom {command} <dataset_uri> <memory_gb> <max_parallelism>");
        return ExitCode::from(2);
    };
    let uri = match DatasetUri::new(dataset_uri) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(
                command,
                format,
                "vortex encoded-read readiness failed",
                &error,
            );
        }
    };
    let memory_gb: u64 = match memory_gb_text.parse() {
        Ok(v) => v,
        Err(_) => {
            return emit_error(
                command,
                format,
                "vortex encoded-read readiness failed",
                &ShardLoomError::InvalidOperation(
                    "memory_gb must be an unsigned integer".to_string(),
                ),
            );
        }
    };
    let max_parallelism: usize = match max_parallelism_text.parse() {
        Ok(v) => v,
        Err(_) => {
            return emit_error(
                command,
                format,
                "vortex encoded-read readiness failed",
                &ShardLoomError::InvalidOperation(
                    "max_parallelism must be an unsigned integer".to_string(),
                ),
            );
        }
    };
    let source = match UniversalInputSource::from_dataset_uri(uri) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(
                command,
                format,
                "vortex encoded-read readiness failed",
                &error,
            );
        }
    };
    let input_plan = match plan_native_vortex_universal_input(source) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(
                command,
                format,
                "vortex encoded-read readiness failed",
                &error,
            );
        }
    };
    if input_plan.has_errors() || !input_plan.source.is_native_vortex() {
        return ExitCode::from(1);
    }
    let read_report = match plan_vortex_read_from_universal_input(input_plan) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(
                command,
                format,
                "vortex encoded-read readiness failed",
                &error,
            );
        }
    };
    let runtime_report = match build_vortex_runtime_task_graph(read_report) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(
                command,
                format,
                "vortex encoded-read readiness failed",
                &error,
            );
        }
    };
    let sizing_report = match shardloom_vortex::size_vortex_runtime_task_graph(
        runtime_report,
        AdaptiveSizingPolicy::memory_limited(ByteSize::from_gib(memory_gb)),
    ) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(
                command,
                format,
                "vortex encoded-read readiness failed",
                &error,
            );
        }
    };
    let budget = match MemoryBudget::from_gib(memory_gb) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(
                command,
                format,
                "vortex encoded-read readiness failed",
                &error,
            );
        }
    };
    let memory_report = match shardloom_vortex::plan_vortex_memory_safety(sizing_report, budget) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(
                command,
                format,
                "vortex encoded-read readiness failed",
                &error,
            );
        }
    };
    let scheduler_report =
        match shardloom_vortex::plan_vortex_scheduler_queue(memory_report, max_parallelism) {
            Ok(v) => v,
            Err(error) => {
                return emit_error(
                    command,
                    format,
                    "vortex encoded-read readiness failed",
                    &error,
                );
            }
        };
    let report = match evaluate_vortex_encoded_read_readiness(scheduler_report) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(
                command,
                format,
                "vortex encoded-read readiness failed",
                &error,
            );
        }
    };
    let is_supported = !report.has_errors()
        && matches!(
            report.status,
            VortexEncodedReadReadinessStatus::ReadyForFutureEncodedRead
                | VortexEncodedReadReadinessStatus::ReadyForContract
                | VortexEncodedReadReadinessStatus::NoEncodedReadCandidates
        );
    emit(
        command,
        format,
        if is_supported {
            CommandStatus::Success
        } else {
            CommandStatus::Unsupported
        },
        "vortex encoded-read readiness report".to_string(),
        report.to_human_text(),
        report.diagnostics.clone(),
        vec![
            (
                "fallback_execution_allowed".to_string(),
                "false".to_string(),
            ),
            (
                "mode".to_string(),
                "vortex_encoded_read_readiness".to_string(),
            ),
            ("readiness_only".to_string(), "true".to_string()),
            ("encoded_read_executed".to_string(), "false".to_string()),
            ("data_read".to_string(), "false".to_string()),
            ("data_decoded".to_string(), "false".to_string()),
            ("data_materialized".to_string(), "false".to_string()),
            ("object_store_io".to_string(), "false".to_string()),
            ("write_io".to_string(), "false".to_string()),
            ("spill_io_performed".to_string(), "false".to_string()),
            ("external_effects_executed".to_string(), "false".to_string()),
            ("execution".to_string(), "not_performed".to_string()),
            ("memory_gb".to_string(), memory_gb.to_string()),
            ("max_parallelism".to_string(), max_parallelism.to_string()),
        ],
    );
    if is_supported {
        ExitCode::SUCCESS
    } else {
        ExitCode::from(1)
    }
}

#[allow(clippy::too_many_lines)]
pub(crate) fn handle_vortex_encoded_read_probe(
    mut args: std::vec::IntoIter<String>,
    format: OutputFormat,
) -> ExitCode {
    let command = "vortex-encoded-read-probe";
    let Some(dataset_uri) = args.next() else {
        eprintln!("usage: shardloom {command} <dataset_uri> <memory_gb> <max_parallelism>");
        return ExitCode::from(2);
    };
    let Some(memory_gb_text) = args.next() else {
        eprintln!("usage: shardloom {command} <dataset_uri> <memory_gb> <max_parallelism>");
        return ExitCode::from(2);
    };
    let Some(max_parallelism_text) = args.next() else {
        eprintln!("usage: shardloom {command} <dataset_uri> <memory_gb> <max_parallelism>");
        return ExitCode::from(2);
    };
    let uri = match DatasetUri::new(dataset_uri) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(command, format, "vortex encoded-read probe failed", &error);
        }
    };
    let memory_gb: u64 = match memory_gb_text.parse() {
        Ok(v) => v,
        Err(_) => {
            return emit_error(
                command,
                format,
                "vortex encoded-read probe failed",
                &ShardLoomError::InvalidOperation(
                    "memory_gb must be an unsigned integer".to_string(),
                ),
            );
        }
    };
    let max_parallelism: usize = match max_parallelism_text.parse() {
        Ok(v) => v,
        Err(_) => {
            return emit_error(
                command,
                format,
                "vortex encoded-read probe failed",
                &ShardLoomError::InvalidOperation(
                    "max_parallelism must be an unsigned integer".to_string(),
                ),
            );
        }
    };
    let source = match UniversalInputSource::from_dataset_uri(uri) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(command, format, "vortex encoded-read probe failed", &error);
        }
    };
    let input_plan = match plan_native_vortex_universal_input(source) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(command, format, "vortex encoded-read probe failed", &error);
        }
    };
    if input_plan.has_errors() {
        emit(
            command,
            format,
            CommandStatus::Unsupported,
            "vortex encoded-read probe failed".to_string(),
            input_plan.to_human_text(),
            input_plan.diagnostics.clone(),
            vec![],
        );
        return ExitCode::from(1);
    }
    let read_report = match plan_vortex_read_from_universal_input(input_plan) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(command, format, "vortex encoded-read probe failed", &error);
        }
    };
    if read_report.has_errors() {
        emit(
            command,
            format,
            CommandStatus::Unsupported,
            "vortex encoded-read probe failed".to_string(),
            read_report.to_human_text(),
            read_report.diagnostics.clone(),
            vec![],
        );
        return ExitCode::from(1);
    }
    let runtime_report = match build_vortex_runtime_task_graph(read_report) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(command, format, "vortex encoded-read probe failed", &error);
        }
    };
    if runtime_report.has_errors() {
        emit(
            command,
            format,
            CommandStatus::Unsupported,
            "vortex encoded-read probe failed".to_string(),
            runtime_report.to_human_text(),
            runtime_report.diagnostics.clone(),
            vec![],
        );
        return ExitCode::from(1);
    }
    let sizing_report = match shardloom_vortex::size_vortex_runtime_task_graph(
        runtime_report,
        AdaptiveSizingPolicy::memory_limited(ByteSize::from_gib(memory_gb)),
    ) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(command, format, "vortex encoded-read probe failed", &error);
        }
    };
    if sizing_report.has_errors() {
        emit(
            command,
            format,
            CommandStatus::Unsupported,
            "vortex encoded-read probe failed".to_string(),
            sizing_report.to_human_text(),
            sizing_report.diagnostics.clone(),
            vec![],
        );
        return ExitCode::from(1);
    }
    let budget = match MemoryBudget::from_gib(memory_gb) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(command, format, "vortex encoded-read probe failed", &error);
        }
    };
    let memory_report = match shardloom_vortex::plan_vortex_memory_safety(sizing_report, budget) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(command, format, "vortex encoded-read probe failed", &error);
        }
    };
    if memory_report.has_errors() {
        emit(
            command,
            format,
            CommandStatus::Unsupported,
            "vortex encoded-read probe failed".to_string(),
            memory_report.to_human_text(),
            memory_report.diagnostics.clone(),
            vec![],
        );
        return ExitCode::from(1);
    }
    let scheduler_report =
        match shardloom_vortex::plan_vortex_scheduler_queue(memory_report, max_parallelism) {
            Ok(v) => v,
            Err(error) => {
                return emit_error(command, format, "vortex encoded-read probe failed", &error);
            }
        };
    if scheduler_report.has_errors() {
        emit(
            command,
            format,
            CommandStatus::Unsupported,
            "vortex encoded-read probe failed".to_string(),
            scheduler_report.to_human_text(),
            scheduler_report.diagnostics.clone(),
            vec![],
        );
        return ExitCode::from(1);
    }
    let readiness = match evaluate_vortex_encoded_read_readiness(scheduler_report) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(command, format, "vortex encoded-read probe failed", &error);
        }
    };
    if readiness.has_errors() {
        emit(
            command,
            format,
            CommandStatus::Unsupported,
            "vortex encoded-read probe failed".to_string(),
            readiness.to_human_text(),
            readiness.diagnostics.clone(),
            vec![],
        );
        return ExitCode::from(1);
    }
    let api = vortex_encoded_read_public_api_boundary();
    let report = match plan_vortex_encoded_read_probe(api, readiness) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(command, format, "vortex encoded-read probe failed", &error);
        }
    };
    emit(
        command,
        format,
        if report.has_errors() {
            CommandStatus::Unsupported
        } else {
            CommandStatus::Success
        },
        "vortex encoded-read probe report".to_string(),
        report.to_human_text(),
        report.diagnostics.clone(),
        vec![
            (
                "fallback_execution_allowed".to_string(),
                "false".to_string(),
            ),
            ("mode".to_string(), "vortex_encoded_read_probe".to_string()),
            ("probe_only".to_string(), "true".to_string()),
            ("data_read".to_string(), "false".to_string()),
            ("data_decoded".to_string(), "false".to_string()),
            ("data_materialized".to_string(), "false".to_string()),
            ("object_store_io".to_string(), "false".to_string()),
            ("write_io".to_string(), "false".to_string()),
            ("spill_io_performed".to_string(), "false".to_string()),
            ("external_effects_executed".to_string(), "false".to_string()),
            ("execution".to_string(), "not_performed".to_string()),
            ("memory_gb".to_string(), memory_gb.to_string()),
            ("max_parallelism".to_string(), max_parallelism.to_string()),
        ],
    );
    if report.has_errors() {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

#[allow(clippy::too_many_lines)]
pub(crate) fn handle_vortex_encoded_read_execute(
    mut args: std::vec::IntoIter<String>,
    format: OutputFormat,
) -> ExitCode {
    let command = "vortex-encoded-read-execute";
    let Some(dataset_uri) = args.next() else {
        eprintln!("usage: shardloom {command} <dataset_uri> <memory_gb> <max_parallelism>");
        return ExitCode::from(2);
    };
    let Some(memory_gb_text) = args.next() else {
        eprintln!("usage: shardloom {command} <dataset_uri> <memory_gb> <max_parallelism>");
        return ExitCode::from(2);
    };
    let Some(max_parallelism_text) = args.next() else {
        eprintln!("usage: shardloom {command} <dataset_uri> <memory_gb> <max_parallelism>");
        return ExitCode::from(2);
    };
    let uri = match DatasetUri::new(dataset_uri) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(
                command,
                format,
                "vortex encoded-read execute failed",
                &error,
            );
        }
    };
    let memory_gb: u64 = match memory_gb_text.parse() {
        Ok(v) => v,
        Err(_) => {
            return emit_error(
                command,
                format,
                "vortex encoded-read execute failed",
                &ShardLoomError::InvalidOperation(
                    "memory_gb must be an unsigned integer".to_string(),
                ),
            );
        }
    };
    let max_parallelism: usize = match max_parallelism_text.parse() {
        Ok(v) => v,
        Err(_) => {
            return emit_error(
                command,
                format,
                "vortex encoded-read execute failed",
                &ShardLoomError::InvalidOperation(
                    "max_parallelism must be an unsigned integer".to_string(),
                ),
            );
        }
    };
    let source = match UniversalInputSource::from_dataset_uri(uri) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(
                command,
                format,
                "vortex encoded-read execute failed",
                &error,
            );
        }
    };
    let input_plan = match plan_native_vortex_universal_input(source) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(
                command,
                format,
                "vortex encoded-read execute failed",
                &error,
            );
        }
    };
    let read_report = match plan_vortex_read_from_universal_input(input_plan) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(
                command,
                format,
                "vortex encoded-read execute failed",
                &error,
            );
        }
    };
    let runtime_report = match build_vortex_runtime_task_graph(read_report) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(
                command,
                format,
                "vortex encoded-read execute failed",
                &error,
            );
        }
    };
    let sizing_report = match shardloom_vortex::size_vortex_runtime_task_graph(
        runtime_report,
        AdaptiveSizingPolicy::memory_limited(ByteSize::from_gib(memory_gb)),
    ) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(
                command,
                format,
                "vortex encoded-read execute failed",
                &error,
            );
        }
    };
    let budget = match MemoryBudget::from_gib(memory_gb) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(
                command,
                format,
                "vortex encoded-read execute failed",
                &error,
            );
        }
    };
    let memory_report = match shardloom_vortex::plan_vortex_memory_safety(sizing_report, budget) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(
                command,
                format,
                "vortex encoded-read execute failed",
                &error,
            );
        }
    };
    let scheduler_report =
        match shardloom_vortex::plan_vortex_scheduler_queue(memory_report, max_parallelism) {
            Ok(v) => v,
            Err(error) => {
                return emit_error(
                    command,
                    format,
                    "vortex encoded-read execute failed",
                    &error,
                );
            }
        };
    let readiness_report = match evaluate_vortex_encoded_read_readiness(scheduler_report) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(
                command,
                format,
                "vortex encoded-read execute failed",
                &error,
            );
        }
    };
    let report = match execute_vortex_encoded_read_contract(readiness_report) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(
                command,
                format,
                "vortex encoded-read execute failed",
                &error,
            );
        }
    };
    emit(
        command,
        format,
        if report.has_errors() {
            CommandStatus::Unsupported
        } else {
            CommandStatus::Success
        },
        "vortex encoded-read executor skeleton report".to_string(),
        report.to_human_text(),
        report.diagnostics.clone(),
        vec![
            (
                "fallback_execution_allowed".to_string(),
                "false".to_string(),
            ),
            (
                "mode".to_string(),
                "vortex_encoded_read_execute".to_string(),
            ),
            (
                "executor_feature_enabled".to_string(),
                vortex_encoded_read_executor_feature_enabled().to_string(),
            ),
            ("encoded_read_executed".to_string(), "false".to_string()),
            ("data_read".to_string(), "false".to_string()),
            ("data_decoded".to_string(), "false".to_string()),
            ("data_materialized".to_string(), "false".to_string()),
            ("object_store_io".to_string(), "false".to_string()),
            ("write_io".to_string(), "false".to_string()),
            ("spill_io_performed".to_string(), "false".to_string()),
            ("external_effects_executed".to_string(), "false".to_string()),
            ("execution".to_string(), "not_performed".to_string()),
            ("memory_gb".to_string(), memory_gb.to_string()),
            ("max_parallelism".to_string(), max_parallelism.to_string()),
        ],
    );
    if report.has_errors() {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

pub(crate) fn handle_vortex_encoded_read_spike(
    args: std::vec::IntoIter<String>,
    format: OutputFormat,
) -> ExitCode {
    let command = "vortex-encoded-read-spike";
    let parsed = match parse_vortex_spike_args(command, args) {
        Ok(v) => v,
        Err(code) => return code,
    };
    let (memory_gb, max_parallelism, execute_local_count, report, local_execution_report) =
        match run_vortex_encoded_read_spike(parsed.0, parsed.1, parsed.2, parsed.3) {
            Ok(v) => v,
            Err(error) => {
                return emit_error(command, format, "vortex encoded-read spike failed", &error);
            }
        };
    let local_execution_failed = local_execution_report
        .as_ref()
        .is_some_and(shardloom_vortex::VortexLocalExecutionReport::has_errors);
    let mut diagnostics = report.diagnostics.clone();
    if let Some(local) = &local_execution_report {
        diagnostics.extend(local.diagnostics.clone());
    }
    let human_text = local_execution_report.as_ref().map_or_else(
        || report.to_human_text(),
        |local| format!("{}\n\n{}", report.to_human_text(), local.to_human_text()),
    );
    let fields = vortex_encoded_read_spike_fields(
        memory_gb,
        max_parallelism,
        execute_local_count,
        &report,
        local_execution_report.as_ref(),
    );
    emit(
        command,
        format,
        if report.has_errors() || local_execution_failed {
            CommandStatus::Unsupported
        } else {
            CommandStatus::Success
        },
        "vortex encoded-read spike report".to_string(),
        human_text,
        diagnostics,
        fields,
    );
    if report.has_errors() || local_execution_failed {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}
