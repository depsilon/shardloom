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
    VortexEncodedReadBoundaryRequest, VortexEncodedReadFixtureRef,
    VortexEncodedReadMetadataProbeRequest, VortexEncodedReadReadinessStatus,
    build_vortex_runtime_task_graph, evaluate_vortex_encoded_read_readiness,
    execute_vortex_encoded_read_contract, plan_native_vortex_universal_input,
    plan_vortex_encoded_read_boundary, plan_vortex_encoded_read_probe,
    plan_vortex_read_from_universal_input, probe_vortex_encoded_read_metadata,
    vortex_encoded_read_executor_feature_enabled, vortex_encoded_read_public_api_boundary,
};

use crate::cli_output::{emit, emit_error};

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
    let signals = match crate::parse_vortex_encoded_read_boundary_signals(&signals_raw) {
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
        crate::vortex_encoded_read_boundary_fields(&report),
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
    let signals = match crate::parse_vortex_encoded_read_metadata_probe_signals(&signals_raw) {
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
        crate::vortex_encoded_read_metadata_probe_fields(&report),
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
    let parsed = match crate::parse_vortex_spike_args(command, args) {
        Ok(v) => v,
        Err(code) => return code,
    };
    let (memory_gb, max_parallelism, execute_local_count, report, local_execution_report) =
        match crate::run_vortex_encoded_read_spike(parsed.0, parsed.1, parsed.2, parsed.3) {
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
    let fields = crate::vortex_encoded_read_spike_fields(
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
