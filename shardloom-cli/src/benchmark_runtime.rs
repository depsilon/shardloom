//! Executable local benchmark command handlers.
//!
//! These handlers remain local benchmark harness surfaces. External engines are
//! comparison-only baselines and must not become fallback execution paths.

use std::{
    path::PathBuf,
    process::ExitCode,
    time::{Duration, Instant},
};

use shardloom_core::{
    BaselineEngine, BenchmarkComparisonReport, BenchmarkEvidenceState, BenchmarkMetric,
    BenchmarkPlan, BenchmarkResult, BenchmarkScenario, CommandStatus, CorrectnessValidationMode,
    DatasetUri, Diagnostic, DiagnosticCode, ExpectedOutcome, MetricValue, OutputFormat,
    ShardLoomError, ShardLoomExecutionMode, ShardLoomExecutionModeSelectionReport,
    ShardLoomExecutionModeSelectionRequest, WorkloadClass,
};
use shardloom_vortex::{TraditionalRuntimeEvidenceLevel, VortexLocalExecutionReport};

use crate::{
    cli_output::{emit, emit_error},
    cli_time::{duration_micros, micros_to_millis, saturating_u128_to_u64},
    cli_unknown_arg_error,
    vortex_primitive_execution::local_encoded_count_correctness_fixture_for_target,
};

#[allow(clippy::too_many_lines)]
pub(crate) fn handle_traditional_analytics_run(
    mut args: std::vec::IntoIter<String>,
    format: OutputFormat,
) -> ExitCode {
    let Some(scenario_text) = args.next() else {
        eprintln!(
            "usage: shardloom traditional-analytics-run <scenario> <fact_input> <dim_input> [--workspace <dir>] [--input-format auto|csv|jsonl|parquet|arrow-ipc|avro|orc] [--cdc-delta <csv>] [--compat-output-format csv|jsonl|parquet|arrow-ipc|avro|orc] [--verify-native-replay] [--write-result-vortex] [--execution-mode auto|compatibility_import_certified|direct_compatibility_transient] [--memory-gb <cap>] [--max-parallelism <cap>]"
        );
        return ExitCode::from(2);
    };
    let Some(fact_csv) = args.next() else {
        eprintln!(
            "usage: shardloom traditional-analytics-run <scenario> <fact_input> <dim_input> [--workspace <dir>] [--input-format auto|csv|jsonl|parquet|arrow-ipc|avro|orc] [--cdc-delta <csv>] [--compat-output-format csv|jsonl|parquet|arrow-ipc|avro|orc] [--verify-native-replay] [--write-result-vortex] [--execution-mode auto|compatibility_import_certified|direct_compatibility_transient] [--memory-gb <cap>] [--max-parallelism <cap>]"
        );
        return ExitCode::from(2);
    };
    let Some(dim_csv) = args.next() else {
        eprintln!(
            "usage: shardloom traditional-analytics-run <scenario> <fact_input> <dim_input> [--workspace <dir>] [--input-format auto|csv|jsonl|parquet|arrow-ipc|avro|orc] [--cdc-delta <csv>] [--compat-output-format csv|jsonl|parquet|arrow-ipc|avro|orc] [--verify-native-replay] [--write-result-vortex] [--execution-mode auto|compatibility_import_certified|direct_compatibility_transient] [--memory-gb <cap>] [--max-parallelism <cap>]"
        );
        return ExitCode::from(2);
    };
    let mut workspace_dir: Option<PathBuf> = None;
    let mut input_format: Option<shardloom_vortex::TraditionalAnalyticsInputFormat> = None;
    let mut cdc_delta_csv: Option<PathBuf> = None;
    let mut compatibility_output_format: Option<shardloom_vortex::TraditionalAnalyticsInputFormat> =
        None;
    let mut verify_native_vortex_replay = false;
    let mut write_result_vortex = false;
    let mut memory_gb: Option<u32> = None;
    let mut max_parallelism: Option<usize> = None;
    let mut requested_execution_mode = ShardLoomExecutionMode::CompatibilityImportCertified;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--workspace" => {
                let Some(value) = args.next() else {
                    eprintln!("usage: shardloom traditional-analytics-run ... --workspace <dir>");
                    return ExitCode::from(2);
                };
                workspace_dir = Some(PathBuf::from(value));
            }
            "--input-format" => {
                let Some(value) = args.next() else {
                    eprintln!(
                        "usage: shardloom traditional-analytics-run ... --input-format auto|csv|jsonl|parquet|arrow-ipc|avro|orc"
                    );
                    return ExitCode::from(2);
                };
                if value != "auto" {
                    match shardloom_vortex::TraditionalAnalyticsInputFormat::parse(&value) {
                        Ok(parsed) => input_format = Some(parsed),
                        Err(error) => {
                            return emit_error(
                                "traditional-analytics-run",
                                format,
                                "traditional analytics run failed",
                                &error,
                            );
                        }
                    }
                }
            }
            "--cdc-delta" => {
                let Some(value) = args.next() else {
                    eprintln!("usage: shardloom traditional-analytics-run ... --cdc-delta <csv>");
                    return ExitCode::from(2);
                };
                cdc_delta_csv = Some(PathBuf::from(value));
            }
            "--compat-output-format" => {
                let Some(value) = args.next() else {
                    eprintln!(
                        "usage: shardloom traditional-analytics-run ... --compat-output-format csv|jsonl|parquet|arrow-ipc|avro|orc"
                    );
                    return ExitCode::from(2);
                };
                match shardloom_vortex::TraditionalAnalyticsInputFormat::parse(&value) {
                    Ok(parsed) => compatibility_output_format = Some(parsed),
                    Err(error) => {
                        return emit_error(
                            "traditional-analytics-run",
                            format,
                            "traditional analytics run failed",
                            &error,
                        );
                    }
                }
            }
            "--verify-native-replay" => {
                verify_native_vortex_replay = true;
            }
            "--write-result-vortex" => {
                write_result_vortex = true;
            }
            "--execution-mode" => {
                let Some(value) = args.next() else {
                    eprintln!(
                        "usage: shardloom traditional-analytics-run ... --execution-mode auto|compatibility_import_certified|direct_compatibility_transient"
                    );
                    return ExitCode::from(2);
                };
                let parsed_mode = match ShardLoomExecutionMode::parse(&value) {
                    Ok(
                        mode @ (ShardLoomExecutionMode::Auto
                        | ShardLoomExecutionMode::CompatibilityImportCertified
                        | ShardLoomExecutionMode::DirectCompatibilityTransient),
                    ) => mode,
                    Ok(mode) => {
                        return emit_error(
                            "traditional-analytics-run",
                            format,
                            "traditional analytics run failed",
                            &ShardLoomError::InvalidOperation(format!(
                                "traditional-analytics-run does not support execution mode {}; use traditional-analytics-vortex-run for prepared/native Vortex inputs; fallback execution was not attempted",
                                mode.as_str()
                            )),
                        );
                    }
                    Err(error) => {
                        return emit_error(
                            "traditional-analytics-run",
                            format,
                            "traditional analytics run failed",
                            &error,
                        );
                    }
                };
                requested_execution_mode = parsed_mode;
            }
            "--memory-gb" => {
                let Some(value) = args.next() else {
                    eprintln!("usage: shardloom traditional-analytics-run ... --memory-gb <cap>");
                    return ExitCode::from(2);
                };
                match value.parse::<u32>() {
                    Ok(parsed) if parsed > 0 => memory_gb = Some(parsed),
                    _ => {
                        return emit_error(
                            "traditional-analytics-run",
                            format,
                            "traditional analytics run failed",
                            &ShardLoomError::InvalidOperation(format!(
                                "traditional-analytics-run invalid --memory-gb value: {value}"
                            )),
                        );
                    }
                }
            }
            "--max-parallelism" => {
                let Some(value) = args.next() else {
                    eprintln!(
                        "usage: shardloom traditional-analytics-run ... --max-parallelism <cap>"
                    );
                    return ExitCode::from(2);
                };
                match value.parse::<usize>() {
                    Ok(parsed) if parsed > 0 => max_parallelism = Some(parsed),
                    _ => {
                        return emit_error(
                            "traditional-analytics-run",
                            format,
                            "traditional analytics run failed",
                            &ShardLoomError::InvalidOperation(format!(
                                "traditional-analytics-run invalid --max-parallelism value: {value}"
                            )),
                        );
                    }
                }
            }
            extra => {
                return emit_error(
                    "traditional-analytics-run",
                    format,
                    "traditional analytics run failed",
                    &cli_unknown_arg_error("traditional-analytics-run", extra),
                );
            }
        }
    }

    let scenario = match shardloom_vortex::TraditionalAnalyticsScenario::parse(&scenario_text) {
        Ok(scenario) => scenario,
        Err(error) => {
            return emit_error(
                "traditional-analytics-run",
                format,
                "traditional analytics run failed",
                &error,
            );
        }
    };
    let workspace_dir = workspace_dir.unwrap_or_else(|| {
        std::env::temp_dir().join(format!(
            "shardloom-traditional-analytics-{}",
            std::process::id()
        ))
    });
    let fact_path = PathBuf::from(fact_csv);
    let dim_path = PathBuf::from(dim_csv);
    let input_format = input_format.unwrap_or_else(|| {
        shardloom_vortex::TraditionalAnalyticsInputFormat::infer_from_paths(&fact_path, &dim_path)
    });
    let direct_transient_unsupported =
        direct_transient_unsupported_reason(DirectTransientAdmissionFacts {
            scenario,
            input_format,
            cdc_delta_requested: cdc_delta_csv.is_some(),
            compatibility_output_requested: compatibility_output_format.is_some(),
            verify_native_vortex_replay,
            write_result_vortex,
        });

    if requested_execution_mode == ShardLoomExecutionMode::DirectCompatibilityTransient {
        if let Some(reason) = direct_transient_unsupported {
            return emit_direct_compatibility_transient_unsupported(
                format,
                input_format,
                verify_native_vortex_replay,
                write_result_vortex,
                reason,
            );
        }
    }
    let request = shardloom_vortex::TraditionalAnalyticsRequest::new(
        scenario,
        fact_path,
        dim_path,
        workspace_dir,
    )
    .with_input_format(input_format)
    .with_cdc_delta_csv(cdc_delta_csv)
    .with_compatibility_output_format(compatibility_output_format)
    .with_native_vortex_replay_verification(verify_native_vortex_replay)
    .with_result_vortex_write(write_result_vortex)
    .with_requested_execution_mode(requested_execution_mode)
    .with_resource_policy(
        shardloom_vortex::TraditionalAnalyticsResourcePolicy::from_hints(
            memory_gb,
            max_parallelism,
        ),
    );
    if requested_execution_mode == ShardLoomExecutionMode::DirectCompatibilityTransient {
        let report = match shardloom_vortex::run_traditional_direct_transient_csv_smoke(request) {
            Ok(report) => report,
            Err(error) => {
                return emit_error(
                    "traditional-analytics-run",
                    format,
                    "traditional analytics direct transient smoke failed",
                    &error,
                );
            }
        };
        emit(
            "traditional-analytics-run",
            format,
            CommandStatus::Success,
            "direct compatibility transient CSV smoke".to_string(),
            report.to_human_text(),
            report.diagnostics.clone(),
            report.fields(),
        );
        return ExitCode::SUCCESS;
    }
    let report = match shardloom_vortex::run_traditional_analytics_benchmark(request) {
        Ok(report) => report,
        Err(error) => {
            return emit_error(
                "traditional-analytics-run",
                format,
                "traditional analytics run failed",
                &error,
            );
        }
    };
    emit(
        "traditional-analytics-run",
        format,
        CommandStatus::Success,
        "traditional analytics universal I/O smoke".to_string(),
        report.to_human_text(),
        report.diagnostics.clone(),
        report.fields(),
    );
    ExitCode::SUCCESS
}

#[derive(Debug, Clone, Copy)]
#[allow(clippy::struct_excessive_bools)]
struct DirectTransientAdmissionFacts {
    scenario: shardloom_vortex::TraditionalAnalyticsScenario,
    input_format: shardloom_vortex::TraditionalAnalyticsInputFormat,
    cdc_delta_requested: bool,
    compatibility_output_requested: bool,
    verify_native_vortex_replay: bool,
    write_result_vortex: bool,
}

fn direct_transient_unsupported_reason(
    facts: DirectTransientAdmissionFacts,
) -> Option<&'static str> {
    if facts.input_format != shardloom_vortex::TraditionalAnalyticsInputFormat::Csv {
        return Some("direct transient smoke currently supports local CSV input only");
    }
    if !matches!(
        facts.scenario,
        shardloom_vortex::TraditionalAnalyticsScenario::SelectiveFilter
            | shardloom_vortex::TraditionalAnalyticsScenario::FilterProjectionLimit
    ) {
        return Some(
            "direct transient smoke currently supports only selective filter or filter + projection + limit",
        );
    }
    if facts.cdc_delta_requested {
        return Some("direct transient smoke does not support CDC delta input");
    }
    if facts.compatibility_output_requested {
        return Some("direct transient smoke does not support compatibility output writers");
    }
    if facts.verify_native_vortex_replay || facts.write_result_vortex {
        return Some("direct transient smoke does not support Vortex replay or result-sink writes");
    }
    None
}

fn emit_direct_compatibility_transient_unsupported(
    format: OutputFormat,
    input_format: shardloom_vortex::TraditionalAnalyticsInputFormat,
    certification_requested: bool,
    result_sink_requested: bool,
    unsupported_detail: &str,
) -> ExitCode {
    let report = ShardLoomExecutionModeSelectionReport::from_request(
        ShardLoomExecutionModeSelectionRequest::new(
            ShardLoomExecutionMode::DirectCompatibilityTransient,
        )
        .with_source_format(input_format.as_str())
        .with_workload_constitution("local_vortex_analytics_v1")
        .with_compatibility_input(true)
        .with_certification_requested(certification_requested)
        .with_result_sink_requested(result_sink_requested),
    );
    let mut fields = report.fields();
    fields.extend([
        (
            "admission_surface".to_string(),
            "traditional_analytics_direct_transient".to_string(),
        ),
        (
            "unsupported_detail".to_string(),
            unsupported_detail.to_string(),
        ),
        ("runtime_execution".to_string(), "false".to_string()),
        ("query_execution".to_string(), "false".to_string()),
        ("data_read".to_string(), "false".to_string()),
        ("data_materialized".to_string(), "false".to_string()),
        ("write_io".to_string(), "false".to_string()),
        ("no_runtime".to_string(), "true".to_string()),
        ("no_fallback".to_string(), "true".to_string()),
        ("no_effects".to_string(), "true".to_string()),
    ]);
    emit(
        "traditional-analytics-run",
        format,
        CommandStatus::Unsupported,
        "direct compatibility transient admission".to_string(),
        format!("{unsupported_detail}; no runtime execution was attempted"),
        vec![Diagnostic::unsupported(
            DiagnosticCode::NotImplemented,
            "direct_compatibility_transient",
            format!("{unsupported_detail}; no runtime execution was attempted"),
            Some(
                "Use compatibility_import_certified for certified ingest/stage evidence, or restrict direct transient mode to the supported local CSV selective-filter or filter + projection + limit smoke paths."
                    .to_string(),
            ),
        )],
        fields,
    );
    ExitCode::from(1)
}

#[allow(clippy::too_many_lines)]
pub(crate) fn handle_traditional_analytics_vortex_run(
    mut args: std::vec::IntoIter<String>,
    format: OutputFormat,
) -> ExitCode {
    let Some(scenario_text) = args.next() else {
        eprintln!(
            "usage: shardloom traditional-analytics-vortex-run <scenario> <fact_vortex> <dim_vortex> [--cdc-delta-vortex <path>] [--workspace <dir>] [--write-result-vortex] [--execution-mode auto|native_vortex|prepared_vortex]"
        );
        return ExitCode::from(2);
    };
    let Some(fact_vortex) = args.next() else {
        eprintln!(
            "usage: shardloom traditional-analytics-vortex-run <scenario> <fact_vortex> <dim_vortex> [--cdc-delta-vortex <path>] [--workspace <dir>] [--write-result-vortex] [--execution-mode auto|native_vortex|prepared_vortex]"
        );
        return ExitCode::from(2);
    };
    let Some(dim_vortex) = args.next() else {
        eprintln!(
            "usage: shardloom traditional-analytics-vortex-run <scenario> <fact_vortex> <dim_vortex> [--cdc-delta-vortex <path>] [--workspace <dir>] [--write-result-vortex] [--execution-mode auto|native_vortex|prepared_vortex]"
        );
        return ExitCode::from(2);
    };
    let mut requested_execution_mode = ShardLoomExecutionMode::NativeVortex;
    let mut workspace_dir: Option<PathBuf> = None;
    let mut cdc_delta_vortex: Option<PathBuf> = None;
    let mut write_result_vortex = false;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--cdc-delta-vortex" => {
                let Some(path) = args.next() else {
                    eprintln!(
                        "usage: shardloom traditional-analytics-vortex-run ... --cdc-delta-vortex <path>"
                    );
                    return ExitCode::from(2);
                };
                cdc_delta_vortex = Some(PathBuf::from(path));
            }
            "--workspace" => {
                let Some(path) = args.next() else {
                    eprintln!(
                        "usage: shardloom traditional-analytics-vortex-run ... --workspace <dir>"
                    );
                    return ExitCode::from(2);
                };
                workspace_dir = Some(PathBuf::from(path));
            }
            "--write-result-vortex" => {
                write_result_vortex = true;
            }
            "--execution-mode" => {
                let Some(value) = args.next() else {
                    eprintln!(
                        "usage: shardloom traditional-analytics-vortex-run ... [--cdc-delta-vortex <path>] [--workspace <dir>] [--write-result-vortex] [--execution-mode auto|native_vortex|prepared_vortex]"
                    );
                    return ExitCode::from(2);
                };
                match ShardLoomExecutionMode::parse(&value) {
                    Ok(
                        ShardLoomExecutionMode::Auto
                        | ShardLoomExecutionMode::NativeVortex
                        | ShardLoomExecutionMode::PreparedVortex,
                    ) => {
                        requested_execution_mode = ShardLoomExecutionMode::parse(&value)
                            .expect("execution mode was already parsed");
                    }
                    Ok(mode) => {
                        return emit_error(
                            "traditional-analytics-vortex-run",
                            format,
                            "traditional analytics native Vortex run failed",
                            &ShardLoomError::InvalidOperation(format!(
                                "traditional-analytics-vortex-run does not support execution mode {}; fallback execution was not attempted",
                                mode.as_str()
                            )),
                        );
                    }
                    Err(error) => {
                        return emit_error(
                            "traditional-analytics-vortex-run",
                            format,
                            "traditional analytics native Vortex run failed",
                            &error,
                        );
                    }
                }
            }
            extra => {
                return emit_error(
                    "traditional-analytics-vortex-run",
                    format,
                    "traditional analytics native Vortex run failed",
                    &cli_unknown_arg_error("traditional-analytics-vortex-run", extra),
                );
            }
        }
    }

    let scenario = match shardloom_vortex::TraditionalAnalyticsScenario::parse(&scenario_text) {
        Ok(scenario) => scenario,
        Err(error) => {
            return emit_error(
                "traditional-analytics-vortex-run",
                format,
                "traditional analytics native Vortex run failed",
                &error,
            );
        }
    };
    let request = shardloom_vortex::TraditionalAnalyticsVortexRequest::new(
        scenario,
        PathBuf::from(fact_vortex),
        PathBuf::from(dim_vortex),
    )
    .with_cdc_delta_vortex(cdc_delta_vortex)
    .with_requested_execution_mode(requested_execution_mode)
    .with_result_workspace_dir(workspace_dir)
    .with_result_vortex_write(write_result_vortex);
    let report = match shardloom_vortex::run_traditional_analytics_vortex_benchmark(request) {
        Ok(report) => report,
        Err(error) => {
            return emit_error(
                "traditional-analytics-vortex-run",
                format,
                "traditional analytics native Vortex run failed",
                &error,
            );
        }
    };
    emit(
        "traditional-analytics-vortex-run",
        format,
        CommandStatus::Success,
        "traditional analytics native Vortex smoke".to_string(),
        report.to_human_text(),
        report.diagnostics.clone(),
        report.fields(),
    );
    ExitCode::SUCCESS
}

#[allow(clippy::too_many_lines)]
pub(crate) fn handle_traditional_analytics_vortex_batch_run(
    mut args: std::vec::IntoIter<String>,
    format: OutputFormat,
) -> ExitCode {
    const USAGE: &str = "usage: shardloom traditional-analytics-vortex-batch-run <scenario_csv> <fact_vortex> <dim_vortex> [--cdc-delta-vortex <path>] [--workspace <dir>] [--write-result-vortex] [--execution-mode auto|native_vortex|prepared_vortex] [--evidence-level minimal_runtime|certified|full_replay]";
    let Some(scenario_list) = args.next() else {
        eprintln!("{USAGE}");
        return ExitCode::from(2);
    };
    let Some(fact_vortex) = args.next() else {
        eprintln!("{USAGE}");
        return ExitCode::from(2);
    };
    let Some(dim_vortex) = args.next() else {
        eprintln!("{USAGE}");
        return ExitCode::from(2);
    };
    let mut requested_execution_mode = ShardLoomExecutionMode::NativeVortex;
    let mut requested_evidence_level: Option<TraditionalRuntimeEvidenceLevel> = None;
    let mut workspace_dir: Option<PathBuf> = None;
    let mut cdc_delta_vortex: Option<PathBuf> = None;
    let mut write_result_vortex = false;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--cdc-delta-vortex" => {
                let Some(path) = args.next() else {
                    eprintln!(
                        "usage: shardloom traditional-analytics-vortex-batch-run ... --cdc-delta-vortex <path>"
                    );
                    return ExitCode::from(2);
                };
                cdc_delta_vortex = Some(PathBuf::from(path));
            }
            "--workspace" => {
                let Some(path) = args.next() else {
                    eprintln!(
                        "usage: shardloom traditional-analytics-vortex-batch-run ... --workspace <dir>"
                    );
                    return ExitCode::from(2);
                };
                workspace_dir = Some(PathBuf::from(path));
            }
            "--write-result-vortex" => {
                write_result_vortex = true;
            }
            "--execution-mode" => {
                let Some(value) = args.next() else {
                    eprintln!("{USAGE}");
                    return ExitCode::from(2);
                };
                match ShardLoomExecutionMode::parse(&value) {
                    Ok(
                        ShardLoomExecutionMode::Auto
                        | ShardLoomExecutionMode::NativeVortex
                        | ShardLoomExecutionMode::PreparedVortex,
                    ) => {
                        requested_execution_mode = ShardLoomExecutionMode::parse(&value)
                            .expect("execution mode was already parsed");
                    }
                    Ok(mode) => {
                        return emit_error(
                            "traditional-analytics-vortex-batch-run",
                            format,
                            "traditional analytics native Vortex batch run failed",
                            &ShardLoomError::InvalidOperation(format!(
                                "traditional-analytics-vortex-batch-run does not support execution mode {}; fallback execution was not attempted",
                                mode.as_str()
                            )),
                        );
                    }
                    Err(error) => {
                        return emit_error(
                            "traditional-analytics-vortex-batch-run",
                            format,
                            "traditional analytics native Vortex batch run failed",
                            &error,
                        );
                    }
                }
            }
            "--evidence-level" => {
                let Some(value) = args.next() else {
                    eprintln!("{USAGE}");
                    return ExitCode::from(2);
                };
                match TraditionalRuntimeEvidenceLevel::parse(&value) {
                    Ok(level) => {
                        requested_evidence_level = Some(level);
                    }
                    Err(error) => {
                        return emit_error(
                            "traditional-analytics-vortex-batch-run",
                            format,
                            "traditional analytics native Vortex batch run failed",
                            &error,
                        );
                    }
                }
            }
            extra => {
                return emit_error(
                    "traditional-analytics-vortex-batch-run",
                    format,
                    "traditional analytics native Vortex batch run failed",
                    &cli_unknown_arg_error("traditional-analytics-vortex-batch-run", extra),
                );
            }
        }
    }

    let scenarios = match parse_traditional_analytics_scenario_csv(&scenario_list) {
        Ok(scenarios) => scenarios,
        Err(error) => {
            return emit_error(
                "traditional-analytics-vortex-batch-run",
                format,
                "traditional analytics native Vortex batch run failed",
                &error,
            );
        }
    };
    let request = shardloom_vortex::TraditionalAnalyticsVortexBatchRequest::new(
        scenarios,
        PathBuf::from(fact_vortex),
        PathBuf::from(dim_vortex),
    )
    .with_cdc_delta_vortex(cdc_delta_vortex)
    .with_requested_execution_mode(requested_execution_mode)
    .with_result_workspace_dir(workspace_dir)
    .with_result_vortex_write(write_result_vortex);
    let request = if let Some(evidence_level) = requested_evidence_level {
        request.with_evidence_level(evidence_level)
    } else {
        request
    };
    let report = match shardloom_vortex::run_traditional_analytics_vortex_batch_benchmark(request) {
        Ok(report) => report,
        Err(error) => {
            return emit_error(
                "traditional-analytics-vortex-batch-run",
                format,
                "traditional analytics native Vortex batch run failed",
                &error,
            );
        }
    };
    emit(
        "traditional-analytics-vortex-batch-run",
        format,
        CommandStatus::Success,
        "traditional analytics native Vortex batch smoke".to_string(),
        report.to_human_text(),
        report.diagnostics.clone(),
        report.fields(),
    );
    ExitCode::SUCCESS
}

#[allow(clippy::too_many_lines)]
pub(crate) fn handle_traditional_analytics_prepare_batch_run(
    mut args: std::vec::IntoIter<String>,
    format: OutputFormat,
) -> ExitCode {
    const USAGE: &str = "usage: shardloom traditional-analytics-prepare-batch-run <scenario_csv> <fact_input> <dim_input> --workspace <dir> [--input-format auto|csv|jsonl|parquet|arrow-ipc|avro|orc] [--cdc-delta <path>] [--result-workspace <dir>] [--write-result-vortex] [--evidence-level minimal_runtime|certified|full_replay] [--memory-gb <cap>] [--max-parallelism <cap>]";
    let Some(scenario_list) = args.next() else {
        eprintln!("{USAGE}");
        return ExitCode::from(2);
    };
    let Some(fact_input) = args.next() else {
        eprintln!("{USAGE}");
        return ExitCode::from(2);
    };
    let Some(dim_input) = args.next() else {
        eprintln!("{USAGE}");
        return ExitCode::from(2);
    };
    let mut workspace_dir: Option<PathBuf> = None;
    let mut input_format: Option<shardloom_vortex::TraditionalAnalyticsInputFormat> = None;
    let mut cdc_delta_input: Option<PathBuf> = None;
    let mut result_workspace_dir: Option<PathBuf> = None;
    let mut requested_evidence_level: Option<TraditionalRuntimeEvidenceLevel> = None;
    let mut write_result_vortex = false;
    let mut memory_gb: Option<u32> = None;
    let mut max_parallelism: Option<usize> = None;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--workspace" => {
                let Some(value) = args.next() else {
                    eprintln!("{USAGE}");
                    return ExitCode::from(2);
                };
                workspace_dir = Some(PathBuf::from(value));
            }
            "--input-format" => {
                let Some(value) = args.next() else {
                    eprintln!("{USAGE}");
                    return ExitCode::from(2);
                };
                if value != "auto" {
                    match shardloom_vortex::TraditionalAnalyticsInputFormat::parse(&value) {
                        Ok(parsed) => input_format = Some(parsed),
                        Err(error) => {
                            return emit_error(
                                "traditional-analytics-prepare-batch-run",
                                format,
                                "traditional analytics prepare/batch run failed",
                                &error,
                            );
                        }
                    }
                }
            }
            "--cdc-delta" => {
                let Some(value) = args.next() else {
                    eprintln!("{USAGE}");
                    return ExitCode::from(2);
                };
                cdc_delta_input = Some(PathBuf::from(value));
            }
            "--result-workspace" => {
                let Some(value) = args.next() else {
                    eprintln!("{USAGE}");
                    return ExitCode::from(2);
                };
                result_workspace_dir = Some(PathBuf::from(value));
            }
            "--write-result-vortex" => {
                write_result_vortex = true;
            }
            "--evidence-level" => {
                let Some(value) = args.next() else {
                    eprintln!("{USAGE}");
                    return ExitCode::from(2);
                };
                match TraditionalRuntimeEvidenceLevel::parse(&value) {
                    Ok(level) => requested_evidence_level = Some(level),
                    Err(error) => {
                        return emit_error(
                            "traditional-analytics-prepare-batch-run",
                            format,
                            "traditional analytics prepare/batch run failed",
                            &error,
                        );
                    }
                }
            }
            "--memory-gb" => {
                let Some(value) = args.next() else {
                    eprintln!("{USAGE}");
                    return ExitCode::from(2);
                };
                match value.parse::<u32>() {
                    Ok(parsed) if parsed > 0 => memory_gb = Some(parsed),
                    _ => {
                        return emit_error(
                            "traditional-analytics-prepare-batch-run",
                            format,
                            "traditional analytics prepare/batch run failed",
                            &ShardLoomError::InvalidOperation(format!(
                                "traditional-analytics-prepare-batch-run invalid --memory-gb value: {value}"
                            )),
                        );
                    }
                }
            }
            "--max-parallelism" => {
                let Some(value) = args.next() else {
                    eprintln!("{USAGE}");
                    return ExitCode::from(2);
                };
                match value.parse::<usize>() {
                    Ok(parsed) if parsed > 0 => max_parallelism = Some(parsed),
                    _ => {
                        return emit_error(
                            "traditional-analytics-prepare-batch-run",
                            format,
                            "traditional analytics prepare/batch run failed",
                            &ShardLoomError::InvalidOperation(format!(
                                "traditional-analytics-prepare-batch-run invalid --max-parallelism value: {value}"
                            )),
                        );
                    }
                }
            }
            extra => {
                return emit_error(
                    "traditional-analytics-prepare-batch-run",
                    format,
                    "traditional analytics prepare/batch run failed",
                    &cli_unknown_arg_error("traditional-analytics-prepare-batch-run", extra),
                );
            }
        }
    }

    let Some(workspace_dir) = workspace_dir else {
        return emit_error(
            "traditional-analytics-prepare-batch-run",
            format,
            "traditional analytics prepare/batch run failed",
            &ShardLoomError::InvalidOperation(
                "traditional-analytics-prepare-batch-run requires --workspace for caller-owned prepared artifacts; fallback execution was not attempted".to_string(),
            ),
        );
    };
    let scenarios = match parse_traditional_analytics_scenario_csv(&scenario_list) {
        Ok(scenarios) => scenarios,
        Err(error) => {
            return emit_error(
                "traditional-analytics-prepare-batch-run",
                format,
                "traditional analytics prepare/batch run failed",
                &error,
            );
        }
    };
    let fact_path = PathBuf::from(fact_input);
    let dim_path = PathBuf::from(dim_input);
    let input_format = input_format.unwrap_or_else(|| {
        shardloom_vortex::TraditionalAnalyticsInputFormat::infer_from_paths(&fact_path, &dim_path)
    });
    let mut request = shardloom_vortex::TraditionalAnalyticsPreparedBatchRequest::new(
        scenarios,
        fact_path,
        dim_path,
        workspace_dir,
    )
    .with_input_format(input_format)
    .with_cdc_delta_input(cdc_delta_input)
    .with_result_workspace_dir(result_workspace_dir)
    .with_result_vortex_write(write_result_vortex)
    .with_resource_policy(
        shardloom_vortex::TraditionalAnalyticsResourcePolicy::from_hints(
            memory_gb,
            max_parallelism,
        ),
    );
    if let Some(evidence_level) = requested_evidence_level {
        request = request.with_evidence_level(evidence_level);
    }
    let report = match shardloom_vortex::run_traditional_analytics_prepared_batch_benchmark(request)
    {
        Ok(report) => report,
        Err(error) => {
            return emit_error(
                "traditional-analytics-prepare-batch-run",
                format,
                "traditional analytics prepare/batch run failed",
                &error,
            );
        }
    };
    emit(
        "traditional-analytics-prepare-batch-run",
        format,
        CommandStatus::Success,
        "traditional analytics prepare-once batch smoke".to_string(),
        report.to_human_text(),
        report.diagnostics(),
        report.fields(),
    );
    ExitCode::SUCCESS
}

fn parse_traditional_analytics_scenario_csv(
    value: &str,
) -> shardloom_core::Result<Vec<shardloom_vortex::TraditionalAnalyticsScenario>> {
    let mut scenarios = Vec::new();
    for scenario in value.split(',').map(str::trim) {
        if scenario.is_empty() {
            continue;
        }
        scenarios.push(shardloom_vortex::TraditionalAnalyticsScenario::parse(
            scenario,
        )?);
    }
    if scenarios.is_empty() {
        return Err(ShardLoomError::InvalidOperation(
            "traditional-analytics-vortex-batch-run requires at least one comma-separated scenario; fallback execution was not attempted".to_string(),
        ));
    }
    Ok(scenarios)
}

pub(crate) fn handle_vortex_count_benchmark(
    args: std::vec::IntoIter<String>,
    format: OutputFormat,
) -> ExitCode {
    let (uri, memory_gb, max_parallelism, iteration_count) =
        match parse_vortex_count_benchmark_args(args) {
            Ok(parsed) => parsed,
            Err(code) => return code,
        };
    let mut iterations = Vec::new();
    for _ in 0..iteration_count {
        let started = Instant::now();
        let (encoded_report, local_report) =
            match crate::vortex_primitive_execution::run_vortex_approved_local_encoded_count(
                uri.clone(),
                memory_gb,
                max_parallelism,
            ) {
                Ok(reports) => reports,
                Err(error) => {
                    return emit_error(
                        "vortex-count-benchmark",
                        format,
                        "vortex count benchmark failed",
                        &error,
                    );
                }
            };
        let duration = started.elapsed();
        iterations.push(VortexCountBenchmarkIterationSummary::from_reports(
            duration,
            &encoded_report,
            &local_report,
        ));
    }
    let report = match VortexCountBenchmarkReport::from_iterations(
        uri,
        memory_gb,
        max_parallelism,
        iteration_count,
        iterations,
    ) {
        Ok(report) => report,
        Err(error) => {
            return emit_error(
                "vortex-count-benchmark",
                format,
                "vortex count benchmark report failed",
                &error,
            );
        }
    };
    let has_errors = report.has_errors();
    emit(
        "vortex-count-benchmark",
        format,
        if has_errors {
            CommandStatus::Unsupported
        } else {
            CommandStatus::Success
        },
        "vortex local encoded count benchmark".to_string(),
        report.to_human_text(),
        report.diagnostics.clone(),
        vortex_count_benchmark_fields(&report),
    );
    if has_errors {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

fn parse_vortex_count_benchmark_args(
    mut args: std::vec::IntoIter<String>,
) -> std::result::Result<(DatasetUri, u64, usize, usize), ExitCode> {
    let Some(dataset_uri) = args.next() else {
        eprintln!(
            "usage: shardloom vortex-count-benchmark <dataset_uri> <memory_gb> <max_parallelism> [--iterations <n>]"
        );
        return Err(ExitCode::from(2));
    };
    let Some(memory_gb_text) = args.next() else {
        eprintln!(
            "usage: shardloom vortex-count-benchmark <dataset_uri> <memory_gb> <max_parallelism> [--iterations <n>]"
        );
        return Err(ExitCode::from(2));
    };
    let Some(max_parallelism_text) = args.next() else {
        eprintln!(
            "usage: shardloom vortex-count-benchmark <dataset_uri> <memory_gb> <max_parallelism> [--iterations <n>]"
        );
        return Err(ExitCode::from(2));
    };
    let uri = DatasetUri::new(dataset_uri).map_err(|_| ExitCode::from(2))?;
    let memory_gb = memory_gb_text.parse().map_err(|_| ExitCode::from(2))?;
    let max_parallelism = max_parallelism_text
        .parse()
        .map_err(|_| ExitCode::from(2))?;
    let mut iterations = 3_usize;
    while let Some(option) = args.next() {
        if option != "--iterations" {
            eprintln!("unknown option for shardloom vortex-count-benchmark: {option}");
            return Err(ExitCode::from(2));
        }
        let Some(iterations_text) = args.next() else {
            eprintln!("usage: shardloom vortex-count-benchmark ... --iterations <n>");
            return Err(ExitCode::from(2));
        };
        iterations = iterations_text.parse().map_err(|_| ExitCode::from(2))?;
        if iterations == 0 {
            eprintln!("shardloom vortex-count-benchmark requires at least one iteration");
            return Err(ExitCode::from(2));
        }
    }
    Ok((uri, memory_gb, max_parallelism, iterations))
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub(crate) struct VortexCountBenchmarkIterationSummary {
    duration_micros: u64,
    count_result: Option<u64>,
    arrays_read_count: usize,
    rows_counted: u64,
    data_read: bool,
    data_decoded: bool,
    data_materialized: bool,
    row_read: bool,
    arrow_converted: bool,
    object_store_io: bool,
    write_io: bool,
    spill_io_performed: bool,
    external_effects_executed: bool,
    fallback_execution_allowed: bool,
    diagnostics: Vec<Diagnostic>,
}

impl VortexCountBenchmarkIterationSummary {
    pub(crate) fn from_reports(
        duration: Duration,
        encoded_report: &shardloom_vortex::VortexEncodedReadExecutionReport,
        local_report: &VortexLocalExecutionReport,
    ) -> Self {
        let mut diagnostics = encoded_report.diagnostics.clone();
        diagnostics.extend(local_report.diagnostics.clone());
        Self {
            duration_micros: duration_micros(duration),
            count_result: encoded_report.count_result,
            arrays_read_count: encoded_report.arrays_read_count,
            rows_counted: encoded_report.rows_counted,
            data_read: encoded_report.data_read || local_report.data_read,
            data_decoded: encoded_report.data_decoded || local_report.data_decoded,
            data_materialized: encoded_report.data_materialized || local_report.data_materialized,
            row_read: encoded_report.row_read,
            arrow_converted: encoded_report.arrow_converted,
            object_store_io: encoded_report.object_store_io || local_report.object_store_io,
            write_io: encoded_report.write_io || local_report.write_io,
            spill_io_performed: encoded_report.spill_io_performed
                || local_report.spill_io_performed,
            external_effects_executed: encoded_report.external_effects_executed
                || local_report.external_effects_executed,
            fallback_execution_allowed: encoded_report.fallback_execution_allowed
                || local_report.fallback_execution_allowed,
            diagnostics,
        }
    }

    #[cfg(test)]
    pub(crate) fn synthetic_success(duration_micros: u64, count: u64) -> Self {
        Self {
            duration_micros,
            count_result: Some(count),
            arrays_read_count: 1,
            rows_counted: count,
            data_read: true,
            data_decoded: false,
            data_materialized: false,
            row_read: false,
            arrow_converted: false,
            object_store_io: false,
            write_io: false,
            spill_io_performed: false,
            external_effects_executed: false,
            fallback_execution_allowed: false,
            diagnostics: Vec::new(),
        }
    }

    fn has_errors(&self) -> bool {
        self.count_result.is_none()
            || !self.data_read
            || self.data_decoded
            || self.data_materialized
            || self.row_read
            || self.arrow_converted
            || self.object_store_io
            || self.write_io
            || self.spill_io_performed
            || self.external_effects_executed
            || self.fallback_execution_allowed
            || diagnostics_have_errors(&self.diagnostics)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct VortexCountBenchmarkReport {
    dataset_uri: DatasetUri,
    memory_gb: u64,
    max_parallelism: usize,
    iterations_requested: usize,
    iterations: Vec<VortexCountBenchmarkIterationSummary>,
    pub(crate) correctness_evidence: BenchmarkEvidenceState,
    benchmark_result: BenchmarkResult,
    comparison_report: BenchmarkComparisonReport,
    pub(crate) diagnostics: Vec<Diagnostic>,
}

impl VortexCountBenchmarkReport {
    pub(crate) fn from_iterations(
        dataset_uri: DatasetUri,
        memory_gb: u64,
        max_parallelism: usize,
        iterations_requested: usize,
        iterations: Vec<VortexCountBenchmarkIterationSummary>,
    ) -> shardloom_core::Result<Self> {
        let count_result = consistent_count_result(&iterations);
        let correctness_evidence =
            local_count_benchmark_correctness_evidence(&dataset_uri, count_result);
        let benchmark_result = local_count_benchmark_result(&iterations)?;
        let plan = local_count_benchmark_plan(&dataset_uri);
        let comparison_report = BenchmarkComparisonReport::from_plan_and_results(
            &plan,
            vec![benchmark_result.clone()],
            correctness_evidence,
        );
        let diagnostics = iterations
            .iter()
            .flat_map(|iteration| iteration.diagnostics.clone())
            .collect::<Vec<_>>();
        Ok(Self {
            dataset_uri,
            memory_gb,
            max_parallelism,
            iterations_requested,
            iterations,
            correctness_evidence,
            benchmark_result,
            comparison_report,
            diagnostics,
        })
    }

    fn iterations_completed(&self) -> usize {
        self.iterations.len()
    }

    pub(crate) fn count_result(&self) -> Option<u64> {
        consistent_count_result(&self.iterations)
    }

    fn result_consistent(&self) -> bool {
        self.count_result().is_some()
            && self
                .iterations
                .iter()
                .all(|iteration| iteration.count_result == self.count_result())
    }

    fn total_duration_micros(&self) -> u64 {
        saturating_u128_to_u64(
            self.iterations
                .iter()
                .map(|iteration| u128::from(iteration.duration_micros))
                .sum(),
        )
    }

    fn min_duration_micros(&self) -> Option<u64> {
        self.iterations
            .iter()
            .map(|iteration| iteration.duration_micros)
            .min()
    }

    fn max_duration_micros(&self) -> Option<u64> {
        self.iterations
            .iter()
            .map(|iteration| iteration.duration_micros)
            .max()
    }

    fn avg_duration_micros(&self) -> Option<u64> {
        (!self.iterations.is_empty()).then(|| {
            saturating_u128_to_u64(
                u128::from(self.total_duration_micros()) / self.iterations.len() as u128,
            )
        })
    }

    fn total_rows_counted(&self) -> u64 {
        saturating_u128_to_u64(
            self.iterations
                .iter()
                .map(|iteration| u128::from(iteration.rows_counted))
                .sum(),
        )
    }

    fn total_arrays_read(&self) -> usize {
        self.iterations
            .iter()
            .map(|iteration| iteration.arrays_read_count)
            .sum()
    }

    fn has_unsafe_effects(&self) -> bool {
        self.iterations.iter().any(|iteration| {
            iteration.data_decoded
                || iteration.data_materialized
                || iteration.row_read
                || iteration.arrow_converted
                || iteration.object_store_io
                || iteration.write_io
                || iteration.spill_io_performed
                || iteration.external_effects_executed
                || iteration.fallback_execution_allowed
        })
    }

    pub(crate) fn has_errors(&self) -> bool {
        self.iterations_completed() != self.iterations_requested
            || self.iterations.is_empty()
            || !self.result_consistent()
            || self.has_unsafe_effects()
            || self
                .iterations
                .iter()
                .any(VortexCountBenchmarkIterationSummary::has_errors)
    }

    fn native_vortex_admission_status(&self) -> &'static str {
        if self.has_errors() {
            "blocked_diagnostics_or_effects"
        } else if matches!(self.correctness_evidence, BenchmarkEvidenceState::Present) {
            "admitted_fixture_certified"
        } else {
            "executed_uncertified_runtime_input"
        }
    }

    fn native_vortex_admission_support_status(&self) -> &'static str {
        if matches!(
            self.native_vortex_admission_status(),
            "admitted_fixture_certified"
        ) {
            "fixture_certified"
        } else {
            "executable_uncertified"
        }
    }

    fn native_vortex_admission_lane_claim_allowed(&self) -> bool {
        matches!(
            self.native_vortex_admission_status(),
            "admitted_fixture_certified"
        )
    }

    pub(crate) fn to_human_text(&self) -> String {
        format!(
            "vortex local encoded count benchmark\nengine: shardloom\nscenario: local encoded count\ndataset: {}\niterations: {}/{}\ncount: {}\ntotal query runtime micros: {}\navg query runtime micros: {}\nexternal baselines: pandas,polars,duckdb,spark,datafusion,dask comparison-only not executed\ncomparison status: {}\nclaim gate: {}\nfallback execution: disabled",
            self.dataset_uri.as_str(),
            self.iterations_completed(),
            self.iterations_requested,
            self.count_result()
                .map_or_else(|| "unknown".to_string(), |count| count.to_string()),
            self.total_duration_micros(),
            self.avg_duration_micros()
                .map_or_else(|| "unknown".to_string(), |duration| duration.to_string()),
            self.comparison_report.status.as_str(),
            self.comparison_report.claim_gate().status.as_str(),
        )
    }
}

fn diagnostics_have_errors(diagnostics: &[Diagnostic]) -> bool {
    diagnostics
        .iter()
        .any(|diagnostic| matches!(diagnostic.severity.as_str(), "error" | "fatal"))
}

fn consistent_count_result(iterations: &[VortexCountBenchmarkIterationSummary]) -> Option<u64> {
    let first = iterations.first()?.count_result?;
    iterations
        .iter()
        .all(|iteration| iteration.count_result == Some(first))
        .then_some(first)
}

fn local_count_benchmark_correctness_evidence(
    dataset_uri: &DatasetUri,
    count_result: Option<u64>,
) -> BenchmarkEvidenceState {
    let Some(count_result) = count_result else {
        return BenchmarkEvidenceState::Missing;
    };
    if local_encoded_count_correctness_fixture_for_target(dataset_uri).is_some_and(|fixture| {
        matches!(
            fixture.expected,
            ExpectedOutcome::EncodedCount { count } if count == count_result
        )
    }) {
        BenchmarkEvidenceState::Present
    } else {
        BenchmarkEvidenceState::Missing
    }
}

fn local_count_benchmark_result(
    iterations: &[VortexCountBenchmarkIterationSummary],
) -> shardloom_core::Result<BenchmarkResult> {
    let mut result = BenchmarkResult::new("local encoded count", BaselineEngine::ShardLoom)?;
    let total_micros = iterations
        .iter()
        .map(|iteration| u128::from(iteration.duration_micros))
        .sum();
    let rows_scanned = iterations
        .iter()
        .map(|iteration| u128::from(iteration.rows_counted))
        .sum();
    result.add_metric(
        BenchmarkMetric::WallTimeMillis,
        MetricValue::U64(micros_to_millis(saturating_u128_to_u64(total_micros))),
    );
    result.add_metric(
        BenchmarkMetric::QueryRuntimeMillis,
        MetricValue::U64(micros_to_millis(saturating_u128_to_u64(total_micros))),
    );
    result.add_metric(
        BenchmarkMetric::RowsScanned,
        MetricValue::U64(saturating_u128_to_u64(rows_scanned)),
    );
    result.add_metric(BenchmarkMetric::BytesDecoded, MetricValue::U64(0));
    result.add_metric(BenchmarkMetric::RowsMaterialized, MetricValue::U64(0));
    result.add_metric(BenchmarkMetric::SpillRequiredBytes, MetricValue::U64(0));
    result.add_metric(BenchmarkMetric::ObjectStoreRequests, MetricValue::U64(0));
    Ok(result)
}

fn local_count_benchmark_plan(dataset_uri: &DatasetUri) -> BenchmarkPlan {
    let mut plan = BenchmarkPlan::new();
    let mut scenario = BenchmarkScenario::new(
        "local encoded count",
        WorkloadClass::SingleNodeEncodedExecution,
    )
    .expect("valid local count benchmark scenario");
    scenario.dataset_name = Some(dataset_uri.as_str().to_string());
    scenario.dataset_scale = Some("runtime_input".to_string());
    scenario.storage_format = Some("vortex".to_string());
    scenario.query_or_operation = Some("count_all".to_string());
    scenario.correctness_validation = CorrectnessValidationMode::ExpectedOutput;
    for engine in [
        BaselineEngine::ShardLoom,
        BaselineEngine::Pandas,
        BaselineEngine::Polars,
        BaselineEngine::DuckDb,
        BaselineEngine::Spark,
        BaselineEngine::DataFusion,
        BaselineEngine::Dask,
    ] {
        scenario.add_baseline(engine);
    }
    for metric in [
        BenchmarkMetric::WallTimeMillis,
        BenchmarkMetric::QueryRuntimeMillis,
        BenchmarkMetric::RowsScanned,
        BenchmarkMetric::BytesDecoded,
        BenchmarkMetric::RowsMaterialized,
        BenchmarkMetric::SpillRequiredBytes,
        BenchmarkMetric::ObjectStoreRequests,
    ] {
        scenario.add_required_metric(metric);
    }
    plan.add_scenario(scenario);
    plan
}

pub(crate) fn vortex_count_benchmark_fields(
    report: &VortexCountBenchmarkReport,
) -> Vec<(String, String)> {
    let mut fields = Vec::new();
    append_vortex_count_benchmark_identity_fields(&mut fields, report);
    append_vortex_count_benchmark_admission_fields(&mut fields, report);
    append_vortex_count_benchmark_claim_fields(&mut fields, report);
    append_vortex_count_benchmark_timing_fields(&mut fields, report);
    append_vortex_count_benchmark_effect_fields(&mut fields, report);
    fields
}

fn append_vortex_count_benchmark_identity_fields(
    fields: &mut Vec<(String, String)>,
    report: &VortexCountBenchmarkReport,
) {
    push_field(fields, "mode", "vortex_count_benchmark");
    push_field(fields, "benchmark_engine", "shardloom");
    push_field(fields, "benchmark_scope", "local_encoded_count");
    push_field(fields, "dataset_uri", report.dataset_uri.as_str());
    push_u64_field(fields, "memory_gb", report.memory_gb);
    push_count_field(fields, "max_parallelism", report.max_parallelism);
    push_count_field(fields, "iterations_requested", report.iterations_requested);
    push_count_field(
        fields,
        "iterations_completed",
        report.iterations_completed(),
    );
    push_bool_field(fields, "benchmark_execution_implemented", true);
    push_field(
        fields,
        "external_baselines",
        "pandas,polars,duckdb,spark,datafusion,dask",
    );
    push_bool_field(fields, "external_baseline_execution", false);
    push_bool_field(fields, "external_baselines_comparison_only", true);
    push_bool_field(fields, "external_baseline_results_required", true);
}

fn append_vortex_count_benchmark_admission_fields(
    fields: &mut Vec<(String, String)>,
    report: &VortexCountBenchmarkReport,
) {
    append_vortex_count_benchmark_admission_identity_fields(fields, report);
    append_vortex_count_benchmark_admission_evidence_fields(fields);
    append_vortex_count_benchmark_admission_claim_fields(fields, report);
}

fn append_vortex_count_benchmark_admission_identity_fields(
    fields: &mut Vec<(String, String)>,
    report: &VortexCountBenchmarkReport,
) {
    push_field(
        fields,
        "native_vortex_admission_schema_version",
        "shardloom.native_vortex_admission.v1",
    );
    push_field(
        fields,
        "native_vortex_admission_lane_ref",
        "local_vortex_count_scalar",
    );
    push_field(
        fields,
        "native_vortex_admission_status",
        report.native_vortex_admission_status(),
    );
    push_field(
        fields,
        "native_vortex_admission_support_status",
        report.native_vortex_admission_support_status(),
    );
    push_field(
        fields,
        "native_vortex_admission_execution_mode",
        "native_vortex",
    );
    push_field(
        fields,
        "native_vortex_admission_source_surface",
        "local_vortex_file_scan",
    );
    push_field(
        fields,
        "native_vortex_admission_operator_surface",
        "count_all",
    );
    push_field(
        fields,
        "native_vortex_admission_sink_surface",
        "typed_scalar_result",
    );
    push_field(
        fields,
        "native_vortex_admission_provider_kind",
        "vortex_scan",
    );
    push_field(
        fields,
        "native_vortex_admission_provider_api_surface",
        "VortexFile::scan,ScanBuilder::into_array_iter",
    );
    push_field(
        fields,
        "native_vortex_admission_feature_gate",
        "vortex-encoded-read-spike",
    );
    push_field(
        fields,
        "native_vortex_admission_shardloom_policy",
        "local_fixture_scan_count_only",
    );
}

fn append_vortex_count_benchmark_admission_evidence_fields(fields: &mut Vec<(String, String)>) {
    push_field(
        fields,
        "native_vortex_admission_compute_row_ref",
        "compute_row.local_vortex_count",
    );
    push_field(
        fields,
        "native_vortex_admission_correctness_refs",
        "cg5.local_vortex_count,query_primitive_correctness",
    );
    push_field(
        fields,
        "native_vortex_admission_benchmark_refs",
        "vortex-count-benchmark.local_fixture_smoke",
    );
    push_field(
        fields,
        "native_vortex_admission_execution_certificate_refs",
        "certificates/cg16/local-vortex-count/execution.json",
    );
    push_field(
        fields,
        "native_vortex_admission_native_io_refs",
        "certificates/cg19/local-vortex-count/native-io.json",
    );
    push_field(
        fields,
        "native_vortex_admission_materialization_decode_refs",
        "native_vortex_source_to_scalar_count_result",
    );
}

fn append_vortex_count_benchmark_admission_claim_fields(
    fields: &mut Vec<(String, String)>,
    report: &VortexCountBenchmarkReport,
) {
    push_field(
        fields,
        "native_vortex_admission_claim_gate_status",
        "fixture_smoke_only",
    );
    push_field(
        fields,
        "native_vortex_admission_claim_boundary",
        "local_count_all_fixture_smoke_only_not_universal_native_vortex",
    );
    push_bool_field(
        fields,
        "native_vortex_admission_lane_claim_allowed",
        report.native_vortex_admission_lane_claim_allowed(),
    );
    push_bool_field(fields, "native_vortex_admission_fallback_attempted", false);
    push_bool_field(
        fields,
        "native_vortex_admission_external_engine_invoked",
        false,
    );
}

fn append_vortex_count_benchmark_claim_fields(
    fields: &mut Vec<(String, String)>,
    report: &VortexCountBenchmarkReport,
) {
    let claim_gate = report.comparison_report.claim_gate();
    push_field(
        fields,
        "comparison_status",
        report.comparison_report.status.as_str(),
    );
    push_count_field(
        fields,
        "comparison_missing_result_count",
        report.comparison_report.missing_results.len(),
    );
    push_field(fields, "claim_gate_status", claim_gate.status.as_str());
    push_field(
        fields,
        "correctness_evidence",
        report.correctness_evidence.as_str(),
    );
    push_field(
        fields,
        "benchmark_evidence_for_claims",
        report.comparison_report.benchmark_evidence.as_str(),
    );
    push_count_field(
        fields,
        "shardloom_metric_count",
        report.benchmark_result.metrics.len(),
    );
    push_bool_field(
        fields,
        "performance_claim_allowed",
        claim_gate.can_publish_performance_claim(),
    );
    push_bool_field(fields, "fallback_execution_allowed", false);
}

fn append_vortex_count_benchmark_timing_fields(
    fields: &mut Vec<(String, String)>,
    report: &VortexCountBenchmarkReport,
) {
    push_field(
        fields,
        "count",
        &report
            .count_result()
            .map_or_else(|| "unknown".to_string(), |count| count.to_string()),
    );
    push_bool_field(fields, "result_consistent", report.result_consistent());
    push_u64_field(
        fields,
        "total_query_runtime_micros",
        report.total_duration_micros(),
    );
    push_field(
        fields,
        "avg_query_runtime_micros",
        &report
            .avg_duration_micros()
            .map_or_else(|| "unknown".to_string(), |duration| duration.to_string()),
    );
    push_field(
        fields,
        "min_query_runtime_micros",
        &report
            .min_duration_micros()
            .map_or_else(|| "unknown".to_string(), |duration| duration.to_string()),
    );
    push_field(
        fields,
        "max_query_runtime_micros",
        &report
            .max_duration_micros()
            .map_or_else(|| "unknown".to_string(), |duration| duration.to_string()),
    );
    push_u64_field(
        fields,
        "total_query_runtime_millis",
        micros_to_millis(report.total_duration_micros()),
    );
    push_field(
        fields,
        "avg_query_runtime_millis",
        &report.avg_duration_micros().map_or_else(
            || "unknown".to_string(),
            |duration| micros_to_millis(duration).to_string(),
        ),
    );
    push_bool_field(fields, "startup_latency_measured", false);
    push_u64_field(fields, "total_rows_counted", report.total_rows_counted());
    push_count_field(fields, "total_arrays_read", report.total_arrays_read());
}

fn append_vortex_count_benchmark_effect_fields(
    fields: &mut Vec<(String, String)>,
    report: &VortexCountBenchmarkReport,
) {
    let any = |predicate: fn(&VortexCountBenchmarkIterationSummary) -> bool| {
        report.iterations.iter().any(predicate)
    };
    push_bool_field(fields, "data_read", any(|iteration| iteration.data_read));
    push_bool_field(
        fields,
        "data_decoded",
        any(|iteration| iteration.data_decoded),
    );
    push_bool_field(
        fields,
        "data_materialized",
        any(|iteration| iteration.data_materialized),
    );
    push_bool_field(fields, "row_read", any(|iteration| iteration.row_read));
    push_bool_field(
        fields,
        "arrow_converted",
        any(|iteration| iteration.arrow_converted),
    );
    push_bool_field(
        fields,
        "object_store_io",
        any(|iteration| iteration.object_store_io),
    );
    push_bool_field(fields, "write_io", any(|iteration| iteration.write_io));
    push_bool_field(
        fields,
        "spill_io_performed",
        any(|iteration| iteration.spill_io_performed),
    );
    push_bool_field(
        fields,
        "external_effects_executed",
        any(|iteration| iteration.external_effects_executed),
    );
    push_bool_field(fields, "fallback_attempted", false);
    push_bool_field(
        fields,
        "unsafe_effects_observed",
        report.has_unsafe_effects(),
    );
}

fn push_field(fields: &mut Vec<(String, String)>, key: &str, value: &str) {
    fields.push((key.to_string(), value.to_string()));
}

fn push_count_field(fields: &mut Vec<(String, String)>, key: &str, value: usize) {
    push_field(fields, key, &value.to_string());
}

fn push_u64_field(fields: &mut Vec<(String, String)>, key: &str, value: u64) {
    push_field(fields, key, &value.to_string());
}

fn push_bool_field(fields: &mut Vec<(String, String)>, key: &str, value: bool) {
    push_field(fields, key, &value.to_string());
}
