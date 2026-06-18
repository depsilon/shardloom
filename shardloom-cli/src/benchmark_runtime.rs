//! Executable local benchmark command handlers.
//!
//! These handlers remain local benchmark harness surfaces. External engines are
//! comparison-only baselines and must not become fallback execution paths.

use std::{
    path::{Path, PathBuf},
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
use shardloom_vortex::{
    TraditionalRuntimeEvidenceLevel, TraditionalRuntimeEvidenceTier, VortexLocalExecutionReport,
};

use crate::{
    cli_output::{emit, emit_error, emit_timed},
    cli_time::{duration_micros, micros_to_millis, saturating_u128_to_u64},
    cli_unknown_arg_error,
    vortex_primitive_execution::local_encoded_count_correctness_fixture_for_target,
};

const TRADITIONAL_ANALYTICS_RUN_USAGE: &str = "usage: shardloom traditional-analytics-run <scenario> <fact_input> <dim_input> [--workspace <dir>] [--input-format auto|csv|jsonl|parquet|arrow-ipc|avro|orc] [--cdc-delta <csv>] [--compat-output-format csv|jsonl|parquet|arrow-ipc|avro|orc] [--verify-native-replay] [--write-result-vortex] [--preserve-all-text-columns-for-reuse] [--execution-mode auto|compatibility_import_certified|direct_compatibility_transient] [--memory-gb <cap>] [--max-parallelism <cap>]";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NativeVortexResultExportFormat {
    Jsonl,
    Csv,
}

impl NativeVortexResultExportFormat {
    fn parse(value: &str) -> Result<Self, ShardLoomError> {
        match value.trim().to_ascii_lowercase().as_str() {
            "jsonl" | "json-lines" | "ndjson" => Ok(Self::Jsonl),
            "csv" => Ok(Self::Csv),
            other => Err(ShardLoomError::InvalidOperation(format!(
                "native Vortex result export format {other:?} is unsupported; use jsonl or csv; fallback execution was not attempted"
            ))),
        }
    }

    const fn as_str(self) -> &'static str {
        match self {
            Self::Jsonl => "jsonl",
            Self::Csv => "csv",
        }
    }

    const fn materialization_boundary(self) -> &'static str {
        match self {
            Self::Jsonl => "native_vortex_result_json_to_jsonl_sink",
            Self::Csv => "native_vortex_result_json_to_csv_sink",
        }
    }

    fn render(self, result_json: &str) -> Vec<u8> {
        match self {
            Self::Jsonl => {
                let mut content = String::with_capacity(result_json.len() + 1);
                content.push_str(result_json);
                content.push('\n');
                content.into_bytes()
            }
            Self::Csv => {
                let escaped = result_json.replace('"', "\"\"");
                format!("result_json\n\"{escaped}\"\n").into_bytes()
            }
        }
    }
}

fn write_native_vortex_result_export(
    output_path: &Path,
    output_format: NativeVortexResultExportFormat,
    allow_overwrite: bool,
    result_json: &str,
) -> shardloom_core::Result<Vec<(String, String)>> {
    let workspace_root = shardloom_core::infer_local_output_workspace_root(output_path)?;
    let content = output_format.render(result_json);
    let report = shardloom_core::write_workspace_safe_bytes(
        workspace_root,
        output_path,
        allow_overwrite,
        format!("native Vortex result {} export", output_format.as_str()),
        &content,
    )?;
    let mut fields = vec![
        (
            "native_vortex_result_export_performed".to_string(),
            "true".to_string(),
        ),
        (
            "native_vortex_result_export_format".to_string(),
            output_format.as_str().to_string(),
        ),
        (
            "native_vortex_result_export_path".to_string(),
            output_path.display().to_string(),
        ),
        (
            "native_vortex_result_export_materialization_boundary".to_string(),
            output_format.materialization_boundary().to_string(),
        ),
        (
            "native_vortex_result_export_rows_written".to_string(),
            "1".to_string(),
        ),
        (
            "native_vortex_result_export_source".to_string(),
            "provider_result_json_after_native_vortex_execution".to_string(),
        ),
        (
            "native_vortex_result_export_external_engine_invoked".to_string(),
            "false".to_string(),
        ),
        (
            "native_vortex_result_export_fallback_attempted".to_string(),
            "false".to_string(),
        ),
    ];
    fields.extend(report.evidence_fields("native_vortex_result_export"));
    Ok(fields)
}

fn parse_native_vortex_result_fanout(
    value: &str,
) -> shardloom_core::Result<(PathBuf, NativeVortexResultExportFormat)> {
    let Some((format, path)) = value.split_once('=') else {
        return Err(ShardLoomError::InvalidOperation(
            "native Vortex result fanout output must use format=path syntax; fallback execution was not attempted"
                .to_string(),
        ));
    };
    let output_format = NativeVortexResultExportFormat::parse(format)?;
    if path.trim().is_empty() {
        return Err(ShardLoomError::InvalidOperation(
            "native Vortex result fanout output path must not be empty; fallback execution was not attempted"
                .to_string(),
        ));
    }
    Ok((PathBuf::from(path), output_format))
}

fn upsert_report_field(fields: &mut Vec<(String, String)>, key: &str, value: &str) {
    if let Some((_, existing)) = fields.iter_mut().find(|(field, _)| field == key) {
        *existing = value.to_string();
    } else {
        fields.push((key.to_string(), value.to_string()));
    }
}

fn record_evidence_render_timing(
    fields: &mut Vec<(String, String)>,
    human_elapsed: Duration,
    report_fields_elapsed: Duration,
) {
    let human_micros = duration_micros(human_elapsed).to_string();
    let report_fields_micros = duration_micros(report_fields_elapsed).to_string();
    let total_micros = duration_micros(human_elapsed + report_fields_elapsed).to_string();
    let status = "rust_cli_report_fields_and_human_text_measured_separately";
    let mut status_keys = Vec::new();
    let mut saw_top_level = false;
    for (key, value) in fields.iter_mut() {
        if key == "evidence_render_micros" || key.ends_with("_evidence_render_micros") {
            if key == "evidence_render_micros" {
                saw_top_level = true;
            }
            value.clone_from(&total_micros);
            let status_key = if key == "evidence_render_micros" {
                "evidence_render_timing_status".to_string()
            } else {
                format!(
                    "{}evidence_render_timing_status",
                    key.strip_suffix("evidence_render_micros")
                        .unwrap_or_default()
                )
            };
            status_keys.push(status_key);
        }
    }
    if !saw_top_level {
        fields.push(("evidence_render_micros".to_string(), total_micros));
        status_keys.push("evidence_render_timing_status".to_string());
    }
    upsert_report_field(fields, "human_evidence_render_micros", &human_micros);
    upsert_report_field(fields, "report_fields_build_micros", &report_fields_micros);
    for key in status_keys {
        upsert_report_field(fields, &key, status);
    }
}

fn compact_benchmark_human_text(command: &str, tier: &str) -> String {
    format!(
        "{command}: compact machine-readable benchmark evidence emitted; evidence_tier={tier}; human evidence rendering deferred"
    )
}

fn render_human_text_for_tier(
    format: OutputFormat,
    tier: Option<TraditionalRuntimeEvidenceTier>,
) -> bool {
    matches!(format, OutputFormat::Text)
        || match tier {
            Some(tier) => tier.publication_human_render_required(),
            None => true,
        }
}

#[allow(clippy::too_many_lines)]
pub(crate) fn handle_traditional_analytics_run(
    mut args: std::vec::IntoIter<String>,
    format: OutputFormat,
) -> ExitCode {
    let Some(scenario_text) = args.next() else {
        eprintln!("{TRADITIONAL_ANALYTICS_RUN_USAGE}");
        return ExitCode::from(2);
    };
    let Some(fact_csv) = args.next() else {
        eprintln!("{TRADITIONAL_ANALYTICS_RUN_USAGE}");
        return ExitCode::from(2);
    };
    let Some(dim_csv) = args.next() else {
        eprintln!("{TRADITIONAL_ANALYTICS_RUN_USAGE}");
        return ExitCode::from(2);
    };
    let mut workspace_dir: Option<PathBuf> = None;
    let mut input_format: Option<shardloom_vortex::TraditionalAnalyticsInputFormat> = None;
    let mut cdc_delta_csv: Option<PathBuf> = None;
    let mut compatibility_output_format: Option<shardloom_vortex::TraditionalAnalyticsInputFormat> =
        None;
    let mut verify_native_vortex_replay = false;
    let mut write_result_vortex = false;
    let mut preserve_all_text_columns_for_reuse = false;
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
            "--preserve-all-text-columns-for-reuse" => {
                preserve_all_text_columns_for_reuse = true;
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
            cdc_delta_requested: cdc_delta_csv.is_some(),
            compatibility_output_requested: compatibility_output_format.is_some(),
            verify_native_vortex_replay,
            write_result_vortex,
        });

    if requested_execution_mode == ShardLoomExecutionMode::DirectCompatibilityTransient
        && let Some(reason) = direct_transient_unsupported
    {
        return emit_direct_compatibility_transient_unsupported(
            format,
            input_format,
            verify_native_vortex_replay,
            write_result_vortex,
            reason,
        );
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
    .with_all_text_columns_preserved_for_reuse(preserve_all_text_columns_for_reuse)
    .with_requested_execution_mode(requested_execution_mode)
    .with_resource_policy(
        shardloom_vortex::TraditionalAnalyticsResourcePolicy::from_hints(
            memory_gb,
            max_parallelism,
        ),
    );
    if requested_execution_mode == ShardLoomExecutionMode::DirectCompatibilityTransient {
        let report =
            match shardloom_vortex::run_traditional_direct_transient_local_input_smoke(request) {
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
        let human_render_start = Instant::now();
        let human_text = report.to_human_text();
        let human_render_elapsed = human_render_start.elapsed();
        let report_fields_start = Instant::now();
        let mut fields = report.fields();
        let report_fields_elapsed = report_fields_start.elapsed();
        record_evidence_render_timing(&mut fields, human_render_elapsed, report_fields_elapsed);
        emit_timed(
            "traditional-analytics-run",
            format,
            CommandStatus::Success,
            "direct compatibility transient local-input smoke".to_string(),
            human_text,
            report.diagnostics.clone(),
            fields,
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
    let human_render_start = Instant::now();
    let human_text = report.to_human_text();
    let human_render_elapsed = human_render_start.elapsed();
    let report_fields_start = Instant::now();
    let mut fields = report.fields();
    let report_fields_elapsed = report_fields_start.elapsed();
    record_evidence_render_timing(&mut fields, human_render_elapsed, report_fields_elapsed);
    emit_timed(
        "traditional-analytics-run",
        format,
        CommandStatus::Success,
        "traditional analytics universal I/O smoke".to_string(),
        human_text,
        report.diagnostics.clone(),
        fields,
    );
    ExitCode::SUCCESS
}

#[derive(Debug, Clone, Copy)]
#[allow(clippy::struct_excessive_bools)]
struct DirectTransientAdmissionFacts {
    scenario: shardloom_vortex::TraditionalAnalyticsScenario,
    cdc_delta_requested: bool,
    compatibility_output_requested: bool,
    verify_native_vortex_replay: bool,
    write_result_vortex: bool,
}

fn direct_transient_unsupported_reason(
    facts: DirectTransientAdmissionFacts,
) -> Option<&'static str> {
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
                "Use compatibility_import_certified for certified ingest/stage evidence, or restrict direct transient mode to admitted local-input selective-filter or filter + projection + limit smoke paths."
                    .to_string(),
            ),
        )],
        fields,
    );
    ExitCode::from(1)
}

#[allow(clippy::too_many_lines)]
pub(crate) fn handle_traditional_analytics_vortex_run(
    args: std::vec::IntoIter<String>,
    format: OutputFormat,
) -> ExitCode {
    handle_traditional_analytics_vortex_run_with_facade(
        args,
        format,
        "traditional-analytics-vortex-run",
        Vec::new(),
    )
}

#[allow(clippy::too_many_lines)]
pub(crate) fn handle_vortex_production_runtime_run(
    args: std::vec::IntoIter<String>,
    format: OutputFormat,
) -> ExitCode {
    handle_traditional_analytics_vortex_run_with_facade(
        args,
        format,
        "vortex-production-runtime-run",
        Vec::new(),
    )
}

#[allow(clippy::too_many_lines)]
pub(crate) fn handle_traditional_analytics_vortex_run_with_facade(
    mut args: std::vec::IntoIter<String>,
    format: OutputFormat,
    emit_command: &'static str,
    extra_fields: Vec<(String, String)>,
) -> ExitCode {
    let usage = format!(
        "usage: shardloom {emit_command} <scenario> <fact_vortex> <dim_vortex> [--cdc-delta-vortex <path>] [--workspace <dir>] [--write-result-vortex] [--result-output <path>] [--result-output-format jsonl|csv] [--allow-overwrite] [--execution-mode auto|native_vortex|prepared_vortex] [--memory-gb <cap>] [--max-parallelism <cap>]"
    );
    let Some(scenario_text) = args.next() else {
        eprintln!("{usage}");
        return ExitCode::from(2);
    };
    let Some(fact_vortex) = args.next() else {
        eprintln!("{usage}");
        return ExitCode::from(2);
    };
    let Some(dim_vortex) = args.next() else {
        eprintln!("{usage}");
        return ExitCode::from(2);
    };
    let mut requested_execution_mode = ShardLoomExecutionMode::NativeVortex;
    let mut workspace_dir: Option<PathBuf> = None;
    let mut cdc_delta_vortex: Option<PathBuf> = None;
    let mut write_result_vortex = false;
    let mut result_output: Option<PathBuf> = None;
    let mut result_output_format: Option<NativeVortexResultExportFormat> = None;
    let mut result_fanout_outputs = Vec::<(PathBuf, NativeVortexResultExportFormat)>::new();
    let mut result_output_allow_overwrite = false;
    let mut memory_gb: Option<u32> = None;
    let mut max_parallelism: Option<usize> = None;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--cdc-delta-vortex" => {
                let Some(path) = args.next() else {
                    eprintln!("usage: shardloom {emit_command} ... --cdc-delta-vortex <path>");
                    return ExitCode::from(2);
                };
                cdc_delta_vortex = Some(PathBuf::from(path));
            }
            "--workspace" => {
                let Some(path) = args.next() else {
                    eprintln!("usage: shardloom {emit_command} ... --workspace <dir>");
                    return ExitCode::from(2);
                };
                workspace_dir = Some(PathBuf::from(path));
            }
            "--write-result-vortex" => {
                write_result_vortex = true;
            }
            "--result-output" => {
                let Some(path) = args.next() else {
                    eprintln!("usage: shardloom {emit_command} ... --result-output <path>");
                    return ExitCode::from(2);
                };
                result_output = Some(PathBuf::from(path));
            }
            "--result-output-format" => {
                let Some(value) = args.next() else {
                    eprintln!(
                        "usage: shardloom {emit_command} ... --result-output-format jsonl|csv"
                    );
                    return ExitCode::from(2);
                };
                match NativeVortexResultExportFormat::parse(&value) {
                    Ok(parsed) => result_output_format = Some(parsed),
                    Err(error) => {
                        return emit_error(
                            emit_command,
                            format,
                            "native Vortex provider runtime failed",
                            &error,
                        );
                    }
                }
            }
            "--fanout-output" => {
                let Some(value) = args.next() else {
                    eprintln!("usage: shardloom {emit_command} ... --fanout-output format=path");
                    return ExitCode::from(2);
                };
                match parse_native_vortex_result_fanout(&value) {
                    Ok(parsed) => result_fanout_outputs.push(parsed),
                    Err(error) => {
                        return emit_error(
                            emit_command,
                            format,
                            "native Vortex result export failed",
                            &error,
                        );
                    }
                }
            }
            "--allow-overwrite" => {
                result_output_allow_overwrite = true;
            }
            "--execution-mode" => {
                let Some(value) = args.next() else {
                    eprintln!("{usage}");
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
                            emit_command,
                            format,
                            "native Vortex provider runtime failed",
                            &ShardLoomError::InvalidOperation(format!(
                                "{emit_command} does not support execution mode {}; fallback execution was not attempted",
                                mode.as_str()
                            )),
                        );
                    }
                    Err(error) => {
                        return emit_error(
                            emit_command,
                            format,
                            "native Vortex provider runtime failed",
                            &error,
                        );
                    }
                }
            }
            "--memory-gb" => {
                let Some(value) = args.next() else {
                    eprintln!("usage: shardloom {emit_command} ... --memory-gb <cap>");
                    return ExitCode::from(2);
                };
                match value.parse::<u32>() {
                    Ok(parsed) if parsed > 0 => memory_gb = Some(parsed),
                    _ => {
                        return emit_error(
                            emit_command,
                            format,
                            "native Vortex provider runtime failed",
                            &ShardLoomError::InvalidOperation(format!(
                                "{emit_command} invalid --memory-gb value: {value}"
                            )),
                        );
                    }
                }
            }
            "--max-parallelism" => {
                let Some(value) = args.next() else {
                    eprintln!("usage: shardloom {emit_command} ... --max-parallelism <cap>");
                    return ExitCode::from(2);
                };
                match value.parse::<usize>() {
                    Ok(parsed) if parsed > 0 => max_parallelism = Some(parsed),
                    _ => {
                        return emit_error(
                            emit_command,
                            format,
                            "native Vortex provider runtime failed",
                            &ShardLoomError::InvalidOperation(format!(
                                "{emit_command} invalid --max-parallelism value: {value}"
                            )),
                        );
                    }
                }
            }
            extra => {
                return emit_error(
                    emit_command,
                    format,
                    "native Vortex provider runtime failed",
                    &cli_unknown_arg_error(emit_command, extra),
                );
            }
        }
    }

    let scenario = match shardloom_vortex::TraditionalAnalyticsScenario::parse(&scenario_text) {
        Ok(scenario) => scenario,
        Err(error) => {
            return emit_error(
                emit_command,
                format,
                "native Vortex provider runtime failed",
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
    .with_result_vortex_write(write_result_vortex)
    .with_resource_policy(
        shardloom_vortex::TraditionalAnalyticsResourcePolicy::from_hints(
            memory_gb,
            max_parallelism,
        ),
    );
    let report = match shardloom_vortex::run_traditional_analytics_vortex_benchmark(request) {
        Ok(report) => report,
        Err(error) => {
            return emit_error(
                emit_command,
                format,
                "native Vortex provider runtime failed",
                &error,
            );
        }
    };
    let mut result_export_fields = match (result_output.as_ref(), result_output_format) {
        (Some(path), Some(output_format)) => match write_native_vortex_result_export(
            path,
            output_format,
            result_output_allow_overwrite,
            &report.result_json,
        ) {
            Ok(fields) => fields,
            Err(error) => {
                return emit_error(
                    emit_command,
                    format,
                    "native Vortex result export failed",
                    &error,
                );
            }
        },
        (Some(_), None) => {
            return emit_error(
                emit_command,
                format,
                "native Vortex result export failed",
                &ShardLoomError::InvalidOperation(
                    "--result-output requires --result-output-format jsonl|csv; fallback execution was not attempted".to_string(),
                ),
            );
        }
        (None, Some(_)) => {
            return emit_error(
                emit_command,
                format,
                "native Vortex result export failed",
                &ShardLoomError::InvalidOperation(
                    "--result-output-format requires --result-output <path>; fallback execution was not attempted".to_string(),
                ),
            );
        }
        (None, None) => Vec::new(),
    };
    if !result_fanout_outputs.is_empty() {
        let mut fanout_formats = Vec::with_capacity(result_fanout_outputs.len());
        let mut fanout_paths = Vec::with_capacity(result_fanout_outputs.len());
        for (index, (path, output_format)) in result_fanout_outputs.iter().enumerate() {
            let fields = match write_native_vortex_result_export(
                path,
                *output_format,
                result_output_allow_overwrite,
                &report.result_json,
            ) {
                Ok(fields) => fields,
                Err(error) => {
                    return emit_error(
                        emit_command,
                        format,
                        "native Vortex result fanout export failed",
                        &error,
                    );
                }
            };
            fanout_formats.push(output_format.as_str());
            fanout_paths.push(path.display().to_string());
            for (key, value) in fields {
                result_export_fields.push((
                    format!("native_vortex_result_export_fanout_{index}_{key}"),
                    value,
                ));
            }
        }
        result_export_fields.extend([
            (
                "native_vortex_result_export_fanout_performed".to_string(),
                "true".to_string(),
            ),
            (
                "native_vortex_result_export_fanout_count".to_string(),
                result_fanout_outputs.len().to_string(),
            ),
            (
                "native_vortex_result_export_fanout_formats".to_string(),
                fanout_formats.join(","),
            ),
            (
                "native_vortex_result_export_fanout_paths".to_string(),
                fanout_paths.join(","),
            ),
        ]);
    }
    let human_render_start = Instant::now();
    let human_text = report.to_human_text();
    let human_render_elapsed = human_render_start.elapsed();
    let report_fields_start = Instant::now();
    let mut fields = report.fields();
    fields.extend(result_export_fields);
    fields.extend(extra_fields);
    let report_fields_elapsed = report_fields_start.elapsed();
    record_evidence_render_timing(&mut fields, human_render_elapsed, report_fields_elapsed);
    emit_timed(
        emit_command,
        format,
        CommandStatus::Success,
        "native Vortex provider runtime".to_string(),
        human_text,
        report.diagnostics.clone(),
        fields,
    );
    ExitCode::SUCCESS
}

#[allow(clippy::too_many_lines)]
pub(crate) fn handle_traditional_analytics_vortex_batch_run(
    mut args: std::vec::IntoIter<String>,
    format: OutputFormat,
) -> ExitCode {
    const USAGE: &str = "usage: shardloom traditional-analytics-vortex-batch-run <scenario_csv> <fact_vortex> <dim_vortex> [--cdc-delta-vortex <path>] [--workspace <dir>] [--write-result-vortex] [--execution-mode auto|native_vortex|prepared_vortex] [--evidence-level minimal_runtime|certified|full_replay] [--evidence-tier runtime_minimal|metadata_sink|full_vortex_replay|publication_full] [--memory-gb <cap>] [--max-parallelism <cap>]";
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
    let mut requested_evidence_tier: Option<TraditionalRuntimeEvidenceTier> = None;
    let mut workspace_dir: Option<PathBuf> = None;
    let mut cdc_delta_vortex: Option<PathBuf> = None;
    let mut write_result_vortex = false;
    let mut memory_gb: Option<u32> = None;
    let mut max_parallelism: Option<usize> = None;
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
            "--evidence-tier" => {
                let Some(value) = args.next() else {
                    eprintln!("{USAGE}");
                    return ExitCode::from(2);
                };
                match TraditionalRuntimeEvidenceTier::parse(&value) {
                    Ok(tier) => {
                        requested_evidence_tier = Some(tier);
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
            "--memory-gb" => {
                let Some(value) = args.next() else {
                    eprintln!(
                        "usage: shardloom traditional-analytics-vortex-batch-run ... --memory-gb <cap>"
                    );
                    return ExitCode::from(2);
                };
                match value.parse::<u32>() {
                    Ok(parsed) if parsed > 0 => memory_gb = Some(parsed),
                    _ => {
                        return emit_error(
                            "traditional-analytics-vortex-batch-run",
                            format,
                            "traditional analytics native Vortex batch run failed",
                            &ShardLoomError::InvalidOperation(format!(
                                "traditional-analytics-vortex-batch-run invalid --memory-gb value: {value}"
                            )),
                        );
                    }
                }
            }
            "--max-parallelism" => {
                let Some(value) = args.next() else {
                    eprintln!(
                        "usage: shardloom traditional-analytics-vortex-batch-run ... --max-parallelism <cap>"
                    );
                    return ExitCode::from(2);
                };
                match value.parse::<usize>() {
                    Ok(parsed) if parsed > 0 => max_parallelism = Some(parsed),
                    _ => {
                        return emit_error(
                            "traditional-analytics-vortex-batch-run",
                            format,
                            "traditional analytics native Vortex batch run failed",
                            &ShardLoomError::InvalidOperation(format!(
                                "traditional-analytics-vortex-batch-run invalid --max-parallelism value: {value}"
                            )),
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
    .with_result_vortex_write(write_result_vortex)
    .with_resource_policy(
        shardloom_vortex::TraditionalAnalyticsResourcePolicy::from_hints(
            memory_gb,
            max_parallelism,
        ),
    );
    let mut request = if let Some(evidence_level) = requested_evidence_level {
        request.with_evidence_level(evidence_level)
    } else {
        request
    };
    if let Some(evidence_tier) = requested_evidence_tier {
        request = request.with_evidence_tier(evidence_tier);
    }
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
    let human_render_start = Instant::now();
    let human_text = if render_human_text_for_tier(format, Some(report.selected_evidence_tier)) {
        report.to_human_text()
    } else {
        compact_benchmark_human_text(
            "traditional-analytics-vortex-batch-run",
            report.selected_evidence_tier.as_str(),
        )
    };
    let human_render_elapsed = human_render_start.elapsed();
    let report_fields_start = Instant::now();
    let mut fields = report.fields();
    let report_fields_elapsed = report_fields_start.elapsed();
    record_evidence_render_timing(&mut fields, human_render_elapsed, report_fields_elapsed);
    emit_timed(
        "traditional-analytics-vortex-batch-run",
        format,
        CommandStatus::Success,
        "traditional analytics native Vortex batch smoke".to_string(),
        human_text,
        report.diagnostics.clone(),
        fields,
    );
    ExitCode::SUCCESS
}

#[allow(clippy::too_many_lines)]
pub(crate) fn handle_traditional_analytics_prepare_batch_run(
    mut args: std::vec::IntoIter<String>,
    format: OutputFormat,
) -> ExitCode {
    const USAGE: &str = "usage: shardloom traditional-analytics-prepare-batch-run <scenario_csv> <fact_input> <dim_input> --workspace <dir> [--input-format auto|csv|jsonl|parquet|arrow-ipc|avro|orc] [--cdc-delta <path>] [--result-workspace <dir>] [--write-result-vortex] [--evidence-level minimal_runtime|certified|full_replay] [--evidence-tier runtime_minimal|metadata_sink|full_vortex_replay|publication_full] [--memory-gb <cap>] [--max-parallelism <cap>]";
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
    let mut requested_evidence_tier: Option<TraditionalRuntimeEvidenceTier> = None;
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
            "--evidence-tier" => {
                let Some(value) = args.next() else {
                    eprintln!("{USAGE}");
                    return ExitCode::from(2);
                };
                match TraditionalRuntimeEvidenceTier::parse(&value) {
                    Ok(tier) => requested_evidence_tier = Some(tier),
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
    if let Some(evidence_tier) = requested_evidence_tier {
        request = request.with_evidence_tier(evidence_tier);
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
    let human_render_start = Instant::now();
    let selected_evidence_tier = report.batch_report.selected_evidence_tier;
    let human_text = if render_human_text_for_tier(format, Some(selected_evidence_tier)) {
        report.to_human_text()
    } else {
        compact_benchmark_human_text(
            "traditional-analytics-prepare-batch-run",
            selected_evidence_tier.as_str(),
        )
    };
    let human_render_elapsed = human_render_start.elapsed();
    let report_fields_start = Instant::now();
    let mut fields = report.fields();
    let report_fields_elapsed = report_fields_start.elapsed();
    record_evidence_render_timing(&mut fields, human_render_elapsed, report_fields_elapsed);
    emit_timed(
        "traditional-analytics-prepare-batch-run",
        format,
        CommandStatus::Success,
        "traditional analytics prepare-once batch smoke".to_string(),
        human_text,
        report.diagnostics(),
        fields,
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

pub(crate) fn handle_operator_microkernel_benchmark(
    args: std::vec::IntoIter<String>,
    format: OutputFormat,
) -> ExitCode {
    let iterations = match parse_operator_microkernel_benchmark_args(args) {
        Ok(iterations) => iterations,
        Err(code) => return code,
    };
    #[cfg(feature = "vortex-traditional-analytics-benchmark")]
    {
        let report =
            match shardloom_vortex::traditional_analytics::run_traditional_operator_microkernel_benchmark(
                iterations,
            ) {
                Ok(report) => report,
                Err(error) => {
                    return emit_error(
                        "operator-microkernel-benchmark",
                        format,
                        "operator microkernel benchmark failed",
                        &error,
                    );
                }
            };
        let has_errors = report.has_errors();
        emit(
            "operator-microkernel-benchmark",
            format,
            if has_errors {
                CommandStatus::Unsupported
            } else {
                CommandStatus::Success
            },
            "ShardLoom focused operator microkernel benchmark".to_string(),
            report.to_human_text(),
            Vec::new(),
            report.fields(),
        );
        if has_errors {
            ExitCode::from(1)
        } else {
            ExitCode::SUCCESS
        }
    }
    #[cfg(not(feature = "vortex-traditional-analytics-benchmark"))]
    {
        let diagnostic = Diagnostic::unsupported(
            DiagnosticCode::NotImplemented,
            "operator-microkernel-benchmark",
            "operator microkernel benchmark requires the vortex-traditional-analytics-benchmark feature",
            Some(
                "Build shardloom-cli with --features vortex-traditional-analytics-benchmark before running this focused benchmark."
                    .to_string(),
            ),
        );
        emit(
            "operator-microkernel-benchmark",
            format,
            CommandStatus::Unsupported,
            "ShardLoom focused operator microkernel benchmark".to_string(),
            "operator microkernel benchmark unavailable; feature disabled\nfallback execution: disabled"
                .to_string(),
            vec![diagnostic],
            vec![
                (
                    "operator_microkernel_schema_version".to_string(),
                    "not_executed".to_string(),
                ),
                (
                    "operator_microkernel_iterations_requested".to_string(),
                    iterations.to_string(),
                ),
                (
                    "operator_microkernel_iterations_completed".to_string(),
                    "0".to_string(),
                ),
                (
                    "operator_microkernel_claim_gate_status".to_string(),
                    "not_claim_grade".to_string(),
                ),
                (
                    "operator_microkernel_fallback_attempted".to_string(),
                    "false".to_string(),
                ),
                (
                    "operator_microkernel_external_engine_invoked".to_string(),
                    "false".to_string(),
                ),
            ],
        );
        ExitCode::from(1)
    }
}

fn parse_operator_microkernel_benchmark_args(
    mut args: std::vec::IntoIter<String>,
) -> std::result::Result<usize, ExitCode> {
    let mut iterations = 3_usize;
    while let Some(option) = args.next() {
        if option != "--iterations" {
            eprintln!("usage: shardloom operator-microkernel-benchmark [--iterations <n>]");
            return Err(ExitCode::from(2));
        }
        let Some(iterations_text) = args.next() else {
            eprintln!("usage: shardloom operator-microkernel-benchmark [--iterations <n>]");
            return Err(ExitCode::from(2));
        };
        iterations = iterations_text.parse().map_err(|_| ExitCode::from(2))?;
        if iterations == 0 {
            eprintln!("shardloom operator-microkernel-benchmark requires at least one iteration");
            return Err(ExitCode::from(2));
        }
    }
    Ok(iterations)
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

#[cfg(test)]
mod tests {
    use super::*;

    fn field_value<'a>(fields: &'a [(String, String)], key: &str) -> &'a str {
        fields
            .iter()
            .find_map(|(field_key, value)| (field_key == key).then_some(value.as_str()))
            .unwrap_or_else(|| panic!("missing field {key}"))
    }

    #[test]
    fn benchmark_runtime_compact_human_render_policy_tracks_evidence_tier() {
        assert!(render_human_text_for_tier(
            OutputFormat::Text,
            Some(TraditionalRuntimeEvidenceTier::RuntimeMinimal)
        ));
        assert!(!render_human_text_for_tier(
            OutputFormat::Json,
            Some(TraditionalRuntimeEvidenceTier::RuntimeMinimal)
        ));
        assert!(!render_human_text_for_tier(
            OutputFormat::Json,
            Some(TraditionalRuntimeEvidenceTier::MetadataSink)
        ));
        assert!(!render_human_text_for_tier(
            OutputFormat::Json,
            Some(TraditionalRuntimeEvidenceTier::FullVortexReplay)
        ));
        assert!(render_human_text_for_tier(
            OutputFormat::Json,
            Some(TraditionalRuntimeEvidenceTier::PublicationFull)
        ));
        assert_eq!(
            compact_benchmark_human_text("traditional-analytics-vortex-batch-run", "metadata_sink"),
            "traditional-analytics-vortex-batch-run: compact machine-readable benchmark evidence emitted; evidence_tier=metadata_sink; human evidence rendering deferred",
        );
    }

    #[test]
    fn benchmark_runtime_records_split_evidence_render_timing_fields() {
        let mut fields = vec![
            ("evidence_render_micros".to_string(), "0".to_string()),
            (
                "computed_evidence_render_micros".to_string(),
                "0".to_string(),
            ),
        ];
        record_evidence_render_timing(
            &mut fields,
            Duration::from_micros(7),
            Duration::from_micros(11),
        );

        assert_eq!(field_value(&fields, "evidence_render_micros"), "18");
        assert_eq!(
            field_value(&fields, "computed_evidence_render_micros"),
            "18"
        );
        assert_eq!(field_value(&fields, "human_evidence_render_micros"), "7");
        assert_eq!(field_value(&fields, "report_fields_build_micros"), "11");
        assert_eq!(
            field_value(&fields, "evidence_render_timing_status"),
            "rust_cli_report_fields_and_human_text_measured_separately"
        );
        assert_eq!(
            field_value(&fields, "computed_evidence_render_timing_status"),
            "rust_cli_report_fields_and_human_text_measured_separately"
        );
    }
}
