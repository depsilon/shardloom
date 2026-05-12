//! Executable local benchmark command handlers.
//!
//! These handlers remain local benchmark harness surfaces. External engines are
//! comparison-only baselines and must not become fallback execution paths.

use std::{path::PathBuf, process::ExitCode};

use shardloom_core::{CommandStatus, OutputFormat, ShardLoomError};

use crate::{
    cli_output::{emit, emit_error},
    cli_unknown_arg_error,
};

#[allow(clippy::too_many_lines)]
pub(crate) fn handle_traditional_analytics_run(
    mut args: std::vec::IntoIter<String>,
    format: OutputFormat,
) -> ExitCode {
    let Some(scenario_text) = args.next() else {
        eprintln!(
            "usage: shardloom traditional-analytics-run <scenario> <fact_input> <dim_input> [--workspace <dir>] [--input-format auto|csv|jsonl|parquet|arrow-ipc|avro|orc] [--compat-output-format csv|jsonl|parquet|arrow-ipc|avro|orc] [--memory-gb <cap>] [--max-parallelism <cap>]"
        );
        return ExitCode::from(2);
    };
    let Some(fact_csv) = args.next() else {
        eprintln!(
            "usage: shardloom traditional-analytics-run <scenario> <fact_input> <dim_input> [--workspace <dir>] [--input-format auto|csv|jsonl|parquet|arrow-ipc|avro|orc] [--compat-output-format csv|jsonl|parquet|arrow-ipc|avro|orc] [--memory-gb <cap>] [--max-parallelism <cap>]"
        );
        return ExitCode::from(2);
    };
    let Some(dim_csv) = args.next() else {
        eprintln!(
            "usage: shardloom traditional-analytics-run <scenario> <fact_input> <dim_input> [--workspace <dir>] [--input-format auto|csv|jsonl|parquet|arrow-ipc|avro|orc] [--compat-output-format csv|jsonl|parquet|arrow-ipc|avro|orc] [--memory-gb <cap>] [--max-parallelism <cap>]"
        );
        return ExitCode::from(2);
    };
    let mut workspace_dir: Option<PathBuf> = None;
    let mut input_format: Option<shardloom_vortex::TraditionalAnalyticsInputFormat> = None;
    let mut compatibility_output_format: Option<shardloom_vortex::TraditionalAnalyticsInputFormat> =
        None;
    let mut memory_gb: Option<u32> = None;
    let mut max_parallelism: Option<usize> = None;
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
    let request = shardloom_vortex::TraditionalAnalyticsRequest::new(
        scenario,
        fact_path,
        dim_path,
        workspace_dir,
    )
    .with_input_format(input_format)
    .with_compatibility_output_format(compatibility_output_format)
    .with_resource_policy(
        shardloom_vortex::TraditionalAnalyticsResourcePolicy::from_hints(
            memory_gb,
            max_parallelism,
        ),
    );
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

pub(crate) fn handle_traditional_analytics_vortex_run(
    mut args: std::vec::IntoIter<String>,
    format: OutputFormat,
) -> ExitCode {
    let Some(scenario_text) = args.next() else {
        eprintln!(
            "usage: shardloom traditional-analytics-vortex-run <scenario> <fact_vortex> <dim_vortex>"
        );
        return ExitCode::from(2);
    };
    let Some(fact_vortex) = args.next() else {
        eprintln!(
            "usage: shardloom traditional-analytics-vortex-run <scenario> <fact_vortex> <dim_vortex>"
        );
        return ExitCode::from(2);
    };
    let Some(dim_vortex) = args.next() else {
        eprintln!(
            "usage: shardloom traditional-analytics-vortex-run <scenario> <fact_vortex> <dim_vortex>"
        );
        return ExitCode::from(2);
    };
    if let Some(extra) = args.next() {
        return emit_error(
            "traditional-analytics-vortex-run",
            format,
            "traditional analytics native Vortex run failed",
            &cli_unknown_arg_error("traditional-analytics-vortex-run", &extra),
        );
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
    );
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
