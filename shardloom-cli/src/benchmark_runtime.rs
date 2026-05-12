//! Executable local benchmark command handlers.
//!
//! These handlers remain local benchmark harness surfaces. External engines are
//! comparison-only baselines and must not become fallback execution paths.

use std::{path::PathBuf, process::ExitCode, time::Instant};

use shardloom_core::{CommandStatus, DatasetUri, OutputFormat, ShardLoomError};

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
        let (encoded_report, local_report) = match crate::run_vortex_approved_local_encoded_count(
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
        iterations.push(crate::VortexCountBenchmarkIterationSummary::from_reports(
            duration,
            &encoded_report,
            &local_report,
        ));
    }
    let report = match crate::VortexCountBenchmarkReport::from_iterations(
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
        crate::vortex_count_benchmark_fields(&report),
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
