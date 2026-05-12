//! Engine runtime, streaming, sizing, and scheduling planning handlers.
//!
//! These handlers emit report-only runtime planning surfaces. They do not read
//! datasets, execute tasks, collect runtime profiles, write data, materialize
//! outputs, invoke external engines, or provide fallback execution.

use std::process::ExitCode;

use shardloom_core::{
    CommandStatus, DatasetRef, DatasetUri, OutputFormat, OutputTarget, ShardLoomError,
};
use shardloom_exec::{
    AdaptiveSizer, AdaptiveSizingPolicy, BackpressurePlanInput, BoundedMemoryPolicy, ByteSize,
    DynamicSizingFeedbackInput, EncodedStreamingBatchPlanInput, ParallelismLimit, ParallelismPlan,
    RuntimePlanSkeleton, SizeEstimate, SizingInput, SizingPlan, StreamingPlanSkeleton,
    plan_backpressure, plan_dynamic_runtime_promotion_gate, plan_dynamic_sizing_feedback,
    plan_encoded_streaming_batches,
};

use crate::{
    backpressure_plan_fields,
    cli_output::{emit, emit_error},
    cli_unknown_arg_error, dynamic_runtime_promotion_gate_fields, dynamic_sizing_feedback_fields,
    dynamic_work_shaping_fields, dynamic_work_shaping_report_for_profile,
    encoded_streaming_batch_plan_fields, parse_sizing_feedback_signals, streaming_plan_fields,
};

const STREAMING_BATCH_COMMAND: &str = "streaming-batch-plan";
const STREAMING_BATCH_SUMMARY: &str = "encoded streaming-batch planning failed";
const STREAMING_BATCH_USAGE: &str = "usage: shardloom streaming-batch-plan <dataset_uri> <target_uri> <memory_gb> <max_parallelism> [batch_mib]";

struct StreamingBatchArgs {
    dataset_ref: DatasetRef,
    output_target: OutputTarget,
    memory_gb: u64,
    max_parallelism: usize,
    batch_mib: Option<u64>,
}

pub(crate) fn handle_streaming_plan(
    mut args: impl Iterator<Item = String>,
    format: OutputFormat,
) -> ExitCode {
    let Some(dataset_uri) = args.next() else {
        eprintln!("usage: shardloom streaming-plan <dataset_uri> <target_uri>");
        return ExitCode::from(2);
    };
    let Some(target_uri) = args.next() else {
        eprintln!("usage: shardloom streaming-plan <dataset_uri> <target_uri>");
        return ExitCode::from(2);
    };
    let dataset_uri = match DatasetUri::new(dataset_uri) {
        Ok(uri) => uri,
        Err(error) => {
            eprintln!("invalid dataset uri: {error}");
            return ExitCode::from(2);
        }
    };
    let dataset_ref = match DatasetRef::from_uri(dataset_uri) {
        Ok(dataset_ref) => dataset_ref,
        Err(error) => {
            eprintln!("failed to create dataset reference: {error}");
            return ExitCode::from(2);
        }
    };
    let target_uri = match DatasetUri::new(target_uri) {
        Ok(uri) => uri,
        Err(error) => {
            eprintln!("invalid target uri: {error}");
            return ExitCode::from(2);
        }
    };
    let output_target = OutputTarget::from_uri(target_uri);
    let plan = StreamingPlanSkeleton::for_vortex_to_target(dataset_ref, output_target);
    emit(
        "streaming-plan",
        format,
        if plan.has_errors() {
            CommandStatus::Unsupported
        } else {
            CommandStatus::Success
        },
        "streaming plan".to_string(),
        plan.to_human_text(),
        plan.diagnostics.clone(),
        streaming_plan_fields(&plan),
    );
    if plan.has_errors() {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

pub(crate) fn handle_streaming_batch_plan(
    args: impl Iterator<Item = String>,
    format: OutputFormat,
) -> ExitCode {
    let parsed = match parse_streaming_batch_args(args, format) {
        Ok(parsed) => parsed,
        Err(exit_code) => return exit_code,
    };
    let memory = BoundedMemoryPolicy::required(ByteSize::from_gib(parsed.memory_gb));
    let mut input = match EncodedStreamingBatchPlanInput::for_vortex_to_target(
        parsed.dataset_ref,
        parsed.output_target,
        memory,
        parsed.max_parallelism,
    ) {
        Ok(input) => input,
        Err(error) => {
            return emit_error(
                STREAMING_BATCH_COMMAND,
                format,
                STREAMING_BATCH_SUMMARY,
                &error,
            );
        }
    };
    if let Some(batch_mib) = parsed.batch_mib {
        input = input.with_estimated_batch_bytes(ByteSize::from_mib(batch_mib));
    }
    let report = match plan_encoded_streaming_batches(input) {
        Ok(report) => report,
        Err(error) => {
            return emit_error(
                STREAMING_BATCH_COMMAND,
                format,
                STREAMING_BATCH_SUMMARY,
                &error,
            );
        }
    };
    emit(
        STREAMING_BATCH_COMMAND,
        format,
        if report.has_errors() {
            CommandStatus::Unsupported
        } else {
            CommandStatus::Success
        },
        "encoded streaming-batch plan".to_string(),
        report.to_human_text(),
        report.diagnostics.clone(),
        encoded_streaming_batch_plan_fields(&report, parsed.memory_gb, parsed.batch_mib),
    );
    if report.has_errors() {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

fn parse_streaming_batch_args(
    mut args: impl Iterator<Item = String>,
    format: OutputFormat,
) -> Result<StreamingBatchArgs, ExitCode> {
    let dataset_uri = next_streaming_batch_arg(&mut args)?;
    let target_uri = next_streaming_batch_arg(&mut args)?;
    let memory_gb_text = next_streaming_batch_arg(&mut args)?;
    let max_parallelism_text = next_streaming_batch_arg(&mut args)?;
    let batch_mib = parse_streaming_batch_mib(&mut args, format)?;
    let dataset_ref = parse_streaming_batch_dataset_ref(dataset_uri, format)?;
    let output_target = parse_streaming_batch_output_target(target_uri, format)?;
    let memory_gb = parse_positive_streaming_batch_u64(&memory_gb_text, "memory_gb", format)?;
    let max_parallelism =
        parse_positive_streaming_batch_usize(&max_parallelism_text, "max_parallelism", format)?;
    Ok(StreamingBatchArgs {
        dataset_ref,
        output_target,
        memory_gb,
        max_parallelism,
        batch_mib,
    })
}

fn next_streaming_batch_arg(args: &mut impl Iterator<Item = String>) -> Result<String, ExitCode> {
    if let Some(value) = args.next() {
        Ok(value)
    } else {
        eprintln!("{STREAMING_BATCH_USAGE}");
        Err(ExitCode::from(2))
    }
}

fn parse_streaming_batch_mib(
    args: &mut impl Iterator<Item = String>,
    format: OutputFormat,
) -> Result<Option<u64>, ExitCode> {
    let batch_mib = match args.next() {
        Some(value) => match value.parse::<u64>() {
            Ok(parsed) if parsed > 0 => Some(parsed),
            _ => {
                return Err(emit_error(
                    STREAMING_BATCH_COMMAND,
                    format,
                    STREAMING_BATCH_SUMMARY,
                    &ShardLoomError::InvalidOperation(
                        "batch_mib must be a positive integer".to_string(),
                    ),
                ));
            }
        },
        None => None,
    };
    if let Some(extra) = args.next() {
        return Err(emit_error(
            STREAMING_BATCH_COMMAND,
            format,
            STREAMING_BATCH_SUMMARY,
            &cli_unknown_arg_error(STREAMING_BATCH_COMMAND, &extra),
        ));
    }
    Ok(batch_mib)
}

fn parse_streaming_batch_dataset_ref(
    dataset_uri: String,
    format: OutputFormat,
) -> Result<DatasetRef, ExitCode> {
    let dataset_uri = match DatasetUri::new(dataset_uri) {
        Ok(uri) => uri,
        Err(error) => {
            return Err(emit_error(
                STREAMING_BATCH_COMMAND,
                format,
                "invalid dataset uri",
                &ShardLoomError::InvalidOperation(format!("invalid dataset uri: {error}")),
            ));
        }
    };
    let dataset_ref = match DatasetRef::from_uri(dataset_uri) {
        Ok(dataset_ref) => dataset_ref,
        Err(error) => {
            return Err(emit_error(
                STREAMING_BATCH_COMMAND,
                format,
                "failed to create dataset reference",
                &error,
            ));
        }
    };
    Ok(dataset_ref)
}

fn parse_streaming_batch_output_target(
    target_uri: String,
    format: OutputFormat,
) -> Result<OutputTarget, ExitCode> {
    let target_uri = match DatasetUri::new(target_uri) {
        Ok(uri) => uri,
        Err(error) => {
            return Err(emit_error(
                STREAMING_BATCH_COMMAND,
                format,
                "invalid target uri",
                &ShardLoomError::InvalidOperation(format!("invalid target uri: {error}")),
            ));
        }
    };
    Ok(OutputTarget::from_uri(target_uri))
}

fn parse_positive_streaming_batch_u64(
    value: &str,
    arg_name: &str,
    format: OutputFormat,
) -> Result<u64, ExitCode> {
    match value.parse() {
        Ok(parsed) if parsed > 0 => Ok(parsed),
        _ => Err(positive_streaming_batch_arg_error(arg_name, format)),
    }
}

fn parse_positive_streaming_batch_usize(
    value: &str,
    arg_name: &str,
    format: OutputFormat,
) -> Result<usize, ExitCode> {
    match value.parse() {
        Ok(parsed) if parsed > 0 => Ok(parsed),
        _ => Err(positive_streaming_batch_arg_error(arg_name, format)),
    }
}

fn positive_streaming_batch_arg_error(arg_name: &str, format: OutputFormat) -> ExitCode {
    emit_error(
        STREAMING_BATCH_COMMAND,
        format,
        STREAMING_BATCH_SUMMARY,
        &ShardLoomError::InvalidOperation(format!("{arg_name} must be a positive integer")),
    )
}

pub(crate) fn handle_backpressure_plan(
    mut args: impl Iterator<Item = String>,
    format: OutputFormat,
) -> ExitCode {
    let Some(memory_gb_text) = args.next() else {
        eprintln!("usage: shardloom backpressure-plan <memory_gb> <max_parallelism> [chunk_mib]");
        return ExitCode::from(2);
    };
    let Some(max_parallelism_text) = args.next() else {
        eprintln!("usage: shardloom backpressure-plan <memory_gb> <max_parallelism> [chunk_mib]");
        return ExitCode::from(2);
    };
    let memory_gb: u64 = match memory_gb_text.parse() {
        Ok(value) => value,
        Err(_) => {
            return emit_error(
                "backpressure-plan",
                format,
                "backpressure planning failed",
                &shardloom_core::ShardLoomError::InvalidOperation(
                    "memory_gb must be an unsigned integer".to_string(),
                ),
            );
        }
    };
    let max_parallelism: usize = match max_parallelism_text.parse() {
        Ok(value) => value,
        Err(_) => {
            return emit_error(
                "backpressure-plan",
                format,
                "backpressure planning failed",
                &shardloom_core::ShardLoomError::InvalidOperation(
                    "max_parallelism must be an unsigned integer".to_string(),
                ),
            );
        }
    };
    let chunk_mib = match args.next() {
        Some(value) => match value.parse::<u64>() {
            Ok(parsed) => Some(parsed),
            Err(_) => {
                return emit_error(
                    "backpressure-plan",
                    format,
                    "backpressure planning failed",
                    &shardloom_core::ShardLoomError::InvalidOperation(
                        "chunk_mib must be an unsigned integer".to_string(),
                    ),
                );
            }
        },
        None => None,
    };
    let memory = BoundedMemoryPolicy::required(ByteSize::from_gib(memory_gb));
    let mut input = match BackpressurePlanInput::new(memory, max_parallelism) {
        Ok(input) => input,
        Err(error) => {
            return emit_error(
                "backpressure-plan",
                format,
                "backpressure planning failed",
                &error,
            );
        }
    };
    if let Some(chunk_mib) = chunk_mib {
        input = input.with_estimated_chunk_bytes(ByteSize::from_mib(chunk_mib));
    }
    let report = match plan_backpressure(input) {
        Ok(report) => report,
        Err(error) => {
            return emit_error(
                "backpressure-plan",
                format,
                "backpressure planning failed",
                &error,
            );
        }
    };
    emit(
        "backpressure-plan",
        format,
        if report.has_errors() {
            CommandStatus::Unsupported
        } else {
            CommandStatus::Success
        },
        "backpressure plan".to_string(),
        report.to_human_text(),
        report.diagnostics.clone(),
        backpressure_plan_fields(&report),
    );
    if report.has_errors() {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

pub(crate) fn handle_runtime_plan(
    mut args: impl Iterator<Item = String>,
    format: OutputFormat,
) -> ExitCode {
    let Some(dataset_uri) = args.next() else {
        eprintln!("usage: shardloom runtime-plan <dataset_uri>");
        return ExitCode::from(2);
    };
    emit_runtime_or_task_plan("runtime-plan", "runtime plan", dataset_uri, format, false)
}

pub(crate) fn handle_task_plan(
    mut args: impl Iterator<Item = String>,
    format: OutputFormat,
) -> ExitCode {
    let Some(dataset_uri) = args.next() else {
        eprintln!("usage: shardloom task-plan <dataset_uri>");
        return ExitCode::from(2);
    };
    emit_runtime_or_task_plan("task-plan", "task plan", dataset_uri, format, true)
}

fn emit_runtime_or_task_plan(
    command: &str,
    summary: &str,
    dataset_uri: String,
    format: OutputFormat,
    graph_only: bool,
) -> ExitCode {
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
    let plan = match RuntimePlanSkeleton::for_dataset(dataset) {
        Ok(plan) => plan,
        Err(error) => {
            eprintln!("failed to build {summary}: {error}");
            return ExitCode::from(2);
        }
    };
    emit(
        command,
        format,
        CommandStatus::Success,
        summary.to_string(),
        if graph_only {
            plan.graph.summary()
        } else {
            plan.to_human_text()
        },
        if graph_only {
            vec![]
        } else {
            plan.diagnostics.clone()
        },
        vec![],
    );
    ExitCode::SUCCESS
}

pub(crate) fn handle_sizing_plan(
    mut args: impl Iterator<Item = String>,
    format: OutputFormat,
) -> ExitCode {
    let Some(dataset_uri) = args.next() else {
        eprintln!("usage: shardloom sizing-plan <dataset_uri> --memory-gb <gb>");
        return ExitCode::from(2);
    };
    let Some(memory_flag) = args.next() else {
        eprintln!("usage: shardloom sizing-plan <dataset_uri> --memory-gb <gb>");
        return ExitCode::from(2);
    };
    if memory_flag != "--memory-gb" {
        eprintln!("usage: shardloom sizing-plan <dataset_uri> --memory-gb <gb>");
        return ExitCode::from(2);
    }
    let Some(memory_gb_raw) = args.next() else {
        eprintln!("usage: shardloom sizing-plan <dataset_uri> --memory-gb <gb>");
        return ExitCode::from(2);
    };
    let memory_gb = match memory_gb_raw.parse::<u64>() {
        Ok(value) if value > 0 => value,
        _ => {
            return emit_error(
                "sizing-plan",
                format,
                "invalid memory setting",
                &shardloom_core::ShardLoomError::InvalidOperation(
                    "memory-gb must be a positive integer".to_string(),
                ),
            );
        }
    };
    let uri = match DatasetUri::new(dataset_uri) {
        Ok(uri) => uri,
        Err(error) => {
            return emit_error(
                "sizing-plan",
                format,
                "invalid dataset uri",
                &shardloom_core::ShardLoomError::InvalidOperation(format!(
                    "invalid dataset uri: {error}"
                )),
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
    let policy = AdaptiveSizingPolicy::memory_limited(ByteSize::from_gib(memory_gb));
    let sizer = AdaptiveSizer::new(policy.clone());
    let input = SizingInput::new(
        shardloom_core::SegmentId::new("placeholder-segment").expect("valid segment id"),
        SizeEstimate::unknown(),
    );
    let decision = sizer.decide_for_segment(&input);
    let parallelism = ParallelismPlan::new(ParallelismLimit::auto(), 1, 1, "planning skeleton");
    let mut plan = SizingPlan::new(policy, parallelism);
    plan.add_decision(input.segment_id.clone(), decision);
    emit(
        "sizing-plan",
        format,
        CommandStatus::Success,
        "sizing plan".to_string(),
        format!("dataset: {}\n{}", dataset.summary(), plan.to_human_text()),
        vec![],
        vec![],
    );
    ExitCode::SUCCESS
}

pub(crate) fn handle_sizing_feedback_plan(
    mut args: impl Iterator<Item = String>,
    format: OutputFormat,
) -> ExitCode {
    let Some(memory_gb_text) = args.next() else {
        eprintln!("usage: shardloom sizing-feedback-plan <memory_gb> <signals>");
        return ExitCode::from(2);
    };
    let Some(signals_raw) = args.next() else {
        eprintln!("usage: shardloom sizing-feedback-plan <memory_gb> <signals>");
        return ExitCode::from(2);
    };
    if let Some(extra) = args.next() {
        return emit_error(
            "sizing-feedback-plan",
            format,
            "dynamic sizing feedback planning failed",
            &cli_unknown_arg_error("sizing-feedback-plan", &extra),
        );
    }
    let memory_gb: u64 = match memory_gb_text.parse() {
        Ok(value) if value > 0 => value,
        _ => {
            return emit_error(
                "sizing-feedback-plan",
                format,
                "dynamic sizing feedback planning failed",
                &shardloom_core::ShardLoomError::InvalidOperation(
                    "memory_gb must be a positive integer".to_string(),
                ),
            );
        }
    };
    let signals = match parse_sizing_feedback_signals(&signals_raw) {
        Ok(signals) => signals,
        Err(error) => {
            return emit_error(
                "sizing-feedback-plan",
                format,
                "dynamic sizing feedback planning failed",
                &error,
            );
        }
    };
    let mut input = DynamicSizingFeedbackInput::new(AdaptiveSizingPolicy::memory_limited(
        ByteSize::from_gib(memory_gb),
    ));
    for signal in signals {
        input.add_signal(signal);
    }
    let report = plan_dynamic_sizing_feedback(input);
    emit(
        "sizing-feedback-plan",
        format,
        if report.has_errors() {
            CommandStatus::Unsupported
        } else {
            CommandStatus::Success
        },
        "dynamic sizing feedback plan".to_string(),
        report.to_human_text(),
        report.diagnostics.clone(),
        dynamic_sizing_feedback_fields(&report, memory_gb, &signals_raw),
    );
    if report.has_errors() {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

pub(crate) fn handle_dynamic_work_shaping_plan(
    mut args: impl Iterator<Item = String>,
    format: OutputFormat,
) -> ExitCode {
    let profile = args.next();
    if let Some(extra) = args.next() {
        return emit_error(
            "dynamic-work-shaping-plan",
            format,
            "dynamic work shaping planning failed",
            &shardloom_core::ShardLoomError::InvalidOperation(format!(
                "unknown extra dynamic-work-shaping-plan argument: {extra}"
            )),
        );
    }
    let report = match dynamic_work_shaping_report_for_profile(profile.as_deref()) {
        Ok(report) => report,
        Err(error) => {
            return emit_error(
                "dynamic-work-shaping-plan",
                format,
                "dynamic work shaping planning failed",
                &error,
            );
        }
    };
    emit(
        "dynamic-work-shaping-plan",
        format,
        if report.has_errors() {
            CommandStatus::Unsupported
        } else {
            CommandStatus::Success
        },
        "dynamic work shaping plan".to_string(),
        report.to_human_text(),
        report.diagnostics.clone(),
        dynamic_work_shaping_fields(&report),
    );
    if report.has_errors() {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

pub(crate) fn handle_dynamic_runtime_gate(format: OutputFormat) -> ExitCode {
    let report = plan_dynamic_runtime_promotion_gate();
    emit(
        "cg8-runtime-promotion-gate",
        format,
        if report.has_errors() {
            CommandStatus::Unsupported
        } else {
            CommandStatus::Success
        },
        "CG-8 runtime promotion gate".to_string(),
        report.to_human_text(),
        report.diagnostics.clone(),
        dynamic_runtime_promotion_gate_fields(&report),
    );
    if report.has_errors() {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}
