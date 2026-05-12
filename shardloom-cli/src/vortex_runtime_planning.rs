//! Vortex runtime readiness planning CLI handlers.
//!
//! These handlers preserve existing report-only Vortex runtime planning,
//! memory, scheduling, and readiness contracts without reading data, writing
//! outputs, invoking external engines, or enabling fallback execution.

use std::process::ExitCode;

use shardloom_core::{CommandStatus, DatasetUri, OutputFormat, ShardLoomError};
use shardloom_exec::{AdaptiveSizingPolicy, ByteSize, MemoryBudget};
use shardloom_vortex::{
    build_vortex_runtime_task_graph, evaluate_vortex_execution_readiness,
    plan_native_vortex_universal_input, plan_vortex_memory_safety,
    plan_vortex_read_from_universal_input, plan_vortex_scheduler_queue,
    size_vortex_runtime_task_graph,
};

use crate::{
    adaptive_sizing_report_fields,
    cli_output::{emit, emit_error},
    memory_bridge_report_fields, readiness_is_blocked, scheduler_bridge_report_fields,
};

#[allow(clippy::too_many_lines)]
pub(crate) fn handle_vortex_adaptive_sizing(
    mut args: std::vec::IntoIter<String>,
    format: OutputFormat,
) -> ExitCode {
    let Some(dataset_uri) = args.next() else {
        eprintln!("usage: shardloom vortex-adaptive-sizing <dataset_uri> <memory_gb>");
        return ExitCode::from(2);
    };
    let Some(memory_gb_text) = args.next() else {
        eprintln!("usage: shardloom vortex-adaptive-sizing <dataset_uri> <memory_gb>");
        return ExitCode::from(2);
    };
    let uri = match DatasetUri::new(dataset_uri) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(
                "vortex-adaptive-sizing",
                format,
                "vortex adaptive sizing failed",
                &error,
            );
        }
    };
    let memory_gb: u64 = match memory_gb_text.parse() {
        Ok(v) => v,
        Err(_) => {
            return emit_error(
                "vortex-adaptive-sizing",
                format,
                "vortex adaptive sizing failed",
                &ShardLoomError::InvalidOperation(
                    "memory_gb must be an unsigned integer".to_string(),
                ),
            );
        }
    };
    let source = match shardloom_core::UniversalInputSource::from_dataset_uri(uri.clone()) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(
                "vortex-adaptive-sizing",
                format,
                "vortex adaptive sizing failed",
                &error,
            );
        }
    };
    let input_plan = match plan_native_vortex_universal_input(source) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(
                "vortex-adaptive-sizing",
                format,
                "vortex adaptive sizing failed",
                &error,
            );
        }
    };
    if input_plan.has_errors() || !input_plan.source.is_native_vortex() {
        emit(
            "vortex-adaptive-sizing",
            format,
            CommandStatus::Unsupported,
            "vortex adaptive sizing report".to_string(),
            input_plan.to_human_text(),
            input_plan.diagnostics.clone(),
            vec![
                (
                    "fallback_execution_allowed".to_string(),
                    "false".to_string(),
                ),
                ("mode".to_string(), "vortex_adaptive_sizing".to_string()),
                ("execution".to_string(), "not_performed".to_string()),
            ],
        );
        return ExitCode::from(1);
    }
    let read_report = match plan_vortex_read_from_universal_input(input_plan.clone()) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(
                "vortex-adaptive-sizing",
                format,
                "vortex adaptive sizing failed",
                &error,
            );
        }
    };
    if read_report.has_errors() {
        emit(
            "vortex-adaptive-sizing",
            format,
            CommandStatus::Unsupported,
            "vortex adaptive sizing report".to_string(),
            read_report.to_human_text(),
            read_report.diagnostics.clone(),
            vec![
                (
                    "fallback_execution_allowed".to_string(),
                    "false".to_string(),
                ),
                ("mode".to_string(), "vortex_adaptive_sizing".to_string()),
                ("execution".to_string(), "not_performed".to_string()),
            ],
        );
        return ExitCode::from(1);
    }
    let runtime_report = match build_vortex_runtime_task_graph(read_report) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(
                "vortex-adaptive-sizing",
                format,
                "vortex adaptive sizing failed",
                &error,
            );
        }
    };
    if runtime_report.has_errors() {
        emit(
            "vortex-adaptive-sizing",
            format,
            CommandStatus::Unsupported,
            "vortex adaptive sizing report".to_string(),
            runtime_report.to_human_text(),
            runtime_report.diagnostics.clone(),
            vec![
                (
                    "fallback_execution_allowed".to_string(),
                    "false".to_string(),
                ),
                ("mode".to_string(), "vortex_adaptive_sizing".to_string()),
                ("execution".to_string(), "not_performed".to_string()),
            ],
        );
        return ExitCode::from(1);
    }
    let policy = AdaptiveSizingPolicy::memory_limited(ByteSize::from_gib(memory_gb));
    let report = match size_vortex_runtime_task_graph(runtime_report, policy) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(
                "vortex-adaptive-sizing",
                format,
                "vortex adaptive sizing failed",
                &error,
            );
        }
    };
    emit(
        "vortex-adaptive-sizing",
        format,
        if report.has_errors() {
            CommandStatus::Unsupported
        } else {
            CommandStatus::Success
        },
        "vortex adaptive sizing report".to_string(),
        report.to_human_text(),
        report.diagnostics.clone(),
        adaptive_sizing_report_fields(&report, memory_gb, input_plan.source.is_native_vortex()),
    );
    if report.has_errors() {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

#[allow(clippy::too_many_lines)]
pub(crate) fn handle_vortex_memory_plan(
    mut args: std::vec::IntoIter<String>,
    format: OutputFormat,
) -> ExitCode {
    let Some(dataset_uri) = args.next() else {
        eprintln!("usage: shardloom vortex-memory-plan <dataset_uri> <memory_gb>");
        return ExitCode::from(2);
    };
    let Some(memory_gb_text) = args.next() else {
        eprintln!("usage: shardloom vortex-memory-plan <dataset_uri> <memory_gb>");
        return ExitCode::from(2);
    };
    let uri = match DatasetUri::new(dataset_uri) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(
                "vortex-memory-plan",
                format,
                "vortex memory plan failed",
                &error,
            );
        }
    };
    let memory_gb: u64 = match memory_gb_text.parse() {
        Ok(v) => v,
        Err(_) => {
            return emit_error(
                "vortex-memory-plan",
                format,
                "vortex memory plan failed",
                &ShardLoomError::InvalidOperation(
                    "memory_gb must be an unsigned integer".to_string(),
                ),
            );
        }
    };
    let source = match shardloom_core::UniversalInputSource::from_dataset_uri(uri.clone()) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(
                "vortex-memory-plan",
                format,
                "vortex memory plan failed",
                &error,
            );
        }
    };
    let input_plan = match plan_native_vortex_universal_input(source) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(
                "vortex-memory-plan",
                format,
                "vortex memory plan failed",
                &error,
            );
        }
    };
    if !input_plan.source.is_native_vortex() {
        return ExitCode::from(1);
    }
    let read_report = match plan_vortex_read_from_universal_input(input_plan.clone()) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(
                "vortex-memory-plan",
                format,
                "vortex memory plan failed",
                &error,
            );
        }
    };
    let runtime_report = match build_vortex_runtime_task_graph(read_report) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(
                "vortex-memory-plan",
                format,
                "vortex memory plan failed",
                &error,
            );
        }
    };
    let sizing_policy = AdaptiveSizingPolicy::memory_limited(ByteSize::from_gib(memory_gb));
    let sizing_report = match size_vortex_runtime_task_graph(runtime_report, sizing_policy) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(
                "vortex-memory-plan",
                format,
                "vortex memory plan failed",
                &error,
            );
        }
    };
    if sizing_report.has_errors() {
        emit(
            "vortex-memory-plan",
            format,
            CommandStatus::Unsupported,
            "vortex memory planning report".to_string(),
            sizing_report.to_human_text(),
            sizing_report.diagnostics.clone(),
            vec![
                (
                    "fallback_execution_allowed".to_string(),
                    "false".to_string(),
                ),
                ("mode".to_string(), "vortex_memory_plan".to_string()),
                ("execution".to_string(), "not_performed".to_string()),
            ],
        );
        return ExitCode::from(1);
    }
    let budget = match MemoryBudget::from_gib(memory_gb) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(
                "vortex-memory-plan",
                format,
                "vortex memory plan failed",
                &error,
            );
        }
    };
    let report = match plan_vortex_memory_safety(sizing_report, budget) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(
                "vortex-memory-plan",
                format,
                "vortex memory plan failed",
                &error,
            );
        }
    };
    emit(
        "vortex-memory-plan",
        format,
        if report.has_errors() {
            CommandStatus::Unsupported
        } else {
            CommandStatus::Success
        },
        "vortex memory planning report".to_string(),
        report.to_human_text(),
        report.diagnostics.clone(),
        memory_bridge_report_fields(&report, memory_gb, input_plan.source.is_native_vortex()),
    );
    if report.has_errors() {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

#[allow(clippy::too_many_lines)]
pub(crate) fn handle_vortex_schedule_plan(
    mut args: std::vec::IntoIter<String>,
    format: OutputFormat,
) -> ExitCode {
    let Some(dataset_uri) = args.next() else {
        eprintln!(
            "usage: shardloom vortex-schedule-plan <dataset_uri> <memory_gb> <max_parallelism>"
        );
        return ExitCode::from(2);
    };
    let Some(memory_gb_text) = args.next() else {
        eprintln!(
            "usage: shardloom vortex-schedule-plan <dataset_uri> <memory_gb> <max_parallelism>"
        );
        return ExitCode::from(2);
    };
    let Some(max_parallelism_text) = args.next() else {
        eprintln!(
            "usage: shardloom vortex-schedule-plan <dataset_uri> <memory_gb> <max_parallelism>"
        );
        return ExitCode::from(2);
    };
    let uri = match DatasetUri::new(dataset_uri) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(
                "vortex-schedule-plan",
                format,
                "vortex schedule plan failed",
                &error,
            );
        }
    };
    let memory_gb: u64 = match memory_gb_text.parse() {
        Ok(v) => v,
        Err(_) => {
            return emit_error(
                "vortex-schedule-plan",
                format,
                "vortex schedule plan failed",
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
                "vortex-schedule-plan",
                format,
                "vortex schedule plan failed",
                &ShardLoomError::InvalidOperation(
                    "max_parallelism must be an unsigned integer".to_string(),
                ),
            );
        }
    };
    let source = match shardloom_core::UniversalInputSource::from_dataset_uri(uri) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(
                "vortex-schedule-plan",
                format,
                "vortex schedule plan failed",
                &error,
            );
        }
    };
    let input_plan = match plan_native_vortex_universal_input(source) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(
                "vortex-schedule-plan",
                format,
                "vortex schedule plan failed",
                &error,
            );
        }
    };
    if input_plan.has_errors() || !input_plan.source.is_native_vortex() {
        return ExitCode::from(1);
    }
    let read_report = match plan_vortex_read_from_universal_input(input_plan.clone()) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(
                "vortex-schedule-plan",
                format,
                "vortex schedule plan failed",
                &error,
            );
        }
    };
    if read_report.has_errors() {
        return ExitCode::from(1);
    }
    let runtime_report = match build_vortex_runtime_task_graph(read_report) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(
                "vortex-schedule-plan",
                format,
                "vortex schedule plan failed",
                &error,
            );
        }
    };
    if runtime_report.has_errors() {
        return ExitCode::from(1);
    }
    let sizing_policy = AdaptiveSizingPolicy::memory_limited(ByteSize::from_gib(memory_gb));
    let sizing_report = match size_vortex_runtime_task_graph(runtime_report, sizing_policy) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(
                "vortex-schedule-plan",
                format,
                "vortex schedule plan failed",
                &error,
            );
        }
    };
    if sizing_report.has_errors() {
        return ExitCode::from(1);
    }
    let budget = match MemoryBudget::from_gib(memory_gb) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(
                "vortex-schedule-plan",
                format,
                "vortex schedule plan failed",
                &error,
            );
        }
    };
    let memory_report = match plan_vortex_memory_safety(sizing_report, budget) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(
                "vortex-schedule-plan",
                format,
                "vortex schedule plan failed",
                &error,
            );
        }
    };
    if memory_report.has_errors() {
        return ExitCode::from(1);
    }
    let report = match plan_vortex_scheduler_queue(memory_report, max_parallelism) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(
                "vortex-schedule-plan",
                format,
                "vortex schedule plan failed",
                &error,
            );
        }
    };
    emit(
        "vortex-schedule-plan",
        format,
        if report.has_errors() {
            CommandStatus::Unsupported
        } else {
            CommandStatus::Success
        },
        "vortex scheduler queue planning report".to_string(),
        report.to_human_text(),
        report.diagnostics.clone(),
        scheduler_bridge_report_fields(&report, memory_gb, max_parallelism),
    );
    if report.has_errors() {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

#[allow(clippy::too_many_lines)]
pub(crate) fn handle_vortex_execution_readiness(
    mut args: std::vec::IntoIter<String>,
    format: OutputFormat,
) -> ExitCode {
    let is_dry_run = false;
    let command = "vortex-execution-readiness";
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
            return emit_error(command, format, "vortex readiness planning failed", &error);
        }
    };
    let memory_gb: u64 = match memory_gb_text.parse() {
        Ok(v) => v,
        Err(_) => {
            return emit_error(
                command,
                format,
                "vortex readiness planning failed",
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
                "vortex readiness planning failed",
                &ShardLoomError::InvalidOperation(
                    "max_parallelism must be an unsigned integer".to_string(),
                ),
            );
        }
    };
    let source = match shardloom_core::UniversalInputSource::from_dataset_uri(uri) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(command, format, "vortex readiness planning failed", &error);
        }
    };
    let input_plan = match plan_native_vortex_universal_input(source) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(command, format, "vortex readiness planning failed", &error);
        }
    };
    if input_plan.has_errors() || !input_plan.source.is_native_vortex() {
        return ExitCode::from(1);
    }
    let read_report = match plan_vortex_read_from_universal_input(input_plan) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(command, format, "vortex readiness planning failed", &error);
        }
    };
    let runtime_report = match build_vortex_runtime_task_graph(read_report) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(command, format, "vortex readiness planning failed", &error);
        }
    };
    let sizing_report = match size_vortex_runtime_task_graph(
        runtime_report,
        AdaptiveSizingPolicy::memory_limited(ByteSize::from_gib(memory_gb)),
    ) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(command, format, "vortex readiness planning failed", &error);
        }
    };
    let budget = match MemoryBudget::from_gib(memory_gb) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(command, format, "vortex readiness planning failed", &error);
        }
    };
    let memory_report = match plan_vortex_memory_safety(sizing_report, budget) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(command, format, "vortex readiness planning failed", &error);
        }
    };
    let scheduler_report = match plan_vortex_scheduler_queue(memory_report, max_parallelism) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(command, format, "vortex readiness planning failed", &error);
        }
    };
    let readiness_report = match evaluate_vortex_execution_readiness(scheduler_report) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(command, format, "vortex readiness planning failed", &error);
        }
    };
    let text = if is_dry_run {
        readiness_report.dry_run_contract.to_human_text()
    } else {
        readiness_report.to_human_text()
    };
    emit(
        command,
        format,
        if readiness_report.has_errors() || readiness_is_blocked(readiness_report.status) {
            CommandStatus::Unsupported
        } else {
            CommandStatus::Success
        },
        if is_dry_run {
            "vortex dry-run contract".to_string()
        } else {
            "vortex execution readiness report".to_string()
        },
        text,
        readiness_report.diagnostics.clone(),
        vec![
            (
                "fallback_execution_allowed".to_string(),
                "false".to_string(),
            ),
            (
                "mode".to_string(),
                if is_dry_run {
                    "vortex_dry_run".to_string()
                } else {
                    "vortex_execution_readiness".to_string()
                },
            ),
            ("plan_only".to_string(), "true".to_string()),
            ("dry_run_only".to_string(), "true".to_string()),
            ("tasks_executed".to_string(), "false".to_string()),
            ("data_executed".to_string(), "false".to_string()),
            ("data_read".to_string(), "false".to_string()),
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
    if readiness_report.has_errors() || readiness_is_blocked(readiness_report.status) {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}
