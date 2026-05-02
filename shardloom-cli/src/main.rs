//! Command-line entry point for `ShardLoom`.
//!
//! The `CLI` remains intentionally small in setup phase and exposes basic
//! introspection commands for workspace bring-up.

use std::process::ExitCode;

use shardloom_core::{
    ChangeSet, CommandStatus, CorrectnessValidationPlan, DatasetManifest, DatasetRef, DatasetUri,
    IncrementalPlanSkeleton, KernelRegistrySnapshot, ManifestId, OutputEnvelope, OutputFormat,
    OutputTarget, ShardLoomError, SnapshotId, SnapshotRef, TranslationPlan, WriteIntent,
};
use shardloom_exec::{
    AdaptiveSizer, AdaptiveSizingPolicy, ByteSize, MemoryBudget, MemoryOwner, MemoryPoolPlan,
    OomSafetyPlan, OperatorMemoryClass, ParallelismLimit, ParallelismPlan, RuntimePlanSkeleton,
    SizeEstimate, SizingInput, SizingPlan, SpillPlan, SpillPolicy, StreamingPlanSkeleton,
};
use shardloom_plan::{
    EstimateReport, ExplainReport, NativePlanDocument, OptimizerPhase, OptimizerPlanSkeleton,
    PlanExportRequest, PlanId, PlanImportRequest, PlanInteropFormat, ScanPlanSkeleton, ScanRequest,
};
use shardloom_vortex::{VortexFileRef, VortexReadPlan, VortexWriteOptions, VortexWritePlan};

fn main() -> ExitCode {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    run(args)
}

fn parse_output_format(args: Vec<String>) -> Result<(Vec<String>, OutputFormat), String> {
    let mut filtered = Vec::with_capacity(args.len());
    let mut format = OutputFormat::Text;
    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        if arg == "--format" {
            let Some(value) = iter.next() else {
                return Err("missing value for --format; expected text or json".to_string());
            };
            format = OutputFormat::parse(&value).map_err(|e| e.to_string())?;
        } else {
            filtered.push(arg);
        }
    }
    Ok((filtered, format))
}

fn detect_requested_output_format(args: &[String]) -> OutputFormat {
    let mut format = OutputFormat::Text;
    let mut iter = args.iter();
    while let Some(arg) = iter.next() {
        if arg == "--format" {
            if let Some(value) = iter.next() {
                if let Ok(parsed) = OutputFormat::parse(value) {
                    format = parsed;
                }
            } else {
                break;
            }
        }
    }
    format
}

fn emit(
    command: &str,
    format: OutputFormat,
    status: CommandStatus,
    summary: String,
    text: String,
    diagnostics: Vec<shardloom_core::Diagnostic>,
    fields: Vec<(String, String)>,
) {
    let mut envelope = OutputEnvelope::new(command, status, summary, text);
    for diagnostic in diagnostics {
        envelope.add_diagnostic(diagnostic);
    }
    for (key, value) in fields {
        envelope = envelope.with_field(key, value);
    }
    println!("{}", envelope.render(format));
}

fn emit_error(
    command: &str,
    format: OutputFormat,
    summary: &str,
    error: &ShardLoomError,
) -> ExitCode {
    let envelope = OutputEnvelope::from_error(command, summary, error);
    match format {
        OutputFormat::Text => eprintln!("{}", envelope.to_text()),
        OutputFormat::Json => println!("{}", envelope.to_json()),
    }
    ExitCode::from(2)
}

fn parse_plan_interop_format(value: &str) -> PlanInteropFormat {
    match value {
        "native" => PlanInteropFormat::ShardLoomNative,
        "agent" => PlanInteropFormat::AgentPlanSpec,
        "substrait-like" => PlanInteropFormat::SubstraitLike,
        "json-like" => PlanInteropFormat::JsonLike,
        _ => PlanInteropFormat::Unknown,
    }
}

#[allow(clippy::too_many_lines)]
fn run(args: Vec<String>) -> ExitCode {
    let requested_format = detect_requested_output_format(&args);
    let (args, format) = match parse_output_format(args) {
        Ok(parsed) => parsed,
        Err(message) => {
            return emit_error(
                "cli",
                requested_format,
                "cli argument parsing failed",
                &ShardLoomError::InvalidOperation(message),
            );
        }
    };
    let mut args = args.into_iter();

    match args.next().as_deref() {
        Some("status") => {
            let status = shardloom_exec::status();
            emit(
                "status",
                format,
                CommandStatus::Success,
                "engine status".to_string(),
                format!("{}\nfallback execution: disabled", status.summary),
                vec![],
                vec![(
                    "fallback_execution_allowed".to_string(),
                    "false".to_string(),
                )],
            );
            ExitCode::SUCCESS
        }
        Some("capabilities") => {
            let capabilities = shardloom_core::EngineCapabilities::current();
            emit(
                "capabilities",
                format,
                CommandStatus::Success,
                "engine capabilities".to_string(),
                capabilities.to_human_text(),
                vec![],
                vec![
                    (
                        "fallback_execution_allowed".to_string(),
                        "false".to_string(),
                    ),
                    ("native_input".to_string(), "vortex".to_string()),
                    ("native_output".to_string(), "vortex".to_string()),
                ],
            );
            ExitCode::SUCCESS
        }
        Some("plan-ir") => {
            let plan_id = match PlanId::new("plan-placeholder") {
                Ok(v) => v,
                Err(error) => return emit_error("plan-ir", format, "invalid plan id", &error),
            };
            let mut document = NativePlanDocument::empty(plan_id);
            document.validate_skeleton();
            emit(
                "plan-ir",
                format,
                CommandStatus::Warning,
                "native plan ir skeleton".to_string(),
                document.to_human_text(),
                document.validation.diagnostics.clone(),
                vec![
                    (
                        "fallback_execution_allowed".to_string(),
                        "false".to_string(),
                    ),
                    ("mode".to_string(), "plan_ir".to_string()),
                    ("execution".to_string(), "not_performed".to_string()),
                    ("interop_format".to_string(), "native".to_string()),
                    ("validation_required".to_string(), "true".to_string()),
                ],
            );
            ExitCode::SUCCESS
        }
        Some("plan-import") => {
            let Some(format_raw) = args.next() else {
                eprintln!("usage: shardloom plan-import <format> <source_label>");
                return ExitCode::from(2);
            };
            let Some(source_label) = args.next() else {
                eprintln!("usage: shardloom plan-import <format> <source_label>");
                return ExitCode::from(2);
            };
            let format_kind = parse_plan_interop_format(&format_raw);
            let request = match PlanImportRequest::not_implemented(format_kind, source_label) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error("plan-import", format, "invalid import request", &error);
                }
            };
            emit(
                "plan-import",
                format,
                CommandStatus::Unsupported,
                "plan import skeleton".to_string(),
                request.summary(),
                request.diagnostics.clone(),
                vec![
                    (
                        "fallback_execution_allowed".to_string(),
                        "false".to_string(),
                    ),
                    ("mode".to_string(), "plan_import".to_string()),
                    ("execution".to_string(), "not_performed".to_string()),
                    (
                        "interop_format".to_string(),
                        format_kind.as_str().to_string(),
                    ),
                    ("validation_required".to_string(), "true".to_string()),
                ],
            );
            ExitCode::from(1)
        }
        Some("plan-export") => {
            let Some(format_raw) = args.next() else {
                eprintln!("usage: shardloom plan-export <format>");
                return ExitCode::from(2);
            };
            let format_kind = parse_plan_interop_format(&format_raw);
            let request = PlanExportRequest::not_implemented(format_kind);
            emit(
                "plan-export",
                format,
                CommandStatus::Unsupported,
                "plan export skeleton".to_string(),
                request.summary(),
                request.diagnostics.clone(),
                vec![
                    (
                        "fallback_execution_allowed".to_string(),
                        "false".to_string(),
                    ),
                    ("mode".to_string(), "plan_export".to_string()),
                    ("execution".to_string(), "not_performed".to_string()),
                    (
                        "interop_format".to_string(),
                        format_kind.as_str().to_string(),
                    ),
                    ("validation_required".to_string(), "false".to_string()),
                ],
            );
            ExitCode::from(1)
        }
        Some("memory-plan") => {
            let Some(memory_gb) = args.next() else {
                eprintln!("usage: shardloom memory-plan <memory_gb>");
                return ExitCode::from(2);
            };
            let memory_gb = match memory_gb.parse::<u64>() {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        "memory-plan",
                        format,
                        "invalid memory_gb",
                        &ShardLoomError::InvalidOperation(format!("invalid memory_gb: {error}")),
                    );
                }
            };
            let budget = match MemoryBudget::from_gib(memory_gb) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error("memory-plan", format, "invalid memory budget", &error);
                }
            };
            let plan = OomSafetyPlan::new(MemoryPoolPlan::new(budget));
            emit(
                "memory-plan",
                format,
                CommandStatus::Success,
                "memory plan".to_string(),
                plan.to_human_text(),
                vec![],
                vec![("mode".to_string(), "plan_only".to_string())],
            );
            ExitCode::SUCCESS
        }
        Some("spill-plan") => {
            let Some(operator_label) = args.next() else {
                eprintln!("usage: shardloom spill-plan <operator_label> <memory_gb>");
                return ExitCode::from(2);
            };
            let Some(memory_gb) = args.next() else {
                eprintln!("usage: shardloom spill-plan <operator_label> <memory_gb>");
                return ExitCode::from(2);
            };
            let memory_gb = match memory_gb.parse::<u64>() {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        "spill-plan",
                        format,
                        "invalid memory_gb",
                        &ShardLoomError::InvalidOperation(format!("invalid memory_gb: {error}")),
                    );
                }
            };
            let budget = match MemoryBudget::from_gib(memory_gb) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error("spill-plan", format, "invalid memory budget", &error);
                }
            };
            let pool = MemoryPoolPlan::new(budget);
            let lower = operator_label.to_lowercase();
            let class = if lower.contains("sort") {
                OperatorMemoryClass::Sort
            } else if lower.contains("join") {
                OperatorMemoryClass::Join
            } else if lower.contains("agg") || lower.contains("aggregate") {
                OperatorMemoryClass::Aggregate
            } else {
                OperatorMemoryClass::Unknown
            };
            let owner = match MemoryOwner::new(class, operator_label) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error("spill-plan", format, "invalid operator label", &error);
                }
            };
            let spill_plan = SpillPlan::spill_not_implemented(owner, SpillPolicy::BestEffort);
            let mut plan = OomSafetyPlan::new(pool);
            plan.add_spill_plan(spill_plan);
            let status = if plan.has_errors() {
                CommandStatus::Unsupported
            } else {
                CommandStatus::Success
            };
            emit(
                "spill-plan",
                format,
                status,
                "spill plan".to_string(),
                plan.to_human_text(),
                vec![],
                vec![("mode".to_string(), "plan_only".to_string())],
            );
            if plan.has_errors() {
                ExitCode::from(1)
            } else {
                ExitCode::SUCCESS
            }
        }
        Some("correctness-plan") => {
            let plan = CorrectnessValidationPlan::default_foundation_plan();
            emit(
                "correctness-plan",
                format,
                CommandStatus::Success,
                "correctness validation foundation plan".to_string(),
                plan.to_human_text(),
                vec![],
                vec![
                    (
                        "fallback_execution_allowed".to_string(),
                        "false".to_string(),
                    ),
                    ("mode".to_string(), "correctness_plan".to_string()),
                    ("status".to_string(), "planned".to_string()),
                    (
                        "external_baselines".to_string(),
                        "test_oracles_only".to_string(),
                    ),
                ],
            );
            ExitCode::SUCCESS
        }
        Some("kernel-registry") => {
            let snapshot = KernelRegistrySnapshot::empty();
            emit(
                "kernel-registry",
                format,
                CommandStatus::Success,
                "kernel registry snapshot".to_string(),
                snapshot.summary(),
                vec![],
                vec![
                    (
                        "fallback_execution_allowed".to_string(),
                        "false".to_string(),
                    ),
                    ("mode".to_string(), "kernel_registry_snapshot".to_string()),
                    ("status".to_string(), "empty".to_string()),
                    ("execution".to_string(), "not_performed".to_string()),
                ],
            );
            ExitCode::SUCCESS
        }
        Some("doctor") => {
            emit("doctor", format, CommandStatus::Success, "doctor checks".to_string(), "ShardLoom doctor\nfallback execution: disabled\nnative input target: vortex\nnative output target: vortex\nstatus: early implementation skeleton".to_string(), vec![], vec![("native_input".to_string(), "vortex".to_string()), ("native_output".to_string(), "vortex".to_string())]);
            ExitCode::SUCCESS
        }
        Some("explain") => {
            let operation = args
                .next()
                .unwrap_or_else(|| "<unspecified operation>".to_string());
            let report = ExplainReport::unsupported(
                operation,
                "planning",
                "Real planning is not implemented yet.",
            );
            emit(
                "explain",
                format,
                CommandStatus::Unsupported,
                "explain plan".to_string(),
                report.to_human_text(),
                report.diagnostics.clone(),
                vec![("mode".to_string(), "plan_only".to_string())],
            );
            if report.has_errors() {
                ExitCode::from(1)
            } else {
                ExitCode::SUCCESS
            }
        }
        Some("benchmark-plan") => {
            let plan = shardloom_core::BenchmarkPlan::default_foundation_plan();
            emit(
                "benchmark-plan",
                format,
                CommandStatus::Success,
                "benchmark plan".to_string(),
                plan.to_human_text(),
                vec![],
                vec![],
            );
            ExitCode::SUCCESS
        }
        Some("manifest-plan") => {
            let Some(dataset_uri) = args.next() else {
                eprintln!("usage: shardloom manifest-plan <dataset_uri>");
                return ExitCode::from(2);
            };
            let uri = match DatasetUri::new(dataset_uri) {
                Ok(uri) => uri,
                Err(error) => {
                    return emit_error(
                        "manifest-plan",
                        format,
                        "invalid dataset uri",
                        &ShardLoomError::InvalidOperation(format!("invalid dataset uri: {error}")),
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
            let snapshot =
                SnapshotRef::new(SnapshotId::new("snapshot-placeholder").expect("valid"));
            let manifest = DatasetManifest::new(
                ManifestId::new("manifest-placeholder").expect("valid"),
                dataset,
                snapshot,
            );
            emit(
                "manifest-plan",
                format,
                CommandStatus::Success,
                "manifest plan".to_string(),
                manifest.summary(),
                vec![],
                vec![("mode".to_string(), "plan_only".to_string())],
            );
            ExitCode::SUCCESS
        }
        Some("incremental-plan") => {
            let Some(snapshot_id) = args.next() else {
                eprintln!("usage: shardloom incremental-plan <snapshot_id>");
                return ExitCode::from(2);
            };
            let snapshot_id = match SnapshotId::new(snapshot_id) {
                Ok(snapshot) => snapshot,
                Err(error) => {
                    eprintln!("invalid snapshot id: {error}");
                    return ExitCode::from(2);
                }
            };
            let change_set = ChangeSet::new(snapshot_id);
            let plan = IncrementalPlanSkeleton::from_change_set(change_set);
            emit(
                "incremental-plan",
                format,
                CommandStatus::Success,
                "incremental plan".to_string(),
                plan.to_human_text(),
                vec![],
                vec![],
            );
            ExitCode::SUCCESS
        }
        Some("write-intent") => {
            let Some(target_uri) = args.next() else {
                eprintln!("usage: shardloom write-intent <target_uri>");
                return ExitCode::from(2);
            };
            let uri = match DatasetUri::new(target_uri) {
                Ok(uri) => uri,
                Err(error) => {
                    eprintln!("invalid dataset uri: {error}");
                    return ExitCode::from(2);
                }
            };
            let intent = WriteIntent::write_not_implemented(OutputTarget::from_uri(uri));
            emit(
                "write-intent",
                format,
                CommandStatus::Unsupported,
                "write intent".to_string(),
                intent.summary(),
                intent.diagnostics.clone(),
                vec![],
            );
            if intent.has_errors() {
                ExitCode::from(1)
            } else {
                ExitCode::SUCCESS
            }
        }
        Some("scan-plan") => {
            let Some(dataset_uri) = args.next() else {
                eprintln!("usage: shardloom scan-plan <dataset_uri>");
                return ExitCode::from(2);
            };
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
            let request = ScanRequest::new(dataset);
            let skeleton = ScanPlanSkeleton::plan_only(request);
            emit(
                "scan-plan",
                format,
                if skeleton.has_errors() {
                    CommandStatus::Unsupported
                } else {
                    CommandStatus::Success
                },
                "scan plan".to_string(),
                skeleton.to_human_text(),
                skeleton.diagnostics.clone(),
                vec![("mode".to_string(), "plan_only".to_string())],
            );
            ExitCode::SUCCESS
        }
        Some("streaming-plan") => {
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
                vec![],
            );
            if plan.has_errors() {
                ExitCode::from(1)
            } else {
                ExitCode::SUCCESS
            }
        }
        Some("runtime-plan") => {
            let Some(dataset_uri) = args.next() else {
                eprintln!("usage: shardloom runtime-plan <dataset_uri>");
                return ExitCode::from(2);
            };
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
                    eprintln!("failed to build runtime plan: {error}");
                    return ExitCode::from(2);
                }
            };
            emit(
                "runtime-plan",
                format,
                CommandStatus::Success,
                "runtime plan".to_string(),
                plan.to_human_text(),
                plan.diagnostics.clone(),
                vec![],
            );
            ExitCode::SUCCESS
        }
        Some("sizing-plan") => {
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
                        &ShardLoomError::InvalidOperation(
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
                        &ShardLoomError::InvalidOperation(format!("invalid dataset uri: {error}")),
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
            let parallelism =
                ParallelismPlan::new(ParallelismLimit::auto(), 1, 1, "planning skeleton");
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
        Some("task-plan") => {
            let Some(dataset_uri) = args.next() else {
                eprintln!("usage: shardloom task-plan <dataset_uri>");
                return ExitCode::from(2);
            };
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
                    eprintln!("failed to build task plan: {error}");
                    return ExitCode::from(2);
                }
            };
            emit(
                "task-plan",
                format,
                CommandStatus::Success,
                "task plan".to_string(),
                plan.graph.summary(),
                vec![],
                vec![],
            );
            ExitCode::SUCCESS
        }
        Some("vortex-plan") => {
            let Some(dataset_uri) = args.next() else {
                eprintln!("usage: shardloom vortex-plan <dataset_uri>");
                return ExitCode::from(2);
            };
            let uri = match DatasetUri::new(dataset_uri) {
                Ok(uri) => uri,
                Err(error) => {
                    eprintln!("invalid dataset uri: {error}");
                    return ExitCode::from(2);
                }
            };
            let file_ref = match VortexFileRef::from_uri(uri) {
                Ok(file_ref) => file_ref,
                Err(error) => {
                    eprintln!("{error}");
                    return ExitCode::from(2);
                }
            };
            emit(
                "vortex-plan",
                format,
                CommandStatus::Success,
                "vortex read plan".to_string(),
                VortexReadPlan::metadata_only(file_ref).to_human_text(),
                vec![],
                vec![("mode".to_string(), "metadata_only".to_string())],
            );
            ExitCode::SUCCESS
        }
        Some("translation-plan") => {
            let Some(target_uri) = args.next() else {
                eprintln!("usage: shardloom translation-plan <target_uri>");
                return ExitCode::from(2);
            };
            let uri = match DatasetUri::new(target_uri) {
                Ok(uri) => uri,
                Err(error) => {
                    eprintln!("invalid dataset uri: {error}");
                    return ExitCode::from(2);
                }
            };
            let target = OutputTarget::from_uri(uri);
            let plan = TranslationPlan::for_target(target);
            emit(
                "translation-plan",
                format,
                if plan.has_errors() {
                    CommandStatus::Unsupported
                } else {
                    CommandStatus::Success
                },
                "translation plan".to_string(),
                plan.to_human_text(),
                plan.diagnostics.clone(),
                vec![],
            );
            if plan.has_errors() {
                ExitCode::from(1)
            } else {
                ExitCode::SUCCESS
            }
        }
        Some("vortex-output-plan") => {
            let Some(target_uri) = args.next() else {
                eprintln!("usage: shardloom vortex-output-plan <target_uri>");
                return ExitCode::from(2);
            };
            let uri = match DatasetUri::new(target_uri) {
                Ok(uri) => uri,
                Err(error) => {
                    eprintln!("invalid dataset uri: {error}");
                    return ExitCode::from(2);
                }
            };
            let file_ref = match VortexFileRef::from_uri(uri) {
                Ok(file_ref) => file_ref,
                Err(error) => {
                    eprintln!("{error}");
                    return ExitCode::from(2);
                }
            };
            emit(
                "vortex-output-plan",
                format,
                CommandStatus::Success,
                "vortex output plan".to_string(),
                VortexWritePlan::planned(file_ref, VortexWriteOptions::native_defaults())
                    .to_human_text(),
                vec![],
                vec![("target_format".to_string(), "vortex".to_string())],
            );
            ExitCode::SUCCESS
        }
        Some("optimizer-plan") => {
            let report = OptimizerPlanSkeleton::not_implemented(
                OptimizerPhase::VortexPhysical,
                "optimizer_execution",
                "ShardLoom optimizer planning skeleton exists, but real optimizer execution is not implemented yet.",
            );
            emit(
                "optimizer-plan",
                format,
                CommandStatus::Unsupported,
                "optimizer plan skeleton".to_string(),
                report.to_human_text(),
                report.diagnostics.clone(),
                vec![
                    (
                        "fallback_execution_allowed".to_string(),
                        "false".to_string(),
                    ),
                    ("mode".to_string(), "optimizer_plan".to_string()),
                    ("status".to_string(), "not_implemented".to_string()),
                    ("execution".to_string(), "not_performed".to_string()),
                    ("optimizer_phase".to_string(), "vortex_physical".to_string()),
                ],
            );
            ExitCode::from(1)
        }
        Some("estimate") => {
            let operation = args
                .next()
                .unwrap_or_else(|| "<unspecified operation>".to_string());
            let report = EstimateReport::unsupported(
                operation,
                "estimation",
                "Real estimation is not implemented yet.",
            );
            emit(
                "estimate",
                format,
                CommandStatus::Unsupported,
                "estimate plan".to_string(),
                report.to_human_text(),
                report.diagnostics.clone(),
                vec![("mode".to_string(), "plan_only".to_string())],
            );
            if report.has_errors() {
                ExitCode::from(1)
            } else {
                ExitCode::SUCCESS
            }
        }
        _ => {
            eprintln!(
                "usage: shardloom <status|capabilities|kernel-registry|doctor|manifest-plan|incremental-plan|write-intent|scan-plan|runtime-plan|task-plan|sizing-plan|translation-plan|vortex-plan|vortex-output-plan|optimizer-plan|explain|estimate|benchmark-plan|correctness-plan> [--format text|json]"
            );
            ExitCode::from(2)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn explain_unsupported_returns_non_zero() {
        let code = run(vec!["explain".to_string(), "demo-op".to_string()]);
        assert_ne!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn estimate_unsupported_returns_non_zero() {
        let code = run(vec!["estimate".to_string(), "demo-op".to_string()]);
        assert_ne!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn optimizer_plan_returns_non_zero() {
        let code = run(vec!["optimizer-plan".to_string()]);
        assert_ne!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn manifest_plan_with_dataset_uri_returns_success() {
        let code = run(vec![
            "manifest-plan".to_string(),
            "file://tmp/test.vortex".to_string(),
        ]);
        assert_eq!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn incremental_plan_with_snapshot_id_returns_success() {
        let code = run(vec!["incremental-plan".to_string(), "snap-1".to_string()]);
        assert_eq!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn write_intent_with_target_uri_returns_non_zero() {
        let code = run(vec![
            "write-intent".to_string(),
            "file://tmp/out.vortex".to_string(),
        ]);
        assert_ne!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn scan_plan_missing_dataset_uri_returns_non_zero() {
        let code = run(vec!["scan-plan".to_string()]);
        assert_ne!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn sizing_plan_with_dataset_uri_returns_success() {
        let code = run(vec![
            "sizing-plan".to_string(),
            "file://tmp/test.vortex".to_string(),
            "--memory-gb".to_string(),
            "8".to_string(),
        ]);
        assert_eq!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn scan_plan_with_dataset_uri_returns_success() {
        let code = run(vec![
            "scan-plan".to_string(),
            "file://tmp/test.vortex".to_string(),
        ]);
        assert_eq!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn translation_plan_with_vortex_uri_returns_success() {
        let code = run(vec![
            "translation-plan".to_string(),
            "file://tmp/out.vortex".to_string(),
        ]);
        assert_eq!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn translation_plan_with_unknown_uri_returns_non_zero() {
        let code = run(vec![
            "translation-plan".to_string(),
            "file://tmp/out.unknown".to_string(),
        ]);
        assert_ne!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn vortex_plan_with_vortex_uri_returns_success() {
        let code = run(vec![
            "vortex-plan".to_string(),
            "file://tmp/test.vortex".to_string(),
        ]);
        assert_eq!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn vortex_output_plan_with_vortex_uri_returns_success() {
        let code = run(vec![
            "vortex-output-plan".to_string(),
            "file://tmp/test.vortex".to_string(),
        ]);
        assert_eq!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn vortex_plan_with_non_vortex_uri_returns_non_zero() {
        let code = run(vec![
            "vortex-plan".to_string(),
            "file://tmp/test.parquet".to_string(),
        ]);
        assert_ne!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn correctness_plan_returns_success() {
        let code = run(vec!["correctness-plan".to_string()]);
        assert_eq!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn detect_requested_output_format_preserves_json_for_trailing_format_flag() {
        let args = vec![
            "status".to_string(),
            "--format".to_string(),
            "json".to_string(),
            "--format".to_string(),
        ];
        assert_eq!(detect_requested_output_format(&args), OutputFormat::Json);
    }

    #[test]
    fn plan_ir_returns_success() {
        let code = run(vec!["plan-ir".to_string()]);
        assert_eq!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn plan_import_returns_non_zero_for_not_implemented() {
        let code = run(vec![
            "plan-import".to_string(),
            "substrait-like".to_string(),
            "fixture".to_string(),
        ]);
        assert_ne!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn plan_export_returns_non_zero_for_not_implemented() {
        let code = run(vec!["plan-export".to_string(), "native".to_string()]);
        assert_ne!(code, ExitCode::SUCCESS);
    }
}
