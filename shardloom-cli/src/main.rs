//! Command-line entry point for `ShardLoom`.
//!
//! The `CLI` remains intentionally small in setup phase and exposes basic
//! introspection commands for workspace bring-up.

use std::process::ExitCode;

use shardloom_core::{
    ChangeSet, CommandStatus, DatasetManifest, DatasetRef, DatasetUri, IncrementalPlanSkeleton,
    ManifestId, OutputEnvelope, OutputFormat, OutputTarget, SnapshotId, SnapshotRef,
    TranslationPlan, WriteIntent,
};
use shardloom_exec::{
    AdaptiveSizer, AdaptiveSizingPolicy, ByteSize, ParallelismLimit, ParallelismPlan,
    RuntimePlanSkeleton, SizeEstimate, SizingInput, SizingPlan, StreamingPlanSkeleton,
};
use shardloom_plan::{EstimateReport, ExplainReport, ScanPlanSkeleton, ScanRequest};
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

#[allow(clippy::too_many_lines)]
fn run(args: Vec<String>) -> ExitCode {
    let (args, format) = match parse_output_format(args) {
        Ok(parsed) => parsed,
        Err(message) => {
            eprintln!("{message}");
            return ExitCode::from(2);
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
                    eprintln!("memory-gb must be a positive integer");
                    return ExitCode::from(2);
                }
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
                "usage: shardloom <status|capabilities|doctor|manifest-plan|incremental-plan|write-intent|scan-plan|runtime-plan|task-plan|sizing-plan|translation-plan|vortex-plan|vortex-output-plan|explain|estimate|benchmark-plan> [--format text|json]"
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
}
