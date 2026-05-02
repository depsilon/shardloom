//! Command-line entry point for `ShardLoom`.
//!
//! The `CLI` remains intentionally small in setup phase and exposes basic
//! introspection commands for workspace bring-up.

use std::process::ExitCode;

use shardloom_core::{DatasetRef, DatasetUri, OutputTarget, TranslationPlan};
use shardloom_plan::{EstimateReport, ExplainReport, ScanPlanSkeleton, ScanRequest};
use shardloom_vortex::{VortexFileRef, VortexReadPlan, VortexWriteOptions, VortexWritePlan};

fn main() -> ExitCode {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    run(args)
}

#[allow(clippy::too_many_lines)]
fn run(args: Vec<String>) -> ExitCode {
    let mut args = args.into_iter();

    match args.next().as_deref() {
        Some("status") => {
            let status = shardloom_exec::status();
            println!("{}", status.summary);
            println!("fallback execution: disabled");
            ExitCode::SUCCESS
        }
        Some("capabilities") => {
            let capabilities = shardloom_core::EngineCapabilities::current();
            println!("{}", capabilities.to_human_text());
            ExitCode::SUCCESS
        }
        Some("doctor") => {
            println!("ShardLoom doctor");
            println!("fallback execution: disabled");
            println!("native input target: vortex");
            println!("native output target: vortex");
            println!("status: early implementation skeleton");
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
            println!("{}", report.to_human_text());
            if report.has_errors() {
                ExitCode::from(1)
            } else {
                ExitCode::SUCCESS
            }
        }
        Some("benchmark-plan") => {
            let plan = shardloom_core::BenchmarkPlan::default_foundation_plan();
            println!("{}", plan.to_human_text());
            ExitCode::SUCCESS
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
            println!("{}", skeleton.to_human_text());
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
            println!(
                "{}",
                VortexReadPlan::metadata_only(file_ref).to_human_text()
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
            println!("{}", plan.to_human_text());
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
            println!(
                "{}",
                VortexWritePlan::planned(file_ref, VortexWriteOptions::native_defaults())
                    .to_human_text()
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
            println!("{}", report.to_human_text());
            if report.has_errors() {
                ExitCode::from(1)
            } else {
                ExitCode::SUCCESS
            }
        }
        _ => {
            eprintln!(
                "usage: shardloom-cli <status|capabilities|doctor|scan-plan|translation-plan|vortex-plan|vortex-output-plan|explain|estimate|benchmark-plan>"
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
    fn scan_plan_missing_dataset_uri_returns_non_zero() {
        let code = run(vec!["scan-plan".to_string()]);
        assert_ne!(code, ExitCode::SUCCESS);
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
