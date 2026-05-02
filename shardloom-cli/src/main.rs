//! Command-line entry point for `ShardLoom`.
//!
//! The `CLI` remains intentionally small in setup phase and exposes basic
//! introspection commands for workspace bring-up.

use std::process::ExitCode;

use shardloom_core::{DatasetRef, DatasetUri};
use shardloom_plan::{EstimateReport, ExplainReport, ScanPlanSkeleton, ScanRequest};

fn main() -> ExitCode {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    run(args)
}

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
            let dataset_uri = args
                .next()
                .unwrap_or_else(|| "<unspecified dataset_uri>".to_string());
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
                "usage: shardloom-cli <status|capabilities|doctor|scan-plan|explain|estimate|benchmark-plan>"
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
}
