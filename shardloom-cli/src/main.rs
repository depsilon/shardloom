//! Command-line entry point for `ShardLoom`.
//!
//! The `CLI` remains intentionally small in setup phase and exposes basic
//! introspection commands for workspace bring-up.

use std::process::ExitCode;

use shardloom_plan::{EstimateReport, ExplainReport};

fn main() -> ExitCode {
    let mut args = std::env::args();
    let _bin = args.next();

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
            ExitCode::SUCCESS
        }
        _ => {
            eprintln!("usage: shardloom-cli <status|capabilities|doctor|explain|estimate>");
            ExitCode::from(2)
        }
    }
}
