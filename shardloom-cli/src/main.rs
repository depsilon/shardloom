//! Command-line entry point for `ShardLoom`.
//!
//! The `CLI` remains intentionally small in setup phase and exposes basic
//! introspection commands for workspace bring-up.

use std::process::ExitCode;

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
        _ => {
            eprintln!("usage: shardloom-cli <status|capabilities|doctor>");
            ExitCode::from(2)
        }
    }
}
