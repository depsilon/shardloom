//! Command-line entry point for `ShardLoom`.
//!
//! The `CLI` remains intentionally small in setup phase and exposes basic
//! introspection commands for workspace bring-up.

use std::process::ExitCode;

fn main() -> ExitCode {
    let mut args = std::env::args();
    let _bin = args.next();

    if let Some("status") = args.next().as_deref() {
        let status = shardloom_exec::status();
        println!("{}", status.summary);
        ExitCode::SUCCESS
    } else {
        eprintln!("usage: shardloom-cli status");
        ExitCode::from(2)
    }
}
