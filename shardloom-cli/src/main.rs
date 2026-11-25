#[cfg(feature = "cli-mimalloc")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use std::process::ExitCode;

fn main() -> ExitCode {
    shardloom_runtime::cli_main()
}
