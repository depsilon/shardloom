// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 ShardLoom contributors

use std::process::Command;

pub(crate) fn small_stack(case: &'static str, run: impl FnOnce() + Send + 'static) {
    const MARKER: &str = "SHARDLOOM_ARROW_SCHEMA_SMALL_STACK";
    if std::env::var(MARKER).as_deref() != Ok(case) {
        let output = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", case, "--nocapture"])
            .env(MARKER, case)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "small-stack child failed: {}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        return;
    }
    std::thread::Builder::new()
        .stack_size(128 * 1024)
        .spawn(run)
        .unwrap()
        .join()
        .unwrap();
}
