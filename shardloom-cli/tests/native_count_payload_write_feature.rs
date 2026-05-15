#![cfg(feature = "vortex-write")]

use std::process::Command;

fn field(key: &str, value: &str) -> String {
    format!("{{\"key\":\"{key}\",\"value\":\"{value}\"}}")
}

#[test]
fn feature_enabled_cli_writes_native_count_payload() {
    let unique = format!(
        "shardloom-cli-vortex-write-feature-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let root = std::env::temp_dir().join(unique);
    let workspace = root.join("stage");
    std::fs::create_dir_all(&workspace).unwrap();
    let workspace_arg = workspace.to_string_lossy().into_owned();

    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "vortex-native-count-payload-write",
            "file://tmp/out.vortex",
            workspace_arg.as_str(),
            "42",
            "write-intent-ready,staged-output-ready,finalized-manifest-ready,payload-content-available,local-workspace,feature-gate-enabled",
            "none",
            "--format",
            "json",
        ])
        .output()
        .expect("vortex native count payload write command runs");

    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    let stderr = String::from_utf8(output.stderr).expect("stderr is utf8");
    assert!(output.status.success(), "stdout={stdout} stderr={stderr}");
    assert!(stderr.is_empty(), "stderr={stderr}");
    assert!(stdout.contains(&field("feature_enabled", "true")));
    assert!(stdout.contains(&field("native_vortex_payload_written", "true")));
    assert!(stdout.contains(&field("output_payload_written", "true")));
    assert!(stdout.contains(&field("vortex_file_written", "true")));
    assert!(stdout.contains(&field("upstream_vortex_write_called", "true")));
    assert!(stdout.contains(&field("fallback_execution_allowed", "false")));
    assert!(stdout.contains(&field("object_store_io", "false")));
    assert!(workspace.join("_shardloom_output_payload.vortex").exists());

    let _ = std::fs::remove_file(workspace.join("_shardloom_output_payload.vortex"));
    let _ = std::fs::remove_dir(&workspace);
    let _ = std::fs::remove_dir(&root);
}
