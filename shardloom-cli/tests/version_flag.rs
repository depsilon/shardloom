use std::process::Command;

fn run_cli(args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(args)
        .output()
        .expect("shardloom command runs")
}

#[test]
fn version_flag_prints_cli_version_text() {
    let output = run_cli(&["--version"]);

    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        output.stderr.is_empty(),
        "stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert_eq!(
        stdout.trim(),
        format!("shardloom {}", env!("CARGO_PKG_VERSION"))
    );
}

#[test]
fn version_flag_supports_json_envelope() {
    let output = run_cli(&["--version", "--format", "json"]);

    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        output.stderr.is_empty(),
        "stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains("\"command\":\"version\""));
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&format!(
        "{{\"key\":\"cli_binary_version\",\"value\":\"{}\"}}",
        env!("CARGO_PKG_VERSION")
    )));
    assert!(stdout.contains(
        "{\"key\":\"version_source\",\"value\":\"Cargo.toml#[workspace.package].version\"}"
    ));
    assert!(stdout.contains("{\"key\":\"fallback_attempted\",\"value\":\"false\"}"));
    assert!(stdout.contains("{\"key\":\"external_engine_invoked\",\"value\":\"false\"}"));
}
