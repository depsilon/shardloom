#![allow(dead_code)]

use std::process::Command;

pub fn run_command(args: &[&str], expect_success: bool) -> String {
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(args)
        .output()
        .expect("shardloom command runs");

    assert_eq!(
        output.status.success(),
        expect_success,
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        output.stderr.is_empty(),
        "stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );

    String::from_utf8(output.stdout).expect("stdout is utf8")
}

pub fn field(key: &str, value: &str) -> String {
    format!("{{\"key\":\"{key}\",\"value\":\"{value}\"}}")
}

pub fn assert_contains(output: &str, fragment: &str, case_name: &str) {
    assert!(
        output.contains(fragment),
        "{case_name}: missing fragment {fragment}\nstdout={output}"
    );
}

pub fn assert_common_typed_slots(output: &str, command: &str, status: &str) {
    assert!(output.contains("\"schema_version\":\"shardloom.output.v2\""));
    assert!(output.contains(&format!("\"command\":\"{command}\"")));
    assert!(output.contains(&format!("\"status\":\"{status}\"")));
    assert!(output.contains("\"fallback\":{\"attempted\":false,\"allowed\":false"));
    assert!(output.contains("\"diagnostics\":["));
    assert!(output.contains("\"result\":{\"fields\":["));
    assert!(output.contains("\"result_refs\":["));
    assert!(output.contains("\"artifacts\":["));
    assert!(output.contains("\"artifact_refs\":["));
    assert!(output.contains("\"certificates\":["));
    assert!(output.contains("\"policy\":{\"fields\":["));
    assert!(output.contains("\"lifecycle\":{\"fields\":["));
    assert!(output.contains("\"capability_snapshot\":{\"fields\":["));
}
