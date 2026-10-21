// SPDX-License-Identifier: Apache-2.0
#![allow(dead_code)]

use serde_json::Value;
use sha2::Digest as _;
use std::{
    env,
    fs::{self, File},
    io::{self, Read, Write as _},
    path::{Path, PathBuf},
    process::{Command, Output},
};

pub fn workload(label: &str, scope: &str, expected_count: usize) {
    let python = required_absolute("SHARDLOOM_NATIVE_EXCHANGE_PYTHON");
    let fixtures = required_absolute("SHARDLOOM_EXCHANGE_FIXTURES");
    let base = required_absolute("SHARDLOOM_NATIVE_EXCHANGE_EVIDENCE");
    let evidence = derived_evidence(&base, label);
    let script = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crate directory has repository parent")
        .join("scripts/run_native_exchange_experiment.py");
    let executable = PathBuf::from(env!("CARGO_BIN_EXE_shardloom-native-exchange-experiment"));
    let output = Command::new(python)
        .arg(script)
        .arg("--executable")
        .arg(executable)
        .arg("--fixtures")
        .arg(fixtures)
        .arg("--evidence")
        .arg(&evidence)
        .arg("--scope")
        .arg(scope)
        .arg("--cohort")
        .arg("preflight")
        .output()
        .expect("launch native exchange workload supervisor");
    assert_process_success(&output, &format!("{label} workload"));
    let report: Value =
        serde_json::from_slice(&fs::read(&evidence).expect("read fresh workload receipt"))
            .expect("workload receipt is JSON");
    assert_eq!(report["status"], "passed", "{label}: {report}");
    let workflows = report["workflows"].as_array().expect("workflows array");
    assert_eq!(workflows.len(), expected_count, "{label}: {report}");
    for row in workflows {
        assert_eq!(row["status"], "passed", "{label}: {row}");
        assert_eq!(row["owned_processes_drained"], true, "{label}: {row}");
    }
}

pub fn failure_workload(label: &str, expected_count: usize) {
    workload(label, "failures", expected_count);
}

pub fn protocol_suite(label: &str, selectors: &[(&str, usize)]) {
    let executable = required_absolute("SHARDLOOM_NATIVE_EXCHANGE_UNIT_TESTS");
    assert!(
        executable.is_file(),
        "unit test executable does not exist: {}",
        executable.display()
    );
    let executable_arg = executable.to_string_lossy().into_owned();
    let initial = identity(&executable).expect("inspect unit test executable");
    let mut all_names = Vec::new();
    let mut runs = Vec::new();
    for (selector, expected) in selectors {
        let discovery = Command::new(&executable)
            .arg(selector)
            .arg("--list")
            .output()
            .expect("discover selected unit tests");
        assert_process_success(&discovery, &format!("discover {selector}"));
        let names = listed_tests(&discovery.stdout);
        assert_eq!(
            names.len(),
            *expected,
            "selector {selector} discovered {names:?}"
        );
        all_names.extend(names.iter().cloned());
        runs.push((
            selector,
            names,
            String::from_utf8_lossy(&discovery.stdout).into_owned(),
            String::from_utf8_lossy(&discovery.stderr).into_owned(),
        ));
    }
    assert_eq!(
        identity(&executable).unwrap(),
        initial,
        "unit binary changed during discovery"
    );
    let mut receipts = Vec::new();
    for (selector, names, discovery_stdout, discovery_stderr) in runs {
        let output = Command::new(&executable)
            .arg(selector)
            .arg("--include-ignored")
            .arg("--nocapture")
            .arg("--test-threads=1")
            .output()
            .expect("run selected unit tests");
        assert_process_success(&output, &format!("run {selector}"));
        let expected = names.len();
        let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
        let summary = format!("test result: ok. {expected} passed; 0 failed;");
        assert!(
            stdout
                .lines()
                .any(|line| line.trim_start().starts_with(&summary)),
            "selector {selector} lacked exact success summary {summary:?}: {stdout}"
        );
        receipts.push(serde_json::json!({"selector": selector, "expected_count": expected,
            "test_names": names,
            "discovery_command": [&executable_arg, selector, "--list"],
            "discovery_stdout": discovery_stdout, "discovery_stderr": discovery_stderr,
            "run_command": [&executable_arg, selector, "--include-ignored", "--nocapture", "--test-threads=1"],
            "stdout": stdout, "stderr": String::from_utf8_lossy(&output.stderr)}));
        assert_eq!(
            identity(&executable).unwrap(),
            initial,
            "unit binary changed during execution"
        );
    }
    let base = required_absolute("SHARDLOOM_NATIVE_EXCHANGE_EVIDENCE");
    let evidence = derived_evidence(&base, label);
    let receipt = serde_json::json!({"status":"passed", "unit_test_executable":initial,
        "test_names":all_names, "discovery_and_runs":receipts});
    let bytes = serde_json::to_vec_pretty(&receipt).expect("serialize protocol receipt");
    let mut file = File::options()
        .write(true)
        .create_new(true)
        .open(&evidence)
        .expect("create fresh protocol receipt");
    file.write_all(&bytes).expect("write protocol receipt");
}

fn required_absolute(name: &str) -> PathBuf {
    let value = env::var_os(name).unwrap_or_else(|| panic!("{name} is required"));
    let path = PathBuf::from(value);
    assert!(path.is_absolute(), "{name} must be an absolute path");
    path
}

fn derived_evidence(base: &Path, label: &str) -> PathBuf {
    let mut path = base.as_os_str().to_os_string();
    path.push(format!("-{label}.json"));
    let path = PathBuf::from(path);
    assert!(
        !path.exists(),
        "evidence path must be fresh: {}",
        path.display()
    );
    path
}

fn identity(path: &Path) -> io::Result<Value> {
    let before = fs::metadata(path)?;
    let mut file = File::open(path)?;
    let mut hash = sha2::Sha256::new();
    let mut buffer = [0_u8; 8 * 1024];
    loop {
        let count = file.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        hash.update(&buffer[..count]);
    }
    let after = fs::metadata(path)?;
    assert_eq!(
        (before.len(), before.modified().ok()),
        (after.len(), after.modified().ok()),
        "unit test executable metadata changed while hashing"
    );
    let sha256 = hash
        .finalize()
        .iter()
        .fold(String::with_capacity(64), |mut value, byte| {
            use std::fmt::Write as _;
            write!(value, "{byte:02x}").expect("writing into a string cannot fail");
            value
        });
    Ok(
        serde_json::json!({"path":path, "bytes":after.len(), "sha256":sha256,
        "modified":format!("{:?}", after.modified().ok())}),
    )
}

fn listed_tests(stdout: &[u8]) -> Vec<String> {
    String::from_utf8_lossy(stdout)
        .lines()
        .filter_map(|line| line.strip_suffix(": test").map(str::to_owned))
        .collect()
}

fn assert_process_success(output: &Output, label: &str) {
    assert!(
        output.status.success(),
        "{label} failed ({})\nstdout:\n{}\nstderr:\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}
