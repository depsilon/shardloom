use std::collections::BTreeSet;
use std::process::Command;

use serde_json::Value;

fn metadata(args: &[&str], expected_code: i32) -> Value {
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .arg("command-metadata")
        .args(args)
        .args(["--format", "json"])
        .output()
        .expect("CLI metadata executes");
    assert_eq!(output.status.code(), Some(expected_code));
    let value: Value = serde_json::from_slice(&output.stdout).expect("stable JSON envelope");
    assert_eq!(value["schema_version"], "shardloom.output.v2");
    assert_eq!(value["command"], "command-metadata");
    assert_eq!(value["fallback"]["attempted"], false);
    assert_eq!(value["fallback"]["allowed"], false);
    value
}

fn field<'a>(value: &'a Value, key: &str) -> &'a str {
    value["result"]["fields"]
        .as_array()
        .expect("result fields")
        .iter()
        .find(|entry| entry["key"] == key)
        .and_then(|entry| entry["value"].as_str())
        .expect("requested metadata field")
}

fn commands(value: &Value) -> BTreeSet<&str> {
    let names: BTreeSet<_> = field(value, "registered_commands").split(',').collect();
    assert_eq!(
        names.len().to_string(),
        field(value, "registered_command_count")
    );
    names
}

#[test]
fn executable_metadata_partitions_audiences_and_preserves_default_inventory() {
    let all = metadata(&[], 0);
    let explicit_all = metadata(&["--audience", "all"], 0);
    let application = metadata(&["--audience", "application"], 0);
    let maintainer = metadata(&["--audience", "maintainer"], 0);
    assert_eq!(all, explicit_all);
    assert_eq!(all["status"], "success");
    let application = commands(&application);
    let maintainer = commands(&maintainer);
    assert!(application.is_disjoint(&maintainer));
    assert_eq!(
        application
            .union(&maintainer)
            .copied()
            .collect::<BTreeSet<_>>(),
        commands(&all)
    );
    assert!(application.contains("run"));
    assert!(!application.contains("python-worker"));
    assert!(maintainer.contains("python-worker"));
    let selected = metadata(&["--audience", "application", "run"], 0);
    assert_eq!(field(&selected, "selected_command"), "run");
    assert_eq!(field(&selected, "selected_command_audience"), "application");
    for args in [
        vec!["--audience", "application", "python-worker"],
        vec!["--audience", "invalid"],
        vec!["--audience"],
        vec!["--completion", "invalid"],
    ] {
        let rejected = metadata(&args, 2);
        assert_eq!(rejected["status"], "error");
        assert_eq!(
            rejected["diagnostics"]
                .as_array()
                .expect("diagnostics")
                .len(),
            1
        );
    }
}

#[test]
fn executable_completion_uses_application_audience_and_retains_json_fields() {
    for shell in ["bash", "zsh", "fish"] {
        let json = metadata(&["--completion", shell], 0);
        assert_eq!(field(&json, "completion_shell"), shell);
        assert_eq!(field(&json, "completion_audience"), "application");
        let script = field(&json, "completion_script");
        assert!(script.contains("vortex-prepare"));
        assert!(!script.contains("python-worker"));
        let text = Command::new(env!("CARGO_BIN_EXE_shardloom"))
            .args(["command-metadata", "--completion", shell])
            .output()
            .expect("completion text executes");
        assert!(text.status.success());
        assert_eq!(
            String::from_utf8(text.stdout).expect("UTF8").trim(),
            script.trim()
        );
    }
}
