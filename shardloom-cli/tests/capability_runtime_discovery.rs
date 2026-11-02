use std::{collections::BTreeMap, process::Command};

fn discover(scope: Option<&str>) -> serde_json::Value {
    let mut command = Command::new(env!("CARGO_BIN_EXE_shardloom"));
    command.arg("capabilities");
    command.args(scope);
    let output = command.args(["--format", "json"]).output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(value["status"], "success");
    assert_eq!(value["fallback"]["attempted"], false);
    value
}

fn fields(value: &serde_json::Value) -> BTreeMap<&str, &str> {
    let entries = value["fields"].as_array().unwrap();
    let fields: BTreeMap<_, _> = entries
        .iter()
        .map(|entry| {
            (
                entry["key"].as_str().unwrap(),
                entry["value"].as_str().unwrap(),
            )
        })
        .collect();
    assert_eq!(fields.len(), entries.len(), "duplicate discovery fields");
    fields
}

#[test]
fn default_and_scoped_runtime_discovery_share_feature_bound_facts() {
    let default = discover(None);
    let default_fields = fields(&default);
    let runtime: BTreeMap<_, _> = default_fields
        .iter()
        .filter(|(key, _)| key.starts_with("runtime_capabilit"))
        .map(|(&key, &value)| (key, value))
        .collect();
    assert_eq!(
        runtime["runtime_capabilities_schema_version"],
        "shardloom.runtime_capabilities.v1"
    );
    assert_eq!(
        runtime["runtime_capabilities_scope"],
        "admitted_local_execution_not_certification"
    );
    let queries = if cfg!(all(unix, feature = "vortex-local-primitives")) {
        "partially_supported"
    } else {
        "disabled"
    };
    assert_eq!(runtime["runtime_capability_frontend_sql_status"], queries);
    assert_eq!(
        runtime["runtime_capability_frontend_dataframe_api_status"],
        queries
    );
    let read = if cfg!(all(
        unix,
        any(
            feature = "vortex-local-primitives",
            feature = "vortex-encoded-read-spike"
        )
    )) {
        "partially_supported"
    } else {
        "disabled"
    };
    assert_eq!(
        runtime["runtime_capability_native_input_vortex_status"],
        read
    );
    let write = if cfg!(all(unix, feature = "vortex-write")) {
        "partially_supported"
    } else {
        "disabled"
    };
    assert_eq!(
        runtime["runtime_capability_native_output_vortex_status"],
        write
    );
    assert_eq!(
        runtime["runtime_capability_compatibility_output_iceberg_compatible_status"],
        "planned"
    );
    assert_eq!(
        runtime["runtime_capability_compatibility_output_delta_compatible_status"],
        "planned"
    );

    for scope in [
        None,
        Some("sql"),
        Some("dataframe"),
        Some("python"),
        Some("adapters"),
        Some("universal-adapters"),
        Some("certification"),
        Some("compatibility"),
    ] {
        let report = discover(scope);
        let all_fields = fields(&report);
        let scoped: BTreeMap<_, _> = all_fields
            .iter()
            .filter(|(key, _)| key.starts_with("runtime_capabilit"))
            .map(|(&key, &value)| (key, value))
            .collect();
        assert_eq!(scoped, runtime, "scope={scope:?}");
        for key in [
            "fallback_execution_allowed",
            "fallback_attempted",
            "runtime_execution",
            "filesystem_probe",
            "network_probe",
            "parser_executed",
        ] {
            assert_eq!(all_fields[key], "false", "{scope:?} {key}");
        }
        assert_eq!(all_fields["side_effect_free"], "true");
        assert_human_status(&report, &runtime);
        if scope == Some("dataframe") {
            assert_eq!(all_fields["support_status"], "report_only");
            assert!(
                report["human_text"]
                    .as_str()
                    .unwrap()
                    .contains("certification status: planned/report-only")
            );
        }
    }
}

fn assert_human_status(report: &serde_json::Value, runtime: &BTreeMap<&str, &str>) {
    let text = report["human_text"].as_str().unwrap();
    for (label, field) in [
        ("sql", "frontend_sql"),
        ("dataframe_api", "frontend_dataframe_api"),
        ("native inputs: vortex", "native_input_vortex"),
        ("native outputs: vortex", "native_output_vortex"),
    ] {
        let status = runtime[format!("runtime_capability_{field}_status").as_str()];
        assert!(text.contains(&format!("{label} ({status})")), "{text}");
    }
}
