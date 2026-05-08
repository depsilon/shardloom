use std::process::Command;

const KERNEL_REGISTRY_FIELD_KEYS: [&str; 23] = [
    "fallback_execution_allowed",
    "mode",
    "status",
    "registered_kernel_count",
    "physical_kernel_schema_version",
    "physical_kernel_registry_id",
    "physical_kernel_required_slot_count",
    "physical_kernel_present_slot_count",
    "physical_kernel_missing_slot_count",
    "physical_kernel_reference_only_rejected_count",
    "physical_kernel_runtime_execution_allowed",
    "physical_kernel_fallback_execution_allowed",
    "metadata_physical_kernel_schema_version",
    "metadata_physical_kernel_supported_primitives",
    "metadata_physical_kernel_contextual_only",
    "metadata_physical_kernel_requires_correctness_evidence",
    "metadata_physical_kernel_requires_memory_safety_evidence",
    "metadata_physical_kernel_requires_benchmark_for_production",
    "metadata_physical_kernel_runtime_execution",
    "metadata_physical_kernel_fallback_execution_allowed",
    "write_io",
    "execution",
    "plan_only",
];

#[test]
fn kernel_registry_json_fields_include_physical_kernel_blockers() {
    let output = run_kernel_registry_json();
    let keys = field_keys(&output);

    assert_eq!(keys.as_slice(), KERNEL_REGISTRY_FIELD_KEYS);
    assert!(output.contains(
        "{\"key\":\"physical_kernel_schema_version\",\"value\":\"shardloom.physical_kernel_registry_plan.v1\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"physical_kernel_registry_id\",\"value\":\"cg7.1-physical-operator-foundation.kernel-registry\"}"
    ));
    assert!(output.contains("{\"key\":\"registered_kernel_count\",\"value\":\"0\"}"));
    assert!(output.contains("{\"key\":\"physical_kernel_required_slot_count\",\"value\":\"6\"}"));
    assert!(output.contains("{\"key\":\"physical_kernel_present_slot_count\",\"value\":\"0\"}"));
    assert!(output.contains("{\"key\":\"physical_kernel_missing_slot_count\",\"value\":\"6\"}"));
    assert!(
        output.contains(
            "{\"key\":\"physical_kernel_runtime_execution_allowed\",\"value\":\"false\"}"
        )
    );
    assert!(
        output.contains(
            "{\"key\":\"physical_kernel_fallback_execution_allowed\",\"value\":\"false\"}"
        )
    );
    assert!(output.contains(
        "{\"key\":\"metadata_physical_kernel_schema_version\",\"value\":\"shardloom.vortex_metadata_physical_kernel.v1\"}"
    ));
    assert!(output.contains(
        "{\"key\":\"metadata_physical_kernel_supported_primitives\",\"value\":\"count_all,count_where,filter_predicate\"}"
    ));
    assert!(
        output
            .contains("{\"key\":\"metadata_physical_kernel_contextual_only\",\"value\":\"true\"}")
    );
    assert!(output.contains(
        "{\"key\":\"metadata_physical_kernel_requires_correctness_evidence\",\"value\":\"true\"}"
    ));
    assert!(
        output.contains(
            "{\"key\":\"metadata_physical_kernel_runtime_execution\",\"value\":\"false\"}"
        )
    );
    assert!(output.contains(
        "{\"key\":\"metadata_physical_kernel_fallback_execution_allowed\",\"value\":\"false\"}"
    ));
    assert!(output.contains("\"allowed\":false"));
    assert!(output.contains("\"attempted\":false"));
}

fn run_kernel_registry_json() -> String {
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["kernel-registry", "--format", "json"])
        .output()
        .expect("shardloom binary executes");

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

    String::from_utf8(output.stdout).expect("stdout is utf8")
}

fn field_keys(output: &str) -> Vec<&str> {
    output
        .split("{\"key\":\"")
        .skip(1)
        .map(|part| {
            part.split_once('"').map_or_else(
                || panic!("field key terminator missing in {part}"),
                |(key, _)| key,
            )
        })
        .collect()
}
